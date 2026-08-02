use crate::download::config::DownloadConfig;
use crate::download::mmap_writer::MmapWriter;
use crate::download::rate_limit::TokenBucket;
use crate::download::resume::{ResumeState, SegmentSnapshot};
use crate::download::scheduler::WorkStealingScheduler;
use crate::download::source::SourceManager;
use crate::download::worker::DownloadWorker;
use anyhow::{anyhow, bail, Context, Result};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_LENGTH, RANGE};
use reqwest::{Client, StatusCode};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy)]
pub struct Progress {
    pub total: u64,
    pub completed: u64,
    pub speed: f64,
    pub connections: usize,
}

#[derive(Debug)]
pub struct DownloadReport {
    pub path: PathBuf,
    pub total: u64,
    pub elapsed: Duration,
}

struct ProbeInfo {
    total: u64,
    supports_range: bool,
}

/// 后台下载句柄：pause/resume/cancel/join。
#[allow(dead_code)]
pub struct DownloadHandle {
    scheduler: Arc<WorkStealingScheduler>,
    limiter: TokenBucket,
    cancel_token: CancellationToken,
    join: tokio::task::JoinHandle<Result<DownloadReport>>,
    pub path: PathBuf,
    pub total: u64,
}

impl DownloadHandle {
    pub fn scheduler_ref(&self) -> Arc<WorkStealingScheduler> {
        Arc::clone(&self.scheduler)
    }

    #[allow(dead_code)]
    pub fn limiter_ref(&self) -> TokenBucket {
        self.limiter.clone()
    }

    pub fn cancel_token_ref(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    #[allow(dead_code)]
    pub fn pause(&self) {
        self.scheduler.set_paused(true);
    }

    #[allow(dead_code)]
    pub fn resume(&self) {
        self.scheduler.set_paused(false);
    }

    #[allow(dead_code)]
    pub fn is_paused(&self) -> bool {
        self.scheduler.is_paused()
    }

    /// 优雅取消：worker 在当前块结束后停止，控制文件保留以便续传。
    #[allow(dead_code)]
    pub fn cancel(&self) {
        self.cancel_token.cancel();
        self.scheduler.set_paused(false); // 唤醒被暂停阻塞的 worker
    }

    /// 强制终止后台任务。
    #[allow(dead_code)]
    pub fn abort(&self) {
        self.join.abort();
    }

    #[allow(dead_code)]
    pub async fn progress(&self) -> Progress {
        let completed = self.scheduler.completed_bytes().await;
        Progress {
            total: self.total,
            completed,
            speed: 0.0,
            connections: self.scheduler.active_connections().await,
        }
    }

    pub async fn join(self) -> Result<DownloadReport> {
        match self.join.await {
            Ok(r) => r,
            Err(e) => {
                if e.is_cancelled() {
                    bail!("任务已被取消")
                } else {
                    Err(anyhow!("下载任务异常退出: {e}"))
                }
            }
        }
    }
}

async fn probe(
    client: &Client,
    url: &str,
    cancel: &CancellationToken,
    timeout: u64,
) -> Result<ProbeInfo> {
    // 用 GET Range: bytes=0-0 权威探测总长与 Range 支持
    // （有些服务器 HEAD 谎报 Accept-Ranges，必须实测）
    let req = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .header(ACCEPT_ENCODING, "identity")
        // 探测只读响应头，可以设总超时（下载阶段不用总超时，见 build_client）
        .timeout(Duration::from_secs(timeout.max(5)));
    let resp = tokio::select! {
        r = req.send() => r.context("探测请求失败")?,
        _ = cancel.cancelled() => bail!("下载已取消"),
    };

    if resp.status() == StatusCode::PARTIAL_CONTENT {
        let content_range = resp
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| anyhow!("206 响应缺少 Content-Range"))?;
        // bytes 0-0/12345
        let total = content_range
            .split('/')
            .nth(1)
            .and_then(|s| s.trim().parse::<u64>().ok())
            .ok_or_else(|| anyhow!("Content-Range 解析失败: {content_range}"))?;
        if total == 0 {
            bail!("文件大小为 0");
        }
        Ok(ProbeInfo {
            total,
            supports_range: true,
        })
    } else if resp.status() == StatusCode::OK {
        // HEAD 作为总长兜底
        let head_len = client
            .head(url)
            .timeout(Duration::from_secs(timeout.max(5)))
            .send()
            .await
            .ok()
            .and_then(|r| {
                r.headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string())
            })
            .and_then(|s| s.parse::<u64>().ok());
        let total = resp
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .or(head_len)
            .ok_or_else(|| anyhow!("无法获取文件大小"))?;
        if total == 0 {
            bail!("文件大小为 0");
        }
        Ok(ProbeInfo {
            total,
            supports_range: false,
        })
    } else {
        bail!("探测失败: HTTP {}", resp.status())
    }
}

/// 从 URL 提取文件名（百分号解码）。
pub fn filename_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    if path.ends_with('/') {
        return "download.bin".to_string();
    }
    let name = path.rsplit('/').next().unwrap_or("").to_string();
    if name.is_empty() {
        return "download.bin".to_string();
    }
    percent_decode(&name)
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_val(bytes[i + 1]);
            let lo = hex_val(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn build_client(config: &DownloadConfig) -> Result<Client> {
    let ua = if config.user_agent.trim().is_empty() {
        crate::download::config::default_user_agent()
    } else {
        config.user_agent.clone()
    };
    let mut builder = Client::builder().user_agent(ua);
    if !config.referer.trim().is_empty() || !config.header.is_empty() {
        let mut headers = reqwest::header::HeaderMap::new();
        if !config.referer.trim().is_empty() {
            if let Ok(v) = reqwest::header::HeaderValue::from_str(config.referer.trim()) {
                headers.insert(reqwest::header::REFERER, v);
            }
        }
        for h in &config.header {
            let Some((name, value)) = h.split_once(':') else {
                tracing::warn!("忽略无效的请求头（缺少冒号）: {h}");
                continue;
            };
            let name = name.trim();
            let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) else {
                tracing::warn!("忽略无效的请求头名称: {h}");
                continue;
            };
            if let Ok(v) = reqwest::header::HeaderValue::from_str(value.trim()) {
                headers.append(name, v);
            } else {
                tracing::warn!("忽略无效的请求头值: {h}");
            }
        }
        builder = builder.default_headers(headers);
    }
    builder
        // 不能用总超时（timeout）：那会在 30s 掐断所有耗时更长的下载。
        // 改用 read_timeout（每次成功读后重置），只检测停滞的连接。
        .read_timeout(Duration::from_secs(config.timeout.max(5)))
        .connect_timeout(Duration::from_secs(config.timeout.max(5)))
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("HTTP 客户端构建失败")
}

/// 探测并启动后台下载。返回句柄后可 pause/resume/cancel/join。
/// `cancel_token` 同时用于探测阶段与下载阶段；探测期间取消会立即中止。
/// `limiter` 提供共享限速器（None 则按 config.rate_limit 新建）。
/// `on_progress` 约每秒回调一次（在后台任务中执行）。
pub async fn start_download(
    urls: Vec<String>,
    out_path: Option<PathBuf>,
    config: DownloadConfig,
    sha256: Option<String>,
    limiter: Option<TokenBucket>,
    cancel_token: CancellationToken,
    on_progress: impl Fn(Progress) + Send + Sync + 'static,
) -> Result<DownloadHandle> {
    if urls.is_empty() {
        bail!("未提供下载地址");
    }
    let client = build_client(&config)?;
    let client = Arc::new(client);

    // 探测：逐个源尝试，第一个成功的作为主源；失败源禁用
    let mut probe_info: Option<ProbeInfo> = None;
    let mut failed_urls: Vec<String> = Vec::new();
    for url in &urls {
        if cancel_token.is_cancelled() {
            bail!("下载已取消");
        }
        match probe(&client, url, &cancel_token, config.timeout).await {
            Ok(info) => {
                probe_info = Some(info);
                break;
            }
            Err(e) => {
                tracing::warn!("源 {url} 探测失败: {e:#}");
                failed_urls.push(url.clone());
            }
        }
    }
    let probe = probe_info.ok_or_else(|| anyhow!("所有下载源均不可用"))?;
    let mut config = config;
    if !probe.supports_range {
        config.split = 1;
    }

    let path = match out_path {
        Some(p) => {
            if let Some(parent) = p.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("创建目录失败: {}", parent.display()))?;
                }
            }
            p
        }
        None => PathBuf::from(filename_from_url(&urls[0])),
    };

    let control_path = ResumeState::control_path(&path);

    // 续传前提：磁盘文件已存在、长度与探测一致，且服务器支持 Range。
    // 若输出文件被删而 .gkdl 残留，直接续传会用全 0 占位并跳过 Complete 段 → 损坏；
    // 无 Range 支持时 worker 只能整文件从头下载，无法按偏移续传。
    let file_ok = std::fs::metadata(&path)
        .map(|m| m.len() == probe.total)
        .unwrap_or(false);

    // 尝试续传
    let mut resumed = false;
    let scheduler = if control_path.exists() && probe.supports_range && file_ok {
        match ResumeState::load(&control_path) {
            Ok(state) if state.total == probe.total && state.urls == urls => {
                let segments: Vec<crate::download::segment::Segment> = state
                    .segments
                    .iter()
                    .map(|snap| {
                        let mut s = crate::download::segment::Segment::new(
                            0,
                            snap.start,
                            snap.end,
                            config.speed_window,
                        );
                        s.written = snap.written;
                        s.state = if snap.complete {
                            crate::download::segment::SegmentState::Complete
                        } else {
                            crate::download::segment::SegmentState::Pending
                        };
                        s
                    })
                    .collect();
                let completed_start: u64 = segments
                    .iter()
                    .filter(|s| s.state == crate::download::segment::SegmentState::Complete)
                    .map(|s| s.written)
                    .sum();
                resumed = true;
                tracing::info!(
                    "检测到续传控制文件，从 {}/{} bytes 继续",
                    completed_start,
                    probe.total
                );
                Arc::new(WorkStealingScheduler::from_resume(segments, config.clone()))
            }
            _ => {
                tracing::warn!("控制文件无效，重新开始下载");
                Arc::new(WorkStealingScheduler::new(probe.total, config.clone()))
            }
        }
    } else {
        if control_path.exists() {
            tracing::warn!("无法续传（文件缺失/大小不符或无 Range 支持），重新开始下载");
        }
        Arc::new(WorkStealingScheduler::new(probe.total, config.clone()))
    };

    tracing::info!(
        "开始下载: {} -> {} ({} bytes, {} 线程{})",
        urls[0],
        path.display(),
        probe.total,
        config.split,
        if resumed { ", 续传模式" } else { "" }
    );

    let writer = if resumed {
        Arc::new(MmapWriter::resume(&path, probe.total)?)
    } else {
        Arc::new(MmapWriter::new(&path, probe.total)?)
    };
    let source_mgr = Arc::new(SourceManager::new(&urls));
    for u in &failed_urls {
        source_mgr.disable(u);
    }
    let limiter = match limiter {
        Some(l) => l,
        None => TokenBucket::new(config.rate_limit),
    };
    let limiter_task = limiter.clone();

    let total = probe.total;
    let supports_range = probe.supports_range;
    let start = Instant::now();

    // 后台任务：监控 + 保存控制文件 + workers + 校验
    let sched_task = Arc::clone(&scheduler);
    let writer_task = Arc::clone(&writer);
    let source_mgr_task = Arc::clone(&source_mgr);
    let client_task = Arc::clone(&client);
    let cancel_task = cancel_token.clone();
    let control_path_task = control_path.clone();
    let path_task = path.clone();
    let urls_task = urls.clone();
    let sha256_task = sha256.clone();
    let cfg_task = config.clone();

    let join = tokio::spawn(async move {
        // 进度监控 + 定期保存控制文件
        let sched_clone = sched_task.clone();
        let start_clone = start;
        let control_path_clone = control_path_task.clone();
        let urls_clone = urls_task.clone();
        let sha256_clone = sha256_task.clone();
        let cfg_clone = cfg_task.clone();
        let out_path_str = path_task.display().to_string();
        let cancel_clone = cancel_task.clone();
        let monitor = tokio::spawn(async move {
            // 立即上报一次初始进度（总长已知）
            on_progress(Progress {
                total,
                completed: sched_clone.completed_bytes().await,
                speed: 0.0,
                connections: sched_clone.active_connections().await,
            });
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if cancel_clone.is_cancelled() || sched_clone.all_done().await {
                    return;
                }
                let completed = sched_clone.completed_bytes().await;
                let elapsed = start_clone.elapsed().as_secs_f64();
                let speed = if elapsed > 0.0 {
                    completed as f64 / elapsed
                } else {
                    0.0
                };
                on_progress(Progress {
                    total,
                    completed,
                    speed,
                    connections: sched_clone.active_connections().await,
                });

                // 保存控制文件
                let segments: Vec<SegmentSnapshot> = sched_clone
                    .segments()
                    .await
                    .iter()
                    .map(|s| SegmentSnapshot {
                        start: s.start,
                        end: s.end,
                        written: s.written,
                        complete: s.state == crate::download::segment::SegmentState::Complete,
                    })
                    .collect();
                let state = ResumeState {
                    version: 1,
                    urls: urls_clone.clone(),
                    total,
                    out_path: out_path_str.clone(),
                    sha256: sha256_clone.clone(),
                    rate_limit: cfg_clone.rate_limit,
                    split: cfg_clone.split,
                    min_split_size: cfg_clone.min_split_size,
                    segments,
                    updated_at: chrono::Utc::now().timestamp(),
                };
                if let Err(e) = state.save(&control_path_clone) {
                    tracing::warn!("保存控制文件失败: {}", e);
                }
            }
        });

        // 启动 workers
        let mut handles = Vec::new();
        for i in 0..cfg_task.split {
            let worker = DownloadWorker::new(
                i,
                Arc::clone(&client_task),
                Arc::clone(&source_mgr_task),
                Arc::clone(&sched_task),
                Arc::clone(&writer_task),
                limiter_task.clone(),
                cfg_task.clone(),
                supports_range,
                cancel_task.clone(),
            );
            handles.push(tokio::spawn(async move { worker.run().await }));
        }

        for h in handles {
            h.await.context("worker 协程异常退出")?;
        }
        monitor.abort();

        if !sched_task.all_done().await {
            let failed = sched_task
                .segments()
                .await
                .iter()
                .filter(|s| s.state == crate::download::segment::SegmentState::Cancelled)
                .count();
            bail!(
                "下载失败: {} 字节未完成（{} 个段失败）",
                sched_task.total_remaining().await,
                failed
            );
        }

        writer_task.flush_all()?;
        drop(writer_task); // 释放 mmap，便于读取校验

        // SHA-256 校验
        if let Some(expected) = &sha256_task {
            tracing::info!("正在校验 SHA-256...");
            let result = crate::hash::verify_sha256(&path_task, expected);
            if let Err(e) = result {
                bail!("{e:#}");
            }
            tracing::info!("SHA-256 校验通过");
        }

        // 成功后删除控制文件
        if control_path_task.exists() {
            if let Err(e) = std::fs::remove_file(&control_path_task) {
                tracing::warn!("删除控制文件失败: {}", e);
            }
        }

        let report = DownloadReport {
            path: path_task,
            total,
            elapsed: start.elapsed(),
        };
        tracing::info!(
            "下载完成: {} ({} bytes, 耗时 {:.2}s)",
            report.path.display(),
            report.total,
            report.elapsed.as_secs_f64()
        );
        Ok(report)
    });

    Ok(DownloadHandle {
        scheduler,
        limiter,
        cancel_token,
        join,
        path,
        total,
    })
}

/// 阻塞式下载（CLI 用）。
#[allow(dead_code)]
pub async fn download(
    urls: Vec<String>,
    out_path: Option<PathBuf>,
    config: DownloadConfig,
    sha256: Option<String>,
    on_progress: impl Fn(Progress) + Send + Sync + 'static,
) -> Result<DownloadReport> {
    let handle = start_download(
        urls,
        out_path,
        config,
        sha256,
        None,
        CancellationToken::new(),
        on_progress,
    )
    .await?;
    handle.join().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_extraction() {
        assert_eq!(filename_from_url("https://a.com/b/c.zip"), "c.zip");
        assert_eq!(filename_from_url("https://a.com/b/c.zip?x=1"), "c.zip");
        assert_eq!(filename_from_url("https://a.com/b/"), "download.bin");
        assert_eq!(filename_from_url("https://a.com/%E6%B5%8B.txt"), "测.txt");
    }
}
