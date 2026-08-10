use crate::app::db::{DownloadRecord, SegmentRecord, StateDb};
use crate::download::config::DownloadConfig;
use crate::download::curl::{HttpClient, ProbeInfo};
use crate::download::pwrite_writer::PwriteWriter;
use crate::download::rate_limit::TokenBucket;
use crate::download::scheduler::WorkStealingScheduler;
use crate::download::segment::{Segment, SegmentState};
use crate::download::source::SourceManager;
use crate::download::worker::DownloadWorker;
use crate::download::writer::{DownloadWriter, MemoryWriter, StreamingWriter};
use anyhow::{anyhow, bail, Context, Result};
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
    client: &HttpClient,
    url: &str,
    cancel: &CancellationToken,
    timeout: u64,
) -> Result<ProbeInfo> {
    // 用 GET Range: bytes=0-0 权威探测总长与 Range 支持
    // （有些服务器 HEAD 谎报 Accept-Ranges，必须实测）
    client.probe(url, cancel, timeout).await
}

/// 判定是否走「整文件流式 + gzip/deflate」路径：仅当允许压缩且该下载无法/无需分段时。
pub fn should_use_streaming(probe: &ProbeInfo, config: &DownloadConfig) -> bool {
    if !config.allow_compression {
        return false;
    }
    !probe.supports_range
        || !probe.total_known
        || probe.total < config.min_split_size.max(1).saturating_mul(2)
}

/// 从 URL 提取文件名（百分号解码 + 路径分隔符清洗）。
pub fn filename_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    if path.ends_with('/') {
        return "download.bin".to_string();
    }
    let name = path.rsplit('/').next().unwrap_or("").to_string();
    if name.is_empty() {
        return "download.bin".to_string();
    }
    sanitize_filename(&percent_decode(&name))
}

/// 清洗从 URL 推导的文件名。百分号解码可能重新引入路径分隔符（`%2F`、`%5C`），
/// 或直接得到 `..`；这类名字用作输出路径会逃逸到目标目录之外，统一替换/兜底。
fn sanitize_filename(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        match c {
            '/' | '\\' => out.push('_'),
            _ => out.push(c),
        }
    }
    if out == "." || out == ".." {
        return "download.bin".into();
    }
    out
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

fn build_client(config: &DownloadConfig) -> Result<HttpClient> {
    HttpClient::new(config)
}

/// 构造持久化任务记录（写入状态库）。
#[allow(clippy::too_many_arguments)]
fn make_record(
    gid: &str,
    urls: &[String],
    total: u64,
    out_path: &str,
    sha256: &Option<String>,
    cfg: &DownloadConfig,
    memory_mode: bool,
    status: &str,
    segments: Vec<SegmentRecord>,
) -> DownloadRecord {
    let now = chrono::Utc::now().timestamp();
    DownloadRecord {
        gid: gid.to_string(),
        urls: urls.to_vec(),
        total,
        out_path: out_path.to_string(),
        sha256: sha256.clone(),
        rate_limit: cfg.rate_limit,
        split: cfg.split,
        min_split_size: cfg.min_split_size,
        memory_mode,
        status: status.to_string(),
        created_at: now,
        updated_at: now,
        segments,
    }
}

/// 探测并启动后台下载。返回句柄后可 pause/resume/cancel/join。
/// `cancel_token` 同时用于探测阶段与下载阶段；探测期间取消会立即中止。
/// `limiter` 提供共享限速器（None 则按 config.rate_limit 新建）。
/// `db` 为状态数据库（None 则不持久化、无法跨进程续传）；`gid` 为任务标识
/// （None 则自动生成）。`on_progress` 约每秒回调一次（在后台任务中执行）。
#[allow(clippy::too_many_arguments)]
pub async fn start_download(
    urls: Vec<String>,
    out_path: Option<PathBuf>,
    config: DownloadConfig,
    sha256: Option<String>,
    limiter: Option<TokenBucket>,
    cancel_token: CancellationToken,
    db: Option<Arc<StateDb>>,
    gid: Option<String>,
    on_progress: impl Fn(Progress) + Send + Sync + 'static,
) -> Result<DownloadHandle> {
    if urls.is_empty() {
        bail!("未提供下载地址");
    }
    let gid = gid.unwrap_or_else(crate::app::task_manager::TaskManager::gen_gid);
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
    // 流式（压缩）路径：允许压缩且该下载无法/无需分段。
    // 探测拿不到大小且不允许流式 → 保持原有的「无法获取文件大小」错误语义。
    let streaming = should_use_streaming(&probe, &config);
    if !probe.total_known && !config.allow_compression {
        bail!("无法获取文件大小");
    }
    // 流式模式恒为单连接，纠正 split 便于日志/状态库记录与实际一致
    if streaming {
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

    // 内存模式判定：阈值 > 0 且文件大小在钳制后的阈值内
    let current_opt = config.memory_threshold > 0
        && probe.total
            <= config
                .memory_threshold
                .min(crate::download::config::MEMORY_THRESHOLD_CAP);

    // 磁盘续传前提：磁盘文件已存在、长度与探测一致，且服务器支持 Range。
    // 若输出文件被删而记录残留，直接续传会用全 0 占位并跳过 Complete 段 → 损坏；
    // 无 Range 支持时 worker 只能整文件从头下载，无法按偏移续传。
    let file_ok = std::fs::metadata(&path)
        .map(|m| m.len() == probe.total)
        .unwrap_or(false);

    // 尝试续传（查状态库）；流式/压缩下载无法按偏移续传，跳过
    let resume_record = if streaming {
        None
    } else {
        db.as_ref().and_then(|db| {
            db.find_resume(&path.display().to_string(), &urls, probe.total)
                .ok()
                .flatten()
        })
    };

    let mut resumed = false;
    let mut memory_mode = current_opt;
    let scheduler = if streaming {
        // 流式：单段顺序流，已知总长为解压后大小，未知则用 u64::MAX（流结束即完成）
        memory_mode = false;
        WorkStealingScheduler::streaming(config.clone(), probe.total_known.then_some(probe.total))
    } else if let Some(rec) = &resume_record {
        // 空段表（历史残留/写入中断）下续传会让 all_done 恒真 → 全 0 占位文件被当作成功，必须重下
        if !rec.memory_mode && probe.supports_range && file_ok && !rec.segments.is_empty() {
            let segments: Vec<Segment> = rec
                .segments
                .iter()
                .map(|snap| {
                    let mut s = Segment::new(0, snap.start, snap.end, config.speed_window);
                    s.written = snap.written;
                    s.state = if snap.complete {
                        SegmentState::Complete
                    } else {
                        SegmentState::Pending
                    };
                    s
                })
                .collect();
            let completed_start: u64 = segments
                .iter()
                .filter(|s| s.state == SegmentState::Complete)
                .map(|s| s.written)
                .sum();
            resumed = true;
            memory_mode = false;
            tracing::info!(
                "检测到续传记录，从 {}/{} bytes 继续",
                completed_start,
                probe.total
            );
            WorkStealingScheduler::from_resume(segments, config.clone())
        } else {
            // 内存模式跨进程重启（数据只在 RAM，无法恢复已下字节）或续传条件不满足 → 重新下载
            tracing::warn!("无法按段续传（内存模式重启/无 Range 支持/文件缺失），重新开始下载");
            WorkStealingScheduler::new(probe.total, config.clone())
        }
    } else {
        WorkStealingScheduler::new(probe.total, config.clone())
    };
    let scheduler = Arc::new(scheduler);

    tracing::info!(
        "开始下载: {} -> {} ({} bytes, {} 线程{}, {})",
        urls[0],
        path.display(),
        probe.total,
        config.split,
        if resumed { ", 续传模式" } else { "" },
        if memory_mode {
            "内存模式"
        } else if streaming {
            "流式模式(压缩)"
        } else {
            "磁盘模式"
        }
    );

    let writer: Arc<dyn DownloadWriter> = if streaming {
        Arc::new(StreamingWriter::new(&path)?)
    } else if memory_mode {
        Arc::new(MemoryWriter::new(probe.total))
    } else if resumed {
        Arc::new(PwriteWriter::resume(&path, probe.total)?)
    } else {
        Arc::new(PwriteWriter::new(&path, probe.total)?)
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

    // 先落一次初始状态（探测完成、调度器就绪）
    if let Some(db) = &db {
        let segments: Vec<SegmentRecord> = scheduler.segment_records().await;
        let rec = make_record(
            &gid,
            &urls,
            total,
            &path.display().to_string(),
            &sha256,
            &config,
            memory_mode,
            "active",
            segments,
        );
        if let Err(e) = db.upsert_download(&rec) {
            tracing::warn!("写入初始下载状态失败: {}", e);
        }
    }

    // 后台任务：监控 + 保存状态 + workers + 校验
    let sched_task = Arc::clone(&scheduler);
    let writer_task = Arc::clone(&writer);
    let source_mgr_task = Arc::clone(&source_mgr);
    let client_task = Arc::clone(&client);
    let cancel_task = cancel_token.clone();
    let path_task = path.clone();
    let urls_task = urls.clone();
    let sha256_task = sha256.clone();
    let cfg_task = config.clone();
    let db_task = db;
    let gid_task = gid.clone();

    let join = tokio::spawn(async move {
        let result: Result<DownloadReport> = (async {
            // 进度监控 + 定期保存状态到数据库
            let sched_clone = sched_task.clone();
            let start_clone = start;
            let urls_clone = urls_task.clone();
            let sha256_clone = sha256_task.clone();
            let cfg_clone = cfg_task.clone();
            let out_path_str = path_task.display().to_string();
            let cancel_clone = cancel_task.clone();
            let gid_clone = gid_task.clone();
            let db_clone = db_task.clone();
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

                    // 保存状态（段表）到数据库
                    if let Some(db) = &db_clone {
                        let segments: Vec<SegmentRecord> = sched_clone.segment_records().await;
                        let rec = make_record(
                            &gid_clone,
                            &urls_clone,
                            total,
                            &out_path_str,
                            &sha256_clone,
                            &cfg_clone,
                            memory_mode,
                            "active",
                            segments,
                        );
                        if let Err(e) = db.upsert_download(&rec) {
                            tracing::warn!("保存下载状态失败: {}", e);
                        }
                    }
                }
            });

            // 启动 workers：流式模式只允许单连接（防窃取破坏顺序流）。
            // split 可能被外部配置为 0（YAML/RPC 直传），调度器 internal_split 已用
            // max(1) 兜底，这里同样钳制，否则 0 个 worker 会让 all_done 永不成立而挂死。
            let worker_count = if streaming { 1 } else { cfg_task.split.max(1) };
            let mut handles = Vec::new();
            for i in 0..worker_count {
                let worker = DownloadWorker::new(
                    i,
                    Arc::clone(&client_task),
                    Arc::clone(&source_mgr_task),
                    Arc::clone(&sched_task),
                    Arc::clone(&writer_task),
                    limiter_task.clone(),
                    cfg_task.clone(),
                    supports_range,
                    streaming,
                    cancel_task.clone(),
                );
                handles.push(tokio::spawn(async move { worker.run().await }));
            }

            // 汇总 worker 结果；无论成败都先中止 monitor，防止 worker 协程异常时
            // 监控协程继续泄漏运行（持有一组 Arc 引用与进度回调，永不结束）。
            let workers_result: Result<()> = (async {
                for h in handles {
                    h.await.context("worker 协程异常退出")?;
                }
                Ok(())
            })
            .await;
            monitor.abort();
            workers_result?;

            if !sched_task.all_done().await {
                let failed = sched_task
                    .segments()
                    .await
                    .iter()
                    .filter(|s| s.state == SegmentState::Cancelled)
                    .count();
                bail!(
                    "下载失败: {} 字节未完成（{} 个段失败）",
                    sched_task.total_remaining().await,
                    failed
                );
            }

            // 流式模式报告实际已写字节（未知大小/压缩流的真实文件大小）
            let report_total = if streaming {
                writer_task.total()
            } else {
                total
            };
            writer_task.flush_all()?;
            if memory_mode || streaming {
                writer_task.finalize(&path_task)?;
            }
            drop(writer_task); // 释放文件句柄，便于读取校验

            // SHA-256 校验
            if let Some(expected) = &sha256_task {
                tracing::info!("正在校验 SHA-256...");
                crate::app::hash::verify_sha256(&path_task, expected)?;
                tracing::info!("SHA-256 校验通过");
            }

            let report = DownloadReport {
                path: path_task,
                total: report_total,
                elapsed: start.elapsed(),
            };
            Ok(report)
        })
        .await;

        match result {
            Ok(report) => {
                if let Some(db) = &db_task {
                    if let Err(e) = db.set_status(&gid_task, "complete") {
                        tracing::warn!("标记任务完成失败: {}", e);
                    }
                }
                tracing::info!(
                    "下载完成: {} ({} bytes, 耗时 {:.2}s)",
                    report.path.display(),
                    report.total,
                    report.elapsed.as_secs_f64()
                );
                Ok(report)
            }
            Err(e) => {
                // 取消 → 保留记录为 paused（可续传）；其它失败 → error
                let status = if cancel_task.is_cancelled() {
                    "paused"
                } else {
                    "error"
                };
                if let Some(db) = &db_task {
                    if let Err(e) = db.set_status(&gid_task, status) {
                        tracing::warn!("标记任务状态失败: {}", e);
                    }
                }
                Err(e)
            }
        }
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
    db: Option<Arc<StateDb>>,
    on_progress: impl Fn(Progress) + Send + Sync + 'static,
) -> Result<DownloadReport> {
    let handle = start_download(
        urls,
        out_path,
        config,
        sha256,
        None,
        CancellationToken::new(),
        db,
        None,
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

    #[test]
    fn filename_sanitizes_path_traversal() {
        // 百分号解码重新引入路径分隔符 / 反斜杠 → 替换为下划线
        assert_eq!(
            filename_from_url("https://a.com/%2E%2E%2Fevil.txt"),
            ".._evil.txt"
        );
        assert_eq!(filename_from_url("https://a.com/a%5Cb.txt"), "a_b.txt");
        // 解码结果为 `.` / `..` → 兜底为 download.bin
        assert_eq!(filename_from_url("https://a.com/%2E%2E"), "download.bin");
        assert_eq!(filename_from_url("https://a.com/%2E"), "download.bin");
    }

    fn cfg_with_compression(on: bool) -> DownloadConfig {
        DownloadConfig {
            min_split_size: 256 * 1024,
            allow_compression: on,
            ..Default::default()
        }
    }

    #[test]
    fn streaming_when_no_range() {
        let probe = ProbeInfo {
            total: 10 * 1024 * 1024,
            total_known: true,
            supports_range: false,
        };
        assert!(should_use_streaming(&probe, &cfg_with_compression(true)));
        // 不允许压缩时回退到原有单流降级
        assert!(!should_use_streaming(&probe, &cfg_with_compression(false)));
    }

    #[test]
    fn streaming_when_size_unknown() {
        let probe = ProbeInfo {
            total: 0,
            total_known: false,
            supports_range: false,
        };
        assert!(should_use_streaming(&probe, &cfg_with_compression(true)));
        assert!(!should_use_streaming(&probe, &cfg_with_compression(false)));
    }

    #[test]
    fn streaming_when_small_file() {
        let probe = ProbeInfo {
            total: 128 * 1024, // < 2 * min_split_size
            total_known: true,
            supports_range: true,
        };
        assert!(should_use_streaming(&probe, &cfg_with_compression(true)));
        // 边界：恰好 2 倍最小分片 → 仍可分段，不流式
        let probe2 = ProbeInfo {
            total: 512 * 1024,
            total_known: true,
            supports_range: true,
        };
        assert!(!should_use_streaming(&probe2, &cfg_with_compression(true)));
    }

    #[test]
    fn no_streaming_for_segmentable_download() {
        let probe = ProbeInfo {
            total: 1024 * 1024,
            total_known: true,
            supports_range: true,
        };
        assert!(!should_use_streaming(&probe, &cfg_with_compression(true)));
    }
}
