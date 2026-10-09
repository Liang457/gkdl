use crate::download::config::DownloadConfig;
use crate::download::curl::{curl_error_text, CurlMsg, HttpClient};
use crate::download::rate_limit::TokenBucket;
use crate::download::scheduler::WorkStealingScheduler;
use crate::download::segment::Segment;
use crate::download::source::{Source, SourceManager};
use crate::download::writer::DownloadWriter;
use anyhow::Result;
use curl::easy::Easy;
use std::sync::Arc;
use std::time::Instant;

/// 常驻连接：Easy 句柄跨段复用（句柄的连接缓存保留到目标主机的空闲连接）。
/// 段流干净结束后停回槽位，下一段（含工作窃取分到的段）直接复用，
/// 省一次 DNS/TCP/TLS 握手，也不向服务器暴露「高频新建连接」的爬虫特征。
type PooledConn = (String, Easy);

enum SegOutcome {
    Complete,
    Failed,
    Killed,
    Cancelled,
    Paused,
}

/// 单个数据块的处理结果。
enum ChunkAction {
    /// 继续读下一块
    Continue,
    /// 停止读流，走收尾（段被截短写满或空块）
    Stop,
    /// 直接返回该结果
    Outcome(SegOutcome),
}

/// 单个 worker 任务：拉段 → 下载 → 写盘 → 完成/归还。
pub struct DownloadWorker {
    worker_id: usize,
    client: Arc<HttpClient>,
    source_mgr: Arc<SourceManager>,
    scheduler: Arc<WorkStealingScheduler>,
    writer: Arc<dyn DownloadWriter>,
    limiter: TokenBucket,
    config: DownloadConfig,
    supports_range: bool,
    /// 流式（单连接整文件）模式：无 Range、可 gzip/deflate、流结束即完成、重试从头开始。
    streaming: bool,
    cancel_token: tokio_util::sync::CancellationToken,
    /// 常驻连接槽位（探测预热连接是它的初始值，仅 worker 0 持有）。
    /// `run(&self)` 以共享引用运行且跨 await，用 Mutex 实现一次性取出。
    pooled: std::sync::Mutex<Option<PooledConn>>,
}

/// 每 worker 的写缓冲。
struct WriteBuffer {
    buf: Vec<u8>,
    offset: u64,
    writer: Arc<dyn DownloadWriter>,
    max: usize,
}

impl WriteBuffer {
    fn new(writer: Arc<dyn DownloadWriter>, max: usize) -> Self {
        Self {
            buf: Vec::with_capacity(max),
            offset: 0,
            writer,
            max,
        }
    }

    fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if self.buf.is_empty() {
            self.offset = offset;
        } else if offset != self.offset + self.buf.len() as u64 {
            // 区间不连续：先刷
            self.flush()?;
            self.offset = offset;
        }
        self.buf.extend_from_slice(data);
        if self.buf.len() >= self.max {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.writer.write_at(self.offset, &self.buf)?;
        self.writer.flush_range(self.offset, self.buf.len() as u64);
        self.buf.clear();
        Ok(())
    }
}

impl DownloadWorker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        worker_id: usize,
        client: Arc<HttpClient>,
        source_mgr: Arc<SourceManager>,
        scheduler: Arc<WorkStealingScheduler>,
        writer: Arc<dyn DownloadWriter>,
        limiter: TokenBucket,
        config: DownloadConfig,
        supports_range: bool,
        streaming: bool,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            worker_id,
            client,
            source_mgr,
            scheduler,
            writer,
            limiter,
            config,
            supports_range,
            streaming,
            cancel_token,
            pooled: std::sync::Mutex::new(None),
        }
    }

    /// 附带常驻连接的构造（探测预热句柄由 worker 0 初始持有）。
    pub fn with_pooled_conn(mut self, pooled: Option<PooledConn>) -> Self {
        self.pooled = std::sync::Mutex::new(pooled);
        self
    }

    pub async fn run(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
                return;
            }
            // 限流冷却期间不发起新请求（重试与窃取均在此排队），可被取消打断
            if !self.wait_cooldown().await {
                return;
            }
            let seg = match self.scheduler.steal_or_checkout(self.worker_id).await {
                Some(s) => s,
                None => {
                    // 无可分配段且无法窃取：所有工作已完成或失败
                    return;
                }
            };

            let mut seg = seg;
            let outcome = self.download_segment(&mut seg).await;

            match outcome {
                SegOutcome::Complete => {
                    if self.scheduler.complete(&seg).await {
                        return;
                    }
                }
                SegOutcome::Cancelled => {
                    self.scheduler.cancel(&seg).await;
                    return;
                }
                SegOutcome::Paused => {
                    // 暂停：段归还 Pending，等待恢复后继续拉取
                    self.scheduler.cancel(&seg).await;
                    self.scheduler.wait_resume().await;
                }
                SegOutcome::Killed => {
                    // 慢线程被杀：归还段但不记重试（调度行为，非网络失败）
                    self.scheduler.cancel(&seg).await;
                }
                SegOutcome::Failed => {
                    match self.scheduler.record_failure(seg.seg_id, seg.written).await {
                        FailureAction::Retry { delay_ms } => {
                            self.scheduler.cancel(&seg).await;
                            // 段级指数退避后回到循环顶部，再过全局限流冷却
                            if !self.sleep_cancellable(delay_ms).await {
                                return;
                            }
                        }
                        FailureAction::Abandon => {
                            self.scheduler.abandon(&seg).await;
                            return;
                        }
                    }
                }
            }
        }
    }

    /// 等待全局限流冷却结束。返回 false 表示已取消。
    async fn wait_cooldown(&self) -> bool {
        loop {
            let ms = self.scheduler.cooldown_remaining_ms();
            if ms == 0 {
                return true;
            }
            if !self.sleep_cancellable(ms.min(200)).await {
                return false;
            }
        }
    }

    /// 可被取消打断的睡眠。返回 false 表示已取消。
    async fn sleep_cancellable(&self, ms: u64) -> bool {
        if ms == 0 {
            return !self.cancel_token.is_cancelled();
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(ms)) => true,
            _ = self.cancel_token.cancelled() => false,
        }
    }

    /// 段流干净结束后把句柄停回常驻槽位：连接缓存可能仍保留空闲连接，
    /// 下一段（含窃取的段）直接复用。槽位已有句柄时以最新收到的为准
    /// （旧的可能是换源前的残留）。
    fn park_conn(&self, url: &str, easy: Easy) {
        let mut guard = self.pooled.lock().unwrap();
        *guard = Some((url.to_string(), easy));
    }

    async fn download_segment(&self, seg: &mut Segment) -> SegOutcome {
        if self.cancel_token.is_cancelled() {
            return SegOutcome::Cancelled;
        }
        let mut buf = WriteBuffer::new(Arc::clone(&self.writer), self.config.write_buffer_size);
        let source = match self.source_mgr.get_source() {
            Some(s) => s,
            None => return SegOutcome::Failed,
        };

        let start = seg.position_to_write();
        let end = seg.end; // exclusive

        // 常驻连接仅在 URL 与当前源一致时复用；不一致（换源）或取出失败则
        // 静默回退到新建连接：复用只是优化，不影响正确性。
        // 检查与取出在同一次加锁内完成，不留锁窗口。
        let pooled = {
            let mut guard = self.pooled.lock().unwrap();
            match guard.as_ref() {
                Some((url, _)) if *url == source.url => guard.take(),
                _ => None,
            }
        };
        let (mut rx, _handle) = if self.streaming {
            // 流式整文件：无 Range，允许 gzip/deflate（libcurl 自动解压）；
            // 重试/恢复无法断点，截断文件并从头部重新下载。
            if let Err(e) = self.writer.reset() {
                tracing::warn!("worker {} 重置临时文件失败: {}", self.worker_id, e);
                self.source_mgr.report_failure(&source);
                return SegOutcome::Failed;
            }
            seg.written = 0;
            seg.tick_bytes = 0;
            seg.clear_speed();
            self.scheduler.reset_written(seg.seg_id).await;
            let opened = match pooled {
                Some((_, easy)) => match self.client.stream_whole_warm(easy, &source.url) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        tracing::debug!(
                            "worker {} 常驻连接启动流式下载失败，回退新建连接: {}",
                            self.worker_id,
                            e
                        );
                        self.client.stream_whole(&source.url)
                    }
                },
                None => self.client.stream_whole(&source.url),
            };
            match opened {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("worker {} 创建流式下载失败: {}", self.worker_id, e);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            }
        } else {
            let range = if self.supports_range {
                Some((start, end.saturating_sub(1)))
            } else {
                None
            };
            let opened = match pooled {
                Some((_, easy)) => {
                    match self.client.stream_segment_warm(easy, &source.url, range) {
                        Ok(v) => Ok(v),
                        Err(e) => {
                            tracing::debug!(
                                "worker {} 常驻连接启动分段下载失败，回退新建连接: {}",
                                self.worker_id,
                                e
                            );
                            self.client.stream_segment(&source.url, range)
                        }
                    }
                }
                None => self.client.stream_segment(&source.url, range),
            };
            match opened {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("worker {} 创建下载流失败: {}", self.worker_id, e);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            }
        };

        // 读超时语义：两次 chunk 间隔超时（每次读后重置），更贴合低网速
        let timeout = std::time::Duration::from_secs(self.client.timeout_secs());
        // 提前退出路径统一先刷盘，防止「已计入 written 但仍在写缓冲」的字节丢失：
        // 否则调度器/状态库记录的进度会跳过这些字节，续传生成损坏文件。
        let flush_early = |buf: &mut WriteBuffer| {
            if let Err(e) = buf.flush() {
                tracing::warn!("worker {} 提前退出刷盘失败: {}", self.worker_id, e);
            }
            SegOutcome::Failed
        };
        let mut status = 0u32;
        let mut first_chunk: Option<Vec<u8>> = None;

        // 等待首个数据块；期间更新状态码（重定向会收到多个响应头块）
        let mut early_end = false;
        loop {
            let msg = match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!("worker {} 请求超时", self.worker_id);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            };
            match msg {
                Some(CurlMsg::Headers { status: s }) => status = s,
                Some(CurlMsg::Data(c)) => {
                    first_chunk = Some(c);
                    break;
                }
                Some(CurlMsg::End(Err(e), _)) => {
                    if self.cancel_token.is_cancelled() {
                        return SegOutcome::Cancelled;
                    }
                    tracing::warn!(
                        "worker {} 请求失败: {}",
                        self.worker_id,
                        curl_error_text(&e)
                    );
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
                Some(CurlMsg::End(Ok(()), easy)) => {
                    // 干净结束：句柄停回槽位供下一段复用
                    self.park_conn(&source.url, easy);
                    early_end = true;
                    break;
                }
                None => {
                    tracing::warn!(
                        "worker {} 下载通道关闭（curl 线程提前结束）",
                        self.worker_id
                    );
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            }
        }

        // 分段：200 或 206 均接受；流式：未发 Range，必须 200（206 视为异常）
        let ok_status = if self.streaming {
            status == 200
        } else {
            status == 200 || status == 206
        };
        if !ok_status {
            if status == 403 || status == 429 {
                // 限流/反爬信号：设置任务级全局冷却，所有 worker 暂停发起新请求，
                // 避免重试风暴把临时限流升级成长期封禁
                self.scheduler.hit_rate_limit();
                tracing::warn!(
                    "worker {} 收到 HTTP {}（疑似限流/反爬），任务全局冷却 {}ms",
                    self.worker_id,
                    status,
                    self.scheduler.cooldown_remaining_ms()
                );
            } else {
                tracing::warn!(
                    "worker {} 收到异常 HTTP 状态 {}: {}",
                    self.worker_id,
                    status,
                    source.url
                );
            }
            self.source_mgr.report_failure(&source);
            return SegOutcome::Failed;
        }

        // 服务器对 Range 请求返回 200 整文件：字节位置错位，视为失败（引擎会降级处理）
        if self.supports_range && status == 200 && start > 0 {
            tracing::warn!(
                "worker {} Range 请求被回 200 整文件（字节错位）: {}",
                self.worker_id,
                source.url
            );
            self.source_mgr.report_failure(&source);
            return SegOutcome::Failed;
        }

        // 未收到任何数据即干净结束（0 字节响应）：走正常完成判定而非读流 None 误报失败
        if early_end && first_chunk.is_none() {
            return self.finish_segment(seg, &mut buf, &source, false).await;
        }

        let mut last_tick = Instant::now();
        let mut killed = false;

        if let Some(chunk) = first_chunk {
            match self
                .handle_chunk(seg, &mut buf, &source, chunk, &mut last_tick, &mut killed)
                .await
            {
                ChunkAction::Continue => {}
                ChunkAction::Stop => {
                    return self.finish_segment(seg, &mut buf, &source, killed).await
                }
                ChunkAction::Outcome(o) => return o,
            }
        }

        loop {
            let msg = match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!("worker {} 读流超时", self.worker_id);
                    flush_early(&mut buf);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            };
            let chunk = match msg {
                Some(CurlMsg::Data(c)) => c,
                Some(CurlMsg::Headers { .. }) => continue,
                Some(CurlMsg::End(Ok(()), easy)) => {
                    // 干净结束：句柄停回槽位供下一段复用
                    self.park_conn(&source.url, easy);
                    break;
                }
                Some(CurlMsg::End(Err(e), _)) => {
                    if self.cancel_token.is_cancelled() {
                        if let Err(e) = buf.flush() {
                            tracing::warn!("worker {} 取消刷盘失败: {}", self.worker_id, e);
                            self.source_mgr.report_failure(&source);
                            return SegOutcome::Failed;
                        }
                        return SegOutcome::Cancelled;
                    }
                    tracing::warn!(
                        "worker {} 读流错误: {}",
                        self.worker_id,
                        curl_error_text(&e)
                    );
                    flush_early(&mut buf);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
                None => {
                    tracing::warn!(
                        "worker {} 下载通道关闭（curl 线程提前结束）",
                        self.worker_id
                    );
                    flush_early(&mut buf);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            };
            match self
                .handle_chunk(seg, &mut buf, &source, chunk, &mut last_tick, &mut killed)
                .await
            {
                ChunkAction::Continue => {}
                ChunkAction::Stop => {
                    return self.finish_segment(seg, &mut buf, &source, killed).await
                }
                ChunkAction::Outcome(o) => return o,
            }
        }

        self.finish_segment(seg, &mut buf, &source, killed).await
    }

    /// 处理单个数据块。
    async fn handle_chunk(
        &self,
        seg: &mut Segment,
        buf: &mut WriteBuffer,
        source: &Source,
        chunk: Vec<u8>,
        last_tick: &mut Instant,
        killed: &mut bool,
    ) -> ChunkAction {
        if self.cancel_token.is_cancelled() {
            // 刷盘保留已收字节（进度计数已含缓冲数据），取消后才能干净续传
            if let Err(e) = buf.flush() {
                tracing::warn!("worker {} 取消刷盘失败: {}", self.worker_id, e);
                self.source_mgr.report_failure(source);
                return ChunkAction::Outcome(SegOutcome::Failed);
            }
            return ChunkAction::Outcome(SegOutcome::Cancelled);
        }
        // 暂停检测：刷盘后归还段，等待恢复
        if self.scheduler.is_paused() {
            if let Err(e) = buf.flush() {
                tracing::warn!("worker {} 暂停刷盘失败: {}", self.worker_id, e);
                self.source_mgr.report_failure(source);
                return ChunkAction::Outcome(SegOutcome::Failed);
            }
            self.scheduler
                .update_progress(seg.seg_id, seg.written, None)
                .await;
            return ChunkAction::Outcome(SegOutcome::Paused);
        }
        if chunk.is_empty() {
            return ChunkAction::Stop;
        }

        // 限速
        self.limiter.consume(chunk.len(), &self.cancel_token).await;

        let offset = seg.position_to_write();
        if let Err(e) = buf.write(offset, &chunk) {
            tracing::warn!("worker {} 写盘失败: {}", self.worker_id, e);
            self.source_mgr.report_failure(source);
            return ChunkAction::Outcome(SegOutcome::Failed);
        }
        seg.written += chunk.len() as u64;
        seg.tick_bytes += chunk.len() as u64;

        // 速度采样 + 慢线程检测
        let now = Instant::now();
        let dt = now.duration_since(*last_tick).as_secs_f64();
        if dt >= self.config.sample_interval {
            let speed = seg.tick_bytes as f64 / dt;
            seg.record_speed(speed);
            seg.tick_bytes = 0;
            *last_tick = now;

            // 同步进度与速度采样到调度器，并获取被工作窃取截短后的段尾
            if let Some(cur_end) = self
                .scheduler
                .update_progress(seg.seg_id, seg.written, Some(speed))
                .await
            {
                if cur_end < seg.end {
                    seg.end = cur_end;
                }
                if seg.position_to_write() >= seg.end {
                    // 段已被截短且本地已写满，剩余部分由窃取者负责
                    return ChunkAction::Stop;
                }
            }

            if self.scheduler.check_slow(self.worker_id, seg).await {
                tracing::warn!("worker {} 被判定为慢线程，杀停归还段", self.worker_id);
                *killed = true;
                // 走 finish_segment：先刷盘（written 已含缓冲字节）再归还段，避免数据洞
                return ChunkAction::Stop;
            }
        }
        ChunkAction::Continue
    }

    /// 段收尾：刷盘 + 慢线程清理 + 判定 Complete/Failed。
    async fn finish_segment(
        &self,
        seg: &mut Segment,
        buf: &mut WriteBuffer,
        source: &Source,
        killed: bool,
    ) -> SegOutcome {
        if let Err(e) = buf.flush() {
            tracing::warn!("worker {} 刷盘失败: {}", self.worker_id, e);
            self.source_mgr.report_failure(source);
            return SegOutcome::Failed;
        }

        if killed {
            // 慢线程被杀：段归还但不记 source 失败（换源重试由调度处理）
            self.scheduler.clear_worker(self.worker_id).await;
            return SegOutcome::Killed;
        }

        self.source_mgr.report_success(source);

        if self.streaming {
            // 流式：未知大小 → 流结束即完成；已知大小 → 必须写满段长，
            // 否则视为截断（服务器提前结束但连接干净关闭，Content-Length 无法兜底）。
            if seg.end == u64::MAX || seg.is_complete() {
                SegOutcome::Complete
            } else {
                SegOutcome::Failed
            }
        } else if seg.is_complete() {
            SegOutcome::Complete
        } else {
            // 响应提前结束，段未满
            SegOutcome::Failed
        }
    }
}

use crate::download::scheduler::FailureAction;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::pwrite_writer::PwriteWriter;

    fn tmp_path(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("gkdl_wbuf_{name}_{}.tmp", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn write_buffer_writes_exact_bytes_at_exact_offsets() {
        // 连续写合入缓冲；区间不连续立即按旧偏移刷盘；缓冲满即落盘。
        // 守护：逐字节回读内容与偏移，写错位置的缓冲实现在此暴露。
        let p = tmp_path("offsets");
        let writer: Arc<dyn DownloadWriter> = Arc::new(PwriteWriter::new(&p, 256).unwrap());
        let mut buf = WriteBuffer::new(Arc::clone(&writer), 64);

        buf.write(0, &[1u8; 10]).unwrap(); // 入缓冲
        assert_eq!(buf.buf.len(), 10);
        buf.write(50, &[2u8; 10]).unwrap(); // 不连续 → 先刷 [0..10)，从 50 重新起缓冲
        assert_eq!(buf.offset, 50);
        assert_eq!(buf.buf.len(), 10);
        buf.write(60, &[3u8; 70]).unwrap(); // 连续追加 → 80 ≥ 64 → 刷 [50..130)
        assert!(buf.buf.is_empty());
        buf.write(200, &[4u8; 10]).unwrap(); // 入缓冲
        buf.flush().unwrap(); // 刷 [200..210)
        assert!(buf.buf.is_empty());

        drop(writer);
        let file = std::fs::read(&p).unwrap();
        assert_eq!(&file[..10], &[1u8; 10], "偏移 0 的内容");
        assert_eq!(&file[50..60], &[2u8; 10], "偏移 50 的内容");
        assert_eq!(&file[60..130], &[3u8; 70], "偏移 60 起的连续追加");
        assert_eq!(&file[200..210], &[4u8; 10], "偏移 200 的内容");
        assert!(file[10..50].iter().all(|&b| b == 0), "未写入区域应保持 0");
        assert!(file[130..200].iter().all(|&b| b == 0), "未写入区域应保持 0");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn write_buffer_flushes_exactly_at_capacity() {
        // 恰好写满 max 的那次 write 应立即落盘（>= 判定）且内容精确
        let p = tmp_path("cap");
        let writer: Arc<dyn DownloadWriter> = Arc::new(PwriteWriter::new(&p, 64).unwrap());
        let mut buf = WriteBuffer::new(Arc::clone(&writer), 64);
        buf.write(0, &[7u8; 64]).unwrap();
        assert!(buf.buf.is_empty(), "写满即刷");
        drop(writer);
        let file = std::fs::read(&p).unwrap();
        assert_eq!(file, [7u8; 64]);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn park_conn_keeps_latest_handle_and_url() {
        // 槽位以最新停回的句柄为准（旧的可能是换源前的残留）
        let config = DownloadConfig::default();
        let p = tmp_path("park");
        let worker = DownloadWorker::new(
            0,
            Arc::new(HttpClient::new(&config).unwrap()),
            Arc::new(SourceManager::new(&["http://a".into()])),
            Arc::new(WorkStealingScheduler::new(1024, config.clone())),
            Arc::new(PwriteWriter::new(&p, 16).unwrap()),
            TokenBucket::new(0),
            config,
            true,
            false,
            tokio_util::sync::CancellationToken::new(),
        );
        worker.park_conn("http://a", Easy::new());
        assert_eq!(
            worker.pooled.lock().unwrap().as_ref().unwrap().0,
            "http://a"
        );
        worker.park_conn("http://b", Easy::new());
        assert_eq!(
            worker.pooled.lock().unwrap().as_ref().unwrap().0,
            "http://b",
            "再次停回应以最新句柄为准"
        );
        std::fs::remove_file(&p).ok();
    }
}
