use crate::download::config::DownloadConfig;
use crate::download::mmap_writer::MmapWriter;
use crate::download::rate_limit::TokenBucket;
use crate::download::scheduler::WorkStealingScheduler;
use crate::download::segment::Segment;
use crate::download::source::SourceManager;
use anyhow::Result;
use reqwest::Client;
use std::sync::Arc;
use std::time::Instant;

enum SegOutcome {
    Complete,
    Failed,
    Killed,
    Cancelled,
    Paused,
}

/// 单个 worker 协程：拉段 → 下载 → 写盘 → 完成/归还。
pub struct DownloadWorker {
    worker_id: usize,
    client: Arc<Client>,
    source_mgr: Arc<SourceManager>,
    scheduler: Arc<WorkStealingScheduler>,
    writer: Arc<MmapWriter>,
    limiter: TokenBucket,
    config: DownloadConfig,
    supports_range: bool,
    cancel_token: tokio_util::sync::CancellationToken,
}

/// 每 worker 的写缓冲。
struct WriteBuffer {
    buf: Vec<u8>,
    offset: u64,
    writer: Arc<MmapWriter>,
    max: usize,
}

impl WriteBuffer {
    fn new(writer: Arc<MmapWriter>, max: usize) -> Self {
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
        client: Arc<Client>,
        source_mgr: Arc<SourceManager>,
        scheduler: Arc<WorkStealingScheduler>,
        writer: Arc<MmapWriter>,
        limiter: TokenBucket,
        config: DownloadConfig,
        supports_range: bool,
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
            cancel_token,
        }
    }

    pub async fn run(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
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
                        FailureAction::Retry => {
                            self.scheduler.cancel(&seg).await;
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
        let mut req = self.client.get(&source.url);
        if self.supports_range {
            let last = end.saturating_sub(1);
            req = req.header(reqwest::header::RANGE, format!("bytes={start}-{last}"));
        }
        req = req.header(reqwest::header::ACCEPT_ENCODING, "identity");

        // 响应头阶段设超时（客户端本身只有 read_timeout，不覆盖头部等待）
        let send = tokio::time::timeout(
            std::time::Duration::from_secs(self.config.timeout.max(5)),
            req.send(),
        );
        let resp = match send.await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                if self.cancel_token.is_cancelled() {
                    return SegOutcome::Cancelled;
                }
                tracing::debug!("worker {} 请求失败: {}", self.worker_id, e);
                self.source_mgr.report_failure(&source);
                return SegOutcome::Failed;
            }
            Err(_) => {
                tracing::debug!("worker {} 请求超时", self.worker_id);
                self.source_mgr.report_failure(&source);
                return SegOutcome::Failed;
            }
        };

        let status = resp.status();
        if !(status == reqwest::StatusCode::OK || status == reqwest::StatusCode::PARTIAL_CONTENT) {
            self.source_mgr.report_failure(&source);
            return SegOutcome::Failed;
        }

        // 服务器对 Range 请求返回 200 整文件：字节位置错位，视为失败（引擎会降级处理）
        if self.supports_range && status == reqwest::StatusCode::OK && start > 0 {
            self.source_mgr.report_failure(&source);
            return SegOutcome::Failed;
        }

        let mut stream = resp.bytes_stream();
        let mut last_tick = Instant::now();
        let mut killed = false;

        while let Some(chunk) = stream.next().await {
            if self.cancel_token.is_cancelled() {
                return SegOutcome::Cancelled;
            }
            // 暂停检测：刷盘后归还段，等待恢复
            if self.scheduler.is_paused() {
                if let Err(e) = buf.flush() {
                    tracing::warn!("worker {} 暂停刷盘失败: {}", self.worker_id, e);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
                self.scheduler
                    .update_progress(seg.seg_id, seg.written)
                    .await;
                return SegOutcome::Paused;
            }
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    tracing::debug!("worker {} 读流错误: {}", self.worker_id, e);
                    self.source_mgr.report_failure(&source);
                    return SegOutcome::Failed;
                }
            };
            if chunk.is_empty() {
                break;
            }

            // 限速
            self.limiter.consume(chunk.len(), &self.cancel_token).await;

            let offset = seg.position_to_write();
            if let Err(e) = buf.write(offset, &chunk) {
                tracing::warn!("worker {} 写盘失败: {}", self.worker_id, e);
                self.source_mgr.report_failure(&source);
                return SegOutcome::Failed;
            }
            seg.written += chunk.len() as u64;
            seg.tick_bytes += chunk.len() as u64;

            // 速度采样 + 慢线程检测
            let now = Instant::now();
            let dt = now.duration_since(last_tick).as_secs_f64();
            if dt >= self.config.sample_interval {
                let speed = seg.tick_bytes as f64 / dt;
                seg.record_speed(speed);
                seg.tick_bytes = 0;
                last_tick = now;

                // 同步进度到调度器，并获取被工作窃取截短后的段尾
                if let Some(cur_end) = self
                    .scheduler
                    .update_progress(seg.seg_id, seg.written)
                    .await
                {
                    if cur_end < seg.end {
                        seg.end = cur_end;
                    }
                    if seg.position_to_write() >= seg.end {
                        // 段已被截短且本地已写满，剩余部分由窃取者负责
                        break;
                    }
                }

                if self.scheduler.check_slow(self.worker_id, seg).await {
                    tracing::warn!("worker {} 被判定为慢线程，杀停归还段", self.worker_id);
                    killed = true;
                    break;
                }
            }
        }

        if let Err(e) = buf.flush() {
            tracing::warn!("worker {} 刷盘失败: {}", self.worker_id, e);
            self.source_mgr.report_failure(&source);
            return SegOutcome::Failed;
        }

        if killed {
            // 慢线程被杀：段归还但不记 source 失败（换源重试由调度处理）
            self.scheduler.clear_worker(self.worker_id).await;
            return SegOutcome::Killed;
        }

        self.source_mgr.report_success(&source);

        if seg.is_complete() {
            SegOutcome::Complete
        } else {
            // 响应提前结束，段未满
            SegOutcome::Failed
        }
    }
}

use crate::download::scheduler::FailureAction;
use futures_util::StreamExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_buffer_flushes_contiguously() {
        let p = std::env::temp_dir().join("gkdl_wbuf.tmp");
        let writer = Arc::new(MmapWriter::new(&p, 1024).unwrap());
        let mut buf = WriteBuffer::new(Arc::clone(&writer), 64);
        buf.write(0, &[1u8; 40]).unwrap();
        assert_eq!(buf.buf.len(), 40);
        buf.write(40, &[2u8; 40]).unwrap(); // 触发 flush（>=64）
        assert!(buf.buf.len() < 40);
        buf.flush().unwrap();
        drop(writer);
        std::fs::remove_file(&p).ok();
    }
}
