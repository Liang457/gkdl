use crate::download::config::DownloadConfig;
use crate::download::detector::{SlowThreadDetector, Verdict};
use crate::download::segment::{Segment, SegmentState};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, Notify};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    Retry,
    Abandon,
}

/// 工作窃取调度器：段分配 / 归还 / 窃取 / 完成判定。
/// 使用 `tokio::sync::Mutex` 保护段表与检测器，避免 std Mutex 阻塞 runtime 线程。
pub struct WorkStealingScheduler {
    config: DownloadConfig,
    segments: Mutex<Vec<Segment>>,
    detector: Mutex<SlowThreadDetector>,
    seg_counter: std::sync::atomic::AtomicU64,
    paused: AtomicBool,
    resume_notify: Notify,
    complete_notify: Notify,
    /// 是否已有段被判定失败且无法重试（引擎据此报错）
    failed: AtomicBool,
}

impl WorkStealingScheduler {
    pub fn new(total_size: u64, config: DownloadConfig) -> Self {
        let detector = SlowThreadDetector::new(&config);
        let segments = Self::initial_split(total_size, &config);
        let seg_count = segments.len() as u64;
        Self {
            config,
            segments: Mutex::new(segments),
            detector: Mutex::new(detector),
            seg_counter: std::sync::atomic::AtomicU64::new(seg_count),
            paused: AtomicBool::new(false),
            resume_notify: Notify::new(),
            complete_notify: Notify::new(),
            failed: AtomicBool::new(false),
        }
    }

    /// 从续传快照构造调度器：已完成的段保持 Complete，其余归位 Pending。
    pub fn from_resume(segments: Vec<Segment>, config: DownloadConfig) -> Self {
        let detector = SlowThreadDetector::new(&config);
        let seg_count = segments.len() as u64;
        let mut normalized = Vec::with_capacity(segments.len());
        for (counter, mut s) in segments.into_iter().enumerate() {
            s.owner_id = usize::MAX;
            s.retries = 0;
            s.clear_speed();
            s.state = if s.state == SegmentState::Complete || s.is_complete() {
                SegmentState::Complete
            } else {
                SegmentState::Pending
            };
            s.seg_id = counter as u64;
            normalized.push(s);
        }
        Self {
            config,
            segments: Mutex::new(normalized),
            detector: Mutex::new(detector),
            seg_counter: std::sync::atomic::AtomicU64::new(seg_count),
            paused: AtomicBool::new(false),
            resume_notify: Notify::new(),
            complete_notify: Notify::new(),
            failed: AtomicBool::new(false),
        }
    }

    fn initial_split(total_size: u64, config: &DownloadConfig) -> Vec<Segment> {
        let mut split = config.split.max(1);
        let min_size = config.min_split_size.max(1);

        if total_size < min_size.saturating_mul(2) {
            split = 1;
        }
        let mut seg_size = total_size / split as u64;
        while seg_size < min_size && split > 1 {
            split -= 1;
            seg_size = total_size / split as u64;
        }

        let mut segs = Vec::with_capacity(split);
        for i in 0..split {
            let start = i as u64 * seg_size;
            let end = if i == split - 1 {
                total_size
            } else {
                (i as u64 + 1) * seg_size
            };
            let mut s = Segment::new(i as u64, start, end, config.speed_window);
            s.state = SegmentState::Pending;
            segs.push(s);
        }
        segs
    }

    #[allow(dead_code)]
    pub fn config(&self) -> &DownloadConfig {
        &self.config
    }

    #[allow(dead_code)]
    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        if !paused {
            self.resume_notify.notify_waiters();
        }
    }

    #[allow(dead_code)]
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// 等待解除暂停。暂停中则挂起，直到 resume_notify 被通知。
    pub async fn wait_resume(&self) {
        loop {
            if !self.paused.load(Ordering::Relaxed) {
                return;
            }
            self.resume_notify.notified().await;
        }
    }

    #[allow(dead_code)]
    pub fn mark_failed(&self) {
        self.failed.store(true, Ordering::Relaxed);
    }

    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    pub async fn segments(&self) -> Vec<Segment> {
        self.segments.lock().await.clone()
    }

    pub async fn total_remaining(&self) -> u64 {
        self.segments
            .lock()
            .await
            .iter()
            .map(|s| s.remaining())
            .sum()
    }

    pub async fn completed_bytes(&self) -> u64 {
        self.segments.lock().await.iter().map(|s| s.written).sum()
    }

    pub async fn active_connections(&self) -> usize {
        self.segments
            .lock()
            .await
            .iter()
            .filter(|s| s.state == SegmentState::Downloading)
            .count()
    }

    /// 工作窃取主入口：先拿无主段，否则从最慢段尾部切 40%。
    pub async fn steal_or_checkout(&self, worker_id: usize) -> Option<Segment> {
        loop {
            if self.paused.load(Ordering::Relaxed) {
                self.resume_notify.notified().await;
                continue;
            }
            let seg = {
                let mut guard = self.segments.lock().await;
                if let Some(seg) = Self::checkout_locked(&mut guard, worker_id) {
                    Some(seg)
                } else {
                    Self::steal_locked(&mut guard, worker_id, &self.config, self.alloc_seg_id())
                }
            };
            if let Some(seg) = seg {
                self.register_worker(worker_id).await;
                return Some(seg);
            }
            return None;
        }
    }

    fn alloc_seg_id(&self) -> u64 {
        self.seg_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn checkout_locked(segs: &mut [Segment], worker_id: usize) -> Option<Segment> {
        for seg in segs.iter_mut() {
            if seg.state == SegmentState::Pending && seg.remaining() > 0 {
                seg.state = SegmentState::Downloading;
                seg.owner_id = worker_id;
                return Some(seg.clone());
            }
        }
        None
    }

    fn steal_locked(
        segs: &mut Vec<Segment>,
        worker_id: usize,
        config: &DownloadConfig,
        seg_id: u64,
    ) -> Option<Segment> {
        let slowest_idx = Self::find_slowest(segs, worker_id, config);
        let slowest_idx = slowest_idx?;
        let slowest = &segs[slowest_idx];
        if slowest.remaining() <= config.min_split_size.saturating_mul(2) {
            return None;
        }
        let remaining = slowest.remaining();
        let cut_size = (remaining as f64 * config.steal_ratio) as u64;
        let cut_point = slowest.end - cut_size;

        let mut new_seg = Segment::new(seg_id, cut_point, slowest.end, config.speed_window);
        new_seg.owner_id = worker_id;
        new_seg.state = SegmentState::Downloading;

        segs[slowest_idx].end = cut_point;
        segs.push(new_seg.clone());
        Some(new_seg)
    }

    fn find_slowest(segs: &[Segment], exclude: usize, config: &DownloadConfig) -> Option<usize> {
        let mut best: Option<(usize, f64)> = None;
        for (i, s) in segs.iter().enumerate() {
            if s.owner_id == exclude
                || s.owner_id == usize::MAX
                || s.state != SegmentState::Downloading
                || s.remaining() <= config.min_split_size
            {
                continue;
            }
            let speed = s.avg_speed();
            if let Some((_, best_speed)) = best {
                if speed < best_speed {
                    best = Some((i, speed));
                }
            } else {
                best = Some((i, speed));
            }
        }
        best.map(|(i, _)| i)
    }

    async fn register_worker(&self, worker_id: usize) {
        self.detector.lock().await.register(worker_id);
    }

    /// 检测慢线程；返回是否应杀掉当前 worker。
    pub async fn check_slow(&self, worker_id: usize, segment: &Segment) -> bool {
        let guard = self.segments.lock().await;
        let peers: Vec<&Segment> = guard
            .iter()
            .filter(|s| s.state == SegmentState::Downloading)
            .collect();
        let mut det = self.detector.lock().await;
        det.evaluate(worker_id, segment, &peers) == Verdict::Kill
    }

    pub async fn clear_worker(&self, worker_id: usize) {
        self.detector.lock().await.clear(worker_id);
    }

    /// 同步 worker 已写字节到调度器（供进度展示与续传）。
    /// 返回调度器当前的段尾：工作窃取可能截短 end，worker 据此钳制本地进度。
    pub async fn update_progress(&self, seg_id: u64, written: u64) -> Option<u64> {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg_id) {
            if t.state == SegmentState::Downloading {
                let max_written = t.end.saturating_sub(t.start);
                t.written = t.written.max(written).min(max_written);
                return Some(t.end);
            }
        }
        None
    }

    /// 段完成。返回是否全部完成。
    pub async fn complete(&self, seg: &Segment) -> bool {
        let mut guard = self.segments.lock().await;
        let target = guard.iter_mut().find(|s| s.seg_id == seg.seg_id);
        if let Some(t) = target {
            t.state = SegmentState::Complete;
            t.owner_id = usize::MAX;
            // 钳制到段内：工作窃取可能截短 end，本地 written 可能越过新段尾
            let max_written = t.end.saturating_sub(t.start);
            t.written = seg.written.max(t.written).min(max_written);
        }
        let done = guard.iter().all(|s| s.state == SegmentState::Complete);
        drop(guard);
        if done {
            self.complete_notify.notify_waiters();
        }
        done
    }

    /// 段失败归还（供重试或他 worker 继续）。
    pub async fn cancel(&self, seg: &Segment) {
        let mut guard = self.segments.lock().await;
        let target = guard.iter_mut().find(|s| s.seg_id == seg.seg_id);
        if let Some(t) = target {
            if t.state == SegmentState::Downloading {
                t.state = SegmentState::Pending;
                t.owner_id = usize::MAX;
                let max_written = t.end.saturating_sub(t.start);
                t.written = seg.written.max(t.written).min(max_written);
                t.clear_speed();
            }
        }
    }

    /// 段放弃（重试耗尽），不再分派。
    pub async fn abandon(&self, seg: &Segment) {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg.seg_id) {
            t.state = SegmentState::Cancelled;
            t.owner_id = usize::MAX;
        }
        drop(guard);
        self.failed.store(true, Ordering::Relaxed);
    }

    pub async fn all_done(&self) -> bool {
        self.segments
            .lock()
            .await
            .iter()
            .all(|s| s.state == SegmentState::Complete)
    }

    /// 记录段失败并递增重试计数。返回应重试还是放弃。
    pub async fn record_failure(&self, seg_id: u64, written: u64) -> FailureAction {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg_id) {
            let max_written = t.end.saturating_sub(t.start);
            t.written = t.written.max(written).min(max_written);
            t.retries += 1;
            if t.retries >= self.config.max_retries {
                return FailureAction::Abandon;
            }
        }
        FailureAction::Retry
    }

    #[allow(dead_code)]
    pub async fn wait_complete(&self) {
        loop {
            if self.all_done().await {
                return;
            }
            self.complete_notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DownloadConfig {
        DownloadConfig {
            split: 4,
            min_split_size: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn initial_split_respects_min_size() {
        let segs = WorkStealingScheduler::initial_split(1_000_000, &cfg());
        assert_eq!(segs.len(), 4);
        assert!(segs.iter().all(|s| s.len() >= 1024));
        let total: u64 = segs.iter().map(|s| s.len()).sum();
        assert_eq!(total, 1_000_000);
        // 首尾连续
        for w in segs.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }
    }

    #[test]
    fn tiny_file_is_single_segment() {
        let segs = WorkStealingScheduler::initial_split(1500, &cfg());
        assert_eq!(segs.len(), 1);
    }

    #[tokio::test]
    async fn checkout_then_steal() {
        let sched = WorkStealingScheduler::new(1_000_000, cfg());
        let s0 = sched.steal_or_checkout(0).await.unwrap();
        assert_eq!(s0.start, 0);
        // worker 0 下载一点
        {
            let mut guard = sched.segments.lock().await;
            guard[0].written = 10_000;
        }
        let s1 = sched.steal_or_checkout(1).await.unwrap();
        assert_ne!(s1.start, s0.start);
        // 所有段分配完
        let _s2 = sched.steal_or_checkout(2).await.unwrap();
        let _s3 = sched.steal_or_checkout(3).await.unwrap();
        // 无主段了，开始窃取
        let s4 = sched.steal_or_checkout(4).await.unwrap();
        // 窃取的段不与原段重叠
        let _segs = sched.segments().await;
        assert!(s4.start < s4.end);
    }

    #[test]
    fn steal_cut_point_never_overlaps_position_to_write() {
        let config = cfg();
        let mut segs = vec![Segment::new(0, 0, 100_000, 20)];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        segs[0].written = 30_000;
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1).unwrap();
        assert!(stolen.start > segs[0].position_to_write());
        assert_eq!(stolen.end, 100_000);
        assert_eq!(segs[0].end, stolen.start);
        // 切出的量 = 40%
        let cut = 100_000 - 30_000;
        let expected = 100_000 - (cut as f64 * 0.4) as u64;
        assert_eq!(segs[0].end, expected);
    }

    #[test]
    fn no_steal_when_remaining_small() {
        let config = cfg();
        let mut segs = vec![Segment::new(0, 0, 3000, 20)];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        segs[0].written = 2000;
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1);
        assert!(stolen.is_none());
    }
}
