use crate::app::db::SegmentRecord;
use crate::download::config::DownloadConfig;
use crate::download::detector::{SlowThreadDetector, Verdict};
use crate::download::segment::{Segment, SegmentState};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use tokio::sync::{watch, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// 重试：携带建议退避毫秒数（段级指数退避；全局限流冷却由 worker 循环顶部另行等待）
    Retry {
        delay_ms: u64,
    },
    Abandon,
}

/// 段级退避基数（毫秒）：第 n 次失败等待 基数 × 2^(n-1)。
const SEGMENT_BACKOFF_BASE_MS: u64 = 500;

/// 当前 Unix 时间（毫秒）。冷却截止时刻存原子量只能用墙钟；
/// 系统对时最多让一次冷却提前/延后结束，无碍语义。
fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 工作窃取调度器：段分配 / 归还 / 窃取 / 完成判定。
/// 使用 `tokio::sync::Mutex` 保护段表与检测器，避免 std Mutex 阻塞 runtime 线程。
pub struct WorkStealingScheduler {
    config: DownloadConfig,
    segments: Mutex<Vec<Segment>>,
    detector: Mutex<SlowThreadDetector>,
    seg_counter: std::sync::atomic::AtomicU64,
    paused: AtomicBool,
    /// 暂停状态变更信号：`set_paused` 使版本号 +1，`wait_resume` 据此唤醒。
    /// 不用 `Notify`：`notify_waiters` 在「检查标志后、注册等待前」发出的
    /// 唤醒会丢失（worker 永久挂起）；watch 的版本判定没有这个窗口。
    resume_tx: watch::Sender<()>,
    resume_rx: watch::Receiver<()>,
    /// 是否已有段被判定失败且无法重试（引擎据此报错）
    failed: AtomicBool,
    /// 流式（单连接整文件）模式：禁止工作窃取，段数恒为 1。
    streaming: bool,
    /// 全局限流冷却截止时刻（Unix 毫秒）：收到 403/429 时由 worker 设置，
    /// 所有 worker 发起新请求（取段/重试）前须等它过期。0 表示无冷却。
    cooldown_until_ms: AtomicI64,
    /// 当前冷却时长（毫秒）：冷却未过期期间再次命中则翻倍（指数退避，封顶），
    /// 已过期则重置回基数——避免间隔很久的两次限流被错误地累进放大。
    cooldown_step_ms: AtomicU64,
}

impl WorkStealingScheduler {
    pub fn new(total_size: u64, config: DownloadConfig) -> Self {
        let detector = SlowThreadDetector::new(&config);
        let segments = Self::initial_split(total_size, &config);
        let seg_count = segments.len() as u64;
        let (resume_tx, resume_rx) = watch::channel(());
        Self {
            config,
            segments: Mutex::new(segments),
            detector: Mutex::new(detector),
            seg_counter: std::sync::atomic::AtomicU64::new(seg_count),
            paused: AtomicBool::new(false),
            resume_tx,
            resume_rx,
            failed: AtomicBool::new(false),
            streaming: false,
            cooldown_until_ms: AtomicI64::new(0),
            cooldown_step_ms: AtomicU64::new(0),
        }
    }

    /// 流式（单连接整文件）模式构造：仅一个段覆盖 `0..total`。
    /// `known_total` 为探测已知大小（解压后大小），未知时用 `u64::MAX` 表示流式结束即完成。
    pub fn streaming(config: DownloadConfig, known_total: Option<u64>) -> Self {
        let end = known_total.unwrap_or(u64::MAX);
        let mut seg = Segment::new(0, 0, end, config.speed_window);
        seg.state = SegmentState::Pending;
        let detector = SlowThreadDetector::new(&config);
        let (resume_tx, resume_rx) = watch::channel(());
        Self {
            config,
            segments: Mutex::new(vec![seg]),
            detector: Mutex::new(detector),
            seg_counter: std::sync::atomic::AtomicU64::new(1),
            paused: AtomicBool::new(false),
            resume_tx,
            resume_rx,
            failed: AtomicBool::new(false),
            streaming: true,
            cooldown_until_ms: AtomicI64::new(0),
            cooldown_step_ms: AtomicU64::new(0),
        }
    }

    /// 从续传快照构造调度器：已完成的段保持 Complete，其余归位 Pending。
    pub fn from_resume(segments: Vec<Segment>, config: DownloadConfig) -> Self {
        let detector = SlowThreadDetector::new(&config);
        let seg_count = segments.len() as u64;
        let (resume_tx, resume_rx) = watch::channel(());
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
            resume_tx,
            resume_rx,
            failed: AtomicBool::new(false),
            streaming: false,
            cooldown_until_ms: AtomicI64::new(0),
            cooldown_step_ms: AtomicU64::new(0),
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

    pub fn config(&self) -> &DownloadConfig {
        &self.config
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
        // 先写标志再发版本号：任何变更都唤醒等待者，由其重查标志
        let _ = self.resume_tx.send(());
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    /// 等待解除暂停。暂停中则挂起，直到再次 `set_paused`（版本号 +1）。
    pub async fn wait_resume(&self) {
        // 只克隆一次接收端：每轮循环内保持旧版本号，变更判定才可靠。
        let mut rx = self.resume_rx.clone();
        loop {
            if !self.paused.load(Ordering::Relaxed) {
                return;
            }
            if rx.changed().await.is_err() {
                return; // 发送端已释放（调度器销毁），视为已解除暂停
            }
        }
    }

    pub fn has_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    pub async fn segments(&self) -> Vec<Segment> {
        self.segments.lock().await.clone()
    }

    /// 持久化用的轻量段快照：不含 `speed_history` 等运行时数据，避免监控任务每秒全量克隆。
    /// `end` 钳到 `i64::MAX`：流式未知大小的段以 `u64::MAX` 为哨兵，SQLite 只存 i64。
    pub async fn segment_records(&self) -> Vec<SegmentRecord> {
        let guard = self.segments.lock().await;
        guard
            .iter()
            .map(|s| SegmentRecord {
                seg_id: s.seg_id,
                start: s.start,
                end: s.end.min(i64::MAX as u64),
                written: s.written,
                complete: s.state == SegmentState::Complete,
            })
            .collect()
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
        // 暂停时在 wait_resume 中等待恢复（watch 版本号，不丢唤醒）；
        // 恢复后只取一次，拿不到段/无法窃取即返回 None（worker 退出）。
        self.wait_resume().await;
        let seg = {
            let mut guard = self.segments.lock().await;
            if let Some(seg) = Self::checkout_locked(&mut guard, worker_id) {
                Some(seg)
            } else {
                Self::steal_locked(
                    &mut guard,
                    worker_id,
                    &self.config,
                    self.alloc_seg_id(),
                    self.streaming,
                )
            }
        };
        if let Some(seg) = seg {
            self.register_worker(worker_id).await;
            return Some(seg);
        }
        None
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
        streaming: bool,
    ) -> Option<Segment> {
        // 流式模式只有单段顺序流，禁止窃取，避免破坏写偏移
        if streaming {
            return None;
        }
        let slowest_idx = Self::find_slowest(segs, worker_id, config);
        let slowest_idx = slowest_idx?;
        let slowest = &segs[slowest_idx];
        if slowest.remaining() <= config.min_split_size.saturating_mul(2) {
            return None;
        }
        let remaining = slowest.remaining();
        // 钳制窃取比例到 (0,1]：越界值会让 cut_size >= remaining，导致
        // slowest.end - cut_size 下溢（debug panic / release 回绕成巨大偏移的段）。
        let cut_size = (remaining as f64 * config.steal_ratio.clamp(0.0, 1.0)) as u64;
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

    /// 同步 worker 已写字节与速度采样到调度器（供进度展示、续传与慢线程检测）。
    /// 返回调度器当前的段尾：工作窃取可能截短 end，worker 据此钳制本地进度。
    /// worker 只持有段的本地克隆，速度采样必须在此回写，否则慢线程检测
    /// 与窃取最慢段拿不到真实速度。
    pub async fn update_progress(
        &self,
        seg_id: u64,
        written: u64,
        speed: Option<f64>,
    ) -> Option<u64> {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg_id) {
            if t.state == SegmentState::Downloading {
                let max_written = t.end.saturating_sub(t.start);
                t.written = t.written.max(written).min(max_written);
                if let Some(speed) = speed {
                    t.record_speed(speed);
                }
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
        guard.iter().all(|s| s.state == SegmentState::Complete)
    }

    /// 段失败归还（供重试或他 worker 继续）。
    pub async fn cancel(&self, seg: &Segment) {
        let mut guard = self.segments.lock().await;
        let target = guard.iter_mut().find(|s| s.seg_id == seg.seg_id);
        if let Some(t) = target {
            if t.state == SegmentState::Downloading {
                let max_written = t.end.saturating_sub(t.start);
                t.written = seg.written.max(t.written).min(max_written);
                t.clear_speed();
                t.owner_id = usize::MAX;
                // 窃取截短段尾后，worker 本地 written 可能已写满新分配区间
                // （字节已在盘上）；若仍置 Pending，剩余 0 字节的段永远不会
                // 被 checkout，整个下载被误判失败。
                if t.written >= max_written {
                    t.state = SegmentState::Complete;
                } else {
                    t.state = SegmentState::Pending;
                }
            }
        }
    }

    /// 段放弃（重试耗尽），不再分派。
    pub async fn abandon(&self, seg: &Segment) {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg.seg_id) {
            t.state = SegmentState::Cancelled;
            t.owner_id = usize::MAX;
            tracing::warn!(
                "段 {} 重试 {} 次耗尽，放弃（已下载 {}/{} bytes）",
                t.seg_id,
                self.config.max_retries,
                t.written,
                t.end.saturating_sub(t.start)
            );
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

    /// 流式模式专用：把段的已写进度归零（重试/恢复时从头重新下载）。
    pub async fn reset_written(&self, seg_id: u64) {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg_id) {
            t.written = 0;
            t.clear_speed();
        }
    }

    /// 命中限流信号（HTTP 403/429）：设置/延长全局限流冷却。
    ///
    /// 冷却未过期期间再次命中 → 时长翻倍（指数退避，封顶 `cooldown_max_ms`）；
    /// 已过期后命中 → 重置回基数 `cooldown_base_ms`（间隔很久的两次限流不累进）。
    /// 原子操作实现，无锁：worker 在请求路径上高频查询 `cooldown_remaining_ms`。
    pub fn hit_rate_limit(&self) {
        let now = unix_now_ms();
        let base = self.config.cooldown_base_ms.max(100);
        let max = self.config.cooldown_max_ms.max(base);
        let step = if self.cooldown_until_ms.load(Ordering::Relaxed) > now {
            self.cooldown_step_ms.load(Ordering::Relaxed).max(base) * 2
        } else {
            base
        }
        .min(max);
        self.cooldown_step_ms.store(step, Ordering::Relaxed);
        self.cooldown_until_ms
            .store(now.saturating_add(step as i64), Ordering::Relaxed);
    }

    /// 距全局限流冷却结束还剩多少毫秒（0 = 无冷却）。
    pub fn cooldown_remaining_ms(&self) -> u64 {
        let now = unix_now_ms();
        let until = self.cooldown_until_ms.load(Ordering::Relaxed);
        until.saturating_sub(now).max(0) as u64
    }

    /// 记录段失败并递增重试计数。返回应重试（含段级指数退避毫秒数）还是放弃。
    pub async fn record_failure(&self, seg_id: u64, written: u64) -> FailureAction {
        let mut guard = self.segments.lock().await;
        if let Some(t) = guard.iter_mut().find(|s| s.seg_id == seg_id) {
            let max_written = t.end.saturating_sub(t.start);
            t.written = t.written.max(written).min(max_written);
            t.retries += 1;
            if t.retries >= self.config.max_retries {
                return FailureAction::Abandon;
            }
            // 指数退避：500ms → 1s → 2s → …（位移封顶，防 max_retries 配得极大时溢出）
            let shift = (t.retries - 1).min(5);
            return FailureAction::Retry {
                delay_ms: SEGMENT_BACKOFF_BASE_MS.saturating_mul(1 << shift),
            };
        }
        // 未知 seg_id 不应发生（段表只增不删、id 单调分配）；宁可放弃也不零延迟重试
        tracing::error!("record_failure: 未知段 {seg_id}，按放弃处理");
        FailureAction::Abandon
    }

    /// 终态清理：清空各段速度历史并收缩段表容量，释放工作窃取期间积累的临时内存。
    /// 段的区间/进度/状态信息保留，供 tellStatus 的 bitfield 展示使用。
    pub async fn trim(&self) {
        let mut guard = self.segments.lock().await;
        for s in guard.iter_mut() {
            s.clear_speed();
            s.speed_history.shrink_to_fit();
        }
        guard.shrink_to_fit();
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
        assert!(segs.iter().all(|s| s.byte_len() >= 1024));
        let total: u64 = segs.iter().map(|s| s.byte_len()).sum();
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

    #[test]
    fn streaming_scheduler_has_single_segment_and_no_steal() {
        let sched = WorkStealingScheduler::streaming(cfg(), Some(10_000));
        let segs = sched.segments.try_lock().unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].start, 0);
        assert_eq!(segs[0].end, 10_000);
        // 未知总长用 u64::MAX
        drop(segs);
        let sched2 = WorkStealingScheduler::streaming(cfg(), None);
        let segs2 = sched2.segments.try_lock().unwrap();
        assert_eq!(segs2[0].end, u64::MAX);
        drop(segs2);
        // 流式模式禁止窃取
        let mut segs3 = vec![Segment::new(0, 0, 100_000, 20)];
        segs3[0].owner_id = 0;
        segs3[0].state = SegmentState::Downloading;
        assert!(WorkStealingScheduler::steal_locked(&mut segs3, 1, &cfg(), 1, true).is_none());
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
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1, false).unwrap();
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
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1, false);
        assert!(stolen.is_none());
    }

    #[test]
    fn steal_picks_slowest_segment() {
        let config = cfg();
        // 两个下载中的段：seg0 快（速度 1000），seg1 慢（速度 10）
        let mut segs = vec![
            Segment::new(0, 0, 200_000, 20),
            Segment::new(1, 200_000, 400_000, 20),
        ];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[0].record_speed(1000.0);
        }
        segs[1].owner_id = 1;
        segs[1].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[1].record_speed(10.0);
        }
        // worker 2 来窃取，应从最慢的 seg1 切
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 2, &config, 10, false).unwrap();
        // 窃取的段来自 seg1 的尾部
        assert_eq!(stolen.end, 400_000);
        assert!(stolen.start > 200_000, "窃取起点应在 seg1 范围内");
        // seg1 被截短
        assert_eq!(segs[1].end, stolen.start);
    }

    #[test]
    fn steal_ratio_is_40_percent() {
        let config = cfg();
        let mut segs = vec![Segment::new(0, 0, 100_000, 20)];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        segs[0].written = 0;
        // remaining = 100_000, cut = 40_000, cut_point = 60_000
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1, false).unwrap();
        assert_eq!(stolen.start, 60_000);
        assert_eq!(stolen.end, 100_000);
        assert_eq!(segs[0].end, 60_000);
        // 窃取量 = 40% of remaining
        let expected_cut = (100_000.0 * 0.4) as u64;
        assert_eq!(stolen.end - stolen.start, expected_cut);
    }

    #[test]
    fn no_steal_from_own_segment() {
        let config = cfg();
        let mut segs = vec![Segment::new(0, 0, 200_000, 20)];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[0].record_speed(10.0);
        }
        // worker 0 不能从自己的段窃取
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 0, &config, 1, false);
        assert!(stolen.is_none());
    }

    #[test]
    fn no_steal_from_pending_or_complete() {
        let config = cfg();
        let mut segs = vec![
            Segment::new(0, 0, 200_000, 20),
            Segment::new(1, 200_000, 400_000, 20),
        ];
        // seg0 是 Pending，seg1 是 Complete
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Pending;
        segs[1].owner_id = 1;
        segs[1].state = SegmentState::Complete;
        // 没有 Downloading 段可窃取
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 2, &config, 10, false);
        assert!(stolen.is_none());
    }

    #[test]
    fn find_slowest_returns_lowest_speed() {
        let config = cfg();
        let mut segs = vec![
            Segment::new(0, 0, 200_000, 20),
            Segment::new(1, 200_000, 400_000, 20),
            Segment::new(2, 400_000, 600_000, 20),
        ];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[0].record_speed(500.0);
        }
        segs[1].owner_id = 1;
        segs[1].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[1].record_speed(50.0);
        }
        segs[2].owner_id = 2;
        segs[2].state = SegmentState::Downloading;
        for _ in 0..6 {
            segs[2].record_speed(200.0);
        }
        // worker 3 来查找最慢段，应返回 seg1（速度 50）
        let idx = WorkStealingScheduler::find_slowest(&segs, 3, &config).unwrap();
        assert_eq!(idx, 1);
    }

    #[test]
    fn rate_limit_cooldown_doubles_caps_and_resets() {
        let config = DownloadConfig {
            cooldown_base_ms: 1000,
            cooldown_max_ms: 4000,
            ..Default::default()
        };
        let sched = WorkStealingScheduler::new(1000, config);
        // 首次命中：基数
        sched.hit_rate_limit();
        let r1 = sched.cooldown_remaining_ms();
        assert!(r1 > 800 && r1 <= 1000, "首次冷却应为基数: {r1}");
        // 冷却期内再命中：翻倍
        sched.hit_rate_limit();
        let r2 = sched.cooldown_remaining_ms();
        assert!(r2 > 1800 && r2 <= 2000, "冷却期内应翻倍: {r2}");
        // 封顶 cooldown_max_ms
        sched.hit_rate_limit();
        sched.hit_rate_limit();
        let r3 = sched.cooldown_remaining_ms();
        assert!(r3 > 3000 && r3 <= 4000, "冷却应封顶: {r3}");
        // 过期后命中：重置回基数（间隔很久的两次限流不累进）
        sched
            .cooldown_until_ms
            .store(unix_now_ms() - 1, Ordering::Relaxed);
        sched.hit_rate_limit();
        let r4 = sched.cooldown_remaining_ms();
        assert!(r4 > 800 && r4 <= 1000, "过期后应重置回基数: {r4}");
    }

    #[test]
    fn cooldown_expires_to_zero() {
        let sched = WorkStealingScheduler::new(1000, DownloadConfig::default());
        assert_eq!(sched.cooldown_remaining_ms(), 0, "初始无冷却");
        sched
            .cooldown_until_ms
            .store(unix_now_ms() - 1, Ordering::Relaxed);
        assert_eq!(sched.cooldown_remaining_ms(), 0, "过期后应视为无冷却");
    }

    #[tokio::test]
    async fn record_failure_backs_off_exponentially_then_abandons() {
        // cfg() 的 max_retries 为默认值 3
        let sched = WorkStealingScheduler::new(10_000, cfg());
        sched.steal_or_checkout(0).await.unwrap();
        let d1 = sched.record_failure(0, 0).await;
        assert_eq!(d1, FailureAction::Retry { delay_ms: 500 });
        let d2 = sched.record_failure(0, 0).await;
        assert_eq!(d2, FailureAction::Retry { delay_ms: 1000 });
        let d3 = sched.record_failure(0, 0).await;
        assert_eq!(d3, FailureAction::Abandon);
    }

    #[tokio::test]
    async fn update_progress_records_speed_samples_to_scheduler() {
        // 守护：update_progress 必须把速度采样回写调度器——worker 只持有本地克隆，
        // 不回写则 check_slow 与 find_slowest 拿不到真实速度，慢线程杀停与窃取失效。
        let sched = WorkStealingScheduler::new(1_000_000, cfg());
        sched.steal_or_checkout(0).await.unwrap(); // seg0 -> Downloading
        for i in 0..6 {
            sched.update_progress(0, 100 * i, Some(50.0)).await;
        }
        let segs = sched.segments().await;
        assert_eq!(segs[0].speed_history.len(), 6, "速度采样应回写到调度器");
        assert_eq!(segs[0].avg_speed(), 50.0);
        // 无采样（如暂停路径）不应清空既有历史
        sched.update_progress(0, 600, None).await;
        let segs = sched.segments().await;
        assert_eq!(segs[0].speed_history.len(), 6);
    }

    #[tokio::test]
    async fn segment_records_clamps_streaming_sentinel_end() {
        // 守护：流式未知大小的段以 u64::MAX 作 end 哨兵，持久化前钳到 i64::MAX
        // （SQLite 只存 i64）。
        let sched = WorkStealingScheduler::streaming(cfg(), None);
        let recs = sched.segment_records().await;
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].end, i64::MAX as u64);
        assert!(recs[0].end < u64::MAX);
    }

    /// 单段调度器（split=1），供断言「全任务级」状态的测试使用。
    fn single_cfg() -> DownloadConfig {
        DownloadConfig {
            split: 1,
            min_split_size: 1024,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn cancel_marks_fully_written_segment_complete() {
        // 守护：窃取截短段尾后，worker 本地可能已写满原区间（字节已在盘上），
        // cancel 应判 Complete；留成 0 剩余的 Pending 段会令 all_done 永不成立。
        let sched = WorkStealingScheduler::new(10_000, single_cfg());
        let seg = sched.steal_or_checkout(0).await.unwrap();
        let mut local = seg.clone();
        local.written = local.end - local.start; // 本地写满（含被截短的区间）
        sched.cancel(&local).await;
        let segs = sched.segments().await;
        assert_eq!(
            segs[0].state,
            SegmentState::Complete,
            "写满的段应判 Complete"
        );
        assert!(sched.all_done().await, "全部写满后 all_done 应成立");
    }

    #[tokio::test]
    async fn complete_clamps_written_to_stolen_segment_end() {
        // 段被窃取截短后，worker 本地 written 可能越过新段尾（重叠区写的是相同
        // 字节）。complete 必须把 written 钳回段长，否则进度统计虚高。
        let sched = WorkStealingScheduler::new(100_000, cfg());
        let seg = sched.steal_or_checkout(0).await.unwrap();
        {
            let mut guard = sched.segments.lock().await;
            guard[0].end = 60_000; // 模拟被窃取截短
        }
        let mut local = seg.clone();
        local.written = 100_000; // 本地仍按原段长上报
        sched.complete(&local).await;
        let segs = sched.segments().await;
        assert_eq!(segs[0].written, 60_000, "written 应钳制到截短后的段长");
    }

    #[tokio::test]
    async fn from_resume_keeps_complete_and_resets_runtime_state() {
        // 续传快照归一化：Complete 段原样保留；其余归位 Pending 且保留 written；
        // owner/retries/速度历史等运行时状态全部清零；seg_id 重排为 0..n。
        let mut done = Segment::new(99, 0, 50_000, 20);
        done.written = 50_000;
        done.state = SegmentState::Complete;
        done.owner_id = 7;
        done.retries = 3;
        done.record_speed(123.0);
        let mut part = Segment::new(100, 50_000, 100_000, 20);
        part.written = 10_000;
        part.state = SegmentState::Downloading;
        part.owner_id = 8;
        part.retries = 2;
        part.record_speed(5.0);

        let sched = WorkStealingScheduler::from_resume(vec![done, part], cfg());
        let segs = sched.segments().await;
        assert_eq!(segs[0].seg_id, 0);
        assert_eq!(segs[1].seg_id, 1);
        assert_eq!(segs[0].state, SegmentState::Complete);
        assert_eq!(segs[0].written, 50_000);
        assert_eq!(
            segs[1].state,
            SegmentState::Pending,
            "Downloading 应归位 Pending"
        );
        assert_eq!(segs[1].written, 10_000, "部分进度的段应保留 written");
        for s in &segs {
            assert_eq!(s.owner_id, usize::MAX, "owner 应清零");
            assert_eq!(s.retries, 0, "retries 应清零");
            assert!(s.speed_history.is_empty(), "速度历史应清空");
        }
        // Pending 段可被 checkout，且从 written 处继续（position = start + written）
        let got = sched.steal_or_checkout(0).await.unwrap();
        assert_eq!(got.seg_id, 1);
        assert_eq!(got.position_to_write(), 60_000);
    }

    #[tokio::test]
    async fn record_failure_unknown_segment_abandons() {
        // 未知 seg_id 不应发生（段表只增不删）；兜底按放弃处理，不零延迟重试
        let sched = WorkStealingScheduler::new(10_000, cfg());
        assert_eq!(
            sched.record_failure(12_345, 0).await,
            FailureAction::Abandon
        );
    }

    #[tokio::test]
    async fn abandon_marks_failed_and_prevents_redownload() {
        // 重试耗尽放弃：段标记 Cancelled 不再分派，failed 置位供引擎报错
        let sched = WorkStealingScheduler::new(10_000, single_cfg()); // max_retries 默认 3
        let seg = sched.steal_or_checkout(0).await.unwrap();
        for _ in 0..3 {
            sched.record_failure(seg.seg_id, 0).await;
        }
        sched.abandon(&seg).await;
        assert!(sched.has_failed(), "放弃后 failed 应置位");
        let segs = sched.segments().await;
        assert_eq!(segs[0].state, SegmentState::Cancelled);
        assert!(
            sched.steal_or_checkout(1).await.is_none(),
            "Cancelled 段不得再被 checkout/窃取"
        );
        assert!(!sched.all_done().await, "放弃的段不算完成");
    }

    #[test]
    fn steal_ratio_out_of_range_is_clamped() {
        // steal_ratio 越界钳制到 (0,1]：不钳制会 cut >= remaining，
        // 导致 slowest.end - cut_size 下溢（debug panic / release 回绕成巨大段）。
        let mut config = cfg();
        config.steal_ratio = 5.0;
        let mut segs = vec![Segment::new(0, 0, 100_000, 20)];
        segs[0].owner_id = 0;
        segs[0].state = SegmentState::Downloading;
        segs[0].written = 10_000;
        let stolen = WorkStealingScheduler::steal_locked(&mut segs, 1, &config, 1, false).unwrap();
        assert!(
            stolen.start >= segs[0].position_to_write(),
            "切点不得越过已写位置"
        );
        assert_eq!(stolen.end, 100_000);
        assert_eq!(stolen.start, segs[0].end, "原段尾应截到窃取段起点");
        assert!(segs[0].end <= 100_000);
    }

    #[test]
    fn hit_rate_limit_clamps_tiny_base_and_max() {
        // base/max 配 0：base 钳到 100ms，max 钳到 >= base，冷却不为 0 也不超上限
        let config = DownloadConfig {
            cooldown_base_ms: 0,
            cooldown_max_ms: 0,
            ..Default::default()
        };
        let sched = WorkStealingScheduler::new(1000, config);
        sched.hit_rate_limit();
        let r1 = sched.cooldown_remaining_ms();
        assert!(r1 > 0 && r1 <= 100, "最小冷却基数应为 100ms: {r1}");
        // 冷却期内再命中翻倍后仍被 max(=base) 封顶
        sched.hit_rate_limit();
        let r2 = sched.cooldown_remaining_ms();
        assert!(r2 > 0 && r2 <= 100, "冷却应封顶在 100ms: {r2}");
    }
}
