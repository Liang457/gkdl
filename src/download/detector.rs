use crate::download::config::DownloadConfig;
use crate::download::segment::Segment;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// 慢线程三层检测：Layer1 阈值 → Layer2 持续确认 → Layer3 P10 确认。
pub struct SlowThreadDetector {
    ratio: f64,
    confirm_count: usize,
    min_samples: usize,
    grace_period: Duration,
    strikes: HashMap<usize, usize>,
    birth_time: HashMap<usize, Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    Warning,
    Kill,
}

impl SlowThreadDetector {
    pub fn new(config: &DownloadConfig) -> Self {
        Self {
            // 钳制到合法区间：负 grace_period 会让 Duration::from_secs_f64 panic；
            // 越界 ratio 会破坏阈值判定（负阈值使慢线程检测失效）。
            ratio: config.slow_ratio.clamp(0.0, 1.0),
            confirm_count: config.slow_confirm,
            min_samples: config.slow_min_samples,
            grace_period: Duration::from_secs_f64(config.grace_period.max(0.0)),
            strikes: HashMap::new(),
            birth_time: HashMap::new(),
        }
    }

    pub fn register(&mut self, worker_id: usize) {
        self.birth_time.insert(worker_id, Instant::now());
        self.strikes.insert(worker_id, 0);
    }

    /// `peers` 为当前所有下载中的段。返回 healthy / warning / kill。
    pub fn evaluate(&mut self, worker_id: usize, segment: &Segment, peers: &[&Segment]) -> Verdict {
        let birth = self.birth_time.get(&worker_id).copied();
        if let Some(b) = birth {
            if b.elapsed() < self.grace_period {
                return Verdict::Healthy;
            }
        }

        if segment.speed_history.len() < self.min_samples {
            return Verdict::Healthy;
        }

        let avg = segment.avg_speed();
        let mut peer_speeds: Vec<f64> = peers
            .iter()
            .filter(|s| s.owner_id != worker_id && s.speed_history.len() >= self.min_samples)
            .map(|s| s.avg_speed())
            .filter(|v| v.is_finite())
            .collect();
        if peer_speeds.is_empty() {
            return Verdict::Healthy;
        }
        peer_speeds.sort_by(|a, b| a.total_cmp(b));
        let median = peer_speeds[peer_speeds.len() / 2];
        let threshold = median * self.ratio;

        let strikes = self.strikes.entry(worker_id).or_insert(0);
        if avg >= threshold {
            *strikes = 0;
            return Verdict::Healthy;
        }

        *strikes += 1;
        if *strikes < self.confirm_count {
            return Verdict::Warning;
        }

        // Layer 3：P10 确认
        if segment.p10_speed() < threshold {
            return Verdict::Kill;
        }
        Verdict::Warning
    }

    pub fn clear(&mut self, worker_id: usize) {
        self.strikes.remove(&worker_id);
        self.birth_time.remove(&worker_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::config::DownloadConfig;
    use crate::download::segment::{Segment, SegmentState};
    use std::time::Duration;

    fn make_seg(id: u64, speeds: &[f64]) -> Segment {
        let mut s = Segment::new(id, 0, 10_000, 20);
        for v in speeds {
            s.record_speed(*v);
        }
        s.state = SegmentState::Downloading;
        s
    }

    #[tokio::test]
    async fn not_killed_within_grace_period() {
        let mut det = SlowThreadDetector::new(&DownloadConfig::default());
        det.register(0);
        let mut seg = make_seg(0, &[100.0; 6]);
        seg.owner_id = 0;
        // 刚注册，宽限期内
        assert_eq!(det.evaluate(0, &seg, &[]), Verdict::Healthy);
    }

    #[tokio::test]
    async fn kills_consistently_slow_thread() {
        let mut det = SlowThreadDetector::new(&DownloadConfig::default());
        // 手动把 birth_time 改成过去
        det.register(0);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));
        det.register(1);
        det.birth_time
            .insert(1, Instant::now() - Duration::from_secs(10));

        let slow = make_seg(0, &[10.0; 6]);
        let fast = make_seg(1, &[1000.0; 6]);
        let peers = vec![&slow, &fast];
        let mut seg = slow.clone();
        seg.owner_id = 0;

        let mut verdict = Verdict::Healthy;
        for _ in 0..5 {
            verdict = det.evaluate(0, &seg, &peers);
        }
        assert_eq!(verdict, Verdict::Kill);
    }

    #[test]
    fn fast_thread_stays_healthy() {
        let mut det = SlowThreadDetector::new(&DownloadConfig::default());
        det.register(0);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));
        let seg = make_seg(0, &[1000.0; 6]);
        assert_eq!(det.evaluate(0, &seg, &[]), Verdict::Healthy);
    }

    /// 速度 < 中位数 30% 时，连续 3 次确认后应返回 Kill
    #[test]
    fn slow_below_30_percent_median_killed_after_3_confirms() {
        let config = DownloadConfig {
            grace_period: 0.0, // 取消宽限期
            ..Default::default()
        };
        let mut det = SlowThreadDetector::new(&config);
        det.register(0);
        det.register(1);
        // 将 birth_time 设到过去，跳过宽限期
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));
        det.birth_time
            .insert(1, Instant::now() - Duration::from_secs(10));

        // worker 0 速度 10，worker 1 速度 1000
        // 中位数 = 1000，阈值 = 300，10 < 300 → 慢
        let slow_peer = make_seg(1, &[1000.0; 6]);
        let mut my_seg = make_seg(0, &[10.0; 6]);
        my_seg.owner_id = 0;
        let peers = vec![&slow_peer];

        // 前 2 次应为 Warning（未达到 confirm_count=3）
        let v1 = det.evaluate(0, &my_seg, &peers);
        assert_eq!(v1, Verdict::Warning, "第 1 次应为 Warning");
        let v2 = det.evaluate(0, &my_seg, &peers);
        assert_eq!(v2, Verdict::Warning, "第 2 次应为 Warning");
        // 第 3 次应触发 Kill（p10 也低于阈值）
        let v3 = det.evaluate(0, &my_seg, &peers);
        assert_eq!(v3, Verdict::Kill, "第 3 次应为 Kill");
    }

    /// 速度恢复到阈值以上后，strike 计数应重置
    #[test]
    fn recovery_resets_strikes() {
        let config = DownloadConfig {
            grace_period: 0.0,
            ..Default::default()
        };
        let mut det = SlowThreadDetector::new(&config);
        det.register(0);
        det.register(1);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));
        det.birth_time
            .insert(1, Instant::now() - Duration::from_secs(10));

        let fast_peer = make_seg(1, &[1000.0; 6]);
        let peers = vec![&fast_peer];

        // 先慢 2 次（Warning）
        let mut slow_seg = make_seg(0, &[10.0; 6]);
        slow_seg.owner_id = 0;
        det.evaluate(0, &slow_seg, &peers);
        det.evaluate(0, &slow_seg, &peers);

        // 速度恢复，应重置 strike
        let mut fast_seg = make_seg(0, &[5000.0; 6]);
        fast_seg.owner_id = 0;
        let v = det.evaluate(0, &fast_seg, &peers);
        assert_eq!(v, Verdict::Healthy, "恢复后应为 Healthy");

        // 再次变慢，strike 从 0 开始，应又是 Warning 而非 Kill
        let mut slow_seg2 = make_seg(0, &[10.0; 6]);
        slow_seg2.owner_id = 0;
        let v2 = det.evaluate(0, &slow_seg2, &peers);
        assert_eq!(v2, Verdict::Warning, "重置后第 1 次应为 Warning");
    }

    /// 样本不足时不应判定为慢
    #[test]
    fn insufficient_samples_stays_healthy() {
        let config = DownloadConfig {
            grace_period: 0.0,
            slow_min_samples: 6,
            ..Default::default()
        };
        let mut det = SlowThreadDetector::new(&config);
        det.register(0);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));

        // 只有 3 个样本，不足 min_samples=6
        let mut seg = make_seg(0, &[1.0; 3]); // 极慢但样本不足
        seg.owner_id = 0;
        let fast_peer = make_seg(1, &[1000.0; 6]);
        let peers = vec![&fast_peer];
        assert_eq!(det.evaluate(0, &seg, &peers), Verdict::Healthy);
    }

    /// 没有对等节点时不应判定为慢
    #[test]
    fn no_peers_stays_healthy() {
        let config = DownloadConfig {
            grace_period: 0.0,
            ..Default::default()
        };
        let mut det = SlowThreadDetector::new(&config);
        det.register(0);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));

        let mut seg = make_seg(0, &[1.0; 6]); // 极慢但无对等
        seg.owner_id = 0;
        assert_eq!(det.evaluate(0, &seg, &[]), Verdict::Healthy);
    }

    /// 速度恰好等于阈值时应为 Healthy（>= threshold）
    #[test]
    fn speed_exactly_at_threshold_is_healthy() {
        let config = DownloadConfig {
            grace_period: 0.0,
            slow_ratio: 0.3,
            ..Default::default()
        };
        let mut det = SlowThreadDetector::new(&config);
        det.register(0);
        det.register(1);
        det.birth_time
            .insert(0, Instant::now() - Duration::from_secs(10));
        det.birth_time
            .insert(1, Instant::now() - Duration::from_secs(10));

        // peer 速度 1000，阈值 = 300
        let peer = make_seg(1, &[1000.0; 6]);
        // worker 速度恰好 300 = threshold
        let mut seg = make_seg(0, &[300.0; 6]);
        seg.owner_id = 0;
        let peers = vec![&peer];
        assert_eq!(det.evaluate(0, &seg, &peers), Verdict::Healthy);
    }
}
