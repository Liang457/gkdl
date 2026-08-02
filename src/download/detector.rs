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
            ratio: config.slow_ratio,
            confirm_count: config.slow_confirm,
            min_samples: config.slow_min_samples,
            grace_period: Duration::from_secs_f64(config.grace_period),
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
}
