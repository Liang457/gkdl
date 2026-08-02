use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    Pending,
    Downloading,
    Complete,
    Cancelled,
}

/// 一段连续字节区间。`start`/`end` 均为全局偏移，`end` 为 exclusive。
#[derive(Debug, Clone)]
pub struct Segment {
    pub seg_id: u64,
    pub start: u64,
    pub end: u64,
    pub written: u64,
    pub owner_id: usize,
    pub state: SegmentState,
    pub retries: u32,
    pub speed_history: VecDeque<f64>,
    pub speed_window: usize,
    /// 上一次采样的时间点
    pub last_sample_at: Option<std::time::Instant>,
    /// 上个采样周期内的字节数
    pub tick_bytes: u64,
}

impl Segment {
    pub fn new(seg_id: u64, start: u64, end: u64, speed_window: usize) -> Self {
        Self {
            seg_id,
            start,
            end,
            written: 0,
            owner_id: usize::MAX,
            state: SegmentState::Pending,
            retries: 0,
            speed_history: VecDeque::with_capacity(speed_window),
            speed_window,
            last_sample_at: None,
            tick_bytes: 0,
        }
    }

    pub fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.start + self.written)
    }

    pub fn position_to_write(&self) -> u64 {
        self.start + self.written
    }

    pub fn is_complete(&self) -> bool {
        self.written >= (self.end - self.start)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn avg_speed(&self) -> f64 {
        if self.speed_history.len() < 2 {
            return f64::INFINITY;
        }
        self.speed_history.iter().sum::<f64>() / self.speed_history.len() as f64
    }

    /// P10 分位数，样本不足时返回 inf（不判慢）
    pub fn p10_speed(&self) -> f64 {
        if self.speed_history.len() < 5 {
            return f64::INFINITY;
        }
        let mut sorted: Vec<f64> = self.speed_history.iter().copied().collect();
        sorted.sort_by(|a, b| a.total_cmp(b));
        let idx = ((sorted.len() as f64) * 0.1) as usize;
        sorted[idx]
    }

    pub fn record_speed(&mut self, speed: f64) {
        if self.speed_history.len() == self.speed_window {
            self.speed_history.pop_front();
        }
        self.speed_history.push_back(speed);
    }

    pub fn clear_speed(&mut self) {
        self.speed_history.clear();
        self.tick_bytes = 0;
        self.last_sample_at = None;
    }
}
