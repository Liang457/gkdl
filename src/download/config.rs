use serde::{Deserialize, Serialize};

/// 内存模式阈值上界：超过该值强制磁盘，防误配导致内存暴涨。
pub const MEMORY_THRESHOLD_CAP: u64 = 64 * 1024 * 1024;

/// 下载参数（对应《多线程下载算法设计》§4 参数表）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadConfig {
    /// 初始并发连接数
    #[serde(default = "default_split")]
    pub split: usize,
    /// 最小碎片字节数
    #[serde(default = "default_min_split_size")]
    pub min_split_size: u64,
    /// 内存模式阈值（字节）：文件小于等于该值则不下临时文件、直接在内存累积。
    /// `<= 0` 表示禁用该功能（全部走磁盘）。上限受 `MEMORY_THRESHOLD_CAP` 钳制。
    #[serde(default = "default_memory_threshold")]
    pub memory_threshold: u64,
    /// 写盘缓冲
    #[serde(default = "default_write_buffer_size")]
    pub write_buffer_size: usize,
    /// 速度采样间隔（秒）
    #[serde(default = "default_sample_interval")]
    pub sample_interval: f64,
    /// 速度滑动窗口大小
    #[serde(default = "default_speed_window")]
    pub speed_window: usize,
    /// 慢线程阈值（中位数 × 此比例）
    #[serde(default = "default_slow_ratio")]
    pub slow_ratio: f64,
    /// 连续确认次数
    #[serde(default = "default_slow_confirm")]
    pub slow_confirm: usize,
    /// 至少采样次数才判定
    #[serde(default = "default_slow_min_samples")]
    pub slow_min_samples: usize,
    /// 新线程宽限期（秒）
    #[serde(default = "default_grace_period")]
    pub grace_period: f64,
    /// 窃取比例
    #[serde(default = "default_steal_ratio")]
    pub steal_ratio: f64,
    /// 单段最大重试
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// 全局限速 bytes/s，0=不限
    #[serde(default)]
    pub rate_limit: u64,
    /// 连接/读超时（秒）
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    /// 分块大小（字节），用于 tellStatus 的 pieceLength/numPieces/bitfield 展示
    #[serde(default = "default_piece_length")]
    pub piece_length: u64,
    /// 全局并发下载数上限，0=不限
    #[serde(default)]
    pub max_concurrent_downloads: usize,
    /// 默认下载目录，空表示当前目录
    #[serde(default)]
    pub dir: String,
    /// 请求 User-Agent，空则使用默认浏览器 UA
    #[serde(default = "default_user_agent")]
    pub user_agent: String,
    /// Referer 请求头，空则不发送
    #[serde(default)]
    pub referer: String,
    /// 附加请求头（`NAME: value` 列表），随每个请求发送
    #[serde(default)]
    pub header: Vec<String>,
}

fn default_split() -> usize {
    8
}
fn default_min_split_size() -> u64 {
    256 * 1024
}
fn default_memory_threshold() -> u64 {
    8 * 1024 * 1024
}
fn default_write_buffer_size() -> usize {
    64 * 1024
}
fn default_sample_interval() -> f64 {
    0.5
}
fn default_speed_window() -> usize {
    20
}
fn default_slow_ratio() -> f64 {
    0.3
}
fn default_slow_confirm() -> usize {
    3
}
fn default_slow_min_samples() -> usize {
    6
}
fn default_grace_period() -> f64 {
    2.0
}
fn default_steal_ratio() -> f64 {
    0.4
}
fn default_max_retries() -> u32 {
    3
}
fn default_timeout() -> u64 {
    30
}
fn default_piece_length() -> u64 {
    1024 * 1024
}

/// 默认 User-Agent：Firefox 浏览器 UA（公开给 engine 作空值兜底）。
pub fn default_user_agent() -> String {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:154.0) Gecko/20100101 Firefox/154.0".into()
}

impl Default for DownloadConfig {
    fn default() -> Self {
        Self {
            split: default_split(),
            min_split_size: default_min_split_size(),
            memory_threshold: default_memory_threshold(),
            write_buffer_size: default_write_buffer_size(),
            sample_interval: default_sample_interval(),
            speed_window: default_speed_window(),
            slow_ratio: default_slow_ratio(),
            slow_confirm: default_slow_confirm(),
            slow_min_samples: default_slow_min_samples(),
            grace_period: default_grace_period(),
            steal_ratio: default_steal_ratio(),
            max_retries: default_max_retries(),
            rate_limit: 0,
            timeout: default_timeout(),
            piece_length: default_piece_length(),
            max_concurrent_downloads: 0,
            dir: String::new(),
            user_agent: default_user_agent(),
            referer: String::new(),
            header: Vec::new(),
        }
    }
}
