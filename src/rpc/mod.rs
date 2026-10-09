//! aria2 兼容的 JSON-RPC 层：HTTP POST / GET JSONP / WebSocket 传输与方法分发。

pub mod client;
pub mod methods;
pub mod protocol;
pub mod server;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownKind {
    Graceful,
    Force,
}

#[derive(Clone)]
pub struct ShutdownHandle {
    tx: mpsc::UnboundedSender<ShutdownKind>,
}

impl ShutdownHandle {
    pub fn new(tx: mpsc::UnboundedSender<ShutdownKind>) -> Self {
        Self { tx }
    }

    pub fn request(&self, kind: ShutdownKind) {
        let _ = self.tx.send(kind);
    }
}

/// 全局选项（可运行时修改）。
#[derive(Debug, Clone)]
pub struct GlobalOptions {
    pub max_overall_download_limit: u64,
    pub max_concurrent_downloads: usize,
    pub split: usize,
    pub min_split_size: u64,
    pub memory_threshold: u64,
    pub retries: u32,
    pub timeout: u64,
    pub dir: PathBuf,
    /// aria2 兼容字段（save-session）：无实际效果，任务状态已由 SQLite 持续持久化。
    pub save_session: bool,
    pub piece_length: u64,
    pub user_agent: String,
    pub referer: String,
    pub header: Vec<String>,
    /// 是否允许 gzip/deflate 压缩传输（单连接整文件下载）。
    pub allow_compression: bool,
}

impl Default for GlobalOptions {
    fn default() -> Self {
        Self {
            max_overall_download_limit: 0,
            max_concurrent_downloads: 0,
            split: 8,
            min_split_size: 256 * 1024,
            memory_threshold: crate::download::config::DownloadConfig::default().memory_threshold,
            retries: 3,
            timeout: 30,
            dir: PathBuf::from("."),
            save_session: false,
            piece_length: 1024 * 1024,
            user_agent: crate::download::config::default_user_agent(),
            referer: String::new(),
            header: Vec::new(),
            allow_compression: crate::download::config::DownloadConfig::default().allow_compression,
        }
    }
}

pub type GlobalOptionsRef = Arc<Mutex<GlobalOptions>>;
