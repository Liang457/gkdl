//! gkdl - Rust 多线程下载器（aria2/AriaNg 兼容 JSON-RPC）。
//!
//! 双模式：CLI 直连下载 + daemon 常驻（JSON-RPC + 系统托盘）。

pub mod app;
pub mod download;
pub mod rpc;
