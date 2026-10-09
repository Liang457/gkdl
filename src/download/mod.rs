//! 下载引擎：源探测、工作窃取调度、多连接分段下载与落盘。

pub mod config;
pub mod curl;
pub mod detector;
pub mod engine;
pub mod pwrite_writer;
pub mod rate_limit;
pub mod scheduler;
pub mod segment;
pub mod source;
pub mod worker;
pub mod writer;
