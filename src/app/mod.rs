//! 应用层：CLI、配置、日志、钩子、托盘、任务管理等跨引擎/RPC 的编排模块。

pub mod cli;
pub mod config;
pub mod db;
pub mod hash;
pub mod hooks;
pub mod logging;
pub mod task_manager;
pub mod tray;
