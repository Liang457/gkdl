use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "gkdl",
    version,
    about = "Rust 多线程下载器（aria2/AriaNg 兼容 JSON-RPC）"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// 直接下载一个 URL（CLI 直连模式）
    Download(DownloadArgs),
    /// 常驻后台：提供 JSON-RPC 服务（aria2/AriaNg 兼容）+ 系统托盘
    Daemon(DaemonArgs),
    /// 向运行中的 daemon 提交下载
    Add(AddArgs),
    /// 控制 daemon 任务
    Pause(GidArgs),
    /// 控制 daemon 任务
    Resume(GidArgs),
    /// 控制 daemon 任务
    Remove(GidArgs),
}

#[derive(clap::Args, Debug, Default)]
pub struct DaemonArgs {
    /// 配置文件路径
    #[arg(short, long)]
    pub config: Option<std::path::PathBuf>,
    /// RPC 监听地址（不传则用配置文件，再退回默认值）
    #[arg(long)]
    pub host: Option<String>,
    /// RPC 端口（不传则用配置文件，再退回默认值）
    #[arg(long)]
    pub port: Option<u16>,
    /// RPC 密钥（token）
    #[arg(long)]
    pub rpc_secret: Option<String>,
    /// 不显示托盘图标
    #[arg(long)]
    pub no_tray: bool,
}

#[derive(clap::Args, Debug)]
pub struct AddArgs {
    /// 下载地址（首个为主源，其余为镜像）
    #[arg(required = true)]
    pub urls: Vec<String>,
    /// 输出目录
    #[arg(short = 'd', long)]
    pub dir: Option<std::path::PathBuf>,
    /// 输出文件名
    #[arg(short = 'o', long)]
    pub out: Option<String>,
    /// RPC 端口
    #[arg(long, default_value_t = 6800)]
    pub port: u16,
    /// RPC 密钥
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct GidArgs {
    /// 任务 GID
    pub gid: String,
    /// RPC 端口
    #[arg(long, default_value_t = 6800)]
    pub port: u16,
    /// RPC 密钥
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args, Debug)]
pub struct DownloadArgs {
    /// 下载地址（首个为主源，其余为镜像）
    #[arg(required = true)]
    pub urls: Vec<String>,

    /// 输出路径
    #[arg(short = 'o', long)]
    pub output: Option<PathBuf>,

    /// 并发连接数
    #[arg(short = 's', long, default_value_t = 8)]
    pub split: usize,

    /// 全局限速 bytes/s，0=不限
    #[arg(long)]
    pub max_speed: Option<u64>,

    /// 期望 SHA-256 值（hex）
    #[arg(long)]
    pub sha256: Option<String>,

    /// 最小切片大小（字节）
    #[arg(long)]
    pub min_split_size: Option<u64>,

    /// 内存模式阈值（字节）：小于等于该值直接下载到内存，完成后原子落盘；0 或负值禁用
    #[arg(long)]
    pub memory_threshold: Option<u64>,

    /// 禁用内存模式（等价 --memory-threshold 0）
    #[arg(long)]
    pub no_memory: bool,

    /// 单段最大重试次数
    #[arg(long)]
    pub retries: Option<u32>,

    /// 连接/读超时（秒）
    #[arg(long)]
    pub timeout: Option<u64>,

    /// 允许 gzip/deflate 压缩传输（仅单连接整文件下载；默认开，--no-compression 关闭）
    #[arg(long)]
    pub no_compression: bool,

    /// 下载完成后执行的脚本（CLI 直连模式）
    #[arg(long)]
    pub post_script: Option<PathBuf>,
}
