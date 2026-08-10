#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use clap::Parser;
use gkdl::app::cli::{Cli, Command, DaemonArgs};
use gkdl::app::config::{self, ConfigStore, LogConfig};
use gkdl::app::db::StateDb;
use gkdl::app::hooks::HookConfig;
use gkdl::app::logging;
use gkdl::app::task_manager::{self, TaskManager};
use gkdl::app::tray;
use gkdl::download::config::DownloadConfig;
use gkdl::rpc;
use gkdl::rpc::{GlobalOptions, ShutdownHandle, ShutdownKind};
use std::io::{IsTerminal, Write};
use std::sync::Arc;

fn main() {
    // 发布版为 GUI 子系统（无控制台窗口）。从终端启动时附着到父进程控制台，
    // 使 CLI 子命令的输出可见；双击启动时静默失败，保持无窗口。
    #[cfg(windows)]
    attach_parent_console();

    let cli = Cli::parse();
    // daemon 模式由 logging::init_logging 接管；其它命令用默认 stdout 日志
    if !matches!(cli.command, Some(Command::Daemon(_))) {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .init();
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime 构建失败");

    let code = rt.block_on(async { run(cli).await });
    std::process::exit(code);
}

async fn run(cli: Cli) -> i32 {
    match cli.command {
        Some(Command::Download(args)) => run_download(args).await,
        Some(Command::Daemon(args)) => run_daemon(args).await,
        // 无子命令（例如直接双击运行）时进入 RPC daemon 模式
        None => run_daemon(DaemonArgs::default()).await,
        Some(Command::Add(args)) => {
            let mut params = vec![serde_json::json!(args.urls)];
            let mut opts = serde_json::Map::new();
            if let Some(dir) = args.dir {
                opts.insert("dir".into(), serde_json::json!(dir.display().to_string()));
            }
            if let Some(out) = args.out {
                opts.insert("out".into(), serde_json::json!(out));
            }
            if !opts.is_empty() {
                params.push(serde_json::Value::Object(opts));
            }
            let resp = rpc_client_call(args.port, args.rpc_secret, "aria2.addUri", params).await;
            print_rpc(resp)
        }
        Some(Command::Pause(args)) => {
            let resp = rpc_client_call(
                args.port,
                args.rpc_secret,
                "aria2.pause",
                vec![serde_json::json!(args.gid)],
            )
            .await;
            print_rpc(resp)
        }
        Some(Command::Resume(args)) => {
            let resp = rpc_client_call(
                args.port,
                args.rpc_secret,
                "aria2.unpause",
                vec![serde_json::json!(args.gid)],
            )
            .await;
            print_rpc(resp)
        }
        Some(Command::Remove(args)) => {
            let resp = rpc_client_call(
                args.port,
                args.rpc_secret,
                "aria2.remove",
                vec![serde_json::json!(args.gid)],
            )
            .await;
            print_rpc(resp)
        }
    }
}

async fn rpc_client_call(
    port: u16,
    secret: Option<String>,
    method: &str,
    params: Vec<serde_json::Value>,
) -> anyhow::Result<serde_json::Value> {
    gkdl::rpc::client::call(port, secret, method, params).await
}

fn print_rpc(resp: anyhow::Result<serde_json::Value>) -> i32 {
    match resp {
        Ok(v) => {
            if !v["error"].is_null() {
                eprintln!(
                    "RPC 错误: {} {}",
                    v["error"]["code"].as_i64().unwrap_or(-1),
                    v["error"]["message"].as_str().unwrap_or("")
                );
                return 1;
            }
            if let Some(result) = v.get("result") {
                println!("{}", result);
            }
            0
        }
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

async fn run_download(args: gkdl::app::cli::DownloadArgs) -> i32 {
    let mut config = DownloadConfig {
        split: args.split.max(1),
        ..Default::default()
    };
    if let Some(v) = args.max_speed {
        config.rate_limit = v;
    }
    if let Some(v) = args.min_split_size {
        config.min_split_size = v;
    }
    if let Some(v) = args.memory_threshold {
        config.memory_threshold = v;
    }
    if args.no_memory {
        config.memory_threshold = 0;
    }
    if let Some(v) = args.retries {
        config.max_retries = v;
    }
    if let Some(v) = args.timeout {
        config.timeout = v;
    }
    if args.no_compression {
        config.allow_compression = false;
    }

    let hook = HookConfig {
        script: args.post_script.clone(),
        ..Default::default()
    };
    // CLI 直连模式同样走状态库（无 .gkdl 控制文件），可跨次续传；打不开则降级为无持久化
    let state_db = config::default_config_path()
        .parent()
        .map(|p| p.join("state.db"))
        .and_then(|p| match StateDb::open(&p) {
            Ok(db) => Some(std::sync::Arc::new(db)),
            Err(e) => {
                eprintln!("警告: 打开状态数据库失败，本次下载将不保留续传信息: {e:#}");
                None
            }
        });
    let mgr = TaskManager::new(hook, config.clone(), state_db);

    let tty = std::io::stdout().is_terminal();
    let gid = match mgr
        .add_download(args.urls, args.output, config, args.sha256)
        .await
    {
        Ok(gid) => gid,
        Err(e) => {
            eprintln!("下载失败: {e:#}");
            return 1;
        }
    };

    let task = mgr.get(&gid).unwrap();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let status = task.status();
        if status == task_manager::TaskStatus::Complete || status == task_manager::TaskStatus::Error
        {
            break;
        }
        if tty {
            print_progress(ProgressView {
                total: task.total.load(std::sync::atomic::Ordering::Relaxed),
                completed: task.completed.load(std::sync::atomic::Ordering::Relaxed),
                speed: task.speed.load(std::sync::atomic::Ordering::Relaxed) as f64,
                connections: task.connections.load(std::sync::atomic::Ordering::Relaxed),
            });
        }
    }
    if tty {
        println!();
    }

    match mgr.wait_finished(&gid).await {
        Ok(()) => {
            if tty {
                println!(
                    "下载完成: {} ({} bytes)",
                    task.out_path.display(),
                    task.total.load(std::sync::atomic::Ordering::Relaxed)
                );
            }
            0
        }
        Err(e) => {
            eprintln!("下载失败: {e:#}");
            // 清理流式/内存模式的临时文件残留（磁盘模式的部分文件保留以便续传）
            let _ = std::fs::remove_file(task.out_path.with_extension("gkdl.tmp"));
            1
        }
    }
}

async fn run_daemon(args: DaemonArgs) -> i32 {
    // 加载配置：默认路径 %APPDATA%\gkdl\config.yaml，--config 覆盖
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(config::default_config_path);
    let file_cfg = match config::Config::load_or_create(&config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("加载配置失败: {e:#}");
            return 1;
        }
    };

    // CLI 参数优先于配置文件
    let mut daemon_cfg = file_cfg.daemon.clone();
    daemon_cfg.host = args.host.clone();
    daemon_cfg.port = args.port;
    daemon_cfg.no_tray = daemon_cfg.no_tray || args.no_tray;
    if let Some(sec) = &args.rpc_secret {
        daemon_cfg.rpc_secret = sec.clone();
    }

    let config_dir = config_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| std::path::PathBuf::from("."));

    // 初始化日志（daemon 模式）
    let _ = logging::init_logging(&config_dir, &file_cfg.log);

    // 打开状态数据库（SQLite，替换下载目录下的 .gkdl 控制文件）
    let state_db_path = {
        let p = std::path::PathBuf::from(file_cfg.state.db_path.trim());
        if p.is_absolute() {
            p
        } else {
            config_dir.join(p)
        }
    };
    let state_db = match StateDb::open(&state_db_path) {
        Ok(db) => {
            tracing::info!("状态数据库: {}", db.path.display());
            db
        }
        Err(e) => {
            eprintln!("打开状态数据库失败: {e:#}");
            return 1;
        }
    };

    // 合并下载配置
    let download_cfg = file_cfg.download.clone();
    let global_opts = GlobalOptions {
        split: download_cfg.split,
        min_split_size: download_cfg.min_split_size,
        memory_threshold: download_cfg.memory_threshold,
        retries: download_cfg.max_retries,
        timeout: download_cfg.timeout,
        piece_length: download_cfg.piece_length,
        max_overall_download_limit: download_cfg.rate_limit,
        max_concurrent_downloads: download_cfg.max_concurrent_downloads,
        user_agent: download_cfg.user_agent.clone(),
        referer: download_cfg.referer.clone(),
        header: download_cfg.header.clone(),
        allow_compression: download_cfg.allow_compression,
        dir: if download_cfg.dir.is_empty() {
            std::path::PathBuf::from(".")
        } else {
            std::path::PathBuf::from(&download_cfg.dir)
        },
        ..Default::default()
    };

    // 下载后命令：单独的配置文件，每行一条命令，下载文件路径作为最后一个参数传入
    let hook = {
        let commands_file = if file_cfg.hook.commands_file.is_empty() {
            config_dir.join("hooks.txt")
        } else {
            let p = std::path::PathBuf::from(&file_cfg.hook.commands_file);
            if p.is_absolute() {
                p
            } else {
                config_dir.join(p)
            }
        };
        if !commands_file.exists() {
            let _ = std::fs::create_dir_all(&config_dir);
            let _ = std::fs::write(
                &commands_file,
                "# 每行一条命令，下载完成后执行；下载文件路径会作为最后一个参数传入\n# 可用环境变量：GKDL_URL / GKDL_GID / GKDL_SIZE / GKDL_SHA256\n",
            );
        }
        HookConfig {
            commands_file: Some(commands_file),
            script: None,
            timeout_sec: file_cfg.hook.timeout_sec,
        }
    };

    let mgr = TaskManager::new(hook, download_cfg, Some(std::sync::Arc::new(state_db)));
    mgr.set_max_concurrent(global_opts.max_concurrent_downloads);
    mgr.set_global_rate_limit(global_opts.max_overall_download_limit);
    let global = Arc::new(tokio::sync::Mutex::new(global_opts));
    let secret = daemon_cfg.rpc_secret.clone();

    // 恢复上次中断的任务（视为暂停，不自动重启）
    match mgr.rehydrate_from_db().await {
        Ok(n) => {
            if n > 0 {
                tracing::info!("恢复 {} 个中断任务（暂停状态）", n);
            }
        }
        Err(e) => tracing::warn!("恢复中断任务失败: {e:#}"),
    }

    // 后台清理：定期删除超过保留期的已结束记录（启动时先清一次，之后每 6 小时）
    {
        let retention = file_cfg.state.retention_days;
        let db_for_cleanup = mgr.db().clone();
        tokio::spawn(async move {
            loop {
                if let Some(db) = &db_for_cleanup {
                    match db.cleanup_completed(retention) {
                        Ok(n) => {
                            if n > 0 {
                                tracing::info!("清理了 {} 条过期任务记录", n);
                            }
                        }
                        Err(e) => tracing::warn!("清理过期任务记录失败: {e:#}"),
                    }
                    // 无条件收缩 WAL 与页缓存，防止下载产生的大 WAL 常驻内存
                    if let Err(e) = db.release_memory() {
                        tracing::warn!("收缩状态库内存失败: {e:#}");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(6 * 3600)).await;
            }
        });
    }

    // 运行时配置存储：RPC 修改的设置会同步写回 config.yaml
    let store = Arc::new(ConfigStore::new(config_path.clone(), file_cfg.clone()));

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown_handle = ShutdownHandle::new(shutdown_tx);

    let state = Arc::new(rpc::server::AppState {
        mgr: Arc::clone(&mgr),
        global: Arc::clone(&global),
        session_id: format!("{:016x}", rand::random::<u64>()),
        shutdown: shutdown_handle,
        secret: secret.clone(),
        config: Some(Arc::clone(&store)),
    });

    let addr = match rpc::server::serve(state, &daemon_cfg.host, daemon_cfg.port).await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("RPC 服务启动失败: {e:#}");
            return 1;
        }
    };
    println!("gkdl daemon 已启动: http://{addr}");
    tracing::info!(
        "daemon 启动: host={} port={} secret={} tray={}",
        daemon_cfg.host,
        daemon_cfg.port,
        if secret.is_empty() {
            "无"
        } else {
            "已配置"
        },
        !daemon_cfg.no_tray
    );

    // 托盘（可选）
    let (tray_tx, mut tray_rx) = tokio::sync::mpsc::unbounded_channel::<tray::TrayCommand>();
    if !daemon_cfg.no_tray {
        let log_dir = resolve_log_dir(&config_dir, &file_cfg.log);
        let aria_ng_url = if daemon_cfg.aria_ng_url.is_empty() {
            None
        } else {
            Some(daemon_cfg.aria_ng_url.clone())
        };
        tray::spawn_tray(tray_tx, config_dir.clone(), log_dir, aria_ng_url);
    }

    // 主循环：等待 shutdown / 托盘命令
    let (grace_shutdown_tx, mut grace_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (force_shutdown_tx, mut force_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

    tokio::spawn(async move {
        match shutdown_rx.recv().await {
            Some(ShutdownKind::Graceful) => {
                let _ = grace_shutdown_tx.send(());
            }
            Some(ShutdownKind::Force) => {
                let _ = force_shutdown_tx.send(());
            }
            None => {}
        }
    });

    loop {
        tokio::select! {
            _ = grace_rx.recv() => {
                tracing::info!("收到 shutdown，3 秒后退出");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                break;
            }
            _ = force_rx.recv() => {
                tracing::info!("收到 forceShutdown，立即退出");
                break;
            }
            cmd = tray_rx.recv() => {
                match cmd {
                    Some(tray::TrayCommand::Quit) => {
                        tracing::info!("托盘退出，3 秒后关闭");
                        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                        break;
                    }
                    None => {}
                }
            }
        }
    }
    0
}

/// 解析日志文件所在目录（与 logging::init_logging 的解析规则一致）。
fn resolve_log_dir(config_dir: &std::path::Path, log_cfg: &LogConfig) -> std::path::PathBuf {
    let p = std::path::PathBuf::from(&log_cfg.file);
    let resolved = if p.is_absolute() {
        p
    } else {
        config_dir.join(p)
    };
    resolved
        .parent()
        .map(|d| d.to_path_buf())
        .unwrap_or_else(|| config_dir.to_path_buf())
}

/// Windows：GUI 子系统程序默认不分配控制台。从终端启动时附着到父进程控制台，
/// 恢复 stdout/stderr 输出；双击启动（无父控制台）则保持无窗口。
#[cfg(windows)]
fn attach_parent_console() {
    use windows_sys::Win32::System::Console::{
        AttachConsole, GetStdHandle, SetStdHandle, ATTACH_PARENT_PROCESS, STD_ERROR_HANDLE,
        STD_OUTPUT_HANDLE,
    };
    unsafe {
        if AttachConsole(ATTACH_PARENT_PROCESS) == 0 {
            return;
        }
        let out = GetStdHandle(STD_OUTPUT_HANDLE);
        if !out.is_null() {
            SetStdHandle(STD_OUTPUT_HANDLE, out);
        }
        let err = GetStdHandle(STD_ERROR_HANDLE);
        if !err.is_null() {
            SetStdHandle(STD_ERROR_HANDLE, err);
        }
    }
}

fn print_progress(p: ProgressView) {
    let pct = if p.total > 0 {
        p.completed as f64 * 100.0 / p.total as f64
    } else {
        0.0
    };
    let speed = human_bytes(p.speed);
    print!(
        "\r进度 {:>6.1}%  {}/{}  {}B/s  连接{}",
        pct, p.completed, p.total, speed, p.connections
    );
    let _ = std::io::stdout().flush();
}

struct ProgressView {
    total: u64,
    completed: u64,
    speed: f64,
    connections: usize,
}

fn human_bytes(v: f64) -> String {
    let units = ["", "K", "M", "G", "T"];
    let mut val = v;
    let mut idx = 0;
    while val >= 1024.0 && idx < units.len() - 1 {
        val /= 1024.0;
        idx += 1;
    }
    format!("{:.1}{}", val, units[idx])
}
