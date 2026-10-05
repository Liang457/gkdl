use crate::app::db::{DownloadRecord, StateDb};
use crate::app::hooks::{self, HookConfig, HookContext};
use crate::download::config::DownloadConfig;
use crate::download::engine::{self, Progress};
use crate::download::rate_limit::TokenBucket;
use anyhow::{anyhow, bail, Result};
use rand::RngExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Waiting,
    Active,
    Paused,
    Error,
    Complete,
    Removed,
}

impl TaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Waiting => "waiting",
            TaskStatus::Active => "active",
            TaskStatus::Paused => "paused",
            TaskStatus::Error => "error",
            TaskStatus::Complete => "complete",
            TaskStatus::Removed => "removed",
        }
    }
}

/// 单个下载任务。
pub struct Task {
    pub gid: String,
    pub urls: Vec<String>,
    pub out_path: PathBuf,
    pub status: Mutex<TaskStatus>,
    pub total: AtomicU64,
    pub completed: AtomicU64,
    pub speed: AtomicU64,
    pub connections: AtomicUsize,
    pub error_code: AtomicI32,
    pub error_message: Mutex<Option<String>>,
    pub sha256: Option<String>,
    pub created_at: i64,
    pub scheduler: Mutex<Option<Arc<crate::download::scheduler::WorkStealingScheduler>>>,
    pub limiter: Mutex<Option<crate::download::rate_limit::TokenBucket>>,
    pub cancel_token: Mutex<Option<CancellationToken>>,
    pub driver: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// 每任务限速（config.rate_limit，0=不限/共享全局）
    pub rate_limit: AtomicU64,
    /// 任务级取消令牌：排队等待槽位期间即可被取消
    pub task_token: CancellationToken,
    /// 排队中暂停/恢复通知：unpause 时唤醒正在等待槽位的 driver
    pub resume_notify: Notify,
}

impl Task {
    pub fn status(&self) -> TaskStatus {
        *self.status.lock().unwrap()
    }
}

#[derive(Debug, Clone)]
pub enum TaskEvent {
    Started {
        gid: String,
    },
    Paused {
        gid: String,
    },
    Stopped {
        gid: String,
    },
    Complete {
        gid: String,
    },
    #[allow(dead_code)]
    Error {
        gid: String,
        code: i32,
    },
}

/// 并发下载门控：限制同时下载的任务数。`max == 0` 表示不限。
pub struct ConcurrencyGate {
    max: AtomicU64,
    active: AtomicU64,
    /// 状态变更信号（watch 版本号）：`set_max` 与槽位释放必然 +1。
    /// `acquire` 依赖版本判定可靠唤醒，规避 `Notify::notify_waiters`
    /// 在「检查后、注册前」发出导致排队任务永久挂起的竞态。
    notify_tx: watch::Sender<()>,
    notify_rx: watch::Receiver<()>,
}

impl ConcurrencyGate {
    pub fn new() -> Self {
        let (notify_tx, notify_rx) = watch::channel(());
        Self {
            max: AtomicU64::new(0),
            active: AtomicU64::new(0),
            notify_tx,
            notify_rx,
        }
    }

    pub fn set_max(&self, n: u64) {
        self.max.store(n, Ordering::Relaxed);
        let _ = self.notify_tx.send(());
    }

    /// 尝试获取槽位；排队期间被取消则返回 None。
    pub async fn acquire(&self, cancelled: &CancellationToken) -> Option<ConcurrencyPermit<'_>> {
        // 接收端在等待前克隆并保持旧版本号：任何已发生的变更都会让 changed() 立即返回
        let mut rx = self.notify_rx.clone();
        loop {
            if cancelled.is_cancelled() {
                return None;
            }
            let max = self.max.load(Ordering::Relaxed);
            if max == 0 {
                // 不限并发：返回不占槽位的守卫（Drop 时不递减 active）
                return Some(ConcurrencyPermit {
                    gate: self,
                    counted: false,
                });
            }
            let cur = self.active.load(Ordering::Relaxed);
            if cur < max {
                if self
                    .active
                    .compare_exchange_weak(cur, cur + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    return Some(ConcurrencyPermit {
                        gate: self,
                        counted: true,
                    });
                }
                continue;
            }
            tokio::select! {
                r = rx.changed() => {
                    if r.is_err() {
                        return None; // 发送端已释放（门控销毁）
                    }
                }
                _ = cancelled.cancelled() => return None,
            }
        }
    }
}

impl Default for ConcurrencyGate {
    fn default() -> Self {
        Self::new()
    }
}

/// 并发槽位守卫：Drop 时释放并唤醒等待者。
pub struct ConcurrencyPermit<'a> {
    gate: &'a ConcurrencyGate,
    /// 是否实际占了 active 计数（max==0 时不占）
    counted: bool,
}

impl Drop for ConcurrencyPermit<'_> {
    fn drop(&mut self) {
        if self.counted {
            self.gate.active.fetch_sub(1, Ordering::SeqCst);
        }
        // 先释放槽位再发信号，等待者醒来后能看到可用的槽位
        let _ = self.gate.notify_tx.send(());
    }
}

/// 输出路径占用状态。
///
/// 多个任务可指向同一输出路径（默认按文件名解析）。记录当前占用该路径的任务，
/// 以及该路径上是否已存在一份完整下载结果，用于防止「旧任务迟到的清理」误删
/// 「新任务已下载完成」的文件。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct PathOwnership {
    /// 当前占用该输出路径的任务 GID（无占用者为 None）。
    owner: Option<String>,
    /// 该路径上是否已存在一份完整下载结果。新任务开始下载时清除；
    /// 置位后任何其它任务的清理都不得删除该文件。
    complete: bool,
}

/// 任务管理器：GID 任务表 + 状态机 + 事件广播 + 触发 hook。
pub struct TaskManager {
    tasks: RwLock<HashMap<String, Arc<Task>>>,
    events: broadcast::Sender<TaskEvent>,
    hook: HookConfig,
    global_limiter: crate::download::rate_limit::TokenBucket,
    gate: ConcurrencyGate,
    /// 状态数据库（None 则不做持久化/跨进程续传）
    db: Option<Arc<StateDb>>,
    /// 输出路径占用状态（路径 → 属主/完成标记），防止误删共享路径的下载结果。
    path_state: RwLock<HashMap<PathBuf, PathOwnership>>,
}

impl TaskManager {
    pub fn new(
        hook: HookConfig,
        _default_config: DownloadConfig,
        db: Option<Arc<StateDb>>,
    ) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(128);
        Arc::new(Self {
            tasks: RwLock::new(HashMap::new()),
            events: tx,
            hook,
            global_limiter: crate::download::rate_limit::TokenBucket::new(0),
            gate: ConcurrencyGate::new(),
            db,
            path_state: RwLock::new(HashMap::new()),
        })
    }

    /// 设置全局并发下载数上限（0=不限）。
    pub fn set_max_concurrent(&self, n: usize) {
        self.gate.set_max(n as u64);
    }

    /// 设置全局共享限速（max-overall-download-limit）。
    pub fn set_global_rate_limit(&self, rate: u64) {
        self.global_limiter.set_rate(rate);
        // 只应用到未单独限速的任务（rate_limit == 0 表示共享全局桶）；
        // 自带独立限速桶（rate_limit > 0）的任务不受全局限速影响。
        for t in self.all_tasks() {
            if t.rate_limit.load(Ordering::Relaxed) > 0 {
                continue;
            }
            let limiter = {
                let guard = t.limiter.lock().unwrap();
                guard.as_ref().cloned()
            };
            if let Some(l) = limiter {
                l.set_rate(rate);
            }
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskEvent> {
        self.events.subscribe()
    }

    /// 状态数据库引用（None 表示未启用持久化）。
    pub fn db(&self) -> Option<Arc<StateDb>> {
        self.db.clone()
    }

    pub fn get(&self, gid: &str) -> Option<Arc<Task>> {
        self.tasks.read().unwrap().get(gid).cloned()
    }

    pub fn all_tasks(&self) -> Vec<Arc<Task>> {
        self.tasks.read().unwrap().values().cloned().collect()
    }

    pub fn gen_gid() -> String {
        let mut rng = rand::rng();
        (0..16)
            .map(|_| format!("{:x}", rng.random_range(0..16)))
            .collect()
    }

    /// 添加下载并加入队列。任务先在 waiting 队列等待并发槽位，随后自动启动。
    /// 返回 GID（立即返回，不阻塞探测）。
    pub async fn add_download(
        self: &Arc<Self>,
        urls: Vec<String>,
        out_path: Option<PathBuf>,
        config: DownloadConfig,
        sha256: Option<String>,
    ) -> Result<String> {
        if urls.is_empty() {
            bail!("未提供下载地址");
        }
        let gid = Self::gen_gid();
        let out_path_resolved =
            out_path.unwrap_or_else(|| PathBuf::from(engine::filename_from_url(&urls[0])));

        // 单独限速用独立桶；否则共享全局桶
        let limiter = if config.rate_limit > 0 {
            crate::download::rate_limit::TokenBucket::new(config.rate_limit)
        } else {
            self.global_limiter.clone()
        };

        let task_token = CancellationToken::new();
        let task = Arc::new(Task {
            gid: gid.clone(),
            urls: urls.clone(),
            out_path: out_path_resolved,
            status: Mutex::new(TaskStatus::Waiting),
            total: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            speed: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            error_code: AtomicI32::new(0),
            error_message: Mutex::new(None),
            sha256: sha256.clone(),
            created_at: chrono::Utc::now().timestamp(),
            scheduler: Mutex::new(None),
            limiter: Mutex::new(None),
            cancel_token: Mutex::new(None),
            driver: Mutex::new(None),
            rate_limit: AtomicU64::new(config.rate_limit),
            task_token,
            resume_notify: Notify::new(),
        });

        self.tasks
            .write()
            .unwrap()
            .insert(gid.clone(), Arc::clone(&task));

        // 登记该任务对该输出路径的占用（清除旧的「已有完整结果」标记）
        self.claim_path(&task.out_path, &gid);
        self.spawn_driver(task, config, sha256, limiter).await;
        Ok(gid)
    }

    /// 从状态库恢复中断任务（active/paused）。恢复后的任务进入「暂停」状态，
    /// 不自动重启；用户 unpause 后驱动协程会按状态库记录续传（磁盘）或重下（内存）。
    pub async fn rehydrate_from_db(self: &Arc<Self>) -> Result<usize> {
        let Some(db) = &self.db else {
            return Ok(0);
        };
        let records = db.list_resumable()?;
        let mut count = 0usize;
        for rec in records {
            if self.get(&rec.gid).is_some() {
                continue;
            }
            self.rehydrate_task(rec).await?;
            count += 1;
        }
        Ok(count)
    }

    async fn rehydrate_task(self: &Arc<Self>, rec: DownloadRecord) -> Result<()> {
        // 用记录里的分片/限速参数重建配置（UA/超时等沿用默认值，续传时影响不大）
        let config = DownloadConfig {
            split: rec.split.max(1),
            min_split_size: rec.min_split_size,
            rate_limit: rec.rate_limit,
            ..Default::default()
        };
        let limiter = if rec.rate_limit > 0 {
            crate::download::rate_limit::TokenBucket::new(rec.rate_limit)
        } else {
            self.global_limiter.clone()
        };
        let completed = rec.completed_bytes();
        let task_token = CancellationToken::new();
        let task = Arc::new(Task {
            gid: rec.gid.clone(),
            urls: rec.urls.clone(),
            out_path: PathBuf::from(&rec.out_path),
            status: Mutex::new(TaskStatus::Paused),
            total: AtomicU64::new(rec.total),
            completed: AtomicU64::new(completed),
            speed: AtomicU64::new(0),
            connections: AtomicUsize::new(0),
            error_code: AtomicI32::new(0),
            error_message: Mutex::new(None),
            sha256: rec.sha256.clone(),
            created_at: rec.created_at,
            scheduler: Mutex::new(None),
            limiter: Mutex::new(None),
            cancel_token: Mutex::new(None),
            driver: Mutex::new(None),
            rate_limit: AtomicU64::new(rec.rate_limit),
            task_token,
            resume_notify: Notify::new(),
        });
        let gid = rec.gid.clone();
        self.tasks
            .write()
            .unwrap()
            .insert(gid.clone(), Arc::clone(&task));
        self.claim_path(&task.out_path, &gid);
        self.spawn_driver(task, config, rec.sha256, limiter).await;
        tracing::info!("已恢复中断任务 {}（暂停，等待用户恢复）", gid);
        Ok(())
    }

    /// 启动任务的驱动协程：等待并发槽位 → 启动引擎 → 监听状态/事件。
    /// `task.status` 的初始值决定行为（Waiting 自动启动；Paused 等待 unpause）。
    async fn spawn_driver(
        self: &Arc<Self>,
        task: Arc<Task>,
        config: DownloadConfig,
        sha256: Option<String>,
        limiter: TokenBucket,
    ) {
        let mgr = Arc::clone(self);
        let task_for_driver = Arc::clone(&task);
        let progress_task = Arc::clone(&task);
        let urls = task.urls.clone();
        let limiter_for_engine = limiter.clone();
        let driver = tokio::spawn(async move {
            // 等待并发槽位（max=0 立即通过）；排队期间被移除/取消则放弃。
            // 排队中被暂停的任务不占槽位，等 unpause 唤醒后再抢槽。
            let permit = loop {
                if task_for_driver.task_token.is_cancelled()
                    || *task_for_driver.status.lock().unwrap() == TaskStatus::Removed
                {
                    return;
                }
                if *task_for_driver.status.lock().unwrap() == TaskStatus::Paused {
                    // 排队中被暂停：等待 unpause；同时监听取消/删除，防止永久挂起
                    tokio::select! {
                        _ = task_for_driver.resume_notify.notified() => {}
                        _ = task_for_driver.task_token.cancelled() => return,
                    }
                    continue;
                }
                let Some(p) = mgr.gate.acquire(&task_for_driver.task_token).await else {
                    return;
                };
                // 抢到槽位后若被暂停/移除：归还槽位（p 在此 Drop）重新循环
                if task_for_driver.task_token.is_cancelled()
                    || *task_for_driver.status.lock().unwrap() != TaskStatus::Waiting
                {
                    continue;
                }
                break p;
            };
            let _permit = permit;
            if task_for_driver.task_token.is_cancelled()
                || *task_for_driver.status.lock().unwrap() == TaskStatus::Removed
            {
                return;
            }

            // 探测与下载共用的取消令牌：启动前即存入任务，保证探测阶段也可被取消
            let ct = CancellationToken::new();
            *task_for_driver.cancel_token.lock().unwrap() = Some(ct.clone());

            let handle = match engine::start_download(
                urls.clone(),
                Some(task_for_driver.out_path.clone()),
                config,
                sha256.clone(),
                Some(limiter_for_engine),
                ct,
                mgr.db.clone(),
                Some(task_for_driver.gid.clone()),
                move |p: Progress| {
                    progress_task.total.store(p.total, Ordering::Relaxed);
                    progress_task
                        .completed
                        .store(p.completed, Ordering::Relaxed);
                    progress_task.speed.store(p.speed as u64, Ordering::Relaxed);
                    progress_task
                        .connections
                        .store(p.connections, Ordering::Relaxed);
                },
            )
            .await
            {
                Ok(h) => h,
                Err(e) => {
                    // 探测失败；若任务已被移除/取消则不再报错（任务已删除）
                    if task_for_driver.status() != TaskStatus::Removed
                        && !task_for_driver.task_token.is_cancelled()
                    {
                        mgr.finish_error(&task_for_driver, e).await;
                    }
                    return;
                }
            };

            // 探测期间任务已被移除/取消：取消引擎并清理临时文件，绝不重新激活
            if task_for_driver.task_token.is_cancelled()
                || *task_for_driver.status.lock().unwrap() == TaskStatus::Removed
            {
                handle.cancel();
                let _ = handle.join().await;
                mgr.cleanup_incomplete_files(&task_for_driver).await;
                return;
            }

            {
                let mut limiter_slot = task_for_driver.limiter.lock().unwrap();
                *limiter_slot = Some(limiter);
            }
            {
                let mut sched = task_for_driver.scheduler.lock().unwrap();
                *sched = Some(handle.scheduler_ref());
            }

            // 排队期间已被暂停：以暂停状态启动
            let start_paused = *task_for_driver.status.lock().unwrap() == TaskStatus::Paused;
            if start_paused {
                handle.scheduler_ref().set_paused(true);
                *task_for_driver.status.lock().unwrap() = TaskStatus::Paused;
                let _ = mgr.events.send(TaskEvent::Paused {
                    gid: task_for_driver.gid.clone(),
                });
            } else {
                *task_for_driver.status.lock().unwrap() = TaskStatus::Active;
                let _ = mgr.events.send(TaskEvent::Started {
                    gid: task_for_driver.gid.clone(),
                });
            }

            match handle.join().await {
                Ok(report) => {
                    task_for_driver.total.store(report.total, Ordering::Relaxed);
                    task_for_driver
                        .completed
                        .store(report.total, Ordering::Relaxed);
                    // 标记该路径已有完整结果：之后任何旧任务迟到的清理都不得删除此文件
                    mgr.mark_path_complete(&task_for_driver.out_path, &task_for_driver.gid);
                    *task_for_driver.status.lock().unwrap() = TaskStatus::Complete;
                    let _ = mgr.events.send(TaskEvent::Complete {
                        gid: task_for_driver.gid.clone(),
                    });
                    mgr.run_hook(&task_for_driver).await;
                }
                Err(e) => {
                    mgr.finish_error(&task_for_driver, e).await;
                }
            }

            // 任务已被移除：清理控制文件与部分文件
            if *task_for_driver.status.lock().unwrap() == TaskStatus::Removed {
                mgr.cleanup_incomplete_files(&task_for_driver).await;
            }

            // 终态：释放调度器内的速度历史等临时内存（保留段表供 tellStatus 展示）
            let sched = {
                let guard = task_for_driver.scheduler.lock().unwrap();
                guard.as_ref().map(Arc::clone)
            };
            if let Some(sched) = sched {
                sched.trim().await;
            }
            // 终态：收缩状态库 WAL 并释放页缓存，避免下载期间的段写入占用常驻内存
            if let Some(db) = &mgr.db {
                if let Err(e) = db.release_memory() {
                    tracing::warn!("收缩状态库内存失败: {}", e);
                }
            }
        });
        *task.driver.lock().unwrap() = Some(driver);
    }

    async fn finish_error(self: &Arc<Self>, task: &Arc<Task>, err: anyhow::Error) {
        // 用户取消/移除不是异常：引擎收尾时会以取消错误结束，此处不记失败、不发错误事件
        if task.status() == TaskStatus::Removed || task.task_token.is_cancelled() {
            tracing::debug!("任务 {} 已被用户取消，忽略引擎错误: {err:#}", task.gid);
            return;
        }
        let msg = format!("{err:#}");
        tracing::error!("任务 {} 失败: {}", task.gid, msg);
        *task.error_message.lock().unwrap() = Some(msg.clone());
        task.error_code
            .store(map_error_code(&msg), Ordering::Relaxed);
        if task.status() != TaskStatus::Removed {
            *task.status.lock().unwrap() = TaskStatus::Error;
            let _ = self.events.send(TaskEvent::Error {
                gid: task.gid.clone(),
                code: task.error_code.load(Ordering::Relaxed),
            });
        }
    }

    async fn run_hook(self: &Arc<Self>, task: &Arc<Task>) {
        let file_path = task.out_path.clone();
        let url = task.urls.first().cloned().unwrap_or_default();
        let gid = task.gid.clone();
        let size = task.total.load(Ordering::Relaxed);
        let sha256 = task.sha256.clone();
        let timeout = self.hook.timeout_sec;
        let ctx = HookContext {
            file_path: &file_path,
            url: &url,
            gid: &gid,
            size,
            sha256: sha256.as_deref(),
            timeout,
            hide_window: self.hook.hide_window,
        };

        // 命令列表文件：每行一条命令，逐条执行
        if let Some(cmds_file) = &self.hook.commands_file {
            match hooks::load_commands(cmds_file) {
                Ok(commands) if !commands.is_empty() => {
                    if let Err(e) = hooks::run_post_download_commands(&commands, ctx).await {
                        tracing::warn!("下载后命令执行失败: {e}");
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!("读取下载后命令配置失败: {e}"),
            }
        }
        // 旧式单脚本方式（CLI --post-script）
        if let Some(script) = &self.hook.script {
            if let Err(e) = hooks::run_post_download_script(script, ctx).await {
                tracing::warn!("下载后脚本执行失败: {e}");
            }
        }
    }

    pub async fn pause(&self, gid: &str) -> Result<()> {
        let task = self.get(gid).ok_or_else(|| anyhow!("GID {} 不存在", gid))?;
        let sched = {
            let guard = task.scheduler.lock().unwrap();
            guard.as_ref().map(Arc::clone)
        };
        match sched {
            Some(s) => {
                s.set_paused(true);
                if task.status() != TaskStatus::Paused {
                    *task.status.lock().unwrap() = TaskStatus::Paused;
                }
                let _ = self.events.send(TaskEvent::Paused {
                    gid: gid.to_string(),
                });
                tracing::info!("任务 {} 已暂停", gid);
                Ok(())
            }
            None => {
                // 排队中的任务：仅切换状态，driver 启动引擎时保持暂停
                if task.status() == TaskStatus::Waiting {
                    *task.status.lock().unwrap() = TaskStatus::Paused;
                    let _ = self.events.send(TaskEvent::Paused {
                        gid: gid.to_string(),
                    });
                    tracing::info!("任务 {} 已暂停（排队中）", gid);
                    Ok(())
                } else {
                    bail!("任务尚未启动")
                }
            }
        }
    }

    pub async fn unpause(&self, gid: &str) -> Result<()> {
        let task = self.get(gid).ok_or_else(|| anyhow!("GID {} 不存在", gid))?;
        let sched = {
            let guard = task.scheduler.lock().unwrap();
            guard.as_ref().map(Arc::clone)
        };
        match sched {
            Some(s) => {
                s.set_paused(false);
                if task.status() == TaskStatus::Paused {
                    *task.status.lock().unwrap() = TaskStatus::Active;
                }
                let _ = self.events.send(TaskEvent::Started {
                    gid: gid.to_string(),
                });
                Ok(())
            }
            None => {
                // 排队中：回到 waiting 队列并唤醒正在等槽位的 driver
                if task.status() == TaskStatus::Paused {
                    *task.status.lock().unwrap() = TaskStatus::Waiting;
                    task.resume_notify.notify_one();
                    Ok(())
                } else {
                    bail!("任务尚未启动")
                }
            }
        }
    }

    /// 移除任务（保留下载结果；未完成的临时文件由 driver 清理）。
    pub async fn remove(&self, gid: &str, force: bool) -> Result<()> {
        let task = self.get(gid).ok_or_else(|| anyhow!("GID {} 不存在", gid))?;
        let status = task.status();
        if status == TaskStatus::Active && !force {
            bail!("任务正在下载，请使用 forceRemove");
        }
        // 取消排队中的 driver 与运行中的引擎
        task.task_token.cancel();
        task.resume_notify.notify_one(); // 唤醒排队中被暂停阻塞的 driver，避免其永久挂起
        if let Some(ct) = task.cancel_token.lock().unwrap().as_ref() {
            ct.cancel();
        }
        if let Some(s) = task.scheduler.lock().unwrap().as_ref() {
            s.set_paused(false); // 唤醒被暂停阻塞的 worker
        }
        *task.status.lock().unwrap() = TaskStatus::Removed;
        tracing::info!("任务 {} 已移除", gid);
        let _ = self.events.send(TaskEvent::Stopped {
            gid: gid.to_string(),
        });
        self.tasks.write().unwrap().remove(gid);
        if status != TaskStatus::Active {
            // 引擎不在运行，立即清理（Active 任务由 driver 结束后清理）
            self.cleanup_incomplete_files(&task).await;
        }
        Ok(())
    }

    /// 移除单个下载结果（stopped 任务）。未完成的任务同时清理临时文件；
    /// 已完成/出错任务也一并删除状态库记录。
    pub async fn remove_download_result(&self, gid: &str) -> Result<()> {
        let task = self.get(gid).ok_or_else(|| anyhow!("GID {} 不存在", gid))?;
        if matches!(
            task.status(),
            TaskStatus::Active | TaskStatus::Waiting | TaskStatus::Paused
        ) {
            bail!("任务仍在队列或下载中");
        }
        if let Some(db) = &self.db {
            if let Err(e) = db.delete(gid) {
                tracing::warn!("删除任务 {} 的状态记录失败: {}", gid, e);
            }
        }
        self.cleanup_incomplete_files(&task).await;
        self.tasks.write().unwrap().remove(gid);
        Ok(())
    }

    /// 登记路径占用：新任务开始使用该输出路径（清除旧的「已有完整结果」标记）。
    fn claim_path(&self, path: &Path, gid: &str) {
        self.path_state.write().unwrap().insert(
            path.to_path_buf(),
            PathOwnership {
                owner: Some(gid.to_string()),
                complete: false,
            },
        );
    }

    /// 标记该路径已有完整下载结果（任务下载完成时调用）。
    /// 此后其它任务（含旧任务迟到的清理）都不得删除该文件。
    fn mark_path_complete(&self, path: &Path, gid: &str) {
        let mut m = self.path_state.write().unwrap();
        if let Some(s) = m.get_mut(path) {
            if s.owner.as_deref() == Some(gid) {
                s.complete = true;
            }
        }
    }

    /// 释放该任务对该路径的占用（保留 complete 标记，防止旧任务清理误删已完成文件）。
    /// 无占用且无完整结果标记的条目已无意义，直接移除避免 path_state 长期驻留增长。
    fn release_path(&self, path: &Path, gid: &str) {
        let mut m = self.path_state.write().unwrap();
        if let Some(s) = m.get_mut(path) {
            if s.owner.as_deref() == Some(gid) {
                s.owner = None;
                if !s.complete {
                    m.remove(path);
                }
            }
        }
    }

    /// 判断本任务能否删除该路径下的文件：
    /// 仅当该路径无「已有完整结果」标记，且当前占用者不是其它任务时才允许删除。
    fn can_delete_path(&self, path: &Path, gid: &str) -> bool {
        let m = self.path_state.read().unwrap();
        match m.get(path) {
            None => true,
            Some(s) if s.complete => false,
            Some(s) => s.owner.as_deref().is_none_or(|o| o == gid),
        }
    }

    /// 清理未完成下载的临时文件：状态库记录 + 残留控制文件 + 部分文件。
    /// 已完整下载的任务保留文件。
    async fn cleanup_incomplete_files(&self, task: &Task) {
        let total = task.total.load(Ordering::Relaxed);
        let completed = task.completed.load(Ordering::Relaxed);
        // 先释放本任务的路径占用；释放后若该路径已被其它任务占用，
        // 或已标记「完整结果」，则不得删除该路径下的文件。
        self.release_path(&task.out_path, &task.gid);
        if total > 0 && completed >= total {
            return;
        }
        // 删除状态库记录（含段，级联）
        if let Some(db) = &self.db {
            if let Err(e) = db.delete(&task.gid) {
                tracing::warn!("删除任务 {} 的状态记录失败: {}", task.gid, e);
            }
        }
        // 清理旧版控制文件残留（新版本不再生成，仅防御性删除）
        let legacy = PathBuf::from(format!("{}.gkdl", task.out_path.display()));
        std::fs::remove_file(&legacy).ok();
        std::fs::remove_file(legacy.with_extension("gkdl.tmp")).ok();
        // 清理流式/内存模式残留的临时文件（`{stem}.gkdl.tmp`，与旧版命名不同）
        std::fs::remove_file(task.out_path.with_extension("gkdl.tmp")).ok();
        // 仅在确认该路径仍归属本任务（或已无占用且无完整结果）时删除输出文件，
        // 防止旧任务迟到的清理误删新任务已下载完成的文件。
        if self.can_delete_path(&task.out_path, &task.gid) {
            std::fs::remove_file(&task.out_path).ok();
        }
    }

    /// 清理已完成/出错/已移除的任务。
    pub async fn purge(&self) {
        let finished: Vec<Arc<Task>> = self
            .all_tasks()
            .into_iter()
            .filter(|t| {
                matches!(
                    t.status(),
                    TaskStatus::Complete | TaskStatus::Error | TaskStatus::Removed
                )
            })
            .collect();
        let mut guard = self.tasks.write().unwrap();
        for t in &finished {
            if let Some(db) = &self.db {
                if let Err(e) = db.delete(&t.gid) {
                    tracing::warn!("清除任务 {} 的状态记录失败: {}", t.gid, e);
                }
            }
            guard.remove(&t.gid);
        }
        drop(guard);
        // 释放各任务对输出路径的占用（保留 complete 标记，避免旧清理误删已完成文件）
        for t in &finished {
            self.release_path(&t.out_path, &t.gid);
        }
    }

    /// 等待某任务进入终态（complete/error），并等待 driver（含 hook）结束。
    pub async fn wait_finished(&self, gid: &str) -> Result<()> {
        let mut rx = self.subscribe();
        let result = loop {
            if let Some(task) = self.get(gid) {
                match task.status() {
                    TaskStatus::Complete => break Ok(()),
                    TaskStatus::Error => {
                        let msg = task
                            .error_message
                            .lock()
                            .unwrap()
                            .clone()
                            .unwrap_or_default();
                        break Err(anyhow!(msg));
                    }
                    TaskStatus::Removed => break Err(anyhow!("任务已移除")),
                    _ => {}
                }
            } else {
                break Err(anyhow!("GID {} 不存在", gid));
            }
            match rx.recv().await {
                Ok(_) => continue,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break Err(anyhow!("任务管理器已关闭")),
            }
        };
        if let Some(task) = self.get(gid) {
            // 先取出 driver 再 await，避免持锁跨 await 点
            let driver = task.driver.lock().unwrap().take();
            if let Some(driver) = driver {
                driver.await.ok();
            }
        }
        result
    }
}

fn map_error_code(msg: &str) -> i32 {
    let lower = msg.to_lowercase();
    if lower.contains("sha-256") || lower.contains("校验") || lower.contains("checksum") {
        24
    } else if lower.contains("超时") || lower.contains("timeout") {
        2
    } else if lower.contains("404") || lower.contains("not found") || lower.contains("不存在") {
        3
    } else if lower.contains("网络") || lower.contains("connect") || lower.contains("dns") {
        6
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_mapping() {
        assert_eq!(map_error_code("SHA-256 校验失败: xxx"), 24);
        assert_eq!(map_error_code("连接超时"), 2);
        assert_eq!(map_error_code("HTTP 404 Not Found"), 3);
        assert_eq!(map_error_code("dns error"), 6);
        assert_eq!(map_error_code("未知错误"), 1);
    }

    #[test]
    fn gid_format() {
        let gid = TaskManager::gen_gid();
        assert_eq!(gid.len(), 16);
        assert!(gid.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn path_ownership_blocks_stale_cleanup() {
        let mgr = TaskManager::new(HookConfig::default(), DownloadConfig::default(), None);
        let f = PathBuf::from("C:/dl/f.bin");
        let old = "aaaa000000000001";
        let fresh = "bbbb000000000002";

        // 任务 A 占用路径，其自己的清理允许删除
        mgr.claim_path(&f, old);
        assert!(mgr.can_delete_path(&f, old), "属主自身可删除自己的部分文件");

        // 新任务接管路径后，旧任务的清理不得再删除该文件
        mgr.claim_path(&f, fresh);
        assert!(
            !mgr.can_delete_path(&f, old),
            "旧任务不得删除新任务占用路径下的文件"
        );
        assert!(mgr.can_delete_path(&f, fresh));

        // 新任务下载完成后标记完整结果：任何其它任务的清理都不允许删除
        mgr.mark_path_complete(&f, fresh);
        assert!(
            !mgr.can_delete_path(&f, old),
            "存在完整结果时旧任务不得删除文件"
        );
        assert!(
            !mgr.can_delete_path(&f, fresh),
            "存在完整结果时即使属主也不应删除（由 complete 分支拦截）"
        );

        // 新任务被移除：释放占用，但 complete 标记保留 → 旧任务仍不能删除
        mgr.release_path(&f, fresh);
        assert!(
            !mgr.can_delete_path(&f, old),
            "释放后旧任务仍不得删除已有完整结果的文件"
        );

        // 新的下载再次接管：清除 complete 标记，其自身可删除，旧任务仍不可
        mgr.claim_path(&f, "cccc000000000003");
        assert!(mgr.can_delete_path(&f, "cccc000000000003"));
        assert!(!mgr.can_delete_path(&f, old));

        // 无占用且无完整结果的条目在释放时被移除 → 判定退回默认防御路径（允许删除）
        mgr.release_path(&f, "cccc000000000003");
        assert!(
            mgr.can_delete_path(&f, old),
            "释放后无占用且无完整结果，条目应被移除并允许删除"
        );

        // 无任何记录/占用的路径允许删除（旧版防御性清理路径）
        let fresh_path = PathBuf::from("C:/dl/other.bin");
        assert!(mgr.can_delete_path(&fresh_path, old));
    }
}
