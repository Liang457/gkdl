use crate::app::hooks::{self, HookConfig, HookContext};
use crate::download::config::DownloadConfig;
use crate::download::engine::{self, Progress};
use crate::download::resume::ResumeState;
use anyhow::{anyhow, bail, Result};
use rand::RngExt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;
use tokio::sync::Notify;
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
    notify: Notify,
}

impl ConcurrencyGate {
    pub fn new() -> Self {
        Self {
            max: AtomicU64::new(0),
            active: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub fn set_max(&self, n: u64) {
        self.max.store(n, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    /// 尝试获取槽位；排队期间被取消则返回 None。
    pub async fn acquire(&self, cancelled: &CancellationToken) -> Option<ConcurrencyPermit<'_>> {
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
                _ = self.notify.notified() => {}
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
        self.gate.notify.notify_waiters();
    }
}

/// 任务管理器：GID 任务表 + 状态机 + 事件广播 + 触发 hook。
pub struct TaskManager {
    tasks: RwLock<HashMap<String, Arc<Task>>>,
    events: broadcast::Sender<TaskEvent>,
    hook: HookConfig,
    global_limiter: crate::download::rate_limit::TokenBucket,
    gate: ConcurrencyGate,
}

impl TaskManager {
    pub fn new(hook: HookConfig, _default_config: DownloadConfig) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(128);
        Arc::new(Self {
            tasks: RwLock::new(HashMap::new()),
            events: tx,
            hook,
            global_limiter: crate::download::rate_limit::TokenBucket::new(0),
            gate: ConcurrencyGate::new(),
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
        let gid = Self::gen_gid();
        let out_path_resolved =
            out_path.unwrap_or_else(|| PathBuf::from(engine::filename_from_url(&urls[0])));

        // 单独限速用独立桶；否则共享全局桶
        let limiter = if config.rate_limit > 0 {
            crate::download::rate_limit::TokenBucket::new(config.rate_limit)
        } else {
            self.global_limiter.clone()
        };
        let limiter_for_engine = limiter.clone();

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

        let mgr = Arc::clone(self);
        let task_for_driver = Arc::clone(&task);
        let progress_task = Arc::clone(&task);
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
                mgr.cleanup_incomplete_files(&task_for_driver);
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
                    task_for_driver
                        .completed
                        .store(report.total, Ordering::Relaxed);
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
                mgr.cleanup_incomplete_files(&task_for_driver);
            }

            // 终态：释放调度器内的速度历史等临时内存（保留段表供 tellStatus 展示）
            let sched = {
                let guard = task_for_driver.scheduler.lock().unwrap();
                guard.as_ref().map(Arc::clone)
            };
            if let Some(sched) = sched {
                sched.trim().await;
            }
        });
        *task.driver.lock().unwrap() = Some(driver);

        Ok(gid)
    }

    async fn finish_error(self: &Arc<Self>, task: &Arc<Task>, err: anyhow::Error) {
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
                Ok(())
            }
            None => {
                // 排队中的任务：仅切换状态，driver 启动引擎时保持暂停
                if task.status() == TaskStatus::Waiting {
                    *task.status.lock().unwrap() = TaskStatus::Paused;
                    let _ = self.events.send(TaskEvent::Paused {
                        gid: gid.to_string(),
                    });
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
        let _ = self.events.send(TaskEvent::Stopped {
            gid: gid.to_string(),
        });
        self.tasks.write().unwrap().remove(gid);
        if status != TaskStatus::Active {
            // 引擎不在运行，立即清理（Active 任务由 driver 结束后清理）
            self.cleanup_incomplete_files(&task);
        }
        Ok(())
    }

    /// 移除单个下载结果（stopped 任务）。未完成的任务同时清理临时文件。
    pub async fn remove_download_result(&self, gid: &str) -> Result<()> {
        let task = self.get(gid).ok_or_else(|| anyhow!("GID {} 不存在", gid))?;
        if matches!(
            task.status(),
            TaskStatus::Active | TaskStatus::Waiting | TaskStatus::Paused
        ) {
            bail!("任务仍在队列或下载中");
        }
        self.cleanup_incomplete_files(&task);
        self.tasks.write().unwrap().remove(gid);
        Ok(())
    }

    /// 清理未完成下载的临时文件：控制文件 + .gkdl.tmp + 部分文件。
    /// 已完整下载的任务保留文件。
    fn cleanup_incomplete_files(&self, task: &Task) {
        let total = task.total.load(Ordering::Relaxed);
        let completed = task.completed.load(Ordering::Relaxed);
        if total > 0 && completed >= total {
            return;
        }
        let control = ResumeState::control_path(&task.out_path);
        std::fs::remove_file(&control).ok();
        std::fs::remove_file(control.with_extension("gkdl.tmp")).ok();
        std::fs::remove_file(&task.out_path).ok();
    }

    /// 清理已完成/出错/已移除的任务。
    pub async fn purge(&self) {
        let finished: Vec<String> = self
            .all_tasks()
            .into_iter()
            .filter(|t| {
                matches!(
                    t.status(),
                    TaskStatus::Complete | TaskStatus::Error | TaskStatus::Removed
                )
            })
            .map(|t| t.gid.clone())
            .collect();
        let mut guard = self.tasks.write().unwrap();
        for gid in finished {
            guard.remove(&gid);
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
}
