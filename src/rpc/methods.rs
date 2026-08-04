use super::{GlobalOptions, GlobalOptionsRef, ShutdownHandle, ShutdownKind};
use crate::app::config::ConfigStore;
use crate::app::task_manager::{Task, TaskManager, TaskStatus};
use crate::download::config::DownloadConfig;
use crate::download::segment::Segment;
use anyhow::Result as AResult;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub struct MethodCtx<'a> {
    pub mgr: &'a Arc<TaskManager>,
    pub global: &'a GlobalOptionsRef,
    pub session_id: &'a str,
    pub shutdown: &'a ShutdownHandle,
    pub secret: &'a str,
    pub config: Option<&'a ConfigStore>,
}

type RpcResult = AResult<Value, (i32, String)>;

fn err(code: i32, msg: impl Into<String>) -> (i32, String) {
    (code, msg.into())
}

fn s(v: impl ToString) -> Value {
    json!(v.to_string())
}

fn str_num(v: u64) -> Value {
    json!(v.to_string())
}

fn str_bool(b: bool) -> Value {
    json!(b.to_string())
}

/// 把运行时全局选项同步回 Config（用于 RPC 修改后写回 config.yaml）。
fn sync_global_to_config(cfg: &mut crate::app::config::Config, g: &GlobalOptions) {
    cfg.download.split = g.split;
    cfg.download.min_split_size = g.min_split_size;
    cfg.download.memory_threshold = g.memory_threshold;
    cfg.download.max_retries = g.retries;
    cfg.download.timeout = g.timeout;
    cfg.download.piece_length = g.piece_length;
    cfg.download.rate_limit = g.max_overall_download_limit;
    cfg.download.max_concurrent_downloads = g.max_concurrent_downloads;
    cfg.download.dir = g.dir.display().to_string();
    cfg.download.user_agent = g.user_agent.clone();
    cfg.download.referer = g.referer.clone();
    cfg.download.header = g.header.clone();
}

/// 解析 aria2 大小写法：裸数字或带 K/M/G 后缀（1024 进制，大小写不敏感）。
fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, mult) = match s.chars().last().map(|c| c.to_ascii_uppercase()) {
        Some('K') => (&s[..s.len() - 1], 1024u64),
        Some('M') => (&s[..s.len() - 1], 1024 * 1024),
        Some('G') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    Some(num.trim().parse::<u64>().ok()?.saturating_mul(mult))
}

fn parse_bool(s: &str) -> Option<bool> {
    match s {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

/// 解析 aria2 的 `header` 选项：数组或单个字符串，均为 `NAME: value`。
fn header_list(v: &Value) -> Vec<String> {
    match v {
        Value::Array(a) => a
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        Value::String(s) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// 根据每个段已写的字节区间计算分块位图。位图按 nibble（每 4 位）编码、MSB 优先，与 AriaNg 解析一致。
/// 段可能比 piece 小，先合并已写区间（含下载中的部分进度）再按 piece 判定。
fn piece_bitfield(segments: &[Segment], total: u64, piece_length: u64) -> (u64, String) {
    if total == 0 {
        return (0, String::new());
    }
    let npieces = total.div_ceil(piece_length) as usize;
    let mut bits = vec![false; npieces];

    // 合并各段已写字节的连续区间。written 可能因工作窃取截短 end 而越过段界，需钳制在段内。
    let mut ranges: Vec<(u64, u64)> = segments
        .iter()
        .map(|s| {
            let filled_end = (s.start + s.written).min(s.end);
            (s.start, filled_end)
        })
        .filter(|(s, e)| e > s)
        .collect();
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (s, e) in ranges {
        if let Some(last) = merged.last_mut() {
            if s <= last.1 {
                last.1 = last.1.max(e);
                continue;
            }
        }
        merged.push((s, e));
    }

    for (s, e) in merged {
        let first = (s / piece_length) as usize;
        let last = ((e - 1) / piece_length).min(npieces as u64 - 1) as usize;
        for (p, slot) in bits
            .iter_mut()
            .enumerate()
            .skip(first)
            .take(last - first + 1)
        {
            let ps = p as u64 * piece_length;
            let pe = (ps + piece_length).min(total);
            if ps >= s && pe <= e {
                *slot = true;
            }
        }
    }

    let nbytes = npieces.div_ceil(4);
    let mut out = String::with_capacity(nbytes);
    for nb in 0..nbytes {
        let mut v = 0u8;
        for j in 0..4 {
            let p = nb * 4 + j;
            if p < npieces && bits[p] {
                v |= 1 << (3 - j);
            }
        }
        out.push(char::from_digit(v as u32, 16).unwrap());
    }
    (npieces as u64, out)
}

/// 校验 token：若配置了 secret，params[0] 必须是 "token:<secret>"。
pub fn check_token(secret: &str, params: &[Value]) -> bool {
    if secret.is_empty() {
        return true;
    }
    if let Some(Value::String(t)) = params.first() {
        if let Some(rest) = t.strip_prefix("token:") {
            // 恒定时间比较
            let a = rest.as_bytes();
            let b = secret.as_bytes();
            if a.len() != b.len() {
                return false;
            }
            let mut diff = 0u8;
            for (x, y) in a.iter().zip(b.iter()) {
                diff |= x ^ y;
            }
            return diff == 0;
        }
    }
    false
}

fn task_status_value(task: &Task) -> Value {
    json!(task.status().as_str())
}

/// aria2 风格的 tellStatus 结果。
async fn tell_status(task: &Arc<Task>) -> Value {
    let total = task.total.load(Ordering::Relaxed);
    let completed = task.completed.load(Ordering::Relaxed);
    let speed = task.speed.load(Ordering::Relaxed);
    let connections = task.connections.load(Ordering::Relaxed);
    let error_code = task.error_code.load(Ordering::Relaxed);
    let error_message = task
        .error_message
        .lock()
        .unwrap()
        .clone()
        .unwrap_or_default();
    let dir = task
        .out_path
        .parent()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| ".".into());

    // 分块信息：从调度器段表推导
    let (piece_length, num_pieces, bitfield) = {
        let sched = task.scheduler.lock().unwrap().clone();
        match sched {
            Some(s) => {
                let pl = s.config().piece_length.max(1);
                let segs = s.segments().await;
                let (n, bf) = piece_bitfield(&segs, total, pl);
                (pl, n, bf)
            }
            None => (0, 0, String::new()),
        }
    };

    let files = json!([
        {
            "index": s(1),
            "path": task.out_path.display().to_string(),
            "length": str_num(total),
            "completedLength": str_num(completed),
            "selected": str_bool(true),
            "uris": task.urls.iter().map(|u| json!({
                "uri": u,
                "status": "used"
            })).collect::<Vec<_>>(),
        }
    ]);

    json!({
        "gid": task.gid,
        "status": task_status_value(task),
        "totalLength": str_num(total),
        "completedLength": str_num(completed),
        "uploadLength": "0",
        "downloadSpeed": str_num(speed),
        "uploadSpeed": "0",
        "connections": str_num(connections as u64),
        "numSeeders": "0",
        "seeder": str_bool(false),
        "pieceLength": str_num(piece_length),
        "numPieces": str_num(num_pieces),
        "bitfield": bitfield,
        "errorCode": str_num(error_code as u64),
        "errorMessage": error_message,
        "followedBy": [],
        "following": "",
        "belongsTo": "",
        "dir": dir,
        "files": files,
    })
}

fn parse_gid(gid: &Value) -> Result<String, (i32, String)> {
    match gid {
        Value::String(s) => Ok(s.clone()),
        _ => Err(err(1, "GID 必须是字符串")),
    }
}

fn req_gid(params: &[Value]) -> Result<String, (i32, String)> {
    match params.first() {
        Some(v) => parse_gid(v),
        None => Err(err(1, "缺少 GID 参数")),
    }
}

async fn list_tasks_status(tasks: impl Iterator<Item = Arc<Task>>) -> Value {
    let mut arr = Vec::new();
    for t in tasks {
        arr.push(tell_status(&t).await);
    }
    Value::Array(arr)
}

/// aria2.getVersion 对外声明的启用特性（统一开关）。
///
/// 已实现并声明：
/// - `Async DNS`：tokio 异步 DNS
/// - `GZip`：reqwest 已启用 gzip 解压（见 Cargo.toml 中 reqwest 的 `gzip` feature）
/// - `HTTPS`：rustls
/// - `Message Digest`：SHA-256 校验（src/app/hash.rs）
/// - `XML-RPC`：JSON-RPC over HTTP/WebSocket
///
/// 尚未实现、故不声明（若日后实现，把对应字符串加进此数组即可对外启用）：
/// - `BitTorrent`（含 magnet、tracker、DHT 等）
/// - `Metalink`
/// - `Firefox3 Cookie`
pub const ENABLED_FEATURES: &[&str] = &["Async DNS", "GZip", "HTTPS", "Message Digest", "XML-RPC"];

async fn dispatch_inner(ctx: MethodCtx<'_>, method: &str, params: &[Value]) -> RpcResult {
    match method {
        "aria2.getVersion" => Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "enabledFeatures": ENABLED_FEATURES,
        })),

        "aria2.getSessionInfo" => Ok(json!({
            "sessionId": ctx.session_id,
        })),

        "aria2.getGlobalOption" => {
            let global = ctx.global.lock().await;
            Ok(json!({
                "max-concurrent-downloads": str_num(global.max_concurrent_downloads as u64),
                "max-overall-download-limit": str_num(global.max_overall_download_limit),
                "split": str_num(global.split as u64),
                "min-split-size": str_num(global.min_split_size),
                "memory-threshold": str_num(global.memory_threshold),
                "max-tries": str_num(global.retries as u64),
                "timeout": str_num(global.timeout),
                "connect-timeout": str_num(global.timeout),
                "piece-length": str_num(global.piece_length),
                "dir": global.dir.display().to_string(),
                "save-session": str_bool(global.save_session),
                "user-agent": global.user_agent.clone(),
                "referer": global.referer.clone(),
                "header": global.header.clone(),
            }))
        }

        "aria2.changeGlobalOption" => {
            let opts = params
                .last()
                .and_then(|v| v.as_object())
                .ok_or_else(|| err(1, "缺少选项"))?;
            let mut global = ctx.global.lock().await;
            if let Some(v) = opts.get("max-overall-download-limit") {
                if let Some(rate) = v.as_str().and_then(parse_size) {
                    global.max_overall_download_limit = rate;
                    ctx.mgr.set_global_rate_limit(rate);
                }
            }
            if let Some(v) = opts.get("max-concurrent-downloads") {
                if let Some(n) = v.as_str().and_then(|s| s.parse::<usize>().ok()) {
                    global.max_concurrent_downloads = n;
                    ctx.mgr.set_max_concurrent(n);
                }
            }
            if let Some(v) = opts.get("split") {
                if let Some(n) = v.as_str().and_then(|s| s.parse::<usize>().ok()) {
                    global.split = n;
                }
            }
            if let Some(v) = opts.get("min-split-size") {
                if let Some(n) = v.as_str().and_then(parse_size) {
                    global.min_split_size = n;
                }
            }
            if let Some(v) = opts.get("memory-threshold") {
                if let Some(n) = v.as_str().and_then(parse_size) {
                    global.memory_threshold = n;
                }
            }
            if let Some(v) = opts.get("max-tries") {
                if let Some(n) = v.as_str().and_then(|s| s.parse::<u32>().ok()) {
                    global.retries = n;
                }
            }
            if let Some(v) = opts.get("timeout") {
                if let Some(n) = v.as_str().and_then(|s| s.parse::<u64>().ok()) {
                    global.timeout = n;
                }
            }
            if let Some(v) = opts.get("connect-timeout") {
                if let Some(n) = v.as_str().and_then(|s| s.parse::<u64>().ok()) {
                    global.timeout = n;
                }
            }
            if let Some(v) = opts.get("piece-length") {
                if let Some(n) = v.as_str().and_then(parse_size) {
                    if n > 0 {
                        global.piece_length = n;
                    }
                }
            }
            if let Some(v) = opts.get("dir") {
                if let Some(d) = v.as_str() {
                    global.dir = PathBuf::from(d);
                }
            }
            if let Some(v) = opts.get("save-session") {
                global.save_session = v.as_str().and_then(parse_bool).unwrap_or(false);
            }
            if let Some(v) = opts.get("user-agent") {
                if let Some(s) = v.as_str() {
                    if !s.trim().is_empty() {
                        global.user_agent = s.trim().to_string();
                    }
                }
            }
            if let Some(v) = opts.get("referer") {
                if let Some(s) = v.as_str() {
                    global.referer = s.trim().to_string();
                }
            }
            if let Some(v) = opts.get("header") {
                global.header = header_list(v);
            }
            // 其余不支持/未知选项静默忽略（与 aria2 一致）

            // 同步回配置文件：aria2ng 提交的设置修改要落实到 config.yaml
            if let Some(store) = ctx.config {
                {
                    let mut cfg = store.inner.lock().await;
                    sync_global_to_config(&mut cfg, &global);
                }
                if let Err(e) = store.save().await {
                    tracing::warn!("保存配置到文件失败: {e}");
                }
            }
            Ok(json!("OK"))
        }

        "aria2.getOption" => {
            let gid = req_gid(params)?;
            let task = ctx.mgr.get(&gid).ok_or_else(|| err(1, "GID 不存在"))?;
            let (split, min_split_size, max_tries, timeout, piece_length) = {
                let sched = task.scheduler.lock().unwrap().clone();
                match sched {
                    Some(s) => {
                        let c = s.config();
                        (
                            c.split,
                            c.min_split_size,
                            c.max_retries,
                            c.timeout,
                            c.piece_length,
                        )
                    }
                    None => (0, 0, 0, 0, 0),
                }
            };
            let rate = task.rate_limit.load(Ordering::Relaxed);
            Ok(json!({
                "dir": task.out_path.parent().map(|p| p.display().to_string()).unwrap_or_else(|| ".".into()),
                "out": task.out_path.file_name().map(|f| f.to_string_lossy().into_owned()).unwrap_or_default(),
                "split": str_num(split as u64),
                "min-split-size": str_num(min_split_size),
                "max-tries": str_num(max_tries as u64),
                "timeout": str_num(timeout),
                "piece-length": str_num(piece_length),
                "max-download-limit": str_num(rate),
            }))
        }

        "aria2.changeOption" => {
            let gid = req_gid(params)?;
            let opts = params
                .get(1)
                .and_then(|v| v.as_object())
                .ok_or_else(|| err(1, "缺少选项"))?;
            let task = ctx.mgr.get(&gid).ok_or_else(|| err(1, "GID 不存在"))?;
            // 支持在线修改每任务限速（已有独立限速桶时生效）；
            // 其余选项静默忽略（部分支持，与 aria2 只允许少量选项 active 时修改一致）
            if let Some(v) = opts.get("max-download-limit") {
                if let Some(rate) = v.as_str().and_then(parse_size) {
                    let current = task.rate_limit.load(Ordering::Relaxed);
                    if current > 0 {
                        if let Some(l) = task.limiter.lock().unwrap().clone() {
                            l.set_rate(rate);
                        }
                        task.rate_limit.store(rate, Ordering::Relaxed);
                    }
                }
            }
            Ok(json!("OK"))
        }

        "aria2.addUri" => {
            let uris: Vec<String> = params
                .first()
                .and_then(|v| v.as_array())
                .ok_or_else(|| err(1, "缺少 URIs 参数"))?
                .iter()
                .filter_map(|u| u.as_str().map(|s| s.to_string()))
                .collect();
            if uris.is_empty() {
                return Err(err(1, "URIs 列表为空"));
            }
            for u in &uris {
                if !(u.starts_with("http://") || u.starts_with("https://")) {
                    return Err(err(1, format!("无效的 URI: {u}")));
                }
            }

            let opts = params.get(1).and_then(|v| v.as_object());
            let global = ctx.global.lock().await;
            let mut config = DownloadConfig {
                split: global.split,
                min_split_size: global.min_split_size,
                memory_threshold: global.memory_threshold,
                max_retries: global.retries,
                timeout: global.timeout,
                piece_length: global.piece_length,
                user_agent: global.user_agent.clone(),
                referer: global.referer.clone(),
                header: global.header.clone(),
                ..Default::default()
            };
            let mut out_path: Option<PathBuf> = None;
            let mut dir = global.dir.clone();
            drop(global);

            if let Some(opts) = opts {
                if let Some(d) = opts.get("dir").and_then(|v| v.as_str()) {
                    dir = PathBuf::from(d);
                }
                if let Some(o) = opts.get("out").and_then(|v| v.as_str()) {
                    out_path = Some(dir.join(o));
                }
                if let Some(s) = opts.get("split").and_then(|v| v.as_str()) {
                    if let Ok(n) = s.parse::<usize>() {
                        config.split = n.max(1);
                    }
                }
                if let Some(s) = opts.get("min-split-size").and_then(|v| v.as_str()) {
                    if let Some(n) = parse_size(s) {
                        config.min_split_size = n;
                    }
                }
                if let Some(s) = opts.get("memory-threshold").and_then(|v| v.as_str()) {
                    if let Some(n) = parse_size(s) {
                        config.memory_threshold = n;
                    }
                }
                if let Some(s) = opts.get("max-download-limit").and_then(|v| v.as_str()) {
                    if let Some(n) = parse_size(s) {
                        config.rate_limit = n;
                    }
                }
                if let Some(s) = opts.get("max-tries").and_then(|v| v.as_str()) {
                    if let Ok(n) = s.parse::<u32>() {
                        config.max_retries = n;
                    }
                }
                if let Some(s) = opts.get("timeout").and_then(|v| v.as_str()) {
                    if let Ok(n) = s.parse::<u64>() {
                        config.timeout = n;
                    }
                }
                if let Some(s) = opts.get("piece-length").and_then(|v| v.as_str()) {
                    if let Some(n) = parse_size(s) {
                        if n > 0 {
                            config.piece_length = n;
                        }
                    }
                }
                if let Some(s) = opts.get("user-agent").and_then(|v| v.as_str()) {
                    if !s.trim().is_empty() {
                        config.user_agent = s.trim().to_string();
                    }
                }
                if let Some(s) = opts.get("referer").and_then(|v| v.as_str()) {
                    if !s.trim().is_empty() {
                        config.referer = s.trim().to_string();
                    }
                }
                if let Some(h) = opts.get("header") {
                    let list = header_list(h);
                    if !list.is_empty() {
                        config.header = list;
                    }
                }
            }

            let out_path = if out_path.is_some() {
                out_path
            } else {
                Some(dir.join(crate::download::engine::filename_from_url(&uris[0])))
            };

            let gid = ctx
                .mgr
                .add_download(uris, out_path, config, None)
                .await
                .map_err(|e| err(1, format!("{e:#}")))?;
            Ok(json!(gid))
        }

        "aria2.tellStatus" => {
            let gid = req_gid(params)?;
            let task = ctx.mgr.get(&gid).ok_or_else(|| err(1, "GID 不存在"))?;
            Ok(tell_status(&task).await)
        }

        "aria2.tellActive" => Ok(list_tasks_status(
            ctx.mgr
                .all_tasks()
                .into_iter()
                .filter(|t| t.status() == TaskStatus::Active),
        )
        .await),

        "aria2.tellWaiting" => {
            let offset = params.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let num = params.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let mut waiting: Vec<_> = ctx
                .mgr
                .all_tasks()
                .into_iter()
                // aria2 语义：waiting 队列包含 paused 任务
                .filter(|t| matches!(t.status(), TaskStatus::Waiting | TaskStatus::Paused))
                .collect();
            waiting.sort_by_key(|t| t.created_at);
            let slice = apply_range(waiting, offset, num);
            Ok(list_tasks_status(slice.into_iter()).await)
        }

        "aria2.tellStopped" => {
            let offset = params.first().and_then(|v| v.as_i64()).unwrap_or(0);
            let num = params.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
            let mut stopped: Vec<_> = ctx
                .mgr
                .all_tasks()
                .into_iter()
                .filter(|t| {
                    matches!(
                        t.status(),
                        TaskStatus::Complete | TaskStatus::Error | TaskStatus::Removed
                    )
                })
                .collect();
            stopped.sort_by_key(|t| std::cmp::Reverse(t.created_at));
            let slice = apply_range(stopped, offset, num);
            Ok(list_tasks_status(slice.into_iter()).await)
        }

        "aria2.getGlobalStat" => {
            let tasks = ctx.mgr.all_tasks();
            let active = tasks
                .iter()
                .filter(|t| t.status() == TaskStatus::Active)
                .count();
            let waiting = tasks
                .iter()
                // aria2 语义：waiting 队列含 paused 任务
                .filter(|t| matches!(t.status(), TaskStatus::Waiting | TaskStatus::Paused))
                .count();
            let stopped = tasks
                .iter()
                .filter(|t| {
                    matches!(
                        t.status(),
                        TaskStatus::Complete | TaskStatus::Error | TaskStatus::Removed
                    )
                })
                .count();
            let speed: u64 = tasks
                .iter()
                .filter(|t| t.status() == TaskStatus::Active)
                .map(|t| t.speed.load(Ordering::Relaxed))
                .sum();
            Ok(json!({
                "downloadSpeed": str_num(speed),
                "uploadSpeed": "0",
                "numActive": str_num(active as u64),
                "numWaiting": str_num(waiting as u64),
                "numStopped": str_num(stopped as u64),
                "numStoppedTotal": str_num(stopped as u64),
            }))
        }

        "aria2.pause" | "aria2.forcePause" => {
            let gid = req_gid(params)?;
            ctx.mgr
                .pause(&gid)
                .await
                .map_err(|e| err(1, format!("{e:#}")))?;
            Ok(json!("OK"))
        }

        "aria2.pauseAll" | "aria2.forcePauseAll" => {
            for t in ctx.mgr.all_tasks() {
                if matches!(t.status(), TaskStatus::Active | TaskStatus::Waiting) {
                    let _ = ctx.mgr.pause(&t.gid).await;
                }
            }
            Ok(json!("OK"))
        }

        "aria2.unpause" => {
            let gid = req_gid(params)?;
            ctx.mgr
                .unpause(&gid)
                .await
                .map_err(|e| err(1, format!("{e:#}")))?;
            Ok(json!("OK"))
        }

        "aria2.unpauseAll" => {
            for t in ctx.mgr.all_tasks() {
                if t.status() == TaskStatus::Paused {
                    let _ = ctx.mgr.unpause(&t.gid).await;
                }
            }
            Ok(json!("OK"))
        }

        "aria2.remove" | "aria2.forceRemove" => {
            let gid = req_gid(params)?;
            let force = method == "aria2.forceRemove";
            ctx.mgr
                .remove(&gid, force)
                .await
                .map_err(|e| err(1, format!("{e:#}")))?;
            Ok(json!("OK"))
        }

        "aria2.purgeDownloadResult" => {
            ctx.mgr.purge().await;
            Ok(json!("OK"))
        }

        "aria2.removeDownloadResult" => {
            let gid = req_gid(params)?;
            ctx.mgr
                .remove_download_result(&gid)
                .await
                .map_err(|e| err(1, format!("{e:#}")))?;
            Ok(json!("OK"))
        }

        "aria2.getPeers" => {
            let _gid = req_gid(params)?;
            // 仅支持 HTTP 下载，无 BT 对等点
            Ok(json!([]))
        }

        "aria2.changePosition" => {
            let _gid = req_gid(params)?;
            // 本实现无等待队列排序，返回 0
            Ok(json!("0"))
        }

        "aria2.shutdown" => {
            ctx.shutdown.request(ShutdownKind::Graceful);
            Ok(json!("OK"))
        }

        "aria2.forceShutdown" => {
            ctx.shutdown.request(ShutdownKind::Force);
            Ok(json!("OK"))
        }

        "aria2.saveSession" => {
            // 本实现控制文件即会话，任务进行中即持久化
            Ok(json!("OK"))
        }

        "system.listMethods" => Ok(json!(ALL_METHODS)),

        "system.listNotifications" => Ok(json!([
            "aria2.onDownloadStart",
            "aria2.onDownloadPause",
            "aria2.onDownloadStop",
            "aria2.onDownloadComplete",
            "aria2.onDownloadError",
        ])),

        "system.multicall" => unreachable!("由 dispatch 处理"),

        _ => Err(err(1, format!("No such method: {method}"))),
    }
}

/// 方法分发入口。`system.multicall` 在此展开（禁止递归）。
pub async fn dispatch(ctx: MethodCtx<'_>, method: &str, params: &[Value]) -> RpcResult {
    if method != "system.multicall" {
        return dispatch_inner(ctx, method, params).await;
    }

    let calls = params
        .first()
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| err(1, "缺少 multicall 参数"))?;
    let mut results = Vec::new();
    for call in calls {
        let name = call
            .get("methodName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let args = call
            .get("params")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if name == "system.multicall" {
            results.push(json!({
                "fault": {"code": 1, "message": "禁止递归 system.multicall"}
            }));
            continue;
        }
        // aria2 约定：token 在各子调用 params[0] 中（system.* 除外），逐条校验并剥离
        let auth_ok = check_token(ctx.secret, &args);
        let args: Vec<Value> = args
            .into_iter()
            .filter(|p| !p.as_str().map(|s| s.starts_with("token:")).unwrap_or(false))
            .collect();
        if !auth_ok {
            results.push(json!({
                "fault": {"code": 1, "message": "身份认证失败"}
            }));
            continue;
        }
        match dispatch_inner(ctx, &name, &args).await {
            Ok(v) => results.push(json!([v])),
            Err((code, msg)) => results.push(json!({"fault": {"code": code, "message": msg}})),
        }
    }
    Ok(Value::Array(results))
}

pub const ALL_METHODS: [&str; 30] = [
    "aria2.addUri",
    "aria2.getVersion",
    "aria2.getSessionInfo",
    "aria2.getGlobalOption",
    "aria2.changeGlobalOption",
    "aria2.getOption",
    "aria2.changeOption",
    "aria2.tellStatus",
    "aria2.tellActive",
    "aria2.tellWaiting",
    "aria2.tellStopped",
    "aria2.getGlobalStat",
    "aria2.getPeers",
    "aria2.pause",
    "aria2.forcePause",
    "aria2.pauseAll",
    "aria2.forcePauseAll",
    "aria2.unpause",
    "aria2.unpauseAll",
    "aria2.remove",
    "aria2.forceRemove",
    "aria2.purgeDownloadResult",
    "aria2.removeDownloadResult",
    "aria2.changePosition",
    "aria2.shutdown",
    "aria2.forceShutdown",
    "aria2.saveSession",
    "system.multicall",
    "system.listMethods",
    "system.listNotifications",
];

fn apply_range(mut items: Vec<Arc<Task>>, offset: i64, num: i64) -> Vec<Arc<Task>> {
    let start = if offset < 0 {
        (items.len() as i64 + offset).max(0) as usize
    } else {
        offset as usize
    };
    if start >= items.len() {
        return Vec::new();
    }
    let end = if num < 0 {
        items.len()
    } else {
        (start + num as usize).min(items.len())
    };
    items.drain(start..end).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::segment::SegmentState;

    #[test]
    fn token_check() {
        assert!(check_token("", &[]));
        assert!(check_token("abc", &[json!("token:abc")]));
        assert!(!check_token("abc", &[json!("token:xyz")]));
        assert!(!check_token("abc", &[json!("abc")]));
    }

    #[test]
    fn enabled_features_are_honest() {
        // 只声明已实现的能力；未实现的 BitTorrent/Metalink 等不得对外声称
        for f in ENABLED_FEATURES {
            assert!(!matches!(*f, "BitTorrent" | "Metalink" | "Firefox3 Cookie"));
        }
        assert!(ENABLED_FEATURES.contains(&"HTTPS"));
        assert!(ENABLED_FEATURES.contains(&"Message Digest"));
    }

    #[tokio::test]
    async fn tell_status_shape() {
        let task = Arc::new(Task {
            gid: "0123456789abcdef".into(),
            urls: vec!["http://example.com/f".into()],
            out_path: std::path::PathBuf::from("C:/dl/f.zip"),
            status: std::sync::Mutex::new(TaskStatus::Active),
            total: std::sync::atomic::AtomicU64::new(100),
            completed: std::sync::atomic::AtomicU64::new(50),
            speed: std::sync::atomic::AtomicU64::new(1024),
            connections: std::sync::atomic::AtomicUsize::new(4),
            error_code: std::sync::atomic::AtomicI32::new(0),
            error_message: std::sync::Mutex::new(None),
            sha256: None,
            created_at: 0,
            scheduler: std::sync::Mutex::new(None),
            limiter: std::sync::Mutex::new(None),
            cancel_token: std::sync::Mutex::new(None),
            driver: std::sync::Mutex::new(None),
            rate_limit: std::sync::atomic::AtomicU64::new(0),
            task_token: tokio_util::sync::CancellationToken::new(),
            resume_notify: tokio::sync::Notify::new(),
        });
        let v = tell_status(&task).await;
        let obj = v.as_object().unwrap();
        assert_eq!(obj["totalLength"], json!("100"));
        assert_eq!(obj["downloadSpeed"], json!("1024"));
        assert_eq!(obj["connections"], json!("4"));
        assert_eq!(obj["status"], json!("active"));
        assert_eq!(obj["pieceLength"], json!("0"));
        assert_eq!(obj["numPieces"], json!("0"));
        assert_eq!(obj["bitfield"], json!(""));
        // 与 aria2 一致：无完整性校验进行时不返回校验相关字段，
        // 否则 AriaNg 会误判任务为"等待验证"而不显示下载速度
        assert!(obj.get("verifiedLength").is_none());
        assert!(obj.get("verifyIntegrityPending").is_none());
        // 非 BT 任务不返回 bittorrent/infoHash，避免 AriaNg 误判为 BT 下载
        assert!(obj.get("bittorrent").is_none());
        assert!(obj.get("infoHash").is_none());
    }

    #[test]
    fn size_parsing() {
        assert_eq!(parse_size("0"), Some(0));
        assert_eq!(parse_size("1024"), Some(1024));
        assert_eq!(parse_size("1M"), Some(1024 * 1024));
        assert_eq!(parse_size("100k"), Some(102400));
        assert_eq!(parse_size("2G"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("abc"), None);
    }

    #[test]
    fn bitfield_encoding() {
        // 4 MiB，1 MiB 分块，前三块完成
        let segs = vec![Segment {
            seg_id: 0,
            start: 0,
            end: 3 * 1024 * 1024,
            written: 3 * 1024 * 1024,
            owner_id: usize::MAX,
            state: SegmentState::Complete,
            retries: 0,
            speed_history: std::collections::VecDeque::new(),
            speed_window: 20,
            last_sample_at: None,
            tick_bytes: 0,
        }];
        let (n, bf) = piece_bitfield(&segs, 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(n, 4);
        // 位 0/1/2 置位，位 3 清零 → 二进制 1110 → nibble 0xE
        assert_eq!(bf, "e");
    }

    #[test]
    fn bitfield_merges_smaller_segments() {
        // 1 MiB 分块，但段为 512 KiB：两块合起来才能覆盖一个 piece
        let mk_seg = |start: u64, end: u64| Segment {
            seg_id: start,
            start,
            end,
            written: end - start,
            owner_id: usize::MAX,
            state: SegmentState::Complete,
            retries: 0,
            speed_history: std::collections::VecDeque::new(),
            speed_window: 20,
            last_sample_at: None,
            tick_bytes: 0,
        };
        // 仅第一段完成：piece0 只覆盖一半 → 位 0 不应置位
        let segs = vec![mk_seg(0, 512 * 1024)];
        let (n, bf) = piece_bitfield(&segs, 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(n, 4);
        assert_eq!(bf, "0");

        // 前两段完成：合并为 [0, 1MiB) → piece0 置位
        let segs = vec![mk_seg(0, 512 * 1024), mk_seg(512 * 1024, 1024 * 1024)];
        let (_, bf) = piece_bitfield(&segs, 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(bf, "8");
    }

    #[test]
    fn bitfield_reflects_partial_progress() {
        // 4 MiB，1 MiB 分块，单段下载中已写 2.5 MiB：piece0/1 完成、piece2 部分 → 位 1100
        let seg = Segment {
            seg_id: 0,
            start: 0,
            end: 4 * 1024 * 1024,
            written: 5 * 512 * 1024,
            owner_id: 0,
            state: SegmentState::Downloading,
            retries: 0,
            speed_history: std::collections::VecDeque::new(),
            speed_window: 20,
            last_sample_at: None,
            tick_bytes: 0,
        };
        let (n, bf) = piece_bitfield(&[seg], 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(n, 4);
        assert_eq!(bf, "c");

        // 多段部分下载同样反映：前两 MiB 写完 + 第三段写一半 → 相邻区间合并为 [0,3MiB) → 位 1110
        let mk = |start: u64, end: u64, written: u64, state: SegmentState| Segment {
            seg_id: start,
            start,
            end,
            written,
            owner_id: usize::MAX,
            state,
            retries: 0,
            speed_history: std::collections::VecDeque::new(),
            speed_window: 20,
            last_sample_at: None,
            tick_bytes: 0,
        };
        let segs = vec![
            mk(
                0,
                2 * 1024 * 1024,
                2 * 1024 * 1024,
                SegmentState::Downloading,
            ),
            mk(
                2 * 1024 * 1024,
                4 * 1024 * 1024,
                1024 * 1024,
                SegmentState::Downloading,
            ),
        ];
        let (_, bf) = piece_bitfield(&segs, 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(bf, "e");
    }

    #[test]
    fn bitfield_clamps_written_past_segment_end() {
        // 工作窃取截短了段尾，但 worker 本地 written 仍按旧 end 累计，越过段界 → 钳制到段内
        let seg = Segment {
            seg_id: 0,
            start: 0,
            end: 1024 * 1024,
            written: 2 * 1024 * 1024,
            owner_id: 0,
            state: SegmentState::Downloading,
            retries: 0,
            speed_history: std::collections::VecDeque::new(),
            speed_window: 20,
            last_sample_at: None,
            tick_bytes: 0,
        };
        let (_, bf) = piece_bitfield(&[seg], 4 * 1024 * 1024, 1024 * 1024);
        assert_eq!(bf, "8");
    }
}
