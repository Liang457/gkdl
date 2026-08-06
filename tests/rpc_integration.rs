mod common;

use common::TestServer;
use gkdl::app::config::{Config, ConfigStore};
use gkdl::app::db::StateDb;
use gkdl::app::hooks::HookConfig;
use gkdl::app::task_manager::TaskManager;
use gkdl::download::config::DownloadConfig;
use gkdl::rpc::server::{self, AppState};
use gkdl::rpc::{GlobalOptions, ShutdownHandle, ShutdownKind};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

fn out_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("gkdl_it");
    std::fs::create_dir_all(&dir).ok();
    dir.join(format!("{}_{}.bin", name, std::process::id()))
}

#[tokio::test]
async fn post_download_hook_runs_after_success() {
    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("hook_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let marker = std::env::temp_dir()
        .join("gkdl_it")
        .join(format!("hook_marker_{}.txt", std::process::id()));
    let _ = std::fs::remove_file(&marker);

    // 写一个 .bat 脚本：把环境变量写入 marker 文件
    let script = std::env::temp_dir()
        .join("gkdl_it")
        .join(format!("hook_script_{}.bat", std::process::id()));
    let marker_str = marker.display().to_string();
    std::fs::write(
        &script,
        format!(
            "@echo off\r\necho %GKDL_URL% > \"{}\"\r\necho %GKDL_GID% >> \"{}\"\r\necho %GKDL_SIZE% >> \"{}\"\r\n",
            marker_str, marker_str, marker_str
        ),
    )
    .unwrap();

    let hook = HookConfig {
        script: Some(script.clone()),
        commands_file: None,
        timeout_sec: 30,
    };
    let mgr = TaskManager::new(hook, DownloadConfig::default(), None);
    let gid = mgr
        .add_download(
            vec![server.url()],
            Some(out.clone()),
            DownloadConfig::default(),
            None,
        )
        .await
        .expect("添加下载失败");
    mgr.wait_finished(&gid).await.expect("任务失败");

    // 等待 hook 完成
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let content = std::fs::read_to_string(&marker).expect("hook 未运行，marker 不存在");
    assert!(content.contains(&server.url()), "GKDL_URL 未传递");
    assert!(content.contains(&gid), "GKDL_GID 未传递");
    assert!(content.contains("524288"), "GKDL_SIZE 未传递");

    let _ = std::fs::remove_file(&marker);
    let _ = std::fs::remove_file(&script);
    let _ = std::fs::remove_file(&out);
}

async fn start_server(
    secret: &str,
) -> (
    String,
    Arc<TaskManager>,
    ShutdownHandle,
    tokio::sync::mpsc::UnboundedReceiver<ShutdownKind>,
) {
    // 内部使用内存数据库，便于驱动清理路径按新逻辑工作
    let db = Arc::new(StateDb::open(std::path::Path::new(":memory:")).unwrap());
    let (base, mgr, _db, shutdown, rx) = start_server_with_db(secret, Some(db)).await;
    (base, mgr, shutdown, rx)
}

/// 带外部状态库的服务器（测试需要断言 DB 内容时使用）。
async fn start_server_with_db(
    secret: &str,
    db: Option<Arc<StateDb>>,
) -> (
    String,
    Arc<TaskManager>,
    Option<Arc<StateDb>>,
    ShutdownHandle,
    tokio::sync::mpsc::UnboundedReceiver<ShutdownKind>,
) {
    let mgr = TaskManager::new(HookConfig::default(), DownloadConfig::default(), db.clone());
    let global = Arc::new(Mutex::new(GlobalOptions::default()));
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = ShutdownHandle::new(tx);
    let state = Arc::new(AppState {
        mgr: Arc::clone(&mgr),
        global: Arc::clone(&global),
        session_id: "test-session".into(),
        shutdown: shutdown.clone(),
        secret: secret.into(),
        config: None,
    });
    let addr = server::serve(state, "127.0.0.1", 0)
        .await
        .expect("serve 失败");
    (format!("http://{addr}"), mgr, db, shutdown, rx)
}

/// libcurl POST 原始 JSON 请求，返回 (状态码, JSON)。
async fn call_raw(base: &str, body: &str) -> (u32, serde_json::Value) {
    let url = format!("{base}/jsonrpc");
    let body_str = body.to_string();
    let (status, text) = tokio::task::spawn_blocking(move || {
        let mut easy = curl::easy::Easy::new();
        easy.url(&url).unwrap();
        easy.post(true).unwrap();
        easy.post_fields_copy(body_str.as_bytes()).unwrap();
        let mut list = curl::easy::List::new();
        list.append("Content-Type: application/json").unwrap();
        easy.http_headers(list).unwrap();
        let mut buf = Vec::new();
        {
            let mut transfer = easy.transfer();
            transfer
                .write_function(|d| {
                    buf.extend_from_slice(d);
                    Ok(d.len())
                })
                .unwrap();
            let _ = transfer.perform();
        }
        let status = easy.response_code().unwrap_or(0);
        let text = String::from_utf8_lossy(&buf).into_owned();
        (status, text)
    })
    .await
    .unwrap();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// libcurl GET 请求 `/jsonrpc`（值会做 URL 编码），返回 (状态码, JSON)。
async fn call_get(base: &str, query: &[(&str, &str)]) -> (u32, serde_json::Value) {
    let mut url = format!("{base}/jsonrpc");
    for (i, (k, v)) in query.iter().enumerate() {
        url.push(if i == 0 { '?' } else { '&' });
        url.push_str(k);
        url.push('=');
        url.push_str(&urlencode(v));
    }
    let (status, text) = tokio::task::spawn_blocking(move || {
        let mut easy = curl::easy::Easy::new();
        easy.url(&url).unwrap();
        let mut buf = Vec::new();
        {
            let mut transfer = easy.transfer();
            transfer
                .write_function(|d| {
                    buf.extend_from_slice(d);
                    Ok(d.len())
                })
                .unwrap();
            let _ = transfer.perform();
        }
        let status = easy.response_code().unwrap_or(0);
        let text = String::from_utf8_lossy(&buf).into_owned();
        (status, text)
    })
    .await
    .unwrap();
    let json = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn call(base: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
    let body = serde_json::json!({"jsonrpc":"2.0","id":"t","method":method,"params":params});
    call_raw(base, &body.to_string()).await.1
}

#[tokio::test]
async fn change_global_option_persists_to_config_file() {
    // aria2ng 通过 changeGlobalOption 修改设置后，应同步写回 config.yaml
    let cfg_path = std::env::temp_dir()
        .join("gkdl_it")
        .join(format!("cfg_persist_{}.yaml", std::process::id()));
    let _ = std::fs::remove_file(&cfg_path);
    let store = ConfigStore::new(cfg_path.clone(), Config::default());

    let mgr = TaskManager::new(HookConfig::default(), DownloadConfig::default(), None);
    let global = Arc::new(Mutex::new(GlobalOptions::default()));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let shutdown = ShutdownHandle::new(tx);
    let state = Arc::new(AppState {
        mgr: Arc::clone(&mgr),
        global: Arc::clone(&global),
        session_id: "test-session".into(),
        shutdown: shutdown.clone(),
        secret: String::new(),
        config: Some(Arc::new(store.clone())),
    });
    let addr = server::serve(state, "127.0.0.1", 0)
        .await
        .expect("serve 失败");
    let base = format!("http://{addr}");

    let v = call(
        &base,
        "aria2.changeGlobalOption",
        serde_json::json!([{
            "split": "4",
            "max-overall-download-limit": "1M",
            "timeout": "15",
            "dir": "C:/dl",
        }]),
    )
    .await;
    assert_eq!(v["result"], "OK");

    let loaded = Config::load(&cfg_path).expect("配置文件应已落盘");
    assert_eq!(loaded.download.split, 4);
    assert_eq!(loaded.download.rate_limit, 1024 * 1024);
    assert_eq!(loaded.download.timeout, 15);
    assert_eq!(loaded.download.dir, "C:/dl");
    std::fs::remove_file(&cfg_path).ok();
}

#[tokio::test]
async fn aria_ng_without_secret_omits_params() {
    // AriaNg 未配置 secret 时，对无参方法省略 params 字段（GET/POST 皆如此）。
    // GKDL 需按 aria2 行为把缺失 params 视为空数组。
    let (base, _mgr, _shutdown, _rx) = start_server("").await;

    let (status, v) = call_raw(
        &base,
        r#"{"jsonrpc":"2.0","id":"1","method":"aria2.getVersion"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(v["result"]["version"].is_string());
    assert!(v["error"].is_null());

    let (status, v) = call_raw(
        &base,
        r#"{"jsonrpc":"2.0","id":"2","method":"aria2.getGlobalOption"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(v["result"]["dir"].is_string());
    assert!(v["error"].is_null());

    let (status, v) = call_raw(
        &base,
        r#"{"jsonrpc":"2.0","id":"3","method":"aria2.getGlobalStat"}"#,
    )
    .await;
    assert_eq!(status, 200);
    assert!(v["result"]["numActive"].is_string());
}

#[tokio::test]
async fn multicall_authenticates_inner_calls() {
    let (base, _mgr, _shutdown, _rx) = start_server("s3cret").await;

    // AriaNg 的 system.multicall 顶层不带 token，token 在各子调用 params[0]
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "m",
        "method": "system.multicall",
        "params": [[
            {
                "methodName": "aria2.getGlobalStat",
                "params": ["token:s3cret"]
            },
            {
                "methodName": "aria2.getVersion",
                "params": ["token:s3cret"]
            }
        ]]
    });
    let (status, v) = call_raw(&base, &body.to_string()).await;
    assert_eq!(status, 200);
    let arr = v["result"].as_array().expect("multicall 应返回数组");
    assert_eq!(arr.len(), 2);
    assert!(arr[0][0]["numActive"].is_string());
    assert!(arr[1][0]["version"].is_string());

    // 错误 token 的子调用应产生 fault 而不影响整体返回
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "m",
        "method": "system.multicall",
        "params": [[
            {
                "methodName": "aria2.tellActive",
                "params": ["token:wrong"]
            }
        ]]
    });
    let (status, v) = call_raw(&base, &body.to_string()).await;
    assert_eq!(status, 200);
    assert_eq!(v["result"][0]["fault"]["code"], 1);
}

#[tokio::test]
async fn rpc_server_serves_aria2_methods() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("rpc_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, mut rx) = start_server("s3cret").await;

    // getVersion 成功（无 token 也允许）
    let v = call(&base, "aria2.getVersion", serde_json::json!([])).await;
    assert!(v["result"]["version"].is_string());
    assert!(v["error"].is_null());
    // 只声明已实现的能力，不得声称 BitTorrent/Metalink 等未实现特性
    let features: Vec<_> = v["result"]["enabledFeatures"]
        .as_array()
        .expect("enabledFeatures 应为数组")
        .iter()
        .filter_map(|f| f.as_str())
        .collect();
    assert!(features.contains(&"HTTPS"));
    assert!(features.contains(&"Message Digest"));
    assert!(!features.contains(&"BitTorrent"));
    assert!(!features.contains(&"Metalink"));

    // 鉴权失败：错误 token → 400 且延迟
    let t0 = std::time::Instant::now();
    let body = serde_json::json!({"jsonrpc":"2.0","id":"a","method":"aria2.tellActive","params":["token:wrong"]});
    let (status, _) = call_raw(&base, &body.to_string()).await;
    assert_eq!(status, 400);
    assert!(t0.elapsed() >= std::time::Duration::from_millis(900));

    // addUri + tellStatus
    let v = call(
        &base,
        "aria2.addUri",
        serde_json::json!(["token:s3cret", [server.url()]]),
    )
    .await;
    let gid = v["result"].as_str().expect("addUri 应返回 gid").to_string();
    assert_eq!(gid.len(), 16);

    let v = call(
        &base,
        "aria2.tellStatus",
        serde_json::json!(["token:s3cret", gid]),
    )
    .await;
    assert_eq!(v["result"]["gid"], gid);
    // 非 BT 任务不返回 bittorrent/infoHash，避免 AriaNg 误判为 BT 下载
    assert!(v["result"].get("bittorrent").is_none());
    assert!(v["result"].get("infoHash").is_none());

    // getGlobalStat
    let v = call(
        &base,
        "aria2.getGlobalStat",
        serde_json::json!(["token:s3cret"]),
    )
    .await;
    assert!(v["result"]["numStopped"].is_string());

    // 等待下载完成
    mgr.wait_finished(&gid).await.expect("RPC 添加的任务失败");
    let v = call(
        &base,
        "aria2.tellStatus",
        serde_json::json!(["token:s3cret", gid]),
    )
    .await;
    assert_eq!(v["result"]["status"], "complete");

    // 第二个任务：验证 tellStopped(-1)（AriaNg 默认调用）返回全部 stopped、最新在前。
    // 等待 >1s 确保 created_at 不同秒，避免排序歧义。
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let out2 = out_path("rpc_it_2");
    let _ = std::fs::remove_file(&out2);
    let v = call(
        &base,
        "aria2.addUri",
        serde_json::json!([
            "token:s3cret",
            [server.url()],
            {
                "dir": out2.parent().unwrap().display().to_string(),
                "out": out2.file_name().unwrap().to_string_lossy()
            }
        ]),
    )
    .await;
    let gid2 = v["result"].as_str().expect("addUri 应返回 gid").to_string();
    mgr.wait_finished(&gid2).await.expect("第二个任务失败");

    let v = call(
        &base,
        "aria2.tellStopped",
        serde_json::json!(["token:s3cret", -1, 1000]),
    )
    .await;
    let stopped = v["result"].as_array().expect("tellStopped 应返回数组");
    assert_eq!(stopped.len(), 2, "tellStopped(-1) 应返回全部 stopped 任务");
    assert_eq!(stopped[0]["gid"], gid2, "最新完成的任务应排在前面");
    assert_eq!(stopped[0]["status"], "complete");
    assert_eq!(stopped[1]["gid"], gid, "最早的任务应在后面");
    let _ = std::fs::remove_file(&out2);

    // 未知方法
    let v = call(&base, "aria2.nope", serde_json::json!(["token:s3cret"])).await;
    assert_eq!(v["error"]["code"], 1);
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("No such method"));

    // shutdown 通知主循环
    let v = call(&base, "aria2.shutdown", serde_json::json!(["token:s3cret"])).await;
    assert_eq!(v["result"], "OK");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(rx.try_recv().ok(), Some(ShutdownKind::Graceful));

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

/// 轮询 tellStatus 直到状态等于 want，返回最后一次响应。
async fn wait_status(base: &str, gid: &str, want: &str, timeout_ms: u64) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    loop {
        let v = call(base, "aria2.tellStatus", serde_json::json!([gid])).await;
        let status = v["result"]["status"].as_str().unwrap_or("").to_string();
        if status == want {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "等待状态 {want} 超时，当前 {status}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// addUri 带选项，返回 gid。
async fn add_uri_with_opts(
    base: &str,
    url: &str,
    out: &std::path::Path,
    extra: serde_json::Value,
) -> String {
    let dir = out.parent().unwrap().display().to_string();
    let name = out.file_name().unwrap().to_string_lossy().into_owned();
    let mut opts = serde_json::json!({ "dir": dir, "out": name });
    if let Some(obj) = opts.as_object_mut() {
        if let Some(extra_obj) = extra.as_object() {
            for (k, v) in extra_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    let v = call(base, "aria2.addUri", serde_json::json!([[url], opts])).await;
    v["result"].as_str().expect("addUri 应返回 gid").to_string()
}

#[tokio::test]
async fn force_pause_freezes_progress_and_unpause_resumes() {
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("pause_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;

    // 限速下载，便于暂停
    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({ "max-download-limit": "131072" }),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // AriaNg 实际调用的是 forcePause
    let v = call(&base, "aria2.forcePause", serde_json::json!([gid])).await;
    assert_eq!(v["result"], "OK");

    let paused = wait_status(&base, &gid, "paused", 5000).await;
    assert_eq!(paused["result"]["downloadSpeed"], "0");

    // 暂停后进度冻结
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let a = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    let done_a: u64 = a["result"]["completedLength"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    let b = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    let done_b: u64 = b["result"]["completedLength"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(done_a, done_b, "暂停后进度应冻结");

    // 暂停任务出现在 tellWaiting（aria2 语义）
    let w = call(&base, "aria2.tellWaiting", serde_json::json!([0, 100])).await;
    let gids: Vec<&str> = w["result"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["gid"].as_str())
        .collect();
    assert!(gids.contains(&gid.as_str()), "暂停任务应出现在 tellWaiting");

    // 解除限速后恢复下载
    let v = call(
        &base,
        "aria2.changeOption",
        serde_json::json!([gid, { "max-download-limit": "0" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");
    let v = call(&base, "aria2.unpause", serde_json::json!([gid])).await;
    assert_eq!(v["result"], "OK");

    mgr.wait_finished(&gid).await.expect("恢复后任务应完成");
    let v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    assert_eq!(v["result"]["status"], "complete");

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn force_remove_deletes_control_and_partial_files() {
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("remove_it");
    let _ = std::fs::remove_file(&out);

    let db = Arc::new(StateDb::open(std::path::Path::new(":memory:")).unwrap());
    let (base, _mgr, _db, _shutdown, _rx) = start_server_with_db("", Some(db.clone())).await;

    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({
            "max-download-limit": "131072",
            "memory-threshold": "0", // 磁盘模式：下载中即有部分文件
        }),
    )
    .await;
    // 等状态库出现记录（monitor 约 1s 保存一次）
    tokio::time::sleep(std::time::Duration::from_millis(1400)).await;
    assert!(
        db.load_download(&gid).unwrap().is_some(),
        "下载中状态库应存在记录"
    );
    assert!(out.exists(), "磁盘模式下载中应存在部分文件");

    let v = call(&base, "aria2.forceRemove", serde_json::json!([gid])).await;
    assert_eq!(v["result"], "OK");

    // driver 结束后删除状态库记录与部分文件
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
    while db.load_download(&gid).unwrap().is_some() || out.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "删除超时：记录={} 文件={}",
            db.load_download(&gid).unwrap().is_some(),
            out.exists()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn cancel_during_probe_cleans_up_and_does_not_reactivate() {
    // 慢探测服务器（/slow 延迟 2s）：在探测阶段 forceRemove，任务应立即取消且不产生临时文件
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start_slow(data, false, 2000).await;
    let out = out_path("probe_cancel_it");
    let control = format!("{}.gkdl", out.display());
    let tmp = format!("{}.gkdl.tmp", out.display());
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(&control);
    let _ = std::fs::remove_file(&tmp);

    let (base, mgr, _shutdown, _rx) = start_server("").await;

    let gid = add_uri_with_opts(&base, &server.url_slow(), &out, serde_json::json!({})).await;

    // 等 driver 进入探测阶段（探测请求已发出，仍被 /slow 阻塞）
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 探测期间 forceRemove
    let v = call(&base, "aria2.forceRemove", serde_json::json!([gid])).await;
    assert_eq!(v["result"], "OK", "forceRemove 应成功");
    assert!(mgr.get(&gid).is_none(), "任务应已从注册表移除");

    let v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    assert!(
        v["error"].as_object().is_some(),
        "已移除任务 tellStatus 应返回错误：{v}"
    );

    // 等慢探测本应完成的时刻之后：任务不得被重新激活，也不得产生任何文件
    // （修复前 driver 会在探测完成后重新置 Active 并下载整文件）
    tokio::time::sleep(std::time::Duration::from_millis(2800)).await;
    assert!(mgr.get(&gid).is_none(), "被移除的任务不应重新激活");
    assert!(!out.exists(), "不应创建部分文件");
    assert!(!std::path::Path::new(&control).exists(), "不应存在控制文件");
    assert!(
        !std::path::Path::new(&tmp).exists(),
        "不应存在控制文件临时文件"
    );

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(&control);
    let _ = std::fs::remove_file(&tmp);
}

#[tokio::test]
async fn force_remove_with_slow_rate_limit_stops_engine_promptly() {
    // 极低限速（102 B/s）下取消任务：引擎应立即退出并清理状态库记录。
    // 修复前 worker 卡死在 consume() 的 sleep 中（需 ~642s 才攒够一个分块），清理迟迟不发生。
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("slow_cancel_it");
    let _ = std::fs::remove_file(&out);

    let db = Arc::new(StateDb::open(std::path::Path::new(":memory:")).unwrap());
    let (base, mgr, _db, _shutdown, _rx) = start_server_with_db("", Some(db.clone())).await;

    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({ "max-download-limit": "102" }),
    )
    .await;

    // 等状态库出现记录（monitor 约 1s 保存一次）
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert!(
        db.load_download(&gid).unwrap().is_some(),
        "下载中状态库应存在记录"
    );

    let v = call(&base, "aria2.forceRemove", serde_json::json!([gid])).await;
    assert_eq!(v["result"], "OK");
    assert!(mgr.get(&gid).is_none(), "任务应已从注册表移除");

    // 取消后引擎应立即退出并清理状态库记录
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
    while db.load_download(&gid).unwrap().is_some() {
        assert!(std::time::Instant::now() < deadline, "状态库记录删除超时");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let _ = std::fs::remove_file(&out);
}

#[tokio::test]
async fn removed_task_late_cleanup_does_not_delete_newer_download() {
    // 回归场景：A 源（探测成功但下载请求挂起）下载中途 forceRemove，清理被推迟到
    // driver（引擎仍卡在挂起的连接上）；随后用 B 源下载同一路径并成功完成。
    // 修复前 A 的迟到清理会按自身 total/completed 判定未完成而误删 B 已完成的文件。
    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data.clone(), false).await;
    let hang_url = format!("http://{}/hang_download", server.addr);
    let out = out_path("late_cleanup_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let db = Arc::new(StateDb::open(std::path::Path::new(":memory:")).unwrap());
    let (base, mgr, _db, _shutdown, _rx) = start_server_with_db("", Some(db.clone())).await;

    // 任务1：A 源。内存模式（不持有文件句柄，模拟小文件默认场景）+ 短读超时
    // （min 5s），保证其 worker 在挂起连接上最多 5s 后超时退出，从而让「迟到清理」
    // 稳定发生在 B 下载完成之后。
    let gid_a = add_uri_with_opts(
        &base,
        &hang_url,
        &out,
        serde_json::json!({ "timeout": "5" }),
    )
    .await;

    // 等任务1进入 active 且状态库出现记录（探测完成、下载请求已发出）
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(8000);
    loop {
        let v = call(&base, "aria2.tellStatus", serde_json::json!([gid_a])).await;
        if v["result"]["status"] == "active" && db.load_download(&gid_a).unwrap().is_some() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "任务1未进入 active");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // 任务1 强制移除：文件清理被推迟到 driver（引擎仍卡在挂起的下载请求上）
    let v = call(&base, "aria2.forceRemove", serde_json::json!([gid_a])).await;
    assert_eq!(v["result"], "OK");

    // 任务2：B 源正常下载同一路径并完成（小文件默认内存模式）
    let gid_b = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({ "split": "2" }),
    )
    .await;
    mgr.wait_finished(&gid_b).await.expect("B 源下载失败");

    // 等任务1 的 driver 完成迟到清理（其状态库记录被删除）
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(15000);
    while db.load_download(&gid_a).unwrap().is_some() {
        assert!(
            std::time::Instant::now() < deadline,
            "任务1的迟到清理未完成"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // 关键断言：任务2 下载完成的文件必须仍然存在且内容完整
    assert!(out.exists(), "旧任务迟到清理误删了新任务的下载文件");
    let bytes = std::fs::read(&out).unwrap();
    assert_eq!(bytes, data, "下载文件内容被破坏");

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn tell_status_reports_piece_info() {
    let data: Vec<u8> = (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("piece_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(&base, &server.url(), &out, serde_json::json!({})).await;

    // 等探测完成，且调度器已就绪（pieceLength 非 0）
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(5000);
    let mut v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    while v["result"]["totalLength"].as_str().unwrap_or("0") == "0"
        || v["result"]["pieceLength"].as_str().unwrap_or("0") == "0"
    {
        assert!(std::time::Instant::now() < deadline, "探测超时");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    }
    assert_eq!(v["result"]["pieceLength"], "1048576");
    assert_eq!(v["result"]["numPieces"], "4");

    mgr.wait_finished(&gid).await.expect("下载失败");
    v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    assert_eq!(v["result"]["status"], "complete");
    assert_eq!(v["result"]["bitfield"], "f", "4 块全部完成后位图应为 f");

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn tell_status_bitfield_grows_mid_download() {
    // 8 MiB 文件、单线程限速下载：下载过程中位图应逐步体现已完成区块（复现活跃任务位图全 0 的问题）
    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("bitfield_grows_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({
            "split": "1",
            "max-download-limit": "512K",
            "piece-length": "512K",
        }),
    )
    .await;

    // 轮询：活跃状态下位图应出现非 0 值（部分区块已完成的体现）
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(15000);
    loop {
        let v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
        let status = v["result"]["status"].as_str().unwrap_or("");
        let bitfield = v["result"]["bitfield"].as_str().unwrap_or("");
        if status == "active" && !bitfield.is_empty() && bitfield != "0" && bitfield != "0000" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "活跃期间位图未体现部分进度"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    mgr.wait_finished(&gid).await.expect("下载失败");
    let v = call(&base, "aria2.tellStatus", serde_json::json!([gid])).await;
    assert_eq!(v["result"]["status"], "complete");
    assert_eq!(
        v["result"]["bitfield"], "ffff",
        "16 块全部完成后位图应为 ffff"
    );

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn global_option_supports_common_keys_and_applies() {
    let (base, _mgr, _shutdown, _rx) = start_server("").await;

    let v = call(&base, "aria2.getGlobalOption", serde_json::json!([])).await;
    let opts = v["result"].as_object().expect("应为扁平对象");
    for key in [
        "dir",
        "max-concurrent-downloads",
        "max-overall-download-limit",
        "split",
        "min-split-size",
        "max-tries",
        "timeout",
        "connect-timeout",
        "piece-length",
        "save-session",
    ] {
        assert!(opts.contains_key(key), "缺少选项 {key}");
    }

    // 未知选项忽略、已知选项生效
    let v = call(
        &base,
        "aria2.changeGlobalOption",
        serde_json::json!([{ "split": "2", "no-such-option": "1M" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");

    let v = call(&base, "aria2.getGlobalOption", serde_json::json!([])).await;
    assert_eq!(v["result"]["split"], "2");

    // 大小带单位解析
    let v = call(
        &base,
        "aria2.changeGlobalOption",
        serde_json::json!([{ "max-overall-download-limit": "1M" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");
    let v = call(&base, "aria2.getGlobalOption", serde_json::json!([])).await;
    assert_eq!(v["result"]["max-overall-download-limit"], "1048576");
}

#[tokio::test]
async fn change_option_updates_task_rate_limit() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("chgopt_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({ "max-download-limit": "524288" }),
    )
    .await;
    assert_eq!(
        mgr.get(&gid)
            .unwrap()
            .rate_limit
            .load(std::sync::atomic::Ordering::Relaxed),
        524288
    );

    let v = call(
        &base,
        "aria2.changeOption",
        serde_json::json!([gid, { "max-download-limit": "1048576" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");
    assert_eq!(
        mgr.get(&gid)
            .unwrap()
            .rate_limit
            .load(std::sync::atomic::Ordering::Relaxed),
        1048576
    );

    // 未知选项不报错
    let v = call(
        &base,
        "aria2.changeOption",
        serde_json::json!([gid, { "no-such-opt": "x" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");

    mgr.wait_finished(&gid).await.expect("下载失败");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn remove_download_result_removes_only_target() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out1 = out_path("rdr1");
    let out2 = out_path("rdr2");
    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid1 = add_uri_with_opts(&base, &server.url(), &out1, serde_json::json!({})).await;
    let gid2 = add_uri_with_opts(&base, &server.url(), &out2, serde_json::json!({})).await;
    mgr.wait_finished(&gid1).await.expect("任务1失败");
    mgr.wait_finished(&gid2).await.expect("任务2失败");

    let v = call(
        &base,
        "aria2.removeDownloadResult",
        serde_json::json!([gid1]),
    )
    .await;
    assert_eq!(v["result"], "OK");
    assert!(mgr.get(&gid1).is_none(), "gid1 应被移除");
    assert!(mgr.get(&gid2).is_some(), "gid2 应保留");

    let v = call(
        &base,
        "aria2.removeDownloadResult",
        serde_json::json!([gid2]),
    )
    .await;
    assert_eq!(v["result"], "OK");
    assert!(mgr.get(&gid2).is_none());

    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));
}

#[tokio::test]
async fn max_concurrent_downloads_limits_concurrency() {
    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out1 = out_path("conc1");
    let out2 = out_path("conc2");
    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;

    let v = call(
        &base,
        "aria2.changeGlobalOption",
        serde_json::json!([{ "max-concurrent-downloads": "1" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");

    // 任务1 限速慢下载占住唯一槽位
    let gid1 = add_uri_with_opts(
        &base,
        &server.url(),
        &out1,
        serde_json::json!({ "max-download-limit": "131072" }),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        mgr.get(&gid1).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Active
    );

    // 任务2 应进入 waiting 队列
    let gid2 = add_uri_with_opts(&base, &server.url(), &out2, serde_json::json!({})).await;
    assert_eq!(
        mgr.get(&gid2).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Waiting,
        "槽位不足时任务2应在等待队列"
    );

    // 任务1 完成后任务2 自动启动并完成
    mgr.wait_finished(&gid1).await.expect("任务1失败");
    mgr.wait_finished(&gid2).await.expect("任务2失败");
    assert_eq!(
        mgr.get(&gid2).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Complete
    );

    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));
}

/// 回归测试：排队中被暂停的任务被删除后，driver 应立即退出而非永久挂起。
/// 之前 remove() 不通知 resume_notify，driver 卡在 notified().await 泄漏 Arc<Task>。
///
/// 时序：gid1 慢下载占住槽位 → gid2 进入 waiting → pause(gid2) 使其 Paused。
/// gid1 完成后 gid2 抢到槽位但发现已暂停，于是 driver 阻塞在 resume_notify 等待；
/// 此时 remove(gid2) 必须能唤醒 driver 使其退出（否则 is_finished() 永远为 false）。
#[tokio::test]
async fn removing_paused_queued_task_does_not_hang_driver() {
    let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out1 = out_path("hang_conc1");
    let out2 = out_path("hang_conc2");
    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let v = call(
        &base,
        "aria2.changeGlobalOption",
        serde_json::json!([{ "max-concurrent-downloads": "1" }]),
    )
    .await;
    assert_eq!(v["result"], "OK");

    // 任务1 慢下载占住唯一槽位
    let gid1 = add_uri_with_opts(
        &base,
        &server.url(),
        &out1,
        serde_json::json!({ "max-download-limit": "131072" }),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert_eq!(
        mgr.get(&gid1).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Active
    );

    // 任务2 进入 waiting 队列，然后暂停（排队中暂停）
    let gid2 = add_uri_with_opts(&base, &server.url(), &out2, serde_json::json!({})).await;
    assert_eq!(
        mgr.get(&gid2).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Waiting
    );
    let task2 = mgr.get(&gid2).unwrap();
    let v = call(&base, "aria2.forcePause", serde_json::json!([gid2])).await;
    assert_eq!(v["result"], "OK");
    assert_eq!(
        mgr.get(&gid2).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Paused,
        "任务2 应为排队中暂停"
    );

    // 任务1 完成 → 槽位释放 → 任务2 driver 抢到槽位后因已暂停，阻塞在 resume_notify
    mgr.wait_finished(&gid1).await.expect("任务1失败");
    // 稍等，让任务2 的 driver 进入 resume_notify 等待
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        mgr.get(&gid2).unwrap().status(),
        gkdl::app::task_manager::TaskStatus::Paused,
        "任务2 应仍处于暂停（等 unpause）"
    );

    // 删除暂停的排队任务
    let v = call(&base, "aria2.forceRemove", serde_json::json!([gid2])).await;
    assert_eq!(v["result"], "OK");
    assert!(mgr.get(&gid2).is_none(), "任务2 应从任务表移除");

    // 关键断言：driver JoinHandle 必须退出。若 remove() 未唤醒 resume_notify，
    // driver 永久卡在 notified().await，is_finished() 永远为 false（此处超时即失败）。
    let driver = task2.driver.lock().unwrap().take().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !driver.is_finished() {
        assert!(
            std::time::Instant::now() < deadline,
            "删除暂停的排队任务后 driver 未退出（resume_notify 未被唤醒）"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    drop(driver);

    let _ = std::fs::remove_file(&out1);
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));
}

#[tokio::test]
async fn add_uri_sets_custom_user_agent() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("ua_custom");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({ "user-agent": "MyCustomUA/1.0" }),
    )
    .await;
    mgr.wait_finished(&gid).await.expect("任务失败");

    let uas = server.user_agents().await;
    assert!(
        uas.iter().any(|u| u == "MyCustomUA/1.0"),
        "服务器应收到自定义 UA，实际: {uas:?}"
    );
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn add_uri_applies_referer_and_custom_headers() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("headers");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(
        &base,
        &server.url(),
        &out,
        serde_json::json!({
            "referer": "https://example.com/video/1",
            "header": ["X-Test-Header: hello", "Origin: https://example.com"]
        }),
    )
    .await;
    mgr.wait_finished(&gid).await.expect("任务失败");

    let refs = server.request_headers("referer").await;
    assert!(
        refs.iter().any(|r| r == "https://example.com/video/1"),
        "服务器应收到 referer，实际: {refs:?}"
    );
    let xt = server.request_headers("x-test-header").await;
    assert!(
        xt.iter().any(|v| v == "hello"),
        "服务器应收到自定义 header，实际: {xt:?}"
    );
    let origin = server.request_headers("origin").await;
    assert!(
        origin.iter().any(|v| v == "https://example.com"),
        "服务器应收到自定义 Origin 头，实际: {origin:?}"
    );

    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn add_uri_defaults_to_browser_user_agent() {
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("ua_default");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;
    let gid = add_uri_with_opts(&base, &server.url(), &out, serde_json::json!({})).await;
    mgr.wait_finished(&gid).await.expect("任务失败");

    let expected = gkdl::download::config::default_user_agent();
    let uas = server.user_agents().await;
    assert!(
        uas.iter().any(|u| u == &expected),
        "服务器应收到默认浏览器 UA，实际: {uas:?}"
    );
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

#[tokio::test]
async fn get_jsonp_base64_params_are_recognized() {
    // AriaNg 的 GET 请求格式：params 是 base64 编码的 JSON 数组
    let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
    let server = TestServer::start(data, false).await;
    let out = out_path("jsonp_cjk");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let (base, mgr, _shutdown, _rx) = start_server("").await;

    // 复刻用户给出的载荷：含 CJK 文件名、自定义 UA、referer
    let out_name = "《蔚蓝档案》3rd PV.mp4";
    let out_path = std::env::temp_dir().join("gkdl_it").join(out_name);
    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(format!("{}.gkdl", out_path.display()));
    let json = serde_json::json!([
        [server.url()],
        {
            "dir": std::env::temp_dir().join("gkdl_it").display().to_string(),
            "referer": "https://example.com/video/1",
            "user-agent": "Mozilla/5.0 Custom/1.0",
            "out": out_name
        }
    ]);
    let b64 = base64_encode(json.to_string().as_bytes());
    let id = "《蔚蓝档案》3rd PV.mp4";
    let (status, v) = call_get(
        &base,
        &[("method", "aria2.addUri"), ("id", id), ("params", &b64)],
    )
    .await;
    assert_eq!(status, 200);
    let gid = v["result"].as_str().expect("GET addUri 应返回 gid");
    assert_eq!(v["id"].as_str(), Some(id), "id 应原样回显");

    mgr.wait_finished(gid).await.expect("任务失败");
    assert!(out_path.exists(), "out 选项应生效: {}", out_path.display());

    // 自定义 UA 生效
    let uas = server.user_agents().await;
    assert!(
        uas.iter().any(|u| u == "Mozilla/5.0 Custom/1.0"),
        "服务器应收到 GET 中 user-agent，实际: {uas:?}"
    );

    // referer 选项应作为 Referer 请求头发送
    let refs = server.request_headers("referer").await;
    assert!(
        refs.iter().any(|r| r == "https://example.com/video/1"),
        "服务器应收到 referer，实际: {refs:?}"
    );

    let _ = std::fs::remove_file(&out_path);
    let _ = std::fs::remove_file(format!("{}.gkdl", out_path.display()));
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));
}

/// 测试用 base64 编码（标准字母表，兼容 b64decode）。
fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}
