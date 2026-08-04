//! aria2 JSON-RPC 客户端（CLI 子命令用）：基于 libcurl 的阻塞 POST，经 `spawn_blocking` 接入异步。

use anyhow::{anyhow, Context, Result};
use curl::easy::{Easy, List};
use serde_json::Value;

/// 调用 daemon 的 `/jsonrpc`。`secret` 非空时自动注入 `token:` 参数。
pub async fn call(
    port: u16,
    secret: Option<String>,
    method: &str,
    params: Vec<Value>,
) -> Result<Value> {
    let url = format!("http://127.0.0.1:{port}/jsonrpc");
    let mut body_params = params;
    if let Some(sec) = secret {
        if !sec.is_empty() {
            body_params.insert(0, serde_json::json!(format!("token:{sec}")));
        }
    }
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "cli",
        "method": method,
        "params": body_params,
    });
    let body_str = body.to_string();

    let handle = tokio::task::spawn_blocking(move || {
        curl::init();
        let mut easy = Easy::new();
        easy.url(&url)
            .map_err(|e| anyhow!("连接 daemon 失败: {e}"))?;
        easy.post(true)?;
        easy.post_fields_copy(body_str.as_bytes())?;
        let mut list = List::new();
        list.append("Content-Type: application/json-rpc")?;
        easy.http_headers(list)?;
        easy.connect_timeout(std::time::Duration::from_secs(10))?;
        easy.timeout(std::time::Duration::from_secs(30))?;

        let mut buf = Vec::new();
        {
            let mut transfer = easy.transfer();
            transfer
                .write_function(|data| {
                    buf.extend_from_slice(data);
                    Ok(data.len())
                })
                .context("设置响应回调失败")?;
            transfer
                .perform()
                .map_err(|e| anyhow!("连接 daemon 失败: {e}"))?;
        }
        let status = easy.response_code().unwrap_or(0);
        if !(200..300).contains(&status) {
            anyhow::bail!("连接 daemon 失败: HTTP {status}");
        }
        let text = String::from_utf8(buf).context("daemon 响应不是 UTF-8")?;
        serde_json::from_str(&text).context("daemon 响应解析失败")
    })
    .await
    .context("RPC 调用任务异常退出")??;
    Ok(handle)
}
