//! 本地测试 HTTP 服务器：支持 Range、可配置失败路径、可忽略 Range。
//! 另提供 `/gzip`（Content-Encoding: gzip）与 `/nosize`（无 Content-Length）端点用于流式压缩测试。
#![allow(dead_code)]
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

pub struct TestServer {
    pub addr: String,
    data: Arc<Vec<u8>>,
    fail_paths: Arc<Vec<String>>,
    ignore_range: bool,
    /// `/slow` 路径响应前等待的毫秒数（用于制造慢探测）
    slow_ms: u64,
    /// 收到的请求头（小写名 → 值列表，按请求先后），用于断言 UA/Referer/自定义头生效
    headers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    _guard: Mutex<()>,
}

impl TestServer {
    /// 启动服务器。`data` 为文件内容，`ignore_range` 为 true 时忽略 Range 请求（始终 200 全量）。
    pub async fn start(data: Vec<u8>, ignore_range: bool) -> Self {
        Self::start_slow(data, ignore_range, 0).await
    }

    /// 启动服务器，`/slow` 路径在响应前等待 `delay_ms` 毫秒（模拟慢探测）。
    pub async fn start_slow(data: Vec<u8>, ignore_range: bool, delay_ms: u64) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let data = Arc::new(data);
        let fail_paths = Arc::new(vec!["/fail".to_string(), "/broken".to_string()]);
        let headers = Arc::new(Mutex::new(HashMap::new()));
        let server_data = Arc::clone(&data);
        let server_fail = Arc::clone(&fail_paths);
        let server_headers = Arc::clone(&headers);

        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let data = Arc::clone(&server_data);
                let fail = Arc::clone(&server_fail);
                let headers = Arc::clone(&server_headers);
                tokio::spawn(async move {
                    handle_conn(socket, data, fail, headers, ignore_range, delay_ms).await;
                });
            }
        });

        Self {
            addr,
            data,
            fail_paths,
            ignore_range,
            slow_ms: delay_ms,
            headers,
            _guard: Mutex::new(()),
        }
    }

    /// 服务器已收到的 User-Agent 头（按请求先后）。
    pub async fn user_agents(&self) -> Vec<String> {
        self.request_headers("user-agent").await
    }

    /// 服务器已收到指定请求头（名不区分大小写）的所有值。
    pub async fn request_headers(&self, name: &str) -> Vec<String> {
        self.headers
            .lock()
            .await
            .get(&name.to_ascii_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    pub fn url(&self) -> String {
        format!("http://{}/file.bin", self.addr)
    }

    /// 慢探测路径：响应前等待 slow_ms 毫秒。
    pub fn url_slow(&self) -> String {
        format!("http://{}/slow", self.addr)
    }

    pub fn fail_url(&self) -> String {
        format!("http://{}/fail", self.addr)
    }

    pub fn broken_url(&self) -> String {
        format!("http://{}/broken", self.addr)
    }

    /// gzip 压缩端点：请求带 `Accept-Encoding: gzip` 时返回 `Content-Encoding: gzip` 的压缩体。
    pub fn gzip_url(&self) -> String {
        format!("http://{}/gzip", self.addr)
    }

    /// 无 Content-Length 端点：body 到连接关闭为止（模拟未知大小）。
    pub fn nosize_url(&self) -> String {
        format!("http://{}/nosize", self.addr)
    }
}

async fn handle_conn(
    mut socket: TcpStream,
    data: Arc<Vec<u8>>,
    fail_paths: Arc<Vec<String>>,
    headers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ignore_range: bool,
    slow_ms: u64,
) {
    let mut buf = [0u8; 8192];
    let mut request = Vec::new();
    // 读请求头
    loop {
        let n = match socket.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => n,
            Err(_) => return,
        };
        request.extend_from_slice(&buf[..n]);
        if request.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if request.len() > 64 * 1024 {
            return;
        }
    }
    let req = String::from_utf8_lossy(&request).into_owned();
    let mut lines = req.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("GET").to_string();
    let path = parts.next().unwrap_or("/").to_string();

    if fail_paths.contains(&path) {
        respond(
            &mut socket,
            "404 Not Found",
            "text/plain",
            None,
            &b"not found"[..],
        )
        .await;
        return;
    }

    if path != "/file.bin"
        && path != "/slow"
        && path != "/hang_download"
        && path != "/gzip"
        && path != "/nosize"
    {
        respond(
            &mut socket,
            "404 Not Found",
            "text/plain",
            None,
            &b"nope"[..],
        )
        .await;
        return;
    }

    // 慢探测路径：先等待，模拟网络延迟
    if path == "/slow" && slow_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(slow_ms)).await;
    }

    // 解析 Range
    let mut range: Option<(u64, u64)> = None;
    for line in lines {
        if line.to_ascii_lowercase().starts_with("range:") {
            let value = line[6..].trim();
            if let Some(r) = parse_range(value, data.len() as u64) {
                range = Some(r);
            }
        }
    }

    // 记录请求头（小写名 → 值），用于断言 UA/Referer/自定义头生效
    let mut seen: HashMap<String, Vec<String>> = HashMap::new();
    for line in req.lines().skip(1) {
        if let Some((name, value)) = line.split_once(':') {
            seen.entry(name.trim().to_ascii_lowercase())
                .or_default()
                .push(value.trim().to_string());
        }
    }
    {
        let mut store = headers.lock().await;
        for (k, vs) in seen {
            store.entry(k).or_default().extend(vs);
        }
    }

    if method == "HEAD" {
        if path == "/nosize" {
            // 未知大小：HEAD 也不给 Content-Length
            let headers = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(headers.as_bytes()).await;
            return;
        }
        let headers = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
            data.len()
        );
        let _ = socket.write_all(headers.as_bytes()).await;
        return;
    }

    // /gzip：忽略 Range（200 全量），请求带 gzip 编码则回压缩体，否则回原始体
    if path == "/gzip" {
        let accept_encoding = {
            let store = headers.lock().await;
            store
                .get("accept-encoding")
                .cloned()
                .unwrap_or_default()
                .join(",")
        };
        if accept_encoding.to_ascii_lowercase().contains("gzip") {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(&data).unwrap();
            let gz = enc.finish().unwrap();
            let hdrs = format!(
                "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                gz.len()
            );
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.write_all(&gz).await;
        } else {
            let hdrs = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                data.len()
            );
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.write_all(&data).await;
        }
        let _ = socket.flush().await;
        return;
    }

    // /nosize：200 全量但无 Content-Length，body 到连接关闭为止
    if path == "/nosize" {
        let hdrs = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
        let _ = socket.write_all(hdrs.as_bytes()).await;
        let _ = socket.write_all(&data).await;
        let _ = socket.flush().await;
        return;
    }

    match range {
        Some((start, end)) if !ignore_range && path != "/hang_download" => {
            let slice = &data[start as usize..=end as usize];
            let headers = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                slice.len(),
                start,
                end,
                data.len()
            );
            let _ = socket.write_all(headers.as_bytes()).await;
            let _ = socket.write_all(slice).await;
        }
        _ if path == "/hang_download" => {
            // 探测请求（Range: bytes=0-0）正常响应，其余请求挂起不响应，
            // 用于模拟「服务器卡死/迟迟不返回」使旧任务引擎停在半途。
            if range == Some((0, 0)) {
                let headers = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: 1\r\nContent-Range: bytes 0-0/{}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    data.len()
                );
                let _ = socket.write_all(headers.as_bytes()).await;
                let _ = socket.write_all(&data[..1]).await;
            } else {
                tokio::time::sleep(std::time::Duration::from_secs(600)).await;
            }
        }
        _ => {
            // 200 全量
            let headers = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                data.len()
            );
            let _ = socket.write_all(headers.as_bytes()).await;
            let _ = socket.write_all(&data).await;
        }
    }
    let _ = socket.flush().await;
}

fn parse_range(value: &str, total: u64) -> Option<(u64, u64)> {
    let spec = value.strip_prefix("bytes=")?;
    let (s, e) = spec.split_once('-')?;
    let start: u64 = s.trim().parse().ok()?;
    let end = if e.is_empty() {
        total - 1
    } else {
        e.trim().parse::<u64>().ok()?.min(total - 1)
    };
    if start > end {
        return None;
    }
    Some((start, end))
}

async fn respond(socket: &mut TcpStream, status: &str, _ct: &str, len: Option<u64>, body: &[u8]) {
    let headers = format!(
        "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        status,
        len.unwrap_or(body.len() as u64)
    );
    let _ = socket.write_all(headers.as_bytes()).await;
    let _ = socket.write_all(body).await;
    let _ = socket.flush().await;
}
