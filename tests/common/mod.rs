//! 本地测试 HTTP 服务器：HTTP/1.1 keep-alive、Range、可配置故障路径。
//!
//! 除返回字节外，还记录每次请求的观测数据（连接号、路径、Range、时刻）与
//! 累计连接数，供测试断言「分段并行」「断点续传从已写字节处继续」「跨段
//! 连接复用（多请求少连接）」等协议行为，而不只验证最终文件内容。
//!
//! 端点一览：
//! - `/file.bin`      常规文件（支持 Range；构造时可选忽略 Range）
//! - `/slow`          每个请求响应前等待 `delay_ms`（模拟慢探测/慢连接）
//! - `/hang_download` 探测正常，下载请求挂起不响应（模拟远端卡死）
//! - `/gzip`          请求带 gzip 编码时回压缩体，否则回原始体（忽略 Range）
//! - `/nosize`        200 无 Content-Length，靠连接关闭界定 body（未知大小）
//! - `/block`         探测正常，非探测请求先拒绝 `block_first` 次（403/429/500）
//! - `/truncated`     探测正常，前 N 个段请求 Content-Length 报全长但只发 40% 即断开
//! - `/flaky`         探测正常，段请求只先发 `flaky_bytes` 字节即断开（每次如此）
//! - `/redirect`      302 → /file.bin
//! - `/fail` `/broken` 404
#![allow(dead_code)]
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// 单个请求的观测记录。
#[derive(Debug, Clone)]
pub struct ReqRecord {
    /// 服务该请求的 TCP 连接序号（按 accept 顺序从 0 计）。
    pub conn: usize,
    /// HTTP 方法（GET/HEAD/...）。
    pub method: String,
    pub path: String,
    /// 请求的 Range 区间（含端点）；无 Range 头为 None。
    pub range: Option<(u64, u64)>,
}

struct Opts {
    ignore_range: bool,
    slow_ms: u64,
    /// `/file.bin` 的错峰延迟：第 N 条连接的首字节延迟 N×stagger_ms，
    /// 用于制造「先完成的 worker 确定性窃取慢连接段尾」的场景。
    stagger_ms: u64,
    block_first: usize,
    block_status: u16,
    truncate_first: usize,
    flaky_bytes: u64,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            ignore_range: false,
            slow_ms: 0,
            stagger_ms: 0,
            block_first: 0,
            block_status: 403,
            truncate_first: 0,
            flaky_bytes: 0,
        }
    }
}

pub struct TestServer {
    pub addr: String,
    requests: Arc<Mutex<Vec<ReqRecord>>>,
    conn_count: Arc<AtomicUsize>,
    /// 收到的请求头（小写名 → 值列表，按请求先后），用于断言 UA/Referer/自定义头生效
    headers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    /// `/block` 路径收到的每个请求的时刻（含探测与拒绝），用于断言重试间隔
    block_times: Arc<Mutex<Vec<Instant>>>,
}

impl TestServer {
    /// 启动服务器。`data` 为文件内容，`ignore_range` 为 true 时忽略 Range 请求（始终 200 全量）。
    pub async fn start(data: Vec<u8>, ignore_range: bool) -> Self {
        Self::start_inner(
            data,
            Opts {
                ignore_range,
                ..Default::default()
            },
        )
        .await
    }

    /// 启动服务器，`/slow` 路径在响应前等待 `delay_ms` 毫秒（模拟慢探测）。
    pub async fn start_slow(data: Vec<u8>, ignore_range: bool, delay_ms: u64) -> Self {
        Self::start_inner(
            data,
            Opts {
                ignore_range,
                slow_ms: delay_ms,
                ..Default::default()
            },
        )
        .await
    }

    /// 启动服务器，`/block` 路径：探测（Range 0-0）正常 206，
    /// 其余请求先拒绝 `block_first` 次再恢复正常（模拟远端限流/反爬）。
    /// `status` 指定拒绝状态码（403/429 触发冷却，500 不触发）。
    pub async fn start_block_status(data: Vec<u8>, block_first: usize, status: u16) -> Self {
        Self::start_inner(
            data,
            Opts {
                block_first,
                block_status: status,
                ..Default::default()
            },
        )
        .await
    }

    /// 启动服务器，`/truncated` 路径：探测正常，前 `truncate_first` 个段请求
    /// Content-Length 报全长但只发送 40% 即断开（模拟传输中断，curl 报 partial file）。
    pub async fn start_truncated(data: Vec<u8>, truncate_first: usize) -> Self {
        Self::start_inner(
            data,
            Opts {
                truncate_first,
                ..Default::default()
            },
        )
        .await
    }

    /// 启动服务器，`/flaky` 路径：探测正常，每个段请求只先发 `flaky_bytes` 字节即断开。
    pub async fn start_flaky(data: Vec<u8>, flaky_bytes: u64) -> Self {
        Self::start_inner(
            data,
            Opts {
                flaky_bytes,
                ..Default::default()
            },
        )
        .await
    }

    /// 启动服务器，`/file.bin` 的第 N 条连接响应前等待 N×`stagger_ms` 毫秒，
    /// 制造确定性的工作窃取场景（先完成的 worker 窃取慢连接的段尾）。
    pub async fn start_staggered(data: Vec<u8>, stagger_ms: u64) -> Self {
        Self::start_inner(
            data,
            Opts {
                stagger_ms,
                ..Default::default()
            },
        )
        .await
    }

    async fn start_inner(data: Vec<u8>, opts: Opts) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let data = Arc::new(data);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let conn_count = Arc::new(AtomicUsize::new(0));
        let headers = Arc::new(Mutex::new(HashMap::new()));
        let block_times = Arc::new(Mutex::new(Vec::new()));

        // 以下字段打包给连接处理任务
        let server_data = Arc::clone(&data);
        let server_requests = Arc::clone(&requests);
        let server_conn = Arc::clone(&conn_count);
        let server_headers = Arc::clone(&headers);
        let server_block_times = Arc::clone(&block_times);
        let Opts {
            ignore_range,
            slow_ms,
            stagger_ms,
            block_first,
            block_status,
            truncate_first,
            flaky_bytes,
        } = opts;
        let block_left = Arc::new(AtomicUsize::new(block_first));
        let truncate_left = Arc::new(AtomicUsize::new(truncate_first));

        let task_addr = addr.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                let conn_id = server_conn.fetch_add(1, Ordering::SeqCst);
                let data = Arc::clone(&server_data);
                let requests = Arc::clone(&server_requests);
                let headers = Arc::clone(&server_headers);
                let block_times = Arc::clone(&server_block_times);
                let block_left = Arc::clone(&block_left);
                let truncate_left = Arc::clone(&truncate_left);
                let conn_addr = task_addr.clone();
                tokio::spawn(handle_conn(
                    socket,
                    data,
                    requests,
                    headers,
                    block_times,
                    block_left,
                    truncate_left,
                    ignore_range,
                    slow_ms,
                    stagger_ms,
                    block_status,
                    flaky_bytes,
                    conn_id,
                    conn_addr,
                ));
            }
        });

        Self {
            addr,
            requests,
            conn_count,
            headers,
            block_times,
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

    /// 全部请求的观测记录（按到达先后）。
    pub async fn request_log(&self) -> Vec<ReqRecord> {
        self.requests.lock().await.clone()
    }

    /// 服务器累计接受的 TCP 连接数。
    pub fn connection_count(&self) -> usize {
        self.conn_count.load(Ordering::SeqCst)
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

    /// 限流端点：探测正常，非探测请求先拒绝若干次再恢复。
    pub fn block_url(&self) -> String {
        format!("http://{}/block", self.addr)
    }

    /// 截断端点：前 N 个段请求 Content-Length 报全长但只发 40% 即断开。
    pub fn truncated_url(&self) -> String {
        format!("http://{}/truncated", self.addr)
    }

    /// 断流端点：每个段请求只先发 flaky_bytes 字节即断开。
    pub fn flaky_url(&self) -> String {
        format!("http://{}/flaky", self.addr)
    }

    /// 重定向端点：302 → /file.bin。
    pub fn redirect_url(&self) -> String {
        format!("http://{}/redirect", self.addr)
    }

    /// `/block` 路径收到的请求时刻（按先后，含探测与拒绝）。
    pub async fn block_request_times(&self) -> Vec<Instant> {
        self.block_times.lock().await.clone()
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_conn(
    mut socket: TcpStream,
    data: Arc<Vec<u8>>,
    requests: Arc<Mutex<Vec<ReqRecord>>>,
    headers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    block_times: Arc<Mutex<Vec<Instant>>>,
    block_left: Arc<AtomicUsize>,
    truncate_left: Arc<AtomicUsize>,
    ignore_range: bool,
    slow_ms: u64,
    stagger_ms: u64,
    block_status: u16,
    flaky_bytes: u64,
    conn_id: usize,
    addr: String,
) {
    let mut buf = [0u8; 8192];
    // HTTP/1.1 keep-alive：同一连接循环读取后续请求，
    // 引擎的跨段连接复用（curl 句柄复用）依赖服务器不主动断开。
    loop {
        // 读一个请求（GET 无 body，读到头结束标记即可）
        let mut raw = Vec::new();
        loop {
            let n = match socket.read(&mut buf).await {
                Ok(0) => return,
                Ok(n) => n,
                Err(_) => return,
            };
            raw.extend_from_slice(&buf[..n]);
            if raw.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
            if raw.len() > 64 * 1024 {
                return;
            }
        }
        let req = String::from_utf8_lossy(&raw).into_owned();
        let mut lines = req.lines();
        let request_line = lines.next().unwrap_or("");
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("GET").to_string();
        let path = parts.next().unwrap_or("/").to_string();

        // 解析 Range 与请求头
        let mut range: Option<(u64, u64)> = None;
        let mut seen: HashMap<String, Vec<String>> = HashMap::new();
        for line in lines.skip(1) {
            if let Some((name, value)) = line.split_once(':') {
                let lname = name.trim().to_ascii_lowercase();
                let lvalue = value.trim().to_string();
                if lname == "range" {
                    if let Some(r) = parse_range(&lvalue, data.len() as u64) {
                        range = Some(r);
                    }
                }
                seen.entry(lname).or_default().push(lvalue);
            }
        }

        // 记录请求观测数据（协议行为断言的依据）
        requests.lock().await.push(ReqRecord {
            conn: conn_id,
            method: method.clone(),
            path: path.clone(),
            range,
        });
        {
            let mut store = headers.lock().await;
            for (k, vs) in seen {
                store.entry(k).or_default().extend(vs);
            }
        }

        let total = data.len() as u64;

        // 失败路径与未知路径：404 后关闭
        if path == "/fail" || path == "/broken" || !is_known_path(&path) {
            let hdrs = "HTTP/1.1 404 Not Found\r\nContent-Length: 4\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.write_all(b"nope").await;
            let _ = socket.flush().await;
            return;
        }

        // /file.bin 的错峰延迟（制造确定性窃取场景）
        if stagger_ms > 0 && path == "/file.bin" {
            tokio::time::sleep(Duration::from_millis(stagger_ms * conn_id as u64)).await;
        }
        // /slow：每个请求响应前等待，模拟网络延迟
        if path == "/slow" && slow_ms > 0 {
            tokio::time::sleep(Duration::from_millis(slow_ms)).await;
        }

        // /block：探测（Range 0-0）正常 206；其余请求先拒绝 block_left 次
        // 再恢复正常。记录每次请求时刻，供测试断言重试被冷却拉开。
        if path == "/block" {
            block_times.lock().await.push(Instant::now());
            if range == Some((0, 0)) {
                send_range_slice(&mut socket, &data, 0, 0, total, None, false).await;
                continue;
            }
            if block_left.fetch_sub(1, Ordering::SeqCst) > 0 {
                let body = b"blocked";
                let hdrs = format!(
                    "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    block_status,
                    status_reason(block_status),
                    body.len()
                );
                let _ = socket.write_all(hdrs.as_bytes()).await;
                let _ = socket.write_all(&body[..]).await;
                let _ = socket.flush().await;
                return;
            }
            send_range_slice(&mut socket, &data, 0, total - 1, total, None, false).await;
            continue;
        }

        if method == "HEAD" {
            if path == "/nosize" {
                // 未知大小：HEAD 也不给 Content-Length
                let hdrs = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
                let _ = socket.write_all(hdrs.as_bytes()).await;
                return;
            }
            let hdrs = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\n\r\n"
            );
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.flush().await;
            continue;
        }

        // /redirect：302 到 /file.bin（探测与每个段请求都会先经过这里）
        if path == "/redirect" {
            let hdrs = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://{addr}/file.bin\r\nContent-Length: 0\r\n\r\n"
            );
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.flush().await;
            continue;
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
                let mut enc =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                enc.write_all(&data).unwrap();
                let gz = enc.finish().unwrap();
                let hdrs = format!(
                    "HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
                    gz.len()
                );
                let _ = socket.write_all(hdrs.as_bytes()).await;
                let _ = socket.write_all(&gz).await;
            } else {
                let hdrs = format!("HTTP/1.1 200 OK\r\nContent-Length: {total}\r\n\r\n");
                let _ = socket.write_all(hdrs.as_bytes()).await;
                let _ = socket.write_all(&data).await;
            }
            let _ = socket.flush().await;
            continue;
        }

        // /nosize：200 全量但无 Content-Length，body 到连接关闭为止
        if path == "/nosize" {
            let hdrs = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
            let _ = socket.write_all(hdrs.as_bytes()).await;
            let _ = socket.write_all(&data).await;
            let _ = socket.flush().await;
            return; // 连接关闭是未知大小的 body 终止符，必须断开
        }

        // /truncated：探测（0-0）正常放行且不消耗截断名额；
        // 前 N 个段请求 Content-Length 报全长、只发 40% 即断开。
        if path == "/truncated" {
            if range == Some((0, 0)) {
                send_range_slice(&mut socket, &data, 0, 0, total, None, false).await;
                continue;
            }
            let (start, end) = range.unwrap_or((0, total - 1));
            // fetch_update 保证计数器耗尽后不再截断（fetch_sub 会在 0 处下溢回绕）
            let cut = truncate_left
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .ok()
                .filter(|&n| n > 0)
                .map(|_| (end - start + 1) * 2 / 5);
            send_range_slice(&mut socket, &data, start, end, total, cut, cut.is_some()).await;
            if cut.is_some() {
                return; // 提前断开，模拟传输中断
            }
            continue;
        }

        // /flaky：探测（0-0）正常放行；段请求只先发 flaky_bytes 字节即断开（每次如此）
        if path == "/flaky" {
            if range == Some((0, 0)) {
                send_range_slice(&mut socket, &data, 0, 0, total, None, false).await;
                continue;
            }
            let (start, end) = range.unwrap_or((0, total - 1));
            let prefix = flaky_bytes.min(end - start + 1);
            send_range_slice(&mut socket, &data, start, end, total, Some(prefix), true).await;
            return;
        }

        // /hang_download：探测请求（0-0）正常响应；其余请求挂起不响应，
        // 模拟服务器卡死使任务停在半途。
        if path == "/hang_download" {
            if range == Some((0, 0)) {
                send_range_slice(&mut socket, &data, 0, 0, total, None, false).await;
                continue;
            }
            tokio::time::sleep(Duration::from_secs(600)).await;
            return;
        }

        // /file.bin 与 /slow 的常规服务
        match range {
            Some((start, end)) if !ignore_range => {
                send_range_slice(&mut socket, &data, start, end, total, None, false).await;
            }
            _ => {
                // 200 全量（无 Range，或服务器忽略 Range）
                let hdrs = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {total}\r\nAccept-Ranges: bytes\r\n\r\n"
                );
                let _ = socket.write_all(hdrs.as_bytes()).await;
                let _ = socket.write_all(&data).await;
                let _ = socket.flush().await;
            }
        }
    }
}

fn is_known_path(path: &str) -> bool {
    matches!(
        path,
        "/file.bin"
            | "/slow"
            | "/hang_download"
            | "/gzip"
            | "/nosize"
            | "/block"
            | "/truncated"
            | "/flaky"
            | "/redirect"
    )
}

fn status_reason(code: u16) -> &'static str {
    match code {
        403 => "Forbidden",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

/// 206 范围切片响应。`prefix` 为 None 发送完整切片；Some(n) 只发送前 n 字节
/// （Content-Length 仍报全长——客户端将因响应提前结束而报 partial file）。
/// `close` 为 true 时带 Connection: close，响应后由调用方结束连接。
async fn send_range_slice(
    socket: &mut TcpStream,
    data: &[u8],
    start: u64,
    end: u64,
    total: u64,
    prefix: Option<u64>,
    close: bool,
) {
    let slice = &data[start as usize..=end as usize];
    let n = prefix.unwrap_or(slice.len() as u64).min(slice.len() as u64);
    let conn = if close { "Connection: close\r\n" } else { "" };
    let hdrs = format!(
        "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/{total}\r\nAccept-Ranges: bytes\r\n{conn}\r\n",
        slice.len()
    );
    let _ = socket.write_all(hdrs.as_bytes()).await;
    let _ = socket.write_all(&slice[..n as usize]).await;
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
