//! libcurl HTTP 传输层：基于 curl crate 的 Easy 句柄实现探测与分段下载。
//!
//! 分段/探测强制 `Accept-Encoding: identity`（必须拿原始字节，不做解压），
//! 整文件流式可用 gzip/deflate（libcurl 自动解压）；请求走 `spawn_blocking`，
//! 回调经 tokio 通道传回异步侧（通道满即 TCP 背压）。

use crate::download::config::DownloadConfig;
use anyhow::{anyhow, bail, Result};
use curl::easy::{Easy, HttpVersion, List, WriteError};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// 确保 libcurl 全局初始化（内部 Once 保证幂等，重复调用无害）。
fn ensure_curl_init() {
    curl::init();
}

/// 探测结果：总长（0 表示未知）与 Range 支持情况。
#[derive(Debug, Clone, Copy)]
pub struct ProbeInfo {
    pub total: u64,
    pub total_known: bool,
    pub supports_range: bool,
}

/// 分段下载的流式消息（从阻塞线程经 tokio 通道传出）。
pub enum CurlMsg {
    /// 一个响应头块结束（重定向会收到多个），携带该块的状态码。
    Headers { status: u32 },
    /// 一块响应体数据。
    Data(Vec<u8>),
    /// 传输结束（成功或失败）。一并归还 Easy 句柄——其连接缓存可能仍保留
    /// 到目标主机的空闲连接，worker 据此跨段复用连接（每段省一次 DNS/TCP/TLS
    /// 握手，也避免向服务器暴露「高频新建连接」的爬虫特征）。接收端丢弃时
    /// 句柄随通道一起销毁，连接自然关闭。
    End(Result<(), curl::Error>, Easy),
}

/// 基于 libcurl 的 HTTP 客户端（线程安全，可跨任务共享）。
#[derive(Clone)]
pub struct HttpClient {
    user_agent: String,
    referer: Option<String>,
    headers: Vec<(String, String)>,
    timeout: u64,
}

/// 响应头解析状态（探测用）。
#[derive(Default)]
struct HeaderState {
    status: u32,
    content_range: Option<String>,
    content_length: Option<String>,
    /// 探测已消费的 body 字节数（206 干净收尾用）。
    consumed: u64,
    /// 探测中途掐断过 body（此类连接不可复用）。
    aborted: bool,
}

/// 探测阶段愿意完整读掉的响应体上限。Range 0-0 的正常响应只有 1 字节；
/// 留出余量防畸形服务器，超出即掐断（放弃复用）。
const PROBE_BODY_CAP: u64 = 64 * 1024;

/// 增量解析一行响应头；遇到新的状态行会重置当前块（支持重定向多响应）。
fn update_header_state(st: &mut HeaderState, data: &[u8]) {
    let line = String::from_utf8_lossy(data);
    let lower = line.to_ascii_lowercase();
    if line.starts_with("HTTP/") || lower.starts_with(":status:") {
        let parsed = if line.starts_with("HTTP/") {
            line.split_whitespace()
                .nth(1)
                .and_then(|s| s.parse::<u32>().ok())
        } else {
            lower
                .split(':')
                .nth(1)
                .and_then(|s| s.trim().parse::<u32>().ok())
        };
        st.status = parsed.unwrap_or(0);
        st.content_range = None;
        st.content_length = None;
    } else if lower.starts_with("content-range:") {
        st.content_range = line.split_once(':').map(|(_, v)| v.trim().to_string());
    } else if lower.starts_with("content-length:") {
        st.content_length = line.split_once(':').map(|(_, v)| v.trim().to_string());
    }
}

/// 把 curl 错误转成带关键词的中文文案（供 map_error_code 匹配）。
pub fn curl_error_text(e: &curl::Error) -> String {
    if e.is_operation_timedout() {
        "连接/读超时(timeout)".to_string()
    } else if e.is_couldnt_resolve_host() {
        "无法解析主机(dns error)".to_string()
    } else if e.is_couldnt_connect() {
        "连接失败(connect)".to_string()
    } else if e.is_partial_file() {
        "响应提前结束(partial)".to_string()
    } else if e.is_write_error() || e.is_aborted_by_callback() {
        "传输被中止".to_string()
    } else {
        e.to_string()
    }
}

impl HttpClient {
    /// 依据下载配置构建客户端（解析 UA/Referer/自定义头）。
    pub fn new(config: &DownloadConfig) -> Result<Self> {
        ensure_curl_init();
        let ua = if config.user_agent.trim().is_empty() {
            crate::download::config::default_user_agent()
        } else {
            config.user_agent.clone()
        };
        let referer = if config.referer.trim().is_empty() {
            None
        } else {
            Some(config.referer.trim().to_string())
        };
        let mut headers = Vec::new();
        for h in &config.header {
            let Some((name, value)) = h.split_once(':') else {
                tracing::warn!("忽略无效的请求头（缺少冒号）: {h}");
                continue;
            };
            let name = name.trim();
            let value = value.trim();
            if name.is_empty()
                || name.chars().any(|c| c.is_control() || c == ' ')
                || value.contains(['\r', '\n'])
            {
                tracing::warn!("忽略无效的请求头: {h}");
                continue;
            }
            headers.push((name.to_string(), value.to_string()));
        }
        Ok(Self {
            user_agent: ua,
            referer,
            headers,
            // 钳制最少 5s：0 在 curl 里表示禁用超时，请求会永久挂起
            timeout: config.timeout.max(5),
        })
    }

    /// 请求超时秒数（构造时已钳制最少 5s）。
    pub fn timeout_secs(&self) -> u64 {
        self.timeout
    }

    /// IDN 主机名转 punycode（中文域名可用）；已是 ASCII/无主机时原样返回。
    ///
    /// 边界：带端口（`例子.中国:8080`）、IPv6 字面量（`[::1]` 跳过）、已 punycode（跳过）、
    /// 用户信息（`user:pass@`）原样保留。
    pub fn normalize_url(url: &str) -> String {
        let Some(scheme_sep) = url.find("://") else {
            return url.to_string();
        };
        let scheme = &url[..scheme_sep];
        let rest = &url[scheme_sep + 3..];
        let auth_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        let authority = &rest[..auth_end];
        let tail = &rest[auth_end..];

        // 用户信息（user:pass@）与主机分离
        let (userinfo, hostport) = match authority.rfind('@') {
            Some(i) => (&authority[..=i], &authority[i + 1..]),
            None => ("", authority),
        };

        let (host, port) = if hostport.starts_with('[') {
            // IPv6 字面量：原样保留（跳过 IDN）
            let host = if let Some(end) = hostport.find(']') {
                &hostport[..=end]
            } else {
                hostport
            };
            let port = &hostport[host.len()..];
            (host, port)
        } else {
            match hostport.rsplit_once(':') {
                Some((h, p)) if !p.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => {
                    (h, &hostport[h.len()..])
                }
                _ => (hostport, ""),
            }
        };

        if host.is_ascii() {
            return url.to_string();
        }
        let puny = idna::domain_to_ascii(host).unwrap_or_else(|_| host.to_string());
        format!("{scheme}://{userinfo}{puny}{port}{tail}")
    }

    /// 构造统一配置的 Easy 句柄：URL(normalized)/UA/Referer/自定义头/Range/重定向/连接超时。
    /// `encoding` 为 `Accept-Encoding` 值；分段/探测传 `"identity"`，流式整文件可传 `"gzip, deflate"`。
    pub fn easy_with_encoding(
        &self,
        url: &str,
        range: Option<(u64, u64)>,
        encoding: &str,
    ) -> Result<Easy> {
        ensure_curl_init();
        let mut easy = Easy::new();
        self.apply_request_opts(&mut easy, url, range, encoding)?;
        Ok(easy)
    }

    /// 在（新建或探测复用的）句柄上套用全部请求选项。复用句柄时旧回调/URL/Range
    /// 均被覆盖；探测阶段设置的总超时在此清零（下载依赖低速兜底 + async 侧读超时）。
    fn apply_request_opts(
        &self,
        easy: &mut Easy,
        url: &str,
        range: Option<(u64, u64)>,
        encoding: &str,
    ) -> Result<()> {
        easy.url(&Self::normalize_url(url))?;
        easy.useragent(&self.user_agent)?;
        if let Some(r) = &self.referer {
            easy.referer(r)?;
        }
        if !self.headers.is_empty() {
            let mut list = List::new();
            for (name, value) in &self.headers {
                list.append(&format!("{name}: {value}"))?;
            }
            easy.http_headers(list)?;
        }
        easy.accept_encoding(encoding)?;
        if let Some((start, end)) = range {
            easy.range(&format!("{start}-{end}"))?;
        }
        easy.follow_location(true)?;
        easy.max_redirections(10)?;
        easy.connect_timeout(Duration::from_secs(self.timeout))?;
        // HTTPS 走 HTTP/2（ALPN 协商，失败自动回落 1.1）；HTTP 保持 1.1（不尝试 h2c）。
        easy.http_version(HttpVersion::V2TLS)?;
        // 低速兜底：服务器长时间无数据传输（卡死/静默）时，让阻塞线程自行退出。
        // 否则 async 侧读超时放弃段、接收端被丢弃后，spawn_blocking 里的 curl 仍会
        // 阻塞在 recv() 上无期限地占用一个 blocking 线程（泄漏）。语义与 async 侧
        // 「两次 chunk 间隔超时」一致：正常传输有数据流动不会触发。
        easy.low_speed_limit(1)?;
        easy.low_speed_time(Duration::from_secs(self.timeout))?;
        // 清除探测阶段可能设置的整段总超时（0=禁用）
        easy.timeout(Duration::ZERO)?;
        Ok(())
    }

    /// 分段/探测用的 Easy 句柄：强制 `Accept-Encoding: identity`，拿原始字节。
    pub fn easy(&self, url: &str, range: Option<(u64, u64)>) -> Result<Easy> {
        self.easy_with_encoding(url, range, "identity")
    }

    /// 分段下载（复用探测阶段建立的连接）：句柄的连接缓存里保留着同主机的
    /// 空闲连接，跳过 DNS/TCP/TLS 握手直接发请求。仅当探测以「干净收尾」
    /// 结束时调用方才持有句柄；选项覆盖失败则由调用方回退到新建连接。
    /// 先 `reset()` 清掉探测残留选项（Range/总超时/旧回调）——reset 不清
    /// 连接缓存，空闲连接仍然可复用。
    pub fn stream_segment_warm(
        &self,
        mut easy: Easy,
        url: &str,
        range: Option<(u64, u64)>,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        easy.reset();
        self.stream_impl_with(easy, url, range, "identity")
    }

    /// 阻塞线程中发起流式分段下载：返回消息接收端与后台任务句柄。
    /// 通道满时 `blocking_send` 阻塞即 TCP 背压；接收端被丢弃时传输中止。
    pub fn stream_segment(
        &self,
        url: &str,
        range: Option<(u64, u64)>,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        self.stream_impl(url, range, "identity")
    }

    /// 整文件流式下载（单连接）：无 Range，请求 gzip/deflate 由 libcurl 自动解压。
    pub fn stream_whole(
        &self,
        url: &str,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        self.stream_impl(url, None, "gzip, deflate")
    }

    /// 整文件流式下载的预热句柄变体（语义同 `stream_segment_warm`）。
    pub fn stream_whole_warm(
        &self,
        mut easy: Easy,
        url: &str,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        easy.reset();
        self.stream_impl_with(easy, url, None, "gzip, deflate")
    }

    fn stream_impl(
        &self,
        url: &str,
        range: Option<(u64, u64)>,
        encoding: &str,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        ensure_curl_init();
        let easy = self.easy_with_encoding(url, range, encoding)?;
        self.stream_impl_with(easy, url, range, encoding)
    }

    /// 流式下载的公共实现：`easy` 可为新建句柄或探测复用的预热句柄。
    fn stream_impl_with(
        &self,
        mut easy: Easy,
        url: &str,
        range: Option<(u64, u64)>,
        encoding: &str,
    ) -> Result<(
        tokio::sync::mpsc::Receiver<CurlMsg>,
        tokio::task::JoinHandle<()>,
    )> {
        ensure_curl_init();
        self.apply_request_opts(&mut easy, url, range, encoding)?;
        let (tx, rx) = tokio::sync::mpsc::channel::<CurlMsg>(4);

        // 头回调：状态行更新 status，空行表示一个响应头块结束 → 发送 Headers。
        let tx_headers = tx.clone();
        let mut status = 0u32;
        easy.header_function(move |data: &[u8]| {
            let line = String::from_utf8_lossy(data);
            let lower = line.to_ascii_lowercase();
            if line.starts_with("HTTP/") || lower.starts_with(":status:") {
                let parsed = if line.starts_with("HTTP/") {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|s| s.parse::<u32>().ok())
                } else {
                    lower
                        .split(':')
                        .nth(1)
                        .and_then(|s| s.trim().parse::<u32>().ok())
                };
                if let Some(s) = parsed {
                    status = s;
                    // 状态行出现即上报；HTTP/2 头块无空行结束标记，不能只靠空行触发
                    if tx_headers
                        .blocking_send(CurlMsg::Headers { status })
                        .is_err()
                    {
                        return false; // 接收端已丢弃，中止传输
                    }
                }
            } else if (data == b"\r\n" || data == b"\n")
                && tx_headers
                    .blocking_send(CurlMsg::Headers { status })
                    .is_err()
            {
                return false; // 接收端已丢弃，中止传输
            }
            true
        })?;

        let tx_write = tx.clone();
        let tx_end = tx;
        easy.write_function(move |data: &[u8]| -> Result<usize, WriteError> {
            if tx_write
                .blocking_send(CurlMsg::Data(data.to_vec()))
                .is_err()
            {
                return Ok(0); // 0 → curl 以 CURLE_WRITE_ERROR 中止
            }
            Ok(data.len())
        })?;

        let handle = tokio::task::spawn_blocking(move || {
            let result = easy.perform();
            // 归还句柄（连接缓存随句柄保留）；接收端已丢弃则发送失败，句柄就地销毁
            let _ = tx_end.blocking_send(CurlMsg::End(result, easy));
        });
        Ok((rx, handle))
    }
    /// 异步探测：GET `Range: bytes=0-0` 权威探测总长与 Range 支持，可被取消。
    /// 成功且连接「干净收尾」时一并返回该 Easy 句柄——其连接缓存保留了到
    /// 目标主机的空闲连接，可供首个 worker 复用（省一次 DNS/TCP/TLS 握手）。
    pub async fn probe(
        &self,
        url: &str,
        cancel: &CancellationToken,
    ) -> Result<(ProbeInfo, Option<Easy>)> {
        let this = self.clone();
        let url_owned = url.to_string();
        let (tx, rx) = tokio::sync::oneshot::channel::<(Result<ProbeInfo, String>, Option<Easy>)>();
        let join = tokio::task::spawn_blocking(move || {
            let result = this.probe_blocking(&url_owned);
            let _ = tx.send(result);
        });

        tokio::select! {
            r = rx => match r {
                Ok((Ok(info), warm)) => Ok((info, warm)),
                Ok((Err(e), _)) => Err(anyhow!("探测失败: {e}")),
                Err(_) => Err(anyhow!("探测任务异常退出")),
            },
            _ = cancel.cancelled() => {
                join.abort();
                bail!("下载已取消")
            }
        }
    }

    /// 阻塞探测实现（在 spawn_blocking 内执行）。返回值第二项为可复用的
    /// Easy 句柄：仅当 206 响应被完整消费（传输干净结束、空闲连接入缓存）
    /// 时才有值；掐断过 body 或请求失败均返回 None。
    fn probe_blocking(&self, url: &str) -> (Result<ProbeInfo, String>, Option<Easy>) {
        let url = Self::normalize_url(url);
        let mut easy = match self.easy(&url, Some((0, 0))) {
            Ok(e) => e,
            Err(e) => return (Err(format!("{e:#}")), None),
        };
        if let Err(e) = easy.timeout(Duration::from_secs(self.timeout)) {
            return (Err(curl_error_text(&e)), None);
        }

        let state = Arc::new(Mutex::new(HeaderState::default()));
        let cb = Arc::clone(&state);
        if let Err(e) = easy.header_function(move |data: &[u8]| {
            update_header_state(&mut cb.lock().unwrap(), data);
            true
        }) {
            return (Err(curl_error_text(&e)), None);
        }
        // 206：完整读掉极小的 body（Range 0-0 → 1 字节），让 perform 干净结束、
        // 空闲连接进入句柄缓存供首个 worker 复用；其余状态码（200 整文件/重定向
        // 带体等）：立即掐断，避免为探测拖回整个文件（此类连接不可复用）。
        let cb_write = Arc::clone(&state);
        if let Err(e) = easy.write_function(move |data: &[u8]| -> Result<usize, WriteError> {
            let mut st = cb_write.lock().unwrap();
            if st.status == 206 && st.consumed + data.len() as u64 <= PROBE_BODY_CAP {
                st.consumed += data.len() as u64;
                Ok(data.len())
            } else {
                st.aborted = true;
                Ok(0) // curl 以 CURLE_WRITE_ERROR 中止
            }
        }) {
            return (Err(curl_error_text(&e)), None);
        }

        let perf = easy.perform();
        let st = state.lock().unwrap();
        let status = st.status;
        let content_range = st.content_range.clone();
        let content_length = st.content_length.clone();
        // 「干净收尾」要求 perform 本身成功：中途出错（partial/recv error 等）
        // 时连接状态不可信，即使头部是完整 206 也不交给下游复用。
        let reusable = status == 206 && !st.aborted && perf.is_ok();
        drop(st);

        if status == 0 {
            let msg = match perf {
                Ok(_) => "无响应".to_string(),
                Err(e) => curl_error_text(&e),
            };
            return (Err(msg), None);
        }

        if status == 206 {
            // Content-Range: bytes 0-0/12345
            let total = match content_range
                .as_deref()
                .and_then(|cr| cr.split('/').nth(1))
                .and_then(|s| s.trim().parse::<u64>().ok())
            {
                Some(t) => t,
                None => {
                    return (
                        Err(format!("206 响应缺少 Content-Range: {content_range:?}")),
                        None,
                    )
                }
            };
            if total == 0 {
                return (Err("文件大小为 0".to_string()), None);
            }
            let warm = reusable.then_some(easy);
            (
                Ok(ProbeInfo {
                    total,
                    total_known: true,
                    supports_range: true,
                }),
                warm,
            )
        } else if status == 200 {
            // 总长兜底：优先 GET 的 Content-Length，缺失时 HEAD；都拿不到则视为大小未知
            // （是否接受未知大小由引擎结合 allow_compression 决定）
            let head_total = self.head_total_blocking(&url).ok().flatten();
            let total = content_length
                .and_then(|s| s.parse::<u64>().ok())
                .or(head_total);
            if let Some(total) = total {
                if total == 0 {
                    return (Err("文件大小为 0".to_string()), None);
                }
                (
                    Ok(ProbeInfo {
                        total,
                        total_known: true,
                        supports_range: false,
                    }),
                    None, // body 被掐断，连接不可复用
                )
            } else {
                (
                    Ok(ProbeInfo {
                        total: 0,
                        total_known: false,
                        supports_range: false,
                    }),
                    None,
                )
            }
        } else {
            (Err(format!("探测失败: HTTP {status}")), None)
        }
    }

    /// HEAD 请求拿 Content-Length（200 无总长的兜底）。
    fn head_total_blocking(&self, url: &str) -> Result<Option<u64>, String> {
        let mut easy = self.easy(url, None).map_err(|e| format!("{e:#}"))?;
        easy.nobody(true).map_err(|e| curl_error_text(&e))?; // HEAD
        easy.timeout(Duration::from_secs(self.timeout))
            .map_err(|e| curl_error_text(&e))?;

        let state = Arc::new(Mutex::new(HeaderState::default()));
        let cb = Arc::clone(&state);
        easy.header_function(move |data| {
            update_header_state(&mut cb.lock().unwrap(), data);
            true
        })
        .map_err(|e| curl_error_text(&e))?;
        let _ = easy.perform();
        let st = state.lock().unwrap();
        Ok(st
            .content_length
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_ascii_url_unchanged() {
        assert_eq!(
            HttpClient::normalize_url("https://a.com/b/c.zip"),
            "https://a.com/b/c.zip"
        );
        assert_eq!(
            HttpClient::normalize_url("https://a.com:8080/b/c.zip?x=1"),
            "https://a.com:8080/b/c.zip?x=1"
        );
        assert_eq!(
            HttpClient::normalize_url("http://[::1]/file.bin"),
            "http://[::1]/file.bin"
        );
        assert_eq!(HttpClient::normalize_url("not-a-url"), "not-a-url");
    }

    #[test]
    fn normalize_idn_domain_to_punycode() {
        assert_eq!(
            HttpClient::normalize_url("http://例子.中国/文件.zip"),
            "http://xn--fsqu00a.xn--fiqs8s/文件.zip"
        );
        assert_eq!(
            HttpClient::normalize_url("http://例子.中国:8080/文件.zip"),
            "http://xn--fsqu00a.xn--fiqs8s:8080/文件.zip"
        );
        // 已 punycode：跳过
        assert_eq!(
            HttpClient::normalize_url("http://xn--fsqu00a.xn--fiqs8s/a"),
            "http://xn--fsqu00a.xn--fiqs8s/a"
        );
        // 用户信息保留
        assert_eq!(
            HttpClient::normalize_url("http://user:pass@例子.中国/a"),
            "http://user:pass@xn--fsqu00a.xn--fiqs8s/a"
        );
    }

    #[test]
    fn curl_error_text_falls_back_to_display() {
        // 无效方案 URL 在 perform 时报 CURLE_UNSUPPORTED_PROTOCOL，文本非空
        let mut easy = curl::easy::Easy::new();
        easy.url("not-a-real-url").unwrap();
        let e = easy.perform().unwrap_err();
        assert!(!curl_error_text(&e).is_empty());
    }
}
