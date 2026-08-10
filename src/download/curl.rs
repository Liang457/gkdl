//! libcurl HTTP 传输层：基于 curl crate 的 Easy 句柄实现探测与分段下载。
//!
//! 所有请求强制 `Accept-Encoding: identity`（分段下载必须拿原始字节，不做解压）；
//! 探测/下载走 `spawn_blocking`，回调经 tokio 通道传回异步侧（通道满即 TCP 背压）。

use crate::download::config::DownloadConfig;
use anyhow::{anyhow, bail, Result};
use curl::easy::{Easy, List, WriteError};
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
    /// 传输结束（成功或失败）。
    End(Result<(), curl::Error>),
}

/// 基于 libcurl 的 HTTP 客户端（线程安全，可跨协程共享）。
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
}

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
            timeout: config.timeout,
        })
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
        easy.connect_timeout(Duration::from_secs(self.timeout.max(5)))?;
        Ok(easy)
    }

    /// 分段/探测用的 Easy 句柄：强制 `Accept-Encoding: identity`，拿原始字节。
    pub fn easy(&self, url: &str, range: Option<(u64, u64)>) -> Result<Easy> {
        self.easy_with_encoding(url, range, "identity")
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
        // 分段下载必须请求 identity 编码，拿原始字节
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
        let mut easy = self.easy_with_encoding(url, range, encoding)?;
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
            let _ = tx_end.blocking_send(CurlMsg::End(result));
        });
        Ok((rx, handle))
    }

    /// 异步探测：GET `Range: bytes=0-0` 权威探测总长与 Range 支持，可被取消。
    pub async fn probe(
        &self,
        url: &str,
        cancel: &CancellationToken,
        timeout: u64,
    ) -> Result<ProbeInfo> {
        let this = self.clone();
        let url_owned = url.to_string();
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<ProbeInfo, String>>();
        let join = tokio::task::spawn_blocking(move || {
            let result = this.probe_blocking(&url_owned, timeout);
            let _ = tx.send(result);
        });
        tokio::select! {
            r = rx => match r {
                Ok(Ok(info)) => Ok(info),
                Ok(Err(e)) => Err(anyhow!("探测失败: {e}")),
                Err(_) => Err(anyhow!("探测任务异常退出")),
            },
            _ = cancel.cancelled() => {
                join.abort();
                bail!("下载已取消")
            }
        }
    }

    /// 阻塞探测实现（在 spawn_blocking 内执行）。
    fn probe_blocking(&self, url: &str, timeout: u64) -> Result<ProbeInfo, String> {
        let url = Self::normalize_url(url);
        let mut easy = self
            .easy(&url, Some((0, 0)))
            .map_err(|e| format!("{e:#}"))?;
        easy.timeout(Duration::from_secs(timeout.max(5)))
            .map_err(|e| curl_error_text(&e))?;

        let state = Arc::new(Mutex::new(HeaderState::default()));
        let cb = Arc::clone(&state);
        easy.header_function(move |data| {
            update_header_state(&mut cb.lock().unwrap(), data);
            true
        })
        .map_err(|e| curl_error_text(&e))?;
        // 收到响应头后立即掐断 body（Ok(0)），忽略由此产生的写错误
        easy.write_function(|_: &[u8]| -> Result<usize, WriteError> { Ok(0) })
            .map_err(|e| curl_error_text(&e))?;

        let perf = easy.perform();
        let st = state.lock().unwrap();
        let status = st.status;
        let content_range = st.content_range.clone();
        let content_length = st.content_length.clone();
        drop(st);

        if status == 0 {
            return Err(match perf {
                Ok(_) => "无响应".to_string(),
                Err(e) => curl_error_text(&e),
            });
        }

        if status == 206 {
            // Content-Range: bytes 0-0/12345
            let total = content_range
                .as_deref()
                .and_then(|cr| cr.split('/').nth(1))
                .and_then(|s| s.trim().parse::<u64>().ok())
                .ok_or_else(|| format!("206 响应缺少 Content-Range: {content_range:?}"))?;
            if total == 0 {
                return Err("文件大小为 0".to_string());
            }
            Ok(ProbeInfo {
                total,
                total_known: true,
                supports_range: true,
            })
        } else if status == 200 {
            // 总长兜底：优先 GET 的 Content-Length，缺失时 HEAD；都拿不到则视为大小未知
            // （是否接受未知大小由引擎结合 allow_compression 决定）
            let head_total = self.head_total_blocking(&url, timeout).ok().flatten();
            let total = content_length
                .and_then(|s| s.parse::<u64>().ok())
                .or(head_total);
            if let Some(total) = total {
                if total == 0 {
                    return Err("文件大小为 0".to_string());
                }
                Ok(ProbeInfo {
                    total,
                    total_known: true,
                    supports_range: false,
                })
            } else {
                Ok(ProbeInfo {
                    total: 0,
                    total_known: false,
                    supports_range: false,
                })
            }
        } else {
            Err(format!("探测失败: HTTP {status}"))
        }
    }

    /// HEAD 请求拿 Content-Length（200 无总长的兜底）。
    fn head_total_blocking(&self, url: &str, timeout: u64) -> Result<Option<u64>, String> {
        let mut easy = self.easy(url, None).map_err(|e| format!("{e:#}"))?;
        easy.nobody(true).map_err(|e| curl_error_text(&e))?; // HEAD
        easy.timeout(Duration::from_secs(timeout.max(5)))
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
