# libcurl 迁移计划

> 将下载引擎的 HTTP 客户端从 `reqwest` 迁移到 `libcurl`（Rust 绑定 `curl` crate）。
> 本项目从一开始就打算用 libcurl，`reqwest` 只是实施时的替代实现，现决定迁移并**弃用 `reqwest`**。

---

## 0. 背景与目标

- **动机**：下载库本来就该是 libcurl，`reqwest` 为临时选择，现回归本意。
- **目标**：完整移除 `reqwest`（不共存），下载引擎改用 libcurl。
- **链接方式**：静态链接（`curl-sys` 的 `static-curl` 特性，内置 curl 8.21.0 源码，Windows 自动启用 Schannel TLS）。
- **保留行为**：分段/工作窃取/慢线程三层检测/令牌桶限速/暂停续传/内存模式/SHA-256 校验/多源 failover/JSON-RPC，全部不动。

---

## 1. 依赖变更（Cargo.toml）

- **删除**：`reqwest`（含 rustls/gzip/stream/query 特性）——该库自此**弃用**，代码中不再引用。
- **新增**：`curl = { version = "0.4.50", features = ["static-curl", "http2"] }`
  - 保留默认 `ssl` 特性 → Windows 自动用 **Schannel**（系统证书库，无需 CA bundle，国内环境友好）。
  - `http2` 特性引入 `libnghttp2-sys`（纯 cc 编译，约 +19s），与 reqwest 的 h2 能力对齐。
- **新增**：`idna = "1"`（纯 Rust，URL 同款 IDN 实现，用于中文域名转码）。

**构建影响**：rustls 移除后 `aws-lc-rs` 消失，**构建不再需要 cmake**，只需 C 编译器（机器上已有）。curl-sys 首次从内置源码编译 curl 8.21.0，约 1–3 分钟，一次性。

---

## 2. curl 静态构建启用的功能

基于 `curl-sys 0.4.90` 的 `build.rs` / `Cargo.toml` 逐项核对：

- **协议**：HTTP、HTTPS（Schannel）、FILE、MQTT/MQTTS、WS/WSS；FTP/FTPS 仅在加 `protocol-ftp` 特性时才有（默认无）。
- **TLS/认证**：SSL（Schannel）、SSPI、NativeCA（Windows 证书库）。
- **传输**：HTTP/2（libnghttp2）、IPv6、Largefile（64 位偏移）、线程安全。
- **其它**：zlib（libz-sys 是 curl-sys 必选依赖，gzip/deflate 解压可用）、线程化 DNS（等价官方 "AsynchDNS"）、HSTS、alt-svc、重定向。

**与官方 curl.se/windows 包的差距及是否影响本项目**：

| 官方包功能 | curl-sys 静态版 | 影响 |
|---|---|---|
| HTTP3 / proxy-HTTP3 | ❌ 无 QUIC 后端 | 无回归（reqwest 也不支持 h3）；Range 下载用不上 |
| brotli / zstd 解压 | ❌ 未集成 | 无影响——下载引擎始终发 `Accept-Encoding: identity`，分段下载必须拿原始字节，不解压 |
| libz（官方 zlib-ng） | ✅ 普通 zlib | 等效 |
| IDN (WinIDN) | ❌ | **需自行补齐**（见 §3），用 `idna` crate 解决 |
| PSL / Kerberos / SPNEGO / libssh2 / 各类非 HTTP 协议 | ❌ | 与本项目无关 |

**结论**：curl-sys 静态构建是"纯 HTTP/HTTPS 下载器"的精简版，HTTP 域内该有的全有，缺失的非 HTTP 无关、h3 无回归，仅 IDN 需要自己补。

---

## 3. 关键决策

### 3.1 IDN（中文域名）——必须补齐

reqwest 默认的 hickory-dns resolver 内部用 `idna` 转码，所以现在中文域名（如 `http://例子.中国/文件.zip`）可用；curl 的线程化 resolver 无 IDN，会解析失败，这是**真回归**。

**方案**：加 `idna` crate（纯 Rust、无 C 依赖），在 curl 传输层加 `normalize_url()`：
- 解析主机名 → 非 ASCII 时用 `idna::domain_to_ascii` 转 punycode（`例子.中国` → `xn--fsqu00a.xn--fiqs8s`）→ 重建 URL 交给 `easy.url()`。
- 路径/查询串原样保留（curl 对 UTF-8 路径行为与 reqwest 一致，无回归）。
- 边界：带端口（`例子.中国:8080`）、IPv6 字面量（`[::1]` 跳过）、已 punycode（跳过）。
- 在 `HttpClient` 构造 Easy 处统一应用，probe 与 worker 自动覆盖。

### 3.2 brotli/zstd——不引入

分段下载器必须请求 `Accept-Encoding: identity`（Range 偏移精确 + 用户要真实文件而非压缩流），aria2/IDM 等同理。服务器只在客户端索要时才回 br/gzip 流，因此不存在"必须 br"的下载场景。reqwest 现虽开 gzip 特性，但 worker/probe 都强制发 identity，从未解压过——换 curl 零回归。**结论：不引入 brotli、不引入 zstd。**

### 3.3 为什么不用动态链接

官方 curl.se/windows DLL 虽内置 WinIDN + brotli，但：

- curl-sys 在 Windows 只认 vcpkg 探测，无对接任意预编译 DLL 的官方路径；
- curl.se 包是 mingw 构建，`.dll.a` 导入库与 MSVC 链接器不兼容，需手工 `lib /def:` 生成 `.lib` 或 hack build.rs；
- 必须随 exe 分发 `libcurl-x64.dll`（单 exe 变双文件）；版本锁定外部下载源；
- 根目录遗留的 `libcurl-x64.dll` 是陈旧版本（未使用、已 gitignore），不能用。

**结论**：静态链接 + `idna` 补齐，DLL 方案不采用。

---

## 4. 实施步骤

### 阶段 1 · 新建 `src/download/curl.rs`（HTTP 传输层）

- `HttpClient::new(&DownloadConfig)`：保存 UA/Referer/自定义头/timeout。
- `normalize_url(url) -> String`：IDN 主机转 punycode（见 §3.1）。
- `easy(&self, url, range) -> Result<Easy>`：统一设置 url(normalized)、User-Agent、Referer、自定义头、`Range`、`Accept-Encoding: identity`、`FOLLOWLOCATION(true)`+`MAXREDIRS(10)`、`CONNECTTIMEOUT(timeout.max(5))`。
- `curl::init()`：模块级 `OnceLock` 一次性初始化并保活。
- `stream_segment(&self, url, range) -> (Receiver<CurlMsg>, JoinHandle)`：
  - `enum CurlMsg { Headers{status: u32}, Data(Vec<u8>), End(Result<(), curl::Error>) }`
  - `spawn_blocking` 内跑 `Easy.transfer()`；`header_function` 在状态行与空头行处发 `Headers`（HTTP/2 无空行结束标记，必须靠状态行触发）；`write_function` 用 `tx.blocking_send`（通道满→阻塞即 TCP 背压；rx 被丢→报错中止）。
- `probe(&self, url, cancel, timeout) -> Result<ProbeInfo>`：`spawn_blocking` + `tokio::select!` 竞争 cancel；`header_function` 抓 `Content-Range`/`Content-Length`；`write_function` 返回 `Ok(0)` 立即掐断 body；忽略 write 错误，用 `response_code()` + 捕获头解析；200 时 `NOBODY` HEAD 兜底总长。

### 阶段 2 · `src/download/engine.rs`

- `build_client` → `HttpClient::new(&config)`；`probe` 换 curl 实现；`start_download` 传 `Arc<HttpClient>` 给 worker。
- 探测多源循环、超时、cancel、内存/磁盘判定、续传记录、监控落库、SHA-256 校验——全部不动。

### 阶段 3 · `src/download/worker.rs`

- `client: Arc<Client>` → `Arc<HttpClient>`。
- `download_segment` 替换请求+`bytes_stream` 部分为 `stream_segment`：
  1. 首个 recv（带读超时）收 `Headers` → 校验 200/206；`supports_range && status==200 && start>0` → 判定失败（Range 被忽略）。
  2. 循环 `tokio::time::timeout(timeout.max(5), rx.recv())` 取 `Data`，**保留 chunk 内全部现有逻辑**：cancel 检查、暂停检测（刷盘归还段）、限速 `consume`、`WriteBuffer` 写盘、`update_progress`（工作窃取截尾）、`check_slow` 慢线程。
  3. `End`：`Ok` → 按 `is_complete` 判 Complete/Failed；`Err` → Failed（`report_failure` 换源）。
- 读超时语义由"每次读后重置"变为"两次 chunk 间隔超时"，更贴合低网速；暂停/取消语义与现状一致（drop rx → 传输中止）。

### 阶段 4 · RPC 客户端与测试

- 新建 `src/rpc/client.rs`：`pub async fn call(port, secret, method, params) -> Result<serde_json::Value>`，内部 `spawn_blocking` 跑 curl Easy POST `/jsonrpc`（`post_fields_copy` + `write_function` 收响应）。`src/rpc/mod.rs` 声明子模块。
- `main.rs` 的 `rpc_client_call` 改为调它。
- `tests/rpc_integration.rs` 中 3 处 `reqwest::Client` 改用它（自带 spawn_blocking，不卡测试运行时）。

### 阶段 5 · 收尾与文档

- `src/rpc/methods.rs`：`ENABLED_FEATURES`（现 `["Async DNS","GZip","HTTPS",...]`）改为 libcurl 实际能力（如 Threaded DNS / HTTPS (Schannel) / HTTP/2）；更新 277 行附近 gzip 注释。
- 删除根目录陈旧 `libcurl-x64.dll`（已 gitignore）。
- 更新 `AGENTS.md`、`README.md`、`doc/design/实施计划.md`：依赖表、reqwest/rustls/aws-lc-rs/cmake、gzip、libcurl-x64.dll 遗留说明。
- 全量验证。

---

## 5. 关键注意点

- **错误文案兼容** `map_error_code`（`src/app/task_manager.rs`，按关键词 超时/timeout、404/not found、网络/connect/dns 匹配）：curl 错误转 anyhow 时保留关键词（如 CURLE_OPERATION_TIMEDOUT → "连接/读超时(timeout)"、CURLE_COULDNT_RESOLVE_HOST → "无法解析主机(dns error)"、HTTP 404 → "HTTP 404 Not Found"）。
- 探测/下载始终发 `Accept-Encoding: identity`，不引入 brotli/zstd 解压。
- HTTP/3 无回归（reqwest 本就不支持）。
- 首次构建 curl-sys 编译 curl 源码较慢，一次性。

---

## 6. 验证

- `cargo test`（64 单测 + 30 集成全过，重点：`multi_thread_range_download_matches`、`rate_limit_slows_download`、`interrupted_download_resumes_from_db`、`multi_source_failover_skips_bad_url`）。
- `cargo clippy --all-targets -- -D warnings`、`cargo fmt --check`。
- 新增 IDN 单测：`normalize_url("http://例子.中国/文件.zip")` → punycode 主机；含端口/已 punycode/IPv6 用例。
