# gkdl — Cool-GK Downloader

> **GKDL** 是 **Cool-GK Downloader** 的缩写，是一个用 Rust 编写的多线程下载器。

> ⚠️ 个人项目，自用为主，不保证可用性；本仓库仅用于存放代码，不开放 issue / PR。

Rust 实现的多线程下载器（Windows 可用），带 **aria2/AriaNg 兼容 JSON-RPC** 服务与 **系统托盘**。

## 特性

- **多线程 Range 切片下载**：按 `split` 预切，快线程完成自己的段后自动**工作窃取**最慢段的尾部 40%
- **慢线程三层检测**：低于中位速度 30% → 连续确认 → P10 尾部确认 → 杀停归还段
- **断点续传**：`<文件名>.gkdl` 控制文件每秒保存段进度，中断后自动续传
- **SHA-256 校验**：完成后流式校验，失败即报错（`errorCode=24`）
- **令牌桶限速**：全局共享桶，10ms 粒度，最多攒 1 秒
- **多源容错**：源轮转、失败累计禁用、探测失败源自动跳过、无 Range 服务器降级单流
- **下载后脚本**：仅成功后触发，60s 超时，传文件路径 + 环境变量
- **aria2/AriaNg 兼容 JSON-RPC**：HTTP POST / JSONP / WebSocket，`system.multicall`，CORS，token 鉴权
- **常驻 daemon**：YAML 配置（`%APPDATA%\gkdl\config.yaml`）、滚动日志 + 启动归档（tar.xz→gz→tar）、系统托盘、内嵌状态页

## 构建

```powershell
cargo build --release
# 产物: target\release\gkdl.exe
```

## 用法

### 直连下载

```powershell
gkdl <url> [-o <path>] [-s 8] [--max-speed <B/s>] [--sha256 <hex>] [--min-split-size <B>] [--retries <n>] [--post-script <script>]
```

示例：

```powershell
gkdl https://example.com/file.zip -s 8 --max-speed 1048576 --sha256 e5b8...cde9
```

### daemon（常驻后台 + JSON-RPC + 托盘）

```powershell
gkdl daemon [--config <path>] [--host 127.0.0.1] [--port 6800] [--rpc-secret <secret>] [--no-tray]
```

首次运行自动生成 `%APPDATA%\gkdl\config.yaml`。启动后可用 **AriaNg** 连接：

```
RPC 地址: http://127.0.0.1:6800/jsonrpc
WebSocket: ws://127.0.0.1:6800/jsonrpc
状态页:   http://127.0.0.1:6800/
```

### 控制 daemon

```powershell
gkdl add <url> [--dir <dir>] [--out <name>] [--port 6800]
gkdl pause|resume|remove <gid> [--port 6800]
```

## 配置文件

```yaml
daemon:
  host: 127.0.0.1
  port: 6800
  rpc_secret: ""
  no_tray: false
download:
  split: 8
  min_split_size: 262144
  max_speed: 0          # 0=不限
  retries: 3
  timeout: 30
log:
  level: info
  file: logs/gkdl.log
  max_size_mb: 10
  backup_count: 5
  archive_enabled: true
  retention_days: 90
hook:
  post_download_script: ""   # 下载成功后执行的脚本
  timeout_sec: 60
```

优先级：默认值 < 配置文件 < CLI 参数。

## 下载后脚本

仅任务成功完成且 SHA-256 通过后运行（失败/校验失败不触发）。退出码仅记日志。

- 参数：`<script> <文件绝对路径>`
- 环境变量：`GKDL_URL`、`GKDL_GID`、`GKDL_SIZE`、`GKDL_SHA256`
- `.ps1` 用 `powershell -ExecutionPolicy Bypass -File` 执行，其余直接执行

## 测试

```powershell
cargo test
```

包含单元测试（切片/窃取/慢线程/令牌桶）与集成测试（本地 Range HTTP 服务器：多线程一致性、多源降级、单流回退、SHA-256、限速、断点续传、hook 触发、RPC 兼容性）。

## 设计文档

见 `doc/`：`design/` 下为内部设计（多线程下载算法、慢线程处理、实施计划），`reference/` 下为参考与源码分析（aria2 RPC 兼容规范、aria2 源码分析、PCL2 算法分析）。

## 许可证

MIT — 详见 `LICENSE` 文件。本项目仅供学习参考，使用/分发带来的任何后果由使用者自行承担。
