# gkdl

Rust 多线程下载器，兼容 aria2/AriaNg 的 JSON-RPC，带 Windows 系统托盘。

## 架构

```
CLI ──┐
AriaNg ──▶ gkdl daemon
           ├── JSON-RPC 服务 (HTTP POST / JSONP / WebSocket /jsonrpc)
           ├── 任务管理 (TaskManager: GID / 状态机 / 事件广播)
           ├── 下载引擎 (libcurl 传输 / 工作窃取调度 / 慢线程检测)
           ├── 状态存储 (SQLite: 段进度 / 断点续传)
           └── 系统托盘 (打开 AriaNg / 退出)
```

## 功能

- **多线程切片下载** — 按 `split` 预切段，空闲线程窃取最慢段尾部 40%
- **慢线程检测** — 速度持续低于中位 30% 的线程被杀停，段归还重下
- **小文件内存模式** — 不超过阈值（默认 8MB）先下进内存，完成后原子落盘
- **断点续传** — 段进度存 SQLite，中断/重启后自动恢复
- **SHA-256 校验** — 完成后校验，不匹配报错（errorCode=24）
- **全局限速** — 全局令牌桶，10ms 粒度
- **多源容错** — 多 URL 主/镜像轮转，无 Range 服务器自动降级单流
- **下载后命令** — 成功后触发，可传参 + 环境变量
- **aria2 兼容 RPC** — 支持 AriaNg，HTTP / JSONP / WebSocket，token 鉴权
- **系统托盘** — 常驻后台，托盘直达 AriaNg

## 快速开始

### 构建

```powershell
cargo build --release
# 产物: target\release\gkdl.exe
```

### 直接下载

```powershell
gkdl download <url> [-o <path>] [-s 8] [--max-speed <B/s>] [--sha256 <hex>] [--memory-threshold <B>] [--no-memory]
```

### daemon（后台 + RPC + 托盘）

```powershell
gkdl daemon [--config <path>] [--port 6800] [--rpc-secret <secret>] [--no-tray]
# 首次运行自动生成 %APPDATA%\gkdl\config.yaml
```

不带子命令直接运行也进入 daemon 模式。配合 AriaNg：

```
RPC:   http://127.0.0.1:6800/jsonrpc
WS:    ws://127.0.0.1:6800/jsonrpc
状态页: http://127.0.0.1:6800/
```

### 控制任务

```powershell
gkdl add <url> [-d <dir>] [-o <name>]
gkdl pause|resume|remove <gid>
```

## 配置文件

`%APPDATA%\gkdl\config.yaml`，优先级：默认值 < 配置文件 < 命令行参数。核心字段：

```yaml
daemon:
  host: 127.0.0.1
  port: 6800
  rpc_secret: ""
  aria2ng_url: ""        # 托盘「打开 aria2ng」地址，留空不显示
download:
  split: 8
  memory_threshold: 8388608   # 小文件内存模式阈值，0=禁用，上限 64MB
  rate_limit: 0               # 全局限速 B/s，0=不限
  timeout: 30
state:
  retention_days: 7           # 已完成/失败记录保留天数，0=永久
hook:
  commands_file: "hooks.txt"  # 下载后命令文件，每行一条
  timeout_sec: 60
```

## 下载后命令

daemon 模式读 `hooks.txt`（首次自动生成），每行一条命令，下载文件路径作为最后一个参数传入；`.ps1` 用 `powershell -NoProfile -NonInteractive -File` 执行。可用环境变量 `GKDL_URL / GKDL_GID / GKDL_SIZE / GKDL_SHA256`。仅任务成功且 SHA-256 通过后触发。CLI 直连模式用 `--post-script <script>` 指定单个脚本。

## 目录结构

```
gkdl/
├── src/
│   ├── main.rs              # CLI 入口（GUI 子系统，终端启动附着父控制台）
│   ├── lib.rs
│   ├── download/            # 下载引擎
│   │   ├── curl.rs          # libcurl 传输 + IDN 转码
│   │   ├── engine.rs        # 探测 / 内存 vs 磁盘模式
│   │   ├── scheduler.rs     # 工作窃取调度
│   │   ├── worker.rs        # 单连接下载循环
│   │   ├── detector.rs      # 慢线程检测
│   │   ├── pwrite_writer.rs # 偏移写盘
│   │   ├── writer.rs        # 内存写入器
│   │   ├── rate_limit.rs    # 全局限速
│   │   ├── source.rs        # 多源容错
│   │   ├── segment.rs       # 段管理
│   │   └── config.rs        # 下载参数
│   ├── rpc/                 # aria2 兼容 JSON-RPC
│   │   ├── server.rs        # HTTP / JSONP / WebSocket
│   │   ├── methods.rs       # aria2.* 方法分发
│   │   ├── protocol.rs      # 请求解析
│   │   └── client.rs        # CLI 侧 RPC 客户端
│   └── app/                 # 应用层
│       ├── task_manager.rs  # 任务管理 / 状态机
│       ├── db.rs            # SQLite 状态库
│       ├── config.rs        # YAML 配置
│       ├── hooks.rs         # 下载后命令
│       ├── logging.rs       # 日志轮转 / 归档
│       ├── tray.rs          # 系统托盘
│       ├── hash.rs          # SHA-256
│       └── cli.rs           # 命令行参数
├── tests/                   # 集成测试（本地 Range HTTP 服务器）
├── doc/                     # 设计文档
├── build.rs
└── Cargo.toml
```

## 运行环境

- Windows（托盘依赖 winit/tray-icon）
- libcurl 静态链接（内置 curl 8.21.0），TLS 走 Schannel 系统证书，无需 CA 配置
- 中文域名（IDN）由 `idna` 转码
- SQLite 状态库（rusqlite bundled）
- 构建需要 PATH 上有 C 编译器，无需 cmake / OpenSSL

## 许可证

MIT，详见 LICENSE。

## 致谢

核心下载算法受 [aria2](https://github.com/aria2/aria2) 与 [PCL2](https://github.com/Hex-Dragon/PCL2) 启发，在此表示感谢。
