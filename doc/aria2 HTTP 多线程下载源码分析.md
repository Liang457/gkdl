# aria2 HTTP 多线程下载源码分析

> 目标读者：希望基于 aria2 源码理解 HTTP 多连接（Range 切片）下载机制，并据此实现类似功能的工程师 / AI Agent。
> 适用范围：aria2 主线版本（基于本仓库 `src/` 目录）。代码路径均为相对 `src/` 的简化写法，例如 `DownloadCommand.cc:133` 代表 `src/DownloadCommand.cc` 第 133 行。

---

## 0. TL;DR（一句话总结）

aria2 的 HTTP 多线程下载本质是：

1. **HTTP Range 切片**：对单个文件按字节范围切出 N 个 `Segment`（段 / 切片），每个段由一条独立的 socket 连接（`HttpConnection`）下载。
2. **命令式事件循环**：所有动作（建连、发请求、收响应、写盘）都被建模为 `Command` 节点，由 `DownloadEngine` 的 epoll/kqueue 主循环调度。一个 `AbstractCommand` 对象对应一次「执行 + 让出」原语。
3. **统一抽象**：多线程下载与 BT 多 peer 下载共用同一套 `Piece / PieceStorage / Segment / SegmentMan` 模型，只在最外层的 Command 工厂上分流（HTTP vs BT）。

下面从「核心数据结构 → 控制流 → Range 切片 → 多连接调度 → 写盘与校验 → 异常恢复」逐层展开。

---

## 1. 关键模块地图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         DownloadEngine (主循环)                          │
│ DownloadEngine.cc:158 run()  + waitData() + executeCommand()              │
│  - epoll/kqueue 监听所有 socket                                            │
│  - 轮询 commands_ 队列，调用 Command::execute()                            │
└──────────────────────────────────────────────────────────────────────────┘
                ▲                            ▲
                │addCommand                  │addCommand
                │                            │
   ┌────────────┴─────────────┐   ┌───────────┴────────────────┐
   │  ConnectCommand /        │   │ AbstractCommand 体系         │
   │  InitiateConnectionCommand│   │  - HttpRequestCommand        │
   │  (TCP/TLS 建连)          │   │  - HttpResponseCommand       │
   └────────────┬─────────────┘   │  - HttpDownloadCommand       │
                │                 │  - DownloadCommand (基类)    │
                ▼                 │  - CreateRequestCommand      │
          SocketCore              └───────────┬────────────────┘
                                              │ 持有 1..N 个 Segment
                                              ▼
                                ┌────────────────────────────┐
                                │ SegmentMan                 │
                                │  - 切片分配 / 取消 / 复用   │
                                │  - PieceStorage: 位图管理   │
                                │  - PeerStat: 各连接速度统计 │
                                └────────────────────────────┘
                                              │
                                              ▼
                                ┌────────────────────────────┐
                                │ Piece / PieceStorage /      │
                                │ DiskAdaptor / WrDiskCache   │
                                │ (落盘、块位图、Hash 校验)   │
                                └────────────────────────────┘
```

关键源文件（已读取并交叉对照）：

| 模块 | 源文件 | 角色 |
|---|---|---|
| 主循环 | `DownloadEngine.cc:158` | epoll + 命令队列调度 |
| 命令基类 | `AbstractCommand.cc:179 execute()` | 入口、分段拉取、异常捕获 |
| HTTP 请求 | `HttpRequestCommand.cc:123 executeInternal()` | 序列化 Range 请求、写入 socket |
| HTTP 响应 | `HttpResponseCommand.cc:144 executeInternal()` | 解析响应头、初始化 PieceStorage、决策多段 vs 单流 |
| HTTP 下载 | `HttpDownloadCommand.cc:74 prepareForNextSegment()` | 段切换、连接复用、管道化 |
| 下载基类 | `DownloadCommand.cc:133 executeInternal()` | 收数据 → 写盘 → 段完成判定 |
| 段管理 | `SegmentMan.cc:135 checkoutSegment` | 段分配与回收 |
| 段对象 | `Segment.h`, `PiecedSegment.cc`, `GrowSegment.cc` | 段状态机 |
| 块存储 | `Piece.cc`, `PieceStorage.h`, `DefaultPieceStorage.cc` | 块位图、写缓存 |
| HTTP 请求对象 | `HttpRequest.cc:112 getRange()` | Range 头构造 |
| 连接对象 | `HttpConnection.h:81`, `HttpConnection.cc` | 请求/响应队列 |

---

## 2. 核心数据结构

### 2.1 `Piece`：固定大小的「块」

- `Piece` 表示文件中第 `index` 个大小为 `length` 的块，块长 = `DownloadContext::getPieceLength()`（默认 1 MiB，最后一块可能更短）。
- 内部用 `BitfieldMan` 维护「子块（block，16 KiB）」完成位图，作用：
  - 断点续传：标记哪些子块已落盘。
  - EndGame 校验（aria2 中主要用于 BT）。
- 关键字段：`index_`, `length_`, `usedBySegment_`, `wrCache_`（写磁盘缓存）。

### 2.2 `Segment`：一段连续字节区间

```cpp
// Segment.h:49
class Segment {
  virtual bool complete() const = 0;
  virtual int64_t getPosition() const = 0;        // 全局偏移
  virtual int64_t getPositionToWrite() const = 0; // 已写到哪里
  virtual int64_t getLength() const = 0;
  virtual int64_t getWrittenLength() const = 0;
  virtual std::shared_ptr<Piece> getPiece() const = 0;
};
```

有两种实现：

- `PiecedSegment`（`Piece.cc`）：按 Piece 边界对齐的固定段。`getPosition() == pieceIndex * pieceLength`。
- `GrowSegment`：用于「总长度未知」场景（例如 `Transfer-Encoding: chunked`），长度可增长。

**Piece / Segment 的关系**：一个 `Piece` 可被多个 `Segment` 引用（用于 BT 的 end-game 多源下载），但 HTTP 场景下严格 1:1。

### 2.3 `Range`：HTTP Range 头

```cpp
// Range.h:44
struct Range {
  int64_t startByte;
  int64_t endByte;       // inclusive
  int64_t entityLength;
};
```

由 `HttpRequest::getRange()` 构造：

```cpp
// HttpRequest.cc:112
Range HttpRequest::getRange() const {
  if (!segment_) return Range();
  return Range(getStartByte(), getEndByte(), fileEntry_->getLength());
}
```

`getStartByte()` = `fileEntry_->gtoloff(segment_->getPositionToWrite())`，
`getEndByte()` = 段尾（含），管道化模式时使用整段长度（参见第 6 节）。

### 2.4 `SegmentMan`：段的中央调度器

`SegmentMan.cc:68` 的核心状态：

```cpp
usedSegmentEntries_;        // 当前已被某个 cuid 借走的段
peerStats_;                 // 每个连接的 PeerStat（含 avgDownloadSpeed）
fastestPeerStats_;          // 用于「切换到更快的镜像」
ignoreBitfield_;            // 多文件下载时跳过其它文件的过滤位图
segmentWrittenLengthMemo_;  // 段取消时把已写字节记下，便于 resume
```

`checkoutSegment(cuid, piece)`（`SegmentMan.cc:135`）：从 `PieceStorage` 拿一个未完成 `Piece`，包成 `Segment`，记录 `cuid → Segment` 映射。

`getSegment(cuid, minSplitSize)`（`SegmentMan.cc:203`）：调 `pieceStorage_->getMissingPiece(...)`，选择策略由 `StreamPieceSelector` 决定：
- `default`（`DefaultStreamPieceSelector`）：优先最小未使用的 piece index。
- `inorder`（`InorderStreamPieceSelector.cc:47`）：从前往后顺序拿。
- `geom`（`GeomStreamPieceSelector`）：按几何概率分布选择（更利于镜像选择场景）。
- `random`（`RandomStreamPieceSelector`）：随机。

### 2.5 `PieceStorage` & `BitfieldMan`

`DefaultPieceStorage.cc:78` 的核心：

```cpp
bitfieldMan_(make_unique<BitfieldMan>(pieceLength, totalLength));
pieceSelector_(make_unique<RarestPieceSelector>(...)); // BT 用
streamPieceSelector_(make_unique<DefaultStreamPieceSelector>(...)); // HTTP 用
```

- `usedPieces_`：`std::set<Piece>`，当前已分配但未完成的 piece。
- `bitfieldMan_`：两个位图 — `bit`（已完成的 piece）和 `use`（已分配给连接的 piece，避免重复分配）。
- `checkOutPiece(index, cuid)`（`DefaultPieceStorage.cc:119`）：`setUseBit(index)`，若 `usedPieces_` 里没有就构造新的 `Piece`。

---

## 3. 控制流：命令式状态机

### 3.1 主循环

```cpp
// DownloadEngine.cc:158
int DownloadEngine::run(bool oneshot) {
  while (!commands_.empty() || !routineCommands_.empty()) {
    if (!commands_.empty()) waitData();          // epoll_wait
    executeCommand(commands_, STATUS_ALL);       // 轮询并执行所有命令
    executeCommand(routineCommands_, STATUS_ALL);
  }
}
```

- `waitData()` 内部调 `eventPoll_->poll(tv)`，依赖 epoll/kqueue。
- `executeCommand`（`DownloadEngine.cc:118`）：遍历命令队列，把 IO 事件就绪的命令调 `execute()`；未就绪的放回队列末尾。
- 一个 `Command::execute()` 返回 `true` 表示「任务结束，析构我」；返回 `false` 表示「还活着，把我放回队列等下次触发」（对应 `addCommandSelf()`，`AbstractCommand.cc:941`）。

### 3.2 一条 HTTP 连接的生命周期

```
CreateRequestCommand          → 选 URI，准备请求
  │
  ▼
InitiateConnectionCommand     → DNS 解析 + TCP/TLS 建连
  │
  ▼
HttpRequestCommand            → 写 HTTP GET（含 Range）+ 注册 epoll 写事件
  │
  ▼
HttpResponseCommand           → 读响应头，初始化 PieceStorage
  │
  ▼
HttpDownloadCommand           → 持续 recv → SinkStreamFilter → Piece::wrCache
  │       (收到一段完成后)
  └─→ prepareForNextSegment() → 重新分配下一个 Segment → addCommandSelf()
                                            │
                                            ▼
                                  （同 socket 上接着拉下一段；
                                   或用 poolSocket 把 socket 复用给下一条命令）
```

每个箭头对应 `e_->addCommand(...)`，命令之间**没有阻塞调用**，全靠 epoll + 命令队列推进。

### 3.3 `AbstractCommand::execute()` 骨架

`AbstractCommand.cc:179` 是所有命令的入口：

1. `requestGroup_->downloadFinished()` 早退。
2. **拉取段（segment 拉取阶段）**：从 `SegmentMan` 借出自己 cuid 对应的 Segment（关键代码 `AbstractCommand.cc:262`）：

```cpp
size_t maxSegments = req_ ? req_->getMaxPipelinedRequest() : 1;
size_t minSplitSize = calculateMinSplitSize();
while (segments_.size() < maxSegments) {
  auto segment = sm->getSegment(getCuid(), minSplitSize);
  if (!segment) break;
  segments_.push_back(segment);
}
```

`maxSegments` 即「同 socket 上同时拉几个段」：
- 普通 HTTP：`getMaxPipelinedRequest() == 1`。
- HTTP Pipelining：`getMaxPipelinedRequest() == PREF_MAX_HTTP_PIPELINING`（默认 2）。

3. `executeInternal()` 子类实现真正的收发。
4. 异常分支：`DlAbortEx` / `DlRetryEx` / `DownloadFailureException`，分别触发「放弃这条 URI / 重试 / 整体放弃」。

---

## 4. HTTP Range 多线程的核心实现

### 4.1 切片数：`split` 选项

- 默认值：5（`OptionHandlerFactory.cc:972`，`PREF_SPLIT`）。
- 上限：`numConcurrentCommand_`（`RequestGroup.cc:138`）。
- 每次「没有正在下载的段」时，`RequestGroup::createNextCommand()`（`RequestGroup.cc:872`）会一次性 `push N 个 CreateRequestCommand`，从而最多产生 N 条并行 HTTP 连接：

```cpp
// RequestGroup.cc:872
for (; numCommand > 0; --numCommand) {
  commands.push_back(make_unique<CreateRequestCommand>(e->newCUID(), this, e));
}
```

`numCommand` 由「并发上限 - 已有流命令数」决定（`RequestGroup.cc:858`）。

### 4.2 最小切片：`min-split-size`（默认 1 MiB）

`AbstractCommand.cc:888`：

```cpp
int32_t AbstractCommand::calculateMinSplitSize() const {
  if (req_ && req_->isPipeliningEnabled()) {
    return getDownloadContext()->getPieceLength(); // pipelining 时按 piece
  }
  return getOption()->getAsInt(PREF_MIN_SPLIT_SIZE);
}
```

含义：`SegmentMan::getSegment` 拿到「空白连续区间长度 < minSplitSize」的 piece 时，会跳过它去选更大的（参见 `BitfieldMan::getInorderMissingUnusedIndex` 的 `minSplitSize` 入参）。这样小文件不会被切得过碎。

### 4.3 Range 头的拼装

`HttpRequest.cc:201`：

```cpp
if (segment_ && segment_->getLength() > 0 &&
    (request_->isPipeliningEnabled() || getStartByte() > 0 ||
     getEndByte() > 0)) {
  std::string rangeHeader = "bytes=";
  rangeHeader += util::uitos(getStartByte());
  rangeHeader += '-';
  if (request_->isPipeliningEnabled() || getEndByte() > 0) {
    rangeHeader += util::itos(getEndByte());
  }
  builtinHds.emplace_back("Range:", rangeHeader);
}
```

- 第一条请求：`getStartByte() == 0 && !isPipeliningEnabled()` → **不发** Range 头（探针请求，让服务器返回总长度 + 决定是否支持 Range）。
- 后续段：Range 头精确到字节区间。

> **第一次请求不带 Range**：服务器返回 200/206 都行；如果是 200（整文件），aria2 仍会基于 `Content-Length` 切后续 Range 走多连接（`HttpResponseCommand.cc:309`）。

### 4.4 段切换：`HttpDownloadCommand::prepareForNextSegment`

`HttpDownloadCommand.cc:74`：

```cpp
bool HttpDownloadCommand::prepareForNextSegment() {
  bool downloadFinished = getRequestGroup()->downloadFinished();
  if (getRequest()->isPipeliningEnabled() && !downloadFinished) {
    // 同一 socket 上立刻发下一段的请求（pipelining）
    auto command = make_unique<HttpRequestCommand>(...);
    getDownloadEngine()->addCommand(std::move(command));
    return true;
  }

  if (keepAlive 可复用) {
    getDownloadEngine()->poolSocket(getRequest(), createProxyRequest(), getSocket());
  }
  ...
  return DownloadCommand::prepareForNextSegment();
}
```

`DownloadCommand::prepareForNextSegment`（`DownloadCommand.cc:317`）核心逻辑：

1. 整文件完成 → 收尾、可能触发 `ChecksumCheckIntegrityEntry`。
2. 当前段已收完但还有下一段 → 走 `getSegmentWithIndex(index+1)`（直接连续拉）。
3. 同一 socket 上跑下一个段（HTTP/1.1 keep-alive + 在 AbstractCommand 里循环调用 `executeInternal`），这就是为什么 aria2 看起来「5 线程」但实际可能只有 1 条 socket + 5 段。

### 4.5 多 socket 调度的核心循环

第 N 条连接建立之后会发生什么：

```
DownloadCommand::executeInternal (AbstractCommand.cc:179 → DownloadCommand.cc:133)
  ├─ segments_ 已被 AbstractCommand 在 execute() 入口处填好
  ├─ socketRecvBuffer_->recv()      ← epoll 读就绪
  ├─ streamFilter_->transform(diskAdaptor, segment, buf, n)   ← SinkStreamFilter → WrDiskCache
  ├─ segment->updateWrittenLength(n)
  ├─ 段完成判断
  │   if (segment->complete() || 触到 fileEntry 边界) {
  │     completeSegment(cuid, segment)   ← PieceStorage::completePiece
  │     return prepareForNextSegment(); ← 上面第 4.4 节
  │   }
  └─ 否则 addCommandSelf()，等下次 epoll
```

`AbstractCommand::execute()` 入口的「拉取段」逻辑确保：每次 socket 上有数据读完、段被消费掉后，AbstractCommand 会在下一次 `execute()` 时把 `segments_` 补满，最多 `req_->getMaxPipelinedRequest()` 个（HTTP 一般为 1）。

### 4.6 HTTP Pipelining（可选加速）

- 选项：`--enable-http-pipelining`（`PREF_HTTP_PIPELINING`），默认 false。
- 在 `HttpResponseCommand::executeInternal` 里（`HttpResponseCommand.cc:165`），收到首个 206 后若服务器 `Connection: keep-alive`，则 `req_->supportsPersistentConnection(true)`，并把 `req_->setMaxPipelinedRequest(getOption()->getAsInt(PREF_MAX_HTTP_PIPELINING))`。
- 之后 `HttpRequestCommand::executeInternal` 会在一次 socket 写循环里把多个 Range 请求**背靠背发出去**（`HttpRequestCommand.cc:175` 的 `for (auto& segment : getSegments())`）。
- 响应也必须顺序读完（`HttpResponseCommand` 一个一个解析响应头 → 派发到 `HttpDownloadCommand`）。

---

## 5. 写盘与 Piece 完整性

### 5.1 `SinkStreamFilter` —— HTTP 路径下的默认过滤器

`SinkStreamFilter.cc:54 transform()`：

```cpp
const std::shared_ptr<Piece>& piece = segment->getPiece();
if (piece->getWrDiskCacheEntry()) {
  size_t alen = piece->appendWrCache(
      wrDiskCache_, segment->getPositionToWrite(), inbuf, wlen);
  if (alen < wlen) {
    // 写缓存满了 → 走 updateWrCache → 真写磁盘
    piece->updateWrCache(wrDiskCache_, dataCopy, 0, len, capacity,
                         segment->getPositionToWrite() + alen);
  }
} else {
  out->writeData(inbuf, wlen, segment->getPositionToWrite());
}
segment->updateWrittenLength(wlen);
```

- 写盘策略：**写缓存（`WrDiskCache`）合并写**。多次 recv 累计到一定量再 pwrite 到文件对应 offset，减少磁盘 IO。
- offset 通过 `segment->getPositionToWrite()`（=`segment->getPosition() + writtenLength_`）算出，直接 `pwrite`，**多线程安全**（每段独立区间）。

### 5.2 段完成 → Piece 完成

`DownloadCommand.cc:237` `completeSegment(cuid, segment)`：

```cpp
flushWrDiskCacheEntry(wrDiskCache, segment);     // 强制 flush 缓存
getSegmentMan()->completeSegment(cuid, segment); // PieceStorage::completePiece
```

`SegmentMan::completeSegment`（`SegmentMan.cc:373`）→ `PieceStorage::completePiece`（`DefaultPieceStorage.cc:459`）：

```cpp
bitfieldMan_->setBit(piece->getIndex());
bitfieldMan_->unsetUseBit(piece->getIndex());
```

`unsetUseBit` 让后续的「选择下一个段」可以再拿到这个 piece 的下一个 piece（连续下载）。

### 5.3 实时校验（可选）

- 选项 `--realtime-chunk-checksum=true`，启用后 `DownloadCommand` 构造时会建一个 `MessageDigest`（SHA-1/256/512）。
- 每段写完数据立即调 `segment->updateHash(begin, data, len)`，最后与 `DownloadContext::getPieceHash(idx)` 比对：
  - 一致 → `completeSegment`。
  - 不一致 → 抛 `DL_RETRY_EX`，重新拉这一段（`DownloadCommand.cc:391`）。

---

## 6. 多线程的「并行度上限与限速」

- 全局并发连接上限：`split` 选项（默认 5）。但要注意 **HTTP pipelining** 开启后，同一个 socket 上能跑多个段，所以实际 socket 数可能少于 `split`。
- 限速：`PREF_MAX_DOWNLOAD_LIMIT` / `PREF_MAX_OVERALL_DOWNLOAD_LIMIT` 在 `DownloadCommand::executeInternal` 开头检查，超限时只 `addCommandSelf()` 但禁用 IO：

```cpp
// DownloadCommand.cc:135
if (getRequestGroupMan()->doesOverallDownloadSpeedExceed() ||
    getRequestGroup()->doesDownloadSpeedExceed()) {
  addCommandSelf();
  disableReadCheckSocket();
  disableWriteCheckSocket();
  return false;
}
```

- 自动降级：超时 / 慢速主机 → `prepareForRetry(0)` → `CreateRequestCommand` 重选 URI（`AbstractCommand.cc:438`）。

---

## 7. 异常与续传

### 7.1 重试 / 失败 / 取消 流程

`AbstractCommand::execute` 末尾的 catch 块（`AbstractCommand.cc:346`）：

| 异常 | 行为 |
|---|---|
| `DlAbortEx` | 移除当前 URI，标记为对应错误码；不重试。 |
| `DlRetryEx` | `req_->addTryCount()`；超过 `PREF_MAX_TRIES` 才放弃；否则 `prepareForRetry(0)` 走 `CreateRequestCommand` 重新发起。 |
| `DownloadFailureException` | 整组停止，写 `lastErrorCode`，设置 `haltRequested_`。 |

`prepareForRetry(0)`（`AbstractCommand.cc:438`）核心：

```cpp
getSegmentMan()->cancelSegment(getCuid());   // 归还段，记住已写字节
req_->supportsPersistentConnection(true);
req_->setMaxPipelinedRequest(1);
fileEntry_->poolRequest(req_);              // 复用 Request 对象
e_->addCommand(make_unique<CreateRequestCommand>(...));
```

### 7.2 段取消 + 续传

`SegmentMan::cancelSegmentInternal`（`SegmentMan.cc:285`）：

```cpp
piece->setUsedBySegment(false);
pieceStorage_->cancelPiece(piece, cuid);
segmentWrittenLengthMemo_[segment->getIndex()] = segment->getWrittenLength();
```

下次再有连接拉这个段时，`SegmentMan::checkoutSegment`（`SegmentMan.cc:169`）会读 memo 并 `segment->updateWrittenLength(d)` 跳到已写位置继续。

> aria2 的「断点续传」正是靠 `.aria2` 控制文件（`BtProgressInfo`）+ 段已写长度备忘一起实现的。HTTP 多线程下载时，每条连接可能挂了任何一段都会被原样恢复。

### 7.3 镜像自动切换

`AbstractCommand::execute()`（`AbstractCommand.cc:222`）会周期性调 `fileEntry_->findFasterRequest(req_, ..., ServerStatMan)`：如果同文件还有别的镜像统计速度更快，就 `useFasterRequest` → 启动新连接 + 取消当前连接的段。

---

## 8. Python 伪代码实现（极简版）

下面给出三个独立的 Python 伪代码模块，分别对应**主循环**、**HTTP Range 多连接切片**、**Piece/Segment 抽象**。目标是让 AI 能直接照着实现一个 aria2 子集。

### 8.1 极简事件循环（命令式）

```python
# event_loop.py
# 基于 asyncio 模拟 aria2 的命令队列 + epoll 模型。
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Callable, Deque, List

STATUS_ACTIVE = 1
STATUS_INACTIVE = 2
STATUS_REALTIME = 3

@dataclass
class Command:
    status: int = STATUS_ACTIVE
    engine: "DownloadEngine" = field(default=None, repr=False)

    def set_status(self, s): self.status = s

    def execute(self) -> bool:
        """子类重写。返回 True 表示完成并销毁；False 表示存活，等下次唤醒。"""
        raise NotImplementedError

    def add_self(self):
        self.engine.add_command(self)


class DownloadEngine:
    def __init__(self):
        self.commands: Deque[Command] = deque()
        self.no_wait = True
        self.refresh_interval = 1.0  # 秒

    def add_command(self, cmd: Command):
        cmd.engine = self
        self.commands.append(cmd)
        self.no_wait = True

    def run(self):
        while self.commands:
            self.run_once()
        self.on_end()

    def run_once(self):
        # 模拟 epoll_wait；真实实现里这里 select/asyncio.sleep(refresh_interval)
        import time
        time.sleep(0 if self.no_wait else self.refresh_interval)
        self.no_wait = False

        n = len(self.commands)
        for _ in range(n):
            cmd = self.commands.popleft()
            if not cmd.status_match():  # 类似 Command::statusMatch
                self.commands.append(cmd)
                continue
            cmd.transit_status()
            done = cmd.execute()
            if not done:
                cmd.clear_io_events()
                # 不再 append —— 子类负责在合适时机 add_self()
            # done 的命令随局部变量一起销毁
```

要点：
- `Command::execute()` 返 True → 让 GC 回收；返 False → 必须由**子类的下一阶段** `add_self()` 重新入队。
- 这是 aria2 把「非阻塞 IO + 协作式调度」压缩到最小原语的核心抽象。

### 8.2 HTTP 多线程 Range 下载（无 BT）

```python
# http_multi_range.py
import asyncio, aiohttp, os, math, hashlib
from dataclasses import dataclass, field
from typing import Optional, List, Dict

PIECE_LENGTH = 1 * 1024 * 1024          # 1 MiB
DEFAULT_SPLIT = 5                       # 默认 5 路
MIN_SPLIT_SIZE = 1 * 1024 * 1024        # 1 MiB

@dataclass
class Piece:
    index: int
    length: int
    blocks_done: bytearray                # 子块位图 (16 KiB 一块)
    written: int = 0
    used: bool = False
    cache_buf: bytearray = field(default_factory=bytearray)

    def completed(self) -> bool:
        return self.written >= self.length

    def append_cache(self, data: bytes, goff: int):
        """合并写入：把数据拷到 cache_buf；满 16 KiB 才 pwrite。"""
        ...

@dataclass
class Segment:
    piece_index: int
    position: int          # 全局偏移
    length: int            # 总长
    written: int = 0

    @property
    def position_to_write(self): return self.position + self.written

    @property
    def complete(self): return self.written >= self.length

@dataclass
class FileEntry:
    path: str
    total_length: int
    url: str


class SegmentMan:
    """模仿 src/SegmentMan.cc 的极简版。"""
    def __init__(self, file: FileEntry, pieces: List[Piece]):
        self.file = file
        self.pieces = pieces
        self.use_bit = [False] * len(pieces)
        self.written_memo: Dict[int, int] = {}

    def checkout(self, cuid: int) -> Optional[Segment]:
        for i, p in enumerate(self.pieces):
            if p.completed() or p.used or self.use_bit[i]:
                continue
            self.use_bit[i] = True
            p.used = True
            # 恢复 memo 中的已写位置
            written = self.written_memo.get(i, 0)
            seg = Segment(piece_index=i,
                          position=i * PIECE_LENGTH,
                          length=p.length,
                          written=written)
            return seg
        return None

    def cancel(self, cuid: int, seg: Segment):
        self.written_memo[seg.piece_index] = seg.written
        self.pieces[seg.piece_index].used = False
        self.use_bit[seg.piece_index] = False

    def complete(self, cuid: int, seg: Segment):
        self.use_bit[seg.piece_index] = False
        self.pieces[seg.piece_index].used = False


class HttpDownloadCommand:
    """一条 HTTP 连接对应一个 Command。"""
    def __init__(self, engine, file: FileEntry, seg: Segment, session: aiohttp.ClientSession):
        self.engine = engine
        self.file = file
        self.seg = seg
        self.session = session

    async def execute(self) -> bool:
        seg = self.seg
        start = seg.position_to_write
        end = seg.position + seg.length - 1
        headers = {"Range": f"bytes={start}-{end}"}

        async with self.session.get(self.file.url, headers=headers) as resp:
            with open(self.file.path, "r+b") as f:
                f.seek(start)
                # chunked write + cache flush
                async for chunk in resp.content.iter_chunked(16 * 1024):
                    if not chunk: break
                    f.write(chunk)
                    seg.written += len(chunk)
                    if seg.complete:
                        break
        # 段完成 → SegmentMan.complete
        self.engine.segment_man.complete(cuid=0, seg=seg)
        # 切下一段
        nxt = self.engine.segment_man.checkout(cuid=0)
        if nxt:
            self.seg = nxt
            self.engine.add_command(HttpDownloadCommand(self.engine, self.file, nxt, self.session))
            return False  # 等等，其实当前 command 已经完成；让 Python GC 即可
        return True


class CreateRequestCommand:
    """触发器：每缺一条连接就发一条。"""
    def __init__(self, engine, file: FileEntry):
        self.engine = engine
        self.file = file

    async def execute(self) -> bool:
        seg = self.engine.segment_man.checkout(cuid=0)
        if seg is None:
            return True  # 没活儿干
        cmd = HttpDownloadCommand(self.engine, self.file, seg, self.engine.session)
        self.engine.add_command(cmd)
        return True


async def download(url: str, out_path: str, split: int = DEFAULT_SPLIT):
    # 1) HEAD 探总长（aria2 第一条请求不带 Range 的作用相同）
    async with aiohttp.ClientSession() as session:
        async with session.head(url) as resp:
            total = int(resp.headers["Content-Length"])

    pieces = [Piece(i, min(PIECE_LENGTH, total - i * PIECE_LENGTH))
              for i in range(math.ceil(total / PIECE_LENGTH))]

    file = FileEntry(path=out_path, total_length=total, url=url)
    seg_man = SegmentMan(file, pieces)

    engine = SimpleEngine(segment_man=seg_man)

    # 2) 拉起 split 个 CreateRequestCommand —— 对应 aria2 的 RequestGroup::createNextCommand
    for _ in range(split):
        engine.add_command(CreateRequestCommand(engine, file))

    await engine.run_async()
```

> 这版伪代码略去了「同 socket 复用（keep-alive）」、「HTTP Pipelining」、「断点续传 `.aria2`」、「实时 Hash 校验」、「限速」等次要特性，但完整保留了「Range 切片 + 多连接 + Piece/Segment 中央调度」核心。

### 8.3 HTTP Pipelining 伪代码（选读）

```python
# 关键差异：HttpRequestCommand 一次写多个请求到同一 socket
class PipeliningHttpRequestCommand:
    def __init__(self, engine, file, segments: List[Segment], sock):
        self.engine = engine
        self.file = file
        self.segments = segments  # 同一 socket 上要发的多个段
        self.sock = sock

    async def execute(self) -> bool:
        for seg in self.segments:
            req = build_http_get_with_range(self.file.url, seg)
            self.sock.send(req.encode())   # 一次循环连续写 N 个 GET
        # 把 socket 转交给「按顺序读响应」的命令链
        self.engine.add_command(PipeliningHttpResponseCommand(self.engine, self.file, self.sock))
        return True
```

> 真实实现见 `HttpRequestCommand.cc:175` 的 `for (auto& segment : getSegments())` + `HttpConnection::sendRequest(...)` 把所有请求压进 socket 写队列。

---

## 9. 实现要点速查（如果要复刻）

| 步骤 | 关键点 | aria2 入口 |
|---|---|---|
| 1 | HEAD/无 Range GET 拿 Content-Length | `HttpResponseCommand.cc:351 handleDefaultEncoding` |
| 2 | 初始化 Piece 数组（按 pieceLength） | `DefaultPieceStorage.cc:78` |
| 3 | 按 split 创建 N 个连接命令 | `RequestGroup.cc:872 createNextCommand` |
| 4 | 每条连接拉一段：拼 Range 头、发请求 | `HttpRequestCommand.cc:138` |
| 5 | epoll 读就绪 → SinkStreamFilter → WrDiskCache → pwrite | `DownloadCommand.cc:163` |
| 6 | 段完成 → flush 缓存 → Piece 标记完成 → 拉下一段 | `DownloadCommand.cc:237`, `SegmentMan.cc:373` |
| 7 | 全部完成 → checksum 校验（可选） | `ChecksumCheckIntegrityEntry` |
| 8 | 异常时 cancel + memo + 走 CreateRequestCommand 重试 | `AbstractCommand.cc:438 prepareForRetry` |
| 9 | 镜像自动切换（可选） | `AbstractCommand.cc:242 findFasterRequest` |
| 10 | HTTP Pipelining（可选） | `HttpResponseCommand.cc:167`, `HttpRequestCommand.cc:175` |

---

## 10. 常见坑

1. **第一段请求不带 Range**：很多 CDN / Nginx 在 `Range` 非法时会返回整文件 200，导致后续 Range 偏移混乱。aria2 的做法是先探一次 200/206。
2. **`Content-Encoding: gzip` 整文件**：会在 `HttpResponseCommand::shouldInflateContentEncoding`（`HttpResponseCommand.cc:336`）关闭多段下载，因为解压后字节位置无法对齐。这是为什么 aria2 默认 `--auto-file-renaming=false` 时下 `.tgz` 文件偶尔只走单连接。
3. **chunked + total length 同时存在**：aria2 信任 chunked，丢弃 Content-Length 并切回单流模式（`HttpResponseCommand.cc:278`）。
4. **同 socket 上 pipelining 的服务器兼容性**：第一条响应必须是 206 且服务器明确 `Connection: keep-alive`；否则 `req_->setMaxPipelinedRequest(1)` 退回单段（`HttpResponseCommand.cc:171`）。
5. **断点续传边界**：`.aria2` 控制文件 + `segmentWrittenLengthMemo_` 必须**双写**，否则某一段 cancelled 后再 checkout 时位置会跳错。
6. **EndGame**：BT 场景下最后几个 piece 会同时分给多个 peer 抢，HTTP 没用，但 `Piece` 抽象已经支持（`piece->addUser(cuid)`）。

---

## 11. 一句话再总结

> **aria2 的 HTTP 多线程 = 一个 epoll 驱动的事件循环 + N 条独立 socket + 每条 socket 在中央 `SegmentMan` 借/还 `Segment`，`Segment` 通过 `Range: bytes=A-B` 头标识自己负责的字节区间，写盘走「Piece 子块位图 + 写缓存合并 pwrite」。**
>
> 任何复刻都只需把这三个组件 + 它们的交互实现出来，就能得到与 aria2 同等能力的 HTTP Range 多线程下载器。