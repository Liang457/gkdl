# aria2 / AriaNg 兼容 JSON-RPC 规范

> 目标读者：实现与 AriaNg (1.3.x) 通信的服务端或客户端。
>
> 本规范基于 aria2 服务端源码（`RpcRequest/Response`、`RpcMethod(Factory/Impl)`、`rpc_helper`、`HttpServer(Body)Command`、`WebSocketSession(Man)`、`json`、`FeatureConfig`）与 AriaNg 1.3.14 源码（`aria2RpcService`、`aria2HttpRpcService`、`aria2WebSocketRpcService`、`aria2RpcConstants`、`aria2Errors`、`aria2Options`、`ariaNgSettingService`、`ariaNgCommonService`、`exportCommandApiDialog`、`command` 控制器）逐行梳理。
>
> 凡 AriaNg 实际发送/接收的字段、默认值、超时、重连、ID 格式、错误处理、分批策略，本规范均精确记录。

---

## 0. 核心事实速查

| 项 | 值 | 来源 |
|----|----|------|
| 服务端口 | 6800（默认；AriaNg 默认） | `constants.js:35` |
| 默认协议 | `http`（AriaNg）/ `ws`（若用户切到 WS） | `constants.js:37`、`ariaNgSettingService.js:498-504` |
| 接口路径 | `/jsonrpc` | `constants.js:36` |
| HTTP 方法 | `POST`（默认）/ `GET`（仅 JSONP） | `constants.js:38`、`aria2HttpRpcService.js:66-71` |
| JSON-RPC 版本 | `2.0` | `aria2RpcConstants.js:5` |
| RPC 服务前缀 | `aria2.` | `aria2RpcConstants.js:6` |
| 系统服务前缀 | `system.` | `aria2RpcConstants.js:7` |
| Token 前缀 | `token:` | `aria2RpcConstants.js:8` |
| 应用前缀（ID 用） | `AriaNg` | `constants.js:6` |
| HTTP 请求超时 | 20000 ms | `constants.js:17` |
| WS 重连间隔 | 5000 ms（默认） | `constants.js:42` |
| `id` 格式 | Base64(`AriaNg_<unix>_<random>`) | `ariaNgCommonService.js:91-96` |
| `id` 类型 | string（AriaNg 用 Base64 字符串） | 同上 |
| 错误对象 | `{code, message}` | `RpcResponse.cc:71-77` |
| 响应 Content-Type | `application/json-rpc` 或 `text/javascript`（JSONP） | `HttpServerBodyCommand.cc:97-100` |
| 通知 Content-Type | 走 WS text frame | `WebSocketSessionMan.cc:79` |
| WebSocket 协议版本 | RFC 6455 | `aria2c.rst:3805` |
| CORS Allow-Methods | `POST, GET, OPTIONS` | `HttpServerBodyCommand.cc:205-206` |
| 请求体大小上限 | `rpc-max-request-size`（默认 2 MiB） | `WebSocketSession.cc:314` |

---

## 1. 总体架构

```
AriaNg 客户端                                    aria2 服务端
─────────────────                                ────────────────
aria2RpcService                                  RpcMethodFactory
  ├─ 装配 {jsonrpc, method, id, params}          RpcMethod::execute()
  └─ 选 transport                                ├─ authorize(token)
       ├─ aria2HttpRpcService (POST/GET)         ├─ process(method)
       │     └─ $http / base64+urlencode         └─ RpcResponse
       └─ aria2WebSocketRpcService (WS)              ├─ toJson()  → {"id","jsonrpc","result|error"}
             ├─ sendIdStates[id] = ctx                └─ toJsonBatch() → [{...}, ...]
             ├─ onMessage: by id 响应 / by method 通知
             ├─ onClose: planToReconnect
             └─ onOpen: callbacks
```

**AriaNg 端的两条铁律**（任何兼容实现必须支持）：

1. **请求体固定形状**：`{jsonrpc:"2.0", id, method, params}`。`id` 必填字符串；服务端响应里 `id` 必须 **原样回传**。
2. **响应判别**：
   - 有 `id` 且无 `method` → RPC 响应，按 `result` / `error` 分支
   - 有 `method` 且无 `id` → 服务器通知
   - 既无 `id` 也无 `method` → 忽略

---

## 2. 传输

### 2.1 HTTP POST（AriaNg 默认）

```
POST /jsonrpc HTTP/1.1
Content-Type: application/json
<请求体为 JSON 字符串>
```

响应头：

- `Content-Type: application/json-rpc`
- 可选 `Content-Encoding: gzip`

HTTP 状态码（`HttpServerBodyCommand.cc:116-129`）：

| 响应 code | HTTP 状态 |
|-----------|-----------|
| 0 | 200 |
| 1 | 400 |
| -32600 | 400 |
| -32601 | 404 |
| 其他 | 500 |

### 2.2 HTTP GET（JSONP）— AriaNg 通过设置 `httpMethod=GET` 触发

URL 模板（AriaNg 实际生成，`aria2HttpRpcService.js:9-51`）：

```
{RPCURL}?method={M}&id={I}&params={B64_ENC_JSON_ARRAY}&jsoncallback={CB}
```

| 字段 | 必选 | 生成规则 |
|------|------|----------|
| `method` | 单调用必选 | 字符串，percent-encode |
| `id` | 单调用必选 | 字符串，percent-encode |
| `params` | 必选 | `btoa(JSON.stringify(params))` → `encodeURIComponent()` |
| `jsoncallback` | 可选 | 用户配的回调名 |

**批量 GET 特殊**：不带 `method` 和 `id`，整个 JSON 数组经 `JSON.stringify` 后 base64 → percent-encode 进 `params`。

服务端解析（`json.cc:108-164`）：`?` → split `&` → percent-decode → base64 decode → 拼出 `{"method":...,"id":...,"params":[...]}` 字符串再走 JSON 解析。

### 2.3 WebSocket（AriaNg 用 `protocol=ws|wss` 切换）

```
WS /jsonrpc HTTP/1.1
```

- 协议：RFC 6455
- 客户端发送：text frame（`aria2WebSocketRpcService.js:272`）
- 服务端响应：text frame
- `aria2WebSocketRpcService.js:111-114` 用了 `$websocket(rpcUrl, { maxTimeout: 1, reconnectInterval: 5000 })`（即 angular-websocket 库），不指定子协议

WS 路径与 HTTP 相同（`/jsonrpc`）。

### 2.4 CORS

浏览器调 POST 时会先发 `OPTIONS`：

```
OPTIONS /jsonrpc HTTP/1.1
Origin: https://example.com
Access-Control-Request-Method: POST
Access-Control-Request-Headers: content-type,x-custom
```

服务端回（`HttpServerBodyCommand.cc:195-218`）：

```
HTTP/1.1 200 OK
Access-Control-Allow-Methods: POST, GET, OPTIONS
Access-Control-Max-Age: 1728000
Access-Control-Allow-Headers: <回显 Access-Control-Request-Headers>
```

仅当同时存在 `Origin`、`Access-Control-Request-Method` 且服务端允许该 Origin 时返回。

### 2.5 自定义请求头

AriaNg 用户可在 `rpcRequestHeaders` 设置，格式 `Name: Value\nName: Value`（`aria2HttpRpcService.js:73-87`）。AriaNg 会逐行 `split(':')` 拆分后塞进 HTTP 请求头（**仅 HTTP 路径**）。

### 2.6 连接生命周期（AriaNg 视角）

- `connectionSuccessCallback`：HTTP 收到 2xx / WS `onOpen`
- `connectionFailedCallback`：HTTP 网络失败 / WS 不可用（且不重连）
- `connectionReconnectingCallback`：WS 触发 `reconnect()`（如用户主动点重连按钮）
- `connectionWaitingToReconnectCallback`：WS 关闭但 `webSocketReconnectInterval > 0`，进入等待
- `operationSuccessCallback` / `operationErrorCallback`：方法调用成功/失败
- `firstSuccessCallback`：第一次成功连接（用于 UI 提示）

`aria2WebSocketRpcService.js:184-219` 实现了"重连时把待响应全部 reject 并回调 errorCallback"的逻辑。

---

## 3. 请求格式

### 3.1 单请求

```json
{
  "jsonrpc": "2.0",
  "id":      "<Base64(AriaNg_<ts>_<random>)>",
  "method":  "aria2.<short>",
  "params":  [<token?>, <arg1>, <arg2>, ...]
}
```

### 3.2 批量请求

顶层是 JSON 数组，每项结构同 3.1。批量请求里某项不是 Object 时整体报 `-32600`（`HttpServerBodyCommand.cc:305-309`）。

### 3.3 通知（客户端 → 服务端）

aria2 **HTTP 通道不支持**；WS 通道虽然按 JSON-RPC 2.0 允许，但 **aria2 服务端不响应未带 `id` 的请求**（`processJsonRpcRequest` 第一行 `popValue("id")` 失败时直接报 -32600）。AriaNg 自身从不发通知（`aria2WebSocketRpcService.js` 中只发送 `request`），所以兼容实现可不支持。

### 3.4 服务端主动通知（仅 WS）

`aria2c.rst:3814-3819`、`WebSocketSessionMan.cc:68-84`：

```json
{
  "jsonrpc": "2.0",
  "method":  "aria2.onDownloadStart",
  "params":  [{"gid": "2089b05ecca3d829"}]
}
```

| 通知 | 触发 |
|------|------|
| `aria2.onDownloadStart` | 下载开始 |
| `aria2.onDownloadPause` | 暂停 |
| `aria2.onDownloadStop` | 用户停止 |
| `aria2.onDownloadComplete` | 完成（BT 在做种结束后才发） |
| `aria2.onDownloadError` | 因错误停止 |
| `aria2.onBtDownloadComplete` | BT 下载完成但仍在做种 |

`params` 总是长度 1 的数组，元素是 `event` object（当前实现仅含 `gid` 字段）。客户端不得回包通知。

### 3.5 鉴权扩展：`token:`

> 实现者务必优先支持——AriaNg 是这样做的：`if (secret && !isSystemMethod) finalParams.push('token:' + secret)`（`aria2RpcService.js:160-162`）。

格式：

```json
{"method":"aria2.addUri",
 "params":["token:YOUR_SECRET", ["http://x"], {"dir":"/tmp"}]}
```

- 仅当第一个参数是 **字符串** 且以 `token:` 开头时才被识别为 token
- 命中后从数组中 `pop_front` 移除，token 部分不再传给业务方法
- 服务端需将 `YOUR_SECRET` 与 `--rpc-secret` 配置做 **恒定时间** 比较（`RpcMethod.cc:86-89` + `validateToken`）
- 鉴权失败：抛 `"Unauthorized"`，响应里设 `authorized=NOTAUTHORIZED`，HTTP/WS 响应 **延迟 1 秒** 发出（`HttpServerBodyCommand.cc:151-153`）
- `system.multicall` 鉴权特殊：外层 call 的 `params[0]` 不当 token 处理；每个内层 call 必须自带 token（`aria2c.rst:2370-2373`、`RpcMethodImpl.cc:1421-1478`）

---

## 4. 响应格式

### 4.1 单响应（`RpcResponse.cc:155-178`）

```json
{"id":"<原值>","jsonrpc":"2.0","result":<任意>}
{"id":"<原值>","jsonrpc":"2.0","error":{"code":<int>,"message":"<string>"}}
```

**字段顺序固定**：`id` → `jsonrpc` → `result`/`error`。客户端应按字段名解析。

**AriaNg 判别逻辑**（`aria2HttpRpcService.js:108-133` 与 `aria2WebSocketRpcService.js:76-86`）：

- `data.result` 存在 → 走 successCallback
- `data.error` 存在 → 走 errorCallback（`errorCallback(id, error)`）
- AriaNg 还会对 `error.message` 在 `aria2RpcErrors` 表里查友好提示（`aria2RpcService.js:118-132`）

### 4.2 批量响应（`RpcResponse.cc:200-244`）

顶层是 JSON 数组，元素同 4.1。非法 Object 在请求数组中不会出现在结果里；成功项是原始 `result`，失败项是 `error` 对象。

### 4.3 JSONP 响应

`callback(...)` 包裹，Content-Type `text/javascript`（`HttpServerBodyCommand.cc:97-100`）。

### 4.4 错误对象

```json
{"code":<int>, "message":"<string>"}
```

| code | 触发 |
|------|------|
| -32700 | JSON 解析失败（`HttpServerBodyCommand.cc:279`、`WebSocketSession.cc:172`） |
| -32600 | `id` 缺失；顶层非 object（批量里某项非 object） |
| -32602 | `params` 是 object 而非 array（不支持命名参数） |
| 1 | 鉴权失败 `"Unauthorized"` / 未知方法 `"No such method: X"` / 业务异常（无对应 GID、参数越界等） |
| 其他保留 | 内部错误 |

`aria2Errors.js` 还给出 AriaNg 侧的 `errorCode` 字段（任务 `errorCode`/`errorMessage` 字段，见 §6.3）映射 0–32（其中 7、31 保留），可用于 UI 提示。

### 4.5 字段类型

| JSON | aria2 ValueBase | 序列化 |
|------|-----------------|--------|
| object | Dict | `{...}` |
| array | List | `[...]` |
| string | String | `"..."` |
| integer | Integer | **十进制文本（无引号）** |
| true/false | Bool | `true` / `false`（**无引号**） |
| null | Null | `null` |

> ⚠️ **整数与布尔**：服务端响应中所有数值字段（`gid` 除外）都是 **字符串**——`util::itos` 把整数序列化为十进制文本再用 String 包装。布尔序列化为字符串 `"true"` / `"false"`。AriaNg 代码里就这么处理（`task-detail.js` 等多处先 `parseInt` 再用）。兼容实现务必遵循，否则 AriaNg 会显示异常。
>
> 例外：响应顶层 `error.code` 是真正的 JSON 整数（`RpcResponse.cc:73-76` 直接 `Integer::g(code)`）；`jsonrpc` 是字符串 `"2.0"`。

---

## 5. 全部方法（按 AriaNg 实际调用顺序列出）

> **签名约定**：
> - `[secret]` 表示可选的 `token:xxx`，**仅在**服务端配置了 `--rpc-secret` 时需要
> - 业务参数前的 `[secret]` **总是存在位置 0**（不提供就跳过），不要因为业务参数数量固定就硬编码位置
> - AriaNg 内部用 `buildRequestContext`（`aria2RpcService.js:134-223`）统一加 `aria2.` 前缀与 `token:`

### 5.1 `aria2.*` — 任务管理（AriaNg 实际调用的方法）

| 方法 | 实际调用 | params（AriaNg 实际发送顺序） | result |
|------|----------|--------------------------------|--------|
| `aria2.addUri` | `aria2TaskService.newUriTask` | `["token:?"]?, [urls], [options], [position=undefined]` | GID string |
| `aria2.addTorrent` | `newTorrentTask` | `["token:?"]?, [torrentB64], [uris=[]], [options], [position]` | GID string |
| `aria2.addMetalink` | `newMetalinkTask` | `["token:?"]?, [metalinkB64], [options], [position]` | `[GID, ...]` |
| `aria2.remove` | `removeTasks` | `["token:?"]?, gid` | GID string |
| `aria2.forceRemove` | `removeTasks` force | `["token:?"]?, gid` | GID string |
| `aria2.pause` | `pauseTasks` | `["token:?"]?, gid` | GID string |
| `aria2.forcePause` | force pause | `["token:?"]?, gid` | GID string |
| `aria2.pauseAll` | 全停 | `["token:?"]?` | `"OK"` |
| `aria2.forcePauseAll` | 全强停 | `["token:?"]?` | `"OK"` |
| `aria2.unpause` | `unpause` | `["token:?"]?, gid` | GID string |
| `aria2.unpauseAll` | 全部继续 | `["token:?"]?` | `"OK"` |
| `aria2.changePosition` | `changeTaskPosition` | `["token:?"]?, gid, pos:int, how:"POS_SET"\|"POS_CUR"\|"POS_END"` | 目标 int |
| `aria2.changeUri` | （未在 AriaNg 1.3.14 调用） | `["token:?"]?, gid, fileIndex:int≥1, delUris:[], addUris:[], [position]` | `[delCount, addCount]` |
| `aria2.tellStatus` | `getTaskStatus` | `["token:?"]?, gid, [keys]` | status object |
| `aria2.getUris` | （未在 AriaNg 1.3.14 调用） | `["token:?"]?, gid` | `[{uri, status:"used"\|"waiting"}]` |
| `aria2.getFiles` | `tellStatus` 内的 `files` 字段 | `["token:?"]?, gid` | file object array |
| `aria2.getServers` | （未在 AriaNg 1.3.14 调用） | `["token:?"]?, gid` | server object array |
| `aria2.getPeers` | `getBtTaskPeers` | `["token:?"]?, gid` | peer object array |
| `aria2.tellActive` | list 下载中 | `["token:?"]?, [keys]` | status object array |
| `aria2.tellWaiting` | list 等待中 | `["token:?"]?, offset:int, num:int, [keys]` | status object array |
| `aria2.tellStopped` | list 已停止 | `["token:?"]?, offset:int=-1, num:int=1000, [keys]` | status object array |
| `aria2.getOption` | `getTaskOptions` | `["token:?"]?, gid` | options object |
| `aria2.changeOption` | `setTaskOption` | `["token:?"]?, gid, {key:value}` | `"OK"` |
| `aria2.getGlobalOption` | `getGlobalOption` | `["token:?"]?` | options object |
| `aria2.changeGlobalOption` | `setGlobalOption` | `["token:?"]?, {key:value}` | `"OK"` |
| `aria2.purgeDownloadResult` | `clearStoppedTasks` | `["token:?"]?` | `"OK"` |
| `aria2.removeDownloadResult` | `removeDownloadResult` | `["token:?"]?, gid` | `"OK"` |
| `aria2.getVersion` | 测试连接 + debug 页 | `["token:?"]?` | `{version, enabledFeatures}` |
| `aria2.getSessionInfo` | （未在 AriaNg 1.3.14 调用） | `["token:?"]?` | `{sessionId: hex}` |
| `aria2.getGlobalStat` | `refreshGlobalStat` | `["token:?"]?` | globalStat object |
| `aria2.saveSession` | `saveSession` | `["token:?"]?` | `"OK"` |
| `aria2.shutdown` | 优雅关闭 | `["token:?"]?` | `"OK"`（3 秒后实际退出） |
| `aria2.forceShutdown` | 强制关闭 | `["token:?"]?` | `"OK"`（3 秒后） |

### 5.2 `system.*` — 系统方法

| 方法 | AriaNg 行为 | 鉴权要求 |
|------|------------|----------|
| `system.multicall` | AriaNg 在 `getTaskStatusAndBtPeers` 用它批量发 `tellStatus + getPeers` | 递归禁止；每个内层 call 自带 token |
| `system.listMethods` | debug 页（`controllers/debug.js:34`） | 可免 token |
| `system.listNotifications` | debug 页 | 可免 token |

> `system.multicall` 的 params 是 JSON 数组；外层不再加 `token:`；每个内层对象形如 `{"methodName":"aria2.tellStatus","params":["token:?", gid]}`。结果数组中 **成功项是单元素数组 `[result]`**（兼容 XML-RPC 时代的 `system.multicall`），失败项直接是 `{code, message}`。

### 5.3 AriaNg 默认 `keys` 列表

> AriaNg 用 `getBasicTaskParams()` 和 `getFullTaskParams()`（`aria2RpcService.js:286-310`）选择 `tellStatus` / `tellActive` / `tellWaiting` / `tellStopped` 返回的字段。`keys` 缺省或传 `null` 等价于返回全部字段。`keys` **过滤的是字段名**，不影响嵌套对象内部结构。

**基础字段**（basic，12 个）：
```
gid, totalLength, completedLength, uploadSpeed, downloadSpeed,
connections, numSeeders, seeder, status, errorCode, verifiedLength, verifyIntegrityPending
```

**全量字段**（full = basic 12 个 + 3 个）：
```
gid, totalLength, completedLength, uploadSpeed, downloadSpeed,
connections, numSeeders, seeder, status, errorCode,
verifiedLength, verifyIntegrityPending,
files, bittorrent, infoHash
```

### 5.4 `tellStatus` 完整字段（`RpcMethodImpl.cc:616-691`）

| key | JSON 类型 | 说明 |
|-----|-----------|------|
| `gid` | string (hex) | 16 字符 |
| `totalLength` | string (int) | 字节，受 `--select-file` 影响 |
| `completedLength` | string (int) | 字节 |
| `uploadLength` | string (int) | 累计 |
| `bitfield` | string (hex) | 已下载位图（BT） |
| `downloadSpeed` | string (int) | B/s |
| `uploadSpeed` | string (int) | B/s |
| `infoHash` | string (hex) | BT |
| `numSeeders` | string (int) | BT |
| `seeder` | string (bool) | 是否做种 |
| `pieceLength` | string (int) | 字节 |
| `numPieces` | string (int) | 分片数 |
| `connections` | string (int) | 当前连接数 |
| `errorCode` | string (int) | 0=无错；见 `aria2Errors.js` |
| `errorMessage` | string | 错误描述（仅 errorCode≠0 时） |
| `followedBy` | [string] | 后续 GID（BT 续作） |
| `following` | string | 前驱 GID |
| `belongsTo` | string | 所属 GID（metalink） |
| `status` | string | `"active"` \| `"waiting"` \| `"paused"` \| `"error"` \| `"complete"` \| `"removed"` |
| `verifiedLength` | string (int) | 已校验字节 |
| `verifyIntegrityPending` | string (bool) | 校验挂起 |
| `files` | [file] | 嵌套 |
| `dir` | string | 保存目录 |
| `bittorrent` | object | BT 才有（见下） |

### 5.5 `files[]`（`getFiles` / `tellStatus.files`）

| key | 类型 |
|-----|------|
| `index` | string (int)，1-based |
| `path` | string |
| `length` | string (int) |
| `completedLength` | string (int) |
| `selected` | string (bool) |
| `uris` | `[{uri, status: "used"\|"waiting"}]` |

### 5.6 `bittorrent`（`tellStatus.bittorrent`）

仅当 `keys` 包含 `bittorrent` 且为 BT 任务时存在（`gatherBitTorrentMetadata`，`RpcMethodImpl.cc:693-...`）：

| key | 类型 | 说明 |
|-----|------|------|
| `announceList` | [[string]] | tracker 列表（按 tier 分组） |
| `comment` | string | 备注 |
| `creationDate` | string (int) | unix 秒 |
| `mode` | string | `"single"` \| `"multi"` |
| `info` | object | `{name: string}` 子对象 |
| `name` | string | 顶层别名（=info.name） |

> 兼容性提示：部分早期 aria2 把 `name` 放在 `info` 外面，部分新版本也在外面。**两边都检查**最稳妥。

### 5.7 `getUris`

```json
[{"uri":"http://x/y","status":"used"},
 {"uri":"http://mirror/y","status":"waiting"}]
```

`status` 取值 `"used"` / `"waiting"`（`aria2c.rst:2820`、源码 `VLB_USED` / `VLB_WAITING`）。

### 5.8 `getPeers`

| key | 类型 |
|-----|------|
| `peerId` | string (hex) |
| `ip` | string |
| `port` | string (int) |
| `bitfield` | string (hex) |
| `amChoking` | string (bool) |
| `peerChoking` | string (bool) |
| `downloadSpeed` | string (int) |
| `uploadSpeed` | string (int) |
| `seeder` | string (bool) |

### 5.9 `getServers`

| 顶层 key | 类型 | 说明 |
|----------|------|------|
| `index` | string (int) | 文件 1-based index |
| `servers` | `[{uri, currentUri, downloadSpeed}]` | 当前连接 |

### 5.10 `getVersion`

```json
{
  "version": "1.36.0",
  "enabledFeatures": ["Async DNS", "BitTorrent", "Firefox3 Cookie", "GZip",
                     "HTTPS", "Message Digest", "Metalink", "XML-RPC", "SFTP"]
}
```

> ⚠️ 实际字符串是 **`"Firefox3 Cookie"`**（带数字 3；`FeatureConfig.cc:136`），不是 `"Firefox Cookies"`。AriaNg 的常量与 `aria2c.rst` 都用前者。

`enabledFeatures` 列表（来自 `FeatureConfig.cc:115-189`）：

| 字符串 | 编译宏 |
|--------|--------|
| `Async DNS` | `ENABLE_ASYNC_DNS` |
| `BitTorrent` | `ENABLE_BITTORRENT` |
| `Firefox3 Cookie` | `HAVE_SQLITE3` |
| `GZip` | `HAVE_ZLIB` |
| `HTTPS` | `ENABLE_SSL` |
| `Message Digest` | 总是 |
| `Metalink` | `ENABLE_METALINK` |
| `XML-RPC` | `ENABLE_XML_RPC` |
| `SFTP` | `HAVE_LIBSSH2` |

### 5.11 `getSessionInfo`

```json
{"sessionId": "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0"}
```

`sessionId` 是 16 字符 hex。

### 5.12 `getGlobalStat`

```json
{
  "downloadSpeed": "12345",
  "uploadSpeed":   "678",
  "numWaiting":    "0",
  "numStopped":    "3",
  "numStoppedTotal":"42",
  "numActive":     "2"
}
```

AriaNg 轮询间隔默认 1000ms（`constants.js:43`），缓存最近 120 条（`globalStatStorageCapacity`）。

### 5.13 `changePosition` 的 `how` 参数

| 值 | 含义 |
|----|------|
| `"POS_SET"` | 绝对位置（**AriaNg 唯一使用**） |
| `"POS_CUR"` | 相对当前位置 |
| `"POS_END"` | 相对队尾 |

AriaNg `changeTaskPosition` 固定传 `"POS_SET"`（`aria2TaskService.js:864`）。

### 5.14 options 对象

- 几乎所有 value 都是 **字符串**（如 `"true"`、`"1024"`、`"/tmp"`）
- AriaNg 仅 `header` 选项的 value 是 **数组**（`submitFormat: 'array'`，元素间用 `\n` 分隔——`aria2Options.js:243-249`）。`aria2` 服务端也接受 `index-out` 用数组，但 AriaNg 1.3.14 实际未使用该选项。其他 option 几乎都是字符串（`"true"`、`"1024"`、`"/tmp"`）。
- `getOption(gid)` 只返回对单个任务可设的项（`RpcMethod.cc:1174-1184`）
- `getGlobalOption()` 返回所有已定义项，**但会排除 `rpc-secret` 与未定义项**（`RpcMethodImpl.cc:1215-1216`）
- `changeOption` / `changeGlobalOption` 中只接受 `getChangeOption` / `getChangeOptionForReserved` / `getChangeGlobalOption` 标记为可改的项（`RpcMethod.cc:150-198`），其他静默忽略

完整可改项清单与初始项清单见 `aria2 --help` 与 `OptionHandler` 定义。

---

## 6. 数据交换样例

### 6.1 POST 添加下载

请求：
```http
POST /jsonrpc HTTP/1.1
Content-Type: application/json

{"jsonrpc":"2.0","id":"QWhyaWFOZ18xNzIxMjU2MDBfMC4xMjM0NTY=",
 "method":"aria2.addUri",
 "params":[["http://example.org/file.iso"],{"dir":"/downloads","split":"16"}]}
```

响应：
```http
HTTP/1.1 200 OK
Content-Type: application/json-rpc

{"id":"QWhyaWFOZ18xNzIxMjU2MDBfMC4xMjM0NTY=","jsonrpc":"2.0",
 "result":"2089b05ecca3d829"}
```

### 6.2 POST 带 secret（token）

```json
{"jsonrpc":"2.0","id":"...","method":"aria2.addUri",
 "params":["token:mysecret",["http://example.org/file"],{"dir":"/data"}]}
```

### 6.3 GET（JSONP）

```
GET /jsonrpc?method=aria2.tellStatus&id=foo&params=WyIyMDg5YjA1ZWNjYTNkODI5Il0%3D&jsoncallback=cb HTTP/1.1
```

`params` 解码：`WyIyMDg5YjA1ZWNjYTNkODI5Il0=` → `["2089b05ecca3d829"]`

响应：
```
HTTP/1.1 200 OK
Content-Type: text/javascript

cb({"id":"foo","jsonrpc":"2.0","result":{"gid":"2089b05ecca3d829","status":"active", ... }});
```

### 6.4 GET 批量

```
GET /jsonrpc?params=W3sianNvbnJwYyI6...LCB7Impzb25ycGMiOiA...fV0%3D
```

`params` 解码后即整个批量 JSON 数组。

### 6.5 批量（POST）

```json
[
  {"jsonrpc":"2.0","id":"1","method":"aria2.getVersion"},
  {"jsonrpc":"2.0","id":"2","method":"aria2.tellActive","params":[]}
]
```

响应：

```json
[
  {"id":"1","jsonrpc":"2.0","result":{"version":"1.36.0","enabledFeatures":[...]}},
  {"id":"2","jsonrpc":"2.0","result":[]}
]
```

### 6.6 `system.multicall`（AriaNg `getTaskStatusAndBtPeers` 实际请求）

请求：
```json
{"jsonrpc":"2.0","id":"...","method":"system.multicall","params":[[
  {"methodName":"aria2.tellStatus","params":["2089b05ecca3d829"]},
  {"methodName":"aria2.getPeers",   "params":["2089b05ecca3d829"]}
]]}
```

> 注意：上面省略了 `token:`；若服务端要求鉴权，每个内层 call 的 `params[0]` 应为 `"token:xxx"`。

响应（成功项是 `[result]`，失败项是 `error` 对象）：
```json
{"id":"...","jsonrpc":"2.0","result":[
  [{"gid":"2089b05ecca3d829","status":"active", ... }],
  [{"peerId":"...","ip":"1.2.3.4", ...}]
]}
```

### 6.7 WebSocket

```
> {"jsonrpc":"2.0","id":"...","method":"aria2.tellActive","params":[]}
< {"id":"...","jsonrpc":"2.0","result":[]}
< {"jsonrpc":"2.0","method":"aria2.onDownloadStart","params":[{"gid":"2089b05ecca3d829"}]}
```

### 6.8 业务错误

```json
{"id":"...","jsonrpc":"2.0",
 "error":{"code":1,"message":"Unauthorized"}}
```

HTTP 状态 400，body 仍为上面这串 JSON，Content-Type `application/json-rpc`。

### 6.9 解析错误

```json
{"id":null,"jsonrpc":"2.0",
 "error":{"code":-32700,"message":"Parse error."}}
```

HTTP 状态 400，`id` 为 `null`（因为解析失败前 id 不可知）。

---

## 7. AriaNg 端约定细节

### 7.1 ID 生成

`ariaNgCommonService.js:91-96`：

```js
generateUniqueId() {
    var sourceId = 'AriaNg_' + Math.round(Date.now()/1000) + '_' + Math.random();
    return base64.encode(sourceId);   // 标准 base64（不是 base64url）
}
```

兼容实现应：

- 接受任何字符串作为 `id`（AriaNg 不会发数字 id）
- **原样回传** `id`（包括 base64 中的 `=` 填充字符）

### 7.2 成功 / 失败判别

`aria2HttpRpcService.js:108-133`：

- HTTP 2xx 且 body 有 `data.id` → 成功：调 `successCallback(data.id, data.result)`，失败调 `errorCallback(data.id, data.error)`
- HTTP 非 2xx 或网络错误 → 失败：`{id: '-1', error: {message: 'Cannot connect to aria2!'}}`

WS 路径（`aria2WebSocketRpcService.js:50-109`）：

- `content.id` 存在 → 响应；查 `sendIdStates[uniqueId]`，找到则 resolve + 调回调
- `content.method` 存在且无 `id` → 通知；查 `eventCallbacks[method]`
- 二者皆无 → 丢弃

### 7.3 重连 / 超时

- HTTP：`$http` 默认走 Angular `$http` 服务，超时 20000ms（`constants.js:17`）
- WS：
  - 断开时若 `webSocketReconnectInterval > 0`（默认 5000ms）→ 5 秒后自动 `reconnect()`
  - 关闭瞬间会 reject 所有未响应请求，errorCallback 收到 `{message: 'Cannot connect to aria2!'}`
  - 用户也可手动调用 `reconnect()` 触发（设置页面有"重连"按钮）

### 7.4 `secret` 字段在 AriaNg 中的存储

- UI 录入原文
- 存储到 localStorage 前 base64 编码（`ariaNgSettingService.js:614`）
- 读取时 base64 解码（`ariaNgSettingService.js:247-256, 507-508`）
- 发送时拼成 `"token:" + decodedSecret`
- 路由命令 `#!/settings/rpc/set/...?secret=BASE64URL(...)` 时用 base64url 解码（`controllers/command.js:73`）

### 7.5 命令行（hash 路由）API

AriaNg 监听两个 hash 路由（`core/router.js:74`）：

1. `#!/new/task?url=BASE64URL(URL)&pause=true&<optKey>=<val>&...`
   - 调 `aria2TaskService.newUriTask` 添加下载
   - 只接受 `aria2AllOptions` 中已知的 optKey（`controllers/command.js:24`）

2. `#!/settings/rpc/set?protocol=http|https|ws|wss&host=...&port=...&interface=...&secret=BASE64URL(...)`
   - 调 `ariaNgSettingService.setDefaultRpcSetting` 切换服务端
   - `secret` 用 base64url 编码（`exportCommandApiDialog.js:74`）

### 7.6 通知订阅

`aria2RpcService.js:63-76, 276-283`：

```js
registerEvent('onDownloadStart', onDownloadStartCallbacks);
...
rpcImplementService.on(fullName, cb);   // fullName = 'aria2.onDownloadStart'
```

兼容实现：仅在 WS 通道上发通知即可；HTTP 不需要支持。

---

## 8. 错误码速查

### 8.1 协议层

| code | 触发 |
|------|------|
| -32700 | JSON 解析失败 |
| -32600 | id 缺失；批量里某项非 object；HTTP 通道发通知（无 id） |
| -32602 | params 是 object（命名参数） |
| 1 | 鉴权失败 `"Unauthorized"`、未知方法 `"No such method: X"`、参数缺失或越界、`GID not found` 等所有业务异常 |

注意：**未知方法** 在 aria2 中实际 code 是 `1` 而非 `-32601`（`NoSuchMethodRpcMethod` 走 `RpcMethod::execute` 抛 `DL_ABORT_EX`），但 **HTTP 状态码** 仍按 -32601 映射到 404。

### 8.2 任务层（`tellStatus.errorCode`）

来自 `aria2Errors.js`，数值字段，AriaNg 用作 UI 提示映射：

| code | 含义 |
|------|------|
| 0 | 成功（无错） |
| 1 | 未知 |
| 2 | 操作超时 |
| 3 | 资源未找到 |
| 4 | 资源未找到（max-file-not-found） |
| 5 | 下载因速度过低被中止 |
| 6 | 网络问题 |
| 7 | 保留（部分下载未完成） |
| 8 | 服务器不支持断点续传 |
| 9 | 磁盘空间不足 |
| 10 | piece 长度不一致 |
| 11 | 同时下载同一文件 |
| 12 | 同时下载同一 BT |
| 13 | 文件已存在 |
| 14 | 文件重命名失败 |
| 15 | 文件打开失败 |
| 16 | 文件创建失败 |
| 17 | I/O 错误 |
| 18 | 目录创建失败 |
| 19 | 名称解析失败 |
| 20 | metalink 解析失败 |
| 21 | FTP 命令失败 |
| 22 | HTTP 响应头异常 |
| 23 | 重定向过多 |
| 24 | HTTP 鉴权失败 |
| 25 | bencoded 文件解析失败（torrent） |
| 26 | torrent 文件损坏 |
| 27 | magnet URI 非法 |
| 28 | 选项错误 |
| 29 | 服务器过载 |
| 30 | JSON-RPC 请求解析失败（保留） |
| 31 | 保留 |
| 32 | 校验和验证失败 |

### 8.3 AriaNg 已知 RPC 错误消息（`aria2RpcConstants.js:9-14`）

```js
aria2RpcErrors = {
  Unauthorized: { message: 'Unauthorized', tipTextKey: 'rpc.error.unauthorized' }
}
```

仅一项；其他错误消息直接展示原文。

---

## 9. 实现 / 兼容提示

1. **整数、布尔全部以字符串输出**——除 `error.code`、`jsonrpc`、`id` 字段外，所有数值/布尔字段均为字符串。客户端解析务必先字符串再 `parseInt`。
2. **响应字段顺序固定**：`id` → `jsonrpc` → `result`/`error`。不要依赖位置。
3. **`jsonrpc` 字段总输出 `"2.0"`**——即使客户端没发，AriaNg 始终发送。
4. **不支持命名参数**——`params` 必须是 array，传 object 报 -32602。
5. **HTTP/WS 三种传输鉴权一致**——`params[0]` 为 `token:xxx`。
6. **通知仅 WS**——HTTP 路径上不会发送 `aria2.onXxx`。
7. **WS 推送的 text frame 不需要回包**——AriaNg 收到通知只更新 UI，不会发响应。
8. **id 原样回传**——无论客户端发字符串还是数字。
9. **JSONP 批量请求**：`method` 和 `id` 都不能出现，`params` 装整个数组。
10. **CORS**：浏览器内调用必须支持 `OPTIONS` 预检；`Access-Control-Request-Headers` 要回显。
11. **鉴权失败响应延迟 1 秒**——客户端超时时间请 ≥ 1s。
12. **`--rpc-secret` 未配置时**：服务端不会拒绝请求；`params[0]` 仍按 `token:` 协议剥离（如果有）。AriaNg 行为是"未配 secret 时不发 `token:`"。
13. **`changeOption` / `changeGlobalOption`**：未知选项静默忽略；非 `getChangeOption` / `getChangeOptionForReserved` / `getChangeGlobalOption` 标记的项也会忽略。
14. **`shutdown` / `forceShutdown`**：服务端 3 秒后才真正退出（`goingShutdown`），给客户端留时间收响应。
15. **`tellStatus` / `tellActive` / `tellWaiting` / `tellStopped` 的 `keys` 缺省/空/null/省略都返回全部**。
16. **`info.name` vs `name`**：BT 元信息下都有 `name` 字段。`info` 子对象下也常有 `name`。客户端要兼容只取一个。
17. **任务 `errorCode` 字段**是数字字符串（`errorCode: "9"` 表示磁盘空间不足），不要解析成 boolean。
18. **WS 文本帧内容**必须是合法 JSON 字符串；不要在前面加任何空白或 BOM。
19. **批量响应**长度 = 批量请求中合法 Object 的个数；非法 Object 静默丢弃。
20. **`system.multicall` 嵌套**禁止递归（`Recursive system.multicall forbidden.`）。

---

## 10. AriaNg 默认设置一览

`src/scripts/config/constants.js`：

| 键 | 默认值 |
|----|--------|
| `rpcHost` | `""`（首次启动需手动配） |
| `rpcPort` | `6800` |
| `rpcInterface` | `jsonrpc` |
| `protocol` | `http` |
| `httpMethod` | `POST` |
| `rpcRequestHeaders` | `""` |
| `secret` | `""` |
| `webSocketReconnectInterval` | `5000` |
| `httpRequestTimeout` | `20000`（ms） |
| `globalStatRefreshInterval` | `1000`（ms） |
| `downloadTaskRefreshInterval` | `1000`（ms） |
| `appPrefix` | `AriaNg` |

AriaNg 默认会把 `appPrefix` 嵌入 `id`，对兼容实现无影响（任意字符串皆可）。

---

## 11. 附录：完整方法清单

> 与 `RpcMethodFactory.cc:53-94` 一致，标记依赖编译宏：

```
aria2.addUri
aria2.addTorrent            (ENABLE_BITTORRENT)
aria2.addMetalink           (ENABLE_METALINK)
aria2.getPeers              (ENABLE_BITTORRENT)
aria2.remove
aria2.pause
aria2.forcePause
aria2.pauseAll
aria2.forcePauseAll
aria2.unpause
aria2.unpauseAll
aria2.forceRemove
aria2.changePosition
aria2.tellStatus
aria2.getUris
aria2.getFiles
aria2.getServers
aria2.tellActive
aria2.tellWaiting
aria2.tellStopped
aria2.getOption
aria2.changeUri
aria2.changeOption
aria2.getGlobalOption
aria2.changeGlobalOption
aria2.purgeDownloadResult
aria2.removeDownloadResult
aria2.getVersion
aria2.getSessionInfo
aria2.shutdown
aria2.forceShutdown
aria2.getGlobalStat
aria2.saveSession
system.multicall
system.listMethods
system.listNotifications
```

通知（`allNotificationsNames()`）：

```
aria2.onDownloadStart
aria2.onDownloadPause
aria2.onDownloadStop
aria2.onDownloadComplete
aria2.onDownloadError
aria2.onBtDownloadComplete   (ENABLE_BITTORRENT)
```

`enabledFeatures` 完整列表：

```
Async DNS              (ENABLE_ASYNC_DNS)
BitTorrent             (ENABLE_BITTORRENT)
Firefox3 Cookie        (HAVE_SQLITE3)
GZip                   (HAVE_ZLIB)
HTTPS                  (ENABLE_SSL)
Message Digest         (always)
Metalink               (ENABLE_METALINK)
XML-RPC                (ENABLE_XML_RPC)
SFTP                   (HAVE_LIBSSH2)
```

---

## 12. 验证清单（实现者用）

实现完一个 aria2 RPC 兼容服务后，逐条验证：

- [ ] POST `/jsonrpc` 接受 `{jsonrpc,id,method,params}` 数组
- [ ] GET `/jsonrpc?method=&id=&params=BASE64&jsoncallback=` 返回 `cb({...});`
- [ ] GET `/jsonrpc?params=BASE64(ARRAY)` 处理批量
- [ ] `OPTIONS /jsonrpc` 返回 CORS 头
- [ ] `Content-Type: application/json` POST 请求体可被解析
- [ ] 响应体格式为 `{"id":<原值>,"jsonrpc":"2.0","result"|"error":...}`（字段顺序）
- [ ] 业务整数/布尔字段都是字符串
- [ ] 鉴权失败延迟 1 秒返回 400
- [ ] 未知方法 code=1，message 含 `"No such method:"`
- [ ] `params` 是 object 时报 -32602
- [ ] 缺 `id` 时报 -32600
- [ ] WebSocket `/jsonrpc` 接受 text frame JSON
- [ ] 事件触发时主动推送 `aria2.onXxx` 通知
- [ ] `system.multicall` 接受 `[{methodName, params}, ...]`，递归调用返回错误
- [ ] `shutdown` 返回 "OK" 后 3 秒才真正退出
- [ ] 通知不带 `id` 字段
