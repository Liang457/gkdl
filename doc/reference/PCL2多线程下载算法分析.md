# PCL2 多线程下载算法分析（关键多线程算法版）

> 源码仓库：`Hex-Dragon/PCL2`
> 核心文件：`Plain Craft Launcher 2/Modules/Base/ModNet.vb`（2009 行）
> 分析对象：基于 `main` 分支最新提交 `bbb4fc31a8fe`
> 范围：仅保留「多线程下载算法」主线

---

## 1. 下载主流程（中文时序）

```
业务调用 NetDownloadByLoader(Urls, LocalPath)
  → 构造 NetFile（Urls 经 SecretCdnSign 签名去重 → Sources）
  → 创建 LoaderDownload，启动后台线程
    → NetManager.Start(Me)：登记到全局 AllFiles
    → 已存在文件查重（大小 + Hash）
      → 命中：直接复制，进度计 20%
      → 未命中：置 WaitingToDownload
    → ThreadStarter ×2 + StatRefresher ×1 接管
      → 20ms 轮询 WaitingFiles / OngoingFiles
      → 速度 < SpeedLimitLow 才追加线程
      → 新线程发 Range 请求 → 写 .tmp / MemoryStream
      → 限速令牌桶：100ms replenish，写前检查余量
      → 完成 → Merge（按链表顺序拼接 .tmp）
      → 校验大小/Hash → Finish / Fail
```

关键入口：
- `NetDownloadByLoader`：`ModNet.vb:200`
- `LoaderDownload.Start`：`ModNet.vb:1472-1549`
- `NetFile.Thread`（下载主循环）：`ModNet.vb:975-1145`
- `NetManager.Start`：`ModNet.vb:1878-1915`
- `StartManager`（调度线程）：`ModNet.vb:1795-1871`
- `Merge`：`ModNet.vb:1239-1323`

---

## 2. 多线程拆分策略

### 2.1 拆分时机

两个 `ThreadStarter` 后台线程每 20ms 轮询触发（`ModNet.vb:1801-1845`）：

1. **WaitingFiles**：尚未建链的文件，直接启动新线程
2. **OngoingFiles**：正在下载的文件，仅当全局速度低于 `NetTaskSpeedLimitLow` 时才追加

### 2.2 拆分逻辑

核心方法 `TryBeginThread()`（`ModNet.vb:887-936`）：

```vb
' 伪代码
If NetTaskThreadCount >= NetTaskThreadLimit Then Return Nothing
If Not HasAvailableSource() Then Return Nothing

If Threads Is Nothing Then
    ' 首个线程：从 0 开始
    StartPosition = 0
    StartSource = FirstThreadSource 的下一可用源
ElseIf 存在已取消但有余量的线程 Then
    ' 失败点续传
    StartPosition = FailedThread.DownloadStart + FailedThread.DownloadDone
    StartSource = 失败线程源的后一可用源
Else
    ' 动态扩碎片：找剩余最大的线程
    FilePieceMax = 找 DownloadUndone 最大的线程
    If FilePieceMax.DownloadUndone < FilePieceLimit(256KB) Then Return Nothing
    StartPosition = FilePieceMax.DownloadEnd - FilePieceMax.DownloadUndone * 0.4
End If

' 创建 NetThread 并插入链表
```

### 2.3 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `FilePieceLimit` | 256 KB | 最小碎片大小，小于此值不再拆分 |
| 动态切割比例 | 40% | 从最大碎片末尾往前 40% 处切开 |
| BMCLAPI 限流 | 100 ms | 新启线程后若源含 bmclapi 域名则 Sleep |
| 每线程延迟 | 250 ms | `i * 250ms` 避免瞬间并发过多 |

---

## 3. Range 分片下载

每个 `NetThread` 发请求时（`ModNet.vb:991`）：

```vb
If Not Th.IsFirstThread Then
    HttpRequest.Headers.Range = New Headers.RangeHeaderValue(Th.DownloadStart, Nothing)
End If
```

- **首个线程**：不带 Range，用于探测文件大小和校验
- **后续线程**：带 `Range: bytes=Start-`，从指定位置下载到文件末尾
- **服务端验证**：检查 `ContentLength == FileSize - DownloadStart`，不一致则抛 `RangeNotSupportedException`

### 3.1 大小获取流程

1. 首个线程连接成功，读取 `ContentLength`
2. `ContentLength < 0`：文件大小未知，`IsUnknownSize = True`，所有线程默认下载到 1TB 虚拟终点
3. `ContentLength >= 0`：设置 `FileSize`，后续线程验证预期大小

---

## 4. 动态线程数控制

### 4.1 全局调度

`StartManager` 启动 **3 个常驻线程**（`ModNet.vb:1847-1849`）：

| 线程 | 行号 | 职责 |
|------|------|------|
| `ThreadStarter(0)` | 1847 | 处理 `File.Uuid Mod 2 = 0` 的文件 |
| `ThreadStarter(1)` | 1848 | 处理 `File.Uuid Mod 2 = 1` 的文件 |
| `StatRefresher` | 1849 | 100ms tick 刷新统计 + 限速令牌桶 |

### 4.2 线程启动器主循环

```text
while true
    sleep 20ms
    把 AllFiles 按 Uuid 奇偶分到 WaitingFiles / OngoingFiles
    for file in WaitingFiles:
        if NetTaskThreadCount >= NetTaskThreadLimit: break
        file.TryBeginThread()
    if Speed >= NetTaskSpeedLimitLow: continue
    for file in OngoingFiles:
        if PreparingCount > DownloadingCount: continue  ' 避免空跑
        file.TryBeginThread()
```

### 4.3 速度采样

- 记录最近 30 次速度采样
- 加权平均计算显示速度（越新权重越高）
- 取近 1 秒平均速度的 85% 作为动态速度下限 `NetTaskSpeedLimitLow`

### 4.4 停止追加条件

- `NetTaskThreadCount >= NetTaskThreadLimit`
- `Speed >= NetTaskSpeedLimitLow`
- `PreparingCount > DownloadingCount`

---

## 5. 多源容错与降级

### 5.1 数据结构

```vb
Public Class NetFile
    Public Sources As List(Of NetSource)        ' 主源列表，支持多线程
    Public SourcesOnce As ConcurrentList(Of NetSource)  ' 降级单线程源列表
    Private FirstThreadSource As Integer = 0    ' 首个线程使用的源索引
End Class
```

### 5.2 源切换策略

| 场景 | 行为 |
|------|------|
| 首个线程失败 | `FirstThreadSource` 递增，下一个线程换源 |
| 某个线程失败 | 从 `Source.Id + 1` 开始找下一个可用源 |
| 源被禁用 | 移入 `SourcesOnce`，只能单线程使用 |
| 全部源重试 | 首次下载/合并失败时，所有源降级为单线程模式 |

### 5.3 源禁用条件

满足以下任一条件时，源被标记 `IsFailed`（`ModNet.vb:1155-1160`）：

- 返回 416（Range Not Satisfied）
- 返回 502/404
- 返回 403/429（非 BMCLAPI 源）
- "未能解析" / "无返回数据" / "空间不足"
- 连续失败次数 >= `NetTaskThreadLimit` 且下载量 < 1B
- 总失败次数 > `NetTaskThreadLimit + 2`

### 5.4 单线程退化重试

触发条件（`ModNet.vb:1181-1196`）：

```vb
If Not Retried Then
    Retried = True
    SourcesOnce.Clear()
    For Each Source In Sources
        SourcesOnce.Add(Source)
        Source.IsFailed = True
    Next
    Reset()  ' 重置文件大小、清空线程、清零进度
    State = NetState.WaitingToDownload
End If
```

---

## 6. 限速算法

PCL2 使用 **令牌桶限速**（`ModNet.vb:1070-1074` + `:1855`）：

### 6.1 参数

| 参数 | 说明 |
|------|------|
| `NetTaskSpeedLimitHigh` | 用户设置的上限，-1 表示不限速 |
| `NetTaskSpeedLimitLeft` | 当前可用余量，初始为 -1（不限） |
| replenish 间隔 | 100ms |
| replenish 量 | `High / 10` |

### 6.2 限速逻辑

```vb
' 每次写数据前检查
While NetTaskSpeedLimitHigh > 0 AndAlso NetTaskSpeedLimitLeft <= 0
    Thread.Sleep(16)
End While

' 消耗令牌
Interlocked.Add(NetTaskSpeedLimitLeft, -RealDataCount)
```

### 6.3 速度档位

用户设置值映射到实际速度（`ModNet.vb:396-412`）：

| 设置值 | 实际限速 |
|--------|----------|
| 0~14 | `(值+1) * 0.1 MB/s` |
| 15~31 | `(值-11) * 0.5 MB/s` |
| 32~41 | `(值-21) * 1 MB/s` |
| 42+ | 不限速 |

---

## 7. 合并与校验

### 7.1 合并触发条件

```vb
If (DownloadDone >= FileSize OrElse (FileSize = -1 AndAlso DownloadDone > 0)) _
   AndAlso State < NetState.Merging _
   AndAlso Th.State <> NetState.Canceled Then
    Merge(Th)
End If
```

### 7.2 合并策略

| 情况 | 行为 |
|------|------|
| `IsNoSplit` | 从内存 `MemoryStream` 直接写入 |
| 仅有一个线程 | 直接复制 `.tmp` 文件 |
| 多个线程 | 按链表顺序读取每个 `.tmp`，写入最终文件 |

### 7.3 合并失败重试

- 最多重试 3 次
- 每次间隔 `500ms * 重试次数`
- 重试失败则禁用当前源并重启下载

### 7.4 校验

```vb
' 大小校验
If Check.ActualSize <> FileSize AndAlso Check.ActualSize > 0 Then Throw

' Hash 校验
CheckResult = Check.Check(LocalPath)
If CheckResult IsNot Nothing Then Throw New Exception(CheckResult)
```

---

## 8. 已存在文件复用

`LoaderDownload.Start()` 启动后会先扫描 MC 文件夹（`ModNet.vb:1513-1544`）。

### 8.1 扫描策略

```vb
' 平均分配到最多 8 个检查线程
ThreadCount = (FilesToCheck.Count \ 40).Clamp(1, 8)
```

### 8.2 分类查找

| 文件路径特征 | 查找方式 |
|-------------|----------|
| `\assets\` | 在 MC 文件夹下按相同相对路径查找 |
| `\libraries\` | 同上 |
| `\versions\` | 在各版本文件夹下按扩展名查找 |
| `\mods\` 等 | 按类别文件夹遍历，要求有 Hash |

### 8.3 匹配条件

1. 文件大小一致（快速过滤）
2. Hash 校验通过（精确验证）
3. 命中则直接复制，进度算 20%

---

## 9. 与 aria2 的差异

| 维度 | PCL2（ModNet.vb） | aria2 |
|------|------------------|-------|
| 形态 | 进程内 .NET HttpClient + Thread | 独立进程 + RPC |
| 协议 | HTTP/HTTPS | HTTP/HTTPS/FTP/SFTP/BT/Metalink |
| 多源 | `Sources` 轮询 + `SourcesOnce` 单线程回退 | `--max-tries` + 多种选择策略 |
| 切片 | 链式 `NetThread`，按 256KB 动态触发 | 固定 `--split=N`，可按 piece size 自适应 |
| 续传 | `Range: bytes=Start-` | 同 |
| 限速 | 令牌桶，写前忙等 | `--max-overall-download-limit` |
| 速度下限 | 动态上调，慢才追加线程 | 无（被动接受新连接） |
| 校验 | 合并后 `FileChecker.Check` | 完整性校验 + `.aria2` 控制文件 |
| DNS | 自实现 `DNSLookup`，按 `IPReliability` 选 IP | `--async-dns` + 缓存 |
| 重定向 | 手动替换 `RequestUri.Host` 钉 IP | 跟随系统解析 |
| 平台依赖 | 强依赖 Windows | 跨平台 |
| 第三方 | 无外部可执行 | `aria2c.exe` / 独立服务 |

---

## 10. 关键源码摘录

### 10.1 线程启动核心

```vb
' 文件：ModNet.vb，约第 881 行
Public Function TryBeginThread() As NetThread
    ' 1. 检查全局线程配额
    If NetTaskThreadCount >= NetTaskThreadLimit Then Return Nothing
    
    ' 2. 检查是否有可用源
    If Not HasAvailableSource() Then Return Nothing
    
    ' 3. 确定线程的起始位置和下载源
    If IsNoSplit Then
        StartPosition = 0
    ElseIf 存在已取消但有余量的线程 Then
        StartPosition = FailedThread.DownloadStart + FailedThread.DownloadDone
    Else
        FilePieceMax = 找 DownloadUndone 最大的线程
        If FilePieceMax.DownloadUndone < FilePieceLimit(256KB) Then Return Nothing
        StartPosition = FilePieceMax.DownloadEnd - FilePieceMax.DownloadUndone * 0.4
    End If
    
    ' 4. 创建线程并插入链表
    ' 5. 返回 NetThread
End Function
```

### 10.2 下载主循环

```vb
' 文件：ModNet.vb，约第 1067 行
While (IsUnknownSize OrElse Th.DownloadUndone > 0) AndAlso
      HttpDataCount > 0 AndAlso Not IsProgramEnding AndAlso
      State < NetState.Merging AndAlso
      (Not Th.Source.IsFailed OrElse Th.Source.SingleThread = Th)
    ' 限速等待
    While NetTaskSpeedLimitHigh > 0 AndAlso NetTaskSpeedLimitLeft <= 0
        Thread.Sleep(16)
    End While
    
    ' 写入数据
    ResultStream.Write(ResponseBytes, 0, RealDataCount)
    
    ' 速度过慢检测
    If DeltaTime > 5000 AndAlso DeltaTime > RealDataCount AndAlso
       Th.Source.SingleThread Is Nothing Then
        Throw New TimeoutException("由于速度过慢断开链接")
    End If
    
    ' 继续读取
    HttpDataCount = ResponseStream.ReadAsync(...)
End While
```

### 10.3 源失败处理

```vb
' 文件：ModNet.vb，约第 1146 行
Private Sub SourceFail(Th As NetThread, ex As Exception, IsMergeFailure As Boolean)
    ' 增加失败计数
    Interlocked.Increment(Th.Source.FailCount)
    Th.State = NetState.Canceled
    
    ' 判断是否禁用源
    If IsRangeNotSupported OrElse
       ex.Message.Contains("(502)") OrElse
       Th.Source.FailCount >= NetTaskThreadLimit.Clamp(5, 30) Then
        
        ' 标记源为失败
        Th.Source.IsFailed = True
        SourcesOnce.Remove(Th.Source)
        
        ' 还有可用源则继续，否则全量重试或最终失败
        If HasAvailableSource() AndAlso Not IsMergeFailure Then
            ' 继续
        ElseIf Not Retried Then
            ' 全量重试
            Retried = True
            SourcesOnce.Clear()
            For Each Source In Sources
                SourcesOnce.Add(Source)
                Source.IsFailed = True
            Next
            Reset()
            State = NetState.WaitingToDownload
        Else
            Fail(ExampleEx)
        End If
    End If
End Sub
```

---

## 11. 待核条目

1. `NetTaskThreadLimit` 的默认值取决于 `Settings.Get("ToolDownloadThread")` 缺省值 — 需在 `ModBase` / 设置页确认
2. `FilePieceLimit = 256KB` 触发再分片的策略 — 注释明确写为 `FUTURE: 下载引擎重做`，为临时实现
3. `ThreadStarter` 用 `Uuid Mod 2` 分片 — 行为可推断为「分摊锁竞争」，但是否在 2 个线程之外可配置 / 动态扩缩需在 `StartManager` 确认
4. `Merge` 重试 3 次的退避是线性 `500 * RetryCount` ms — 是否在重试之间清空已写盘内容未做确认
5. `HasDownloadingTask` 与自定义下载过滤的判断依赖 `Task.Name.ToString.Contains("自定义下载")` — `Name` 在 `New` 中是字符串，但 `ToString` 行为是否始终如预期需在子类确认
6. `RunInUi() AndAlso Not Url.Contains("//127.")` 在 `SendRequest` 入口直接 `Throw` — 是否所有调用方都在非 UI 线程上需逐一确认
7. `NetRequestByLoader` 伪装成「下载网页文本」会走完整 `LoaderDownload`，因此其行为完全等价于「下载文件然后读出文本」

---

## 算法总结

| 特性 | 实现方式 |
|------|----------|
| 多线程拆分 | 基于 HTTP Range，动态按 40% 比例切割最长碎片 |
| 动态扩缩 | 速度低于下限时自动追加线程，上限可配置 |
| 多源容错 | 首个线程切换源，失败源降级为单线程，全部重试 |
| 限速 | 令牌桶，100ms replenish，写前检查余量 |
| 合并 | 按链表顺序拼接临时文件，失败重试 3 次 |
| 跳过 | 已存在文件通过大小/Hash 匹配后直接复制 |
