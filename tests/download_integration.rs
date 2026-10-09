//! 下载引擎集成测试。
//!
//! 测试分三层：
//! 1. **内容正确性** —— 各种模式（分段/单流/内存/磁盘/压缩流式）下，最终文件
//!    必须逐字节等于源数据；
//! 2. **协议行为** —— 借助测试服务器的请求观测日志（Range/连接号/时刻）断言
//!    引擎「怎么下」：确实分段并行、续传真的从已写字节处继续（而非从头重下）、
//!    403/429 触发任务全局冷却、跨段连接复用（请求数多于连接数）等；
//! 3. **故障恢复** —— 截断响应、下载中途换源、读超时、重定向、持续 5xx 等
//!    异常路径必须有界收敛且不损坏数据。
mod common;

use common::{ReqRecord, TestServer};
use gkdl::app::db::StateDb;
use gkdl::download::config::DownloadConfig;
use gkdl::download::engine;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;

fn make_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn out_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("gkdl_it");
    std::fs::create_dir_all(&dir).ok();
    dir.join(format!("{}_{}.bin", name, std::process::id()))
}

fn remove_quiet(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.gkdl", path.display()));
    let _ = std::fs::remove_file(format!("{}.gkdl.tmp", path.display()));
}

fn temp_db(name: &str) -> Arc<StateDb> {
    let p =
        std::env::temp_dir()
            .join("gkdl_it")
            .join(format!("{}_{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    Arc::new(StateDb::open(&p).unwrap())
}

/// 请求日志中某路径的「段请求」Range 列表（排除探测请求 Range 0-0 与无 Range 请求）。
fn segment_ranges(log: &[ReqRecord], path: &str) -> Vec<(u64, u64)> {
    log.iter()
        .filter(|r| r.method == "GET" && r.path == path)
        .filter_map(|r| r.range)
        .filter(|&r| r != (0, 0))
        .collect()
}

fn assert_file_matches(path: &std::path::Path, data: &[u8], what: &str) {
    let written = std::fs::read(path).unwrap_or_else(|e| panic!("读输出文件失败: {e}"));
    assert_eq!(written, data, "{what}内容不一致");
}

// ============================================================================
// 一、内容正确性 + 分段协议行为
// ============================================================================

/// 多线程分段下载：内容逐字节一致，且确实以多个 Range 请求在多条连接上并行下载，
/// 而不是退化成单流。
#[tokio::test]
async fn multi_thread_range_download_matches() {
    let data = make_data(1_048_576); // 1 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("range");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "分段下载");

    // 协议行为：至少 4 个段请求；有非 0 起点（真的分段了）也有文件尾；
    // 并行下载应使用多条连接。
    let log = server.request_log().await;
    let mut ranges = segment_ranges(&log, "/file.bin");
    assert!(
        ranges.len() >= 4,
        "应有至少 4 个段请求（含窃取碎片），实际 {ranges:?}"
    );
    ranges.sort();
    assert_eq!(ranges[0].0, 0, "第一个段应从 0 开始: {ranges:?}");
    assert!(
        ranges.iter().any(|&(s, _)| s > 0),
        "应有非 0 起点的段请求（证明分段下载）: {ranges:?}"
    );
    assert!(
        ranges.iter().any(|&(_, e)| e == data.len() as u64 - 1),
        "应有覆盖到文件尾的段请求: {ranges:?}"
    );
    assert!(
        server.connection_count() >= 3,
        "分段下载应使用多条并行连接，实际 {}",
        server.connection_count()
    );
    remove_quiet(&out);
}

/// 多源探测级故障转移：前两个源 404，第三个源正常，最终内容一致。
#[tokio::test]
async fn multi_source_failover_skips_bad_url() {
    let data = make_data(512 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("failover");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        ..Default::default()
    };
    let urls = vec![server.fail_url(), server.broken_url(), server.url()];
    let report = engine::download(urls, Some(out.clone()), config, None, None, |_| {})
        .await
        .expect("多源下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "多源下载");
    remove_quiet(&out);
}

/// 服务器不支持 Range 且禁用压缩：降级为单连接整文件下载（磁盘模式）。
/// 下载请求不得携带 Range 头，且只有一个下载请求（探测之外）。
#[tokio::test]
async fn no_range_support_falls_back_to_single_stream_download() {
    let data = make_data(1024 * 1024);
    let server = TestServer::start(data.clone(), true).await; // ignore_range
    let out = out_path("norange");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 8,
        min_split_size: 32 * 1024,
        memory_threshold: 0,      // 磁盘模式
        allow_compression: false, // 走单流降级而非压缩流式
        ..Default::default()
    };
    let report = engine::download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("无 Range 服务器下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "单流降级");

    let log = server.request_log().await;
    // 探测到 200 后，生产代码还会补发一个 HEAD 兜底取总长——只统计 GET
    let downloads: Vec<_> = log
        .iter()
        .filter(|r| r.method == "GET" && r.path == "/file.bin" && r.range.is_none())
        .collect();
    assert_eq!(
        downloads.len(),
        1,
        "应恰好一个无 Range 的整文件下载请求，日志: {log:?}"
    );
    assert!(
        segment_ranges(&log, "/file.bin").is_empty(),
        "服务器不支持 Range 时不得发出分段 Range 请求"
    );
    remove_quiet(&out);
}

/// SHA-256 校验：正确 hash 通过；错误 hash 下载成功但校验失败并报错。
#[tokio::test]
async fn sha256_verification_passes_and_fails() {
    let data = make_data(256 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let config = DownloadConfig::default();
    let digest = hex::encode(Sha256::digest(&data));

    // 正确 hash：下载 + 校验通过
    let out1 = out_path("hash_ok");
    remove_quiet(&out1);
    engine::download(
        vec![server.url()],
        Some(out1.clone()),
        config.clone(),
        Some(digest),
        None,
        |_| {},
    )
    .await
    .expect("正确 hash 应通过");
    remove_quiet(&out1);

    // 错误 hash：报 SHA-256 错误
    let out2 = out_path("hash_bad");
    remove_quiet(&out2);
    let err = engine::download(
        vec![server.url()],
        Some(out2.clone()),
        config,
        Some("0".repeat(64)),
        None,
        |_| {},
    )
    .await
    .expect_err("错误 hash 应失败");
    assert!(
        err.to_string().contains("SHA-256"),
        "错误信息应提及 SHA-256: {err}"
    );
    remove_quiet(&out2);
}

// ============================================================================
// 二、断点续传 / 故障恢复
// ============================================================================

/// 磁盘模式（memory_threshold=0）下中断后从状态库续传。
/// 关键断言：续传阶段的段请求 Range 必须恰好从状态库记录的已写字节处开始
/// （跳过已完成前缀）——若引擎回归成「从头重下」，此测试会失败。
#[tokio::test]
async fn interrupted_download_resumes_from_written_offset() {
    let data = make_data(4 * 1024 * 1024); // 4 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("resume_it");
    remove_quiet(&out);
    let db = temp_db("resume_it");

    // 第一次：单连接限速下载（4 MiB @ 2 MiB/s ≈ 2s），约 1.2s 后取消
    let config = DownloadConfig {
        split: 1,
        min_split_size: 64 * 1024,
        rate_limit: 2 * 1024 * 1024,
        memory_threshold: 0,
        allow_compression: false,
        ..Default::default()
    };
    let handle = engine::start_download(
        vec![server.url()],
        Some(out.clone()),
        config.clone(),
        None,
        None,
        tokio_util::sync::CancellationToken::new(),
        Some(Arc::clone(&db)),
        Some("resume_test_gid".into()),
        |_| {},
    )
    .await
    .expect("启动下载失败");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle.cancel();
    let _ = handle.join().await; // 取消后返回 Err，忽略

    // 取消后状态库中应有 paused 记录，且保存了可观的段进度
    let rec = db
        .load_download("resume_test_gid")
        .unwrap()
        .expect("应有记录");
    assert_eq!(rec.status, "paused", "取消后应为 paused");
    assert_eq!(rec.segments.len(), 1, "split=1 应只有一个段记录");
    let written = rec.segments[0].written;
    assert!(
        written > 256 * 1024,
        "取消前应已下载可观字节且被状态库保存，实际 {written}"
    );
    assert!(
        written < data.len() as u64,
        "取消时不应已下载完成: {written}"
    );

    // 第二次：续传并完成
    let before = server.request_log().await.len();
    let handle = engine::start_download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        tokio_util::sync::CancellationToken::new(),
        Some(Arc::clone(&db)),
        Some("resume_test_gid".into()),
        |_| {},
    )
    .await
    .expect("续传启动失败");
    let report = handle.join().await.expect("续传下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "续传");
    assert_eq!(
        db.load_download("resume_test_gid").unwrap().unwrap().status,
        "complete"
    );

    // 关键断言：续传只发一个段请求，Range 起点恰为状态库记录的 written
    let log = server.request_log().await;
    let ranges = segment_ranges(&log[before..], "/file.bin");
    assert_eq!(
        ranges,
        vec![(written, data.len() as u64 - 1)],
        "续传应只请求未完成区间 [{}..{}]，实际 {ranges:?}",
        written,
        data.len() - 1
    );
    remove_quiet(&out);
}

/// 截断响应（Content-Length 报全长但提前断开）：段失败后指数退避重试，
/// 重试请求必须从已写字节处（截断点）发起 Range 续传，最终内容一致。
/// （split=1 排除工作窃取干扰——多段下的窃取交互由连接复用测试覆盖。）
#[tokio::test]
async fn truncated_segments_retry_and_resume_from_offset() {
    let data = make_data(1024 * 1024); // 1 MiB，单段
    let server = TestServer::start_truncated(data.clone(), 1).await;
    let out = out_path("trunc_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 1,
        min_split_size: 64 * 1024,
        memory_threshold: 0,
        allow_compression: false,
        ..Default::default()
    };
    engine::download(
        vec![server.truncated_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("截断重试后应成功");
    assert_file_matches(&out, &data, "截断重试");

    // 请求序列：初始段（在所请求区间的 40% 处被截断）
    //          + 重试段（Range 恰从截断点继续，不从头重下）
    let total = data.len() as u64;
    let cut = total * 2 / 5; // 服务器截断点：所请求区间的前 40%
    let actual = segment_ranges(&server.request_log().await, "/truncated");
    assert_eq!(
        actual,
        vec![(0, total - 1), (cut, total - 1)],
        "应为 1 个初始段 + 1 个从截断点续传的重试段"
    );
    remove_quiet(&out);
}

/// 下载中途源失效：worker 在源 A 截断失败后换到源 B，且必须从源 A 停下的
/// 精确字节偏移继续（段进度跨源保留），而不是在源 B 从头重下。
#[tokio::test]
async fn mid_download_failover_resumes_at_exact_offset_on_next_source() {
    let data = make_data(512 * 1024);
    let server = TestServer::start_flaky(data.clone(), 1000).await;
    let out = out_path("failover_mid");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 1,
        min_split_size: 64 * 1024,
        memory_threshold: 0,
        allow_compression: false,
        ..Default::default()
    };
    let urls = vec![server.flaky_url(), server.url()];
    engine::download(urls, Some(out.clone()), config, None, None, |_| {})
        .await
        .expect("换源续传下载失败");
    assert_file_matches(&out, &data, "换源续传");

    let log = server.request_log().await;
    let flaky_segs = segment_ranges(&log, "/flaky");
    assert_eq!(
        flaky_segs,
        vec![(0, data.len() as u64 - 1)],
        "源 A 应收到恰好一个整段请求（在 1000 字节处被截断）: {flaky_segs:?}"
    );
    let good_segs = segment_ranges(&log, "/file.bin");
    assert_eq!(
        good_segs,
        vec![(1000, data.len() as u64 - 1)],
        "源 B 应从源 A 停下的偏移 1000 处继续: {good_segs:?}"
    );
    remove_quiet(&out);
}

/// 读超时：服务器接受下载请求后挂起不响应，引擎必须在 timeout 秒级失败退出，
/// 而不是无限挂起。外层 30s 兜底把「挂死」回归变成测试失败。
#[tokio::test]
async fn hung_download_fails_within_read_timeout() {
    let data = make_data(1024 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("hang_it");
    remove_quiet(&out);
    let url = format!("http://{}/hang_download", server.addr);

    let config = DownloadConfig {
        split: 1,
        min_split_size: 64 * 1024,
        timeout: 5,     // worker 读超时下限 5s
        max_retries: 1, // 一次失败即放弃，避免反复等超时
        memory_threshold: 0,
        allow_compression: false,
        ..Default::default()
    };
    let t0 = std::time::Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        engine::download(vec![url], Some(out.clone()), config, None, None, |_| {}),
    )
    .await
    .expect("引擎应在读超时内退出，而非挂死");
    let elapsed = t0.elapsed();
    let err = result.expect_err("挂起的下载应失败");
    assert!(err.to_string().contains("下载失败"), "应报下载失败: {err}");
    assert!(
        elapsed >= Duration::from_millis(4500),
        "应等待读超时（~5s）而非立即失败: {elapsed:?}"
    );
    remove_quiet(&out);
}

/// 302 重定向：探测与分段请求都应跟随到最终地址，最终内容正确。
#[tokio::test]
async fn redirect_is_followed_to_final_content() {
    let data = make_data(300 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("redirect_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 2,
        min_split_size: 32 * 1024,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.redirect_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("重定向下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "重定向下载");

    let log = server.request_log().await;
    let redirected = log.iter().filter(|r| r.path == "/redirect").count();
    let final_hits = log.iter().filter(|r| r.path == "/file.bin").count();
    assert!(
        redirected >= 2,
        "探测与段请求都应先命中重定向路径: {redirected}"
    );
    assert!(
        final_hits >= redirected,
        "每次重定向都应跟随到最终路径: {redirected} → {final_hits}"
    );
    remove_quiet(&out);
}

// ============================================================================
// 三、限速 / 反爬（全局冷却 + 连接复用）
// ============================================================================

/// 限速吞吐：1 MiB @ 512 KiB/s 理想耗时 ~2s。
/// 下界证明限速确实生效，上界证明限速器没有把下载挂死。
#[tokio::test]
async fn rate_limit_slows_download() {
    let data = make_data(1_048_576); // 1 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("ratelimit");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        rate_limit: 512 * 1024, // 512 KB/s
        ..Default::default()
    };
    let t0 = std::time::Instant::now();
    engine::download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("限速下载失败");
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1500),
        "1 MiB @ 512 KB/s 至少需 ~2s，实际 {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "限速下载不应长时间挂死: {elapsed:?}"
    );
    assert_file_matches(&out, &data, "限速下载");
    remove_quiet(&out);
}

/// HTTP 403 与 429 都触发任务全局冷却：重试被拉开 ≥ 冷却基数的间隔，
/// 且恰好「探测 + 1 次拒绝 + 1 次重试」3 个请求（不形成重试风暴）。
#[tokio::test]
async fn http_rate_limit_status_triggers_global_cooldown() {
    for status in [403u16, 429] {
        let data = make_data(64 * 1024);
        let server = TestServer::start_block_status(data.clone(), 1, status).await;
        let out = out_path("cooldown");
        remove_quiet(&out);

        let config = DownloadConfig {
            split: 1,
            min_split_size: 16 * 1024,
            max_retries: 3,
            cooldown_base_ms: 1500,
            cooldown_max_ms: 10_000,
            timeout: 10,
            allow_compression: false,
            ..Default::default()
        };
        let report = engine::download(
            vec![server.block_url()],
            Some(out.clone()),
            config,
            None,
            None,
            |_| {},
        )
        .await
        .unwrap_or_else(|e| panic!("HTTP {status} 冷却后重试应成功: {e}"));
        assert_eq!(report.total as usize, data.len());
        assert_file_matches(&out, &data, "限流重试");

        // 请求序列：探测(206) + 首段 403/429 + 冷却后重试 206
        let times = server.block_request_times().await;
        assert_eq!(
            times.len(),
            3,
            "HTTP {status}: 应恰好 3 次请求（探测/拒绝/重试），实际: {}",
            times.len()
        );
        let gap = times[2].duration_since(times[1]);
        assert!(
            gap >= Duration::from_millis(1300),
            "HTTP {status}: 重试应被冷却拉开 ≥1.3s，实际 {gap:?}"
        );
        assert!(
            gap < Duration::from_secs(10),
            "HTTP {status}: 冷却不应等满上限: {gap:?}"
        );
        remove_quiet(&out);
    }
}

/// 持续 HTTP 500（非限流信号）不得触发全局冷却：按段级退避（500ms）快速重试、
/// 耗尽后放弃。冷却基数配成 10s——若 500 被误当限流，耗时会暴涨到 10s+。
#[tokio::test]
async fn http_500_fails_fast_without_cooldown() {
    let data = make_data(64 * 1024);
    let server = TestServer::start_block_status(data.clone(), usize::MAX, 500).await;
    let out = out_path("err500");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 1,
        min_split_size: 16 * 1024,
        max_retries: 2,
        cooldown_base_ms: 10_000,
        cooldown_max_ms: 60_000,
        timeout: 10,
        allow_compression: false,
        ..Default::default()
    };
    let t0 = std::time::Instant::now();
    let err = engine::download(
        vec![server.block_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect_err("持续 500 应失败");
    let elapsed = t0.elapsed();
    assert!(err.to_string().contains("下载失败"), "应报下载失败: {err}");
    assert!(
        elapsed >= Duration::from_millis(450),
        "应至少经历一次段级退避（500ms）: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(6),
        "500 不应触发 10s 全局冷却（仅段级退避）: {elapsed:?}"
    );
    let times = server.block_request_times().await;
    assert_eq!(
        times.len(),
        3,
        "应恰好 3 次请求（探测 + 2 次段尝试），实际: {}",
        times.len()
    );
    remove_quiet(&out);
}

/// 反爬核心行为：跨段连接复用。错峰延迟（第 N 条连接首字节延迟 N×400ms）
/// 制造确定性的工作窃取：先完成的 worker 在同一条连接上顺序处理多个段。
/// keep-alive 下请求数必须严格多于连接数——若回归成「每段新建连接」
/// （正是触发远端 WAF 的爬虫特征），此测试失败。
#[tokio::test]
async fn connection_reuse_serves_many_requests_over_few_connections() {
    let data = make_data(2 * 1024 * 1024);
    let server = TestServer::start_staggered(data.clone(), 400).await;
    let out = out_path("reuse_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        memory_threshold: 0,
        allow_compression: false,
        ..Default::default()
    };
    engine::download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("下载失败");
    assert_file_matches(&out, &data, "连接复用下载");

    let log = server.request_log().await;
    let requests = log.len();
    let conns = server.connection_count();
    assert!(
        requests >= 6,
        "探测 + 4 个初始段 + 至少 1 次窃取，实际 {requests} 个请求"
    );
    assert!(
        conns < requests,
        "跨段复用下连接数应严格少于请求数（反爬核心行为）: {conns} 连接 / {requests} 请求"
    );
    let mut per_conn: std::collections::HashMap<usize, usize> = std::collections::HashMap::new();
    for r in &log {
        *per_conn.entry(r.conn).or_insert(0) += 1;
    }
    let max_per_conn = per_conn.values().copied().max().unwrap_or(0);
    assert!(
        max_per_conn >= 2,
        "应有单条连接承载 ≥2 个请求（连接被复用）: {per_conn:?}"
    );
    remove_quiet(&out);
}

// ============================================================================
// 四、内存模式 / 流式（压缩）模式
// ============================================================================

/// 内存模式：下载期间不产生输出文件，完成后原子落盘。
#[tokio::test]
async fn small_file_memory_mode_no_file_until_done() {
    let data = make_data(256 * 1024); // 256 KiB < 8 MiB 默认阈值
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("memory_it");
    remove_quiet(&out);
    let db = temp_db("memory_it");

    let config = DownloadConfig {
        split: 4,
        rate_limit: 256 * 1024,   // 256 KB/s → 256 KiB 约 1s，留足观察窗口
        allow_compression: false, // 关闭压缩，保证走内存模式
        ..Default::default()
    };
    let handle = engine::start_download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        tokio_util::sync::CancellationToken::new(),
        Some(Arc::clone(&db)),
        Some("memory_gid".into()),
        |_| {},
    )
    .await
    .expect("启动下载失败");

    // 下载进行中：输出文件不应存在（内存模式不预分配）
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !out.exists(),
        "内存模式下载中不应产生输出文件: {}",
        out.display()
    );

    handle.join().await.expect("下载失败");
    assert!(out.exists(), "下载完成后应写入输出文件");
    assert_file_matches(&out, &data, "内存模式");
    // 无 .gkdl 控制文件、无临时文件残留
    assert!(
        !std::path::Path::new(&format!("{}.gkdl", out.display())).exists(),
        "不应存在控制文件"
    );
    assert!(!out.with_extension("gkdl.tmp").exists(), "不应残留临时文件");
    // 状态库记录 memory_mode 标记
    let rec = db.load_download("memory_gid").unwrap().unwrap();
    assert!(rec.memory_mode, "应为内存模式记录");
    assert_eq!(rec.status, "complete");
    remove_quiet(&out);
}

/// 阈值设 0（禁用内存模式）：小文件也走磁盘（下载中文件即存在且预分配全尺寸）。
#[tokio::test]
async fn memory_mode_disabled_uses_disk() {
    let data = make_data(256 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nmemory");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        rate_limit: 2 * 1024 * 1024,
        memory_threshold: 0,      // 禁用内存模式
        allow_compression: false, // 关闭压缩，保证走磁盘预分配
        ..Default::default()
    };
    let handle = engine::start_download(
        vec![server.url()],
        Some(out.clone()),
        config,
        None,
        None,
        tokio_util::sync::CancellationToken::new(),
        None,
        None,
        |_| {},
    )
    .await
    .expect("启动下载失败");

    // 磁盘模式：文件预分配，下载中即存在
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(out.exists(), "磁盘模式下载中应已创建文件");
    let meta = std::fs::metadata(&out).unwrap();
    assert_eq!(meta.len(), data.len() as u64, "磁盘模式预分配全尺寸");

    handle.join().await.expect("下载失败");
    assert_file_matches(&out, &data, "磁盘模式");
    remove_quiet(&out);
}

/// 慢服务器路径：/slow 响应前等待 delay_ms，验证下载仍能成功完成
#[tokio::test]
async fn slow_server_path_download_completes() {
    let data = make_data(512 * 1024); // 512 KiB
    let server = TestServer::start_slow(data.clone(), false, 200).await;
    let out = out_path("slow_path");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        // 短宽限期以便慢线程检测能快速触发
        grace_period: 0.5,
        slow_confirm: 3,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.url_slow()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("慢服务器下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "慢服务器下载");
    remove_quiet(&out);
}

/// 慢服务器与失败路径混合：验证多源故障转移时慢路径仍能作为备选源
#[tokio::test]
async fn slow_server_with_multi_source_failover() {
    let data = make_data(256 * 1024);
    let server = TestServer::start_slow(data.clone(), false, 100).await;
    let out = out_path("slow_failover");
    remove_quiet(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 32 * 1024,
        grace_period: 0.5,
        ..Default::default()
    };
    let urls = vec![server.fail_url(), server.url_slow(), server.url()];
    let report = engine::download(urls, Some(out.clone()), config, None, None, |_| {})
        .await
        .expect("多源慢服务器下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "多源慢服务器下载");
    remove_quiet(&out);
}

/// gzip 压缩下载：服务器回 `Content-Encoding: gzip`，引擎应请求压缩并自动解压得到原始内容。
#[tokio::test]
async fn gzip_streaming_download_decompresses() {
    let data = make_data(512 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("gzip_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        allow_compression: true,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.gzip_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("gzip 下载失败");
    // 探测拿到的是原始大小（identity），流式解压后应为相同字节数
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "gzip 解压");

    // 服务器应收到过带 gzip 的 Accept-Encoding
    let enc = server.request_headers("accept-encoding").await;
    assert!(
        enc.iter().any(|v| v.to_ascii_lowercase().contains("gzip")),
        "应请求 gzip 编码，实际: {enc:?}"
    );
    assert!(!out.with_extension("gkdl.tmp").exists(), "不应残留临时文件");
    remove_quiet(&out);
}

/// 未知大小下载：服务器不提供 Content-Length，引擎应走流式整文件下载并成功。
#[tokio::test]
async fn unknown_size_streaming_download() {
    let data = make_data(300 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nosize_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        allow_compression: true,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.nosize_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("未知大小下载失败");
    // 报告总长为实际写出的字节数
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "未知大小下载");
    remove_quiet(&out);
}

/// 关闭压缩：即使服务器支持 gzip，也不应发送 Accept-Encoding: gzip。
#[tokio::test]
async fn compression_disabled_sends_identity() {
    let data = make_data(128 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nocompress_it");
    remove_quiet(&out);

    let config = DownloadConfig {
        allow_compression: false,
        ..Default::default()
    };
    let report = engine::download(
        vec![server.gzip_url()],
        Some(out.clone()),
        config,
        None,
        None,
        |_| {},
    )
    .await
    .expect("关闭压缩下载失败");
    assert_eq!(report.total as usize, data.len());
    assert_file_matches(&out, &data, "关闭压缩");

    // 服务器应未收到 gzip 编码请求
    let enc = server.request_headers("accept-encoding").await;
    assert!(
        !enc.iter().any(|v| v.to_ascii_lowercase().contains("gzip")),
        "关闭压缩后不应请求 gzip，实际: {enc:?}"
    );
    remove_quiet(&out);
}
