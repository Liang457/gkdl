mod common;

use common::TestServer;
use gkdl::app::db::StateDb;
use gkdl::download::config::DownloadConfig;
use gkdl::download::engine;
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

fn temp_db(name: &str) -> Arc<StateDb> {
    let p =
        std::env::temp_dir()
            .join("gkdl_it")
            .join(format!("{}_{}.db", name, std::process::id()));
    let _ = std::fs::remove_file(&p);
    Arc::new(StateDb::open(&p).unwrap())
}

#[tokio::test]
async fn multi_thread_range_download_matches() {
    let data = make_data(1_048_576); // 1 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("range");
    let _ = std::fs::remove_file(&out);

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

    let written = std::fs::read(&out).expect("读输出文件失败");
    assert_eq!(written, data, "下载内容不一致");
    std::fs::remove_file(&out).ok();
}

#[tokio::test]
async fn multi_source_failover_skips_bad_url() {
    let data = make_data(512 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("failover");
    let _ = std::fs::remove_file(&out);

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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "多源下载内容不一致");
    std::fs::remove_file(&out).ok();
}

#[tokio::test]
async fn server_without_range_falls_back_to_single_stream() {
    let data = make_data(300 * 1024);
    let server = TestServer::start(data.clone(), true).await; // ignore_range = true
    let out = out_path("norange");
    let _ = std::fs::remove_file(&out);

    let config = DownloadConfig {
        split: 8,
        min_split_size: 32 * 1024,
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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "单流降级内容不一致");
    std::fs::remove_file(&out).ok();
}

#[tokio::test]
async fn sha256_verification_passes_and_fails() {
    let data = make_data(256 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let config = DownloadConfig::default();

    // 正确 hash
    let out1 = out_path("hash_ok");
    let _ = std::fs::remove_file(&out1);
    let digest = gkdl::app::hash::sha256_file(&{
        std::fs::write(&out1, &data).unwrap();
        out1.clone()
    })
    .unwrap();
    std::fs::remove_file(&out1).ok();
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
    std::fs::remove_file(&out1).ok();

    // 错误 hash
    let out2 = out_path("hash_bad");
    let _ = std::fs::remove_file(&out2);
    let bad = "0".repeat(64);
    let err = engine::download(
        vec![server.url()],
        Some(out2.clone()),
        config,
        Some(bad),
        None,
        |_| {},
    )
    .await;
    assert!(err.is_err(), "错误 hash 应失败");
    assert!(err.unwrap_err().to_string().contains("SHA-256"));
    std::fs::remove_file(&out2).ok();
}

#[tokio::test]
async fn rate_limit_slows_download() {
    let data = make_data(1_048_576); // 1 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("ratelimit");
    let _ = std::fs::remove_file(&out);

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
    std::fs::remove_file(&out).ok();
}

/// 磁盘模式（memory_threshold=0）下，中断后可从状态库续传。
#[tokio::test]
async fn interrupted_download_resumes_from_db() {
    let data = make_data(4 * 1024 * 1024); // 4 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("resume_it");
    let _ = std::fs::remove_file(&out);
    let db = temp_db("resume_it");

    // 第一次：限速慢速下载，约 1 秒后取消（内存阈值 0 → 磁盘模式）
    let config = DownloadConfig {
        split: 4,
        rate_limit: 2 * 1024 * 1024,
        min_split_size: 256 * 1024,
        memory_threshold: 0,
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

    // 取消后状态库中应有 paused 记录（段进度保留）
    let rec = db
        .load_download("resume_test_gid")
        .unwrap()
        .expect("应有记录");
    assert_eq!(rec.status, "paused", "取消后应为 paused");
    let written: u64 = rec.segments.iter().map(|s| s.written).sum();
    assert!(written > 0, "取消后应已下载部分字节");

    // 第二次：重新下载应从记录续传并成功（沿用同一 gid，状态库标记 complete）
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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "续传内容不一致");
    // 完成后状态库应标记 complete
    assert_eq!(
        db.load_download("resume_test_gid").unwrap().unwrap().status,
        "complete"
    );
    std::fs::remove_file(&out).ok();
}

/// 内存模式：下载期间不产生输出文件，完成后原子落盘。
#[tokio::test]
async fn small_file_memory_mode_no_file_until_done() {
    let data = make_data(256 * 1024); // 256 KiB < 8 MiB 默认阈值
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("memory_it");
    let _ = std::fs::remove_file(&out);
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
    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "内存模式内容不一致");
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
    std::fs::remove_file(&out).ok();
}

/// 阈值设 0（禁用内存模式）：小文件也走磁盘（下载中文件即存在）。
#[tokio::test]
async fn memory_mode_disabled_uses_disk() {
    let data = make_data(256 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nmemory");
    let _ = std::fs::remove_file(&out);

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
    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "磁盘模式内容不一致");
    std::fs::remove_file(&out).ok();
}

/// 慢服务器路径：/slow 响应前等待 delay_ms，验证下载仍能成功完成
#[tokio::test]
async fn slow_server_path_download_completes() {
    let data = make_data(512 * 1024); // 512 KiB
                                      // 启动带慢延迟的服务器：/slow 路径响应前等待 200ms
    let server = TestServer::start_slow(data.clone(), false, 200).await;
    let out = out_path("slow_path");
    let _ = std::fs::remove_file(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        // 短宽限期以便慢线程检测能快速触发
        grace_period: 0.5,
        slow_confirm: 3,
        ..Default::default()
    };
    // 使用 /slow 路径下载
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

    let written = std::fs::read(&out).expect("读输出文件失败");
    assert_eq!(written, data, "慢服务器下载内容不一致");
    std::fs::remove_file(&out).ok();
}

/// 慢服务器与失败路径混合：验证多源故障转移时慢路径仍能作为备选源
#[tokio::test]
async fn slow_server_with_multi_source_failover() {
    let data = make_data(256 * 1024);
    let server = TestServer::start_slow(data.clone(), false, 100).await;
    let out = out_path("slow_failover");
    let _ = std::fs::remove_file(&out);

    let config = DownloadConfig {
        split: 4,
        min_split_size: 32 * 1024,
        grace_period: 0.5,
        ..Default::default()
    };
    // 慢路径作为备选源，失败路径优先
    let urls = vec![server.fail_url(), server.url_slow(), server.url()];
    let report = engine::download(urls, Some(out.clone()), config, None, None, |_| {})
        .await
        .expect("多源慢服务器下载失败");
    assert_eq!(report.total as usize, data.len());

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "多源慢服务器下载内容不一致");
    std::fs::remove_file(&out).ok();
}

/// gzip 压缩下载：服务器回 `Content-Encoding: gzip`，引擎应请求压缩并自动解压得到原始内容。
#[tokio::test]
async fn gzip_streaming_download_decompresses() {
    let data = make_data(512 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("gzip_it");
    let _ = std::fs::remove_file(&out);

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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "gzip 解压后内容不一致");
    // 服务器应收到过带 gzip 的 Accept-Encoding
    let enc = server.request_headers("accept-encoding").await;
    assert!(
        enc.iter().any(|v| v.to_ascii_lowercase().contains("gzip")),
        "应请求 gzip 编码，实际: {enc:?}"
    );
    assert!(!out.with_extension("gkdl.tmp").exists(), "不应残留临时文件");
    std::fs::remove_file(&out).ok();
}

/// 未知大小下载：服务器不提供 Content-Length，引擎应走流式整文件下载并成功。
#[tokio::test]
async fn unknown_size_streaming_download() {
    let data = make_data(300 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nosize_it");
    let _ = std::fs::remove_file(&out);

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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "未知大小下载内容不一致");
    std::fs::remove_file(&out).ok();
}

/// 关闭压缩：即使服务器支持 gzip，也不应发送 Accept-Encoding: gzip。
#[tokio::test]
async fn compression_disabled_sends_identity() {
    let data = make_data(128 * 1024);
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("nocompress_it");
    let _ = std::fs::remove_file(&out);

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

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "关闭压缩内容不一致");
    // 服务器应未收到 gzip 编码请求
    let enc = server.request_headers("accept-encoding").await;
    assert!(
        !enc.iter().any(|v| v.to_ascii_lowercase().contains("gzip")),
        "关闭压缩后不应请求 gzip，实际: {enc:?}"
    );
    std::fs::remove_file(&out).ok();
}
