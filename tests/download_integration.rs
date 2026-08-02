mod common;

use common::TestServer;
use gkdl::download::config::DownloadConfig;
use gkdl::download::engine;
use std::time::Duration;

fn make_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 251) as u8).collect()
}

fn out_path(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join("gkdl_it");
    std::fs::create_dir_all(&dir).ok();
    dir.join(format!("{}_{}.bin", name, std::process::id()))
}

#[tokio::test]
async fn multi_thread_range_download_matches() {
    let data = make_data(1_048_576); // 1 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("range");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        ..Default::default()
    };
    let report = engine::download(vec![server.url()], Some(out.clone()), config, None, |_| {})
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
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let config = DownloadConfig {
        split: 4,
        min_split_size: 64 * 1024,
        ..Default::default()
    };
    let urls = vec![server.fail_url(), server.broken_url(), server.url()];
    let report = engine::download(urls, Some(out.clone()), config, None, |_| {})
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
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let config = DownloadConfig {
        split: 8,
        min_split_size: 32 * 1024,
        ..Default::default()
    };
    let report = engine::download(vec![server.url()], Some(out.clone()), config, None, |_| {})
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
    let _ = std::fs::remove_file(format!("{}.gkdl", out1.display()));
    let digest = gkdl::hash::sha256_file(&{
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
        |_| {},
    )
    .await
    .expect("正确 hash 应通过");
    std::fs::remove_file(&out1).ok();

    // 错误 hash
    let out2 = out_path("hash_bad");
    let _ = std::fs::remove_file(&out2);
    let _ = std::fs::remove_file(format!("{}.gkdl", out2.display()));
    let bad = "0".repeat(64);
    let err = engine::download(
        vec![server.url()],
        Some(out2.clone()),
        config,
        Some(bad),
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
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    let config = DownloadConfig {
        split: 4,
        rate_limit: 512 * 1024, // 512 KB/s
        ..Default::default()
    };
    let t0 = std::time::Instant::now();
    engine::download(vec![server.url()], Some(out.clone()), config, None, |_| {})
        .await
        .expect("限速下载失败");
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1500),
        "1 MiB @ 512 KB/s 至少需 ~2s，实际 {elapsed:?}"
    );
    std::fs::remove_file(&out).ok();
}

#[tokio::test]
async fn interrupted_download_resumes_from_control_file() {
    let data = make_data(4 * 1024 * 1024); // 4 MiB
    let server = TestServer::start(data.clone(), false).await;
    let out = out_path("resume_it");
    let _ = std::fs::remove_file(&out);
    let _ = std::fs::remove_file(format!("{}.gkdl", out.display()));

    // 第一次：限速慢速下载，约 1 秒后取消
    let config = DownloadConfig {
        split: 4,
        rate_limit: 2 * 1024 * 1024,
        min_split_size: 256 * 1024,
        ..Default::default()
    };
    let handle = engine::start_download(
        vec![server.url()],
        Some(out.clone()),
        config.clone(),
        None,
        None,
        tokio_util::sync::CancellationToken::new(),
        |_| {},
    )
    .await
    .expect("启动下载失败");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle.cancel();
    let _ = handle.join().await; // 取消后返回 Err，忽略

    let control = format!("{}.gkdl", out.display());
    assert!(
        std::path::Path::new(&control).exists(),
        "取消后应存在控制文件"
    );

    // 第二次：重新下载应续传并成功
    let report = engine::download(vec![server.url()], Some(out.clone()), config, None, |_| {})
        .await
        .expect("续传下载失败");
    assert_eq!(report.total as usize, data.len());

    let written = std::fs::read(&out).unwrap();
    assert_eq!(written, data, "续传内容不一致");
    assert!(
        !std::path::Path::new(&control).exists(),
        "成功后应删除控制文件"
    );
    std::fs::remove_file(&out).ok();
}
