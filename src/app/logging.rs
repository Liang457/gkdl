use crate::app::config::LogConfig;
use anyhow::{Context, Result};
use chrono::Local;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::Level;
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields};
use tracing_subscriber::fmt::{MakeWriter, Subscriber};
use tracing_subscriber::util::SubscriberInitExt;

/// 日志写入器：文件 + stdout 双输出。
struct DualWriter {
    file: Arc<RotatingFileWriter>,
}

struct DualGuard {
    file: Arc<RotatingFileWriter>,
    stdout: io::Stdout,
}

impl Write for DualGuard {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.file.write_bytes(buf)?;
        let _ = self.stdout.write(buf);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush_log()?;
        self.stdout.flush()
    }
}

impl<'a> MakeWriter<'a> for DualWriter {
    type Writer = DualGuard;

    fn make_writer(&'a self) -> Self::Writer {
        DualGuard {
            file: Arc::clone(&self.file),
            stdout: io::stdout(),
        }
    }
}

/// 按大小轮转的日志文件写入器（内部 Mutex 串行化并发写）。
struct RotatingFileWriter {
    file: Mutex<File>,
    path: PathBuf,
    current_size: AtomicU64,
    max_size: u64,
    backup_count: u32,
}

impl RotatingFileWriter {
    fn new(path: PathBuf, max_size_mb: u64, backup_count: u32) -> Self {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap_or_else(|e| {
                // 日志文件无法打开时回退到临时目录，而非 panic
                eprintln!("打开日志文件失败 ({}): {e}，回退到临时目录", path.display());
                let fallback = std::env::temp_dir().join("gkdl_fallback.log");
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&fallback)
                    .expect("临时日志文件也无法打开")
            });
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        Self {
            file: Mutex::new(file),
            path,
            current_size: AtomicU64::new(size),
            max_size: max_size_mb.saturating_mul(1024 * 1024).max(1024),
            backup_count,
        }
    }

    fn write_bytes(&self, buf: &[u8]) -> io::Result<usize> {
        // 大小检查与轮转必须在同一把锁内完成：否则并发写入会同时越过阈值、
        // 触发两次 rotate（二次重命名丢失一份备份）甚至互相覆盖文件句柄。
        let mut file = self
            .file
            .lock()
            .map_err(|_| io::Error::other("日志锁 poisoned"))?;
        let current = self.current_size.load(Ordering::Relaxed);
        if current + buf.len() as u64 > self.max_size {
            self.rotate_locked(&mut file);
        }
        let n = file.write(buf)?;
        self.current_size.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    fn flush_log(&self) -> io::Result<()> {
        if let Ok(mut f) = self.file.lock() {
            f.flush()?;
        }
        Ok(())
    }

    /// 轮转（调用方须已持有文件锁）：gkdl.log → .1 → .2 → ... → .N，丢弃最旧。
    fn rotate_locked(&self, file: &mut File) {
        let _ = file.flush();
        for i in (1..=self.backup_count).rev() {
            let from = self.rotate_name(i - 1);
            let to = self.rotate_name(i);
            if i == self.backup_count {
                let _ = std::fs::remove_file(&to);
            }
            if from.exists() {
                let _ = std::fs::rename(&from, &to);
            }
        }
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .unwrap_or_else(|e| {
                eprintln!("重新打开日志文件失败: {e}");
                // 回退：截断当前文件重新打开
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&self.path)
                    .expect("日志文件完全无法访问")
            });
        *file = new_file;
        self.current_size.store(0, Ordering::Relaxed);
    }

    fn rotate_name(&self, i: u32) -> PathBuf {
        if i == 0 {
            self.path.clone()
        } else {
            let mut name = self.path.as_os_str().to_os_string();
            name.push(format!(".{i}"));
            PathBuf::from(name)
        }
    }
}

/// 自定义日志格式：`YYYY-MM-DD HH:MM:SS,mmm - <target> - <LEVEL> - <message>`
struct GkdlFormat;

impl<S, N> FormatEvent<S, N> for GkdlFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let now = Local::now();
        let target = event.metadata().target();
        write!(
            writer,
            "{} - {} - {:>5} - ",
            now.format("%Y-%m-%d %H:%M:%S,%3f"),
            target,
            event.metadata().level()
        )?;
        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

fn parse_level(s: &str) -> Level {
    match s.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" | "warning" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    }
}

/// 初始化日志：文件 + stdout 双输出、轮转、启动归档、保留清理。
pub fn init_logging(config_dir: &Path, log_cfg: &LogConfig) -> Result<()> {
    let log_path = resolve_log_path(config_dir, &log_cfg.file);

    if log_cfg.archive_enabled {
        if let Err(e) = archive_old_logs(config_dir, log_cfg, &log_path) {
            eprintln!("日志归档失败: {e}");
        }
    }

    let rotator = Arc::new(RotatingFileWriter::new(
        log_path.clone(),
        log_cfg.max_size_mb,
        log_cfg.backup_count,
    ));
    let writer = DualWriter {
        file: Arc::clone(&rotator),
    };

    let subscriber = Subscriber::builder()
        .event_format(GkdlFormat)
        .with_max_level(parse_level(&log_cfg.level))
        .with_writer(writer)
        .finish();

    subscriber.try_init().context("日志初始化失败")?;
    tracing::info!("日志已初始化: {}", log_path.display());
    Ok(())
}

fn resolve_log_path(config_dir: &Path, file: &str) -> PathBuf {
    let p = PathBuf::from(file);
    if p.is_absolute() {
        p
    } else {
        config_dir.join(p)
    }
}

/// 启动归档：把 mtime 日期 ≠ 今天的 `gkdl.log*` 打包进 archive/。
fn archive_old_logs(config_dir: &Path, log_cfg: &LogConfig, log_path: &Path) -> Result<()> {
    let log_dir = log_path
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| config_dir.to_path_buf());
    if !log_dir.exists() {
        return Ok(());
    }

    let base_name = log_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "gkdl.log".into());

    let archive_dir = log_dir.join("archive");
    std::fs::create_dir_all(&archive_dir).ok();

    let today = Local::now().date_naive();
    let mut files: Vec<PathBuf> = Vec::new();
    let entries = std::fs::read_dir(&log_dir).with_context(|| "扫描日志目录失败")?;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        if !name.starts_with(&base_name) || name.contains(".tmp") {
            continue;
        }
        let mtime = entry
            .metadata()
            .ok()
            .and_then(|m| m.modified().ok())
            .unwrap_or(std::time::SystemTime::now());
        let date = chrono::DateTime::<chrono::Local>::from(mtime).date_naive();
        if date != today {
            files.push(path);
        }
    }

    if !files.is_empty() {
        let stamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        let stem = format!("logs_{stamp}");

        // xz → gz → 纯 tar
        let xz_path = archive_dir.join(format!("{stem}.tar.xz"));
        if compress_archive(&xz_path, &files, "xz").is_ok() {
            for f in &files {
                let _ = std::fs::remove_file(f);
            }
            tracing::info!("已归档 {} 个日志文件 -> {}", files.len(), xz_path.display());
        } else {
            let gz_path = archive_dir.join(format!("{stem}.tar.gz"));
            if compress_archive(&gz_path, &files, "gz").is_ok() {
                for f in &files {
                    let _ = std::fs::remove_file(f);
                }
                tracing::info!("已归档 {} 个日志文件 -> {}", files.len(), gz_path.display());
            } else {
                let tar_path = archive_dir.join(format!("{stem}.tar"));
                if compress_archive(&tar_path, &files, "tar").is_ok() {
                    for f in &files {
                        let _ = std::fs::remove_file(f);
                    }
                    tracing::info!(
                        "已归档 {} 个日志文件 -> {}",
                        files.len(),
                        tar_path.display()
                    );
                } else {
                    tracing::warn!("日志归档全部失败");
                }
            }
        }
    }

    cleanup_archive(&archive_dir, log_cfg.retention_days);
    Ok(())
}

fn compress_archive(path: &Path, files: &[PathBuf], kind: &str) -> Result<()> {
    let file = File::create(path)?;
    match kind {
        "xz" => {
            let enc = xz2::write::XzEncoder::new(file, 6);
            write_tar(enc, files)?;
        }
        "gz" => {
            let enc = flate2::write::GzEncoder::new(file, flate2::Compression::default());
            write_tar(enc, files)?;
        }
        _ => write_tar(file, files)?,
    }
    Ok(())
}

fn write_tar<W: Write>(writer: W, files: &[PathBuf]) -> Result<()> {
    let mut builder = tar::Builder::new(writer);
    for f in files {
        let name = f
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut file = File::open(f)?;
        builder.append_file(&name, &mut file)?;
    }
    let mut inner = builder.into_inner()?;
    inner.flush()?;
    Ok(())
}

fn cleanup_archive(archive_dir: &Path, retention_days: u32) {
    let cutoff = std::time::Duration::from_secs(retention_days as u64 * 86400);
    let now = std::time::SystemTime::now();
    if let Ok(entries) = std::fs::read_dir(archive_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Ok(modified) = entry.metadata().and_then(|m| m.modified()) {
                if now
                    .duration_since(modified)
                    .map(|d| d > cutoff)
                    .unwrap_or(false)
                {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parsing() {
        assert_eq!(parse_level("debug"), Level::DEBUG);
        assert_eq!(parse_level("WARN"), Level::WARN);
        assert_eq!(parse_level("bogus"), Level::INFO);
    }

    #[test]
    fn rotating_writer_rotates() {
        let dir = std::env::temp_dir().join(format!("gkdl_log_{}", std::process::id()));
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("gkdl.log");
        let w = RotatingFileWriter::new(path.clone(), 0, 3); // max 1024 bytes
        let chunk = vec![b'x'; 600];
        for _ in 0..10 {
            w.write_bytes(&chunk).unwrap();
        }
        // 至少产生 .1 备份
        assert!(dir.join("gkdl.log.1").exists());
        std::fs::remove_dir_all(&dir).ok();
    }
}
