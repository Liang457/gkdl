use anyhow::{bail, Context, Result};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Mutex;

/// 写盘抽象：`PwriteWriter`（预分配 + 偏移写）、`MemoryWriter`（小文件内存模式）、
/// `StreamingWriter`（未知大小整文件顺序流）共用。
///
/// 并发模型：各 worker 只写自己的非重叠区间，`write_at` 无需同步。
pub trait DownloadWriter: Send + Sync {
    /// 把数据写入 offset 处。调用方必须保证区间不与其它并发写重叠。
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()>;
    /// 把给定区间刷到磁盘（内存模式为 no-op）。
    fn flush_range(&self, offset: u64, len: u64);
    /// 全量刷盘（内存模式为 no-op，真正的落盘在 `finalize`）。
    fn flush_all(&self) -> Result<()>;
    /// 文件总字节数。
    fn total(&self) -> u64;
    /// 下载完成后收尾：内存/流式模式在此把累积数据写入 `out_path`；磁盘模式为 no-op。
    fn finalize(&self, out_path: &Path) -> Result<()>;
    /// 重置（截断为 0），供流式下载重试/恢复时从头开始。默认 no-op。
    fn reset(&self) -> Result<()> {
        Ok(())
    }
}

/// 小文件内存模式写器：数据累积在 RAM，下载完成后一次性原子落盘。
///
/// `write_at` 通过裸指针写入 `UnsafeCell<Vec<u8>>`，并发写必须落在
/// 互不重叠的区间（由 scheduler 保证）。
pub struct MemoryWriter {
    inner: std::cell::UnsafeCell<Vec<u8>>,
    total: u64,
}

// MemoryWriter 通过 `UnsafeCell` 提供内部可变性；并发安全由调用方
// （scheduler 保证非重叠区间）约束，与 `PwriteWriter` 的偏移写语义一致。
unsafe impl Sync for MemoryWriter {}

impl MemoryWriter {
    /// 分配 `total` 字节的零初始化缓冲（等价于磁盘模式的文件预分配）。
    pub fn new(total: u64) -> Self {
        let buf = vec![0u8; total as usize];
        Self {
            inner: std::cell::UnsafeCell::new(buf),
            total,
        }
    }

    fn buf_ptr(&self) -> *mut u8 {
        unsafe { (*self.inner.get()).as_mut_ptr() }
    }

    /// 只读访问整个缓冲（仅供单元测试断言使用）。
    #[allow(dead_code)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { &*self.inner.get() }
    }
}

impl DownloadWriter for MemoryWriter {
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let offset = offset as usize;
        if offset + data.len() > self.total as usize {
            bail!(
                "写越界: offset={} len={} total={}",
                offset,
                data.len(),
                self.total
            );
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.buf_ptr().add(offset), data.len());
        }
        Ok(())
    }

    fn flush_range(&self, _offset: u64, _len: u64) {
        // 内存中无磁盘页，无需 flush
    }

    fn flush_all(&self) -> Result<()> {
        Ok(())
    }

    fn total(&self) -> u64 {
        self.total
    }

    /// 把缓冲原子写入 `out_path`：写 `.gkdl.tmp` → fsync → rename。
    /// 崩溃最坏情况是残留临时文件，目标文件完整或不存在。
    fn finalize(&self, out_path: &Path) -> Result<()> {
        let buf = unsafe { &*self.inner.get() };
        if let Some(parent) = out_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("创建目录失败: {}", parent.display()))?;
            }
        }
        let tmp = out_path.with_extension("gkdl.tmp");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)
                .with_context(|| format!("创建临时文件失败: {}", tmp.display()))?;
            f.write_all(buf)
                .with_context(|| format!("写临时文件失败: {}", tmp.display()))?;
            f.sync_all()
                .with_context(|| format!("同步临时文件失败: {}", tmp.display()))?;
        }
        std::fs::rename(&tmp, out_path)
            .with_context(|| format!("临时文件重命名失败: {}", out_path.display()))?;
        Ok(())
    }
}

/// 整文件流式写器（未知大小/压缩流）：数据顺序追加到 `.gkdl.tmp`，完成后原子 rename。
///
/// 只允许单连接顺序写：`write_at` 忽略偏移、按文件当前长度追加（由单个流式 worker 驱动）。
/// 总长不可预知（压缩后解压大小、无 Content-Length 等），因此不做预分配、不参与续传。
pub struct StreamingWriter {
    file: Mutex<Option<std::fs::File>>,
    /// 已写字节数（原子计数，`total()` 可在线程外读取）。
    written: std::sync::atomic::AtomicU64,
}

impl StreamingWriter {
    /// 创建/截断临时文件。`out_path` 仅用于推导临时文件名，真正的写入发生在 `finalize`。
    pub fn new(out_path: &Path) -> Result<Self> {
        let tmp = out_path.with_extension("gkdl.tmp");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("创建临时文件失败: {}", tmp.display()))?;
        Ok(Self {
            file: Mutex::new(Some(file)),
            written: std::sync::atomic::AtomicU64::new(0),
        })
    }
}

impl DownloadWriter for StreamingWriter {
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut guard = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("临时文件锁异常"))?;
        let file = guard
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("临时文件已关闭"))?;
        // 顺序追加：显式 seek 到 offset（单连接流式写，调用方保证 offset 连续递增）
        use std::io::{Seek, SeekFrom, Write};
        file.seek(SeekFrom::Start(offset))?;
        let mut written = 0usize;
        while written < data.len() {
            let n = file.write(&data[written..])?;
            if n == 0 {
                bail!("写入停滞: offset={}", offset + written as u64);
            }
            written += n;
        }
        drop(guard);
        self.written
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    fn flush_range(&self, _offset: u64, _len: u64) {}

    fn flush_all(&self) -> Result<()> {
        let guard = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("临时文件锁异常"))?;
        if let Some(file) = guard.as_ref() {
            file.sync_all().with_context(|| "临时文件 sync 失败")?;
        }
        Ok(())
    }

    fn total(&self) -> u64 {
        self.written.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 收尾：sync、关闭句柄后原子 rename 到 `out_path`（覆盖旧文件）。
    fn finalize(&self, out_path: &Path) -> Result<()> {
        self.flush_all()?;
        // 关闭文件句柄（Windows 下 rename 需要文件未被占用）
        let mut guard = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("临时文件锁异常"))?;
        *guard = None;
        drop(guard);
        let tmp = out_path.with_extension("gkdl.tmp");
        std::fs::rename(&tmp, out_path)
            .with_context(|| format!("临时文件重命名失败: {}", out_path.display()))?;
        Ok(())
    }

    /// 重试/恢复：截断临时文件为 0 并从头部重新写。
    fn reset(&self) -> Result<()> {
        let guard = self
            .file
            .lock()
            .map_err(|_| anyhow::anyhow!("临时文件锁异常"))?;
        if let Some(file) = guard.as_ref() {
            file.set_len(0).with_context(|| "重置临时文件失败")?;
        }
        self.written.store(0, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::time::{SystemTime, UNIX_EPOCH};
    fn tmp_path(name: &str) -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gkdl_mem_{name}_{ts}.bin"))
    }

    #[test]
    fn write_and_finalize_atomic() {
        let out = tmp_path("atomic");
        let w = MemoryWriter::new(100);
        w.write_at(0, b"hello").unwrap();
        w.write_at(5, b" world").unwrap();
        w.flush_all().unwrap();
        w.finalize(&out).unwrap();
        // 目标文件内容正确
        let mut f = std::fs::File::open(&out).unwrap();
        let mut buf = vec![0u8; 100];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(&buf[..11], b"hello world");
        // 无残留临时文件
        assert!(!out.with_extension("gkdl.tmp").exists());
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn out_of_bounds_rejected() {
        let w = MemoryWriter::new(10);
        assert!(w.write_at(5, b"123456").is_err());
    }

    #[test]
    fn empty_file_ok() {
        let out = tmp_path("empty");
        let w = MemoryWriter::new(0);
        assert!(w.write_at(0, &[]).is_ok());
        w.finalize(&out).unwrap();
        let meta = std::fs::metadata(&out).unwrap();
        assert_eq!(meta.len(), 0);
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn concurrent_disjoint_writes_match_contract() {
        // 模拟两个 worker 写互不重叠区间（并发安全的前提）
        let w = std::sync::Arc::new(MemoryWriter::new(1024));
        let mut handles = Vec::new();
        for (lo, hi, byte) in [(0u64, 512u64, 1u8), (512, 1024, 2)] {
            let w = std::sync::Arc::clone(&w);
            handles.push(std::thread::spawn(move || {
                for i in lo..hi {
                    w.write_at(i, &[byte]).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let slice = w.as_slice();
        assert!(slice[..512].iter().all(|&b| b == 1));
        assert!(slice[512..].iter().all(|&b| b == 2));
    }

    #[test]
    fn seek_read_back() {
        let out = tmp_path("seek");
        let w = MemoryWriter::new(64);
        w.write_at(10, b"abc").unwrap();
        w.finalize(&out).unwrap();
        let mut f = std::fs::File::open(&out).unwrap();
        f.seek(SeekFrom::Start(10)).unwrap();
        let mut b = [0u8; 3];
        f.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"abc");
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn streaming_writer_appends_and_finalizes() {
        let out = tmp_path("stream");
        let w = StreamingWriter::new(&out).unwrap();
        w.write_at(0, b"hello").unwrap();
        w.write_at(5, b" world").unwrap();
        assert_eq!(w.total(), 11);
        w.flush_all().unwrap();
        w.finalize(&out).unwrap();
        let written = std::fs::read(&out).unwrap();
        assert_eq!(written, b"hello world");
        // 无残留临时文件
        assert!(!out.with_extension("gkdl.tmp").exists());
        std::fs::remove_file(&out).ok();
    }

    #[test]
    fn streaming_writer_reset_truncates() {
        let out = tmp_path("stream_reset");
        let w = StreamingWriter::new(&out).unwrap();
        w.write_at(0, b"abcdef").unwrap();
        assert_eq!(w.total(), 6);
        w.reset().unwrap();
        assert_eq!(w.total(), 0);
        w.write_at(0, b"xy").unwrap();
        w.finalize(&out).unwrap();
        let written = std::fs::read(&out).unwrap();
        assert_eq!(written, b"xy");
        std::fs::remove_file(&out).ok();
    }
}
