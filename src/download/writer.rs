use anyhow::{bail, Context, Result};
use std::path::Path;

/// 写盘抽象：`MmapWriter`（预分配 + mmap）与 `MemoryWriter`（小文件内存模式）共用。
///
/// 并发模型与旧版一致：各 worker 只写自己的非重叠区间，因此 `write_at` 无需同步。
pub trait DownloadWriter: Send + Sync {
    /// 把数据写入 offset 处。调用方必须保证区间不与其它并发写重叠。
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()>;
    /// 把给定区间刷到磁盘（内存模式为 no-op）。
    fn flush_range(&self, offset: u64, len: u64);
    /// 全量刷盘（内存模式为 no-op，真正的落盘在 `finalize`）。
    fn flush_all(&self) -> Result<()>;
    /// 文件总字节数。
    fn total(&self) -> u64;
    /// 下载完成后收尾：内存模式在此把累积数据原子写入 `out_path`；磁盘模式为 no-op。
    fn finalize(&self, out_path: &Path) -> Result<()>;
}

/// 小文件内存模式写器：数据累积在 RAM，下载完成后一次性原子落盘。
///
/// `write_at` 通过裸指针写入 `UnsafeCell<Vec<u8>>`，前置条件与 mmap 相同：
/// 并发写必须落在互不重叠的区间（由 scheduler 保证）。
pub struct MemoryWriter {
    inner: std::cell::UnsafeCell<Vec<u8>>,
    total: u64,
}

// MemoryWriter 通过 `UnsafeCell` 提供内部可变性；并发安全由调用方
// （scheduler 保证非重叠区间）约束，与 `MmapWriter` 的 mmap 语义一致。
unsafe impl Sync for MemoryWriter {}

impl MemoryWriter {
    /// 分配 `total` 字节的零初始化缓冲（占位语义与 mmap 预分配一致）。
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
    fn concurrent_disjoint_writes_match_mmap_contract() {
        // 模拟两个 worker 写互不重叠区间（等价于 mmap 的前置条件）
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
}
