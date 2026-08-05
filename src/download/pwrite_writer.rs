use crate::download::writer::DownloadWriter;
use anyhow::{bail, Context, Result};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// 预分配 + 偏移写盘（pwrite）。
///
/// 各 worker 只写自己的非重叠区间，因此 `write_at` 并发安全：
/// Windows 用 overlapped `seek_write`（显式偏移，不共享文件指针），
/// Unix 用等价的 `write_at`。
///
/// 与旧版整文件 mmap 相比，pwrite 的页面归属系统页缓存而非进程工作集，
/// 大文件下载时进程工作集不再随文件大小虚高。
pub struct PwriteWriter {
    file: File,
    total: u64,
    path: std::path::PathBuf,
}

impl PwriteWriter {
    /// 创建/截断文件并预分配 `total` 字节。
    pub fn new(path: &Path, total: u64) -> Result<Self> {
        Self::open(path, total, true)
    }

    /// 续传：打开已存在文件（不截断），`total` 需与现有文件一致。
    pub fn resume(path: &Path, total: u64) -> Result<Self> {
        Self::open(path, total, false)
    }

    fn open(path: &Path, total: u64, truncate: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(truncate)
            .open(path)
            .with_context(|| format!("打开输出文件失败: {}", path.display()))?;

        file.set_len(total).with_context(|| "预分配文件失败")?;

        Ok(Self {
            file,
            total,
            path: path.to_path_buf(),
        })
    }

    #[allow(dead_code)]
    pub fn total(&self) -> u64 {
        self.total
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 把数据写入 offset 处。调用方必须保证区间不与其它并发写重叠。
    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        if offset.saturating_add(data.len() as u64) > self.total {
            bail!(
                "写越界: offset={} len={} total={}",
                offset,
                data.len(),
                self.total
            );
        }
        // 短写循环补齐：WriteFile 对普通文件通常一次写完，但仍需防御性处理
        let mut written = 0usize;
        while written < data.len() {
            let n = self.pwrite(&data[written..], offset + written as u64)?;
            if n == 0 {
                bail!(
                    "写入停滞: offset={} len={}",
                    offset + written as u64,
                    data.len() - written
                );
            }
            written += n;
        }
        Ok(())
    }

    /// 平台相关的偏移写：Windows 用 overlapped `seek_write`（等价 pwrite），Unix 用 `write_at`。
    fn pwrite(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        #[cfg(windows)]
        {
            std::os::windows::fs::FileExt::seek_write(&self.file, buf, offset)
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::FileExt::write_at(&self.file, buf, offset)
        }
    }

    /// 刷新给定区间到磁盘（pwrite 已直接落 IO，无需额外刷新）。
    pub fn flush_range(&self, _offset: u64, _len: u64) {}

    pub fn flush_all(&self) -> Result<()> {
        self.file.sync_all().with_context(|| "文件 sync 失败")?;
        Ok(())
    }
}

impl DownloadWriter for PwriteWriter {
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        PwriteWriter::write_at(self, offset, data)
    }

    fn flush_range(&self, offset: u64, len: u64) {
        PwriteWriter::flush_range(self, offset, len);
    }

    fn flush_all(&self) -> Result<()> {
        PwriteWriter::flush_all(self)
    }

    fn total(&self) -> u64 {
        self.total
    }

    /// 磁盘模式：文件已随下载预分配并落盘，无需额外收尾。
    fn finalize(&self, _out_path: &Path) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path() -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gkdl_pw_{}.tmp", ts))
    }

    #[test]
    fn write_and_read_back() {
        let p = tmp_path();
        let writer = PwriteWriter::new(&p, 100).unwrap();
        writer.write_at(0, b"hello").unwrap();
        writer.write_at(5, b" world").unwrap();
        writer.flush_all().unwrap();
        drop(writer);

        let mut f = File::open(&p).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        let mut buf = vec![0u8; 11];
        f.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"hello world");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn out_of_bounds_rejected() {
        let p = tmp_path();
        let writer = PwriteWriter::new(&p, 10).unwrap();
        assert!(writer.write_at(5, b"123456").is_err());
        drop(writer);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_file_ok() {
        let p = tmp_path();
        let writer = PwriteWriter::new(&p, 0).unwrap();
        assert!(writer.write_at(0, &[]).is_ok());
        drop(writer);
        let meta = std::fs::metadata(&p).unwrap();
        assert_eq!(meta.len(), 0);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn concurrent_disjoint_writes_match_pwrite_contract() {
        // 模拟两个 worker 写互不重叠区间（等价于 mmap 的前置条件）
        let p = tmp_path();
        let w = std::sync::Arc::new(PwriteWriter::new(&p, 1024).unwrap());
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
        drop(w);
        let mut f = File::open(&p).unwrap();
        let mut buf = vec![0u8; 1024];
        f.read_exact(&mut buf).unwrap();
        assert!(buf[..512].iter().all(|&b| b == 1));
        assert!(buf[512..].iter().all(|&b| b == 2));
        std::fs::remove_file(&p).ok();
    }
}
