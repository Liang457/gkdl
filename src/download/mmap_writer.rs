use crate::download::writer::DownloadWriter;
use anyhow::{bail, Context, Result};
use memmap2::MmapRaw;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// 预分配 + mmap 写盘。
/// 各 worker 只写自己的非重叠区间，因此 `write_at` 并发安全。
pub struct MmapWriter {
    mmap: Option<MmapRaw>,
    file: File,
    total: u64,
    path: std::path::PathBuf,
}

impl MmapWriter {
    /// 创建/截断文件并映射。`total == 0` 时不映射（空文件）。
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

        let mmap = if total == 0 {
            None
        } else {
            let map = MmapOptions::new()
                .len(total as usize)
                .map_raw(&file)
                .with_context(|| "mmap 映射失败")?;
            Some(map)
        };

        Ok(Self {
            mmap,
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
    ///
    /// # Safety
    /// 通过原始指针写入 mmap。所有调用方只写自己的非重叠字节区间，
    /// 不存在数据竞争（这是本模块并发模型的先决条件，由 scheduler 保证）。
    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
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
        if let Some(map) = &self.mmap {
            unsafe {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    map.as_mut_ptr().add(offset),
                    data.len(),
                );
            }
        } else {
            // total == 0 且要写数据：不可能，但保险起见用 file
            use std::io::{Seek, SeekFrom, Write};
            let mut f = &self.file;
            f.seek(SeekFrom::Start(offset as u64))?;
            f.write_all(data)?;
        }
        Ok(())
    }

    /// 刷新给定区间到磁盘。
    pub fn flush_range(&self, offset: u64, len: u64) {
        if let Some(map) = &self.mmap {
            let _ = map.flush_range(offset as usize, len as usize);
        }
    }

    pub fn flush_all(&self) -> Result<()> {
        if let Some(map) = &self.mmap {
            map.flush().with_context(|| "mmap flush 失败")?;
        }
        self.file.sync_all().with_context(|| "文件 sync 失败")?;
        Ok(())
    }
}

use memmap2::MmapOptions;

impl DownloadWriter for MmapWriter {
    fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        MmapWriter::write_at(self, offset, data)
    }

    fn flush_range(&self, offset: u64, len: u64) {
        MmapWriter::flush_range(self, offset, len);
    }

    fn flush_all(&self) -> Result<()> {
        MmapWriter::flush_all(self)
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
        std::env::temp_dir().join(format!("gkdl_mmap_{}.tmp", ts))
    }

    #[test]
    fn write_and_read_back() {
        let p = tmp_path();
        let writer = MmapWriter::new(&p, 100).unwrap();
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
        let writer = MmapWriter::new(&p, 10).unwrap();
        assert!(writer.write_at(5, b"123456").is_err());
        drop(writer);
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_file_ok() {
        let p = tmp_path();
        let writer = MmapWriter::new(&p, 0).unwrap();
        assert!(writer.write_at(0, &[]).is_ok());
        drop(writer);
        let meta = std::fs::metadata(&p).unwrap();
        assert_eq!(meta.len(), 0);
        std::fs::remove_file(&p).ok();
    }
}
