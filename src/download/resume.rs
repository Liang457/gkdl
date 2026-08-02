use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// 控制文件中单个段的状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentSnapshot {
    pub start: u64,
    pub end: u64,
    pub written: u64,
    pub complete: bool,
}

/// 断点续传控制文件（`<文件名>.gkdl`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    pub version: u32,
    pub urls: Vec<String>,
    pub total: u64,
    pub out_path: String,
    pub sha256: Option<String>,
    pub rate_limit: u64,
    pub split: usize,
    pub min_split_size: u64,
    pub segments: Vec<SegmentSnapshot>,
    pub updated_at: i64,
}

impl ResumeState {
    pub fn control_path(out_path: &Path) -> PathBuf {
        let mut name = out_path.as_os_str().to_os_string();
        name.push(".gkdl");
        PathBuf::from(name)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self).context("序列化控制文件失败")?;
        let tmp = path.with_extension("gkdl.tmp");
        std::fs::write(&tmp, json).with_context(|| format!("写控制文件失败: {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("控制文件重命名失败: {}", path.display()))?;
        Ok(())
    }

    pub fn load(path: &Path) -> Result<ResumeState> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("读控制文件失败: {}", path.display()))?;
        let state: ResumeState = serde_json::from_str(&content).context("解析控制文件失败")?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn roundtrip() {
        let p = std::env::temp_dir().join(format!(
            "gkdl_resume_{}.gkdl",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let state = ResumeState {
            version: 1,
            urls: vec!["http://example.com/f.bin".into()],
            total: 1000,
            out_path: "C:/x/f.bin".into(),
            sha256: None,
            rate_limit: 0,
            split: 4,
            min_split_size: 65536,
            segments: vec![SegmentSnapshot {
                start: 0,
                end: 500,
                written: 300,
                complete: false,
            }],
            updated_at: 12345,
        };
        state.save(&p).unwrap();
        let loaded = ResumeState::load(&p).unwrap();
        assert_eq!(loaded.total, 1000);
        assert_eq!(loaded.segments[0].written, 300);
        assert_eq!(loaded.urls, vec!["http://example.com/f.bin"]);
        std::fs::remove_file(&p).ok();
    }
}
