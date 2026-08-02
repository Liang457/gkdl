use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

/// 流式计算文件的 SHA-256（hex 小写）。
pub fn sha256_file(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("打开文件失败: {}", path.display()))?;
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).context("读取文件失败")?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    let digest = hasher.finalize();
    Ok(hex_lower(&digest))
}

/// 比对文件 SHA-256 与期望值（大小写不敏感）。
pub fn verify_sha256(path: &Path, expected: &str) -> Result<()> {
    let actual = sha256_file(path)?;
    if !actual.eq_ignore_ascii_case(expected.trim()) {
        bail!("SHA-256 校验失败: 期望 {}, 实际 {}", expected, actual);
    }
    Ok(())
}

fn hex_lower(data: &[u8]) -> String {
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path() -> std::path::PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gkdl_hash_{}.tmp", ts))
    }

    #[test]
    fn known_sha256() {
        let p = tmp_path();
        let mut f = File::create(&p).unwrap();
        f.write_all(b"hello world").unwrap();
        drop(f);
        // echo -n "hello world" | sha256sum
        assert_eq!(
            sha256_file(&p).unwrap(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn verify_case_insensitive_and_mismatch() {
        let p = tmp_path();
        std::fs::write(&p, b"abc").unwrap();
        assert!(verify_sha256(
            &p,
            "BAA781679813987A601719F87D8FCB65A3C2E5F5D6B9F0F5A6F7E8A9B0C1D2E3F"
        )
        .is_err());
        std::fs::remove_file(&p).ok();
    }
}
