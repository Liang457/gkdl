use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// 已完成记录默认保留天数（可配置；0 = 永久保留）。
pub const DEFAULT_RETENTION_DAYS: u32 = 7;

/// 段状态快照（持久化到数据库）。
#[derive(Debug, Clone)]
pub struct SegmentRecord {
    pub seg_id: u64,
    pub start: u64,
    pub end: u64,
    pub written: u64,
    pub complete: bool,
}

/// 下载任务记录（持久化到 SQLite）。
#[derive(Debug, Clone)]
pub struct DownloadRecord {
    pub gid: String,
    pub urls: Vec<String>,
    pub total: u64,
    pub out_path: String,
    pub sha256: Option<String>,
    pub rate_limit: u64,
    pub split: usize,
    pub min_split_size: u64,
    pub memory_mode: bool,
    pub status: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub segments: Vec<SegmentRecord>,
}

impl DownloadRecord {
    pub fn completed_bytes(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| if s.complete { s.written } else { 0 })
            .sum()
    }
}

/// 轻量 SQLite 状态库。
///
/// 单连接 + `std::sync::Mutex` 串行写（WAL 模式下读并发、写串行，够用）。
/// 所有操作均为快速小事务，调用方（监控任务每秒一次）不会长时间阻塞。
pub struct StateDb {
    conn: Mutex<Connection>,
    pub path: PathBuf,
}

impl StateDb {
    /// 打开（不存在则创建）数据库并迁移表结构。路径父目录不存在时自动创建。
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("创建数据库目录失败: {}", parent.display()))?;
            }
        }
        let conn = Connection::open(path)
            .with_context(|| format!("打开状态数据库失败: {}", path.display()))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .context("设置 WAL 模式失败")?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .context("设置 synchronous 失败")?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .context("启用外键失败")?;
        // 收紧页缓存，降低常驻内存（默认 2000 页 ≈ 8MB，这里压到 2MB）
        conn.pragma_update(None, "cache_size", -2048)
            .context("设置 cache_size 失败")?;
        Self::migrate(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
            path: path.to_path_buf(),
        })
    }

    fn migrate(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS downloads (
                gid            TEXT PRIMARY KEY,
                urls           TEXT NOT NULL,
                total          INTEGER NOT NULL,
                out_path       TEXT NOT NULL,
                sha256         TEXT,
                rate_limit     INTEGER NOT NULL,
                split          INTEGER NOT NULL,
                min_split_size INTEGER NOT NULL,
                memory_mode    INTEGER NOT NULL,
                status         TEXT NOT NULL,
                created_at     INTEGER NOT NULL,
                updated_at     INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS segments (
                gid      TEXT NOT NULL,
                seg_id   INTEGER NOT NULL,
                start    INTEGER NOT NULL,
                \"end\"     INTEGER NOT NULL,
                written  INTEGER NOT NULL,
                complete INTEGER NOT NULL,
                PRIMARY KEY (gid, seg_id),
                FOREIGN KEY (gid) REFERENCES downloads(gid) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_downloads_resume
                ON downloads (out_path, total, status, updated_at);
            ",
        )
        .context("迁移数据库结构失败")?;
        Ok(())
    }

    fn urls_to_json(urls: &[String]) -> String {
        // Vec<String> 的序列化无失败路径；空串兜底会静默破坏 find_resume 的精确匹配
        serde_json::to_string(urls).expect("序列化 urls 失败")
    }

    fn urls_from_json(s: &str) -> Vec<String> {
        serde_json::from_str(s).unwrap_or_default()
    }

    /// 写入（整条替换）下载记录及其段。显式事务保证记录与段要么都写要么都不写，
    /// 避免进程中途崩溃留下「有记录无段」的半成品（会导致续传时静默损坏）。
    pub fn upsert_download(&self, rec: &DownloadRecord) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().context("开启写入事务失败")?;
        tx.execute(
            "INSERT OR REPLACE INTO downloads
                (gid, urls, total, out_path, sha256, rate_limit, split, min_split_size,
                 memory_mode, status, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                rec.gid,
                Self::urls_to_json(&rec.urls),
                rec.total,
                rec.out_path,
                rec.sha256,
                rec.rate_limit,
                rec.split,
                rec.min_split_size,
                rec.memory_mode as i64,
                rec.status,
                rec.created_at,
                rec.updated_at,
            ],
        )
        .context("写入下载记录失败")?;
        tx.execute("DELETE FROM segments WHERE gid = ?1", params![rec.gid])
            .context("清理旧段记录失败")?;
        let mut stmt = tx
            .prepare(
                "INSERT OR REPLACE INTO segments
                    (gid, seg_id, start, \"end\", written, complete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )
            .context("准备段写入失败")?;
        for s in &rec.segments {
            stmt.execute(params![
                rec.gid,
                s.seg_id,
                s.start,
                s.end,
                s.written,
                s.complete as i64,
            ])
            .context("写入段记录失败")?;
        }
        drop(stmt);
        tx.commit().context("提交写入事务失败")?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn row_to_record(
        conn: &Connection,
        gid: &str,
        urls: &str,
        total: i64,
        out_path: &str,
        sha256: Option<String>,
        rate_limit: i64,
        split: i64,
        min_split_size: i64,
        memory_mode: i64,
        status: &str,
        created_at: i64,
        updated_at: i64,
    ) -> Result<DownloadRecord> {
        let mut stmt = conn
            .prepare(
                "SELECT seg_id, start, \"end\", written, complete FROM segments
                 WHERE gid = ?1 ORDER BY seg_id",
            )
            .context("准备读取段记录失败")?;
        let segments = stmt
            .query_map(params![gid], |row| {
                Ok(SegmentRecord {
                    seg_id: row.get::<_, i64>(0)? as u64,
                    start: row.get::<_, i64>(1)? as u64,
                    end: row.get::<_, i64>(2)? as u64,
                    written: row.get::<_, i64>(3)? as u64,
                    complete: row.get::<_, i64>(4)? != 0,
                })
            })
            .context("查询段记录失败")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("解析段记录失败")?;
        Ok(DownloadRecord {
            gid: gid.to_string(),
            urls: Self::urls_from_json(urls),
            total: total as u64,
            out_path: out_path.to_string(),
            sha256,
            rate_limit: rate_limit as u64,
            split: split as usize,
            min_split_size: min_split_size as u64,
            memory_mode: memory_mode != 0,
            status: status.to_string(),
            created_at,
            updated_at,
            segments,
        })
    }

    /// 按 GID 读取完整记录。
    pub fn load_download(&self, gid: &str) -> Result<Option<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT gid, urls, total, out_path, sha256, rate_limit, split, min_split_size,
                        memory_mode, status, created_at, updated_at
                 FROM downloads WHERE gid = ?1",
                params![gid],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                    ))
                },
            )
            .optional()
            .context("查询下载记录失败")?;
        match row {
            Some(r) => Ok(Some(Self::row_to_record(
                &conn, &r.0, &r.1, r.2, &r.3, r.4, r.5, r.6, r.7, r.8, &r.9, r.10, r.11,
            )?)),
            None => Ok(None),
        }
    }

    /// 查找可续传记录：输出路径 + 总长 + URL 列表一致且状态未终结。
    pub fn find_resume(
        &self,
        out_path: &str,
        urls: &[String],
        total: u64,
    ) -> Result<Option<DownloadRecord>> {
        let urls_json = Self::urls_to_json(urls);
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT gid, urls, total, out_path, sha256, rate_limit, split, min_split_size,
                        memory_mode, status, created_at, updated_at
                 FROM downloads
                 WHERE out_path = ?1 AND total = ?2 AND urls = ?3
                   AND status IN ('active', 'paused')
                 ORDER BY updated_at DESC LIMIT 1",
                params![out_path, total as i64, urls_json],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, i64>(7)?,
                        row.get::<_, i64>(8)?,
                        row.get::<_, String>(9)?,
                        row.get::<_, i64>(10)?,
                        row.get::<_, i64>(11)?,
                    ))
                },
            )
            .optional()
            .context("查询续传记录失败")?;
        match row {
            Some(r) => Ok(Some(Self::row_to_record(
                &conn, &r.0, &r.1, r.2, &r.3, r.4, r.5, r.6, r.7, r.8, &r.9, r.10, r.11,
            )?)),
            None => Ok(None),
        }
    }

    /// 更新任务状态并刷新 `updated_at`。
    pub fn set_status(&self, gid: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE downloads SET status = ?1, updated_at = ?2 WHERE gid = ?3",
            params![status, chrono::Utc::now().timestamp(), gid],
        )
        .context("更新任务状态失败")?;
        Ok(())
    }

    /// 删除整条记录及其段（级联）。
    pub fn delete(&self, gid: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM downloads WHERE gid = ?1", params![gid])
            .context("删除下载记录失败")?;
        Ok(())
    }

    /// 列出可恢复任务（active/paused），用于 daemon 重启后的任务重建。
    pub fn list_resumable(&self) -> Result<Vec<DownloadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT gid, urls, total, out_path, sha256, rate_limit, split, min_split_size,
                        memory_mode, status, created_at, updated_at
                 FROM downloads WHERE status IN ('active', 'paused')
                 ORDER BY created_at",
            )
            .context("准备查询可恢复任务失败")?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                ))
            })
            .context("查询可恢复任务失败")?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("解析可恢复任务失败")?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(Self::row_to_record(
                &conn, &r.0, &r.1, r.2, &r.3, r.4, r.5, r.6, r.7, r.8, &r.9, r.10, r.11,
            )?);
        }
        Ok(out)
    }

    /// 清理超过保留期的已结束记录（complete/error）。`retention_days == 0` 表示永久保留。
    /// 出错记录既不可续传也不会被恢复，若不清理会永久残留，故一并纳入保留期清理。
    /// 返回被清理的记录数，并在必要时 checkpoint 收缩 WAL。
    pub fn cleanup_completed(&self, retention_days: u32) -> Result<usize> {
        if retention_days == 0 {
            return Ok(0);
        }
        let cutoff = chrono::Utc::now().timestamp() - i64::from(retention_days) * 86_400;
        let conn = self.conn.lock().unwrap();
        let deleted = conn
            .execute(
                "DELETE FROM downloads WHERE status IN ('complete', 'error') AND updated_at < ?1",
                params![cutoff],
            )
            .context("清理过期记录失败")?;
        if deleted > 0 {
            let _ = conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);");
        }
        Ok(deleted)
    }

    /// 下载终态调用：收缩 WAL 到空并释放页缓存中不再需要的帧，
    /// 避免大文件下载期间积累的 WAL 页（默认最多 ~2000 页）常驻内存。
    pub fn release_memory(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA shrink_memory;")
            .context("收缩状态库内存失败")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_db() -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("gkdl_db_{ts}.db"))
    }

    fn sample(gid: &str) -> DownloadRecord {
        DownloadRecord {
            gid: gid.into(),
            urls: vec!["http://example.com/f.bin".into()],
            total: 1000,
            out_path: "C:/x/f.bin".into(),
            sha256: None,
            rate_limit: 0,
            split: 4,
            min_split_size: 65536,
            memory_mode: false,
            status: "active".into(),
            created_at: 1,
            updated_at: 1,
            segments: vec![SegmentRecord {
                seg_id: 0,
                start: 0,
                end: 500,
                written: 300,
                complete: false,
            }],
        }
    }

    #[test]
    fn roundtrip_and_find_resume() {
        let p = tmp_db();
        let db = StateDb::open(&p).unwrap();
        let rec = sample("a1b2c3d4e5f6a7b8");
        db.upsert_download(&rec).unwrap();

        let loaded = db.load_download("a1b2c3d4e5f6a7b8").unwrap().unwrap();
        assert_eq!(loaded.total, 1000);
        assert_eq!(loaded.urls, rec.urls);
        assert_eq!(loaded.segments[0].written, 300);
        assert!(!loaded.memory_mode);

        let found = db
            .find_resume("C:/x/f.bin", &rec.urls, 1000)
            .unwrap()
            .unwrap();
        assert_eq!(found.gid, rec.gid);
        // 不匹配则查不到
        assert!(db
            .find_resume("C:/x/other.bin", &rec.urls, 1000)
            .unwrap()
            .is_none());
        // 完成后不再命中续传
        db.set_status(&rec.gid, "complete").unwrap();
        assert!(db
            .find_resume("C:/x/f.bin", &rec.urls, 1000)
            .unwrap()
            .is_none());
        // 删除级联清段
        db.delete(&rec.gid).unwrap();
        assert!(db.load_download(&rec.gid).unwrap().is_none());
        std::fs::remove_file(&p).ok();
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }

    #[test]
    fn upsert_is_idempotent() {
        let p = tmp_db();
        let db = StateDb::open(&p).unwrap();
        db.upsert_download(&sample("gid0")).unwrap();
        db.upsert_download(&sample("gid0")).unwrap();
        assert!(db.load_download("gid0").unwrap().is_some());
        std::fs::remove_file(&p).ok();
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }

    #[test]
    fn cleanup_only_removes_expired_completed_or_error() {
        let p = tmp_db();
        let db = StateDb::open(&p).unwrap();
        let mut old = sample("old");
        old.status = "complete".into();
        old.updated_at = chrono::Utc::now().timestamp() - 20 * 86_400;
        let mut fresh = sample("fresh");
        fresh.status = "complete".into();
        fresh.updated_at = chrono::Utc::now().timestamp();
        let mut old_err = sample("old_err");
        old_err.status = "error".into();
        old_err.updated_at = chrono::Utc::now().timestamp() - 20 * 86_400;
        let mut active = sample("active");
        active.status = "active".into();
        active.updated_at = chrono::Utc::now().timestamp() - 20 * 86_400;
        db.upsert_download(&old).unwrap();
        db.upsert_download(&fresh).unwrap();
        db.upsert_download(&old_err).unwrap();
        db.upsert_download(&active).unwrap();

        let deleted = db.cleanup_completed(7).unwrap();
        assert_eq!(deleted, 2);
        assert!(db.load_download("old").unwrap().is_none());
        assert!(db.load_download("old_err").unwrap().is_none());
        assert!(db.load_download("fresh").unwrap().is_some());
        assert!(db.load_download("active").unwrap().is_some());
        // 0 = 永久保留
        let db2 = StateDb::open(&p).unwrap();
        db2.upsert_download(&fresh).unwrap();
        assert_eq!(db2.cleanup_completed(0).unwrap(), 0);
        std::fs::remove_file(&p).ok();
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }

    #[test]
    fn list_resumable_matches_status() {
        let p = tmp_db();
        let db = StateDb::open(&p).unwrap();
        let mut a = sample("a");
        a.status = "active".into();
        let mut b = sample("b");
        b.status = "paused".into();
        let mut c = sample("c");
        c.status = "complete".into();
        for r in [&a, &b, &c] {
            db.upsert_download(r).unwrap();
        }
        let resumable = db.list_resumable().unwrap();
        let gids: Vec<_> = resumable.iter().map(|r| r.gid.as_str()).collect();
        assert_eq!(gids, vec!["a", "b"]);
        std::fs::remove_file(&p).ok();
        let _ = std::fs::remove_file(format!("{}-wal", p.display()));
        let _ = std::fs::remove_file(format!("{}-shm", p.display()));
    }
}
