use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// 单个下载源。
#[derive(Debug)]
pub struct Source {
    pub url: String,
    fail_count: AtomicUsize,
    pub is_failed: AtomicBool,
    pub max_fails: usize,
}

impl Source {
    pub fn new(url: String) -> Self {
        Self {
            url,
            fail_count: AtomicUsize::new(0),
            is_failed: AtomicBool::new(false),
            max_fails: 5,
        }
    }
}

/// 多源管理器：轮转分配、失败累计禁用。
pub struct SourceManager {
    sources: Vec<Arc<Source>>,
    idx: Mutex<usize>,
}

impl SourceManager {
    pub fn new(urls: &[String]) -> Self {
        let sources = urls
            .iter()
            .map(|u| Arc::new(Source::new(u.clone())))
            .collect();
        Self {
            sources,
            idx: Mutex::new(0),
        }
    }

    /// 返回下一个未禁用的源；全部禁用返回 None。
    pub fn get_source(&self) -> Option<Arc<Source>> {
        let len = self.sources.len();
        if len == 0 {
            return None;
        }
        let mut idx = self.idx.lock().unwrap();
        for _ in 0..len {
            let src = &self.sources[*idx % len];
            *idx += 1;
            if !src.is_failed.load(Ordering::Relaxed) {
                return Some(Arc::clone(src));
            }
        }
        None
    }

    pub fn report_failure(&self, source: &Source) {
        let count = source.fail_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= source.max_fails {
            // 仅在「未禁用 → 禁用」的瞬间记一次，避免后续失败重复刷日志
            if !source.is_failed.swap(true, Ordering::Relaxed) {
                tracing::warn!("下载源 {} 连续失败 {} 次，已禁用", source.url, count);
            }
        }
    }

    pub fn report_success(&self, source: &Source) {
        let count = source.fail_count.load(Ordering::Relaxed);
        if count > 0 {
            source.fail_count.store(count - 1, Ordering::Relaxed);
        }
    }

    /// 探测失败的直接禁用。
    pub fn disable(&self, url: &str) {
        for s in &self.sources {
            if s.url == url {
                s.is_failed.store(true, Ordering::Relaxed);
            }
        }
    }

    /// 全部源是否已禁用（仅供单元测试断言使用）。
    #[allow(dead_code)]
    pub fn all_failed(&self) -> bool {
        self.sources
            .iter()
            .all(|s| s.is_failed.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotation_and_failure_disable() {
        let mgr = SourceManager::new(&["a".into(), "b".into()]);
        let a = mgr.get_source().unwrap();
        let b = mgr.get_source().unwrap();
        assert_eq!(a.url, "a");
        assert_eq!(b.url, "b");
        // 让 a 失败到禁用
        for _ in 0..5 {
            mgr.report_failure(&a);
        }
        assert!(a.is_failed.load(Ordering::Relaxed));
        // 之后只能拿到 b
        let x = mgr.get_source().unwrap();
        assert_eq!(x.url, "b");
    }

    #[test]
    fn all_failed_when_everything_disabled() {
        let mgr = SourceManager::new(&["a".into()]);
        let a = mgr.get_source().unwrap();
        for _ in 0..5 {
            mgr.report_failure(&a);
        }
        assert!(mgr.all_failed());
        assert!(mgr.get_source().is_none());
    }
}
