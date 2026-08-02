use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;

/// 全局令牌桶限速器。`rate == 0` 表示不限速。
/// 10ms 粒度，最多攒 1 秒令牌。`consume` 可被 CancellationToken 取消。
#[derive(Clone)]
pub struct TokenBucket {
    inner: Arc<Inner>,
}

struct Inner {
    rate: AtomicU64,
    tokens: AtomicU64,
    last_refill: Mutex<Instant>,
}

use std::sync::Mutex;

impl TokenBucket {
    pub fn new(rate: u64) -> Self {
        Self {
            inner: Arc::new(Inner {
                rate: AtomicU64::new(rate),
                tokens: AtomicU64::new(0),
                last_refill: Mutex::new(Instant::now()),
            }),
        }
    }

    #[allow(dead_code)]
    pub fn set_rate(&self, rate: u64) {
        self.inner.rate.store(rate, Ordering::Relaxed);
    }

    #[allow(dead_code)]
    pub fn rate(&self) -> u64 {
        self.inner.rate.load(Ordering::Relaxed)
    }

    fn refill(&self, now: Instant) {
        let mut last = match self.inner.last_refill.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if now < *last {
            return;
        }
        let elapsed = now.duration_since(*last).as_secs_f64();
        if elapsed <= 0.0 {
            return;
        }
        let rate = self.inner.rate.load(Ordering::Relaxed);
        let mut tokens = self.inner.tokens.load(Ordering::Relaxed);
        let add = (elapsed * rate as f64) as u64;
        tokens = (tokens + add).min(rate); // 最多攒 1 秒
        self.inner.tokens.store(tokens, Ordering::Relaxed);
        *last = now;
    }

    /// 等待直到拿到 n 字节的令牌。若不限速则立即返回。
    /// 等待期间 `cancel` 被取消则立即返回（即使令牌不足）。
    pub async fn consume(&self, n_bytes: usize, cancel: &CancellationToken) {
        let rate = self.inner.rate.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        let n = n_bytes as u64;
        loop {
            if cancel.is_cancelled() {
                return;
            }
            let now = Instant::now();
            self.refill(now);
            // 使用 CAS 循环原子扣减令牌，避免多 worker 竞态超扣
            let mut tokens = self.inner.tokens.load(Ordering::Relaxed);
            loop {
                if tokens < n {
                    break;
                }
                match self.inner.tokens.compare_exchange_weak(
                    tokens,
                    tokens - n,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(actual) => tokens = actual,
                }
            }
            let deficit = n.saturating_sub(tokens);
            let wait = (deficit as f64 / rate as f64).min(0.01);
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs_f64(wait)) => {}
                _ = cancel.cancelled() => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn no_limit_returns_immediately() {
        let bucket = TokenBucket::new(0);
        let t0 = Instant::now();
        bucket.consume(100_000, &CancellationToken::new()).await;
        assert!(t0.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn limited_rate_throttles() {
        let bucket = TokenBucket::new(1000);
        let t0 = Instant::now();
        bucket.consume(500, &CancellationToken::new()).await;
        assert!(t0.elapsed() >= Duration::from_millis(450));
    }

    #[tokio::test]
    async fn cancelled_consume_returns_immediately() {
        // 极低速率 + 大量令牌需求：未取消时应长时间阻塞，取消后立即返回
        let bucket = TokenBucket::new(100);
        let cancel = CancellationToken::new();
        cancel.cancel();
        let t0 = Instant::now();
        bucket.consume(10 * 1024 * 1024, &cancel).await;
        assert!(t0.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn cancelled_during_wait_returns_immediately() {
        let bucket = TokenBucket::new(100);
        let cancel = CancellationToken::new();
        let t0 = Instant::now();
        // 先睡 10ms 等待令牌，期间取消
        let task = tokio::spawn({
            let bucket = bucket.clone();
            let cancel = cancel.clone();
            async move { bucket.consume(10 * 1024 * 1024, &cancel).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        cancel.cancel();
        task.await.unwrap();
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "取消后应快速返回"
        );
    }

    #[test]
    fn bucket_caps_at_one_second() {
        let bucket = TokenBucket::new(1000);
        // 模拟 10 秒过去
        let mut last = bucket.inner.last_refill.lock().unwrap();
        *last = Instant::now() - Duration::from_secs(10);
        drop(last);
        bucket.refill(Instant::now());
        assert_eq!(bucket.inner.tokens.load(Ordering::Relaxed), 1000);
    }
}
