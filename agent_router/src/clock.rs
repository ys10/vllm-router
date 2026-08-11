use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockReading {
    pub unix_ms: u64,
    pub monotonic_ms: u64,
}

pub trait Clock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> ClockReading;
}

#[derive(Debug)]
pub struct SystemClock {
    origin: Instant,
}

impl Default for SystemClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> ClockReading {
        let unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let monotonic_ms = self
            .origin
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        ClockReading {
            unix_ms,
            monotonic_ms,
        }
    }
}

#[derive(Debug)]
pub struct ManualClock {
    unix_ms: AtomicU64,
    monotonic_ms: AtomicU64,
}

impl ManualClock {
    pub fn new(unix_ms: u64, monotonic_ms: u64) -> Self {
        Self {
            unix_ms: AtomicU64::new(unix_ms),
            monotonic_ms: AtomicU64::new(monotonic_ms),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let delta = duration.as_millis().try_into().unwrap_or(u64::MAX);
        self.unix_ms.fetch_add(delta, Ordering::SeqCst);
        self.monotonic_ms.fetch_add(delta, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> ClockReading {
        ClockReading {
            unix_ms: self.unix_ms.load(Ordering::SeqCst),
            monotonic_ms: self.monotonic_ms.load(Ordering::SeqCst),
        }
    }
}
