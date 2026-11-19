//! Small cancellation-aware helpers.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use meili_connect_core::{EventSender, SourceEvent};
use rand::Rng;
use tokio_util::sync::CancellationToken;

/// Send one event. `false` when cancelled or the engine went away.
pub async fn send(events: &EventSender, event: SourceEvent, cancel: &CancellationToken) -> bool {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        r = events.send(event) => r.is_ok(),
    }
}

/// Sleep unless cancelled. `false` when cancelled.
pub async fn sleep(d: Duration, cancel: &CancellationToken) -> bool {
    if d.is_zero() {
        return !cancel.is_cancelled();
    }
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        _ = tokio::time::sleep(d) => true,
    }
}

/// Exponential backoff with full jitter: `base * 2^attempt`, capped.
pub fn backoff(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let exp = base.saturating_mul(1u32 << attempt.min(16));
    let max = exp.min(cap);
    let millis = u64::try_from(max.as_millis()).unwrap_or(u64::MAX).max(1);
    let low = millis / 2;
    Duration::from_millis(rand::rng().random_range(low..=millis))
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded() {
        for attempt in 0..40 {
            let d = backoff(attempt, Duration::from_millis(100), Duration::from_secs(10));
            assert!(
                d <= Duration::from_secs(10) && d >= Duration::from_millis(50),
                "{d:?}"
            );
        }
    }

    #[tokio::test]
    async fn sleep_honours_cancel() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(!sleep(Duration::from_secs(3600), &cancel).await);
    }
}
