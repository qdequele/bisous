//! `Scan` of each table: paginated, optionally parallel segments, optional
//! RCU rate limit, throttling absorbed with backoff.
//!
//! Memory is bounded by one page (≤ 1 MB, or `page_limit` items) per
//! segment plus the engine's bounded channel.

use std::time::Duration;

use aws_sdk_dynamodb::types::ReturnConsumedCapacity;
use meili_connect_core::{EventSender, SourceError, SourceEvent};
use tokio::sync::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::config::ScanOptions;
use crate::convert::item_to_change;
use crate::errors::{ErrClass, classify, to_source_error};
use crate::source::TableInfo;
use crate::util::{backoff, send, sleep};

/// Consecutive throttled pages tolerated before giving up as `Transient`.
const MAX_THROTTLES: u32 = 40;

/// Shared by a table's segments: spaces requests so consumed read capacity
/// stays under `rate` RCU/s on average.
struct RateLimiter {
    rate: f64,
    next: Mutex<Instant>,
}

impl RateLimiter {
    fn new(rate: f64) -> Self {
        Self {
            rate,
            next: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self, cancel: &CancellationToken) -> bool {
        let at = *self.next.lock().await;
        sleep(at.saturating_duration_since(Instant::now()), cancel).await
    }

    async fn consume(&self, units: f64) {
        let secs = (units / self.rate).clamp(0.0, 3600.0);
        let delay = Duration::try_from_secs_f64(secs).unwrap_or_default();
        let mut next = self.next.lock().await;
        let base = (*next).max(Instant::now());
        *next = base + delay;
    }
}

/// `Ok(true)` when the table was read completely, `Ok(false)` when the
/// engine went away.
pub async fn scan_table(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &TableInfo,
    opts: &ScanOptions,
    events: &EventSender,
    cancel: &CancellationToken,
) -> Result<bool, SourceError> {
    let limiter = opts.max_rcu_per_second.map(RateLimiter::new);
    let total = opts.segments.max(1);
    let segments = (0..total).map(|seg| {
        let segment = (total > 1).then_some((seg, total));
        scan_segment(
            dynamo,
            table,
            opts,
            segment,
            limiter.as_ref(),
            events,
            cancel,
        )
    });
    let done = futures::future::try_join_all(segments).await?;
    Ok(done.into_iter().all(|d| d))
}

async fn scan_segment(
    dynamo: &aws_sdk_dynamodb::Client,
    table: &TableInfo,
    opts: &ScanOptions,
    segment: Option<(u32, u32)>,
    limiter: Option<&RateLimiter>,
    events: &EventSender,
    cancel: &CancellationToken,
) -> Result<bool, SourceError> {
    let mut start_key = None;
    let mut throttles = 0u32;
    loop {
        if let Some(l) = limiter
            && !l.wait(cancel).await
        {
            return Ok(false);
        }
        let mut req = dynamo
            .scan()
            .table_name(&table.name)
            .set_exclusive_start_key(start_key.clone())
            .return_consumed_capacity(ReturnConsumedCapacity::Total);
        if let Some(limit) = opts.page_limit {
            req = req.limit(i32::try_from(limit).unwrap_or(i32::MAX));
        }
        if let Some((seg, total)) = segment {
            req = req
                .segment(i32::try_from(seg).unwrap_or_default())
                .total_segments(i32::try_from(total).unwrap_or(1));
        }
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(false),
            r = req.send() => r,
        };
        let page = match result {
            Ok(page) => page,
            Err(e) => {
                let (class, msg) = classify("Scan", &e);
                if class == ErrClass::Throttle && throttles < MAX_THROTTLES {
                    throttles += 1;
                    tracing::debug!(table = %table.name, throttles, "scan throttled, backing off");
                    let wait = backoff(
                        throttles,
                        Duration::from_millis(100),
                        Duration::from_secs(20),
                    );
                    if !sleep(wait, cancel).await {
                        return Ok(false);
                    }
                    continue;
                }
                return Err(to_source_error(class, msg));
            }
        };
        throttles = 0;
        let items = page.items.unwrap_or_default();
        if let Some(l) = limiter {
            let units = page
                .consumed_capacity
                .as_ref()
                .and_then(|c| c.capacity_units())
                // Lower bound when the endpoint does not report it.
                .unwrap_or(items.len() as f64 * 0.5);
            l.consume(units).await;
        }
        for item in &items {
            let change = item_to_change(&table.name, &table.key_names, item)?;
            if !send(events, SourceEvent::Change(change), cancel).await {
                return Ok(false);
            }
        }
        match page.last_evaluated_key {
            Some(k) if !k.is_empty() => start_key = Some(k),
            _ => return Ok(true),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn rate_limiter_spaces_requests() {
        let l = RateLimiter::new(10.0);
        let cancel = CancellationToken::new();
        let t0 = Instant::now();
        assert!(l.wait(&cancel).await);
        l.consume(20.0).await; // 2 s worth at 10 RCU/s
        assert!(l.wait(&cancel).await);
        let waited = Instant::now() - t0;
        assert!(
            waited >= Duration::from_secs(2) && waited < Duration::from_millis(2100),
            "{waited:?}"
        );
    }
}
