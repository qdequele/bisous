//! Quotas and bounds: syncs per tenant, concurrent source probes, request
//! time, dead letters, the Lab outbox backlog. Each one keeps a tenant (or
//! an unreachable Lab) from growing the server's work or storage without
//! bound.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::ApiError;

/// The configured bounds (see `.env.example`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Limits {
    /// Syncs one tenant may own (`MAX_SYNCS_PER_TENANT`). The global scope
    /// (standalone admin) is not limited.
    pub max_syncs_per_tenant: u64,
    /// `check` / `discover` running at once, in all (`PROBE_CONCURRENCY`).
    pub probe_concurrency: usize,
    /// … and per tenant (`PROBE_CONCURRENCY_PER_TENANT`).
    pub probe_concurrency_per_tenant: usize,
    /// Longest an API request may take (`REQUEST_TIMEOUT_SECS`). Changes to
    /// a sync run to completion even when the request times out.
    pub request_timeout: Duration,
    /// Dead letters stored per sync (`DEAD_LETTERS_MAX_PER_SYNC`).
    pub dead_letters_max_per_sync: u64,
    /// Unsent Lab events beyond which usage stays counted in memory instead
    /// of being enqueued (`LAB_OUTBOX_MAX_PENDING`).
    pub outbox_max_pending: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_syncs_per_tenant: 20,
            probe_concurrency: 16,
            probe_concurrency_per_tenant: 4,
            request_timeout: Duration::from_secs(180),
            dead_letters_max_per_sync: 10_000,
            outbox_max_pending: 100_000,
        }
    }
}

/// Slots for `check` and `discover`: one global pool, one pool per tenant.
/// A call that finds no free slot is refused (`429`), never queued.
pub struct ProbeSlots {
    global: Arc<Semaphore>,
    per_tenant_max: usize,
    per_tenant: Mutex<HashMap<String, Arc<Semaphore>>>,
}

/// Held for the duration of one probe.
pub struct ProbePermit {
    _global: OwnedSemaphorePermit,
    _tenant: OwnedSemaphorePermit,
}

impl ProbeSlots {
    pub fn new(global: usize, per_tenant: usize) -> Self {
        Self {
            global: Arc::new(Semaphore::new(global.max(1))),
            per_tenant_max: per_tenant.max(1),
            per_tenant: Mutex::new(HashMap::new()),
        }
    }

    pub fn try_acquire(&self, tenant: Option<&str>) -> Result<ProbePermit, ApiError> {
        let busy = || {
            ApiError::new(
                StatusCode::TOO_MANY_REQUESTS,
                "too_many_probes",
                "too many source checks are running; retry in a moment",
            )
        };
        let tenant_pool = {
            let mut map = self
                .per_tenant
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Forget the pools nobody uses, so the map stays small.
            let max = self.per_tenant_max;
            map.retain(|_, s| Arc::strong_count(s) > 1 || s.available_permits() < max);
            Arc::clone(
                map.entry(tenant.unwrap_or_default().to_owned())
                    .or_insert_with(|| Arc::new(Semaphore::new(max))),
            )
        };
        let tenant = tenant_pool.try_acquire_owned().map_err(|_| busy())?;
        let global = Arc::clone(&self.global)
            .try_acquire_owned()
            .map_err(|_| busy())?;
        Ok(ProbePermit {
            _global: global,
            _tenant: tenant,
        })
    }
}

/// Bounds every request by `timeout`.
pub async fn request_timeout(
    State(timeout): State<Duration>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "request_timeout",
            format!(
                "the request did not complete within {} s",
                timeout.as_secs()
            ),
        )
        .into_response(),
    }
}

/// Lets a repeated log line through at most once per `every`, counting
/// what it held back.
#[derive(Debug)]
pub struct LogLimiter {
    every: Duration,
    last: Option<Instant>,
    suppressed: u64,
}

impl LogLimiter {
    pub fn new(every: Duration) -> Self {
        Self {
            every,
            last: None,
            suppressed: 0,
        }
    }

    /// `Some(n)` when the line may be logged now, `n` being how many were
    /// held back since the last one.
    pub fn allow(&mut self) -> Option<u64> {
        let now = Instant::now();
        if self
            .last
            .is_none_or(|t| now.duration_since(t) >= self.every)
        {
            self.last = Some(now);
            Some(std::mem::take(&mut self.suppressed))
        } else {
            self.suppressed += 1;
            None
        }
    }

    /// Start over (the condition cleared).
    pub fn reset(&mut self) {
        self.last = None;
        self.suppressed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_slots_are_bounded_globally_and_per_tenant() {
        let slots = ProbeSlots::new(3, 2);
        let a1 = slots.try_acquire(Some("a")).unwrap();
        let _a2 = slots.try_acquire(Some("a")).unwrap();
        assert_eq!(
            slots.try_acquire(Some("a")).err().map(|e| e.code),
            Some("too_many_probes")
        );
        let _b1 = slots.try_acquire(Some("b")).unwrap();
        // Global pool exhausted.
        assert!(slots.try_acquire(Some("c")).is_err());
        drop(a1);
        assert!(slots.try_acquire(Some("c")).is_ok());
    }

    #[test]
    fn the_limiter_logs_once_per_period() {
        let mut l = LogLimiter::new(Duration::from_secs(3600));
        assert_eq!(l.allow(), Some(0));
        assert_eq!(l.allow(), None);
        assert_eq!(l.allow(), None);
        l.reset();
        assert_eq!(l.allow(), Some(0));
        let mut fast = LogLimiter::new(Duration::ZERO);
        assert_eq!(fast.allow(), Some(0));
        assert_eq!(fast.allow(), Some(0));
    }
}
