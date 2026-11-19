//! Can a Lab tenant still spend? (Lab mode only.)
//!
//! `GET {LAB_URL}/internal/accounts/{id}` with the instance credentials
//! (`X-Lab-Instance-Id` + `Authorization: Bearer <LAB_INSTANCE_SECRET>`),
//! like glutony's `AccountCreditCache`: an answer younger than
//! [`FRESH_FOR`] is reused without asking. When the Lab cannot answer, the
//! last known answer is used, whatever its age: within [`STALE_FOR`]
//! quietly, past it with a warning. A tenant the Lab never answered for is
//! allowed: an unreachable Lab never holds every sync. After a failed call
//! the Lab is not asked again for [`FAILURE_BACKOFF`] (any tenant), so a
//! hung Lab costs one timeout per backoff, not one per check.
//!
//! Only tenant ids shaped like a Lab account id (a UUID) are checked; any
//! other tenant (standalone, admin-chosen ids) can always spend.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::config::LabEventsConfig;
use crate::lab::H_INSTANCE_ID;
use crate::limits::LogLimiter;

/// A lookup younger than this is reused without asking the Lab.
pub const FRESH_FOR: Duration = Duration::from_secs(30);
/// Past this age, a lookup still answers when the Lab cannot, with a
/// warning.
pub const STALE_FOR: Duration = Duration::from_secs(300);
/// After a failed call, the Lab is not asked again for this long.
pub const FAILURE_BACKOFF: Duration = Duration::from_secs(10);
/// The `message` of a sync held because its team is out of credits.
pub const OUT_OF_CREDITS: &str = "out_of_credits";

/// `GET /internal/accounts/{id}`. `can_spend` is the contract's answer;
/// without it, an active account with a positive balance can spend
/// (glutony's rule).
#[derive(Debug, Clone, Deserialize)]
struct AccountLookup {
    #[serde(default)]
    active: bool,
    #[serde(default)]
    can_spend: Option<bool>,
    #[serde(default)]
    credits: Option<Credits>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
struct Credits {
    balance: i64,
}

impl AccountLookup {
    fn can_spend(&self) -> bool {
        self.can_spend
            .unwrap_or_else(|| self.active && self.credits.is_some_and(|c| c.balance > 0))
    }
}

#[derive(Debug, Clone, Copy)]
struct Cached {
    at: Instant,
    can_spend: bool,
}

/// Is `id` a canonical (hyphenated, lowercase or uppercase) UUID? Also
/// what makes it safe as a URL path segment.
pub fn is_lab_account(id: &str) -> bool {
    id.len() == 36
        && id.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

pub struct CreditCache {
    lab: LabEventsConfig,
    http: reqwest::Client,
    fresh: Duration,
    stale: Duration,
    entries: Mutex<HashMap<String, Cached>>,
    last_failure: Mutex<Option<Instant>>,
    log: Mutex<LogLimiter>,
}

impl std::fmt::Debug for CreditCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreditCache")
            .field("lab", &self.lab)
            .finish_non_exhaustive()
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl CreditCache {
    pub fn new(lab: LabEventsConfig) -> Result<Self, reqwest::Error> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self {
            lab,
            http,
            fresh: FRESH_FOR,
            stale: STALE_FOR,
            entries: Mutex::new(HashMap::new()),
            last_failure: Mutex::new(None),
            log: Mutex::new(LogLimiter::new(Duration::from_secs(300))),
        })
    }

    /// Other cache windows (tests, the end-to-end script).
    pub fn with_windows(mut self, fresh: Duration, stale: Duration) -> Self {
        self.fresh = fresh;
        self.stale = stale.max(fresh);
        self
    }

    fn url(&self, account: &str) -> String {
        format!("{}/internal/accounts/{account}", self.lab.url)
    }

    async fn fetch(&self, account: &str) -> Result<bool, String> {
        let resp = self
            .http
            .get(self.url(account))
            .header(H_INSTANCE_ID, &self.lab.instance_id)
            .bearer_auth(self.lab.instance_secret.expose())
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    "timeout".to_owned()
                } else {
                    "unreachable".to_owned()
                }
            })?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("status {}", status.as_u16()));
        }
        let lookup: AccountLookup = resp
            .json()
            .await
            .map_err(|_| "malformed answer".to_owned())?;
        Ok(lookup.can_spend())
    }

    /// May `tenant` keep spending? Never fails: see the module
    /// documentation for what an unreachable Lab means.
    pub async fn can_spend(&self, tenant: &str) -> bool {
        if !is_lab_account(tenant) {
            return true;
        }
        let cached = lock(&self.entries).get(tenant).copied();
        if let Some(c) = cached
            && c.at.elapsed() < self.fresh
        {
            return c.can_spend;
        }
        let backing_off = lock(&self.last_failure).is_some_and(|t| t.elapsed() < FAILURE_BACKOFF);
        let fetched = if backing_off {
            Err("backing off after a failed call".to_owned())
        } else {
            self.fetch(tenant).await
        };
        match fetched {
            Ok(can_spend) => {
                *lock(&self.last_failure) = None;
                lock(&self.entries).insert(
                    tenant.to_owned(),
                    Cached {
                        at: Instant::now(),
                        can_spend,
                    },
                );
                can_spend
            }
            Err(reason) => {
                if !backing_off {
                    *lock(&self.last_failure) = Some(Instant::now());
                }
                match cached {
                    Some(c) if c.at.elapsed() < self.stale => c.can_spend,
                    Some(c) => {
                        if let Some(suppressed) = lock(&self.log).allow() {
                            tracing::warn!(
                                tenant,
                                reason,
                                suppressed,
                                age_secs = c.at.elapsed().as_secs(),
                                can_spend = c.can_spend,
                                "the Lab cannot tell whether this team can spend; keeping the \
                                 last known answer"
                            );
                        }
                        c.can_spend
                    }
                    None => {
                        if let Some(suppressed) = lock(&self.log).allow() {
                            tracing::warn!(
                                tenant,
                                reason,
                                suppressed,
                                "the Lab cannot tell whether this team can spend and never did; \
                                 letting its syncs run"
                            );
                        }
                        true
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use meili_connect_core::Secret;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "11111111-2222-3333-4444-555555555555";

    fn cache(server: &MockServer) -> CreditCache {
        CreditCache::new(LabEventsConfig {
            url: server.uri(),
            instance_id: "inst-1".into(),
            instance_secret: Secret::new("instance-secret"),
        })
        .unwrap()
    }

    async fn answer(server: &MockServer, body: serde_json::Value) {
        server.reset().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .and(header("x-lab-instance-id", "inst-1"))
            .and(header("authorization", "Bearer instance-secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    #[test]
    fn only_lab_account_ids_are_checked() {
        assert!(is_lab_account(ACCOUNT));
        assert!(is_lab_account(&ACCOUNT.to_uppercase()));
        for id in [
            "team-a",
            "..",
            "11111111-2222-3333-4444-55555555555",
            "x".repeat(36).as_str(),
        ] {
            assert!(!is_lab_account(id), "{id}");
        }
    }

    #[tokio::test]
    async fn the_answer_follows_the_lab_and_is_cached_while_fresh() {
        let server = MockServer::start().await;
        answer(&server, json!({"active": true, "can_spend": true})).await;
        let c = cache(&server).with_windows(Duration::from_millis(200), Duration::from_secs(5));
        assert!(c.can_spend(ACCOUNT).await);
        answer(&server, json!({"active": true, "can_spend": false})).await;
        assert!(c.can_spend(ACCOUNT).await, "still fresh");
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!c.can_spend(ACCOUNT).await, "asked again once stale");
        // Without `can_spend`, glutony's rule: active with a positive balance.
        answer(&server, json!({"active": true, "credits": {"balance": 0}})).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(!c.can_spend(ACCOUNT).await);
        answer(&server, json!({"active": true, "credits": {"balance": 12}})).await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(c.can_spend(ACCOUNT).await);
        // Not a Lab account: never asked.
        server.reset().await;
        assert!(c.can_spend("team-a").await);
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unreachable_lab_keeps_the_last_answer_and_never_holds_the_unknown() {
        let server = MockServer::start().await;
        answer(&server, json!({"active": true, "can_spend": false})).await;
        let c = cache(&server).with_windows(Duration::from_millis(50), Duration::from_millis(100));
        assert!(!c.can_spend(ACCOUNT).await);
        // The Lab fails: within the stale window, then past it, the last
        // known answer holds.
        server.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(!c.can_spend(ACCOUNT).await, "stale but known");
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!c.can_spend(ACCOUNT).await, "past the stale window: kept");
        // Never answered for this one: allowed.
        let other = "99999999-2222-3333-4444-555555555555";
        assert!(c.can_spend(other).await);
        // One failed call, then the backoff: no hammering.
        let calls = server.received_requests().await.unwrap().len();
        for _ in 0..5 {
            c.can_spend(other).await;
        }
        assert_eq!(server.received_requests().await.unwrap().len(), calls);
    }

    #[tokio::test]
    async fn a_dead_lab_lets_everything_run() {
        let c = CreditCache::new(LabEventsConfig {
            url: "http://127.0.0.1:9".into(),
            instance_id: "i".into(),
            instance_secret: Secret::new("s"),
        })
        .unwrap();
        assert!(c.can_spend(ACCOUNT).await);
    }
}
