//! Live Postgres tests. They skip (pass with a notice) when `DATABASE_URL`
//! is unset. Each test gets its own schema, so they run in parallel.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::Arc;

use sqlx::Connection;

use super::*;
use crate::test_support::{invalid_specs, letter, pr, skipped_letter, spec, state};

async fn store_with_key(key: SecretKey) -> Option<PgStore> {
    store_with(key, &[]).await
}

/// A store whose connections also carry these server settings.
async fn store_with(key: SecretKey, settings: &[(&str, &str)]) -> Option<PgStore> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL is unset: skipping Postgres store test");
        return None;
    };
    let schema = format!("mc_test_{}", Uuid::new_v4().simple());
    let mut conn = sqlx::PgConnection::connect(&url).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&mut conn)
        .await
        .unwrap();
    conn.close().await.unwrap();
    let options = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())])
        .options(settings.iter().copied());
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .unwrap();
    Some(PgStore::from_pool(pool, key).await.unwrap())
}

async fn store() -> Option<PgStore> {
    store_with_key(SecretKey::generate()).await
}

#[tokio::test]
async fn connect_and_migrations_are_idempotent() {
    let Some(store) = store().await else { return };
    let again = PgStore::from_pool(store.pool().clone(), SecretKey::generate()).await;
    assert!(again.is_ok());
    let url = Secret::new(std::env::var("DATABASE_URL").unwrap());
    assert!(PgStore::connect(&url, SecretKey::generate()).await.is_ok());
    let bad = PgStore::connect(&Secret::new("not a url"), SecretKey::generate()).await;
    assert!(matches!(bad, Err(StoreError::Fatal(m)) if !m.contains("not a url")));
}

#[tokio::test]
async fn state_round_trip() {
    let Some(store) = store().await else { return };
    // State needs its sync.
    assert!(matches!(
        store.save(&state("ghost")).await,
        Err(StoreError::NotFound(_))
    ));
    store
        .create_sync(None, &spec("s1", None, "n"))
        .await
        .unwrap();
    assert!(store.load("s1").await.unwrap().is_none());
    let mut st = state("s1");
    store.save(&st).await.unwrap();
    assert_eq!(store.load("s1").await.unwrap(), Some(st.clone()));
    st.phase = SyncPhase::Held;
    st.message = Some("mass delete guard".into());
    st.checkpoint = None;
    st.resyncing = true;
    store.save(&st).await.unwrap();
    assert_eq!(store.load("s1").await.unwrap(), Some(st.clone()));
    // The resync's reason survives a restart.
    for reason in [ResyncReason::System, ResyncReason::User] {
        st.resync_reason = Some(reason);
        store.save(&st).await.unwrap();
        assert_eq!(store.load("s1").await.unwrap(), Some(st.clone()));
    }
    // A row written before the column existed reads as no reason.
    sqlx::query("UPDATE sync_state SET resync_reason = NULL")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(store.load("s1").await.unwrap().unwrap().resync_reason, None);
}

#[tokio::test]
async fn phases_and_dead_letters_are_counted() {
    let Some(store) = store().await else { return };
    crate::test_support::phases_and_letters_contract(&store).await;
}

#[tokio::test]
async fn usage_is_durable_per_lane() {
    let Some(store) = store().await else { return };
    crate::test_support::ledger_contract(&store).await;
}

/// Progress recorded while drains run: every unit ends up in exactly one
/// event.
#[tokio::test]
async fn concurrent_progress_and_drains_lose_and_double_nothing() {
    let Some(store) = store().await else { return };
    let store = Arc::new(store);
    store.set_usage_lanes(&[Lane::Lab]);
    store
        .create_sync(Some("t"), &spec("s", Some("t"), "a"))
        .await
        .unwrap();
    let writers: Vec<_> = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                for _ in 0..25 {
                    store
                        .record_progress("s", &crate::test_support::units(1, 0, 0, 3, 0))
                        .await
                        .unwrap();
                }
            })
        })
        .collect();
    let build = |p: &PendingUsage| serde_json::json!({ "n": p.usage.documents_streamed, "b": p.usage.bytes });
    let drainer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            for _ in 0..40 {
                store.drain_usage(Lane::Lab, None, 0, &build).await.unwrap();
                tokio::task::yield_now().await;
            }
        })
    };
    for w in writers {
        w.await.unwrap();
    }
    drainer.await.unwrap();
    store.drain_usage(Lane::Lab, None, 0, &build).await.unwrap();
    let lease = store
        .lease_events(10_000, Duration::from_secs(30))
        .await
        .unwrap();
    let docs: u64 = lease
        .events
        .iter()
        .map(|e| e.payload["n"].as_u64().unwrap())
        .sum();
    let bytes: u64 = lease
        .events
        .iter()
        .map(|e| e.payload["b"].as_u64().unwrap())
        .sum();
    assert_eq!((docs, bytes), (200, 600));
}

#[tokio::test]
async fn units_add_up_per_day() {
    let Some(store) = store().await else { return };
    store
        .create_sync(None, &spec("s", None, "a"))
        .await
        .unwrap();
    store
        .record_progress("s", &crate::test_support::units(2, 3, 4, 100, 7))
        .await
        .unwrap();
    store
        .record_progress("s", &crate::test_support::units(1, 0, 0, 10, 1))
        .await
        .unwrap();
    let today = Utc::now().date_naive();
    let p = store.progress(None, "s", today, today).await.unwrap();
    assert_eq!(
        p[0].usage,
        Usage {
            documents_streamed: 3,
            documents_snapshot: 3,
            documents_resync: 4,
            bytes: 110,
            sync_seconds: 8,
        }
    );
    assert_eq!(p[0].written, 10);
}

#[tokio::test]
async fn specs_are_sealed_at_rest() {
    let Some(store) = store().await else { return };
    store
        .create_sync(Some("t"), &spec("s1", Some("t"), "a"))
        .await
        .unwrap();
    store
        .create_sync(Some("u"), &spec("s2", Some("u"), "b"))
        .await
        .unwrap();
    let raw: Vec<u8> = sqlx::query_scalar("SELECT spec_sealed FROM syncs WHERE id = 's1'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(raw[0], 1);
    let text = String::from_utf8_lossy(&raw);
    assert!(!text.contains("hunter2") && !text.contains("meili-key"));

    // A wrong key fails clearly.
    let other = PgStore {
        pool: store.pool().clone(),
        key: SecretKey::generate(),
        limits: DeadLetterLimits::default(),
        lanes: AtomicU8::new(0),
    };
    let row = other.get_sync(Some("t"), "s1").await.unwrap().unwrap();
    assert!(
        matches!(&row, SyncRow::Unreadable(u) if u.name == "a"),
        "{row:?}"
    );
    // One unreadable row does not hide the others.
    let listed = other.list_syncs(Some("t")).await.unwrap();
    assert!(listed.iter().all(|r| matches!(r, SyncRow::Unreadable(_))));
    assert!(!listed.is_empty());

    // A blob moved onto another tenant's row is refused, and boot reports
    // the bad row without failing the others.
    sqlx::query("UPDATE syncs SET spec_sealed = $1 WHERE id = 's2'")
        .bind(&raw)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.get_sync(Some("u"), "s2").await,
        Ok(Some(SyncRow::Unreadable(_)))
    ));
    let boot = store.list_all_for_boot().await.unwrap();
    assert_eq!(boot.len(), 2);
    assert_eq!(boot[0].spec.as_ref().unwrap().id, "s1");
    assert!(boot[1].spec.is_err());
    assert_eq!(boot[1].tenant.as_deref(), Some("u"));
}

#[tokio::test]
async fn tenant_isolation_on_every_method() {
    let Some(store) = store().await else { return };
    let a = spec("a1", Some("ta"), "orders");
    let g = spec("g1", None, "orders");
    store.create_sync(Some("ta"), &a).await.unwrap();
    // The same name in another scope is fine.
    store
        .create_sync(Some("tb"), &spec("b1", Some("tb"), "orders"))
        .await
        .unwrap();
    store.create_sync(None, &g).await.unwrap();

    // create: same name in scope (tenant and global), id reuse, mismatch.
    assert!(matches!(
        store
            .create_sync(Some("ta"), &spec("a2", Some("ta"), "orders"))
            .await,
        Err(RepoError::Conflict(_))
    ));
    assert!(matches!(
        store.create_sync(None, &spec("g2", None, "orders")).await,
        Err(RepoError::Conflict(_))
    ));
    assert!(matches!(
        store
            .create_sync(Some("tb"), &spec("a1", Some("tb"), "x"))
            .await,
        Err(RepoError::Conflict(_))
    ));
    assert!(matches!(
        store.create_sync(Some("tb"), &a).await,
        Err(RepoError::Invalid(_))
    ));

    // get
    assert_eq!(
        store
            .get_sync(Some("ta"), "a1")
            .await
            .unwrap()
            .unwrap()
            .readable()
            .unwrap()
            .spec,
        a
    );
    assert!(store.get_sync(Some("tb"), "a1").await.unwrap().is_none());
    assert!(store.get_sync(None, "a1").await.unwrap().is_none());
    assert!(store.get_sync(Some("ta"), "g1").await.unwrap().is_none());

    // list
    let ids = |v: Vec<SyncRow>| v.iter().map(|s| s.id().to_owned()).collect::<Vec<_>>();
    assert_eq!(ids(store.list_syncs(Some("ta")).await.unwrap()), ["a1"]);
    assert_eq!(ids(store.list_syncs(Some("tb")).await.unwrap()), ["b1"]);
    assert_eq!(ids(store.list_syncs(None).await.unwrap()), ["g1"]);
    assert!(store.list_syncs(Some("tc")).await.unwrap().is_empty());

    // update
    let mut stolen = a.clone();
    stolen.tenant = Some("tb".into());
    stolen.name = "mine".into();
    assert!(matches!(
        store.update_sync(Some("tb"), &stolen).await,
        Err(RepoError::NotFound(_))
    ));
    let mut renamed = a.clone();
    renamed.name = "orders-v2".into();
    let updated = store.update_sync(Some("ta"), &renamed).await.unwrap();
    assert_eq!(updated.spec.name, "orders-v2");
    assert!(updated.updated_at >= updated.created_at);
    assert_eq!(
        store
            .get_sync(Some("ta"), "a1")
            .await
            .unwrap()
            .unwrap()
            .readable()
            .unwrap()
            .spec,
        renamed
    );
    let a3 = spec("a3", Some("ta"), "other");
    store.create_sync(Some("ta"), &a3).await.unwrap();
    let mut clash = a3.clone();
    clash.name = "orders-v2".into();
    assert!(matches!(
        store.update_sync(Some("ta"), &clash).await,
        Err(RepoError::Conflict(_))
    ));
    store.delete_sync(Some("ta"), "a3").await.unwrap();

    // dead letters, progress, delete
    store.push_dead_letters(&[letter("a1", "1")]).await.unwrap();
    store.record_progress("a1", &pr(1, 0, 1)).await.unwrap();
    let today = Utc::now().date_naive();
    let page = DeadLetterPage::default();
    for t in [Some("tb"), None] {
        assert!(matches!(
            store.list_dead_letters(t, "a1", page).await,
            Err(RepoError::NotFound(_))
        ));
        assert!(matches!(
            store.delete_dead_letters(t, "a1", &[1]).await,
            Err(RepoError::NotFound(_))
        ));
        assert!(matches!(
            store.clear_dead_letters(t, "a1").await,
            Err(RepoError::NotFound(_))
        ));
        assert!(matches!(
            store.progress(t, "a1", today, today).await,
            Err(RepoError::NotFound(_))
        ));
        assert!(matches!(
            store.delete_sync(t, "a1").await,
            Err(RepoError::NotFound(_))
        ));
    }
    assert_eq!(
        store
            .list_dead_letters(Some("ta"), "a1", page)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .progress(Some("ta"), "a1", today, today)
            .await
            .unwrap()
            .len(),
        1
    );

    // boot sees every tenant
    assert_eq!(store.list_all_for_boot().await.unwrap().len(), 3);
    store.delete_sync(Some("ta"), "a1").await.unwrap();
    assert!(store.get_sync(Some("ta"), "a1").await.unwrap().is_none());
}

#[tokio::test]
async fn delete_cascades() {
    let Some(store) = store().await else { return };
    store
        .create_sync(None, &spec("s", None, "a"))
        .await
        .unwrap();
    store
        .create_sync(None, &spec("o", None, "b"))
        .await
        .unwrap();
    store.save(&state("s")).await.unwrap();
    store.save(&state("o")).await.unwrap();
    store
        .push_dead_letters(&[letter("s", "1"), letter("s", "2"), letter("o", "3")])
        .await
        .unwrap();
    store.record_progress("s", &pr(1, 1, 1)).await.unwrap();
    store.record_progress("o", &pr(1, 1, 1)).await.unwrap();

    store.delete_sync(None, "s").await.unwrap();
    for table in ["sync_state", "dead_letters", "sync_progress"] {
        let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {table} WHERE sync_id = 's'"
        )))
        .fetch_one(store.pool())
        .await
        .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    assert!(store.load("o").await.unwrap().is_some());
    assert_eq!(
        store
            .list_dead_letters(None, "o", DeadLetterPage::default())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dead_letters_page_and_delete() {
    let Some(store) = store().await else { return };
    store
        .create_sync(None, &spec("s", None, "a"))
        .await
        .unwrap();
    store
        .create_sync(None, &spec("o", None, "b"))
        .await
        .unwrap();
    let letters: Vec<_> = (0..5).map(|i| letter("s", &i.to_string())).collect();
    store.push_dead_letters(&letters).await.unwrap();
    store.push_dead_letters(&[letter("o", "x")]).await.unwrap();
    store.push_dead_letters(&[]).await.unwrap();

    let p1 = store
        .list_dead_letters(
            None,
            "s",
            DeadLetterPage {
                after: None,
                limit: 2,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        p1.iter().map(|d| d.letter.clone()).collect::<Vec<_>>(),
        letters[..2].to_vec()
    );
    let p2 = store
        .list_dead_letters(
            None,
            "s",
            DeadLetterPage {
                after: Some(p1[1].id),
                limit: 10,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        p2.iter().map(|d| d.letter.clone()).collect::<Vec<_>>(),
        letters[2..].to_vec()
    );
    let other = store
        .list_dead_letters(None, "o", DeadLetterPage::default())
        .await
        .unwrap();
    // Ids of another sync are ignored.
    assert_eq!(
        store
            .delete_dead_letters(None, "s", &[p1[0].id, other[0].id])
            .await
            .unwrap(),
        1
    );
    assert_eq!(store.clear_dead_letters(None, "s").await.unwrap(), 4);
    assert_eq!(
        store
            .list_dead_letters(None, "o", DeadLetterPage::default())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(matches!(
        store.push_dead_letters(&[letter("ghost", "1")]).await,
        Err(StoreError::NotFound(_))
    ));
}

#[tokio::test]
async fn progress_is_additive() {
    let Some(store) = store().await else { return };
    let store = Arc::new(store);
    store
        .create_sync(None, &spec("s", None, "a"))
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store.record_progress("s", &pr(3, 2, 1)).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let today = Utc::now().date_naive();
    let yesterday = today.pred_opt().unwrap();
    sqlx::query(
        "INSERT INTO sync_progress (sync_id, day, written, deleted, rejected)
         VALUES ('s', $1, 7, 0, 0)",
    )
    .bind(yesterday)
    .execute(store.pool())
    .await
    .unwrap();
    let p = store.progress(None, "s", yesterday, today).await.unwrap();
    assert_eq!(
        p,
        [
            DailyProgress {
                day: yesterday,
                written: 7,
                deleted: 0,
                rejected: 0,
                dead_letters_dropped: 0,
                usage: Default::default(),
            },
            DailyProgress {
                day: today,
                written: 60,
                deleted: 40,
                rejected: 20,
                dead_letters_dropped: 0,
                usage: Default::default(),
            }
        ]
    );
    assert_eq!(
        store.progress(None, "s", today, today).await.unwrap().len(),
        1
    );
    assert!(
        store
            .progress(None, "s", today, yesterday)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn outbox_concurrent_leasers_never_share() {
    let Some(store) = store().await else { return };
    let store = Arc::new(store);
    let mut all = HashSet::new();
    for i in 0..200 {
        let id = store
            .enqueue_event(Some("t"), &serde_json::json!({ "n": i }))
            .await
            .unwrap();
        all.insert(id);
    }
    async fn leaser(store: Arc<PgStore>) -> Vec<(Uuid, Vec<Uuid>)> {
        let mut got = Vec::new();
        loop {
            let lease = store
                .lease_events(7, Duration::from_secs(300))
                .await
                .unwrap();
            if lease.events.is_empty() {
                return got;
            }
            got.push((
                lease.token,
                lease.events.into_iter().map(|e| e.id).collect(),
            ));
        }
    }
    let (la, lb) = tokio::join!(
        tokio::spawn(leaser(store.clone())),
        tokio::spawn(leaser(store.clone()))
    );
    let (la, lb) = (la.unwrap(), lb.unwrap());
    let flat = |l: &[(Uuid, Vec<Uuid>)]| {
        l.iter()
            .flat_map(|(_, ids)| ids.clone())
            .collect::<Vec<_>>()
    };
    let (a, b) = (flat(&la), flat(&lb));
    let sa: HashSet<_> = a.iter().copied().collect();
    let sb: HashSet<_> = b.iter().copied().collect();
    assert_eq!(sa.len(), a.len());
    assert_eq!(sb.len(), b.len());
    assert!(sa.is_disjoint(&sb), "an event was leased twice");
    assert_eq!(&sa | &sb, all);

    // Nothing is free while leases run; marking sent is idempotent.
    assert!(
        store
            .lease_events(10, Duration::from_secs(60))
            .await
            .unwrap()
            .events
            .is_empty()
    );
    let mut marked = 0;
    for (token, ids) in la.iter().chain(&lb) {
        // Another lease's token marks nothing.
        assert_eq!(
            store.mark_events_sent(Uuid::new_v4(), ids).await.unwrap(),
            0
        );
        marked += store.mark_events_sent(*token, ids).await.unwrap();
        assert_eq!(store.mark_events_sent(*token, ids).await.unwrap(), 0);
    }
    assert_eq!(marked, 200);
}

#[tokio::test]
async fn outbox_expired_lease_is_released_in_order() {
    let Some(store) = store().await else { return };
    let first = store
        .enqueue_event(None, &serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let second = store
        .enqueue_event(Some("t"), &serde_json::json!({ "n": 2 }))
        .await
        .unwrap();
    let stale = store.lease_events(10, Duration::ZERO).await.unwrap();
    let leased = &stale.events;
    assert_eq!(
        leased.iter().map(|e| e.id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(leased[1].tenant.as_deref(), Some("t"));
    assert_eq!(leased[1].payload, serde_json::json!({ "n": 2 }));
    let again = store
        .lease_events(1, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(again.events[0].id, first);
    // The expired lease was taken over: its holder can no longer mark it.
    assert_eq!(
        store.mark_events_sent(stale.token, &[first]).await.unwrap(),
        0
    );
    assert_eq!(
        store.mark_events_sent(again.token, &[first]).await.unwrap(),
        1
    );
    let rest = store
        .lease_events(10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(
        rest.events.iter().map(|e| e.id).collect::<Vec<_>>(),
        [second]
    );
    assert!(
        store
            .lease_events(0, Duration::ZERO)
            .await
            .unwrap()
            .events
            .is_empty()
    );
}

#[tokio::test]
async fn timeouts_and_read_only_transactions_are_transient() {
    let Some(store) = store().await else { return };
    // 57014: statement timeout.
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("SET LOCAL statement_timeout = 10")
        .execute(&mut *tx)
        .await
        .unwrap();
    let err = sqlx::query("SELECT pg_sleep(1)")
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert!(
        matches!(from_sqlx(err), RepoError::Transient(m) if m.contains("57014")),
        "statement timeout"
    );
    drop(tx);
    // 25006: what a demoted primary answers to a write.
    let mut tx = store.pool().begin().await.unwrap();
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .unwrap();
    let err = sqlx::query("INSERT INTO lab_events_outbox (id, payload) VALUES ($1, '{}')")
        .bind(Uuid::new_v4())
        .execute(&mut *tx)
        .await
        .unwrap_err();
    let mapped = StoreError::from(from_sqlx(err));
    assert!(
        matches!(&mapped, StoreError::Transient(m) if m.contains("25006")),
        "{mapped:?}"
    );
}

/// Under a nested-loop plan an `IN (SELECT … LIMIT n FOR UPDATE SKIP
/// LOCKED)` subquery can be re-run per outer row and lease more than `n`
/// rows. Force that plan shape and race leasers: no call may exceed its
/// limit, and no event may be leased twice.
#[tokio::test]
async fn outbox_lease_never_exceeds_its_limit() {
    let Some(store) = store_with(
        SecretKey::generate(),
        &[
            ("enable_hashjoin", "off"),
            ("enable_mergejoin", "off"),
            ("enable_material", "off"),
            ("enable_memoize", "off"),
            ("enable_indexscan", "off"),
            ("enable_bitmapscan", "off"),
        ],
    )
    .await
    else {
        return;
    };
    let store = Arc::new(store);
    for i in 0..600 {
        store
            .enqueue_event(None, &serde_json::json!({ "n": i }))
            .await
            .unwrap();
    }
    async fn leaser(store: Arc<PgStore>) -> Vec<Uuid> {
        let mut got = Vec::new();
        loop {
            let batch = store
                .lease_events(5, Duration::from_secs(300))
                .await
                .unwrap()
                .events;
            assert!(
                batch.len() <= 5,
                "leased {} rows for a limit of 5",
                batch.len()
            );
            if batch.is_empty() {
                return got;
            }
            got.extend(batch.into_iter().map(|e| e.id));
        }
    }
    let handles: Vec<_> = (0..4)
        .map(|_| tokio::spawn(leaser(store.clone())))
        .collect();
    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    let distinct: HashSet<_> = all.iter().copied().collect();
    assert_eq!(distinct.len(), all.len(), "an event was leased twice");
    assert_eq!(distinct.len(), 600);
}

/// The picking query runs exactly once per lease (a materialized CTE), so
/// no plan can make it pick twice.
#[tokio::test]
async fn outbox_lease_picks_once() {
    let Some(store) = store().await else { return };
    let plan: Json<serde_json::Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (FORMAT JSON) {LEASE_SQL}"
    )))
    .bind(5_i64)
    .bind(60.0_f64)
    .bind(Uuid::new_v4())
    .bind("lab")
    .fetch_one(store.pool())
    .await
    .unwrap();
    let text = plan.0.to_string();
    assert!(text.contains(r#""CTE Name":"picked""#), "{text}");
    assert_eq!(
        text.matches(r#""Node Type":"LockRows""#).count(),
        1,
        "{text}"
    );
}

#[tokio::test]
async fn outbox_prunes_old_sent_events_in_batches() {
    let Some(store) = store().await else { return };
    // 2 500 events sent a day ago: three delete batches.
    sqlx::query(
        "INSERT INTO lab_events_outbox (id, payload, created_at, sent_at)
         SELECT gen_random_uuid(), '{}', now() - interval '2 days', now() - interval '1 day'
         FROM generate_series(1, 2500)",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let recent = store
        .enqueue_event(None, &serde_json::json!({ "n": 1 }))
        .await
        .unwrap();
    let unsent = store
        .enqueue_event(None, &serde_json::json!({ "n": 2 }))
        .await
        .unwrap();
    let lease = store
        .lease_events(1, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(lease.events[0].id, recent);
    store
        .mark_events_sent(lease.token, &[recent])
        .await
        .unwrap();

    assert_eq!(
        store
            .prune_sent_events(Duration::from_secs(3600))
            .await
            .unwrap(),
        2500
    );
    assert_eq!(store.prune_sent_events(Duration::ZERO).await.unwrap(), 1);
    assert_eq!(store.prune_sent_events(Duration::ZERO).await.unwrap(), 0);
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM lab_events_outbox")
        .fetch_all(store.pool())
        .await
        .unwrap();
    assert_eq!(left, [unsent]);
}

#[tokio::test]
async fn dead_letters_are_capped_per_sync_and_bounded_in_size() {
    let Some(store) = store().await else { return };
    let store = store.with_dead_letter_limits(DeadLetterLimits {
        max_per_sync: 3,
        max_payload_bytes: 64,
    });
    store
        .create_sync(None, &spec("a", None, "a"))
        .await
        .unwrap();
    let mut big = letter("a", "big");
    big.payload = serde_json::json!({"doc": "x".repeat(1000)});
    store.push_dead_letters(&[big]).await.unwrap();
    let batch: Vec<_> = (0..5).map(|i| letter("a", &i.to_string())).collect();
    store.push_dead_letters(&batch).await.unwrap();
    let got = store
        .list_dead_letters(None, "a", DeadLetterPage::default())
        .await
        .unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].letter.payload["truncated"], true);
    let today = Utc::now().date_naive();
    let p = store.progress(None, "a", today, today).await.unwrap();
    assert_eq!(p[0].dead_letters_dropped, 3);
}

#[tokio::test]
async fn deferred_events_wait_for_their_backoff() {
    let Some(store) = store().await else { return };
    let id = store
        .enqueue_event(Some("t"), &serde_json::json!({}))
        .await
        .unwrap();
    assert_eq!(store.count_unsent().await.unwrap(), 1);
    let lease = store
        .lease_events(10, Duration::from_millis(10))
        .await
        .unwrap();
    let backoff = RetryBackoff {
        base: Duration::from_secs(60),
        max: Duration::from_secs(3600),
    };
    assert_eq!(
        store
            .defer_events(lease.token, &[id], backoff)
            .await
            .unwrap(),
        1
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    let again = store
        .lease_events(10, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(again.events.is_empty());
    assert_eq!(store.count_unsent().await.unwrap(), 1);
}

#[tokio::test]
async fn invalid_specs_are_refused() {
    let Some(store) = store().await else { return };
    for bad in invalid_specs("s", Some("t")) {
        assert!(matches!(
            store.create_sync(Some("t"), &bad).await,
            Err(RepoError::Invalid(_))
        ));
    }
    store
        .create_sync(Some("t"), &spec("s", Some("t"), "n"))
        .await
        .unwrap();
    for bad in invalid_specs("s", Some("t")) {
        assert!(matches!(
            store.update_sync(Some("t"), &bad).await,
            Err(RepoError::Invalid(_))
        ));
    }
    assert_eq!(
        store
            .get_sync(Some("t"), "s")
            .await
            .unwrap()
            .unwrap()
            .readable()
            .unwrap()
            .spec,
        spec("s", Some("t"), "n")
    );
}

/// What `MemStore::new` mirrors: state of an unknown sync is not found.
#[tokio::test]
async fn unknown_syncs_are_not_found() {
    let Some(store) = store().await else { return };
    assert!(matches!(
        store.record_progress("ghost", &pr(1, 0, 0)).await,
        Err(StoreError::NotFound(_))
    ));
}

/// Skipped source records are dead letters with a JSON null payload and a
/// source position as the id.
#[tokio::test]
async fn skipped_records_round_trip_as_dead_letters() {
    let Some(store) = store().await else { return };
    store
        .create_sync(None, &spec("s", None, "n"))
        .await
        .unwrap();
    let letters = vec![
        skipped_letter("s", "orders/3@1842"),
        letter("s", "1"),
        skipped_letter("s", "public.orders@0/16B3748"),
    ];
    store.push_dead_letters(&letters).await.unwrap();
    let got = store
        .list_dead_letters(None, "s", DeadLetterPage::default())
        .await
        .unwrap();
    assert_eq!(
        got.into_iter().map(|d| d.letter).collect::<Vec<_>>(),
        letters
    );
}
