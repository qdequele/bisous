//! Live Postgres tests. They skip (pass with a notice) when `DATABASE_URL`
//! is unset. Each test gets its own schema, so they run in parallel.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::sync::Arc;

use sqlx::Connection;

use super::*;
use crate::test_support::{letter, spec, state};

async fn store_with_key(key: SecretKey) -> Option<PgStore> {
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
        .options([("search_path", schema.as_str())]);
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
    assert_eq!(store.load("s1").await.unwrap(), Some(st));
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
    };
    assert!(matches!(
        other.get_sync(Some("t"), "s1").await,
        Err(RepoError::Fatal(_))
    ));

    // A blob moved onto another tenant's row is refused, and boot reports
    // the bad row without failing the others.
    sqlx::query("UPDATE syncs SET spec_sealed = $1 WHERE id = 's2'")
        .bind(&raw)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(matches!(
        store.get_sync(Some("u"), "s2").await,
        Err(RepoError::Fatal(_))
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
            .spec,
        a
    );
    assert!(store.get_sync(Some("tb"), "a1").await.unwrap().is_none());
    assert!(store.get_sync(None, "a1").await.unwrap().is_none());
    assert!(store.get_sync(Some("ta"), "g1").await.unwrap().is_none());

    // list
    let ids = |v: Vec<StoredSync>| v.into_iter().map(|s| s.spec.id).collect::<Vec<_>>();
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
    store.record_progress("a1", 1, 0, 1).await.unwrap();
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
    store.record_progress("s", 1, 1, 1).await.unwrap();
    store.record_progress("o", 1, 1, 1).await.unwrap();

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
            store.record_progress("s", 3, 2, 1).await.unwrap();
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
                rejected: 0
            },
            DailyProgress {
                day: today,
                written: 60,
                deleted: 40,
                rejected: 20
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
    async fn leaser(store: Arc<PgStore>) -> Vec<Uuid> {
        let mut got = Vec::new();
        loop {
            let batch = store
                .lease_events(7, Duration::from_secs(300))
                .await
                .unwrap();
            if batch.is_empty() {
                return got;
            }
            got.extend(batch.into_iter().map(|e| e.id));
        }
    }
    let (a, b) = tokio::join!(
        tokio::spawn(leaser(store.clone())),
        tokio::spawn(leaser(store.clone()))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
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
            .is_empty()
    );
    let ids: Vec<Uuid> = all.iter().copied().collect();
    assert_eq!(store.mark_events_sent(&ids).await.unwrap(), 200);
    assert_eq!(store.mark_events_sent(&ids).await.unwrap(), 0);
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
    let leased = store.lease_events(10, Duration::ZERO).await.unwrap();
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
    assert_eq!(again[0].id, first);
    store.mark_events_sent(&[first]).await.unwrap();
    let rest = store
        .lease_events(10, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(rest.iter().map(|e| e.id).collect::<Vec<_>>(), [second]);
    assert!(
        store
            .lease_events(0, Duration::ZERO)
            .await
            .unwrap()
            .is_empty()
    );
}
