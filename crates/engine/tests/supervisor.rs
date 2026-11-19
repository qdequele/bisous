//! `Supervisor` against the fakes, with short intervals.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::*;
use meili_connect_core::{SinkError, SourceError, SyncPhase};
use meili_connect_engine::{
    EngineError, Supervisor, SupervisorConfig, SupervisorError, SyncStatus,
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

fn supervisor(h: &Harness) -> Supervisor {
    let config = SupervisorConfig {
        sync: options(),
        backoff_initial: Duration::from_millis(5),
        backoff_max: Duration::from_millis(40),
        healthy_after: Duration::from_secs(30),
        paused_retry: Duration::from_millis(30),
        ..SupervisorConfig::default()
    };
    Supervisor::new(h.deps(), config, CancellationToken::new())
}

async fn status(s: &Supervisor) -> SyncStatus {
    match s.status(SYNC_ID).await {
        Ok(Some(status)) => status,
        other => panic!("no status: {other:?}"),
    }
}

async fn wait_status(s: &Supervisor, what: &str, cond: impl Fn(&SyncStatus) -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let st = status(s).await;
        if cond(&st) {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("timed out waiting for {what}: {st:?}");
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn streaming(st: &SyncStatus) -> bool {
    st.running && st.phase == SyncPhase::Streaming
}

#[tokio::test]
async fn transient_errors_are_retried_with_backoff() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    {
        let mut st = h.origin.state();
        for _ in 0..3 {
            st.open_faults
                .push_back(SourceError::Transient("connection reset".into()));
        }
    }
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    let st = status(&s).await;
    assert_eq!(st.consecutive_failures, 3);
    assert!(
        st.last_error
            .as_deref()
            .is_some_and(|e| e.contains("connection reset"))
    );
    assert!(st.message.is_none(), "cleared once healthy again");
    assert_eq!(h.origin.state().opens.len(), 4);
    s.shutdown().await;
    assert!(!status(&s).await.running);
    // Shutdown leaves the phase alone so the sync resumes on next start.
    assert_eq!(h.store.phase(SYNC_ID), Some(SyncPhase::Streaming));
}

#[tokio::test]
async fn a_sink_transient_error_is_retried() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    h.sink
        .state()
        .apply_faults
        .push_back(SinkError::Transient("429".into()));
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    assert_eq!(h.sink.docs(INDEX).len(), 1);
    assert_eq!(status(&s).await.consecutive_failures, 1);
    s.shutdown().await;
}

#[tokio::test]
async fn a_paused_source_is_retried_slowly() {
    let h = Harness::new();
    h.origin
        .state()
        .open_faults
        .push_back(SourceError::Paused("project paused".into()));
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    let history = h.store.inner().history.clone();
    let paused = history
        .iter()
        .find(|s| s.phase == SyncPhase::SourcePaused)
        .expect("SourcePaused persisted");
    assert!(
        paused
            .message
            .as_deref()
            .is_some_and(|m| m.contains("project paused"))
    );
    s.shutdown().await;
}

#[tokio::test]
async fn a_config_error_holds_the_sync() {
    let h = Harness::new();
    h.origin
        .state()
        .open_faults
        .push_back(SourceError::Config("password authentication failed".into()));
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "held", |st| !st.running && st.phase == SyncPhase::Held).await;
    assert!(
        status(&s)
            .await
            .message
            .is_some_and(|m| m.contains("password authentication failed"))
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.origin.state().opens.len(), 1, "no retry");
}

#[tokio::test]
async fn a_sink_config_error_holds_the_sync() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    h.sink
        .state()
        .apply_faults
        .push_back(SinkError::Config("invalid API key".into()));
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "held", |st| !st.running && st.phase == SyncPhase::Held).await;
}

#[tokio::test]
async fn a_fatal_error_fails_the_sync() {
    let h = Harness::new();
    h.origin
        .state()
        .open_faults
        .push_back(SourceError::Fatal("unsupported column type".into()));
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "failed", |st| {
        !st.running && st.phase == SyncPhase::Failed
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.origin.state().opens.len(), 1, "no retry");

    // A failed sync can be started again by the user.
    s.start(spec()).expect("start again");
    wait_status(&s, "streaming", streaming).await;
    s.shutdown().await;
}

#[tokio::test]
async fn position_lost_is_resynced() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    s.stop(SYNC_ID).await.expect("stop");
    h.origin.state().min_resume = 1_000;
    s.restart(SYNC_ID).await.expect("restart");
    wait_for("second snapshot", 5, || h.origin.state().snapshots == 2).await;
    wait_status(&s, "streaming", |st| streaming(st) && !st.resyncing).await;
    assert!(h.sink.calls().iter().any(|c| c.starts_with("swap")));
    s.shutdown().await;
}

#[tokio::test]
async fn stop_pauses_and_restart_resumes() {
    let h = Harness::new();
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    assert!(matches!(
        s.start(spec()),
        Err(SupervisorError::AlreadyRunning(_))
    ));

    s.stop(SYNC_ID).await.expect("stop");
    let st = status(&s).await;
    assert!(!st.running);
    assert_eq!(st.phase, SyncPhase::Paused);

    h.origin.tx(vec![upsert(3, json!({}))]);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(h.sink.docs(INDEX).is_empty(), "stopped syncs do not apply");

    s.restart(SYNC_ID).await.expect("restart");
    wait_status(&s, "streaming", streaming).await;
    wait_for("doc 3", 5, || h.sink.docs(INDEX).contains_key("3")).await;
    assert_eq!(h.origin.state().snapshots, 1, "resumed, not re-snapshotted");
    s.shutdown().await;
}

#[tokio::test]
async fn resync_command_rebuilds_through_a_shadow() {
    let h = Harness::new();
    h.origin
        .seed(vec![upsert(1, json!({})), upsert(2, json!({}))]);
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    wait_for("2 docs", 5, || h.sink.docs(INDEX).len() == 2).await;

    s.resync(SYNC_ID).await.expect("resync");
    wait_for("second snapshot", 5, || h.origin.state().snapshots == 2).await;
    wait_status(&s, "streaming", |st| streaming(st) && !st.resyncing).await;
    let calls = h.sink.calls();
    assert!(
        calls
            .iter()
            .any(|c| c == "swap products products__connect_01234567"),
        "{calls:?}"
    );
    s.shutdown().await;
}

#[tokio::test]
async fn release_lets_a_held_truncate_through() {
    let h = Harness::new();
    h.origin
        .seed(vec![upsert(1, json!({})), upsert(2, json!({}))]);
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    wait_for("2 docs", 5, || h.sink.docs(INDEX).len() == 2).await;

    h.origin.truncate(COLLECTION);
    wait_status(&s, "held", |st| !st.running && st.phase == SyncPhase::Held).await;
    assert_eq!(h.sink.docs(INDEX).len(), 2);

    s.release(SYNC_ID).await.expect("release");
    wait_for("cleared", 5, || h.sink.docs(INDEX).is_empty()).await;
    wait_status(&s, "streaming", |st| streaming(st) && st.message.is_none()).await;
    s.shutdown().await;
}

#[tokio::test]
async fn unknown_syncs_and_removal() {
    let h = Harness::new();
    let s = supervisor(&h);
    assert!(matches!(
        s.stop("nope").await,
        Err(SupervisorError::NotFound(_))
    ));
    assert!(s.status("nope").await.expect("status").is_none());

    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    assert_eq!(s.sync_ids(), vec![SYNC_ID.to_owned()]);
    s.remove(SYNC_ID).await.expect("remove");
    assert!(s.sync_ids().is_empty());
    assert!(!h.origin.state().stream_active);
}

#[tokio::test]
async fn shutdown_refuses_new_syncs() {
    let h = Harness::new();
    let s = supervisor(&h);
    s.shutdown().await;
    assert!(matches!(
        s.start(spec()),
        Err(SupervisorError::ShuttingDown)
    ));
}

#[tokio::test]
async fn start_validates_the_spec() {
    let h = Harness::new();
    let s = supervisor(&h);
    let mut bad = spec();
    bad.streams[0].index = "not a uid".into();
    assert!(matches!(
        s.start(bad),
        Err(SupervisorError::Engine(EngineError::Spec(_)))
    ));
    assert!(s.sync_ids().is_empty());
}

#[tokio::test]
async fn endless_position_loss_holds_the_sync() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    {
        let mut st = h.origin.state();
        for _ in 0..50 {
            st.stream_faults
                .push_back((0, SourceError::PositionLost("slot invalidated".into())));
        }
    }
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "held", |st| !st.running && st.phase == SyncPhase::Held).await;
    let st = status(&s).await;
    assert!(
        st.message
            .as_deref()
            .is_some_and(|m| m.contains("retention")),
        "{st:?}"
    );
    // The first snapshot, then three resyncs.
    assert_eq!(h.origin.state().snapshots, 4);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.origin.state().snapshots, 4, "no further resync");
}

fn sink_faults(h: &Harness, n: usize) {
    let mut st = h.sink.state();
    for _ in 0..n {
        st.apply_faults
            .push_back(SinkError::Transient("embedder unavailable".into()));
    }
}

#[tokio::test]
async fn a_batch_that_keeps_failing_transiently_holds_the_sync() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    sink_faults(&h, 50);
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "held", |st| !st.running && st.phase == SyncPhase::Held).await;
    let st = status(&s).await;
    assert!(
        st.message
            .as_deref()
            .is_some_and(|m| m.contains("embedder unavailable")),
        "{st:?}"
    );
    assert_eq!(st.consecutive_failures, 10);
    assert_eq!(h.sink.state().apply_faults.len(), 40);
}

#[tokio::test]
async fn progress_resets_the_transient_sink_count() {
    let h = Harness::new();
    h.origin.seed(vec![upsert(1, json!({}))]);
    sink_faults(&h, 9);
    let s = supervisor(&h);
    s.start(spec()).expect("start");
    wait_status(&s, "streaming", streaming).await;
    wait_for("doc 1", 5, || h.sink.docs(INDEX).len() == 1).await;

    sink_faults(&h, 9);
    h.origin.tx(vec![upsert(2, json!({}))]);
    wait_for("doc 2", 5, || h.sink.docs(INDEX).len() == 2).await;
    wait_status(&s, "streaming", streaming).await;
    s.shutdown().await;
}
