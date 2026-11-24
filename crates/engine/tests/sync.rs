//! `run_sync` against the fakes.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::*;
use meili_connect_core::{
    ResyncReason, SinkError, SourceError, StoreError, SyncPhase, SyncSpec, SyncState, Usage,
};
use meili_connect_engine::{EngineError, SyncOptions, run_sync, shadow_index_uid};
use serde_json::json;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn spawn(
    h: &Harness,
    spec: SyncSpec,
    opts: SyncOptions,
) -> (CancellationToken, JoinHandle<Result<(), EngineError>>) {
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(run_sync(spec, h.deps(), opts, cancel.clone()));
    (cancel, handle)
}

async fn finish(handle: JoinHandle<Result<(), EngineError>>) -> Result<(), EngineError> {
    match tokio::time::timeout(Duration::from_secs(5), handle).await {
        Ok(Ok(result)) => result,
        Ok(Err(e)) => panic!("run_sync panicked: {e}"),
        Err(_) => panic!("run_sync did not stop"),
    }
}

async fn stop(cancel: CancellationToken, handle: JoinHandle<Result<(), EngineError>>) {
    cancel.cancel();
    let result = finish(handle).await;
    assert!(result.is_ok(), "{result:?}");
}

/// Waits until the destination matches the origin and the whole log is
/// checkpointed.
async fn wait_synced(h: &Harness) {
    wait_for("destination in sync", 5, || {
        h.sink.docs(INDEX) == h.origin.expected()
            && h.store.checkpoint(SYNC_ID) == Some(h.origin.log_len())
            && h.store.phase(SYNC_ID) == Some(SyncPhase::Streaming)
    })
    .await;
}

fn seed_rows(h: &Harness, ids: std::ops::RangeInclusive<i64>) {
    h.origin.seed(
        ids.map(|i| upsert(i, json!({"name": format!("p{i}"), "price": i})))
            .collect(),
    );
}

#[tokio::test]
async fn fresh_sync_is_gapless() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    // Written while the snapshot runs: absent from the snapshot, so they
    // must come from the stream that starts at the position captured first.
    h.origin.state().during_snapshot = vec![
        (patch(1, json!({"price": 9}), &[]), true),
        (upsert(4, json!({"name": "p4"})), true),
        (delete(2), true),
    ];

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;

    let docs = h.sink.docs(INDEX);
    assert_eq!(
        docs.keys().cloned().collect::<Vec<_>>(),
        vec!["1", "3", "4"]
    );
    assert_eq!(docs["1"]["price"], json!(9));
    let st = h.origin.state();
    assert_eq!(st.snapshots, 1);
    assert_eq!(st.opens, vec![None]);
    drop(st);
    // A first sync into a missing index writes directly.
    assert!(!h.sink.calls().iter().any(|c| c.starts_with("swap")));
    let state = h.store.state(SYNC_ID).expect("state saved");
    assert!(!state.resyncing);
    assert!(state.message.is_none());
    assert_eq!(h.store.inner().progress[SYNC_ID].1, 1);
    h.assert_acks_sound();
}

#[tokio::test]
async fn first_sync_into_an_empty_index_writes_directly() {
    let h = Harness::new();
    h.sink
        .create(INDEX, json!({"rankingRules": ["words"]}), vec![]);
    seed_rows(&h, 1..=2);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    let calls = h.sink.calls();
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("swap") || c.starts_with("copy"))
    );
}

#[tokio::test]
async fn resume_from_the_saved_checkpoint() {
    let h = Harness::new();
    seed_rows(&h, 1..=2);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    let saved = h.store.checkpoint(SYNC_ID);

    h.origin
        .tx(vec![upsert(5, json!({"name": "p5"})), delete(1)]);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;

    let st = h.origin.state();
    assert_eq!(st.snapshots, 1, "no second snapshot on resume");
    assert_eq!(st.opens.len(), 2);
    assert_eq!(st.opens[1].as_ref().map(pos), saved);
    drop(st);
    h.assert_acks_sound();
}

#[tokio::test]
async fn changes_after_the_last_checkpoint_are_applied_but_not_acknowledged() {
    let h = Harness::new();
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;

    h.origin
        .tx(vec![upsert(1, json!({})), upsert(2, json!({}))]); // log 0..3
    h.origin.uncommitted(vec![upsert(3, json!({}))]); // log 3
    wait_for("3 docs", 5, || h.sink.docs(INDEX).len() == 3).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(h.store.checkpoint(SYNC_ID), Some(3));
    assert!(h.origin.state().acks.iter().all(|a| pos(a) <= 3));

    h.origin.tx(vec![upsert(4, json!({}))]);
    wait_synced(&h).await;
    assert_eq!(h.store.checkpoint(SYNC_ID), Some(6));
    stop(cancel, handle).await;
    h.assert_acks_sound();
}

#[tokio::test]
async fn killed_between_apply_and_save_replays_idempotently() {
    let h = Harness::new();
    seed_rows(&h, 1..=2);
    let (_cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    let before = h.store.checkpoint(SYNC_ID);

    // The save after the next apply fails: the process "dies" there.
    h.store.inner().fail_once = Some(Box::new(|s| {
        s.checkpoint.as_ref().is_some_and(|c| pos(c) > 0)
    }));
    h.origin.tx(vec![
        patch(1, json!({"price": 100}), &[]),
        upsert(3, json!({})),
    ]);
    let result = finish(handle).await;
    assert!(
        matches!(result, Err(EngineError::Store(StoreError::Transient(_)))),
        "{result:?}"
    );
    // Applied, but not checkpointed and not acknowledged.
    assert_eq!(h.sink.docs(INDEX).len(), 3);
    assert_eq!(h.store.checkpoint(SYNC_ID), before);
    assert!(h.origin.state().acks.iter().all(|a| Some(pos(a)) <= before));
    assert!(!h.origin.state().stream_active);
    let applied_before = h.sink.state().applied_ops;

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert!(
        h.sink.state().applied_ops > applied_before,
        "the batch was replayed"
    );
    assert_eq!(h.sink.docs(INDEX)["1"]["price"], json!(100));
    h.assert_acks_sound();
}

#[tokio::test]
async fn a_sink_error_stops_the_source_and_keeps_the_checkpoint() {
    let h = Harness::new();
    let (_cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    wait_for("stream running", 5, || h.origin.state().stream_active).await;

    h.sink
        .state()
        .apply_faults
        .push_back(SinkError::Transient("503".into()));
    h.origin.tx(vec![upsert(1, json!({}))]);
    let result = finish(handle).await;
    assert!(
        matches!(result, Err(EngineError::Sink(SinkError::Transient(_)))),
        "{result:?}"
    );
    assert_eq!(h.store.checkpoint(SYNC_ID), Some(0));
    assert!(!h.origin.state().stream_active, "source task stopped");
    h.assert_acks_sound();
}

#[tokio::test]
async fn a_source_error_is_returned() {
    let h = Harness::new();
    h.origin
        .state()
        .stream_faults
        .push_back((0, SourceError::Fatal("unsupported type".into())));
    h.origin.tx(vec![upsert(1, json!({}))]);
    let (_cancel, handle) = spawn(&h, spec(), options());
    let result = finish(handle).await;
    assert!(
        matches!(result, Err(EngineError::Source(SourceError::Fatal(_)))),
        "{result:?}"
    );
}

#[tokio::test]
async fn rejected_documents_are_dead_lettered_without_failing() {
    let h = Harness::new();
    h.sink.state().reject.insert("2".into());
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;

    h.origin.tx(vec![
        upsert(1, json!({})),
        upsert(2, json!({"bad": true})),
        upsert(3, json!({})),
    ]);
    wait_for("checkpoint", 5, || {
        h.store.checkpoint(SYNC_ID) == Some(h.origin.log_len())
    })
    .await;
    h.origin.tx(vec![upsert(4, json!({}))]);
    wait_for("later changes", 5, || h.sink.docs(INDEX).len() == 3).await;
    assert!(!handle.is_finished());
    stop(cancel, handle).await;

    let st = h.store.inner();
    assert_eq!(st.dead.len(), 1);
    let letter = &st.dead[0];
    assert_eq!(letter.document_id, "2");
    assert_eq!(letter.index, INDEX);
    assert_eq!(letter.payload["kind"], json!("replace"));
    assert_eq!(letter.payload["doc"]["bad"], json!(true));
    assert_eq!(st.progress[SYNC_ID], (3, 0, 1));
}

fn resync_calls_ok(h: &Harness) {
    let shadow = shadow_index_uid(INDEX, SYNC_ID);
    assert_eq!(shadow, "products__connect_01234567");
    let calls = h.sink.calls();
    let idx = |c: &str| {
        calls
            .iter()
            .position(|x| x == c)
            .unwrap_or_else(|| panic!("missing call `{c}` in {calls:?}"))
    };
    let ensure = idx(&format!("ensure {shadow}"));
    let copy = idx(&format!("copy_settings {INDEX} {shadow}"));
    let swap = idx(&format!("swap {INDEX} {shadow}"));
    let delete = calls
        .iter()
        .rposition(|x| x == &format!("delete {shadow}"))
        .unwrap_or(0);
    assert!(ensure < swap && copy < swap && swap < delete, "{calls:?}");
    assert!(!h.sink.state().indexes.contains_key(&shadow));
    assert_eq!(
        h.sink.state().indexes[INDEX].settings,
        json!({"rankingRules": ["words"]})
    );
    assert!(h.store.inner().history.iter().any(|s| s.resyncing));
    assert!(!h.store.state(SYNC_ID).expect("state").resyncing);
}

#[tokio::test]
async fn position_lost_on_open_resyncs_through_a_shadow_index() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    h.sink
        .state()
        .indexes
        .get_mut(INDEX)
        .expect("live index")
        .settings = json!({"rankingRules": ["words"]});

    // Data changed while nobody followed the stream, and the position is gone.
    h.origin
        .seed(vec![delete(1), upsert(10, json!({"name": "p10"}))]);
    h.origin.state().min_resume = 1_000;

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("resynced", 5, || {
        h.sink.docs(INDEX) == h.origin.expected()
            && h.store.phase(SYNC_ID) == Some(SyncPhase::Streaming)
            && h.store.state(SYNC_ID).is_some_and(|s| !s.resyncing)
    })
    .await;
    stop(cancel, handle).await;

    let st = h.origin.state();
    assert_eq!(st.snapshots, 2);
    assert_eq!(st.opens.last(), Some(&None));
    drop(st);
    resync_calls_ok(&h);
    h.assert_acks_sound();
}

#[tokio::test]
async fn position_lost_while_streaming_resyncs() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.sink
        .create(INDEX, json!({"rankingRules": ["words"]}), vec![]);
    // The first stream loses its position at once; the live index is not
    // empty by then, so the resync goes through a shadow.
    h.origin
        .state()
        .stream_faults
        .push_back((0, SourceError::PositionLost("slot invalidated".into())));
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("resynced", 5, || {
        h.origin.state().snapshots == 2
            && h.sink.docs(INDEX) == h.origin.expected()
            && h.store.state(SYNC_ID).is_some_and(|s| !s.resyncing)
    })
    .await;
    stop(cancel, handle).await;
    resync_calls_ok(&h);
}

#[tokio::test]
async fn explicit_resync_request() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.sink
        .create(INDEX, json!({"rankingRules": ["words"]}), vec![]);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    // A stale document only a resync removes.
    h.sink
        .state()
        .indexes
        .get_mut(INDEX)
        .expect("live")
        .docs
        .insert("999".into(), doc(json!({"id": "999"})));

    let mut opts = options();
    opts.resync = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_for("resynced", 5, || {
        h.origin.state().snapshots == 2 && h.sink.docs(INDEX) == h.origin.expected()
    })
    .await;
    wait_synced(&h).await;
    stop(cancel, handle).await;
    resync_calls_ok(&h);
}

#[tokio::test]
async fn truncate_is_held_then_released() {
    let h = Harness::new();
    seed_rows(&h, 1..=4);
    h.sink
        .create(INDEX, json!({"rankingRules": ["words"]}), vec![]);
    let (_cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;

    h.origin.tx(vec![upsert(5, json!({}))]);
    wait_synced(&h).await;
    let before = h.store.checkpoint(SYNC_ID);
    h.origin.truncate(COLLECTION);
    h.origin.tx(vec![upsert(7, json!({"name": "p7"}))]);

    let result = finish(handle).await;
    assert!(matches!(result, Err(EngineError::Held(_))), "{result:?}");
    let state = h.store.state(SYNC_ID).expect("state");
    assert_eq!(state.phase, SyncPhase::Held);
    assert!(
        state
            .message
            .as_deref()
            .is_some_and(|m| m.contains("truncated")),
        "{state:?}"
    );
    assert_eq!(h.sink.docs(INDEX).len(), 5, "nothing applied");
    assert_eq!(h.store.checkpoint(SYNC_ID), before);

    // A held sync stays held on a plain restart.
    let result = finish(spawn(&h, spec(), options()).1).await;
    assert!(matches!(result, Err(EngineError::Held(_))), "{result:?}");
    assert_eq!(h.sink.docs(INDEX).len(), 5);

    let mut opts = options();
    opts.release_hold = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert_eq!(
        h.sink.docs(INDEX).keys().cloned().collect::<Vec<_>>(),
        vec!["7"]
    );
    assert_eq!(
        h.sink.state().indexes[INDEX].settings,
        json!({"rankingRules": ["words"]}),
        "settings survive the truncate"
    );
    // Cleared in place: no shadow index to leak on a crash.
    let calls = h.sink.calls();
    assert!(calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert!(
        !calls
            .iter()
            .any(|c| c.starts_with("swap") || c.contains("__connect_")),
        "{calls:?}"
    );
    assert!(h.store.state(SYNC_ID).expect("state").message.is_none());
    h.assert_acks_sound();
}

#[tokio::test]
async fn mass_delete_is_held_then_released() {
    let h = Harness::new();
    seed_rows(&h, 1..=10);
    let (_cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;

    // 3 of 10: under the 50% guard.
    h.origin.tx(vec![delete(1), delete(2), delete(3)]);
    wait_synced(&h).await;
    let before = h.store.checkpoint(SYNC_ID);

    // 6 of 7: held.
    h.origin.tx((4..=9).map(delete).collect());
    let result = finish(handle).await;
    assert!(matches!(result, Err(EngineError::Held(_))), "{result:?}");
    assert_eq!(h.store.phase(SYNC_ID), Some(SyncPhase::Held));
    assert_eq!(h.sink.docs(INDEX).len(), 7);
    assert_eq!(
        h.store.checkpoint(SYNC_ID),
        before,
        "held batch not checkpointed"
    );

    let mut opts = options();
    opts.release_hold = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert_eq!(h.sink.docs(INDEX).len(), 1);
    h.assert_acks_sound();
}

#[tokio::test]
async fn no_guard_applies_mass_deletes() {
    let h = Harness::new();
    seed_rows(&h, 1..=4);
    let mut spec = spec();
    spec.mass_delete_guard = None;
    let (cancel, handle) = spawn(&h, spec, options());
    wait_synced(&h).await;
    h.origin.tx((1..=4).map(delete).collect());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert!(h.sink.docs(INDEX).is_empty());
}

#[tokio::test]
async fn cancellation_mid_snapshot_restarts_the_snapshot() {
    let h = Harness::new();
    seed_rows(&h, 1..=200);
    h.origin.state().snapshot_delay = Duration::from_millis(2);
    let mut opts = options();
    opts.max_docs = 10;
    let (cancel, handle) = spawn(&h, spec(), opts.clone());
    wait_for("some rows applied", 5, || !h.sink.docs(INDEX).is_empty()).await;
    let started = tokio::time::Instant::now();
    stop(cancel, handle).await;
    assert!(started.elapsed() < Duration::from_secs(1));

    let state = h.store.state(SYNC_ID).expect("state");
    assert_eq!(state.phase, SyncPhase::Snapshotting);
    assert!(
        state.checkpoint.is_none(),
        "an interrupted snapshot is not checkpointed"
    );
    assert!(h.sink.docs(INDEX).len() < 200);
    assert!(h.origin.state().acks.is_empty());

    h.origin.state().snapshot_delay = Duration::ZERO;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert_eq!(h.origin.state().snapshots, 2);
}

#[tokio::test]
async fn cancellation_mid_stream_is_prompt() {
    let h = Harness::new();
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    wait_for("stream running", 5, || h.origin.state().stream_active).await;
    let started = tokio::time::Instant::now();
    stop(cancel, handle).await;
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(!h.origin.state().stream_active);
}

#[tokio::test]
async fn cancellation_while_the_sink_is_slow() {
    let h = Harness::new();
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    h.sink.state().apply_delay = Duration::from_secs(30);
    h.origin.tx(vec![upsert(1, json!({}))]);
    wait_for("apply in flight", 5, || {
        !h.sink.state().in_flight.is_empty()
    })
    .await;
    let started = tokio::time::Instant::now();
    stop(cancel, handle).await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(h.store.checkpoint(SYNC_ID), Some(0));
}

#[tokio::test]
async fn a_slow_sink_back_pressures_the_source() {
    let h = Harness::new();
    let mut opts = options();
    opts.channel_capacity = 4;
    opts.max_docs = 10;
    opts.max_delay = Duration::from_secs(5);
    h.sink.state().apply_delay = Duration::from_millis(2);
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_synced(&h).await;

    h.origin
        .tx((1..=500).map(|i| upsert(i, json!({"n": i}))).collect());
    wait_for("500 docs", 20, || h.sink.docs(INDEX).len() == 500).await;
    stop(cancel, handle).await;

    let st = h.sink.state();
    let max_in_flight = st.in_flight.iter().copied().max().unwrap_or(0);
    // At most one batch being applied, a full channel, and one event in
    // the source's hands (plus the commit marker).
    assert!(max_in_flight <= 10 + 4 + 2, "in flight: {max_in_flight}");
    assert!(
        st.batch_sizes.iter().all(|&n| n <= 10),
        "{:?}",
        st.batch_sizes
    );
}

#[tokio::test]
async fn unmapped_collections_and_patches() {
    let h = Harness::new();
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    let mut other = upsert(1, json!({"x": 1}));
    other.collection = "public.other".into();
    h.origin.tx(vec![
        other,
        upsert(2, json!({"a": 1, "b": 2})),
        patch(2, json!({"a": 5}), &["b"]),
    ]);
    wait_for("checkpoint", 5, || {
        h.store.checkpoint(SYNC_ID) == Some(h.origin.log_len())
    })
    .await;
    stop(cancel, handle).await;
    let docs = h.sink.docs(INDEX);
    assert_eq!(docs.len(), 1);
    // Upsert + patch coalesced into one replace: `b` is null, as when the
    // two land in separate batches.
    assert_eq!(docs["2"], doc(json!({"id": "2", "a": 5, "b": null})));
    assert_eq!(h.sink.state().batch_sizes.last(), Some(&1));
}

#[tokio::test]
async fn a_spec_without_streams_is_rejected() {
    let h = Harness::new();
    let mut spec = spec();
    spec.streams.clear();
    let result = finish(spawn(&h, spec, options()).1).await;
    assert!(matches!(result, Err(EngineError::Spec(_))), "{result:?}");
}

#[tokio::test]
async fn a_spec_sharing_an_index_is_rejected() {
    let h = Harness::new();
    let mut spec = spec();
    let mut other = spec.streams[0].clone();
    other.collection = "public.other".into();
    spec.streams.push(other);
    let result = finish(spawn(&h, spec, options()).1).await;
    assert!(matches!(result, Err(EngineError::Spec(_))), "{result:?}");
    assert!(h.origin.state().opens.is_empty(), "nothing ran");
}

#[tokio::test]
async fn skipped_records_are_dead_lettered_and_do_not_block_checkpoints() {
    let h = Harness::new();
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;

    h.origin
        .skipped(COLLECTION, "products/0@17", "undecodable payload");
    h.origin
        .skipped("public.unmapped", "unmapped/1@3", "no key");
    h.origin.tx(vec![upsert(1, json!({}))]);
    wait_synced(&h).await;
    assert!(!handle.is_finished());
    stop(cancel, handle).await;

    let st = h.store.inner();
    assert_eq!(st.dead.len(), 2, "{:?}", st.dead);
    assert_eq!(st.dead[0].index, INDEX);
    assert_eq!(st.dead[0].document_id, "products/0@17");
    assert_eq!(st.dead[0].payload, json!(null));
    assert_eq!(st.dead[0].reason, "undecodable payload");
    assert_eq!(st.dead[1].index, "public.unmapped");
    assert_eq!(st.progress[SYNC_ID], (1, 0, 2));
    drop(st);
    h.assert_acks_sound();
}

#[tokio::test]
async fn a_source_may_drop_its_ack_receiver() {
    let h = Harness::new();
    h.origin.state().drop_acks = true;
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    h.origin.tx(vec![upsert(1, json!({}))]);
    wait_synced(&h).await;
    h.origin.tx(vec![upsert(2, json!({}))]);
    wait_synced(&h).await;
    assert!(!handle.is_finished());
    stop(cancel, handle).await;
}

#[tokio::test]
async fn an_index_keyed_differently_is_refused_before_any_shadow() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.sink.create(
        INDEX,
        json!({}),
        vec![("x".into(), doc(json!({"sku": "x"})))],
    );
    h.sink
        .state()
        .indexes
        .get_mut(INDEX)
        .expect("live")
        .primary_key = "sku".into();

    let result = finish(spawn(&h, spec(), options()).1).await;
    assert!(
        matches!(result, Err(EngineError::Sink(SinkError::Config(_)))),
        "{result:?}"
    );
    let calls = h.sink.calls();
    assert!(
        !calls.iter().any(|c| c.contains("__connect_")),
        "no shadow built: {calls:?}"
    );
    assert_eq!(h.sink.state().indexes[INDEX].primary_key, "sku");
    assert_eq!(h.sink.docs(INDEX).len(), 1, "live index untouched");
    assert_eq!(h.origin.state().snapshots, 0);
}

// ---------------------------------------------------------------- metering

fn metered(h: &Harness) -> Usage {
    h.store
        .inner()
        .usage
        .get(SYNC_ID)
        .copied()
        .unwrap_or_default()
}

#[tokio::test]
async fn accepted_documents_are_metered_by_kind_with_their_bytes() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    // Rejected documents are never metered.
    h.sink.state().reject.insert("2".into());
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("snapshot applied", 5, || {
        h.store.phase(SYNC_ID) == Some(SyncPhase::Streaming)
    })
    .await;
    let u = metered(&h);
    assert_eq!(
        (
            u.documents_snapshot,
            u.documents_streamed,
            u.documents_resync
        ),
        (2, 0, 0)
    );
    assert_eq!(u.bytes, 2 * BYTES_PER_DOC);

    h.origin
        .tx(vec![upsert(4, json!({"name": "p4"})), delete(1)]);
    wait_for("streamed", 5, || metered(&h).documents_streamed == 2).await;
    stop(cancel, handle).await;
    let u = metered(&h);
    assert_eq!(u.documents_snapshot, 2, "{u:?}");
    assert_eq!(u.documents_resync, 0);
    assert_eq!(u.bytes, 4 * BYTES_PER_DOC);
    assert_eq!(
        h.store.inner().progress[SYNC_ID].2,
        1,
        "the rejection is counted apart"
    );
}

#[tokio::test]
async fn a_user_resync_is_a_snapshot_and_a_lost_position_a_free_resync() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    assert_eq!(metered(&h).documents_snapshot, 3);

    // The user asks.
    let mut opts = options();
    opts.resync = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_for("user resync", 5, || h.origin.state().snapshots == 2).await;
    wait_synced(&h).await;
    stop(cancel, handle).await;
    let u = metered(&h);
    assert_eq!((u.documents_snapshot, u.documents_resync), (6, 0));

    // The origin lost the position: the engine resyncs on its own.
    h.origin.state().min_resume = 1_000;
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("system resync", 5, || h.origin.state().snapshots == 3).await;
    wait_for("resynced", 5, || {
        h.store
            .state(SYNC_ID)
            .is_some_and(|s| !s.resyncing && s.phase == SyncPhase::Streaming)
    })
    .await;
    stop(cancel, handle).await;
    let u = metered(&h);
    assert_eq!((u.documents_snapshot, u.documents_resync), (6, 3), "{u:?}");
    let state = h.store.state(SYNC_ID).expect("state");
    assert_eq!(state.resync_reason, None, "cleared once done");
}

fn interrupted_resync(reason: ResyncReason) -> SyncState {
    SyncState {
        sync_id: SYNC_ID.into(),
        phase: SyncPhase::Snapshotting,
        checkpoint: None,
        message: None,
        resyncing: true,
        resync_reason: Some(reason),
        filling: Vec::new(),
        updated_at: chrono::Utc::now(),
    }
}

#[tokio::test]
async fn a_resync_interrupted_by_a_crash_keeps_its_reason() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    // What a crash in the middle of a system resync leaves behind.
    h.store
        .inner()
        .states
        .insert(SYNC_ID.into(), interrupted_resync(ResyncReason::System));
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    let u = metered(&h);
    assert_eq!((u.documents_snapshot, u.documents_resync), (0, 3), "{u:?}");

    // A user resync interrupted the same way stays billed.
    h.store
        .inner()
        .states
        .insert(SYNC_ID.into(), interrupted_resync(ResyncReason::User));
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("resynced", 5, || metered(&h).documents_snapshot == 3).await;
    stop(cancel, handle).await;
    assert_eq!(metered(&h).documents_resync, 3);
}

#[test]
fn a_state_saved_before_the_reason_existed_still_reads() {
    let old = json!({
        "sync_id": "s", "phase": "snapshotting", "checkpoint": null, "message": null,
        "resyncing": true, "updated_at": "2026-11-19T10:00:00Z"
    });
    let state: SyncState = serde_json::from_value(old).expect("old state");
    assert_eq!(state.resync_reason, None);
    assert!(state.filling.is_empty());
}

// ------------------------------------------- a first copy that is interrupted

/// What an interruption (a crash, a deploy, a hold) in the middle of a first
/// copy leaves in the store: no checkpoint, and the indexes the copy was
/// filling from empty.
fn interrupted_first_copy(phase: SyncPhase, message: Option<&str>) -> SyncState {
    SyncState {
        sync_id: SYNC_ID.into(),
        phase,
        checkpoint: None,
        message: message.map(str::to_owned),
        resyncing: false,
        resync_reason: None,
        filling: vec![INDEX.into()],
        updated_at: chrono::Utc::now(),
    }
}

/// The settings of an index whose embedder holds an API key: Meilisearch
/// only gives the key back redacted, so they cannot be copied to a shadow.
fn embedder_settings() -> serde_json::Value {
    json!({"embedders": {"default": {"source": "openAi", "apiKey": "sk-XXXX..."}}})
}

fn shadow_calls(h: &Harness) -> Vec<String> {
    let shadow = shadow_index_uid(INDEX, SYNC_ID);
    h.sink
        .calls()
        .into_iter()
        .filter(|c| c.contains(&shadow) || c.starts_with("copy_settings") || c.starts_with("swap"))
        .collect()
}

#[tokio::test]
async fn a_first_copy_records_the_indexes_it_fills_from_empty_before_writing() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.origin.state().snapshot_delay = Duration::from_millis(400);

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("the copy says which index it fills", 5, || {
        h.store
            .state(SYNC_ID)
            .is_some_and(|s| s.filling == vec![INDEX.to_owned()])
    })
    .await;
    // Recorded before the first document: an interruption right after the
    // first batch must already find it.
    assert!(h.sink.docs(INDEX).is_empty(), "nothing written yet");
    assert_eq!(h.store.phase(SYNC_ID), Some(SyncPhase::Snapshotting));

    wait_synced(&h).await;
    stop(cancel, handle).await;
    let state = h.store.state(SYNC_ID).expect("state");
    assert!(
        state.filling.is_empty(),
        "cleared once the copy is complete"
    );
    assert!(shadow_calls(&h).is_empty(), "{:?}", h.sink.calls());
}

/// Batches of the interrupted run may still be queued on Meilisearch when
/// the next one starts, the index still counting zero documents: it is
/// emptied all the same, and the delete runs after them.
#[tokio::test]
async fn a_recorded_index_is_emptied_even_when_it_counts_no_document_yet() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.sink.create(INDEX, embedder_settings(), vec![]);
    h.store.inner().states.insert(
        SYNC_ID.into(),
        interrupted_first_copy(SyncPhase::Snapshotting, None),
    );

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;

    let calls = h.sink.calls();
    assert!(calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert!(shadow_calls(&h).is_empty(), "{calls:?}");
}

#[tokio::test]
async fn an_interrupted_first_copy_is_refilled_in_place() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    // The partial copy: row 1, and row 99 that the origin deleted since.
    h.sink.create(
        INDEX,
        embedder_settings(),
        vec![
            ("1".into(), doc(json!({"id": "1", "name": "old"}))),
            ("99".into(), doc(json!({"id": "99", "name": "gone"}))),
        ],
    );
    h.store.inner().states.insert(
        SYNC_ID.into(),
        interrupted_first_copy(SyncPhase::Snapshotting, None),
    );

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;

    // Emptied, then written to directly: no shadow index, so no settings to
    // copy, and the document of the deleted row is gone.
    assert_eq!(h.sink.docs(INDEX), h.origin.expected());
    let calls = h.sink.calls();
    assert!(calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert!(shadow_calls(&h).is_empty(), "{calls:?}");
    assert_eq!(h.sink.state().indexes[INDEX].settings, embedder_settings());

    let state = h.store.state(SYNC_ID).expect("state");
    assert!(state.filling.is_empty());
    assert!(
        h.store.inner().history.iter().all(|s| !s.resyncing),
        "a first copy, not a resync"
    );
    assert_eq!(metered(&h).documents_snapshot, 3);
    h.assert_acks_sound();
}

#[tokio::test]
async fn a_held_first_copy_resumes_in_place_even_when_a_resync_is_asked() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    h.sink.create(
        INDEX,
        embedder_settings(),
        vec![("99".into(), doc(json!({"id": "99", "name": "gone"})))],
    );
    h.store.inner().states.insert(
        SYNC_ID.into(),
        interrupted_first_copy(SyncPhase::Held, Some("Meilisearch configuration error: x")),
    );

    // Resuming a held sync, or pressing Resync on it, starts a run with the
    // stored state: both must take the same path.
    let mut opts = options();
    opts.resync = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_synced(&h).await;
    stop(cancel, handle).await;

    assert_eq!(h.sink.docs(INDEX), h.origin.expected());
    assert!(shadow_calls(&h).is_empty(), "{:?}", h.sink.calls());
    let state = h.store.state(SYNC_ID).expect("state");
    assert!(
        state.filling.is_empty() && !state.resyncing && state.message.is_none(),
        "{state:?}"
    );
}

/// The limit that stays: an index with a keyed embedder that held documents
/// before the sync cannot be rebuilt aside, and is not this sync's to empty.
/// Held, nothing touched (`embedder_settings` makes the fake sink refuse the
/// copy like Meilisearch's redacted keys make the real one).
#[tokio::test]
async fn a_populated_index_with_a_keyed_embedder_that_is_not_ours_is_held_untouched() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    let theirs = vec![("99".to_owned(), doc(json!({"id": "99", "name": "theirs"})))];
    h.sink.create(INDEX, embedder_settings(), theirs.clone());

    let result = finish(spawn(&h, spec(), options()).1).await;
    assert!(
        matches!(&result, Err(EngineError::Sink(SinkError::Config(m))) if m.contains("API key")),
        "{result:?}"
    );
    assert_eq!(h.sink.docs(INDEX), theirs.into_iter().collect());
    let calls = h.sink.calls();
    assert!(!calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert!(
        h.store.inner().history.iter().all(|s| s.filling.is_empty()),
        "never recorded as this sync's"
    );
}

/// A state that still lists an index although the sync has a checkpoint
/// (written by an older binary after a rollback, say) must not empty a
/// complete index: resuming the stream drops the stale entry.
#[tokio::test]
async fn resuming_a_stream_drops_a_stale_entry_without_emptying_anything() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    h.store
        .inner()
        .states
        .get_mut(SYNC_ID)
        .expect("state")
        .filling = vec![INDEX.into()];

    h.origin.tx(vec![upsert(4, json!({"name": "p4"}))]);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_for("the entry is dropped", 5, || {
        h.store.state(SYNC_ID).is_some_and(|s| s.filling.is_empty())
    })
    .await;
    wait_synced(&h).await;
    stop(cancel, handle).await;

    assert_eq!(h.sink.docs(INDEX).len(), 4);
    let calls = h.sink.calls();
    assert!(!calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert_eq!(h.origin.state().snapshots, 1, "no second snapshot");
}

/// The whole life of a first copy: once it is complete the index is no
/// longer this sync's to empty, and a resync builds it aside again.
#[tokio::test]
async fn a_completed_first_copy_is_resynced_through_a_shadow_again() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;
    h.sink
        .state()
        .indexes
        .get_mut(INDEX)
        .expect("live index")
        .settings = json!({"rankingRules": ["words"]});

    let mut opts = options();
    opts.resync = true;
    let (cancel, handle) = spawn(&h, spec(), opts);
    wait_for("resynced", 5, || {
        h.origin.state().snapshots == 2
            && h.store
                .state(SYNC_ID)
                .is_some_and(|s| s.phase == SyncPhase::Streaming && !s.resyncing)
    })
    .await;
    stop(cancel, handle).await;

    resync_calls_ok(&h);
    let calls = h.sink.calls();
    assert!(!calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
}

#[tokio::test]
async fn an_index_that_held_documents_before_the_sync_is_never_emptied() {
    let h = Harness::new();
    seed_rows(&h, 1..=3);
    // Somebody else's documents: the sync has no state yet.
    h.sink.create(
        INDEX,
        json!({"rankingRules": ["words"]}),
        vec![("99".into(), doc(json!({"id": "99", "name": "theirs"})))],
    );

    let (cancel, handle) = spawn(&h, spec(), options());
    wait_synced(&h).await;
    stop(cancel, handle).await;

    // Built aside and swapped in, as before: the live index kept serving.
    resync_calls_ok(&h);
    let calls = h.sink.calls();
    assert!(!calls.contains(&format!("delete_all {INDEX}")), "{calls:?}");
    assert!(
        h.store.inner().history.iter().all(|s| s.filling.is_empty()),
        "not an index this sync fills from empty"
    );
}
