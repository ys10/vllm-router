mod support;

use agent_router::{
    IngestMode, ManualClock, MetricsSample, SnapshotStore, StateMessage, StateProvider,
};
use std::sync::Arc;
use std::time::Duration;
use support::{server_load_json, session_placement_json};

#[test]
fn phase_one_fixtures_deserialize_and_pass_runtime_validation() {
    for fixture in [
        include_str!("../spec/fixtures/server-load-v1.json"),
        include_str!("../spec/fixtures/session-placement-v1.json"),
    ] {
        let message: StateMessage = serde_json::from_str(fixture).unwrap();
        message.validate().unwrap();
    }
}

#[test]
fn state_store_rejects_out_of_order_messages_and_publishes_consistent_snapshot() {
    let clock = Arc::new(ManualClock::new(10_000, 100));
    let store = SnapshotStore::new(clock.clone());

    let newest: StateMessage = serde_json::from_str(&server_load_json(5, 10_000, 20_000)).unwrap();
    let older: StateMessage = serde_json::from_str(&server_load_json(4, 10_001, 20_001)).unwrap();

    assert!(store.apply(newest, IngestMode::Live).unwrap());
    let stable_reader = store.snapshot();
    assert!(!store.apply(older, IngestMode::Live).unwrap());

    let worker = stable_reader.workers.values().next().unwrap();
    assert_eq!(worker.load.as_ref().unwrap().value.seq, 5);
    assert_eq!(store.snapshot().workers.len(), 1);
}

#[test]
fn state_store_merges_metrics_and_expires_each_source_by_local_freshness() {
    let clock = Arc::new(ManualClock::new(10_000, 100));
    let store = SnapshotStore::new(clock.clone());
    let load: StateMessage = serde_json::from_str(&server_load_json(1, 10_000, 20_000)).unwrap();
    let placement: StateMessage =
        serde_json::from_str(&session_placement_json(2, 10_000, 70_000)).unwrap();

    store.apply(load, IngestMode::Live).unwrap();
    store.apply(placement, IngestMode::Live).unwrap();
    let worker_key = store.snapshot().workers.keys().next().unwrap().clone();
    store.apply_metrics(
        &worker_key,
        MetricsSample {
            running_requests: Some(4),
            waiting_requests: Some(2),
            kv_cache_usage_ratio: Some(0.5),
            prompt_tokens_total: Some(100.0),
            generation_tokens_total: Some(40.0),
        },
        Duration::from_secs(2),
    );

    assert_eq!(
        store.snapshot().workers[&worker_key]
            .metrics
            .as_ref()
            .unwrap()
            .value
            .running_requests,
        Some(4)
    );

    clock.advance(Duration::from_millis(2_001));
    store.expire_stale();
    assert!(store.snapshot().workers[&worker_key].metrics.is_none());
    assert!(store.snapshot().workers[&worker_key].load.is_some());

    clock.advance(Duration::from_millis(1_000));
    store.expire_stale();
    assert!(store.snapshot().workers[&worker_key].load.is_none());
    assert_eq!(store.snapshot().session_placements.len(), 1);

    clock.advance(Duration::from_millis(12_000));
    store.expire_stale();
    assert!(store.snapshot().session_placements.is_empty());
    assert!(store.snapshot().workers.is_empty());
}

#[test]
fn bootstrap_uses_observed_time_and_rejects_stale_snapshots() {
    let clock = Arc::new(ManualClock::new(20_000, 500));
    let store = SnapshotStore::new(clock);
    let stale: StateMessage = serde_json::from_str(&server_load_json(1, 10_000, 30_000)).unwrap();

    assert!(!store.apply(stale, IngestMode::Bootstrap).unwrap());
    assert!(store.snapshot().workers.is_empty());
}

#[test]
fn bootstrap_rejects_a_snapshot_at_its_hard_expiry_boundary() {
    let clock = Arc::new(ManualClock::new(20_000, 500));
    let store = SnapshotStore::new(clock);
    let expired: StateMessage = serde_json::from_str(&server_load_json(1, 19_000, 20_000)).unwrap();

    assert!(!store.apply(expired, IngestMode::Bootstrap).unwrap());
}

#[test]
fn wire_identity_requires_canonical_lowercase_uuid() {
    let uppercase_uuid = server_load_json(1, 10_000, 20_000).replace(
        support::INSTANCE_ID,
        &support::INSTANCE_ID.to_ascii_uppercase(),
    );

    assert!(serde_json::from_str::<StateMessage>(&uppercase_uuid).is_err());
}

#[tokio::test]
async fn independent_expiry_task_removes_state_while_sources_are_idle() {
    let clock = Arc::new(ManualClock::new(10_000, 100));
    let store = SnapshotStore::new(clock.clone());
    let load: StateMessage = serde_json::from_str(&server_load_json(1, 10_000, 20_000)).unwrap();
    store.apply(load, IngestMode::Live).unwrap();
    clock.advance(Duration::from_millis(3_001));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let expiry_store = store.clone();

    let task = tokio::spawn(async move {
        expiry_store
            .run_expirer(Duration::from_millis(1), shutdown_rx)
            .await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while store
            .snapshot()
            .workers
            .values()
            .any(|worker| worker.load.is_some())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send(true).unwrap();
    task.await.unwrap();
}
