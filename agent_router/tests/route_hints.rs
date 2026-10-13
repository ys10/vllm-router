use agent_router::{
    ManualClock, RedisIngestError, RedisReadBatch, RouteHintAck, RouteHintAckPublisher,
    RouteHintAcker, RouteHintConnection, RouteHintConnector, RouteHintIngestor,
    RouteHintRedisConfig, RouteHintState, RouteHintsDocument, SnapshotStore, StateProvider,
};
use async_trait::async_trait;
use serde_json::json;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CLUSTER: &str = "prod-a";
const NOW_UNIX_MS: u64 = 1_760_000_000_000;

fn session(index: u8) -> String {
    format!("{index:02x}").repeat(32)
}

fn hint(
    session_index: u8,
    server: &str,
    state: &str,
    valid_for_ms: u64,
    version: u64,
) -> serde_json::Value {
    json!({
        "session_key": session(session_index),
        "source_server": "vllm-p0",
        "preferred_server": server,
        "plan_id": format!("plan-{session_index}"),
        "state": state,
        "valid_until_ms": NOW_UNIX_MS + valid_for_ms,
        "state_version": version,
    })
}

fn document(version: u64, hints: Vec<serde_json::Value>) -> String {
    document_from("apc-a", version, NOW_UNIX_MS, hints)
}

fn document_from(
    producer: &str,
    version: u64,
    generated_at_ms: u64,
    hints: Vec<serde_json::Value>,
) -> String {
    json!({
        "schema_version": 1,
        "message_type": "route_hints",
        "cluster_id": CLUSTER,
        "producer_id": producer,
        "state_version": version,
        "generated_at_ms": generated_at_ms,
        "hints": hints,
    })
    .to_string()
}

fn store() -> (Arc<ManualClock>, SnapshotStore) {
    let clock = Arc::new(ManualClock::new(NOW_UNIX_MS, 500));
    (clock.clone(), SnapshotStore::new(clock))
}

fn config() -> RouteHintRedisConfig {
    RouteHintRedisConfig {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        read_block: Duration::from_millis(1),
        ..RouteHintRedisConfig::production("redis://unused", CLUSTER)
    }
}

#[test]
fn fixtures_match_runtime_types() {
    let document: RouteHintsDocument =
        serde_json::from_str(include_str!("../spec/fixtures/route-hints-v1.json")).unwrap();
    document.validate(Some(CLUSTER)).unwrap();
    assert_eq!(document.hints[0].state, RouteHintState::Ready);

    let ack: RouteHintAck =
        serde_json::from_str(include_str!("../spec/fixtures/route-hint-ack-v1.json")).unwrap();
    assert_eq!(ack.message_type, "route_hint_ack");
    assert_eq!(ack.session_key, document.hints[0].session_key);
}

#[test]
fn newer_document_replaces_hints_and_older_versions_are_ignored() {
    let (_, store) = store();
    let first = document(5, vec![hint(1, "vllm-p1", "ready", 10_000, 4)]);
    let older = document(4, vec![hint(2, "vllm-p2", "ready", 10_000, 3)]);
    let newer = document(6, vec![hint(3, "vllm-p2", "active", 10_000, 6)]);

    assert_eq!(store.apply_route_hint_batch(&[first], CLUSTER).accepted, 1);
    assert_eq!(store.apply_route_hint_batch(&[older], CLUSTER).ignored, 1);
    let hints = store.snapshot().route_hints.clone();
    assert_eq!(hints.document_version, 5);
    assert!(hints.hints.contains_key(&session(1)));

    store.apply_route_hint_batch(&[newer], CLUSTER);
    let hints = store.snapshot().route_hints.clone();
    assert_eq!(hints.document_version, 6);
    assert_eq!(
        hints.hints.keys().cloned().collect::<Vec<_>>(),
        vec![session(3)]
    );
}

#[test]
fn empty_document_revokes_every_hint() {
    let (_, store) = store();
    store.apply_route_hint_batch(
        &[document(1, vec![hint(1, "vllm-p1", "ready", 10_000, 1)])],
        CLUSTER,
    );

    store.apply_route_hint_batch(&[document(2, vec![])], CLUSTER);

    assert!(store.snapshot().route_hints.hints.is_empty());
}

#[test]
fn only_ready_and_active_hints_are_retained() {
    let (_, store) = store();
    let states = ["preparing", "ready", "active", "expired", "cancelled"];
    let hints = states
        .iter()
        .enumerate()
        .map(|(index, state)| hint(index as u8, "vllm-p1", state, 10_000, 1))
        .collect();

    store.apply_route_hint_batch(&[document(1, hints)], CLUSTER);

    let retained: Vec<_> = store.snapshot().route_hints.hints.keys().cloned().collect();
    assert_eq!(retained, vec![session(1), session(2)]);
}

#[test]
fn validity_is_converted_to_monotonic_with_clock_skew_margin() {
    let (clock, store) = store();
    let doc = document(
        1,
        vec![
            hint(1, "vllm-p1", "ready", 5_000, 1),
            hint(2, "vllm-p1", "ready", 2_000, 1),
        ],
    );

    store.apply_route_hint_batch(&[doc], CLUSTER);
    let snapshot = store.snapshot();
    let retained = &snapshot.route_hints.hints;
    assert_eq!(
        retained.len(),
        1,
        "a hint inside the skew margin is unusable"
    );
    assert_eq!(retained[&session(1)].valid_until_monotonic_ms, 500 + 3_000);

    clock.advance(Duration::from_millis(3_000));
    assert!(store.expire_stale());
    assert!(store.snapshot().route_hints.hints.is_empty());
}

#[test]
fn highest_hint_version_wins_for_one_session() {
    let (_, store) = store();
    let mut stale = hint(1, "vllm-p1", "ready", 10_000, 3);
    stale["plan_id"] = json!("plan-old");
    let current = hint(1, "vllm-p2", "ready", 10_000, 7);

    store.apply_route_hint_batch(&[document(8, vec![current, stale])], CLUSTER);

    assert_eq!(
        store.snapshot().route_hints.hints[&session(1)].preferred_server,
        "vllm-p2"
    );
}

#[test]
fn invalid_documents_are_rejected() {
    let (_, store) = store();
    let mut foreign: serde_json::Value =
        serde_json::from_str(&document(1, vec![hint(1, "vllm-p1", "ready", 10_000, 1)])).unwrap();
    foreign["cluster_id"] = json!("prod-b");
    let mut bad_session = hint(1, "vllm-p1", "ready", 10_000, 1);
    bad_session["session_key"] = json!("not-a-digest");
    let mut extra = hint(1, "vllm-p1", "ready", 10_000, 1);
    extra["unexpected"] = json!(true);

    let report = store.apply_route_hint_batch(
        &[
            foreign.to_string(),
            document(2, vec![bad_session]),
            document(3, vec![extra]),
            "{".to_string(),
        ],
        CLUSTER,
    );

    assert_eq!(report.invalid, 4);
    assert_eq!(store.snapshot().route_hints.document_version, 0);
}

#[test]
fn acker_sends_one_ack_per_exact_version_without_blocking() {
    let (_, store) = store();
    store.apply_route_hint_batch(
        &[document(1, vec![hint(1, "vllm-p1", "ready", 10_000, 4)])],
        CLUSTER,
    );
    let ready = store.snapshot().route_hints.hints[&session(1)].clone();
    let (acker, mut receiver) = RouteHintAcker::new(CLUSTER, "router-a", 1);

    assert!(acker.acknowledge(&ready, 11));
    assert!(
        !acker.acknowledge(&ready, 12),
        "same version is acknowledged once"
    );
    let mut active = ready.clone();
    active.state_version = 5;
    assert!(
        !acker.acknowledge(&active, 13),
        "full queue drops instead of blocking"
    );
    assert_eq!(acker.dropped(), 1);

    let ack = receiver.try_recv().unwrap();
    assert_eq!(
        (
            ack.plan_id.as_str(),
            ack.state_version,
            ack.observed_at_ms,
            ack.router_id.as_str()
        ),
        ("plan-1", 4, 11, "router-a")
    );
    assert!(
        acker.acknowledge(&active, 14),
        "dropped version can be retried"
    );
    assert_eq!(receiver.try_recv().unwrap().state_version, 5);
}

#[derive(Debug, Default)]
struct FakeRedis {
    snapshot: Option<String>,
    stream: Mutex<VecDeque<Vec<String>>>,
    acks: Mutex<Vec<String>>,
    failing_publishes: Mutex<usize>,
}

#[derive(Debug)]
struct FakeConnector(Arc<FakeRedis>);

#[derive(Debug)]
struct FakeConnection(Arc<FakeRedis>);

#[async_trait]
impl RouteHintConnector for FakeConnector {
    async fn connect(
        &self,
        _config: &RouteHintRedisConfig,
    ) -> Result<Box<dyn RouteHintConnection>, RedisIngestError> {
        Ok(Box::new(FakeConnection(self.0.clone())))
    }
}

#[async_trait]
impl RouteHintConnection for FakeConnection {
    async fn bootstrap(
        &mut self,
        _config: &RouteHintRedisConfig,
    ) -> Result<RedisReadBatch, RedisIngestError> {
        Ok(RedisReadBatch {
            cursor: "1-0".into(),
            messages: self.0.snapshot.clone().into_iter().collect(),
        })
    }

    async fn read_after(
        &mut self,
        _config: &RouteHintRedisConfig,
        cursor: &str,
    ) -> Result<RedisReadBatch, RedisIngestError> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        Ok(RedisReadBatch {
            cursor: cursor.to_string(),
            messages: self
                .0
                .stream
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default(),
        })
    }

    async fn publish_ack(
        &mut self,
        _config: &RouteHintRedisConfig,
        ack: &str,
    ) -> Result<(), RedisIngestError> {
        let mut failing = self.0.failing_publishes.lock().unwrap();
        if *failing > 0 {
            *failing -= 1;
            return Err(RedisIngestError::Connection("link lost".into()));
        }
        self.0.acks.lock().unwrap().push(ack.to_string());
        Ok(())
    }
}

async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition not reached");
}

#[tokio::test]
async fn ingestor_bootstraps_snapshot_then_follows_stream() {
    let (_, store) = store();
    let store = Arc::new(store);
    let redis = Arc::new(FakeRedis {
        snapshot: Some(document(3, vec![hint(1, "vllm-p1", "ready", 10_000, 3)])),
        stream: Mutex::new(VecDeque::from([vec![document(
            4,
            vec![hint(2, "vllm-p2", "active", 10_000, 4)],
        )]])),
        ..FakeRedis::default()
    });
    let ingestor =
        RouteHintIngestor::with_connector(config(), Arc::new(FakeConnector(redis)), store.clone());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move { ingestor.run(shutdown_rx).await });

    wait_until(|| store.snapshot().route_hints.document_version == 4).await;
    assert!(store.snapshot().route_hints.hints.contains_key(&session(2)));

    shutdown_tx.send(true).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn ack_publisher_retries_and_writes_protocol_json() {
    let (_, store) = store();
    store.apply_route_hint_batch(
        &[document(1, vec![hint(1, "vllm-p1", "ready", 10_000, 9)])],
        CLUSTER,
    );
    let ready = store.snapshot().route_hints.hints[&session(1)].clone();
    let redis = Arc::new(FakeRedis {
        failing_publishes: Mutex::new(1),
        ..FakeRedis::default()
    });
    let publisher = Arc::new(RouteHintAckPublisher::with_connector(
        config(),
        Arc::new(FakeConnector(redis.clone())),
    ));
    let (acker, receiver) = RouteHintAcker::new(CLUSTER, "router-a", 8);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn({
        let publisher = publisher.clone();
        async move { publisher.run(receiver, shutdown_rx).await }
    });

    assert!(acker.acknowledge(&ready, 77));
    wait_until(|| !redis.acks.lock().unwrap().is_empty()).await;

    let ack: RouteHintAck = serde_json::from_str(&redis.acks.lock().unwrap()[0]).unwrap();
    assert_eq!((ack.state_version, ack.observed_at_ms), (9, 77));
    assert_eq!(publisher.failed(), 0);
    shutdown_tx.send(true).unwrap();
    task.await.unwrap();
}

#[test]
fn restarted_producer_replaces_hints_despite_a_lower_version() {
    let (_, store) = store();
    store.apply_route_hint_batch(
        &[document_from(
            "apc-old",
            90,
            NOW_UNIX_MS,
            vec![hint(1, "vllm-p1", "ready", 10_000, 90)],
        )],
        CLUSTER,
    );

    let report = store.apply_route_hint_batch(
        &[document_from(
            "apc-new",
            1,
            NOW_UNIX_MS + 5,
            vec![hint(2, "vllm-p2", "ready", 10_000, 1)],
        )],
        CLUSTER,
    );

    assert_eq!(report.accepted, 1);
    let hints = store.snapshot().route_hints.clone();
    assert_eq!(
        (hints.producer_id.as_str(), hints.document_version),
        ("apc-new", 1)
    );
    assert_eq!(
        hints.hints.keys().cloned().collect::<Vec<_>>(),
        vec![session(2)]
    );
}

#[test]
fn late_document_from_a_replaced_producer_is_ignored() {
    let (_, store) = store();
    store.apply_route_hint_batch(
        &[document_from("apc-new", 1, NOW_UNIX_MS + 5, vec![])],
        CLUSTER,
    );

    let report = store.apply_route_hint_batch(
        &[document_from(
            "apc-old",
            91,
            NOW_UNIX_MS,
            vec![hint(1, "vllm-p1", "ready", 10_000, 91)],
        )],
        CLUSTER,
    );

    assert_eq!(report.ignored, 1);
    assert_eq!(store.snapshot().route_hints.producer_id, "apc-new");
}

#[test]
fn acker_acknowledges_a_restarted_producer_reusing_plan_and_version() {
    let (_, store) = store();
    let (acker, mut receiver) = RouteHintAcker::new(CLUSTER, "router-a", 4);
    store.apply_route_hint_batch(
        &[document_from(
            "apc-old",
            3,
            NOW_UNIX_MS,
            vec![hint(1, "vllm-p1", "ready", 10_000, 3)],
        )],
        CLUSTER,
    );
    let old = store.snapshot().route_hints.hints[&session(1)].clone();
    assert!(acker.acknowledge(&old, 1));

    store.apply_route_hint_batch(
        &[document_from(
            "apc-new",
            3,
            NOW_UNIX_MS + 1,
            vec![hint(1, "vllm-p1", "ready", 10_000, 3)],
        )],
        CLUSTER,
    );
    let new = store.snapshot().route_hints.hints[&session(1)].clone();

    assert_eq!(
        (new.plan_id.as_str(), new.state_version),
        (old.plan_id.as_str(), old.state_version)
    );
    assert!(
        acker.acknowledge(&new, 2),
        "same plan and version from a new producer is a new hint"
    );
    assert_eq!(receiver.try_recv().unwrap().observed_at_ms, 1);
    assert_eq!(receiver.try_recv().unwrap().observed_at_ms, 2);
}
