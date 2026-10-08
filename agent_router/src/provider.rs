use crate::metrics::{MetricsPublisher, MetricsSample};
use crate::route_hints::{RouteHintSet, RouteHintsDocument};
use crate::state::{
    IngestMode, ServerIdentity, ServerLoadState, SessionPlacementState, StateError,
};
use crate::{Clock, StateMessage, SystemClock};
use arc_swap::ArcSwap;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClusterSnapshot {
    pub version: u64,
    pub published_at_unix_ms: u64,
    pub workers: BTreeMap<String, WorkerSnapshot>,
    pub session_placements: BTreeMap<String, FreshValue<SessionPlacementState>>,
    pub route_hints: RouteHintSet,
    pub source_health: SourceHealth,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct WorkerSnapshot {
    pub identity: Option<ServerIdentity>,
    pub load: Option<FreshValue<ServerLoadState>>,
    pub metrics: Option<FreshValue<MetricsSample>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FreshValue<T> {
    pub value: T,
    pub ingested_at_monotonic_ms: u64,
    pub fresh_until_monotonic_ms: u64,
}

impl<T> FreshValue<T> {
    pub fn is_fresh(&self, monotonic_ms: u64) -> bool {
        monotonic_ms < self.fresh_until_monotonic_ms
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SourceHealth {
    pub redis_connected: bool,
    pub redis_last_error: Option<String>,
    pub redis_last_connected_unix_ms: Option<u64>,
}

pub trait StateProvider: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> Arc<ClusterSnapshot>;
    fn monotonic_ms(&self) -> u64;
}

#[derive(Debug)]
pub struct MockStateProvider {
    snapshot: ArcSwap<ClusterSnapshot>,
    clock: Arc<dyn Clock>,
}

impl Default for MockStateProvider {
    fn default() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(ClusterSnapshot::default()),
            clock: Arc::new(SystemClock::default()),
        }
    }
}

impl MockStateProvider {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(ClusterSnapshot::default()),
            clock,
        }
    }

    pub fn publish(&self, snapshot: ClusterSnapshot) {
        self.snapshot.store(Arc::new(snapshot));
    }
}

impl StateProvider for MockStateProvider {
    fn snapshot(&self) -> Arc<ClusterSnapshot> {
        self.snapshot.load_full()
    }

    fn monotonic_ms(&self) -> u64 {
        self.clock.now().monotonic_ms
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ApplyBatchReport {
    pub accepted: usize,
    pub ignored: usize,
    pub invalid: usize,
}

#[derive(Clone)]
pub struct SnapshotStore {
    shared: Arc<SharedStore>,
}

struct SharedStore {
    clock: Arc<dyn Clock>,
    state: Mutex<MutableState>,
    snapshot: ArcSwap<ClusterSnapshot>,
}

impl std::fmt::Debug for SnapshotStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotStore")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct MutableState {
    version: u64,
    workers: BTreeMap<String, WorkerSnapshot>,
    session_placements: BTreeMap<String, FreshValue<SessionPlacementState>>,
    latest_sequence: BTreeMap<String, u64>,
    route_hints: RouteHintSet,
    source_health: SourceHealth,
}

impl Default for SnapshotStore {
    fn default() -> Self {
        Self::new(Arc::new(SystemClock::default()))
    }
}

impl SnapshotStore {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            shared: Arc::new(SharedStore {
                clock,
                state: Mutex::new(MutableState::default()),
                snapshot: ArcSwap::from_pointee(ClusterSnapshot::default()),
            }),
        }
    }

    pub fn apply(&self, message: StateMessage, mode: IngestMode) -> Result<bool, StateError> {
        let now = self.shared.clock.now();
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        let changed = apply_message(&mut state, message, mode, now)?;
        if changed {
            self.publish_locked(&mut state, now.unix_ms);
        }
        Ok(changed)
    }

    pub fn now(&self) -> crate::ClockReading {
        self.shared.clock.now()
    }

    pub fn monotonic_ms(&self) -> u64 {
        self.now().monotonic_ms
    }

    pub fn apply_json_batch(&self, messages: &[String], mode: IngestMode) -> ApplyBatchReport {
        let now = self.shared.clock.now();
        let mut report = ApplyBatchReport::default();
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        let mut changed = false;
        for raw in messages {
            let message = match serde_json::from_str::<StateMessage>(raw) {
                Ok(message) => message,
                Err(error) => {
                    tracing::warn!(error = %error, "discarding malformed router state message");
                    report.invalid += 1;
                    continue;
                }
            };
            match apply_message(&mut state, message, mode, now) {
                Ok(true) => {
                    report.accepted += 1;
                    changed = true;
                }
                Ok(false) => report.ignored += 1,
                Err(error) => {
                    tracing::warn!(error = %error, "discarding invalid router state message");
                    report.invalid += 1;
                }
            }
        }
        if changed {
            self.publish_locked(&mut state, now.unix_ms);
        }
        report
    }

    /// Apply AgentRoute route-hint documents; only a newer version replaces hints.
    pub fn apply_route_hint_batch(
        &self,
        messages: &[String],
        cluster_id: &str,
    ) -> ApplyBatchReport {
        let now = self.shared.clock.now();
        let mut report = ApplyBatchReport::default();
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        let mut changed = false;
        for raw in messages {
            let document = match serde_json::from_str::<RouteHintsDocument>(raw) {
                Ok(document) => document,
                Err(error) => {
                    tracing::warn!(error = %error, "discarding malformed route-hint document");
                    report.invalid += 1;
                    continue;
                }
            };
            if let Err(error) = document.validate(Some(cluster_id)) {
                tracing::warn!(error = %error, "discarding invalid route-hint document");
                report.invalid += 1;
                continue;
            }
            match state.route_hints.replacement(&document, now) {
                Some(replacement) => {
                    state.route_hints = replacement;
                    report.accepted += 1;
                    changed = true;
                }
                None => report.ignored += 1,
            }
        }
        if changed {
            self.publish_locked(&mut state, now.unix_ms);
        }
        report
    }

    pub fn expire_stale(&self) -> bool {
        let now = self.shared.clock.now();
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        let mut changed = state.route_hints.expire(now.monotonic_ms);
        for worker in state.workers.values_mut() {
            if worker
                .load
                .as_ref()
                .is_some_and(|entry| !entry.is_fresh(now.monotonic_ms))
            {
                worker.load = None;
                changed = true;
            }
            if worker
                .metrics
                .as_ref()
                .is_some_and(|entry| !entry.is_fresh(now.monotonic_ms))
            {
                worker.metrics = None;
                changed = true;
            }
        }
        let before = state.session_placements.len();
        state
            .session_placements
            .retain(|_, entry| entry.is_fresh(now.monotonic_ms));
        changed |= before != state.session_placements.len();
        let workers_with_placements: std::collections::BTreeSet<String> = state
            .session_placements
            .values()
            .filter_map(|entry| entry.value.server.worker_key().ok())
            .collect();
        let before = state.workers.len();
        state.workers.retain(|worker_key, worker| {
            worker.load.is_some()
                || worker.metrics.is_some()
                || workers_with_placements.contains(worker_key)
        });
        changed |= before != state.workers.len();
        if changed {
            self.publish_locked(&mut state, now.unix_ms);
        }
        changed
    }

    pub async fn run_expirer(
        &self,
        interval: Duration,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let interval = interval.max(Duration::from_millis(1));
        loop {
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    self.expire_stale();
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    pub fn set_redis_status(&self, connected: bool, error: Option<String>) {
        let now = self.shared.clock.now();
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        if state.source_health.redis_connected == connected
            && state.source_health.redis_last_error == error
        {
            return;
        }
        state.source_health.redis_connected = connected;
        state.source_health.redis_last_error = error;
        if connected {
            state.source_health.redis_last_connected_unix_ms = Some(now.unix_ms);
        }
        self.publish_locked(&mut state, now.unix_ms);
    }

    fn publish_locked(&self, state: &mut MutableState, unix_ms: u64) {
        state.version = state.version.saturating_add(1);
        self.shared.snapshot.store(Arc::new(ClusterSnapshot {
            version: state.version,
            published_at_unix_ms: unix_ms,
            workers: state.workers.clone(),
            session_placements: state.session_placements.clone(),
            route_hints: state.route_hints.clone(),
            source_health: state.source_health.clone(),
        }));
    }
}

impl StateProvider for SnapshotStore {
    fn snapshot(&self) -> Arc<ClusterSnapshot> {
        self.shared.snapshot.load_full()
    }

    fn monotonic_ms(&self) -> u64 {
        SnapshotStore::monotonic_ms(self)
    }
}

impl MetricsPublisher for SnapshotStore {
    fn publish_metrics(&self, worker_key: &str, sample: MetricsSample, fresh_for: Duration) {
        let now = self.shared.clock.now();
        let fresh_ms = fresh_for.as_millis().try_into().unwrap_or(u64::MAX);
        let mut state = self.shared.state.lock().expect("snapshot store poisoned");
        let worker = state.workers.entry(worker_key.to_string()).or_default();
        worker.metrics = Some(FreshValue {
            value: sample,
            ingested_at_monotonic_ms: now.monotonic_ms,
            fresh_until_monotonic_ms: now.monotonic_ms.saturating_add(fresh_ms),
        });
        self.publish_locked(&mut state, now.unix_ms);
    }
}

impl SnapshotStore {
    pub fn apply_metrics(&self, worker_key: &str, sample: MetricsSample, fresh_for: Duration) {
        self.publish_metrics(worker_key, sample, fresh_for);
    }
}

fn apply_message(
    state: &mut MutableState,
    message: StateMessage,
    mode: IngestMode,
    now: crate::ClockReading,
) -> Result<bool, StateError> {
    message.validate()?;
    let Some(remaining_fresh_ms) = message.remaining_fresh_ms(mode, now)? else {
        return Ok(false);
    };
    let entity_key = message.entity_key()?;
    if state
        .latest_sequence
        .get(&entity_key)
        .is_some_and(|latest| *latest >= message.seq())
    {
        return Ok(false);
    }
    let worker_key = message.server().worker_key()?;
    let fresh_until = now.monotonic_ms.saturating_add(remaining_fresh_ms);
    let sequence = message.seq();
    match message {
        StateMessage::ServerLoad(load) => {
            let worker = state.workers.entry(worker_key).or_default();
            worker.identity = Some(load.server.clone());
            worker.load = Some(FreshValue {
                value: load,
                ingested_at_monotonic_ms: now.monotonic_ms,
                fresh_until_monotonic_ms: fresh_until,
            });
        }
        StateMessage::SessionPlacement(placement) => {
            state
                .workers
                .entry(worker_key.clone())
                .or_default()
                .identity = Some(placement.server.clone());
            let placement_key = format!("{}:{worker_key}", placement.payload.session_key);
            state.session_placements.insert(
                placement_key,
                FreshValue {
                    value: placement,
                    ingested_at_monotonic_ms: now.monotonic_ms,
                    fresh_until_monotonic_ms: fresh_until,
                },
            );
        }
    }
    state.latest_sequence.insert(entity_key, sequence);
    Ok(true)
}
