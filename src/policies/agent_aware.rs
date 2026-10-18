use super::{LoadBalancingPolicy, RequestHeaders};
use crate::config::AgentAwareConfig;
use crate::core::Worker;
use crate::metrics::RouterMetrics;
use agent_router::{
    projected_server_load_ms, BalanceThresholds, ClusterSnapshot, FreshValue, HbmDramScorer,
    HbmDramScorerConfig, MetricsScraper, RedisIngestConfig, RedisIngestor, RequestOutcome,
    RequestScore, RequestScorer, ReqwestMetricsFetcher, ReservationLedger, ReservationView,
    RolloutPercent, RouteHint, RouteHintAck, RouteHintAckPublisher, RouteHintAcker,
    RouteHintIngestor, RouteHintOutcome, RouteHintRedisConfig, RoutingContext, ServerLoadState,
    SnapshotMetricsTargets, SnapshotStore, StateProvider, WorkerRole, WorkerSnapshot,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, warn};

const ROUTE_HINT_ACK_QUEUE: usize = 1_024;

#[derive(Debug)]
struct ScoredCandidate {
    worker_key: String,
    server_id: String,
    reservation_key: String,
    base_load_ms: f64,
    score: RequestScore,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentAwareRolloutStats {
    pub shadow_evaluated_attempts: u64,
    pub shadow_agreement_attempts: u64,
    pub shadow_disagreement_attempts: u64,
    pub enforced_attempts: u64,
    pub baseline_attempts: u64,
    pub state_fallback_attempts: u64,
}

#[derive(Debug, Default)]
struct RolloutCounters {
    shadow_evaluated_attempts: AtomicU64,
    shadow_agreement_attempts: AtomicU64,
    shadow_disagreement_attempts: AtomicU64,
    enforced_attempts: AtomicU64,
    baseline_attempts: AtomicU64,
    state_fallback_attempts: AtomicU64,
}

impl RolloutCounters {
    fn record(&self, decision: &SelectionDecision) {
        match decision.shadow_comparison() {
            ShadowComparison::Agreement => {
                self.shadow_evaluated_attempts
                    .fetch_add(1, Ordering::Relaxed);
                self.shadow_agreement_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            ShadowComparison::Disagreement => {
                self.shadow_evaluated_attempts
                    .fetch_add(1, Ordering::Relaxed);
                self.shadow_disagreement_attempts
                    .fetch_add(1, Ordering::Relaxed);
            }
            ShadowComparison::Unavailable => {
                self.state_fallback_attempts.fetch_add(1, Ordering::Relaxed);
            }
        }
        match decision.action {
            RolloutAction::Enforced => {
                self.enforced_attempts.fetch_add(1, Ordering::Relaxed);
            }
            RolloutAction::Baseline => {
                self.baseline_attempts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn snapshot(&self) -> AgentAwareRolloutStats {
        AgentAwareRolloutStats {
            shadow_evaluated_attempts: self.shadow_evaluated_attempts.load(Ordering::Relaxed),
            shadow_agreement_attempts: self.shadow_agreement_attempts.load(Ordering::Relaxed),
            shadow_disagreement_attempts: self.shadow_disagreement_attempts.load(Ordering::Relaxed),
            enforced_attempts: self.enforced_attempts.load(Ordering::Relaxed),
            baseline_attempts: self.baseline_attempts.load(Ordering::Relaxed),
            state_fallback_attempts: self.state_fallback_attempts.load(Ordering::Relaxed),
        }
    }
}

/// Per-outcome counts of route-hint evaluation for attempts with a session.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AgentRouteHintStats {
    pub applied: u64,
    pub shadow_only: u64,
    pub target_imbalanced: u64,
    pub target_unavailable: u64,
    pub expired: u64,
    pub no_hint: u64,
}

#[derive(Debug, Default)]
struct RouteHintCounters {
    applied: AtomicU64,
    shadow_only: AtomicU64,
    target_imbalanced: AtomicU64,
    target_unavailable: AtomicU64,
    expired: AtomicU64,
    no_hint: AtomicU64,
}

impl RouteHintCounters {
    fn record(&self, outcome: RouteHintOutcome) {
        let counter = match outcome {
            RouteHintOutcome::Applied => &self.applied,
            RouteHintOutcome::ShadowOnly => &self.shadow_only,
            RouteHintOutcome::TargetImbalanced => &self.target_imbalanced,
            RouteHintOutcome::TargetUnavailable => &self.target_unavailable,
            RouteHintOutcome::Expired => &self.expired,
            RouteHintOutcome::NoHint => &self.no_hint,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> AgentRouteHintStats {
        AgentRouteHintStats {
            applied: self.applied.load(Ordering::Relaxed),
            shadow_only: self.shadow_only.load(Ordering::Relaxed),
            target_imbalanced: self.target_imbalanced.load(Ordering::Relaxed),
            target_unavailable: self.target_unavailable.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            no_hint: self.no_hint.load(Ordering::Relaxed),
        }
    }
}

/// A valid hint whose target maps to exactly one routable candidate.
struct HintTarget {
    hint: RouteHint,
    candidate_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RolloutAction {
    Baseline,
    Enforced,
}

impl RolloutAction {
    fn label(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Enforced => "enforced",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShadowComparison {
    Agreement,
    Disagreement,
    Unavailable,
}

impl ShadowComparison {
    fn label(self) -> &'static str {
        match self {
            Self::Agreement => "agreement",
            Self::Disagreement => "disagreement",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SelectionDecision {
    selected_index: usize,
    baseline_index: usize,
    shadow_index: Option<usize>,
    action: RolloutAction,
}

impl SelectionDecision {
    fn new(baseline_index: usize, shadow_index: Option<usize>, rollout_included: bool) -> Self {
        match (shadow_index, rollout_included) {
            (Some(selected_index), true) => Self {
                selected_index,
                baseline_index,
                shadow_index,
                action: RolloutAction::Enforced,
            },
            _ => Self {
                selected_index: baseline_index,
                baseline_index,
                shadow_index,
                action: RolloutAction::Baseline,
            },
        }
    }

    fn shadow_comparison(self) -> ShadowComparison {
        match self.shadow_index {
            Some(shadow_index) if shadow_index == self.baseline_index => {
                ShadowComparison::Agreement
            }
            Some(_) => ShadowComparison::Disagreement,
            None => ShadowComparison::Unavailable,
        }
    }

    fn is_enforced(self) -> bool {
        self.action == RolloutAction::Enforced
    }
}

#[derive(Clone)]
struct RuntimeConfig {
    redis_url: String,
    cluster_id: String,
    metrics_scrape_interval: Duration,
    metrics_timeout: Duration,
    metrics_fresh_for: Duration,
    expiry_interval: Duration,
}

struct AgentRuntime {
    config: RuntimeConfig,
    store: Arc<SnapshotStore>,
    route_hint_acks: Mutex<Option<tokio::sync::mpsc::Receiver<RouteHintAck>>>,
    started: AtomicBool,
    shutdown: Mutex<Option<tokio::sync::watch::Sender<bool>>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl std::fmt::Debug for AgentRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentRuntime")
            .field("redis_url", &"<redacted>")
            .field("cluster_id", &self.config.cluster_id)
            .field("started", &self.started.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl AgentRuntime {
    fn start(&self) -> Result<(), String> {
        if self
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let fetcher = match ReqwestMetricsFetcher::new(self.config.metrics_timeout) {
            Ok(fetcher) => Arc::new(fetcher),
            Err(error) => {
                self.started.store(false, Ordering::Release);
                return Err(error.to_string());
            }
        };
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let state: Arc<dyn StateProvider> = self.store.clone();
        let metrics = MetricsScraper::new(
            Arc::new(SnapshotMetricsTargets::new(state)),
            fetcher,
            self.store.clone(),
            self.config.metrics_fresh_for,
        );
        let redis = RedisIngestor::new(
            RedisIngestConfig::production(
                self.config.redis_url.clone(),
                self.config.cluster_id.clone(),
            ),
            self.store.clone(),
        );

        let redis_task = tokio::spawn({
            let shutdown = shutdown_rx.clone();
            async move { redis.run(shutdown).await }
        });
        let metrics_interval = self.config.metrics_scrape_interval;
        let metrics_task = tokio::spawn({
            let shutdown = shutdown_rx.clone();
            async move { metrics.run(metrics_interval, shutdown).await }
        });
        let mut tasks = Vec::with_capacity(5);
        if let Some(acks) = self
            .route_hint_acks
            .lock()
            .expect("agent runtime poisoned")
            .take()
        {
            let hint_config = RouteHintRedisConfig::production(
                self.config.redis_url.clone(),
                self.config.cluster_id.clone(),
            );
            let ingestor = RouteHintIngestor::new(hint_config.clone(), self.store.clone());
            let publisher = RouteHintAckPublisher::new(hint_config);
            tasks.push(tokio::spawn({
                let shutdown = shutdown_rx.clone();
                async move { ingestor.run(shutdown).await }
            }));
            tasks.push(tokio::spawn({
                let shutdown = shutdown_rx.clone();
                async move { publisher.run(acks, shutdown).await }
            }));
        }
        let expiry_store = self.store.clone();
        let expiry_interval = self.config.expiry_interval;
        let expiry_task =
            tokio::spawn(
                async move { expiry_store.run_expirer(expiry_interval, shutdown_rx).await },
            );

        *self.shutdown.lock().expect("agent runtime poisoned") = Some(shutdown_tx);
        tasks.extend([redis_task, metrics_task, expiry_task]);
        self.tasks
            .lock()
            .expect("agent runtime poisoned")
            .extend(tasks);
        Ok(())
    }
}

impl Drop for AgentRuntime {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.get_mut().ok().and_then(Option::take) {
            let _ = shutdown.send(true);
        }
    }
}

pub struct AgentAwarePolicy {
    state: Arc<dyn StateProvider>,
    scorer: Arc<dyn RequestScorer>,
    cluster_secret: Vec<u8>,
    reservations: Arc<ReservationLedger>,
    fallback_cursor: AtomicUsize,
    fallback_reservation_ms: f64,
    rollout: RolloutPercent,
    rollout_counters: RolloutCounters,
    route_hint_acker: Option<Arc<RouteHintAcker>>,
    route_hint_counters: RouteHintCounters,
    candidate_role: WorkerRole,
    runtime: Option<AgentRuntime>,
}

impl std::fmt::Debug for AgentAwarePolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AgentAwarePolicy")
            .field("cluster_secret", &"<redacted>")
            .field("rollout_percent", &self.rollout.get())
            .field("rollout_stats", &self.rollout_counters.snapshot())
            .field("route_hints_enabled", &self.route_hint_acker.is_some())
            .field("candidate_role", &self.candidate_role)
            .field(
                "active_reservations",
                &self.reservations.active_reservations(),
            )
            .field("runtime", &self.runtime)
            .finish_non_exhaustive()
    }
}

impl AgentAwarePolicy {
    pub fn from_config(config: &AgentAwareConfig) -> Result<Self, String> {
        let redis_url = url::Url::parse(config.redis_url.expose_secret())
            .map_err(|_| "agent-aware Redis URL is invalid".to_string())?;
        if !matches!(redis_url.scheme(), "redis" | "rediss") {
            return Err("agent-aware Redis URL must use redis or rediss".to_string());
        }
        if config.cluster_id.len() > 128
            || !config
                .cluster_id
                .bytes()
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric())
            || config
                .cluster_id
                .bytes()
                .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-".contains(&byte)))
        {
            return Err("agent-aware cluster ID is invalid".to_string());
        }
        if config.fallback_reservation_ms == 0
            || config.metrics_scrape_interval_ms == 0
            || config.metrics_timeout_ms == 0
            || config.metrics_fresh_ms == 0
            || config.expiry_interval_ms == 0
        {
            return Err("agent-aware reservation and runtime intervals must be positive".into());
        }
        // Still validated so existing configs keep their meaning-free contract, but
        // selection no longer uses fixed balance bands (see `select_shadow_candidate`).
        BalanceThresholds::new(
            config.balance_abs_threshold_ms,
            config.balance_rel_threshold,
        )
        .map_err(|error| error.to_string())?;
        let rollout =
            RolloutPercent::new(config.rollout_percent).map_err(|error| error.to_string())?;
        let scorer_config = HbmDramScorerConfig {
            default_prefill_tokens: config.default_prefill_tokens,
            default_decode_tokens: config.default_decode_tokens,
            dram_block_restore_ms: config.dram_block_restore_ms,
            minimum_reservation_ms: config.minimum_reservation_ms as f64,
        };
        let store = Arc::new(SnapshotStore::default());
        let state: Arc<dyn StateProvider> = store.clone();
        let (route_hint_acker, route_hint_acks) = if config.route_hints_enabled {
            let (acker, receiver) = RouteHintAcker::new(
                config.cluster_id.clone(),
                format!("router-{}", uuid::Uuid::new_v4()),
                ROUTE_HINT_ACK_QUEUE,
            );
            (Some(Arc::new(acker)), Some(receiver))
        } else {
            (None, None)
        };
        let scorer = Arc::new(
            HbmDramScorer::new(config.cluster_secret.expose_secret(), scorer_config)
                .map_err(|error| error.to_string())?,
        );
        Ok(Self {
            state,
            scorer,
            cluster_secret: config.cluster_secret.expose_secret().as_bytes().to_vec(),
            reservations: Arc::new(ReservationLedger::default()),
            fallback_cursor: AtomicUsize::new(0),
            fallback_reservation_ms: config.fallback_reservation_ms as f64,
            rollout,
            rollout_counters: RolloutCounters::default(),
            route_hint_acker,
            route_hint_counters: RouteHintCounters::default(),
            candidate_role: WorkerRole::Unified,
            runtime: Some(AgentRuntime {
                config: RuntimeConfig {
                    redis_url: config.redis_url.expose_secret().to_string(),
                    cluster_id: config.cluster_id.clone(),
                    metrics_scrape_interval: Duration::from_millis(
                        config.metrics_scrape_interval_ms,
                    ),
                    metrics_timeout: Duration::from_millis(config.metrics_timeout_ms),
                    metrics_fresh_for: Duration::from_millis(config.metrics_fresh_ms),
                    expiry_interval: Duration::from_millis(config.expiry_interval_ms),
                },
                store,
                route_hint_acks: Mutex::new(route_hint_acks),
                started: AtomicBool::new(false),
                shutdown: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
            }),
        })
    }

    pub fn with_state_provider(
        state: Arc<dyn StateProvider>,
        cluster_secret: impl AsRef<[u8]>,
        thresholds: BalanceThresholds,
        scorer_config: HbmDramScorerConfig,
    ) -> Result<Self, String> {
        Self::with_state_provider_and_rollout(
            state,
            cluster_secret,
            thresholds,
            scorer_config,
            RolloutPercent::new(100).expect("100 percent rollout is valid"),
        )
    }

    pub fn with_state_provider_and_rollout(
        state: Arc<dyn StateProvider>,
        cluster_secret: impl AsRef<[u8]>,
        thresholds: BalanceThresholds,
        scorer_config: HbmDramScorerConfig,
        rollout: RolloutPercent,
    ) -> Result<Self, String> {
        let scorer = Arc::new(
            HbmDramScorer::new(cluster_secret.as_ref(), scorer_config)
                .map_err(|error| error.to_string())?,
        );
        Self::with_scorer_and_rollout(state, cluster_secret, thresholds, scorer, 100.0, rollout)
    }

    pub fn with_scorer(
        state: Arc<dyn StateProvider>,
        cluster_secret: impl AsRef<[u8]>,
        thresholds: BalanceThresholds,
        scorer: Arc<dyn RequestScorer>,
        fallback_reservation_ms: f64,
    ) -> Result<Self, String> {
        Self::with_scorer_and_rollout(
            state,
            cluster_secret,
            thresholds,
            scorer,
            fallback_reservation_ms,
            RolloutPercent::new(100).expect("100 percent rollout is valid"),
        )
    }

    pub fn with_scorer_and_rollout(
        state: Arc<dyn StateProvider>,
        cluster_secret: impl AsRef<[u8]>,
        thresholds: BalanceThresholds,
        scorer: Arc<dyn RequestScorer>,
        fallback_reservation_ms: f64,
        rollout: RolloutPercent,
    ) -> Result<Self, String> {
        let cluster_secret = cluster_secret.as_ref();
        BalanceThresholds::new(thresholds.absolute_ms, thresholds.relative)
            .map_err(|error| error.to_string())?;
        if cluster_secret.is_empty() {
            return Err("cluster secret must not be empty".to_string());
        }
        if !fallback_reservation_ms.is_finite() || fallback_reservation_ms <= 0.0 {
            return Err("fallback reservation must be finite and positive".to_string());
        }
        Ok(Self {
            state,
            scorer,
            cluster_secret: cluster_secret.to_vec(),
            reservations: Arc::new(ReservationLedger::default()),
            fallback_cursor: AtomicUsize::new(0),
            fallback_reservation_ms,
            rollout,
            rollout_counters: RolloutCounters::default(),
            route_hint_acker: None,
            route_hint_counters: RouteHintCounters::default(),
            candidate_role: WorkerRole::Unified,
            runtime: None,
        })
    }

    /// Score only workers that publish this role, e.g. Prefill for PD routing.
    pub fn with_candidate_role(mut self, role: WorkerRole) -> Self {
        self.candidate_role = role;
        self
    }

    /// Enable AgentRoute hints, acknowledging consumed hints through `acker`.
    pub fn with_route_hint_acker(mut self, acker: Arc<RouteHintAcker>) -> Self {
        self.route_hint_acker = Some(acker);
        self
    }

    pub fn route_hint_stats(&self) -> AgentRouteHintStats {
        self.route_hint_counters.snapshot()
    }

    pub fn start_background_tasks(&self) -> Result<(), String> {
        match &self.runtime {
            Some(runtime) => runtime.start(),
            None => Ok(()),
        }
    }

    pub fn reservations(&self) -> Arc<ReservationLedger> {
        self.reservations.clone()
    }

    pub fn rollout_stats(&self) -> AgentAwareRolloutStats {
        self.rollout_counters.snapshot()
    }

    fn candidates_from_snapshot(
        &self,
        snapshot: &ClusterSnapshot,
        now: u64,
        session_key: Option<&str>,
        workers: &[Arc<dyn Worker>],
        context: &RoutingContext,
    ) -> Option<Vec<ScoredCandidate>> {
        let mut fresh_loads: HashMap<
            (String, u64),
            (&String, &WorkerSnapshot, &FreshValue<ServerLoadState>),
        > = HashMap::with_capacity(snapshot.workers.len());
        for (worker_key, snapshot_worker) in &snapshot.workers {
            let (Some(identity), Some(load)) = (
                snapshot_worker.identity.as_ref(),
                snapshot_worker.load.as_ref(),
            ) else {
                continue;
            };
            if !load.is_fresh(now)
                || identity.role != self.candidate_role
                || context
                    .model_id
                    .as_deref()
                    .is_some_and(|model| model != identity.model_id)
            {
                continue;
            }
            let endpoint_key = (
                normalize_endpoint(&identity.endpoint).to_string(),
                identity.dp_rank,
            );
            let replace = fresh_loads
                .get(&endpoint_key)
                .is_none_or(|(_, _, current)| {
                    (load.value.observed_at_ms, load.value.seq)
                        > (current.value.observed_at_ms, current.value.seq)
                });
            if replace {
                fresh_loads.insert(endpoint_key, (worker_key, snapshot_worker, load));
            }
        }
        let mut candidates = Vec::with_capacity(workers.len());

        for (router_index, worker) in workers.iter().enumerate() {
            let endpoint_key = (
                normalize_endpoint(worker.base_url()).to_string(),
                worker.dp_rank().unwrap_or(0) as u64,
            );
            let Some((worker_key, snapshot_worker, load)) = fresh_loads.get(&endpoint_key).copied()
            else {
                debug!(router_index, "agent-aware state incomplete; using baseline");
                return None;
            };
            let placement = session_key.and_then(|session_key| {
                snapshot
                    .session_placements
                    .get(&format!("{session_key}:{worker_key}"))
                    .filter(|placement| placement.is_fresh(now))
                    .map(|placement| &placement.value)
            });
            let base_load_ms = projected_server_load_ms(&load.value.payload).ok()?
                + unreported_metrics_load_ms(
                    snapshot_worker,
                    &load.value,
                    now,
                    self.fallback_reservation_ms,
                );
            let score = self.scorer.score(context, &load.value, placement).ok()?;
            candidates.push(ScoredCandidate {
                worker_key: worker_key.clone(),
                server_id: load.value.server.server_id.clone(),
                reservation_key: worker_reservation_key(worker.as_ref()),
                base_load_ms,
                score,
            });
        }

        Some(candidates)
    }

    /// Resolve the session's hint to one candidate, or say why it cannot apply.
    fn hint_target(
        &self,
        snapshot: &ClusterSnapshot,
        now: u64,
        session_key: Option<&str>,
        candidates: Option<&Vec<ScoredCandidate>>,
    ) -> Result<HintTarget, RouteHintOutcome> {
        let hint = session_key
            .and_then(|session_key| snapshot.route_hints.hints.get(session_key))
            .ok_or(RouteHintOutcome::NoHint)?;
        if !hint.is_valid(now) {
            return Err(RouteHintOutcome::Expired);
        }
        let candidates = candidates.ok_or(RouteHintOutcome::TargetUnavailable)?;
        let mut matching = candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.server_id == hint.preferred_server)
            .map(|(index, _)| index);
        match (matching.next(), matching.next()) {
            (Some(candidate_index), None) => Ok(HintTarget {
                hint: hint.clone(),
                candidate_index,
            }),
            _ => Err(RouteHintOutcome::TargetUnavailable),
        }
    }

    fn select_with_rollout(
        &self,
        workers: &[Arc<dyn Worker>],
        context: &RoutingContext,
    ) -> Option<usize> {
        if workers.is_empty() {
            return None;
        }
        let baseline_index = self.fallback_cursor.fetch_add(1, Ordering::Relaxed) % workers.len();
        let snapshot = self.state.snapshot();
        let now = self.state.monotonic_ms();
        let session_key = context.session_key(&self.cluster_secret).ok();
        let candidates =
            self.candidates_from_snapshot(&snapshot, now, session_key.as_deref(), workers, context);
        let hint_target = match (&self.route_hint_acker, session_key.as_deref()) {
            (Some(_), Some(session_key)) => {
                Some(self.hint_target(&snapshot, now, Some(session_key), candidates.as_ref()))
            }
            _ => None,
        };
        let mut hint_outcome = None;
        // A Route Hint does not make its target a holder: the target reports
        // the migrated prefix itself as it arrives (placements), and the hint
        // only breaks ties.
        let kv = candidates.as_ref().map(|candidates| kv_holders(candidates));
        let hinted_index = hint_target
            .as_ref()
            .and_then(|target| target.as_ref().ok())
            .map(|target| target.candidate_index);
        let selected = self
            .reservations
            .choose_and_reserve(context.routing_id(), |reservations| {
                let shadow = candidates
                    .as_ref()
                    .zip(kv.as_ref())
                    .and_then(|(candidates, kv)| {
                        select_shadow_candidate(candidates, reservations, kv, hinted_index)
                    });
                let mut shadow_index = shadow;
                if let Some(target) = &hint_target {
                    hint_outcome = Some(match target {
                        Ok(target) if shadow == Some(target.candidate_index) => {
                            shadow_index = Some(target.candidate_index);
                            RouteHintOutcome::Applied
                        }
                        Ok(_) => RouteHintOutcome::TargetImbalanced,
                        Err(outcome) => *outcome,
                    });
                }
                let decision = SelectionDecision::new(
                    baseline_index,
                    shadow_index,
                    self.rollout.includes(context.routing_id()),
                );
                if hint_outcome == Some(RouteHintOutcome::Applied) && !decision.is_enforced() {
                    hint_outcome = Some(RouteHintOutcome::ShadowOnly);
                }
                let selected_candidate = candidates
                    .as_ref()
                    .and_then(|candidates| candidates.get(decision.selected_index));
                let reservation_key = selected_candidate.map_or_else(
                    || worker_reservation_key(workers[decision.selected_index].as_ref()),
                    |candidate| candidate.reservation_key.clone(),
                );
                let reservation_ms = match (selected_candidate, kv.as_ref()) {
                    (Some(_), Some(kv)) => kv.own_work_ms[decision.selected_index],
                    (Some(candidate), None) => candidate.score.estimated_service_ms,
                    (None, _) => self.fallback_reservation_ms,
                };
                Some((decision, reservation_key, reservation_ms))
            });
        match selected {
            Ok(Some(decision)) => {
                if let Some(outcome) = hint_outcome {
                    self.observe_hint(outcome, hint_target.as_ref());
                }
                self.observe_decision(context, decision);
                Some(decision.selected_index)
            }
            Ok(None) => None,
            Err(error) => {
                warn!(error = %error, "agent-aware reservation failed");
                None
            }
        }
    }

    fn observe_hint(
        &self,
        outcome: RouteHintOutcome,
        target: Option<&Result<HintTarget, RouteHintOutcome>>,
    ) {
        self.route_hint_counters.record(outcome);
        RouterMetrics::record_agent_route_hint(outcome.label());
        if outcome != RouteHintOutcome::Applied {
            return;
        }
        if let (Some(acker), Some(Ok(target))) = (&self.route_hint_acker, target) {
            acker.acknowledge(&target.hint, unix_now_ms());
        }
    }

    fn observe_decision(&self, context: &RoutingContext, decision: SelectionDecision) {
        self.rollout_counters.record(&decision);
        RouterMetrics::record_agent_rollout_decision(
            self.rollout.get(),
            decision.action.label(),
            decision.shadow_comparison().label(),
        );
        debug!(
            routing_id = %context.routing_id(),
            rollout_percent = self.rollout.get(),
            rollout_bucket = agent_router::rollout_bucket(context.routing_id()),
            baseline_index = decision.baseline_index,
            shadow_index = ?decision.shadow_index,
            selected_index = decision.selected_index,
            enforced = decision.is_enforced(),
            "agent-aware rollout decision"
        );
    }
}

impl LoadBalancingPolicy for AgentAwarePolicy {
    fn select_worker_with_headers(
        &self,
        workers: &[Arc<dyn Worker>],
        _request_text: Option<&str>,
        _headers: Option<&RequestHeaders>,
    ) -> Option<usize> {
        if workers.is_empty() {
            return None;
        }
        Some(self.fallback_cursor.fetch_add(1, Ordering::Relaxed) % workers.len())
    }

    fn select_worker_with_context(
        &self,
        workers: &[Arc<dyn Worker>],
        context: &RoutingContext,
        _headers: Option<&RequestHeaders>,
    ) -> Option<usize> {
        self.select_with_rollout(workers, context)
    }

    fn on_request_finish(
        &self,
        _worker: &Arc<dyn Worker>,
        context: &RoutingContext,
        _outcome: RequestOutcome,
    ) {
        if !self.reservations.release_by_id(context.routing_id()) {
            warn!("agent-aware request finished without an active reservation");
        }
    }

    fn name(&self) -> &'static str {
        "agent_aware"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

fn normalize_endpoint(endpoint: &str) -> &str {
    endpoint.trim_end_matches('/')
}

/// Which candidates already hold the session's KV, and each candidate's own work.
struct KvHolders {
    /// Candidate indices that reported a compatible placement for the session
    /// (HBM, DRAM or local SSD), including migration targets as KV arrives.
    holders: Vec<usize>,
    /// This request's service estimate per candidate, from the prefix that
    /// candidate actually holds.
    own_work_ms: Vec<f64>,
}

fn kv_holders(candidates: &[ScoredCandidate]) -> KvHolders {
    KvHolders {
        holders: candidates
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.score.holds_kv())
            .map(|(index, _)| index)
            .collect(),
        own_work_ms: candidates
            .iter()
            .map(|candidate| candidate.score.estimated_service_ms)
            .collect(),
    }
}

/// Pick the candidate with the earliest estimated completion: projected queue
/// (reported load plus this Router's in-flight reservations) plus this request's
/// own work there. There are no fixed balance bands; every term is measured.
///
/// A session whose prefix some candidates report holding (HBM, DRAM or local
/// SSD, from their placement reports) chooses only among them; own work
/// reflects how much each holds. The Router never drops a prefix to rebalance:
/// that is AgentRoute's job, which migrates the KV ahead of the next request.
/// The migration target reports the prefix as it arrives, so it joins the
/// holders and wins once it is actually the earliest to finish. Requests whose
/// prefix nobody holds go to the earliest-finishing (least-loaded) candidate.
/// A Route Hint does not make its target a holder; it only breaks ties.
fn select_shadow_candidate(
    candidates: &[ScoredCandidate],
    reservations: &ReservationView<'_>,
    kv: &KvHolders,
    hinted_index: Option<usize>,
) -> Option<usize> {
    let all: Vec<usize> = (0..candidates.len()).collect();
    let pool = if kv.holders.is_empty() {
        &all
    } else {
        &kv.holders
    };
    let queue_ms = |index: usize| {
        candidates[index].base_load_ms
            + reservations.reserved_ms(&candidates[index].reservation_key)
    };
    pool.iter().copied().min_by(|left, right| {
        let completion = |index: usize| queue_ms(index) + kv.own_work_ms[index];
        completion(*left)
            .total_cmp(&completion(*right))
            .then_with(|| (hinted_index != Some(*left)).cmp(&(hinted_index != Some(*right))))
            .then_with(|| queue_ms(*left).total_cmp(&queue_ms(*right)))
            .then_with(|| {
                candidates[*left]
                    .worker_key
                    .cmp(&candidates[*right].worker_key)
            })
    })
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            elapsed.as_millis().try_into().unwrap_or(u64::MAX)
        })
}

fn worker_reservation_key(worker: &dyn Worker) -> String {
    format!("router-worker:{}", worker.url())
}

fn unreported_metrics_load_ms(
    worker: &WorkerSnapshot,
    load: &ServerLoadState,
    now: u64,
    request_load_ms: f64,
) -> f64 {
    let Some(metrics) = worker
        .metrics
        .as_ref()
        .filter(|metrics| metrics.is_fresh(now))
    else {
        return 0.0;
    };
    let extra_running = metrics
        .value
        .running_requests
        .map(|running| running.saturating_sub(load.payload.running_count))
        .unwrap_or_default();
    let extra_waiting = metrics
        .value
        .waiting_requests
        .map(|waiting| waiting.saturating_sub(load.payload.waiting_count))
        .unwrap_or_default();
    extra_running.saturating_add(extra_waiting) as f64 * request_load_ms
}
