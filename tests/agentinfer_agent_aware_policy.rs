use agent_router::{
    BalanceThresholds, ClusterSnapshot, FreshValue, HbmDramScorerConfig, ManualClock, MessageType,
    MetricsSample, MockStateProvider, RequestOutcome, RolloutPercent, RouteHint, RouteHintAck,
    RouteHintAcker, RouteHintState, RoutingContext, ServerIdentity, ServerLoadPayload,
    ServerLoadState, SessionContext, SessionPlacementPayload, SessionPlacementState, StateProvider,
    WorkerRole, WorkerSnapshot,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vllm_router_rs::core::{BasicWorker, Worker, WorkerType};
use vllm_router_rs::policies::{AgentAwarePolicy, LoadBalancingPolicy, RequestLifecycle};

fn identity(server_id: &str, instance_id: &str, endpoint: &str) -> ServerIdentity {
    ServerIdentity {
        cluster_id: "cluster-a".to_string(),
        server_id: server_id.to_string(),
        instance_id: uuid::Uuid::parse_str(instance_id).unwrap(),
        model_id: "model-a".to_string(),
        model_revision: "revision-a".to_string(),
        tokenizer_revision: "tokenizer-a".to_string(),
        role: WorkerRole::Unified,
        dp_rank: 0,
        endpoint: endpoint.to_string(),
    }
}

fn load(server: ServerIdentity, waiting_prefill_tokens: u64) -> ServerLoadState {
    ServerLoadState {
        schema_version: 1,
        message_type: MessageType::ServerLoad,
        seq: 1,
        observed_at_ms: 1_000,
        expires_at_ms: 11_000,
        server,
        payload: ServerLoadPayload {
            waiting_count: u64::from(waiting_prefill_tokens > 0),
            running_count: 0,
            waiting_prefill_tokens,
            waiting_decode_tokens_remaining: 0,
            running_prefill_tokens_remaining: 0,
            running_decode_tokens_remaining: 0,
            prefill_tokens_per_second_ewma: 1_000.0,
            decode_tokens_per_second_ewma: 100.0,
            kv_cache_usage_ratio: 0.1,
            requests: Vec::new(),
            ssd_load_tokens_per_second_ewma: None,
        },
    }
}

fn workers() -> Vec<Arc<dyn Worker>> {
    vec![
        Arc::new(BasicWorker::new(
            "http://worker-a:8000".to_string(),
            WorkerType::Regular,
        )),
        Arc::new(BasicWorker::new(
            "http://worker-b:8000".to_string(),
            WorkerType::Regular,
        )),
    ]
}

fn snapshot_with_loads(worker_a_tokens: u64, worker_b_tokens: u64) -> ClusterSnapshot {
    let server_a = identity(
        "server-a",
        "550e8400-e29b-41d4-a716-446655440000",
        "http://worker-a:8000",
    );
    let server_b = identity(
        "server-b",
        "123e4567-e89b-42d3-a456-426614174000",
        "http://worker-b:8000",
    );
    ClusterSnapshot {
        version: 1,
        published_at_unix_ms: 1_000,
        workers: BTreeMap::from([
            (
                server_a.worker_key().unwrap(),
                WorkerSnapshot {
                    identity: Some(server_a.clone()),
                    load: Some(FreshValue {
                        value: load(server_a, worker_a_tokens),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
            (
                server_b.worker_key().unwrap(),
                WorkerSnapshot {
                    identity: Some(server_b.clone()),
                    load: Some(FreshValue {
                        value: load(server_b, worker_b_tokens),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
        ]),
        session_placements: BTreeMap::new(),
        route_hints: Default::default(),
        source_health: Default::default(),
    }
}

fn context_for_rollout(percent: RolloutPercent, included: bool) -> Arc<RoutingContext> {
    loop {
        let context = Arc::new(
            RoutingContext::new(
                "/v1/chat/completions",
                Some("model-a".to_string()),
                None,
                false,
            )
            .with_prompt_tokens(0)
            .with_decode_tokens(0),
        );
        if percent.includes(context.routing_id()) == included {
            return context;
        }
    }
}

#[derive(Debug)]
struct CountingStateProvider {
    snapshot: Arc<ClusterSnapshot>,
    snapshot_reads: AtomicUsize,
    clock_reads: AtomicUsize,
}

impl StateProvider for CountingStateProvider {
    fn snapshot(&self) -> Arc<ClusterSnapshot> {
        self.snapshot_reads.fetch_add(1, Ordering::SeqCst);
        self.snapshot.clone()
    }

    fn monotonic_ms(&self) -> u64 {
        self.clock_reads.fetch_add(1, Ordering::SeqCst);
        0
    }
}

#[test]
fn agent_policy_keeps_a_session_on_its_kv_holder_and_releases() {
    let secret = b"cluster-secret";
    let clock = Arc::new(ManualClock::new(1_000, 0));
    let provider = Arc::new(MockStateProvider::new(clock.clone()));
    let server_a = identity(
        "server-a",
        "550e8400-e29b-41d4-a716-446655440000",
        "http://worker-a:8000",
    );
    let server_b = identity(
        "server-b",
        "123e4567-e89b-42d3-a456-426614174000",
        "http://worker-b:8000",
    );
    let worker_a_key = server_a.worker_key().unwrap();
    let worker_b_key = server_b.worker_key().unwrap();
    let context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        true,
    )
    .with_prompt_tokens(80)
    .with_decode_tokens(0)
    .with_session(SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(2),
        expected_parent_prefix_digest: Some("c".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    });
    let placement = SessionPlacementState {
        schema_version: 1,
        message_type: MessageType::SessionPlacement,
        seq: 2,
        observed_at_ms: 1_000,
        expires_at_ms: 61_000,
        server: server_a.clone(),
        payload: SessionPlacementPayload {
            session_key: context.session_key(secret).unwrap(),
            session_epoch: 2,
            prefix_digest: "c".repeat(64),
            cached_prefix_tokens: 80,
            block_size_tokens: 16,
            hbm_blocks: 5,
            dram_blocks: 0,
            hbm_cached_tokens: 80,
            dram_cached_tokens: 0,
            prefix_tokens: None,
            ssd_blocks: 0,
            ssd_cached_tokens: 0,
        },
    };
    provider.publish(ClusterSnapshot {
        version: 1,
        published_at_unix_ms: 1_000,
        workers: BTreeMap::from([
            (
                worker_a_key.clone(),
                WorkerSnapshot {
                    identity: Some(server_a),
                    load: Some(FreshValue {
                        value: load(
                            identity(
                                "server-a",
                                "550e8400-e29b-41d4-a716-446655440000",
                                "http://worker-a:8000",
                            ),
                            // 50 ms queued: less than the 80 ms another worker
                            // would spend recomputing the cached 80 tokens.
                            50,
                        ),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
            (
                worker_b_key,
                WorkerSnapshot {
                    identity: Some(server_b.clone()),
                    load: Some(FreshValue {
                        value: load(server_b, 0),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
        ]),
        session_placements: BTreeMap::from([(
            format!("{}:{worker_a_key}", context.session_key(secret).unwrap()),
            FreshValue {
                value: placement,
                ingested_at_monotonic_ms: 0,
                fresh_until_monotonic_ms: 15_000,
            },
        )]),
        route_hints: Default::default(),
        source_health: Default::default(),
    });

    let policy = Arc::new(
        AgentAwarePolicy::with_state_provider(
            provider.clone() as Arc<dyn StateProvider>,
            secret,
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
        )
        .unwrap(),
    );
    let workers: Vec<Arc<dyn Worker>> = vec![
        Arc::new(BasicWorker::new(
            "http://worker-a:8000".to_string(),
            WorkerType::Regular,
        )),
        Arc::new(BasicWorker::new(
            "http://worker-b:8000".to_string(),
            WorkerType::Regular,
        )),
    ];

    assert_eq!(
        policy.select_worker_with_context(&workers, &context, None),
        Some(0)
    );
    assert_eq!(policy.reservations().active_reservations(), 1);

    // Load accounting is checked with a request that has no KV anywhere.
    let metrics_context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        true,
    )
    .with_prompt_tokens(80)
    .with_decode_tokens(0);
    let mut lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[0].clone(),
        Arc::new(context),
    );
    lifecycle.finish(RequestOutcome::Succeeded);
    assert_eq!(policy.reservations().active_reservations(), 0);

    let mut metrics_snapshot = (*provider.snapshot()).clone();
    metrics_snapshot
        .workers
        .get_mut(&worker_a_key)
        .unwrap()
        .metrics = Some(FreshValue {
        value: MetricsSample {
            running_requests: Some(0),
            waiting_requests: Some(5),
            ..MetricsSample::default()
        },
        ingested_at_monotonic_ms: 0,
        fresh_until_monotonic_ms: 3_000,
    });
    provider.publish(metrics_snapshot);

    assert_eq!(
        policy.select_worker_with_context(&workers, &metrics_context, None),
        Some(1)
    );
    let mut lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[1].clone(),
        Arc::new(metrics_context),
    );
    lifecycle.finish(RequestOutcome::Succeeded);
    assert_eq!(policy.reservations().active_reservations(), 0);
}

#[test]
fn stale_state_falls_back_without_io_and_cancel_releases_reservation() {
    let clock = Arc::new(ManualClock::new(1_000, 0));
    let provider = Arc::new(MockStateProvider::new(clock.clone()));
    let policy = Arc::new(
        AgentAwarePolicy::with_state_provider(
            provider as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
        )
        .unwrap(),
    );
    let workers: Vec<Arc<dyn Worker>> = vec![Arc::new(BasicWorker::new(
        "http://worker-a:8000".to_string(),
        WorkerType::Regular,
    ))];
    let context = Arc::new(RoutingContext::default());

    clock.advance(Duration::from_secs(4));
    assert_eq!(
        policy.select_worker_with_context(&workers, &context, None),
        Some(0)
    );
    assert_eq!(policy.reservations().active_reservations(), 1);
    assert_eq!(policy.rollout_stats().baseline_attempts, 1);
    assert_eq!(policy.rollout_stats().state_fallback_attempts, 1);

    let lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[0].clone(),
        context,
    );
    drop(lifecycle);
    assert_eq!(policy.reservations().active_reservations(), 0);
}

#[test]
fn fallback_reservation_remains_visible_after_state_recovers() {
    let clock = Arc::new(ManualClock::new(1_000, 0));
    let provider = Arc::new(MockStateProvider::new(clock));
    let policy = Arc::new(
        AgentAwarePolicy::with_state_provider(
            provider.clone() as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
        )
        .unwrap(),
    );
    let workers: Vec<Arc<dyn Worker>> = vec![
        Arc::new(BasicWorker::new(
            "http://worker-a:8000".to_string(),
            WorkerType::Regular,
        )),
        Arc::new(BasicWorker::new(
            "http://worker-b:8000".to_string(),
            WorkerType::Regular,
        )),
    ];
    let fallback_context = Arc::new(
        RoutingContext::new(
            "/v1/chat/completions",
            Some("model-a".to_string()),
            None,
            false,
        )
        .with_prompt_tokens(0)
        .with_decode_tokens(0),
    );

    assert_eq!(
        policy.select_worker_with_context(&workers, &fallback_context, None),
        Some(0)
    );

    let server_a = identity(
        "server-a",
        "550e8400-e29b-41d4-a716-446655440000",
        "http://worker-a:8000",
    );
    let server_b = identity(
        "server-b",
        "123e4567-e89b-42d3-a456-426614174000",
        "http://worker-b:8000",
    );
    provider.publish(ClusterSnapshot {
        version: 1,
        published_at_unix_ms: 1_000,
        workers: BTreeMap::from([
            (
                server_a.worker_key().unwrap(),
                WorkerSnapshot {
                    identity: Some(server_a.clone()),
                    load: Some(FreshValue {
                        value: load(server_a, 0),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
            (
                server_b.worker_key().unwrap(),
                WorkerSnapshot {
                    identity: Some(server_b.clone()),
                    load: Some(FreshValue {
                        value: load(server_b, 50),
                        ingested_at_monotonic_ms: 0,
                        fresh_until_monotonic_ms: 3_000,
                    }),
                    metrics: None,
                },
            ),
        ]),
        session_placements: BTreeMap::new(),
        route_hints: Default::default(),
        source_health: Default::default(),
    });
    let fresh_context = Arc::new(
        RoutingContext::new(
            "/v1/chat/completions",
            Some("model-a".to_string()),
            None,
            false,
        )
        .with_prompt_tokens(0)
        .with_decode_tokens(0),
    );

    assert_eq!(
        policy.select_worker_with_context(&workers, &fresh_context, None),
        Some(1)
    );
    assert_eq!(policy.reservations().active_reservations(), 2);

    let mut fallback_lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[0].clone(),
        fallback_context,
    );
    fallback_lifecycle.finish(RequestOutcome::Succeeded);
    let mut fresh_lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[1].clone(),
        fresh_context,
    );
    fresh_lifecycle.finish(RequestOutcome::Succeeded);
    assert_eq!(policy.reservations().active_reservations(), 0);
}

#[test]
fn shadow_and_rollout_reserve_only_the_worker_that_will_receive_the_request() {
    let provider = Arc::new(MockStateProvider::new(Arc::new(ManualClock::new(1_000, 0))));
    provider.publish(snapshot_with_loads(100, 0));
    let workers = workers();

    let shadow = Arc::new(
        AgentAwarePolicy::with_state_provider_and_rollout(
            provider.clone() as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
            RolloutPercent::new(0).unwrap(),
        )
        .unwrap(),
    );
    let shadow_context = context_for_rollout(RolloutPercent::new(0).unwrap(), false);
    assert_eq!(
        shadow.select_worker_with_context(&workers, &shadow_context, None),
        Some(0)
    );
    assert_eq!(
        shadow
            .reservations()
            .reserved_ms("router-worker:http://worker-a:8000"),
        1.0
    );
    assert_eq!(
        shadow
            .reservations()
            .reserved_ms("router-worker:http://worker-b:8000"),
        0.0
    );
    assert_eq!(
        shadow.rollout_stats(),
        vllm_router_rs::policies::AgentAwareRolloutStats {
            shadow_evaluated_attempts: 1,
            shadow_agreement_attempts: 0,
            shadow_disagreement_attempts: 1,
            enforced_attempts: 0,
            baseline_attempts: 1,
            state_fallback_attempts: 0,
        }
    );
    let mut lifecycle = RequestLifecycle::start(
        shadow.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[0].clone(),
        shadow_context,
    );
    lifecycle.finish(RequestOutcome::Succeeded);

    let enforced = Arc::new(
        AgentAwarePolicy::with_state_provider_and_rollout(
            provider as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
            RolloutPercent::new(100).unwrap(),
        )
        .unwrap(),
    );
    let enforced_context = context_for_rollout(RolloutPercent::new(100).unwrap(), true);
    assert_eq!(
        enforced.select_worker_with_context(&workers, &enforced_context, None),
        Some(1)
    );
    assert_eq!(enforced.rollout_stats().enforced_attempts, 1);
    assert_eq!(enforced.rollout_stats().baseline_attempts, 0);
    let mut lifecycle = RequestLifecycle::start(
        enforced.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[1].clone(),
        enforced_context,
    );
    lifecycle.finish(RequestOutcome::Succeeded);
}

#[test]
fn shadow_stats_record_agreement_with_the_baseline() {
    let provider = Arc::new(MockStateProvider::new(Arc::new(ManualClock::new(1_000, 0))));
    provider.publish(snapshot_with_loads(0, 100));
    let workers = workers();
    let policy = Arc::new(
        AgentAwarePolicy::with_state_provider_and_rollout(
            provider as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
            RolloutPercent::new(0).unwrap(),
        )
        .unwrap(),
    );
    let context = context_for_rollout(RolloutPercent::new(0).unwrap(), false);

    assert_eq!(
        policy.select_worker_with_context(&workers, &context, None),
        Some(0)
    );
    assert_eq!(
        policy.rollout_stats(),
        vllm_router_rs::policies::AgentAwareRolloutStats {
            shadow_evaluated_attempts: 1,
            shadow_agreement_attempts: 1,
            shadow_disagreement_attempts: 0,
            enforced_attempts: 0,
            baseline_attempts: 1,
            state_fallback_attempts: 0,
        }
    );
    let mut lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[0].clone(),
        context,
    );
    lifecycle.finish(RequestOutcome::Succeeded);
}

#[test]
fn partial_rollout_uses_the_stable_routing_bucket() {
    let provider = Arc::new(MockStateProvider::new(Arc::new(ManualClock::new(1_000, 0))));
    provider.publish(snapshot_with_loads(100, 0));
    let workers = workers();
    let rollout = RolloutPercent::new(25).unwrap();

    for (included, expected_worker) in [(true, 1), (false, 0)] {
        let policy = Arc::new(
            AgentAwarePolicy::with_state_provider_and_rollout(
                provider.clone() as Arc<dyn StateProvider>,
                b"cluster-secret",
                BalanceThresholds::default(),
                HbmDramScorerConfig::default(),
                rollout,
            )
            .unwrap(),
        );
        let context = context_for_rollout(rollout, included);
        assert_eq!(
            policy.select_worker_with_context(&workers, &context, None),
            Some(expected_worker)
        );
        assert_eq!(
            policy.rollout_stats().enforced_attempts,
            usize::from(included) as u64
        );
        let mut lifecycle = RequestLifecycle::start(
            policy.clone() as Arc<dyn LoadBalancingPolicy>,
            workers[expected_worker].clone(),
            context,
        );
        lifecycle.finish(RequestOutcome::Succeeded);
    }
}

#[test]
fn worker_selection_reads_one_local_snapshot_without_runtime_io() {
    let provider = Arc::new(CountingStateProvider {
        snapshot: Arc::new(snapshot_with_loads(100, 0)),
        snapshot_reads: AtomicUsize::new(0),
        clock_reads: AtomicUsize::new(0),
    });
    let policy = Arc::new(
        AgentAwarePolicy::with_state_provider_and_rollout(
            provider.clone() as Arc<dyn StateProvider>,
            b"cluster-secret",
            BalanceThresholds::default(),
            HbmDramScorerConfig::default(),
            RolloutPercent::new(100).unwrap(),
        )
        .unwrap(),
    );
    let workers = workers();
    let context = context_for_rollout(RolloutPercent::new(100).unwrap(), true);

    assert_eq!(
        policy.select_worker_with_context(&workers, &context, None),
        Some(1)
    );
    assert_eq!(provider.snapshot_reads.load(Ordering::SeqCst), 1);
    assert_eq!(provider.clock_reads.load(Ordering::SeqCst), 1);

    let mut lifecycle = RequestLifecycle::start(
        policy.clone() as Arc<dyn LoadBalancingPolicy>,
        workers[1].clone(),
        context,
    );
    lifecycle.finish(RequestOutcome::Succeeded);
}

const HINT_SECRET: &[u8] = b"cluster-secret";

fn session_context(percent: RolloutPercent, included: bool) -> Arc<RoutingContext> {
    loop {
        let context = Arc::new(
            RoutingContext::new(
                "/v1/chat/completions",
                Some("model-a".to_string()),
                None,
                false,
            )
            .with_prompt_tokens(64)
            .with_decode_tokens(0)
            .with_session(SessionContext {
                tenant_id: "tenant-a".to_string(),
                session_id: "session-hint".to_string(),
                session_epoch: Some(1),
                expected_parent_prefix_digest: None,
                model_revision: Some("revision-a".to_string()),
                tokenizer_revision: Some("tokenizer-a".to_string()),
            }),
        );
        if percent.includes(context.routing_id()) == included {
            return context;
        }
    }
}

fn with_hint(
    mut snapshot: ClusterSnapshot,
    context: &RoutingContext,
    preferred_server: &str,
    valid_until_monotonic_ms: u64,
) -> ClusterSnapshot {
    let session_key = context.session_key(HINT_SECRET).unwrap();
    snapshot.route_hints.document_version = 9;
    snapshot.route_hints.hints.insert(
        session_key.clone(),
        RouteHint {
            producer_id: "apc-test".to_string(),
            session_key,
            source_server: Some("server-a".to_string()),
            preferred_server: preferred_server.to_string(),
            plan_id: "plan-1".to_string(),
            state: RouteHintState::Ready,
            state_version: 7,
            valid_until_monotonic_ms,
        },
    );
    snapshot
}

struct HintHarness {
    policy: AgentAwarePolicy,
    acks: tokio::sync::mpsc::Receiver<RouteHintAck>,
}

fn hint_policy(
    snapshot: ClusterSnapshot,
    rollout: RolloutPercent,
    enable_hints: bool,
) -> HintHarness {
    let provider = Arc::new(MockStateProvider::new(Arc::new(ManualClock::new(1_000, 0))));
    provider.publish(snapshot);
    let mut policy = AgentAwarePolicy::with_state_provider_and_rollout(
        provider,
        HINT_SECRET,
        BalanceThresholds::new(250.0, 1.2).unwrap(),
        HbmDramScorerConfig::default(),
        rollout,
    )
    .unwrap();
    let (acker, acks) = RouteHintAcker::new("cluster-a", "router-test", 8);
    if enable_hints {
        policy = policy.with_route_hint_acker(Arc::new(acker));
    }
    HintHarness { policy, acks }
}

const SERVERS: [&str; 2] = ["server-a", "server-b"];

/// Index the policy picks without any hint for the same loads.
fn unhinted_choice(full: RolloutPercent, context: &Arc<RoutingContext>) -> usize {
    let harness = hint_policy(snapshot_with_loads(0, 0), full, true);
    route(&harness.policy, context)
}

fn route(policy: &AgentAwarePolicy, context: &Arc<RoutingContext>) -> usize {
    let workers = workers();
    let selected = policy
        .select_worker_with_context(&workers, context, None)
        .unwrap();
    policy.on_request_finish(&workers[selected], context, RequestOutcome::Succeeded);
    selected
}

#[test]
fn valid_hint_target_holding_migrated_kv_is_applied_and_acks_once() {
    let full = RolloutPercent::new(100).unwrap();
    let context = session_context(full, true);
    let hinted = 1 - unhinted_choice(full, &context);
    let snapshot = with_hint(snapshot_with_loads(0, 0), &context, SERVERS[hinted], 5_000);
    let mut harness = hint_policy(snapshot, full, true);

    assert_eq!(route(&harness.policy, &context), hinted);
    assert_eq!(route(&harness.policy, &session_context(full, true)), hinted);

    assert_eq!(harness.policy.route_hint_stats().applied, 2);
    let ack = harness.acks.try_recv().unwrap();
    assert_eq!(
        (ack.plan_id.as_str(), ack.state_version, ack.session_key),
        ("plan-1", 7, context.session_key(HINT_SECRET).unwrap())
    );
    assert!(harness.acks.try_recv().is_err(), "one ACK per hint version");
}

#[test]
fn imbalanced_hint_target_is_not_used() {
    let full = RolloutPercent::new(100).unwrap();
    let context = session_context(full, true);
    let snapshot = with_hint(snapshot_with_loads(0, 50_000), &context, "server-b", 5_000);
    let mut harness = hint_policy(snapshot, full, true);

    assert_eq!(route(&harness.policy, &context), 0);
    assert_eq!(harness.policy.route_hint_stats().target_imbalanced, 1);
    assert!(harness.acks.try_recv().is_err());
}

#[test]
fn expired_or_unroutable_hints_fall_through_to_normal_scoring() {
    let full = RolloutPercent::new(100).unwrap();
    let context = session_context(full, true);
    let expired = with_hint(snapshot_with_loads(0, 0), &context, "server-b", 0);
    let mut expired = hint_policy(expired, full, true);
    let unknown = with_hint(snapshot_with_loads(0, 0), &context, "server-z", 5_000);
    let mut unknown = hint_policy(unknown, full, true);

    route(&expired.policy, &context);
    route(&unknown.policy, &session_context(full, true));

    assert_eq!(expired.policy.route_hint_stats().expired, 1);
    assert_eq!(unknown.policy.route_hint_stats().target_unavailable, 1);
    assert!(expired.acks.try_recv().is_err());
    assert!(unknown.acks.try_recv().is_err());
}

#[test]
fn shadow_only_rollout_never_acknowledges_a_hint() {
    let shadow = RolloutPercent::new(0).unwrap();
    let context = session_context(shadow, false);
    let snapshot = with_hint(snapshot_with_loads(0, 0), &context, "server-b", 5_000);
    let mut harness = hint_policy(snapshot, shadow, true);

    route(&harness.policy, &context);

    assert_eq!(harness.policy.route_hint_stats().shadow_only, 1);
    assert!(harness.acks.try_recv().is_err());
}

#[test]
fn hints_are_ignored_unless_enabled() {
    let full = RolloutPercent::new(100).unwrap();
    let context = session_context(full, true);
    let unhinted = unhinted_choice(full, &context);
    let snapshot = with_hint(
        snapshot_with_loads(0, 0),
        &context,
        SERVERS[1 - unhinted],
        5_000,
    );
    let harness = hint_policy(snapshot, full, false);

    assert_eq!(route(&harness.policy, &context), unhinted);
    assert_eq!(harness.policy.route_hint_stats(), Default::default());
}

fn as_prefill(mut snapshot: ClusterSnapshot) -> ClusterSnapshot {
    for worker in snapshot.workers.values_mut() {
        if let Some(identity) = worker.identity.as_mut() {
            identity.role = WorkerRole::Prefill;
        }
        if let Some(load) = worker.load.as_mut() {
            load.value.server.role = WorkerRole::Prefill;
        }
    }
    snapshot
}

#[test]
fn prefill_role_policy_scores_prefill_workers_and_applies_hints() {
    let full = RolloutPercent::new(100).unwrap();
    let context = session_context(full, true);
    let unhinted = unhinted_choice(full, &context);
    let snapshot = as_prefill(with_hint(
        snapshot_with_loads(0, 0),
        &context,
        SERVERS[1 - unhinted],
        5_000,
    ));
    let mut harness = hint_policy(snapshot.clone(), full, true);
    harness.policy = harness.policy.with_candidate_role(WorkerRole::Prefill);

    assert_eq!(route(&harness.policy, &context), 1 - unhinted);
    assert_eq!(harness.policy.route_hint_stats().applied, 1);
    assert!(harness.acks.try_recv().is_ok());

    // A Unified-role policy sees no candidates among Prefill workers.
    let unified = hint_policy(snapshot, full, true);
    route(&unified.policy, &context);
    assert_eq!(unified.policy.route_hint_stats().target_unavailable, 1);
}

fn placement_on(
    server: ServerIdentity,
    context: &RoutingContext,
    cached_tokens: u64,
) -> FreshValue<SessionPlacementState> {
    FreshValue {
        value: SessionPlacementState {
            schema_version: 1,
            message_type: MessageType::SessionPlacement,
            seq: 2,
            observed_at_ms: 1_000,
            expires_at_ms: 61_000,
            server,
            payload: SessionPlacementPayload {
                session_key: context.session_key(HINT_SECRET).unwrap(),
                session_epoch: 1,
                prefix_digest: "d".repeat(64),
                cached_prefix_tokens: cached_tokens,
                block_size_tokens: 16,
                hbm_blocks: cached_tokens / 16,
                dram_blocks: 0,
                hbm_cached_tokens: cached_tokens,
                dram_cached_tokens: 0,
                prefix_tokens: None,
                ssd_blocks: 0,
                ssd_cached_tokens: 0,
            },
        },
        ingested_at_monotonic_ms: 0,
        fresh_until_monotonic_ms: 15_000,
    }
}

fn kv_session_context() -> Arc<RoutingContext> {
    let full = RolloutPercent::new(100).unwrap();
    loop {
        let context = Arc::new(
            RoutingContext::new(
                "/v1/chat/completions",
                Some("model-a".to_string()),
                None,
                false,
            )
            .with_prompt_tokens(800)
            .with_decode_tokens(0)
            .with_session(SessionContext {
                tenant_id: "tenant-a".to_string(),
                session_id: "session-kv".to_string(),
                session_epoch: Some(1),
                expected_parent_prefix_digest: Some("d".repeat(64)),
                model_revision: Some("revision-a".to_string()),
                tokenizer_revision: Some("tokenizer-a".to_string()),
            }),
        );
        if full.includes(context.routing_id()) {
            return context;
        }
    }
}

fn cached_kv_session(snapshot: &mut ClusterSnapshot, context: &RoutingContext) -> String {
    let (key_a, server_a) = snapshot
        .workers
        .iter()
        .find(|(_, worker)| worker.identity.as_ref().unwrap().server_id == "server-a")
        .map(|(key, worker)| (key.clone(), worker.identity.clone().unwrap()))
        .unwrap();
    snapshot.session_placements.insert(
        format!("{}:{key_a}", context.session_key(HINT_SECRET).unwrap()),
        placement_on(server_a, context, 768),
    );
    key_a
}

#[test]
fn a_kv_holder_wins_unless_another_queue_is_shorter_by_more_than_the_recompute() {
    // Worker A holds 768 of 800 prompt tokens; at 1000 tok/s, moving to idle
    // worker B costs 768 ms of extra prefill.
    let full = RolloutPercent::new(100).unwrap();
    let context = kv_session_context();

    // A: 500 ms queued + 32 ms own work beats B's 800 ms.
    let mut short = snapshot_with_loads(500, 0);
    cached_kv_session(&mut short, &context);
    assert_eq!(route(&hint_policy(short, full, true).policy, &context), 0);

    // A: 2000 ms queued + 32 ms; B finishes first even recomputing.
    let mut long = snapshot_with_loads(2_000, 0);
    cached_kv_session(&mut long, &context);
    assert_eq!(route(&hint_policy(long, full, true).policy, &context), 1);
}

#[test]
fn request_without_kv_goes_to_the_least_loaded_worker_without_thresholds() {
    let full = RolloutPercent::new(100).unwrap();
    let context = kv_session_context();
    // 100 ms of queued work on A is inside the old 250 ms "balanced" band, which
    // used to make A and B equivalent; the projected load alone decides now.
    let toward_b = hint_policy(snapshot_with_loads(100, 0), full, true);
    assert_eq!(route(&toward_b.policy, &context), 1);
    let toward_a = hint_policy(snapshot_with_loads(0, 100), full, true);
    assert_eq!(route(&toward_a.policy, &context), 0);
}

#[test]
fn ssd_held_kv_counts_as_its_restore_time_at_the_measured_rate() {
    // Worker A's HBM no longer holds the session but its local SSD tier holds
    // 768 tokens, restored at a measured 10k tok/s (77 ms) instead of 768 ms
    // of recompute.
    let full = RolloutPercent::new(100).unwrap();
    let context = kv_session_context();
    let route_with_queue = |queued_tokens: u64| {
        let mut snapshot = snapshot_with_loads(queued_tokens, 0);
        let (key_a, server_a) = snapshot
            .workers
            .iter()
            .find(|(_, worker)| worker.identity.as_ref().unwrap().server_id == "server-a")
            .map(|(key, worker)| (key.clone(), worker.identity.clone().unwrap()))
            .unwrap();
        let worker_a = snapshot.workers.get_mut(&key_a).unwrap();
        worker_a
            .load
            .as_mut()
            .unwrap()
            .value
            .payload
            .ssd_load_tokens_per_second_ewma = Some(10_000.0);
        let mut placement = placement_on(server_a, &context, 768);
        placement.value.payload.hbm_blocks = 0;
        placement.value.payload.hbm_cached_tokens = 0;
        placement.value.payload.ssd_blocks = 48;
        placement.value.payload.ssd_cached_tokens = 768;
        placement.value.payload.prefix_tokens = Some(800);
        snapshot.session_placements.insert(
            format!("{}:{key_a}", context.session_key(HINT_SECRET).unwrap()),
            placement,
        );
        route(&hint_policy(snapshot, full, true).policy, &context)
    };

    // A: 500 ms queued + ~109 ms beats B's 800 ms recompute.
    assert_eq!(route_with_queue(500), 0);
    // A: 2000 ms queued; B is faster despite recomputing.
    assert_eq!(route_with_queue(2_000), 1);
}
