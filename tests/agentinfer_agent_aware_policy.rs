use agent_router::{
    BalanceThresholds, ClusterSnapshot, FreshValue, HbmDramScorerConfig, ManualClock, MessageType,
    MetricsSample, MockStateProvider, RequestOutcome, RolloutPercent, RoutingContext,
    ServerIdentity, ServerLoadPayload, ServerLoadState, SessionContext, SessionPlacementPayload,
    SessionPlacementState, StateProvider, WorkerRole, WorkerSnapshot,
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
fn agent_policy_selects_fastest_worker_inside_balanced_subset_and_releases() {
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
                            100,
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

    let metrics_context = context.clone();
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
