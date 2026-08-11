mod support;

use agent_router::{
    maximal_balanced_subset, projected_server_load_ms, BalanceThresholds, HbmDramScorer,
    HbmDramScorerConfig, IngestMode, RequestScorer, ReservationError, ReservationLedger,
    RoutingContext, ServerLoadState, SessionContext, SessionPlacementState, SnapshotStore,
    StateProvider,
};
use std::sync::{Arc, Barrier};

fn load_state() -> ServerLoadState {
    serde_json::from_str(&support::server_load_json(1, 1_000_000, 1_010_000)).unwrap()
}

fn placement_state() -> SessionPlacementState {
    serde_json::from_str(&support::session_placement_json(2, 1_000_000, 1_060_000)).unwrap()
}

#[test]
fn maximal_balanced_subset_uses_and_semantics_and_includes_equal_boundaries() {
    let thresholds = BalanceThresholds::new(250.0, 1.2).unwrap();

    assert_eq!(
        maximal_balanced_subset(&[350.0, 100.0, 351.0, 120.0], thresholds).unwrap(),
        vec![1, 3, 0]
    );
    assert_eq!(
        maximal_balanced_subset(&[251.0, 0.0, 250.0], thresholds).unwrap(),
        vec![1, 2]
    );
    assert!(maximal_balanced_subset(&[], thresholds).unwrap().is_empty());
}

#[test]
fn maximal_balanced_subset_satisfies_cutoff_and_order_properties() {
    for minimum in [0.0, 1.0, 100.0, 1_000.0] {
        for absolute_ms in [0.0, 50.0, 250.0] {
            for relative in [1.0, 1.2, 2.0] {
                let thresholds = BalanceThresholds::new(absolute_ms, relative).unwrap();
                let loads = [
                    minimum + 500.0,
                    minimum,
                    minimum + 50.0,
                    minimum + 250.0,
                    minimum + 1.0,
                ];
                let selected = maximal_balanced_subset(&loads, thresholds).unwrap();
                let cutoff = (minimum + absolute_ms).max(minimum * relative);

                assert!(selected.contains(&1));
                assert!(selected.windows(2).all(|pair| {
                    loads[pair[0]] < loads[pair[1]]
                        || (loads[pair[0]] == loads[pair[1]] && pair[0] < pair[1])
                }));
                for (index, load) in loads.iter().enumerate() {
                    assert_eq!(selected.contains(&index), *load <= cutoff);
                }
            }
        }
    }
}

#[test]
fn projected_load_is_token_work_converted_to_milliseconds() {
    let load = load_state();

    assert_eq!(projected_server_load_ms(&load.payload).unwrap(), 984.0);
    let invalid_rate = agent_router::ServerLoadPayload {
        prefill_tokens_per_second_ewma: 0.0,
        ..load.payload.clone()
    };
    assert!(projected_server_load_ms(&invalid_rate).is_err());
    let overflowing_projection = agent_router::ServerLoadPayload {
        prefill_tokens_per_second_ewma: f64::MIN_POSITIVE,
        ..load.payload
    };
    assert!(projected_server_load_ms(&overflowing_projection).is_err());
}

#[test]
fn hbm_dram_scorer_uses_only_exact_compatible_placement() {
    let secret = b"cluster-secret";
    let mut placement = placement_state();
    let load = load_state();
    let session = SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(2),
        expected_parent_prefix_digest: Some("c".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    };
    let context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        false,
    )
    .with_prompt_tokens(80)
    .with_decode_tokens(20)
    .with_session(session.clone());
    placement.payload.session_key = context.session_key(secret).unwrap();

    let scorer = HbmDramScorer::new(
        secret,
        HbmDramScorerConfig {
            default_prefill_tokens: 512,
            default_decode_tokens: 128,
            dram_block_restore_ms: 2.0,
            minimum_reservation_ms: 1.0,
        },
    )
    .unwrap();

    let score = scorer.score(&context, &load, Some(&placement)).unwrap();
    assert_eq!(score.hbm_reused_tokens, 32);
    assert_eq!(score.dram_reused_tokens, 16);
    assert_eq!(score.hbm_saved_ms, 32.0);
    assert_eq!(score.dram_net_saved_ms, 14.0);
    assert_eq!(score.estimated_service_ms, 234.0);

    let mismatched = context.with_session(SessionContext {
        session_epoch: Some(3),
        ..session
    });
    let score = scorer.score(&mismatched, &load, Some(&placement)).unwrap();
    assert_eq!(score.hbm_reused_tokens, 0);
    assert_eq!(score.dram_reused_tokens, 0);
    assert_eq!(score.estimated_service_ms, 280.0);
}

#[test]
fn compatible_prefix_sets_a_lower_bound_when_prompt_length_is_unknown() {
    let secret = b"cluster-secret";
    let mut placement = placement_state();
    let load = load_state();
    let context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        false,
    )
    .with_decode_tokens(0)
    .with_session(SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(2),
        expected_parent_prefix_digest: Some("c".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    });
    placement.payload.session_key = context.session_key(secret).unwrap();
    let scorer = HbmDramScorer::new(
        secret,
        HbmDramScorerConfig {
            default_prefill_tokens: 16,
            default_decode_tokens: 0,
            dram_block_restore_ms: 2.0,
            minimum_reservation_ms: 1.0,
        },
    )
    .unwrap();

    let score = scorer.score(&context, &load, Some(&placement)).unwrap();
    assert_eq!(score.hbm_reused_tokens, 32);
    assert_eq!(score.dram_reused_tokens, 16);
    assert_eq!(score.estimated_service_ms, 2.0);
}

#[test]
fn scorer_rejects_zero_block_size_instead_of_panicking() {
    let secret = b"cluster-secret";
    let load = load_state();
    let mut placement = placement_state();
    let context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        false,
    )
    .with_prompt_tokens(80)
    .with_session(SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(2),
        expected_parent_prefix_digest: Some("c".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    });
    placement.payload.session_key = context.session_key(secret).unwrap();
    placement.payload.block_size_tokens = 0;
    let scorer = HbmDramScorer::new(secret, HbmDramScorerConfig::default()).unwrap();

    assert!(scorer.score(&context, &load, Some(&placement)).is_err());
}

#[test]
fn stale_placement_can_be_rejected_before_scoring_from_one_snapshot() {
    let store = Arc::new(SnapshotStore::default());
    let load = load_state();
    let placement = placement_state();
    store
        .apply(
            agent_router::StateMessage::ServerLoad(load),
            IngestMode::Live,
        )
        .unwrap();
    store
        .apply(
            agent_router::StateMessage::SessionPlacement(placement),
            IngestMode::Live,
        )
        .unwrap();

    let snapshot = store.snapshot();
    assert_eq!(snapshot.session_placements.len(), 1);
    assert!(snapshot
        .session_placements
        .values()
        .all(|entry| entry.is_fresh(store.monotonic_ms())));
}

#[test]
fn reservation_selection_is_atomic_and_release_is_exact() {
    let ledger = ReservationLedger::default();
    let first = RoutingContext::default().routing_id();
    let second = RoutingContext::default().routing_id();

    let selected = ledger
        .choose_and_reserve(first, |reservations| {
            assert_eq!(reservations.reserved_ms("worker-a"), 0.0);
            Some(("worker-a", "worker-a".to_string(), 100.0))
        })
        .unwrap();
    assert_eq!(selected, Some("worker-a"));

    let selected = ledger
        .choose_and_reserve(second, |reservations| {
            assert_eq!(reservations.reserved_ms("worker-a"), 100.0);
            Some(("worker-b", "worker-b".to_string(), 80.0))
        })
        .unwrap();
    assert_eq!(selected, Some("worker-b"));
    assert_eq!(ledger.active_reservations(), 2);

    assert!(!ledger.release(first, "worker-b"));
    assert_eq!(ledger.reserved_ms("worker-a"), 100.0);
    assert!(ledger.release(first, "worker-a"));
    assert_eq!(ledger.reserved_ms("worker-a"), 0.0);

    let duplicate =
        ledger.choose_and_reserve(second, |_| Some(("worker-a", "worker-a".to_string(), 50.0)));
    assert!(matches!(
        duplicate,
        Err(ReservationError::DuplicateReservation(id)) if id == second
    ));
    assert!(ledger.release(second, "worker-b"));
    assert_eq!(ledger.active_reservations(), 0);

    let fractional = RoutingContext::default().routing_id();
    ledger
        .choose_and_reserve(fractional, |_| Some(((), "worker-a".to_string(), 0.0001)))
        .unwrap();
    assert_eq!(ledger.reserved_ms("worker-a"), 0.001);
    assert!(ledger.release_by_id(fractional));
    assert_eq!(ledger.reserved_ms("worker-a"), 0.0);
}

#[test]
fn concurrent_reservations_observe_prior_writes_and_stay_balanced() {
    const REQUESTS: usize = 32;
    let ledger = Arc::new(ReservationLedger::default());
    let barrier = Arc::new(Barrier::new(REQUESTS));
    let mut handles = Vec::new();

    for _ in 0..REQUESTS {
        let ledger = ledger.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let reservation_id = RoutingContext::default().routing_id();
            barrier.wait();
            let worker = ledger
                .choose_and_reserve(reservation_id, |reservations| {
                    let worker = if reservations.reserved_ms("worker-a")
                        <= reservations.reserved_ms("worker-b")
                    {
                        "worker-a"
                    } else {
                        "worker-b"
                    };
                    Some((worker, worker.to_string(), 10.0))
                })
                .unwrap()
                .unwrap();
            (reservation_id, worker)
        }));
    }

    let completed: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(ledger.active_reservations(), REQUESTS);
    assert_eq!(ledger.reserved_ms("worker-a"), 160.0);
    assert_eq!(ledger.reserved_ms("worker-b"), 160.0);
    assert_eq!(
        completed
            .iter()
            .filter(|(_, worker)| *worker == "worker-a")
            .count(),
        REQUESTS / 2
    );

    for (reservation_id, worker) in completed {
        assert!(ledger.release(reservation_id, worker));
    }
    assert_eq!(ledger.active_reservations(), 0);
}
