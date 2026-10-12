# Agent Router

`agent_router` contains AgentInfer-owned routing contracts and the Rust state
runtime used directly by this Router fork. Keeping it as an in-repository crate
lets the fork build independently while preserving a focused extension boundary.

The versioned contracts live in [`spec/`](spec/):

- `policy-v1.json` contains identity, freshness, Redis key, and load-balance
  semantics.
- `redis-state-v1.schema.json` describes the two Redis state message types.
- `protocol-v1.md` defines semantics that JSON Schema cannot express.
- `fixtures/` contains examples shared by producers, the Router, and tests.

Validate the complete contract with:

```bash
python3 scripts/check_agent_router_contract.py
```

The Rust crate provides:

- `RoutingContext`, request outcomes, and session-key derivation shared with
  the Router policy lifecycle.
- `StateProvider`, `MockStateProvider`, and `SnapshotStore`; readers take one
  immutable `Arc<ClusterSnapshot>` while background writers publish through
  `ArcSwap`.
- `RedisIngestor`, which bootstraps from the two sorted-set indexes, consumes
  the per-Router stream cursor, retries with bounded exponential backoff, and
  re-bootstraps after every reconnect.
- `MetricsScraper`, which fetches supported vLLM Prometheus gauges and counters
  into the same snapshot without putting HTTP or Redis I/O on request paths.
- `SnapshotStore::run_expirer`, an independent task that removes stale Redis
  and metrics entries even when both sources are idle or disconnected.
- `maximal_balanced_subset`, the v1 absolute-and-relative threshold algorithm,
  plus an extensible `RequestScorer` and the default HBM/DRAM scorer.
- `ReservationLedger`, which serializes selection with local projected-load
  reservation and releases by the request-scoped routing ID.

The v0.1.15 integration constructs a `RoutingContext` once per incoming
generation request, carries a
`RequestLifecycle` guard through each retry attempt and streaming response, and
adds the `agent_aware` policy to the regular HTTP Router. The policy starts the
Redis ingestor, metrics scraper, and independent expirer once. Worker selection
reads one immutable snapshot and performs no Redis or HTTP I/O.

For the Rust CLI, configure the production policy with environment variables
so credentials do not need to appear in process arguments:

```bash
export AGENT_ROUTER_REDIS_URL='redis://router-user:password@redis:6379'
export AGENT_ROUTER_CLUSTER_ID='prod-a'
export AGENT_ROUTER_CLUSTER_SECRET='<shared HMAC secret>'
vllm-router --policy agent_aware \
  --worker-urls http://worker-a:8000 http://worker-b:8000
```

The Python `PolicyType.AgentAware` uses the same environment variables.
`--agent-dram-block-restore-ms` overrides the DRAM restore cost on the Rust
CLI. `--agent-balance-abs-threshold-ms` and `--agent-balance-rel-threshold` are
accepted for compatibility but no longer affect selection (see
`spec/protocol-v1.md`, "Selection"). Production defaults to shadow-only. Set
`--agent-rollout-percent 0|1|5|25|100` or `AGENT_ROUTER_ROLLOUT_PERCENT` for
Python, and promote in that order after checking
`vllm_router_agent_rollout_decisions_total`. Agent-aware routing is
intentionally rejected for PD and OpenAI
modes. If any regular worker lacks a matching fresh load record, the whole
candidate set uses round-robin fallback; partial state is never treated as
zero load.

Run the Rust verification with:

```bash
cargo test --locked --manifest-path agent_router/Cargo.toml
```
