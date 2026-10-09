# AgentInfer Router Fork

The `dev` branch is based on the official vLLM Router `v0.1.15` tag and
contains AgentInfer's regular HTTP agent-aware routing implementation.

The in-repository `agent_router` crate owns the Redis state protocol, immutable
snapshots, metrics ingestion, load-balancing and cache-affinity scoring,
reservations, and rollout policy. The Router crate owns HTTP request lifecycle
integration and forwards canonical Agent Router request metadata to vLLM.

With `--agent-route-hints` (or `AGENT_ROUTER_ROUTE_HINTS=1` for the Python
binding) the agent-aware policy also consumes AgentRoute route hints and
acknowledges the exact hint versions it forwards to; see "AgentRoute route
hints" in `agent_router/spec/protocol-v1.md`. Hints are off by default.

In vLLM PD mode, `--prefill-policy agent_aware` routes prefill workers
(published with role `prefill`) through the same scoring, reservations and
hints; the decode policy must not be agent-aware.

Run the complete Rust verification with:

```bash
cargo test --locked --manifest-path agent_router/Cargo.toml
cargo test --locked --lib
cargo clippy --locked --all-targets --all-features -- -D warnings
```

On Rust/clippy 1.99 the last command reports lints in upstream Router files
(`fetch_update` deprecation, `double_must_use` from `async_trait`, and others)
that predate the AgentInfer changes; the `agent_router` crate itself is clean.
