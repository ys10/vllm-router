# AgentInfer Router Fork

The `dev` branch is based on the official vLLM Router `v0.1.15` tag and
contains AgentInfer's regular HTTP agent-aware routing implementation.

The in-repository `agent_router` crate owns the Redis state protocol, immutable
snapshots, metrics ingestion, load-balancing and cache-affinity scoring,
reservations, and rollout policy. The Router crate owns HTTP request lifecycle
integration and forwards canonical Agent Router request metadata to vLLM.

Run the complete Rust verification with:

```bash
cargo test --locked --manifest-path agent_router/Cargo.toml
cargo test --locked --lib
cargo clippy --locked --all-targets --all-features -- -D warnings
```
