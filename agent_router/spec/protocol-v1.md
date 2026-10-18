# Agent Router state protocol v1

## Scope

This contract defines the state exchanged between vLLM servers and the Router,
plus the request identity forwarded from the Router to vLLM. It does not define
transport code or the Router policy implementation.

## Worker identity

A routable worker is identified by:

```text
(cluster_id, server_id, instance_id, model_id, model_revision,
 tokenizer_revision, role, dp_rank)
```

`server_id` is configured by the deployment and is unique within a cluster. It
must not be derived from a mutable URL. In Kubernetes, a Pod UID is preferred
over a reusable Pod name.

`instance_id` is a lowercase, hyphenated UUIDv4 generated on every server
process start. A restarted process must never reuse its predecessor's value.
Sequence numbers are an unsigned 64-bit counter that increases globally across
all messages emitted by one instance. A Router discards a message older than
the latest sequence number seen for the same entity and instance.

The endpoint is routing metadata, not identity.

`worker_key` is the lowercase SHA-256 digest of every identity field in the
listed order. Each field is first rendered as canonical UTF-8 text, including
`dp_rank` as base-10 without leading zeroes, then encoded as a four-byte
big-endian length followed by its bytes. It prevents collisions between DP
ranks, roles, models, and restarted process instances that share one Server.

## Session and prefix identity

Raw tenant, session, request, prompt, and token identifiers must not be stored
in Redis. Producers derive:

```text
frame(text) = u32be(len(UTF-8(text))) || UTF-8(text)
session_key = HMAC-SHA256(cluster_secret, frame(tenant_id) || frame(session_id))
request_key = HMAC-SHA256(cluster_secret, frame(tenant_id) || frame(request_id))
```

`session_epoch` increments when a session is reset, truncated, forked, changes
model/tokenizer revision, or otherwise stops being append-only.

`prefix_digest` is SHA-256 over `frame(model_revision)`,
`frame(tokenizer_revision)`, an unsigned 64-bit big-endian token count, and
each ordered token ID as unsigned 32-bit big-endian. A placement is reusable
only when session key, session epoch, model revision, tokenizer revision, and
expected parent prefix digest match the incoming request.

## Request identity transport

The regular HTTP Router accepts raw identity in top-level `session_params` and
forwards it to vLLM through `vllm_xargs` using these canonical fields:

```text
agent_router_tenant_id
agent_router_session_id
agent_router_session_epoch
agent_router_expected_parent_prefix_digest
agent_router_request_id
```

Tenant ID and session ID are required together. Session epoch and parent prefix
digest are optional, but a producer publishes reusable placement only when the
session epoch is present. The request ID is the Router request ID when supplied;
otherwise it is derived from the stable routing ID. It remains unchanged across
retries.

The Router preserves unrelated `vllm_xargs` entries and an existing canonical
entry with the same value. A conflicting canonical entry or a non-object
`vllm_xargs` value is a client error. This prevents Router scoring identity and
vLLM publication identity from diverging.

HBM, DRAM and SSD block counts are disjoint residency counts for the prefix
identified by `prefix_digest`. They must not double-count a block.
`hbm_cached_tokens`, `dram_cached_tokens` and `ssd_cached_tokens` identify the
reusable token count in each tier, including any partial block; tiers are
consumed in that order from the start of the prefix. Their sum must equal
`cached_prefix_tokens`, and each tier's token count cannot exceed its block
capacity.

`prefix_tokens` (optional) is the length of the prefix the digest identifies.
When absent it equals `cached_prefix_tokens`. A producer publishes the digest
of the session's full registered prefix and lets the tier counts shrink as HBM
blocks are evicted, so a partially evicted session keeps matching its expected
parent prefix digest. `ssd_blocks` / `ssd_cached_tokens` (optional, default 0)
describe a local SSD tier such as LMCache on the same worker.

A load document may carry `ssd_load_tokens_per_second_ewma`, the measured rate
of loading KV from that SSD tier. The scorer credits SSD tokens with the
recompute time they save net of loading them at that rate; without a measured
rate they are not credited but still make the worker a holder of the
session's KV (see Selection).

Every load entry must report a non-negative `decode_tokens_remaining`. An
unknown decode budget is invalid protocol data and must not be encoded as zero.
The producer should derive it from the request's decode limit and generated
token count before publishing.

## Redis layout

Every key uses the cluster ID as a Redis Cluster hash tag:

```text
agentinfer:router:v1:{cluster_id}:worker:{worker_key}:load
agentinfer:router:v1:{cluster_id}:session:{session_key}:worker:{worker_key}
agentinfer:router:v1:{cluster_id}:load-index
agentinfer:router:v1:{cluster_id}:session-placement-index
agentinfer:router:v1:{cluster_id}:events
```

Snapshot values are JSON documents conforming to
`redis-state-v1.schema.json`. Indexes are sorted sets whose score is
`expires_at_ms` and whose member identifies the snapshot key. The stream
contains the same envelope as each snapshot update in the field named `data`:

```text
XADD agentinfer:router:v1:{cluster_id}:events * data <json-envelope>
```

A producer must atomically perform snapshot `SET PX`, index `ZADD`, and stream
`XADD` with a Lua script or transaction. The stream is replay support, not the
source of truth. Router bootstrap reads non-expired index members and their
snapshots before consuming the stream. Each Router maintains its own stream
cursor; a shared consumer group must not partition state among Routers.

## Freshness and expiry

Load is published every second, considered fresh for 3 seconds, and stored with
a 10-second Redis TTL. Session placement is published on change plus a
5-second heartbeat, considered fresh for 15 seconds, and stored with a
60-second TTL. When a previously published session disappears from a server's
snapshot, the producer immediately publishes a zero-residency placement with
the canonical empty-prefix digest, replacing the old affinity state.

The Router records a monotonic ingestion timestamp. Live-stream freshness uses
that timestamp and therefore does not depend on wall-clock synchronization.
Bootstrap validates `observed_at_ms` with at most 2 seconds of clock skew.
Expired state is never used for cache-affinity scoring.

## Selection: earliest estimated completion among KV holders

Earlier versions first kept a "balanced subset" of candidates within
`max(m + 250 ms, m * 1.2)` of the minimum projected load `m` and then preferred
cache reuse inside it. Those fixed bands ignored that leaving the KV holder
recomputes the whole prefix: on a saturated 4P1D deployment 1.8% of later-round
requests left their holder, raising prefill work by about 6% and slowing every
queue. The bands are gone; `--agent-balance-abs-threshold-ms` and
`--agent-balance-rel-threshold` are still validated but no longer affect
selection.

For each candidate the policy estimates

```text
completion_ms = projected_load_ms + local_reservations_ms + own_work_ms
```

where `own_work_ms` is the scorer's service estimate for this request on that
candidate (cached tokens save their prefill time). Every term is a measured or
reported quantity; there are no tuned constants.

If any candidate reports holding the session's prefix (a compatible
placement in HBM, DRAM or local SSD), the policy picks the earliest
`completion_ms` among those holders; `own_work_ms` reflects how much of the
prefix each holds (SSD restored at the measured rate). Otherwise (no cached KV
anywhere, e.g. the first request of a session) it picks the earliest among all
candidates, i.e. the least-loaded one. The Router never drops a prefix just to
rebalance load.

AgentRoute pulls load toward a node by migrating a session's KV there ahead of
the session's next request. The target engine reports the migrated prefix as a
session placement as it arrives, so it joins the holders and is chosen once it
is the earliest to finish. A Route Hint does not make its target a holder; it
only breaks ties.

Ties prefer the hint target, then the lower projected queue, then the worker
key.

## Shadow decisions and rollout

The regular HTTP Router uses round-robin as the baseline. Every attempt first
computes an agent-aware shadow decision from one immutable local snapshot and
the current local reservations. `rollout_percent = 0` is shadow-only: the
baseline worker receives the request, while the shadow comparison is observed.
The only valid rollout stages are 0, 1, 5, 25, and 100 percent.

The rollout bucket is `routing_id mod 100`. A `RoutingContext` keeps the same
random routing ID across retries, so membership is stable for the request and
the lower-percentage cohorts are strict subsets of higher-percentage cohorts.
If the bucket is below the configured percentage and a complete shadow
decision exists, that decision is enforced. Missing, stale, mismatched, or
invalid state always uses the baseline.

Selection and reservation are one atomic operation. Only the worker that will
actually receive the attempt is reserved; a shadow recommendation never creates
a second reservation. Counters and Prometheus metrics count attempts, with
action (`baseline` or `enforced`) and shadow comparison (`agreement`,
`disagreement`, or `unavailable`). Promotion proceeds through shadow, 1, 5, 25,
and 100 percent after observing each stage.

In vLLM PD mode (`--vllm-pd-disaggregation`) the agent-aware policy may be the
prefill policy only. It scores workers whose identity role is `prefill`, uses
the same session placements, reservations and route hints, and its
reservation covers the prefill phase: it is released when the prefill response
returns (or the attempt is dropped). Decode keeps its own non-agent-aware
policy. Requests forwarded to both workers carry the canonical `vllm_xargs`.

## Projected load, scoring, and reservations

The v1 Redis token-work projection is:

```text
redis_load_ms = 1000 * (waiting_prefill_tokens
                        + running_prefill_tokens_remaining) / prefill_tps_ewma
              + 1000 * (waiting_decode_tokens_remaining
                        + running_decode_tokens_remaining) / decode_tps_ewma
```

Fresh vLLM metrics may observe running or waiting requests before their Redis
details arrive. Only the positive count difference is added, using the
configured fallback request cost, so Redis and metrics do not double-count the
same request. The projected queue adds this lag estimate and all local
reservations to `redis_load_ms`.

The default scorer estimates this request's own service time (`own_work_ms`).
HBM tokens save their prefill recomputation time. DRAM tokens are reused only
when their recomputation time exceeds the configured block-restore cost; the
net difference is the DRAM saving. Exact session key, epoch, model revision,
tokenizer revision, and parent prefix digest matching is required. When prompt
length is unknown, an exact placement's cached prefix token count is a lower
bound on prefill work.

`RequestScorer` is the extension boundary for future compute-time models. The
selected request's estimated service time is reserved locally in integer
microseconds. Reading current reservation totals, selecting the worker, and
installing the new reservation are one mutex-protected operation, so concurrent
requests cannot all observe the same pre-reservation load. Reservations use the
Router's stable worker URL identity, so they remain visible while Redis state
transitions between missing, stale, fresh, or a restarted server instance.

Each retry attempt releases its previous reservation before selecting again.
Non-streaming body completion, streaming completion, retryable status,
transport failure, serialization failure, client cancellation, and guard drop
all converge on the same exactly-once lifecycle release. The streaming bridge
uses a bounded channel so slow clients apply backpressure to the upstream body.

## Source precedence and failure

Redis supplies per-request work and per-session placement. vLLM metrics cover
aggregate requests observed ahead of Redis delivery. Local Router reservations
cover requests selected but not yet visible in either source.

No Redis operation is allowed on the synchronous worker-selection path.
When restarted instances overlap for one endpoint and DP rank, the Router uses
the fresh load with the greatest `(observed_at_ms, seq)`. If any routable worker
still has missing, stale, or mismatched Redis load state, the complete candidate
set uses round-robin fallback. It never blocks request forwarding and never
interprets missing state as zero load.

## AgentRoute route hints

AgentRoute (the global KV control plane in AgentInfer) publishes versioned
route hints after it has verified that a session's KV prefix is READY on a
target worker. Hints are an additional, optional input to the agent-aware
policy; without them routing behaves exactly as described above.

### Redis layout

```text
agentinfer:agent-route:v1:{cluster_id}:route-hints        # latest document, SET PX
agentinfer:agent-route:v1:{cluster_id}:route-hint-events  # XADD data <document>
agentinfer:agent-route:v1:{cluster_id}:route-hint-acks    # XADD data <ack>
```

The document conforms to `route-hints-v1.schema.json`:

```json
{
  "schema_version": 1,
  "message_type": "route_hints",
  "cluster_id": "...",
  "producer_id": "<AgentRoute process id>",
  "state_version": 42,
  "generated_at_ms": 1760000000000,
  "hints": [
    {
      "session_key": "<Router v1 session key>",
      "source_server": "<server_id or null>",
      "preferred_server": "<server_id>",
      "plan_id": "...",
      "state": "ready",
      "valid_until_ms": 1760000005000,
      "state_version": 41
    }
  ]
}
```

`session_key` is the same 64-hex keyed session identity the Router derives for
session placement. `preferred_server` is a Router v1 `server_id`.

### Ingestion

`state_version` orders documents from one producer. A document from the same
`producer_id` replaces every previously ingested hint only when its
`state_version` is greater than the last accepted one; older or equal versions
are ignored. A restarted AgentRoute process uses a new `producer_id` and a new
version sequence, so a document from a different producer replaces the set
unless its `generated_at_ms` is older than the current document's. An empty
`hints` array revokes all hints. Only `ready`
and `active` hints are retained. Bootstrap reads the latest snapshot key before
consuming the event stream with a Router-local cursor, mirroring Router v1
state.

`valid_until_ms` is a producer wall-clock deadline. At ingestion the Router
converts the remaining validity, reduced by the 2-second clock-skew allowance,
into a monotonic deadline. Expired hints are never used.

### Routing

Hints never bypass the safety rules above. For a request whose session key has
a valid hint, the agent-aware policy uses the hint target instead of the scored
shadow choice only if all hold:

1. agent-aware state is complete for the candidate set;
2. exactly one candidate has a fresh load whose identity has
   `server_id == preferred_server`;
3. that candidate has the earliest estimated completion among the session's KV
   holders (the hint source and target are holders; see Selection);
4. the request is in the enforced rollout cohort.

Otherwise the normal decision is used and the hint outcome is recorded as one
of `no_hint`, `expired`, `target_unavailable`, `target_imbalanced`, or
`shadow_only`. A hint is never a reason to fall back to the baseline.

### Acknowledgement

When a request is actually forwarded to the hint target, the Router emits one
acknowledgement per `(plan_id, session_key, state_version)`:

```json
{
  "schema_version": 1,
  "message_type": "route_hint_ack",
  "cluster_id": "...",
  "router_id": "<process-unique id>",
  "plan_id": "...",
  "session_key": "...",
  "state_version": 41,
  "observed_at_ms": 1760000001234
}
```

`state_version` is the hint's own version, so AgentRoute can reject an
acknowledgement for a superseded hint. Acknowledgements leave the selection
path through a bounded in-memory queue drained by a background task; when the
queue is full the acknowledgement is dropped and counted, never awaited.
AgentRoute promotes a READY hint to ACTIVE only after such an acknowledgement.
