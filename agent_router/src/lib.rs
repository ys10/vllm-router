// `async_trait` marks its boxed futures `#[must_use]`; clippy >= 1.99 flags that.
#![allow(clippy::double_must_use)]

mod clock;
mod context;
mod metrics;
mod provider;
mod redis_ingest;
mod reservation;
mod rollout;
mod route_hint_ingest;
mod route_hints;
mod routing;
mod state;

pub use clock::{Clock, ClockReading, ManualClock, SystemClock};
pub use context::{ContextError, RequestOutcome, RoutingContext, SessionContext};
pub use metrics::{
    parse_vllm_metrics, MetricsError, MetricsFetcher, MetricsPublisher, MetricsSample,
    MetricsScrapeReport, MetricsScraper, MetricsTarget, MetricsTargetProvider,
    ReqwestMetricsFetcher, SnapshotMetricsTargets,
};
pub use provider::{
    ApplyBatchReport, ClusterSnapshot, FreshValue, MockStateProvider, SnapshotStore, SourceHealth,
    StateProvider, WorkerSnapshot,
};
pub use redis_ingest::{
    RedisClientConnector, RedisConnector, RedisIngestConfig, RedisIngestError, RedisIngestor,
    RedisReadBatch, RedisStateConnection,
};
pub use reservation::{ReservationError, ReservationLedger, ReservationView};
pub use rollout::{rollout_bucket, RolloutError, RolloutPercent, ROLLOUT_STAGES};
pub use route_hint_ingest::{
    RouteHintAckPublisher, RouteHintClientConnector, RouteHintConnection, RouteHintConnector,
    RouteHintIngestor, RouteHintRedisConfig,
};
pub use route_hints::{
    RouteHint, RouteHintAck, RouteHintAcker, RouteHintOutcome, RouteHintPayload, RouteHintSet,
    RouteHintState, RouteHintsDocument,
};
pub use routing::{
    maximal_balanced_subset, projected_server_load_ms, BalanceThresholds, HbmDramScorer,
    HbmDramScorerConfig, RequestScore, RequestScorer, RoutingError,
};
pub use state::{
    IngestMode, MessageType, RequestLoad, RequestPhase, RequestState, ServerIdentity,
    ServerLoadPayload, ServerLoadState, SessionPlacementPayload, SessionPlacementState, StateError,
    StateMessage, WorkerRole, LOAD_FRESH_MS, MAX_CLOCK_SKEW_MS, SESSION_FRESH_MS,
};
