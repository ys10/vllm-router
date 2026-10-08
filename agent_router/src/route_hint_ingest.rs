//! Background Redis I/O for AgentRoute route hints and their acknowledgements.

use crate::redis_ingest::{
    read_stream_after, redis_error, stream_tail, validate_cluster_id, wait_or_shutdown,
    STREAM_FIELD,
};
use crate::route_hints::RouteHintAck;
use crate::{RedisIngestError, RedisReadBatch, SnapshotStore};
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const ACK_PUBLISH_ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct RouteHintRedisConfig {
    pub redis_url: String,
    pub cluster_id: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub read_block: Duration,
    pub ack_stream_maxlen: usize,
}

impl std::fmt::Debug for RouteHintRedisConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteHintRedisConfig")
            .field("redis_url", &"<redacted>")
            .field("cluster_id", &self.cluster_id)
            .field("initial_backoff", &self.initial_backoff)
            .field("max_backoff", &self.max_backoff)
            .field("read_block", &self.read_block)
            .field("ack_stream_maxlen", &self.ack_stream_maxlen)
            .finish()
    }
}

impl RouteHintRedisConfig {
    pub fn production(redis_url: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            cluster_id: cluster_id.into(),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            read_block: Duration::from_secs(1),
            ack_stream_maxlen: 10_000,
        }
    }

    fn key_prefix(&self) -> Result<String, RedisIngestError> {
        validate_cluster_id(&self.cluster_id)?;
        Ok(format!("agentinfer:agent-route:v1:{{{}}}", self.cluster_id))
    }

    pub fn snapshot_key(&self) -> Result<String, RedisIngestError> {
        Ok(format!("{}:route-hints", self.key_prefix()?))
    }

    pub fn stream_key(&self) -> Result<String, RedisIngestError> {
        Ok(format!("{}:route-hint-events", self.key_prefix()?))
    }

    pub fn ack_stream_key(&self) -> Result<String, RedisIngestError> {
        Ok(format!("{}:route-hint-acks", self.key_prefix()?))
    }
}

#[async_trait]
pub trait RouteHintConnector: Send + Sync + std::fmt::Debug {
    async fn connect(
        &self,
        config: &RouteHintRedisConfig,
    ) -> Result<Box<dyn RouteHintConnection>, RedisIngestError>;
}

#[async_trait]
pub trait RouteHintConnection: Send + std::fmt::Debug {
    /// Stream tail cursor plus the latest snapshot document, if present.
    async fn bootstrap(
        &mut self,
        config: &RouteHintRedisConfig,
    ) -> Result<RedisReadBatch, RedisIngestError>;

    async fn read_after(
        &mut self,
        config: &RouteHintRedisConfig,
        cursor: &str,
    ) -> Result<RedisReadBatch, RedisIngestError>;

    async fn publish_ack(
        &mut self,
        config: &RouteHintRedisConfig,
        ack: &str,
    ) -> Result<(), RedisIngestError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RouteHintClientConnector;

#[async_trait]
impl RouteHintConnector for RouteHintClientConnector {
    async fn connect(
        &self,
        config: &RouteHintRedisConfig,
    ) -> Result<Box<dyn RouteHintConnection>, RedisIngestError> {
        config.key_prefix()?;
        let client = redis::Client::open(config.redis_url.as_str())
            .map_err(|error| RedisIngestError::Connection(error.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| RedisIngestError::Connection(error.to_string()))?;
        Ok(Box::new(RouteHintRedisConnection { connection }))
    }
}

#[derive(Debug)]
struct RouteHintRedisConnection {
    connection: redis::aio::MultiplexedConnection,
}

#[async_trait]
impl RouteHintConnection for RouteHintRedisConnection {
    async fn bootstrap(
        &mut self,
        config: &RouteHintRedisConfig,
    ) -> Result<RedisReadBatch, RedisIngestError> {
        let cursor = stream_tail(&mut self.connection, &config.stream_key()?).await?;
        let snapshot: Option<String> = redis::cmd("GET")
            .arg(config.snapshot_key()?)
            .query_async(&mut self.connection)
            .await
            .map_err(redis_error)?;
        Ok(RedisReadBatch {
            cursor,
            messages: snapshot.into_iter().collect(),
        })
    }

    async fn read_after(
        &mut self,
        config: &RouteHintRedisConfig,
        cursor: &str,
    ) -> Result<RedisReadBatch, RedisIngestError> {
        read_stream_after(
            &mut self.connection,
            &config.stream_key()?,
            cursor,
            config.read_block,
        )
        .await
    }

    async fn publish_ack(
        &mut self,
        config: &RouteHintRedisConfig,
        ack: &str,
    ) -> Result<(), RedisIngestError> {
        let _: String = redis::cmd("XADD")
            .arg(config.ack_stream_key()?)
            .arg("MAXLEN")
            .arg("~")
            .arg(config.ack_stream_maxlen)
            .arg("*")
            .arg(STREAM_FIELD)
            .arg(ack)
            .query_async(&mut self.connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }
}

/// Keeps `SnapshotStore` route hints current from Redis.
pub struct RouteHintIngestor {
    config: RouteHintRedisConfig,
    connector: Arc<dyn RouteHintConnector>,
    store: Arc<SnapshotStore>,
}

impl std::fmt::Debug for RouteHintIngestor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteHintIngestor")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RouteHintIngestor {
    pub fn new(config: RouteHintRedisConfig, store: Arc<SnapshotStore>) -> Self {
        Self::with_connector(config, Arc::new(RouteHintClientConnector), store)
    }

    pub fn with_connector(
        config: RouteHintRedisConfig,
        connector: Arc<dyn RouteHintConnector>,
        store: Arc<SnapshotStore>,
    ) -> Self {
        Self {
            config,
            connector,
            store,
        }
    }

    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut backoff = self.config.initial_backoff;
        while !*shutdown.borrow() {
            match self.connector.connect(&self.config).await {
                Ok(mut connection) => match connection.bootstrap(&self.config).await {
                    Ok(batch) => {
                        self.store
                            .apply_route_hint_batch(&batch.messages, &self.config.cluster_id);
                        backoff = self.config.initial_backoff;
                        if self
                            .consume(&mut *connection, batch.cursor, &mut shutdown)
                            .await
                        {
                            break;
                        }
                    }
                    Err(error) => warn_ingest(&error),
                },
                Err(error) => warn_ingest(&error),
            }
            if wait_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = std::cmp::min(backoff.saturating_mul(2), self.config.max_backoff);
            self.store.expire_stale();
        }
    }

    async fn consume(
        &self,
        connection: &mut dyn RouteHintConnection,
        mut cursor: String,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        loop {
            if *shutdown.borrow() {
                return true;
            }
            let read = connection.read_after(&self.config, &cursor);
            tokio::select! {
                result = read => match result {
                    Ok(batch) => {
                        cursor = batch.cursor;
                        let is_empty = batch.messages.is_empty();
                        self.store
                            .apply_route_hint_batch(&batch.messages, &self.config.cluster_id);
                        self.store.expire_stale();
                        if is_empty {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(error) => {
                        warn_ingest(&error);
                        return false;
                    }
                },
                changed = shutdown.changed() => {
                    return changed.is_err() || *shutdown.borrow();
                }
            }
        }
    }
}

fn warn_ingest(error: &RedisIngestError) {
    tracing::warn!(error = %error, "route-hint Redis ingestion failed; retrying");
}

/// Drains acknowledgements queued by `RouteHintAcker` into the ACK stream.
pub struct RouteHintAckPublisher {
    config: RouteHintRedisConfig,
    connector: Arc<dyn RouteHintConnector>,
    failed: AtomicU64,
}

impl std::fmt::Debug for RouteHintAckPublisher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RouteHintAckPublisher")
            .field("config", &self.config)
            .field("failed", &self.failed())
            .finish_non_exhaustive()
    }
}

impl RouteHintAckPublisher {
    pub fn new(config: RouteHintRedisConfig) -> Self {
        Self::with_connector(config, Arc::new(RouteHintClientConnector))
    }

    pub fn with_connector(
        config: RouteHintRedisConfig,
        connector: Arc<dyn RouteHintConnector>,
    ) -> Self {
        Self {
            config,
            connector,
            failed: AtomicU64::new(0),
        }
    }

    /// Acknowledgements abandoned after bounded retries.
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }

    pub async fn run(
        &self,
        mut receiver: tokio::sync::mpsc::Receiver<RouteHintAck>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        let mut connection: Option<Box<dyn RouteHintConnection>> = None;
        loop {
            let ack = tokio::select! {
                ack = receiver.recv() => match ack {
                    Some(ack) => ack,
                    None => return,
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                    continue;
                }
            };
            let payload = match serde_json::to_string(&ack) {
                Ok(payload) => payload,
                Err(error) => {
                    tracing::warn!(error = %error, "route-hint ACK serialization failed");
                    self.failed.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };
            if !self
                .publish_with_retry(&mut connection, &payload, &mut shutdown)
                .await
            {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn publish_with_retry(
        &self,
        connection: &mut Option<Box<dyn RouteHintConnection>>,
        payload: &str,
        shutdown: &mut tokio::sync::watch::Receiver<bool>,
    ) -> bool {
        let mut backoff = self.config.initial_backoff;
        for attempt in 1..=ACK_PUBLISH_ATTEMPTS {
            if connection.is_none() {
                match self.connector.connect(&self.config).await {
                    Ok(connected) => *connection = Some(connected),
                    Err(error) => tracing::warn!(error = %error, "route-hint ACK connect failed"),
                }
            }
            if let Some(active) = connection.as_mut() {
                match active.publish_ack(&self.config, payload).await {
                    Ok(()) => return true,
                    Err(error) => {
                        tracing::warn!(error = %error, "route-hint ACK publish failed");
                        *connection = None;
                    }
                }
            }
            if attempt < ACK_PUBLISH_ATTEMPTS {
                if wait_or_shutdown(backoff, shutdown).await {
                    return false;
                }
                backoff = std::cmp::min(backoff.saturating_mul(2), self.config.max_backoff);
            }
        }
        false
    }
}
