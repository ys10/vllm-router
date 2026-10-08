use crate::{IngestMode, SnapshotStore};
use async_trait::async_trait;
use redis::streams::{StreamRangeReply, StreamReadReply};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const STREAM_FIELD: &str = "data";

#[derive(Clone)]
pub struct RedisIngestConfig {
    pub redis_url: String,
    pub cluster_id: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub read_block: Duration,
}

impl std::fmt::Debug for RedisIngestConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisIngestConfig")
            .field("redis_url", &"<redacted>")
            .field("cluster_id", &self.cluster_id)
            .field("initial_backoff", &self.initial_backoff)
            .field("max_backoff", &self.max_backoff)
            .field("read_block", &self.read_block)
            .finish()
    }
}

impl RedisIngestConfig {
    pub fn production(redis_url: impl Into<String>, cluster_id: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            cluster_id: cluster_id.into(),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            read_block: Duration::from_secs(1),
        }
    }

    fn key_prefix(&self) -> Result<String, RedisIngestError> {
        validate_cluster_id(&self.cluster_id)?;
        Ok(format!("agentinfer:router:v1:{{{}}}", self.cluster_id))
    }

    fn stream_key(&self) -> Result<String, RedisIngestError> {
        Ok(format!("{}:events", self.key_prefix()?))
    }

    fn index_keys(&self) -> Result<[String; 2], RedisIngestError> {
        let prefix = self.key_prefix()?;
        Ok([
            format!("{prefix}:load-index"),
            format!("{prefix}:session-placement-index"),
        ])
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RedisReadBatch {
    pub cursor: String,
    pub messages: Vec<String>,
}

#[async_trait]
pub trait RedisConnector: Send + Sync + std::fmt::Debug {
    async fn connect(
        &self,
        config: &RedisIngestConfig,
    ) -> Result<Box<dyn RedisStateConnection>, RedisIngestError>;
}

#[async_trait]
pub trait RedisStateConnection: Send + std::fmt::Debug {
    async fn bootstrap(
        &mut self,
        config: &RedisIngestConfig,
        now_unix_ms: u64,
    ) -> Result<RedisReadBatch, RedisIngestError>;

    async fn read_after(
        &mut self,
        config: &RedisIngestConfig,
        cursor: &str,
    ) -> Result<RedisReadBatch, RedisIngestError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RedisClientConnector;

#[async_trait]
impl RedisConnector for RedisClientConnector {
    async fn connect(
        &self,
        config: &RedisIngestConfig,
    ) -> Result<Box<dyn RedisStateConnection>, RedisIngestError> {
        config.key_prefix()?;
        let client = redis::Client::open(config.redis_url.as_str())
            .map_err(|error| RedisIngestError::Connection(error.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|error| RedisIngestError::Connection(error.to_string()))?;
        Ok(Box::new(RedisConnection { connection }))
    }
}

#[derive(Debug)]
struct RedisConnection {
    connection: redis::aio::MultiplexedConnection,
}

#[async_trait]
impl RedisStateConnection for RedisConnection {
    async fn bootstrap(
        &mut self,
        config: &RedisIngestConfig,
        now_unix_ms: u64,
    ) -> Result<RedisReadBatch, RedisIngestError> {
        let cursor = stream_tail(&mut self.connection, &config.stream_key()?).await?;

        let mut messages = Vec::new();
        for index_key in config.index_keys()? {
            let snapshot_keys: Vec<String> = redis::cmd("ZRANGEBYSCORE")
                .arg(index_key)
                .arg(format!("({now_unix_ms}"))
                .arg("+inf")
                .query_async(&mut self.connection)
                .await
                .map_err(redis_error)?;
            if snapshot_keys.is_empty() {
                continue;
            }
            let values: Vec<Option<String>> = redis::cmd("MGET")
                .arg(snapshot_keys)
                .query_async(&mut self.connection)
                .await
                .map_err(redis_error)?;
            messages.extend(values.into_iter().flatten());
        }
        Ok(RedisReadBatch { cursor, messages })
    }

    async fn read_after(
        &mut self,
        config: &RedisIngestConfig,
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
}

pub(crate) fn validate_cluster_id(cluster_id: &str) -> Result<(), RedisIngestError> {
    if cluster_id.len() > 128
        || !cluster_id
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || cluster_id
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-".contains(&byte)))
    {
        return Err(RedisIngestError::Protocol(
            "cluster_id must match [A-Za-z0-9][A-Za-z0-9._-]*".into(),
        ));
    }
    Ok(())
}

/// Latest stream ID, or `0-0` for an empty stream.
pub(crate) async fn stream_tail(
    connection: &mut redis::aio::MultiplexedConnection,
    stream_key: &str,
) -> Result<String, RedisIngestError> {
    let tail: StreamRangeReply = redis::cmd("XREVRANGE")
        .arg(stream_key)
        .arg("+")
        .arg("-")
        .arg("COUNT")
        .arg(1)
        .query_async(connection)
        .await
        .map_err(redis_error)?;
    Ok(tail
        .ids
        .first()
        .map(|entry| entry.id.clone())
        .unwrap_or_else(|| "0-0".to_string()))
}

/// Blocking XREAD of the `data` field after `cursor`.
pub(crate) async fn read_stream_after(
    connection: &mut redis::aio::MultiplexedConnection,
    stream_key: &str,
    cursor: &str,
    read_block: Duration,
) -> Result<RedisReadBatch, RedisIngestError> {
    let block_ms: u64 = read_block.as_millis().try_into().unwrap_or(u64::MAX);
    let reply: StreamReadReply = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(256)
        .arg("BLOCK")
        .arg(block_ms)
        .arg("STREAMS")
        .arg(stream_key)
        .arg(cursor)
        .query_async(connection)
        .await
        .map_err(redis_error)?;
    let mut next_cursor = cursor.to_string();
    let mut messages = Vec::new();
    for key in reply.keys {
        for entry in key.ids {
            next_cursor = entry.id.clone();
            match entry.get::<String>(STREAM_FIELD) {
                Some(message) => messages.push(message),
                None => tracing::warn!(
                    stream_id = %entry.id,
                    field = STREAM_FIELD,
                    "discarding Redis stream entry without state envelope"
                ),
            }
        }
    }
    Ok(RedisReadBatch {
        cursor: next_cursor,
        messages,
    })
}

pub(crate) fn redis_error(error: redis::RedisError) -> RedisIngestError {
    RedisIngestError::Connection(error.to_string())
}

pub struct RedisIngestor {
    config: RedisIngestConfig,
    connector: Arc<dyn RedisConnector>,
    store: Arc<SnapshotStore>,
}

impl std::fmt::Debug for RedisIngestor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedisIngestor")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RedisIngestor {
    pub fn new(config: RedisIngestConfig, store: Arc<SnapshotStore>) -> Self {
        Self::with_connector(config, Arc::new(RedisClientConnector), store)
    }

    pub fn with_connector(
        config: RedisIngestConfig,
        connector: Arc<dyn RedisConnector>,
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
                Ok(mut connection) => {
                    match connection
                        .bootstrap(&self.config, self.store.now().unix_ms)
                        .await
                    {
                        Ok(batch) => {
                            self.store
                                .apply_json_batch(&batch.messages, IngestMode::Bootstrap);
                            self.store.set_redis_status(true, None);
                            backoff = self.config.initial_backoff;
                            if self
                                .consume_connection(&mut *connection, batch.cursor, &mut shutdown)
                                .await
                            {
                                break;
                            }
                        }
                        Err(error) => {
                            self.store.set_redis_status(false, Some(error.to_string()));
                        }
                    }
                }
                Err(error) => {
                    self.store.set_redis_status(false, Some(error.to_string()));
                }
            }
            if wait_or_shutdown(backoff, &mut shutdown).await {
                break;
            }
            backoff = std::cmp::min(backoff.saturating_mul(2), self.config.max_backoff);
            self.store.expire_stale();
        }
        self.store.set_redis_status(false, None);
    }

    async fn consume_connection(
        &self,
        connection: &mut dyn RedisStateConnection,
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
                        self.store.apply_json_batch(&batch.messages, IngestMode::Live);
                        self.store.expire_stale();
                        if is_empty {
                            tokio::task::yield_now().await;
                        }
                    }
                    Err(error) => {
                        self.store.set_redis_status(false, Some(error.to_string()));
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

pub(crate) async fn wait_or_shutdown(
    duration: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedisIngestError {
    #[error("Redis connection failed: {0}")]
    Connection(String),
    #[error("invalid Redis router protocol: {0}")]
    Protocol(String),
}
