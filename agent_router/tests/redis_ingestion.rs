mod support;

use agent_router::{
    ManualClock, RedisClientConnector, RedisConnector, RedisIngestConfig, RedisIngestor,
    RedisReadBatch, RedisStateConnection, SnapshotStore, StateProvider,
};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::server_load_json;

#[derive(Debug)]
struct FakeConnector {
    connections: Mutex<VecDeque<FakeConnection>>,
    attempts: Mutex<usize>,
}

#[tokio::test]
async fn rejects_cluster_ids_that_do_not_start_with_an_alphanumeric_character() {
    let config = RedisIngestConfig {
        redis_url: "redis://unused".to_string(),
        cluster_id: ".invalid".to_string(),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        read_block: Duration::from_millis(1),
    };

    let error = RedisClientConnector.connect(&config).await.unwrap_err();
    assert!(matches!(error, agent_router::RedisIngestError::Protocol(_)));
}

#[test]
fn redis_config_debug_redacts_credentials() {
    let config = RedisIngestConfig::production(
        "redis://router:top-secret@redis.example:6379/0",
        "cluster-a",
    );

    let debug = format!("{config:?}");
    assert!(!debug.contains("top-secret"));
    assert!(!debug.contains("router:"));
}

#[async_trait]
impl RedisConnector for FakeConnector {
    async fn connect(
        &self,
        _config: &RedisIngestConfig,
    ) -> Result<Box<dyn RedisStateConnection>, agent_router::RedisIngestError> {
        *self.attempts.lock().unwrap() += 1;
        self.connections
            .lock()
            .unwrap()
            .pop_front()
            .map(|connection| Box::new(connection) as Box<dyn RedisStateConnection>)
            .ok_or_else(|| agent_router::RedisIngestError::Connection("no fake connection".into()))
    }
}

#[derive(Debug)]
struct FakeConnection {
    bootstrap: Vec<String>,
    reads: VecDeque<Result<RedisReadBatch, agent_router::RedisIngestError>>,
}

#[async_trait]
impl RedisStateConnection for FakeConnection {
    async fn bootstrap(
        &mut self,
        _config: &RedisIngestConfig,
        _now_unix_ms: u64,
    ) -> Result<RedisReadBatch, agent_router::RedisIngestError> {
        Ok(RedisReadBatch {
            cursor: "1-0".to_string(),
            messages: self.bootstrap.clone(),
        })
    }

    async fn read_after(
        &mut self,
        _config: &RedisIngestConfig,
        cursor: &str,
    ) -> Result<RedisReadBatch, agent_router::RedisIngestError> {
        self.reads.pop_front().unwrap_or_else(|| {
            Ok(RedisReadBatch {
                cursor: cursor.to_string(),
                messages: Vec::new(),
            })
        })
    }
}

#[tokio::test]
async fn reconnects_bootstraps_again_and_keeps_newest_state() {
    let clock = Arc::new(ManualClock::new(10_000, 0));
    let store = Arc::new(SnapshotStore::new(clock));
    let connector = Arc::new(FakeConnector {
        connections: Mutex::new(VecDeque::from([
            FakeConnection {
                bootstrap: vec![server_load_json(1, 10_000, 20_000)],
                reads: VecDeque::from([Err(agent_router::RedisIngestError::Connection(
                    "link lost".into(),
                ))]),
            },
            FakeConnection {
                bootstrap: vec![server_load_json(2, 10_000, 20_000)],
                reads: VecDeque::new(),
            },
        ])),
        attempts: Mutex::new(0),
    });
    let ingestor = RedisIngestor::with_connector(
        RedisIngestConfig {
            redis_url: "redis://unused".to_string(),
            cluster_id: "cluster-a".to_string(),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            read_block: Duration::from_millis(1),
        },
        connector.clone(),
        store.clone(),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let task = tokio::spawn(async move { ingestor.run(shutdown_rx).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let seq = store
                .snapshot()
                .workers
                .values()
                .next()
                .and_then(|worker| worker.load.as_ref())
                .map(|entry| entry.value.seq);
            if seq == Some(2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    shutdown_tx.send(true).unwrap();
    task.await.unwrap();

    assert!(*connector.attempts.lock().unwrap() >= 2);
    assert!(!store.snapshot().source_health.redis_connected);
}
