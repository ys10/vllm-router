mod support;

use agent_router::{RedisClientConnector, RedisConnector, RedisIngestConfig, StateMessage};
use std::time::Duration;
use support::{server_load_json, RedisProcess};

#[tokio::test]
async fn real_redis_bootstrap_captures_tail_then_reads_new_stream_events() {
    let Some(redis) = RedisProcess::start().await else {
        eprintln!("redis-server is unavailable; skipping integration test");
        return;
    };
    let config = RedisIngestConfig {
        redis_url: redis.url.clone(),
        cluster_id: "cluster-a".to_string(),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        read_block: Duration::from_millis(100),
    };
    let first = server_load_json(1, 10_000, 20_000);
    let message: StateMessage = serde_json::from_str(&first).unwrap();
    let worker_key = message.server().worker_key().unwrap();
    let prefix = "agentinfer:router:v1:{cluster-a}";
    let snapshot_key = format!("{prefix}:worker:{worker_key}:load");
    let mut writer = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&snapshot_key)
        .arg(&first)
        .query_async::<()>(&mut writer)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(format!("{prefix}:load-index"))
        .arg(20_000)
        .arg(&snapshot_key)
        .query_async::<()>(&mut writer)
        .await
        .unwrap();
    redis::cmd("XADD")
        .arg(format!("{prefix}:events"))
        .arg("*")
        .arg("data")
        .arg(&first)
        .query_async::<String>(&mut writer)
        .await
        .unwrap();

    let mut reader = RedisClientConnector.connect(&config).await.unwrap();
    let bootstrap = reader.bootstrap(&config, 10_000).await.unwrap();
    assert_eq!(bootstrap.messages, vec![first]);

    let second = server_load_json(2, 10_001, 20_001);
    redis::cmd("XADD")
        .arg(format!("{prefix}:events"))
        .arg("*")
        .arg("data")
        .arg(&second)
        .query_async::<String>(&mut writer)
        .await
        .unwrap();
    let incremental = reader.read_after(&config, &bootstrap.cursor).await.unwrap();
    assert_eq!(incremental.messages, vec![second]);
    assert_ne!(incremental.cursor, bootstrap.cursor);
}
