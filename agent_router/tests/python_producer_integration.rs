mod support;

use agent_router::{
    RedisClientConnector, RedisConnector, RedisIngestConfig, RoutingContext, SessionContext,
    StateMessage,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::RedisProcess;

fn find_agentinfer_root() -> Option<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|candidate| candidate.join("worker_kv/router_publisher.py").is_file())
        .map(Path::to_path_buf)
}

#[tokio::test]
async fn python_producer_snapshots_are_consumed_by_rust_router() {
    let Some(redis) = RedisProcess::start().await else {
        eprintln!("redis-server is unavailable; skipping integration test");
        return;
    };
    let Some(repository) = find_agentinfer_root() else {
        eprintln!("AgentInfer Python producer is unavailable; skipping integration test");
        return;
    };
    let script = r#"
import sys
import time
import redis

from worker_kv.router_model import AgentRouterServerIdentity, derive_session_key
from worker_kv.router_publisher import RedisAgentRouterStatePublisher

server = AgentRouterServerIdentity(
    cluster_id="cluster-a",
    server_id="server-a",
    instance_id="123e4567-e89b-42d3-a456-426614174000",
    model_id="model-a",
    model_revision="revision-a",
    tokenizer_revision="tokenizer-a",
    role="unified",
    dp_rank=0,
    endpoint="http://127.0.0.1:8000",
)
publisher = RedisAgentRouterStatePublisher(
    redis.Redis.from_url(sys.argv[1]),
    server=server,
)
now_ms = time.time_ns() // 1_000_000
publisher.publish_load({
    "waiting_count": 1,
    "running_count": 1,
    "waiting_prefill_tokens": 64,
    "waiting_decode_tokens_remaining": 8,
    "running_prefill_tokens_remaining": 0,
    "running_decode_tokens_remaining": 16,
    "prefill_tokens_per_second_ewma": 1000.0,
    "decode_tokens_per_second_ewma": 100.0,
    "kv_cache_usage_ratio": 0.25,
    "requests": [],
}, observed_at_ms=now_ms)
publisher.publish_placements(({
    "session_key": derive_session_key(
        "cluster-secret", "tenant-a", "session-a"
    ),
    "session_epoch": 2,
    "prefix_digest": "b" * 64,
    "cached_prefix_tokens": 32,
    "block_size_tokens": 16,
    "hbm_blocks": 2,
    "dram_blocks": 0,
    "hbm_cached_tokens": 32,
    "dram_cached_tokens": 0,
},), observed_at_ms=now_ms)
publisher.close()
"#;
    let status = Command::new("python3")
        .args(["-c", script, redis.url.as_str()])
        .env("PYTHONPATH", &repository)
        .current_dir(&repository)
        .status()
        .expect("failed to execute Python Router state producer");
    assert!(status.success(), "Python Router state producer failed");

    let config = RedisIngestConfig {
        redis_url: redis.url.clone(),
        cluster_id: "cluster-a".to_string(),
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        read_block: Duration::from_millis(100),
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let mut reader = RedisClientConnector.connect(&config).await.unwrap();
    let bootstrap = reader.bootstrap(&config, now_ms).await.unwrap();
    assert_eq!(bootstrap.messages.len(), 2);

    let messages: Vec<StateMessage> = bootstrap
        .messages
        .iter()
        .map(|raw| serde_json::from_str(raw).unwrap())
        .collect();
    for message in &messages {
        message.validate().unwrap();
    }
    let load = messages.iter().find_map(|message| match message {
        StateMessage::ServerLoad(load) => Some(load),
        StateMessage::SessionPlacement(_) => None,
    });
    assert_eq!(load.unwrap().payload.running_count, 1);
    let placement = messages.iter().find_map(|message| match message {
        StateMessage::SessionPlacement(placement) => Some(placement),
        StateMessage::ServerLoad(_) => None,
    });
    let placement = placement.unwrap();
    let context = RoutingContext::default().with_session(SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(2),
        expected_parent_prefix_digest: Some("b".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    });
    assert_eq!(
        context.session_key(b"cluster-secret").unwrap(),
        placement.payload.session_key
    );
}
