#![allow(dead_code)]

use serde_json::json;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub const INSTANCE_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

pub struct RedisProcess {
    child: Child,
    pub url: String,
}

impl RedisProcess {
    pub async fn start() -> Option<Self> {
        let status = match Command::new("redis-server")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(status) => status,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
            Err(error) => panic!("failed to probe redis-server: {error}"),
        };
        assert!(status.success(), "redis-server --version failed");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let child = Command::new("redis-server")
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--appendonly",
                "no",
                "--loglevel",
                "warning",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to start redis-server");
        let process = Self {
            child,
            url: format!("redis://127.0.0.1:{port}/"),
        };
        for _ in 0..100 {
            if let Ok(client) = redis::Client::open(process.url.as_str()) {
                if client.get_multiplexed_async_connection().await.is_ok() {
                    return Some(process);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("redis-server did not become ready")
    }
}

impl Drop for RedisProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn server() -> serde_json::Value {
    json!({
        "cluster_id": "cluster-a",
        "server_id": "server-a",
        "instance_id": INSTANCE_ID,
        "model_id": "model-a",
        "model_revision": "revision-a",
        "tokenizer_revision": "tokenizer-a",
        "role": "unified",
        "dp_rank": 0,
        "endpoint": "http://127.0.0.1:8000"
    })
}

pub fn server_load_json(seq: u64, observed_at_ms: u64, expires_at_ms: u64) -> String {
    json!({
        "schema_version": 1,
        "message_type": "server_load",
        "seq": seq,
        "observed_at_ms": observed_at_ms,
        "expires_at_ms": expires_at_ms,
        "server": server(),
        "payload": {
            "waiting_count": 1,
            "running_count": 2,
            "waiting_prefill_tokens": 16,
            "waiting_decode_tokens_remaining": 32,
            "running_prefill_tokens_remaining": 8,
            "running_decode_tokens_remaining": 64,
            "prefill_tokens_per_second_ewma": 1000.0,
            "decode_tokens_per_second_ewma": 100.0,
            "kv_cache_usage_ratio": 0.4,
            "requests": []
        }
    })
    .to_string()
}

#[allow(dead_code)]
pub fn session_placement_json(seq: u64, observed_at_ms: u64, expires_at_ms: u64) -> String {
    json!({
        "schema_version": 1,
        "message_type": "session_placement",
        "seq": seq,
        "observed_at_ms": observed_at_ms,
        "expires_at_ms": expires_at_ms,
        "server": server(),
        "payload": {
            "session_key": "b".repeat(64),
            "session_epoch": 2,
            "prefix_digest": "c".repeat(64),
            "cached_prefix_tokens": 48,
            "block_size_tokens": 16,
            "hbm_blocks": 2,
            "dram_blocks": 1,
            "hbm_cached_tokens": 32,
            "dram_cached_tokens": 16
        }
    })
    .to_string()
}
