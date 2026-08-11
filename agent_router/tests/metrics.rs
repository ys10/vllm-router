use agent_router::{
    parse_vllm_metrics, MetricsFetcher, MetricsScraper, MetricsTarget, MetricsTargetProvider,
    ReqwestMetricsFetcher, SnapshotStore, StateProvider,
};
use async_trait::async_trait;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn parses_supported_vllm_metrics_without_confusing_histogram_buckets() {
    let sample = parse_vllm_metrics(
        r#"
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="model-a"} 3
vllm:num_requests_waiting{model_name="model-a"} 2
vllm:kv_cache_usage_perc{model_name="model-a"} 0.625
vllm:prompt_tokens_total{model_name="model-a"} 120
vllm:generation_tokens_total{model_name="model-a"} 45
vllm:request_success_total{finished_reason="stop"} 9
"#,
    )
    .unwrap();

    assert_eq!(sample.running_requests, Some(3));
    assert_eq!(sample.waiting_requests, Some(2));
    assert_eq!(sample.kv_cache_usage_ratio, Some(0.625));
    assert_eq!(sample.prompt_tokens_total, Some(120.0));
    assert_eq!(sample.generation_tokens_total, Some(45.0));
}

#[test]
fn missing_metrics_fields_remain_unknown_instead_of_becoming_zero() {
    let sample =
        parse_vllm_metrics("vllm:num_requests_running 2\nprocess_resident_memory_bytes 4096\n")
            .unwrap();

    assert_eq!(sample.running_requests, Some(2));
    assert_eq!(sample.waiting_requests, None);
    assert_eq!(sample.kv_cache_usage_ratio, None);
    assert_eq!(sample.prompt_tokens_total, None);
    assert_eq!(sample.generation_tokens_total, None);
}

#[derive(Debug)]
struct StaticTargets(Vec<MetricsTarget>);

impl MetricsTargetProvider for StaticTargets {
    fn targets(&self) -> Vec<MetricsTarget> {
        self.0.clone()
    }
}

#[derive(Debug)]
struct FakeFetcher;

#[async_trait]
impl MetricsFetcher for FakeFetcher {
    async fn fetch(&self, _url: &str) -> Result<String, agent_router::MetricsError> {
        Ok("vllm:num_requests_running 7\nvllm:num_requests_waiting 1\n".to_string())
    }
}

#[tokio::test]
async fn scraper_publishes_samples_to_the_shared_snapshot() {
    let store = Arc::new(SnapshotStore::default());
    let scraper = MetricsScraper::new(
        Arc::new(StaticTargets(vec![MetricsTarget {
            worker_key: "worker-a".to_string(),
            url: "http://worker-a/metrics".to_string(),
        }])),
        Arc::new(FakeFetcher),
        store.clone(),
        Duration::from_secs(3),
    );

    let report = scraper.scrape_once().await;

    assert_eq!(report.succeeded, 1);
    assert_eq!(
        store.snapshot().workers["worker-a"]
            .metrics
            .as_ref()
            .unwrap()
            .value
            .running_requests,
        Some(7)
    );
}

#[tokio::test]
async fn reqwest_fetcher_reads_a_real_prometheus_endpoint() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        let mut request = [0_u8; 1024];
        let _ = socket.read(&mut request).unwrap();
        let body = "vllm:num_requests_running 5\n";
        write!(
            socket,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .unwrap();
    });
    let fetcher = ReqwestMetricsFetcher::new(Duration::from_secs(1)).unwrap();

    let body = fetcher
        .fetch(&format!("http://{address}/metrics"))
        .await
        .unwrap();

    server.join().unwrap();
    assert_eq!(parse_vllm_metrics(&body).unwrap().running_requests, Some(5));
}
