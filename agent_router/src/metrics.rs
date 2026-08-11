use crate::StateProvider;
use async_trait::async_trait;
use prometheus_parse::{Scrape, Value};
use std::io;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricsSample {
    pub running_requests: Option<u64>,
    pub waiting_requests: Option<u64>,
    pub kv_cache_usage_ratio: Option<f64>,
    pub prompt_tokens_total: Option<f64>,
    pub generation_tokens_total: Option<f64>,
}

pub fn parse_vllm_metrics(input: &str) -> Result<MetricsSample, MetricsError> {
    // prometheus-parse 0.2.x rejects the colon allowed by the Prometheus
    // metric-name grammar, so normalize only the name token before parsing.
    let lines = input.lines().map(|line| Ok(normalize_metric_name(line)));
    let scrape = Scrape::parse(lines).map_err(|error| MetricsError::Parse(error.to_string()))?;
    let mut sample = MetricsSample::default();
    for metric in scrape.samples {
        let Some(value) = scalar_value(&metric.value) else {
            continue;
        };
        if !value.is_finite() || value < 0.0 {
            return Err(MetricsError::Parse(format!(
                "metric {} must be finite and non-negative",
                metric.metric
            )));
        }
        match metric.metric.as_str() {
            "vllm_num_requests_running" => {
                sample.running_requests = Some(add_count(sample.running_requests, value)?);
            }
            "vllm_num_requests_waiting" => {
                sample.waiting_requests = Some(add_count(sample.waiting_requests, value)?);
            }
            "vllm_kv_cache_usage_perc" => {
                if value > 1.0 {
                    return Err(MetricsError::Parse(
                        "vllm:kv_cache_usage_perc must be between zero and one".into(),
                    ));
                }
                sample.kv_cache_usage_ratio =
                    Some(sample.kv_cache_usage_ratio.unwrap_or_default().max(value));
            }
            "vllm_prompt_tokens_total" => {
                sample.prompt_tokens_total =
                    Some(sample.prompt_tokens_total.unwrap_or_default() + value);
            }
            "vllm_generation_tokens_total" => {
                sample.generation_tokens_total =
                    Some(sample.generation_tokens_total.unwrap_or_default() + value);
            }
            _ => {}
        }
    }
    Ok(sample)
}

fn normalize_metric_name(line: &str) -> String {
    let trimmed = line.trim_start();
    if let Some(rest) = trimmed
        .strip_prefix("# HELP ")
        .or_else(|| trimmed.strip_prefix("# TYPE "))
    {
        let split = rest.find(char::is_whitespace).unwrap_or(rest.len());
        return line.replacen(&rest[..split], &rest[..split].replace(':', "_"), 1);
    }
    let split = trimmed
        .find(|character: char| character == '{' || character.is_whitespace())
        .unwrap_or(trimmed.len());
    line.replacen(&trimmed[..split], &trimmed[..split].replace(':', "_"), 1)
}

fn scalar_value(value: &Value) -> Option<f64> {
    match value {
        Value::Counter(value) | Value::Gauge(value) | Value::Untyped(value) => Some(*value),
        Value::Histogram(_) | Value::Summary(_) => None,
    }
}

fn add_count(current: Option<u64>, value: f64) -> Result<u64, MetricsError> {
    if value.fract() != 0.0 || value > u64::MAX as f64 {
        return Err(MetricsError::Parse(
            "request count metric must be an unsigned integer".into(),
        ));
    }
    Ok(current.unwrap_or_default().saturating_add(value as u64))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsTarget {
    pub worker_key: String,
    pub url: String,
}

pub trait MetricsTargetProvider: Send + Sync + std::fmt::Debug {
    fn targets(&self) -> Vec<MetricsTarget>;
}

pub struct SnapshotMetricsTargets {
    state: Arc<dyn StateProvider>,
}

impl SnapshotMetricsTargets {
    pub fn new(state: Arc<dyn StateProvider>) -> Self {
        Self { state }
    }
}

impl std::fmt::Debug for SnapshotMetricsTargets {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotMetricsTargets")
            .finish_non_exhaustive()
    }
}

impl MetricsTargetProvider for SnapshotMetricsTargets {
    fn targets(&self) -> Vec<MetricsTarget> {
        self.state
            .snapshot()
            .workers
            .iter()
            .filter_map(|(worker_key, worker)| {
                let identity = worker.identity.as_ref()?;
                Some(MetricsTarget {
                    worker_key: worker_key.clone(),
                    url: format!("{}/metrics", identity.endpoint.trim_end_matches('/')),
                })
            })
            .collect()
    }
}

#[async_trait]
pub trait MetricsFetcher: Send + Sync + std::fmt::Debug {
    async fn fetch(&self, url: &str) -> Result<String, MetricsError>;
}

#[derive(Debug)]
pub struct ReqwestMetricsFetcher {
    client: reqwest::Client,
}

impl ReqwestMetricsFetcher {
    pub fn new(timeout: Duration) -> Result<Self, MetricsError> {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| MetricsError::Http(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl MetricsFetcher for ReqwestMetricsFetcher {
    async fn fetch(&self, url: &str) -> Result<String, MetricsError> {
        self.client
            .get(url)
            .send()
            .await
            .map_err(|error| MetricsError::Http(error.to_string()))?
            .error_for_status()
            .map_err(|error| MetricsError::Http(error.to_string()))?
            .text()
            .await
            .map_err(|error| MetricsError::Http(error.to_string()))
    }
}

pub trait MetricsPublisher: Send + Sync + std::fmt::Debug {
    fn publish_metrics(&self, worker_key: &str, sample: MetricsSample, fresh_for: Duration);
}

pub struct MetricsScraper {
    targets: Arc<dyn MetricsTargetProvider>,
    fetcher: Arc<dyn MetricsFetcher>,
    publisher: Arc<dyn MetricsPublisher>,
    fresh_for: Duration,
}

impl std::fmt::Debug for MetricsScraper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MetricsScraper")
            .field("fresh_for", &self.fresh_for)
            .finish_non_exhaustive()
    }
}

impl MetricsScraper {
    pub fn new(
        targets: Arc<dyn MetricsTargetProvider>,
        fetcher: Arc<dyn MetricsFetcher>,
        publisher: Arc<dyn MetricsPublisher>,
        fresh_for: Duration,
    ) -> Self {
        Self {
            targets,
            fetcher,
            publisher,
            fresh_for,
        }
    }

    pub async fn scrape_once(&self) -> MetricsScrapeReport {
        let mut report = MetricsScrapeReport::default();
        for target in self.targets.targets() {
            match self
                .fetcher
                .fetch(&target.url)
                .await
                .and_then(|body| parse_vllm_metrics(&body))
            {
                Ok(sample) => {
                    self.publisher
                        .publish_metrics(&target.worker_key, sample, self.fresh_for);
                    report.succeeded += 1;
                }
                Err(error) => {
                    tracing::warn!(
                        worker_key = %target.worker_key,
                        url = %target.url,
                        error = %error,
                        "vLLM metrics scrape failed"
                    );
                    report.failed += 1;
                }
            }
        }
        report
    }

    pub async fn run(&self, interval: Duration, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            self.scrape_once().await;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetricsScrapeReport {
    pub succeeded: usize,
    pub failed: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("metrics HTTP request failed: {0}")]
    Http(String),
    #[error("invalid Prometheus metrics: {0}")]
    Parse(String),
}

impl From<io::Error> for MetricsError {
    fn from(error: io::Error) -> Self {
        Self::Parse(error.to_string())
    }
}
