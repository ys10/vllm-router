use agent_router::{RequestOutcome, RoutingContext};
use std::sync::{Arc, Mutex};
use vllm_router_rs::core::{BasicWorker, Worker, WorkerType};
use vllm_router_rs::policies::{CacheAwareConfig, CacheAwarePolicy};
use vllm_router_rs::policies::{LoadBalancingPolicy, RequestHeaders, RequestLifecycle};
use vllm_router_rs::protocols::spec::{ChatCompletionRequest, GenerationRequest};

#[derive(Debug, Default)]
struct RecordingPolicy {
    starts: Mutex<usize>,
    finishes: Mutex<Vec<RequestOutcome>>,
}

impl LoadBalancingPolicy for RecordingPolicy {
    fn select_worker_with_headers(
        &self,
        _workers: &[Arc<dyn Worker>],
        _request_text: Option<&str>,
        _headers: Option<&RequestHeaders>,
    ) -> Option<usize> {
        Some(0)
    }

    fn on_request_start(&self, _worker: &Arc<dyn Worker>, _context: &RoutingContext) {
        *self.starts.lock().unwrap() += 1;
    }

    fn on_request_finish(
        &self,
        _worker: &Arc<dyn Worker>,
        _context: &RoutingContext,
        outcome: RequestOutcome,
    ) {
        self.finishes.lock().unwrap().push(outcome);
    }

    fn name(&self) -> &'static str {
        "recording"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[test]
fn lifecycle_finishes_once_and_drop_reports_cancellation() {
    let policy = Arc::new(RecordingPolicy::default());
    let worker: Arc<dyn Worker> = Arc::new(BasicWorker::new(
        "http://worker-a:8000".to_string(),
        WorkerType::Regular,
    ));
    let context = Arc::new(RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        None,
        false,
    ));

    {
        let mut lifecycle =
            RequestLifecycle::start(policy.clone(), worker.clone(), context.clone());
        lifecycle.finish(RequestOutcome::Succeeded);
        lifecycle.finish(RequestOutcome::ServerError);
    }
    {
        let _cancelled = RequestLifecycle::start(policy.clone(), worker, context);
    }

    assert_eq!(*policy.starts.lock().unwrap(), 2);
    assert_eq!(
        *policy.finishes.lock().unwrap(),
        vec![RequestOutcome::Succeeded, RequestOutcome::Cancelled]
    );
}

#[test]
fn cache_aware_tracks_inflight_load_through_the_generic_lifecycle() {
    let policy: Arc<dyn LoadBalancingPolicy> =
        Arc::new(CacheAwarePolicy::with_config(CacheAwareConfig {
            eviction_interval_secs: 0,
            ..CacheAwareConfig::default()
        }));
    let worker: Arc<dyn Worker> = Arc::new(BasicWorker::new(
        "http://worker-a:8000".to_string(),
        WorkerType::Regular,
    ));
    let context = Arc::new(RoutingContext::default());

    let mut lifecycle = RequestLifecycle::start(policy, worker.clone(), context);
    assert_eq!(worker.load(), 1);

    lifecycle.finish(RequestOutcome::TransportError);
    assert_eq!(worker.load(), 0);
    drop(lifecycle);
    assert_eq!(worker.load(), 0);
}

#[test]
fn chat_request_builds_structured_routing_context() {
    let request: ChatCompletionRequest = serde_json::from_value(serde_json::json!({
        "model": "model-a",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 128,
        "stream": true,
        "session_params": {
            "tenant_id": "tenant-a",
            "session_id": "session-a",
            "session_epoch": 5,
            "expected_parent_prefix_digest": "e".repeat(64)
        }
    }))
    .unwrap();

    let context = request.routing_context("/v1/chat/completions", Some("model-a"));

    assert_eq!(context.model_id.as_deref(), Some("model-a"));
    assert_eq!(context.max_decode_tokens, Some(128));
    assert!(context.is_stream);
    assert_eq!(context.session.unwrap().session_epoch, Some(5));
}
