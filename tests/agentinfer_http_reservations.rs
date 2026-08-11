use agent_router::ReservationLedger;
use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{header::CONTENT_TYPE, StatusCode},
    response::Response,
    routing::{get, post},
    Router as AxumRouter,
};
use futures_util::stream;
use http_body_util::BodyExt;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use vllm_router_rs::config::{
    AgentAwareConfig, PolicyConfig, RetryConfig, RouterConfig, RoutingMode,
};
use vllm_router_rs::policies::AgentAwarePolicy;
use vllm_router_rs::protocols::spec::ChatCompletionRequest;
use vllm_router_rs::routers::http::router::Router;
use vllm_router_rs::server::AppContext;

#[derive(Clone, Copy)]
enum BackendMode {
    RetryThenStream,
    StalledStream,
    NonStreaming,
}

#[derive(Clone)]
struct BackendState {
    mode: BackendMode,
    attempts: Arc<AtomicUsize>,
}

struct TestBackend {
    url: String,
    attempts: Arc<AtomicUsize>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl TestBackend {
    async fn stop(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

async fn spawn_backend(mode: BackendMode) -> TestBackend {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let app = AxumRouter::new()
        .route("/health", get(|| async { StatusCode::OK }))
        .route("/v1/chat/completions", post(infer))
        .with_state(BackendState {
            mode,
            attempts: attempts.clone(),
        });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });
    TestBackend {
        url: format!("http://{address}"),
        attempts,
        shutdown: Some(shutdown_tx),
        task,
    }
}

async fn infer(State(state): State<BackendState>) -> Response<Body> {
    let attempt = state.attempts.fetch_add(1, Ordering::SeqCst);
    match state.mode {
        BackendMode::RetryThenStream if attempt == 0 => Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::from("retry"))
            .unwrap(),
        BackendMode::RetryThenStream => {
            let chunks = stream::iter([
                Ok::<_, Infallible>(Bytes::from_static(b"data: {\"delta\":\"ok\"}\n\n")),
                Ok(Bytes::from_static(b"data: [DONE]\n\n")),
            ]);
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }
        BackendMode::StalledStream => {
            let chunks = stream::pending::<Result<Bytes, Infallible>>();
            Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(chunks))
                .unwrap()
        }
        BackendMode::NonStreaming => Response::builder()
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap(),
    }
}

async fn test_router(worker_url: String, max_retries: u32) -> (Router, Arc<ReservationLedger>) {
    let mut config = RouterConfig::new(
        RoutingMode::Regular {
            worker_urls: vec![worker_url.clone()],
        },
        PolicyConfig::AgentAware {
            config: Box::new(AgentAwareConfig::production(
                "redis://127.0.0.1:1",
                "test-cluster",
                "test-cluster-secret",
            )),
        },
    );
    config.worker_startup_timeout_secs = 2;
    config.worker_startup_check_interval_secs = 1;
    config.retry = RetryConfig {
        max_retries,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
        backoff_multiplier: 1.0,
        jitter_factor: 0.0,
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))
        .build()
        .unwrap();
    let context = Arc::new(AppContext::new(config, client, 128, None, Vec::new()).unwrap());
    let router = Router::new(vec![worker_url], &context).await.unwrap();
    let policy = context.policy_registry.get_default_policy();
    let reservations = policy
        .as_any()
        .downcast_ref::<AgentAwarePolicy>()
        .unwrap()
        .reservations();
    (router, reservations)
}

fn chat_request(stream: bool) -> ChatCompletionRequest {
    serde_json::from_value(serde_json::json!({
        "model": "model-a",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 8,
        "stream": stream
    }))
    .unwrap()
}

async fn wait_for_release(reservations: &ReservationLedger) {
    for _ in 0..50 {
        if reservations.active_reservations() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(reservations.active_reservations(), 0);
}

#[tokio::test]
async fn streaming_retry_and_completion_release_each_attempt() {
    let backend = spawn_backend(BackendMode::RetryThenStream).await;
    let (router, reservations) = test_router(backend.url.clone(), 2).await;

    let response = router
        .route_typed_request(None, &chat_request(true), "/v1/chat/completions", None)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(backend.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(reservations.active_reservations(), 1);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(body.windows(12).any(|window| window == b"data: [DONE]"));
    wait_for_release(&reservations).await;
    backend.stop().await;
}

#[tokio::test]
async fn dropping_stalled_streaming_response_releases_cancelled_reservation() {
    let backend = spawn_backend(BackendMode::StalledStream).await;
    let (router, reservations) = test_router(backend.url.clone(), 1).await;

    let response = router
        .route_typed_request(None, &chat_request(true), "/v1/chat/completions", None)
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(reservations.active_reservations(), 1);
    drop(response);

    wait_for_release(&reservations).await;
    backend.stop().await;
}

#[tokio::test]
async fn transport_error_releases_non_streaming_reservation() {
    let backend = spawn_backend(BackendMode::NonStreaming).await;
    let worker_url = backend.url.clone();
    let (router, reservations) = test_router(worker_url, 1).await;
    backend.stop().await;

    let response = router
        .route_typed_request(None, &chat_request(false), "/v1/chat/completions", None)
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(reservations.active_reservations(), 0);
}
