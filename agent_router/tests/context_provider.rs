use agent_router::{
    ClusterSnapshot, MockStateProvider, RequestOutcome, RoutingContext, SessionContext,
    StateProvider,
};
use serde_json::json;
use std::collections::HashMap;

#[test]
fn routing_context_derives_protocol_session_key_without_storing_it_in_redis() {
    let context = RoutingContext::new(
        "/v1/chat/completions",
        Some("model-a".to_string()),
        Some("session-routing-text".to_string()),
        true,
    )
    .with_request_id("request-7")
    .with_decode_tokens(128)
    .with_session(SessionContext {
        tenant_id: "tenant-a".to_string(),
        session_id: "session-a".to_string(),
        session_epoch: Some(3),
        expected_parent_prefix_digest: Some("a".repeat(64)),
        model_revision: Some("revision-a".to_string()),
        tokenizer_revision: Some("tokenizer-a".to_string()),
    });

    assert_eq!(context.request_id.as_deref(), Some("request-7"));
    assert_eq!(context.max_decode_tokens, Some(128));
    let debug = format!("{context:?}");
    assert!(!debug.contains("session-a"));
    assert!(!debug.contains("session-routing-text"));
    assert_eq!(
        context.session_key(b"cluster-secret").unwrap(),
        "a97d79affa503b679e43dd1ff99922fb058535fd1fa9610d8604f3086deb24af"
    );
}

#[test]
fn session_context_requires_tenant_and_session_and_reads_optional_version_fields() {
    let params: HashMap<String, serde_json::Value> = serde_json::from_value(json!({
        "tenant_id": "tenant-a",
        "session_id": "session-a",
        "session_epoch": 4,
        "expected_parent_prefix_digest": "d".repeat(64),
        "model_revision": "revision-a",
        "tokenizer_revision": "tokenizer-a"
    }))
    .unwrap();

    let session = SessionContext::from_params(&params).unwrap();

    assert_eq!(session.session_epoch, Some(4));
    assert_eq!(session.expected_parent_prefix_digest, Some("d".repeat(64)));
    assert_eq!(session.model_revision.as_deref(), Some("revision-a"));
    assert!(RequestOutcome::Succeeded.is_success());
    assert!(!RequestOutcome::Cancelled.is_success());
}

#[test]
fn mock_provider_publishes_immutable_snapshots() {
    let provider = MockStateProvider::default();
    let original = provider.snapshot();

    provider.publish(ClusterSnapshot {
        version: 7,
        ..ClusterSnapshot::default()
    });

    assert_eq!(original.version, 0);
    assert_eq!(provider.snapshot().version, 7);
}
