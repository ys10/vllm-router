use vllm_router_rs::config::{
    AgentAwareConfig, PolicyConfig, RouterConfig, RoutingMode, SecretString,
};

#[test]
fn agent_config_validates_and_redacts_redis_credentials_and_cluster_secret() {
    let policy = PolicyConfig::AgentAware {
        config: Box::new(AgentAwareConfig::production(
            "redis://router-user:redis-password@127.0.0.1:6379",
            "cluster-a",
            "cluster-secret-value",
        )),
    };
    let config = RouterConfig::new(
        RoutingMode::Regular {
            worker_urls: vec!["http://127.0.0.1:8000".to_string()],
        },
        policy,
    );

    config.validate().unwrap();
    let debug = format!("{:?}", config.policy);
    assert!(!debug.contains("redis-password"));
    assert!(!debug.contains("cluster-secret-value"));
    assert!(debug.contains("<redacted>"));

    let serialized = serde_json::to_value(&config.policy).unwrap();
    assert_eq!(serialized["type"], "agent_aware");
    assert_eq!(serialized["config"]["balance_abs_threshold_ms"], 250.0);
    assert_eq!(serialized["config"]["balance_rel_threshold"], 1.2);
    assert_eq!(serialized["config"]["rollout_percent"], 0);
}

#[test]
fn invalid_agent_config_errors_do_not_echo_secrets() {
    let mut agent = AgentAwareConfig::production(
        "not-a-redis-url-with-password",
        "cluster-a",
        "cluster-secret-value",
    );
    agent.cluster_secret = SecretString::new("");
    let config = RouterConfig::new(
        RoutingMode::Regular {
            worker_urls: Vec::new(),
        },
        PolicyConfig::AgentAware {
            config: Box::new(agent),
        },
    );

    let error = config.validate().unwrap_err().to_string();
    assert!(error.contains("<redacted>"));
    assert!(!error.contains("password"));
    assert!(!error.contains("cluster-secret-value"));
}

#[test]
fn agent_policy_is_rejected_outside_regular_http_mode() {
    let agent = AgentAwareConfig::production(
        "redis://127.0.0.1:6379",
        "cluster-a",
        "cluster-secret-value",
    );
    let config = RouterConfig::new(
        RoutingMode::OpenAI {
            worker_urls: vec!["http://127.0.0.1:8000".to_string()],
        },
        PolicyConfig::AgentAware {
            config: Box::new(agent),
        },
    );

    let error = config.validate().unwrap_err().to_string();
    assert!(error.contains("regular HTTP routing only"));
}

#[test]
fn agent_rollout_rejects_percentages_outside_the_declared_stages() {
    let mut agent = AgentAwareConfig::production(
        "redis://127.0.0.1:6379",
        "cluster-a",
        "cluster-secret-value",
    );
    agent.rollout_percent = 10;
    let config = RouterConfig::new(
        RoutingMode::Regular {
            worker_urls: vec!["http://127.0.0.1:8000".to_string()],
        },
        PolicyConfig::AgentAware {
            config: Box::new(agent),
        },
    );

    let error = config.validate().unwrap_err().to_string();
    assert!(error.contains("0, 1, 5, 25, 100"));
    assert!(!error.contains("cluster-secret-value"));
}
