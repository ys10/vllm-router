use super::*;

/// Configuration validator
pub struct ConfigValidator;

impl ConfigValidator {
    /// Validate a complete router configuration
    pub fn validate(config: &RouterConfig) -> ConfigResult<()> {
        // Check if service discovery is enabled (either via discovery config or vLLM mode)
        let has_service_discovery = config.discovery.as_ref().is_some_and(|d| d.enabled)
            || matches!(
                &config.mode,
                RoutingMode::VllmPrefillDecode {
                    discovery_address: Some(_),
                    ..
                }
            );

        Self::validate_mode(&config.mode, has_service_discovery)?;
        Self::validate_policy(&config.policy)?;
        Self::validate_server_settings(config)?;

        if let Some(discovery) = &config.discovery {
            Self::validate_discovery(discovery, &config.mode)?;
        }

        if let Some(metrics) = &config.metrics {
            Self::validate_metrics(metrics)?;
        }

        Self::validate_compatibility(config)?;

        // Validate effective retry/CB configs (respect disable flags)
        let retry_cfg = config.effective_retry_config();
        let cb_cfg = config.effective_circuit_breaker_config();
        Self::validate_retry(&retry_cfg)?;
        Self::validate_circuit_breaker(&cb_cfg)?;

        Ok(())
    }

    /// Validate routing mode configuration
    fn validate_mode(mode: &RoutingMode, has_service_discovery: bool) -> ConfigResult<()> {
        match mode {
            RoutingMode::Regular { worker_urls } => {
                // Validate URLs if any are provided
                if !worker_urls.is_empty() {
                    Self::validate_urls(worker_urls)?;
                }
                // Note: We allow empty worker URLs even without service discovery
                // to let the router start and fail at runtime when routing requests.
                // This matches legacy behavior and test expectations.
            }
            RoutingMode::VllmPrefillDecode {
                prefill_urls,
                decode_urls,
                prefill_policy,
                decode_policy,
                discovery_address: _,
            } => {
                // Only require URLs if service discovery is disabled
                if !has_service_discovery {
                    if prefill_urls.is_empty() {
                        return Err(ConfigError::ValidationFailed {
                            reason: "vLLM PD mode requires at least one prefill worker URL"
                                .to_string(),
                        });
                    }
                    if decode_urls.is_empty() {
                        return Err(ConfigError::ValidationFailed {
                            reason: "vLLM PD mode requires at least one decode worker URL"
                                .to_string(),
                        });
                    }
                }

                // Validate URLs if any are provided
                if !prefill_urls.is_empty() {
                    let prefill_url_strings: Vec<String> =
                        prefill_urls.iter().map(|(url, _)| url.clone()).collect();
                    Self::validate_urls(&prefill_url_strings)?;
                }
                if !decode_urls.is_empty() {
                    Self::validate_urls(decode_urls)?;
                }

                // Validate bootstrap ports
                for (_url, port) in prefill_urls {
                    if let Some(port) = port {
                        if *port == 0 {
                            return Err(ConfigError::InvalidValue {
                                field: "bootstrap_port".to_string(),
                                value: port.to_string(),
                                reason: "Port must be between 1 and 65535".to_string(),
                            });
                        }
                    }
                }

                // Validate optional prefill and decode policies
                if let Some(p_policy) = prefill_policy {
                    Self::validate_policy(p_policy)?;
                }
                if let Some(d_policy) = decode_policy {
                    Self::validate_policy(d_policy)?;
                }
            }
            RoutingMode::OpenAI { worker_urls } => {
                // Require exactly one worker URL for OpenAI router
                if worker_urls.len() != 1 {
                    return Err(ConfigError::ValidationFailed {
                        reason: "OpenAI mode requires exactly one --worker-urls entry".to_string(),
                    });
                }
                // Validate URL format
                if let Err(e) = url::Url::parse(&worker_urls[0]) {
                    return Err(ConfigError::ValidationFailed {
                        reason: format!("Invalid OpenAI worker URL '{}': {}", worker_urls[0], e),
                    });
                }
            }
        }
        Ok(())
    }

    /// Validate policy configuration
    fn validate_policy(policy: &PolicyConfig) -> ConfigResult<()> {
        match policy {
            PolicyConfig::Random | PolicyConfig::RoundRobin => {
                // No specific validation needed
            }
            PolicyConfig::CacheAware {
                cache_threshold,
                balance_abs_threshold: _,
                balance_rel_threshold,
                eviction_interval_secs,
                max_tree_size,
            } => {
                if !(0.0..=1.0).contains(cache_threshold) {
                    return Err(ConfigError::InvalidValue {
                        field: "cache_threshold".to_string(),
                        value: cache_threshold.to_string(),
                        reason: "Must be between 0.0 and 1.0".to_string(),
                    });
                }

                if *balance_rel_threshold < 1.0 {
                    return Err(ConfigError::InvalidValue {
                        field: "balance_rel_threshold".to_string(),
                        value: balance_rel_threshold.to_string(),
                        reason: "Must be >= 1.0".to_string(),
                    });
                }

                if *eviction_interval_secs == 0 {
                    return Err(ConfigError::InvalidValue {
                        field: "eviction_interval_secs".to_string(),
                        value: eviction_interval_secs.to_string(),
                        reason: "Must be > 0".to_string(),
                    });
                }

                if *max_tree_size == 0 {
                    return Err(ConfigError::InvalidValue {
                        field: "max_tree_size".to_string(),
                        value: max_tree_size.to_string(),
                        reason: "Must be > 0".to_string(),
                    });
                }
            }
            PolicyConfig::PowerOfTwo {
                load_check_interval_secs,
            } => {
                if *load_check_interval_secs == 0 {
                    return Err(ConfigError::InvalidValue {
                        field: "load_check_interval_secs".to_string(),
                        value: load_check_interval_secs.to_string(),
                        reason: "Must be > 0".to_string(),
                    });
                }
            }
            PolicyConfig::ConsistentHash { virtual_nodes } => {
                if *virtual_nodes == 0 {
                    return Err(ConfigError::InvalidValue {
                        field: "virtual_nodes".to_string(),
                        value: virtual_nodes.to_string(),
                        reason: "Must be > 0".to_string(),
                    });
                }
            }
            PolicyConfig::RendezvousHash => {
                // No specific validation needed
            }
            PolicyConfig::AgentAware { config } => {
                agent_router::RolloutPercent::new(config.rollout_percent).map_err(|error| {
                    ConfigError::InvalidValue {
                        field: "agent_aware.rollout_percent".to_string(),
                        value: config.rollout_percent.to_string(),
                        reason: error.to_string(),
                    }
                })?;
                let redis_url =
                    url::Url::parse(config.redis_url.expose_secret()).map_err(|_| {
                        ConfigError::InvalidValue {
                            field: "agent_aware.redis_url".to_string(),
                            value: "<redacted>".to_string(),
                            reason: "Must be a valid redis:// or rediss:// URL".to_string(),
                        }
                    })?;
                if !matches!(redis_url.scheme(), "redis" | "rediss") {
                    return Err(ConfigError::InvalidValue {
                        field: "agent_aware.redis_url".to_string(),
                        value: "<redacted>".to_string(),
                        reason: "Must use the redis or rediss scheme".to_string(),
                    });
                }
                if config.cluster_secret.is_empty() {
                    return Err(ConfigError::MissingRequired {
                        field: "agent_aware.cluster_secret".to_string(),
                    });
                }
                if config.cluster_id.len() > 128
                    || !config
                        .cluster_id
                        .bytes()
                        .next()
                        .is_some_and(|byte| byte.is_ascii_alphanumeric())
                    || config
                        .cluster_id
                        .bytes()
                        .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-".contains(&byte)))
                {
                    return Err(ConfigError::InvalidValue {
                        field: "agent_aware.cluster_id".to_string(),
                        value: config.cluster_id.clone(),
                        reason: "Must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}".to_string(),
                    });
                }
                if !config.balance_abs_threshold_ms.is_finite()
                    || config.balance_abs_threshold_ms < 0.0
                {
                    return Err(ConfigError::InvalidValue {
                        field: "agent_aware.balance_abs_threshold_ms".to_string(),
                        value: config.balance_abs_threshold_ms.to_string(),
                        reason: "Must be finite and >= 0".to_string(),
                    });
                }
                if !config.balance_rel_threshold.is_finite() || config.balance_rel_threshold < 1.0 {
                    return Err(ConfigError::InvalidValue {
                        field: "agent_aware.balance_rel_threshold".to_string(),
                        value: config.balance_rel_threshold.to_string(),
                        reason: "Must be finite and >= 1.0".to_string(),
                    });
                }
                if !config.dram_block_restore_ms.is_finite() || config.dram_block_restore_ms < 0.0 {
                    return Err(ConfigError::InvalidValue {
                        field: "agent_aware.dram_block_restore_ms".to_string(),
                        value: config.dram_block_restore_ms.to_string(),
                        reason: "Must be finite and >= 0".to_string(),
                    });
                }
                for (field, value) in [
                    ("minimum_reservation_ms", config.minimum_reservation_ms),
                    ("fallback_reservation_ms", config.fallback_reservation_ms),
                    (
                        "metrics_scrape_interval_ms",
                        config.metrics_scrape_interval_ms,
                    ),
                    ("metrics_timeout_ms", config.metrics_timeout_ms),
                    ("metrics_fresh_ms", config.metrics_fresh_ms),
                    ("expiry_interval_ms", config.expiry_interval_ms),
                ] {
                    if value == 0 {
                        return Err(ConfigError::InvalidValue {
                            field: format!("agent_aware.{field}"),
                            value: value.to_string(),
                            reason: "Must be > 0".to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Validate server configuration
    fn validate_server_settings(config: &RouterConfig) -> ConfigResult<()> {
        if config.port == 0 {
            return Err(ConfigError::InvalidValue {
                field: "port".to_string(),
                value: config.port.to_string(),
                reason: "Port must be > 0".to_string(),
            });
        }

        if config.max_payload_size == 0 {
            return Err(ConfigError::InvalidValue {
                field: "max_payload_size".to_string(),
                value: config.max_payload_size.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }

        if config.request_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "request_timeout_secs".to_string(),
                value: config.request_timeout_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }

        if config.worker_startup_timeout_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "worker_startup_timeout_secs".to_string(),
                value: config.worker_startup_timeout_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }

        if config.worker_startup_check_interval_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "worker_startup_check_interval_secs".to_string(),
                value: config.worker_startup_check_interval_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }

        Ok(())
    }

    /// Validate service discovery configuration
    fn validate_discovery(discovery: &DiscoveryConfig, mode: &RoutingMode) -> ConfigResult<()> {
        if !discovery.enabled {
            return Ok(()); // No validation needed if disabled
        }

        if discovery.port == 0 {
            return Err(ConfigError::InvalidValue {
                field: "discovery.port".to_string(),
                value: discovery.port.to_string(),
                reason: "Port must be > 0".to_string(),
            });
        }

        if discovery.check_interval_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "discovery.check_interval_secs".to_string(),
                value: discovery.check_interval_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }

        // Validate selectors based on mode
        match mode {
            RoutingMode::Regular { .. } => {
                if discovery.selector.is_empty() {
                    return Err(ConfigError::ValidationFailed {
                        reason: "Regular mode with service discovery requires a non-empty selector"
                            .to_string(),
                    });
                }
            }
            RoutingMode::VllmPrefillDecode { .. } => {
                if discovery.prefill_selector.is_empty() && discovery.decode_selector.is_empty() {
                    return Err(ConfigError::ValidationFailed {
                        reason: "vLLM PD mode with service discovery requires at least one non-empty selector (prefill or decode)".to_string(),
                    });
                }
            }
            RoutingMode::OpenAI { .. } => {
                // OpenAI mode doesn't use service discovery
                return Err(ConfigError::ValidationFailed {
                    reason: "OpenAI mode does not support service discovery".to_string(),
                });
            }
        }

        Ok(())
    }

    /// Validate metrics configuration
    fn validate_metrics(metrics: &MetricsConfig) -> ConfigResult<()> {
        if metrics.port == 0 {
            return Err(ConfigError::InvalidValue {
                field: "metrics.port".to_string(),
                value: metrics.port.to_string(),
                reason: "Port must be > 0".to_string(),
            });
        }

        if metrics.host.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "metrics.host".to_string(),
                value: metrics.host.clone(),
                reason: "Host cannot be empty".to_string(),
            });
        }

        Ok(())
    }

    /// Validate retry configuration
    fn validate_retry(retry: &RetryConfig) -> ConfigResult<()> {
        if retry.max_retries < 1 {
            return Err(ConfigError::InvalidValue {
                field: "retry.max_retries".to_string(),
                value: retry.max_retries.to_string(),
                reason: "Must be >= 1 (set to 1 to effectively disable retries)".to_string(),
            });
        }
        if retry.initial_backoff_ms == 0 {
            return Err(ConfigError::InvalidValue {
                field: "retry.initial_backoff_ms".to_string(),
                value: retry.initial_backoff_ms.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }
        if retry.max_backoff_ms < retry.initial_backoff_ms {
            return Err(ConfigError::InvalidValue {
                field: "retry.max_backoff_ms".to_string(),
                value: retry.max_backoff_ms.to_string(),
                reason: "Must be >= initial_backoff_ms".to_string(),
            });
        }
        if retry.backoff_multiplier < 1.0 {
            return Err(ConfigError::InvalidValue {
                field: "retry.backoff_multiplier".to_string(),
                value: retry.backoff_multiplier.to_string(),
                reason: "Must be >= 1.0".to_string(),
            });
        }
        if !(0.0..=1.0).contains(&retry.jitter_factor) {
            return Err(ConfigError::InvalidValue {
                field: "retry.jitter_factor".to_string(),
                value: retry.jitter_factor.to_string(),
                reason: "Must be between 0.0 and 1.0".to_string(),
            });
        }
        Ok(())
    }

    /// Validate circuit breaker configuration
    fn validate_circuit_breaker(cb: &CircuitBreakerConfig) -> ConfigResult<()> {
        if cb.failure_threshold < 1 {
            return Err(ConfigError::InvalidValue {
                field: "circuit_breaker.failure_threshold".to_string(),
                value: cb.failure_threshold.to_string(),
                reason: "Must be >= 1 (set to u32::MAX to effectively disable CB)".to_string(),
            });
        }
        if cb.success_threshold < 1 {
            return Err(ConfigError::InvalidValue {
                field: "circuit_breaker.success_threshold".to_string(),
                value: cb.success_threshold.to_string(),
                reason: "Must be >= 1".to_string(),
            });
        }
        if cb.timeout_duration_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "circuit_breaker.timeout_duration_secs".to_string(),
                value: cb.timeout_duration_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }
        if cb.window_duration_secs == 0 {
            return Err(ConfigError::InvalidValue {
                field: "circuit_breaker.window_duration_secs".to_string(),
                value: cb.window_duration_secs.to_string(),
                reason: "Must be > 0".to_string(),
            });
        }
        Ok(())
    }

    /// Validate compatibility between different configuration sections
    fn validate_compatibility(config: &RouterConfig) -> ConfigResult<()> {
        let is_agent_aware =
            |policy: &PolicyConfig| matches!(policy, PolicyConfig::AgentAware { .. });
        match &config.mode {
            RoutingMode::Regular { .. } => {}
            RoutingMode::VllmPrefillDecode { decode_policy, .. } => {
                // Agent-aware chooses the Prefill worker; decode keeps its own policy.
                if is_agent_aware(decode_policy.as_ref().unwrap_or(&config.policy)) {
                    return Err(ConfigError::IncompatibleConfig {
                        reason: "Agent-aware policy can route vLLM PD prefill only; set a non-agent-aware --decode-policy"
                            .to_string(),
                    });
                }
            }
            _ => {
                if is_agent_aware(&config.policy) {
                    return Err(ConfigError::IncompatibleConfig {
                        reason: "Agent-aware policy supports regular and vLLM PD routing only"
                            .to_string(),
                    });
                }
            }
        }

        // IGW mode is independent - skip other compatibility checks when enabled
        if config.enable_igw {
            return Ok(());
        }

        // All policies are now supported for both router types thanks to the unified trait design
        // No mode/policy restrictions needed anymore

        // Check if service discovery is enabled for worker count validation.
        // This covers both K8s service discovery (config.discovery) and vLLM ZMQ
        // service discovery (VllmPrefillDecode { discovery_address: Some(_) }).
        let has_vllm_discovery = matches!(
            &config.mode,
            RoutingMode::VllmPrefillDecode {
                discovery_address: Some(_),
                ..
            }
        );
        let has_service_discovery =
            config.discovery.as_ref().is_some_and(|d| d.enabled) || has_vllm_discovery;

        // Only validate worker counts if service discovery is disabled
        if !has_service_discovery {
            // Check if power-of-two policy makes sense with insufficient workers
            if let PolicyConfig::PowerOfTwo { .. } = &config.policy {
                let worker_count = config.mode.worker_count();
                if worker_count < 2 {
                    return Err(ConfigError::IncompatibleConfig {
                        reason: "Power-of-two policy requires at least 2 workers".to_string(),
                    });
                }
            }

            // For vLLM PD mode, validate that policies have sufficient workers
            if let RoutingMode::VllmPrefillDecode {
                prefill_urls,
                decode_urls,
                prefill_policy,
                decode_policy,
                ..
            } = &config.mode
            {
                // Check power-of-two for prefill
                if let Some(PolicyConfig::PowerOfTwo { .. }) = prefill_policy {
                    if prefill_urls.len() < 2 {
                        return Err(ConfigError::IncompatibleConfig {
                            reason: "Power-of-two policy for prefill requires at least 2 prefill workers".to_string(),
                        });
                    }
                }

                // Check power-of-two for decode
                if let Some(PolicyConfig::PowerOfTwo { .. }) = decode_policy {
                    if decode_urls.len() < 2 {
                        return Err(ConfigError::IncompatibleConfig {
                            reason:
                                "Power-of-two policy for decode requires at least 2 decode workers"
                                    .to_string(),
                        });
                    }
                }
            }
        }

        // DP-aware routing is now automatically enabled when data_parallel_size > 1
        // and is compatible with service discovery

        // MoRI-IO requires service discovery: ZMQ addresses are obtained via instance
        // registration and are not available in direct URL mode.
        if config.kv_connector == KvConnector::MoriIO && !has_vllm_discovery {
            return Err(ConfigError::IncompatibleConfig {
                reason: "MoRI-IO KV connector requires service discovery to be enabled \
                         (ZMQ addresses are obtained via instance registration). Please \
                        run with `--vllm-discovery-address ${address}`"
                    .to_string(),
            });
        }

        Ok(())
    }

    /// Validate URL format
    fn validate_urls(urls: &[String]) -> ConfigResult<()> {
        for url in urls {
            if url.is_empty() {
                return Err(ConfigError::InvalidValue {
                    field: "worker_url".to_string(),
                    value: url.clone(),
                    reason: "URL cannot be empty".to_string(),
                });
            }

            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(ConfigError::InvalidValue {
                    field: "worker_url".to_string(),
                    value: url.clone(),
                    reason: "URL must start with http:// or https://".to_string(),
                });
            }

            // Basic URL validation
            match ::url::Url::parse(url) {
                Ok(parsed) => {
                    // Additional validation
                    if parsed.host_str().is_none() {
                        return Err(ConfigError::InvalidValue {
                            field: "worker_url".to_string(),
                            value: url.clone(),
                            reason: "URL must have a valid host".to_string(),
                        });
                    }
                }
                Err(e) => {
                    return Err(ConfigError::InvalidValue {
                        field: "worker_url".to_string(),
                        value: url.clone(),
                        reason: format!("Invalid URL format: {}", e),
                    });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_regular_mode() {
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec!["http://worker:8000".to_string()],
            },
            PolicyConfig::Random,
        );

        assert!(ConfigValidator::validate(&config).is_ok());
    }

    #[test]
    fn test_validate_empty_worker_urls() {
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec![],
            },
            PolicyConfig::Random,
        );

        // Empty worker URLs are now allowed to match legacy behavior
        assert!(ConfigValidator::validate(&config).is_ok());
    }

    #[test]
    fn test_validate_empty_worker_urls_with_service_discovery() {
        let mut config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec![],
            },
            PolicyConfig::Random,
        );

        // Enable service discovery
        config.discovery = Some(DiscoveryConfig {
            enabled: true,
            selector: vec![("app".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        });

        // Should pass validation since service discovery is enabled
        assert!(ConfigValidator::validate(&config).is_ok());
    }

    #[test]
    fn test_validate_invalid_urls() {
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec!["invalid-url".to_string()],
            },
            PolicyConfig::Random,
        );

        assert!(ConfigValidator::validate(&config).is_err());
    }

    #[test]
    fn test_validate_cache_aware_thresholds() {
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec![
                    "http://worker1:8000".to_string(),
                    "http://worker2:8000".to_string(),
                ],
            },
            PolicyConfig::CacheAware {
                cache_threshold: 1.5, // Invalid: > 1.0
                balance_abs_threshold: 32,
                balance_rel_threshold: 1.1,
                eviction_interval_secs: 60,
                max_tree_size: 1000,
            },
        );

        assert!(ConfigValidator::validate(&config).is_err());
    }

    #[test]
    fn test_validate_cache_aware_single_worker() {
        // Cache-aware with single worker should be allowed (even if not optimal)
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec!["http://worker1:8000".to_string()],
            },
            PolicyConfig::CacheAware {
                cache_threshold: 0.5,
                balance_abs_threshold: 32,
                balance_rel_threshold: 1.1,
                eviction_interval_secs: 60,
                max_tree_size: 1000,
            },
        );

        assert!(ConfigValidator::validate(&config).is_ok());
    }

    #[test]
    fn test_validate_pd_mode() {
        let config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![("http://prefill:8000".to_string(), Some(8081))],
                decode_urls: vec!["http://decode:8000".to_string()],
                prefill_policy: None,
                decode_policy: None,
                discovery_address: None,
            },
            PolicyConfig::Random,
        );

        assert!(ConfigValidator::validate(&config).is_ok());
    }

    #[test]
    fn test_validate_roundrobin_with_pd_mode() {
        // RoundRobin with PD mode is now supported
        let config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![("http://prefill:8000".to_string(), None)],
                decode_urls: vec!["http://decode:8000".to_string()],
                prefill_policy: None,
                decode_policy: None,
                discovery_address: None,
            },
            PolicyConfig::RoundRobin,
        );

        let result = ConfigValidator::validate(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_cache_aware_with_pd_mode() {
        // CacheAware with PD mode is now supported
        let config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![("http://prefill:8000".to_string(), None)],
                decode_urls: vec!["http://decode:8000".to_string()],
                prefill_policy: None,
                decode_policy: None,
                discovery_address: None,
            },
            PolicyConfig::CacheAware {
                cache_threshold: 0.5,
                balance_abs_threshold: 32,
                balance_rel_threshold: 1.1,
                eviction_interval_secs: 60,
                max_tree_size: 1000,
            },
        );

        let result = ConfigValidator::validate(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_power_of_two_with_regular_mode() {
        // PowerOfTwo with Regular mode is now supported
        let config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec![
                    "http://worker1:8000".to_string(),
                    "http://worker2:8000".to_string(),
                ],
            },
            PolicyConfig::PowerOfTwo {
                load_check_interval_secs: 60,
            },
        );

        let result = ConfigValidator::validate(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_pd_mode_with_separate_policies() {
        // Test PD mode with different policies for prefill and decode
        let config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![
                    ("http://prefill1:8000".to_string(), None),
                    ("http://prefill2:8000".to_string(), None),
                ],
                decode_urls: vec![
                    "http://decode1:8000".to_string(),
                    "http://decode2:8000".to_string(),
                ],
                prefill_policy: Some(PolicyConfig::CacheAware {
                    cache_threshold: 0.5,
                    balance_abs_threshold: 32,
                    balance_rel_threshold: 1.1,
                    eviction_interval_secs: 60,
                    max_tree_size: 1000,
                }),
                decode_policy: Some(PolicyConfig::PowerOfTwo {
                    load_check_interval_secs: 60,
                }),
                discovery_address: None,
            },
            PolicyConfig::Random, // Main policy as fallback
        );

        let result = ConfigValidator::validate(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_pd_mode_power_of_two_insufficient_workers() {
        // Test that power-of-two policy requires at least 2 workers
        let config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![("http://prefill1:8000".to_string(), None)], // Only 1 prefill
                decode_urls: vec![
                    "http://decode1:8000".to_string(),
                    "http://decode2:8000".to_string(),
                ],
                prefill_policy: Some(PolicyConfig::PowerOfTwo {
                    load_check_interval_secs: 60,
                }), // Requires 2+ workers
                decode_policy: None,
                discovery_address: None,
            },
            PolicyConfig::Random,
        );

        let result = ConfigValidator::validate(&config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("prefill requires at least 2"));
        }
    }

    #[test]
    fn test_moriio_requires_service_discovery() {
        let mut config = RouterConfig::new(
            RoutingMode::Regular {
                worker_urls: vec!["http://worker:8000".to_string()],
            },
            PolicyConfig::Random,
        );
        config.kv_connector = KvConnector::MoriIO;
        config.discovery = None;

        let result = ConfigValidator::validate(&config);
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e
                .to_string()
                .contains("MoRI-IO KV connector requires service discovery"));
        }
    }

    #[test]
    fn test_moriio_with_service_discovery_is_valid() {
        let mut config = RouterConfig::new(
            RoutingMode::VllmPrefillDecode {
                prefill_urls: vec![],
                decode_urls: vec![],
                prefill_policy: None,
                decode_policy: None,
                discovery_address: Some("0.0.0.0:36367".to_string()),
            },
            PolicyConfig::Random,
        );
        config.kv_connector = KvConnector::MoriIO;

        let result = ConfigValidator::validate(&config);
        assert!(result.is_ok());
    }
}
