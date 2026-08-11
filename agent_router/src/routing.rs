use crate::{RoutingContext, ServerLoadPayload, ServerLoadState, SessionPlacementState};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BalanceThresholds {
    pub absolute_ms: f64,
    pub relative: f64,
}

impl BalanceThresholds {
    pub fn new(absolute_ms: f64, relative: f64) -> Result<Self, RoutingError> {
        if !absolute_ms.is_finite() || absolute_ms < 0.0 {
            return Err(RoutingError::InvalidConfig(
                "absolute load threshold must be finite and non-negative",
            ));
        }
        if !relative.is_finite() || relative < 1.0 {
            return Err(RoutingError::InvalidConfig(
                "relative load threshold must be finite and at least one",
            ));
        }
        Ok(Self {
            absolute_ms,
            relative,
        })
    }
}

impl Default for BalanceThresholds {
    fn default() -> Self {
        Self {
            absolute_ms: 250.0,
            relative: 1.2,
        }
    }
}

pub fn maximal_balanced_subset(
    projected_loads_ms: &[f64],
    thresholds: BalanceThresholds,
) -> Result<Vec<usize>, RoutingError> {
    BalanceThresholds::new(thresholds.absolute_ms, thresholds.relative)?;
    if projected_loads_ms
        .iter()
        .any(|load| !load.is_finite() || *load < 0.0)
    {
        return Err(RoutingError::InvalidLoad(
            "projected loads must be finite and non-negative",
        ));
    }
    let Some(minimum) = projected_loads_ms.iter().copied().reduce(f64::min) else {
        return Ok(Vec::new());
    };
    let cutoff = (minimum + thresholds.absolute_ms).max(minimum * thresholds.relative);
    let mut balanced: Vec<usize> = projected_loads_ms
        .iter()
        .enumerate()
        .filter_map(|(index, load)| (*load <= cutoff).then_some(index))
        .collect();
    balanced.sort_by(|left, right| {
        projected_loads_ms[*left]
            .total_cmp(&projected_loads_ms[*right])
            .then_with(|| left.cmp(right))
    });
    Ok(balanced)
}

pub fn projected_server_load_ms(payload: &ServerLoadPayload) -> Result<f64, RoutingError> {
    validate_rate(payload.prefill_tokens_per_second_ewma, "prefill throughput")?;
    validate_rate(payload.decode_tokens_per_second_ewma, "decode throughput")?;
    let prefill_tokens = payload
        .waiting_prefill_tokens
        .saturating_add(payload.running_prefill_tokens_remaining);
    let decode_tokens = payload
        .waiting_decode_tokens_remaining
        .saturating_add(payload.running_decode_tokens_remaining);
    Ok(
        milliseconds(prefill_tokens, payload.prefill_tokens_per_second_ewma)?
            + milliseconds(decode_tokens, payload.decode_tokens_per_second_ewma)?,
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HbmDramScorerConfig {
    pub default_prefill_tokens: u64,
    pub default_decode_tokens: u64,
    pub dram_block_restore_ms: f64,
    pub minimum_reservation_ms: f64,
}

impl Default for HbmDramScorerConfig {
    fn default() -> Self {
        Self {
            default_prefill_tokens: 512,
            default_decode_tokens: 128,
            dram_block_restore_ms: 0.1,
            minimum_reservation_ms: 1.0,
        }
    }
}

impl HbmDramScorerConfig {
    fn validate(self) -> Result<Self, RoutingError> {
        if !self.dram_block_restore_ms.is_finite() || self.dram_block_restore_ms < 0.0 {
            return Err(RoutingError::InvalidConfig(
                "DRAM block restore cost must be finite and non-negative",
            ));
        }
        if !self.minimum_reservation_ms.is_finite() || self.minimum_reservation_ms <= 0.0 {
            return Err(RoutingError::InvalidConfig(
                "minimum reservation must be finite and positive",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RequestScore {
    pub estimated_service_ms: f64,
    pub hbm_reused_tokens: u64,
    pub dram_reused_tokens: u64,
    pub hbm_saved_ms: f64,
    pub dram_net_saved_ms: f64,
    pub dram_restore_ms: f64,
}

pub trait RequestScorer: Send + Sync + std::fmt::Debug {
    fn score(
        &self,
        context: &RoutingContext,
        load: &ServerLoadState,
        placement: Option<&SessionPlacementState>,
    ) -> Result<RequestScore, RoutingError>;
}

#[derive(Clone)]
pub struct HbmDramScorer {
    cluster_secret: Vec<u8>,
    config: HbmDramScorerConfig,
}

impl std::fmt::Debug for HbmDramScorer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HbmDramScorer")
            .field("cluster_secret", &"<redacted>")
            .field("config", &self.config)
            .finish()
    }
}

impl HbmDramScorer {
    pub fn new(
        cluster_secret: impl AsRef<[u8]>,
        config: HbmDramScorerConfig,
    ) -> Result<Self, RoutingError> {
        let cluster_secret = cluster_secret.as_ref();
        if cluster_secret.is_empty() {
            return Err(RoutingError::InvalidConfig(
                "cluster secret must not be empty",
            ));
        }
        Ok(Self {
            cluster_secret: cluster_secret.to_vec(),
            config: config.validate()?,
        })
    }

    fn compatible_placement<'a>(
        &self,
        context: &RoutingContext,
        load: &ServerLoadState,
        placement: Option<&'a SessionPlacementState>,
    ) -> Option<&'a SessionPlacementState> {
        let placement = placement?;
        let session = context.session.as_ref()?;
        let session_key = context.session_key(&self.cluster_secret).ok()?;
        let load_worker_key = load.server.worker_key().ok()?;
        let placement_worker_key = placement.server.worker_key().ok()?;
        if load_worker_key != placement_worker_key
            || context.model_id.as_deref() != Some(placement.server.model_id.as_str())
            || session.model_revision.as_deref() != Some(placement.server.model_revision.as_str())
            || session.tokenizer_revision.as_deref()
                != Some(placement.server.tokenizer_revision.as_str())
            || session.session_epoch != Some(placement.payload.session_epoch)
            || session.expected_parent_prefix_digest.as_deref()
                != Some(placement.payload.prefix_digest.as_str())
            || session_key != placement.payload.session_key
        {
            return None;
        }
        Some(placement)
    }
}

impl RequestScorer for HbmDramScorer {
    fn score(
        &self,
        context: &RoutingContext,
        load: &ServerLoadState,
        placement: Option<&SessionPlacementState>,
    ) -> Result<RequestScore, RoutingError> {
        let prefill_rate = load.payload.prefill_tokens_per_second_ewma;
        let decode_rate = load.payload.decode_tokens_per_second_ewma;
        validate_rate(prefill_rate, "prefill throughput")?;
        validate_rate(decode_rate, "decode throughput")?;

        let placement = self.compatible_placement(context, load, placement);
        let prompt_tokens = context
            .prompt_tokens
            .unwrap_or(self.config.default_prefill_tokens)
            .max(
                placement
                    .map(|placement| placement.payload.cached_prefix_tokens)
                    .unwrap_or_default(),
            );
        let decode_tokens = context
            .max_decode_tokens
            .unwrap_or(self.config.default_decode_tokens);
        let base_prefill_ms = milliseconds(prompt_tokens, prefill_rate)?;
        let decode_ms = milliseconds(decode_tokens, decode_rate)?;

        let mut score = RequestScore::default();
        if let Some(placement) = placement {
            if placement.payload.block_size_tokens == 0 {
                return Err(RoutingError::InvalidLoad("KV block size must be positive"));
            }
            score.hbm_reused_tokens = placement.payload.hbm_cached_tokens.min(prompt_tokens);
            let remaining = prompt_tokens.saturating_sub(score.hbm_reused_tokens);
            let candidate_dram_tokens = placement.payload.dram_cached_tokens.min(remaining);
            let dram_blocks = candidate_dram_tokens
                .saturating_add(placement.payload.block_size_tokens - 1)
                / placement.payload.block_size_tokens;
            let dram_blocks = dram_blocks.min(placement.payload.dram_blocks);
            let restore_ms = dram_blocks as f64 * self.config.dram_block_restore_ms;
            let recompute_ms = milliseconds(candidate_dram_tokens, prefill_rate)?;

            score.hbm_saved_ms = milliseconds(score.hbm_reused_tokens, prefill_rate)?;
            if recompute_ms > restore_ms {
                score.dram_reused_tokens = candidate_dram_tokens;
                score.dram_restore_ms = restore_ms;
                score.dram_net_saved_ms = recompute_ms - restore_ms;
            }
        }

        score.estimated_service_ms =
            (base_prefill_ms + decode_ms - score.hbm_saved_ms - score.dram_net_saved_ms)
                .max(self.config.minimum_reservation_ms);
        Ok(score)
    }
}

fn milliseconds(tokens: u64, tokens_per_second: f64) -> Result<f64, RoutingError> {
    let duration = tokens as f64 * 1_000.0 / tokens_per_second;
    if !duration.is_finite() {
        return Err(RoutingError::InvalidLoad(
            "token work must project to finite milliseconds",
        ));
    }
    Ok(duration)
}

fn validate_rate(rate: f64, name: &'static str) -> Result<(), RoutingError> {
    if !rate.is_finite() || rate <= 0.0 {
        return Err(RoutingError::InvalidLoad(name));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("invalid routing configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid routing load: {0}")]
    InvalidLoad(&'static str),
}
