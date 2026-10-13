use crate::ClockReading;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const LOAD_FRESH_MS: u64 = 3_000;
pub const SESSION_FRESH_MS: u64 = 15_000;
pub const MAX_CLOCK_SKEW_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IngestMode {
    Bootstrap,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageType {
    ServerLoad,
    SessionPlacement,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerRole {
    Unified,
    Prefill,
    Decode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerIdentity {
    pub cluster_id: String,
    pub server_id: String,
    #[serde(deserialize_with = "deserialize_instance_id")]
    pub instance_id: uuid::Uuid,
    pub model_id: String,
    pub model_revision: String,
    pub tokenizer_revision: String,
    pub role: WorkerRole,
    pub dp_rank: u64,
    pub endpoint: String,
}

impl ServerIdentity {
    pub fn worker_key(&self) -> Result<String, StateError> {
        let instance_id = self.instance_id.to_string();
        let dp_rank = self.dp_rank.to_string();
        let fields = [
            self.cluster_id.as_str(),
            self.server_id.as_str(),
            instance_id.as_str(),
            self.model_id.as_str(),
            self.model_revision.as_str(),
            self.tokenizer_revision.as_str(),
            match self.role {
                WorkerRole::Unified => "unified",
                WorkerRole::Prefill => "prefill",
                WorkerRole::Decode => "decode",
            },
            dp_rank.as_str(),
        ];
        let mut digest = Sha256::new();
        for field in fields {
            let bytes = field.as_bytes();
            let length: u32 = bytes
                .len()
                .try_into()
                .map_err(|_| StateError::Invalid("worker identity field too long".into()))?;
            digest.update(length.to_be_bytes());
            digest.update(bytes);
        }
        Ok(hex::encode(digest.finalize()))
    }

    fn validate(&self) -> Result<(), StateError> {
        for (name, value) in [
            ("cluster_id", self.cluster_id.as_str()),
            ("server_id", self.server_id.as_str()),
        ] {
            validate_identifier(name, value)?;
        }
        for (name, value) in [
            ("model_id", self.model_id.as_str()),
            ("model_revision", self.model_revision.as_str()),
            ("tokenizer_revision", self.tokenizer_revision.as_str()),
        ] {
            if value.is_empty() || value.chars().count() > 256 {
                return Err(StateError::Invalid(format!(
                    "{name} must contain between 1 and 256 characters"
                )));
            }
        }
        if self.instance_id.get_version_num() != 4 {
            return Err(StateError::Invalid("instance_id must be UUIDv4".into()));
        }
        let endpoint = reqwest::Url::parse(&self.endpoint)
            .map_err(|error| StateError::Invalid(format!("invalid endpoint: {error}")))?;
        if !matches!(endpoint.scheme(), "http" | "https") {
            return Err(StateError::Invalid(
                "endpoint must use http or https".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Waiting,
    Running,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestPhase {
    Prefill,
    Decode,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestLoad {
    pub request_key: String,
    pub state: RequestState,
    pub phase: RequestPhase,
    pub prefill_tokens_remaining: u64,
    pub decode_tokens_generated: u64,
    pub decode_tokens_remaining: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerLoadPayload {
    pub waiting_count: u64,
    pub running_count: u64,
    pub waiting_prefill_tokens: u64,
    pub waiting_decode_tokens_remaining: u64,
    pub running_prefill_tokens_remaining: u64,
    pub running_decode_tokens_remaining: u64,
    pub prefill_tokens_per_second_ewma: f64,
    pub decode_tokens_per_second_ewma: f64,
    pub kv_cache_usage_ratio: f64,
    pub requests: Vec<RequestLoad>,
    /// Measured rate of loading KV from the local SSD tier (LMCache), if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssd_load_tokens_per_second_ewma: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerLoadState {
    pub schema_version: u32,
    pub message_type: MessageType,
    pub seq: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub server: ServerIdentity,
    pub payload: ServerLoadPayload,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPlacementPayload {
    pub session_key: String,
    pub session_epoch: u64,
    pub prefix_digest: String,
    pub cached_prefix_tokens: u64,
    pub block_size_tokens: u64,
    pub hbm_blocks: u64,
    pub dram_blocks: u64,
    pub hbm_cached_tokens: u64,
    pub dram_cached_tokens: u64,
    /// Length of the prefix identified by `prefix_digest`. Absent means the
    /// cached tokens are the whole prefix (`cached_prefix_tokens`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_tokens: Option<u64>,
    /// Blocks and tokens of the prefix held only on the local SSD tier, beyond
    /// those in HBM and DRAM.
    #[serde(default)]
    pub ssd_blocks: u64,
    #[serde(default)]
    pub ssd_cached_tokens: u64,
}

impl SessionPlacementPayload {
    pub fn prefix_tokens(&self) -> u64 {
        self.prefix_tokens.unwrap_or(self.cached_prefix_tokens)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPlacementState {
    pub schema_version: u32,
    pub message_type: MessageType,
    pub seq: u64,
    pub observed_at_ms: u64,
    pub expires_at_ms: u64,
    pub server: ServerIdentity,
    pub payload: SessionPlacementPayload,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StateMessage {
    ServerLoad(ServerLoadState),
    SessionPlacement(SessionPlacementState),
}

impl StateMessage {
    pub fn validate(&self) -> Result<(), StateError> {
        let (schema_version, message_type, seq, observed, expires, server) = match self {
            Self::ServerLoad(state) => (
                state.schema_version,
                state.message_type,
                state.seq,
                state.observed_at_ms,
                state.expires_at_ms,
                &state.server,
            ),
            Self::SessionPlacement(state) => (
                state.schema_version,
                state.message_type,
                state.seq,
                state.observed_at_ms,
                state.expires_at_ms,
                &state.server,
            ),
        };
        if schema_version != 1 {
            return Err(StateError::Invalid("schema_version must be 1".into()));
        }
        if seq == 0 {
            return Err(StateError::Invalid("seq must be positive".into()));
        }
        if observed == 0 || expires <= observed {
            return Err(StateError::Invalid(
                "expires_at_ms must be greater than observed_at_ms".into(),
            ));
        }
        server.validate()?;
        match self {
            Self::ServerLoad(state) => {
                if message_type != MessageType::ServerLoad {
                    return Err(StateError::Invalid(
                        "server load has the wrong message_type".into(),
                    ));
                }
                validate_load(&state.payload)
            }
            Self::SessionPlacement(state) => {
                if message_type != MessageType::SessionPlacement {
                    return Err(StateError::Invalid(
                        "session placement has the wrong message_type".into(),
                    ));
                }
                validate_placement(&state.payload)
            }
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::ServerLoad(state) => state.seq,
            Self::SessionPlacement(state) => state.seq,
        }
    }

    pub fn observed_at_ms(&self) -> u64 {
        match self {
            Self::ServerLoad(state) => state.observed_at_ms,
            Self::SessionPlacement(state) => state.observed_at_ms,
        }
    }

    pub fn expires_at_ms(&self) -> u64 {
        match self {
            Self::ServerLoad(state) => state.expires_at_ms,
            Self::SessionPlacement(state) => state.expires_at_ms,
        }
    }

    pub fn server(&self) -> &ServerIdentity {
        match self {
            Self::ServerLoad(state) => &state.server,
            Self::SessionPlacement(state) => &state.server,
        }
    }

    pub fn freshness_ms(&self) -> u64 {
        match self {
            Self::ServerLoad(_) => LOAD_FRESH_MS,
            Self::SessionPlacement(_) => SESSION_FRESH_MS,
        }
    }

    pub fn entity_key(&self) -> Result<String, StateError> {
        let worker_key = self.server().worker_key()?;
        Ok(match self {
            Self::ServerLoad(_) => format!("load:{worker_key}"),
            Self::SessionPlacement(state) => {
                format!("session:{}:{worker_key}", state.payload.session_key)
            }
        })
    }

    pub fn remaining_fresh_ms(
        &self,
        mode: IngestMode,
        now: ClockReading,
    ) -> Result<Option<u64>, StateError> {
        if mode == IngestMode::Live {
            return Ok(Some(self.freshness_ms()));
        }
        if self.observed_at_ms() > now.unix_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
            return Err(StateError::Invalid(
                "bootstrap observed_at_ms is too far in the future".into(),
            ));
        }
        if self.expires_at_ms() <= now.unix_ms {
            return Ok(None);
        }
        let age = now.unix_ms.saturating_sub(self.observed_at_ms());
        Ok(self
            .freshness_ms()
            .checked_sub(age)
            .filter(|value| *value > 0))
    }
}

fn deserialize_instance_id<'de, D>(deserializer: D) -> Result<uuid::Uuid, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    let parsed = uuid::Uuid::parse_str(&raw).map_err(serde::de::Error::custom)?;
    if parsed.get_version_num() != 4 || raw != parsed.hyphenated().to_string() {
        return Err(serde::de::Error::custom(
            "instance_id must be a canonical lowercase UUIDv4",
        ));
    }
    Ok(parsed)
}

fn validate_identifier(name: &str, value: &str) -> Result<(), StateError> {
    if value.len() > 128
        || !value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(StateError::Invalid(format!(
            "{name} must match [A-Za-z0-9][A-Za-z0-9._-]{{0,127}}"
        )));
    }
    Ok(())
}

fn validate_digest(name: &str, digest: &str) -> Result<(), StateError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StateError::Invalid(format!(
            "{name} must be a lowercase SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_load(payload: &ServerLoadPayload) -> Result<(), StateError> {
    if !payload.prefill_tokens_per_second_ewma.is_finite()
        || payload.prefill_tokens_per_second_ewma <= 0.0
        || !payload.decode_tokens_per_second_ewma.is_finite()
        || payload.decode_tokens_per_second_ewma <= 0.0
    {
        return Err(StateError::Invalid(
            "throughput EWMAs must be finite and positive".into(),
        ));
    }
    if payload
        .ssd_load_tokens_per_second_ewma
        .is_some_and(|rate| !rate.is_finite() || rate <= 0.0)
    {
        return Err(StateError::Invalid(
            "SSD load rate must be finite and positive when present".into(),
        ));
    }
    if !payload.kv_cache_usage_ratio.is_finite()
        || !(0.0..=1.0).contains(&payload.kv_cache_usage_ratio)
    {
        return Err(StateError::Invalid(
            "kv_cache_usage_ratio must be between zero and one".into(),
        ));
    }
    for request in &payload.requests {
        validate_digest("request_key", &request.request_key)?;
    }
    Ok(())
}

fn validate_placement(payload: &SessionPlacementPayload) -> Result<(), StateError> {
    validate_digest("session_key", &payload.session_key)?;
    validate_digest("prefix_digest", &payload.prefix_digest)?;
    if payload.block_size_tokens == 0 {
        return Err(StateError::Invalid(
            "block_size_tokens must be positive".into(),
        ));
    }
    if payload
        .hbm_cached_tokens
        .saturating_add(payload.dram_cached_tokens)
        .saturating_add(payload.ssd_cached_tokens)
        != payload.cached_prefix_tokens
    {
        return Err(StateError::Invalid(
            "HBM, DRAM and SSD cached token counts must sum to cached_prefix_tokens".into(),
        ));
    }
    if payload.cached_prefix_tokens > payload.prefix_tokens() {
        return Err(StateError::Invalid(
            "cached_prefix_tokens cannot exceed prefix_tokens".into(),
        ));
    }
    if payload.hbm_cached_tokens > payload.hbm_blocks.saturating_mul(payload.block_size_tokens)
        || payload.dram_cached_tokens
            > payload
                .dram_blocks
                .saturating_mul(payload.block_size_tokens)
        || payload.ssd_cached_tokens > payload.ssd_blocks.saturating_mul(payload.block_size_tokens)
    {
        return Err(StateError::Invalid(
            "cached tokens exceed tier block capacity".into(),
        ));
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("invalid router state: {0}")]
    Invalid(String),
}
