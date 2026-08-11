use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Eq, PartialEq)]
pub struct RoutingContext {
    routing_id: Uuid,
    pub route: String,
    pub model_id: Option<String>,
    pub request_text: Option<String>,
    pub request_id: Option<String>,
    pub is_stream: bool,
    pub prompt_tokens: Option<u64>,
    pub max_decode_tokens: Option<u64>,
    pub session: Option<SessionContext>,
}

impl Default for RoutingContext {
    fn default() -> Self {
        Self {
            routing_id: Uuid::new_v4(),
            route: String::new(),
            model_id: None,
            request_text: None,
            request_id: None,
            is_stream: false,
            prompt_tokens: None,
            max_decode_tokens: None,
            session: None,
        }
    }
}

impl std::fmt::Debug for RoutingContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RoutingContext")
            .field("route", &self.route)
            .field("model_id", &self.model_id)
            .field("has_request_text", &self.request_text.is_some())
            .field("has_request_id", &self.request_id.is_some())
            .field("is_stream", &self.is_stream)
            .field("prompt_tokens", &self.prompt_tokens)
            .field("max_decode_tokens", &self.max_decode_tokens)
            .field("has_session", &self.session.is_some())
            .finish()
    }
}

impl RoutingContext {
    pub fn new(
        route: impl Into<String>,
        model_id: Option<String>,
        request_text: Option<String>,
        is_stream: bool,
    ) -> Self {
        Self {
            route: route.into(),
            model_id,
            request_text,
            is_stream,
            ..Self::default()
        }
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn routing_id(&self) -> Uuid {
        self.routing_id
    }

    pub fn with_prompt_tokens(mut self, prompt_tokens: u64) -> Self {
        self.prompt_tokens = Some(prompt_tokens);
        self
    }

    pub fn with_decode_tokens(mut self, max_decode_tokens: u64) -> Self {
        self.max_decode_tokens = Some(max_decode_tokens);
        self
    }

    pub fn with_session(mut self, session: SessionContext) -> Self {
        self.session = Some(session);
        self
    }

    pub fn with_session_params(
        mut self,
        params: Option<&HashMap<String, serde_json::Value>>,
    ) -> Self {
        self.session = params.and_then(SessionContext::from_params);
        self
    }

    pub fn session_key(&self, cluster_secret: &[u8]) -> Result<String, ContextError> {
        self.session
            .as_ref()
            .ok_or(ContextError::MissingSession)?
            .session_key(cluster_secret)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SessionContext {
    pub tenant_id: String,
    pub session_id: String,
    pub session_epoch: Option<u64>,
    pub expected_parent_prefix_digest: Option<String>,
    pub model_revision: Option<String>,
    pub tokenizer_revision: Option<String>,
}

impl std::fmt::Debug for SessionContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionContext")
            .field("tenant_id", &"<redacted>")
            .field("session_id", &"<redacted>")
            .field("session_epoch", &self.session_epoch)
            .field(
                "has_expected_parent_prefix_digest",
                &self.expected_parent_prefix_digest.is_some(),
            )
            .field("has_model_revision", &self.model_revision.is_some())
            .field("has_tokenizer_revision", &self.tokenizer_revision.is_some())
            .finish()
    }
}

impl SessionContext {
    pub fn from_params(params: &HashMap<String, serde_json::Value>) -> Option<Self> {
        let tenant_id = nonempty_string(params.get("tenant_id")?)?;
        let session_id = nonempty_string(params.get("session_id")?)?;
        let expected_parent_prefix_digest = match params.get("expected_parent_prefix_digest") {
            Some(value) => Some(nonempty_string(value)?.to_string()),
            None => None,
        };
        if expected_parent_prefix_digest
            .as_deref()
            .is_some_and(|digest| !is_sha256(digest))
        {
            return None;
        }
        Some(Self {
            tenant_id: tenant_id.to_string(),
            session_id: session_id.to_string(),
            session_epoch: params.get("session_epoch").and_then(|value| value.as_u64()),
            expected_parent_prefix_digest,
            model_revision: params
                .get("model_revision")
                .and_then(nonempty_string)
                .map(str::to_string),
            tokenizer_revision: params
                .get("tokenizer_revision")
                .and_then(nonempty_string)
                .map(str::to_string),
        })
    }

    pub fn session_key(&self, cluster_secret: &[u8]) -> Result<String, ContextError> {
        if cluster_secret.is_empty() {
            return Err(ContextError::EmptyClusterSecret);
        }
        if self.tenant_id.is_empty() || self.session_id.is_empty() {
            return Err(ContextError::EmptySessionIdentity);
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(cluster_secret)
            .map_err(|_| ContextError::EmptyClusterSecret)?;
        mac.update(&frame(&self.tenant_id)?);
        mac.update(&frame(&self.session_id)?);
        Ok(hex::encode(mac.finalize().into_bytes()))
    }
}

fn nonempty_string(value: &serde_json::Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn frame(value: &str) -> Result<Vec<u8>, ContextError> {
    let bytes = value.as_bytes();
    let length: u32 = bytes
        .len()
        .try_into()
        .map_err(|_| ContextError::IdentityTooLong)?;
    let mut framed = Vec::with_capacity(4 + bytes.len());
    framed.extend_from_slice(&length.to_be_bytes());
    framed.extend_from_slice(bytes);
    Ok(framed)
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContextError {
    #[error("routing context has no session")]
    MissingSession,
    #[error("cluster secret must not be empty")]
    EmptyClusterSecret,
    #[error("tenant_id and session_id must not be empty")]
    EmptySessionIdentity,
    #[error("identity field is too long")]
    IdentityTooLong,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestOutcome {
    Succeeded,
    ClientError,
    ServerError,
    RetryableFailure,
    TransportError,
    Cancelled,
}

impl RequestOutcome {
    pub fn is_success(self) -> bool {
        self == Self::Succeeded
    }
}
