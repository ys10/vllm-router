//! AgentRoute route hints: versioned documents in, exact-version ACKs out.
//!
//! See `spec/protocol-v1.md` ("AgentRoute route hints").  Nothing in this module
//! performs I/O; the selection path only reads an immutable snapshot and hands
//! acknowledgements to a bounded queue.

use crate::state::{StateError, MAX_CLOCK_SKEW_MS};
use crate::ClockReading;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteHintState {
    Preparing,
    Ready,
    Active,
    Expired,
    Cancelled,
}

impl RouteHintState {
    fn is_routable(self) -> bool {
        matches!(self, Self::Ready | Self::Active)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteHintPayload {
    pub session_key: String,
    pub source_server: Option<String>,
    pub preferred_server: String,
    pub plan_id: String,
    pub state: RouteHintState,
    pub valid_until_ms: u64,
    pub state_version: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteHintsDocument {
    pub schema_version: u8,
    pub message_type: String,
    pub cluster_id: String,
    pub producer_id: String,
    pub state_version: u64,
    pub generated_at_ms: u64,
    pub hints: Vec<RouteHintPayload>,
}

impl RouteHintsDocument {
    pub fn validate(&self, cluster_id: Option<&str>) -> Result<(), StateError> {
        if self.schema_version != 1 {
            return Err(StateError::Invalid("unsupported schema_version".into()));
        }
        if self.message_type != "route_hints" {
            return Err(StateError::Invalid(
                "message_type must be route_hints".into(),
            ));
        }
        validate_identifier("cluster_id", &self.cluster_id)?;
        validate_identifier("producer_id", &self.producer_id)?;
        if cluster_id.is_some_and(|expected| expected != self.cluster_id) {
            return Err(StateError::Invalid(
                "route hints belong to another cluster".into(),
            ));
        }
        for hint in &self.hints {
            validate_session_key(&hint.session_key)?;
            validate_identifier("preferred_server", &hint.preferred_server)?;
            if let Some(source) = &hint.source_server {
                validate_identifier("source_server", source)?;
            }
            if hint.plan_id.is_empty() {
                return Err(StateError::Invalid("plan_id must not be empty".into()));
            }
        }
        Ok(())
    }
}

/// A routable hint retained by the Router, valid until a monotonic deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteHint {
    /// Producer whose version sequence `state_version` belongs to.
    pub producer_id: String,
    pub session_key: String,
    pub source_server: Option<String>,
    pub preferred_server: String,
    pub plan_id: String,
    pub state: RouteHintState,
    pub state_version: u64,
    pub valid_until_monotonic_ms: u64,
}

impl RouteHint {
    pub fn is_valid(&self, monotonic_ms: u64) -> bool {
        monotonic_ms < self.valid_until_monotonic_ms
    }
}

/// Routable hints from one accepted document, keyed by session key.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteHintSet {
    pub producer_id: String,
    pub document_version: u64,
    pub generated_at_ms: u64,
    pub hints: BTreeMap<String, RouteHint>,
}

impl RouteHintSet {
    /// Build the replacement set if `document` is newer than this one.
    ///
    /// Versions only order documents from one producer.  A restarted producer
    /// starts a new version sequence, so a different producer replaces the
    /// set unless its document was generated before the current one.
    pub fn replacement(
        &self,
        document: &RouteHintsDocument,
        now: ClockReading,
    ) -> Option<RouteHintSet> {
        let newer = if document.producer_id == self.producer_id {
            document.state_version > self.document_version
        } else {
            document.generated_at_ms >= self.generated_at_ms
        };
        if !newer {
            return None;
        }
        let mut hints: BTreeMap<String, RouteHint> = BTreeMap::new();
        for payload in &document.hints {
            if !payload.state.is_routable() {
                continue;
            }
            let remaining = payload
                .valid_until_ms
                .saturating_sub(now.unix_ms)
                .saturating_sub(MAX_CLOCK_SKEW_MS);
            if remaining == 0 {
                continue;
            }
            let hint = RouteHint {
                producer_id: document.producer_id.clone(),
                session_key: payload.session_key.clone(),
                source_server: payload.source_server.clone(),
                preferred_server: payload.preferred_server.clone(),
                plan_id: payload.plan_id.clone(),
                state: payload.state,
                state_version: payload.state_version,
                valid_until_monotonic_ms: now.monotonic_ms.saturating_add(remaining),
            };
            let newer = hints
                .get(&hint.session_key)
                .is_none_or(|current| hint.state_version > current.state_version);
            if newer {
                hints.insert(hint.session_key.clone(), hint);
            }
        }
        Some(RouteHintSet {
            producer_id: document.producer_id.clone(),
            document_version: document.state_version,
            generated_at_ms: document.generated_at_ms,
            hints,
        })
    }

    /// Drop expired hints; returns whether anything changed.
    pub fn expire(&mut self, monotonic_ms: u64) -> bool {
        let before = self.hints.len();
        self.hints.retain(|_, hint| hint.is_valid(monotonic_ms));
        before != self.hints.len()
    }
}

/// Why a valid hint did or did not determine the selected worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteHintOutcome {
    NoHint,
    Expired,
    TargetUnavailable,
    TargetImbalanced,
    ShadowOnly,
    Applied,
}

impl RouteHintOutcome {
    pub fn label(self) -> &'static str {
        match self {
            Self::NoHint => "no_hint",
            Self::Expired => "expired",
            Self::TargetUnavailable => "target_unavailable",
            Self::TargetImbalanced => "target_imbalanced",
            Self::ShadowOnly => "shadow_only",
            Self::Applied => "applied",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteHintAck {
    pub schema_version: u8,
    pub message_type: String,
    pub cluster_id: String,
    pub router_id: String,
    pub plan_id: String,
    pub session_key: String,
    pub state_version: u64,
    pub observed_at_ms: u64,
}

/// Hot-path side of acknowledgements: dedupe and enqueue without awaiting.
#[derive(Debug)]
pub struct RouteHintAcker {
    cluster_id: String,
    router_id: String,
    sender: tokio::sync::mpsc::Sender<RouteHintAck>,
    acknowledged: Mutex<HashMap<(String, String, String), u64>>,
    dropped: AtomicU64,
}

const MAX_TRACKED_ACKS: usize = 16_384;

impl RouteHintAcker {
    /// Create an acker and the receiver its background publisher drains.
    pub fn new(
        cluster_id: impl Into<String>,
        router_id: impl Into<String>,
        capacity: usize,
    ) -> (Self, tokio::sync::mpsc::Receiver<RouteHintAck>) {
        let (sender, receiver) = tokio::sync::mpsc::channel(capacity.max(1));
        (
            Self {
                cluster_id: cluster_id.into(),
                router_id: router_id.into(),
                sender,
                acknowledged: Mutex::new(HashMap::new()),
                dropped: AtomicU64::new(0),
            },
            receiver,
        )
    }

    /// Enqueue one acknowledgement per exact hint version; never blocks.
    pub fn acknowledge(&self, hint: &RouteHint, observed_at_ms: u64) -> bool {
        // Versions restart with a new producer, so dedupe within one producer.
        let key = (
            hint.producer_id.clone(),
            hint.plan_id.clone(),
            hint.session_key.clone(),
        );
        {
            let mut acknowledged = self.acknowledged.lock().expect("acker poisoned");
            if acknowledged
                .get(&key)
                .is_some_and(|version| *version >= hint.state_version)
            {
                return false;
            }
            if acknowledged.len() >= MAX_TRACKED_ACKS {
                // A repeated ACK is idempotent upstream; bounded memory wins.
                acknowledged.clear();
            }
            acknowledged.insert(key.clone(), hint.state_version);
        }
        let ack = RouteHintAck {
            schema_version: 1,
            message_type: "route_hint_ack".into(),
            cluster_id: self.cluster_id.clone(),
            router_id: self.router_id.clone(),
            plan_id: hint.plan_id.clone(),
            session_key: hint.session_key.clone(),
            state_version: hint.state_version,
            observed_at_ms,
        };
        if self.sender.try_send(ack).is_ok() {
            return true;
        }
        self.dropped.fetch_add(1, Ordering::Relaxed);
        // Allow a later request to retry this version.
        self.acknowledged
            .lock()
            .expect("acker poisoned")
            .remove(&key);
        false
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn validate_identifier(name: &str, value: &str) -> Result<(), StateError> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte));
    if valid {
        Ok(())
    } else {
        Err(StateError::Invalid(format!(
            "{name} must match [A-Za-z0-9][A-Za-z0-9._-]{{0,127}}"
        )))
    }
}

fn validate_session_key(value: &str) -> Result<(), StateError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(StateError::Invalid(
            "session_key must be 64 lowercase hex characters".into(),
        ))
    }
}
