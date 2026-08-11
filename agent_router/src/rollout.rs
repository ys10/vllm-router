use uuid::Uuid;

pub const ROLLOUT_STAGES: [u8; 5] = [0, 1, 5, 25, 100];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RolloutPercent(u8);

impl RolloutPercent {
    pub fn new(percent: u8) -> Result<Self, RolloutError> {
        if !ROLLOUT_STAGES.contains(&percent) {
            return Err(RolloutError::UnsupportedPercent(percent));
        }
        Ok(Self(percent))
    }

    pub fn get(self) -> u8 {
        self.0
    }

    pub fn includes(self, routing_id: Uuid) -> bool {
        rollout_bucket(routing_id) < self.0
    }
}

pub fn rollout_bucket(routing_id: Uuid) -> u8 {
    (routing_id.as_u128() % 100) as u8
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum RolloutError {
    #[error("unsupported rollout percentage {0}; expected one of 0, 1, 5, 25, 100")]
    UnsupportedPercent(u8),
}
