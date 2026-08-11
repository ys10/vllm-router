use std::collections::BTreeMap;
use std::sync::Mutex;
use uuid::Uuid;

#[derive(Clone, Debug)]
struct Reservation {
    worker_key: String,
    load_micros: u64,
}

#[derive(Debug, Default)]
struct ReservationState {
    by_request: BTreeMap<Uuid, Reservation>,
    totals_micros: BTreeMap<String, u64>,
}

#[derive(Debug, Default)]
pub struct ReservationLedger {
    state: Mutex<ReservationState>,
}

pub struct ReservationView<'a> {
    totals_micros: &'a BTreeMap<String, u64>,
}

impl ReservationView<'_> {
    pub fn reserved_ms(&self, worker_key: &str) -> f64 {
        self.totals_micros
            .get(worker_key)
            .copied()
            .unwrap_or_default() as f64
            / 1_000.0
    }
}

impl ReservationLedger {
    pub fn choose_and_reserve<T>(
        &self,
        reservation_id: Uuid,
        choose: impl FnOnce(&ReservationView<'_>) -> Option<(T, String, f64)>,
    ) -> Result<Option<T>, ReservationError> {
        let mut state = self.state.lock().expect("reservation ledger poisoned");
        if state.by_request.contains_key(&reservation_id) {
            return Err(ReservationError::DuplicateReservation(reservation_id));
        }
        let view = ReservationView {
            totals_micros: &state.totals_micros,
        };
        let Some((selected, worker_key, load_ms)) = choose(&view) else {
            return Ok(None);
        };
        let load_micros = load_ms * 1_000.0;
        if worker_key.is_empty()
            || !load_micros.is_finite()
            || load_micros <= 0.0
            || load_micros > u64::MAX as f64
        {
            return Err(ReservationError::InvalidReservation);
        }
        let load_micros = load_micros.ceil() as u64;
        let new_total = state
            .totals_micros
            .get(&worker_key)
            .copied()
            .unwrap_or_default()
            .checked_add(load_micros)
            .ok_or(ReservationError::InvalidReservation)?;
        state.by_request.insert(
            reservation_id,
            Reservation {
                worker_key: worker_key.clone(),
                load_micros,
            },
        );
        state.totals_micros.insert(worker_key, new_total);
        Ok(Some(selected))
    }

    pub fn release(&self, reservation_id: Uuid, worker_key: &str) -> bool {
        let mut state = self.state.lock().expect("reservation ledger poisoned");
        let Some(reservation) = state.by_request.get(&reservation_id) else {
            return false;
        };
        if reservation.worker_key != worker_key {
            return false;
        }
        remove_locked(&mut state, reservation_id);
        true
    }

    /// Release a reservation when the caller owns the request lifecycle guard.
    pub fn release_by_id(&self, reservation_id: Uuid) -> bool {
        let mut state = self.state.lock().expect("reservation ledger poisoned");
        remove_locked(&mut state, reservation_id)
    }

    pub fn reserved_ms(&self, worker_key: &str) -> f64 {
        self.state
            .lock()
            .expect("reservation ledger poisoned")
            .totals_micros
            .get(worker_key)
            .copied()
            .unwrap_or_default() as f64
            / 1_000.0
    }

    pub fn active_reservations(&self) -> usize {
        self.state
            .lock()
            .expect("reservation ledger poisoned")
            .by_request
            .len()
    }
}

fn remove_locked(state: &mut ReservationState, reservation_id: Uuid) -> bool {
    let Some(reservation) = state.by_request.remove(&reservation_id) else {
        return false;
    };
    if let Some(total) = state.totals_micros.get_mut(&reservation.worker_key) {
        *total = total.saturating_sub(reservation.load_micros);
        if *total == 0 {
            state.totals_micros.remove(&reservation.worker_key);
        }
    }
    true
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReservationError {
    #[error("reservation already exists for routing id {0}")]
    DuplicateReservation(Uuid),
    #[error("reservation worker key and load must be valid")]
    InvalidReservation,
}
