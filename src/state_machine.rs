use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PositionState {
    Scanning,
    PendingDCA,
    InPosition,
    Exiting,
    RecoveryScanning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachine {
    pub current_state: PositionState,
    pub shares_held: f64,
    pub entry_price: f64,
    pub side: String, // "UP" or "DOWN"
    pub is_reentry: bool,
    pub last_update: Option<chrono::DateTime<chrono::Local>>,
}

impl StateMachine {
    pub fn new() -> Self {
        Self {
            current_state: PositionState::Scanning,
            shares_held: 0.0,
            entry_price: 0.0,
            side: String::new(),
            is_reentry: false,
            last_update: None,
        }
    }

    pub fn transition_to(&mut self, next: PositionState) {
        log::info!(
            "🔄 STATE TRANSITION: {:?} -> {:?}",
            self.current_state,
            next
        );
        self.current_state = next;
        self.last_update = Some(chrono::Local::now());
    }

    pub fn is_in_position(&self) -> bool {
        match self.current_state {
            PositionState::InPosition | PositionState::PendingDCA | PositionState::Exiting => true,
            _ => false,
        }
    }

    pub fn reset(&mut self) {
        self.current_state = PositionState::Scanning;
        self.shares_held = 0.0;
        self.entry_price = 0.0;
        self.side = String::new();
        self.is_reentry = false;
        self.last_update = Some(chrono::Local::now());
    }
}
