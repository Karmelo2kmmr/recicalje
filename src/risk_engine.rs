pub const HARD_SL_PRICE: f64 = 0.680;
pub const MIN_TP_PRICE: f64 = 0.970;
pub const DCA_START_PRICE: f64 = 0.780;
pub const DCA_MIN_PRICE: f64 = 0.740;
pub const EXPENSIVE_ENTRY_SL_PRICE: f64 = 0.740;
pub const RECOVERY_SL_PRICE: f64 = 0.180;
pub const REENTRY_SL_PRICE: f64 = 0.770;

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    pub active_stop_loss: f64,
    pub active_take_profit: f64,
    pub dca_target_price: f64,
    pub dca_executed: bool,
    pub initial_size: f64,
}

impl RiskEngine {
    pub fn dca_min_price() -> f64 {
        env_f64("DCA_MIN_PRICE", DCA_MIN_PRICE)
    }

    pub fn dca_start_price() -> f64 {
        env_f64("DCA_START_PRICE", DCA_START_PRICE)
    }

    pub fn dca_target_price_env() -> f64 {
        env_f64("DCA_TARGET_PRICE", 0.760)
    }

    pub fn new(initial_size: f64, is_recovery: bool, is_reentry: bool) -> Self {
        let hard_sl = env_f64("HARD_SL_PRICE", HARD_SL_PRICE);
        let take_profit = env_f64("TAKE_PROFIT_PRICE", MIN_TP_PRICE).max(MIN_TP_PRICE);
        let dca_target = Self::dca_target_price_env();
        let sl = if is_reentry {
            REENTRY_SL_PRICE
        } else if is_recovery {
            RECOVERY_SL_PRICE
        } else {
            hard_sl
        };

        Self {
            active_stop_loss: sl,
            active_take_profit: take_profit,
            dca_target_price: dca_target,
            dca_executed: is_reentry, // Disable DCA for re-entries
            initial_size,
        }
    }

    /// Check if the price has breached the HARD STOP LOSS.
    pub fn should_hard_exit(&self, current_price: f64) -> bool {
        current_price <= self.active_stop_loss
    }

    /// Check if the price is in the valid DCA range [0.74, 0.78].
    pub fn is_in_dca_range(&self, current_price: f64) -> bool {
        let dca_min = Self::dca_min_price();
        let dca_max = Self::dca_start_price();
        current_price >= dca_min && current_price <= dca_max
    }

    /// Check if Take Profit (min 0.97) has been reached
    pub fn is_tp_reached(&self, current_price: f64) -> bool {
        current_price >= self.active_take_profit.max(MIN_TP_PRICE)
    }

    /// DCA Size: Same amount in dollars as the first purchase
    pub fn get_dca_size(&self) -> f64 {
        let factor = env_f64("DCA_SIZE_FACTOR", 0.5).clamp(0.0, 1.0);
        (self.initial_size * factor).max(0.0)
    }

    pub fn dca_enabled() -> bool {
        env_bool("ALLOW_DCA", false)
    }

    /// Update SL/TP state after a DCA or other events
    pub fn update_active_levels(&mut self, _avg_price: f64) {
        // Enforce configured HARD SL consistently after position changes.
        self.active_stop_loss = env_f64("HARD_SL_PRICE", HARD_SL_PRICE);

        // Target TP at 0.970 floor (Fixed Target as per user request to ensure execution)
        self.active_take_profit = env_f64("TAKE_PROFIT_PRICE", MIN_TP_PRICE).max(MIN_TP_PRICE);
    }

    pub fn raise_stop_loss_for_expensive_entry(&mut self) {
        self.active_stop_loss = EXPENSIVE_ENTRY_SL_PRICE;
    }
}
