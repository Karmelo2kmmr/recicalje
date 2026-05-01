pub const HARD_SL_PRICE: f64 = 0.680;
pub const MIN_TP_PRICE: f64 = 0.970;
pub const DCA_START_PRICE: f64 = 0.780;
pub const DCA_MIN_PRICE: f64 = 0.740;
pub const EXPENSIVE_ENTRY_SL_PRICE: f64 = 0.740;
pub const RECOVERY_SL_PRICE: f64 = 0.450; // was 0.180 — an 82% loss is not a stop-loss
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

    /// Update SL/TP state after a DCA or other events.
    /// avg_price: the new average entry price after DCA. Used for adaptive SL if enabled.
    pub fn update_active_levels(&mut self, avg_price: f64) {
        let configured_sl = env_f64("HARD_SL_PRICE", HARD_SL_PRICE);

        // P1 FIX: The original code ignored avg_price entirely (_avg_price).
        // After a DCA at a higher cost basis, the SL should tighten to protect
        // the extra capital deployed. Opt-in with ADAPTIVE_SL_AFTER_DCA=true.
        if avg_price > 0.0 && env_bool("ADAPTIVE_SL_AFTER_DCA", false) {
            let max_loss_pct = env_f64("ADAPTIVE_SL_MAX_LOSS_PCT", 0.12); // 12% from avg
            let sl_from_avg = (avg_price * (1.0 - max_loss_pct)).max(configured_sl);
            log::info!(
                "Adaptive SL post-DCA: avg_price={:.4} max_loss={:.0}% -> SL={:.4} (was {:.4})",
                avg_price, max_loss_pct * 100.0, sl_from_avg, self.active_stop_loss
            );
            self.active_stop_loss = sl_from_avg;
        } else {
            // Default: use configured HARD SL consistently
            self.active_stop_loss = configured_sl;
        }

        self.active_take_profit = env_f64("TAKE_PROFIT_PRICE", MIN_TP_PRICE).max(MIN_TP_PRICE);
    }

    pub fn raise_stop_loss_for_expensive_entry(&mut self) {
        self.active_stop_loss = EXPENSIVE_ENTRY_SL_PRICE;
    }
}

pub struct CapitalProtectionEngine {
    pub max_loss_per_trade_usd: f64,
    pub max_daily_loss_usd: f64,
    pub exit_timeout_secs: u64,
    pub daily_loss_accumulated: f64,
    pub is_safe_mode: bool,
}

pub enum ExitStrategy {
    Normal(f64),
    EmergencyLadder,
    MarketDump,
}

impl CapitalProtectionEngine {
    pub fn new() -> Self {
        Self {
            max_loss_per_trade_usd: env_f64("MAX_LOSS_PER_TRADE_USD", 15.0),
            max_daily_loss_usd: env_f64("MAX_DAILY_LOSS_USD", 50.0),
            exit_timeout_secs: env_f64("EXIT_TIMEOUT_SECS", 45.0) as u64,
            daily_loss_accumulated: 0.0,
            is_safe_mode: false,
        }
    }

    pub fn check_daily_loss(&mut self, last_trade_pnl: f64) {
        if last_trade_pnl < 0.0 {
            self.daily_loss_accumulated += last_trade_pnl.abs();
            if self.daily_loss_accumulated >= self.max_daily_loss_usd {
                self.engage_safe_mode("MAX DAILY LOSS REACHED");
            }
        }
    }

    pub fn engage_safe_mode(&mut self, reason: &str) {
        self.is_safe_mode = true;
        log::error!("🚨 CRITICAL SAFE MODE ENGAGED: {} 🚨", reason);
        log::error!("All trading operations have been suspended to protect capital.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daily_loss_limit() {
        let mut protection = CapitalProtectionEngine {
            max_loss_per_trade_usd: 15.0,
            max_daily_loss_usd: 50.0,
            exit_timeout_secs: 45,
            daily_loss_accumulated: 0.0,
            is_safe_mode: false,
        };

        // First trade: loses $20.0
        protection.check_daily_loss(-20.0);
        assert_eq!(protection.daily_loss_accumulated, 20.0);
        assert!(!protection.is_safe_mode);

        // Second trade: loses $35.0 (Total: $55.0) -> Exceeds max daily loss ($50.0)
        protection.check_daily_loss(-35.0);
        assert_eq!(protection.daily_loss_accumulated, 55.0);
        assert!(protection.is_safe_mode);
    }

    #[test]
    fn test_safe_mode_lockout() {
        let mut protection = CapitalProtectionEngine::new();
        assert!(!protection.is_safe_mode);
        
        protection.engage_safe_mode("API INACCESSIBLE DURING EXIT");
        assert!(protection.is_safe_mode);
    }
}
