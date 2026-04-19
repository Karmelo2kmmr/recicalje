use crate::tracker::EntryType;
use log::info;

/// Result of trade validation
#[derive(Debug, Clone)]
pub enum TradeApproval {
    Approved {
        r_ratio: f64,
        risk_dollars: f64,
        reward_dollars: f64,
    },
}

/// Reasons why a trade was rejected
#[derive(Debug, Clone)]
pub enum RejectionReason {
    TooExpensive { entry_price: f64, max_allowed: f64 },
    NoTakeProfit { entry_type: String },
    InsufficientR { r_ratio: f64, min_required: f64 },
    InvalidPrices { reason: String },
    BinanceDivergence { binance_move: f64, poly_move: f64 }, // NEW: Bull Trap protection
}

/// Trade Validator - Enforces minimum R ratio and blocks expensive entries
pub struct TradeValidator {
    min_r_mult: f64,
    max_entry_allowed: f64,
}

impl TradeValidator {
    /// Create a new TradeValidator with configuration from environment
    pub fn new() -> Self {
        let min_r_mult = std::env::var("MIN_R_MULT")
            .unwrap_or("1.2".to_string())
            .parse()
            .unwrap_or(1.2);

        let max_entry_allowed = std::env::var("MAX_ENTRY_ALLOWED")
            .unwrap_or("0.93".to_string())
            .parse()
            .unwrap_or(0.93);

        Self {
            min_r_mult,
            max_entry_allowed,
        }
    }

    /// Validate a trade before execution
    ///
    /// # Arguments
    /// * `entry_price` - The price at which we would enter (best_ask)
    /// * `side` - "UP" or "DOWN"
    /// * `entry_type` - Type of entry (Dip, TriggerDirect, etc.)
    /// * `sl_price` - Calculated stop loss price
    /// * `tp_price` - Take profit price (optional for some entry types)
    /// * `position_size` - Size of position in dollars
    ///
    /// # Returns
    /// * `Ok(TradeApproval)` if trade passes all validation
    /// * `Err(RejectionReason)` if trade should be rejected
    pub fn validate_trade(
        &self,
        entry_price: f64,
        _side: &str,
        entry_type: &EntryType,
        sl_price: f64,
        tp_price: Option<f64>,
        position_size: f64,
    ) -> Result<TradeApproval, RejectionReason> {
        // HARD BLOCK 1: Entry price too expensive
        if entry_price > self.max_entry_allowed {
            info!("🚫 TRADE REJECTED: Entry price {:.4} > max allowed {:.4} (Market is too expensive)", 
                  entry_price, self.max_entry_allowed);
            return Err(RejectionReason::TooExpensive {
                entry_price,
                max_allowed: self.max_entry_allowed,
            });
        }

        // HARD BLOCK 2: No TP for TriggerDirect and Reentry
        match entry_type {
            EntryType::TriggerDirect | EntryType::Reentry => {
                if tp_price.is_none() {
                    info!(
                        "🚫 TRADE REJECTED: {:?} requires TP but none provided",
                        entry_type
                    );
                    return Err(RejectionReason::NoTakeProfit {
                        entry_type: format!("{:?}", entry_type),
                    });
                }
            }
            _ => {} // Dip and DipRecovery can proceed without TP check here
        }

        // Get TP or return error for validation
        let tp = match tp_price {
            Some(tp) => tp,
            None => {
                // For Dip/DipRecovery, if no TP is set, we can't validate R ratio
                // This should not happen in practice, but handle gracefully
                info!(
                    "⚠️ WARNING: No TP set for {:?}, cannot validate R ratio",
                    entry_type
                );
                return Err(RejectionReason::InvalidPrices {
                    reason: "No TP price provided for R calculation".to_string(),
                });
            }
        };

        // Validate prices make sense
        if entry_price <= 0.0 || sl_price <= 0.0 || tp <= 0.0 {
            return Err(RejectionReason::InvalidPrices {
                reason: format!(
                    "Invalid prices: entry={:.4}, sl={:.4}, tp={:.4}",
                    entry_price, sl_price, tp
                ),
            });
        }

        if tp <= entry_price {
            return Err(RejectionReason::InvalidPrices {
                reason: format!("TP {:.4} must be > entry {:.4}", tp, entry_price),
            });
        }

        if sl_price >= entry_price {
            return Err(RejectionReason::InvalidPrices {
                reason: format!("SL {:.4} must be < entry {:.4}", sl_price, entry_price),
            });
        }

        // Calculate R ratio
        // Risk = what we lose if SL hits
        // Reward = what we gain if TP hits
        // In binary options: profit/loss is proportional to price movement * size
        let risk_dollars = (entry_price - sl_price) * position_size;
        let reward_dollars = (tp - entry_price) * position_size;

        let r_ratio = if risk_dollars > 0.0 {
            reward_dollars / risk_dollars
        } else {
            0.0
        };

        // VALIDATION: R must be >= MIN_R_MULT
        // EXCEPTION: TriggerDirect entries use DCA strategy with deferred SL, so skip R check
        if *entry_type == EntryType::TriggerDirect {
            info!("✅ TRADE VALIDATED (TriggerDirect - DCA Strategy) | Entry: {:.4} | SL: Deferred | TP: {:.4}",
                  entry_price, tp);

            // Return approval with placeholder R ratio (will be calculated after DCA)
            return Ok(TradeApproval::Approved {
                r_ratio: 0.0, // Placeholder - actual R calculated after DCA
                risk_dollars: 0.0,
                reward_dollars: 0.0,
            });
        }

        if r_ratio < self.min_r_mult {
            info!(
                "🚫 TRADE REJECTED: R ratio {:.2} < minimum {:.2} (Risk: ${:.2}, Reward: ${:.2})",
                r_ratio, self.min_r_mult, risk_dollars, reward_dollars
            );
            return Err(RejectionReason::InsufficientR {
                r_ratio,
                min_required: self.min_r_mult,
            });
        }

        // Trade approved!
        info!("✅ TRADE VALIDATED: R={:.2} | Risk=${:.2} | Reward=${:.2} | Entry={:.4} | SL={:.4} | TP={:.4}",
              r_ratio, risk_dollars, reward_dollars, entry_price, sl_price, tp);

        Ok(TradeApproval::Approved {
            r_ratio,
            risk_dollars,
            reward_dollars,
        })
    }

    /// LOBO: Check if price movement is correlated with Binance
    pub fn check_correlation(
        &self,
        binance_move_pct: f64,
        side: &str,
    ) -> Result<(), RejectionReason> {
        // If we are betting "UP" but Binance is moving down significantly (>0.05%)
        if side == "UP" && binance_move_pct < -0.05 {
            info!(
                "🚫 CORRELATION ERROR: Betting UP while Binance is dropping ({:.2}%)",
                binance_move_pct
            );
            return Err(RejectionReason::BinanceDivergence {
                binance_move: binance_move_pct,
                poly_move: 0.0,
            });
        }
        // If we are betting "DOWN" but Binance is moving up significantly (>0.05%)
        if side == "DOWN" && binance_move_pct > 0.05 {
            info!(
                "🚫 CORRELATION ERROR: Betting DOWN while Binance is pumping ({:.2}%)",
                binance_move_pct
            );
            return Err(RejectionReason::BinanceDivergence {
                binance_move: binance_move_pct,
                poly_move: 0.0,
            });
        }
        Ok(())
    }
}

impl Default for TradeValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reject_expensive_entry() {
        let validator = TradeValidator {
            min_r_mult: 1.2,
            max_entry_allowed: 0.93,
        };

        let result = validator.validate_trade(
            0.95, // entry_price - too expensive
            "UP",
            &EntryType::TriggerDirect,
            0.75,       // sl_price
            Some(0.98), // tp_price
            10.0,       // position_size
        );

        assert!(result.is_err());
        match result {
            Err(RejectionReason::TooExpensive {
                entry_price,
                max_allowed,
            }) => {
                assert_eq!(entry_price, 0.95);
                assert_eq!(max_allowed, 0.93);
            }
            _ => panic!("Expected TooExpensive rejection"),
        }
    }

    #[test]
    fn test_reject_no_tp_for_trigger_direct() {
        let validator = TradeValidator {
            min_r_mult: 1.2,
            max_entry_allowed: 0.93,
        };

        let result = validator.validate_trade(
            0.90,
            "UP",
            &EntryType::TriggerDirect,
            0.75,
            None, // No TP
            10.0,
        );

        assert!(result.is_err());
        match result {
            Err(RejectionReason::NoTakeProfit { .. }) => {}
            _ => panic!("Expected NoTakeProfit rejection"),
        }
    }

    #[test]
    fn test_reject_insufficient_r() {
        let validator = TradeValidator {
            min_r_mult: 1.2,
            max_entry_allowed: 0.93,
        };

        // Entry 0.90, SL 0.85, TP 0.91
        // Risk = (0.90 - 0.85) * 10 = 0.50
        // Reward = (0.91 - 0.90) * 10 = 0.10
        // R = 0.10 / 0.50 = 0.2 (too low!)
        let result = validator.validate_trade(0.90, "UP", &EntryType::Dip, 0.85, Some(0.91), 10.0);

        assert!(result.is_err());
        match result {
            Err(RejectionReason::InsufficientR {
                r_ratio,
                min_required,
            }) => {
                assert!(r_ratio < min_required);
                assert_eq!(min_required, 1.2);
            }
            _ => panic!("Expected InsufficientR rejection"),
        }
    }

    #[test]
    fn test_approve_good_trade() {
        let validator = TradeValidator {
            min_r_mult: 1.2,
            max_entry_allowed: 0.93,
        };

        // Entry 0.88, SL 0.74, TP 0.965
        // Risk = (0.88 - 0.74) * 10 = 1.40
        // Reward = (0.965 - 0.88) * 10 = 0.85
        // R = 0.85 / 1.40 = 0.607... wait, this is < 1.2
        // Let me recalculate: TP should be higher
        // Entry 0.88, SL 0.74, TP 0.97
        // Risk = 1.40, Reward = (0.97 - 0.88) * 10 = 0.90
        // R = 0.90 / 1.40 = 0.64... still too low
        // Let me use a better example:
        // Entry 0.85, SL 0.72, TP 0.97
        // Risk = (0.85 - 0.72) * 10 = 1.30
        // Reward = (0.97 - 0.85) * 10 = 1.20
        // R = 1.20 / 1.30 = 0.92... still not enough
        // Entry 0.82, SL 0.72, TP 0.95
        // Risk = (0.82 - 0.72) * 10 = 1.00
        // Reward = (0.95 - 0.82) * 10 = 1.30
        // R = 1.30 / 1.00 = 1.3 ✓
        let result = validator.validate_trade(0.82, "UP", &EntryType::Dip, 0.72, Some(0.95), 10.0);

        assert!(result.is_ok());
        match result {
            Ok(TradeApproval::Approved {
                r_ratio,
                risk_dollars,
                reward_dollars,
            }) => {
                assert!(r_ratio >= 1.2);
                assert!(
                    (risk_dollars - 1.0).abs() < 1e-10,
                    "Risk dollars expected 1.0, got {}",
                    risk_dollars
                );
                assert!(
                    (reward_dollars - 1.3).abs() < 1e-10,
                    "Reward dollars expected 1.3, got {}",
                    reward_dollars
                );
            }
            _ => panic!("Expected Approved"),
        }
    }
}
