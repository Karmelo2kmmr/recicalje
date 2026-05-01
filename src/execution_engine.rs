use crate::api::{
    cancel_protective_order, get_actual_balance, place_floor_sell, place_market_sell,
    ExecutorResponse,
};
use log::{error, info, warn};
use std::error::Error;

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

pub struct ExecutionEngine {
    pub limit_sell_attempts: u32,
    pub max_limit_attempts: u32,
}

impl ExecutionEngine {
    pub fn new() -> Self {
        Self {
            limit_sell_attempts: 0,
            max_limit_attempts: 3,
        }
    }

    fn hard_sl_exit_floor() -> f64 {
        env_f64("HARD_SL_EXIT_FLOOR", 0.47).clamp(0.01, 0.99)
    }

    fn allow_hard_sl_market_dump() -> bool {
        env_bool("ALLOW_HARD_SL_MARKET_DUMP", false)
    }

    pub async fn close_position(
        &mut self,
        client: &reqwest::Client,
        token_id: &str,
        shares: f64,
        target_price: f64,
        reason: &str,
    ) -> Result<ExecutorResponse, Box<dyn Error>> {
        let on_chain_bal = match get_actual_balance(token_id).await {
            Ok(bal) => {
                if bal > 0.0 {
                    bal
                } else {
                    shares
                }
            }
            Err(_) => shares,
        };

        info!(
            "EXIT INITIATED (Reason: {}) | Qty Tracked: {:.4} | Qty On-Chain: {:.4} | Target Price: {:.4}",
            reason, shares, on_chain_bal, target_price
        );

        if reason == "HARD_SL" {
            let configured_floor =
                Self::hard_sl_exit_floor().min(crate::risk_engine::HARD_SL_PRICE);
            let sl_floor = if target_price < configured_floor {
                warn!(
                    "Hard SL breached below configured floor: market {:.4} < floor {:.4}. Holding conservative protective floor.",
                    target_price, configured_floor
                );
                configured_floor
            } else {
                target_price.clamp(configured_floor, crate::risk_engine::HARD_SL_PRICE)
            };

            if self.limit_sell_attempts >= self.max_limit_attempts
                && Self::allow_hard_sl_market_dump()
            {
                warn!(
                    "Fallback: max limit attempts reached ({}). Market dump re-enabled by ALLOW_HARD_SL_MARKET_DUMP=true.",
                    self.max_limit_attempts
                );
                return place_market_sell(client, token_id, on_chain_bal, 0.01).await;
            }

            info!("Hard SL exit using protective floor {:.4}", sl_floor);

            let resp = place_floor_sell(client, token_id, on_chain_bal, sl_floor).await;
            return match resp {
                Ok(r) => {
                    self.limit_sell_attempts = 0;
                    Ok(r)
                }
                Err(e) => {
                    self.limit_sell_attempts += 1;
                    error!(
                        "Hard SL protective sell attempt {} failed at floor {:.4}: {}",
                        self.limit_sell_attempts, sl_floor, e
                    );
                    Err(e)
                }
            };
        }

        // P0 FIX: If target_price is 0.0 (empty orderbook), the formula
        // (target_price - 0.01).max(0.10) = 0.10 which causes a massive unnecessary loss.
        // Refuse to sell with a zero reference price — caller must retry with a valid price.
        if target_price <= 0.0 {
            error!(
                "close_position rejected: target_price={:.4} is invalid (empty orderbook?) for reason={}",
                target_price, reason
            );
            return Err("close_position: target_price is 0.0 — refusing to sell at floor. Retry with valid market price.".into());
        }

        let floor_price = (target_price - 0.01).max(0.10);
        let resp = place_floor_sell(client, token_id, on_chain_bal, floor_price).await;

        match resp {
            Ok(r) => {
                self.limit_sell_attempts = 0;
                Ok(r)
            }
            Err(e) => {
                self.limit_sell_attempts += 1;
                error!(
                    "Limit sell attempt {} failed at floor {:.4}: {}",
                    self.limit_sell_attempts, floor_price, e
                );
                Err(e)
            }
        }
    }

    pub async fn cleanup_market_orders(
        id: &str,
        protective_id: Option<String>,
        dca_id: Option<String>,
        client: &reqwest::Client,
    ) {
        // P1 FIX: Previously used `let _ =` which silently swallowed cancel failures.
        // A failed cancel leaves a GTC order live — it can fill later and create a
        // second unintended sell, over-selling more shares than held.
        if let Some(pid) = protective_id {
            info!("Cancelling protective order {} for {}", pid, id);
            match cancel_protective_order(client, &pid).await {
                Ok(status) => info!("Protective order {} cancelled: final_status={}", pid, status),
                Err(e) => error!(
                    "CANCEL FAILED for protective order {} on {} — order may still be live: {}",
                    pid, id, e
                ),
            }
        }
        if let Some(did) = dca_id {
            info!("Cancelling DCA order {} for {}", did, id);
            match cancel_protective_order(client, &did).await {
                Ok(status) => info!("DCA order {} cancelled: final_status={}", did, status),
                Err(e) => error!(
                    "CANCEL FAILED for DCA order {} on {} — order may still be live: {}",
                    did, id, e
                ),
            }
        }
    }
}
