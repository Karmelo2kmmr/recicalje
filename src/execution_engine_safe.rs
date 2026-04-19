use std::error::Error;

use log::{error, info, warn};

use crate::api::{
    cancel_protective_order, get_actual_balance, get_orderbook_depth, place_fak_sell,
    place_floor_sell, place_market_sell, ExecutorResponse,
};

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
        env_f64("HARD_SL_EXIT_FLOOR", 0.40).clamp(0.01, 0.99)
    }

    fn allow_hard_sl_market_dump() -> bool {
        env_bool("ALLOW_HARD_SL_MARKET_DUMP", false)
    }

    fn hard_sl_force_market() -> bool {
        env_bool("HARD_SL_FORCE_MARKET", true)
    }

    fn hard_sl_emergency_limit() -> f64 {
        env_f64("HARD_SL_EMERGENCY_LIMIT", 0.01).clamp(0.01, 0.99)
    }

    fn min_sell_qty() -> f64 {
        env_f64("MIN_SELL_QTY", 0.01).max(0.0)
    }

    fn hard_sl_stage_offsets() -> [f64; 4] {
        [0.0, 0.01, 0.02, 0.03]
    }

    fn time_exit_stage_offsets() -> [f64; 5] {
        [0.0, 0.01, 0.02, 0.03, 0.05]
    }

    fn hard_sl_stage_price(best_bid: f64, offset: f64, floor: f64) -> f64 {
        (best_bid - offset).max(floor).clamp(0.01, 0.99)
    }

    fn visible_bid_size(metrics: &crate::api::OrderbookMetrics) -> f64 {
        metrics
            .bids_depth
            .first()
            .map(|(_, size)| *size)
            .filter(|size| size.is_finite() && *size > 0.0)
            .unwrap_or(0.0)
    }

    fn time_exit_floor() -> f64 {
        env_f64("TIME_EXIT_MIN_FLOOR", 0.05).clamp(0.01, 0.99)
    }

    fn allow_time_exit_market_dump() -> bool {
        env_bool("ALLOW_TIME_EXIT_MARKET_DUMP", true)
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

        if on_chain_bal <= Self::min_sell_qty() {
            self.limit_sell_attempts = 0;
            return Err(format!(
                "Sell skipped: on-chain balance {:.6} is below minimum sell quantity {:.6}",
                on_chain_bal,
                Self::min_sell_qty()
            )
            .into());
        }

        if reason == "HARD_SL" {
            let configured_floor =
                Self::hard_sl_exit_floor().min(crate::risk_engine::HARD_SL_PRICE);
            let emergency_limit = Self::hard_sl_emergency_limit();
            let book = get_orderbook_depth(client, token_id).await;
            let live_best_bid = book.best_bid.unwrap_or(target_price).clamp(0.01, 0.99);
            let immediate_limit = if Self::hard_sl_force_market() {
                emergency_limit
            } else {
                configured_floor.min(live_best_bid)
            };

            info!(
                "HARD SL immediate exit | best_bid {:.4} | target {:.4} | floor {:.4} | emergency_limit {:.4} | qty {:.6}",
                live_best_bid,
                target_price,
                configured_floor,
                immediate_limit,
                on_chain_bal
            );

            let mut total_attempts = 0;
            let mut last_order_id = "unknown".to_string();
            let mut total_filled = 0.0;
            let mut total_notional = 0.0;

            match place_fak_sell(client, token_id, on_chain_bal, immediate_limit).await {
                Ok(resp) => {
                    total_attempts += resp.attempts.max(1);
                    last_order_id = resp.order_id.clone();
                    if resp.shares > 0.0 {
                        let fill_price = resp.fill_price.unwrap_or(immediate_limit);
                        total_filled += resp.shares;
                        total_notional += resp.shares * fill_price;
                    }
                }
                Err(e) => {
                    total_attempts += 1;
                    warn!("HARD SL immediate FAK failed: {}", e);
                }
            }

            let remaining = get_actual_balance(token_id)
                .await
                .unwrap_or((on_chain_bal - total_filled).max(0.0));

            if remaining <= Self::min_sell_qty() {
                self.limit_sell_attempts = 0;
                return Ok(ExecutorResponse {
                    order_id: last_order_id,
                    shares: total_filled.max(on_chain_bal),
                    fill_price: if total_filled > 0.0 {
                        Some(total_notional / total_filled)
                    } else {
                        Some(immediate_limit)
                    },
                    reliable: true,
                    attempts: total_attempts.max(1),
                });
            }

            self.limit_sell_attempts += 1;

            if Self::allow_hard_sl_market_dump() {
                warn!(
                    "HARD SL still has {:.6} shares after immediate FAK. Escalating to emergency resting sell at {:.4}.",
                    remaining,
                    emergency_limit
                );
                return place_market_sell(client, token_id, remaining, emergency_limit).await;
            }

            return Err(format!(
                "HARD_SL immediate exit incomplete: remaining {:.6} after {} attempt(s)",
                remaining,
                total_attempts.max(1)
            )
            .into());
        }

        if reason == "TIME_EXIT" {
            let configured_floor = Self::time_exit_floor();
            let mut remaining = on_chain_bal;
            let mut total_filled = 0.0;
            let mut total_notional = 0.0;
            let mut total_attempts = 0;
            let mut last_order_id = "unknown".to_string();

            for offset in Self::time_exit_stage_offsets() {
                remaining = match get_actual_balance(token_id).await {
                    Ok(balance) if balance > 0.0 => balance,
                    _ => remaining,
                };

                if remaining <= Self::min_sell_qty() {
                    self.limit_sell_attempts = 0;
                    break;
                }

                let book = get_orderbook_depth(client, token_id).await;
                let live_best_bid = book.best_bid.unwrap_or(target_price);
                let stage_price =
                    Self::hard_sl_stage_price(live_best_bid, offset, configured_floor);
                let visible_bid_size = Self::visible_bid_size(&book);
                let sell_qty = if visible_bid_size > Self::min_sell_qty() {
                    remaining.min(visible_bid_size)
                } else {
                    remaining
                };

                info!(
                    "TIME EXIT staged FAK attempt {} | best_bid {:.4} | bid_size {:.6} | offset {:.4} | limit {:.4} | remaining {:.6} | sell_qty {:.6}",
                    total_attempts + 1,
                    live_best_bid,
                    visible_bid_size,
                    offset,
                    stage_price,
                    remaining,
                    sell_qty
                );

                match place_fak_sell(client, token_id, sell_qty, stage_price).await {
                    Ok(resp) => {
                        total_attempts += resp.attempts.max(1);
                        last_order_id = resp.order_id.clone();

                        if resp.shares > 0.0 {
                            let fill_price = resp.fill_price.unwrap_or(stage_price);
                            total_filled += resp.shares;
                            total_notional += resp.shares * fill_price;
                        }

                        remaining = get_actual_balance(token_id)
                            .await
                            .unwrap_or((remaining - resp.shares).max(0.0));

                        if remaining <= Self::min_sell_qty() {
                            self.limit_sell_attempts = 0;
                            return Ok(ExecutorResponse {
                                order_id: last_order_id,
                                shares: if total_filled > 0.0 {
                                    total_filled
                                } else {
                                    on_chain_bal
                                },
                                fill_price: if total_filled > 0.0 {
                                    Some(total_notional / total_filled)
                                } else {
                                    Some(stage_price)
                                },
                                reliable: true,
                                attempts: total_attempts.max(1),
                            });
                        }
                    }
                    Err(e) => {
                        total_attempts += 1;
                        let err_msg = e.to_string();
                        if err_msg
                            .to_lowercase()
                            .contains("no orders found to match with fak order")
                        {
                            warn!(
                                "TIME EXIT staged FAK attempt {} found no immediate match at {:.4}; will keep retrying while market is open.",
                                total_attempts, stage_price
                            );
                        } else {
                            warn!(
                                "TIME EXIT staged FAK attempt {} failed at {:.4}: {}",
                                total_attempts, stage_price, err_msg
                            );
                        }
                    }
                }
            }

            self.limit_sell_attempts += 1;
            remaining = get_actual_balance(token_id).await.unwrap_or(remaining);

            if remaining <= Self::min_sell_qty() {
                self.limit_sell_attempts = 0;
                return Ok(ExecutorResponse {
                    order_id: last_order_id,
                    shares: if total_filled > 0.0 {
                        total_filled
                    } else {
                        on_chain_bal
                    },
                    fill_price: if total_filled > 0.0 {
                        Some(total_notional / total_filled)
                    } else {
                        Some(configured_floor)
                    },
                    reliable: true,
                    attempts: total_attempts.max(1),
                });
            }

            if Self::allow_time_exit_market_dump() {
                warn!(
                    "TIME EXIT staged sells exhausted. Final emergency FAK uses floor {:.4} with {:.6} shares remaining.",
                    configured_floor, remaining
                );
                let book = get_orderbook_depth(client, token_id).await;
                let visible_bid_size = Self::visible_bid_size(&book);
                let final_qty = if visible_bid_size > Self::min_sell_qty() {
                    remaining.min(visible_bid_size)
                } else {
                    remaining
                };
                return place_fak_sell(client, token_id, final_qty, configured_floor).await;
            }

            return Err(format!(
                "TIME_EXIT failed: remaining {:.6} still above minimum after {} staged FAK attempts (floor {:.4})",
                remaining,
                total_attempts.max(1),
                configured_floor
            )
            .into());
        }

        if self.limit_sell_attempts >= self.max_limit_attempts {
            warn!(
                "Fallback: max limit attempts reached ({}). Forcing aggressive market sell.",
                self.max_limit_attempts
            );
            return place_market_sell(client, token_id, on_chain_bal, 0.01).await;
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
        if let Some(pid) = protective_id {
            info!("Cancelling protective order {} for {}", pid, id);
            let _ = cancel_protective_order(client, &pid).await;
        }
        if let Some(did) = dca_id {
            info!("Cancelling DCA order {} for {}", did, id);
            let _ = cancel_protective_order(client, &did).await;
        }
    }
}
