use crate::api::{
    cancel_protective_order, get_actual_balance, place_floor_sell, place_market_sell,
    ExecutorResponse,
};
use log::{error, info, warn};
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, Instant};

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

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Poly425Decision {
    Allow,
    Cooldown { remaining_secs: u64 },
}

#[derive(Debug, Clone, Copy)]
pub struct Poly425Outcome {
    pub consecutive_425s: u32,
    pub silent_telegram: bool,
    pub killed_market: bool,
    pub kill_bot: bool,
    pub cooldown_secs: u64,
    pub latency_score_ms: u64,
}

#[derive(Debug, Clone)]
struct Poly425MarketState {
    consecutive_425s: u32,
    cooldown_until: Option<Instant>,
    last_orderbook_seen: Option<Instant>,
    last_successful_order: Option<Instant>,
    latency_score_ms: u64,
}

impl Default for Poly425MarketState {
    fn default() -> Self {
        Self {
            consecutive_425s: 0,
            cooldown_until: None,
            last_orderbook_seen: None,
            last_successful_order: None,
            latency_score_ms: 0,
        }
    }
}

#[derive(Debug, Default)]
pub struct Poly425Guard {
    markets: HashMap<String, Poly425MarketState>,
}

impl Poly425Guard {
    pub fn new() -> Self {
        Self::default()
    }

    fn consecutive_limit() -> u32 {
        env_u32("POLY_CONSECUTIVE_425_LIMIT", 3).max(1)
    }

    fn cooldown_secs() -> u64 {
        env_u64("POLY_425_COOLDOWN_SECS", 60).max(1)
    }

    fn silent_after() -> u32 {
        env_u32("POLY_SILENT_AFTER_CONSECUTIVE", 2).max(1)
    }

    fn latency_window_ms() -> u64 {
        env_u64("POLY_CLOB_LATENCY_WINDOW_MS", 500).max(1)
    }

    pub fn record_orderbook_seen(&mut self, market_key: &str) {
        self.markets
            .entry(market_key.to_string())
            .or_default()
            .last_orderbook_seen = Some(Instant::now());
    }

    pub fn before_polymarket_order(&mut self, market_key: &str) -> Poly425Decision {
        let state = self.markets.entry(market_key.to_string()).or_default();
        if let Some(until) = state.cooldown_until {
            let now = Instant::now();
            if until > now {
                return Poly425Decision::Cooldown {
                    remaining_secs: until.duration_since(now).as_secs().max(1),
                };
            }
            state.cooldown_until = None;
        }

        Poly425Decision::Allow
    }

    pub fn record_success(&mut self, market_key: &str) {
        let state = self.markets.entry(market_key.to_string()).or_default();
        let now = Instant::now();
        if let Some(seen) = state.last_orderbook_seen {
            state.latency_score_ms = now.duration_since(seen).as_millis() as u64;
        }
        state.consecutive_425s = 0;
        state.cooldown_until = None;
        state.last_successful_order = Some(now);
    }

    pub fn record_425(&mut self, market_key: &str) -> Poly425Outcome {
        let state = self.markets.entry(market_key.to_string()).or_default();
        state.consecutive_425s = state.consecutive_425s.saturating_add(1);

        let now = Instant::now();
        state.latency_score_ms = state
            .last_orderbook_seen
            .map(|seen| now.duration_since(seen).as_millis() as u64)
            .unwrap_or(0);

        let mut cooldown_secs = 2;
        let mut killed_market = false;
        let limit = Self::consecutive_limit();

        if state.consecutive_425s >= limit {
            cooldown_secs = Self::cooldown_secs();
            killed_market = true;
        } else if state.consecutive_425s >= 2 {
            cooldown_secs = 10;
        }

        state.cooldown_until = Some(now + Duration::from_secs(cooldown_secs));

        Poly425Outcome {
            consecutive_425s: state.consecutive_425s,
            silent_telegram: state.consecutive_425s >= Self::silent_after(),
            killed_market,
            kill_bot: state.consecutive_425s > limit,
            cooldown_secs,
            latency_score_ms: state.latency_score_ms,
        }
    }

    pub fn is_latency_high(&self, market_key: &str) -> bool {
        self.markets
            .get(market_key)
            .map(|state| state.latency_score_ms > 2_000)
            .unwrap_or(false)
    }

    pub fn is_orderbook_desynced(&self, market_key: &str) -> bool {
        let Some(state) = self.markets.get(market_key) else {
            return false;
        };
        let Some(seen) = state.last_orderbook_seen else {
            return false;
        };
        let Some(success) = state.last_successful_order else {
            return false;
        };
        seen > success
            && seen.duration_since(success).as_millis() as u64 > Self::latency_window_ms()
    }
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
                Ok(status) => info!(
                    "Protective order {} cancelled: final_status={}",
                    pid, status
                ),
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
