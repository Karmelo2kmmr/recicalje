// =====================================================================
// Emergency Decision Engine — Arbitrage Hammer
// Activates when a normal Stop-Loss order fails due to zero liquidity.
// Decides the optimal action to minimize expected loss.
// =====================================================================

use crate::dual_market::{PositionState, Venue};
use chrono::{DateTime, Utc};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

// ─── 1. TYPES ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HedgeSide {
    Yes,
    No,
}

/// A cross-platform hedge pair created when SL fails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgePair {
    pub id: String, // uuid-like key (twin_key + timestamp)
    pub original_venue: Venue,
    pub original_market_id: String, // Kalshi ticker or PM market id
    pub original_pm_token: String,  // PM token used (YES or NO)
    pub original_side: HedgeSide,
    pub original_shares: f64,
    pub original_avg_price: f64,

    pub hedge_venue: Venue,
    pub hedge_market_id: String,
    pub hedge_pm_token: String,
    pub hedge_side: HedgeSide,
    pub hedge_shares: f64,
    pub hedge_avg_price: f64,
    pub hedge_notional: f64, // always HEDGE_SIZE (5.0 USDC)

    pub total_cost: f64,
    pub guaranteed_payout: f64, // min(original_shares, hedge_shares)
    pub locked_loss: f64,       // total_cost - guaranteed_payout
    pub hedge_ratio: f64,

    pub state: PositionState,
    pub created_at: DateTime<Utc>,
    pub coin: String,
    pub twin_key: String,
}

/// All data the decision engine needs. Computed once per SL failure.
#[derive(Debug, Clone)]
pub struct EmergencyContext {
    pub coin: String,
    pub twin_key: String,

    // Original position
    pub original_shares: f64,
    pub original_avg_price: f64,
    pub original_cost: f64,     // shares * avg_price
    pub best_bid_original: f64, // current bid on original side

    // Opposite side (the hedge)
    pub opposite_ask_pm: f64, // best ask for opposite on Polymarket
    pub opposite_ask_km: f64, // best ask for opposite on Kalshi
    pub hedge_size: f64,      // always 5.0 USDC

    // Market data
    pub seconds_to_expiry: i64,
    pub market_panic_score: f64, // 0.0–1.0 derived from volatility ratio

    // Ids for execution
    pub original_venue: Venue,
    pub original_kalshi_ticker: String,
    pub original_pm_token: String,
    pub pm_yes_token: String,
    pub pm_no_token: String,
    pub buy_yes: bool, // original side direction
    pub is_paper: bool,
}

impl EmergencyContext {
    /// Best opposite ask across both platforms.
    pub fn best_opposite_ask(&self) -> f64 {
        let pm = self.opposite_ask_pm;
        let km = self.opposite_ask_km;
        match (pm > 0.0, km > 0.0) {
            (true, true) => pm.min(km),
            (true, false) => pm,
            (false, true) => km,
            _ => 0.0,
        }
    }

    /// Which platform offers cheapest hedge.
    pub fn cheapest_hedge_venue(&self) -> Venue {
        let pm = self.opposite_ask_pm;
        let km = self.opposite_ask_km;
        if pm > 0.0 && (km <= 0.0 || pm <= km) {
            Venue::Polymarket
        } else {
            Venue::Kalshi
        }
    }

    /// Cost to fully hedge (original_shares at opposite_ask).
    pub fn full_hedge_cost(&self) -> f64 {
        self.original_shares * self.best_opposite_ask()
    }

    /// Locked loss if we do a full hedge.
    pub fn locked_loss_full_hedge(&self) -> f64 {
        (self.original_cost + self.full_hedge_cost() - self.original_shares).max(0.0)
    }
}

#[derive(Debug, Clone)]
pub enum EmergencyAction {
    /// Normal SL still viable — sell the original position.
    SellOriginal,
    /// Buy opposite on cheapest platform, full shares covered.
    FullHedge,
    /// Buy opposite covering hedge_ratio of original shares.
    PartialHedge(f64),
    /// Do nothing this tick — wait for liquidity to return.
    WaitLiquidity,
    /// Market expires soon — hold both sides until settlement.
    ExpiryHold,
}

// ─── 2. DECISION ENGINE ──────────────────────────────────────────────────────

pub fn env_f64_em(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

/// Pure decision function. No side effects.
pub fn decide_emergency_action(ctx: &EmergencyContext) -> EmergencyAction {
    let max_hedge_price = env_f64_em("MAX_HEDGE_PRICE", 0.45);
    let max_locked_loss_pct = env_f64_em("MAX_LOCKED_LOSS_PCT", 0.25);
    let min_hedge_ratio = env_f64_em("MIN_HEDGE_RATIO", 0.40);
    let no_trade_zone_secs = env_f64_em("NO_TRADE_ZONE_SECONDS", 45.0) as i64;
    let force_hedge_score = env_f64_em("FORCE_HEDGE_SCORE", 0.80);
    let emergency_min_ratio = env_f64_em("EMERGENCY_MIN_RATIO", 0.50);
    let sl_exit_floor = env_f64_em("HARD_SL_EXIT_FLOOR", 0.47);

    // Rule 1 — If bid recovered above floor, normal sell is viable
    if ctx.best_bid_original >= sl_exit_floor {
        info!(
            "[EMERGENCY] bid={:.3} >= floor={:.3} → SellOriginal",
            ctx.best_bid_original, sl_exit_floor
        );
        return EmergencyAction::SellOriginal;
    }

    // Rule 2 — Too close to expiry, don't open new position
    if ctx.seconds_to_expiry < no_trade_zone_secs {
        info!(
            "[EMERGENCY] {}s to expiry < {}s zone → ExpiryHold",
            ctx.seconds_to_expiry, no_trade_zone_secs
        );
        return EmergencyAction::ExpiryHold;
    }

    let opp_ask = ctx.best_opposite_ask();

    // Rule 3 — Hedge is too expensive
    if opp_ask <= 0.0 || opp_ask > max_hedge_price {
        warn!(
            "[EMERGENCY] opposite_ask={:.3} > max={:.3} → WaitLiquidity",
            opp_ask, max_hedge_price
        );
        return EmergencyAction::WaitLiquidity;
    }

    // Rule 4+5 — Calculate locked loss for full hedge
    let full_locked_loss = ctx.locked_loss_full_hedge();
    let loss_pct = if ctx.original_cost > 0.0 {
        full_locked_loss / ctx.original_cost
    } else {
        1.0
    };

    if loss_pct <= max_locked_loss_pct {
        info!(
            "[EMERGENCY] locked_loss_pct={:.1}% <= max={:.0}% → FullHedge",
            loss_pct * 100.0,
            max_locked_loss_pct * 100.0
        );
        return EmergencyAction::FullHedge;
    }

    // Rule 6+7 — Partial hedge by ratio
    let max_allowed_loss = ctx.original_cost * max_locked_loss_pct;
    let ratio = (max_allowed_loss / full_locked_loss).clamp(min_hedge_ratio, 1.0);

    if ratio >= min_hedge_ratio {
        info!("[EMERGENCY] partial_ratio={:.2} → PartialHedge", ratio);
        return EmergencyAction::PartialHedge(ratio);
    }

    // Rule 8 — Panic override
    if ctx.market_panic_score >= force_hedge_score {
        warn!(
            "[EMERGENCY] panic_score={:.2} >= force={:.2} → PartialHedge(emergency)",
            ctx.market_panic_score, force_hedge_score
        );
        return EmergencyAction::PartialHedge(emergency_min_ratio);
    }

    EmergencyAction::WaitLiquidity
}

// ─── 3. EXECUTION ────────────────────────────────────────────────────────────

pub struct HedgeExecutionResult {
    pub pair: HedgePair,
    pub telegram_msg: String,
}

/// Execute a hedge action. Simulates in paper mode but follows the same logic path.
pub async fn execute_hedge(
    action: &EmergencyAction,
    ctx: &EmergencyContext,
    http_client: &reqwest::Client,
) -> Option<HedgeExecutionResult> {
    let opp_ask = ctx.best_opposite_ask();
    if opp_ask <= 0.0 {
        warn!("[EMERGENCY] No opposite ask available, cannot hedge.");
        return None;
    }

    let hedge_venue = ctx.cheapest_hedge_venue();
    let hedge_side = if ctx.buy_yes {
        HedgeSide::No
    } else {
        HedgeSide::Yes
    }; // opposite of original

    // Determine shares to hedge based on action
    let ratio = match action {
        EmergencyAction::FullHedge => 1.0,
        EmergencyAction::PartialHedge(r) => *r,
        _ => return None,
    };

    // Fixed 5 USDC hedge — compute shares we can buy
    let hedge_notional = 5.0_f64;
    let hedge_price = opp_ask.min(1.0 - 0.01); // never pay > 0.99
    let hedge_shares = (hedge_notional / hedge_price).floor().max(1.0) * ratio;

    // In paper mode simulate; in live send real limit order
    let filled_price = if ctx.is_paper {
        info!("[EMERGENCY PAPER] Would buy hedge | venue={:?} | side={:?} | shares={:.2} | price={:.4}", hedge_venue, hedge_side, hedge_shares, hedge_price);
        hedge_price
    } else {
        // Live execution — attempt limit buy on cheapest venue
        match &hedge_venue {
            Venue::Kalshi => {
                // Real Kalshi order would go here via kalshi_client — wired in main.rs
                info!("[EMERGENCY LIVE] Kalshi hedge order to be placed via main.rs executor");
                hedge_price
            }
            Venue::Polymarket => {
                // Real Poly order via CLOB daemon
                info!("[EMERGENCY LIVE] Polymarket hedge order to be placed via main.rs executor");
                hedge_price
            }
        }
    };

    let hedge_pm_token = if hedge_side == HedgeSide::Yes {
        ctx.pm_yes_token.clone()
    } else {
        ctx.pm_no_token.clone()
    };

    let total_cost = ctx.original_cost + hedge_notional;
    let guaranteed_payout = hedge_shares.min(ctx.original_shares);
    let locked_loss = (total_cost - guaranteed_payout).max(0.0);
    let locked_loss_pct = if total_cost > 0.0 {
        locked_loss / total_cost * 100.0
    } else {
        0.0
    };

    let now = Utc::now();
    let pair_id = format!("{}-hedge-{}", ctx.twin_key, now.timestamp());

    let pair = HedgePair {
        id: pair_id,
        original_venue: ctx.original_venue.clone(),
        original_market_id: ctx.original_kalshi_ticker.clone(),
        original_pm_token: ctx.original_pm_token.clone(),
        original_side: if ctx.buy_yes {
            HedgeSide::Yes
        } else {
            HedgeSide::No
        },
        original_shares: ctx.original_shares,
        original_avg_price: ctx.original_avg_price,
        hedge_venue,
        hedge_market_id: ctx.original_kalshi_ticker.clone(),
        hedge_pm_token,
        hedge_side,
        hedge_shares,
        hedge_avg_price: filled_price,
        hedge_notional,
        total_cost,
        guaranteed_payout,
        locked_loss,
        hedge_ratio: ratio,
        state: PositionState::Hedged,
        created_at: now,
        coin: ctx.coin.clone(),
        twin_key: ctx.twin_key.clone(),
    };

    let mode_label = if ctx.is_paper {
        "📄 PAPER"
    } else {
        "🔴 LIVE"
    };
    let telegram_msg = format!(
        "🛡️ *HEDGE DE EMERGENCIA* | {}\n• Activo: *{}*\n• Ratio: {:.0}%\n• Pérdida bloqueada: *{:.1}%* (${:.2})\n• Coste hedge: ${:.2} @ {:.3}\n• Payout garantizado: ${:.2}",
        mode_label,
        ctx.coin,
        ratio * 100.0,
        locked_loss_pct,
        locked_loss,
        hedge_notional,
        filled_price,
        guaranteed_payout,
    );

    Some(HedgeExecutionResult { pair, telegram_msg })
}

// ─── 4. HEDGE PAIR MANAGEMENT ────────────────────────────────────────────────

pub struct PairStatus {
    pub pair_value: f64,
    pub pair_pnl: f64,
    pub should_unwind: bool,
    pub should_hold: bool,
    pub telegram_msg: Option<String>,
}

/// Called every loop tick for positions in Hedged state.
pub fn evaluate_hedge_pair(
    pair: &HedgePair,
    bid_original: f64,
    bid_hedge: f64,
    seconds_to_expiry: i64,
) -> PairStatus {
    let target_exit_pnl = env_f64_em("TARGET_EXIT_PNL", -0.08); // -8% max loss to trigger unwind

    let pair_value = bid_original * pair.original_shares + bid_hedge * pair.hedge_shares;
    let pair_pnl = pair_value - pair.total_cost;
    let pair_pnl_pct = if pair.total_cost > 0.0 {
        pair_pnl / pair.total_cost
    } else {
        -1.0
    };

    // Hold until expiry if very close
    if seconds_to_expiry < 60 {
        return PairStatus {
            pair_value,
            pair_pnl,
            should_unwind: false,
            should_hold: true,
            telegram_msg: None,
        };
    }

    // Unwind if pair P&L has recovered enough
    let should_unwind = pair_pnl_pct >= target_exit_pnl && bid_original > 0.01 && bid_hedge > 0.01;

    let telegram_msg = if should_unwind {
        Some(format!(
            "🔓 *UNWIND HEDGE* | {}\n• PairPnL: *{:+.1}%* (${:+.2})\n• Cerrando ambas piernas automáticamente.",
            pair.coin,
            pair_pnl_pct * 100.0,
            pair_pnl,
        ))
    } else {
        None
    };

    PairStatus {
        pair_value,
        pair_pnl,
        should_unwind,
        should_hold: !should_unwind,
        telegram_msg,
    }
}

// ─── 5. PERSISTENCE ──────────────────────────────────────────────────────────

const HEDGE_STATE_FILE: &str = "hedge_pairs.json";

pub fn save_hedge_pairs(pairs: &[HedgePair]) {
    let path = PathBuf::from(HEDGE_STATE_FILE);
    if let Ok(json) = serde_json::to_string_pretty(pairs) {
        if let Err(e) = fs::write(&path, json) {
            warn!("[EMERGENCY] Failed to save hedge_pairs.json: {}", e);
        }
    }
}

pub fn load_hedge_pairs() -> Vec<HedgePair> {
    let path = PathBuf::from(HEDGE_STATE_FILE);
    if !path.exists() {
        return Vec::new();
    }
    match fs::read_to_string(&path) {
        Ok(json) => serde_json::from_str::<Vec<HedgePair>>(&json).unwrap_or_default(),
        Err(e) => {
            warn!("[EMERGENCY] Failed to load hedge_pairs.json: {}", e);
            Vec::new()
        }
    }
}
