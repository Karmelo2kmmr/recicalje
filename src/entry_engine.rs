use crate::volatility::{VolatilityMetrics, VolatilityState};
use std::time::Instant;

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

pub fn distance_threshold_for(symbol: &str, state: VolatilityState) -> f64 {
    let asset = if symbol.contains("BTC") {
        "BTC"
    } else if symbol.contains("ETH") {
        "ETH"
    } else if symbol.contains("SOL") {
        "SOL"
    } else if symbol.contains("XRP") {
        "XRP"
    } else {
        "GENERIC"
    };

    let (suffix, fallback) = match (asset, state) {
        ("BTC", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 77.0),
        ("BTC", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 77.0),
        ("BTC", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 77.0),
        ("ETH", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 2.23),
        ("ETH", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 2.23),
        ("ETH", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 2.23),
        ("SOL", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 0.16),
        ("SOL", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 0.16),
        ("SOL", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 0.16),
        ("XRP", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 0.0018),
        ("XRP", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 0.0018),
        ("XRP", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 0.0018),
        ("GENERIC", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 0.00102),
        ("GENERIC", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 0.00146),
        ("GENERIC", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 0.00195),
        _ => ("NEUTRAL_HIGH", 0.00146),
    };

    env_f64(
        &format!("{}_DISTANCE_THRESHOLD_{}", asset, suffix),
        fallback,
    )
}

pub fn distance_threshold_pct_for(symbol: &str, state: VolatilityState) -> f64 {
    let asset = if symbol.contains("BTC") {
        "BTC"
    } else if symbol.contains("ETH") {
        "ETH"
    } else if symbol.contains("SOL") {
        "SOL"
    } else if symbol.contains("XRP") {
        "XRP"
    } else {
        "GENERIC"
    };

    let (suffix, fallback) = match (asset, state) {
        ("SOL", VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 0.0018),
        ("SOL", VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 0.0024),
        ("SOL", VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 0.0030),
        (_, VolatilityState::LowNeutral) => ("LOW_NEUTRAL", 0.0),
        (_, VolatilityState::NeutralHigh) => ("NEUTRAL_HIGH", 0.0),
        (_, VolatilityState::HighSuperhigh) => ("HIGH_SUPERHIGH", 0.0),
    };

    env_f64(
        &format!("{}_DISTANCE_THRESHOLD_PCT_{}", asset, suffix),
        fallback,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceCheckResult {
    Passed,
    DistanceBlocked,
    DirectionBlocked,
    MissingData,
}

pub struct EntryEngine {
    pub last_attempt: Option<Instant>,
    pub last_failed_attempt: Option<Instant>,
    pub alert_sent: bool,
    pub is_shot_entry: bool,
    pub entry_type: Option<crate::tracker::EntryType>,
}

impl EntryEngine {
    pub fn new() -> Self {
        Self {
            last_attempt: None,
            last_failed_attempt: None,
            alert_sent: false,
            is_shot_entry: false,
            entry_type: None,
        }
    }

    pub fn is_hibernation_window(elapsed_seconds: i32) -> bool {
        elapsed_seconds < 550
    }

    pub fn is_kill_zone(elapsed_seconds: i32) -> bool {
        elapsed_seconds >= 550 && elapsed_seconds <= 815
    }

    pub fn check_volatility_filter(&self, metrics: &VolatilityMetrics) -> bool {
        let max_vol_zscore = env_f64("MAX_VOL_ZSCORE", 2.5);
        if metrics.z_score > max_vol_zscore {
            log::warn!(
                "Extreme volatility detected (Z-Score: {:.2} > {:.2}). Skipping entries.",
                metrics.z_score,
                max_vol_zscore
            );
            return false;
        }

        if env_bool("BLOCK_HIGH_SUPERHIGH", true) && metrics.state == VolatilityState::HighSuperhigh
        {
            log::warn!(
                "Blocked by volatility regime {:?}. now={:.4}% base200={:.4}% z={:.2}",
                metrics.state,
                metrics.vol_now,
                metrics.vol_ma20,
                metrics.z_score
            );
            return false;
        }

        true
    }

    pub fn evaluate_triggers(
        &self,
        current_price: f64,
        trigger_price: f64,
        max_entry: f64,
        min_entry: f64,
    ) -> bool {
        current_price >= trigger_price && current_price >= min_entry && current_price <= max_entry
    }

    pub fn check_asset_distance(
        &self,
        current_binance_price: Option<f64>,
        price_to_beat: Option<f64>,
        symbol: &str,
        side: &str,
        volatility_state: VolatilityState,
        _elapsed_seconds: i32,
        _z_score: f64,
    ) -> DistanceCheckResult {
        let mut absolute_threshold = distance_threshold_for(symbol, volatility_state);
        let divergence_threshold = env_f64("BINANCE_DIRECTION_BLOCK_PCT", 0.0003);

        if let (Some(current), Some(base_price)) = (current_binance_price, price_to_beat) {
            if base_price <= 0.0 {
                return DistanceCheckResult::MissingData;
            }

            let pct_threshold = distance_threshold_pct_for(symbol, volatility_state);
            if pct_threshold > 0.0 {
                absolute_threshold = absolute_threshold.max(base_price * pct_threshold);
            }

            let pct_change = (current - base_price) / base_price;
            let absolute_movement = (current - base_price).abs();

            log::debug!(
                "Distance check {} {} | state {:?} | current {:.6} | ref {:.6} | move {:.6} | required {:.6} | pct {:.4}%",
                symbol,
                side,
                volatility_state,
                current,
                base_price,
                absolute_movement,
                absolute_threshold,
                pct_change * 100.0
            );

            // ── CRITICAL: PTB ALIGNMENT CHECK ──────────────────────────────
            // If BTC is BELOW the strike (PTB) and we want to buy UP = instant loss.
            // If BTC is ABOVE the strike (PTB) and we want to buy DOWN = instant loss.
            // This is the #1 cause of capital destruction.
            let ptb_gap_pct = (current - base_price) / base_price;
            if side == "UP" && current < base_price {
                log::warn!(
                    "PTB ALIGNMENT BLOCK: {} UP entry rejected. BTC {:.2} is BELOW strike {:.2} (gap={:.2}%). Would be an instant loser.",
                    symbol, current, base_price, ptb_gap_pct * 100.0
                );
                return DistanceCheckResult::DirectionBlocked;
            }
            if side == "DOWN" && current > base_price {
                log::warn!(
                    "PTB ALIGNMENT BLOCK: {} DOWN entry rejected. BTC {:.2} is ABOVE strike {:.2} (gap={:.2}%). Would be an instant loser.",
                    symbol, current, base_price, ptb_gap_pct * 100.0
                );
                return DistanceCheckResult::DirectionBlocked;
            }
            // ── END PTB ALIGNMENT CHECK ─────────────────────────────────────

            if side == "UP" && pct_change <= -divergence_threshold {
                log::debug!(
                    "UP entry blocked by Binance divergence on {}: change {:.4}%",
                    symbol,
                    pct_change * 100.0
                );
                return DistanceCheckResult::DirectionBlocked;
            }

            if side == "DOWN" && pct_change >= divergence_threshold {
                log::debug!(
                    "DOWN entry blocked by Binance divergence on {}: change {:.4}%",
                    symbol,
                    pct_change * 100.0
                );
                return DistanceCheckResult::DirectionBlocked;
            }

            if absolute_movement < absolute_threshold {
                log::debug!(
                    "Distance filter blocked {} on {}: move {:.6} < required {:.6} (Binance {:.6} vs PriceToBeat {:.6})",
                    side,
                    symbol,
                    absolute_movement,
                    absolute_threshold,
                    current,
                    base_price
                );
                return DistanceCheckResult::DistanceBlocked;
            }

            DistanceCheckResult::Passed
        } else {
            log::warn!(
                "Distance filter blocked entry: missing current Binance price or Price to Beat for {}",
                symbol
            );
            DistanceCheckResult::MissingData
        }
    }
}
