use log::{info, warn};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct StartupConfig {
    pub live_mode: bool,
    pub scan_interval_sec: u64,
    pub refresh_rate_ms: u64,
    pub max_slippage: f64,
    pub position_size: f64,
    pub max_open_positions: usize,
    pub max_total_exposure_usdc: f64,
    pub max_venue_exposure_usdc: f64,
    pub min_entry_price: f64,
    pub max_entry_price: f64,
    pub hard_sl_price: f64,
    pub hard_sl_exit_floor: f64,
}

pub const LIVE_CROSS_VENUE_HEDGE_ACK: &str = "I_UNDERSTAND_LIVE_HEDGE_RECOVERY";

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(name: &str, default: bool) -> Result<bool, String> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<bool>()
            .map_err(|_| format!("{name} must be 'true' or 'false', got '{}'", value.trim())),
        Err(_) => Ok(default),
    }
}

fn env_f64(name: &str, default: f64) -> Result<f64, String> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<f64>()
            .map_err(|_| format!("{name} must be numeric, got '{}'", value.trim())),
        Err(_) => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .map_err(|_| format!("{name} must be an integer, got '{}'", value.trim())),
        Err(_) => Ok(default),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, String> {
    match std::env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .map_err(|_| format!("{name} must be an integer, got '{}'", value.trim())),
        Err(_) => Ok(default),
    }
}

pub fn cross_venue_hedge_enabled(live_mode: bool) -> Result<bool, String> {
    if live_mode {
        env_bool("ALLOW_LIVE_CROSS_VENUE_HEDGE", false)
    } else {
        env_bool("ALLOW_CROSS_VENUE_HEDGE", false)
    }
}

pub fn validate_live_cross_venue_hedge_settings(
    enabled: bool,
    ack: Option<&str>,
    hedge_size: f64,
    position_size: f64,
    max_total_exposure_usdc: f64,
    max_venue_exposure_usdc: f64,
    max_hedge_price: f64,
    sl_liquidity_threshold: f64,
    hedge_sl_gap: f64,
) -> Result<(), String> {
    if !enabled {
        return Ok(());
    }

    if ack != Some(LIVE_CROSS_VENUE_HEDGE_ACK) {
        return Err(format!(
            "LIVE_CROSS_VENUE_HEDGE_ACK={} is required when ALLOW_LIVE_CROSS_VENUE_HEDGE=true.",
            LIVE_CROSS_VENUE_HEDGE_ACK
        ));
    }

    if !hedge_size.is_finite() || hedge_size <= 0.0 {
        return Err(format!(
            "HEDGE_SIZE ({:.4}) must be a positive finite USDC value.",
            hedge_size
        ));
    }

    if hedge_size > position_size {
        return Err(format!(
            "HEDGE_SIZE ({:.2}) cannot exceed POSITION_SIZE ({:.2}) for live recovery.",
            hedge_size, position_size
        ));
    }

    if hedge_size > max_total_exposure_usdc || hedge_size > max_venue_exposure_usdc {
        return Err(format!(
            "HEDGE_SIZE ({:.2}) must fit max exposure limits ({:.2}/{:.2}).",
            hedge_size, max_total_exposure_usdc, max_venue_exposure_usdc
        ));
    }

    if !(0.01..=0.95).contains(&max_hedge_price) {
        return Err(format!(
            "MAX_HEDGE_PRICE ({:.4}) must stay between 0.01 and 0.95.",
            max_hedge_price
        ));
    }

    if !(0.0..=0.95).contains(&sl_liquidity_threshold) {
        return Err(format!(
            "SL_LIQUIDITY_THRESHOLD ({:.4}) must stay between 0.00 and 0.95.",
            sl_liquidity_threshold
        ));
    }

    if !(0.0..=0.95).contains(&hedge_sl_gap) {
        return Err(format!(
            "HEDGE_SL_GAP ({:.4}) must stay between 0.00 and 0.95.",
            hedge_sl_gap
        ));
    }

    Ok(())
}

fn looks_like_placeholder(value: &str) -> bool {
    let lower = value.to_lowercase();
    lower.contains("aqui")
        || lower.contains("your_")
        || lower.contains("tu_")
        || lower.contains("example")
        || lower.contains("replace_me")
}

fn sync_alias(primary: &str, alias: &str) {
    match (env_string(primary), env_string(alias)) {
        (Some(primary_value), None) => std::env::set_var(alias, primary_value),
        (None, Some(alias_value)) => std::env::set_var(primary, alias_value),
        _ => {}
    }
}

pub fn validate_startup() -> Result<StartupConfig, String> {
    sync_alias("POLYMARKET_PRIVATE_KEY", "WALLET_PRIVATE_KEY");

    let live_mode = !env_bool("PAPER_TRADING", true)?;
    let scan_interval_sec = env_u64("SCAN_INTERVAL_SEC", 30)?;
    let refresh_rate_ms = env_u64("REFRESH_RATE_MS", 1000)?;
    let max_slippage = env_f64("MAX_SLIPPAGE", 0.02)?;
    let position_size = env_f64("POSITION_SIZE", env_f64("DEFAULT_SIZE", 5.0)?)?;
    let max_open_positions = env_usize("MAX_OPEN_POSITIONS", 2)?;
    let max_total_exposure_usdc = env_f64("MAX_TOTAL_EXPOSURE_USDC", position_size * 2.0)?;
    let max_venue_exposure_usdc = env_f64("MAX_VENUE_EXPOSURE_USDC", max_total_exposure_usdc)?;
    let min_entry_price = env_f64("MIN_ENTRY_PRICE", 0.0)?;
    let max_entry_price = env_f64("MAX_ENTRY_PRICE", 0.92)?;
    let hard_sl_price = env_f64("HARD_SL_PRICE", crate::risk_engine::HARD_SL_PRICE)?;
    let hard_sl_exit_floor = env_f64("HARD_SL_EXIT_FLOOR", 0.61)?;
    let dca_min_price = env_f64("DCA_MIN_PRICE", crate::risk_engine::DCA_MIN_PRICE)?;
    let dca_target_price = env_f64("DCA_TARGET_PRICE", 0.745)?;
    let dca_start_price = env_f64("DCA_START_PRICE", crate::risk_engine::DCA_START_PRICE)?;

    if scan_interval_sec == 0 || scan_interval_sec > 300 {
        return Err(format!(
            "SCAN_INTERVAL_SEC={} is invalid. Use a value between 1 and 300 seconds.",
            scan_interval_sec
        ));
    }

    if refresh_rate_ms < 100 || refresh_rate_ms > 60_000 {
        return Err(format!(
            "REFRESH_RATE_MS={} is invalid. Use a value between 100 and 60000 ms.",
            refresh_rate_ms
        ));
    }

    if !(0.0..=0.10).contains(&max_slippage) {
        return Err(format!(
            "MAX_SLIPPAGE={:.4} is invalid. Keep it between 0.00 and 0.10.",
            max_slippage
        ));
    }

    if !(1.0..=100.0).contains(&position_size) {
        return Err(format!(
            "POSITION_SIZE={:.2} is invalid. Use a value between 1 and 100 USDC.",
            position_size
        ));
    }

    if max_open_positions == 0 || max_open_positions > 20 {
        return Err(format!(
            "MAX_OPEN_POSITIONS={} is invalid. Use a value between 1 and 20.",
            max_open_positions
        ));
    }

    if max_total_exposure_usdc < position_size {
        return Err(format!(
            "MAX_TOTAL_EXPOSURE_USDC ({:.2}) must be at least POSITION_SIZE ({:.2}).",
            max_total_exposure_usdc, position_size
        ));
    }

    if max_venue_exposure_usdc < position_size {
        return Err(format!(
            "MAX_VENUE_EXPOSURE_USDC ({:.2}) must be at least POSITION_SIZE ({:.2}).",
            max_venue_exposure_usdc, position_size
        ));
    }

    if !(0.0..1.0).contains(&min_entry_price) || !(0.0..1.0).contains(&max_entry_price) {
        return Err("MIN_ENTRY_PRICE and MAX_ENTRY_PRICE must stay between 0 and 1.".to_string());
    }

    if min_entry_price > 0.0 && min_entry_price >= max_entry_price {
        return Err(format!(
            "MIN_ENTRY_PRICE ({:.4}) must be lower than MAX_ENTRY_PRICE ({:.4}).",
            min_entry_price, max_entry_price
        ));
    }

    if !(0.0..1.0).contains(&hard_sl_price) || !(0.0..1.0).contains(&hard_sl_exit_floor) {
        return Err("HARD_SL_PRICE and HARD_SL_EXIT_FLOOR must stay between 0 and 1.".to_string());
    }

    if hard_sl_exit_floor > hard_sl_price {
        return Err(format!(
            "HARD_SL_EXIT_FLOOR ({:.4}) cannot be above HARD_SL_PRICE ({:.4}).",
            hard_sl_exit_floor, hard_sl_price
        ));
    }

    if !(dca_min_price <= dca_target_price && dca_target_price <= dca_start_price) {
        return Err(format!(
            "DCA prices must satisfy DCA_MIN_PRICE <= DCA_TARGET_PRICE <= DCA_START_PRICE, got {:.4} <= {:.4} <= {:.4}.",
            dca_min_price, dca_target_price, dca_start_price
        ));
    }

    if live_mode {
        let live_ack = env_string("LIVE_TRADING_ACK").ok_or_else(|| {
            "LIVE_TRADING_ACK=I_UNDERSTAND_REAL_MONEY is required when PAPER_TRADING=false."
                .to_string()
        })?;
        if live_ack != "I_UNDERSTAND_REAL_MONEY" {
            return Err(
                "LIVE_TRADING_ACK must exactly equal I_UNDERSTAND_REAL_MONEY for live trading."
                    .to_string(),
            );
        }

        for key in [
            "POLYMARKET_API_KEY",
            "POLYMARKET_API_SECRET",
            "POLYMARKET_API_PASSPHRASE",
            "POLYMARKET_PRIVATE_KEY",
        ] {
            let value = env_string(key)
                .ok_or_else(|| format!("{} is required when PAPER_TRADING=false.", key))?;

            if looks_like_placeholder(&value) {
                return Err(format!(
                    "{} still looks like placeholder text. Replace it before running live.",
                    key
                ));
            }
        }

        if let Some(signature_type) = env_string("POLYMARKET_SIGNATURE_TYPE") {
            let signature_type = signature_type
                .parse::<u8>()
                .map_err(|_| "POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2.".to_string())?;
            if signature_type > 2 {
                return Err("POLYMARKET_SIGNATURE_TYPE must be 0, 1, or 2.".to_string());
            }
            if signature_type > 0 && env_string("POLYMARKET_FUNDER").is_none() {
                return Err(
                    "POLYMARKET_FUNDER is required when POLYMARKET_SIGNATURE_TYPE is 1 or 2."
                        .to_string(),
                );
            }
        }

        if env_bool("ALLOW_LIVE_DCA", false)? {
            return Err(
                "ALLOW_LIVE_DCA=true is disabled by startup safety. Keep live DCA off until separately reviewed."
                    .to_string(),
            );
        }

        validate_live_cross_venue_hedge_settings(
            cross_venue_hedge_enabled(true)?,
            env_string("LIVE_CROSS_VENUE_HEDGE_ACK").as_deref(),
            env_f64("HEDGE_SIZE", position_size)?,
            position_size,
            max_total_exposure_usdc,
            max_venue_exposure_usdc,
            env_f64("MAX_HEDGE_PRICE", 0.45)?,
            env_f64("SL_LIQUIDITY_THRESHOLD", 0.23)?,
            env_f64("HEDGE_SL_GAP", 0.18)?,
        )?;

        for key in ["KALSHI_ACCESS_KEY"] {
            let value = env_string(key)
                .ok_or_else(|| format!("{} is required when PAPER_TRADING=false.", key))?;

            if looks_like_placeholder(&value) {
                return Err(format!(
                    "{} still looks like placeholder text. Replace it before running live.",
                    key
                ));
            }
        }

        if env_string("KALSHI_PRIVATE_KEY").is_none() {
            let private_key_path = env_string("KALSHI_PRIVATE_KEY_PATH").ok_or_else(|| {
                "KALSHI_PRIVATE_KEY or KALSHI_PRIVATE_KEY_PATH is required when PAPER_TRADING=false."
                    .to_string()
            })?;
            if !Path::new(&private_key_path).exists() {
                return Err(format!(
                    "KALSHI_PRIVATE_KEY_PATH points to '{}' but that file does not exist.",
                    private_key_path
                ));
            }
        }

        if env_string("TELEGRAM_BOT_TOKEN").is_none() || env_string("TELEGRAM_CHAT_ID").is_none() {
            return Err(
                "Telegram alerts are required in LIVE mode. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
                    .to_string(),
            );
        }
    } else if env_string("POLYMARKET_PRIVATE_KEY").is_none()
        && env_string("WALLET_PRIVATE_KEY").is_none()
    {
        warn!("Paper mode is active and no wallet key is loaded. That is fine for dry runs.");
    }

    if env_string("TELEGRAM_BOT_TOKEN").is_some() ^ env_string("TELEGRAM_CHAT_ID").is_some() {
        warn!(
            "Telegram is partially configured. Set both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID or neither."
        );
    }

    if max_slippage > 0.03 {
        warn!(
            "MAX_SLIPPAGE is {:.2}%. That is loose for a fast prediction market bot.",
            max_slippage * 100.0
        );
    }

    info!(
        "Startup config validated | mode={} | scan={}s | refresh={}ms | max_slippage={:.2}% | size=${:.2} | max_positions={} | max_exposure=${:.2}/${:.2} | entry_range={:.3}-{:.3} | hard_sl={:.3}/{:.3}",
        if live_mode { "LIVE" } else { "PAPER" },
        scan_interval_sec,
        refresh_rate_ms,
        max_slippage * 100.0,
        position_size,
        max_open_positions,
        max_total_exposure_usdc,
        max_venue_exposure_usdc,
        min_entry_price,
        max_entry_price,
        hard_sl_exit_floor,
        hard_sl_price
    );

    Ok(StartupConfig {
        live_mode,
        scan_interval_sec,
        refresh_rate_ms,
        max_slippage,
        position_size,
        max_open_positions,
        max_total_exposure_usdc,
        max_venue_exposure_usdc,
        min_entry_price,
        max_entry_price,
        hard_sl_price,
        hard_sl_exit_floor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_cross_venue_hedge_requires_explicit_ack() {
        let result = validate_live_cross_venue_hedge_settings(
            true,
            Some("WRONG"),
            3.0,
            3.0,
            10.0,
            10.0,
            0.45,
            0.23,
            0.18,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("LIVE_CROSS_VENUE_HEDGE_ACK"));
    }

    #[test]
    fn live_cross_venue_hedge_accepts_audited_small_recovery() {
        let result = validate_live_cross_venue_hedge_settings(
            true,
            Some(LIVE_CROSS_VENUE_HEDGE_ACK),
            3.0,
            3.0,
            10.0,
            10.0,
            0.45,
            0.23,
            0.18,
        );

        assert!(result.is_ok());
    }
}
