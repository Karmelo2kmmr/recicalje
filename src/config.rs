use log::{info, warn};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct StartupConfig {
    pub live_mode: bool,
    pub scan_interval_sec: u64,
    pub refresh_rate_ms: u64,
    pub max_slippage: f64,
    pub min_entry_price: f64,
    pub max_entry_price: f64,
    pub hard_sl_price: f64,
    pub hard_sl_exit_floor: f64,
}

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
    let min_entry_price = env_f64("MIN_ENTRY_PRICE", 0.81)?;
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

    if !(0.0..1.0).contains(&min_entry_price) || !(0.0..1.0).contains(&max_entry_price) {
        return Err("MIN_ENTRY_PRICE and MAX_ENTRY_PRICE must stay between 0 and 1.".to_string());
    }

    if min_entry_price >= max_entry_price {
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
        let executor_path =
            env_string("LIVE_EXECUTOR_PATH").unwrap_or_else(|| "clob_executor.py".to_string());
        if !Path::new(&executor_path).exists() {
            return Err(format!(
                "LIVE_EXECUTOR_PATH points to '{}' but that file does not exist.",
                executor_path
            ));
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
        "Startup config validated | mode={} | scan={}s | refresh={}ms | max_slippage={:.2}% | entry_range={:.3}-{:.3} | hard_sl={:.3}/{:.3}",
        if live_mode { "LIVE" } else { "PAPER" },
        scan_interval_sec,
        refresh_rate_ms,
        max_slippage * 100.0,
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
        min_entry_price,
        max_entry_price,
        hard_sl_price,
        hard_sl_exit_floor,
    })
}
