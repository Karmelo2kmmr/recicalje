use std::fs;

use chrono::Local;
use log::info;
use serde::{Deserialize, Serialize};

const BASE_EQUITY: f64 = 100.0;
const DAILY_STATE_FILE: &str = "daily_state.json";
const FIXED_ALPHA_STAKE: f64 = 10.0;
const MAX_DRAWDOWN_PCT: f64 = 0.47;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DailyState {
    pub date: String,
    pub start_capital: f64,
}

impl DailyState {
    pub fn load() -> Self {
        if let Ok(content) = fs::read_to_string(DAILY_STATE_FILE) {
            if let Ok(state) = serde_json::from_str(&content) {
                return state;
            }
        }

        let today = Local::now().format("%Y-%m-%d").to_string();
        Self {
            date: today,
            start_capital: BASE_EQUITY,
        }
    }

    pub fn save(&self) {
        if let Ok(content) = serde_json::to_string_pretty(self) {
            let _ = fs::write(DAILY_STATE_FILE, content);
        }
    }
}

pub fn initialize_daily_capital() {
    let mut state = DailyState::load();
    let today = Local::now().format("%Y-%m-%d").to_string();
    let current_equity = compute_equity();

    if state.date != today {
        let yesterday_profit = current_equity - state.start_capital;
        let retention = if yesterday_profit > 0.0 {
            yesterday_profit * 0.5
        } else {
            0.0
        };
        let new_start = state.start_capital + retention;

        info!(
            "[EquityManager] New day {} | prev start ${:.2} | profit ${:.2} | retention ${:.2} | new start ${:.2}",
            today, state.start_capital, yesterday_profit, retention, new_start
        );

        state.date = today;
        state.start_capital = new_start;
        state.save();
    }
}

pub fn compute_equity() -> f64 {
    let content = match fs::read_to_string("trades.csv") {
        Ok(c) => c,
        Err(_) => return BASE_EQUITY,
    };

    let mut total_pnl = 0.0;
    for line in content.lines().skip(1) {
        if let Some(pnl) = extract_pnl(line) {
            total_pnl += pnl;
        }
    }

    BASE_EQUITY + total_pnl
}

pub fn is_kill_switch_active() -> bool {
    let state = DailyState::load();
    let current_equity = compute_equity();

    if current_equity < state.start_capital {
        return is_drawdown_breached(state.start_capital, current_equity);
    }
    false
}

pub fn calculate_alpha_momentum_stake(equity: f64) -> f64 {
    if equity <= 0.0 {
        0.0
    } else {
        FIXED_ALPHA_STAKE.min(equity)
    }
}

pub fn kill_switch_drawdown_pct() -> f64 {
    MAX_DRAWDOWN_PCT
}

fn is_drawdown_breached(start_capital: f64, current_equity: f64) -> bool {
    if start_capital <= 0.0 {
        return false;
    }
    let drawdown = (start_capital - current_equity) / start_capital;
    drawdown >= MAX_DRAWDOWN_PCT
}

fn extract_pnl(line: &str) -> Option<f64> {
    let parts: Vec<&str> = line.split('|').collect();
    if parts.len() < 8 {
        return None;
    }

    let pnl_field = parts[7].trim();
    if pnl_field.starts_with("$+") {
        return pnl_field[2..].trim().parse::<f64>().ok();
    }
    if pnl_field.starts_with("$-") {
        return pnl_field[2..].trim().parse::<f64>().ok().map(|v| -v);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpha_momentum_stake() {
        assert_eq!(calculate_alpha_momentum_stake(100.0), 10.0);
        assert_eq!(calculate_alpha_momentum_stake(10.0), 10.0);
        assert_eq!(calculate_alpha_momentum_stake(7.5), 7.5);
    }

    #[test]
    fn test_drawdown_breach() {
        assert!(!is_drawdown_breached(100.0, 90.0));
        assert!(is_drawdown_breached(100.0, 53.0));
    }
}
