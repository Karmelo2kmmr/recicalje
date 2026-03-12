use std::fs;
use log::info;
use serde::{Serialize, Deserialize};
use chrono::Local;

const BASE_EQUITY: f64 = 100.0;
const DAILY_STATE_FILE: &str = "daily_state.json";
const MIN_STAKE: f64 = 1.0;
const MAX_STAKE: f64 = 150.0;
const MAX_DRAWDOWN_PCT: f64 = 0.47;

/// Porcentajes de balance para los 6 niveles de DCA
pub const DCA_STAKE_PERCENTAGES: [f64; 6] = [
    0.07,   // L1: 7%
    0.04,   // L2: 4%
    0.04,   // L3: 4%
    0.06,   // L4: 6%
    0.07,   // L5: 7%
    0.105,  // L6: 10.5%
];

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
        // Default if not found or error
        let today = Local::now().format("%Y-%m-%d").to_string();
        Self { date: today, start_capital: BASE_EQUITY }
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
        // New day detected
        let yesterday_profit = current_equity - state.start_capital;
        let retention = if yesterday_profit > 0.0 { yesterday_profit * 0.5 } else { 0.0 };
        
        // New start capital = old start + 50% of profit (or just current if loss)
        let new_start = state.start_capital + retention;
        
        info!(
            "📅 [EquityManager] New day! Date: {} | Prev Start: ${:.2} | Profit: ${:.2} | Retention: ${:.2} | New Start: ${:.2}",
            today, state.start_capital, yesterday_profit, retention, new_start
        );

        state.date = today;
        state.start_capital = new_start;
        state.save();
    }
}

/// Reads trades.csv, computes current equity from base $100 + sum of all PNLs.
pub fn compute_equity() -> f64 {
    let content = match fs::read_to_string("trades.csv") {
        Ok(c) => c,
        Err(_) => return BASE_EQUITY,
    };

    let mut total_pnl = 0.0;
    for line in content.lines().skip(1) {
        // PNL field: find token matching $+X.XX or $-X.XX
        if let Some(pnl) = extract_pnl(line) {
            total_pnl += pnl;
        }
    }

    BASE_EQUITY + total_pnl
}

/// Returns the number of consecutive losses for a given strategy.
/// Reads `trades.csv` backwards to find the streak.
pub fn get_consecutive_losses(strategy_name: &str) -> usize {
    let content = match fs::read_to_string("trades.csv") {
        Ok(c) => c,
        Err(_) => return 0,
    };

    let data_lines: Vec<&str> = content
        .lines()
        .skip(1) // Header
        .filter(|l| !l.trim().is_empty())
        .collect();

    let mut losses = 0;
    
    // Iteramos de abajo hacia arriba (más reciente a más antiguo)
    for line in data_lines.iter().rev() {
        // Necesitamos asegurar que esta línea pertenece a nuestra estrategia.
        // Formato aprox: TIME | COIN | SIDE | ENT | EXI | REZ | STATUS | PNL | RET | STRAT | ...
        // El index de STRAT es 9.
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() > 9 {
            let strat = parts[9].trim();
            if strat == strategy_name {
                let status = parts[6].trim();
                // Si encontramos un WIN o una PAUSA, la racha termina.
                if status.contains("CLOSED-WIN") || status.contains("SECURITY-PAUSE") {
                    break;
                }
                // Si encontramos un LOSS, aumentamos contador.
                if status.contains("CLOSED-LOSS") {
                    losses += 1;
                }
                // (Ignoramos PENDING u open states)
            }
        }
    }
    
    losses
}

/// Devuelve true si el drawdown actual supera el 12% del capital inicial del día.
pub fn is_kill_switch_active() -> bool {
    let state = DailyState::load();
    let current_equity = compute_equity();
    
    // Si el current_equity es menor al start, revisar %
    if current_equity < state.start_capital {
        let drawdown = (state.start_capital - current_equity) / state.start_capital;
        if drawdown >= MAX_DRAWDOWN_PCT {
            return true;
        }
    }
    false
}

/// Computes dynamic stake size based on strategy type as a percentage of current equity.
pub fn get_dynamic_stake(strategy_name: &str) -> f64 {
    let current_equity = compute_equity();
    let pct = match strategy_name {
        "Master Lobo" | "Alpha Lobo" => 0.17,  // 17% param Master
        "Micro"                       => 0.06,  // 6% param Micro
        "Fulas" | _                   => 0.13,  // 13% para Fulas
    };
    
    let calculated = current_equity * pct;
    calculated.max(MIN_STAKE).min(MAX_STAKE)
}

/// Calcula los montos exactos para cada nivel de DCA basándose en el capital actual.
pub fn calculate_dca_stakes(equity: f64) -> Vec<f64> {
    DCA_STAKE_PERCENTAGES.iter()
        .map(|&pct| (equity * pct).max(MIN_STAKE).min(MAX_STAKE))
        .collect()
}

/// Extracts the PNL value from a CSV row.
/// PNL format: $+12.34 or $-12.34
fn extract_pnl(line: &str) -> Option<f64> {
    let parts: Vec<&str> = line.split('|').collect();
    if parts.len() < 8 { return None; }
    
    let t = parts[7].trim(); // Index 7 is PNL
    if t.starts_with("$+") {
        if let Ok(v) = t[2..].trim().parse::<f64>() {
            return Some(v);
        }
    } else if t.starts_with("$-") {
        if let Ok(v) = t[2..].trim().parse::<f64>() {
            return Some(-v);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dynamic_stake() {
        // Just verify basic percentages apply (assuming 100 equity return without CSV)
        // Without an explicit mock, compute_equity() defaults to BASE_EQUITY (100.0) if no trades.csv
        assert_eq!(get_dynamic_stake("Master Lobo"), 7.0);
        assert_eq!(get_dynamic_stake("Micro"), 5.0); // 3.5% of 100 = 3.5, clamped to MIN_STAKE=5.0
        assert_eq!(get_dynamic_stake("Fulas"), 5.0); // 2% of 100 = 2.0, clamped to MIN_STAKE=5.0
    }
    
    #[test]
    fn test_kill_switch() {
        let today = Local::now().format("%Y-%m-%d").to_string();
        let test_state = DailyState { date: today, start_capital: 100.0 };
        test_state.save();
        
        // Since we can't easily fake the CSV read inside the test without side effects,
        // we test the basic structure. (In real tests, we would dependency inject the compute_equity func).
        // Since no trades.csv exists in test dir, compute_equity() returns 100.0.
        // Start 100, current 100 => drawdown 0% => no kill switch
        assert_eq!(is_kill_switch_active(), false);
        
        let _ = fs::remove_file(DAILY_STATE_FILE);
    }
}
