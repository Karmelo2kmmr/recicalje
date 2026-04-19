use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct TierSizes {
    pub tier_0: f64,
    pub tier_1: f64,
    pub tier_2: f64,
    pub tier_3: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProfitTiers {
    pub tier_1_trigger: f64,
    pub tier_2_trigger: f64,
    pub tier_3_trigger: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SizingConfig {
    pub capital_inicial: f64,
    pub capital_threshold_percent_mode: f64,
    pub fixed_sizes: HashMap<String, TierSizes>,
    pub profit_tiers: ProfitTiers,
    pub percent_mode: HashMap<String, f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SizingState {
    pub modo_porcentual_activado: bool,
    pub ultimo_capital_total: f64,
    pub ultimo_pnl_valido: f64,
}

impl Default for SizingState {
    fn default() -> Self {
        Self {
            modo_porcentual_activado: false,
            ultimo_capital_total: 0.0,
            ultimo_pnl_valido: 0.0,
        }
    }
}

pub struct SizingEngine {
    config: SizingConfig,
    state: SizingState,
    state_path: PathBuf,
}

impl SizingEngine {
    pub fn new() -> Self {
        // Load config
        let config_path = Path::new("sizing_config.yaml");
        let config: SizingConfig = if config_path.exists() {
            let content = fs::read_to_string(config_path).unwrap_or_default();
            serde_yaml::from_str(&content).unwrap_or_else(|e| {
                error!("Failed to parse sizing_config.yaml: {}", e);
                Self::default_config()
            })
        } else {
            Self::default_config()
        };

        // Load state
        let state_path = PathBuf::from("sizing_state.json");
        let state: SizingState = if state_path.exists() {
            let content = fs::read_to_string(&state_path).unwrap_or_default();
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            SizingState::default()
        };

        Self {
            config,
            state,
            state_path,
        }
    }

    fn default_config() -> SizingConfig {
        // Return a safe fallback if yaml is missing
        SizingConfig {
            capital_inicial: 300.0,
            capital_threshold_percent_mode: 2000.0,
            fixed_sizes: {
                let mut map = HashMap::new();
                map.insert(
                    "BTC".to_string(),
                    TierSizes {
                        tier_0: 5.0,
                        tier_1: 5.0,
                        tier_2: 5.0,
                        tier_3: 5.0,
                    },
                );
                map.insert(
                    "ETH".to_string(),
                    TierSizes {
                        tier_0: 5.0,
                        tier_1: 5.0,
                        tier_2: 5.0,
                        tier_3: 5.0,
                    },
                );
                map.insert(
                    "XRP".to_string(),
                    TierSizes {
                        tier_0: 5.0,
                        tier_1: 5.0,
                        tier_2: 5.0,
                        tier_3: 5.0,
                    },
                );
                map
            },
            profit_tiers: ProfitTiers {
                tier_1_trigger: 120.0,
                tier_2_trigger: 420.0,
                tier_3_trigger: 1500.0,
            },
            percent_mode: {
                let mut map = HashMap::new();
                map.insert("BTC".to_string(), 0.021);
                map.insert("ETH".to_string(), 0.021);
                map.insert("XRP".to_string(), 0.022);
                map
            },
        }
    }

    fn save_state(&self) {
        if let Ok(json) = serde_json::to_string_pretty(&self.state) {
            let _ = fs::write(&self.state_path, json);
        }
    }

    /// Calculates valid PnL and total capital from paper_trades.csv
    /// Ignores trades with entry size < $0.05 or net PnL > $20.0 (simulations/errors)
    pub fn calculate_valid_pnl() -> f64 {
        let is_live = std::env::var("PAPER_TRADING")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "false";
        let log_name = if is_live {
            "real_trades.csv"
        } else {
            "paper_trades.csv"
        };
        let path = Path::new(log_name);

        if !path.exists() {
            return 0.0;
        }

        let mut valid_pnl = 0.0;
        let file = match fs::File::open(path) {
            Ok(f) => f,
            Err(_) => return 0.0,
        };

        let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
        let mut records = Vec::new();
        for result in rdr.records() {
            if let Ok(rec) = result {
                records.push(rec);
            }
        }

        if records.is_empty() {
            return 0.0;
        }

        // We assume headers are standard and MarketID is index 1, EntryPrice is 4, Size is 8, ExitPrice is 7, Side is 6
        // Let's find indices properly:
        let headers = rdr.headers().cloned().unwrap_or_default();
        let size_idx = headers.iter().position(|h| h == "Size").unwrap_or(8);
        let entry_idx = headers.iter().position(|h| h == "EntryPrice").unwrap_or(4);
        let exit_idx = headers.iter().position(|h| h == "ExitPrice").unwrap_or(7);
        let side_idx = headers.iter().position(|h| h == "Side").unwrap_or(6);

        // Map to group DCA entries and handle exits
        let mut m_size = HashMap::new();
        let mut m_entry = HashMap::new();
        let mut m_side = HashMap::new();
        let mut m_exit = HashMap::new();

        for rec in records {
            let mid = rec.get(1).unwrap_or("").trim().to_string();
            let size = rec
                .get(size_idx)
                .unwrap_or("0.0")
                .parse::<f64>()
                .unwrap_or(0.0);
            let entry = rec
                .get(entry_idx)
                .unwrap_or("0.0")
                .parse::<f64>()
                .unwrap_or(0.0);
            let exit = rec
                .get(exit_idx)
                .unwrap_or("0.0")
                .parse::<f64>()
                .unwrap_or(0.0);
            let side = rec.get(side_idx).unwrap_or("").to_string();

            if side == "OPEN" {
                continue;
            }

            // Accumulate sizes (for DCA)
            *m_size.entry(mid.clone()).or_insert(0.0) += size;
            m_entry.insert(mid.clone(), entry); // Last entry price or avg
            m_side.insert(mid.clone(), side);

            if exit > 0.0 {
                m_exit.insert(mid.clone(), exit);
            }
        }

        for (mid, exit) in m_exit {
            if exit == 0.0 {
                continue;
            }
            let size = *m_size.get(&mid).unwrap_or(&0.0);
            let entry = *m_entry.get(&mid).unwrap_or(&0.0);
            let _side = m_side.get(&mid).unwrap_or(&"".to_string());

            if size < 0.05 {
                continue; // Ignore low size trades
            }

            if entry > 0.0 {
                // Calculate PnL roughly
                // Polymarket binary representation: size is number of shares basically, or standard formula
                // Real formula for binary options: Pnl = (exitVal - entryVal) * position_size / entryVal if we bought shares
                // Our sizing system: size is in DOLLARS. Shares = size / entry.
                // Profit = shares * exit - size
                let shares = size / entry;
                let trade_pnl = (shares * exit) - size;

                if trade_pnl > 20.0 {
                    continue; // Ignore outliers
                }

                valid_pnl += trade_pnl;
            }
        }

        valid_pnl
    }

    pub fn get_position_size(&mut self, symbol: &str) -> f64 {
        let pnl_valido = Self::calculate_valid_pnl();
        let capital_total = self.config.capital_inicial + pnl_valido;

        // Update state info
        self.state.ultimo_capital_total = capital_total;
        self.state.ultimo_pnl_valido = pnl_valido;

        // Check if we need to switch to percent mode permanently
        if !self.state.modo_porcentual_activado
            && capital_total >= self.config.capital_threshold_percent_mode
        {
            self.state.modo_porcentual_activado = true;
            info!("🌟 TRANSICIÓN MODO SIZING: Capital total alcanzó ${:.2}. Activando Modo Porcentual de forma permanente.", capital_total);
        }

        self.save_state();

        let final_size: f64;
        let mode_str: &str;
        let details_str: String;

        if self.state.modo_porcentual_activado {
            mode_str = "percent_equity";
            let default_pct = 0.021;
            let current_pct = self.config.percent_mode.get(symbol).unwrap_or(&default_pct);
            final_size = capital_total * current_pct;
            details_str = format!("pct={}", current_pct);
        } else {
            mode_str = "fixed_tier";
            let default_tiers = TierSizes {
                tier_0: 5.0,
                tier_1: 5.0,
                tier_2: 5.0,
                tier_3: 5.0,
            };
            let asset_tiers = self
                .config
                .fixed_sizes
                .get(symbol)
                .unwrap_or(&default_tiers);

            let (tier_name, size) = if pnl_valido < self.config.profit_tiers.tier_1_trigger {
                ("base", asset_tiers.tier_0)
            } else if pnl_valido < self.config.profit_tiers.tier_2_trigger {
                ("tier_12", asset_tiers.tier_1)
            } else if pnl_valido < self.config.profit_tiers.tier_3_trigger {
                ("tier_22", asset_tiers.tier_2)
            } else {
                ("tier_32", asset_tiers.tier_3)
            };
            final_size = size;
            details_str = format!("tier={}", tier_name);
        }

        let rounded_size = (final_size * 100.0).round() / 100.0; // Round to 2 decimals

        // Print MUST-HAVE required user log
        println!(
            "[SIZE] symbol={} capital_total={:.2} pnl={:.2} mode={} {} final_size={:.2}",
            symbol, capital_total, pnl_valido, mode_str, details_str, rounded_size
        );

        rounded_size
    }
}
