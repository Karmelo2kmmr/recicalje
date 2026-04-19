use chrono::{Duration, Local, NaiveDateTime};
use csv::ReaderBuilder;
use log::info;
use serde::Deserialize;
use std::error::Error;

const LOG_FILE: &str = "paper_trades.csv";

#[allow(non_snake_case)]
#[derive(Debug, serde::Serialize, Deserialize, Clone)]
pub struct TradeRecord {
    pub Timestamp: String,
    pub MarketID: String,
    pub Question: String,
    pub Coin: String,
    pub EntryPrice: f64,
    #[serde(rename = "Type")]
    pub TradeType: String, // "PAPER_BUY" or "PAPER_SELL"
    pub Side: String, // "UP" or "DOWN"
    pub ExitPrice: Option<f64>,
    pub Size: f64,
    pub EntryType: String, // Strat: "TriggerDirect", "Dip", etc.
    // Telemetry
    pub SL_Price: String,
    pub TP_Price: String,
    pub R_Ratio: String,
    pub Vol_Now: String,
    pub Vol_MA20: String,
    pub Vol_State: String,
    pub Trigger_Price: String,
    pub setup_tag: Option<String>,
    pub entry_bucket: Option<String>,
    pub signal_score: Option<String>,
    pub reason_entry: Option<String>,
    pub reason_exit: Option<String>,
    pub holding_seconds: Option<String>,
    pub max_favor: Option<String>,
    pub max_adverse: Option<String>,
    pub market_regime: Option<String>,
    // Advanced Exits
    pub ExitIntent: Option<String>,
    pub ExitReason: Option<String>,
    pub ExitConfirmed: Option<String>, // "1" if fill confirmed
    pub ExitOrderId: Option<String>,
    pub ExitFilledShares: Option<String>,
    pub ExitAvgFillPrice: Option<f64>,
    pub ExitTimestamp: Option<String>,
}

pub struct StatsEngine {
    initial_balance: f64,
}

impl StatsEngine {
    pub fn new() -> Self {
        // User requested $100 default
        let initial_balance = std::env::var("PAPER_BALANCE_INITIAL")
            .unwrap_or("100.0".to_string())
            .parse()
            .unwrap_or(100.0);

        Self { initial_balance }
    }

    pub fn record_entry_to_csv(&self, record: &TradeRecord) -> Result<(), Box<dyn Error>> {
        let log_file = crate::stats_reporter::get_log_path();
        let file_exists = log_file.exists();

        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file)?;

        let mut wtr = csv::WriterBuilder::new()
            .has_headers(!file_exists)
            .from_writer(file);

        wtr.serialize(record)?;
        wtr.flush()?;
        Ok(())
    }

    pub async fn generate_daily_report(&self, client: &reqwest::Client) -> String {
        let now = Local::now();
        let start_of_day = now - Duration::hours(12); // "Day" in this context is the last 12h

        // 1. Accumulate trades in a HashMap to handle DCA grouping (v1.3 Fix)
        let mut consolidated_trades: std::collections::HashMap<String, Vec<TradeRecord>> =
            std::collections::HashMap::new();

        let log_file = crate::stats_reporter::get_log_path();

        if let Ok(mut rdr) = ReaderBuilder::new().flexible(true).from_path(&log_file) {
            for result in rdr.deserialize::<TradeRecord>() {
                match result {
                    Ok(record) => {
                        // Parse timestamp
                        if let Ok(ts) =
                            NaiveDateTime::parse_from_str(&record.Timestamp, "%Y-%m-%d %H:%M:%S")
                        {
                            if ts >= start_of_day.naive_local() {
                                consolidated_trades
                                    .entry(record.MarketID.clone())
                                    .or_insert_with(Vec::new)
                                    .push(record);
                            }
                        }
                    }
                    Err(e) => log::debug!("Skipping malformed record in stats engine: {}", e),
                }
            }
        }

        let mut winners = 0;
        let mut losers = 0;
        let mut pending = 0;
        let mut cumulative_pnl = 0.0;
        let mut total_bet = 0.0;
        let mut entry_types = std::collections::HashMap::new();
        let total_markets = consolidated_trades.len();

        for (mid, legs) in consolidated_trades {
            let mut total_size = 0.0;
            let mut total_shares = 0.0;
            let mut final_exit = None;
            let last_leg = legs.last().unwrap(); // Safe due to grouping

            // Record entry type of the first leg for distribution
            if let Some(first) = legs.first() {
                *entry_types.entry(first.EntryType.clone()).or_insert(0) += 1;
            }

            for leg in &legs {
                let size = leg.Size;
                total_size += size;
                total_bet += size;
                if leg.EntryPrice > 0.0 {
                    total_shares += size / leg.EntryPrice;
                }

                // FASE 2: Priorizar ExitAvgFillPrice (confirmado por orderbook) sobre ExitPrice manual
                let confirmed = leg.ExitConfirmed.as_deref().unwrap_or("LEGACY");
                if confirmed == "1" || confirmed == "LEGACY" {
                    if let Some(avg_fill) = leg.ExitAvgFillPrice {
                        final_exit = Some(avg_fill);
                    } else if let Some(exit) = leg.ExitPrice {
                        if exit > 0.0 {
                            final_exit = Some(exit);
                        }
                    }
                }
            }

            // If no exit price in CSV, try checking the API for the last leg
            let exit_price = if let Some(e) = final_exit {
                Some(e)
            } else {
                match self.check_resolution(client, &mid, &last_leg.Side).await {
                    Ok((Resolution::Win, _)) => {
                        // Dejar de actualizar automáticamente el CSV; ahora dependemos de fills confirmados
                        Some(1.0)
                    }
                    Ok((Resolution::Loss, _)) => {
                        let sl = last_leg.SL_Price.parse::<f64>().unwrap_or(0.0);
                        Some(if sl > 0.0 { sl } else { 0.0 })
                    }
                    _ => None,
                }
            };

            if let Some(exit) = exit_price {
                let payout = total_shares * exit;
                let profit = payout - total_size;
                cumulative_pnl += profit;

                if profit > 0.01 {
                    winners += 1;
                } else if profit < -0.01 {
                    losers += 1;
                }
            } else {
                pending += 1;
            }
        }

        let current_balance = self.initial_balance + cumulative_pnl;
        let entries_total = winners + losers;
        let win_rate = if entries_total > 0 {
            (winners as f64 / entries_total as f64) * 100.0
        } else {
            0.0
        };

        // Format entry type distribution
        let mut entry_dist = String::new();
        for (etype, count) in entry_types {
            entry_dist.push_str(&format!("\n• {}: {}", etype, count));
        }

        // 3. Format Message
        let report = format!(
            "📊 *Reporte de Trading (12h)*\n\n\
            📅 Periodo: Last 12 Hours\n\
            🔢 Mercados Operados: {}\n\
            ✅ Ganadores: {} ({:.1}%)\n\
            ❌ Perdedores: {}\n\
            ⏳ Pendientes: {}\n\n\
            💰 Balance Actual: ${:.2}\n\
            📈 PnL Acumulado: ${:.2}\n\
            💸 Volumen Apostado: ${:.2}\n\
            (Balance Inicial: ${:.2})\n\n\
            🏗️ *Distribución por Tipo:*{}\n\n\
            🎯 _Bot Alpha Lobo v1.3 - GCP_",
            total_markets,
            winners,
            win_rate,
            losers,
            pending,
            current_balance,
            cumulative_pnl,
            total_bet,
            self.initial_balance,
            entry_dist
        );

        report
    }

    /// Calculate position size based on Kelly Criterion (LOBO)
    /// Formula: Size = Capital * ((WinProb * Recompensa - LossProb) / Recompensa)
    pub fn calculate_kelly_size(&self) -> f64 {
        // v1.3 Fix: Enable/Disable Kelly from .env
        let kelly_enabled = std::env::var("KELLY_POSITION_SIZING")
            .unwrap_or("true".to_string())
            .parse::<bool>()
            .unwrap_or(true);

        let fixed_size = std::env::var("FIXED_SIZE")
            .unwrap_or("10.0".to_string())
            .parse::<f64>()
            .unwrap_or(10.0);

        if !kelly_enabled {
            info!("🎲 KELLY DISABLED | Using Fixed Size: ${:.2}", fixed_size);
            return fixed_size;
        }

        let mut trades = Vec::new();

        // 1. Read all trades into a vector, then reverse
        if let Ok(mut rdr) = ReaderBuilder::new().flexible(true).from_path(LOG_FILE) {
            let mut all_records = Vec::new();
            for result in rdr.deserialize() {
                if let Ok(record) = result {
                    all_records.push(record);
                }
            }

            for record in all_records.into_iter().rev() {
                let record: TradeRecord = record;
                if record.ExitPrice.is_some() {
                    trades.push(record);
                    if trades.len() >= 10 {
                        break;
                    }
                }
            }
        }

        if trades.is_empty() {
            return fixed_size; // Default to fixed if no history
        }

        let mut _winners = 0;
        let mut total_profit_pct = 0.0;
        let mut total_loss_pct = 0.0;
        let mut win_count = 0;
        let mut loss_count = 0;

        for t in &trades {
            if let Some(exit) = t.ExitPrice {
                let profit_pct = (exit - t.EntryPrice) / t.EntryPrice;
                if profit_pct > 0.0 {
                    _winners += 1;
                    total_profit_pct += profit_pct;
                    win_count += 1;
                } else {
                    total_loss_pct += profit_pct.abs();
                    loss_count += 1;
                }
            }
        }

        let win_rate = if !trades.is_empty() {
            win_count as f64 / (win_count + loss_count) as f64
        } else {
            0.0
        };

        // If Win Rate < 70%, stick to safe fixed size
        if win_rate < 0.70 {
            return fixed_size;
        }

        // Calculate average reward and average loss
        let avg_reward = if win_count > 0 {
            total_profit_pct / win_count as f64
        } else {
            0.10
        }; // Default 10%
        let avg_risk = if loss_count > 0 {
            total_loss_pct / loss_count as f64
        } else {
            0.15
        }; // Default 15%

        // Recompensa (b) = Reward / Risk
        let b = avg_reward / avg_risk;
        let p = win_rate;
        let q = 1.0 - p;

        // Kelly Fraction: f* = (p*b - q) / b
        let kelly_f = (p * b - q) / b;

        // Safety: Limit Kelly to max percentage (User requested 2.5% = 0.025)
        let current_balance = self.initial_balance;

        let max_kelly_pct = std::env::var("MAX_KELLY_FRACTION")
            .unwrap_or("0.025".to_string()) // Default changed to 2.5% as requested
            .parse::<f64>()
            .unwrap_or(0.025);

        let max_absolute_size = std::env::var("MAX_ABSOLUTE_SIZE")
            .unwrap_or("25.0".to_string()) // Set more conservative default
            .parse::<f64>()
            .unwrap_or(25.0);

        let mut suggested_size = current_balance * kelly_f.max(0.0).min(max_kelly_pct);

        // Cap by absolute max
        if suggested_size > max_absolute_size {
            suggested_size = max_absolute_size;
        }

        if suggested_size < fixed_size {
            suggested_size = fixed_size;
        }

        info!(
            "🎲 KELLY CALC | WinRate: {:.1}% | b: {:.2} | Fraction: {:.2} | Suggested Size: ${:.2}",
            win_rate * 100.0,
            b,
            kelly_f,
            suggested_size
        );

        suggested_size
    }

    pub fn count_trades_today(&self) -> Result<usize, Box<dyn Error>> {
        let now = Local::now();
        let start_of_day = now - Duration::hours(12);

        let mut count = 0;

        if let Ok(mut rdr) = ReaderBuilder::new().flexible(true).from_path(LOG_FILE) {
            for result in rdr.deserialize() {
                if let Ok(record) = result {
                    let record: TradeRecord = record;
                    if let Ok(ts) =
                        NaiveDateTime::parse_from_str(&record.Timestamp, "%Y-%m-%d %H:%M:%S")
                    {
                        if ts >= start_of_day.naive_local() {
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    async fn check_resolution(
        &self,
        client: &reqwest::Client,
        market_id: &str,
        trade_side: &str,
    ) -> Result<(Resolution, f64), Box<dyn Error>> {
        let url = format!("https://gamma-api.polymarket.com/markets/{}", market_id);
        let resp = client.get(&url).send().await?;

        if resp.status().is_success() {
            let market: crate::api::Market = resp.json().await?;

            let is_closed = market.closed.unwrap_or(false);

            if is_closed {
                if let Some(prices_str) = market.outcome_prices {
                    if let Ok(prices) = serde_json::from_str::<Vec<String>>(&prices_str) {
                        let yes_price = prices
                            .get(0)
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let no_price = prices
                            .get(1)
                            .and_then(|p| p.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        let winner_side = if yes_price > 0.99 {
                            "UP"
                        } else if no_price > 0.99 {
                            "DOWN"
                        } else {
                            "UNKNOWN"
                        };

                        if winner_side == "UNKNOWN" {
                            return Ok((Resolution::Open, 0.0));
                        }

                        if winner_side == trade_side {
                            return Ok((Resolution::Win, 1.0));
                        } else {
                            return Ok((Resolution::Loss, 0.0));
                        }
                    }
                }
                return Ok((Resolution::Open, 0.0));
            }
            return Ok((Resolution::Open, 0.0));
        }
        Err("API Error".into())
    }

    pub fn update_csv_exit_price(
        &self,
        market_id: &str,
        exit_price: f64,
        exit_intent: &str,
        is_confirmed: bool,
        avg_fill: Option<f64>,
    ) -> Result<(), Box<dyn Error>> {
        let log_file = crate::stats_reporter::get_log_path();
        if !log_file.exists() {
            return Ok(());
        }

        let file = std::fs::File::open(&log_file)?;
        let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(file);
        let mut records: Vec<csv::StringRecord> = Vec::new();
        let headers = rdr.headers()?.clone();

        // Find Column Indices
        let mid_idx = headers.iter().position(|h| h == "MarketID").unwrap_or(1);
        let exit_idx = headers.iter().position(|h| h == "ExitPrice").unwrap_or(7);
        let intent_idx = headers.iter().position(|h| h == "ExitIntent").unwrap_or(26);
        let confirmed_idx = headers
            .iter()
            .position(|h| h == "ExitConfirmed")
            .unwrap_or(28);
        let fill_idx = headers
            .iter()
            .position(|h| h == "ExitAvgFillPrice")
            .unwrap_or(31);
        let ts_idx = headers
            .iter()
            .position(|h| h == "ExitTimestamp")
            .unwrap_or(32);

        for result in rdr.records() {
            records.push(result?);
        }

        let mut updated = false;
        let now_str = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for record in records.iter_mut().rev() {
            if record.get(mid_idx) == Some(market_id) {
                let current_exit = record.get(exit_idx).unwrap_or("0.0");
                if current_exit == "0.0" || current_exit == "0" || current_exit == "" {
                    let mut new_rec = csv::StringRecord::new();
                    for (i, field) in record.iter().enumerate() {
                        if i == exit_idx {
                            new_rec.push_field(&format!("{:.4}", exit_price));
                        } else if i == intent_idx {
                            new_rec.push_field(exit_intent);
                        } else if i == confirmed_idx {
                            new_rec.push_field(if is_confirmed { "1" } else { "0" });
                        } else if i == fill_idx {
                            new_rec.push_field(&format!("{:.4}", avg_fill.unwrap_or(exit_price)));
                        } else if i == ts_idx {
                            new_rec.push_field(&now_str);
                        } else {
                            new_rec.push_field(field);
                        }
                    }

                    // If row was short, append missing fields
                    while new_rec.len() < headers.len() {
                        new_rec.push_field("");
                    }

                    *record = new_rec;
                    updated = true;
                    break;
                }
            }
        }

        if updated {
            let mut wtr = csv::WriterBuilder::new()
                .flexible(true)
                .from_path(&log_file)?;
            wtr.write_record(&headers)?;
            for record in records {
                wtr.write_record(&record)?;
            }
            wtr.flush()?;
        }
        Ok(())
    }
}

#[allow(dead_code)]
enum Resolution {
    Win,
    Loss,
    Open,
}
