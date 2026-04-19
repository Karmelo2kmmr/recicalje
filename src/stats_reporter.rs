use crate::audit::{AuditReport, TradeAuditor, TradeRecord};
use chrono::{DateTime, Duration, FixedOffset, Local, TimeZone, Timelike};
use csv::ReaderBuilder;
use log::info;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;

pub fn get_log_path() -> std::path::PathBuf {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop(); // Remove binary name
    let is_live = std::env::var("PAPER_TRADING")
        .unwrap_or_else(|_| "true".to_string())
        .to_lowercase()
        == "false";
    if is_live {
        path.push("real_trades.csv");
    } else {
        path.push("paper_trades.csv");
    }
    path
}

/// Statistics for a specific asset
#[derive(Debug, Clone, Default)]
pub struct AssetStats {
    pub coin: String,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub net_pnl: f64,
}

/// Statistics for a 6-hour period
#[derive(Debug, Clone)]
pub struct PeriodStats {
    pub period_start: String, // Formatted time string
    pub period_end: String,   // Formatted time string
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub net_pnl: f64,
    pub best_trade_pct: f64,
    pub worst_trade_pct: f64,
    pub smart_delay_trades: usize,
    pub kill_zone_trades: usize,
    pub rapid_action_trades: usize,
    pub smart_delay_wins: usize,
    pub kill_zone_wins: usize,
    pub rapid_action_wins: usize,
    pub sl_650_triggers: usize,
    pub sl_83_triggers: usize,
    pub dca_executions: usize,
    pub full_recovery_trades: usize,
    pub full_recovery_wins: usize,
    pub markets_blocked: usize,
    pub asset_breakdown: Vec<AssetStats>, // NEW: Per-asset metrics
    pub pending_trades: usize,            // NEW: Track trades without exit price
    pub notice: Option<String>,           // NEW: For communicating issues (like missing CSV)
}

#[derive(Debug, Clone)]
pub struct DailyStats {
    pub date: String, // Formatted date string
    pub periods: Vec<PeriodStats>,
    pub total_trades: usize,
    pub total_markets_analyzed: usize,
    pub win_rate: f64,
    pub net_pnl: f64,
    pub best_trade_pct: f64,
    pub worst_trade_pct: f64,
    pub avg_win_pct: f64,
    pub avg_loss_pct: f64,
    pub smart_delay_win_rate: f64,
    pub kill_zone_win_rate: f64,
    pub rapid_action_win_rate: f64,
    pub full_recovery_win_rate: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub asset_breakdown: Vec<AssetStats>, // NEW: Aggregate asset breakdown
    pub pending_trades: usize,            // NEW: Aggregate pending trades
    pub notice: Option<String>,           // NEW: For communicating issues
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
struct CsvTradeRecord {
    Timestamp: String,
    MarketID: String,
    Question: Option<String>,
    Coin: Option<String>,
    EntryPrice: f64,
    #[serde(rename = "Type")]
    TradeType: Option<String>, // "PAPER_BUY" — always present, needed to align columns
    Side: String,
    ExitPrice: Option<f64>,
    Size: f64,
    EntryType: String,
    // Extended telemetry columns — must be present in struct to avoid deserialization failures
    SL_Price: Option<String>,
    TP_Price: Option<String>,
    R_Ratio: Option<String>,
    Vol_Now: Option<String>,
    Vol_MA20: Option<String>,
    Vol_State: Option<String>,
    Trigger_Price: Option<String>,
    setup_tag: Option<String>,
    entry_bucket: Option<String>,
    signal_score: Option<String>,
    reason_entry: Option<String>,
    reason_exit: Option<String>,
    holding_seconds: Option<String>,
    max_favor: Option<String>,
    max_adverse: Option<String>,
    market_regime: Option<String>,
    // FASE 2: Nuevas columnas de salida confirmada (Operative Truth)
    ExitIntent: Option<String>,
    ExitReason: Option<String>,
    ExitConfirmed: Option<String>,
    ExitOrderId: Option<String>,
    ExitFilledShares: Option<String>,
    ExitAvgFillPrice: Option<f64>,
    ExitTimestamp: Option<String>,
}

pub struct StatsReporter {
    initial_balance: f64,
}

impl StatsReporter {
    pub fn new() -> Self {
        let is_live = std::env::var("PAPER_TRADING")
            .unwrap_or_else(|_| "true".to_string())
            .to_lowercase()
            == "false";
        let default_balance = if is_live { "40.0" } else { "100.0" };

        let initial_balance = std::env::var("PAPER_BALANCE_INITIAL")
            .unwrap_or(default_balance.to_string())
            .parse()
            .unwrap_or(100.0);

        Self { initial_balance }
    }

    /// Calculate position size based on Kelly Criterion (LOBO)
    pub fn calculate_kelly_size(&self) -> f64 {
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

        let log_path = get_log_path();
        if let Ok(mut rdr) = ReaderBuilder::new().flexible(true).from_path(&log_path) {
            let mut all_records = Vec::new();
            for result in rdr.deserialize() {
                if let Ok(record) = result {
                    all_records.push(record);
                }
            }

            for record in all_records.into_iter().rev() {
                let record: CsvTradeRecord = record;
                if let Some(exit) = record.ExitPrice {
                    if exit > 0.0 {
                        // FASE 1: Solo usar trades con ExitConfirmed=1 para Kelly.
                        // Si ExitConfirmed no existe (CSV legacy) = LEGACY, se acepta como dato histórico válido.
                        let confirmed = record.ExitConfirmed.as_deref().unwrap_or("LEGACY");
                        if confirmed == "1" || confirmed == "LEGACY" {
                            trades.push(record);
                            if trades.len() >= 10 {
                                break;
                            }
                        }
                    }
                }
            }
        }

        if trades.is_empty() {
            return fixed_size;
        }

        let mut win_count = 0;
        let mut loss_count = 0;
        let mut total_profit_pct = 0.0;
        let mut total_loss_pct = 0.0;

        for t in &trades {
            if let Some(exit) = t.ExitPrice {
                if exit <= 0.0 {
                    continue;
                } // Resilience check

                // PnL calculation is the same for both sides because EntryPrice/ExitPrice
                // always refer to the price of the token actually held (YES or NO).
                let profit_pct = (exit - t.EntryPrice) / t.EntryPrice;

                if profit_pct > 0.0 {
                    win_count += 1;
                    total_profit_pct += profit_pct;
                } else {
                    loss_count += 1;
                    total_loss_pct += profit_pct.abs();
                }
            }
        }

        let total_count = win_count + loss_count;
        let win_rate = if total_count > 0 {
            win_count as f64 / total_count as f64
        } else {
            0.0
        };

        if win_rate < 0.70 {
            return fixed_size;
        }

        let avg_reward = if win_count > 0 {
            total_profit_pct / win_count as f64
        } else {
            0.10
        };
        let avg_risk = if loss_count > 0 {
            total_loss_pct / loss_count as f64
        } else {
            0.15
        };

        let b = avg_reward / avg_risk;
        let p = win_rate;
        let q = 1.0 - p;

        let kelly_f = (p * b - q) / b;

        let max_kelly_pct = std::env::var("MAX_KELLY_FRACTION")
            .unwrap_or("0.025".to_string())
            .parse::<f64>()
            .unwrap_or(0.025);

        let max_absolute_size = std::env::var("MAX_ABSOLUTE_SIZE")
            .unwrap_or("25.0".to_string())
            .parse::<f64>()
            .unwrap_or(25.0);

        let mut suggested_size = self.initial_balance * kelly_f.max(0.0).min(max_kelly_pct);

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

    /// Get the start of a 6-hour period relative to now in Eastern Time
    fn get_period_start(now_local: DateTime<Local>, offset_periods: i32) -> DateTime<FixedOffset> {
        // Eastern Time is currently UTC-4 (EDT)
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let et_now = now_local.with_timezone(&et_offset);

        let hour = et_now.hour();
        let period_hour = (hour / 6) * 6;

        let base_period_et = et_now
            .date_naive()
            .and_hms_opt(period_hour, 0, 0)
            .and_then(|naive| et_offset.from_local_datetime(&naive).single())
            .unwrap();

        if offset_periods == 0 {
            base_period_et
        } else {
            base_period_et + Duration::hours(offset_periods as i64 * 6)
        }
    }

    /// Load trades from CSV for a specific time period (synchronous - call from spawn_blocking)

    /// Load trades from CSV for a specific time period (synchronous - call from spawn_blocking)
    fn load_trades_for_period_sync(start_ts: i64, end_ts: i64) -> Result<Vec<TradeRecord>, String> {
        let mut trades = Vec::new();

        let log_path = get_log_path();
        if !log_path.exists() {
            return Ok(trades);
        }

        // Use flexible(true) to handle rows with different column counts (from older versions)
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_path(&log_path)
            .map_err(|e| e.to_string())?;

        for result in rdr.deserialize() {
            let record: CsvTradeRecord = match result {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("⚠️ skipping malformed CSV row: {}", e);
                    continue;
                }
            };

            // Parse timestamp - Handle both Space and T separators
            let timestamp_str = record.Timestamp.replace('T', " ");
            let timestamp =
                chrono::NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .and_then(|dt| Local.from_local_datetime(&dt).single())
                    .unwrap_or_else(|| Local::now());

            let ts = timestamp.timestamp();

            // Filter by time period
            if ts >= start_ts && ts < end_ts {
                // Treat 0.0 or 0 as None (Open Trade) for audit to recover
                let exit_price = match record.ExitPrice {
                    Some(p) if p > 0.0 => Some(p),
                    _ => None,
                };

                trades.push(TradeRecord {
                    market_id: record.MarketID,
                    coin: record.Coin.unwrap_or_else(|| "BTC".to_string()),
                    side: record.Side,
                    entry_price: record.EntryPrice,
                    exit_price: exit_price,
                    size: record.Size,
                    timestamp: ts,
                    entry_type: record.EntryType,
                    exit_confirmed: record.ExitConfirmed,
                    exit_avg_fill_price: record.ExitAvgFillPrice,
                });
            }
        }

        Ok(trades)
    }

    /// Calculate statistics for a period
    pub fn calculate_period_stats(
        &self,
        trades: &[TradeRecord],
        start: DateTime<FixedOffset>,
        end: DateTime<FixedOffset>,
    ) -> PeriodStats {
        let mut winning = 0;
        let mut losing = 0;
        let mut net_pnl = 0.0;
        let mut best_pct = f64::MIN;
        let mut worst_pct = f64::MAX;
        let mut smart_delay = 0;
        let mut kill_zone = 0;
        let mut rapid_action = 0;
        let mut smart_delay_wins = 0;
        let mut kill_zone_wins = 0;
        let mut rapid_action_wins = 0;
        let mut sl_650 = 0;
        let mut sl_83 = 0;
        let mut dca = 0;
        let mut full_recovery = 0;
        let mut full_recovery_wins = 0;
        let mut pending_trades = 0;

        let mut asset_map: HashMap<String, AssetStats> = HashMap::new();

        for trade in trades {
            // Track per-asset stats
            let coin = trade.coin.clone();
            let asset_stat = asset_map.entry(coin.clone()).or_insert(AssetStats {
                coin,
                ..Default::default()
            });

            // Categorize by entry type regardless of exit status
            if let Some(exit) = trade.exit_price {
                // Categorize resolved trades by strategy for accurate Win Rate
                match trade.entry_type.as_str() {
                    "Dip" | "DipRecovery" => {
                        smart_delay += 1;
                        dca += 1;
                    }
                    "FullRecovery" => {
                        full_recovery += 1;
                    }
                    _ => {
                        // All others (TriggerDirect, Triggered, Reentry) categorized by time
                        if let Some(dt) = DateTime::from_timestamp(trade.timestamp, 0) {
                            let local_dt: DateTime<Local> = dt.into();
                            let et_dt = crate::api::to_eastern_time(local_dt);
                            let elapsed_mins = (et_dt.hour() * 60 + et_dt.minute()) as i32;

                            if elapsed_mins >= 580 && elapsed_mins < 700 {
                                // Smart Delay (9:40 - 11:40)
                                smart_delay += 1;
                            } else if elapsed_mins >= 700 && elapsed_mins < 780 {
                                // Kill Zone (11:40 - 13:00)
                                kill_zone += 1;
                            } else if elapsed_mins >= 780 && elapsed_mins <= 875 {
                                // Rapid Action (13:00 - 14:35)
                                rapid_action += 1;
                            }
                        }
                    }
                }

                // FIXED: Direction-aware PnL calculation
                // UP  → profit when exit > entry (bought YES shares)
                // DOWN → profit when exit < entry (bought NO shares, which appreciate as YES falls)
                let pnl_pct = ((exit - trade.entry_price) / trade.entry_price) * 100.0;

                let pnl_dollars = (pnl_pct / 100.0) * trade.size;
                net_pnl += pnl_dollars;

                // Update asset PnL
                asset_stat.total_trades += 1;
                asset_stat.net_pnl += pnl_dollars;

                if pnl_pct > 0.0 {
                    winning += 1;
                    asset_stat.winning_trades += 1;

                    // Track wins for strategy
                    match trade.entry_type.as_str() {
                        "Dip" | "DipRecovery" => {
                            smart_delay_wins += 1;
                        }
                        "FullRecovery" => {
                            full_recovery_wins += 1;
                        }
                        _ => {
                            if let Some(dt) = DateTime::from_timestamp(trade.timestamp, 0) {
                                let local_dt: DateTime<Local> = dt.into();
                                let et_dt = crate::api::to_eastern_time(local_dt);
                                let elapsed_mins = (et_dt.hour() * 60 + et_dt.minute()) as i32;

                                if elapsed_mins >= 580 && elapsed_mins < 700 {
                                    smart_delay_wins += 1;
                                } else if elapsed_mins >= 700 && elapsed_mins < 780 {
                                    kill_zone_wins += 1;
                                } else if elapsed_mins >= 780 && elapsed_mins <= 875 {
                                    rapid_action_wins += 1;
                                }
                            }
                        }
                    }
                } else {
                    losing += 1;
                }

                if pnl_pct > best_pct {
                    best_pct = pnl_pct;
                }
                if pnl_pct < worst_pct {
                    worst_pct = pnl_pct;
                }

                // Track SL triggers (approximate based on exit price)
                if exit <= 0.66 {
                    sl_650 += 1;
                } else if exit >= 0.82 && exit <= 0.84 {
                    sl_83 += 1;
                }
            } else {
                // Open trade: Track as pending
                pending_trades += 1;
                // asset_stat.total_trades is NOT incremented for pending trades
            }
        }

        if best_pct == f64::MIN {
            best_pct = 0.0;
        }
        if worst_pct == f64::MAX {
            worst_pct = 0.0;
        }

        let mut asset_breakdown: Vec<AssetStats> = asset_map.into_values().collect();
        asset_breakdown.sort_by(|a, b| a.coin.cmp(&b.coin));

        // IMPORTANT: Total trades for WR should ONLY be resolved trades
        let resolved_trades = winning + losing;

        PeriodStats {
            period_start: start.format("%H:%M").to_string(),
            period_end: end.format("%H:%M").to_string(),
            total_trades: resolved_trades, // Changed to only resolved for WR accuracy
            winning_trades: winning,
            losing_trades: losing,
            net_pnl,
            best_trade_pct: best_pct,
            worst_trade_pct: worst_pct,
            smart_delay_trades: smart_delay,
            kill_zone_trades: kill_zone,
            rapid_action_trades: rapid_action,
            smart_delay_wins,
            kill_zone_wins,
            rapid_action_wins,
            sl_650_triggers: sl_650,
            sl_83_triggers: sl_83,
            dca_executions: dca,
            full_recovery_trades: full_recovery,
            full_recovery_wins: full_recovery_wins,
            markets_blocked: 0,
            asset_breakdown,
            pending_trades,
            notice: None,
        }
    }

    /// Generate 6-hour period report with audit
    pub async fn generate_period_report(
        &self,
        client: &Client,
    ) -> Result<(PeriodStats, AuditReport), Box<dyn Error + Send>> {
        let now_raw = Local::now();
        // Use FixedOffset(-4) for ET consistency
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let _now_et = now_raw.with_timezone(&et_offset);

        // Period calculation
        let period_start = Self::get_period_start(now_raw, -1);
        let period_end = period_start + Duration::hours(6);

        info!(
            "📊 Generating 6-hour period report for PAST period: {} to {}",
            period_start.format("%H:%M"),
            period_end.format("%H:%M")
        );

        // Load trades for this period (using spawn_blocking for CSV I/O)
        let start_ts = period_start.timestamp();
        let end_ts = period_end.timestamp();
        let trades = tokio::task::spawn_blocking(move || {
            Self::load_trades_for_period_sync(start_ts, end_ts)
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?
        .map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn Error + Send>
        })?;

        // Audit trades
        let auditor = TradeAuditor::new(client.clone());
        let audit = auditor.audit_trades(&trades).await;

        // HARMONIZATION: Patch trades with official data if missing locally
        let mut audited_trades = trades.clone();
        for (i, trade) in audited_trades.iter_mut().enumerate() {
            if let Some(audit_res) = audit.results.get(i) {
                if trade.exit_price.is_none() && audit_res.official_exit_price.is_some() {
                    trade.exit_price = audit_res.official_exit_price;
                }
            }
        }

        // Calculate statistics using corrected data
        let mut stats = self.calculate_period_stats(&audited_trades, period_start, period_end);

        // Add notice if data is missing
        let log_path = get_log_path();
        if !log_path.exists() {
            stats.notice = Some(format!("⚠️ No se encontró: {:?}", log_path));
        } else if audited_trades.is_empty() {
            stats.notice = Some("ℹ️ No se encontraron operaciones en este período".to_string());
        }

        Ok((stats, audit))
    }

    /// Generate daily report with audit
    pub async fn generate_daily_report(
        &self,
        client: &Client,
    ) -> Result<(DailyStats, AuditReport), Box<dyn Error + Send>> {
        let now_raw = Local::now();
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let now_et = now_raw.with_timezone(&et_offset);

        // Daily window in ET: 00:00:00 to 23:59:59
        let day_start = now_et
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .and_then(|naive| et_offset.from_local_datetime(&naive).single())
            .unwrap();
        let day_end = day_start + Duration::hours(24);

        info!(
            "📈 Generating daily report summary up to {}",
            now_et.format("%H:%M")
        );

        // Load all trades for the day (using spawn_blocking for CSV I/O)
        let start_ts = day_start.timestamp();
        let end_ts = day_end.timestamp();
        let trades = tokio::task::spawn_blocking(move || {
            Self::load_trades_for_period_sync(start_ts, end_ts)
        })
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send>)?
        .map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)) as Box<dyn Error + Send>
        })?;

        // Audit trades first to get corrected prices
        let auditor = TradeAuditor::new(client.clone());
        let audit = auditor.audit_trades(&trades).await;

        // HARMONIZATION: Patch trades with official data
        let mut audited_trades = trades.clone();
        for (i, trade) in audited_trades.iter_mut().enumerate() {
            if let Some(audit_res) = audit.results.get(i) {
                if trade.exit_price.is_none() && audit_res.official_exit_price.is_some() {
                    trade.exit_price = audit_res.official_exit_price;
                }
            }
        }

        // Calculate period stats for each 6-hour window using corrected data
        let mut periods = Vec::new();
        for i in 0..4 {
            let p_start = day_start + Duration::hours(i as i64 * 6);
            let p_end = p_start + Duration::hours(6);
            let p_start_ts = p_start.timestamp();
            let p_end_ts = p_end.timestamp();
            let p_trades: Vec<_> = audited_trades
                .iter()
                .filter(|t| t.timestamp >= p_start_ts && t.timestamp < p_end_ts)
                .cloned()
                .collect();

            periods.push(self.calculate_period_stats(&p_trades, p_start, p_end));
        }

        // Calculate daily aggregates
        let mut total_resolved = 0;
        let mut pending_trades = 0;
        for period in &periods {
            total_resolved += period.total_trades;
            pending_trades += period.pending_trades;
        }

        let winning: usize = periods.iter().map(|p| p.winning_trades).sum();
        let win_rate = if total_resolved > 0 {
            (winning as f64 / total_resolved as f64) * 100.0
        } else {
            0.0
        };

        let net_pnl: f64 = periods.iter().map(|p| p.net_pnl).sum();
        let best_trade: f64 = periods
            .iter()
            .map(|p| p.best_trade_pct)
            .fold(f64::MIN, f64::max);
        let worst_trade: f64 = periods
            .iter()
            .map(|p| p.worst_trade_pct)
            .fold(f64::MAX, f64::min);

        // Calculate win rates by strategy (aggregated across periods)
        let smart_delay_total: usize = periods.iter().map(|p| p.smart_delay_trades).sum();
        let kill_zone_total: usize = periods.iter().map(|p| p.kill_zone_trades).sum();
        let rapid_action_total: usize = periods.iter().map(|p| p.rapid_action_trades).sum();

        let smart_delay_wins: usize = periods.iter().map(|p| p.smart_delay_wins).sum();
        let kill_zone_wins: usize = periods.iter().map(|p| p.kill_zone_wins).sum();
        let rapid_action_wins: usize = periods.iter().map(|p| p.rapid_action_wins).sum();

        let mut daily_asset_map: HashMap<String, AssetStats> = HashMap::new();
        for period in &periods {
            for asset in &period.asset_breakdown {
                let daily_stat = daily_asset_map
                    .entry(asset.coin.clone())
                    .or_insert(AssetStats {
                        coin: asset.coin.clone(),
                        ..Default::default()
                    });
                daily_stat.total_trades += asset.total_trades;
                daily_stat.winning_trades += asset.winning_trades;
                daily_stat.net_pnl += asset.net_pnl;
            }
        }
        let mut daily_asset_breakdown: Vec<AssetStats> = daily_asset_map.into_values().collect();
        daily_asset_breakdown.sort_by(|a, b| a.coin.cmp(&b.coin));

        let smart_delay_wr = if smart_delay_total > 0 {
            (smart_delay_wins as f64 / smart_delay_total as f64) * 100.0
        } else {
            0.0
        };
        let kill_zone_wr = if kill_zone_total > 0 {
            (kill_zone_wins as f64 / kill_zone_total as f64) * 100.0
        } else {
            0.0
        };
        let rapid_action_wr = if rapid_action_total > 0 {
            (rapid_action_wins as f64 / rapid_action_total as f64) * 100.0
        } else {
            0.0
        };

        let full_recovery_total: usize = periods.iter().map(|p| p.full_recovery_trades).sum();
        let full_recovery_wins: usize = periods.iter().map(|p| p.full_recovery_wins).sum();
        let full_recovery_wr = if full_recovery_total > 0 {
            (full_recovery_wins as f64 / full_recovery_total as f64) * 100.0
        } else {
            0.0
        };

        let daily_stats = DailyStats {
            date: day_start.format("%Y-%m-%d").to_string(),
            periods,
            total_trades: total_resolved,
            total_markets_analyzed: 0,
            win_rate,
            net_pnl,
            best_trade_pct: if best_trade == f64::MIN {
                0.0
            } else {
                best_trade
            },
            worst_trade_pct: if worst_trade == f64::MAX {
                0.0
            } else {
                worst_trade
            },
            avg_win_pct: 0.0,
            avg_loss_pct: 0.0,
            smart_delay_win_rate: smart_delay_wr,
            kill_zone_win_rate: kill_zone_wr,
            rapid_action_win_rate: rapid_action_wr,
            full_recovery_win_rate: full_recovery_wr,
            max_drawdown: 0.0,
            sharpe_ratio: 0.0,
            asset_breakdown: daily_asset_breakdown,
            pending_trades,
            notice: if trades.is_empty() {
                let log_path = get_log_path();
                if !log_path.exists() {
                    Some(format!("⚠️ Archivo no encontrado en: {:?}", log_path))
                } else {
                    Some("ℹ️ No se registraron operaciones en el día de hoy".to_string())
                }
            } else {
                None
            },
        };

        Ok((daily_stats, audit))
    }

    /// Reset daily statistics (called at midnight)
    pub fn reset_daily(&mut self) {
        // No state to reset - periods are calculated on-demand
        info!("🔄 Daily statistics reset at midnight");
    }

    /// Update period start (called every 6 hours)
    pub fn update_period(&mut self) {
        // No state to update - periods are calculated on-demand
        info!("🔄 Period statistics updated");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::TradeRecord;
    use chrono::{Local, TimeZone};

    #[test]
    fn test_calculate_period_stats_up_down() {
        let reporter = StatsReporter::new();
        let now = Local::now();
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let period_start = now.with_timezone(&et_offset) - Duration::hours(6);
        let period_end = now.with_timezone(&et_offset);

        let trades = vec![
            TradeRecord {
                market_id: "up_win".to_string(),
                coin: "BTC".to_string(),
                side: "UP".to_string(),
                entry_price: 0.9,
                exit_price: Some(0.95), // +5.56%
                size: 10.0,
                timestamp: period_start.timestamp() + 3600,
                entry_type: "TriggerDirect".to_string(),
                exit_confirmed: None,
                exit_avg_fill_price: None,
            },
            TradeRecord {
                market_id: "down_win".to_string(),
                coin: "ETH".to_string(),
                side: "DOWN".to_string(),
                entry_price: 0.8,
                exit_price: Some(0.85), // +6.25% (Winning: sold NO token at higher price)
                size: 10.0,
                timestamp: period_start.timestamp() + 7200,
                entry_type: "TriggerDirect".to_string(),
                exit_confirmed: None,
                exit_avg_fill_price: None,
            },
            TradeRecord {
                market_id: "down_loss".to_string(),
                coin: "SOL".to_string(),
                side: "DOWN".to_string(),
                entry_price: 0.8,
                exit_price: Some(0.75), // -6.25% (Losing: sold NO token at lower price)
                size: 10.0,
                timestamp: period_start.timestamp() + 8000,
                entry_type: "TriggerDirect".to_string(),
                exit_confirmed: None,
                exit_avg_fill_price: None,
            },
        ];

        let stats = reporter.calculate_period_stats(&trades, period_start, period_end);

        assert_eq!(stats.total_trades, 3);
        assert_eq!(stats.winning_trades, 2); // up_win and down_win
        assert_eq!(stats.losing_trades, 1); // down_loss

        // PnL Check:
        // up_win:   +5.556%  → +$0.556
        // down_win: +6.25%   → +$0.625
        // down_loss:-6.25%   → -$0.625
        // Total: +$0.556
        assert!((stats.net_pnl - 0.556).abs() < 0.01);
    }

    #[test]
    fn test_calculate_period_stats_with_pending() {
        let reporter = StatsReporter::new();
        // Set time to around 10:00 AM Eastern Time to trigger Smart Delay categorization
        // Note: crate::api::to_eastern_time handles the conversion
        // Set time to fall in Smart Delay window (9:40 - 11:40 ET)
        // With fixed UTC-5 offset, 15:30 UTC -> 10:30 AM (630 mins)
        let base_ts = 1710084600;

        let trades = vec![
            // One completed trade
            TradeRecord {
                market_id: "completed".to_string(),
                coin: "BTC".to_string(),
                side: "UP".to_string(),
                entry_price: 0.5,
                exit_price: Some(0.6),
                size: 10.0,
                timestamp: base_ts,
                entry_type: "TriggerDirect".to_string(),
                exit_confirmed: None,
                exit_avg_fill_price: None,
            },
            // One pending trade (should be counted in strategy but not PnL)
            TradeRecord {
                market_id: "pending".to_string(),
                coin: "ETH".to_string(),
                side: "DOWN".to_string(),
                entry_price: 0.5,
                exit_price: None,
                size: 10.0,
                timestamp: base_ts + 10,
                entry_type: "TriggerDirect".to_string(),
                exit_confirmed: None,
                exit_avg_fill_price: None,
            },
        ];

        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let start = et_offset.timestamp_opt(base_ts - 3600, 0).unwrap();
        let end = et_offset.timestamp_opt(base_ts + 3600, 0).unwrap();
        let stats = reporter.calculate_period_stats(&trades, start, end);

        assert_eq!(stats.total_trades, 1);
        assert_eq!(stats.pending_trades, 1);

        // Strategy categorization - only resolved should be counted for WR
        let total_strat_trades =
            stats.smart_delay_trades + stats.kill_zone_trades + stats.rapid_action_trades;
        assert_eq!(
            total_strat_trades, 1,
            "Only resolved trades should be categorized for strategy WR"
        );

        assert_eq!(stats.winning_trades, 1);
        assert!((stats.net_pnl - 2.0).abs() < 0.01);
    }
}
