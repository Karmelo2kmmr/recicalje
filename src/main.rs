mod binance_ws;
mod polymarket_api;
mod telegram_reporter;
mod strategy_manager;
mod csv_logger;
mod equity_manager;
mod reporting_engine;
mod volatility;

use binance_ws::BinanceWS;
use telegram_reporter::TelegramReporter;
use polymarket_api::PolymarketAPI;
use strategy_manager::{StrategyManager, StrategyState};
use csv_logger::CSVLogger;
use reporting_engine::ReportingEngine;
use volatility::{AtrVolatility, Candle, VolRegime, FastVolatility, AtrRatioVolatility};
use chrono::{Utc, Timelike, FixedOffset, Datelike};
use dotenv::dotenv;
use log::{info, warn, error};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    info!("🚀 Starting Polymarket 5m DCA+Reciclaje Bot...");

    let reporter = TelegramReporter::new().expect("Failed to initialize Telegram Reporter");
    let api = PolymarketAPI::new();
    let csv_logger = CSVLogger::new();
    
    // Active strategies indexed by Market ID
    let mut active_strategies: HashMap<String, StrategyManager> = HashMap::new();
    // Throttling tracking
    let mut last_api_calls: HashMap<String, i64> = HashMap::new();

    let (binance_connector, mut price_rx) = BinanceWS::new();
    
    // Run Binance WS in background
    tokio::spawn(async move {
        binance_connector.run("BTCUSDT").await;
    });

    info!("🔍 Monitoring prices and markets...");

    // Volatility tracking
    let mut vol_monitor = AtrVolatility::new();
    let mut ratio_vol_monitor = AtrRatioVolatility::new();
    let mut is_ratio_warmed_up = false;
    let mut current_candle: Option<Candle> = None;
    let mut last_candle_minute = -1i32;
    let mut current_vol_regime = VolRegime::Mid;
    let mut btc_fast_vol = FastVolatility::new(20);
    let mut current_btc_vol_pct = 0.0;

    // Market timing tracking
    let mut last_market_bucket = 0i64;

    reporter.send_message("🤖 *DCA+Reciclaje Bot Activado*\n🏔️ Peak Range: 0.91-0.95 | Pullback: 0.03\n🛡️ SL Estricto: 0.67 | TP Global: 0.96\n♻️ Reciclaje: L3, L4, L5 (+0.06)").await;

    loop {
        match price_rx.recv().await {
            Ok(price) => {
                // Throttle check
                let now = chrono::Utc::now();
                let now_ms = now.timestamp_millis();
                let _bucket_elapsed_ms = now_ms % 300_000;
                let minute = now.timestamp() / 60;
                let _minute_i32 = (minute % 1440) as i32;

                // 0. Update high-frequency BTC volatility
                let (btc_regime, btc_pct) = btc_fast_vol.update(price);
                current_btc_vol_pct = btc_pct;
                if !is_ratio_warmed_up {
                    current_vol_regime = btc_regime;
                }
                
                let now = chrono::Utc::now();
                let now_ms = now.timestamp_millis();
                let minute = now.timestamp() / 60;
                let minute_i32 = (minute % 1440) as i32;

                // 1. Update Volatility Candle
                if _minute_i32 != last_candle_minute {
                    if let Some(candle) = current_candle {
                        if let Some((_, _, _pct, _regime)) = vol_monitor.update(candle) {
                            // keep old monitor updating for telemetry if needed
                        }
                        if let Some((atr15, atr200, ratio, regime)) = ratio_vol_monitor.update(candle) {
                            current_vol_regime = regime;
                            is_ratio_warmed_up = true;
                            info!("📈 ATR Ratio Update: {:?} (ATR15: {:.4}, ATR200: {:.4}, Ratio: {:.2})", regime, atr15, atr200, ratio);
                        }
                    }
                    current_candle = Some(Candle { high: price, low: price, close: price });
                    last_candle_minute = minute_i32;
                } else if let Some(ref mut candle) = current_candle {
                    candle.high = candle.high.max(price);
                    candle.low = candle.low.min(price);
                    candle.close = price;
                }

                // 2. Check Market Transition (5 min)
                let current_bucket = (now.timestamp() / 300) * 300;
                if current_bucket != last_market_bucket {
                    // Check daily capital initialization
                    equity_manager::initialize_daily_capital();

                    if last_market_bucket != 0 {
                        info!("🏁 5m Market Transition Detected. Closing previous cycle.");
                        
                        // Force close all active strategies before clearing them
                        for strategy in active_strategies.values_mut() {
                            strategy.force_close_on_expiration(price as f64).await;
                        }
                        
                        let current_equity = equity_manager::compute_equity();
                        reporter.send_message(&format!(
                            "🤝 *ya se serro el mercado*\n\
                             • balance actual: *${:.2}*",
                            current_equity
                        )).await;

                        reporter.notify_market_closed().await;
                    }
                    last_market_bucket = current_bucket;
                    active_strategies.clear();
                    last_api_calls.clear();
                    
                    info!("♻️ Refreshing active markets for bucket {}...", current_bucket);

                    let current_equity = equity_manager::compute_equity();
                    let stakes = equity_manager::calculate_dca_stakes(current_equity);
                    let stakes_str = stakes.iter().enumerate()
                        .map(|(i, s)| format!("L{}: ${:.2}", i+1, s))
                        .collect::<Vec<_>>().join(" | ");
                    
                    reporter.send_message(&format!(
                        "📦 *Configurando nuevo ciclo*\n\
                         • Asignación: {}",
                        stakes_str
                    )).await;

                    let markets = api.get_active_5m_markets().await;
                    for m in &markets {
                        if !active_strategies.contains_key(&m.id) {
                            let up_token = m.tokens.iter().find(|t| t.outcome == "Up");
                            let down_token = m.tokens.iter().find(|t| t.outcome == "Down");

                            if let (Some(up), Some(down)) = (up_token, down_token) {
                                info!("🔔 New market detected: {} | ID: {}", m.question, m.id);
                                active_strategies.insert(
                                    m.id.clone(),
                                    StrategyManager::new(
                                        m.id.clone(),
                                        up.token_id.clone(), 
                                        down.token_id.clone(), 
                                        reporter.clone(), 
                                        api.new_instance(),
                                        csv_logger.clone(),
                                        "UP".to_string(),
                                        price as f64,
                                        current_equity,
                                    )
                                );
                            }
                        }
                    }
                }

                // 3. Process Ticks
                let is_hard_close_window = _bucket_elapsed_ms >= 298_500;
                let kill_switch_active = equity_manager::is_kill_switch_active();

                if kill_switch_active && now.second() == 0 {
                    warn!("🚨 KILL SWITCH ACTIVO: Drawdown > 12%. Nuevas entradas bloqueadas.");
                }

                let mut processed = 0;
                for (_id, strategy) in active_strategies.iter_mut() {
                    // --- API Throttling ---
                    let last_call = *last_api_calls.get(_id).unwrap_or(&0);
                    if now_ms - last_call < 1000 {
                        continue;
                    }
                    last_api_calls.insert(_id.clone(), now_ms);
                    processed += 1;

                    // --- Hard Close Guard (298.5s) ---
                    if is_hard_close_window && strategy.state == StrategyState::InPosition {
                        warn!("🚨 HARD CLOSE (298.5s) para {}!", _id);
                        let exit_price = strategy.api.get_market_price(&strategy.current_token_id).await
                            .map(|(b, _a)| b).unwrap_or(0.0);
                        strategy.close_position(exit_price, "HARD-CLOSE-298.5s", "Safety").await;
                        strategy.state = StrategyState::Finished;
                        continue;
                    }

                    if strategy.state == StrategyState::Scanning {
                        if kill_switch_active { continue; }

                        // 1. Process Main Token (Initially UP, but flips to DOWN if peak found there first)
                        if let Some((bid, ask)) = strategy.api.get_market_price(&strategy.token_id_main).await {
                            strategy.current_token_id = strategy.token_id_main.clone();
                            // strategy.side = strategy.main_side.clone(); // Ensure we use the persistent side
                            strategy.tick(bid, ask, current_vol_regime, current_btc_vol_pct, (_bucket_elapsed_ms / 1000) as u64, false).await;
                        }

                        if strategy.state != StrategyState::Scanning { continue; }

                        // 2. Token secundario (DOWN) — si DOWN empieza a formar pico, invertir roles
                        if let Some((bid, ask)) = strategy.api.get_market_price(&strategy.token_id_recovery).await {
                            if ask >= 0.91 && ask <= 0.95 {
                                info!("🔄 Pico 0.91-0.95 detectado en token DOWN. Invirtiendo roles...");
                                let old_main = strategy.token_id_main.clone();
                                strategy.token_id_main = strategy.token_id_recovery.clone();
                                strategy.token_id_recovery = old_main;
                                strategy.current_token_id = strategy.token_id_main.clone();
                                strategy.side = "DOWN".to_string();
                                strategy.main_side = "DOWN".to_string();
                                strategy.tick(bid, ask, current_vol_regime, current_btc_vol_pct, (_bucket_elapsed_ms / 1000) as u64, false).await;
                            }
                        }
                    } else {
                        // InPosition → procesar tick normal
                        if let Some((bid, ask)) = strategy.api.get_market_price(&strategy.current_token_id).await {
                            strategy.tick(bid, ask, current_vol_regime, current_btc_vol_pct, (_bucket_elapsed_ms / 1000) as u64, false).await;
                        }
                    }
                }
                
                if processed > 0 && now.second() % 10 == 0 {
                    info!("⏱️ Scan loop complete for {} strategies", processed);
                }

                // Clean up finished strategies
                active_strategies.retain(|_, s| s.state != StrategyState::Finished);

                // --- Reporting System (ET Time) ---
                let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
                let now_et = Utc::now().with_timezone(&et_offset);
                let hour = now_et.hour();
                let minute = now_et.minute();

                // 1. Reportes de 6 Horas
                let is_6h_time = (hour % 6 == 0) && (minute == 0);
                if is_6h_time {
                    static mut LAST_6H_REPORT: u32 = 99;
                    unsafe {
                        if LAST_6H_REPORT != hour {
                            info!("📊 Generando reporte periódico de 6 horas ({} ET)...", hour);
                            if let Some(report) = ReportingEngine::get_stats_report(&api, 6, "Reporte Periódico (6H)").await {
                                reporter.notify_session_report(&report).await;
                            }
                            LAST_6H_REPORT = hour;
                        }
                    }
                }

                // 2. Reporte Diario: 23:58
                let is_daily_time = (hour == 23) && (minute == 58);
                if is_daily_time {
                    static mut LAST_DAILY_DAY: u32 = 0;
                    let day = now_et.day();
                    unsafe {
                        if LAST_DAILY_DAY != day {
                            info!("📊 Generando reporte diario de cierre (23:58 ET)...");
                            if let Some(report) = ReportingEngine::get_stats_report(&api, 24, "REPORTE DIARIO DE OPERACIONES").await {
                                reporter.notify_session_report(&report).await;
                            }
                            LAST_DAILY_DAY = day;
                        }
                    }
                }
            },
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("⚠️ Binance lag detected: missed {} messages", n);
            },
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                error!("❌ Binance channel closed!");
                break;
            }
        }
    }
}

impl PolymarketAPI {
    pub fn new_instance(&self) -> Self {
        Self::new()
    }
}
