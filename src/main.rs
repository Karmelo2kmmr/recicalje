mod binance_ws;
mod polymarket_api;
mod telegram_reporter;
mod strategy_manager;
mod csv_logger;
mod equity_manager;
mod reporting_engine;

use std::collections::{HashMap, VecDeque};

use binance_ws::BinanceWS;
use chrono::{Datelike, FixedOffset, Timelike, Utc};
use csv_logger::CSVLogger;
use dotenv::dotenv;
use log::{error, info, warn};
use polymarket_api::PolymarketAPI;
use reporting_engine::ReportingEngine;
use strategy_manager::{StrategyManager, StrategyState};
use telegram_reporter::TelegramReporter;

const POLYMARKET_SCAN_INTERVAL_MS: i64 = 1_000;
const POSITION_CHECK_INTERVAL_MS: i64 = 2_000;
const BTC_MOMENTUM_WINDOW_SECS: i64 = 120;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    info!("Starting BTC 5m Alpha Momentum Pure bot...");

    let reporter = TelegramReporter::new().expect("Failed to initialize Telegram Reporter");
    let api = PolymarketAPI::new();
    let csv_logger = CSVLogger::new();

    let mut active_strategies: HashMap<String, StrategyManager> = HashMap::new();
    let mut last_api_calls: HashMap<String, i64> = HashMap::new();

    let (binance_connector, mut price_rx) = BinanceWS::new();
    tokio::spawn(async move {
        binance_connector.run("BTCUSDT").await;
    });

    let mut btc_momentum_prices: VecDeque<(i64, f64)> = VecDeque::new();
    let mut last_market_bucket = 0i64;

    reporter.send_message(
        "🚀 *ALPHA MOMENTUM PURE ACTIVADO*\n\
        • Mercado: *BTC 5m*\n\
        • Entrada: *0.86 a 0.91* con momentum BTC positivo\n\
        • Filtro BTC: *variacion >= 22 USD* vs price to beat\n\
        • Ventana: *3:10 a 4:46*\n\
        • SL duro: *0.72*\n\
        • TP total: *100% @ 0.98*",
    ).await;

    loop {
        match price_rx.recv().await {
            Ok(price) => {
                let now = Utc::now();
                let now_ms = now.timestamp_millis();
                let bucket_elapsed_ms = now_ms % 300_000;
                let bucket_elapsed_secs = (bucket_elapsed_ms / 1_000) as u64;
                let current_bucket = (now.timestamp() / 300) * 300;

                btc_momentum_prices.push_back((now.timestamp(), price));
                while let Some((ts, _)) = btc_momentum_prices.front() {
                    if now.timestamp() - *ts > BTC_MOMENTUM_WINDOW_SECS {
                        btc_momentum_prices.pop_front();
                    } else {
                        break;
                    }
                }
                let binance_momentum_up = btc_momentum_prices
                    .front()
                    .map(|(_, oldest_price)| price > *oldest_price)
                    .unwrap_or(false);

                if current_bucket != last_market_bucket {
                    equity_manager::initialize_daily_capital();

                    if last_market_bucket != 0 {
                        info!("5m market transition detected. Closing previous cycle.");
                        for strategy in active_strategies.values_mut() {
                            strategy.force_close_on_expiration(price).await;
                        }
                        reporter.notify_market_closed().await;
                    }

                    last_market_bucket = current_bucket;
                    active_strategies.clear();
                    last_api_calls.clear();

                    let current_equity = equity_manager::compute_equity();
                    let markets = api.get_active_5m_markets().await;
                    for market in &markets {
                        if active_strategies.contains_key(&market.id) {
                            continue;
                        }

                        let up_token = market.tokens.iter().find(|t| t.outcome == "Up");
                        if let Some(up) = up_token {
                            info!("New BTC 5m market detected: {} | ID: {}", market.question, market.id);
                            active_strategies.insert(
                                market.id.clone(),
                                StrategyManager::new(
                                    market.id.clone(),
                                    up.token_id.clone(),
                                    reporter.clone(),
                                    api.new_instance(),
                                    csv_logger.clone(),
                                    price,
                                    current_equity,
                                ),
                            );
                        }
                    }
                }

                let kill_switch_active = equity_manager::is_kill_switch_active();
                if kill_switch_active && now.second() == 0 {
                    warn!(
                        "KILL SWITCH ACTIVO: drawdown >= {:.0}%. Nuevas entradas bloqueadas.",
                        equity_manager::kill_switch_drawdown_pct() * 100.0
                    );
                }

                let is_hard_close_window = bucket_elapsed_ms >= 297_000;

                for (market_id, strategy) in active_strategies.iter_mut() {
                    let interval = if strategy.state == StrategyState::InPosition {
                        POSITION_CHECK_INTERVAL_MS
                    } else {
                        POLYMARKET_SCAN_INTERVAL_MS
                    };

                    let last_call = *last_api_calls.get(market_id).unwrap_or(&0);
                    if now_ms - last_call < interval {
                        continue;
                    }
                    last_api_calls.insert(market_id.clone(), now_ms);

                    if is_hard_close_window && strategy.state == StrategyState::InPosition {
                        warn!("HARD CLOSE (298.5s) para {}!", market_id);
                        let exit_price = strategy
                            .api
                            .get_market_price(&strategy.current_token_id)
                            .await
                            .map(|(bid, _)| bid)
                            .unwrap_or(0.0);
                        strategy
                            .close_position(exit_price, "HARD-CLOSE-298.5s", "Alpha")
                            .await;
                        continue;
                    }

                    if strategy.state == StrategyState::Scanning && kill_switch_active {
                        continue;
                    }

                    if let Some((bid, ask)) = strategy.api.get_market_price(&strategy.current_token_id).await {
                        strategy
                            .tick(bid, ask, bucket_elapsed_secs, binance_momentum_up, price)
                            .await;
                    }
                }

                active_strategies.retain(|_, strategy| strategy.state != StrategyState::Finished);

                let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
                let now_et = Utc::now().with_timezone(&et_offset);
                let hour = now_et.hour();
                let minute = now_et.minute();

                let is_6h_time = (hour % 6 == 0) && (minute == 0);
                if is_6h_time {
                    static mut LAST_6H_REPORT: u32 = 99;
                    unsafe {
                        if LAST_6H_REPORT != hour {
                            info!("Generando reporte de 6 horas ({} ET)...", hour);
                            if let Some(report) =
                                ReportingEngine::get_stats_report(&api, 6, "Reporte ALPHA (6H)").await
                            {
                                reporter.notify_session_report(&report).await;
                            }
                            LAST_6H_REPORT = hour;
                        }
                    }
                }

                let is_daily_time = (hour == 23) && (minute == 58);
                if is_daily_time {
                    static mut LAST_DAILY_DAY: u32 = 0;
                    let day = now_et.day();
                    unsafe {
                        if LAST_DAILY_DAY != day {
                            info!("Generando reporte diario ALPHA (23:58 ET)...");
                            if let Some(report) = ReportingEngine::get_stats_report(
                                &api,
                                24,
                                "REPORTE DIARIO ALPHA MOMENTUM PURE",
                            )
                            .await
                            {
                                reporter.notify_session_report(&report).await;
                            }
                            LAST_DAILY_DAY = day;
                        }
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                warn!("Binance lag detected: missed {} messages", n);
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                error!("Binance channel closed!");
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
