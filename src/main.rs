use arbitrage_hammer::api;
use arbitrage_hammer::clob_client::PolymarketClobClient;
use arbitrage_hammer::config;
use arbitrage_hammer::dual_market::{DualCapitalManager, DualMarketPair, Platform};
use arbitrage_hammer::entry_engine::DistanceCheckResult;
use arbitrage_hammer::kalshi_client::KalshiClient;
use arbitrage_hammer::telegram::TelegramBot;
use chrono::{DateTime, Local, Timelike};
use log::{debug, error, info, warn};
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Venue {
    Polymarket,
    Kalshi,
}

#[derive(Debug, Clone)]
struct OpenPosition {
    twin_key: String,
    venue: Venue,
    coin: String,
    pm_market_id: String,
    pm_yes_token: String,
    pm_no_token: String,
    kalshi_ticker: String,
    buy_yes: bool,
    entry_price: f64,
    shares: f64,
    notional_usdc: f64,
    dca_executed: bool,
}

impl OpenPosition {
    fn side_label(&self) -> &'static str {
        if self.buy_yes { "UP" } else { "DOWN" }
    }

    fn pm_token_id(&self) -> &str {
        if self.buy_yes {
            self.pm_yes_token.as_str()
        } else {
            self.pm_no_token.as_str()
        }
    }

    fn venue_platform(&self) -> Platform {
        match self.venue {
            Venue::Polymarket => Platform::Polymarket,
            Venue::Kalshi => Platform::Kalshi,
        }
    }
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<bool>().ok())
        .unwrap_or(default)
}

fn parse_token_ids(raw: Option<&str>) -> Option<(String, String)> {
    let ids: Vec<String> = serde_json::from_str(raw?).ok()?;
    let yes = ids.first()?.clone();
    let no = ids.get(1)?.clone();
    Some((yes, no))
}

fn pick_side_from_target(binance_price: f64, target_price: Option<f64>) -> Option<bool> {
    target_price
        .filter(|target| *target > 0.0)
        .map(|target| binance_price >= target)
}

fn average_targets(targets: &[Option<f64>]) -> Option<f64> {
    let values: Vec<f64> = targets
        .iter()
        .flatten()
        .copied()
        .filter(|value| *value > 0.0)
        .collect();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn valid_unit_price(price: Option<f64>) -> Option<f64> {
    price.filter(|value| *value > 0.0 && *value < 1.0)
}

fn pct_move(current: f64, previous: f64) -> Option<f64> {
    if current > 0.0 && previous > 0.0 {
        Some((current - previous) / previous)
    } else {
        None
    }
}

fn is_btc_lead_minute(now: DateTime<Local>) -> bool {
    let lead_minute = env_f64("BTC_LEAD_WINDOW_MINUTE", 8.0)
        .round()
        .clamp(0.0, 14.0) as u32;
    now.minute() % 15 == lead_minute
}

#[derive(Debug, Clone, Copy)]
struct BtcLeadSignal {
    buy_yes: bool,
    pct: f64,
}

fn is_lagging_btc_follower(
    btc: BtcLeadSignal,
    follower_buy_yes: bool,
    follower_pct: Option<f64>,
) -> bool {
    if follower_buy_yes != btc.buy_yes {
        return false;
    }

    let Some(follower_pct) = follower_pct else {
        return false;
    };

    let lag_ratio = env_f64("BTC_LEAD_LAG_RATIO", 0.60).clamp(0.0, 1.0);
    if btc.buy_yes {
        follower_pct >= 0.0 && follower_pct < btc.pct * lag_ratio
    } else {
        follower_pct <= 0.0 && follower_pct > btc.pct * lag_ratio
    }
}

async fn detect_btc_lead_signal(
    http_client: &reqwest::Client,
    twins: &[DualMarketPair],
) -> Option<BtcLeadSignal> {
    if !env_bool("ENABLE_BTC_LEAD_TRACKING", true) {
        return None;
    }

    let btc_twin = twins.iter().find(|twin| twin.coin.eq_ignore_ascii_case("BTC"))?;
    let symbol = "BTCUSDT";
    let vol_metrics = arbitrage_hammer::volatility::get_volatility_metrics(http_client, symbol).await;
    let binance_price = vol_metrics.current_price;
    let btc_side = pick_side_from_target(binance_price, btc_twin.pm_target_price)
        .or_else(|| pick_side_from_target(binance_price, btc_twin.km_target_price))?;
    let side = if btc_side { "UP" } else { "DOWN" };
    let reference_target = average_targets(&[btc_twin.pm_target_price, btc_twin.km_target_price])?;
    let entry_engine = arbitrage_hammer::entry_engine::EntryEngine::new();
    let dist_res = entry_engine.check_asset_distance(
        Some(binance_price),
        Some(reference_target),
        symbol,
        side,
        vol_metrics.state,
        0,
        vol_metrics.z_score,
    );

    if dist_res != DistanceCheckResult::Passed {
        return None;
    }

    let btc_pct = pct_move(vol_metrics.current_price, vol_metrics.last_price)?;
    let min_btc_move = env_f64("BTC_LEAD_MIN_MOVE_PCT", 0.0015);
    let strong_enough = if btc_side {
        btc_pct >= min_btc_move
    } else {
        btc_pct <= -min_btc_move
    };

    if !strong_enough {
        return None;
    }

    info!(
        "BTC LEAD SIGNAL detected | side={} | move={:+.4}%",
        side,
        btc_pct * 100.0
    );

    Some(BtcLeadSignal {
        buy_yes: btc_side,
        pct: btc_pct,
    })
}

fn kalshi_outcome_price_hint(km: &arbitrage_hammer::kalshi_client::Market, outcome: &str) -> Option<f64> {
    km.tokens
        .iter()
        .find(|token| token.outcome.eq_ignore_ascii_case(outcome))
        .and_then(|token| valid_unit_price(Some(token.price)))
}

async fn send_telegram(bot: Option<&TelegramBot>, text: impl Into<String>) {
    if let Some(bot) = bot {
        bot.send_message(&text.into()).await;
    }
}

fn hard_sl_execution_price(reference_price: f64, hard_sl_price: f64, hard_sl_floor: f64) -> f64 {
    reference_price
        .max(hard_sl_floor)
        .min(hard_sl_price)
        .clamp(0.01, 0.99)
}

fn venue_name(venue: Venue) -> &'static str {
    match venue {
        Venue::Polymarket => "Polymarket",
        Venue::Kalshi => "Kalshi",
    }
}

fn platform_name(platform: &Platform) -> &'static str {
    match platform {
        Platform::Polymarket => "Polymarket",
        Platform::Kalshi => "Kalshi",
    }
}

fn asset_label(coin: &str) -> String {
    format!("{}-15M", coin)
}

fn format_entry_message(
    coin: &str,
    side: &str,
    displayed_target: f64,
    binance_price: f64,
    fill_price: f64,
    size: f64,
    chosen_venue: &str,
    chosen_price: f64,
    other_venue: &str,
    other_price: f64,
) -> String {
    let delta = binance_price - displayed_target;
    format!(
        "🚀 ENTRADA DETECTADA\n• Activo: {}\n• Dirección: {}\n• Plataforma elegida: {}\n• Precio detectado: {:.3}\n• {}: {:.3}\n• {}: {:.3}\n• Price to beat: {:.2}\n• {} actual: {:.2} USD\n• Delta vs PTB: {:+.2} USD\n• Precio entrada: {:.3}\n• Monto: ${:.2}",
        asset_label(coin),
        side,
        chosen_venue,
        chosen_price,
        chosen_venue,
        chosen_price,
        other_venue,
        other_price,
        displayed_target,
        coin,
        binance_price,
        delta,
        fill_price,
        size
    )
}

fn format_close_message(
    coin: &str,
    result_won: bool,
    close_reason: &str,
    entry_price: f64,
    exit_price: f64,
    amount: f64,
    pnl: f64,
    platform: &Platform,
    balance_after: f64,
) -> String {
    let title = if result_won {
        "✅ OPERACIÓN GANADA"
    } else {
        "❌ OPERACIÓN PERDIDA"
    };
    let result = if result_won { "GANADA" } else { "PERDIDA" };
    let ret = if amount > 0.0 { (pnl / amount) * 100.0 } else { 0.0 };
    format!(
        "{}\n• Activo: {}\n• Resultado: {}\n• Motivo de cierre: {}\n• Entrada: {:.3}\n• Salida: {:.3}\n• Monto operado: ${:.2}\n• P&L: {:+.2}\n• Retorno: {:+.2}%\n\n🤝🤝 Mercado Cerrado 🤝🤝\n💰{} ${:.2} 💰",
        title,
        asset_label(coin),
        result,
        close_reason,
        entry_price,
        exit_price,
        amount,
        pnl,
        ret,
        platform_name(platform),
        balance_after
    )
}

async fn execute_polymarket_entry(
    http_client: &reqwest::Client,
    poly_client: &PolymarketClobClient,
    token_id: &str,
    usdc_size: f64,
    price: f64,
    paper_mode: bool,
) -> Result<(f64, f64), String> {
    if paper_mode {
        let resp = api::place_initial_buy(http_client, token_id, price, usdc_size, token_id)
            .await
            .map_err(|e| e.to_string())?;
        Ok((resp.shares, resp.fill_price.unwrap_or(price)))
    } else {
        let resp = poly_client
            .buy(token_id, usdc_size, price)
            .await
            .map_err(|e| e.to_string())?;
        let shares = resp.shares_ordered.unwrap_or_else(|| usdc_size / price.max(0.01));
        Ok((shares, price))
    }
}

async fn close_polymarket_position(
    http_client: &reqwest::Client,
    token_id: &str,
    shares: f64,
    market_price: f64,
) -> Result<f64, String> {
    let resp = api::place_market_sell(http_client, token_id, shares, market_price.max(0.01))
        .await
        .map_err(|e| e.to_string())?;
    Ok(resp.fill_price.unwrap_or(market_price.max(0.01)))
}

async fn execute_kalshi_entry(
    kalshi_client: &KalshiClient,
    market_ticker: &str,
    buy_yes: bool,
    size: f64,
    price: f64,
    paper_mode: bool,
) -> Result<(f64, f64), String> {
    if paper_mode {
        info!(
            "PAPER Kalshi buy simulated | market={} | side={} | size={:.2} | price={:.4}",
            market_ticker,
            if buy_yes { "YES" } else { "NO" },
            size,
            price
        );
        Ok((size, price))
    } else {
        let order = if buy_yes {
            kalshi_client.buy_yes(market_ticker, size, price).await
        } else {
            kalshi_client.buy_no(market_ticker, size, price).await
        }
        .map_err(|e| e.to_string())?;

        let shares = order.fill_count_fp.parse::<f64>().unwrap_or(size);
        Ok((shares, price))
    }
}

async fn close_kalshi_position(
    kalshi_client: &KalshiClient,
    market_ticker: &str,
    buy_yes: bool,
    shares: f64,
    market_price: f64,
    paper_mode: bool,
) -> Result<f64, String> {
    if paper_mode {
        info!(
            "PAPER Kalshi close simulated | market={} | side={} | shares={:.2} | price={:.4}",
            market_ticker,
            if buy_yes { "YES" } else { "NO" },
            shares,
            market_price
        );
        Ok(market_price)
    } else {
        let price = market_price.max(0.01);
        if buy_yes {
            kalshi_client
                .sell_yes(market_ticker, shares, price)
                .await
                .map_err(|e| e.to_string())?;
        } else {
            kalshi_client
                .sell_no(market_ticker, shares, price)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(price)
    }
}

async fn build_twin_markets(
    kalshi: &KalshiClient,
    http_client: &reqwest::Client,
    now: DateTime<Local>,
) -> Vec<DualMarketPair> {
    let mut pairs = Vec::new();

    let et_now = api::to_eastern_time(Local::now());
    let poly_markets = match api::get_active_markets(http_client, et_now).await {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to fetch polymarket markets: {}", e);
            return vec![];
        }
    };

    let kalshi_markets = match kalshi.get_active_markets(now).await {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to fetch kalshi markets: {}", e);
            return vec![];
        }
    };

    let re_time = regex::Regex::new(r"(?i)(\d{1,2}:\d{2})\s*(AM|PM)?").unwrap();
    let to_total_mins = |title: &str| -> Option<i32> {
        let caps: Vec<_> = re_time.captures_iter(title).collect();
        if caps.len() < 2 {
            return None;
        }
        let cap = &caps[caps.len() - 2];
        let time_str = cap.get(1)?.as_str();
        let amp_str = cap.get(2).map(|m| m.as_str().to_uppercase());
        let parts: Vec<&str> = time_str.split(':').collect();
        let mut h: i32 = parts[0].parse().ok()?;
        let m: i32 = parts[1].parse().ok()?;
        if let Some(amp) = amp_str {
            if amp == "PM" && h != 12 {
                h += 12;
            } else if amp == "AM" && h == 12 {
                h = 0;
            }
        }
        Some(h * 60 + m)
    };
    let kalshi_open_minutes = |open_time: &Option<String>| -> Option<i32> {
        let raw = open_time.as_deref()?;
        let utc_dt = chrono::DateTime::parse_from_rfc3339(raw).ok()?;
        let eastern = utc_dt.with_timezone(&chrono_tz::America::New_York);
        Some((eastern.hour() as i32) * 60 + eastern.minute() as i32)
    };

    for pm in poly_markets {
        let question_lower = pm.question.to_lowercase();
        let coin = if question_lower.contains("btc") || question_lower.contains("bitcoin") {
            "BTC"
        } else if question_lower.contains("eth") || question_lower.contains("ethereum") {
            "ETH"
        } else if question_lower.contains("sol") || question_lower.contains("solana") {
            "SOL"
        } else if question_lower.contains("xrp") || question_lower.contains("ripple") {
            "XRP"
        } else {
            continue;
        };

        let pm_start_mins = to_total_mins(&pm.question);
        if pm_start_mins.is_none() {
            continue;
        }

        for km in &kalshi_markets {
            let km_ticker = km.id.to_uppercase();
            if !km_ticker.contains(coin)
            {
                continue;
            }

            let km_start_mins = kalshi_open_minutes(&km.open_time);
            if pm_start_mins != km_start_mins {
                debug!(
                    "Skipping twin candidate {} | PM start {:?} != Kalshi start {:?} | ticker={} | title={}",
                    coin,
                    pm_start_mins,
                    km_start_mins,
                    km.id,
                    km.question
                );
                continue;
            }

            let pm_target = match api::fetch_polymarket_price_to_beat(
                http_client,
                &pm.slug.clone().unwrap_or_default(),
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    warn!("Failed to fetch Polymarket PTB for {}: {}", pm.question, e);
                    continue;
                }
            };

            let Some((pm_yes_token, pm_no_token)) = parse_token_ids(pm.clob_token_ids.as_deref())
            else {
                warn!("Skipping Polymarket market with invalid token IDs: {}", pm.question);
                continue;
            };

            pairs.push(DualMarketPair {
                coin: coin.to_string(),
                pm_market_id: pm.id.clone(),
                pm_yes_token,
                pm_no_token,
                kalshi_ticker: km.id.clone(),
                pm_target_price: Some(pm_target),
                km_target_price: km.target_price,
                km_yes_ask_hint: kalshi_outcome_price_hint(km, "Yes"),
                km_no_ask_hint: kalshi_outcome_price_hint(km, "No"),
                is_active: true,
            });
            info!(
                "Linked Twin Market: {} - Start: {:?} | ticker={} | PM target: {:?} | Kalshi target: {:?}",
                coin,
                pm_start_mins,
                km.id,
                Some(pm_target),
                km.target_price
            );
            break;
        }
    }

    pairs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    env_logger::init();

    info!("Starting Arbitrage Hammer...");

    let startup = config::validate_startup().map_err(|e| {
        error!("Startup validation failed: {}", e);
        std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
    })?;
    let paper_mode = !startup.live_mode;
    let telegram_bot = TelegramBot::new();
    if let Some(bot) = telegram_bot.clone() {
        api::init_global_bot(bot);
    }

    let hard_sl_price = env_f64("HARD_SL_PRICE", 0.68);
    let hard_sl_exit_floor = env_f64("HARD_SL_EXIT_FLOOR", 0.47);
    let take_profit_price = env_f64("TAKE_PROFIT_PRICE", 0.98);
    let dca_min_price = env_f64("DCA_MIN_PRICE", 0.74);
    let dca_start_price = env_f64("DCA_START_PRICE", 0.76);
    let dca_size_factor = env_f64("DCA_SIZE_FACTOR", 0.5).clamp(0.0, 1.0);
    let dca_sl_gap = env_f64("DCA_SL_GAP", 0.02).max(0.0);
    let allow_dca = env_bool("ALLOW_DCA", false);

    let http_client = reqwest::Client::new();
    let poly_client = PolymarketClobClient::new();
    let kalshi_client = if paper_mode {
        info!("Paper mode enabled: using Kalshi production market data with simulated execution.");
        KalshiClient::build_prod(
            std::env::var("KALSHI_EMAIL").unwrap_or_default(),
            std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
        )
    } else {
        match KalshiClient::init_prod().await {
            Ok(mut client) => {
                if let Err(e) = client.login().await {
                    error!("Kalshi production login failed: {}", e);
                }
                client
            }
            Err(e) => {
                error!("Cannot init Kalshi Production (falling back to build()): {}", e);
                KalshiClient::build()
            }
        }
    };

    let mut capital_manager = DualCapitalManager::new(paper_mode);
    let mut open_positions: Vec<OpenPosition> = Vec::new();
    let mut last_balance_check = Local::now();
    send_telegram(
        telegram_bot.as_ref(),
        format!(
            "*Arbitrage Hammer iniciado*\nModo: `{}`\nEntry range: `{:.3}-{:.3}`\nHARD SL: `{:.3}`\nDCA: `{}`\nPosition size: `${:.2}`",
            if paper_mode { "PAPER" } else { "LIVE" },
            startup.min_entry_price,
            startup.max_entry_price,
            hard_sl_price,
            if allow_dca { "ON" } else { "OFF" },
            env_f64("POSITION_SIZE", 5.0)
        ),
    )
    .await;

    loop {
        let now = Local::now();
        if now.signed_duration_since(last_balance_check).num_minutes() >= 5 {
            info!("Syncing balances...");
            last_balance_check = now;
        }

        info!("Scanning for twin markets...");
        let twins = build_twin_markets(&kalshi_client, &http_client, now).await;

        if twins.is_empty() {
            info!("No twin markets identified. Sleeping 10s...");
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        let btc_lead_signal = if is_btc_lead_minute(now) {
            detect_btc_lead_signal(&http_client, &twins).await
        } else {
            None
        };

        for twin in &twins {
            let symbol = format!("{}USDT", twin.coin);
            let vol_metrics =
                arbitrage_hammer::volatility::get_volatility_metrics(&http_client, &symbol).await;
            let entry_engine = arbitrage_hammer::entry_engine::EntryEngine::new();
            let binance_price = vol_metrics.current_price;
            let pm_side = pick_side_from_target(binance_price, twin.pm_target_price);
            let km_side = pick_side_from_target(binance_price, twin.km_target_price);
            let buy_yes = match (pm_side, km_side) {
                (Some(pm), Some(km)) if pm == km => pm,
                (Some(pm), Some(km)) => {
                    debug!(
                        "Skipping {} due to side mismatch between venues | PM target {:?} => {} | Kalshi target {:?} => {} | Binance {:.2}",
                        twin.coin,
                        twin.pm_target_price,
                        if pm { "UP" } else { "DOWN" },
                        twin.km_target_price,
                        if km { "UP" } else { "DOWN" },
                        binance_price
                    );
                    continue;
                }
                (Some(pm), None) => pm,
                (None, Some(km)) => km,
                (None, None) => {
                    debug!(
                        "Skipping {} because neither venue exposed a usable target price",
                        twin.coin
                    );
                    continue;
                }
            };
            let side = if buy_yes { "UP" } else { "DOWN" };
            let reference_target = average_targets(&[twin.pm_target_price, twin.km_target_price])
                .unwrap_or(binance_price);
            let displayed_target = twin
                .pm_target_price
                .or(twin.km_target_price)
                .unwrap_or(reference_target);

            let pm_yes_ask = api::get_best_ask(&http_client, &twin.pm_market_id, &twin.pm_yes_token)
                .await
                .unwrap_or(0.0);
            let pm_no_ask = api::get_best_ask(&http_client, &twin.pm_market_id, &twin.pm_no_token)
                .await
                .unwrap_or(0.0);
            let pm_yes_bid = api::get_best_bid(&http_client, &twin.pm_yes_token)
                .await
                .unwrap_or(0.0);
            let pm_no_bid = api::get_best_bid(&http_client, &twin.pm_no_token)
                .await
                .unwrap_or(0.0);

            let ((km_yes_ask_opt, km_no_ask_opt), (km_yes_bid_opt, km_no_bid_opt)) = kalshi_client
                .get_outcome_top_of_book(&twin.kalshi_ticker)
                .await
                .unwrap_or(((None, None), (None, None)));
            let (km_market_yes_ask, km_market_no_ask) = if km_yes_ask_opt.unwrap_or(0.0) <= 0.0
                || km_no_ask_opt.unwrap_or(0.0) <= 0.0
            {
                kalshi_client
                    .get_market_prices(&twin.kalshi_ticker)
                    .await
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };
            let km_yes_source = if valid_unit_price(km_yes_ask_opt).is_some() {
                "orderbook"
            } else if valid_unit_price(km_market_yes_ask).is_some() {
                "market_summary"
            } else if valid_unit_price(twin.km_yes_ask_hint).is_some() {
                "cached_hint"
            } else {
                "none"
            };
            let km_no_source = if valid_unit_price(km_no_ask_opt).is_some() {
                "orderbook"
            } else if valid_unit_price(km_market_no_ask).is_some() {
                "market_summary"
            } else if valid_unit_price(twin.km_no_ask_hint).is_some() {
                "cached_hint"
            } else {
                "none"
            };
            let km_yes_ask = valid_unit_price(km_yes_ask_opt)
                .or(valid_unit_price(km_market_yes_ask))
                .or(valid_unit_price(twin.km_yes_ask_hint))
                .unwrap_or(0.0);
            let km_no_ask = valid_unit_price(km_no_ask_opt)
                .or(valid_unit_price(km_market_no_ask))
                .or(valid_unit_price(twin.km_no_ask_hint))
                .unwrap_or(0.0);
            let km_yes_bid = km_yes_bid_opt.unwrap_or(0.0);
            let km_no_bid = km_no_bid_opt.unwrap_or(0.0);
            debug!(
                "Kalshi price sources {} | YES {:.3} ({}) | NO {:.3} ({})",
                twin.kalshi_ticker, km_yes_ask, km_yes_source, km_no_ask, km_no_source
            );

            let pm_ask = if buy_yes { pm_yes_ask } else { pm_no_ask };
            let km_ask = if buy_yes { km_yes_ask } else { km_no_ask };

            let mut idx = 0usize;
            while idx < open_positions.len() {
                if open_positions[idx].twin_key != twin.kalshi_ticker {
                    idx += 1;
                    continue;
                }

                let position = open_positions[idx].clone();
                let (current_ask, current_bid) = match position.venue {
                    Venue::Polymarket => {
                        if position.buy_yes {
                            (pm_yes_ask, pm_yes_bid)
                        } else {
                            (pm_no_ask, pm_no_bid)
                        }
                    }
                    Venue::Kalshi => {
                        if position.buy_yes {
                            (km_yes_ask, km_yes_bid)
                        } else {
                            (km_no_ask, km_no_bid)
                        }
                    }
                };

                let stop_reference = if current_bid > 0.0 { current_bid } else { current_ask };
                if current_bid > 0.0 && current_bid >= take_profit_price {
                    let fill = match position.venue {
                        Venue::Polymarket => {
                            close_polymarket_position(
                                &http_client,
                                position.pm_token_id(),
                                position.shares,
                                current_bid.max(0.01),
                            )
                            .await
                        }
                        Venue::Kalshi => {
                            close_kalshi_position(
                                &kalshi_client,
                                &position.kalshi_ticker,
                                position.buy_yes,
                                position.shares,
                                current_bid.max(0.01),
                                paper_mode,
                            )
                            .await
                        }
                    };

                    match fill {
                        Ok(fill_price) => {
                            let proceeds = position.shares * fill_price;
                            let pnl = proceeds - position.notional_usdc;
                            capital_manager.add(&position.venue_platform(), proceeds);
                            let balance_after =
                                capital_manager.balance(&position.venue_platform());
                            info!(
                                "Position closed by TAKE PROFIT | coin={} | venue={:?} | side={} | fill={:.4} | shares={:.4}",
                                position.coin,
                                position.venue,
                                position.side_label(),
                                fill_price,
                                position.shares
                            );
                            send_telegram(
                                telegram_bot.as_ref(),
                                format_close_message(
                                    &position.coin,
                                    pnl >= 0.0,
                                    &format!("TP-{:.2}", take_profit_price),
                                    position.entry_price,
                                    fill_price,
                                    position.notional_usdc,
                                    pnl,
                                    &position.venue_platform(),
                                    balance_after,
                                ),
                            )
                            .await;
                            open_positions.remove(idx);
                            continue;
                        }
                        Err(e) => {
                            error!(
                                "Take profit close failed for {} on {:?}: {}",
                                position.coin, position.venue, e
                            );
                            send_telegram(
                                telegram_bot.as_ref(),
                                format!(
                                    "⚠️ ERROR DE CIERRE\n• Activo: {}\n• Venue: {}\n• Motivo: TAKE-PROFIT\n• Error: {}",
                                    asset_label(&position.coin),
                                    venue_name(position.venue.clone()),
                                    e
                                ),
                            )
                            .await;
                        }
                    }
                }
                if stop_reference > 0.0 && stop_reference <= hard_sl_price {
                    let exit_price =
                        hard_sl_execution_price(stop_reference, hard_sl_price, hard_sl_exit_floor);
                    warn!(
                        "HARD SL touched for {} {} on {:?}: ref {:.3} <= {:.3}. Closing in zone at {:.3}.",
                        position.coin,
                        position.side_label(),
                        position.venue,
                        stop_reference,
                        hard_sl_price,
                        exit_price
                    );

                    let fill = match position.venue {
                        Venue::Polymarket => {
                            close_polymarket_position(
                                &http_client,
                                position.pm_token_id(),
                                position.shares,
                                exit_price,
                            )
                            .await
                        }
                        Venue::Kalshi => {
                            close_kalshi_position(
                                &kalshi_client,
                                &position.kalshi_ticker,
                                position.buy_yes,
                                position.shares,
                                exit_price,
                                paper_mode,
                            )
                            .await
                        }
                    };

                    match fill {
                        Ok(fill_price) => {
                            let proceeds = position.shares * fill_price;
                            let pnl = proceeds - position.notional_usdc;
                            capital_manager.add(&position.venue_platform(), proceeds);
                            let balance_after =
                                capital_manager.balance(&position.venue_platform());
                            info!(
                                "Position closed by HARD SL | coin={} | venue={:?} | side={} | fill={:.4} | shares={:.4}",
                                position.coin,
                                position.venue,
                                position.side_label(),
                                fill_price,
                                position.shares
                            );
                            send_telegram(
                                telegram_bot.as_ref(),
                                format_close_message(
                                    &position.coin,
                                    pnl >= 0.0,
                                    &format!("HARD-SL-{:.2}", hard_sl_price),
                                    position.entry_price,
                                    fill_price,
                                    position.notional_usdc,
                                    pnl,
                                    &position.venue_platform(),
                                    balance_after,
                                ),
                            )
                            .await;
                            open_positions.remove(idx);
                            continue;
                        }
                        Err(e) => {
                            error!(
                                "Immediate HARD SL close failed for {} on {:?}: {}",
                                position.coin, position.venue, e
                            );
                            send_telegram(
                                telegram_bot.as_ref(),
                                format!(
                                    "⚠️ ERROR DE CIERRE\n• Activo: {}\n• Venue: {}\n• Motivo: HARD-SL\n• Error: {}",
                                    asset_label(&position.coin),
                                    venue_name(position.venue.clone()),
                                    e
                                ),
                            )
                            .await;
                        }
                    }
                }

                if allow_dca
                    && !position.dca_executed
                    && current_ask >= dca_min_price
                    && current_ask <= dca_start_price
                    && current_ask > hard_sl_price + dca_sl_gap
                    && current_bid > hard_sl_price + dca_sl_gap
                {
                    let dca_size = position.notional_usdc * dca_size_factor;
                    if dca_size > 0.0 && capital_manager.has_funds(&position.venue_platform(), dca_size) {
                        info!(
                            "DCA trigger for {} on {:?} | ask {:.3} | bid {:.3} | size {:.2}",
                            position.coin, position.venue, current_ask, current_bid, dca_size
                        );
                        let dca_result = match position.venue {
                            Venue::Polymarket => {
                                execute_polymarket_entry(
                                    &http_client,
                                    &poly_client,
                                    position.pm_token_id(),
                                    dca_size,
                                    current_ask,
                                    paper_mode,
                                )
                                .await
                            }
                            Venue::Kalshi => {
                                execute_kalshi_entry(
                                    &kalshi_client,
                                    &position.kalshi_ticker,
                                    position.buy_yes,
                                    dca_size,
                                    current_ask,
                                    paper_mode,
                                )
                                .await
                            }
                        };

                        match dca_result {
                            Ok((added_shares, fill_price)) => {
                                let previous_cost = open_positions[idx].entry_price * open_positions[idx].shares;
                                open_positions[idx].shares += added_shares;
                                open_positions[idx].notional_usdc += dca_size;
                                open_positions[idx].dca_executed = true;
                                if open_positions[idx].shares > 0.0 {
                                    open_positions[idx].entry_price =
                                        (previous_cost + added_shares * fill_price) / open_positions[idx].shares;
                                }
                                capital_manager.deduct(&position.venue_platform(), dca_size);
                                info!(
                                    "DCA executed | coin={} | venue={:?} | side={} | added_shares={:.4} | new_avg={:.4}",
                                    position.coin,
                                    position.venue,
                                    position.side_label(),
                                    added_shares,
                                    open_positions[idx].entry_price
                                );
                                send_telegram(
                                    telegram_bot.as_ref(),
                                    format!(
                                        "📦 DCA EJECUTADO\n• Activo: {}\n• Venue: {}\n• Dirección: {}\n• Monto adicional: ${:.2}\n• Shares añadidas: {:.4}\n• Nuevo promedio: {:.4}",
                                        asset_label(&position.coin),
                                        venue_name(position.venue.clone()),
                                        position.side_label(),
                                        dca_size,
                                        added_shares,
                                        open_positions[idx].entry_price
                                    ),
                                )
                                .await;
                            }
                            Err(e) => {
                                warn!(
                                    "DCA failed for {} on {:?}: {}",
                                    position.coin, position.venue, e
                                );
                                send_telegram(
                                    telegram_bot.as_ref(),
                                    format!(
                                        "*DCA falló*\nActivo: `{}`\nVenue: `{:?}`\nLado: `{}`\nMotivo: `{}`",
                                        position.coin,
                                        position.venue,
                                        position.side_label(),
                                        e
                                    ),
                                )
                                .await;
                            }
                        }
                    }
                }

                idx += 1;
            }

            if open_positions.iter().any(|p| p.twin_key == twin.kalshi_ticker) {
                continue;
            }

            if pm_ask == 0.0 || km_ask == 0.0 {
                warn!(
                    "Ignoring market {} due to zero liquidity/ask price. PM({}): {:.2}, KM({}): {:.2}",
                    twin.coin, side, pm_ask, side, km_ask
                );
                continue;
            }

            let dist_res = entry_engine.check_asset_distance(
                Some(binance_price),
                Some(reference_target),
                &symbol,
                side,
                vol_metrics.state,
                0,
                vol_metrics.z_score,
            );

            let mut btc_lead_entry = false;
            if dist_res != DistanceCheckResult::Passed {
                let follower_pct = pct_move(vol_metrics.current_price, vol_metrics.last_price);
                btc_lead_entry = dist_res == DistanceCheckResult::DistanceBlocked
                    && !twin.coin.eq_ignore_ascii_case("BTC")
                    && btc_lead_signal
                        .map(|btc| is_lagging_btc_follower(btc, buy_yes, follower_pct))
                        .unwrap_or(false);

                if btc_lead_entry {
                    if let Some(btc) = btc_lead_signal {
                        info!(
                            "BTC lead tracking allows {} {} | BTC move {:+.4}% | follower move {:+.4}% | normal filter {:?}",
                            twin.coin,
                            side,
                            btc.pct * 100.0,
                            follower_pct.unwrap_or(0.0) * 100.0,
                            dist_res
                        );
                    }
                } else {
                    debug!(
                        "Asset {} failed distance/volatility check. Z={:.2}",
                        twin.coin, vol_metrics.z_score
                    );
                    continue;
                }
            }

            let price_diff_cents = (pm_ask - km_ask).abs() * 100.0;
            let size = env_f64("POSITION_SIZE", 5.0);
            let trigger_min = env_f64("MIN_ENTRY_PRICE", 0.05);
            let trigger_max = env_f64("MAX_ENTRY_PRICE", 0.95);
            let pm_entry_valid = pm_ask >= trigger_min && pm_ask <= trigger_max && pm_ask > hard_sl_price;
            let km_entry_valid = km_ask >= trigger_min && km_ask <= trigger_max && km_ask > hard_sl_price;
            let lagging_is_polymarket = match (pm_entry_valid, km_entry_valid) {
                (true, true) => pm_ask <= km_ask,
                (true, false) => true,
                (false, true) => false,
                (false, false) => {
                    debug!(
                        "Entry range blocked {} {} | no valid venue in {:.3}-{:.3} above HARD SL {:.3} | PM {:.3} | KM {:.3}",
                        twin.coin,
                        side,
                        trigger_min,
                        trigger_max,
                        hard_sl_price,
                        pm_ask,
                        km_ask
                    );
                    continue;
                }
            };
            let chosen_ask = if lagging_is_polymarket { pm_ask } else { km_ask };

            info!(
                "ALPHA LOBO TRIGGER in {}! Side: {} | mode={} | chosen venue={} | chosen ask: {:.3} | PM ask: {:.3}, KM ask: {:.3} | PM target: {:?} | KM target: {:?} | Ref target: {:.2} | Delta: {:.1}c | Z: {:.2}",
                twin.coin,
                side,
                if btc_lead_entry { "BTC_LEAD" } else { "STANDARD" },
                if lagging_is_polymarket { "Polymarket" } else { "Kalshi" },
                chosen_ask,
                pm_ask,
                km_ask,
                twin.pm_target_price,
                twin.km_target_price,
                reference_target,
                price_diff_cents,
                vol_metrics.z_score
            );

            if price_diff_cents >= 1.5 {
                if lagging_is_polymarket {
                    if capital_manager.has_funds(&Platform::Polymarket, size) {
                        match execute_polymarket_entry(
                            &http_client,
                            &poly_client,
                            if buy_yes { &twin.pm_yes_token } else { &twin.pm_no_token },
                            size,
                            pm_ask,
                            paper_mode,
                        )
                        .await
                        {
                            Ok((shares, fill_price)) => {
                                capital_manager.deduct(&Platform::Polymarket, size);
                                open_positions.push(OpenPosition {
                                    twin_key: twin.kalshi_ticker.clone(),
                                    venue: Venue::Polymarket,
                                    coin: twin.coin.clone(),
                                    pm_market_id: twin.pm_market_id.clone(),
                                    pm_yes_token: twin.pm_yes_token.clone(),
                                    pm_no_token: twin.pm_no_token.clone(),
                                    kalshi_ticker: twin.kalshi_ticker.clone(),
                                    buy_yes,
                                    entry_price: fill_price,
                                    shares,
                                    notional_usdc: size,
                                    dca_executed: false,
                                });
                                send_telegram(
                                    telegram_bot.as_ref(),
                                    format_entry_message(
                                        &twin.coin,
                                        side,
                                        displayed_target,
                                        binance_price,
                                        fill_price,
                                        size,
                                        "Polymarket",
                                        pm_ask,
                                        "Kalshi",
                                        km_ask,
                                    ),
                                )
                                .await;
                            }
                            Err(e) => {
                                error!("Polymarket entry failed: {}", e);
                                send_telegram(
                                    telegram_bot.as_ref(),
                                    format!(
                                        "*Entrada falló*\nActivo: `{}`\nVenue: `Polymarket`\nLado: `{}`\nMotivo: `{}`",
                                        twin.coin, side, e
                                    ),
                                )
                                .await;
                            }
                        }
                    }
                } else if capital_manager.has_funds(&Platform::Kalshi, size) {
                    match execute_kalshi_entry(
                        &kalshi_client,
                        &twin.kalshi_ticker,
                        buy_yes,
                        size,
                        km_ask,
                        paper_mode,
                    )
                    .await
                    {
                        Ok((shares, fill_price)) => {
                            capital_manager.deduct(&Platform::Kalshi, size);
                            open_positions.push(OpenPosition {
                                twin_key: twin.kalshi_ticker.clone(),
                                venue: Venue::Kalshi,
                                coin: twin.coin.clone(),
                                pm_market_id: twin.pm_market_id.clone(),
                                pm_yes_token: twin.pm_yes_token.clone(),
                                pm_no_token: twin.pm_no_token.clone(),
                                kalshi_ticker: twin.kalshi_ticker.clone(),
                                buy_yes,
                                entry_price: fill_price,
                                shares,
                                notional_usdc: size,
                                dca_executed: false,
                            });
                            send_telegram(
                                telegram_bot.as_ref(),
                                format_entry_message(
                                    &twin.coin,
                                    side,
                                    displayed_target,
                                    binance_price,
                                    fill_price,
                                    size,
                                    "Kalshi",
                                    km_ask,
                                    "Polymarket",
                                    pm_ask,
                                ),
                            )
                            .await;
                        }
                        Err(e) => {
                            error!("Kalshi entry failed: {}", e);
                            send_telegram(
                                telegram_bot.as_ref(),
                                format!(
                                    "*Entrada falló*\nActivo: `{}`\nVenue: `Kalshi`\nLado: `{}`\nMotivo: `{}`",
                                    twin.coin, side, e
                                ),
                            )
                            .await;
                        }
                    }
                }
            } else if capital_manager.has_funds(&Platform::Polymarket, size) {
                match execute_polymarket_entry(
                    &http_client,
                    &poly_client,
                    if buy_yes { &twin.pm_yes_token } else { &twin.pm_no_token },
                    size,
                    pm_ask,
                    paper_mode,
                )
                .await
                {
                    Ok((shares, fill_price)) => {
                        capital_manager.deduct(&Platform::Polymarket, size);
                        open_positions.push(OpenPosition {
                            twin_key: twin.kalshi_ticker.clone(),
                            venue: Venue::Polymarket,
                            coin: twin.coin.clone(),
                            pm_market_id: twin.pm_market_id.clone(),
                            pm_yes_token: twin.pm_yes_token.clone(),
                            pm_no_token: twin.pm_no_token.clone(),
                            kalshi_ticker: twin.kalshi_ticker.clone(),
                            buy_yes,
                            entry_price: fill_price,
                            shares,
                            notional_usdc: size,
                            dca_executed: false,
                        });
                        send_telegram(
                            telegram_bot.as_ref(),
                            format_entry_message(
                                &twin.coin,
                                side,
                                displayed_target,
                                binance_price,
                                fill_price,
                                size,
                                "Polymarket",
                                pm_ask,
                                "Kalshi",
                                km_ask,
                            ),
                        )
                        .await;
                    }
                    Err(e) => {
                        error!("Polymarket entry failed: {}", e);
                        send_telegram(
                            telegram_bot.as_ref(),
                            format!(
                                "*Entrada falló*\nActivo: `{}`\nVenue: `Polymarket`\nLado: `{}`\nMotivo: `{}`",
                                twin.coin, side, e
                            ),
                        )
                        .await;
                    }
                }
            }
        }

        sleep(Duration::from_secs(15)).await;
    }
}
