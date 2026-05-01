use arbitrage_hammer::api;
use arbitrage_hammer::clob_client::PolymarketClobClient;
use arbitrage_hammer::config;
use arbitrage_hammer::entry_engine;
use arbitrage_hammer::kalshi_client::KalshiClient;
use chrono::{DateTime, Local, Timelike};
use std::collections::HashMap;

fn parse_token_ids(json_str: Option<&str>) -> Option<(String, String)> {
    let ids: Vec<String> = serde_json::from_str(json_str?).ok()?;
    if ids.len() >= 2 {
        Some((ids[0].clone(), ids[1].clone()))
    } else {
        None
    }
}

fn window_elapsed_secs(now: DateTime<Local>, window_start_mins: i32) -> i32 {
    let current_total_secs =
        (now.hour() as i32 * 3600) + (now.minute() as i32 * 60) + now.second() as i32;
    let start_total_secs = window_start_mins * 60;
    (current_total_secs - start_total_secs).rem_euclid(86400)
}

fn is_polymarket_current_et_date(pm: &api::Market, now: DateTime<Local>) -> bool {
    pm.end_date
        .as_deref()
        .and_then(|d| chrono::DateTime::parse_from_rfc3339(d).ok())
        .map(|dt| dt.with_timezone(&Local).date_naive() == now.date_naive())
        .unwrap_or(false)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv::dotenv().ok();

    let startup =
        config::validate_startup().map_err(|e| format!("startup validation failed: {}", e))?;
    let http = reqwest::Client::builder()
        .user_agent("Mozilla/5.0")
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let poly = PolymarketClobClient::new();
    let mut kalshi = KalshiClient::build_prod(
        std::env::var("KALSHI_EMAIL").unwrap_or_default(),
        std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
    );
    kalshi.login().await?;

    let now = Local::now();
    println!("now_et={}", now.format("%Y-%m-%d %H:%M:%S"));
    println!(
        "entry_window={}..{} max_entry={:.3} min_entry={:.3} max_open={}",
        std::env::var("ENTRY_START_SEC").unwrap_or_else(|_| "540".into()),
        std::env::var("ENTRY_END_SEC").unwrap_or_else(|_| "790".into()),
        startup.max_entry_price,
        startup.min_entry_price,
        startup.max_open_positions
    );

    let tag_id = std::env::var("TAG_ID").unwrap_or_else(|_| "102467".to_string());
    let poly_markets = poly
        .get_markets_proxy(&tag_id)
        .await
        .map_err(|e| format!("polymarket markets error: {}", e))?;
    println!("polymarket_feed_count={}", poly_markets.len());

    let mut kalshi_map = HashMap::new();
    let kalshi_markets = kalshi.get_active_markets(now).await?;
    println!("kalshi_active_candidates={}", kalshi_markets.len());
    for km in kalshi_markets {
        let q_up = km.question.to_uppercase();
        let coin = if q_up.contains("BTC") {
            "BTC"
        } else if q_up.contains("ETH") {
            "ETH"
        } else if q_up.contains("SOL") {
            "SOL"
        } else if q_up.contains("XRP") {
            "XRP"
        } else {
            continue;
        };
        if let Some(start) = km
            .open_time
            .as_deref()
            .and_then(api::extract_kalshi_window_start)
        {
            kalshi_map.insert(format!("{}-{}", coin, start), km);
        }
    }

    println!("kalshi_key_count={}", kalshi_map.len());
    for key in kalshi_map.keys() {
        println!("kalshi_key={}", key);
    }

    let mut linked = 0;
    for pm in &poly_markets {
        if !is_polymarket_current_et_date(pm, now) {
            continue;
        }

        let q_up = pm.question.to_uppercase();
        let coin = if q_up.contains("BITCOIN") || q_up.contains("BTC") {
            "BTC"
        } else if q_up.contains("ETHEREUM") || q_up.contains("ETH") {
            "ETH"
        } else if q_up.contains("SOLANA") || q_up.contains("SOL") {
            "SOL"
        } else if q_up.contains("XRP") || q_up.contains("RIPPLE") {
            "XRP"
        } else {
            continue;
        };

        let Some((start, _end)) = api::extract_window_times(&pm.question) else {
            continue;
        };
        let key = format!("{}-{}", coin, start);
        let Some(km) = kalshi_map.get(&key) else {
            continue;
        };
        let Some((pm_yes, pm_no)) = parse_token_ids(pm.clob_token_ids.as_deref()) else {
            continue;
        };
        linked += 1;

        let symbol = format!("{}USDT", coin);
        let vol = arbitrage_hammer::volatility::get_volatility_metrics(&http, &symbol).await;
        let binance_price = vol.current_price;
        let binance_open =
            match arbitrage_hammer::volatility::fetch_binance_candle_open(&http, &symbol, start)
                .await
            {
                Ok(open) => open,
                Err(e) => {
                    println!(
                        "{} key={} reason=BINANCE_OPEN_UNAVAILABLE error={}",
                        coin, key, e
                    );
                    continue;
                }
            };
        let dist = (binance_price - binance_open).abs();
        let signal_up = binance_price > binance_open;
        let threshold = entry_engine::distance_threshold_for(&symbol, vol.state);
        let entry_signal = dist >= threshold;
        let elapsed = window_elapsed_secs(now, start);
        let entry_start = std::env::var("ENTRY_START_SEC")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(540);
        let entry_end = std::env::var("ENTRY_END_SEC")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(790);

        let pm_ask = if signal_up {
            api::get_best_ask(&http, &pm.id, &pm_yes)
                .await
                .unwrap_or(0.0)
        } else {
            api::get_best_ask(&http, &pm.id, &pm_no)
                .await
                .unwrap_or(0.0)
        };
        let ((ky_ask, kn_ask), _) = kalshi
            .get_outcome_top_of_book(&km.id)
            .await
            .unwrap_or(((None, None), (None, None)));
        let km_ask = if signal_up {
            ky_ask.unwrap_or(0.0)
        } else {
            kn_ask.unwrap_or(0.0)
        };
        let chosen_ask = if pm_ask > 0.0 && (km_ask <= 0.0 || pm_ask < km_ask) {
            pm_ask
        } else {
            km_ask
        };

        let reason = if !entry_signal {
            "NO_SIGNAL"
        } else if elapsed < entry_start || elapsed > entry_end {
            "OUTSIDE_ENTRY_WINDOW"
        } else if chosen_ask <= 0.0 {
            "NO_VALID_ASK"
        } else if chosen_ask > startup.max_entry_price {
            "ASK_ABOVE_MAX"
        } else {
            "WOULD_ENTER"
        };

        println!(
            "{} key={} reason={} side={} elapsed={} binance={:.4} open={:.4} dist={:.4}/{:.4} vol={:?} pm_ask={:.3} km_ask={:.3} chosen={:.3} pm='{}' km='{}'",
            coin,
            key,
            reason,
            if signal_up { "UP" } else { "DOWN" },
            elapsed,
            binance_price,
            binance_open,
            dist,
            threshold,
            vol.state,
            pm_ask,
            km_ask,
            chosen_ask,
            pm.question,
            km.id,
        );
    }

    println!("linked_pairs={}", linked);
    Ok(())
}
