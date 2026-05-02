use crate::telegram::TelegramBot;
use chrono::{DateTime, Datelike, FixedOffset, Local, Timelike, Utc};
use chrono_tz::America::New_York;
use log::{debug, error, info, warn};
pub use reqwest::{Client, RequestBuilder, Response};
use serde::Deserialize;
use std::error::Error;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex, OnceLock,
};
use tokio::time::{sleep, Duration};

pub static GLOBAL_BOT: OnceLock<TelegramBot> = OnceLock::new();
const GAMMA_URL: &str = "https://gamma-api.polymarket.com";
static PAPER_BALANCES: OnceLock<Mutex<std::collections::HashMap<String, f64>>> = OnceLock::new();

static CONSECUTIVE_ERRORS: AtomicUsize = AtomicUsize::new(0);
const MAX_CONSECUTIVE_ERRORS: usize = 3;

pub fn init_global_bot(bot: TelegramBot) {
    let _ = GLOBAL_BOT.set(bot);
}

async fn handle_critical_error(msg: &str) {
    let count = CONSECUTIVE_ERRORS.fetch_add(1, Ordering::SeqCst) + 1;
    log::error!(
        "🚨 CRITICAL API REJECTION ({}/{}): {}",
        count,
        MAX_CONSECUTIVE_ERRORS,
        msg
    );

    if let Some(b) = GLOBAL_BOT.get() {
        let err_msg = format!(
            "⚠️ *ALERTA CRÍTICA ({}/{}):*\n\n_{}_",
            count, MAX_CONSECUTIVE_ERRORS, msg
        );
        b.send_message(&err_msg).await;
    }

    if count >= MAX_CONSECUTIVE_ERRORS {
        // SAFETY: Only kill the process if KILL_SWITCH_ENABLED=true is explicitly set.
        // Default is false because killing with open positions causes unmanaged losses.
        let kill_enabled = std::env::var("KILL_SWITCH_ENABLED")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(false);

        if kill_enabled {
            if let Some(b) = GLOBAL_BOT.get() {
                let kill_msg = "💀 *KILL SWITCH ACTIVADO* 💀\n\nEl bot se ha detenido automáticamente tras 3 fallos críticos consecutivos para evitar pérdidas catastróficas fatales.\n\n⚠️ *Causa probable:* Bloqueo de IP (Error 403 / Geoblock) o problema de credenciales.";
                b.send_message(kill_msg).await;
            }
            log::error!("💀 KILL SWITCH ACTIVATED 💀 3 consecutive critical API errors. Terminating bot to prevent catastrophic losses.");
            std::process::exit(1);
        } else {
            log::error!(
                "🚨 {} consecutive critical errors — KILL SWITCH is DISABLED (set KILL_SWITCH_ENABLED=true to enable). Bot will keep retrying to protect open positions.",
                count
            );
            if let Some(b) = GLOBAL_BOT.get() {
                b.send_message(&format!(
                    "🚨 *{} ERRORES CRÍTICOS CONSECUTIVOS*\nEl bot sigue activo para proteger posiciones abiertas.\nRevisa la conexión o credenciales urgentemente.",
                    count
                )).await;
            }
        }
    }
}

fn reset_critical_error() {
    CONSECUTIVE_ERRORS.store(0, Ordering::SeqCst);
}

fn paper_balances() -> &'static Mutex<std::collections::HashMap<String, f64>> {
    PAPER_BALANCES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn paper_balance_get(token_id: &str) -> f64 {
    paper_balances()
        .lock()
        .ok()
        .and_then(|balances| balances.get(token_id).copied())
        .unwrap_or(0.0)
}

fn paper_balance_set(token_id: &str, balance: f64) {
    if let Ok(mut balances) = paper_balances().lock() {
        balances.insert(token_id.to_string(), balance.max(0.0));
    }
}

fn paper_balance_add(token_id: &str, delta: f64) {
    let new_balance = paper_balance_get(token_id) + delta;
    paper_balance_set(token_id, new_balance);
}

fn paper_balance_sub(token_id: &str, delta: f64) {
    let new_balance = (paper_balance_get(token_id) - delta).max(0.0);
    paper_balance_set(token_id, new_balance);
}

#[cfg(test)]
pub fn seed_paper_balance(token_id: &str, balance: f64) {
    paper_balance_set(token_id, balance);
}

fn is_geoblock_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains("trading restricted in your region")
        || lower.contains("geoblock")
        || (lower.contains("status_code=403") && lower.contains("polymarket"))
}

fn is_price_logic_error(msg: &str) -> bool {
    msg.to_lowercase().contains("crosses the book")
}

#[derive(Deserialize, Debug)]
struct BinanceServerTime {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Market {
    pub id: String,
    pub question: String,
    pub slug: Option<String>,
    #[serde(rename = "endDate")]
    pub end_date: Option<String>,
    #[serde(rename = "startDate")]
    pub start_date: Option<String>,
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<String>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>, // Usually "[...]"
    pub closed: Option<bool>,
    pub active: Option<bool>,
}

fn parse_price_to_beat_from_html(html: &str, slug: &str) -> Option<f64> {
    let visible_re = Regex::new(r#"Price to Beat\\?" of \$([0-9,]+(?:\.[0-9]+)?)"#).ok()?;
    if let Some(caps) = visible_re.captures(html) {
        let raw = caps.get(1)?.as_str().replace(',', "");
        if let Ok(value) = raw.parse::<f64>() {
            return Some(value);
        }
    }

    if let Some(value) = parse_price_to_beat_from_next_data(html, slug) {
        return Some(value);
    }

    let escaped_slug = regex::escape(slug);
    let scoped_patterns = [
        format!(
            r#"(?s)"slug":"{}".{{0,4000}}?"priceToBeat":([0-9]+(?:\.[0-9]+)?)"#,
            escaped_slug
        ),
        format!(
            r#"(?s)"priceToBeat":([0-9]+(?:\.[0-9]+)?).{{0,4000}}?"slug":"{}""#,
            escaped_slug
        ),
    ];

    for pattern in scoped_patterns {
        let scoped_re = Regex::new(&pattern).ok()?;
        if let Some(caps) = scoped_re.captures(html) {
            if let Some(value) = caps.get(1).and_then(|m| m.as_str().parse::<f64>().ok()) {
                return Some(value);
            }
        }
    }

    // Last-resort fallback: if the page only exposes one eventMetadata block,
    // use the first priceToBeat found anywhere in the HTML.
    let generic_re = Regex::new(r#""priceToBeat":([0-9]+(?:\.[0-9]+)?)"#).ok()?;
    generic_re
        .captures(html)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<f64>().ok())
}

fn parse_open_price_from_html(html: &str, slug: &str) -> Option<f64> {
    let escaped_slug = regex::escape(slug);
    let patterns = [
        format!(
            r#"(?s)"slug":"{}".{{0,4000}}?"openPrice":([0-9]+(?:\.[0-9]+)?)"#,
            escaped_slug
        ),
        format!(
            r#"(?s)"openPrice":([0-9]+(?:\.[0-9]+)?).{{0,4000}}?"slug":"{}""#,
            escaped_slug
        ),
        r#""openPrice":([0-9]+(?:\.[0-9]+)?)"#.to_string(),
    ];

    for pattern in patterns {
        let re = Regex::new(&pattern).ok()?;
        if let Some(value) = re
            .captures(html)
            .and_then(|caps| caps.get(1))
            .and_then(|m| m.as_str().parse::<f64>().ok())
        {
            return Some(value);
        }
    }

    None
}

fn parse_price_to_beat_from_next_data(html: &str, slug: &str) -> Option<f64> {
    let start_tag =
        r#"<script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">"#;
    let start = html.find(start_tag)? + start_tag.len();
    let rest = &html[start..];
    let end = rest.find("</script>")?;
    let next_data = &rest[..end];
    let escaped_slug = regex::escape(slug);
    let scoped_patterns = [
        format!(
            r#"(?s)"slug":"{}".{{0,4000}}?"priceToBeat":([0-9]+(?:\.[0-9]+)?)"#,
            escaped_slug
        ),
        format!(
            r#"(?s)"priceToBeat":([0-9]+(?:\.[0-9]+)?).{{0,4000}}?"slug":"{}""#,
            escaped_slug
        ),
        format!(
            r#"(?s)"slug":"{}".{{0,4000}}?"openPrice":([0-9]+(?:\.[0-9]+)?)"#,
            escaped_slug
        ),
        format!(
            r#"(?s)"openPrice":([0-9]+(?:\.[0-9]+)?).{{0,4000}}?"slug":"{}""#,
            escaped_slug
        ),
    ];

    for pattern in scoped_patterns {
        let scoped_re = Regex::new(&pattern).ok()?;
        if let Some(caps) = scoped_re.captures(next_data) {
            if let Some(value) = caps.get(1).and_then(|m| m.as_str().parse::<f64>().ok()) {
                return Some(value);
            }
        }
    }

    None
}

pub async fn fetch_polymarket_price_to_beat(
    client: &Client,
    slug: &str,
) -> Result<f64, Box<dyn Error>> {
    let url = format!("https://polymarket.com/event/{}", slug);
    let html = client
        .get(&url)
        .header(
            reqwest::header::USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        )
        .send()
        .await?
        .text()
        .await?;

    parse_price_to_beat_from_html(&html, slug)
        .or_else(|| parse_open_price_from_html(&html, slug))
        .ok_or_else(|| {
            format!(
                "Could not parse Price to Beat from event page for slug {}",
                slug
            )
            .into()
        })
}

#[derive(Deserialize, Debug)]
struct OrderbookResponse {
    asks: Option<Vec<PriceLevel>>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum PriceLevel {
    List(Vec<String>),
    Dict {
        price: String,
        #[allow(dead_code)]
        size: String,
    },
}

use regex::Regex;

async fn send_with_retry(builder: RequestBuilder) -> Result<Response, Box<dyn Error>> {
    let mut last_error = None;
    let mut delay = Duration::from_secs(1); // Start with 1 second

    for i in 0..5 {
        // 5 attempts instead of 3
        match builder
            .try_clone()
            .ok_or("Cannot clone request")?
            .send()
            .await
        {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                warn!(
                    "Request attempt {} failed: {}. Retrying in {:?}...",
                    i + 1,
                    e,
                    delay
                );
                last_error = Some(e);
                sleep(delay).await;
                delay *= 2; // Exponential: 1s, 2s, 4s, 8s, 16s
            }
        }
    }
    Err(last_error.unwrap().into())
}

pub async fn get_active_markets(
    client: &Client,
    now: DateTime<FixedOffset>,
) -> Result<Vec<Market>, Box<dyn Error>> {
    let url = format!("{}/markets", GAMMA_URL);
    let tag_id = std::env::var("TAG_ID").unwrap_or("102467".to_string());

    let mut params = vec![
        ("closed", "false"),
        ("limit", "500"),
        ("order", "volume24hr"),
        ("ascending", "false"),
    ];

    if tag_id != "ALL" {
        params.push(("tag_id", &tag_id));
    }

    let builder = client.get(&url).query(&params);
    let resp = send_with_retry(builder).await?;

    if !resp.status().is_success() {
        return Err(format!("API Error: {}", resp.status()).into());
    }

    let markets: Vec<Market> = resp.json().await?;
    let fetched_total = markets.len();
    debug!("Total markets fetched from Gamma: {}", fetched_total);

    let re = Regex::new(r"(?i)(\d{1,2}:\d{2})\s*(AM|PM)?").unwrap();
    debug!("Scanning with TAG_ID={} at {}", tag_id, now);

    let filtered: Vec<Market> = markets
        .into_iter()
        .filter(|m| {
            let is_crypto = is_crypto_15m(&m.question, &re);
            let is_near = is_market_timing_near(&m.question, &re, now);

            if !is_crypto {
                return false;
            }

            // DIAGNÓSTICO: Ver qué mercados pasan el filtro de crypto pero fallan el de tiempo
            if !is_near {
                debug!("Market {} IS CRYPTO 15m but NOT NEAR timing.", m.question);
                return false;
            }

            info!("💎 Market Linked: {}", m.question);
            true
        })
        .collect();

    info!(
        "[GAMMA] fetched_markets={} | near_window_markets={} | et_now={}",
        fetched_total,
        filtered.len(),
        now.format("%Y-%m-%d %H:%M:%S")
    );

    Ok(filtered)
}

pub fn utc_to_new_york_time(dt: DateTime<Utc>) -> DateTime<FixedOffset> {
    dt.with_timezone(&New_York).fixed_offset()
}

fn is_market_timing_near(title: &str, re: &Regex, now: DateTime<FixedOffset>) -> bool {
    let date_re =
        Regex::new(r"(?i)(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})")
            .unwrap();

    let day_offset = if let Some(date_cap) = date_re.captures(title) {
        let month_str = &date_cap[1].to_lowercase()[..3];
        let day: u32 = date_cap[2].parse().unwrap_or(0);

        let month_num = match month_str {
            "jan" => 1,
            "feb" => 2,
            "mar" => 3,
            "apr" => 4,
            "may" => 5,
            "jun" => 6,
            "jul" => 7,
            "aug" => 8,
            "sep" => 9,
            "oct" => 10,
            "nov" => 11,
            "dec" => 12,
            _ => 0,
        };

        let check_date = |offset: i64| -> bool {
            let d = now + chrono::Duration::days(offset);
            d.month() == month_num && d.day() == day
        };

        if check_date(0) {
            0
        } else if check_date(1) {
            1
        } else if check_date(-1) {
            -1
        } else {
            return false;
        }
    } else {
        return false;
    };

    let caps: Vec<_> = re.captures_iter(title).collect();
    if caps.len() < 2 {
        return false;
    }

    let to_total_mins = |c: &regex::Captures| -> Option<i32> {
        let time_str = c.get(1)?.as_str();
        let amp_str = c.get(2).map(|m| m.as_str().to_uppercase());
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

    let start_mins = match to_total_mins(&caps[caps.len() - 2]) {
        Some(v) => v,
        None => return false,
    };
    let end_mins = match to_total_mins(&caps[caps.len() - 1]) {
        Some(v) => v,
        None => return false,
    };
    let current_mins = (now.hour() * 60 + now.minute()) as i32;

    let day_shift = day_offset * 1440;
    let start_abs = start_mins + day_shift;
    let mut end_abs = end_mins + day_shift;
    if end_abs <= start_abs {
        end_abs += 1440;
    }

    let is_near = (current_mins >= start_abs && current_mins < end_abs)
        || (current_mins >= start_abs - 15 && current_mins < start_abs)
        || (current_mins >= start_abs && current_mins < start_abs + 15);

    if !is_near {
        debug!(
            "Timing mismatch for {}: current={} vs window=[{}-{}]",
            title, current_mins, start_abs, end_abs
        );
    }

    is_near
}

fn is_crypto_15m(title: &str, re: &Regex) -> bool {
    let t = title.to_lowercase();
    let asset_filters: Vec<String> = std::env::var("ASSET_FILTER")
        .ok()
        .map(|v| {
            v.split(',')
                .map(|part| part.trim().to_ascii_uppercase())
                .filter(|part| !part.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // 1. ALLOW BTC, ETH, XRP, OR SOL
    let is_btc = t.contains("btc") || t.contains("bitcoin");
    let is_eth = t.contains("eth") || t.contains("ethereum");
    let is_xrp = t.contains("xrp") || t.contains("ripple");
    let is_sol = t.contains("sol") || t.contains("solana");

    if !is_btc && !is_eth && !is_xrp && !is_sol {
        return false;
    }

    if !asset_filters.is_empty() {
        let matches_filter = asset_filters.iter().any(|filter| match filter.as_str() {
            "BTC" | "BTCUSDT" | "BITCOIN" => is_btc,
            "ETH" | "ETHUSDT" | "ETHEREUM" => is_eth,
            "XRP" | "XRPUSDT" | "RIPPLE" => is_xrp,
            "SOL" | "SOLUSDT" | "SOLANA" => is_sol,
            _ => false,
        });

        if !matches_filter {
            return false;
        }
    }

    // 2. CHECK FOR 15m MARKERS (Ultra-Flexible)
    let has_15m_tag = t.contains("15m") || t.contains("15 min") || t.contains("15-min");

    // Si tiene el tag de 15m explícito, lo aceptamos
    if has_15m_tag {
        debug!("Market matched via 15m tag: {}", title);
        return true;
    }

    // Si no tiene el tag, probamos con el regex de las dos horas
    let caps: Vec<_> = re.captures_iter(title).collect();
    if caps.len() >= 2 {
        let to_mixed_minutes = |c: &regex::Captures| -> Option<i32> {
            let time_str = c.get(1)?.as_str();
            let amp_str = c.get(2).map(|m| m.as_str().to_uppercase());
            let parts: Vec<&str> = time_str.split(':').collect();
            if parts.len() != 2 {
                return None;
            }
            let mut h: i32 = parts[0].parse().ok()?;
            let m: i32 = parts[1].parse().ok()?;
            if let Some(amp) = amp_str {
                if amp == "PM" && h != 12 {
                    h += 12;
                }
                if amp == "AM" && h == 12 {
                    h = 0;
                }
            }
            Some(h * 60 + m)
        };

        let t1 = to_mixed_minutes(&caps[caps.len() - 2]);
        let t2 = to_mixed_minutes(&caps[caps.len() - 1]);

        if let (Some(v1), Some(v2)) = (t1, t2) {
            let diff = (v2 - v1).rem_euclid(1440);
            return diff >= 14 && diff <= 16;
        }
    }

    false
}

pub async fn get_clob_full_prices(
    client: &Client,
    yes_token: &str,
    no_token: &str,
) -> ((Option<f64>, Option<f64>), (Option<f64>, Option<f64>)) {
    let yes_fut = get_orderbook_depth(client, yes_token);
    let no_fut = get_orderbook_depth(client, no_token);
    let (yes_m, no_m) = futures::future::join(yes_fut, no_fut).await;

    let asks = (yes_m.best_ask, no_m.best_ask);
    let bids = (yes_m.best_bid, no_m.best_bid);
    (asks, bids)
}

pub async fn get_market_prices_clob(
    client: &Client,
    yes_token: &str,
    no_token: &str,
) -> (Option<f64>, Option<f64>) {
    // Legacy support: returns bids
    let (_, bids) = get_clob_full_prices(client, yes_token, no_token).await;
    bids
}

pub async fn get_market_prices(client: &Client, market_id: &str) -> (Option<f64>, Option<f64>) {
    let url = format!("{}/markets/{}", GAMMA_URL, market_id);
    let builder = client.get(&url);
    match send_with_retry(builder).await {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(m) = resp.json::<Market>().await {
                    if let Some(prices_str) = m.outcome_prices {
                        debug!("Raw prices for {}: {}", market_id, prices_str);
                        if let Ok(prices) = serde_json::from_str::<Vec<String>>(&prices_str) {
                            let yes = prices.get(0).and_then(|p| p.parse::<f64>().ok());
                            let no = prices.get(1).and_then(|p| p.parse::<f64>().ok());
                            return (yes, no);
                        } else if let Ok(prices) = serde_json::from_str::<Vec<f64>>(&prices_str) {
                            let yes = prices.get(0).copied();
                            let no = prices.get(1).copied();
                            return (yes, no);
                        }
                    }
                }
            }
        }
        Err(e) => error!("Failed to fetch market {} after retries: {}", market_id, e),
    }
    (None, None)
}

pub async fn get_best_ask(client: &Client, _market_id: &str, token_id: &str) -> Option<f64> {
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
    fetch_orderbook(client, &url).await
}

pub async fn get_best_bid(client: &Client, token_id: &str) -> Option<f64> {
    let metrics = get_orderbook_depth(client, token_id).await;
    metrics.best_bid
}

async fn fetch_orderbook(client: &Client, url: &str) -> Option<f64> {
    let builder = client.get(url);
    match send_with_retry(builder).await {
        Ok(resp) => {
            if resp.status().is_success() {
                match resp.json::<OrderbookResponse>().await {
                    Ok(data) => {
                        if let Some(asks) = data.asks {
                            let mut min_price = 100.0;
                            let mut found = false;

                            for level in asks {
                                let price_str = match level {
                                    PriceLevel::List(v) => v.get(0).cloned(),
                                    PriceLevel::Dict { price, .. } => Some(price),
                                };

                                if let Some(s) = price_str {
                                    if let Ok(p) = s.parse::<f64>() {
                                        if p < min_price {
                                            min_price = p;
                                            found = true;
                                        }
                                    }
                                }
                            }
                            if found {
                                return Some(min_price);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse Orderbook JSON for {}: {}", url, e);
                    }
                }
            }
        }
        Err(e) => debug!("Orderbook error for {} after retries: {}", url, e),
    }
    None
}

/// Metrics from orderbook analysis for liquidity prediction
#[derive(Debug, Clone)]
pub struct OrderbookMetrics {
    pub best_ask: Option<f64>,
    pub best_bid: Option<f64>,
    pub total_ask_volume: f64,
    pub depth_near_best: f64, // Volume within 5 cents of best price
    pub spread: f64,
    pub liquidity_score: f64,        // 0.0 to 1.0, higher = more liquid
    pub bids_depth: Vec<(f64, f64)>, // Price/Size pairs for top 5 levels
    pub asks_depth: Vec<(f64, f64)>, // Price/Size pairs for top 5 levels
}

impl Default for OrderbookMetrics {
    fn default() -> Self {
        Self {
            best_ask: None,
            best_bid: None,
            total_ask_volume: 0.0,
            depth_near_best: 0.0,
            spread: 1.0,
            liquidity_score: 0.0,
            bids_depth: Vec::new(),
            asks_depth: Vec::new(),
        }
    }
}

/// Full orderbook response with size information
#[derive(Deserialize, Debug)]
struct FullOrderbookResponse {
    asks: Option<Vec<OrderLevel>>,
    bids: Option<Vec<OrderLevel>>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum OrderLevel {
    List(Vec<String>), // [price, size]
    Dict { price: String, size: String },
}

/// Fetch full orderbook and calculate liquidity metrics
pub async fn get_orderbook_depth(client: &Client, token_id: &str) -> OrderbookMetrics {
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
    let builder = client.get(&url);

    match send_with_retry(builder).await {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(data) = resp.json::<FullOrderbookResponse>().await {
                    return calculate_metrics(data);
                }
            }
        }
        Err(e) => debug!("Orderbook depth error for {}: {}", token_id, e),
    }

    OrderbookMetrics::default()
}

fn calculate_metrics(data: FullOrderbookResponse) -> OrderbookMetrics {
    let mut metrics = OrderbookMetrics::default();

    // Process asks
    let mut best_ask = 100.0;
    let mut total_volume = 0.0;
    let mut near_volume = 0.0;

    if let Some(ref asks) = data.asks {
        let mut sorted_asks = Vec::new();
        for level in asks {
            let (price, size) = match level {
                OrderLevel::List(v) => {
                    let p = v
                        .get(0)
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(100.0);
                    let s = v.get(1).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    (p, s)
                }
                OrderLevel::Dict { price, size } => {
                    let p = price.parse::<f64>().unwrap_or(100.0);
                    let s = size.parse::<f64>().unwrap_or(0.0);
                    (p, s)
                }
            };

            if price < best_ask {
                best_ask = price;
            }
            total_volume += size;
            sorted_asks.push((price, size));
        }

        // Sort by price ascending
        sorted_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        metrics.asks_depth = sorted_asks.iter().take(5).cloned().collect();

        // Calculate near-best volume (within 5 cents)
        if best_ask < 100.0 {
            for (price, size) in &sorted_asks {
                if *price <= best_ask + 0.05 {
                    near_volume += size;
                }
            }
        }
    }

    // Process bids
    let mut best_bid = 0.0;
    if let Some(ref bids) = data.bids {
        let mut sorted_bids = Vec::new();
        for level in bids {
            let (price, size) = match level {
                OrderLevel::List(v) => {
                    let p = v.get(0).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    let s = v.get(1).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    (p, s)
                }
                OrderLevel::Dict { price, size } => {
                    let p = price.parse::<f64>().unwrap_or(0.0);
                    let s = size.parse::<f64>().unwrap_or(0.0);
                    (p, s)
                }
            };
            if price > best_bid {
                best_bid = price;
            }
            sorted_bids.push((price, size));
        }

        // Sort by price descending
        sorted_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        metrics.bids_depth = sorted_bids.iter().take(5).cloned().collect();
    }

    // Populate metrics
    if best_ask < 100.0 {
        metrics.best_ask = Some(best_ask);
    }
    if best_bid > 0.0 {
        metrics.best_bid = Some(best_bid);
    }
    metrics.total_ask_volume = total_volume;
    metrics.depth_near_best = near_volume;
    metrics.spread = if best_ask < 100.0 && best_bid > 0.0 {
        best_ask - best_bid
    } else {
        1.0
    };

    // OPTIMIZED: Calculate liquidity score
    let volume_score = if total_volume > 0.0 {
        (total_volume / 500.0).min(1.0).powf(0.7)
    } else {
        0.0
    };

    let depth_score = if near_volume > 0.0 {
        (near_volume / 200.0).min(1.0).powf(0.5)
    } else {
        0.0
    };

    let spread_score = (1.0 - metrics.spread * 20.0).max(0.0);

    metrics.liquidity_score =
        (depth_score * 0.50 + volume_score * 0.35 + spread_score * 0.15).min(1.0);

    metrics
}

/// Get current time from Binance server (more reliable than local time)
/// Falls back to local time if Binance API fails
pub async fn get_binance_time(client: &Client) -> DateTime<Utc> {
    let url = "https://api.binance.us/api/v3/time";

    // Simple GET without retry to avoid delays
    match client.get(url).send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(time_data) = resp.json::<BinanceServerTime>().await {
                    let secs = time_data.server_time / 1000;
                    let nanos = ((time_data.server_time % 1000) * 1_000_000) as u32;

                    if let Some(dt) = DateTime::from_timestamp(secs, nanos) {
                        return dt;
                    }
                }
            }
        }
        Err(_) => {} // Silent fallback to local time
    }

    // Fallback to UTC clock if Binance time is unavailable.
    Utc::now()
}

// ========================================================
//  LIVE TRADING BRIDGE HELPERS
// ========================================================

/// Returns true if PAPER_TRADING=false (i.e., live mode is enabled)
fn is_live_mode() -> bool {
    std::env::var("PAPER_TRADING")
        .unwrap_or_else(|_| "true".to_string())
        .to_lowercase()
        == "false"
}

/// Path to the Python executor script
fn executor_path() -> String {
    std::env::var("LIVE_EXECUTOR_PATH").unwrap_or_else(|_| "clob_executor.py".to_string())
}

/// Response from the Python executor
#[derive(Clone, Debug)]
pub struct ExecutorResponse {
    pub order_id: String,
    pub shares: f64,
    pub fill_price: Option<f64>,
    pub reliable: bool, // New: For metadata about balance/order info
    pub attempts: u32,  // New: Track execution attempts
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderSubmitErrorKind {
    UnknownAfterTimeout,
    Rejected,
    Transport,
}

pub fn classify_order_submit_error(message: &str) -> OrderSubmitErrorKind {
    let lower = message.to_ascii_lowercase();
    if lower.contains("timed out") || lower.contains("timeout") {
        OrderSubmitErrorKind::UnknownAfterTimeout
    } else if lower.contains("rejected")
        || lower.contains("zero-fill")
        || lower.contains("zero filled")
        || lower.contains("not enough")
    {
        OrderSubmitErrorKind::Rejected
    } else {
        OrderSubmitErrorKind::Transport
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnknownOrderReconcileResult {
    pub found: bool,
    pub filled_size: f64,
    pub order_id: Option<String>,
}

pub async fn reconcile_unknown_order(
    client_order_id: &str,
) -> Result<UnknownOrderReconcileResult, Box<dyn Error>> {
    if client_order_id.trim().is_empty() || !is_live_mode() {
        return Ok(UnknownOrderReconcileResult {
            found: false,
            filled_size: 0.0,
            order_id: None,
        });
    }

    let resp = call_executor(&["find_order", client_order_id]).await?;
    Ok(UnknownOrderReconcileResult {
        found: resp.shares > 0.0 || resp.order_id != "unknown",
        filled_size: resp.shares,
        order_id: Some(resp.order_id),
    })
}

/// Run clob_executor.py asynchronously and parse JSON response.
/// Returns the ExecutorResponse on success, or an error message.
async fn call_executor(args: &[&str]) -> Result<ExecutorResponse, Box<dyn Error>> {
    let exec = executor_path();

    let python_cmd = if cfg!(windows) { "python" } else { "python3" };

    // P0 FIX: 20-second hard timeout prevents sell orders blocking indefinitely
    // while the market moves against open positions.
    let cmd_result = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::process::Command::new(python_cmd)
            .arg(&exec)
            .args(args)
            .output(),
    )
    .await;

    let output = match cmd_result {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            let msg = format!("Failed to launch Python executor ({}): {}", python_cmd, e);
            handle_critical_error(&msg).await;
            return Err(msg.into());
        }
        Err(_) => {
            let cmd_name = args.first().copied().unwrap_or("unknown");
            let msg = format!(
                "Python executor TIMED OUT after 20s | cmd={} — position may be unmanaged",
                cmd_name
            );
            error!("{}", msg);
            handle_critical_error(&msg).await;
            return Err(msg.into());
        }
    };

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stderr.is_empty() {
        log::warn!("🐍 Python stderr: {}", stderr.trim());
    }

    let stdout_str = stdout.trim();

    // Attempt to find the JSON block in case there are library warnings in stdout
    let json_start = stdout_str.find('{');
    let json_end = stdout_str.rfind('}');

    let json_to_parse = match (json_start, json_end) {
        (Some(start), Some(end)) if end >= start => &stdout_str[start..=end],
        _ => stdout_str,
    };

    let parsed_result: Result<serde_json::Value, _> = serde_json::from_str(json_to_parse);

    let parsed = match parsed_result {
        Ok(v) => v,
        Err(e) => {
            let msg = format!(
                "Could not parse executor JSON response: {} | raw: {}",
                e, stdout_str
            );
            handle_critical_error(&msg).await;
            return Err(msg.into());
        }
    };

    if parsed["status"].as_str() == Some("ok") {
        reset_critical_error();

        let order_id = parsed["order_id"].as_str().unwrap_or("unknown").to_string();

        // Extract shares: Prioritize actual 'filled_size' over 'shares_ordered'
        let mut shares = parsed["filled_size"].as_f64().unwrap_or(0.0);
        if shares == 0.0 {
            shares = parsed["shares_ordered"].as_f64().unwrap_or(0.0);
        }
        if shares == 0.0 {
            shares = parsed["shares_sold"].as_f64().unwrap_or(0.0);
        }
        if shares == 0.0 {
            shares = parsed["balance"].as_f64().unwrap_or(0.0);
        }
        if shares == 0.0 {
            shares = parsed["actual_balance"].as_f64().unwrap_or(0.0);
        }

        // P0 FIX: Detect silent zero-fill on sell commands.
        // status:"ok" with shares=0 means the exchange accepted the order
        // but filled nothing. Position is still open — treat as failure.
        let is_sell_cmd = args
            .first()
            .map(|a| *a == "sell" || *a == "sell_fak")
            .unwrap_or(false);
        if is_sell_cmd && shares == 0.0 {
            error!(
                "ZERO-FILL SELL DETECTED: status:ok but 0 shares confirmed | order_id={} | POSITION MAY STILL BE OPEN",
                order_id
            );
            return Err(format!(
                "Sell zero-fill: status:ok but no shares sold (order_id={}). Position still open.",
                order_id
            )
            .into());
        }

        let fill_price = parsed["average_fill_price"].as_f64();
        // P0 FIX: Default reliable=false. Only trust balance data the daemon explicitly marks reliable.
        let reliable = parsed["reliable"].as_bool().unwrap_or(false);
        let attempts = parsed["attempts"].as_u64().unwrap_or(1) as u32;

        info!(
            "EXECUTOR OK | cmd={} | order_id={} | shares={:.6} | fill_price={:?} | reliable={}",
            args.first().copied().unwrap_or("?"),
            order_id,
            shares,
            fill_price,
            reliable
        );

        Ok(ExecutorResponse {
            order_id,
            shares,
            fill_price,
            reliable,
            attempts,
        })
    } else if parsed["status"].as_str() == Some("error_critical") {
        let msg = parsed["message"]
            .as_str()
            .unwrap_or("unknown critical network error");

        // --- GUARD: Don't kill switch for transient market states like "orderbook does not exist" ---
        if msg.to_lowercase().contains("orderbook") {
            reset_critical_error(); // Success in communication, just a market state error
            return Err(format!("Market Error: {}", msg).into());
        }

        if is_geoblock_error(msg) {
            reset_critical_error();
            warn!("Live trading blocked by regional restriction: {}", msg);
            return Err(format!("Live trading blocked by regional restriction: {}", msg).into());
        }

        handle_critical_error(msg).await;
        Err(format!("Critical Executor error: {}", msg).into())
    } else if parsed["status"].as_str() == Some("error_balance") {
        reset_critical_error(); // Balance errors are logic/market errors, not network critical
        let msg = parsed["message"]
            .as_str()
            .unwrap_or("not enough balance or allowance");
        Err(format!("Executor balance error: {}", msg).into())
    } else if parsed["status"].as_str() == Some("error_price") {
        reset_critical_error();
        let msg = parsed["message"]
            .as_str()
            .unwrap_or("invalid price relative to orderbook");
        Err(format!("Executor price error: {}", msg).into())
    } else {
        // Normal error (like a logic error, balance limit, etc. non-network-critical)
        // reset_critical_error() because the API successfully replied with structured JSON rejection
        reset_critical_error();

        let msg = parsed["message"].as_str().unwrap_or("unknown error");
        if is_price_logic_error(msg) {
            return Err(format!("Executor price error: {}", msg).into());
        }
        Err(format!("Executor error: {}", msg).into())
    }
}

/// Run clob_executor.py asking for balance of a token_id and parse JSON response.
/// Fetch details for a specific order by ID.
pub async fn get_order_details(order_id: &str) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        if order_id == "unknown" || order_id.is_empty() {
            return Err("Invalid order ID".into());
        }
        call_executor(&["get_order", order_id]).await
    } else {
        // Simulated response for paper trading
        Ok(ExecutorResponse {
            order_id: order_id.to_string(),
            shares: 0.0,
            fill_price: None,
            reliable: true,
            attempts: 1,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BalanceMeta {
    pub balance: f64,
    pub reliable: bool,
    pub attempts: u32,
}

/// Query real available Conditional Token balance with reliability metadata
pub async fn get_actual_balance_with_meta(token_id: &str) -> Result<BalanceMeta, Box<dyn Error>> {
    if is_live_mode() {
        // Python enriched 'outcome_balance' returns actual_balance, reliable, attempts
        let resp = call_executor(&["outcome_balance", token_id]).await?;
        Ok(BalanceMeta {
            balance: resp.shares, // call_executor maps 'balance' to 'shares' for simplicity
            reliable: resp.reliable,
            attempts: resp.attempts,
        })
    } else {
        Ok(BalanceMeta {
            balance: paper_balance_get(token_id),
            reliable: true,
            attempts: 1,
        })
    }
}

pub async fn get_actual_balance(token_id: &str) -> Result<f64, Box<dyn Error>> {
    let meta = get_actual_balance_with_meta(token_id).await?;
    Ok(meta.balance)
}

/// Normalize order status from exchange SDK for Rust logic.
/// Returns: OPEN, FILLED, CANCELED, EXPIRED, UNKNOWN
pub async fn get_order_status(order_id: &str) -> Result<String, Box<dyn Error>> {
    if order_id.is_empty() || order_id == "unknown" || !is_live_mode() {
        return Ok("UNKNOWN".to_string());
    }

    match call_executor(&["get_order", order_id]).await {
        Ok(resp) => {
            // We use the Python bridge's 'status' mapping if available, or fallback to the status-in-id trick
            Ok(resp.order_id.to_uppercase())
        }
        Err(e) => {
            log::warn!("Order {} lookup failed: {}", order_id, e);
            Ok("UNKNOWN".to_string())
        }
    }
}

#[cfg(test)]
mod unknown_order_tests {
    use super::*;

    #[test]
    fn timeout_error_marks_order_status_unknown() {
        let result = classify_order_submit_error("Python executor TIMED OUT after 20s");

        assert_eq!(result, OrderSubmitErrorKind::UnknownAfterTimeout);
    }
}

/// Place a protective LIMIT SELL order (Stop Loss) after entry.
/// PAPER TRADING: simulates the order and returns a fake ID.
/// LIVE: calls clob_executor.py sell <token_id> <shares> <limit_price>
///
/// CRITICAL: `shares_to_sell` must be the ACTUAL shares held (from resp.shares after buying),
/// NOT a computed value from position_size/sl_price. Over-selling causes 'not enough balance'.
pub async fn place_protective_limit_sell(
    _client: &reqwest::Client,
    token_id: &str,
    shares_to_sell: f64,
    sl_price: f64,
    _market_id: &str,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        // Use actual shares held - 6 decimals precision
        let shares = (shares_to_sell * 1_000_000.0).floor() / 1_000_000.0;
        let shares_str = format!("{:.6}", shares);
        let price_str = format!("{:.4}", sl_price);

        log::info!(
            "🔴 LIVE PROTECTIVE SELL ORDER | Token: {} | Shares: {} | SL Price: {}",
            token_id,
            shares_str,
            price_str
        );

        let resp = call_executor(&["sell", token_id, &shares_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "📋 PROTECTIVE ORDER PLACED (SIMULATED) | Token: {} | Shares: {:.4} | Limit: {:.4}",
            token_id,
            shares_to_sell,
            sl_price
        );
        Ok(ExecutorResponse {
            order_id: "SIMULATED_PROT_ID".to_string(),
            shares: shares_to_sell,
            fill_price: Some(sl_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Place a protective LIMIT SELL order at a fixed floor price (no "market" slippage allowed).
/// Used for Take Profit to ensure we don't sell winners at a loss.
pub async fn place_floor_sell(
    _client: &reqwest::Client,
    token_id: &str,
    shares_to_sell: f64,
    floor_price: f64,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        let shares = (shares_to_sell * 1_000_000.0).floor() / 1_000_000.0;
        let shares_str = format!("{:.6}", shares);
        let price_str = format!("{:.4}", floor_price);

        log::info!(
            "🛡️ LIVE FLOOR SELL | Token: {} | Shares: {} | Floor: {}",
            token_id,
            shares_str,
            price_str
        );

        let resp = call_executor(&["sell", token_id, &shares_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "🛡️ FLOOR SELL PLACED (SIMULATED) | Token: {} | Shares: {:.4} | Price: {:.4}",
            token_id,
            shares_to_sell,
            floor_price
        );
        paper_balance_sub(token_id, shares_to_sell);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_FLOOR_ID".to_string(),
            shares: shares_to_sell,
            fill_price: Some(floor_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Place an aggressive FAK sell that tries to execute immediately at the current book.
/// Used by HARD_SL staged exits to favor execution while preserving a hard minimum floor.
pub async fn place_fak_sell(
    _client: &reqwest::Client,
    token_id: &str,
    shares_to_sell: f64,
    limit_price: f64,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        let shares = (shares_to_sell * 1_000_000.0).floor() / 1_000_000.0;
        let shares_str = format!("{:.6}", shares);
        let price_str = format!("{:.4}", limit_price);

        log::info!(
            "⚡ LIVE FAK SELL | Token: {} | Shares: {} | Limit: {}",
            token_id,
            shares_str,
            price_str
        );

        let resp = call_executor(&["sell_fak", token_id, &shares_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "⚡ FAK SELL EXECUTED (SIMULATED) | Token: {} | Shares: {:.6} | Limit: {:.4}",
            token_id,
            shares_to_sell,
            limit_price
        );
        paper_balance_sub(token_id, shares_to_sell);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_FAK_EXIT".to_string(),
            shares: shares_to_sell,
            fill_price: Some(limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Place an initial MARKET BUY order (using a Limit Buy at a slightly higher price for instant fill).
/// PAPER TRADING: simulates the order.
/// LIVE: calls clob_executor.py buy <token_id> <usdc_size> <limit_price>
pub async fn place_initial_buy(
    _client: &Client,
    token_id: &str,
    limit_price: f64,
    usdc_size: f64,
    _market_id: &str,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        const ZERO_FILL_RETRIES: u32 = 2;
        const ZERO_FILL_RETRY_DELAY_SECS: u64 = 3;

        let price_str = format!("{:.4}", limit_price);
        let size_str = format!("{:.2}", usdc_size);

        log::info!(
            "🟢 LIVE BUY | Token: {} | USDC: {} | Price: {}",
            token_id,
            size_str,
            price_str
        );

        for attempt in 1..=(ZERO_FILL_RETRIES + 1) {
            if attempt > 1 {
                log::info!(
                    "LIVE BUY RETRY | Token: {} | USDC: {} | Price: {} | Attempt: {}/{}",
                    token_id,
                    size_str,
                    price_str,
                    attempt,
                    ZERO_FILL_RETRIES + 1
                );
            }

            let mut resp = call_executor(&["buy", token_id, &size_str, &price_str]).await?;
            resp.attempts = attempt;

            if resp.shares > 0.0 {
                return Ok(resp);
            }

            if attempt <= ZERO_FILL_RETRIES {
                log::warn!(
                    "Initial buy accepted but filled 0 shares for {}. Retrying in {}s ({}/{})...",
                    token_id,
                    ZERO_FILL_RETRY_DELAY_SECS,
                    attempt,
                    ZERO_FILL_RETRIES
                );
                sleep(Duration::from_secs(ZERO_FILL_RETRY_DELAY_SECS)).await;
            } else {
                log::warn!(
                    "Initial buy accepted but filled 0 shares for {} after {} attempts. Aborting entry.",
                    token_id,
                    attempt
                );
                return Ok(resp);
            }
        }

        unreachable!("initial buy retry loop always returns")
    } else {
        log::info!(
            "📋 BUY EXECUTED (SIMULATED) | Token: {} | Size: {} | Price: {}",
            token_id,
            usdc_size,
            limit_price
        );
        let simulated_shares = usdc_size / limit_price;
        paper_balance_add(token_id, simulated_shares);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_ORDER_ID".to_string(),
            shares: simulated_shares,
            fill_price: Some(limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Place a proactive limit BUY order on the opposite side (Full Recovery Insurance).
/// PAPER TRADING: simulates the order.
/// LIVE: calls clob_executor.py buy <token_id> <usdc_size> <limit_price>
pub async fn place_recovery_limit_order(
    _client: &reqwest::Client,
    token_id: &str,
    limit_price: f64,
    position_size: f64,
    _market_id: &str,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        let size_str = format!("{:.2}", position_size);
        let price_str = format!("{:.4}", limit_price);

        log::info!(
            "🟢 LIVE BUY ORDER (Recovery) | Token: {} | Size: ${} | Limit: {}",
            token_id,
            size_str,
            price_str
        );

        let resp = call_executor(&["buy", token_id, &size_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "🛡️ RECOVERY INSURANCE PLACED (SIMULATED) | Token: {} | Limit: {:.4} | Size: ${:.2}",
            token_id,
            limit_price,
            position_size
        );
        let simulated_shares = position_size / limit_price;
        paper_balance_add(token_id, simulated_shares);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_RECOVERY_ID".to_string(),
            shares: simulated_shares,
            fill_price: Some(limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Place a DCA limit BUY order to scale in at a lower price (Safety Net).
/// PAPER TRADING: simulates the order.
/// LIVE: calls clob_executor.py buy <token_id> <usdc_size> <limit_price>
pub async fn place_dca_limit_buy(
    _client: &reqwest::Client,
    token_id: &str,
    limit_price: f64,
    position_size: f64,
    _market_id: &str,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        let size_str = format!("{:.2}", position_size);
        let price_str = format!("{:.4}", limit_price);

        log::info!(
            "🟡 LIVE DCA LIMIT BUY | Token: {} | Size: ${} | Limit: {}",
            token_id,
            size_str,
            price_str
        );

        let resp = call_executor(&["buy", token_id, &size_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "📋 DCA LIMIT BUY PLACED (SIMULATED) | Token: {} | Price: {:.4} | Size: ${:.2}",
            token_id,
            limit_price,
            position_size
        );
        let simulated_shares = position_size / limit_price;
        paper_balance_add(token_id, simulated_shares);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_DCA_ID".to_string(),
            shares: simulated_shares,
            fill_price: Some(limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Cancel a protective or recovery limit order deterministically.
/// PAPER TRADING: simulates cancellation.
/// LIVE: calls clob_executor.py cancel_and_wait <order_id>
/// Returns the final order status string (e.g. "CANCELED", "FILLED", "EXPIRED").
pub async fn cancel_protective_order(
    _client: &reqwest::Client,
    order_id: &str,
) -> Result<String, Box<dyn Error>> {
    if is_live_mode() {
        log::info!("⏳ LIVE CANCEL AND WAIT | ID: {}", order_id);
        // call_executor returns Ok(ExecutorResponse) — order_id field holds final status string
        let resp = call_executor(&["cancel_and_wait", order_id]).await?;
        let final_status = resp.order_id.to_uppercase();
        log::info!(
            "✅ Order {} final status after cancel_and_wait: {}",
            order_id,
            final_status
        );
        Ok(final_status)
    } else {
        log::info!(
            "🗑️ PROTECTIVE ORDER CANCELLED (SIMULATED) | ID: {}",
            order_id
        );
        Ok("CANCELED".to_string())
    }
}

/// Place a Deep Net proactive limit order (GTC).
/// PAPER TRADING: simulates the order.
/// LIVE: calls clob_executor.py buy <token_id> <usdc_size> <limit_price>
pub async fn place_deep_net_order(
    _client: &Client,
    token_id: &str,
    limit_price: f64,
    position_size: f64,
    _market_id: &str,
    side_label: &str,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    if is_live_mode() {
        let size_str = format!("{:.2}", position_size);
        let price_str = format!("{:.4}", limit_price);

        log::info!(
            "🕸️ LIVE DEEP NET BUY | {} | Token: {} | Size: ${} | Price: {}",
            side_label,
            token_id,
            size_str,
            price_str
        );

        let resp = call_executor(&["buy", token_id, &size_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "🛡️ DEEP NET PLACED (SIMULATED) | {} | Token: {} | Limit: {:.4} | Size: ${:.2}",
            side_label,
            token_id,
            limit_price,
            position_size
        );
        Ok(ExecutorResponse {
            order_id: "SIMULATED_DEEP_ID".to_string(),
            shares: position_size / limit_price,
            fill_price: Some(limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

/// Cancel a Deep Net order.
/// PAPER TRADING: simulates cancellation.
/// LIVE: calls clob_executor.py cancel <order_id>
pub async fn cancel_deep_net_order(_client: &Client, order_id: &str) -> Result<(), Box<dyn Error>> {
    if is_live_mode() {
        log::info!("🧹 LIVE CANCEL DEEP NET | ID: {}", order_id);
        call_executor(&["cancel", order_id]).await?;
    } else {
        log::info!("🧹 DEEP NET CANCELLED (SIMULATED) | ID: {}", order_id);
    }
    Ok(())
}

/// Fetch current price for ANY symbol from Binance (US)
pub async fn get_binance_price(client: &Client, symbol: &str) -> Option<f64> {
    #[derive(Deserialize)]
    struct TickerPrice {
        price: String,
    }

    let url = format!(
        "https://api.binance.us/api/v3/ticker/price?symbol={}",
        symbol
    );
    let builder = client.get(&url);

    match builder.send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(ticker) = resp.json::<TickerPrice>().await {
                    return ticker.price.parse::<f64>().ok();
                }
            }
        }
        Err(e) => debug!("Binance ticker error for {}: {}", symbol, e),
    }
    None
}

/// Place an immediate SELL order (Market Sell) to exit a position.
/// Calls clob_executor.py sell <token_id> <shares> 0.01
pub async fn place_market_sell(
    _client: &reqwest::Client,
    token_id: &str,
    shares: f64,
    limit_price: f64,
) -> Result<ExecutorResponse, Box<dyn Error>> {
    // P0 FIX: Previous code overrode limit_price=0.01 (nuclear sell) to HARD_SL_EXIT_FLOOR=0.47,
    // which in an illiquid market would never fill — defeating the last-resort mechanism.
    // Now: respect the caller's intent exactly, only clamp to a valid range.
    let effective_limit_price = limit_price.clamp(0.01, 0.99);
    if limit_price < 0.02 {
        warn!(
            "NUCLEAR SELL: limit_price={:.4} — selling at floor to guarantee exit",
            effective_limit_price
        );
    }

    if is_live_mode() {
        let shares_str = format!("{:.6}", shares);
        let price_str = format!("{:.4}", effective_limit_price);

        log::info!(
            "🔴 LIVE MARKET SELL | Token: {} | Shares: {} | Limit Floor: {}",
            token_id,
            shares_str,
            price_str
        );

        let resp = call_executor(&["sell", token_id, &shares_str, &price_str]).await?;
        Ok(resp)
    } else {
        log::info!(
            "📋 MARKET SELL EXECUTED (SIMULATED) | Token: {} | Shares: {:.6} | Floor: {:.4}",
            token_id,
            shares,
            limit_price
        );
        paper_balance_sub(token_id, shares);
        Ok(ExecutorResponse {
            order_id: "SIMULATED_MARKET_EXIT".to_string(),
            shares,
            fill_price: Some(effective_limit_price),
            reliable: true,
            attempts: 1,
        })
    }
}

pub fn extract_strike(text: &str) -> Option<f64> {
    let re = regex::Regex::new(r"\$([\d,]+(?:\.\d+)?)").unwrap();
    if let Some(cap) = re.captures(text) {
        let val_str = cap.get(1)?.as_str().replace(",", "");
        return val_str.parse().ok();
    }
    None
}

pub fn to_total_mins(text: &str) -> Option<i32> {
    let re_ampm = regex::Regex::new(r"(\d{1,2}):(\d{2})\s*(AM|PM)").unwrap();
    let re_24h = regex::Regex::new(r"(\d{1,2}):(\d{2})").unwrap();

    if let Some(cap) = re_ampm.captures(text) {
        let mut h: i32 = cap.get(1)?.as_str().parse().ok()?;
        let m: i32 = cap.get(2)?.as_str().parse().ok()?;
        let ampm = cap.get(3)?.as_str().to_uppercase();
        if ampm == "PM" && h < 12 {
            h += 12;
        }
        if ampm == "AM" && h == 12 {
            h = 0;
        }
        return Some(h * 60 + m);
    } else if let Some(cap) = re_24h.captures(text) {
        let h: i32 = cap.get(1)?.as_str().parse().ok()?;
        let m: i32 = cap.get(2)?.as_str().parse().ok()?;
        return Some(h * 60 + m);
    }
    None
}

pub fn extract_window_times(title: &str) -> Option<(i32, i32)> {
    // Title example: "XRP Up or Down - April 27, 3:30PM-3:45PM ET"
    let parts: Vec<&str> = title.split(',').collect();
    if parts.len() < 2 {
        return None;
    }
    let time_part = parts[1]; // " 3:30PM-3:45PM ET"
    let times: Vec<&str> = time_part.split('-').collect();
    if times.len() < 2 {
        return None;
    }

    let start = to_total_mins(times[0])?;
    let end = to_total_mins(times[1])?;
    Some((start, end))
}

pub fn to_eastern_time(dt: DateTime<Local>) -> DateTime<chrono_tz::Tz> {
    dt.with_timezone(&chrono_tz::America::New_York)
}

pub fn extract_kalshi_window_start(open_time_iso: &str) -> Option<i32> {
    // ISO format: 2024-04-28T00:00:00Z
    let dt = DateTime::parse_from_rfc3339(open_time_iso).ok()?;
    let dt_et = dt.with_timezone(&chrono_tz::America::New_York);
    Some(dt_et.hour() as i32 * 60 + dt_et.minute() as i32)
}
