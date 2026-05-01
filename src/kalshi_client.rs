use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use chrono::{DateTime, Duration, Local, Utc};
use log::{debug, error, info, warn};
use reqwest::{header, Client, Method};
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::rand_core::OsRng;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;
use serde::Deserialize;
use sha2::Sha256;
use std::collections::HashMap;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Market {
    pub id: String,
    pub question: String,
    pub target_price: Option<f64>,
    pub open_time: Option<String>,
    pub close_time: Option<String>,
    pub condition_id: String,
    pub volume: String,
    pub clob_token_ids: Vec<String>,
    pub tokens: Vec<Token>,
}

#[derive(Debug, Clone)]
pub struct Token {
    pub token_id: String,
    pub outcome: String,
    pub price: f64,
}

#[derive(Debug, Clone, Default)]
pub struct OrderbookMetrics {
    pub best_ask: Option<f64>,
    pub total_asks_volume: f64,
    pub total_bids_volume: f64,
    pub depth_near_best: f64,
    pub spread: f64,
    pub bid_ask_ratio: f64,
    pub liquidity_score: f64,
    pub bids_depth: Vec<(f64, f64)>,
    pub asks_depth: Vec<(f64, f64)>,
}

#[derive(Debug, Clone)]
pub struct OrderExecution {
    pub order_id: String,
    pub filled_size: f64,
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

const KALSHI_API_URL: &str = "https://demo-api.kalshi.co/trade-api/v2";
pub const KALSHI_PROD_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
const KALSHI_V1_PROD_URL: &str = "https://api.elections.kalshi.com/trade-api/v1";

#[derive(Deserialize, Debug)]
struct KalshiLoginResponse {
    token: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct KalshiRawMarketsResponse {
    pub markets: Vec<KalshiRawMarket>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiRawMarket {
    pub id: Option<String>,
    pub ticker: String,
    pub title: Option<String>,
    pub status: Option<String>,
    pub floor_strike: Option<f64>,
    pub yes_ask: Option<f64>,
    pub no_ask: Option<f64>,
    pub yes_bid: Option<f64>,
    pub no_bid: Option<f64>,
    pub yes_ask_dollars: Option<String>,
    pub no_ask_dollars: Option<String>,
    pub yes_bid_dollars: Option<String>,
    pub no_bid_dollars: Option<String>,
    pub close_time: Option<String>,
    pub open_time: Option<String>,
    pub expiration_time: Option<String>,
    pub result: Option<String>,
    pub series_ticker: Option<String>,
    pub event_ticker: Option<String>,
    pub yes_sub_title: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiForecastHistoryResponse {
    pub forecast_history: Vec<KalshiForecastPoint>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiForecastPoint {
    #[serde(rename = "end_period_ts")]
    pub ts: i64,
    #[serde(rename = "numerical_forecast")]
    pub yes_price: Option<f64>,
    pub mean_price: Option<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiCandlesticksResponse {
    pub candlesticks: Vec<KalshiCandle>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiCandle {
    pub ts: i64,
    pub yes_price: Option<KalshiOHLC>,
    pub no_price: Option<KalshiOHLC>,
    pub volume: Option<f64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiOHLC {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Deserialize, Debug)]
struct KalshiOrderbookResponse {
    orderbook_fp: Option<KalshiOrderbookFp>,
    orderbook: Option<KalshiOrderbook>,
}

#[derive(Deserialize, Debug)]
struct KalshiOrderbookFp {
    yes_dollars: Option<Vec<Vec<String>>>,
    no_dollars: Option<Vec<Vec<String>>>,
}

#[derive(Deserialize, Debug)]
struct KalshiOrderbook {
    yes: Option<Vec<Vec<f64>>>,
    no: Option<Vec<Vec<f64>>>,
}

#[derive(serde::Serialize)]
struct CreateOrderRequest {
    ticker: String,
    client_order_id: String,
    side: String,
    action: String,
    count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    yes_price_dollars: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_price_dollars: Option<String>,
    time_in_force: String,
}

#[derive(Deserialize, Debug)]
struct CreateOrderResponse {
    order: KalshiOrder,
}

#[derive(Deserialize, Debug)]
struct CancelOrderResponse {
    order: KalshiOrder,
}

#[derive(Deserialize, Debug)]
struct GetBalanceResponse {
    balance: i64,
    portfolio_value: i64,
}

#[derive(Deserialize, Debug)]
struct GetOrderResponse {
    order: HistoricalOrder,
}

#[derive(Deserialize, Debug)]
struct GetPositionsResponse {
    #[serde(default, alias = "market_positions")]
    pub positions: Vec<KalshiPosition>,
}

#[derive(Deserialize, Debug)]
struct GetFillsResponse {
    fills: Vec<HistoricalFill>,
    #[allow(dead_code)]
    cursor: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KalshiPosition {
    #[serde(default)]
    pub ticker: String,
    #[serde(default)]
    pub position: i64,
    #[serde(default)]
    pub position_fp: String,
    #[serde(default)]
    pub market_ticker: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HistoricalFill {
    pub fill_id: Option<String>,
    pub trade_id: Option<String>,
    pub action: String,
    pub count_fp: String,
    pub created_time: String,
    pub ticker: String,
    pub market_ticker: Option<String>,
    pub side: String,
    pub yes_price_dollars: Option<String>,
    pub no_price_dollars: Option<String>,
    pub is_taker: Option<bool>,
    pub fee_cost: Option<String>,
    pub order_id: String,
    pub subaccount_number: Option<i32>,
    pub ts: Option<i64>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct HistoricalOrder {
    pub order_id: String,
    pub ticker: String,
    pub side: String,
    pub action: String,
    pub fill_count_fp: String,
    pub yes_price_dollars: Option<String>,
    pub no_price_dollars: Option<String>,
    pub taker_fill_cost_dollars: Option<String>,
    pub maker_fill_cost_dollars: Option<String>,
    pub taker_fees_dollars: Option<String>,
    pub maker_fees_dollars: Option<String>,
    pub created_time: Option<String>,
    pub last_update_time: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct KalshiOrder {
    pub order_id: String,
    pub status: String,
    pub fill_count_fp: String,
    pub remaining_count_fp: String,
    pub taker_fill_cost_dollars: Option<String>,
    pub maker_fill_cost_dollars: Option<String>,
}

pub struct KalshiClient {
    pub client: Client,
    email: String,
    password: String,
    bearer_token: Option<String>,
    access_key: Option<String>,
    private_key: Option<RsaPrivateKey>,
    prod: bool,
}

impl KalshiClient {
    pub async fn buy_yes(
        &self,
        ticker: &str,
        size: f64,
        price: f64,
    ) -> Result<KalshiOrder, BoxError> {
        info!("\u{1F6D2} [KALSHI] Buying YES on {}: size={}, price={:.2}", ticker, size, price);
        // P0 FIX: size as u32 truncates — 7.9 becomes 7, leaving orphaned contracts.
        let count = size.round() as u32;
        if (size - size.round()).abs() > 0.1 {
            warn!("Kalshi buy_yes rounding: {:.4} -> {} contracts", size, count);
        }
        self.submit_order(ticker, "yes", "buy", price, count, "immediate_or_cancel").await
    }

    pub async fn buy_no(
        &self,
        ticker: &str,
        size: f64,
        price: f64,
    ) -> Result<KalshiOrder, BoxError> {
        info!("\u{1F6D2} [KALSHI] Buying NO on {}: size={}, price={:.2}", ticker, size, price);
        let count = size.round() as u32;
        if (size - size.round()).abs() > 0.1 {
            warn!("Kalshi buy_no rounding: {:.4} -> {} contracts", size, count);
        }
        self.submit_order(ticker, "no", "buy", price, count, "immediate_or_cancel").await
    }

    pub async fn sell_yes(
        &self,
        ticker: &str,
        size: f64,
        price: f64,
    ) -> Result<KalshiOrder, BoxError> {
        info!("KALSHI SELL YES | ticker={} | size={} | price={:.2}", ticker, size, price);
        let count = size.round() as u32;
        if (size - size.round()).abs() > 0.1 {
            warn!("Kalshi sell_yes rounding: {:.4} -> {} contracts", size, count);
        }
        self.submit_order(ticker, "yes", "sell", price, count, "immediate_or_cancel").await
    }

    pub async fn sell_no(
        &self,
        ticker: &str,
        size: f64,
        price: f64,
    ) -> Result<KalshiOrder, BoxError> {
        info!("KALSHI SELL NO | ticker={} | size={} | price={:.2}", ticker, size, price);
        let count = size.round() as u32;
        if (size - size.round()).abs() > 0.1 {
            warn!("Kalshi sell_no rounding: {:.4} -> {} contracts", size, count);
        }
        self.submit_order(ticker, "no", "sell", price, count, "immediate_or_cancel").await
    }

    pub async fn init_prod() -> Result<Self, BoxError> {
        let mut client = Self::with_mode(
            std::env::var("KALSHI_EMAIL").unwrap_or_default(),
            std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
            true,
        );
        client.login().await?;
        Ok(client)
    }

    pub fn build() -> Self {
        Self::with_mode(
            std::env::var("KALSHI_EMAIL").unwrap_or_default(),
            std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
            false,
        )
    }

    pub fn build_prod(email: String, password: String) -> Self {
        Self::with_mode(email, password, true)
    }

    fn with_mode(email: String, password: String, prod: bool) -> Self {
        let access_key = std::env::var("KALSHI_ACCESS_KEY")
            .ok()
            .filter(|v| !v.is_empty());
        let private_key = load_private_key_from_env();

        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap_or_else(|_| Client::new()),
            email,
            password,
            bearer_token: None,
            access_key,
            private_key,
            prod,
        }
    }

    fn base_url(&self) -> &'static str {
        if self.prod {
            KALSHI_PROD_URL
        } else {
            KALSHI_API_URL
        }
    }

    fn auth_header(&self) -> Option<String> {
        self.bearer_token
            .as_ref()
            .map(|token| format!("Bearer {}", token))
    }

    fn sign_headers(&self, method: &Method, path: &str) -> Option<header::HeaderMap> {
        let access_key = self.access_key.as_ref()?;
        let private_key = self.private_key.as_ref()?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis()
            .to_string();
        let canonical_path = format!("/trade-api/v2{}", path);
        let payload = format!("{}{}{}", timestamp, method.as_str(), canonical_path);
        let signing_key = SigningKey::<Sha256>::new(private_key.clone());
        let signature = signing_key.sign_with_rng(&mut OsRng, payload.as_bytes());
        let signature_b64 = general_purpose::STANDARD.encode(signature.to_vec());

        let mut headers = header::HeaderMap::new();
        headers.insert(
            "KALSHI-ACCESS-KEY",
            header::HeaderValue::from_str(access_key).ok()?,
        );
        headers.insert(
            "KALSHI-ACCESS-TIMESTAMP",
            header::HeaderValue::from_str(&timestamp).ok()?,
        );
        headers.insert(
            "KALSHI-ACCESS-SIGNATURE",
            header::HeaderValue::from_str(&signature_b64).ok()?,
        );
        Some(headers)
    }

    fn request_with_auth(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.client.request(method.clone(), &url);
        if let Some(headers) = self.sign_headers(&method, path) {
            req = req.headers(headers);
        } else if let Some(auth) = self.auth_header() {
            req = req.header(header::AUTHORIZATION, auth);
        }
        req
    }

    async fn submit_order(
        &self,
        ticker: &str,
        side: &str,
        action: &str,
        limit_price: f64,
        count: u32,
        tif: &str,
    ) -> Result<KalshiOrder, BoxError> {
        if count == 0 {
            return Err("Kalshi order rejected: count must be >= 1".into());
        }

        let price = format!("{:.4}", limit_price.clamp(0.01, 0.99));
        let request = CreateOrderRequest {
            ticker: ticker.to_string(),
            client_order_id: format!(
                "{}-{}-{}-{}",
                ticker,
                side,
                action,
                Local::now().timestamp_millis()
            ),
            side: side.to_string(),
            action: action.to_string(),
            count,
            yes_price_dollars: if side == "yes" {
                Some(price.clone())
            } else {
                None
            },
            no_price_dollars: if side == "no" { Some(price) } else { None },
            time_in_force: tif.to_string(),
        };

        let path = "/portfolio/orders";
        let response = self
            .request_with_auth(Method::POST, path)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            // P0 FIX: Detect token expiry explicitly. Silent 401s cause the bot to keep
            // looping while zero orders reach the exchange.
            if status.as_u16() == 401 {
                error!(
                    "KALSHI AUTH EXPIRED (401): Token is invalid or expired. All orders are failing silently. Restart required to re-authenticate. Body: {}",
                    body
                );
                return Err(format!("Kalshi auth expired (401) — restart to re-login: {}", body).into());
            }
            return Err(format!("Kalshi create order failed: {} {}", status, body).into());
        }

        let payload: CreateOrderResponse = response.json().await?;
        let filled = payload.order.fill_count_fp.parse::<f64>().unwrap_or(0.0);
        let remaining = payload
            .order
            .remaining_count_fp
            .parse::<f64>()
            .unwrap_or(0.0);
        let expected = count as f64;

        // P0 FIX: IOC (immediate_or_cancel) orders with zero fills are silently returned as Ok.
        // This is the #1 cause of phantom fills in Kalshi — the bot thinks it bought/sold
        // but nothing actually executed. Treat zero-fill IOC as an error.
        if tif == "immediate_or_cancel" && filled < 0.001 {
            error!(
                "KALSHI IOC ZERO-FILL: ticker={} side={} action={} status={} filled={} remaining={} — NO CONTRACTS EXECUTED",
                ticker, side, action, payload.order.status, filled, remaining
            );
            return Err(format!(
                "Kalshi IOC zero-fill: ticker={} side={} action={} status={} — zero contracts filled. Check liquidity.",
                ticker, side, action, payload.order.status
            ).into());
        }

        if tif == "fill_or_kill" && (filled + 0.0001 < expected || remaining > 0.0001) {
            return Err(format!(
                "Kalshi order not fully filled (FOK): status={} filled={} remaining={}",
                payload.order.status, filled, remaining
            )
            .into());
        }

        info!(
            "KALSHI ORDER OK | ticker={} side={} action={} | filled={} remaining={} | order_id={}",
            ticker, side, action, filled, remaining, payload.order.order_id
        );

        Ok(payload.order)
    }

    pub async fn get_balance_dollars(&self) -> Result<(f64, f64), BoxError> {
        let path = "/portfolio/balance";
        let response = self.request_with_auth(Method::GET, path).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi get balance failed: {} {}", status, body).into());
        }

        let payload: GetBalanceResponse = response.json().await?;
        Ok((
            payload.balance as f64 / 100.0,
            payload.portfolio_value as f64 / 100.0,
        ))
    }

    pub async fn get_outcome_top_of_book(
        &self,
        market_id: &str,
    ) -> Result<((Option<f64>, Option<f64>), (Option<f64>, Option<f64>)), BoxError> {
        let (market_yes_ask, market_no_ask, market_yes_bid, market_no_bid) =
            self.get_market_quotes(market_id).await?;

        let yes_best_ask = market_yes_ask.filter(|price| *price > 0.0 && *price < 1.0);
        let no_best_ask = market_no_ask.filter(|price| *price > 0.0 && *price < 1.0);

        // P1 FIX: Removed bid inference from opposite ask (1.0 - no_ask).
        // In illiquid markets the spread can be wide; the inferred bid would be
        // artificially high, triggering SL exits at wrong prices.
        // Now we only return a real bid if the exchange actually quotes one.
        let yes_bid = market_yes_bid.filter(|price| *price > 0.0 && *price < 1.0);
        let no_bid = market_no_bid.filter(|price| *price > 0.0 && *price < 1.0);

        if yes_bid.is_none() {
            log::debug!("Kalshi yes_bid unavailable for {} — no synthetic bid used", market_id);
        }
        if no_bid.is_none() {
            log::debug!("Kalshi no_bid unavailable for {} — no synthetic bid used", market_id);
        }

        Ok(((yes_best_ask, no_best_ask), (yes_bid, no_bid)))
    }

    pub async fn fetch_recent_fills(&self, limit: u32) -> Result<Vec<HistoricalFill>, BoxError> {
        let path = "/portfolio/fills";
        let response = self
            .request_with_auth(Method::GET, path)
            .query(&[("limit", limit)])
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi fetch fills failed: {} {}", status, body).into());
        }

        let payload: GetFillsResponse = response.json().await?;
        Ok(payload.fills)
    }

    pub async fn fetch_fills_between(
        &self,
        min_ts: Option<i64>,
        max_ts: Option<i64>,
    ) -> Result<Vec<HistoricalFill>, BoxError> {
        let path = "/portfolio/fills";
        let mut cursor: Option<String> = None;
        let mut fills = Vec::new();

        loop {
            let mut req = self.request_with_auth(Method::GET, path);
            let limit = 1000u32.to_string();
            req = req.query(&[("limit", limit.as_str())]);

            if let Some(min_ts) = min_ts {
                let min_ts_str = min_ts.to_string();
                req = req.query(&[("min_ts", min_ts_str.as_str())]);
            }
            if let Some(max_ts) = max_ts {
                let max_ts_str = max_ts.to_string();
                req = req.query(&[("max_ts", max_ts_str.as_str())]);
            }
            if let Some(ref cursor_value) = cursor {
                req = req.query(&[("cursor", cursor_value.as_str())]);
            }

            let response = req.send().await?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                return Err(format!("Kalshi fetch fills failed: {} {}", status, body).into());
            }

            let payload: GetFillsResponse = response.json().await?;
            let next_cursor = payload.cursor.clone();
            fills.extend(payload.fills);

            match next_cursor {
                Some(next) if !next.is_empty() => cursor = Some(next),
                _ => break,
            }
        }

        Ok(fills)
    }

    pub async fn fetch_order(&self, order_id: &str) -> Result<HistoricalOrder, BoxError> {
        let path = format!("/portfolio/orders/{}", order_id);
        let response = self.request_with_auth(Method::GET, &path).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi get order failed: {} {}", status, body).into());
        }

        let payload: GetOrderResponse = response.json().await?;
        Ok(payload.order)
    }

    pub async fn get_portfolio_positions(&self) -> Result<Vec<KalshiPosition>, BoxError> {
        let path = "/portfolio/positions";
        let response = self.request_with_auth(Method::GET, path).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi get positions failed: {} {}", status, body).into());
        }

        let payload: GetPositionsResponse = response.json().await?;
        Ok(payload.positions)
    }

    pub async fn login(&mut self) -> Result<(), BoxError> {
        if self.sign_headers(&Method::GET, "/portfolio").is_some() {
            info!("Kalshi using API key authentication.");
            return Ok(());
        }

        if self.email.is_empty() || self.password.is_empty() {
            return Err("Missing Kalshi email/password for login".into());
        }

        let path = "/login";
        let url = format!("{}{}", self.base_url(), path);
        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "email": self.email,
                "password": self.password,
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi login failed: {} {}", status, body).into());
        }

        let payload: KalshiLoginResponse = response.json().await?;
        self.bearer_token = payload.token;
        Ok(())
    }

    pub async fn fetch_markets(
        &self,
        series: Option<&str>,
    ) -> Result<Vec<KalshiRawMarket>, BoxError> {
        let mut path = "/markets".to_string();
        let mut query = vec![("limit", "1000".to_string())];
        if let Some(series_ticker) = series {
            query.push(("series_ticker", series_ticker.to_string()));
        }

        if !query.is_empty() {
            let query_str = query
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join("&");
            path.push('?');
            path.push_str(&query_str);
        }

        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.client.get(&url);
        if let Some(headers) = self.sign_headers(&Method::GET, "/markets") {
            req = req.headers(headers);
        } else if let Some(auth) = self.auth_header() {
            req = req.header(header::AUTHORIZATION, auth);
        }

        let response = req.send().await?;
        if !response.status().is_success() {
            return Err(format!("Kalshi fetch_markets failed: {}", response.status()).into());
        }

        let payload: KalshiRawMarketsResponse = response.json().await?;
        Ok(payload.markets)
    }

    pub async fn fetch_settled_markets(
        &self,
        series: &str,
        limit: u32,
    ) -> Result<Vec<KalshiRawMarket>, BoxError> {
        let mut markets = self.fetch_markets(Some(series)).await?;
        markets.retain(|market| {
            let status = market
                .status
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            status.contains("settled")
                || status.contains("final")
                || status.contains("resolved")
                || market.result.is_some()
        });
        markets.sort_by(|a, b| b.close_time.cmp(&a.close_time));
        let limit = limit as usize;
        if markets.len() > limit {
            markets.truncate(limit);
        }
        Ok(markets)
    }

    pub async fn v1_fetch_market_uuid_map(
        &self,
        series: &str,
        limit: u32,
    ) -> Result<HashMap<String, String>, BoxError> {
        let markets = self.fetch_markets(Some(series)).await?;
        let mut map = HashMap::new();
        for market in markets.into_iter().take(limit as usize) {
            if let Some(id) = market.id {
                map.insert(market.ticker, id);
            }
        }
        Ok(map)
    }

    pub async fn fetch_forecast_history(
        &self,
        series: &str,
        market_uuid: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<KalshiForecastPoint>, BoxError> {
        let url = format!(
            "{}/series/{}/markets/{}/forecast_history?start_ts={}&end_ts={}",
            KALSHI_V1_PROD_URL, series, market_uuid, start_ts, end_ts
        );
        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            return Err(format!("Kalshi forecast_history failed: {}", response.status()).into());
        }
        let payload: KalshiForecastHistoryResponse = response.json().await?;
        Ok(payload.forecast_history)
    }

    fn market_to_unified(market: KalshiRawMarket) -> Market {
        let question = market
            .title
            .clone()
            .unwrap_or_else(|| market.ticker.clone());

        // 1. Try floor_strike
        // 2. Try subtitle extraction
        // 3. Fallback: Try title extraction
        let target_price = market
            .floor_strike
            .or_else(|| {
                market
                    .yes_sub_title
                    .as_deref()
                    .and_then(extract_target_price_from_subtitle)
            })
            .or_else(|| {
                market
                    .title
                    .as_deref()
                    .and_then(extract_target_price_from_subtitle)
            });

        let yes_price = market
            .yes_ask_dollars
            .as_deref()
            .and_then(parse_decimal_price)
            .or_else(|| market.yes_ask.map(|p| p / 100.0))
            .unwrap_or(0.5);
        let no_price = market
            .no_ask_dollars
            .as_deref()
            .and_then(parse_decimal_price)
            .or_else(|| market.no_ask.map(|p| p / 100.0))
            .unwrap_or(0.5);
        let ticker = market.ticker.clone();

        Market {
            id: ticker.clone(),
            question,
            target_price,
            open_time: market.open_time.clone(),
            close_time: market.close_time.clone(),
            condition_id: market.id.unwrap_or_else(|| ticker.clone()),
            volume: String::new(),
            clob_token_ids: vec![format!("{}_YES", ticker), format!("{}_NO", ticker)],
            tokens: vec![
                Token {
                    token_id: format!("{}_YES", ticker),
                    outcome: "Yes".to_string(),
                    price: yes_price,
                },
                Token {
                    token_id: format!("{}_NO", ticker),
                    outcome: "No".to_string(),
                    price: no_price,
                },
            ],
        }
    }

    fn parse_market_time(value: &Option<String>) -> Option<DateTime<Utc>> {
        value
            .as_deref()
            .and_then(|raw| chrono::DateTime::parse_from_rfc3339(raw).ok())
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn is_closed_status(status: &str) -> bool {
        status.contains("settled")
            || status.contains("final")
            || status.contains("resolved")
            || status.contains("closed")
    }

    fn select_current_market(
        mut markets: Vec<KalshiRawMarket>,
        now: DateTime<Local>,
    ) -> Option<KalshiRawMarket> {
        let now_utc = now.with_timezone(&Utc);

        markets.sort_by(|a, b| {
            let a_open = Self::parse_market_time(&a.open_time)
                .map(|dt| dt.timestamp())
                .unwrap_or(i64::MIN);
            let b_open = Self::parse_market_time(&b.open_time)
                .map(|dt| dt.timestamp())
                .unwrap_or(i64::MIN);
            b_open.cmp(&a_open)
        });

        if let Some(current) = markets.iter().find(|market| {
            let status = market
                .status
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            if Self::is_closed_status(&status) {
                return false;
            }

            let open_time = Self::parse_market_time(&market.open_time);
            let close_time = Self::parse_market_time(&market.close_time);
            match (open_time, close_time) {
                (Some(open), Some(close)) => open <= now_utc && now_utc < close,
                _ => status == "active" || status == "open",
            }
        }) {
            return Some(current.clone());
        }

        if let Some(activeish) = markets.iter().find(|market| {
            let status = market
                .status
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            status == "active" || status == "open"
        }) {
            return Some(activeish.clone());
        }

        markets.into_iter().find(|market| {
            let status = market
                .status
                .as_deref()
                .unwrap_or_default()
                .to_ascii_lowercase();
            !Self::is_closed_status(&status)
        })
    }
}

impl KalshiClient {
    async fn new() -> Result<Self, BoxError>
    where
        Self: Sized,
    {
        Ok(Self::build())
    }

    async fn new_prod() -> Result<Self, BoxError>
    where
        Self: Sized,
    {
        Self::init_prod().await
    }

    pub async fn get_active_markets(&self, _now: DateTime<Local>) -> Result<Vec<Market>, BoxError> {
        let now_utc = _now.with_timezone(&Utc);
        let mut active = Vec::new();
        for series in ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M"] {
            let markets = self.fetch_markets(Some(series)).await?;
            for market in markets {
                let status = market
                    .status
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if Self::is_closed_status(&status) {
                    continue;
                }

                let open_time = Self::parse_market_time(&market.open_time);
                let close_time = Self::parse_market_time(&market.close_time);

                let is_candidate = match (open_time, close_time) {
                    (Some(open), Some(close)) => {
                        // Relaxed: starts in next 15m OR is current
                        let start_window = open - Duration::minutes(15);
                        now_utc >= start_window && now_utc < close
                    }
                    _ => status == "active" || status == "open",
                };

                if is_candidate {
                    active.push(Self::market_to_unified(market));
                }
            }
        }
        Ok(active)
    }

    pub async fn get_market_prices(
        &self,
        market_id: &str,
    ) -> Result<(Option<f64>, Option<f64>), BoxError> {
        let (yes_ask, no_ask, _, _) = self.get_market_quotes(market_id).await?;
        Ok((yes_ask, no_ask))
    }

    pub async fn get_market_quotes(
        &self,
        market_id: &str,
    ) -> Result<(Option<f64>, Option<f64>, Option<f64>, Option<f64>), BoxError> {
        let path = format!("/markets/{}", market_id);
        let url = format!("{}{}", self.base_url(), path);
        let mut req = self.client.get(&url);
        if let Some(headers) = self.sign_headers(&Method::GET, &path) {
            req = req.headers(headers);
        } else if let Some(auth) = self.auth_header() {
            req = req.header(header::AUTHORIZATION, auth);
        }

        #[derive(Deserialize)]
        struct Wrapper {
            market: Option<KalshiRawMarket>,
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(wrapper) = resp.json::<Wrapper>().await {
                    if let Some(market) = wrapper.market {
                        let yes = market
                            .yes_ask_dollars
                            .as_deref()
                            .and_then(parse_decimal_price)
                            .or_else(|| market.yes_ask.map(|p| p / 100.0));
                        let no = market
                            .no_ask_dollars
                            .as_deref()
                            .and_then(parse_decimal_price)
                            .or_else(|| market.no_ask.map(|p| p / 100.0));
                        let yes_bid = market
                            .yes_bid_dollars
                            .as_deref()
                            .and_then(parse_decimal_price)
                            .or_else(|| market.yes_bid.map(|p| p / 100.0));
                        let no_bid = market
                            .no_bid_dollars
                            .as_deref()
                            .and_then(parse_decimal_price)
                            .or_else(|| market.no_bid.map(|p| p / 100.0));
                        return Ok((yes, no, yes_bid, no_bid));
                    }
                }
            }
            Err(err) => error!("Kalshi get_market_prices error for {}: {}", market_id, err),
            _ => {}
        }

        Ok((None, None, None, None))
    }

    async fn get_best_ask(&self, market_id: &str, token_id: &str) -> Result<Option<f64>, BoxError> {
        let synthetic_token_id = if token_id.ends_with("_YES") {
            format!("{}_YES", market_id)
        } else {
            format!("{}_NO", market_id)
        };
        Ok(self
            .get_orderbook_depth(&synthetic_token_id)
            .await?
            .best_ask)
    }

    async fn get_orderbook_depth(&self, token_id: &str) -> Result<OrderbookMetrics, BoxError> {
        let market_id = token_id.trim_end_matches("_YES").trim_end_matches("_NO");
        let is_yes_token = token_id.ends_with("_YES");
        let path = format!("/markets/{}/orderbook", market_id);
        let url = format!("{}{}?depth=20", self.base_url(), path);

        let mut req = self.client.get(&url);
        if let Some(headers) = self.sign_headers(&Method::GET, &path) {
            req = req.headers(headers);
        } else if let Some(auth) = self.auth_header() {
            req = req.header(header::AUTHORIZATION, auth);
        }

        match req.send().await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(orderbook) = resp.json::<KalshiOrderbookResponse>().await {
                    let (yes_asks, no_asks) = parse_kalshi_book_sides(orderbook);

                    let mut asks_depth = if is_yes_token {
                        yes_asks.clone()
                    } else {
                        no_asks.clone()
                    };
                    asks_depth
                        .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

                    let mut bids_depth: Vec<(f64, f64)> = if is_yes_token {
                        no_asks
                            .iter()
                            .map(|(price, size)| ((1.0 - price).clamp(0.0, 1.0), *size))
                            .collect()
                    } else {
                        yes_asks
                            .iter()
                            .map(|(price, size)| ((1.0 - price).clamp(0.0, 1.0), *size))
                            .collect()
                    };
                    bids_depth
                        .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

                    let best_bid = bids_depth.first().map(|(price, _)| *price).unwrap_or(0.0);
                    let best_ask = asks_depth.first().map(|(price, _)| *price);
                    let total_bids_volume: f64 = bids_depth.iter().map(|(_, size)| *size).sum();
                    let total_asks_volume: f64 = asks_depth.iter().map(|(_, size)| *size).sum();
                    let depth_near_best = best_ask
                        .map(|ask| {
                            asks_depth
                                .iter()
                                .filter(|(price, _)| *price <= ask + 0.02)
                                .map(|(_, size)| *size)
                                .sum()
                        })
                        .unwrap_or(0.0);
                    let spread = best_ask.map(|ask| (ask - best_bid).max(0.0)).unwrap_or(1.0);
                    let volume_score = (total_bids_volume / 500.0).min(1.0).powf(0.7);
                    let depth_score = (depth_near_best / 200.0).min(1.0).powf(0.5);
                    let spread_score = (1.0 - spread * 20.0).max(0.0);
                    let liquidity_score =
                        (depth_score * 0.50 + volume_score * 0.35 + spread_score * 0.15).min(1.0);

                    return Ok(OrderbookMetrics {
                        best_ask,
                        total_asks_volume,
                        total_bids_volume,
                        depth_near_best,
                        spread,
                        bid_ask_ratio: if total_asks_volume > 0.0 {
                            total_bids_volume / total_asks_volume
                        } else {
                            0.0
                        },
                        liquidity_score,
                        bids_depth,
                        asks_depth,
                    });
                }
            }
            Err(err) => debug!("Kalshi orderbook_depth error: {}", err),
            _ => {}
        }

        Ok(OrderbookMetrics::default())
    }

    async fn place_protective_limit_sell(
        &self,
        token_id: &str,
        limit_price: f64,
        size: f64,
        market_id: &str,
    ) -> Result<OrderExecution, BoxError> {
        let side = if token_id.ends_with("_YES") {
            "yes"
        } else {
            "no"
        };
        let count = size.floor() as u32;
        // USE immediate_or_cancel for protective sells to ensure partial closure in low liquidity
        let order = self
            .submit_order(
                market_id,
                side,
                "sell",
                limit_price,
                count,
                "immediate_or_cancel",
            )
            .await?;
        let filled = order.fill_count_fp.parse::<f64>().unwrap_or(0.0);

        info!(
            "KALSHI PROTECTIVE SELL | id={} ticker={} side={} limit={:.4} requested={} filled={}",
            order.order_id, market_id, side, limit_price, count, filled
        );
        Ok(OrderExecution {
            order_id: order.order_id,
            filled_size: filled, // Return ACTUAL filled size
        })
    }

    async fn place_recovery_limit_order(
        &self,
        token_id: &str,
        limit_price: f64,
        size: f64,
        market_id: &str,
    ) -> Result<OrderExecution, BoxError> {
        let side = if token_id.ends_with("_YES") {
            "yes"
        } else {
            "no"
        };
        let count = size.floor() as u32;
        // USE immediate_or_cancel for recovery orders to avoid 409 Conflict rejection if liquidity shifts
        let order = self
            .submit_order(
                market_id,
                side,
                "buy",
                limit_price,
                count,
                "immediate_or_cancel",
            )
            .await?;
        let filled = order.fill_count_fp.parse::<f64>().unwrap_or(0.0);

        info!(
            "KALSHI BUY ORDER | id={} ticker={} side={} limit={:.4} requested={} filled={}",
            order.order_id, market_id, side, limit_price, count, filled
        );
        Ok(OrderExecution {
            order_id: order.order_id,
            filled_size: filled,
        })
    }

    async fn cancel_order(&self, order_id: &str) -> Result<(), BoxError> {
        let path = format!("/portfolio/orders/{}", order_id);
        let response = self.request_with_auth(Method::DELETE, &path).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(format!("Kalshi cancel order failed: {} {}", status, body).into());
        }

        let payload: CancelOrderResponse = response.json().await?;
        info!(
            "KALSHI ORDER CANCELLED | id={} status={}",
            payload.order.order_id, payload.order.status
        );
        Ok(())
    }

    async fn get_server_time(&self) -> Result<DateTime<Local>, BoxError> {
        Ok(Local::now())
    }

    async fn get_binance_price_direct(&self, symbol: &str) -> Option<f64> {
        let client = reqwest::Client::new();
        crate::api::get_binance_price(&client, symbol).await
    }
}

fn load_private_key_from_env() -> Option<RsaPrivateKey> {
    if let Ok(raw_pem) = std::env::var("KALSHI_PRIVATE_KEY") {
        let pem = raw_pem
            .trim()
            .trim_matches('"')
            .replace("\\n", "\n")
            .replace("\r\n", "\n");
        if let Ok(key) =
            RsaPrivateKey::from_pkcs8_pem(&pem).or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
        {
            return Some(key);
        }
    }

    let pem_path = std::env::var("KALSHI_PRIVATE_KEY_PATH").ok()?;
    let pem = fs::read_to_string(pem_path).ok()?;
    RsaPrivateKey::from_pkcs8_pem(&pem)
        .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&pem))
        .ok()
}

fn parse_kalshi_book_sides(ob: KalshiOrderbookResponse) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    if let Some(orderbook_fp) = ob.orderbook_fp {
        let yes = parse_fixed_point_levels(orderbook_fp.yes_dollars);
        let no = parse_fixed_point_levels(orderbook_fp.no_dollars);
        return (yes, no);
    }

    if let Some(orderbook) = ob.orderbook {
        let yes = parse_legacy_levels(orderbook.yes);
        let no = parse_legacy_levels(orderbook.no);
        return (yes, no);
    }

    (Vec::new(), Vec::new())
}

fn parse_fixed_point_levels(levels: Option<Vec<Vec<String>>>) -> Vec<(f64, f64)> {
    levels
        .unwrap_or_default()
        .into_iter()
        .filter_map(|level| {
            let price = level.first()?.parse::<f64>().ok()?;
            let size = level.get(1)?.parse::<f64>().ok()?;
            Some((price, size))
        })
        .collect()
}

fn parse_legacy_levels(levels: Option<Vec<Vec<f64>>>) -> Vec<(f64, f64)> {
    levels
        .unwrap_or_default()
        .into_iter()
        .filter_map(|level| {
            let price = *level.first()?;
            let size = *level.get(1)?;
            Some((price / 100.0, size))
        })
        .collect()
}

fn extract_target_price_from_subtitle(text: &str) -> Option<f64> {
    let lower = text.to_ascii_lowercase();
    if lower.contains("tbd") {
        return None;
    }

    // 1. Try "Target Price: $12,345.67"
    if let Ok(re) = regex::Regex::new(r"(?i)target\s*price:\s*\$?([0-9,]+(?:\.[0-9]+)?)") {
        if let Some(caps) = re.captures(text) {
            if let Some(m) = caps.get(1) {
                if let Ok(p) = m.as_str().replace(',', "").parse::<f64>() {
                    return Some(p);
                }
            }
        }
    }

    // 2. Try naked dollar amount "$12,345.67"
    if let Ok(re) = regex::Regex::new(r"\$([0-9,]+(?:\.[0-9]+)?)") {
        if let Some(caps) = re.captures(text) {
            if let Some(m) = caps.get(1) {
                if let Ok(p) = m.as_str().replace(',', "").parse::<f64>() {
                    return Some(p);
                }
            }
        }
    }

    None
}

fn parse_decimal_price(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
}
