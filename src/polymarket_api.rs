use log::{error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};

const GAMMA_URL: &str = "https://gamma-api.polymarket.com";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Market {
    pub id: String,
    pub question: String,
    #[serde(rename = "outcomePrices")]
    pub outcome_prices: Option<String>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<String>,
    pub tokens: Vec<Token>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Token {
    pub token_id: String,
    pub outcome: String, // "Yes" or "No"
    pub price: f64,
}

pub struct PolymarketAPI {
    client: Client,
}

impl PolymarketAPI {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }

    pub async fn get_active_5m_markets(&self) -> Vec<Market> {
        let mut filtered = Vec::new();

        // Calculate the current 5-minute bucket (Unix timestamp in UTC)
        let now = chrono::Utc::now();
        let now_ts = now.timestamp();
        let bucket_start = (now_ts / 300) * 300; // Round down to nearest 5 mins

        // Check current window and next 2 windows
        for i in 0..3 {
            let ts = bucket_start + (i * 300);
            let slug = format!("btc-updown-5m-{}", ts);
            let url = format!("{}/markets/slug/{}", GAMMA_URL, slug);

            match self.client.get(&url).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(m) = resp.json::<serde_json::Value>().await {
                            let question = m["question"].as_str().unwrap_or_default();

                            // Parse token IDs and prices
                            let token_ids: Vec<String> = m["clobTokenIds"]
                                .as_str()
                                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                .unwrap_or_default();

                            let prices: Vec<String> = m["outcomePrices"]
                                .as_str()
                                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                                .unwrap_or_default();

                            if token_ids.is_empty() {
                                continue;
                            }

                            let tokens: Vec<Token> = token_ids
                                .iter()
                                .enumerate()
                                .map(|(i, id)| Token {
                                    token_id: id.clone(),
                                    outcome: if i == 0 {
                                        "Up".to_string()
                                    } else {
                                        "Down".to_string()
                                    },
                                    price: prices
                                        .get(i)
                                        .and_then(|p| p.parse().ok())
                                        .unwrap_or(0.5),
                                })
                                .collect();

                            info!(
                                "🎯 Predicted 5m Market Detected: {} | ID: {}",
                                question, m["id"]
                            );
                            filtered.push(Market {
                                id: m["id"].as_str().unwrap_or_default().to_string(),
                                question: question.to_string(),
                                outcome_prices: m["outcomePrices"].as_str().map(|s| s.to_string()),
                                clob_token_ids: m["clobTokenIds"].as_str().map(|s| s.to_string()),
                                tokens,
                            });
                        }
                    } else if i == 0 {
                        // Only log warning if current bucket is missing
                        warn!(
                            "⚠️ Current 5m slug not found: {} (Status: {})",
                            slug,
                            resp.status()
                        );
                    }
                }
                Err(e) => {
                    error!("Error fetching slug {}: {}", slug, e);
                }
            }
        }
        filtered
    }

    pub async fn get_market_price(&self, token_id: &str) -> Option<(f64, f64)> {
        let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(data) = resp.json::<serde_json::Value>().await {
                    let mut best_bid = 0.0;
                    let mut best_ask = 1.0;
                    let mut found_bid = false;
                    let mut found_ask = false;

                    if let Some(bids) = data["bids"].as_array() {
                        for bid in bids {
                            if let Some(price_str) = bid["price"].as_str() {
                                if let Ok(p) = price_str.parse::<f64>() {
                                    if p > best_bid {
                                        best_bid = p;
                                        found_bid = true;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(asks) = data["asks"].as_array() {
                        for ask in asks {
                            if let Some(price_str) = ask["price"].as_str() {
                                if let Ok(p) = price_str.parse::<f64>() {
                                    if p < best_ask {
                                        best_ask = p;
                                        found_ask = true;
                                    }
                                }
                            }
                        }
                    }

                    if found_bid || found_ask {
                        return Some((best_bid, best_ask));
                    }
                }
            }
            Err(_) => {}
        }
        None
    }

    pub async fn place_order(&self, token_id: &str, price: f64, amount: f64, side: &str) -> bool {
        let is_paper =
            std::env::var("PAPER_TRADING").unwrap_or_else(|_| "true".to_string()) == "true";

        if is_paper {
            info!(
                "📋 [PAPER] {} order on {} at {:.4}: ${:.2}",
                side, token_id, price, amount
            );
            true
        } else {
            warn!("⚠️ [REAL] Real trading not fully implemented in code yet. Requires EIP-712 signing logic.");
            info!("📋 [SKIPPED] {} order on {} at {:.4}: ${:.2} (PAPER_TRADING=false but logic missing)", side, token_id, price, amount);
            false
        }
    }

    pub async fn get_market_outcome(&self, market_id: &str) -> Option<String> {
        let url = format!("{}/markets/{}", GAMMA_URL, market_id);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                if let Ok(m) = resp.json::<serde_json::Value>().await {
                    // Check for resolution status using umaResolutionStatus
                    if m["umaResolutionStatus"].as_str() == Some("resolved") {
                        // Determine winner from outcomePrices ( winner has price ~1.0 )
                        if let Some(prices_str) = m["outcomePrices"].as_str() {
                            if let Ok(prices) = serde_json::from_str::<Vec<String>>(prices_str) {
                                if prices.len() >= 2 {
                                    if prices[0].parse::<f64>().unwrap_or(0.0) > 0.9 {
                                        return Some("0".to_string()); // UP
                                    } else if prices[1].parse::<f64>().unwrap_or(0.0) > 0.9 {
                                        return Some("1".to_string()); // DOWN
                                    }
                                }
                            }
                        }

                        // Fallback to winningOutcomeIndex if available
                        return m["winningOutcomeIndex"].as_i64().map(|i| i.to_string());
                    }
                }
            }
            Err(e) => {
                error!("Error fetching market outcome {}: {}", market_id, e);
            }
        }
        None
    }
}
