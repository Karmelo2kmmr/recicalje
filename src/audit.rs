use log::{info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::error::Error;

/// Result of auditing a single trade
#[derive(Debug, Clone)]
pub struct AuditResult {
    pub market_id: String,
    pub is_verified: bool,
    pub is_pending: bool, // NEW: Market is still open
    pub recorded_exit_price: f64,
    pub official_exit_price: Option<f64>,
    pub discrepancy_pct: f64,
    pub warnings: Vec<String>,
}

/// Comprehensive audit report for a set of trades
#[derive(Debug, Clone)]
pub struct AuditReport {
    pub total_trades: usize,
    pub verified_trades: usize,
    pub pending_verifications: usize, // NEW: Markets not yet closed
    pub failed_verifications: usize,
    pub total_discrepancy: f64,
    pub warnings: Vec<String>,
    pub is_fully_verified: bool,
    pub results: Vec<AuditResult>, // NEW: Include individual results for correction
}

/// Trade record structure (matches CSV format)
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub market_id: String,
    pub coin: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: Option<f64>,
    pub size: f64,
    pub timestamp: i64,
    pub entry_type: String,
    pub exit_confirmed: Option<String>,
    pub exit_avg_fill_price: Option<f64>,
}

pub struct TradeAuditor {
    client: Client,
}

impl TradeAuditor {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Audit a single trade by verifying against Polymarket API
    pub async fn audit_trade(&self, trade: &TradeRecord) -> AuditResult {
        let mut warnings = Vec::new();

        let recorded_exit = trade
            .exit_avg_fill_price
            .or(trade.exit_price)
            .unwrap_or(0.0);
        let is_open_locally = trade.exit_price.is_none();

        // FASE 2: Respect ExitConfirmed flag
        let confirmed_flag = trade.exit_confirmed.as_deref().unwrap_or("LEGACY");
        let is_real_fill = confirmed_flag == "1" || confirmed_flag == "LEGACY";

        if is_open_locally || !is_real_fill {
            warnings
                .push("Trade open or unconfirmed locally - attempting API recovery".to_string());
        } else {
            // Bot closed it and we have a fill price → verified locally
            return AuditResult {
                market_id: trade.market_id.clone(),
                is_verified: true,
                is_pending: false,
                recorded_exit_price: recorded_exit,
                official_exit_price: Some(recorded_exit),
                discrepancy_pct: 0.0,
                warnings: vec![format!(
                    "Bot-confirmed fill ({}): verified locally",
                    confirmed_flag
                )],
            };
        }

        // Fetch official market closure price only if exit_price is None (market expired)
        match self
            .verify_market_closure(&trade.market_id, &trade.side)
            .await
        {
            Ok(official_price) => {
                let discrepancy = ((recorded_exit - official_price) / official_price * 100.0).abs();

                if discrepancy > 1.0 {
                    warnings.push(format!(
                        "Discrepancy {:.2}%: Recorded {:.4} vs Official {:.4}",
                        discrepancy, recorded_exit, official_price
                    ));
                }

                AuditResult {
                    market_id: trade.market_id.clone(),
                    is_verified: discrepancy <= 1.0,
                    is_pending: false,
                    recorded_exit_price: recorded_exit,
                    official_exit_price: Some(official_price),
                    discrepancy_pct: discrepancy,
                    warnings,
                }
            }
            Err(e) => {
                let is_pending = e.to_string().contains("not yet closed");
                if is_pending {
                    warnings.push("Market still open in Polymarket".to_string());
                } else {
                    warnings.push(format!("API verification failed: {}", e));
                }

                AuditResult {
                    market_id: trade.market_id.clone(),
                    is_verified: false,
                    is_pending,
                    recorded_exit_price: recorded_exit,
                    official_exit_price: None,
                    discrepancy_pct: 0.0,
                    warnings,
                }
            }
        }
    }

    /// Verify market closure price from Polymarket API
    async fn verify_market_closure(
        &self,
        market_id: &str,
        side: &str,
    ) -> Result<f64, Box<dyn Error>> {
        let url = format!("https://gamma-api.polymarket.com/markets/{}", market_id);

        let resp = self.client.get(&url).send().await?;

        if !resp.status().is_success() {
            return Err(format!("API returned status: {}", resp.status()).into());
        }

        // Use Value to handle both String and Array for outcomePrices
        let market: serde_json::Value = resp.json().await?;

        // === PASO 1: Verificar umaResolutionStatus ===
        let uma_status = market["umaResolutionStatus"].as_str().unwrap_or("");
        let is_closed = market["closed"].as_bool().unwrap_or(false)
            || market["active"].as_bool() == Some(false);

        // El mercado s├│lo est├í oficialmente resuelto cuando UMA confirma (o est├í cerrado y tiene precios)
        if uma_status != "resolved" && !is_closed {
            return Err("Market not yet resolved by UMA or closed".into());
        }

        // === PASO 2: Obtener Outcome Prices ===
        // outcomePrices puede ser un string JSON "[1, 0]" o un array real [1, 0]
        let prices: Vec<f64> = if let Some(p_str) = market["outcomePrices"].as_str() {
            serde_json::from_str(p_str).unwrap_or_else(|_| Vec::new())
        } else if let Some(p_arr) = market["outcomePrices"].as_array() {
            p_arr.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect()
        } else {
            Vec::new()
        };

        if prices.is_empty() {
            return Err("No outcome prices available".into());
        }

        // === PASO 3: Determinar el precio oficial según el lado ===
        // YES token → prices[0], NO token → prices[1]
        let price = match side {
            "UP" => prices.get(0).copied(),
            "DOWN" => prices.get(1).copied(),
            _ => None,
        };

        price.ok_or_else(|| "Price not found for side".into())
    }

    /// Audit a batch of trades and generate comprehensive report
    pub async fn audit_trades(&self, trades: &[TradeRecord]) -> AuditReport {
        info!("🔍 Starting audit of {} trades...", trades.len());

        let mut verified = 0;
        let mut failed = 0;
        let mut pending = 0;
        let mut total_discrepancy = 0.0;
        let mut all_warnings = Vec::new();
        let mut results = Vec::new();
        for (i, trade) in trades.iter().enumerate() {
            info!(
                "Auditing trade {}/{}: {}",
                i + 1,
                trades.len(),
                trade.market_id
            );

            let result = self.audit_trade(trade).await;

            if result.is_verified {
                verified += 1;
            } else if result.is_pending {
                pending += 1;
            } else if trade.exit_price.is_some() {
                failed += 1;
            }

            total_discrepancy += result.discrepancy_pct;
            all_warnings.extend(result.warnings.clone());
            results.push(result);

            // Small delay to avoid rate limiting
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        }

        let is_fully_verified = failed == 0 && pending == 0 && verified == trades.len();

        if is_fully_verified {
            info!("✅ All trades verified successfully!");
        } else if pending > 0 {
            info!(
                "⏳ Audit completed: {} verified, {} pending",
                verified, pending
            );
        } else {
            warn!("⚠️ Audit completed with {} failed verifications", failed);
        }

        AuditReport {
            total_trades: trades.len(),
            verified_trades: verified,
            pending_verifications: pending,
            failed_verifications: failed,
            total_discrepancy,
            warnings: all_warnings,
            is_fully_verified,
            results,
        }
    }

    /// Reconcile P&L using official market closure prices
    pub async fn reconcile_pnl(
        &self,
        trades: &[TradeRecord],
    ) -> Result<(f64, f64), Box<dyn Error>> {
        let mut recorded_pnl = 0.0;
        let mut official_pnl = 0.0;

        for trade in trades {
            if let Some(exit_price) = trade.exit_price {
                // Calculate recorded P&L (size/entry * exit - size)
                let shares = trade.size / trade.entry_price;
                let recorded_profit = (shares * exit_price) - trade.size;
                recorded_pnl += recorded_profit;

                // Get official price and calculate official P&L
                if let Ok(official_exit) = self
                    .verify_market_closure(&trade.market_id, &trade.side)
                    .await
                {
                    let official_profit = (shares * official_exit) - trade.size;
                    official_pnl += official_profit;
                }

                // Small delay to avoid rate limiting
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        }

        Ok((recorded_pnl, official_pnl))
    }
}
