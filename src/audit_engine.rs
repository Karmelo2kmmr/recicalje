use chrono::{DateTime, Local, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeAudit {
    pub market_id: String,
    pub coin: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub expected_exit: f64,
    pub size: f64,
    pub timestamp: DateTime<Utc>,
    pub fill_discrepancy: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct AuditData {
    pub trades: Vec<TradeAudit>,
    pub daily_equity: HashMap<String, f64>, // Key: "YYYY-MM-DD"
}

pub struct AuditEngine {
    pub data: AuditData,
    pub data_path: String,
}

impl AuditEngine {
    pub fn new(path: &str) -> Self {
        let data = if let Ok(content) = fs::read_to_string(path) {
            serde_json::from_str(&content).unwrap_or_default()
        } else {
            AuditData::default()
        };

        Self {
            data,
            data_path: path.to_string(),
        }
    }

    pub fn save(&self) -> Result<(), Box<dyn Error>> {
        let content = serde_json::to_string_pretty(&self.data)?;
        fs::write(&self.data_path, content)?;
        Ok(())
    }

    /// Record a new trade for auditing.
    pub fn record_trade(&mut self, trade: TradeAudit) {
        self.data.trades.push(trade);
        let _ = self.save();
    }

    /// Calculate ADVANCED metrics for the report.
    pub fn calculate_metrics(&self) -> Result<String, Box<dyn Error>> {
        if self.data.trades.is_empty() {
            return Ok("No trades recorded yet.".to_string());
        }

        let mut total_pnl = 0.0;
        let mut expected_pnl = 0.0;
        let mut worse_fills = 0;
        let mut market_counts: HashMap<String, (f64, i32)> = HashMap::new(); // (sum_pnl, count)

        for t in &self.data.trades {
            let actual_profit = (t.size / t.entry_price) * t.exit_price - t.size;
            let expected_profit = (t.size / t.entry_price) * t.expected_exit - t.size;

            total_pnl += actual_profit;
            expected_pnl += expected_profit;

            if t.exit_price < t.expected_exit {
                worse_fills += 1;
            }

            let entry = market_counts.entry(t.coin.clone()).or_insert((0.0, 0));
            entry.0 += actual_profit;
            entry.1 += 1;
        }

        let worse_fill_pct = (worse_fills as f64 / self.data.trades.len() as f64) * 100.0;

        // Heatmap: Sort markets by PnL
        let mut heatmap: Vec<_> = market_counts.into_iter().collect();
        heatmap.sort_by(|a, b| a.1 .0.partial_cmp(&b.1 .0).unwrap());

        // Max Drawdown (Simplified over all trades)
        let mut max_val: f64 = 0.0;
        let mut cur_val: f64 = 0.0;
        let mut max_dd: f64 = 0.0;
        for t in &self.data.trades {
            cur_val += (t.size / t.entry_price) * t.exit_price - t.size;
            max_val = max_val.max(cur_val);
            let dd = max_val - cur_val;
            max_dd = max_dd.max(dd);
        }

        // Sharpe Ratio Approximation (Standard Deviation of returns)
        let returns: Vec<f64> = self
            .data
            .trades
            .iter()
            .map(|t| (t.exit_price - t.entry_price) / t.entry_price)
            .collect();
        let avg_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns
            .iter()
            .map(|r| (r - avg_return).powi(2))
            .sum::<f64>()
            / returns.len() as f64;
        let sharpe = if variance > 0.0 {
            avg_return / variance.sqrt()
        } else {
            0.0
        };

        let mut report = format!(
            "📊 *REPORT UNIFICADO DE AUDITORÍA*\n\
            💰 PnL Real: ${:.2}\n\
            📈 PnL Esperado: ${:.2}\n\
            ⚠️ Peores Fills: {:.1}%\n\
            📉 Max Drawdown: ${:.2}\n\
            🎯 Sharpe Ratio (aprox): {:.2}\n\n\
            🔥 *HEATMAP DE MERCADOS (Peores)*\n",
            total_pnl, expected_pnl, worse_fill_pct, max_dd, sharpe
        );

        for (coin, (pnl, count)) in heatmap.iter().take(3) {
            report.push_str(&format!("• {}: ${:.2} ({} trades)\n", coin, pnl, count));
        }

        Ok(report)
    }

    /// Update the Daily Equity Curve.
    pub fn update_daily_equity(&mut self) {
        let today = Local::now().format("%Y-%m-%d").to_string();
        let total_pnl: f64 = self
            .data
            .trades
            .iter()
            .map(|t| (t.size / t.entry_price) * t.exit_price - t.size)
            .sum();
        self.data.daily_equity.insert(today, total_pnl);
        let _ = self.save();
    }
}
