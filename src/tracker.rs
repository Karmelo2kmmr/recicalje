use chrono::{DateTime, FixedOffset, Timelike};
use log::{debug, info, warn};
use regex::Regex;
use std::sync::atomic::{AtomicI64, AtomicUsize};
use std::sync::{Arc, Mutex};

use crate::api::Market;
use crate::entry_engine::EntryEngine;
use crate::execution_engine_safe::ExecutionEngine;
use crate::risk_engine::RiskEngine;
use crate::state_machine::{PositionState, StateMachine};
use crate::volatility::VolatilityMetrics;

// === GLOBAL DRAWDOWN PROTECTION ===
pub static CONSECUTIVE_LOSSES: AtomicUsize = AtomicUsize::new(0);
pub static COOL_DOWN_UNTIL: AtomicI64 = AtomicI64::new(0);
pub static CSV_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, PartialEq)]
pub enum MarketStatus {
    Scanning {
        max_yes: f64,
        max_no: f64,
        last_update: Option<std::time::SystemTime>,
    },
    Executed(f64, String), // Entry price, side
    Aborted(String),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TradeState {
    Scanning,
    OpenProtected,
    Closed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntryType {
    Antes,
    Dip,
    TriggerDirect,
    DipRecovery,
    Reentry,
    FullRecovery,
}

#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    Executed(String, f64, String), // Market, Price, Side
    Log(String),                   // Informational
    MarketExpired {
        coin: String,
        traded: bool,
        reason: Option<String>,
    },
}

/// Serializable snapshot for state recovery
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ActiveStateSnapshot {
    pub composite_key: String,
    pub market_id: String,
    pub question: String,
    pub binance_symbol: String,
    pub entry_price: f64,
    pub side: String,
    pub shares_held: f64,
    pub position_size: f64,
    pub active_stop_loss: Option<f64>,
    pub active_take_profit: Option<f64>,
    pub take_profit_price: f64,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub protective_order_id: Option<String>,
    pub entry_type: String,
    pub saved_at: String,
    pub protection_status: Option<String>,
    pub last_balance_check: Option<String>,
    pub exit_in_progress: Option<bool>,
    pub trade_state: String, // "Scanning", "OpenProtected", "Closed"
    pub ultra_aggressive_mode: bool,
    pub is_exiting: bool,
}

pub struct MarketTracker {
    pub market_id: String,
    pub yes_token_id: Option<String>,
    pub no_token_id: Option<String>,
    pub question: String,
    pub binance_symbol: String,

    // Engines
    pub state: StateMachine,
    pub risk: RiskEngine,
    pub entry: EntryEngine,
    pub execution: ExecutionEngine,

    // Metadata
    pub trigger_price: f64,
    pub max_entry_price: f64,
    pub min_entry_price: f64,
    pub vol_metrics: VolatilityMetrics,
    pub start_minutes: Option<u32>,
    pub position_size: f64,
    pub shares_held: f64,
    pub status: MarketStatus,
    pub has_traded_in_session: bool,
    pub active_stop_loss: Option<f64>,
    pub active_take_profit: Option<f64>,
    pub take_profit_price: f64,
    pub protective_order_id: Option<String>,

    // REINFORCED SL FIELDS
    pub trade_state: TradeState,
    pub ultra_aggressive_mode: bool,
    pub is_exiting: bool,
    pub stop_loss_triggered: bool,
    pub last_execution_time: Option<std::time::Instant>,
    pub stats: Arc<crate::stats::StatsEngine>,
    pub has_won_this_window: bool,
    pub sl_count: u8,

    // LOBO 2026: New Execution Timers
    pub trigger_hit_at: Option<std::time::Instant>,
    pub recovery_hit_at: Option<std::time::Instant>,
    pub last_binance_price_update: Option<f64>,
    pub price_to_beat: Option<f64>,
    pub binance_entry_reference: Option<f64>,
    pub last_block_reason: String,
}

impl MarketTracker {
    async fn refresh_protective_order(
        &mut self,
        client: &reqwest::Client,
        token_id: &str,
        shares: f64,
    ) {
        if shares <= Self::min_sell_qty() {
            return;
        }

        let paper_trading = std::env::var("PAPER_TRADING")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);

        // In live trading we cannot simulate a stop-loss with a resting GTC sell:
        // a sell below the market becomes immediately executable and closes early.
        // Keep the position tracked locally and let the real stop trigger only when
        // `check()` sees the live exit bid reach the configured stop level.
        if !paper_trading {
            if let Some(existing_id) = self.protective_order_id.take() {
                let _ = crate::api::cancel_protective_order(client, &existing_id).await;
            }
            self.trade_state = TradeState::OpenProtected;
            info!(
                "Live protective GTC disabled for {}. Logical stop remains armed at {:.4} for {:.6} shares.",
                self.question, self.risk.active_stop_loss, shares
            );
            return;
        }

        if let Some(existing_id) = self.protective_order_id.take() {
            let _ = crate::api::cancel_protective_order(client, &existing_id).await;
        }

        match crate::api::place_protective_limit_sell(
            client,
            token_id,
            shares,
            self.risk.active_stop_loss,
            &self.market_id,
        )
        .await
        {
            Ok(resp) => {
                self.protective_order_id = Some(resp.order_id.clone());
                self.trade_state = TradeState::OpenProtected;
                info!(
                    "Protective order armed for {} at {:.4} with {:.6} shares",
                    self.question, self.risk.active_stop_loss, shares
                );
            }
            Err(e) => {
                warn!(
                    "Could not arm protective order for {} at {:.4}: {}",
                    self.question, self.risk.active_stop_loss, e
                );
            }
        }
    }

    async fn clear_protective_order(&mut self, client: &reqwest::Client) {
        if let Some(existing_id) = self.protective_order_id.take() {
            let _ = crate::api::cancel_protective_order(client, &existing_id).await;
        }
    }

    fn reentries_enabled() -> bool {
        std::env::var("ALLOW_REENTRIES")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    fn dca_enabled() -> bool {
        std::env::var("ALLOW_DCA")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(true)
    }

    fn min_sell_qty() -> f64 {
        std::env::var("MIN_SELL_QTY")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.01)
    }

    fn time_exit_trigger_secs() -> i32 {
        std::env::var("TIME_EXIT_TRIGGER_SECS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .map(|secs| secs.clamp(780, 895))
            .unwrap_or(865)
    }

    fn time_exit_enabled() -> bool {
        std::env::var("ENABLE_TIME_EXIT")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }

    fn dca_sl_gap() -> f64 {
        std::env::var("DCA_SL_GAP")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.02)
            .max(0.0)
    }

    fn is_terminal_exit_error(err_msg: &str) -> bool {
        let msg = err_msg.to_lowercase();
        msg.contains("orderbook does not exist")
            || msg.contains("the orderbook")
    }

    fn is_sol_market(&self) -> bool {
        self.binance_symbol.contains("SOL")
    }

    fn is_expensive_entry(price: f64) -> bool {
        price >= 0.89
    }

    fn is_late_hour_block(now: DateTime<FixedOffset>) -> bool {
        matches!(now.minute(), 8..=12 | 25..=28 | 39..=43 | 55..=59)
    }

    fn should_block_expensive_late_entry(
        &self,
        now: DateTime<FixedOffset>,
        price: f64,
    ) -> bool {
        if std::env::var("DISABLE_LATE_EXPENSIVE_ENTRY_BLOCK")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
        {
            return false;
        }

        Self::is_expensive_entry(price) && Self::is_late_hour_block(now)
    }

    fn asset_label(&self) -> String {
        format!("{}-15M", self.binance_symbol.replace("USDT", ""))
    }

    fn adjusted_trigger_from_orderbook(
        &self,
        base_trigger: f64,
        metrics: &crate::api::OrderbookMetrics,
    ) -> f64 {
        if std::env::var("DISABLE_ORDERBOOK_TRIGGER_ADJUSTMENT")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
        {
            return base_trigger;
        }

        let bid_pressure: f64 = metrics.bids_depth.iter().map(|(_, size)| *size).sum();
        let ask_pressure: f64 = metrics.asks_depth.iter().map(|(_, size)| *size).sum();
        let imbalance = if ask_pressure > 0.0 {
            bid_pressure / ask_pressure
        } else {
            0.0
        };

        let mut trigger = base_trigger;

        if metrics.liquidity_score < 0.25 {
            trigger = trigger.min(0.880);
        }

        if imbalance > 3.0 {
            trigger = trigger.max(0.892);
        }

        trigger
    }

    fn is_valid_reentry_price(&self, price: f64) -> bool {
        price < 0.840 || (0.890..=0.910).contains(&price)
    }

    fn is_sane_contract_price(price: f64) -> bool {
        price.is_finite() && (0.05..=0.99).contains(&price)
    }

    fn format_pnl(pnl: f64) -> String {
        if pnl >= 0.0 {
            format!("+${:.2}", pnl)
        } else {
            format!("-${:.2}", pnl.abs())
        }
    }

    fn format_return_pct(pnl: f64, size: f64) -> String {
        if size <= 0.0 {
            return "0.00%".to_string();
        }

        let pct = (pnl / size) * 100.0;
        if pct >= 0.0 {
            format!("+{:.2}%", pct)
        } else {
            format!("{:.2}%", pct)
        }
    }

    fn format_price_delta(current: f64, reference: Option<f64>) -> String {
        if let Some(price_to_beat) = reference.filter(|v| *v > 0.0) {
            let delta = current - price_to_beat;
            if delta.abs() < 0.01 {
                if delta >= 0.0 {
                    format!("+{:.4} USD", delta)
                } else {
                    format!("{:.4} USD", delta)
                }
            } else {
                if delta >= 0.0 {
                    format!("+{:.2} USD", delta)
                } else {
                    format!("{:.2} USD", delta)
                }
            }
        } else {
            "N/A".to_string()
        }
    }

    fn danger_exit_buffer() -> f64 {
        std::env::var("BINANCE_EARLY_EXIT_BUFFER")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.04)
            .clamp(0.01, 0.10)
    }

    fn early_exit_distance_threshold(&self) -> f64 {
        let base_threshold =
            if self.binance_symbol.contains("SOL") || self.binance_symbol.contains("XRP") {
                match self.vol_metrics.state {
                    crate::volatility::VolatilityState::LowNeutral => 0.00123,
                    crate::volatility::VolatilityState::NeutralHigh
                    | crate::volatility::VolatilityState::HighSuperhigh => 0.00150,
                }
            } else {
                match self.vol_metrics.state {
                    crate::volatility::VolatilityState::LowNeutral => 0.00102,
                    crate::volatility::VolatilityState::NeutralHigh
                    | crate::volatility::VolatilityState::HighSuperhigh => 0.00146,
                }
            };

        let early_threshold: f64 = base_threshold * 0.70_f64;
        early_threshold.max(0.00030_f64)
    }

    fn should_preemptive_exit(&self, current_exit_price: f64) -> Option<(f64, f64)> {
        if self.state.side.is_empty() || self.vol_metrics.current_price <= 0.0 {
            return None;
        }

        let reference = self
            .binance_entry_reference
            .or(self.price_to_beat)
            .filter(|price| *price > 0.0)?;

        let pct_change = (self.vol_metrics.current_price - reference) / reference;
        let adverse_move = if self.state.side == "UP" {
            -pct_change
        } else {
            pct_change
        };
        if adverse_move <= 0.0 {
            return None;
        }

        let threshold = self.early_exit_distance_threshold();
        let danger_zone =
            current_exit_price <= (self.risk.active_stop_loss + Self::danger_exit_buffer());

        if danger_zone && adverse_move >= threshold {
            Some((adverse_move, threshold))
        } else {
            None
        }
    }

    pub fn new(
        m: Market,
        override_trigger: Option<f64>,
        price_to_beat: Option<f64>,
        vol_metrics: VolatilityMetrics,
        binance_symbol: String,
        _recovery_streak: Arc<AtomicUsize>,
        stats: Arc<crate::stats::StatsEngine>,
    ) -> Self {
        let trigger_price = override_trigger.unwrap_or(0.885);
        let max_entry_price = std::env::var("MAX_ENTRY_PRICE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.91);
        let min_entry_price = std::env::var("MIN_ENTRY_PRICE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.86);

        let (yes_token_id, no_token_id) = m
            .clob_token_ids
            .as_ref()
            .and_then(|s| {
                let ids: Result<Vec<String>, _> = serde_json::from_str(s);
                ids.ok().map(|v| (v.get(0).cloned(), v.get(1).cloned()))
            })
            .unwrap_or((None, None));

        let start_minutes = Self::parse_start_time(&m.question);
        let position_size = std::env::var("POSITION_SIZE")
            .unwrap_or("6.0".to_string())
            .parse()
            .unwrap_or(6.0);

        Self {
            market_id: m.id.clone(),
            yes_token_id,
            no_token_id,
            question: m.question.clone(),
            binance_symbol,

            state: StateMachine::new(),
            risk: RiskEngine::new(position_size, false, false),
            entry: EntryEngine::new(),
            execution: ExecutionEngine::new(),

            trigger_price,
            max_entry_price,
            min_entry_price,
            vol_metrics,
            start_minutes,
            position_size,
            shares_held: 0.0,
            status: MarketStatus::Scanning {
                max_yes: 0.0,
                max_no: 0.0,
                last_update: None,
            },
            has_traded_in_session: false,
            active_stop_loss: None,
            active_take_profit: None,
            take_profit_price: 0.97,
            protective_order_id: None,

            // REINFORCED SL DEFAULTS
            trade_state: TradeState::Scanning,
            ultra_aggressive_mode: false,
            is_exiting: false,
            stop_loss_triggered: false,
            last_execution_time: None,
            stats,
            has_won_this_window: false,
            sl_count: 0,
            trigger_hit_at: None,
            recovery_hit_at: None,
            last_binance_price_update: None,
            price_to_beat,
            binance_entry_reference: None,
            last_block_reason: "trigger blocked".to_string(),
        }
    }

    pub fn force_restore_position_state(&mut self) {
        if let MarketStatus::Executed(_, _) = self.status {
            self.trade_state = TradeState::OpenProtected;
            self.is_exiting = false;
            self.has_traded_in_session = true;
            self.active_stop_loss = Some(crate::risk_engine::HARD_SL_PRICE);
            self.stop_loss_triggered = false;

            // Si ya estaba cerca del SL, activar monitoreo agresivo
            // get_current_price_approx uses the last tracked entry/DCA price
            if self.state.entry_price <= 0.72 {
                self.ultra_aggressive_mode = true;
            }

            info!(
                "✅ FORCE RESTORE COMPLETE → {} | Shares: {:.6} | SL: 0.650",
                self.question, self.shares_held
            );
        }
    }

    pub fn get_token_id(&self) -> String {
        if self.state.side == "UP" {
            self.yes_token_id.clone().unwrap_or_default()
        } else {
            self.no_token_id.clone().unwrap_or_default()
        }
    }

    pub async fn update_trade_exit_confirmed(
        &mut self,
        price: f64,
        reason: &str,
        intent: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        info!(
            "📝 RECORDING EXIT: {} @ {:.4} (Reason: {})",
            self.question, price, reason
        );
        let _ = self
            .stats
            .update_csv_exit_price(&self.market_id, price, intent, true, Some(price));
        Ok(())
    }

    fn record_trade_entry(&self, entry_price: f64, size: f64, entry_type: &str) {
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let record = crate::stats::TradeRecord {
            Timestamp: now,
            MarketID: self.market_id.clone(),
            Question: self.question.clone(),
            Coin: self.binance_symbol.replace("USDT", ""),
            EntryPrice: entry_price,
            TradeType: "PAPER_BUY".to_string(),
            Side: self.state.side.clone(),
            ExitPrice: None,
            Size: size,
            EntryType: entry_type.to_string(),
            SL_Price: format!("{:.3}", crate::risk_engine::HARD_SL_PRICE),
            TP_Price: "0.970".to_string(),
            R_Ratio: "N/A".to_string(),
            Vol_Now: format!("{:.4}", self.vol_metrics.z_score),
            Vol_MA20: "N/A".to_string(),
            Vol_State: format!("{:?}", self.vol_metrics.state),
            Trigger_Price: format!("{:.4}", self.trigger_price),
            setup_tag: None,
            entry_bucket: None,
            signal_score: None,
            reason_entry: Some("TrendHammer Trigger".to_string()),
            reason_exit: None,
            holding_seconds: None,
            max_favor: None,
            max_adverse: None,
            market_regime: None,
            ExitIntent: None,
            ExitReason: None,
            ExitConfirmed: None,
            ExitOrderId: None,
            ExitFilledShares: None,
            ExitAvgFillPrice: None,
            ExitTimestamp: None,
        };
        let _ = self.stats.record_entry_to_csv(&record);
    }

    pub async fn check(
        &mut self,
        asks: (Option<f64>, Option<f64>),
        bids: (Option<f64>, Option<f64>),
        client: &reqwest::Client,
        now: DateTime<FixedOffset>,
        vol_metrics: VolatilityMetrics,
        can_trigger_global: bool,
    ) -> Vec<ExecutionEvent> {
        let (yes_ask, no_ask) = (asks.0.unwrap_or(0.0), asks.1.unwrap_or(0.0));
        let (yes_bid, no_bid) = (bids.0.unwrap_or(0.0), bids.1.unwrap_or(0.0));
        let mut events = Vec::new();

        // --- TIME WINDOW CALCULATIONS (FORCED ET) ---
        let mut elapsed_secs = 0;
        if let Some(start) = self.start_minutes {
            // now is already passed as ET from main.rs (loop_time)
            let h = now.hour();
            let m = now.minute();
            let s = now.second();
            let current_total_secs = (h * 3600 + m * 60 + s) as i32;
            let start_total_secs = start as i32 * 60;
            elapsed_secs = (current_total_secs - start_total_secs).rem_euclid(86400);
        }

        // --- HARD SL BULLETPROOF ---
        let current_exit_price = if self.state.side == "UP" {
            yes_bid
        } else {
            no_bid
        };

        if self.shares_held > 0.0
            && current_exit_price <= crate::risk_engine::HARD_SL_PRICE
            && !self.is_exiting
        {
            self.is_exiting = true;
            let tid = self.get_token_id();

            match self
                .execution
                .close_position(
                    client,
                    &tid,
                    self.shares_held,
                    current_exit_price,
                    "HARD_SL",
                )
                .await
            {
                Ok(resp) => {
                    let exit_price = resp.fill_price.unwrap_or(current_exit_price);
                    self.clear_protective_order(client).await;
                    let _ = self
                        .update_trade_exit_confirmed(exit_price, "HARD_SL_FORCED", "FORCED")
                        .await;
                    self.shares_held = 0.0;
                    self.status = MarketStatus::Aborted("Hard SL Ejecutado".to_string());
                    self.trade_state = TradeState::Closed;
                    self.state.reset();
                    return vec![ExecutionEvent::Log(format!(
                        "SL protegido ejecutado @ {:.4} en {}",
                        exit_price, self.question
                    ))];
                }
                Err(e) => {
                    self.is_exiting = false;
                    let err_msg = e.to_string();
                    let remaining_balance = crate::api::get_actual_balance(&tid)
                        .await
                        .unwrap_or(self.shares_held);
                    self.shares_held = remaining_balance;
                    self.state.shares_held = remaining_balance;

                    // ANTI-SPAM: Abort on ghost positions or dust remnants
                    if err_msg.contains("balance is exactly 0.0")
                        || remaining_balance <= Self::min_sell_qty()
                    {
                        self.clear_protective_order(client).await;
                        self.status = MarketStatus::Aborted("Balance Exhausted".to_string());
                        self.trade_state = TradeState::Closed;
                        self.state.reset();
                        self.shares_held = 0.0;
                        self.state.shares_held = 0.0;
                        return vec![ExecutionEvent::Log(format!(
                            "SL reconciliado en {}. El remanente quedo por debajo del minimo operativo y se limpia el estado local.",
                            self.question
                        ))];
                    }

                    if Self::is_terminal_exit_error(&err_msg) {
                        self.status = MarketStatus::Aborted(
                            "Market Terminal On Settlement Path".to_string(),
                        );
                        self.trade_state = TradeState::Closed;
                        self.state.reset();
                        self.shares_held = 0.0;
                        return vec![ExecutionEvent::Log(format!(
                            "Salida terminal sin liquidez/libro en {}. Se detienen reintentos y la posicion pasa a settlement/redeem.",
                            self.question
                        ))];
                    }

                    return vec![ExecutionEvent::Log(format!(
                        "SL detectado en {} pero la salida quedo incompleta: {} | remanente {:.4}",
                        self.question, e, remaining_balance
                    ))];
                }
            }
        }

        // 1. HARD STOP LOSS (PRICE)
        if false
            && self.shares_held > 0.0
            && current_exit_price <= crate::risk_engine::HARD_SL_PRICE
            && !self.is_exiting
        {
            self.is_exiting = true;
            info!(
                "🚨 HARD SL TRIGGERED @ {:.4} (Market: {}) → MARKET SELL INMEDIATO",
                current_exit_price, self.question
            );

            let tid = self.get_token_id();
            let _ = crate::api::place_market_sell(
                client,
                &tid,
                self.shares_held,
                0.01, // precio mínimo para forzar fill
            )
            .await;

            // Registrar salida confirmada
            let _ = self
                .update_trade_exit_confirmed(current_exit_price, "HARD_SL_FORCED", "FORCED")
                .await;
            self.status = MarketStatus::Aborted("Hard SL Ejecutado".to_string());
            self.trade_state = TradeState::Closed;
            self.state.reset();

            return vec![ExecutionEvent::Log(format!(
                "🔴 HARD SL EJECUTADO @ {:.4} en {}",
                current_exit_price, self.question
            ))];
        }

        let time_exit_trigger_secs = Self::time_exit_trigger_secs();

        if Self::time_exit_enabled()
            && self.shares_held > 0.0
            && elapsed_secs >= time_exit_trigger_secs
            && !self.is_exiting
        {
            self.is_exiting = true;
            let tid = self.get_token_id();

            match self
                .execution
                .close_position(
                    client,
                    &tid,
                    self.shares_held,
                    current_exit_price.max(0.10),
                    "TIME_EXIT",
                )
                .await
            {
                Ok(resp) => {
                    let exit_price = resp.fill_price.unwrap_or(current_exit_price);
                    self.clear_protective_order(client).await;
                    let _ = self
                        .update_trade_exit_confirmed(exit_price, "SAFETY_TIME_EXIT", "TIME")
                        .await;
                    self.shares_held = 0.0;
                    self.status = MarketStatus::Aborted(format!(
                        "Safety Time Exit ({}s)",
                        time_exit_trigger_secs
                    ));
                    self.trade_state = TradeState::Closed;
                    self.state.reset();
                    return vec![ExecutionEvent::Log(format!(
                        "TIME EXIT protegido @ {:.4} en {}",
                        exit_price, self.question
                    ))];
                }
                Err(e) => {
                    self.is_exiting = false;
                    let err_msg = e.to_string();
                    let remaining_balance = crate::api::get_actual_balance(&tid)
                        .await
                        .unwrap_or(self.shares_held);
                    self.shares_held = remaining_balance;
                    self.state.shares_held = remaining_balance;

                    // ANTI-SPAM: Abort on ghost positions or dust remnants
                    if err_msg.contains("balance is exactly 0.0")
                        || remaining_balance <= Self::min_sell_qty()
                    {
                        self.clear_protective_order(client).await;
                        self.status =
                            MarketStatus::Aborted("Balance Exhausted (TIME_EXIT)".to_string());
                        self.trade_state = TradeState::Closed;
                        self.state.reset();
                        self.shares_held = 0.0;
                        self.state.shares_held = 0.0;
                        return vec![ExecutionEvent::Log(format!(
                            "TIME EXIT reconciliado en {}. El remanente quedo por debajo del minimo operativo y se limpia el estado local.",
                            self.question
                        ))];
                    }

                    if Self::is_terminal_exit_error(&err_msg) {
                        self.status =
                            MarketStatus::Aborted("Awaiting Redemption (TIME_EXIT)".to_string());
                        self.trade_state = TradeState::Closed;
                        self.state.reset();
                        self.shares_held = 0.0;
                        return vec![ExecutionEvent::Log(format!(
                            "TIME EXIT sin liquidez/libro en {}. Se detienen reintentos y la posicion pasa a liquidacion/redeem.",
                            self.question
                        ))];
                    }

                    return vec![ExecutionEvent::Log(format!(
                        "TIME EXIT detectado en {} pero la salida falló: {}",
                        self.question, err_msg
                    ))];
                }
            }
        }

        // --- ULTRA-AGGRESSIVE DETECTOR (LOBO 2026) ---
        // Rule: If Binance moves > 0.12% in seconds, activate Trend Hammer
        let mut trend_hammer_active = false;
        if let Some(last_binance) = self.last_binance_price_update {
            let binance_move =
                (self.vol_metrics.current_price - last_binance).abs() / last_binance * 100.0;
            if binance_move >= 0.12 {
                if !self.ultra_aggressive_mode {
                    info!("🚀 TREND HAMMER DETECTED: Binance moved {:.3}% in seconds! Activating Ultra-Aggressive Mode for {}", binance_move, self.question);
                    self.ultra_aggressive_mode = true;
                }
                trend_hammer_active = true;
            }
        }
        self.last_binance_price_update = Some(self.vol_metrics.current_price);

        // 2. HARD SAFETY EXIT (TIME) - Minute 14:30 (870 seconds)
        if false && self.shares_held > 0.0 && elapsed_secs >= 870 && !self.is_exiting {
            self.is_exiting = true;
            info!("🕒 SAFETY TIME EXIT (Minute 14:30) @ {:.4} (Market: {}) → CERRANDO POSICIÓN PROACTIVAMENTE", current_exit_price, self.question);

            let tid = self.get_token_id();
            let _ = crate::api::place_market_sell(client, &tid, self.shares_held, 0.01).await;

            let _ = self
                .update_trade_exit_confirmed(current_exit_price, "SAFETY_TIME_EXIT", "TIME")
                .await;
            self.status = MarketStatus::Aborted("Safety Time Exit (14:30)".to_string());
            self.trade_state = TradeState::Closed;
            self.state.reset();

            return vec![ExecutionEvent::Log(format!(
                "🕒 TIME EXIT PROACTIVO (14:30) @ {:.4} en {}",
                current_exit_price, self.question
            ))];
        }

        // Update volatility state
        self.vol_metrics = vol_metrics;
        self.trigger_price = self.vol_metrics.trigger_price;

        // 1. PROACTIVE EXPIRATION (15 min window)
        // Move to top to prevent scanning/trading expired markets
        if let Some(_) = self.start_minutes {
            if elapsed_secs >= 900 {
                if self.shares_held > 0.0
                    && !matches!(
                        &self.status,
                        MarketStatus::Aborted(reason)
                            if reason == "Awaiting Settlement" || reason == "Awaiting Redemption"
                    )
                {
                    self.status = MarketStatus::Aborted("Awaiting Settlement".to_string());
                    self.trade_state = TradeState::Closed;
                    self.state.reset();
                    self.is_exiting = false;
                    self.shares_held = 0.0;
                    self.state.shares_held = 0.0;
                    return vec![ExecutionEvent::Log(format!(
                        "Mercado cerrado sin TP/SL en {}. La posicion queda a settlement/redeem y el resultado final se definira por resolucion.",
                        self.question
                    ))];
                }

                let already_expired = matches!(&self.status, MarketStatus::Aborted(reason) if reason == "Market Expired");
                let can_emit_expired = matches!(
                    self.state.current_state,
                    PositionState::Scanning | PositionState::RecoveryScanning
                );

                if can_emit_expired && !already_expired {
                    self.status = MarketStatus::Aborted("Market Expired".to_string());
                    return vec![ExecutionEvent::MarketExpired {
                        coin: self.binance_symbol.replace("USDT", ""),
                        traded: self.has_traded_in_session,
                        reason: Some(self.last_block_reason.clone()),
                    }];
                }
            }
        }

        // 2. HIBERNATION (0 - 6:00 min)
        if EntryEngine::is_hibernation_window(elapsed_secs) {
            if now.second() % 30 == 0 {
                debug!(
                    "❄️ Market Hibernate ({}) - {}m elapsed",
                    self.question,
                    elapsed_secs / 60
                );
            }
            return events;
        }

        // --- CORE STATE MACHINE ---
        match self.state.current_state {
            PositionState::Scanning => {
                if !can_trigger_global {
                    self.last_block_reason = "trigger blocked".to_string();
                    return events;
                }

                // 0. TIME BLOCK: No entries after 13:35 minutes (815s)
                if elapsed_secs >= 815 {
                    self.last_block_reason = "trigger blocked".to_string();
                    if now.second() % 60 == 0 {
                        info!(
                            "⏳ ENTRY BLOCKED: Safety time threshold reached (13:35+) for {}",
                            self.question
                        );
                    }
                    return events;
                }

                // 1. RISK CHECK: Max 2 SL per market
                if self.sl_count >= 2 {
                    self.last_block_reason = "trigger blocked".to_string();
                    if now.second() % 60 == 0 {
                        info!(
                            "🛑 ENTRY BLOCKED: Max SL limit (2) reached for {}",
                            self.question
                        );
                    }
                    return events;
                }
                if !self.entry.check_volatility_filter(&self.vol_metrics) {
                    self.last_block_reason = "trigger blocked".to_string();
                    return events;
                }

                let mut yes_trigger = self.trigger_price;
                let mut no_trigger = self.trigger_price;

                if let Some(yes_tid) = self.yes_token_id.as_ref() {
                    if yes_ask >= 0.84 && yes_ask <= self.max_entry_price {
                        let metrics = crate::api::get_orderbook_depth(client, yes_tid).await;
                        yes_trigger =
                            self.adjusted_trigger_from_orderbook(self.trigger_price, &metrics);
                    }
                }

                if let Some(no_tid) = self.no_token_id.as_ref() {
                    if no_ask >= 0.84 && no_ask <= self.max_entry_price {
                        let metrics = crate::api::get_orderbook_depth(client, no_tid).await;
                        no_trigger =
                            self.adjusted_trigger_from_orderbook(self.trigger_price, &metrics);
                    }
                }

                let mut buy_yes = self.entry.evaluate_triggers(
                    yes_ask,
                    yes_trigger,
                    self.max_entry_price,
                    self.min_entry_price,
                );
                let mut buy_no = self.entry.evaluate_triggers(
                    no_ask,
                    no_trigger,
                    self.max_entry_price,
                    self.min_entry_price,
                );

                // 2. RE-ENTRY ENFORCEMENT
                if self.has_won_this_window && Self::reentries_enabled() {
                    buy_yes = if Self::is_sane_contract_price(yes_ask) && yes_ask < 0.840 {
                        true
                    } else {
                        buy_yes
                            && Self::is_sane_contract_price(yes_ask)
                            && self.is_valid_reentry_price(yes_ask)
                    };

                    buy_no = if Self::is_sane_contract_price(no_ask) && no_ask < 0.840 {
                        true
                    } else {
                        buy_no
                            && Self::is_sane_contract_price(no_ask)
                            && self.is_valid_reentry_price(no_ask)
                        };
                } else if self.has_won_this_window {
                    buy_yes = false;
                    buy_no = false;
                    self.last_block_reason = "follow-up entry blocked".to_string();
                }

                if self.is_sol_market() {
                    buy_no = false;
                }

                if !buy_yes && !buy_no {
                    self.last_block_reason = "trigger blocked".to_string();
                }


                if buy_yes || buy_no {
                    let side = if buy_yes { "UP" } else { "DOWN" };
                    let price = if buy_yes { yes_ask } else { no_ask };
                    let selected_trigger = if buy_yes { yes_trigger } else { no_trigger };
                    let token_id = if buy_yes {
                        self.yes_token_id.as_ref()
                    } else {
                        self.no_token_id.as_ref()
                    };

                    if !Self::is_sane_contract_price(price) {
                        self.last_block_reason = "trigger blocked".to_string();
                        warn!(
                            "Entry blocked for {}: invalid book price {:.4} on side {}",
                            self.question, price, side
                        );
                        return events;
                    }

                    if self.is_sol_market() && side == "DOWN" {
                        self.last_block_reason = "sol down blocked".to_string();
                        info!("SOL DOWN blocked for {}", self.question);
                        return events;
                    }

                    if self.should_block_expensive_late_entry(now, price) {
                        self.last_block_reason = "late expensive entry blocked".to_string();
                        info!(
                            "Late expensive entry blocked for {} | side={} | price={:.3} | minute={:02}",
                            self.question,
                            side,
                            price,
                            now.minute()
                        );
                        return events;
                    }

                    // --- LOBO 2026: EXECUTION TIMERS & CONFIRMATION ---
                    if !trend_hammer_active && !self.ultra_aggressive_mode {
                        // 1. MOMENTUM DELAY (5s)
                        if self.trigger_hit_at.is_none() {
                            info!(
                                "⏱️ TRIGGER REACHED ({:.3}): Starting 5s Momentum Delay for {}",
                                price, self.question
                            );
                            self.trigger_hit_at = Some(std::time::Instant::now());
                            return events;
                        }

                        let wait_time = if self.vol_metrics.state
                            == crate::volatility::VolatilityState::NeutralHigh
                            && self.vol_metrics.z_score > 1.8
                        {
                            23.0 // SHOT FILTER: 23s confirmation for high Z-Score/Neutral
                        } else {
                            5.0 // STANDARD: 5s momentum delay
                        };

                        if self.trigger_hit_at.unwrap().elapsed().as_secs_f64() < wait_time {
                            return events; // Wait for timer
                        }
                    } else if trend_hammer_active {
                        info!("🔨 TREND HAMMER: Skipping delays for {} entry!", side);
                    }

                    if let Some(tid) = token_id {
                        let token_id_owned = tid.clone();
                        if let Ok(resp) = crate::api::place_initial_buy(
                            client,
                            tid,
                            price,
                            self.position_size,
                            &self.market_id,
                        )
                        .await
                        {
                            // ... rest of logic
                            let actual_price = resp.fill_price.unwrap_or(price);
                            if resp.shares > 0.0 {
                                self.last_block_reason = "operated".to_string();
                                self.trigger_price = selected_trigger;
                                self.state.transition_to(PositionState::InPosition);
                                self.state.side = side.to_string();
                                self.state.entry_price = actual_price;
                                self.state.shares_held = resp.shares;
                                self.shares_held = resp.shares;
                                self.binance_entry_reference = Some(self.vol_metrics.current_price);
                                self.has_traded_in_session = true;
                                self.state.is_reentry = self.has_won_this_window;
                                self.risk = RiskEngine::new(
                                    self.position_size,
                                    false,
                                    self.state.is_reentry,
                                );
                                self.status =
                                    MarketStatus::Executed(actual_price, side.to_string());
                                self.active_stop_loss = Some(self.risk.active_stop_loss);
                                self.active_take_profit = Some(self.risk.active_take_profit);
                                self.record_trade_entry(
                                    actual_price,
                                    self.position_size,
                                    "Initial",
                                );
                                self.refresh_protective_order(
                                    client,
                                    &token_id_owned,
                                    resp.shares,
                                )
                                .await;
                                self.trigger_hit_at = None;
                            } else {
                                log::warn!("⚠️ [ZERO-FILL] Buy order accepted but 0.0 shares received for {}. Aborting entry.", self.question);
                            }

                            if resp.shares > 0.0 {
                                let msg = format!(
                                    "🚀 *ENTRADA DETECTADA*\n\
• Activo: *{}*\n\
• Dirección: *{}*\n\
• Price to beat: {:.2}\n\
• {} actual: {}\n\
• Precio entrada: {:.3}\n\
• Monto: ${:.2}",
                                    self.asset_label(),
                                    side,
                                    self.price_to_beat.unwrap_or(0.0),
                                    self.binance_symbol.replace("USDT", ""),
                                    Self::format_price_delta(
                                        self.vol_metrics.current_price,
                                        self.price_to_beat
                                    ),
                                    actual_price,
                                    self.position_size
                                );

                                events.push(ExecutionEvent::Log(msg));
                            }
                        }
                    }
                } else {
                    self.trigger_hit_at = None;
                }
            }

            PositionState::InPosition | PositionState::PendingDCA => {
                let current_exit_price = if self.state.side == "UP" {
                    yes_bid
                } else {
                    no_bid
                };
                let current_ask_price = if self.state.side == "UP" {
                    yes_ask
                } else {
                    no_ask
                };
                let token_id = if self.state.side == "UP" {
                    self.yes_token_id.as_ref()
                } else {
                    self.no_token_id.as_ref()
                };

                if let Some(tid) = token_id {
                    let token_id_owned = tid.clone();
                    if let Some((adverse_move, threshold)) =
                        self.should_preemptive_exit(current_exit_price)
                    {
                        let actual_shares = match crate::api::get_actual_balance(tid).await {
                            Ok(bal) => {
                                if bal > 0.0 {
                                    bal
                                } else {
                                    self.state.shares_held
                                }
                            }
                            Err(_) => self.state.shares_held,
                        };

                        if actual_shares > Self::min_sell_qty() {
                            let early_exit_target = current_exit_price.max(0.10);
                            info!(
                                "BINANCE EARLY EXIT for {}: adverse move {:.4}% >= {:.4}% with bid {:.3} near SL {:.3}",
                                self.question,
                                adverse_move * 100.0,
                                threshold * 100.0,
                                current_exit_price,
                                self.risk.active_stop_loss
                            );

                            if let Ok(resp) = self
                                .execution
                                .close_position(
                                    client,
                                    tid,
                                    actual_shares,
                                    early_exit_target,
                                    "EARLY_WARNING",
                                )
                                .await
                            {
                                let side = self.state.side.clone();
                                let exit_price = resp.fill_price.unwrap_or(current_exit_price);
                                let pnl = (exit_price - self.state.entry_price) * actual_shares;
                                self.clear_protective_order(client).await;
                                self.shares_held = 0.0;
                                self.state.shares_held = 0.0;
                                self.binance_entry_reference = None;
                                self.status =
                                    MarketStatus::Aborted("BINANCE_EARLY_WARNING".to_string());
                                self.state.reset();
                                let _ = self
                                    .update_trade_exit_confirmed(
                                        exit_price,
                                        "BINANCE_EARLY_WARNING",
                                        "EARLY",
                                    )
                                    .await;
                                events.push(ExecutionEvent::Log(format!(
                                    "⚠️ *SALIDA TEMPRANA POR BINANCE*\n\
• Activo: *{}*\n\
• Dirección: *{}*\n\
• Precio cierre: {:.3}\n\
• Binance en contra: {:.3}%\n\
• Umbral activado: {:.3}%\n\
• P&L: {}\n\
• Estado: salida preventiva antes del SL duro",
                                    self.asset_label(),
                                    side,
                                    exit_price,
                                    adverse_move * 100.0,
                                    threshold * 100.0,
                                    Self::format_pnl(pnl)
                                )));
                                return events;
                            }
                        }
                    }

                    // 1. HARD STOP LOSS (Force balance check)
                    if self.risk.should_hard_exit(current_exit_price) {
                        // RECONCILE: Always check actual blockchain balance before SL
                        let actual_shares = match crate::api::get_actual_balance(tid).await {
                            Ok(bal) => {
                                if bal > 0.0 {
                                    bal
                                } else {
                                    self.state.shares_held
                                }
                            }
                            Err(_) => self.state.shares_held,
                        };

                        info!(
                            "💀 HARD SL TRIGGERED at {:.3} for {} (Qty: {:.4})",
                            self.risk.active_stop_loss, self.question, actual_shares
                        );
                        if actual_shares <= Self::min_sell_qty() {
                            warn!(
                                "Ghost position detected on SL for {}. Balance already zero.",
                                self.question
                            );
                            self.shares_held = 0.0;
                            self.state.shares_held = 0.0;
                            self.status = MarketStatus::Aborted("Balance Exhausted".to_string());
                            self.state.reset();
                            return vec![ExecutionEvent::Log(format!(
                                "Posicion cerrada sin saldo disponible en {}. Se limpia el estado local.",
                                self.question
                            ))];
                        }

                        match self
                            .execution
                            .close_position(
                                client,
                                tid,
                                actual_shares,
                                current_exit_price,
                                "HARD_SL",
                            )
                            .await
                        {
                            Ok(resp) => {
                                let was_primary = self.risk.active_stop_loss > 0.50; // Simple check
                                let side = self.state.side.clone();
                                let entry_price = self.state.entry_price;
                                let exit_price = resp.fill_price.unwrap_or(current_exit_price);
                                let pnl = (exit_price - self.state.entry_price) * actual_shares;
                                let return_pct = Self::format_return_pct(pnl, self.position_size);

                                self.clear_protective_order(client).await;
                                self.shares_held = 0.0;
                                self.binance_entry_reference = None;
                                self.sl_count += 1;
                                self.status = MarketStatus::Aborted("HARD_SL".to_string());

                                // Registrar salida confirmada
                                let _ = self
                                    .update_trade_exit_confirmed(exit_price, "HARD_SL", "SL")
                                    .await;

                                let msg = format!(
                                    "❌ *OPERACIÓN PERDIDA*\n\
• Activo: *{}*\n\
• Resultado: *PERDIDA*\n\
• Motivo de cierre: *HARD-SL-{:.2}*\n\
• Dirección: *{}*\n\
• Entrada: {:.3}\n\
• Salida: {:.3}\n\
• Monto operado: ${:.2}\n\
• P&L: {}\n\
• Retorno: {}",
                                    self.asset_label(),
                                    exit_price,
                                    side,
                                    entry_price,
                                    exit_price,
                                    self.position_size,
                                    Self::format_pnl(pnl),
                                    return_pct
                                );
                                events.push(ExecutionEvent::Log(msg));

                                if was_primary && Self::reentries_enabled() {
                                    self.state.shares_held = 0.0;
                                    self.recovery_hit_at = Some(std::time::Instant::now());
                                    self.state.transition_to(PositionState::RecoveryScanning);
                                    info!(
                                        "🔄 Entering FAKEOUT RECOVERY MODE for {}",
                                        self.question
                                    );
                                } else {
                                    self.state.reset();
                                }
                            }
                            Err(e) => {
                                let remaining_balance = crate::api::get_actual_balance(tid)
                                    .await
                                    .unwrap_or(actual_shares);
                                self.shares_held = remaining_balance;
                                self.state.shares_held = remaining_balance;
                                self.is_exiting = false;

                                if remaining_balance <= Self::min_sell_qty() {
                                    self.clear_protective_order(client).await;
                                    self.shares_held = 0.0;
                                    self.state.shares_held = 0.0;
                                    self.status = MarketStatus::Aborted("HARD_SL".to_string());
                                    self.state.reset();
                                    events.push(ExecutionEvent::Log(format!(
                                        "SL completado tras reconciliacion en {}. El remanente quedo por debajo del minimo operativo.",
                                        self.question
                                    )));
                                } else {
                                    events.push(ExecutionEvent::Log(format!(
                                        "SL escalonado incompleto en {}: {} | remanente {:.4}",
                                        self.question, e, remaining_balance
                                    )));
                                }
                            }
                        }
                    }
                    // 2. TAKE PROFIT (0.97)
                    else if self.risk.is_tp_reached(current_exit_price) {
                        let actual_shares = match crate::api::get_actual_balance(tid).await {
                            Ok(bal) => {
                                if bal > 0.0 {
                                    bal
                                } else {
                                    self.state.shares_held
                                }
                            }
                            Err(_) => self.state.shares_held,
                        };

                        info!(
                            "🎯 TAKE PROFIT reached at {:.3} for {} (Qty: {:.4})",
                            current_exit_price, self.question, actual_shares
                        );
                        if actual_shares <= Self::min_sell_qty() {
                            warn!(
                                "Ghost position detected on TP for {}. Balance already zero.",
                                self.question
                            );
                            self.clear_protective_order(client).await;
                            self.shares_held = 0.0;
                            self.state.shares_held = 0.0;
                            self.status = MarketStatus::Aborted("Balance Exhausted".to_string());
                            self.state.reset();
                            return vec![ExecutionEvent::Log(format!(
                                "Posicion ya cerrada en cadena para {}. Se limpia el estado local.",
                                self.question
                            ))];
                        }

                        if let Ok(resp) = self
                            .execution
                            .close_position(client, tid, actual_shares, 0.97, "TAKE_PROFIT")
                            .await
                        {
                            let side = self.state.side.clone();
                            let entry_price = self.state.entry_price;
                            let exit_price = resp.fill_price.unwrap_or(0.97);
                            let pnl = (exit_price - self.state.entry_price) * actual_shares;
                            let return_pct = Self::format_return_pct(pnl, self.position_size);
                            self.clear_protective_order(client).await;
                            self.state.reset();
                            self.shares_held = 0.0;
                            self.binance_entry_reference = None;
                            self.has_won_this_window = true;
                            self.status = MarketStatus::Scanning {
                                max_yes: 0.0,
                                max_no: 0.0,
                                last_update: None,
                            };

                            let msg = format!(
                                "✅ *OPERACIÓN GANADA*\n\
• Activo: *{}*\n\
• Resultado: *GANADA*\n\
• Motivo de cierre: *TP-{:.2}*\n\
• Dirección: *{}*\n\
• Entrada: {:.3}\n\
• Salida: {:.3}\n\
• Monto operado: ${:.2}\n\
• P&L: {}\n\
• Retorno: {}",
                                self.asset_label(),
                                exit_price,
                                side,
                                entry_price,
                                exit_price,
                                self.position_size,
                                Self::format_pnl(pnl),
                                return_pct
                            );

                            let _ = self
                                .update_trade_exit_confirmed(exit_price, "TAKE_PROFIT", "TP")
                                .await;
                            events.push(ExecutionEvent::Log(msg));
                        }
                    }
                    // 3. DCA TRIGGER (0.73 - 0.77)
                    // Note: DCA trigger uses current_ask_price (buying more tokens)
                    else if Self::dca_enabled()
                        && !self.risk.dca_executed
                        && self.risk.is_in_dca_range(current_ask_price)
                    {
                        if elapsed_secs >= 815 {
                            if now.second() % 60 == 0 {
                                info!(
                                    "⏳ DCA BLOCKED: Safety time threshold reached (13:35+) for {}",
                                    self.question
                                );
                            }
                            return events;
                        }
                        if self.is_sol_market()
                            && self.vol_metrics.state
                                == crate::volatility::VolatilityState::HighSuperhigh
                        {
                            info!(
                                "DCA blocked for {}: SOL with HIGH-SUPERHIGH volatility",
                                self.question
                            );
                            return events;
                        }
                        if current_ask_price <= self.risk.active_stop_loss {
                            warn!(
                                "DCA blocked for {}: ask {:.3} is already at/below SL {:.3}",
                                self.question, current_ask_price, self.risk.active_stop_loss
                            );
                            return events;
                        }
                        if current_exit_price <= self.risk.active_stop_loss {
                            warn!(
                                "DCA blocked for {}: bid {:.3} is already at/below SL {:.3}",
                                self.question, current_exit_price, self.risk.active_stop_loss
                            );
                            return events;
                        }

                        let dca_bid_guard = self.risk.active_stop_loss + Self::dca_sl_gap();
                        if current_exit_price <= dca_bid_guard {
                            warn!(
                                "DCA blocked for {}: bid {:.3} is too close to SL {:.3}",
                                self.question, current_exit_price, self.risk.active_stop_loss
                            );
                            return events;
                        }
                        info!(
                            "💰 DCA Range reached at {:.3} for {}",
                            current_ask_price, self.question
                        );
                        let dca_size = self.risk.get_dca_size();
                        let dca_limit = current_ask_price.clamp(
                            crate::risk_engine::RiskEngine::dca_min_price(),
                            crate::risk_engine::RiskEngine::dca_start_price(),
                        );
                        if dca_limit <= dca_bid_guard {
                            warn!(
                                "DCA blocked for {}: limit {:.3} is too close to/below SL guard {:.3}",
                                self.question, dca_limit, dca_bid_guard
                            );
                            return events;
                        }
                        match crate::api::place_dca_limit_buy(
                            client,
                            tid,
                            dca_limit,
                            dca_size,
                            &self.market_id,
                        )
                        .await
                        {
                            Ok(resp) => {
                                let actual_dca_price = resp.fill_price.unwrap_or(current_ask_price);
                                let previous_shares = self.state.shares_held;
                                let previous_cost = self.state.entry_price * previous_shares;
                                let new_total_shares = previous_shares + resp.shares;

                                self.risk.dca_executed = true;
                                self.state.shares_held = new_total_shares;
                                self.shares_held = new_total_shares;
                                self.binance_entry_reference = Some(self.vol_metrics.current_price);
                                if new_total_shares > 0.0 {
                                    self.state.entry_price = (previous_cost
                                        + actual_dca_price * resp.shares)
                                        / new_total_shares;
                                }
                                self.risk.update_active_levels(self.state.entry_price);
                                self.active_stop_loss = Some(self.risk.active_stop_loss);
                                self.active_take_profit = Some(self.risk.active_take_profit);
                                self.refresh_protective_order(
                                    client,
                                    &token_id_owned,
                                    new_total_shares,
                                )
                                .await;
                                self.status = MarketStatus::Executed(
                                    self.state.entry_price,
                                    self.state.side.clone(),
                                );
                                self.record_trade_entry(actual_dca_price, dca_size, "DCA");
                                let total_position = self.position_size * 2.0;
                                events.push(ExecutionEvent::Log(format!(
                                    "➕ *DCA EJECUTADO*\n\
• Activo: *{}*\n\
• Dirección: *{}*\n\
• Nivel DCA: {:.3}\n\
• Acciones añadidas: {:.4}\n\
• Nuevo promedio: {:.3}\n\
• Total en posición: ${:.2}",
                                    self.asset_label(),
                                    self.state.side,
                                    actual_dca_price,
                                    resp.shares,
                                    self.state.entry_price,
                                    total_position
                                )));
                            }
                            Err(e) => {
                                warn!(
                                    "DCA order failed for {} | side={} | ask={:.3} | bid={:.3} | limit={:.3} | size={:.2} | sl_guard={:.3} | reason={}",
                                    self.question,
                                    self.state.side,
                                    current_ask_price,
                                    current_exit_price,
                                    dca_limit,
                                    dca_size,
                                    dca_bid_guard,
                                    e
                                );
                                events.push(ExecutionEvent::Log(format!(
                                    "DCA no ejecutado en {} | ask {:.3} | bid {:.3} | limit {:.3} | motivo: {}",
                                    self.asset_label(),
                                    current_ask_price,
                                    current_exit_price,
                                    dca_limit,
                                    e
                                )));
                            }
                        }
                    }
                }
            }

            PositionState::RecoveryScanning => {
                // 0. TIME BLOCK: No recoveries after 13:35 minutes (815s)
                if elapsed_secs >= 815 {
                    if now.second() % 60 == 0 {
                        info!(
                            "⏳ RECOVERY BLOCKED: Safety time threshold reached (13:35+) for {}",
                            self.question
                        );
                    }
                    return events;
                }

                // 1. RISK CHECK: Max 2 SL per market
                if self.sl_count >= 2 {
                    if now.second() % 60 == 0 {
                        info!(
                            "🛑 RECOVERY BLOCKED: Max SL limit (2) reached for {}",
                            self.question
                        );
                    }
                    return events;
                }

                // Fakeout recovery re-enters on the same side after an 18s cooldown
                let recovery_side = self.state.side.clone();
                let recovery_price = if recovery_side == "UP" {
                    yes_ask
                } else {
                    no_ask
                };
                let tid = if recovery_side == "UP" {
                    self.yes_token_id.as_ref()
                } else {
                    self.no_token_id.as_ref()
                };

                if self
                    .recovery_hit_at
                    .map(|hit| hit.elapsed().as_secs() < 18)
                    .unwrap_or(false)
                {
                    return events;
                }

                if !Self::is_sane_contract_price(recovery_price) {
                    warn!(
                        "Recovery entry blocked for {}: invalid book price {:.4}",
                        self.question, recovery_price
                    );
                    return events;
                }

                if recovery_price >= 0.85 && recovery_price <= 0.88 {
                    if let Some(target_tid) = tid {
                        if let Ok(resp) = crate::api::place_initial_buy(
                            client,
                            target_tid,
                            recovery_price,
                            self.position_size,
                            &self.market_id,
                        )
                        .await
                        {
                            let actual_recovery_price = resp.fill_price.unwrap_or(recovery_price);
                            self.state.transition_to(PositionState::InPosition);
                            self.state.side = recovery_side.to_string();
                            self.state.entry_price = actual_recovery_price;
                            self.state.shares_held = resp.shares;
                            self.shares_held = resp.shares;
                            self.binance_entry_reference = Some(self.vol_metrics.current_price);

                            self.risk = RiskEngine::new(self.position_size, false, false);
                            self.risk.dca_executed = true;

                            self.status = MarketStatus::Executed(
                                actual_recovery_price,
                                recovery_side.to_string(),
                            );
                            self.recovery_hit_at = None;
                            events.push(ExecutionEvent::Log(format!(
                                "🔥 *FAKEOUT RECOVERY ACTIVADO*\n\
• Activo: *{}*\n\
• Dirección: *{}*\n\
• Precio de entrada: {:.3}\n\
• Ventana recovery: 18s\n\
• Rango validado: 0.85 - 0.88",
                                self.asset_label(),
                                recovery_side,
                                actual_recovery_price
                            )));
                        }
                    }
                }
            }

            PositionState::Exiting => {}
        }

        events
    }

    pub fn snapshot(&self, key: &str) -> ActiveStateSnapshot {
        ActiveStateSnapshot {
            composite_key: key.to_string(),
            market_id: self.market_id.clone(),
            question: self.question.clone(),
            binance_symbol: self.binance_symbol.clone(),
            entry_price: self.state.entry_price,
            side: self.state.side.clone(),
            shares_held: self.shares_held,
            position_size: self.position_size,
            active_stop_loss: Some(self.risk.active_stop_loss),
            active_take_profit: Some(self.risk.active_take_profit),
            take_profit_price: self.risk.active_take_profit,
            yes_token_id: self.yes_token_id.clone(),
            no_token_id: self.no_token_id.clone(),
            protective_order_id: self.protective_order_id.clone(),
            entry_type: format!("{:?}", self.state.current_state),
            saved_at: chrono::Local::now().to_rfc3339(),
            protection_status: None,
            last_balance_check: None,
            exit_in_progress: Some(self.is_exiting),
            trade_state: format!("{:?}", self.trade_state),
            ultra_aggressive_mode: self.ultra_aggressive_mode,
            is_exiting: self.is_exiting,
        }
    }

    fn parse_start_time(title: &str) -> Option<u32> {
        let re = Regex::new(r"(?i)(\d{1,2}:\d{2})\s*(AM|PM)?").ok()?;
        let caps: Vec<_> = re.captures_iter(title).collect();
        if caps.len() < 2 {
            return None;
        }

        let cap = &caps[caps.len().checked_sub(2)?];
        let time_str = cap.get(1)?.as_str();
        let amp_str = cap.get(2).map(|m: regex::Match| m.as_str().to_uppercase());

        let parts: Vec<&str> = time_str.split(':').collect();
        if parts.len() != 2 {
            return None;
        }

        let mut h: u32 = parts[0].parse().ok()?;
        let m: u32 = parts[1].parse().ok()?;

        if let Some(amp) = amp_str {
            if amp == "PM" && h != 12 {
                h += 12;
            } else if amp == "AM" && h == 12 {
                h = 0;
            }
        }

        Some(h * 60 + m)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::Market;
    use crate::state_machine::PositionState;
    use crate::volatility::VolatilityMetrics;
    use chrono::{FixedOffset, TimeZone};

    fn build_test_tracker() -> MarketTracker {
        let market = Market {
            id: "test-market".to_string(),
            question: "XRP Up or Down - April 9, 8:15AM-8:30AM ET".to_string(),
            slug: Some("xrp-up-or-down-test".to_string()),
            outcome_prices: None,
            clob_token_ids: Some("[\"yes-test-token\",\"no-test-token\"]".to_string()),
            closed: Some(false),
            active: Some(true),
        };

        MarketTracker::new(
            market,
            Some(0.885),
            Some(1.36),
            VolatilityMetrics {
                current_price: 1.35,
                last_price: 1.35,
                ..VolatilityMetrics::default()
            },
            "XRPUSDT".to_string(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(crate::stats::StatsEngine::new()),
        )
    }

    #[tokio::test]
    async fn test_refresh_protective_order_in_paper_mode() {
        std::env::set_var("PAPER_TRADING", "true");

        let client = reqwest::Client::new();
        let mut tracker = build_test_tracker();
        tracker.risk.active_stop_loss = 0.68;

        tracker
            .refresh_protective_order(&client, "no-test-token", 6.74)
            .await;

        assert_eq!(
            tracker.protective_order_id.as_deref(),
            Some("SIMULATED_PROT_ID")
        );
        assert_eq!(tracker.trade_state, TradeState::OpenProtected);
    }

    #[tokio::test]
    async fn test_hard_sl_closes_position_in_paper_mode() {
        std::env::set_var("PAPER_TRADING", "true");
        std::env::set_var("HARD_SL_PRICE", "0.68");

        let client = reqwest::Client::new();
        let mut tracker = build_test_tracker();
        crate::api::seed_paper_balance("no-test-token", 6.74);
        tracker.state.transition_to(PositionState::InPosition);
        tracker.state.side = "DOWN".to_string();
        tracker.state.entry_price = 0.85;
        tracker.state.shares_held = 6.74;
        tracker.shares_held = 6.74;
        tracker.status = MarketStatus::Executed(0.85, "DOWN".to_string());
        tracker.trade_state = TradeState::OpenProtected;
        tracker.protective_order_id = Some("SIMULATED_PROT_ID".to_string());
        tracker.risk.active_stop_loss = 0.68;

        let now = FixedOffset::west_opt(4 * 3600)
            .unwrap()
            .with_ymd_and_hms(2026, 4, 9, 8, 27, 0)
            .single()
            .unwrap();

        let events = tracker
            .check(
                (Some(0.32), Some(0.69)),
                (Some(0.31), Some(0.67)),
                &client,
                now,
                VolatilityMetrics {
                    current_price: 1.362,
                    last_price: 1.357,
                    ..VolatilityMetrics::default()
                },
                true,
            )
            .await;

        assert!(events.iter().any(|event| matches!(
            event,
            ExecutionEvent::Log(msg) if msg.contains("SL protegido ejecutado")
        )));
        assert_eq!(tracker.shares_held, 0.0);
        assert!(tracker.protective_order_id.is_none());
        assert_eq!(tracker.trade_state, TradeState::Closed);
        assert!(matches!(tracker.status, MarketStatus::Aborted(_)));
    }
}

pub fn save_active_states(snapshots: &[ActiveStateSnapshot]) {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop();
    path.push("active_state.json");
    if let Ok(json) = serde_json::to_string_pretty(snapshots) {
        let _ = std::fs::write(&path, json);
    }
}

pub fn load_active_states() -> Vec<ActiveStateSnapshot> {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop();
    path.push("active_state.json");
    if !path.exists() {
        return Vec::new();
    }
    if let Ok(json) = std::fs::read_to_string(&path) {
        if let Ok(snaps) = serde_json::from_str::<Vec<ActiveStateSnapshot>>(&json) {
            return snaps;
        }
    }
    Vec::new()
}
