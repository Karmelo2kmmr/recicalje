use chrono::{DateTime, FixedOffset, Timelike};
use log::{debug, error, info, warn};
use regex::Regex;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
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
    pub trade_state: String, 
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

    // P0 FIX: Atomic flag to prevent race conditions when TP and SL trigger in the same tick
    pub closing_in_progress: Arc<std::sync::atomic::AtomicBool>,
    
    // P0 FIX: Global Capital Protection for real exits and safe mode triggers
    pub protection: crate::risk_engine::CapitalProtectionEngine,
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
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        if !paper_trading {
            if let Some(existing_id) = self.protective_order_id.take() {
                match crate::api::cancel_protective_order(client, &existing_id).await {
                    Ok(_) => info!("Live protective GTC {} cancelled for {}.", existing_id, self.question),
                    Err(e) => {
                        error!("FATAL: Failed to cancel live protective GTC {} for {}: {}. RISK OF OVERSELL.", existing_id, self.question, e);
                        self.protective_order_id = Some(existing_id);
                        return; 
                    }
                }
            }
            self.trade_state = TradeState::OpenProtected;
            info!(
                "Live protective GTC disabled for {}. Logical stop remains armed at {:.4} for {:.6} shares.",
                self.question, self.risk.active_stop_loss, shares
            );
            return;
        }

        if let Some(existing_id) = self.protective_order_id.take() {
            match crate::api::cancel_protective_order(client, &existing_id).await {
                Ok(_) => info!("Paper protective GTC {} cancelled.", existing_id),
                Err(e) => {
                    warn!("Could not cancel paper protective order {}: {}", existing_id, e);
                }
            }
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
            match crate::api::cancel_protective_order(client, &existing_id).await {
                Ok(_) => info!("Protective order {} cleared successfully.", existing_id),
                Err(e) => {
                    error!("CRITICAL: Failed to clear protective order {} on exit: {}. Manual check required.", existing_id, e);
                    self.protective_order_id = Some(existing_id);
                }
            }
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
        msg.contains("orderbook does not exist") || msg.contains("the orderbook")
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

    fn should_block_expensive_late_entry(&self, now: DateTime<FixedOffset>, price: f64) -> bool {
        if std::env::var("DISABLE_LATE_EXPENSIVE_ENTRY_BLOCK")
            .ok()
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
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
            .map(|v| {
                matches!(
                    v.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
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
        if pnl >= 0.0 { format!("+${:.2}", pnl) } else { format!("-${:.2}", pnl.abs()) }
    }

    fn format_return_pct(pnl: f64, size: f64) -> String {
        if size <= 0.0 { return "0.00%".to_string(); }
        let pct = (pnl / size) * 100.0;
        if pct >= 0.0 { format!("+{:.2}%", pct) } else { format!("{:.2}%", pct) }
    }

    fn format_price_delta(current: f64, reference: Option<f64>) -> String {
        if let Some(price_to_beat) = reference.filter(|v| *v > 0.0) {
            let delta = current - price_to_beat;
            if delta.abs() < 0.01 {
                if delta >= 0.0 { format!("+{:.4} USD", delta) } else { format!("{:.4} USD", delta) }
            } else {
                if delta >= 0.0 { format!("+{:.2} USD", delta) } else { format!("{:.2} USD", delta) }
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
        (base_threshold * 0.70_f64).max(0.00030_f64)
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
        let adverse_move = if self.state.side == "UP" { -pct_change } else { pct_change };
        if adverse_move <= 0.0 {
            return None;
        }

        let threshold = self.early_exit_distance_threshold();
        let danger_zone = current_exit_price <= (self.risk.active_stop_loss + Self::danger_exit_buffer());

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
        let max_entry_price = std::env::var("MAX_ENTRY_PRICE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.91);
        let min_entry_price = std::env::var("MIN_ENTRY_PRICE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.86);

        let (yes_token_id, no_token_id) = m
            .clob_token_ids
            .as_ref()
            .and_then(|s| {
                let ids: Result<Vec<String>, _> = serde_json::from_str(s);
                ids.ok().map(|v| (v.get(0).cloned(), v.get(1).cloned()))
            })
            .unwrap_or((None, None));

        let start_minutes = Self::parse_start_time(&m.question);
        let position_size = std::env::var("POSITION_SIZE").unwrap_or("6.0".to_string()).parse().unwrap_or(6.0);

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
            status: MarketStatus::Scanning { max_yes: 0.0, max_no: 0.0, last_update: None },
            has_traded_in_session: false,
            active_stop_loss: None,
            active_take_profit: None,
            take_profit_price: 0.97,
            protective_order_id: None,
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
            last_block_reason: String::new(),
            closing_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            protection: crate::risk_engine::CapitalProtectionEngine::new(),
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
        info!("📝 RECORDING EXIT: {} @ {:.4} (Reason: {})", self.question, price, reason);
        let _ = self.stats.update_csv_exit_price(&self.market_id, price, intent, true, Some(price));
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
            setup_tag: None, entry_bucket: None, signal_score: None,
            reason_entry: Some("TrendHammer Trigger".to_string()),
            reason_exit: None, holding_seconds: None, max_favor: None,
            max_adverse: None, market_regime: None, ExitIntent: None,
            ExitReason: None, ExitConfirmed: None, ExitOrderId: None,
            ExitFilledShares: None, ExitAvgFillPrice: None, ExitTimestamp: None,
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
        trend_hammer_active: bool,
    ) -> Vec<ExecutionEvent> {
        let (yes_ask, no_ask) = (asks.0.unwrap_or(0.0), asks.1.unwrap_or(0.0));
        let (yes_bid, no_bid) = (bids.0.unwrap_or(0.0), bids.1.unwrap_or(0.0));
        let mut events = Vec::new();

        let mut elapsed_secs = 0;
        if let Some(start) = self.start_minutes {
            let h = now.hour();
            let m = now.minute();
            let s = now.second();
            let current_total_secs = (h * 3600 + m * 60 + s) as i32;
            let start_total_secs = start as i32 * 60;
            elapsed_secs = (current_total_secs - start_total_secs).rem_euclid(86400);
        }

        match self.state.current_state {
            PositionState::Scanning | PositionState::RecoveryScanning => {
                let mut yes_trigger = self.trigger_price;
                let mut no_trigger = self.trigger_price;

                if let Some(yes_tid) = self.yes_token_id.as_ref() {
                    if yes_ask >= 0.84 && yes_ask <= self.max_entry_price {
                        let metrics = crate::api::get_orderbook_depth(client, yes_tid).await;
                        yes_trigger = self.adjusted_trigger_from_orderbook(self.trigger_price, &metrics);
                    }
                }
                if let Some(no_tid) = self.no_token_id.as_ref() {
                    if no_ask >= 0.84 && no_ask <= self.max_entry_price {
                        let metrics = crate::api::get_orderbook_depth(client, no_tid).await;
                        no_trigger = self.adjusted_trigger_from_orderbook(self.trigger_price, &metrics);
                    }
                }

                let mut buy_yes = self.entry.evaluate_triggers(yes_ask, yes_trigger, self.max_entry_price, self.min_entry_price);
                let mut buy_no = self.entry.evaluate_triggers(no_ask, no_trigger, self.max_entry_price, self.min_entry_price);

                if self.has_won_this_window && Self::reentries_enabled() {
                    buy_yes = if Self::is_sane_contract_price(yes_ask) && yes_ask < 0.840 { true } else { buy_yes && self.is_valid_reentry_price(yes_ask) };
                    buy_no = if Self::is_sane_contract_price(no_ask) && no_ask < 0.840 { true } else { buy_no && self.is_valid_reentry_price(no_ask) };
                } else if self.has_won_this_window {
                    buy_yes = false; buy_no = false;
                    self.last_block_reason = "follow-up entry blocked".to_string();
                }

                if buy_yes && yes_bid <= crate::risk_engine::HARD_SL_PRICE { buy_yes = false; self.last_block_reason = "bid <= HARD_SL".to_string(); }
                if buy_no && no_bid <= crate::risk_engine::HARD_SL_PRICE { buy_no = false; self.last_block_reason = "bid <= HARD_SL".to_string(); }
                if self.is_sol_market() { buy_no = false; } // SOL only UP (UP=yes)

                // Failed entry cooldown (15s)
                if let Some(last_f) = self.entry.last_failed_attempt {
                    if last_f.elapsed().as_secs() < 15 {
                        buy_yes = false;
                        buy_no = false;
                        self.last_block_reason = "failed entry cooldown".to_string();
                    }
                }

                if (buy_yes || buy_no) && !self.is_exiting {
                    // P0 FIX: Never open a position if we already have shares or an exit is pending
                    if self.shares_held > 0.0 || self.closing_in_progress.load(Ordering::SeqCst) {
                        self.last_block_reason = "posición abierta u orden pendiente".to_string();
                        return events;
                    }

                    let side = if buy_yes { "UP" } else { "DOWN" };
                    let price = if buy_yes { yes_ask } else { no_ask };
                    let token_id = if buy_yes { self.yes_token_id.as_ref() } else { self.no_token_id.as_ref() };

                    if !Self::is_sane_contract_price(price) || self.should_block_expensive_late_entry(now, price) {
                        return events;
                    }

                    if !trend_hammer_active && !self.ultra_aggressive_mode {
                        if self.trigger_hit_at.is_none() {
                            self.trigger_hit_at = Some(std::time::Instant::now());
                            return events;
                        }
                        let wait = if self.vol_metrics.state == crate::volatility::VolatilityState::NeutralHigh && self.vol_metrics.z_score > 1.8 { 23.0 } else { 5.0 };
                        if self.trigger_hit_at.unwrap().elapsed().as_secs_f64() < wait { return events; }
                    }

                    if let Some(tid) = token_id {
                        let tid_owned = tid.clone();
                        match crate::api::place_initial_buy(client, tid, price, self.position_size, &self.market_id).await {
                            Ok(resp) => {
                                let actual_price = resp.fill_price.unwrap_or(price);
                                self.state.transition_to(PositionState::InPosition);
                                self.state.side = side.to_string();
                                self.state.entry_price = actual_price;
                                self.state.shares_held = resp.shares;
                                self.shares_held = resp.shares;
                                self.binance_entry_reference = Some(self.vol_metrics.current_price);
                                self.risk.update_active_levels(actual_price);
                                self.active_stop_loss = Some(self.risk.active_stop_loss);
                                self.active_take_profit = Some(self.risk.active_take_profit);
                                self.refresh_protective_order(client, &tid_owned, resp.shares).await;
                                self.status = MarketStatus::Executed(actual_price, side.to_string());
                                self.has_traded_in_session = true;
                                self.record_trade_entry(actual_price, self.position_size, "Initial");
                                events.push(ExecutionEvent::Log(format!("🚀 *ENTRADA EJECUTADA*\n• Activo: *{}*\n• Dirección: *{}*\n• Precio: {:.3}", self.asset_label(), side, actual_price)));
                            }
                            Err(e) => {
                                error!("Entry failed for {}: {}", self.question, e);
                                self.entry.last_failed_attempt = Some(std::time::Instant::now());
                            }
                        }
                    }
                }
            }
            PositionState::InPosition => {
                let current_exit_price = if self.state.side == "UP" { yes_bid } else { no_bid };
                let current_ask_price = if self.state.side == "UP" { yes_ask } else { no_ask };
                let tid = self.get_token_id();
                let token_id_owned = tid.clone();

                if !tid.is_empty() {
                    // 1. EARLY EXIT BY BINANCE
                    if let Some((adverse_move, threshold)) = self.should_preemptive_exit(current_exit_price) {
                        let actual_shares = crate::api::get_actual_balance(&tid).await.unwrap_or(self.shares_held);
                        if actual_shares > Self::min_sell_qty() {
                            if self.closing_in_progress.swap(true, Ordering::SeqCst) { return events; }
                            match self.execution.execute_safe_exit(&mut self.protection, client, &tid, current_exit_price.max(0.10)).await {
                                Ok(resp) => {
                                    let exit_p = resp.fill_price.unwrap_or(current_exit_price);
                                    let pnl = (exit_p - self.state.entry_price) * actual_shares;
                                    self.protection.check_daily_loss(pnl);
                                    self.clear_protective_order(client).await;
                                    self.shares_held = 0.0; self.state.reset();
                                    self.status = MarketStatus::Aborted("EARLY_EXIT".to_string());
                                    self.update_trade_exit_confirmed(exit_p, "EARLY", "EARLY").await.ok();
                                    events.push(ExecutionEvent::Log(format!("⚠️ *SALIDA TEMPRANA*\n• P&L: {}", Self::format_pnl(pnl))));
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                    return events;
                                }
                                Err(e) => {
                                    error!("EARLY_EXIT Safe Mode trigger: {}", e);
                                    self.state.transition_to(PositionState::EmergencyExiting);
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                }
                            }
                        }
                    }

                    // 2. HARD STOP LOSS
                    if self.risk.should_hard_exit(current_exit_price) {
                        let actual_shares = crate::api::get_actual_balance(&tid).await.unwrap_or(self.shares_held);
                        if actual_shares > Self::min_sell_qty() {
                            if self.closing_in_progress.swap(true, Ordering::SeqCst) { return events; }
                            match self.execution.execute_safe_exit(&mut self.protection, client, &tid, current_exit_price).await {
                                Ok(resp) => {
                                    let exit_p = resp.fill_price.unwrap_or(current_exit_price);
                                    let pnl = (exit_p - self.state.entry_price) * actual_shares;
                                    self.protection.check_daily_loss(pnl);
                                    self.clear_protective_order(client).await;
                                    self.shares_held = 0.0; self.state.reset();
                                    self.status = MarketStatus::Aborted("HARD_SL".to_string());
                                    self.update_trade_exit_confirmed(exit_p, "SL", "SL").await.ok();
                                    events.push(ExecutionEvent::Log(format!("❌ *STOP LOSS*\n• P&L: {}", Self::format_pnl(pnl))));
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                    return events;
                                }
                                Err(e) => {
                                    error!("HARD_SL Safe Mode trigger: {}", e);
                                    self.state.transition_to(PositionState::EmergencyExiting);
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                }
                            }
                        }
                    }

                    // 3. TAKE PROFIT
                    if self.risk.is_tp_reached(current_exit_price) {
                        let actual_shares = crate::api::get_actual_balance(&tid).await.unwrap_or(self.shares_held);
                        if actual_shares > Self::min_sell_qty() {
                            if self.closing_in_progress.swap(true, Ordering::SeqCst) { return events; }
                            match self.execution.execute_safe_exit(&mut self.protection, client, &tid, current_exit_price).await {
                                Ok(resp) => {
                                    let exit_p = resp.fill_price.unwrap_or(current_exit_price);
                                    let pnl = (exit_p - self.state.entry_price) * actual_shares;
                                    self.protection.check_daily_loss(pnl);
                                    self.clear_protective_order(client).await;
                                    self.shares_held = 0.0; self.state.reset();
                                    self.status = MarketStatus::Aborted("TAKE_PROFIT".to_string());
                                    self.has_won_this_window = true;
                                    self.update_trade_exit_confirmed(exit_p, "TP", "TP").await.ok();
                                    events.push(ExecutionEvent::Log(format!("✅ *TAKE PROFIT*\n• P&L: {}", Self::format_pnl(pnl))));
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                    return events;
                                }
                                Err(e) => {
                                    error!("TAKE_PROFIT Safe Mode trigger: {}", e);
                                    self.state.transition_to(PositionState::EmergencyExiting);
                                    self.closing_in_progress.store(false, Ordering::SeqCst);
                                }
                            }
                        }
                    }

                    // 4. DCA
                    if Self::dca_enabled() && !self.risk.dca_executed && self.risk.is_in_dca_range(current_ask_price) && elapsed_secs < 815 {
                        let dca_limit = current_ask_price.clamp(RiskEngine::dca_min_price(), RiskEngine::dca_start_price());
                        let tid_owned = tid.clone();
                        match crate::api::place_dca_limit_buy(client, &tid, dca_limit, self.risk.get_dca_size(), &self.market_id).await {
                            Ok(resp) => {
                                let prev_shares = self.state.shares_held;
                                let new_shares = prev_shares + resp.shares;
                                self.state.entry_price = ((self.state.entry_price * prev_shares) + (resp.fill_price.unwrap_or(dca_limit) * resp.shares)) / new_shares;
                                self.state.shares_held = new_shares; self.shares_held = new_shares;
                                self.risk.dca_executed = true;
                                self.risk.update_active_levels(self.state.entry_price);
                                self.refresh_protective_order(client, &tid_owned, new_shares).await;
                                events.push(ExecutionEvent::Log(format!("💰 *DCA EJECUTADO*\n• Nuevo Promedio: {:.3}", self.state.entry_price)));
                            }
                            Err(e) => warn!("DCA failed: {}", e),
                        }
                    }

                    // 5. PANIC TIME EXIT (Anti-Zero Protection)
                    if Self::time_exit_enabled() && elapsed_secs >= Self::time_exit_trigger_secs() {
                        let actual_shares = crate::api::get_actual_balance(&tid).await.unwrap_or(self.shares_held);
                        if actual_shares > Self::min_sell_qty() {
                            let is_profit = current_exit_price > self.state.entry_price;
                            
                            // Si estamos en el último minuto y no hay profit, salir por lo que sea
                            if !is_profit || elapsed_secs >= 885 {
                                if self.closing_in_progress.swap(true, Ordering::SeqCst) { return events; }
                                info!("🚨 PANIC TIME EXIT TRIGGERED: Closing at {}s to avoid 100% loss.", elapsed_secs);
                                match self.execution.execute_safe_exit(&mut self.protection, client, &tid, current_exit_price).await {
                                    Ok(resp) => {
                                        let exit_p = resp.fill_price.unwrap_or(current_exit_price);
                                        let pnl = (exit_p - self.state.entry_price) * actual_shares;
                                        self.protection.check_daily_loss(pnl);
                                        self.clear_protective_order(client).await;
                                        self.shares_held = 0.0; self.state.reset();
                                        self.status = MarketStatus::Aborted("TIME_EXIT".to_string());
                                        self.update_trade_exit_confirmed(exit_p, "TIME", "TIME").await.ok();
                                        events.push(ExecutionEvent::Log(format!("⏰ *SALIDA POR TIEMPO*\n• P&L: {}", Self::format_pnl(pnl))));
                                        self.closing_in_progress.store(false, Ordering::SeqCst);
                                        return events;
                                    }
                                    Err(e) => {
                                        error!("TIME_EXIT Safe Mode trigger: {}", e);
                                        self.state.transition_to(PositionState::EmergencyExiting);
                                        self.closing_in_progress.store(false, Ordering::SeqCst);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
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
            protection_status: None, last_balance_check: None,
            exit_in_progress: Some(self.is_exiting),
            trade_state: format!("{:?}", self.trade_state),
            ultra_aggressive_mode: self.ultra_aggressive_mode,
            is_exiting: self.is_exiting,
        }
    }

    fn parse_start_time(title: &str) -> Option<u32> {
        let re = Regex::new(r"(?i)(\d{1,2}:\d{2})\s*(AM|PM)?").ok()?;
        let caps: Vec<_> = re.captures_iter(title).collect();
        if caps.len() < 2 { return None; }
        let cap = &caps[caps.len().checked_sub(2)?];
        let time_str = cap.get(1)?.as_str();
        let amp_str = cap.get(2).map(|m| m.as_str().to_uppercase());
        let parts: Vec<&str> = time_str.split(':').collect();
        let mut h: u32 = parts[0].parse().ok()?;
        let m: u32 = parts[1].parse().ok()?;
        if let Some(amp) = amp_str {
            if amp == "PM" && h != 12 { h += 12; } else if amp == "AM" && h == 12 { h = 0; }
        }
        Some(h * 60 + m)
    }
}

pub fn save_active_states(snapshots: &[ActiveStateSnapshot]) {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop(); path.push("active_state.json");
    if let Ok(json) = serde_json::to_string_pretty(snapshots) { let _ = std::fs::write(&path, json); }
}

pub fn load_active_states() -> Vec<ActiveStateSnapshot> {
    let mut path = std::env::current_exe().unwrap_or_default();
    path.pop(); path.push("active_state.json");
    if !path.exists() { return Vec::new(); }
    if let Ok(json) = std::fs::read_to_string(&path) {
        if let Ok(snaps) = serde_json::from_str::<Vec<ActiveStateSnapshot>>(&json) { return snaps; }
    }
    Vec::new()
}
