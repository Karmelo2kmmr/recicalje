use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Venue {
    Polymarket,
    Kalshi,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PositionState {
    Open,
    ExitPending,
    ExitFailedZeroFill,
    RecoveryPending,
    PartiallyClosed,
    ManualReviewRequired,
    ExpiredPendingResolution,
    ClosedConfirmed,
    ResolvedConfirmed,
    StopPending,
    HedgeEvaluating,
    HedgePending,
    Hedged,
    Unwinding,
    ExpiryHold,
    EntryUnknownPendingReconcile,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenPosition {
    pub twin_key: String,
    pub venue: Venue,
    pub coin: String,
    pub pm_market_id: String,
    pub pm_yes_token: String,
    pub pm_no_token: String,
    pub kalshi_ticker: String,
    pub buy_yes: bool,
    pub entry_price: f64,
    pub shares: f64,
    pub notional_usdc: f64,
    #[serde(default)]
    pub entry_order_id: Option<String>,
    #[serde(default)]
    pub last_exit_order_id: Option<String>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub opened_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    pub dca_executed: bool,
    pub is_hedge: bool,
    pub hedge_sl_price: Option<f64>,
    pub hedge_tp_price: Option<f64>,
    pub binance_entry_price: f64,
    pub binance_retrace_threshold: f64,
    pub state: PositionState,
    pub hedge_pair_id: Option<String>,
}

impl OpenPosition {
    pub fn venue_platform(&self) -> Platform {
        match self.venue {
            Venue::Polymarket => Platform::Polymarket,
            Venue::Kalshi => Platform::Kalshi,
        }
    }
    pub fn pm_token_id(&self) -> &str {
        if self.buy_yes {
            &self.pm_yes_token
        } else {
            &self.pm_no_token
        }
    }
}

#[derive(Debug, Clone)]
pub struct DualMarketPair {
    pub coin: String,          // e.g. "BTC"
    pub pm_market_id: String,  // Polymarket market ID
    pub pm_yes_token: String,  // Polymarket YES/UP token ID
    pub pm_no_token: String,   // Polymarket NO/DOWN token ID
    pub kalshi_ticker: String, // Kalshi market ticker
    pub km_strike: f64,
    pub pm_strike: f64,
    pub window_start_mins: i32, // Minutes from start of day in ET
}

#[derive(Debug, Clone)]
pub struct PlatformPrices {
    pub polymarket_ask: f64,
    pub polymarket_bid: f64,
    pub kalshi_ask: f64,
    pub kalshi_bid: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Platform {
    Polymarket,
    Kalshi,
}

pub struct DualCapitalManager {
    pub polymarket_balance: f64,
    pub kalshi_balance: f64,
    pub is_paper: bool,
}

impl DualCapitalManager {
    pub fn new(is_paper: bool) -> Self {
        Self {
            polymarket_balance: 100.0,
            kalshi_balance: 100.0,
            is_paper,
        }
    }

    pub fn with_balances(is_paper: bool, polymarket_balance: f64, kalshi_balance: f64) -> Self {
        Self {
            polymarket_balance,
            kalshi_balance,
            is_paper,
        }
    }

    pub fn has_funds(&self, platform: &Platform, required: f64) -> bool {
        match platform {
            Platform::Polymarket => self.polymarket_balance >= required,
            Platform::Kalshi => self.kalshi_balance >= required,
        }
    }

    pub fn deduct(&mut self, platform: &Platform, amount: f64) {
        match platform {
            Platform::Polymarket => {
                self.polymarket_balance = (self.polymarket_balance - amount).max(0.0)
            }
            Platform::Kalshi => self.kalshi_balance = (self.kalshi_balance - amount).max(0.0),
        }
    }

    pub fn add(&mut self, platform: &Platform, amount: f64) {
        match platform {
            Platform::Polymarket => self.polymarket_balance += amount,
            Platform::Kalshi => self.kalshi_balance += amount,
        }
    }

    pub fn balance(&self, platform: &Platform) -> f64 {
        match platform {
            Platform::Polymarket => self.polymarket_balance,
            Platform::Kalshi => self.kalshi_balance,
        }
    }
}
