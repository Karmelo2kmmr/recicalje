use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct DualMarketPair {
    pub coin: String,              // e.g. "BTC"
    pub pm_market_id: String,      // Polymarket market ID
    pub pm_yes_token: String,      // Polymarket YES/UP token ID
    pub pm_no_token: String,       // Polymarket NO/DOWN token ID
    pub kalshi_ticker: String,     // Kalshi market ticker
    pub pm_target_price: Option<f64>,
    pub km_target_price: Option<f64>,
    pub km_yes_ask_hint: Option<f64>,
    pub km_no_ask_hint: Option<f64>,
    pub is_active: bool,
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

    pub fn has_funds(&self, platform: &Platform, required: f64) -> bool {
        match platform {
            Platform::Polymarket => self.polymarket_balance >= required,
            Platform::Kalshi => self.kalshi_balance >= required,
        }
    }

    pub fn deduct(&mut self, platform: &Platform, amount: f64) {
        if self.is_paper {
            match platform {
                Platform::Polymarket => self.polymarket_balance -= amount,
                Platform::Kalshi => self.kalshi_balance -= amount,
            }
        }
    }

    pub fn add(&mut self, platform: &Platform, amount: f64) {
        if self.is_paper {
            match platform {
                Platform::Polymarket => self.polymarket_balance += amount,
                Platform::Kalshi => self.kalshi_balance += amount,
            }
        }
    }

    pub fn balance(&self, platform: &Platform) -> f64 {
        match platform {
            Platform::Polymarket => self.polymarket_balance,
            Platform::Kalshi => self.kalshi_balance,
        }
    }
}
