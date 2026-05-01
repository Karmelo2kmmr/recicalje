use chrono::{DateTime, Utc};
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Venue {
    Polymarket,
    Kalshi,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Asset {
    BTC,
    ETH,
    SOL,
    Other(String),
}

impl std::fmt::Display for Asset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Asset::BTC => write!(f, "BTC"),
            Asset::ETH => write!(f, "ETH"),
            Asset::SOL => write!(f, "SOL"),
            Asset::Other(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NormalizedMarket {
    pub venue: Venue,
    pub asset: Asset,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub strike_price: f64,
    pub side_yes_token: String,
    pub side_no_token: String,
    pub resolution_source: String,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct ValidatedMarketPair {
    pub poly: NormalizedMarket,
    pub kalshi: NormalizedMarket,
    pub strike_diff: f64,
    pub sync_id: String,
}

#[derive(Debug, Clone)]
pub enum ValidationError {
    AssetMismatch,
    ExpiryMismatch,
    StrikeDriftTooHigh(f64),
    UntrustedResolutionSource,
}

pub struct MarketEquivalenceValidator;

impl MarketEquivalenceValidator {
    pub fn validate_pair(
        poly: &NormalizedMarket,
        kalshi: &NormalizedMarket,
        max_strike_drift_btc: f64,
        max_strike_drift_eth: f64,
    ) -> Result<ValidatedMarketPair, ValidationError> {
        // 1. Identity Validations (Zero Tolerance)
        if poly.asset != kalshi.asset {
            return Err(ValidationError::AssetMismatch);
        }
        if poly.end_time != kalshi.end_time {
            return Err(ValidationError::ExpiryMismatch);
        }

        // 2. Strike Validation (Ultra-Fine Tolerance)
        let strike_diff = (poly.strike_price - kalshi.strike_price).abs();
        let max_drift = match poly.asset {
            Asset::BTC => max_strike_drift_btc,
            Asset::ETH => max_strike_drift_eth,
            _ => 0.0, // Other assets require exact match if drift not specified
        };

        if strike_diff > max_drift {
            return Err(ValidationError::StrikeDriftTooHigh(strike_diff));
        }

        // 3. Source Validation
        if !poly.resolution_source.to_lowercase().contains("binance")
            || !kalshi.resolution_source.to_lowercase().contains("binance")
        {
            return Err(ValidationError::UntrustedResolutionSource);
        }

        // 4. Sync ID Generation
        let sync_id = format!(
            "{}-{}-{}",
            poly.asset,
            poly.end_time.format("%Y%m%d"),
            poly.end_time.format("%H%M")
        );

        Ok(ValidatedMarketPair {
            poly: poly.clone(),
            kalshi: kalshi.clone(),
            strike_diff,
            sync_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dummy_market(venue: Venue, strike: f64, res: &str) -> NormalizedMarket {
        NormalizedMarket {
            venue,
            asset: Asset::BTC,
            start_time: Utc.with_ymd_and_hms(2024, 4, 30, 16, 0, 0).unwrap(),
            end_time: Utc.with_ymd_and_hms(2024, 4, 30, 16, 15, 0).unwrap(),
            strike_price: strike,
            side_yes_token: "yes_token".into(),
            side_no_token: "no_token".into(),
            resolution_source: res.into(),
            last_update: Instant::now(),
        }
    }

    #[test]
    fn test_perfect_match() {
        let poly = dummy_market(Venue::Polymarket, 75500.0, "Binance");
        let kalshi = dummy_market(Venue::Kalshi, 75500.0, "Binance");

        let result = MarketEquivalenceValidator::validate_pair(&poly, &kalshi, 0.50, 0.05);
        assert!(result.is_ok());
    }

    #[test]
    fn test_strike_drift_rejection() {
        let poly = dummy_market(Venue::Polymarket, 75500.0, "Binance");
        let kalshi = dummy_market(Venue::Kalshi, 75505.0, "Binance"); // 5.0 difference

        let result = MarketEquivalenceValidator::validate_pair(&poly, &kalshi, 0.50, 0.05);
        assert!(matches!(
            result,
            Err(ValidationError::StrikeDriftTooHigh(_))
        ));
    }

    #[test]
    fn test_time_mismatch() {
        let poly = dummy_market(Venue::Polymarket, 75500.0, "Binance");
        let mut kalshi = dummy_market(Venue::Kalshi, 75500.0, "Binance");
        kalshi.end_time = Utc.with_ymd_and_hms(2024, 4, 30, 16, 30, 0).unwrap(); // 30m vs 15m

        let result = MarketEquivalenceValidator::validate_pair(&poly, &kalshi, 0.50, 0.05);
        assert!(matches!(result, Err(ValidationError::ExpiryMismatch)));
    }
}
