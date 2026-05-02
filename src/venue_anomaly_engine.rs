#[derive(Debug, Clone)]
pub struct QuantConfig {
    pub min_equivalence: f64,
    pub min_liquidity: f64,
    pub max_exec_risk: f64,
    pub min_edge_confidence: f64,

    pub w_eq: f64,
    pub w_lag: f64,
    pub w_mis: f64,
    pub w_div: f64,
    pub w_liq: f64,
    pub w_risk: f64,

    pub stale_penalty_factor: f64,
}

impl QuantConfig {
    pub fn from_env() -> Self {
        fn env_f64(name: &str, default: f64) -> f64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<f64>().ok())
                .unwrap_or(default)
        }

        Self {
            min_equivalence: env_f64("QUANT_MIN_EQUIVALENCE", 90.0),
            min_liquidity: env_f64("QUANT_MIN_LIQUIDITY", 80.0),
            max_exec_risk: env_f64("QUANT_MAX_EXEC_RISK", 35.0),
            min_edge_confidence: env_f64("QUANT_MIN_EDGE_CONFIDENCE", 75.0),

            w_eq: env_f64("WEIGHT_EQUIVALENCE", 1.0),
            w_lag: env_f64("WEIGHT_VENUE_LAG", 1.5),
            w_mis: env_f64("WEIGHT_MISPRICING", 2.0),
            w_div: env_f64("WEIGHT_DIVERGENCE", 2.5),
            w_liq: env_f64("WEIGHT_LIQUIDITY", 1.0),
            w_risk: env_f64("WEIGHT_EXEC_RISK", 1.5),

            stale_penalty_factor: env_f64("STALE_PENALTY_FACTOR", 10.0),
        }
    }
}

impl Default for QuantConfig {
    fn default() -> Self {
        Self {
            min_equivalence: 90.0,
            min_liquidity: 80.0,
            max_exec_risk: 35.0,
            min_edge_confidence: 75.0,

            w_eq: 1.0,
            w_lag: 1.5,
            w_mis: 2.0,
            w_div: 2.5,
            w_liq: 1.0,
            w_risk: 1.5,

            stale_penalty_factor: 10.0,
        }
    }
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct EntryGateConfig {
    pub max_quote_age_ms: u64,
    pub max_entry_spread_pct: f64,
    pub min_edge_confidence: f64,
}

impl Default for EntryGateConfig {
    fn default() -> Self {
        Self {
            max_quote_age_ms: env_u64("MAX_QUOTE_AGE_MS", 750),
            max_entry_spread_pct: env_f64("MAX_ENTRY_SPREAD_PCT", 0.08),
            min_edge_confidence: env_f64("MIN_EDGE_CONFIDENCE", 75.0),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EntryGateInputs {
    pub quote_age_ms: u64,
    pub spread_pct: f64,
    pub edge_confidence: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryGateDecision {
    pub allowed: bool,
    pub reason: Option<String>,
}

pub fn evaluate_entry_gate(config: &EntryGateConfig, inputs: EntryGateInputs) -> EntryGateDecision {
    if inputs.quote_age_ms > config.max_quote_age_ms {
        return EntryGateDecision {
            allowed: false,
            reason: Some("STALE_QUOTE".to_string()),
        };
    }

    if inputs.spread_pct > config.max_entry_spread_pct {
        return EntryGateDecision {
            allowed: false,
            reason: Some("WIDE_SPREAD".to_string()),
        };
    }

    if inputs.edge_confidence < config.min_edge_confidence {
        return EntryGateDecision {
            allowed: false,
            reason: Some(format!("LOW_EDGE_CONFIDENCE:{:.1}", inputs.edge_confidence)),
        };
    }

    EntryGateDecision {
        allowed: true,
        reason: None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct QuantScores {
    pub venue_lag: f64,
    pub equivalence: f64,
    pub mispricing: f64,
    pub divergence: f64,
    pub liquidity_reliability: f64,
    pub execution_risk: f64,
    pub edge_confidence: f64,
    pub is_tradable: bool,
    pub rejection_reason: Option<String>,
}

pub struct AnomalyEngine;

impl AnomalyEngine {
    /// 1. Venue Lag Score
    pub fn calculate_venue_lag(
        binance_delta_5s: f64,
        binance_volatility_5s: f64,
        venue_delta_5s: f64,
        data_age_sec: f64,
    ) -> f64 {
        if binance_volatility_5s == 0.0 {
            return 0.0;
        }
        let z_score = (binance_delta_5s.abs() / binance_volatility_5s).min(10.0);
        let decay = (-2.0 * venue_delta_5s.abs()).exp();
        let age_factor = data_age_sec.clamp(0.0, 10.0);

        let lag_raw = z_score * decay * age_factor * 10.0;
        lag_raw.clamp(0.0, 100.0)
    }

    /// 3. Binary Mispricing Score
    pub fn calculate_mispricing(
        spot_price: f64,
        strike: f64,
        volatility: f64,
        time_rem_sec: f64,
        venue_price: f64,
    ) -> f64 {
        if time_rem_sec <= 0.0 || volatility <= 0.0 {
            return 0.0;
        }
        let t_days = time_rem_sec / 86400.0;
        let z = (spot_price / strike).ln() / (volatility * t_days.sqrt());

        // Approx standard normal CDF
        let p_theo = 1.0 / (1.0 + (-1.702 * z).exp());

        let diff = (venue_price - p_theo).abs();
        (diff * 100.0).clamp(0.0, 100.0)
    }

    /// 4. Venue Divergence Score
    pub fn calculate_divergence(
        ask_poly: f64,
        ask_kalshi: f64,
        spread_poly: f64,
        spread_kalshi: f64,
        est_slippage: f64,
    ) -> f64 {
        let raw_diff = (ask_poly - ask_kalshi).abs();
        let max_spread = spread_poly.max(spread_kalshi);
        let net_diff = (raw_diff - max_spread - est_slippage).max(0.0);
        (net_diff * 100.0).clamp(0.0, 100.0)
    }

    /// 5. Liquidity Reliability Score
    pub fn calculate_liquidity_reliability(vol_top2: f64, order_size: f64, spread_pct: f64) -> f64 {
        if order_size <= 0.0 {
            return 0.0;
        }
        let ratio = vol_top2 / order_size;
        let depth_score = (ratio * 33.3).clamp(0.0, 100.0);
        let spread_penalty = spread_pct * 1000.0;

        (depth_score - spread_penalty).clamp(0.0, 100.0)
    }

    /// 6. Execution Risk Score
    pub fn calculate_execution_risk(ping_ms: f64, timeouts_5m: f64, time_rem_sec: f64) -> f64 {
        let time_penalty = if time_rem_sec > 0.0 {
            300.0 / time_rem_sec
        } else {
            100.0
        };
        let raw_risk = (ping_ms * 0.1) + (timeouts_5m * 10.0) + time_penalty;
        raw_risk.clamp(0.0, 100.0)
    }
}

impl QuantScores {
    pub fn calculate_edge_confidence(
        &mut self,
        config: &QuantConfig,
        data_age_sec: f64,
        time_rem_sec: f64,
    ) {
        let total_weight =
            config.w_eq + config.w_lag + config.w_mis + config.w_div + config.w_liq + config.w_risk;

        let base_score = ((config.w_eq * self.equivalence)
            + (config.w_lag * self.venue_lag)
            + (config.w_mis * self.mispricing)
            + (config.w_div * self.divergence)
            + (config.w_liq * self.liquidity_reliability)
            - (config.w_risk * self.execution_risk))
            / total_weight;

        let stale_penalty = (data_age_sec * config.stale_penalty_factor).min(50.0);
        let time_pressure_penalty = if time_rem_sec < 10.0 { 20.0 } else { 0.0 };

        self.edge_confidence =
            (base_score - stale_penalty - time_pressure_penalty).clamp(0.0, 100.0);
    }

    pub fn should_enter_trade(&mut self, config: &QuantConfig) -> bool {
        if self.equivalence < config.min_equivalence {
            self.rejection_reason = Some("LOW_EQUIVALENCE".into());
            self.is_tradable = false;
            return false;
        }
        if self.liquidity_reliability < config.min_liquidity {
            self.rejection_reason = Some("POOR_LIQUIDITY".into());
            self.is_tradable = false;
            return false;
        }
        if self.execution_risk > config.max_exec_risk {
            self.rejection_reason = Some("HIGH_EXECUTION_RISK".into());
            self.is_tradable = false;
            return false;
        }
        if self.edge_confidence < config.min_edge_confidence {
            self.rejection_reason =
                Some(format!("LOW_EDGE_CONFIDENCE: {:.1}", self.edge_confidence));
            self.is_tradable = false;
            return false;
        }

        self.is_tradable = true;
        true
    }

    pub fn log_scores(&self) {
        log::info!("=== MITHOS OMEGA: QUANT SCORES ===");
        log::info!("Equivalence: {:.1}", self.equivalence);
        log::info!("Venue Lag: {:.1}", self.venue_lag);
        log::info!("Mispricing: {:.1}", self.mispricing);
        log::info!("Divergence: {:.1}", self.divergence);
        log::info!("Liquidity: {:.1}", self.liquidity_reliability);
        log::info!("Exec Risk: {:.1}", self.execution_risk);
        log::info!("----------------------------------");
        if self.is_tradable {
            log::info!("🟢 EDGE CONFIDENCE: {:.1} [TRADABLE]", self.edge_confidence);
        } else {
            log::info!(
                "🔴 EDGE CONFIDENCE: {:.1} [REJECTED: {}]",
                self.edge_confidence,
                self.rejection_reason.as_deref().unwrap_or("UNKNOWN")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_liquidity_score_penalizes_spread() {
        // High volume, but terrible spread (4%)
        let score = AnomalyEngine::calculate_liquidity_reliability(1000.0, 10.0, 0.04);
        // depth_score = (1000/10)*33.3 = 100 (clamped max)
        // spread penalty = 0.04 * 1000 = 40
        // score = 100 - 40 = 60
        assert_eq!(score, 60.0);

        let config = QuantConfig::default();
        let mut scores = QuantScores {
            equivalence: 100.0,
            liquidity_reliability: score,
            ..Default::default()
        };

        assert_eq!(scores.should_enter_trade(&config), false);
        assert_eq!(scores.rejection_reason, Some("POOR_LIQUIDITY".into()));
    }

    #[test]
    fn test_execution_risk_spikes_near_expiry() {
        // Less than 30s remaining
        let score = AnomalyEngine::calculate_execution_risk(50.0, 0.0, 10.0);
        // (50*0.1) + 0 + (300/10) = 5 + 30 = 35. This is right at the max_exec_risk line.
        assert!(score >= 35.0);

        let score_extreme = AnomalyEngine::calculate_execution_risk(100.0, 1.0, 5.0);
        // (100*0.1) + 10 + (300/5) = 10 + 10 + 60 = 80
        assert_eq!(score_extreme, 80.0);

        let config = QuantConfig::default();
        let mut scores = QuantScores {
            equivalence: 100.0,
            liquidity_reliability: 100.0,
            execution_risk: score_extreme,
            ..Default::default()
        };

        assert_eq!(scores.should_enter_trade(&config), false);
        assert_eq!(scores.rejection_reason, Some("HIGH_EXECUTION_RISK".into()));
    }

    #[test]
    fn test_edge_confidence_aggregation() {
        let config = QuantConfig::default();
        let mut scores = QuantScores {
            equivalence: 100.0,
            venue_lag: 80.0,  // Strong lag signal
            mispricing: 50.0, // Good mispricing
            divergence: 20.0, // Solid divergence
            liquidity_reliability: 100.0,
            execution_risk: 10.0, // Low risk
            ..Default::default()
        };

        scores.calculate_edge_confidence(&config, 0.1, 300.0);
        assert!(scores.edge_confidence > 0.0);

        let tradable = scores.should_enter_trade(&config);

        // If the resulting score is high enough, it should be tradable.
        // We log the value for debugging.
        println!("Edge Confidence: {}", scores.edge_confidence);
    }

    #[test]
    fn entry_gate_rejects_stale_quote() {
        let config = EntryGateConfig::default();
        let result = evaluate_entry_gate(
            &config,
            EntryGateInputs {
                quote_age_ms: 1_500,
                spread_pct: 0.01,
                edge_confidence: 95.0,
            },
        );

        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("STALE_QUOTE"));
    }

    #[test]
    fn entry_gate_rejects_wide_spread() {
        let config = EntryGateConfig::default();
        let result = evaluate_entry_gate(
            &config,
            EntryGateInputs {
                quote_age_ms: 10,
                spread_pct: 0.20,
                edge_confidence: 95.0,
            },
        );

        assert!(!result.allowed);
        assert_eq!(result.reason.as_deref(), Some("WIDE_SPREAD"));
    }
}
