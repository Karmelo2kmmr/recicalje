fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CrisisLevel {
    Normal,
    Warning,
    Critical,
    Nuclear,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CrisisInputs {
    pub consecutive_timeouts: u32,
    pub consecutive_partial_fills: u32,
    pub consecutive_desync_failures: u32,
    pub locked_funds_ratio: f64,
    pub price_staleness_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct CrisisConfig {
    pub timeout_weight: u32,
    pub partial_fill_weight: u32,
    pub desync_failure_weight: u32,
    pub warning_score: u32,
    pub critical_score: u32,
    pub nuclear_score: u32,
    pub max_consecutive_timeouts: u32,
    pub max_consecutive_partial_fills: u32,
    pub max_locked_funds_ratio: f64,
    pub max_price_staleness_ms: u64,
}

impl Default for CrisisConfig {
    fn default() -> Self {
        Self {
            timeout_weight: env_u32("CRISIS_TIMEOUT_WEIGHT", 20),
            partial_fill_weight: env_u32("CRISIS_PARTIAL_FILL_WEIGHT", 25),
            desync_failure_weight: env_u32("CRISIS_DESYNC_FAILURE_WEIGHT", 15),
            warning_score: env_u32("CRISIS_WARNING_SCORE", 25),
            critical_score: env_u32("CRISIS_CRITICAL_SCORE", 60),
            nuclear_score: env_u32("CRISIS_NUCLEAR_SCORE", 100),
            max_consecutive_timeouts: env_u32("MAX_CONSECUTIVE_TIMEOUTS", 3),
            max_consecutive_partial_fills: env_u32("MAX_CONSECUTIVE_PARTIAL_FILLS", 2),
            max_locked_funds_ratio: env_f64("MAX_LOCKED_FUNDS_RATIO", 0.25),
            max_price_staleness_ms: env_u64("MAX_PRICE_STALENESS_MS", 5_000),
        }
    }
}

pub fn calculate_crisis_score(inputs: CrisisInputs) -> CrisisLevel {
    calculate_crisis_score_with_config(inputs, CrisisConfig::default())
}

pub fn calculate_crisis_score_with_config(
    inputs: CrisisInputs,
    config: CrisisConfig,
) -> CrisisLevel {
    if inputs.consecutive_timeouts >= config.max_consecutive_timeouts
        || inputs.consecutive_partial_fills >= config.max_consecutive_partial_fills
        || inputs.locked_funds_ratio >= config.max_locked_funds_ratio
        || inputs.price_staleness_ms >= config.max_price_staleness_ms
    {
        return CrisisLevel::Nuclear;
    }

    let mut score = inputs
        .consecutive_timeouts
        .saturating_mul(config.timeout_weight)
        .saturating_add(
            inputs
                .consecutive_partial_fills
                .saturating_mul(config.partial_fill_weight),
        )
        .saturating_add(
            inputs
                .consecutive_desync_failures
                .saturating_mul(config.desync_failure_weight),
        );

    score = score.saturating_add(if inputs.locked_funds_ratio >= 0.10 {
        35
    } else if inputs.locked_funds_ratio >= 0.05 {
        20
    } else {
        0
    });

    score = score.saturating_add(if inputs.price_staleness_ms >= 3_000 {
        50
    } else if inputs.price_staleness_ms >= 1_500 {
        30
    } else if inputs.price_staleness_ms >= 750 {
        15
    } else {
        0
    });

    if score >= config.nuclear_score {
        CrisisLevel::Nuclear
    } else if score >= config.critical_score {
        CrisisLevel::Critical
    } else if score >= config.warning_score {
        CrisisLevel::Warning
    } else {
        CrisisLevel::Normal
    }
}

#[derive(Debug, Clone)]
pub struct CrisisSupervisor {
    config: CrisisConfig,
    last_level: CrisisLevel,
    last_inputs: CrisisInputs,
}

impl CrisisSupervisor {
    pub fn new(config: CrisisConfig) -> Self {
        Self {
            config,
            last_level: CrisisLevel::Normal,
            last_inputs: CrisisInputs::default(),
        }
    }

    pub fn from_env() -> Self {
        Self::new(CrisisConfig::default())
    }

    pub fn evaluate(&mut self, inputs: CrisisInputs) -> CrisisLevel {
        self.last_inputs = inputs;
        self.last_level = calculate_crisis_score_with_config(inputs, self.config);
        self.last_level
    }

    pub fn level(&self) -> CrisisLevel {
        self.last_level
    }

    pub fn inputs(&self) -> CrisisInputs {
        self.last_inputs
    }

    pub fn blocks_entries(&self) -> bool {
        self.last_level >= CrisisLevel::Critical
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nuclear_when_timeouts_stack_with_stale_quotes() {
        let level = calculate_crisis_score(CrisisInputs {
            consecutive_timeouts: 3,
            consecutive_partial_fills: 0,
            consecutive_desync_failures: 0,
            locked_funds_ratio: 0.0,
            price_staleness_ms: 5_500,
        });

        assert_eq!(level, CrisisLevel::Nuclear);
    }

    #[test]
    fn critical_blocks_new_entries() {
        let mut supervisor = CrisisSupervisor::new(CrisisConfig::default());
        let level = supervisor.evaluate(CrisisInputs {
            consecutive_timeouts: 0,
            consecutive_partial_fills: 2,
            consecutive_desync_failures: 0,
            locked_funds_ratio: 0.0,
            price_staleness_ms: 0,
        });

        assert_eq!(level, CrisisLevel::Nuclear);
        assert!(supervisor.blocks_entries());
    }
}
