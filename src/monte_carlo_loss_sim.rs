use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum FailureType {
    PartialFill(f64),
    CancelFailure,
    APITimeoutAmbiguity,
    StaleDataBlindness,
    ReconciliationDesync,
    SlippageSpike(f64),
}

#[derive(Debug, Clone)]
pub struct SimulationConfig {
    pub initial_capital: f64,
    pub position_size: f64,
    pub path_count: u32,
    pub trades_per_path: u32,
    pub ruin_threshold_pct: f64,    // e.g. 0.50 (50% drawdown)
    pub death_threshold_pct: f64,   // e.g. 0.10 (10% capital remaining)
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct AccountPathResult {
    pub final_capital: f64,
    pub max_drawdown_pct: f64,
    pub worst_single_loss: f64,
    pub did_hit_ruin: bool,
    pub did_hit_death: bool,
    pub trade_count: u32,
}

pub struct MonteCarloSim {
    pub config: SimulationConfig,
    pub global_rng: StdRng,
}

impl MonteCarloSim {
    pub fn new(config: SimulationConfig) -> Self {
        Self {
            global_rng: StdRng::seed_from_u64(config.seed),
            config,
        }
    }

    pub fn run_monte_carlo_paths(&mut self) -> Vec<AccountPathResult> {
        let mut results = Vec::with_capacity(self.config.path_count as usize);
        
        for _ in 0..self.config.path_count {
            let path_seed = self.global_rng.gen::<u64>();
            let result = self.run_account_path(path_seed);
            results.push(result);
        }
        
        results
    }

    pub fn run_sensitivity_analysis(initial_capital: f64, sizes: &[f64], paths: u32, trades: u32, seed: u64) {
        println!("====================================================");
        println!("   MITHOS OMEGA: POSITION SIZE SENSITIVITY ANALYSIS");
        println!("====================================================");
        println!("{:<6} | {:<7} | {:<7} | {:<7} | {:<7} | {:<7}", "Size", "P.Ruin", "P.Death", "P50 Cap", "P99 DD", "Worst");
        println!("-------|---------|---------|---------|---------|-------");

        for &size in sizes {
            let config = SimulationConfig {
                initial_capital,
                position_size: size,
                path_count: paths,
                trades_per_path: trades,
                ruin_threshold_pct: 0.50,
                death_threshold_pct: 0.10,
                seed,
            };
            let mut sim = MonteCarloSim::new(config);
            let results = sim.run_monte_carlo_paths();
            
            let ruin_prob = results.iter().filter(|r| r.did_hit_ruin).count() as f64 / paths as f64;
            let death_prob = results.iter().filter(|r| r.did_hit_death).count() as f64 / paths as f64;
            
            let mut final_caps: Vec<f64> = results.iter().map(|r| r.final_capital).collect();
            let mut dds: Vec<f64> = results.iter().map(|r| r.max_drawdown_pct).collect();
            final_caps.sort_by(|a, b| a.partial_cmp(b).unwrap());
            dds.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let p50_cap = get_percentile(&final_caps, 0.50);
            let p99_dd = get_percentile(&dds, 0.99) * 100.0;
            let worst_cap = final_caps.first().unwrap_or(&0.0);

            println!("${:<5} | {:<6.1}% | {:<6.1}% | ${:<6.0} | {:<6.1}% | ${:<5.0}", 
                size, ruin_prob * 100.0, death_prob * 100.0, p50_cap, p99_dd, worst_cap);
        }
        println!("====================================================");
    }

    fn run_account_path(&self, seed: u64) -> AccountPathResult {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut capital = self.config.initial_capital;
        let mut peak_capital = capital;
        let mut max_drawdown = 0.0;
        let mut worst_loss = 0.0;
        let mut did_hit_ruin = false;
        let mut did_hit_death = false;

        for _ in 0..self.config.trades_per_path {
            if capital <= 1.0 {
                did_hit_death = true;
                break;
            }

            let result = self.simulate_single_trade(&mut rng, capital);
            capital += result;

            if capital > peak_capital {
                peak_capital = capital;
            }

            let drawdown = (peak_capital - capital) / peak_capital;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }

            if result < 0.0 && result.abs() > worst_loss {
                worst_loss = result.abs();
            }

            if max_drawdown >= self.config.ruin_threshold_pct {
                did_hit_ruin = true;
            }
            if capital <= (self.config.initial_capital * self.config.death_threshold_pct) {
                did_hit_death = true;
            }
        }

        AccountPathResult {
            final_capital: capital,
            max_drawdown_pct: max_drawdown,
            worst_single_loss: worst_loss,
            did_hit_ruin,
            did_hit_death,
            trade_count: self.config.trades_per_path,
        }
    }

    fn simulate_single_trade(&self, rng: &mut StdRng, current_cap: f64) -> f64 {
        let roll = rng.gen_range(0..1000);
        
        // Base probability of win: 55%
        let is_win = rng.gen_bool(0.55);
        let mut edge = if is_win { 0.15 } else { -0.12 };

        // Toxic event injection
        if roll >= 900 {
            if roll < 950 { // 5% Slippage Spike
                edge -= rng.gen_range(0.05..0.20);
            } else if roll < 970 { // 2% Partial Fill
                if is_win { edge *= 0.3; } else { edge *= 1.5; }
            } else if roll < 985 { // 1.5% Timeout
                edge -= 0.25;
            } else if roll < 995 { // 1% Stale
                edge -= 0.15;
            } else { // 0.5% Cancel Failure
                edge -= 0.40;
            }
        }

        // Random noise slippage
        edge -= rng.gen_range(0.0..0.02);

        self.config.position_size * edge
    }

    pub fn analyze_results(results: &[AccountPathResult], initial_capital: f64) {
        let count = results.len() as f64;
        let ruin_count = results.iter().filter(|r| r.did_hit_ruin).count() as f64;
        let death_count = results.iter().filter(|r| r.did_hit_death).count() as f64;

        let mut final_capitals: Vec<f64> = results.iter().map(|r| r.final_capital).collect();
        let mut drawdowns: Vec<f64> = results.iter().map(|r| r.max_drawdown_pct).collect();

        final_capitals.sort_by(|a, b| a.partial_cmp(b).unwrap());
        drawdowns.sort_by(|a, b| a.partial_cmp(b).unwrap());

        println!("====================================================");
        println!("   MITHOS OMEGA: MONTE CARLO MULTI-PATH REPORT");
        println!("====================================================");
        println!("Total Paths:           {}", results.len());
        println!("Initial Capital:       ${:.2}", initial_capital);
        println!("----------------------------------------------------");
        println!("Probability of Ruin:   {:.2}%", (ruin_count / count) * 100.0);
        println!("Probability of Death:  {:.2}%", (death_count / count) * 100.0);
        println!("----------------------------------------------------");
        println!("Percentile Analysis (Final Capital):");
        println!(" P50 (Median):         ${:.2}", get_percentile(&final_capitals, 0.50));
        println!(" P75:                  ${:.2}", get_percentile(&final_capitals, 0.25)); // Sorted asc, so P75 is at 0.25 index from top? No, 0.75
        println!(" P90:                  ${:.2}", get_percentile(&final_capitals, 0.10));
        println!(" P95:                  ${:.2}", get_percentile(&final_capitals, 0.05));
        println!(" P99:                  ${:.2}", get_percentile(&final_capitals, 0.01));
        println!("----------------------------------------------------");
        println!("Percentile Analysis (Max Drawdown):");
        println!(" P50 (Median):         {:.2}%", get_percentile(&drawdowns, 0.50) * 100.0);
        println!(" P90:                  {:.2}%", get_percentile(&drawdowns, 0.90) * 100.0);
        println!(" P95:                  {:.2}%", get_percentile(&drawdowns, 0.95) * 100.0);
        println!(" P99:                  {:.2}%", get_percentile(&drawdowns, 0.99) * 100.0);
        println!("====================================================");
    }
}

fn get_percentile(data: &[f64], p: f64) -> f64 {
    if data.is_empty() { return 0.0; }
    let idx = (p * (data.len() - 1) as f64) as usize;
    data[idx]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_position_sensitivity() {
        let sizes = vec![1.0, 2.0, 3.0, 5.0, 7.0, 10.0, 13.0, 15.0, 27.0, 33.0];
        MonteCarloSim::run_sensitivity_analysis(100.0, &sizes, 10000, 1000, 42);
    }
}
