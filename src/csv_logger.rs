use std::fs::OpenOptions;
use std::io::Write;

use log::error;

#[derive(Clone)]
pub struct CSVLogger;

impl CSVLogger {
    pub fn new() -> Self {
        Self
    }

    pub fn log_trade(
        &self,
        coin: &str,
        side: &str,
        entry: f64,
        exit: f64,
        rez: &str,
        status: &str,
        pnl: f64,
        ret_pct: f64,
        strat: &str,
        dca: u32,
        market_id: &str,
        equity_before: f64,
        stake: f64,
        equity_after: f64,
        volatility: &str,
    ) {
        let now_et = crate::time_utils::new_york_now();
        let time_str = now_et.format("%H:%M:%S").to_string();
        let date_str = now_et.format("%Y-%m-%d").to_string();

        let dated_filename = format!("trades_{}.csv", date_str);
        let master_filename = "trades.csv".to_string();

        let pnl_str = if pnl >= 0.0 {
            format!("$+{:.2}", pnl)
        } else {
            format!("$-{:.2}", pnl.abs())
        };
        let ret_str = format!("{:.1} %", ret_pct);

        let row = format!(
            "{:<10} | {:<5} | {:<4} | {:<6.3} | {:<6.3} | {:<3} | {:<21} | {:<8} | {:<7} | {:<20} | {:<3} | {:<10} | {:<42} | ${:<9.2}| ${:<8.2}| ${:<9.2}| {}",
            time_str,
            coin,
            side,
            entry,
            exit,
            rez,
            status,
            pnl_str,
            ret_str,
            strat,
            dca,
            volatility,
            market_id,
            equity_before,
            stake,
            equity_after,
            "PENDING"
        );

        for filename in &[dated_filename, master_filename] {
            if !std::path::Path::new(filename).exists() {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(filename) {
                    let _ = writeln!(
                        file,
                        "TIME (ET) | COIN | SIDE | ENTRY | EXIT | REZ | STATUS | PNL | RET% | STRAT | DCA | VOLAT | MARKET_ID | EQUITY_BEFORE | STAKE | EQUITY_AFTER | AUDIT"
                    );
                }
            }

            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(filename) {
                if let Err(e) = writeln!(file, "{}", row) {
                    error!("Failed to write to {}: {}", filename, e);
                }
            }
        }
    }
}
