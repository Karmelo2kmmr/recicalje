use std::fs::File;
use std::io::{BufRead, BufReader, Write};

use chrono::{FixedOffset, Utc};

pub struct SessionStats {
    pub start_time: String,
    pub end_time: String,
    pub net_pnl: f64,
    pub win_rate: f64,
    pub winners: u32,
    pub total_closed: u32,
    pub best_trade_pct: f64,
    pub worst_trade_pct: f64,
    pub alpha_count: u32,
    pub verified_count: u32,
    pub discrepancy_count: u32,
}

pub struct ReportingEngine;

impl ReportingEngine {
    pub async fn get_stats_report(
        api: &crate::polymarket_api::PolymarketAPI,
        hours: u32,
        label: &str,
    ) -> Option<String> {
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let now_et = Utc::now().with_timezone(&et_offset);
        let start_period = now_et - chrono::Duration::hours(hours as i64);

        let start_time_str = start_period.format("%H:%M").to_string();
        let end_time_str = now_et.format("%H:%M").to_string();

        let date_today = now_et.format("%Y-%m-%d").to_string();
        let date_yesterday = (now_et - chrono::Duration::days(1))
            .format("%Y-%m-%d")
            .to_string();

        let filenames = vec![
            format!("trades_{}.csv", date_yesterday),
            format!("trades_{}.csv", date_today),
        ];

        let mut lines = Vec::new();
        let mut header_opt: Option<String> = None;

        for filename in filenames {
            if let Ok(file) = File::open(&filename) {
                let reader = BufReader::new(file);
                let mut file_lines: Vec<String> = reader.lines().map_while(Result::ok).collect();
                if file_lines.is_empty() {
                    continue;
                }

                if header_opt.is_none() {
                    header_opt = Some(file_lines[0].clone());
                }

                let mut file_updated = false;
                for line in file_lines.iter_mut().skip(1) {
                    let mut parts: Vec<String> =
                        line.split('|').map(|s| s.trim().to_string()).collect();
                    if parts.len() < 17 {
                        continue;
                    }

                    let audit_idx = parts.len() - 1;
                    let market_id_idx = 12;
                    let side_idx = 2;
                    let status_idx = 6;

                    if parts[audit_idx] == "PENDING" {
                        let market_id = &parts[market_id_idx];
                        let side = &parts[side_idx];

                        if let Some(winning_index) = api.get_market_outcome(market_id).await {
                            let expected_index = if side == "UP" { "0" } else { "1" };
                            let is_win = parts[status_idx].contains("WIN");
                            let actually_won = winning_index == expected_index;

                            parts[audit_idx] = if is_win == actually_won {
                                "VERIFIED".to_string()
                            } else {
                                "DISCREPANCY".to_string()
                            };
                            *line = parts.join(" | ");
                            file_updated = true;
                        }
                    }

                    lines.push(line.clone());
                }

                if file_updated {
                    if let Ok(mut file) = std::fs::OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(&filename)
                    {
                        let _ = writeln!(file, "{}", header_opt.as_ref().unwrap());
                        for line in file_lines.iter().skip(1) {
                            let _ = writeln!(file, "{}", line);
                        }
                    }
                }
            }
        }

        if lines.is_empty() {
            return None;
        }

        let mut stats = SessionStats {
            start_time: start_time_str,
            end_time: end_time_str,
            net_pnl: 0.0,
            win_rate: 0.0,
            winners: 0,
            total_closed: 0,
            best_trade_pct: f64::NEG_INFINITY,
            worst_trade_pct: f64::INFINITY,
            alpha_count: 0,
            verified_count: 0,
            discrepancy_count: 0,
        };

        for line in lines {
            let parts: Vec<&str> = line.split('|').map(|s| s.trim()).collect();
            if parts.len() < 17 {
                continue;
            }

            let status = parts[6];
            let pnl_str = parts[7].replace('$', "").replace('+', "");
            let pnl: f64 = pnl_str.trim().parse().unwrap_or(0.0);
            let ret_str = parts[8].replace('%', "");
            let ret_pct: f64 = ret_str.trim().parse().unwrap_or(0.0);
            let strat = parts[9];
            let audit_status = parts[16];

            stats.total_closed += 1;
            stats.net_pnl += pnl;
            if status.contains("WIN") {
                stats.winners += 1;
            }
            if strat.contains("ALPHA") {
                stats.alpha_count += 1;
            }
            if audit_status == "VERIFIED" {
                stats.verified_count += 1;
            } else if audit_status == "DISCREPANCY" {
                stats.discrepancy_count += 1;
            }

            stats.best_trade_pct = stats.best_trade_pct.max(ret_pct);
            stats.worst_trade_pct = stats.worst_trade_pct.min(ret_pct);
        }

        if stats.total_closed > 0 {
            stats.win_rate = (stats.winners as f64 / stats.total_closed as f64) * 100.0;
        }
        if !stats.best_trade_pct.is_finite() {
            stats.best_trade_pct = 0.0;
        }
        if !stats.worst_trade_pct.is_finite() {
            stats.worst_trade_pct = 0.0;
        }

        Some(format!(
            "📊 *{}*\n\
            ⏰ {} - {} (ET)\n\n\
            💵 *Resultados*\n\
            • P&L Neto: ${:.2}\n\
            • Tasa de Acierto: {:.1}% ({}/{})\n\
            • Trades Alpha: {}\n\n\
            📈 *Rendimiento*\n\
            • Mejor Trade: {:+.1}%\n\
            • Peor Trade: {:+.1}%\n\n\
            🔍 *Auditoria*\n\
            • ✅ {} verificados\n\
            • ⚠️ {} discrepancias",
            label,
            stats.start_time,
            stats.end_time,
            stats.net_pnl,
            stats.win_rate,
            stats.winners,
            stats.total_closed,
            stats.alpha_count,
            stats.best_trade_pct,
            stats.worst_trade_pct,
            stats.verified_count,
            stats.discrepancy_count
        ))
    }
}
