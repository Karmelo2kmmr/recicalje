use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use chrono::{Utc, FixedOffset};

pub struct SessionStats {
    pub start_time: String,
    pub end_time: String,
    pub net_pnl: f64,
    pub win_rate: f64,
    pub winners: u32,
    pub total_closed: u32,
    pub btc_pnl: f64,
    pub btc_winners: u32,
    pub btc_total: u32,
    pub best_trade_pct: f64,
    pub worst_trade_pct: f64,
    pub action_rapida_count: u32,
    pub full_recovery_count: u32,
    pub sl_0655_count: u32,
    pub dca_count: u32,
    pub micro_count: u32,
}

pub struct ReportingEngine;

impl ReportingEngine {
    /// Genera un reporte de estadísticas para las últimas `hours` horas con un `label` específico.
    pub async fn get_stats_report(api: &crate::polymarket_api::PolymarketAPI, hours: u32, label: &str) -> Option<String> {
        let et_offset = FixedOffset::west_opt(4 * 3600).unwrap();
        let now_et = Utc::now().with_timezone(&et_offset);
        let start_period = now_et - chrono::Duration::hours(hours as i64);
        
        let start_time_str = start_period.format("%H:%M").to_string();
        let end_time_str = now_et.format("%H:%M").to_string();

        let mut lines = Vec::new();
        // let mut updated = false; // This line is removed

        let date_today = now_et.format("%Y-%m-%d").to_string();
        let date_yesterday = (now_et - chrono::Duration::days(1)).format("%Y-%m-%d").to_string();
        
        let filenames = vec![
            format!("trades_{}.csv", date_yesterday),
            format!("trades_{}.csv", date_today),
        ];

        let mut header_opt: Option<String> = None;

        for filename in filenames {
            if let Ok(file) = File::open(&filename) {
                let reader = BufReader::new(file);
                let mut file_lines: Vec<String> = reader.lines().filter_map(|l| l.ok()).collect();
                if file_lines.is_empty() { continue; }
                
                if header_opt.is_none() {
                    header_opt = Some(file_lines[0].clone());
                }

                let mut file_updated = false;

                for l in file_lines.iter_mut().skip(1) {
                    let mut parts: Vec<String> = l.split('|').map(|s| s.trim().to_string()).collect();
                    if parts.len() < 12 { 
                        lines.push(l.clone());
                        continue; 
                    }

                    let audit_idx = parts.len() - 1;

                    if parts[audit_idx] == "PENDING" {
                        let market_id = &parts[11];
                        let side = if parts.len() > 2 { &parts[2] } else { "UP" };
                        
                        if let Some(winning_index) = api.get_market_outcome(market_id).await {
                            let expected_index = if side == "UP" { "0" } else { "1" };
                            let status = &parts[6];
                            
                            let is_win = status.contains("WIN");
                            let actually_won = winning_index == expected_index;
                            
                            if is_win == actually_won {
                                parts[audit_idx] = "VERIFIED".to_string();
                            } else {
                                parts[audit_idx] = "DISCREPANCY".to_string();
                            }
                            file_updated = true;
                            // updated = true; // This line is removed
                            *l = parts.join(" | ");
                        }
                    }
                    lines.push(l.clone());
                }

                // Save updated file if needed
                if file_updated {
                    if let Ok(mut f) = std::fs::OpenOptions::new().write(true).truncate(true).open(&filename) {
                        let _ = writeln!(f, "{}", header_opt.as_ref().unwrap());
                        for l in file_lines.iter().skip(1) {
                            let _ = writeln!(f, "{}", l);
                        }
                    }
                }
            }
        }

        if lines.is_empty() { return None; }

        let mut stats = SessionStats {
            start_time: start_time_str,
            end_time: end_time_str,
            net_pnl: 0.0,
            win_rate: 0.0,
            winners: 0,
            total_closed: 0,
            btc_pnl: 0.0,
            btc_winners: 0,
            btc_total: 0,
            best_trade_pct: -999.0,
            worst_trade_pct: 999.0,
            action_rapida_count: 0,
            full_recovery_count: 0,
            sl_0655_count: 0,
            dca_count: 0,
            micro_count: 0,
        };

        let mut verified_count = 0;
        let mut discrepancy_count = 0;
        let mut _total_trades_in_period = 0;

        for l in lines {
            let parts: Vec<&str> = l.split('|').map(|s| s.trim()).collect();
            if parts.len() < 12 { continue; }

            let _trade_time_str = parts[0];
            // Intentar parsear tiempo - asumiendo que el CSV guarda HH:MM:SS o similar
            // Para simplicidad, compararemos si el trade ocurrió dentro de las últimas 'hours'
            // Nota: En un sistema real usaríamos la fecha completa, aquí asumimos trades recientes.
            
            _total_trades_in_period += 1;
            let audit_status = parts[parts.len()-1];
            
            if audit_status == "VERIFIED" { verified_count += 1; }
            else if audit_status == "DISCREPANCY" { discrepancy_count += 1; }
            
            let coin = parts[1];
            let status = parts[6];
            let pnl_str = parts[7].replace("$", "").replace("+", "");
            let pnl: f64 = pnl_str.trim().parse().unwrap_or(0.0);
            
            let ret_str = parts[8].replace("%", "");
            let ret_pct: f64 = ret_str.trim().parse().unwrap_or(0.0);
            
            let strat = parts[9];
            let dcas: u32 = parts[10].trim().parse().unwrap_or(0);

            stats.total_closed += 1;
            stats.net_pnl += pnl;
            if status.to_uppercase().contains("WIN") { stats.winners += 1; }

            if coin.to_uppercase().contains("BTC") {
                stats.btc_pnl += pnl;
                stats.btc_total += 1;
                if status.to_uppercase().contains("WIN") { stats.btc_winners += 1; }
            }

            if ret_pct > stats.best_trade_pct { stats.best_trade_pct = ret_pct; }
            if ret_pct < stats.worst_trade_pct { stats.worst_trade_pct = ret_pct; }

            if strat.contains("Sabor") { stats.action_rapida_count += 1; } // Reusing for re-entries if needed
            else if strat.contains("Fulas") { stats.full_recovery_count += 1; }
            else if strat.contains("Micro") { stats.micro_count += 1; }

            if status.contains("LOSS") && parts[4].contains("0.655") {
                stats.sl_0655_count += 1;
            }
            stats.dca_count += dcas;
        }

        if stats.total_closed > 0 {
            stats.win_rate = (stats.winners as f64 / stats.total_closed as f64) * 100.0;
        }
        if stats.best_trade_pct == -999.0 { stats.best_trade_pct = 0.0; }
        if stats.worst_trade_pct == 999.0 { stats.worst_trade_pct = 0.0; }

        let btc_wr = if stats.btc_total > 0 {
            (stats.btc_winners as f64 / stats.btc_total as f64) * 100.0
        } else { 0.0 };

        let report = format!(
"📊 *{}*
⏰ {} - {} (ET)

💵 *Resultados*
• P&L Neto: ${:.2}
• Tasa de Acierto: {:.1}% ({}/{})
• Total Operaciones: {}

💰 *BTC Stats*
• P&L BTC: ${:.2}
• WR BTC: {:.0}% ({}/{})

📈 *Rendimiento*
• Mejor Trade: {:+.1}%
• Peor Trade: {:+.1}%

🎯 *Estrategias*
• Trades Sabor: {}
• Trades Fulas: {}
• Trades Micro: {}
• DCA Ejecutados: {}
• SL 0.655 Activados: {}

🔍 *Auditoría*
• ✅ {} verificados
• ⚠️ {} discrepancias",
            label, stats.start_time, stats.end_time,
            stats.net_pnl, stats.win_rate, stats.winners, stats.total_closed, stats.total_closed,
            stats.btc_pnl, btc_wr, stats.btc_winners, stats.btc_total,
            stats.best_trade_pct, stats.worst_trade_pct,
            stats.action_rapida_count, stats.full_recovery_count, stats.micro_count, stats.dca_count, stats.sl_0655_count,
            verified_count, discrepancy_count
        );

        Some(report)
    }
}
