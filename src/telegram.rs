use crate::audit::AuditReport;
use crate::stats_reporter::{DailyStats, PeriodStats};
use log::{error, info};
use reqwest::Client;

#[derive(Clone)]
pub struct TelegramBot {
    token: String,
    chat_id: String,
    client: Client,
}

impl TelegramBot {
    pub fn new() -> Option<Self> {
        let token = std::env::var("TELEGRAM_BOT_TOKEN").ok()?;
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").ok()?;

        if token.is_empty() || chat_id.is_empty() {
            return None;
        }

        let client = Client::builder()
            .no_proxy()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(20))
            .build()
            .ok()?;

        Some(Self {
            token,
            chat_id,
            client,
        })
    }

    pub async fn send_message(&self, text: &str) {
        let url = format!("https://api.telegram.org/bot{}/sendMessage", self.token);

        let params = [
            ("chat_id", &self.chat_id),
            ("text", &text.to_string()),
            ("parse_mode", &"Markdown".to_string()),
        ];

        match self.client.post(&url).form(&params).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let err_body = resp
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    error!("Telegram send failed ({}): {}", text, err_body);
                } else {
                    info!("Telegram message sent: {}", text.replace('\n', " | "));
                }
            }
            Err(e) => error!("Telegram error: {}", e),
        }
    }

    pub fn format_period_report(&self, stats: &PeriodStats, audit: &AuditReport) -> String {
        let win_rate = if stats.total_trades > 0 {
            (stats.winning_trades as f64 / stats.total_trades as f64) * 100.0
        } else {
            0.0
        };

        let audit_status = if audit.is_fully_verified {
            "Auditado"
        } else {
            "Parcial"
        };

        let mut report = format!(
            "*REPORTE DE SESION (6H)*\n\
Periodo: {} - {}\n\
Estado: {}\n\
{}\n\
*Resultados*\n\
- P&L neto: *${:.2}*\n\
- Tasa de acierto: *{:.1}%* ({}/{})\n\
- Operaciones pendientes: {}\n\
- Total operaciones: {}\n\n",
            stats.period_start,
            stats.period_end,
            audit_status,
            stats
                .notice
                .as_deref()
                .map(|n| format!("_{}_\n", n))
                .unwrap_or_default(),
            stats.net_pnl,
            win_rate,
            stats.winning_trades,
            stats.total_trades,
            stats.pending_trades,
            stats.total_trades + stats.pending_trades
        );

        if !stats.asset_breakdown.is_empty() {
            report.push_str("*Por activo*\n");
            for asset in &stats.asset_breakdown {
                let wr = if asset.total_trades > 0 {
                    (asset.winning_trades as f64 / asset.total_trades as f64) * 100.0
                } else {
                    0.0
                };
                report.push_str(&format!(
                    "- {}: *${:.2}* | WR {:.0}% ({}/{})\n",
                    asset.coin, asset.net_pnl, wr, asset.winning_trades, asset.total_trades
                ));
            }
            report.push('\n');
        }

        report.push_str(&format!(
            "*Rendimiento*\n\
- Mejor trade: +{:.1}%\n\
- Peor trade: {:.1}%\n\n\
*Por estrategia*\n\
- Smart Delay: {}\n\
- Kill Zone: {}\n\
- Accion Rapida: {}\n\
- Full Recovery: {}\n\n\
*Gestion de riesgo*\n\
- SL 0.650: {}\n\
- SL 0.83: {}\n\
- DCA ejecutados: {}\n\n\
*Auditoria*\n{}",
            stats.best_trade_pct,
            stats.worst_trade_pct,
            stats.smart_delay_trades,
            stats.kill_zone_trades,
            stats.rapid_action_trades,
            stats.full_recovery_trades,
            stats.sl_650_triggers,
            stats.sl_83_triggers,
            stats.dca_executions,
            if audit.is_fully_verified {
                format!(
                    "- OK: {}/{} trades verificados",
                    audit.verified_trades, audit.total_trades
                )
            } else {
                let mut status = format!("- Verificados: {}", audit.verified_trades);
                if audit.pending_verifications > 0 {
                    status.push_str(&format!("\n- Pendientes: {}", audit.pending_verifications));
                }
                if audit.failed_verifications > 0 {
                    status.push_str(&format!(
                        "\n- Discrepancias: {}",
                        audit.failed_verifications
                    ));
                }
                status
            }
        ));

        report
    }

    pub fn format_daily_report(&self, stats: &DailyStats, audit: &AuditReport) -> String {
        let audit_status = if audit.is_fully_verified {
            "Auditado"
        } else {
            "Parcial"
        };

        let notice_str = stats
            .notice
            .as_deref()
            .map(|n| format!("_{}_\n\n", n))
            .unwrap_or_default();

        let mut report = format!(
            "*REPORTE DIARIO ({})*\n\
Estado: {}\n\
{}\n\
*Resumen financiero*\n\
- P&L neto: *${:.2}*\n\
- Tasa de acierto: *{:.1}%*\n\
- Operaciones pendientes: {}\n\
- Total operaciones: {}\n\n",
            stats.date,
            audit_status,
            notice_str,
            stats.net_pnl,
            stats.win_rate,
            stats.pending_trades,
            stats.total_trades + stats.pending_trades
        );

        if !stats.asset_breakdown.is_empty() {
            report.push_str("*Por activo*\n");
            for asset in &stats.asset_breakdown {
                let wr = if asset.total_trades > 0 {
                    (asset.winning_trades as f64 / asset.total_trades as f64) * 100.0
                } else {
                    0.0
                };
                report.push_str(&format!(
                    "- {}: *${:.2}* | WR {:.0}% ({}/{})\n",
                    asset.coin, asset.net_pnl, wr, asset.winning_trades, asset.total_trades
                ));
            }
            report.push('\n');
        }

        report.push_str(&format!(
            "*Rendimiento*\n\
- Mejor trade: +{:.1}%\n\
- Peor trade: {:.1}%\n\n\
*Eficacia por estrategia (WR)*\n\
- Smart Delay: {:.1}%\n\
- Kill Zone: {:.1}%\n\
- Accion Rapida: {:.1}%\n\
- Full Recovery: {:.1}%\n\n",
            stats.best_trade_pct,
            stats.worst_trade_pct,
            stats.smart_delay_win_rate,
            stats.kill_zone_win_rate,
            stats.rapid_action_win_rate,
            stats.full_recovery_win_rate
        ));

        report.push_str("*Desglose por sesion*\n");
        for (i, period) in stats.periods.iter().enumerate() {
            if period.total_trades > 0 || period.pending_trades > 0 {
                let period_wr = if period.total_trades > 0 {
                    (period.winning_trades as f64 / period.total_trades as f64) * 100.0
                } else {
                    0.0
                };

                report.push_str(&format!(
                    "- Sesion {} ({}): {} cerrados | {} pendientes | WR {:.0}% | ${:.2}\n",
                    i + 1,
                    period.period_end,
                    period.total_trades,
                    period.pending_trades,
                    period_wr,
                    period.net_pnl
                ));
            }
        }

        let audit_summary = {
            let mut status = format!("- Verificados: {}", audit.verified_trades);
            if audit.pending_verifications > 0 {
                status.push_str(&format!("\n- Pendientes: {}", audit.pending_verifications));
            }
            if audit.failed_verifications > 0 {
                status.push_str(&format!(
                    "\n- Fallidos: {} (discrepancia {:.2}%)",
                    audit.failed_verifications, audit.total_discrepancy
                ));
            }
            status
        };

        report.push_str(&format!(
            "\n*Auditoria*\n{}\n_{}_",
            audit_summary,
            if audit.failed_verifications > 0 {
                "Diferencias detectadas"
            } else {
                "Cierre correcto"
            }
        ));

        report
    }
}
