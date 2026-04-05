use log::{error, info};
use reqwest::Client;

#[derive(Clone)]
pub struct TelegramReporter {
    token: String,
    chat_id: String,
    client: Client,
}

impl TelegramReporter {
    pub fn new() -> Option<Self> {
        let token = std::env::var("TELEGRAM_BOT_TOKEN").ok()?;
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").ok()?;

        if token.is_empty() || chat_id.is_empty() {
            return None;
        }

        Some(Self {
            token,
            chat_id,
            client: Client::new(),
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
                    error!("Telegram send failed: {}", err_body);
                } else {
                    info!("Telegram message sent.");
                }
            }
            Err(e) => error!("Telegram error: {}", e),
        }
    }

    pub async fn notify_entry(
        &self,
        asset: &str,
        side: &str,
        entry_price: f64,
        stake_usd: f64,
        price_to_beat: f64,
        delta_to_beat: f64,
    ) {
        let text = format!(
            "🚀 *ENTRADA DETECTADA*\n\
            • Activo: *{}*\n\
            • Dirección: *{}*\n\
            • Price to beat: *{:.2}*\n\
            • BTC actual: *{:+.2} USD*\n\
            • Precio entrada: *{:.3}*\n\
            • Monto: *${:.2}*",
            asset, side, price_to_beat, delta_to_beat, entry_price, stake_usd
        );
        self.send_message(&text).await;
    }

    pub async fn notify_exit(
        &self,
        asset: &str,
        reason: &str,
        entry_price: f64,
        exit_price: f64,
        pnl: f64,
        ret_pct: f64,
        stake_usd: f64,
    ) {
        let (headline, result_label) = if pnl >= 0.0 {
            ("✅ *OPERACIÓN GANADA*", "GANADA")
        } else {
            ("❌ *OPERACIÓN PERDIDA*", "PERDIDA")
        };

        let text = format!(
            "{}\n\
            • Activo: *{}*\n\
            • Resultado: *{}*\n\
            • Motivo de cierre: *{}*\n\
            • Entrada: *{:.3}*\n\
            • Salida: *{:.3}*\n\
            • Monto operado: *${:.2}*\n\
            • P&L: *{}${:.2}*\n\
            • Retorno: *{:+.2}%*",
            headline,
            asset,
            result_label,
            reason,
            entry_price,
            exit_price,
            stake_usd,
            if pnl >= 0.0 { "+" } else { "" },
            pnl,
            ret_pct
        );
        self.send_message(&text).await;
    }

    pub async fn notify_market_closed(&self) {
        self.send_message("🤝 *Mercado Cerrado*").await;
    }

    pub async fn notify_session_report(&self, report_text: &str) {
        self.send_message(report_text).await;
    }
}
