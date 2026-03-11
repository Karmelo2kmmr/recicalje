use reqwest::Client;
use log::{info, error};

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
                    let err_body = resp.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                    error!("Telegram send failed: {}", err_body);
                } else {
                    info!("Telegram message sent.");
                }
            }
            Err(e) => error!("Telegram error: {}", e),
        }
    }

    pub async fn notify_entry(&self, asset: &str, side: &str, price: f64, amount: f64) {
        let text = format!(
            "🚀 *ENTRADA DETECTADA*\n\
            • Activo: *{}*\n\
            • Dirección: *{}*\n\
            • Precio: *{:.2}*\n\
            • Monto: *${:.2}*",
            asset, side, price, amount
        );
        self.send_message(&text).await;
    }

    pub async fn notify_dca(&self, asset: &str, price: f64, amount: f64) {
        let text = format!(
            "➕ *EJECUTANDO DCA*\n\
            • Activo: *{}*\n\
            • Precio DCA: *{:.2}*\n\
            • Monto Adicional: *${:.2}*",
            asset, price, amount
        );
        self.send_message(&text).await;
    }

    pub async fn notify_exit(&self, asset: &str, reason: &str, price: f64, pnl: f64) {
        let emoji = if pnl >= 0.0 { "💰" } else { "📉" };
        let text = format!(
            "{} *POSICIÓN CERRADA ({})*\n\
            • Activo: *{}*\n\
            • Precio Cierre: *{:.2}*\n\
            • P&L: *{}${:.2}*",
            emoji, reason, asset, price, if pnl >= 0.0 { "+" } else { "" }, pnl
        );
        self.send_message(&text).await;
    }

    pub async fn notify_market_closed(&self) {
        let text = "🤝 *Mercado Cerrado*\n———————————————————————————-";
        self.send_message(text).await;
    }

    pub async fn notify_session_report(&self, report_text: &str) {
        self.send_message(report_text).await;
    }
}
