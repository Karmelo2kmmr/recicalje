use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::{self, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ChainlinkPricePayload {
    pub symbol: String,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RTDSMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub timestamp: i64,
    pub payload: Option<ChainlinkPricePayload>,
}

pub struct BinanceWS {
    pub tx: broadcast::Sender<f64>,
}

impl BinanceWS {
    pub fn new() -> (Self, broadcast::Receiver<f64>) {
        let (tx, rx) = broadcast::channel(100);
        (Self { tx }, rx)
    }

    pub async fn run(&self, symbol: &str) {
        let symbol_lower = symbol.to_lowercase();
        let base_symbol = symbol_lower.strip_suffix("usdt").unwrap_or(&symbol_lower);
        let chainlink_symbol = format!("{}/usd", base_symbol);
        let url = "wss://ws-live-data.polymarket.com";
        let subscription = json!({
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": format!("{{\"symbol\":\"{}\"}}", chainlink_symbol)
                }
            ]
        })
        .to_string();

        loop {
            match connect_async(url).await {
                Ok((mut ws_stream, _)) => {
                    info!(
                        "Connected to Polymarket RTDS Chainlink feed for {}",
                        chainlink_symbol
                    );

                    if let Err(e) = ws_stream.send(Message::Text(subscription.clone())).await {
                        error!(
                            "Failed to subscribe to Polymarket RTDS: {}. Reconnecting...",
                            e
                        );
                        time::sleep(Duration::from_secs(5)).await;
                        continue;
                    }

                    let mut ping_interval = time::interval(Duration::from_secs(5));

                    loop {
                        tokio::select! {
                            _ = ping_interval.tick() => {
                                if let Err(e) = ws_stream.send(Message::Text("PING".to_string())).await {
                                    error!("Polymarket RTDS ping failed: {}. Reconnecting...", e);
                                    break;
                                }
                            }
                            msg = ws_stream.next() => {
                                match msg {
                                    Some(Ok(Message::Text(text))) => {
                                        match serde_json::from_str::<RTDSMessage>(&text) {
                                            Ok(message) => {
                                                if message.topic == "crypto_prices_chainlink"
                                                    && message.message_type == "update"
                                                {
                                                    if let Some(payload) = message.payload {
                                                        if payload.symbol.eq_ignore_ascii_case(&chainlink_symbol) {
                                                            let _ = self.tx.send(payload.value);
                                                        }
                                                    }
                                                }
                                            }
                                            Err(_) => {
                                                // Ignore non-price messages such as subscription acks.
                                            }
                                        }
                                    }
                                    Some(Ok(Message::Ping(payload))) => {
                                        if let Err(e) = ws_stream.send(Message::Pong(payload)).await {
                                            error!("Polymarket RTDS pong failed: {}. Reconnecting...", e);
                                            break;
                                        }
                                    }
                                    Some(Ok(Message::Close(_))) => {
                                        warn!("Polymarket RTDS closed, reconnecting...");
                                        break;
                                    }
                                    Some(Err(e)) => {
                                        error!("Polymarket RTDS error: {}, reconnecting...", e);
                                        break;
                                    }
                                    None => {
                                        warn!("Polymarket RTDS stream ended, reconnecting...");
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to connect to Polymarket RTDS: {}. Retrying in 5s...",
                        e
                    );
                    time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }
}
