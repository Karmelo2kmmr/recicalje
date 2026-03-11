use futures_util::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use log::{info, error, warn};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BinancePrice {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub price: String,
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
        let url = format!("wss://stream.binance.us:9443/ws/{}@ticker", symbol.to_lowercase());
        
        loop {
            match connect_async(&url).await {
                Ok((mut ws_stream, _)) => {
                    info!("Connected to Binance WebSocket for {}", symbol);
                    while let Some(msg) = ws_stream.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                match serde_json::from_str::<BinancePrice>(&text) {
                                    Ok(ticker) => {
                                        if let Ok(price) = ticker.price.parse::<f64>() {
                                            let _ = self.tx.send(price);
                                        }
                                    }
                                    Err(_) => {
                                        // info!("WS message not a ticker: {}", text);
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => {
                                warn!("Binance WS closed, reconnecting...");
                                break;
                            }
                            Err(e) => {
                                error!("Binance WS error: {}, reconnecting...", e);
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to connect to Binance WS: {}. Retrying in 5s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }
        }
    }
}
