use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Serialize)]
struct ClobRequest {
    cmd: String,
    token_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    usdc_size: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_qty: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    order_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ClobResponse {
    pub status: String,
    pub message: Option<String>,
    pub order_id: Option<String>,
    pub actual_balance: Option<f64>,
    pub shares_ordered: Option<f64>,
    pub shares_sold: Option<f64>,
}

pub struct PolymarketClobClient {
    pub http_client: Client, // For Gamma API calls (prices, etc)
}

impl PolymarketClobClient {
    pub fn new() -> Self {
        Self {
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
        }
    }

    async fn send_to_daemon(req: ClobRequest) -> Result<ClobResponse, Box<dyn Error>> {
        let mut stream = TcpStream::connect("127.0.0.1:50051").await?;
        let payload = serde_json::to_string(&req)?;
        stream.write_all(payload.as_bytes()).await?;

        let mut buf = vec![0; 4096];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Err("Daemon closed connection".into());
        }

        let resp_str = String::from_utf8_lossy(&buf[..n]);
        let resp: ClobResponse = serde_json::from_str(&resp_str)?;

        if resp.status == "error" {
            return Err(resp.message.unwrap_or_else(|| "Unknown error".into()).into());
        }

        Ok(resp)
    }

    pub async fn buy(&self, token_id: &str, usdc_size: f64, limit_price: f64) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "buy".into(),
            token_id: token_id.into(),
            usdc_size: Some(usdc_size),
            token_qty: None,
            limit_price: Some(limit_price),
            order_type: None,
            order_id: None,
        }).await
    }

    pub async fn sell_gtc(&self, token_id: &str, token_qty: f64, limit_price: f64) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "sell".into(),
            token_id: token_id.into(),
            usdc_size: None,
            token_qty: Some(token_qty),
            limit_price: Some(limit_price),
            order_type: Some("GTC".into()),
            order_id: None,
        }).await
    }

    pub async fn sell_fak(&self, token_id: &str, token_qty: f64, limit_price: f64) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "sell".into(),
            token_id: token_id.into(),
            usdc_size: None,
            token_qty: Some(token_qty),
            limit_price: Some(limit_price),
            order_type: Some("FAK".into()),
            order_id: None,
        }).await
    }

    pub async fn get_balance(&self, token_id: &str) -> Result<f64, Box<dyn Error>> {
        let resp = Self::send_to_daemon(ClobRequest {
            cmd: "reconcile_balance".into(),
            token_id: token_id.into(),
            usdc_size: None,
            token_qty: None,
            limit_price: None,
            order_type: None,
            order_id: None,
        }).await?;
        
        Ok(resp.actual_balance.unwrap_or(0.0))
    }
}
