use log::{debug, info};
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
    #[serde(skip_serializing_if = "Option::is_none")]
    tag_id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ClobResponse {
    pub status: String,
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag_id: Option<String>,
    pub actual_balance: Option<f64>,
    pub filled_size: Option<f64>,
    pub shares_ordered: Option<f64>,
    pub shares_sold: Option<f64>,
    pub collateral_balance: Option<f64>,
    pub collateral_min_allowance: Option<f64>,
    pub order: Option<serde_json::Value>,
    pub data: Option<serde_json::Value>,
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
        // 15-second connect timeout: if the daemon is dead or hung, fail fast
        // so emergency sell retries can proceed instead of waiting forever.
        let stream_result = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            TcpStream::connect("127.0.0.1:50051"),
        )
        .await;

        let mut stream = match stream_result {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(format!("CLOB daemon connect failed: {}", e).into()),
            Err(_) => return Err("CLOB daemon connect timed out after 15s".into()),
        };

        let payload = serde_json::to_string(&req)?;
        stream.write_all(payload.as_bytes()).await?;

        // 30-second read timeout: daemon must respond within this window.
        let read_result = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            // 8MB buffer for 1000+ markets
            let mut resp_bytes = Vec::new();
            let mut buf = vec![0; 8192 * 1024];
            let mut total_read = 0;
            loop {
                let n = stream.read(&mut buf).await?;
                if n == 0 {
                    break;
                }
                total_read += n;
                resp_bytes.extend_from_slice(&buf[..n]);
                if total_read % 262144 == 0 || resp_bytes.ends_with(b"\n") {
                    debug!("Proxy Read: {} bytes...", total_read);
                }
                if resp_bytes.ends_with(b"\n") {
                    break;
                }
            }
            info!("Proxy Read COMPLETE: {} bytes total.", total_read);
            Ok::<Vec<u8>, Box<dyn Error>>(resp_bytes)
        })
        .await;

        let resp_bytes = match read_result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(e)) => return Err(format!("CLOB daemon read failed: {}", e).into()),
            Err(_) => return Err("CLOB daemon read timed out after 30s".into()),
        };

        let resp: ClobResponse = serde_json::from_slice(&resp_bytes)?;

        if resp.status == "error" {
            return Err(resp
                .message
                .unwrap_or_else(|| "Unknown error".into())
                .into());
        }

        Ok(resp)
    }

    pub async fn get_markets_proxy(
        &self,
        tag_id: &str,
    ) -> Result<Vec<crate::api::Market>, Box<dyn Error>> {
        let resp = Self::send_to_daemon(ClobRequest {
            cmd: "get_markets".into(),
            token_id: "".into(),
            usdc_size: None,
            token_qty: None,
            limit_price: None,
            order_type: None,
            order_id: None,
            tag_id: Some(tag_id.into()),
        })
        .await?;

        if let Some(data) = resp.data {
            let markets: Vec<crate::api::Market> = serde_json::from_value(data)?;
            Ok(markets)
        } else {
            Err("No market data in response".into())
        }
    }

    pub async fn ping(&self) -> Result<(), Box<dyn Error>> {
        let _ = Self::send_to_daemon(ClobRequest {
            cmd: "ping".into(),
            token_id: "".into(),
            usdc_size: None,
            token_qty: None,
            limit_price: None,
            order_type: None,
            order_id: None,
            tag_id: None,
        })
        .await?;

        Ok(())
    }

    pub async fn buy(
        &self,
        token_id: &str,
        usdc_size: f64,
        limit_price: f64,
    ) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "buy".into(),
            token_id: token_id.into(),
            usdc_size: Some(usdc_size),
            token_qty: None,
            limit_price: Some(limit_price),
            order_type: None,
            order_id: None,
            tag_id: None,
        })
        .await
    }

    pub async fn sell_gtc(
        &self,
        token_id: &str,
        token_qty: f64,
        limit_price: f64,
    ) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "sell".into(),
            token_id: token_id.into(),
            usdc_size: None,
            token_qty: Some(token_qty),
            limit_price: Some(limit_price),
            order_type: Some("GTC".into()),
            order_id: None,
            tag_id: None,
        })
        .await
    }

    pub async fn sell_fak(
        &self,
        token_id: &str,
        token_qty: f64,
        limit_price: f64,
    ) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "sell".into(),
            token_id: token_id.into(),
            usdc_size: None,
            token_qty: Some(token_qty),
            limit_price: Some(limit_price),
            order_type: Some("FAK".into()),
            order_id: None,
            tag_id: None,
        })
        .await
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
            tag_id: None,
        })
        .await?;

        Ok(resp.actual_balance.unwrap_or(0.0))
    }

    pub async fn get_collateral_balance(&self) -> Result<f64, Box<dyn Error>> {
        let resp = Self::send_to_daemon(ClobRequest {
            cmd: "collateral_status".into(),
            token_id: "".into(),
            usdc_size: None,
            token_qty: None,
            limit_price: None,
            order_type: None,
            order_id: None,
            tag_id: None,
        })
        .await?;

        Ok(resp.collateral_balance.unwrap_or(0.0))
    }

    pub async fn get_order_status(&self, order_id: &str) -> Result<ClobResponse, Box<dyn Error>> {
        Self::send_to_daemon(ClobRequest {
            cmd: "get_order_status".into(),
            token_id: "".into(),
            usdc_size: None,
            token_qty: None,
            limit_price: None,
            order_type: None,
            order_id: Some(order_id.into()),
            tag_id: None,
        })
        .await
    }
}
