use arbitrage_hammer::clob_client::PolymarketClobClient;
use arbitrage_hammer::kalshi_client::KalshiClient;
use chrono::Local;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();
    let http = reqwest::Client::new();
    let poly = PolymarketClobClient::new();
    let mut kalshi = KalshiClient::build_prod(
        std::env::var("KALSHI_EMAIL").unwrap_or_default(),
        std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
    );
    let _ = kalshi.login().await;

    let tag_id = std::env::var("TAG_ID").unwrap_or("102467".to_string());
    let poly_markets = poly.get_markets_proxy(&tag_id).await.unwrap_or_default();

    println!("--- ACTUAL MARKET PRICES ---");
    for pm in poly_markets.iter().take(5) {
        let (pm_yes, pm_no) = match pm.clob_token_ids.as_deref() {
            Some(ids) => {
                let v: Vec<String> = serde_json::from_str(ids).unwrap_or_default();
                if v.len() >= 2 {
                    (v[0].clone(), v[1].clone())
                } else {
                    continue;
                }
            }
            None => continue,
        };

        let yes_ask = arbitrage_hammer::api::get_best_ask(&http, &pm.id, &pm_yes)
            .await
            .unwrap_or(0.0);
        let no_ask = arbitrage_hammer::api::get_best_ask(&http, &pm.id, &pm_no)
            .await
            .unwrap_or(0.0);

        println!(
            "PM: {} | YES Ask: {:.3} | NO Ask: {:.3}",
            pm.question, yes_ask, no_ask
        );
    }
}
