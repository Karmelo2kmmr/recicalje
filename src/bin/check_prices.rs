use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct BinancePrice {
    symbol: String,
    price: String,
}

#[tokio::main]
async fn main() {
    let client = Client::new();
    let symbols = vec!["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"];
    println!("--- PRECIOS REALES BINANCE ---");
    for symbol in symbols {
        let url = format!(
            "https://api.binance.us/api/v3/ticker/price?symbol={}",
            symbol
        );
        if let Ok(resp) = client.get(&url).send().await {
            if let Ok(p) = resp.json::<BinancePrice>().await {
                println!("{}: {}", p.symbol, p.price);
            }
        }
    }
}
