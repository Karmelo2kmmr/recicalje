use arbitrage_hammer::kalshi_client::KalshiClient;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    let mut kalshi = KalshiClient::build_prod(
        std::env::var("KALSHI_EMAIL").unwrap_or_default(),
        std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
    );

    if let Err(err) = kalshi.login().await {
        eprintln!("kalshi_login_error={}", err);
    }

    let limit = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(50);

    match kalshi.fetch_recent_fills(limit).await {
        Ok(fills) => {
            println!("kalshi_fills_count={}", fills.len());
            for fill in fills {
                println!(
                    "fill time={} ticker={} side={} action={} count={} yes_price={} no_price={} fee={} order_id={}",
                    fill.created_time,
                    fill.ticker,
                    fill.side,
                    fill.action,
                    fill.count_fp,
                    fill.yes_price_dollars.as_deref().unwrap_or(""),
                    fill.no_price_dollars.as_deref().unwrap_or(""),
                    fill.fee_cost.as_deref().unwrap_or(""),
                    fill.order_id
                );
            }
        }
        Err(err) => {
            println!("kalshi_fills_error={}", err);
        }
    }

    for ticker in std::env::args().skip(2) {
        let series = ticker
            .split('-')
            .next()
            .unwrap_or(ticker.as_str())
            .to_string();

        match kalshi.fetch_markets(Some(&series)).await {
            Ok(markets) => {
                for market in markets {
                    if market.ticker == ticker {
                        println!(
                            "market ticker={} status={:?} result={:?} title={:?} floor_strike={:?} close_time={:?}",
                            market.ticker,
                            market.status,
                            market.result,
                            market.title,
                            market.floor_strike,
                            market.close_time
                        );
                    }
                }
            }
            Err(err) => {
                println!("market_error ticker={} error={}", ticker, err);
            }
        }
    }
}
