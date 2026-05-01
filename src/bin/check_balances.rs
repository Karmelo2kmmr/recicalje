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

    match kalshi.get_balance_dollars().await {
        Ok((cash_balance, portfolio_value)) => {
            println!("kalshi_cash_balance_usd={:.2}", cash_balance);
            println!("kalshi_portfolio_value_usd={:.2}", portfolio_value);
        }
        Err(err) => {
            println!("kalshi_error={}", err);
        }
    }

    match kalshi.get_portfolio_positions().await {
        Ok(positions) => {
            println!("kalshi_positions_count={}", positions.len());
            for pos in positions.iter().take(20) {
                let contracts = if pos.position_fp.trim().is_empty() {
                    pos.position.to_string()
                } else {
                    pos.position_fp.clone()
                };
                println!(
                    "kalshi_position={} market={} contracts={}",
                    pos.ticker, pos.market_ticker, contracts
                );
            }
        }
        Err(err) => {
            println!("kalshi_positions_error={}", err);
        }
    }
}
