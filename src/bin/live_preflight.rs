use arbitrage_hammer::clob_client::PolymarketClobClient;
use arbitrage_hammer::config::{validate_startup, StartupConfig};
use arbitrage_hammer::kalshi_client::KalshiClient;
use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("live_preflight=starting");
    dotenv::dotenv().ok();
    println!("DEBUG: env POSITION_SIZE={:?}", std::env::var("POSITION_SIZE"));
    println!("DEBUG: env DEFAULT_SIZE={:?}", std::env::var("DEFAULT_SIZE"));

    let startup = validate_startup().map_err(|e| format!("startup_validation_error={}", e))?;
    
    if !startup.live_mode {
        println!("Error: PREFLIGHT_ONLY_FOR_LIVE_MODE");
        std::process::exit(1);
    }
    println!("startup_validation=ok");
    println!("position_size_usdc={:.2}", startup.position_size);
    println!("max_open_positions={}", startup.max_open_positions);
    println!("max_total_exposure_usdc={:.2}", startup.max_total_exposure_usdc);
    println!("max_venue_exposure_usdc={:.2}", startup.max_venue_exposure_usdc);

    // 1. Check CLOB Daemon
    let poly = PolymarketClobClient::new();
    match poly.ping().await {
        Ok(_) => println!("polymarket_daemon_ping=ok"),
        Err(e) => {
            println!("Error: \"polymarket_daemon_error={}\"", e);
            std::process::exit(1);
        }
    }

    // 2. Check Polymarket Collateral
    let collateral = poly
        .get_collateral_balance()
        .await
        .map_err(|err| format!("polymarket_collateral_error={}", err))?;
    println!("polymarket_collateral_usdc={:.2}", collateral);
    
    if collateral < startup.position_size {
        return Err(format!(
            "polymarket_collateral_error=collateral ${:.2} is below POSITION_SIZE ${:.2}",
            collateral, startup.position_size
        )
        .into());
    }

    let tag_id = std::env::var("TAG_ID").unwrap_or_else(|_| "102467".to_string());
    let markets = poly
        .get_markets_proxy(&tag_id)
        .await
        .map_err(|err| format!("polymarket_markets_error={}", err))?;
    println!("polymarket_markets_count={}", markets.len());

    // 3. Check Kalshi
    let kalshi = KalshiClient::init_prod().await.map_err(|e| format!("kalshi_login_error={}", e))?;
    println!("kalshi_login=ok");

    let (cash, portfolio_val) = kalshi.get_balance_dollars().await.map_err(|e| format!("kalshi_balance_error={}", e))?;
    println!("kalshi_cash_balance_usd={:.2}", cash);
    println!("kalshi_portfolio_value_usd={:.2}", portfolio_val);

    let portfolio = kalshi.get_portfolio_positions().await.unwrap_or_default();
    println!("kalshi_positions_count={}", portfolio.len());

    println!("live_preflight=ok_no_orders_placed");
    Ok(())
}
