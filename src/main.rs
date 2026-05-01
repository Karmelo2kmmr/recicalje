use arbitrage_hammer::api;
use arbitrage_hammer::clob_client::PolymarketClobClient;
use arbitrage_hammer::config;
use arbitrage_hammer::dual_market::{
    DualCapitalManager, DualMarketPair, OpenPosition, Platform, PositionState, Venue,
};
use arbitrage_hammer::kalshi_client::KalshiClient;
use arbitrage_hammer::telegram::TelegramBot;
use chrono::{DateTime, Local, Timelike};
use log::{error, info, warn};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use tokio::time::{sleep, Duration};

struct ExecutionFill {
    shares: f64,
    fill_price: f64,
    notional_usdc: f64,
    order_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_position() -> OpenPosition {
        OpenPosition {
            twin_key: "KXETH15M-TEST".to_string(),
            venue: Venue::Kalshi,
            coin: "ETH".to_string(),
            pm_market_id: "pm".to_string(),
            pm_yes_token: "yes".to_string(),
            pm_no_token: "no".to_string(),
            kalshi_ticker: "KXETH15M-TEST".to_string(),
            buy_yes: true,
            entry_price: 0.84,
            shares: 3.0,
            notional_usdc: 2.52,
            entry_order_id: Some("entry".to_string()),
            last_exit_order_id: None,
            last_error: None,
            opened_at: None,
            updated_at: None,
            dca_executed: false,
            is_hedge: false,
            hedge_sl_price: None,
            hedge_tp_price: None,
            binance_entry_price: 0.0,
            binance_retrace_threshold: 0.0,
            state: PositionState::Open,
            hedge_pair_id: None,
        }
    }

    #[test]
    fn selects_polymarket_when_it_is_cheapest_and_funded() {
        let selection = select_entry_venue(0.81, 0.90, true).unwrap();

        assert_eq!(selection.platform, Platform::Polymarket);
        assert_eq!(selection.ask, 0.81);
    }

    #[test]
    fn blocks_when_polymarket_is_cheapest_but_unfunded() {
        let selection = select_entry_venue(0.81, 0.90, false);

        assert_eq!(selection, None);
    }

    #[test]
    fn selects_kalshi_only_when_kalshi_is_cheapest() {
        let selection = select_entry_venue(0.91, 0.86, true).unwrap();

        assert_eq!(selection.platform, Platform::Kalshi);
        assert_eq!(selection.ask, 0.86);
    }

    #[test]
    fn ioc_zero_fill_does_not_close_position() {
        let mut pos = test_position();
        let fill = CloseFill {
            fill_price: 0.002,
            shares_sold: 0.0,
            remaining_shares: Some(3.0),
            order_id: Some("exit".to_string()),
        };

        let result = reconcile_close_fill(&mut pos, &fill, "TAKE-PROFIT");

        assert!(result.is_err());
        assert_eq!(pos.state, PositionState::ExitFailedZeroFill);
        assert_eq!(pos.shares, 3.0);
        assert_eq!(pos.notional_usdc, 2.52);
    }

    #[test]
    fn recovery_zero_fill_does_not_calculate_closed_pnl() {
        let mut pos = test_position();
        pos.state = PositionState::RecoveryPending;
        let fill = CloseFill {
            fill_price: 0.002,
            shares_sold: 0.0,
            remaining_shares: Some(3.0),
            order_id: Some("recovery".to_string()),
        };

        let result = reconcile_close_fill(&mut pos, &fill, "STOP-FAILED");

        assert!(result.is_err());
        assert_eq!(pos.state, PositionState::ExitFailedZeroFill);
        assert!(pos.last_error.unwrap().contains("zero confirmed fills"));
    }

    #[test]
    fn partial_close_updates_remaining_contracts() {
        let mut pos = test_position();
        let fill = CloseFill {
            fill_price: 0.50,
            shares_sold: 1.0,
            remaining_shares: Some(2.0),
            order_id: Some("partial".to_string()),
        };

        let fully_closed = reconcile_close_fill(&mut pos, &fill, "HARD-SL").unwrap();

        assert!(!fully_closed);
        assert_eq!(pos.state, PositionState::PartiallyClosed);
        assert!((pos.shares - 2.0).abs() < 0.0001);
        assert!((pos.notional_usdc - 1.68).abs() < 0.0001);
    }

    #[test]
    fn confirmed_close_marks_closed_confirmed() {
        let mut pos = test_position();
        let fill = CloseFill {
            fill_price: 0.97,
            shares_sold: 3.0,
            remaining_shares: Some(0.0),
            order_id: Some("closed".to_string()),
        };

        let fully_closed = reconcile_close_fill(&mut pos, &fill, "TAKE-PROFIT").unwrap();

        assert!(fully_closed);
        assert_eq!(pos.state, PositionState::ClosedConfirmed);
        assert_eq!(pos.last_error, None);
    }

    #[test]
    fn stale_position_detection_does_not_remove_position() {
        let mut positions = vec![test_position()];
        let stale = stale_positions_for_alert(&mut positions, &[]);

        assert_eq!(stale.len(), 1);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].state, PositionState::Open);
    }

    #[test]
    fn stale_position_detection_ignores_terminal_zero_positions() {
        let mut pos = test_position();
        pos.state = PositionState::ResolvedConfirmed;
        pos.shares = 0.0;
        pos.notional_usdc = 0.0;
        let mut positions = vec![pos];

        let stale = stale_positions_for_alert(&mut positions, &[]);

        assert!(stale.is_empty());
        assert_eq!(positions.len(), 1);
    }

    #[test]
    fn safe_mode_blocks_new_entries() {
        assert!(entries_allowed_by_safe_mode(false));
        assert!(!entries_allowed_by_safe_mode(true));
    }
}

struct CloseFill {
    fill_price: f64,
    shares_sold: f64,
    remaining_shares: Option<f64>,
    order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct EntryVenueSelection {
    venue: &'static str,
    ask: f64,
    other_venue: &'static str,
    other_ask: f64,
    platform: Platform,
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|s| s.to_lowercase() == "true")
        .unwrap_or(default)
}

fn env_i32(key: &str, default: i32) -> i32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn reconcile_close_fill(
    pos: &mut OpenPosition,
    fill: &CloseFill,
    reason: &str,
) -> Result<bool, String> {
    let close_epsilon = position_close_epsilon(pos);

    if fill.shares_sold <= 0.001 {
        pos.state = PositionState::ExitFailedZeroFill;
        pos.last_error = Some(format!(
            "{} close had zero confirmed fills; position remains open",
            reason
        ));
        pos.updated_at = Some(now_rfc3339());
        return Err(pos.last_error.clone().unwrap_or_default());
    }

    let previous_shares = pos.shares.max(0.0);
    let remaining = fill
        .remaining_shares
        .unwrap_or_else(|| (previous_shares - fill.shares_sold).max(0.0));

    if remaining > close_epsilon || fill.shares_sold + close_epsilon < previous_shares {
        let remaining = remaining.max(previous_shares - fill.shares_sold).max(0.0);
        let ratio = if previous_shares > 0.0 {
            (remaining / previous_shares).clamp(0.0, 1.0)
        } else {
            0.0
        };
        pos.shares = remaining;
        pos.notional_usdc *= ratio;
        pos.state = PositionState::PartiallyClosed;
        pos.last_error = Some(format!(
            "{} partial close: sold {:.4}, remaining {:.4}",
            reason, fill.shares_sold, remaining
        ));
        pos.updated_at = Some(now_rfc3339());
        return Ok(false);
    }

    pos.state = PositionState::ClosedConfirmed;
    pos.shares = 0.0;
    pos.notional_usdc = 0.0;
    pos.last_error = None;
    pos.updated_at = Some(now_rfc3339());
    Ok(true)
}

fn entries_allowed_by_safe_mode(safe_mode_active: bool) -> bool {
    !safe_mode_active
}

fn is_terminal_zero_position(pos: &OpenPosition) -> bool {
    pos.shares <= position_close_epsilon(pos)
        && matches!(
            pos.state,
            PositionState::Closed
                | PositionState::ClosedConfirmed
                | PositionState::ResolvedConfirmed
        )
}

fn position_close_epsilon(pos: &OpenPosition) -> f64 {
    match pos.venue {
        Venue::Polymarket => env_f64("POLYMARKET_DUST_SHARES", 0.01),
        Venue::Kalshi => 0.001,
    }
}

fn window_elapsed_secs(now: DateTime<Local>, window_start_mins: i32) -> i32 {
    let current_total_secs =
        (now.hour() as i32 * 3600) + (now.minute() as i32 * 60) + now.second() as i32;
    let start_total_secs = window_start_mins * 60;
    (current_total_secs - start_total_secs).rem_euclid(86400)
}

fn save_state(pos: &Vec<OpenPosition>) {
    let path = PathBuf::from("open_positions.json");
    if let Ok(data) = serde_json::to_string_pretty(pos) {
        let _ = fs::write(path, data);
    }
}

fn load_state() -> Vec<OpenPosition> {
    let path = PathBuf::from("open_positions.json");
    if let Ok(data) = fs::read_to_string(path) {
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        Vec::new()
    }
}

fn prune_terminal_positions(open_positions: &mut Vec<OpenPosition>) -> usize {
    let before = open_positions.len();
    open_positions.retain(|pos| !is_terminal_zero_position(pos));
    before - open_positions.len()
}

fn stale_positions_for_alert(
    open_positions: &mut Vec<OpenPosition>,
    active_twins: &[DualMarketPair],
) -> Vec<OpenPosition> {
    let active_keys: HashSet<&str> = active_twins
        .iter()
        .map(|twin| twin.kalshi_ticker.as_str())
        .collect();

    open_positions
        .iter()
        .filter(|pos| !is_terminal_zero_position(pos))
        .filter(|pos| !active_keys.contains(pos.twin_key.as_str()))
        .cloned()
        .collect()
}

fn total_exposure(open_positions: &[OpenPosition]) -> f64 {
    open_positions.iter().map(|pos| pos.notional_usdc).sum()
}

fn venue_exposure(open_positions: &[OpenPosition], venue: &Venue) -> f64 {
    open_positions
        .iter()
        .filter(|pos| &pos.venue == venue)
        .map(|pos| pos.notional_usdc)
        .sum()
}

fn kalshi_position_contracts(pos: &arbitrage_hammer::kalshi_client::KalshiPosition) -> f64 {
    if !pos.position_fp.trim().is_empty() {
        pos.position_fp.trim().parse::<f64>().unwrap_or(0.0).abs()
    } else {
        (pos.position as f64).abs()
    }
}

fn kalshi_position_buy_yes(pos: &arbitrage_hammer::kalshi_client::KalshiPosition) -> bool {
    if !pos.position_fp.trim().is_empty() {
        pos.position_fp.trim().parse::<f64>().unwrap_or(0.0) >= 0.0
    } else {
        pos.position >= 0
    }
}

async fn kalshi_live_contracts(
    client: &KalshiClient,
    ticker: &str,
    buy_yes: bool,
) -> Result<f64, String> {
    let positions = client
        .get_portfolio_positions()
        .await
        .map_err(|e| format!("Kalshi position reconciliation failed: {}", e))?;

    let target = positions.iter().find(|pos| {
        (pos.ticker == ticker || pos.market_ticker == ticker) && kalshi_position_buy_yes(pos) == buy_yes
    });

    Ok(target.map(kalshi_position_contracts).unwrap_or(0.0))
}

fn select_entry_venue(
    pm_ask: f64,
    km_ask: f64,
    poly_has_funds: bool,
) -> Option<EntryVenueSelection> {
    let pm_available = pm_ask > 0.0;
    let km_available = km_ask > 0.0;

    if pm_available && (!km_available || pm_ask <= km_ask) {
        return poly_has_funds.then_some(EntryVenueSelection {
            venue: "Polymarket",
            ask: pm_ask,
            other_venue: "Kalshi",
            other_ask: km_ask,
            platform: Platform::Polymarket,
        });
    }

    if km_available {
        return Some(EntryVenueSelection {
            venue: "Kalshi",
            ask: km_ask,
            other_venue: "Polymarket",
            other_ask: pm_ask,
            platform: Platform::Kalshi,
        });
    }

    None
}

async fn alert_close_failure(
    telegram: Option<&TelegramBot>,
    pos: &mut OpenPosition,
    reason: &str,
    error_msg: String,
) {
    error!(
        "LIVE EXIT FAILED | coin={} ticker={} venue={:?} reason={} error={}",
        pos.coin, pos.kalshi_ticker, pos.venue, reason, error_msg
    );
    pos.state = if error_msg.to_lowercase().contains("zero-fill")
        || error_msg.to_lowercase().contains("zero sold shares")
    {
        PositionState::ExitFailedZeroFill
    } else if error_msg.to_lowercase().contains("unconfirmed")
        || error_msg.to_lowercase().contains("reconciliation")
    {
        PositionState::ManualReviewRequired
    } else {
        PositionState::RecoveryPending
    };
    pos.last_error = Some(format!("{}: {}", reason, error_msg));
    pos.updated_at = Some(now_rfc3339());

    if let Some(bot) = telegram {
        bot.send_message(&format!(
            "*ALERTA: CIERRE NO EJECUTADO — POSICIÓN SIGUE ABIERTA*\nActivo: `{}`\nVenue: `{:?}`\nTicker: `{}`\nMotivo: `{}`\nEstado: `{:?}`\nError: `{}`",
            pos.coin, pos.venue, pos.kalshi_ticker, reason, pos.state, pos.last_error.as_deref().unwrap_or("")
        ))
        .await;
    }
}

fn parse_token_ids_local(json_str: Option<&str>) -> Option<(String, String)> {
    let s = json_str?;
    let ids: Vec<String> = serde_json::from_str(s).ok()?;
    if ids.len() >= 2 {
        Some((ids[0].clone(), ids[1].clone()))
    } else {
        None
    }
}

fn calc_retrace_threshold(coin: &str, distance: f64) -> f64 {
    let base = if coin == "BTC" {
        40.0
    } else if coin == "ETH" {
        1.5
    } else {
        0.15
    };
    (distance * 0.4).max(base)
}

fn parse_polymarket_matched_size(order: &serde_json::Value) -> f64 {
    for key in ["size_matched", "size_filled", "filled_size", "matched_size"] {
        if let Some(size) = order.get(key).and_then(|v| v.as_f64()) {
            return size;
        }
        if let Some(size) = order
            .get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<f64>().ok())
        {
            return size;
        }
    }

    order
        .get("order")
        .map(parse_polymarket_matched_size)
        .unwrap_or(0.0)
}

fn is_polymarket_current_et_date(pm: &api::Market, now: DateTime<Local>) -> bool {
    let end_date = match pm.end_date.as_deref() {
        Some(d) => d,
        None => {
            // P1 FIX: Do not assume valid if end_date is missing. 
            // Better to skip than to enter a zombie market.
            warn!("Market {} has no end_date — skipping for safety", pm.id);
            return false;
        }
    };

    match chrono::DateTime::parse_from_rfc3339(end_date) {
        Ok(dt) => {
            let dt_local = dt.with_timezone(&Local);
            // Must be today's market
            if dt_local.date_naive() != now.date_naive() {
                return false;
            }
            // Keep the market-discovery close guard aligned with the configured
            // entry window. With ENTRY_END_SEC=850, a hardcoded 60s guard would
            // silently remove markets after 840s even though entries are still
            // configured as allowed.
            let remaining = dt_local.signed_duration_since(now).num_seconds();
            let configured_entry_end = env_i32("ENTRY_END_SEC", 790).clamp(0, 899);
            let default_min_remaining = (900 - configured_entry_end).max(0) as i64;
            let min_remaining =
                env_i32("MIN_MARKET_SECONDS_TO_CLOSE", default_min_remaining as i32)
                    .max(0) as i64;
            if remaining <= 0 {
                return false;
            }
            if remaining < min_remaining {
                warn!(
                    "Market {} closes in {}s — too close to trade (min_remaining={}s)",
                    pm.id, remaining, min_remaining
                );
                return false;
            }
            true
        }
        Err(_) => false,
    }
}

async fn execute_polymarket_entry(
    _http: &reqwest::Client,
    poly: &PolymarketClobClient,
    token: &str,
    size: f64,
    price: f64,
    paper: bool,
) -> Result<ExecutionFill, String> {
    if price <= 0.0 {
        return Err("Polymarket entry rejected: price must be positive".to_string());
    }

    if paper {
        return Ok(ExecutionFill {
            shares: size / price,
            fill_price: price,
            notional_usdc: size,
            order_id: Some("PAPER_ENTRY".to_string()),
        });
    }

    // Polymarket API rejects amounts with more than 2 decimal places.
    // Round down to protect against overspend and avoid API rejection.
    let size_rounded = (size * 100.0).floor() / 100.0;
    if size_rounded <= 0.0 {
        return Err(format!(
            "Polymarket entry rejected: size {:.6} rounds to zero at 2dp",
            size
        ));
    }

    let resp = poly
        .buy(token, size_rounded, price)
        .await
        .map_err(|e| format!("Polymarket buy failed: {}", e))?;
    let mut shares = resp.filled_size.unwrap_or(0.0);
    if shares <= 0.0 {
        if let Some(order_id) = resp.order_id.as_deref() {
            if !order_id.is_empty() && order_id != "unknown" {
                match poly.get_order_status(order_id).await {
                    Ok(status_resp) => {
                        if let Some(order) = status_resp.order.as_ref() {
                            shares = parse_polymarket_matched_size(order);
                            warn!(
                                "Polymarket entry zero-fill recovered from order status | order_id={} shares={:.6}",
                                order_id, shares
                            );
                        }
                    }
                    Err(e) => warn!(
                        "Polymarket entry zero-fill order status lookup failed | order_id={} error={}",
                        order_id, e
                    ),
                }
            }
        }
    }
    if shares <= 0.0 {
        return Err("Polymarket buy returned zero filled shares".to_string());
    }

    Ok(ExecutionFill {
        shares,
        fill_price: price,
        notional_usdc: shares * price,
        order_id: resp.order_id,
    })
}

async fn execute_kalshi_entry(
    client: &KalshiClient,
    ticker: &str,
    buy_yes: bool,
    size: f64,
    price: f64,
    paper: bool,
) -> Result<ExecutionFill, String> {
    if price <= 0.0 {
        return Err("Kalshi entry rejected: price must be positive".to_string());
    }

    let contracts = (size / price).floor();
    if contracts < 1.0 {
        return Err(format!(
            "Kalshi entry rejected: ${:.2} at {:.4} buys less than 1 contract",
            size, price
        ));
    }

    if paper {
        return Ok(ExecutionFill {
            shares: contracts,
            fill_price: price,
            notional_usdc: contracts * price,
            order_id: Some("PAPER_ENTRY".to_string()),
        });
    }

    let max_slippage = env_f64("MAX_SLIPPAGE", 0.02);
    let max_entry_price = env_f64("MAX_ENTRY_PRICE", 0.90);
    let limit_price = (price + max_slippage).min(max_entry_price).min(0.99);

    let order = if buy_yes {
        client.buy_yes(ticker, contracts, limit_price).await
    } else {
        client.buy_no(ticker, contracts, limit_price).await
    }
    .map_err(|e| format!("Kalshi buy failed: {}", e))?;

    let filled = order.fill_count_fp.parse::<f64>().unwrap_or(0.0);
    if filled <= 0.0 {
        return Err(format!(
            "Kalshi buy returned zero fills: order={} status={}",
            order.order_id, order.status
        ));
    }

    let fill_cost = order
        .taker_fill_cost_dollars
        .as_deref()
        .or(order.maker_fill_cost_dollars.as_deref())
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(filled * limit_price);
    let avg_fill_price = if filled > 0.0 {
        fill_cost / filled
    } else {
        limit_price
    };

    Ok(ExecutionFill {
        shares: filled,
        fill_price: avg_fill_price,
        notional_usdc: fill_cost,
        order_id: Some(order.order_id),
    })
}

async fn close_polymarket_position(
    _http: &reqwest::Client,
    poly: &PolymarketClobClient,
    token: &str,
    shares: f64,
    price: f64,
    paper: bool,
) -> Result<CloseFill, String> {
    if paper {
        return Ok(CloseFill {
            fill_price: price,
            shares_sold: shares,
            remaining_shares: Some(0.0),
            order_id: Some("PAPER_EXIT".to_string()),
        });
    }

    if shares <= 0.0 {
        return Err("Polymarket close rejected: shares must be positive".to_string());
    }

    // Polymarket API requires token quantity rounded to a supported precision.
    // Use 4dp for shares (CLOB token amounts), 2dp for USDC notional.
    let shares_rounded = (shares * 10_000.0).floor() / 10_000.0;
    if shares_rounded <= 0.0 {
        return Err(format!(
            "Polymarket close rejected: shares {:.8} rounds to zero at 4dp",
            shares
        ));
    }

    let resp = poly
        .sell_fak(token, shares_rounded, price)
        .await
        .map_err(|e| format!("Polymarket sell failed: {}", e))?;
    let sold = resp.shares_sold.or(resp.filled_size).unwrap_or(0.0);
    if sold <= 0.0 {
        return Err("Polymarket sell returned zero sold shares".to_string());
    }

    Ok(CloseFill {
        fill_price: price,
        shares_sold: sold,
        remaining_shares: None,
        order_id: resp.order_id,
    })
}

async fn close_kalshi_position(
    client: &KalshiClient,
    ticker: &str,
    buy_yes: bool,
    shares: f64,
    price: f64,
    paper: bool,
) -> Result<CloseFill, String> {
    if paper {
        return Ok(CloseFill {
            fill_price: price,
            shares_sold: shares.floor(),
            remaining_shares: Some(0.0),
            order_id: Some("PAPER_EXIT".to_string()),
        });
    }

    let contracts = shares.floor();
    if contracts < 1.0 {
        return Err("Kalshi close rejected: shares must be at least 1 contract".to_string());
    }

    let before_contracts = kalshi_live_contracts(client, ticker, buy_yes).await?;
    if before_contracts < 0.001 {
        return Err(format!(
            "Kalshi close rejected: no live external position for {} {} before sell",
            ticker,
            if buy_yes { "YES" } else { "NO" }
        ));
    }

    let sell_contracts = contracts.min(before_contracts.floor());
    if sell_contracts < 1.0 {
        return Err(format!(
            "Kalshi close rejected: live position {:.4} has less than 1 sellable contract",
            before_contracts
        ));
    }

    let order = if buy_yes {
        client.sell_yes(ticker, sell_contracts, price).await
    } else {
        client.sell_no(ticker, sell_contracts, price).await
    }
    .map_err(|e| format!("Kalshi sell failed: {}", e))?;

    let filled = order.fill_count_fp.parse::<f64>().unwrap_or(0.0);
    if filled <= 0.0 {
        return Err(format!(
            "Kalshi sell returned zero fills: order={} status={}",
            order.order_id, order.status
        ));
    }
    if order.status.to_lowercase() == "canceled" && filled <= 0.0 {
        return Err(format!(
            "Kalshi IOC canceled zero-fill: order={} status={} filled={}",
            order.order_id, order.status, filled
        ));
    }

    let order_confirmed_fill = match client.fetch_order(&order.order_id).await {
        Ok(order_status) => order_status.fill_count_fp.parse::<f64>().unwrap_or(filled),
        Err(e) => {
            return Err(format!(
                "Kalshi close unconfirmed: order={} fill lookup failed: {}",
                order.order_id, e
            ));
        }
    };
    if order_confirmed_fill <= 0.0 {
        return Err(format!(
            "Kalshi close unconfirmed zero-fill: order={} status={}",
            order.order_id, order.status
        ));
    }

    sleep(Duration::from_millis(800)).await;
    let after_contracts = kalshi_live_contracts(client, ticker, buy_yes).await?;
    let externally_reduced = (before_contracts - after_contracts).max(0.0);
    if externally_reduced <= 0.001 {
        return Err(format!(
            "Kalshi close unconfirmed: order={} filled={} but live position did not decrease (before={:.4}, after={:.4})",
            order.order_id, order_confirmed_fill, before_contracts, after_contracts
        ));
    }

    let confirmed_sold = order_confirmed_fill.min(externally_reduced);
    let avg_fill_price = if buy_yes {
        order.yes_price_dollars.as_deref()
    } else {
        order.no_price_dollars.as_deref()
    }
    .and_then(|v| v.parse::<f64>().ok())
    .unwrap_or(price);

    Ok(CloseFill {
        fill_price: avg_fill_price,
        shares_sold: confirmed_sold,
        remaining_shares: Some(after_contracts),
        order_id: Some(order.order_id),
    })
}

async fn close_kalshi_position_aggressive(
    client: &KalshiClient,
    ticker: &str,
    buy_yes: bool,
    shares: f64,
    reference_price: f64,
    paper: bool,
) -> Result<CloseFill, String> {
    let exit_slippage = env_f64("EXIT_MAX_SLIPPAGE", 0.05);
    let price = (reference_price - exit_slippage).clamp(0.01, 0.99);
    close_kalshi_position(client, ticker, buy_yes, shares, price, paper).await
}

fn format_entry_message(
    coin: &str,
    side: &str,
    ptb: f64,
    current: f64,
    fill: f64,
    size: f64,
    venue: &str,
    ask: f64,
    other_venue: &str,
    other_ask: f64,
) -> String {
    let is_ideal = fill <= 0.45;
    let ideal_tag = if is_ideal {
        "\n🌟 **ESCENARIO IDEAL DETECTADO** 🌟"
    } else {
        ""
    };
    format!(
        "🚀 ENTRADA DETECTADA{}\n🟢 Plataforma: {}\n• Activo: {}-15M\n• Dirección: {}\n• Precio detectado: {:.3}\n• {}: {:.3}\n• {}: {:.3}\n• Price to beat: {:.2}\n• {} actual: {:.2} USD\n• Delta vs PTB: {:+.2} USD\n• Precio entrada: {:.3}\n• Monto: ${:.2}",
        ideal_tag, venue.to_uppercase(), coin, side, ask, venue, ask, other_venue, other_ask, ptb, coin, current, current - ptb, fill, size
    )
}

fn format_close_message(
    coin: &str,
    win: bool,
    reason: &str,
    entry: f64,
    exit: f64,
    size: f64,
    pnl: f64,
    platform: &Platform,
    balance: f64,
) -> String {
    let emoji = if win { "✅" } else { "🛑" };
    let res_text = if win { "GANADA" } else { "PERDIDA" };
    format!(
        "{} OPERACIÓN {}\n🟢 Plataforma: {:?}\n• Activo: {}-15M\n• Resultado: {}\n• Motivo: {}\n• Entrada: {:.3}\n• Salida: {:.3}\n• Monto: ${:.2}\n• P&L: {:+.2}\n• Retorno: {:+.2}%\n\n💰 Balance {:?}: ${:.2}",
        emoji, res_text, platform, coin, res_text, reason, entry, exit, size, pnl, (pnl/size)*100.0, platform, balance
    )
}

async fn build_twin_markets(
    kalshi: &KalshiClient,
    poly: &PolymarketClobClient,
    now: DateTime<Local>,
    last_closed: &mut i32,
    telegram: Option<&TelegramBot>,
) -> Vec<DualMarketPair> {
    let current_min = now.minute() as i32;
    if current_min % 15 == 0 && current_min != *last_closed {
        if let Some(bot) = telegram {
            let _ = bot.send_message("🤝 **Mercado Cerrado** 🤝\nFinalizando ventana anterior y escaneando nueva oportunidad.").await;
        }
        *last_closed = current_min;
    }

    let mut pairs = Vec::new();

    let tag_id = std::env::var("TAG_ID").unwrap_or("102467".to_string());
    let poly_list = match poly.get_markets_proxy(&tag_id).await {
        Ok(m) => {
            info!(
                "Successfully fetched {} markets from Polymarket proxy.",
                m.len()
            );
            m
        }
        Err(_) => return Vec::new(),
    };

    let mut kalshi_map = std::collections::HashMap::new();
    let kalshi_markets = match kalshi.get_active_markets(now).await {
        Ok(m) => m,
        Err(e) => {
            info!("⚠️ Kalshi market fetch failed: {}", e);
            Vec::new()
        }
    };
    for km in kalshi_markets {
        let q_up = km.question.to_uppercase();
        let coin = if q_up.contains("BTC") {
            "BTC"
        } else if q_up.contains("ETH") {
            "ETH"
        } else if q_up.contains("SOL") {
            "SOL"
        } else if q_up.contains("XRP") {
            "XRP"
        } else {
            continue;
        };

        let open_time = km.open_time.as_deref().unwrap_or("");
        if let Some(start) = api::extract_kalshi_window_start(open_time) {
            let key = format!("{}-{}", coin, start);
            kalshi_map.insert(key, km);
        }
    }

    for pm in &poly_list {
        if !is_polymarket_current_et_date(pm, now) {
            continue;
        }

        let q_up = pm.question.to_uppercase();
        let coin = if q_up.contains("BITCOIN") || q_up.contains("BTC") {
            "BTC"
        } else if q_up.contains("ETHEREUM") || q_up.contains("ETH") {
            "ETH"
        } else if q_up.contains("SOLANA") || q_up.contains("SOL") {
            "SOL"
        } else if q_up.contains("XRP") || q_up.contains("RIPPLE") {
            "XRP"
        } else {
            continue;
        };

        if let Some((start, _)) = api::extract_window_times(&pm.question) {
            let key = format!("{}-{}", coin, start);
            if let Some(km) = kalshi_map.get(&key) {
                let (pm_yes, pm_no) = match parse_token_ids_local(pm.clob_token_ids.as_deref()) {
                    Some(pair) => pair,
                    None => continue,
                };

                let km_strike = km.target_price.unwrap_or(0.0);
                let pm_strike = api::extract_strike(&pm.question).unwrap_or(km_strike);

                if km_strike <= 0.0 && pm_strike <= 0.0 {
                    continue;
                }

                info!(
                    "🔗 LINKED SUCCESS: {} | Window: {}:{:02} | Strike: {:.2} | KM: {} | PM: {}",
                    coin,
                    start / 60,
                    start % 60,
                    km_strike,
                    km.id,
                    pm.id
                );
                pairs.push(DualMarketPair {
                    coin: coin.to_string(),
                    kalshi_ticker: km.id.clone(),
                    pm_market_id: pm.id.clone(),
                    pm_yes_token: pm_yes,
                    pm_no_token: pm_no,
                    window_start_mins: start,
                    km_strike,
                    pm_strike,
                });
            }
        }
    }
    pairs
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();
    // Try to init logger, ignore if already init. Default to info so a
    // background launch never runs silently because RUST_LOG was not inherited.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();

    info!("🚀 ¡SISTEMA INICIADO Y ESCANEANDO! 🚀");

    let startup =
        config::validate_startup().map_err(|e| format!("Startup validation failed: {}", e))?;
    let paper_mode = !startup.live_mode;
    let telegram_bot = TelegramBot::new();
    let http_client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
        .timeout(Duration::from_secs(30))
        .build()?;
    let poly_client = PolymarketClobClient::new();
    if startup.live_mode {
        poly_client
            .ping()
            .await
            .map_err(|e| format!("Polymarket CLOB daemon is required for LIVE mode: {}", e))?;
    }
    let mut kalshi_client = KalshiClient::build_prod(
        std::env::var("KALSHI_EMAIL").unwrap_or_default(),
        std::env::var("KALSHI_PASSWORD").unwrap_or_default(),
    );
    kalshi_client
        .login()
        .await
        .map_err(|e| format!("Kalshi authentication failed: {}", e))?;

    let mut capital_manager = if startup.live_mode {
        let polymarket_balance = poly_client
            .get_collateral_balance()
            .await
            .map_err(|e| format!("Polymarket collateral preflight failed: {}", e))?;
        let (kalshi_cash_balance, kalshi_portfolio_value) = kalshi_client
            .get_balance_dollars()
            .await
            .map_err(|e| format!("Kalshi balance preflight failed: {}", e))?;
        let require_venue_funds = env_bool("REQUIRE_LIVE_VENUE_FUNDS", true);

        info!(
            "LIVE BALANCES | Polymarket collateral=${:.2} | Kalshi cash=${:.2} portfolio=${:.2}",
            polymarket_balance, kalshi_cash_balance, kalshi_portfolio_value
        );

        if require_venue_funds
            && (polymarket_balance + 0.0001 < startup.position_size
                || kalshi_cash_balance + 0.0001 < startup.position_size)
        {
            let msg = format!(
                "Startup blocked: live balances below POSITION_SIZE ${:.2} | Polymarket ${:.2} | Kalshi cash ${:.2}",
                startup.position_size, polymarket_balance, kalshi_cash_balance
            );
            if let Some(bot) = telegram_bot.as_ref() {
                bot.send_message(&format!("*STARTUP BLOQUEADO*\n{}", msg))
                    .await;
            }
            return Err(msg.into());
        }

        DualCapitalManager::with_balances(false, polymarket_balance, kalshi_cash_balance)
    } else {
        DualCapitalManager::new(true)
    };
    let mut open_positions = load_state();
    let mut market_burned: HashSet<String> = HashSet::new();
    let mut last_closed_minute = -1;

    if startup.live_mode {
        info!("🔍 Reconciling on-chain balances with local state...");
        
        let manage_live_orphans = env_bool("MANAGE_LIVE_ORPHANS", false);

        // A. Verify/Correct existing positions
        let mut i = 0;
        while i < open_positions.len() {
            let pos = &mut open_positions[i];
            let (actual_bal, actual_buy_yes): (f64, Option<bool>) = if pos.venue == Venue::Polymarket {
                match poly_client.get_balance(&pos.pm_token_id()).await {
                    Ok(balance) => (balance, Some(pos.buy_yes)),
                    Err(err) => {
                        warn!(
                            "RECONCILE | {} Polymarket balance check failed; keeping local shares={:.4}: {}",
                            pos.coin, pos.shares, err
                        );
                        (pos.shares, Some(pos.buy_yes))
                    }
                }
            } else {
                match kalshi_client.get_portfolio_positions().await {
                    Ok(portfolio) => {
                        let live_pos = portfolio
                            .iter()
                            .find(|lp| lp.ticker == pos.kalshi_ticker)
                            .cloned();
                        (
                            live_pos.as_ref().map(kalshi_position_contracts).unwrap_or(0.0),
                            live_pos.as_ref().map(kalshi_position_buy_yes),
                        )
                    }
                    Err(err) => {
                        warn!(
                            "RECONCILE | {} Kalshi portfolio check failed; keeping local shares={:.4}: {}",
                            pos.coin, pos.shares, err
                        );
                        (pos.shares, Some(pos.buy_yes))
                    }
                }
            };

            if let Some(actual_buy_yes) = actual_buy_yes {
                if pos.venue == Venue::Kalshi && pos.buy_yes != actual_buy_yes && actual_bal > 0.001 {
                    warn!(
                        "RECONCILE | {} Kalshi side mismatch: JSON={} actual={} | Correcting...",
                        pos.coin,
                        if pos.buy_yes { "YES" } else { "NO" },
                        if actual_buy_yes { "YES" } else { "NO" }
                    );
                    pos.buy_yes = actual_buy_yes;
                }
            }

            let close_epsilon = position_close_epsilon(pos);
            if (actual_bal - pos.shares).abs() > 0.01 {
                warn!("RECONCILE | {} mismatch: JSON={:.4} actual={:.4} | Correcting...", pos.coin, pos.shares, actual_bal);
                if actual_bal <= close_epsilon {
                    open_positions.remove(i);
                    continue;
                } else {
                    pos.shares = actual_bal;
                    pos.notional_usdc = pos.shares * pos.entry_price;
                }
            }
            i += 1;
        }

        // B. ADOPT ORPHANS: Check all active twins for balances NOT in JSON
        let initial_twins = build_twin_markets(&kalshi_client, &poly_client, Local::now(), &mut last_closed_minute, None).await;
        for twin in initial_twins {
            // Check Polymarket Yes/No
            for (token_id, buy_yes) in [(&twin.pm_yes_token, true), (&twin.pm_no_token, false)] {
                let bal = poly_client.get_balance(token_id).await.unwrap_or(0.0);
                if bal > 0.01 && !open_positions.iter().any(|p| p.venue == Venue::Polymarket && p.pm_yes_token == twin.pm_yes_token && p.buy_yes == buy_yes) {
                    if !manage_live_orphans {
                        warn!(
                            "ORPHAN DETECTED UNMANAGED | Polymarket {} {} | shares={:.4}",
                            twin.coin,
                            if buy_yes { "YES" } else { "NO" },
                            bal
                        );
                        if let Some(bot) = telegram_bot.as_ref() {
                            bot.send_message(&format!(
                                "*Posicion manual detectada*\nVenue: `Polymarket`\nActivo: `{}`\nLado: `{}`\nShares: `{:.4}`\nNo sera gestionada por el bot. Para permitirlo, setea `MANAGE_LIVE_ORPHANS=true`.",
                                twin.coin,
                                if buy_yes { "YES" } else { "NO" },
                                bal
                            ))
                            .await;
                        }
                        continue;
                    }
                    info!("🛡️ ORPHAN ADOPTED | Polymarket {} | shares={:.4}", twin.coin, bal);
                    open_positions.push(OpenPosition {
                        twin_key: twin.kalshi_ticker.clone(),
                        venue: Venue::Polymarket,
                        coin: twin.coin.clone(),
                        pm_market_id: twin.pm_market_id.clone(),
                        pm_yes_token: twin.pm_yes_token.clone(),
                        pm_no_token: twin.pm_no_token.clone(),
                        kalshi_ticker: twin.kalshi_ticker.clone(),
                        buy_yes,
                        entry_price: 0.85,
                        shares: bal,
                        notional_usdc: bal * 0.85,
                        entry_order_id: None,
                        last_exit_order_id: None,
                        last_error: None,
                        opened_at: Some(now_rfc3339()),
                        updated_at: Some(now_rfc3339()),
                        dca_executed: true,
                        is_hedge: false,
                        hedge_sl_price: None,
                        hedge_tp_price: None,
                        binance_entry_price: 0.0,
                        binance_retrace_threshold: 0.0,
                        state: PositionState::Open,
                        hedge_pair_id: None,
                    });
                }
            }
            // Check Kalshi (already checked in preflight but for adoption)
            let kalshi_pos = kalshi_client.get_portfolio_positions().await.unwrap_or_default();
            if let Some(lp) = kalshi_pos.iter().find(|p| p.ticker == twin.kalshi_ticker) {
                let bal = kalshi_position_contracts(lp);
                let buy_yes = kalshi_position_buy_yes(lp);
                if bal > 0.1 && !open_positions.iter().any(|p| p.venue == Venue::Kalshi && p.kalshi_ticker == twin.kalshi_ticker) {
                    if !manage_live_orphans {
                        warn!(
                            "ORPHAN DETECTED UNMANAGED | Kalshi {} {} | shares={:.1}",
                            twin.coin,
                            if buy_yes { "YES" } else { "NO" },
                            bal
                        );
                        if let Some(bot) = telegram_bot.as_ref() {
                            bot.send_message(&format!(
                                "*Posicion manual detectada*\nVenue: `Kalshi`\nActivo: `{}`\nLado: `{}`\nShares: `{:.1}`\nNo sera gestionada por el bot. Para permitirlo, setea `MANAGE_LIVE_ORPHANS=true`.",
                                twin.coin,
                                if buy_yes { "YES" } else { "NO" },
                                bal
                            ))
                            .await;
                        }
                        continue;
                    }
                    info!("🛡️ ORPHAN ADOPTED | Kalshi {} | shares={:.1}", twin.coin, bal);
                    open_positions.push(OpenPosition {
                        twin_key: twin.kalshi_ticker.clone(),
                        venue: Venue::Kalshi,
                        coin: twin.coin.clone(),
                        pm_market_id: twin.pm_market_id.clone(),
                        pm_yes_token: twin.pm_yes_token.clone(),
                        pm_no_token: twin.pm_no_token.clone(),
                        kalshi_ticker: twin.kalshi_ticker.clone(),
                        buy_yes,
                        entry_price: 0.85,
                        shares: bal,
                        notional_usdc: bal * 0.85,
                        entry_order_id: None,
                        last_exit_order_id: None,
                        last_error: None,
                        opened_at: Some(now_rfc3339()),
                        updated_at: Some(now_rfc3339()),
                        dca_executed: true,
                        is_hedge: false,
                        hedge_sl_price: None,
                        hedge_tp_price: None,
                        binance_entry_price: 0.0,
                        binance_retrace_threshold: 0.0,
                        state: PositionState::Open,
                        hedge_pair_id: None,
                    });
                }
            }
        }
        
        let pruned = prune_terminal_positions(&mut open_positions);
        if pruned > 0 {
            info!("RECONCILE | pruned {} terminal zero-share positions from active state.", pruned);
        }

        save_state(&open_positions);
        info!("✅ Reconciliation complete. {} active positions managed.", open_positions.len());
    }

    if let Some(bot) = telegram_bot.as_ref() {
        bot.send_message(&format!(
            "*Arbitrage Hammer iniciado*\nModo: `{}`\nPosiciones cargadas desde disco: `{}`",
            if paper_mode { "PAPER" } else { "LIVE" },
            open_positions.len()
        ))
        .await;
    }

    let position_size = startup.position_size;
    let take_profit_price = env_f64("TAKE_PROFIT_PRICE", 0.97);
    let hard_sl_price = env_f64("HARD_SL_PRICE", 0.68);
    let hard_sl_exit_floor = env_f64("HARD_SL_EXIT_FLOOR", 0.47);
    let allow_dca = if startup.live_mode {
        env_bool("ALLOW_LIVE_DCA", false)
    } else {
        env_bool("ALLOW_DCA", true)
    };
    let dca_start_price = env_f64("DCA_START_PRICE", 0.77);
    let dca_min_price = env_f64("DCA_MIN_PRICE", 0.74);
    let dca_size_factor = env_f64("DCA_SIZE_FACTOR", 0.5);
    let entry_start_secs = env_i32("ENTRY_START_SEC", 540);
    let entry_end_secs = env_i32("ENTRY_END_SEC", 790);
    let mut safe_mode_active = false;
    let mut last_alert_times: std::collections::HashMap<String, chrono::DateTime<chrono::Local>> = std::collections::HashMap::new();

    loop {
        let now = chrono::Local::now();
        let twins = build_twin_markets(
            &kalshi_client,
            &poly_client,
            now,
            &mut last_closed_minute,
            telegram_bot.as_ref(),
        )
        .await;

        let stale_positions = stale_positions_for_alert(&mut open_positions, &twins);
        if !stale_positions.is_empty() {
            let mut alerted_any = false;
            let mut alert_details = Vec::new();
            
            for pos in &stale_positions {
                let last_alert = last_alert_times.get(&pos.twin_key).copied().unwrap_or(chrono::Local::now() - chrono::Duration::seconds(61));
                if (chrono::Local::now() - last_alert).num_seconds() > 60 {
                    alert_details.push(format!("{} {} (Estado: {:?}, Prox. intento: 10s)", pos.coin, pos.twin_key, pos.state));
                    last_alert_times.insert(pos.twin_key.clone(), chrono::Local::now());
                    alerted_any = true;
                }
            }

            if alerted_any {
                warn!(
                    "{} persisted open_positions entries are not in current active markets.",
                    stale_positions.len()
                );

                if let Some(bot) = telegram_bot.as_ref() {
                    bot.send_message(&format!(
                        "🚨 *ALERTA ANTI-SPAM (60s)*\n`{}` posiciones no aparecen en el mercado activo actual. No las removi automaticamente:\n{}",
                        stale_positions.len(),
                        alert_details.join("\n")
                    )).await;
                }
            }
            
            // --- POSITION RECOVERY MANAGER (For Stale / Adrift Positions) ---
            for stale_pos in stale_positions {
                let mut j = 0;
                while j < open_positions.len() {
                    if open_positions[j].twin_key == stale_pos.twin_key {
                        break;
                    }
                    j += 1;
                }
                if j >= open_positions.len() { continue; }
                
                // Fetch live positions from Kalshi to check if it really disappeared
                if open_positions[j].venue == Venue::Kalshi {
                    let kalshi_pos = kalshi_client.get_portfolio_positions().await.unwrap_or_default();
                    if let Some(lp) = kalshi_pos.iter().find(|p| p.ticker == open_positions[j].kalshi_ticker) {
                        let bal = kalshi_position_contracts(lp);
                        if bal <= 0.001 {
                            info!("Stale Kalshi position {} resolved/closed. Marking ResolvedConfirmed.", open_positions[j].kalshi_ticker);
                            open_positions[j].shares = 0.0;
                            open_positions[j].notional_usdc = 0.0;
                            open_positions[j].state = PositionState::ResolvedConfirmed;
                            open_positions[j].last_error = None;
                            open_positions[j].updated_at = Some(now_rfc3339());
                            save_state(&open_positions);
                        } else {
                            // Still exists, but market is not active!
                            open_positions[j].state = PositionState::ExpiredPendingResolution;
                            open_positions[j].shares = bal;
                            open_positions[j].notional_usdc = bal * open_positions[j].entry_price;
                            open_positions[j].last_error = Some(format!(
                                "Market no longer active, but live Kalshi inventory remains ({:.4} contracts). Holding under bot control until resolved.",
                                bal
                            ));
                            open_positions[j].updated_at = Some(now_rfc3339());
                            save_state(&open_positions);
                        }
                    } else {
                        // Not in portfolio -> resolved
                        info!("Stale Kalshi position {} not in portfolio. Marking ResolvedConfirmed.", open_positions[j].kalshi_ticker);
                        open_positions[j].shares = 0.0;
                        open_positions[j].notional_usdc = 0.0;
                        open_positions[j].state = PositionState::ResolvedConfirmed;
                        open_positions[j].last_error = None;
                        open_positions[j].updated_at = Some(now_rfc3339());
                        save_state(&open_positions);
                    }
                } else {
                    // Polymarket
                    let bal = poly_client.get_balance(open_positions[j].pm_token_id()).await.unwrap_or(open_positions[j].shares);
                    let close_epsilon = position_close_epsilon(&open_positions[j]);
                    if bal <= close_epsilon {
                        info!("Stale Polymarket position {} resolved/closed. Marking ResolvedConfirmed.", open_positions[j].pm_token_id());
                        open_positions[j].shares = 0.0;
                        open_positions[j].notional_usdc = 0.0;
                        open_positions[j].state = PositionState::ResolvedConfirmed;
                        open_positions[j].last_error = None;
                        open_positions[j].updated_at = Some(now_rfc3339());
                        save_state(&open_positions);
                    } else {
                        open_positions[j].state = PositionState::ExpiredPendingResolution;
                        open_positions[j].shares = bal;
                        open_positions[j].notional_usdc = bal * open_positions[j].entry_price;
                        open_positions[j].last_error = Some(format!(
                            "Market no longer active, but live Polymarket inventory remains ({:.4} shares). Holding under bot control until sell/redeem is confirmed.",
                            bal
                        ));
                        open_positions[j].updated_at = Some(now_rfc3339());
                        save_state(&open_positions);
                    }
                }
            }
        }

        let pruned = prune_terminal_positions(&mut open_positions);
        if pruned > 0 {
            info!("STATE COMPACT | pruned {} terminal zero-share positions.", pruned);
            save_state(&open_positions);
        }
        
        // --- EVALUATE SAFE MODE ---
        safe_mode_active = open_positions.iter().any(|pos| {
            matches!(
                pos.state,
                PositionState::ExitFailedZeroFill
                    | PositionState::RecoveryPending
                    | PositionState::ManualReviewRequired
                    | PositionState::ExpiredPendingResolution
            )
        });

        let mut scan_total = 0usize;
        let mut scan_no_signal = 0usize;
        let mut scan_binance_unavailable = 0usize;
        let mut scan_has_position = 0usize;
        let mut scan_outside_window = 0usize;
        let mut scan_safe_mode = 0usize;
        let mut scan_price_blocked = 0usize;
        let mut scan_funds_blocked = 0usize;
        let mut scan_burned_blocked = 0usize;
        let mut scan_invalid_ask = 0usize;
        let mut scan_attemptable = 0usize;

        for twin in &twins {
            scan_total += 1;
            let symbol = format!("{}USDT", twin.coin);
            let vol_metrics =
                arbitrage_hammer::volatility::get_volatility_metrics(&http_client, &symbol).await;
            let binance_price = vol_metrics.current_price;
            let binance_open = match arbitrage_hammer::volatility::fetch_binance_candle_open(
                &http_client,
                &symbol,
                twin.window_start_mins,
            )
            .await
            {
                Ok(open) if open > 0.0 => open,
                Ok(_) => {
                    scan_binance_unavailable += 1;
                    info!(
                        "⚠️ Binance open invalid for {} window {}:{}",
                        twin.coin,
                        twin.window_start_mins / 60,
                        twin.window_start_mins % 60
                    );
                    continue;
                }
                Err(e) => {
                    scan_binance_unavailable += 1;
                    info!("⚠️ Binance open unavailable for {}: {}", twin.coin, e);
                    continue;
                }
            };

            let signal_dist = (binance_price - binance_open).abs();
            let signal_up = binance_price > binance_open;

            let mut threshold =
                arbitrage_hammer::entry_engine::distance_threshold_for(&symbol, vol_metrics.state);
            let pct_threshold = arbitrage_hammer::entry_engine::distance_threshold_pct_for(&symbol, vol_metrics.state);
            if pct_threshold > 0.0 {
                threshold = threshold.max(binance_open * pct_threshold);
            }
            let entry_signal = signal_dist >= threshold;
            let elapsed_secs = window_elapsed_secs(now, twin.window_start_mins);

            if now.second() % 30 == 0 {
                info!("📊 STATS {} | Binance: {:.2} | Open15m: {:.2} | Dist: {:.2} | Target: {:.2} | Elapsed: {}s | Signal: {}",
                    twin.coin, binance_price, binance_open, signal_dist, threshold, elapsed_secs, if entry_signal { "✅" } else { "❌" });
            }

            // 1. POSITION MANAGEMENT (Every tick for matching positions)
            let mut j = 0;
            while j < open_positions.len() {
                if open_positions[j].twin_key != twin.kalshi_ticker {
                    j += 1;
                    continue;
                }
                let pos = open_positions[j].clone();
                if matches!(
                    pos.state,
                    PositionState::StopPending
                        | PositionState::HedgeEvaluating
                        | PositionState::HedgePending
                        | PositionState::Hedged
                        | PositionState::Unwinding
                        | PositionState::ExpiryHold
                        | PositionState::Closed
                        | PositionState::ClosedConfirmed
                        | PositionState::ExpiredPendingResolution
                        | PositionState::ResolvedConfirmed
                        | PositionState::ManualReviewRequired
                ) && !pos.is_hedge
                {
                    j += 1;
                    continue;
                }

                // ── STOP-FAILED ESCALATING RETRY ──────────────────────────────
                // Positions stuck in StopFailed are retried every tick with
                // increasing aggression so they NEVER bleed to zero silently.
                if matches!(pos.state, PositionState::RecoveryPending | PositionState::ExitFailedZeroFill)
                    && !pos.is_hedge
                {
                    // Count how many times we have already failed by inspecting last_error.
                    let fail_count = pos
                        .last_error
                        .as_deref()
                        .and_then(|e| {
                            e.split("attempt#").nth(1).and_then(|s| s.parse::<u32>().ok())
                        })
                        .unwrap_or(0);

                    // Fetch live price for this venue
                    let (sf_ask, sf_bid) = if pos.venue == Venue::Polymarket {
                        let ask = if pos.buy_yes {
                            api::get_best_ask(&http_client, &pos.pm_market_id, &pos.pm_yes_token).await.unwrap_or(0.0)
                        } else {
                            api::get_best_ask(&http_client, &pos.pm_market_id, &pos.pm_no_token).await.unwrap_or(0.0)
                        };
                        let bid = if pos.buy_yes {
                            api::get_best_bid(&http_client, &pos.pm_yes_token).await.unwrap_or(0.0)
                        } else {
                            api::get_best_bid(&http_client, &pos.pm_no_token).await.unwrap_or(0.0)
                        };
                        (ask, bid)
                    } else {
                        let ((ky_ask, kn_ask), (ky_bid, kn_bid)) = kalshi_client
                            .get_outcome_top_of_book(&pos.kalshi_ticker)
                            .await
                            .unwrap_or(((None, None), (None, None)));
                        let ask = if pos.buy_yes { ky_ask.unwrap_or(0.0) } else { kn_ask.unwrap_or(0.0) };
                        let bid = if pos.buy_yes { ky_bid.unwrap_or(0.0) } else { kn_bid.unwrap_or(0.0) };
                        (ask, bid)
                    };
                    let sf_ref = if sf_bid > 0.0 { sf_bid } else { sf_ask };

                    // Escalation ladder: each failed attempt uses a steeper discount.
                    // After 3 failures we go nuclear (floor = 0.01).
                    let exit_price = if fail_count == 0 {
                        // First retry: bid minus 5% slippage
                        (sf_ref - 0.05).max(0.01)
                    } else if fail_count == 1 {
                        // Second retry: bid minus 15% — more aggressive
                        (sf_ref - 0.15).max(0.01)
                    } else {
                        // Nuclear: sell at floor no matter what to guarantee exit
                        0.01
                    };

                    warn!(
                        "STOP-FAILED RETRY | coin={} ticker={} venue={:?} attempt={} exit_price={:.4} bid={:.4}",
                        pos.coin, pos.kalshi_ticker, pos.venue, fail_count + 1, exit_price, sf_ref
                    );

                    let sf_res = if pos.venue == Venue::Polymarket {
                        close_polymarket_position(
                            &http_client,
                            &poly_client,
                            pos.pm_token_id(),
                            pos.shares,
                            exit_price,
                            paper_mode,
                        ).await
                    } else {
                        close_kalshi_position(
                            &kalshi_client,
                            &pos.kalshi_ticker,
                            pos.buy_yes,
                            pos.shares,
                            exit_price,
                            paper_mode,
                        ).await
                    };

                    match sf_res {
                        Ok(fill) => {
                            match reconcile_close_fill(&mut open_positions[j], &fill, "STOP-FAILED") {
                                Ok(true) => {}
                                Ok(false) => {
                                    let proceeds = fill.shares_sold * fill.fill_price;
                                    capital_manager.add(&pos.venue_platform(), proceeds);
                                    if let Some(bot) = telegram_bot.as_ref() {
                                        bot.send_message(&format!(
                                            "⚠️ *CIERRE PARCIAL — POSICIÓN SIGUE ABIERTA*\nActivo: `{}`\nVendido: `{:.4}`\nRestante: `{:.4}`\nMotivo: `STOP-FAILED`",
                                            pos.coin, fill.shares_sold, open_positions[j].shares
                                        ))
                                        .await;
                                    }
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                                Err(e) => {
                                    warn!("RECOVERY FAILED zero/unconfirmed fill | coin={} error={}", pos.coin, e);
                                    safe_mode_active = true;
                                    if let Some(bot) = telegram_bot.as_ref() {
                                        bot.send_message(&format!(
                                            "🚨 *RECOVERY FAILED — CIERRE NO EJECUTADO*\nActivo: `{}`\nVenue: `{:?}`\nError: `{}`\nLa posición sigue abierta o requiere revisión manual.",
                                            pos.coin, pos.venue, e
                                        ))
                                        .await;
                                    }
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                            }
                            let proceeds = fill.shares_sold * fill.fill_price;
                            capital_manager.add(&pos.venue_platform(), proceeds);
                            let profit = proceeds - pos.notional_usdc;
                            let is_win = proceeds >= pos.notional_usdc;
                            
                            error!(
                                "STOP-FAILED RECOVERED | coin={} ticker={} exit_price={:.4} proceeds={:.4} profit={:.4} after {} attempts",
                                pos.coin, pos.kalshi_ticker, fill.fill_price, proceeds, profit, fail_count + 1
                            );
                            
                            if let Some(bot) = telegram_bot.as_ref() {
                                let msg = format!(
                                    "✅ *CIERRE RECUPERADO* (intento #{})\nActivo: `{}`\nResultado: {}\nEntrada: `{:.4}` | Salida: `{:.4}`\nInversion: `${:.2}` | Retorno: `${:.2}`\nBeneficio: *${:+.2}*\n\n💰 *Cartera {:?}:* `${:.2}`",
                                    fail_count + 1, 
                                    pos.coin, 
                                    if is_win { "🟢 GANANCIA" } else { "🔴 PERDIDA" },
                                    pos.entry_price, 
                                    fill.fill_price, 
                                    pos.notional_usdc, 
                                    proceeds, 
                                    profit,
                                    pos.venue_platform(),
                                    capital_manager.balance(&pos.venue_platform())
                                );
                                bot.send_message(&msg).await;
                            }
                            open_positions[j].last_exit_order_id = fill.order_id;
                            open_positions[j].updated_at = Some(now_rfc3339());
                            open_positions.remove(j);
                            save_state(&open_positions);
                            continue;
                        }
                        Err(e) => {
                            let next_attempt = fail_count + 1;
                            safe_mode_active = true;
                            open_positions[j].last_error = Some(format!(
                                "StopFailed attempt#{} @ {:.4}: {}",
                                next_attempt, exit_price, e
                            ));
                            open_positions[j].updated_at = Some(now_rfc3339());
                            warn!(
                                "STOP-FAILED retry {} failed | coin={} price={:.4} error={}",
                                next_attempt, pos.coin, exit_price, e
                            );
                            save_state(&open_positions);
                            j += 1;
                            continue;
                        }
                    }
                }
                // ── END STOP-FAILED RETRY ─────────────────────────────────────

                // Fetch correct prices for the outcome we actually hold
                let (c_ask, c_bid) = if pos.venue == Venue::Polymarket {
                    let ask = if pos.buy_yes {
                        api::get_best_ask(&http_client, &pos.pm_market_id, &pos.pm_yes_token)
                            .await
                            .unwrap_or(0.0)
                    } else {
                        api::get_best_ask(&http_client, &pos.pm_market_id, &pos.pm_no_token)
                            .await
                            .unwrap_or(0.0)
                    };
                    let bid = if pos.buy_yes {
                        api::get_best_bid(&http_client, &pos.pm_yes_token)
                            .await
                            .unwrap_or(0.0)
                    } else {
                        api::get_best_bid(&http_client, &pos.pm_no_token)
                            .await
                            .unwrap_or(0.0)
                    };
                    (ask, bid)
                } else {
                    let ((ky_ask, kn_ask), (ky_bid, kn_bid)) = kalshi_client
                        .get_outcome_top_of_book(&pos.kalshi_ticker)
                        .await
                        .unwrap_or(((None, None), (None, None)));
                    let ask = if pos.buy_yes {
                        ky_ask.unwrap_or(0.0)
                    } else {
                        kn_ask.unwrap_or(0.0)
                    };
                    let bid = if pos.buy_yes {
                        ky_bid.unwrap_or(0.0)
                    } else {
                        kn_bid.unwrap_or(0.0)
                    };
                    (ask, bid)
                };

                let stop_ref = if c_bid > 0.0 { c_bid } else { c_ask };

                // A. BINANCE RETRACE EXIT
                if pos.binance_retrace_threshold > 0.0 {
                    let triggered = if pos.buy_yes {
                        binance_price < pos.binance_entry_price - pos.binance_retrace_threshold
                    } else {
                        binance_price > pos.binance_entry_price + pos.binance_retrace_threshold
                    };
                    if triggered {
                        let res = if pos.venue == Venue::Polymarket {
                            close_polymarket_position(
                                &http_client,
                                &poly_client,
                                pos.pm_token_id(),
                                pos.shares,
                                stop_ref.max(0.01),
                                paper_mode,
                            )
                            .await
                        } else {
                            close_kalshi_position_aggressive(
                                &kalshi_client,
                                &pos.kalshi_ticker,
                                pos.buy_yes,
                                pos.shares,
                                stop_ref.max(0.01),
                                paper_mode,
                            )
                            .await
                        };
                        match res {
                            Ok(fill) => {
                                match reconcile_close_fill(&mut open_positions[j], &fill, "BINANCE-RETRACE") {
                                    Ok(true) => {}
                                    Ok(false) => {
                                        let proceeds = fill.shares_sold * fill.fill_price;
                                        capital_manager.add(&pos.venue_platform(), proceeds);
                                        warn!(
                                            "BINANCE-RETRACE partial close | coin={} sold={:.4} remaining={:.4}",
                                            pos.coin, fill.shares_sold, open_positions[j].shares
                                        );
                                        save_state(&open_positions);
                                        j += 1;
                                        continue;
                                    }
                                    Err(e) => {
                                        warn!("BINANCE-RETRACE close zero/unconfirmed | coin={} error={}", pos.coin, e);
                                        safe_mode_active = true;
                                        save_state(&open_positions);
                                        j += 1;
                                        continue;
                                    }
                                }
                                let proceeds = fill.shares_sold * fill.fill_price;
                                capital_manager.add(&pos.venue_platform(), proceeds);
                                if let Some(bot) = telegram_bot.as_ref() {
                                    let msg = format_close_message(
                                        &pos.coin,
                                        proceeds >= pos.notional_usdc,
                                        "BINANCE-RETRACE",
                                        pos.entry_price,
                                        fill.fill_price,
                                        pos.notional_usdc,
                                        proceeds - pos.notional_usdc,
                                        &pos.venue_platform(),
                                        capital_manager.balance(&pos.venue_platform()),
                                    );
                                    let _ = bot.send_message(&msg).await;
                                }
                                open_positions[j].last_exit_order_id = fill.order_id;
                                open_positions[j].updated_at = Some(now_rfc3339());
                                open_positions.remove(j);
                                save_state(&open_positions);
                                continue;
                            }
                            Err(e) => {
                                safe_mode_active = true;
                                alert_close_failure(
                                    telegram_bot.as_ref(),
                                    &mut open_positions[j],
                                    "BINANCE-RETRACE",
                                    e,
                                )
                                .await;
                                save_state(&open_positions);
                                j += 1;
                                continue;
                            }
                        }
                    }
                }

                // B. TAKE PROFIT
                if c_bid >= take_profit_price {
                    let res = if pos.venue == Venue::Polymarket {
                        close_polymarket_position(
                            &http_client,
                            &poly_client,
                            pos.pm_token_id(),
                            pos.shares,
                            c_bid,
                            paper_mode,
                        )
                        .await
                    } else {
                        close_kalshi_position(
                            &kalshi_client,
                            &pos.kalshi_ticker,
                            pos.buy_yes,
                            pos.shares,
                            c_bid,
                            paper_mode,
                        )
                        .await
                    };
                    match res {
                        Ok(fill) => {
                            match reconcile_close_fill(&mut open_positions[j], &fill, "TAKE-PROFIT") {
                                Ok(true) => {}
                                Ok(false) => {
                                    let proceeds = fill.shares_sold * fill.fill_price;
                                    capital_manager.add(&pos.venue_platform(), proceeds);
                                    if let Some(bot) = telegram_bot.as_ref() {
                                        bot.send_message(&format!(
                                            "⚠️ *CIERRE PARCIAL — POSICIÓN SIGUE ABIERTA*\nActivo: `{}`\nVendido: `{:.4}`\nRestante: `{:.4}`\nMotivo: `TAKE-PROFIT`",
                                            pos.coin, fill.shares_sold, open_positions[j].shares
                                        ))
                                        .await;
                                    }
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                                Err(e) => {
                                    warn!("TAKE-PROFIT close zero/unconfirmed | coin={} error={}", pos.coin, e);
                                    safe_mode_active = true;
                                    if let Some(bot) = telegram_bot.as_ref() {
                                        bot.send_message(&format!(
                                            "🚨 *CIERRE NO EJECUTADO — POSICIÓN SIGUE ABIERTA*\nActivo: `{}`\nVenue: `{:?}`\nMotivo: `TAKE-PROFIT`\nError: `{}`",
                                            pos.coin, pos.venue, e
                                        ))
                                        .await;
                                    }
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                            }
                            let proceeds = fill.shares_sold * fill.fill_price;
                            capital_manager.add(&pos.venue_platform(), proceeds);
                            if let Some(bot) = telegram_bot.as_ref() {
                                let is_win = proceeds >= pos.notional_usdc;
                                let msg = format_close_message(
                                    &pos.coin,
                                    is_win,
                                    "TAKE-PROFIT",
                                    pos.entry_price,
                                    fill.fill_price,
                                    pos.notional_usdc,
                                    proceeds - pos.notional_usdc,
                                    &pos.venue_platform(),
                                    capital_manager.balance(&pos.venue_platform()),
                                );
                                let _ = bot.send_message(&msg).await;
                            }
                            open_positions[j].last_exit_order_id = fill.order_id;
                            open_positions[j].updated_at = Some(now_rfc3339());
                            open_positions.remove(j);
                            save_state(&open_positions);
                            continue;
                        }
                        Err(e) => {
                            safe_mode_active = true;
                            alert_close_failure(
                                telegram_bot.as_ref(),
                                &mut open_positions[j],
                                "TAKE-PROFIT",
                                e,
                            )
                            .await;
                            save_state(&open_positions);
                            j += 1;
                            continue;
                        }
                    }
                }

                // C. HARD STOP LOSS
                let dynamic_sl = if pos.entry_price > hard_sl_price + 0.10 {
                    hard_sl_price
                } else {
                    pos.entry_price - 0.15
                };

                if stop_ref > 0.0 && stop_ref <= dynamic_sl {
                    let exit = stop_ref.max(hard_sl_exit_floor).max(0.01);
                    let res = if pos.venue == Venue::Polymarket {
                        close_polymarket_position(
                            &http_client,
                            &poly_client,
                            pos.pm_token_id(),
                            pos.shares,
                            exit,
                            paper_mode,
                        )
                        .await
                    } else {
                        close_kalshi_position_aggressive(
                            &kalshi_client,
                            &pos.kalshi_ticker,
                            pos.buy_yes,
                            pos.shares,
                            exit,
                            paper_mode,
                        )
                        .await
                    };
                    match res {
                        Ok(fill) => {
                            match reconcile_close_fill(&mut open_positions[j], &fill, "HARD-SL") {
                                Ok(true) => {}
                                Ok(false) => {
                                    let proceeds = fill.shares_sold * fill.fill_price;
                                    capital_manager.add(&pos.venue_platform(), proceeds);
                                    warn!(
                                        "HARD-SL partial close | coin={} sold={:.4} remaining={:.4}",
                                        pos.coin, fill.shares_sold, open_positions[j].shares
                                    );
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                                Err(e) => {
                                    warn!("HARD-SL close zero/unconfirmed | coin={} error={}", pos.coin, e);
                                    safe_mode_active = true;
                                    save_state(&open_positions);
                                    j += 1;
                                    continue;
                                }
                            }
                            let proceeds = fill.shares_sold * fill.fill_price;
                            capital_manager.add(&pos.venue_platform(), proceeds);
                            if let Some(bot) = telegram_bot.as_ref() {
                                let msg = format_close_message(
                                    &pos.coin,
                                    false,
                                    "HARD-SL",
                                    pos.entry_price,
                                    fill.fill_price,
                                    pos.notional_usdc,
                                    proceeds - pos.notional_usdc,
                                    &pos.venue_platform(),
                                    capital_manager.balance(&pos.venue_platform()),
                                );
                                let _ = bot.send_message(&msg).await;
                            }
                            market_burned.insert(pos.twin_key.clone());
                            open_positions[j].last_exit_order_id = fill.order_id;
                            open_positions[j].updated_at = Some(now_rfc3339());
                            open_positions.remove(j);
                            save_state(&open_positions);
                            continue;
                        }
                        Err(e) => {
                            safe_mode_active = true;
                            alert_close_failure(
                                telegram_bot.as_ref(),
                                &mut open_positions[j],
                                "HARD-SL",
                                e,
                            )
                            .await;
                            let allow_cross_venue_hedge = if startup.live_mode {
                                env_bool("ALLOW_LIVE_CROSS_VENUE_HEDGE", false)
                            } else {
                                env_bool("ALLOW_CROSS_VENUE_HEDGE", false)
                            };
                            let sl_liquidity_threshold = env_f64("SL_LIQUIDITY_THRESHOLD", 0.23);
                            if allow_cross_venue_hedge && stop_ref <= sl_liquidity_threshold {
                                let hedge_buy_yes = !pos.buy_yes;
                                let hedge_size = env_f64("HEDGE_SIZE", position_size);
                                let pm_hedge_ask = if hedge_buy_yes {
                                    api::get_best_ask(
                                        &http_client,
                                        &twin.pm_market_id,
                                        &twin.pm_yes_token,
                                    )
                                    .await
                                    .unwrap_or(0.0)
                                } else {
                                    api::get_best_ask(
                                        &http_client,
                                        &twin.pm_market_id,
                                        &twin.pm_no_token,
                                    )
                                    .await
                                    .unwrap_or(0.0)
                                };
                                let ((ky_ask, kn_ask), _) = kalshi_client
                                    .get_outcome_top_of_book(&twin.kalshi_ticker)
                                    .await
                                    .unwrap_or(((None, None), (None, None)));
                                let km_hedge_ask = if hedge_buy_yes {
                                    ky_ask.unwrap_or(0.0)
                                } else {
                                    kn_ask.unwrap_or(0.0)
                                };

                                let prefer_kalshi = pos.venue == Venue::Polymarket;
                                let hedge_platform = if prefer_kalshi && km_hedge_ask > 0.0 {
                                    Platform::Kalshi
                                } else if !prefer_kalshi && pm_hedge_ask > 0.0 {
                                    Platform::Polymarket
                                } else if pm_hedge_ask > 0.0
                                    && (km_hedge_ask <= 0.0 || pm_hedge_ask < km_hedge_ask)
                                {
                                    Platform::Polymarket
                                } else {
                                    Platform::Kalshi
                                };
                                let hedge_ask = if hedge_platform == Platform::Polymarket {
                                    pm_hedge_ask
                                } else {
                                    km_hedge_ask
                                };

                                if hedge_ask > 0.0 && hedge_ask <= startup.max_entry_price {
                                    let hedge_res = if hedge_platform == Platform::Polymarket {
                                        execute_polymarket_entry(
                                            &http_client,
                                            &poly_client,
                                            if hedge_buy_yes {
                                                &twin.pm_yes_token
                                            } else {
                                                &twin.pm_no_token
                                            },
                                            hedge_size,
                                            hedge_ask,
                                            paper_mode,
                                        )
                                        .await
                                    } else {
                                        execute_kalshi_entry(
                                            &kalshi_client,
                                            &twin.kalshi_ticker,
                                            hedge_buy_yes,
                                            hedge_size,
                                            hedge_ask,
                                            paper_mode,
                                        )
                                        .await
                                    };

                                    match hedge_res {
                                        Ok(fill) => {
                                            let hedge_pair_id = format!(
                                                "{}-hedge-{}",
                                                pos.twin_key,
                                                chrono::Utc::now().timestamp()
                                            );
                                            open_positions.push(OpenPosition {
                                                twin_key: twin.kalshi_ticker.clone(),
                                                venue: if hedge_platform == Platform::Polymarket {
                                                    Venue::Polymarket
                                                } else {
                                                    Venue::Kalshi
                                                },
                                                coin: twin.coin.clone(),
                                                pm_market_id: twin.pm_market_id.clone(),
                                                pm_yes_token: twin.pm_yes_token.clone(),
                                                pm_no_token: twin.pm_no_token.clone(),
                                                kalshi_ticker: twin.kalshi_ticker.clone(),
                                                buy_yes: hedge_buy_yes,
                                                entry_price: fill.fill_price,
                                                shares: fill.shares,
                                                notional_usdc: fill.notional_usdc,
                                                entry_order_id: fill.order_id,
                                                last_exit_order_id: None,
                                                last_error: None,
                                                opened_at: Some(now_rfc3339()),
                                                updated_at: Some(now_rfc3339()),
                                                dca_executed: true,
                                                is_hedge: true,
                                                hedge_sl_price: Some(
                                                    (fill.fill_price
                                                        - env_f64("HEDGE_SL_GAP", 0.18))
                                                    .max(0.01),
                                                ),
                                                hedge_tp_price: Some(env_f64(
                                                    "HEDGE_TP_PRICE",
                                                    take_profit_price,
                                                )),
                                                binance_entry_price: binance_price,
                                                binance_retrace_threshold: 0.0,
                                                state: PositionState::Hedged,
                                                hedge_pair_id: Some(hedge_pair_id.clone()),
                                            });
                                            open_positions[j].state = PositionState::Hedged;
                                            open_positions[j].hedge_pair_id = Some(hedge_pair_id);
                                            open_positions[j].updated_at = Some(now_rfc3339());
                                            if let Some(bot) = telegram_bot.as_ref() {
                                                bot.send_message(&format!(
                                                    "*HEDGE ABIERTO*\nActivo: `{}`\nOriginal: `{:?}`\nHedge: `{:?}`\nPrecio: `{:.3}`\nMonto: `${:.2}`",
                                                    twin.coin,
                                                    pos.venue,
                                                    if hedge_platform == Platform::Polymarket { Venue::Polymarket } else { Venue::Kalshi },
                                                    fill.fill_price,
                                                    fill.notional_usdc
                                                ))
                                                .await;
                                            }
                                        }
                                        Err(hedge_error) => {
                                            open_positions[j].last_error = Some(format!(
                                                "Hard SL failed; hedge also failed: {}",
                                                hedge_error
                                            ));
                                            open_positions[j].updated_at = Some(now_rfc3339());
                                            if let Some(bot) = telegram_bot.as_ref() {
                                                bot.send_message(&format!(
                                                    "*HEDGE FALLIDO*\nActivo: `{}`\nError: `{}`",
                                                    twin.coin, hedge_error
                                                ))
                                                .await;
                                            }
                                        }
                                    }
                                }
                            }
                            save_state(&open_positions);
                            j += 1;
                            continue;
                        }
                    }
                }

                // D. DCA
                if allow_dca
                    && !pos.dca_executed
                    && c_ask > 0.0
                    && c_ask <= dca_start_price
                    && c_ask >= dca_min_price
                {
                    let dca_size = pos.notional_usdc * dca_size_factor;
                    if capital_manager.has_funds(&pos.venue_platform(), dca_size) {
                        let res = if pos.venue == Venue::Polymarket {
                            execute_polymarket_entry(
                                &http_client,
                                &poly_client,
                                pos.pm_token_id(),
                                dca_size,
                                c_ask,
                                paper_mode,
                            )
                            .await
                        } else {
                            execute_kalshi_entry(
                                &kalshi_client,
                                &pos.kalshi_ticker,
                                pos.buy_yes,
                                dca_size,
                                c_ask,
                                paper_mode,
                            )
                            .await
                        };
                        match res {
                            Ok(fill) => {
                                let added = fill.shares;
                                let f_price = fill.fill_price;
                                let old_cost =
                                    open_positions[j].entry_price * open_positions[j].shares;
                                open_positions[j].shares += added;
                                open_positions[j].notional_usdc += fill.notional_usdc;
                                open_positions[j].dca_executed = true;
                                open_positions[j].updated_at = Some(now_rfc3339());
                                if open_positions[j].shares > 0.0 {
                                    open_positions[j].entry_price =
                                        (old_cost + added * f_price) / open_positions[j].shares;
                                }
                                capital_manager.deduct(&pos.venue_platform(), fill.notional_usdc);
                                save_state(&open_positions);
                            }
                            Err(e) => {
                                warn!(
                                    "DCA failed | coin={} venue={:?} ticker={} error={}",
                                    pos.coin, pos.venue, pos.kalshi_ticker, e
                                );
                                open_positions[j].last_error = Some(format!("DCA failed: {}", e));
                                open_positions[j].updated_at = Some(now_rfc3339());
                                save_state(&open_positions);
                            }
                        }
                    }
                }
                j += 1;
            }

            // 2. ENTRY SIGNAL (Only if no existing position for this active twin)
            let has_pos = open_positions
                .iter()
                .any(|p| p.twin_key == twin.kalshi_ticker);
            if has_pos {
                scan_has_position += 1;
            }
            if !entry_signal {
                scan_no_signal += 1;
            }
            if !has_pos && entry_signal {
                if !entries_allowed_by_safe_mode(safe_mode_active) {
                    scan_safe_mode += 1;
                    warn!(
                        "ENTRY BLOCKED {} | safe mode active after failed/unconfirmed exit",
                        twin.coin
                    );
                    continue;
                }

                if open_positions.len() >= startup.max_open_positions {
                    info!(
                        "ENTRY BLOCKED {} | max open positions reached | current={} max={}",
                        twin.coin,
                        open_positions.len(),
                        startup.max_open_positions
                    );
                    continue;
                }

                if total_exposure(&open_positions) + position_size > startup.max_total_exposure_usdc
                {
                    info!(
                        "ENTRY BLOCKED {} | max total exposure | current={:.2} next={:.2} max={:.2}",
                        twin.coin,
                        total_exposure(&open_positions),
                        position_size,
                        startup.max_total_exposure_usdc
                    );
                    continue;
                }

                if elapsed_secs < entry_start_secs || elapsed_secs > entry_end_secs {
                    scan_outside_window += 1;
                    info!(
                        "⏳ ENTRY BLOCKED {} | outside entry window | elapsed={}s | allowed={}..{}s",
                        twin.coin, elapsed_secs, entry_start_secs, entry_end_secs
                    );
                    continue;
                }

                let buy_yes = signal_up;
                let side_label = if buy_yes { "UP" } else { "DOWN" };

                // Fetch potential entry prices
                let pm_ask = if buy_yes {
                    api::get_best_ask(&http_client, &twin.pm_market_id, &twin.pm_yes_token)
                        .await
                        .unwrap_or(0.0)
                } else {
                    api::get_best_ask(&http_client, &twin.pm_market_id, &twin.pm_no_token)
                        .await
                        .unwrap_or(0.0)
                };
                let ((ky_ask, kn_ask), (_ky_bid, _kn_bid)) = kalshi_client
                    .get_outcome_top_of_book(&twin.kalshi_ticker)
                    .await
                    .unwrap_or(((None, None), (None, None)));
                let km_ask = if buy_yes {
                    ky_ask.unwrap_or(0.0)
                } else {
                    kn_ask.unwrap_or(0.0)
                };

                let poly_has_funds = if paper_mode || pm_ask <= 0.0 {
                    true
                } else {
                    match poly_client.get_collateral_balance().await {
                        Ok(balance) if balance + 0.0001 >= position_size => true,
                        Ok(balance) => {
                            scan_funds_blocked += 1;
                            warn!(
                                "ENTRY VENUE SKIP {} | Polymarket collateral ${:.2} < size ${:.2}",
                                twin.coin, balance, position_size
                            );
                            false
                        }
                        Err(e) => {
                            scan_funds_blocked += 1;
                            warn!(
                                "ENTRY VENUE SKIP {} | Polymarket collateral check failed: {}",
                                twin.coin, e
                            );
                            false
                        }
                    }
                };

                // Pick the cheapest venue. If the cheapest venue cannot be used,
                // block the entry instead of paying up on the more expensive venue.
                let Some(selection) = select_entry_venue(pm_ask, km_ask, poly_has_funds) else {
                    if pm_ask > 0.0 && (km_ask <= 0.0 || pm_ask <= km_ask) {
                        info!(
                            "ENTRY BLOCKED {} | Polymarket is cheapest ({:.3} vs Kalshi {:.3}) but has no usable collateral",
                            twin.coin, pm_ask, km_ask
                        );
                    } else {
                        info!(
                            "ENTRY BLOCKED {} | no executable entry venue | Polymarket {:.3} Kalshi {:.3}",
                            twin.coin, pm_ask, km_ask
                        );
                    }
                    continue;
                };

                if market_burned.contains(&twin.kalshi_ticker) {
                    scan_burned_blocked += 1;
                    info!(
                        "ENTRY BLOCKED {} | market burned for this 15m window | ticker={}",
                        twin.coin, twin.kalshi_ticker
                    );
                } else if selection.ask <= 0.0 {
                    scan_invalid_ask += 1;
                    info!(
                        "ENTRY BLOCKED {} | invalid selected ask {:.3} | venue={} | Polymarket {:.3} Kalshi {:.3}",
                        twin.coin, selection.ask, selection.venue, pm_ask, km_ask
                    );
                } else if selection.ask <= startup.max_entry_price {
                    scan_attemptable += 1;
                    let chosen_venue_enum = if selection.platform == Platform::Polymarket {
                        Venue::Polymarket
                    } else {
                        Venue::Kalshi
                    };
                    if venue_exposure(&open_positions, &chosen_venue_enum) + position_size
                        > startup.max_venue_exposure_usdc
                    {
                        info!(
                            "ENTRY BLOCKED {} | max {:?} exposure | current={:.2} next={:.2} max={:.2}",
                            twin.coin,
                            chosen_venue_enum,
                            venue_exposure(&open_positions, &chosen_venue_enum),
                            position_size,
                            startup.max_venue_exposure_usdc
                        );
                        continue;
                    }
                    if !capital_manager.has_funds(&selection.platform, position_size) {
                        info!(
                            "ENTRY BLOCKED {} | insufficient {:?} funds | balance=${:.2} required=${:.2}",
                            twin.coin,
                            selection.platform,
                            capital_manager.balance(&selection.platform),
                            position_size
                        );
                        continue;
                    }

                    let res = if selection.platform == Platform::Polymarket {
                        execute_polymarket_entry(
                            &http_client,
                            &poly_client,
                            if buy_yes {
                                &twin.pm_yes_token
                            } else {
                                &twin.pm_no_token
                            },
                            position_size,
                            selection.ask,
                            paper_mode,
                        )
                        .await
                    } else {
                        execute_kalshi_entry(
                            &kalshi_client,
                            &twin.kalshi_ticker,
                            buy_yes,
                            position_size,
                            selection.ask,
                            paper_mode,
                        )
                        .await
                    };

                    match res {
                        Ok(fill) => {
                            capital_manager.deduct(&selection.platform, fill.notional_usdc);
                            open_positions.push(OpenPosition {
                                twin_key: twin.kalshi_ticker.clone(),
                                venue: if selection.platform == Platform::Polymarket {
                                    Venue::Polymarket
                                } else {
                                    Venue::Kalshi
                                },
                                coin: twin.coin.clone(),
                                pm_market_id: twin.pm_market_id.clone(),
                                pm_yes_token: twin.pm_yes_token.clone(),
                                pm_no_token: twin.pm_no_token.clone(),
                                kalshi_ticker: twin.kalshi_ticker.clone(),
                                buy_yes,
                                entry_price: fill.fill_price,
                                shares: fill.shares,
                                notional_usdc: fill.notional_usdc,
                                entry_order_id: fill.order_id,
                                last_exit_order_id: None,
                                last_error: None,
                                opened_at: Some(now_rfc3339()),
                                updated_at: Some(now_rfc3339()),
                                dca_executed: false,
                                is_hedge: false,
                                hedge_sl_price: None,
                                hedge_tp_price: None,
                                binance_entry_price: binance_price,
                                binance_retrace_threshold: calc_retrace_threshold(
                                    &twin.coin,
                                    signal_dist,
                                ),
                                state: PositionState::Open,
                                hedge_pair_id: None,
                            });
                            save_state(&open_positions);
                            if let Some(bot) = telegram_bot.as_ref() {
                                let msg = format_entry_message(
                                    &twin.coin,
                                    side_label,
                                    binance_open,
                                    binance_price,
                                    fill.fill_price,
                                    fill.notional_usdc,
                                    selection.venue,
                                    selection.ask,
                                    selection.other_venue,
                                    selection.other_ask,
                                );
                                let _ = bot.send_message(&msg).await;
                            }
                        }
                        Err(e) => {
                            warn!(
                                "ENTRY FAILED {} | venue={} ask={:.3} error={}",
                                twin.coin, selection.venue, selection.ask, e
                            );
                            if let Some(bot) = telegram_bot.as_ref() {
                                bot.send_message(&format!(
                                "*Entrada fallida*\nActivo: `{}`\nVenue: `{}`\nAsk: `{:.3}`\nError: `{}`",
                                twin.coin, selection.venue, selection.ask, e
                            ))
                            .await;
                            }
                        }
                    }
                } else {
                    scan_price_blocked += 1;
                    info!(
                        "💸 ENTRY BLOCKED {} | price {:.3} > max {:.3}",
                        twin.coin, selection.ask, startup.max_entry_price
                    );
                }
            }
        }
        info!(
            "SCAN SUMMARY | twins={} no_signal={} binance_unavailable={} has_position={} outside_window={} safe_mode={} price_blocked={} funds_blocked={} burned_blocked={} invalid_ask={} attemptable={} open_positions={}",
            scan_total,
            scan_no_signal,
            scan_binance_unavailable,
            scan_has_position,
            scan_outside_window,
            scan_safe_mode,
            scan_price_blocked,
            scan_funds_blocked,
            scan_burned_blocked,
            scan_invalid_ask,
            scan_attemptable,
            open_positions.len()
        );
        sleep(Duration::from_secs(5)).await;
    }
}
