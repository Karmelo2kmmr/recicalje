use crate::csv_logger::CSVLogger;
use crate::equity_manager;
use crate::polymarket_api::PolymarketAPI;
use crate::telegram_reporter::TelegramReporter;

use log::{info, warn};

const MIN_TRIGGER_PRICE: f64 = 0.86;
const MAX_TRIGGER_PRICE: f64 = 0.91;
const HARD_SL_PRICE: f64 = 0.72;
const FULL_TP_PRICE: f64 = 0.98;
const SEARCH_WINDOW_START: u64 = 190;
const SEARCH_WINDOW_END: u64 = 286;
const MIN_PRICE_TO_BEAT_VARIATION: f64 = 22.0;

#[derive(Debug, PartialEq, Clone)]
pub enum StrategyState {
    Scanning,
    InPosition,
    Finished,
}

pub struct StrategyManager {
    pub state: StrategyState,
    pub current_token_id: String,
    pub reporter: TelegramReporter,
    pub api: PolymarketAPI,
    pub csv_logger: CSVLogger,
    pub side: String,
    pub strategy_name: String,
    pub market_id: String,
    pub strike_price: f64,
    pub entry_price: f64,
    pub initial_notional_usd: f64,
    pub shares_held: f64,
    pub equity_before: f64,
}

impl StrategyManager {
    pub fn new(
        market_id: String,
        token_id_main: String,
        reporter: TelegramReporter,
        api: PolymarketAPI,
        csv_logger: CSVLogger,
        strike_price: f64,
        equity: f64,
    ) -> Self {
        Self {
            state: StrategyState::Scanning,
            current_token_id: token_id_main,
            reporter,
            api,
            csv_logger,
            side: "UP".to_string(),
            strategy_name: "ALPHA-MOMENTUM-PURE".to_string(),
            market_id,
            strike_price,
            entry_price: 0.0,
            initial_notional_usd: 0.0,
            shares_held: 0.0,
            equity_before: equity,
        }
    }

    pub fn position_open(&self) -> bool {
        self.state == StrategyState::InPosition && self.shares_held > 0.0
    }

    pub async fn tick(
        &mut self,
        token_bid: f64,
        token_ask: f64,
        bucket_elapsed: u64,
        binance_momentum_up: bool,
        btc_price: f64,
    ) {
        match self.state {
            StrategyState::Scanning => {
                if bucket_elapsed < SEARCH_WINDOW_START || bucket_elapsed > SEARCH_WINDOW_END {
                    return;
                }

                let btc_variation_vs_strike = btc_price - self.strike_price;
                let in_entry_range =
                    token_ask >= MIN_TRIGGER_PRICE && token_ask <= MAX_TRIGGER_PRICE;

                if !in_entry_range || !binance_momentum_up {
                    return;
                }

                if btc_variation_vs_strike.abs() < MIN_PRICE_TO_BEAT_VARIATION {
                    return;
                }

                let equity_now = equity_manager::compute_equity();
                let stake_usd = equity_manager::calculate_alpha_momentum_stake(equity_now);
                if stake_usd <= 0.0 {
                    return;
                }

                info!(
                    "ALPHA entry | ask {:.3} | price_to_beat {:.2} | btc {:.2} | delta {:.2} | stake ${:.2}",
                    token_ask, self.strike_price, btc_price, btc_variation_vs_strike, stake_usd
                );

                if self
                    .api
                    .place_order(&self.current_token_id, token_ask, stake_usd, "BUY")
                    .await
                {
                    self.entry_price = token_ask;
                    self.initial_notional_usd = stake_usd;
                    self.shares_held = stake_usd / token_ask.max(0.0001);
                    self.equity_before = equity_now;
                    self.state = StrategyState::InPosition;

                    self.reporter
                        .notify_entry(
                            "BTC-5M",
                            &self.side,
                            token_ask,
                            stake_usd,
                            self.strike_price,
                            btc_variation_vs_strike,
                        )
                        .await;
                }
            }
            StrategyState::InPosition => {
                self.check_position(token_bid).await;
            }
            StrategyState::Finished => {}
        }
    }

    async fn check_position(&mut self, token_bid: f64) {
        if self.shares_held <= 0.0 {
            self.state = StrategyState::Finished;
            return;
        }

        if token_bid <= HARD_SL_PRICE {
            warn!("HARD SL ACTIVADO @ {:.3}", token_bid);
            self.exit_all(token_bid, "HARD-SL-0.72").await;
            return;
        }

        if token_bid >= FULL_TP_PRICE {
            info!("ALPHA full TP hit @ {:.3}", token_bid);
            self.exit_all(token_bid, "TP-0.98").await;
        }
    }

    async fn exit_all(&mut self, price: f64, reason: &str) {
        if self.shares_held <= 0.0 {
            self.state = StrategyState::Finished;
            return;
        }

        let shares_to_sell = self.shares_held;
        let notional = shares_to_sell * price;
        if self
            .api
            .place_order(&self.current_token_id, price, notional, "SELL")
            .await
        {
            let total_pnl = (price - self.entry_price) * shares_to_sell;
            let ret_pct = if self.entry_price > 0.0 {
                ((price - self.entry_price) / self.entry_price) * 100.0
            } else {
                0.0
            };
            let equity_after = self.equity_before + total_pnl;
            let status = if total_pnl >= 0.0 {
                "CLOSED-WIN"
            } else {
                "CLOSED-LOSS"
            };

            self.reporter
                .notify_exit(
                    "BTC-5M",
                    reason,
                    self.entry_price,
                    price,
                    total_pnl,
                    ret_pct,
                    self.initial_notional_usd,
                )
                .await;

            self.csv_logger.log_trade(
                "BTC",
                &self.side,
                self.entry_price,
                price,
                "NO",
                status,
                total_pnl,
                ret_pct,
                &self.strategy_name,
                1,
                &self.market_id,
                self.equity_before,
                self.initial_notional_usd,
                equity_after,
                "ALPHA",
            );

            self.reset_position();
        }
    }

    fn reset_position(&mut self) {
        self.entry_price = 0.0;
        self.initial_notional_usd = 0.0;
        self.shares_held = 0.0;
        self.state = StrategyState::Finished;
    }

    pub async fn force_close_on_expiration(&mut self, btc_price: f64) {
        if !self.position_open() {
            return;
        }

        info!(
            "Expirando mercado ALPHA | BTC: {} | Strike: {}",
            btc_price, self.strike_price
        );

        let settlement_price = if btc_price > self.strike_price {
            1.0
        } else {
            0.0
        };
        self.exit_all(settlement_price, "EXPIRATION").await;
    }

    pub async fn close_position(&mut self, price: f64, reason: &str, _strat_name: &str) {
        self.exit_all(price, reason).await;
    }
}
