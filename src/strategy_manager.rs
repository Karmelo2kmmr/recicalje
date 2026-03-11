use crate::telegram_reporter::TelegramReporter;
use crate::polymarket_api::PolymarketAPI;
use crate::csv_logger::CSVLogger;
use crate::volatility::VolRegime;
use crate::equity_manager;

use log::{info, warn};
use std::time::Instant;
use chrono::Timelike;

// ─────────────────────────────────────────────────────────────────────────────
// Estrategia DCA + Reciclaje
// ─────────────────────────────────────────────────────────────────────────────

/// Niveles de compra base de la estrategia (se sobrescriben dinámicamente tras el peak-pullback)
const DCA_PRICES: [f64; 6]    = [0.83, 0.79, 0.75, 0.71, 0.67, 0.63];
/// Porcentaje del stake por nivel (no porcentaje del capital, sino fracción relativa)
/// Estos se usan como multiplicadores del stake base para cada compra
const DCA_STAKES: [f64; 6]    = [1.0,  1.0,  1.0,  1.5,  1.75, 2.5];
/// Cuáles niveles pueden reciclar: L3 (índice 2), L4 (índice 3), L5 (índice 4)
const DCA_CAN_RECYCLE: [bool; 6] = [false, false, true, true, true, false];
/// Ganancia absoluta que activa la venta de reciclaje (+0.06 centavos sobre precio de entrada)
const RECYCLE_PROFIT: f64 = 0.06;
/// Máximo de ciclos de reciclaje permitidos por nivel
const RECYCLE_LIMIT: u32 = 3;
/// Límite inferior del rango de reciclaje
const RECYCLE_RANGE_LO: f64 = 0.68;
/// Límite superior del rango de reciclaje
const RECYCLE_RANGE_HI: f64 = 0.95;
/// Stop Loss global estricto (todas las posiciones)
const GLOBAL_SL: f64 = 0.67; // Usado solo como fallback
/// Take Profit global
const GLOBAL_TP: f64 = 0.98;
/// Take Profit especial para compra 5 (índice 4)
const BUY5_TP: f64 = 0.85;
/// Ventana horaria de trading (hora UTC): 14:00 → 22:00
// Ventana horaria de trading (configurada vía .env)
fn get_trading_hours() -> (u32, u32) {
    let start = std::env::var("TRADING_HOUR_START").unwrap_or_else(|_| "14".to_string()).parse().unwrap_or(14);
    let end = std::env::var("TRADING_HOUR_END").unwrap_or_else(|_| "22".to_string()).parse().unwrap_or(22);
    (start, end)
}

// ─────────────────────────────────────────────────────────────────────────────

/// Representa el estado de un nivel de DCA individual
#[derive(Debug, Clone)]
pub struct DcaLevel {
    /// Precio objetivo de entrada
    pub price: f64,
    /// Multiplicador de stake respecto al stake base
    pub stake_mult: f64,
    /// ¿Puede reciclar?
    pub can_recycle: bool,
    /// ¿Está activa esta posición (tokens comprados)?
    pub is_active: bool,
    /// Cantidad exacta de tokens/USDC invertidos en este nivel
    pub amount: f64,
    /// ¿Esta posición fue vaciada por reciclaje y espera re-compra?
    pub awaiting_rebuy: bool,
    /// Número de veces que se recicló
    pub recycle_count: u32,
    /// Precio objetivo de venta por reciclaje (+6%)
    pub recycle_sell_target: f64,
    /// Monto fijo en USD para este nivel
    pub fixed_stake: f64,
}

impl DcaLevel {
    pub fn new(price: f64, stake_mult: f64, can_recycle: bool, fixed_stake: f64) -> Self {
        let recycle_sell_target = if can_recycle { price + RECYCLE_PROFIT } else { 0.0 };
        Self {
            price,
            stake_mult,
            can_recycle,
            is_active: false,
            amount: 0.0,
            awaiting_rebuy: false,
            recycle_count: 0,
            recycle_sell_target,
            fixed_stake,
        }
    }

    pub fn reset(&mut self) {
        self.is_active = false;
        self.amount = 0.0;
        self.awaiting_rebuy = false;
        self.recycle_count = 0;
    }
}

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Clone)]
pub enum StrategyState {
    Scanning,
    InPosition,  // Al menos una posición abierta
    Finished,
}

pub struct StrategyManager {
    pub state: StrategyState,

    // Niveles DCA
    pub levels: Vec<DcaLevel>,

    // Identificadores
    pub token_id_main: String,
    pub token_id_recovery: String,
    pub current_token_id: String,

    // Posicion Global
    pub average_price: f64,
    pub total_amount: f64,
    pub dca_count: u32,
    pub initial_entry_price: f64,

    // Equity / stake
    pub base_stake: f64,
    pub equity_before: f64,

    // Infraestructura
    pub reporter: TelegramReporter,
    pub api: PolymarketAPI,
    pub csv_logger: CSVLogger,

    pub side: String,
    pub main_side: String,
    pub strategy_name: String,
    pub market_id: String,
    pub strike_price: f64,

    // Volatilidad
    pub current_regime: VolRegime,
    pub recent_asks: Vec<f64>,

    // Timing
    pub entry_time: Option<Instant>,

    // Peak-Pullback Logic
    pub max_observed_price: f64,
    pub peak_detected: bool,
}

impl StrategyManager {
    pub fn new(
        market_id: String,
        token_id_main: String,
        token_id_recovery: String,
        reporter: TelegramReporter,
        api: PolymarketAPI,
        csv_logger: CSVLogger,
        side: String,
        strike_price: f64,
        equity: f64,
    ) -> Self {
        let stakes = equity_manager::calculate_dca_stakes(equity);
        
        // Construir los 6 niveles de DCA
        let levels: Vec<DcaLevel> = (0..6)
            .map(|i| DcaLevel::new(DCA_PRICES[i], DCA_STAKES[i], DCA_CAN_RECYCLE[i], stakes[i]))
            .collect();

        Self {
            state: StrategyState::Scanning,
            levels,
            current_token_id: token_id_main.clone(),
            token_id_main,
            token_id_recovery,
            average_price: 0.0,
            total_amount: 0.0,
            dca_count: 0,
            initial_entry_price: 0.0,
            base_stake: stakes[0], 
            equity_before: equity,
            reporter,
            api,
            csv_logger,
            side: side.clone(),
            main_side: side,
            strategy_name: "DCA-Reciclaje".to_string(),
            market_id,
            strike_price,
            current_regime: VolRegime::Mid,
            recent_asks: Vec::new(),
            entry_time: None,
            max_observed_price: 0.0,
            peak_detected: false,
        }
    }

    /// Recalcula el precio promedio global de todas las posiciones activas
    fn recalculate_average(&mut self) {
        let total_cost: f64 = self.levels.iter()
            .filter(|l| l.is_active)
            .map(|l| l.price * l.amount)
            .sum();
        let total_tokens: f64 = self.levels.iter()
            .filter(|l| l.is_active)
            .map(|l| l.amount / l.price) // aproximación tokens por nivel
            .sum();
        self.total_amount = self.levels.iter()
            .filter(|l| l.is_active)
            .map(|l| l.amount)
            .sum();
        if total_tokens > 0.0 {
            self.average_price = total_cost / self.total_amount;
        }
    }

    pub async fn tick(
        &mut self,
        token_bid: f64,
        token_ask: f64,
        vol_regime: VolRegime,
        vol_pct: f64,
        bucket_elapsed: u64,
        _is_fulas_paused: bool,
    ) {
        // Actualizar histórico de precios
        self.recent_asks.push(token_ask);
        if self.recent_asks.len() > 10 {
            self.recent_asks.remove(0);
        }
        self.current_regime = vol_regime;
        let now = chrono::Utc::now();
        let is_recovery_cutoff = bucket_elapsed > 210;

        match self.state {
            // ─────────────────────────────────────────────────────────────
            // SCANNING: Buscando pico en 0.91-0.95 y pullback de 0.03
            // ─────────────────────────────────────────────────────────────
            StrategyState::Scanning => {
                let (start_hour, end_hour) = get_trading_hours();
                let is_trading_window = now.hour() >= start_hour && now.hour() < end_hour;
                if !is_trading_window {
                    if now.second() % 30 == 0 {
                        info!("🔍 DCA: Outside trading window ({}–{} UTC). Currently: {} UTC. Waiting...", start_hour, end_hour, now.hour());
                    }
                    return;
                }

                // --- NEW FILTERS ---
                // 1. Volatility Filter: No Low
                if vol_regime == VolRegime::Low {
                    if now.second() % 30 == 0 {
                        info!("⏳ DCA: Skipping scan - Low Volatility detected.");
                    }
                    return;
                }

                // 2. Search Window Filter: [166s, 269s] (2:46 to 4:29)
                if bucket_elapsed < 166 || bucket_elapsed > 269 {
                    if now.second() % 30 == 0 {
                        info!("⏳ DCA: Outside search window ({}s). Window: [166s - 269s] (2:46 - 4:29)", bucket_elapsed);
                    }
                    return;
                }
                // -------------------

                if token_ask >= 0.90 {
                    self.peak_detected = true;
                    if token_ask > self.max_observed_price {
                        self.max_observed_price = token_ask;
                        info!("🏔️ DCA: Peak detected at {:.3} (Range: >= 0.90). Tracking pullback...", self.max_observed_price);
                    }
                }

                if self.peak_detected {
                    let trigger_price = self.max_observed_price - 0.02;
                    if token_ask <= trigger_price {
                        info!("🚀 TP/Pullback Triggered! Peak: {:.3} -> Target: {:.3} | Current: {:.3}", self.max_observed_price, trigger_price, token_ask);
                        let stake = self.base_stake;
                        let equity_now = equity_manager::compute_equity();

                        info!(
                            "🎯 DCA Compra 1: Entrando en {:.3} (Pico: {:.3}, Trigger: {:.3}) | Stake: ${:.2} | Régimen: {:?}",
                            token_ask, self.max_observed_price, trigger_price, stake, self.current_regime
                        );

                        if self.api.place_order(&self.current_token_id, token_ask, stake, "BUY").await {
                            // Actualizar los niveles de DCA dinámicamente según el precio de entrada real
                            self.levels[0].price = token_ask;
                            self.levels[1].price = token_ask - 0.03;
                            self.levels[2].price = token_ask - 0.06;
                            self.levels[3].price = token_ask - 0.09;
                            self.levels[4].price = token_ask - 0.12;
                            self.levels[5].price = token_ask - 0.15;

                            for i in 0..6 {
                                if self.levels[i].can_recycle {
                                    self.levels[i].recycle_sell_target = self.levels[i].price + RECYCLE_PROFIT;
                                } else {
                                    self.levels[i].recycle_sell_target = 0.0;
                                }
                            }

                            self.levels[0].is_active = true;
                            self.levels[0].amount = stake;
                            self.initial_entry_price = token_ask;
                            self.equity_before = equity_now;
                            self.dca_count = 1;
                            self.entry_time = Some(Instant::now());
                            self.recalculate_average();
                            self.state = StrategyState::InPosition;

                            self.reporter.send_message(&format!(
                                "🎯 *DCA+RECICLAJE — Compra 1*\n\
                                • Pico detectado: *{:.3}*\n\
                                • Entrada: *{:.3}* (Lado: *{}*)\n\
                                • Stake: *${:.2}*\n\
                                • TP: {:.2} | SL estricto: {:.2}\n\
                                • Régimen: {:?} | σ: {:.2}%",
                                self.max_observed_price, token_ask, self.side, stake, GLOBAL_TP, self.levels[5].price - 0.06, self.current_regime, vol_pct
                            )).await;
                        }
                    }
                }
            }

            // ─────────────────────────────────────────────────────────────
            // IN POSITION: Gestión de DCA, Reciclaje, TP y SL
            // ─────────────────────────────────────────────────────────────
            StrategyState::InPosition => {

                // ── 1. STOP LOSS GLOBAL ESTRICTO ─────────────────────────
                let dynamic_sl = self.levels[5].price - 0.06;
                if token_bid <= dynamic_sl {
                    warn!("🚨 STOP LOSS GLOBAL en {} ≤ {:.2} → Cerrando TODAS las posiciones!", token_bid, dynamic_sl);
                    self.reporter.send_message(&format!(
                        "🚨 *STOP LOSS ESTRICTO*\n\
                        • Precio: *{:.3}*\n\
                        • SL: {:.2}\n\
                        • Cerrando TODAS las posiciones activas.",
                        token_bid, dynamic_sl
                    )).await;
                    self.close_all_positions(token_bid, "SL-GLOBAL").await;
                    self.state = StrategyState::Finished;
                    return;
                }

                // ── 2. TAKE PROFIT GLOBAL ───────────────────────
                if token_bid >= GLOBAL_TP {
                    info!("✅ TAKE PROFIT GLOBAL en {} ≥ {:.2} → Cerrando TODAS las posiciones!", token_bid, GLOBAL_TP);
                    self.reporter.send_message(&format!(
                        "🎉 *TAKE PROFIT GLOBAL*\n\
                        • Precio: *{:.3}*\n\
                        • TP: {:.2}\n\
                        • Cerrando TODAS las posiciones activas.",
                        token_bid, GLOBAL_TP
                    )).await;
                    self.close_all_positions(token_bid, "TP-GLOBAL").await;
                    self.state = StrategyState::Finished;
                    return;
                }

                // ── 3. TP ESPECIAL COMPRA 5 (índice 4) — vender >0.85 ──
                if self.levels[4].is_active && token_bid >= BUY5_TP {
                    info!("✅ TP Compra 5 en {} ≥ 0.85 → Cerrando nivel 5.", token_bid);
                    let amount5 = self.levels[4].amount;
                    if self.api.place_order(&self.current_token_id, token_bid, amount5, "SELL").await {
                        let pnl = (token_bid - self.levels[4].price) * (amount5 / self.levels[4].price);
                        self.reporter.send_message(&format!(
                            "💰 *TP Compra 5 (0.85)*\n\
                            • Precio venta: *{:.3}*\n\
                            • Compra original: 0.67\n\
                            • P&L aprox: *${:.2}*",
                            token_bid, pnl
                        )).await;
                        self.levels[4].is_active = false;
                        self.levels[4].amount = 0.0;
                        self.recalculate_average();
                        // Si ya no queda ninguna posición activa → Finished
                        if self.total_amount == 0.0 {
                            self.state = StrategyState::Finished;
                            return;
                        }
                    }
                }

                // ── 4. LÓGICA DE RECICLAJE (niveles L3, L4, L5) ──
                // Solo dentro del rango de reciclaje
                if token_bid >= RECYCLE_RANGE_LO && token_ask <= RECYCLE_RANGE_HI {
                    for i in 2..=4usize {
                        let lvl = &self.levels[i];
                        if !lvl.can_recycle { continue; }

                        // a) Venta de reciclaje: si está activo y precio Bid supera target, y no supera límite
                        if lvl.is_active && !lvl.awaiting_rebuy && token_bid >= lvl.recycle_sell_target && lvl.recycle_count < RECYCLE_LIMIT {
                            let sell_amount = self.levels[i].amount;
                            let sell_price = token_bid;
                            let entry = self.levels[i].price;
                            let profit_cents = sell_price - entry;
                            info!(
                                "♻️ RECICLAJE Venta nivel {} en {:.3} (+{:.3} centavos sobre {:.3})",
                                i + 1, sell_price, profit_cents, entry
                            );
                            if self.api.place_order(&self.current_token_id, sell_price, sell_amount, "SELL").await {
                                let pnl = profit_cents * (sell_amount / entry);
                                self.reporter.send_message(&format!(
                                    "♻️ *RECICLAJE — Venta Nivel {}*\n\
                                    • Precio venta: *{:.3}*\n\
                                    • Entrada original: {:.3}\n\
                                    • Ganancia: *+{:.3}* centavos | P&L: *${:.2}*\n\
                                    • Ciclo # {}/{}\n\
                                    • Esperando re-compra en {:.3}...",
                                    i + 1, sell_price, entry, profit_cents, pnl, lvl.recycle_count + 1, RECYCLE_LIMIT, entry
                                )).await;
                                self.levels[i].is_active = false;
                                self.levels[i].awaiting_rebuy = true;
                                self.levels[i].recycle_count += 1;
                                self.levels[i].amount = 0.0;
                                self.recalculate_average();
                            }
                            continue;
                        }

                        // b) Re-compra: si está esperando (awaiting_rebuy) y el precio Ask volvió al nivel
                        if lvl.awaiting_rebuy && token_ask <= lvl.price + 0.01 && token_ask >= lvl.price - 0.01 {
                            if is_recovery_cutoff {
                                info!("⏭️ Re-compra nivel {} cancelada — cutoff de tiempo.", i + 1);
                                self.levels[i].awaiting_rebuy = false;
                                continue;
                            }
                            let rebuy_stake = self.levels[i].fixed_stake;
                            info!("♻️ RECICLAJE Re-compra nivel {} en {:.3}", i + 1, token_ask);
                            if self.api.place_order(&self.current_token_id, token_ask, rebuy_stake, "BUY").await {
                                self.reporter.send_message(&format!(
                                    "♻️ *RECICLAJE — Re-compra Nivel {}*\n\
                                    • Precio re-entrada: *{:.3}*\n\
                                    • Stake: *${:.2}*\n\
                                    • Inicia ciclo actual: {}/{}",
                                    i + 1, token_ask, rebuy_stake, self.levels[i].recycle_count, RECYCLE_LIMIT
                                )).await;
                                self.levels[i].is_active = true;
                                self.levels[i].awaiting_rebuy = false;
                                self.levels[i].amount = rebuy_stake;
                                self.recalculate_average();
                            }
                        }
                    }
                }

                // ── 5. ACTIVACIÓN DE NUEVOS NIVELES DCA (compras 2-6) ──
                if !is_recovery_cutoff {
                    for i in 1..6usize {
                        let lvl = &self.levels[i];
                        // Solo activar si no está activa Y no está esperando re-compra
                        if lvl.is_active || lvl.awaiting_rebuy { continue; }
                        // Asegurarnos que la compra anterior ya fue ejecutada (DCA en cascada)
                        if !self.levels[i - 1].is_active && !self.levels[i - 1].awaiting_rebuy { continue; }

                        // ── FLASH CRASH CAPTURE ──────────────────────────────────────────
                        // Si el precio cayó POR DEBAJO del nivel (no solo en la ventana exacta),
                        // compramos igual: obtenemos mejor precio promedio.
                        // El SL ya fue chequeado arriba, así que si llegamos aquí es seguro comprar.
                        if token_ask <= lvl.price {
                            let stake = lvl.fixed_stake;
                            let equity_now = equity_manager::compute_equity();
                            info!(
                                "📊 DCA Compra {}: Entrando en {:.3} | Stake: ${:.2}",
                                i + 1, token_ask, stake
                            );
                            if self.api.place_order(&self.current_token_id, token_ask, stake, "BUY").await {
                                let lvl_price = self.levels[i].price;
                                let can_rec = self.levels[i].can_recycle;
                                let rec_target = self.levels[i].recycle_sell_target;

                                self.levels[i].is_active = true;
                                self.levels[i].amount = stake;
                                if self.equity_before == 0.0 {
                                    self.equity_before = equity_now;
                                }
                                self.dca_count += 1;
                                self.recalculate_average();

                                let recycle_info = if can_rec {
                                    format!("| ♻️ Reciclaje en {:.3}", rec_target)
                                } else {
                                    let tp_lvl = if i == 4 { "0.85" } else { "0.97" };
                                    format!("| TP: {}", tp_lvl)
                                };

                                self.reporter.send_message(&format!(
                                    "📊 *DCA+RECICLAJE — Compra {}*\n\
                                    • Entrada: *{:.3}* (Lado: *{}*) {}\n\
                                    • Stake: *${:.2}*\n\
                                    • Avg. precio global: *{:.3}*\n\
                                    • SL Estricto: {:.2}",
                                    i + 1, lvl_price, self.side, recycle_info, stake, self.average_price, self.levels[5].price - 0.06
                                )).await;
                            }
                        }
                    }
                }

                // ── 6. SL GARANTIZADO: re-chequeo post-DCA ─────────────────────────────
                // Si compramos nuevos niveles y el precio SIGUE bajo la línea de SL,
                // el SL debe disparar de todas formas. Esto cubre flash crashes.
                let dynamic_sl_safety = self.levels[5].price - 0.06;
                if token_bid <= dynamic_sl_safety && self.total_amount > 0.0 {
                    warn!("🚨 [SL-SAFETY] {:.3} <= {:.2} post-DCA — cerrando todo.", token_bid, dynamic_sl_safety);
                    self.reporter.send_message(&format!(
                        "🚨 *SL GARANTIZADO (post-crash)*\n\
                        • Precio: *{:.3}* | SL: {:.2}\n\
                        • Cerrando todas las posiciones activas.",
                        token_bid, dynamic_sl_safety
                    )).await;
                    self.close_all_positions(token_bid, "SL-SAFETY").await;
                    self.state = StrategyState::Finished;
                }
            }

            StrategyState::Finished => {}
        }
    }

    /// Cierra TODAS las posiciones activas con un precio y razón determinados
    async fn close_all_positions(&mut self, price: f64, reason: &str) {
        let total_to_close: f64 = self.levels.iter().filter(|l| l.is_active).map(|l| l.amount).sum();
        if total_to_close == 0.0 {
            info!("⚠️ [close_all] Sin posiciones activas. Razón: {}", reason);
            return;
        }

        info!("🏁 Cerrando TODAS las posiciones: {} @ {:.3} | Total: ${:.2}", reason, price, total_to_close);

        if self.api.place_order(&self.current_token_id, price, total_to_close, "SELL").await {
            let pnl = (price - self.average_price) * (total_to_close / self.average_price.max(0.001));
            let ret_pct = if self.average_price > 0.0 {
                ((price - self.average_price) / self.average_price) * 100.0
            } else {
                0.0
            };
            let status = if reason.contains("TP") || price >= 0.9 { "✅ CLOSED-WIN" } else { "❌ CLOSED-LOSS" };
            let equity_after = self.equity_before + pnl;

            self.reporter.notify_exit("BTC-5M-DCA", reason, price, pnl).await;

            self.csv_logger.log_trade(
                "BTC",
                &self.side,
                self.initial_entry_price,
                price,
                "NO",
                status,
                pnl,
                ret_pct,
                &self.strategy_name,
                self.dca_count,
                &self.market_id,
                self.equity_before,
                total_to_close,
                equity_after,
                &format!("{:?}", self.current_regime),
            );

            // Reset todos los niveles
            for lvl in self.levels.iter_mut() {
                lvl.reset();
            }
            self.total_amount = 0.0;
            self.average_price = 0.0;
            self.dca_count = 0;
        }
    }

    #[allow(dead_code)]
    pub fn reset_for_scanning(&mut self) {
        self.state = StrategyState::Scanning;
        self.total_amount = 0.0;
        self.average_price = 0.0;
        self.dca_count = 0;
        self.current_token_id = self.token_id_main.clone();
        self.side = self.main_side.clone();
        self.max_observed_price = 0.0;
        self.peak_detected = false;
        for lvl in self.levels.iter_mut() {
            lvl.reset();
        }
    }

    pub async fn force_close_on_expiration(&mut self, btc_price: f64) {
        if self.state != StrategyState::InPosition { return; }
        let total: f64 = self.levels.iter().filter(|l| l.is_active).map(|l| l.amount).sum();
        if total == 0.0 { return; }

        info!("⏳ Expirando mercado — Cerrando posiciones. BTC: {} | Strike: {}", btc_price, self.strike_price);

        let settlement_price = if self.side == "UP" {
            if btc_price > self.strike_price { 1.0 } else { 0.0 }
        } else {
            if btc_price < self.strike_price { 1.0 } else { 0.0 }
        };

        info!("🏁 Settlement Price estimado: {}", settlement_price);
        self.close_all_positions(settlement_price, "EXPIRATION").await;
        self.state = StrategyState::Finished;
    }

    pub async fn close_position(&mut self, price: f64, reason: &str, _strat_name: &str) {
        self.close_all_positions(price, reason).await;
    }

    #[allow(dead_code)]
    pub fn has_minimal_rejection(&self) -> bool {
        if self.recent_asks.len() < 3 { return false; }
        let min_recent = self.recent_asks.iter().cloned().fold(f64::INFINITY, f64::min);
        if let Some(&current) = self.recent_asks.last() {
            return current >= min_recent + 0.007;
        }
        false
    }
}
