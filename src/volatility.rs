use std::collections::VecDeque;

// ─────────────────────────────────────────────────────────────────────────────
// FastVolatility: works from the FIRST price tick, no warm-up needed.
// Tracks the last N prices and classifies volatility based on std dev %.
//
//  Low  (<= 1.5%):  mercado tranquilo
//  Mid  (<= 4.0%):  volatilidad normal
//  High (> 4.0%):   mercado muy volátil
// ─────────────────────────────────────────────────────────────────────────────
pub struct FastVolatility {
    window: usize,
    prices: VecDeque<f64>,
}

impl FastVolatility {
    pub fn new(window: usize) -> Self {
        Self { window, prices: VecDeque::with_capacity(window) }
    }

    pub fn update(&mut self, price: f64) -> (VolRegime, f64) {
        if self.prices.len() == self.window {
            self.prices.pop_front();
        }
        self.prices.push_back(price);

        let n = self.prices.len();
        if n < 3 {
            return (VolRegime::Mid, 0.0); // Default while warming up
        }

        let mean = self.prices.iter().sum::<f64>() / n as f64;
        let variance = self.prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / n as f64;
        let std_dev_pct = if mean > 0.0 { variance.sqrt() / mean * 100.0 } else { 0.0 };

        let regime = if std_dev_pct <= 0.015 {
            VolRegime::Low
        } else if std_dev_pct <= 0.05 {
            VolRegime::Mid
        } else {
            VolRegime::High
        };
        (regime, std_dev_pct)
    }

    pub fn std_dev_pct(&self) -> f64 {
        let n = self.prices.len();
        if n < 2 { return 0.0; }
        let mean = self.prices.iter().sum::<f64>() / n as f64;
        let variance = self.prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / n as f64;
        if mean > 0.0 { variance.sqrt() / mean * 100.0 } else { 0.0 }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Candle {
    pub high: f64,
    pub low: f64,
    pub close: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum VolRegime {
    Low,
    Mid,
    High,
}

/// ATR Wilder (RMA) 9 + Volatility Percentile (rolling 500)
pub struct AtrVolatility {
    atr_period: usize,      // 9
    vol_window: usize,      // 500
    // ATR state
    prev_close: Option<f64>,
    atr: Option<f64>,
    tr_sum: f64,
    tr_count: usize,
    // rolling ATR% window
    atrp_window: VecDeque<f64>,
}

impl AtrVolatility {
    /// atr_period = 9, vol_window = 200
    pub fn new() -> Self {
        let atr_period = 9;
        let vol_window = 200;
        Self {
            atr_period,
            vol_window,
            prev_close: None,
            atr: None,
            tr_sum: 0.0,
            tr_count: 0,
            atrp_window: VecDeque::with_capacity(vol_window),
        }
    }

    /// Update with a new candle and return (atr, atr_percent, percentile, regime)
    pub fn update(&mut self, c: Candle) -> Option<(f64, f64, f64, VolRegime)> {
        // Need prev_close to compute TR
        let prev_close = match self.prev_close {
            None => {
                self.prev_close = Some(c.close);
                return None;
            }
            Some(pc) => pc,
        };

        // True Range
        let tr1 = c.high - c.low;
        let tr2 = (c.high - prev_close).abs();
        let tr3 = (c.low - prev_close).abs();
        let tr = tr1.max(tr2).max(tr3);

        // ATR Wilder:
        // 1) First ATR after 9 TRs: simple average
        // 2) Then: ATR = (prev_atr*(n-1) + TR) / n
        let atr_val = if self.atr.is_none() {
            self.tr_sum += tr;
            self.tr_count += 1;

            if self.tr_count < self.atr_period {
                self.prev_close = Some(c.close);
                return None;
            }

            let first_atr = self.tr_sum / self.atr_period as f64;
            self.atr = Some(first_atr);
            first_atr
        } else {
            let prev_atr = self.atr.unwrap();
            let n = self.atr_period as f64;
            let new_atr = (prev_atr * (n - 1.0) + tr) / n;
            self.atr = Some(new_atr);
            new_atr
        };

        self.prev_close = Some(c.close);

        // ATR% (normalized)
        let atrp = if c.close != 0.0 { atr_val / c.close } else { 0.0 };

        // Push into rolling window of ATR%
        if self.atrp_window.len() == self.vol_window {
            self.atrp_window.pop_front();
        }
        self.atrp_window.push_back(atrp);

        // Need substantial data to define percentile (though we can start early)
        // User suggested 500, but for real-time start we might want to return early or use whatever we have.
        // Let's stick to user requirement of full window for best results.
        if self.atrp_window.len() < self.vol_window {
             // Optional: calculate percentile based on current window size if we want earlier results
             // But following user's "println! - necesita muchas más para llenar ATR y ventana 500"
             return None;
        }

        // Percentile: count of values <= current / N
        let n = self.atrp_window.len() as f64;
        let mut count_le = 0usize;
        for &v in self.atrp_window.iter() {
            if v <= atrp {
                count_le += 1;
            }
        }
        let percentile = count_le as f64 / n;

        // Regime thresholds
        let regime = if percentile < 0.20 {
            VolRegime::Low
        } else if percentile > 0.80 {
            VolRegime::High
        } else {
            VolRegime::Mid
        };

        Some((atr_val, atrp, percentile, regime))
    }
}

#[derive(Clone)]
pub struct Atr {
    pub period: usize,
    pub prev_close: Option<f64>,
    pub atr: Option<f64>,
    pub tr_sum: f64,
    pub tr_count: usize,
}

impl Atr {
    pub fn new(period: usize) -> Self {
        Self {
            period,
            prev_close: None,
            atr: None,
            tr_sum: 0.0,
            tr_count: 0,
        }
    }

    pub fn update(&mut self, c: Candle) -> Option<f64> {
        let prev_close = match self.prev_close {
            None => {
                self.prev_close = Some(c.close);
                return None;
            }
            Some(pc) => pc,
        };

        let tr1 = c.high - c.low;
        let tr2 = (c.high - prev_close).abs();
        let tr3 = (c.low - prev_close).abs();
        let tr = tr1.max(tr2).max(tr3);

        let atr_val = if self.atr.is_none() {
            self.tr_sum += tr;
            self.tr_count += 1;

            if self.tr_count < self.period {
                self.prev_close = Some(c.close);
                return None;
            }

            let first_atr = self.tr_sum / self.period as f64;
            self.atr = Some(first_atr);
            first_atr
        } else {
            let prev_atr = self.atr.unwrap();
            let n = self.period as f64;
            let new_atr = (prev_atr * (n - 1.0) + tr) / n;
            self.atr = Some(new_atr);
            new_atr
        };

        self.prev_close = Some(c.close);
        Some(atr_val)
    }
}

pub struct AtrRatioVolatility {
    pub atr15: Atr,
    pub atr200: Atr,
}

impl AtrRatioVolatility {
    pub fn new() -> Self {
        Self {
            atr15: Atr::new(15),
            atr200: Atr::new(200),
        }
    }

    pub fn update(&mut self, c: Candle) -> Option<(f64, f64, f64, VolRegime)> {
        let a15 = self.atr15.update(c);
        let a200 = self.atr200.update(c);

        if let (Some(val15), Some(val200)) = (a15, a200) {
            let ratio = if val200 > 0.0 { val15 / val200 } else { 0.0 };
            
            let regime = if ratio > 2.0 {
                VolRegime::High
            } else if ratio > 1.3 {
                VolRegime::Mid
            } else {
                VolRegime::Low
            };

            Some((val15, val200, ratio, regime))
        } else {
            None
        }
    }
}
