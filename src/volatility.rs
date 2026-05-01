use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use log::{error, info, warn};
use reqwest::{Client, RequestBuilder, Response};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;

pub async fn fetch_binance_candle_open(
    client: &Client,
    symbol: &str,
    start_mins: i32,
) -> Result<f64, Box<dyn std::error::Error>> {
    let now = Utc::now();
    let et_now = now.with_timezone(&New_York);

    // Calculate the target DateTime for the window start today
    let target_et = New_York
        .with_ymd_and_hms(
            et_now.year(),
            et_now.month(),
            et_now.day(),
            (start_mins / 60) as u32,
            (start_mins % 60) as u32,
            0,
        )
        .single()
        .ok_or("Failed to calculate ET timestamp")?;

    // If target is in the future, it might be from yesterday (e.g. market for early morning started late night)
    // But for 15m crypto, usually it's current.
    let target_utc = target_et.with_timezone(&Utc);
    let timestamp = target_utc.timestamp_millis();

    let url = "https://api.binance.com/api/v3/klines";
    let params = [
        ("symbol", symbol),
        ("interval", "1m"),
        ("startTime", &timestamp.to_string()),
        ("limit", "1"),
    ];

    let builder = client.get(url).query(&params);
    let resp = send_with_retry(builder).await?;

    if !resp.status().is_success() {
        return Err(format!("Binance API error: {}", resp.status()).into());
    }

    let klines: Vec<Kline> = resp.json().await?;
    if let Some(k) = klines.first() {
        let open_price = k.1.parse::<f64>()?;
        return Ok(open_price);
    }

    Err("No klines found for the specified timestamp".into())
}

#[derive(Deserialize, Debug)]
pub struct Kline(
    #[allow(dead_code)] u64,
    #[allow(dead_code)] String,
    String,
    String,
    String,
    #[allow(dead_code)] String,
    #[allow(dead_code)] u64,
    #[allow(dead_code)] String,
    #[allow(dead_code)] u64,
    #[allow(dead_code)] String,
    #[allow(dead_code)] String,
    #[allow(dead_code)] String,
);

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

async fn send_with_retry(builder: RequestBuilder) -> Result<Response, Box<dyn std::error::Error>> {
    let mut last_error = None;
    let mut delay = Duration::from_millis(500);

    for i in 0..3 {
        match builder
            .try_clone()
            .ok_or("Cannot clone request")?
            .send()
            .await
        {
            Ok(resp) => return Ok(resp),
            Err(e) => {
                warn!(
                    "Binance attempt {} failed: {}. Retrying in {:?}...",
                    i + 1,
                    e,
                    delay
                );
                last_error = Some(e);
                sleep(delay).await;
                delay *= 2;
            }
        }
    }

    Err(last_error.unwrap().into())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatilityState {
    LowNeutral,
    NeutralHigh,
    HighSuperhigh,
}

#[derive(Debug, Clone, Copy)]
pub struct VolatilityMetrics {
    pub vol_now: f64,
    pub vol_ma20: f64,
    pub state: VolatilityState,
    pub trigger_price: f64,
    pub z_score: f64,
    pub atr9: f64,
    pub atr200: f64,
    pub current_price: f64,
    pub last_price: f64,
}

impl Default for VolatilityMetrics {
    fn default() -> Self {
        let base_trigger = env_f64("TRIGGER_PRICE", 0.885);
        Self {
            vol_now: 0.0,
            vol_ma20: 0.0,
            state: VolatilityState::NeutralHigh,
            trigger_price: base_trigger,
            z_score: 0.0,
            atr9: 0.0,
            atr200: 0.0,
            current_price: 0.0,
            last_price: 0.0,
        }
    }
}

pub async fn get_volatility_metrics(client: &Client, symbol: &str) -> VolatilityMetrics {
    let url = "https://api.binance.com/api/v3/klines";
    let params = [("symbol", symbol), ("interval", "1m"), ("limit", "200")];

    let builder = client.get(url).query(&params);
    match send_with_retry(builder).await {
        Ok(resp) => {
            if resp.status().is_success() {
                if let Ok(klines) = resp.json::<Vec<Kline>>().await {
                    return build_metrics(symbol, &klines);
                }
            }
        }
        Err(e) => warn!(
            "Failed to fetch volatility from Binance for {}: {}. Using default.",
            symbol, e
        ),
    }

    VolatilityMetrics::default()
}

pub async fn get_dynamic_trigger(client: &Client, symbol: &str) -> (f64, VolatilityState) {
    let metrics = get_volatility_metrics(client, symbol).await;
    (metrics.trigger_price, metrics.state)
}

fn build_metrics(symbol: &str, klines: &[Kline]) -> VolatilityMetrics {
    if klines.is_empty() {
        warn!(
            "No klines returned from Binance for {}. Using default metrics.",
            symbol
        );
        return VolatilityMetrics::default();
    }

    let mut closes = Vec::with_capacity(klines.len());
    let mut trs = Vec::with_capacity(klines.len());
    let mut volatilities = Vec::with_capacity(klines.len());

    for (i, k) in klines.iter().enumerate() {
        let high = k.2.parse::<f64>().unwrap_or(0.0);
        let low = k.3.parse::<f64>().unwrap_or(high);
        let close = k.4.parse::<f64>().unwrap_or(high);
        let prev_close = if i > 0 {
            closes.last().copied().unwrap_or(close)
        } else {
            close
        };

        let tr = (high - low)
            .max((high - prev_close).abs())
            .max((low - prev_close).abs());
        let candle_vol = if low > 0.0 {
            (high - low) / low * 100.0
        } else {
            0.0
        };

        closes.push(close);
        trs.push(tr);
        volatilities.push(candle_vol);
    }

    let recent_window = env_usize("VOL_RECENT_WINDOW", 20)
        .clamp(5, volatilities.len().max(5))
        .min(volatilities.len());

    let recent_slice = &volatilities[volatilities.len() - recent_window..];
    let vol_now = average(recent_slice);
    let baseline = average(&volatilities);
    let std_dev = std_dev(&volatilities, baseline);
    let z_score = if std_dev > 0.0 {
        (vol_now - baseline) / std_dev
    } else {
        0.0
    };

    let ratio = if baseline > 0.0 {
        vol_now / baseline
    } else {
        1.0
    };

    let atr9 = average_last(&trs, 9);
    let atr200 = average(&trs);
    let (trigger_price, state) = calculate_trigger_from_vol(ratio, z_score);

    info!(
        "1M VOLATILITY ({}) | now={:.4}% | base200={:.4}% | ratio={:.2} | z={:.2} | atr9={:.4} | atr200={:.4} | state={:?} | trigger={:.4}",
        symbol,
        vol_now,
        baseline,
        ratio,
        z_score,
        atr9,
        atr200,
        state,
        trigger_price
    );

    VolatilityMetrics {
        vol_now,
        vol_ma20: baseline,
        state,
        trigger_price,
        z_score,
        atr9,
        atr200,
        current_price: closes.last().copied().unwrap_or(0.0),
        last_price: closes
            .get(closes.len().saturating_sub(2))
            .copied()
            .unwrap_or(0.0),
    }
}

fn calculate_trigger_from_vol(ratio: f64, z_score: f64) -> (f64, VolatilityState) {
    let base_trigger = env_f64("TRIGGER_PRICE", 0.885);
    let low_neutral_max = env_f64("VOL_LOW_NEUTRAL_MAX_RATIO", 0.95);
    let neutral_high_max = env_f64("VOL_NEUTRAL_HIGH_MAX_RATIO", 1.35);
    let superhigh_z = env_f64("VOL_SUPERHIGH_ZSCORE", 1.80);

    let state = if ratio <= low_neutral_max {
        VolatilityState::LowNeutral
    } else if ratio >= neutral_high_max || z_score >= superhigh_z {
        VolatilityState::HighSuperhigh
    } else {
        VolatilityState::NeutralHigh
    };

    let adjusted_trigger = match state {
        VolatilityState::LowNeutral => base_trigger - env_f64("TRIGGER_OFFSET_LOW_NEUTRAL", 0.0055),
        VolatilityState::NeutralHigh => base_trigger + env_f64("TRIGGER_OFFSET_NEUTRAL_HIGH", 0.0),
        VolatilityState::HighSuperhigh => {
            base_trigger + env_f64("TRIGGER_OFFSET_HIGH_SUPERHIGH", 0.015)
        }
    };

    (adjusted_trigger, state)
}

fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn average_last(values: &[f64], count: usize) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let take = count.min(values.len());
    average(&values[values.len() - take..])
}

fn std_dev(values: &[f64], mean: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|v| {
            let diff = *v - mean;
            diff * diff
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}
