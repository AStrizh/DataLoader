// 🧠 Goals for indicators.rs
// We’ll implement:
//
// VWAP
//
// Triple EMA (9, 14, 21)
//
// RSI (14-period)
//
// ATR (14-period, using high/low/close)
//
// Placeholder for slope indicators (daily + intraday)
//
// Each function takes a DataFrame and appends the new column.


use polars::prelude::*;
use anyhow::Result;

/// Add VWAP column to the DataFrame: (sum(price * volume) / sum(volume))
pub fn add_vwap(df: &mut DataFrame) -> Result<()> {
    let typical_price = (&df["high"] + &df["low"] + &df["close"]) / 3.0;
    let vwap_num = &typical_price * &df["volume"];
    let vwap_denom = &df["volume"];

    let cum_num = vwap_num.cumsum(false)?;
    let cum_denom = vwap_denom.cumsum(false)?;
    let vwap = &cum_num / &cum_denom;

    df.with_column(vwap.rename("vwap"))?;
    Ok(())
}

/// Add triple EMA (9, 14, 21) columns
pub fn add_triple_ema(df: &mut DataFrame) -> Result<()> {
    for period in [9, 14, 21] {
        let ema = df
            .column("close")?
            .f64()?
            .ewm_mean(EWMOptions {
                alpha: None,
                adjust: true,
                bias: false,
                span: Some(period),
                min_periods: Some(1),
            })?;
        df.with_column(Series::new(&format!("ema_{}", period), ema))?;
    }
    Ok(())
}

/// Add RSI (Relative Strength Index, 14-period)
pub fn add_rsi(df: &mut DataFrame) -> Result<()> {
    let close = df.column("close")?.f64()?;
    let mut gains = Vec::with_capacity(close.len());
    let mut losses = Vec::with_capacity(close.len());

    for i in 1..close.len() {
        let delta = close.get(i).unwrap_or(0.0) - close.get(i - 1).unwrap_or(0.0);
        if delta > 0.0 {
            gains.push(delta);
            losses.push(0.0);
        } else {
            gains.push(0.0);
            losses.push(-delta);
        }
    }

    // Pad with 0.0 at the start
    gains.insert(0, 0.0);
    losses.insert(0, 0.0);

    let avg_gain = Series::new("avg_gain", gains).ewm_mean(EWMOptions {
        span: Some(14),
        ..Default::default()
    })?;

    let avg_loss = Series::new("avg_loss", losses).ewm_mean(EWMOptions {
        span: Some(14),
        ..Default::default()
    })?;

    let rs: Vec<_> = avg_gain.f64()?.into_iter().zip(avg_loss.f64()?.into_iter())
        .map(|(g, l)| match (g, l) {
            (Some(g), Some(l)) if l != 0.0 => Some(g / l),
            _ => None,
        }).collect();

    let rsi: Vec<_> = rs.into_iter()
        .map(|rs| rs.map(|val| 100.0 - (100.0 / (1.0 + val))))
        .collect();

    df.with_column(Series::new("rsi_14", rsi))?;

    Ok(())
}

/// Add ATR (Average True Range, 14-period)
pub fn add_atr(df: &mut DataFrame) -> Result<()> {
    let high = df.column("high")?.f64()?;
    let low = df.column("low")?.f64()?;
    let close = df.column("close")?.f64()?;

    let mut tr = vec![0.0];
    for i in 1..close.len() {
        let h = high.get(i).unwrap_or(0.0);
        let l = low.get(i).unwrap_or(0.0);
        let c_prev = close.get(i - 1).unwrap_or(0.0);

        let max1 = h - l;
        let max2 = (h - c_prev).abs();
        let max3 = (l - c_prev).abs();
        tr.push(max1.max(max2).max(max3));
    }

    let atr = Series::new("atr_14", tr).ewm_mean(EWMOptions {
        span: Some(14),
        ..Default::default()
    })?;

    df.with_column(atr)?;
    Ok(())
}

/// Placeholder for slope-based indicators
pub fn add_custom_slope_indicators(df: &mut DataFrame) -> Result<()> {
    // Insert your custom slope logic here (intraday/daily)
    // You could fit a linear regression over a rolling window on close price
    Ok(())
}

/// Run all indicators
pub fn enrich_indicators(df: &mut DataFrame) -> Result<()> {
    add_vwap(df)?;
    add_triple_ema(df)?;
    add_rsi(df)?;
    add_atr(df)?;
    add_custom_slope_indicators(df)?;
    Ok(())
}
