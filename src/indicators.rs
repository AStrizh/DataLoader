use anyhow::Result;
use chrono::{Datelike, Duration, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::New_York;
use polars::prelude::*;
use rayon::join;

// === Helper functions ====================================================

fn week_start(ts: i64) -> i64 {
    let dt_utc = Utc.timestamp_millis_opt(ts).unwrap();
    let dt_et = dt_utc.with_timezone(&New_York);
    let mut start_date = dt_et.date_naive()
        - Duration::days(dt_et.weekday().num_days_from_monday() as i64);
    if dt_et.weekday() == Weekday::Sun && dt_et.hour() >= 18 {
        start_date += Duration::days(7);
    }
    New_York
        .with_ymd_and_hms(start_date.year(), start_date.month(), start_date.day(), 0, 0, 0)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp_millis()
}

fn session_start(ts: i64) -> i64 {
    let dt_utc = Utc.timestamp_millis_opt(ts).unwrap();
    let dt_et = dt_utc.with_timezone(&New_York);
    let date = dt_et.date_naive();
    let start_date = if dt_et.hour() >= 18 { date } else { date - Duration::days(1) };
    New_York
        .with_ymd_and_hms(start_date.year(), start_date.month(), start_date.day(), 18, 0, 0)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp_millis()
}

fn night_session_id(ts: i64) -> Option<i64> {
    let dt_et = Utc.timestamp_millis_opt(ts).unwrap().with_timezone(&New_York);
    let hour = dt_et.hour();
    if hour >= 18 || hour < 8 {
        let date = if hour >= 18 { dt_et.date_naive() } else { dt_et.date_naive() - Duration::days(1) };
        Some(New_York.with_ymd_and_hms(date.year(), date.month(), date.day(), 18, 0, 0).unwrap().with_timezone(&Utc).timestamp_millis())
    } else {
        None
    }
}

fn day_session_id(ts: i64) -> Option<i64> {
    let dt_et = Utc.timestamp_millis_opt(ts).unwrap().with_timezone(&New_York);
    let hour = dt_et.hour();
    if (8..17).contains(&hour) {
        let date = dt_et.date_naive();
        Some(New_York.with_ymd_and_hms(date.year(), date.month(), date.day(), 8, 0, 0).unwrap().with_timezone(&Utc).timestamp_millis())
    } else {
        None
    }
}

fn ema_grouped(values: &[f64], period: usize, keys: &[i64]) -> Vec<f64> {
    if values.is_empty() { return Vec::new(); }
    let k = 2.0 / (period as f64 + 1.0);
    let mut out = Vec::with_capacity(values.len());
    let mut prev = values[0];
    let mut key = keys[0];
    out.push(prev);
    for i in 1..values.len() {
        if keys[i] != key {
            key = keys[i];
            prev = values[i];
        } else {
            prev = values[i] * k + prev * (1.0 - k);
        }
        out.push(prev);
    }
    out
}

fn rma_grouped(values: &[f64], period: usize, keys: &[i64]) -> Vec<f64> {
    let len = values.len();
    let mut out = vec![f64::NAN; len];
    let mut key = keys[0];
    let mut count = 0usize;
    let mut avg = 0.0;
    for i in 0..len {
        if keys[i] != key {
            key = keys[i];
            count = 0;
            avg = 0.0;
        }
        let v = values[i];
        if count < period {
            avg += v;
            count += 1;
            if count == period {
                avg /= period as f64;
                out[i] = avg;
            }
        } else {
            avg = (avg * (period as f64 - 1.0) + v) / period as f64;
            out[i] = avg;
        }
    }
    out
}

fn calc_triple_ema(close: &[f64], keys: &[i64]) -> Vec<Series> {
    [9usize, 14, 21]
        .iter()
        .map(|&p| Series::new(PlSmallStr::from(format!("ema_{p}").as_str()), ema_grouped(close, p, keys)))
        .collect()
}

fn calc_rsi(close: &[f64], keys: &[i64]) -> (Series, Series) {
    let len = close.len();
    let mut gains = vec![0.0; len];
    let mut losses = vec![0.0; len];
    for i in 1..len {
        if keys[i] == keys[i - 1] {
            let delta = close[i] - close[i - 1];
            if delta >= 0.0 { gains[i] = delta; } else { losses[i] = -delta; }
        }
    }
    let avg_gain_ema = ema_grouped(&gains, 14, keys);
    let avg_loss_ema = ema_grouped(&losses, 14, keys);
    let rsi_ema: Vec<f64> = avg_gain_ema.iter().zip(avg_loss_ema.iter()).map(|(g,l)| if *l==0.0 {100.0} else {100.0 - 100.0/(1.0 + g/l)}).collect();
    let avg_gain_rma = rma_grouped(&gains, 14, keys);
    let avg_loss_rma = rma_grouped(&losses, 14, keys);
    let rsi_wilder: Vec<f64> = avg_gain_rma.iter().zip(avg_loss_rma.iter()).map(|(g,l)| if *l==0.0 {100.0} else {100.0 - 100.0/(1.0 + g/l)}).collect();
    (Series::new(PlSmallStr::from("rsi_14_ema"), rsi_ema), Series::new(PlSmallStr::from("rsi_14_wilder"), rsi_wilder))
}

fn calc_atr(high: &[f64], low: &[f64], close: &[f64], keys: &[i64]) -> Series {
    let len = close.len();
    let mut tr = Vec::with_capacity(len);
    tr.push(high[0] - low[0]);
    for i in 1..len {
        if keys[i] != keys[i - 1] {
            tr.push(high[i] - low[i]);
        } else {
            let h = high[i];
            let l = low[i];
            let c_prev = close[i - 1];
            tr.push((h - l).max((h - c_prev).abs()).max((l - c_prev).abs()));
        }
    }
    Series::new(PlSmallStr::from("atr_14"), rma_grouped(&tr, 14, keys))
}

fn calc_vwap_variants(ts: &[i64], high: &[f64], low: &[f64], close: &[f64], volume: &[f64]) -> (Series, Series, Series) {
    let len = close.len();
    let mut vwap = Vec::with_capacity(len);
    let mut vwapn = Vec::with_capacity(len);
    let mut vwapd = Vec::with_capacity(len);

    let mut sess = session_start(ts[0]);
    let mut num = 0.0;
    let mut den = 0.0;

    let mut night_id = night_session_id(ts[0]);
    let mut num_n = 0.0;
    let mut den_n = 0.0;

    let mut day_id = day_session_id(ts[0]);
    let mut num_d = 0.0;
    let mut den_d = 0.0;

    for i in 0..len {
        let tp = (high[i] + low[i] + close[i]) / 3.0;

        let s = session_start(ts[i]);
        if s != sess { sess = s; num = 0.0; den = 0.0; }
        num += tp * volume[i];
        den += volume[i];
        vwap.push(num / den);

        let nid = night_session_id(ts[i]);
        if nid.is_some() {
            if nid != night_id { night_id = nid; num_n = 0.0; den_n = 0.0; }
            num_n += tp * volume[i];
            den_n += volume[i];
            vwapn.push(num_n / den_n);
        } else {
            night_id = None; num_n = 0.0; den_n = 0.0; vwapn.push(f64::NAN);
        }

        let did = day_session_id(ts[i]);
        if did.is_some() {
            if did != day_id { day_id = did; num_d = 0.0; den_d = 0.0; }
            num_d += tp * volume[i];
            den_d += volume[i];
            vwapd.push(num_d / den_d);
        } else {
            day_id = None; num_d = 0.0; den_d = 0.0; vwapd.push(f64::NAN);
        }
    }

    (Series::new(PlSmallStr::from("vwap"), vwap), Series::new(PlSmallStr::from("vwapn"), vwapn), Series::new(PlSmallStr::from("vwapd"), vwapd))
}

/// Run all indicators and append to the DataFrame
pub fn enrich_indicators(df: &mut DataFrame) -> Result<()> {
    let ts: Vec<i64> = df
        .column("timestamp")?
        .cast(&DataType::Int64)?
        .i64()? 
        .into_no_null_iter()
        .collect();
    let high: Vec<f64> = df.column("high")?.f64()?.into_no_null_iter().collect();
    let low: Vec<f64> = df.column("low")?.f64()?.into_no_null_iter().collect();
    let close: Vec<f64> = df.column("close")?.f64()?.into_no_null_iter().collect();
    let volume: Vec<f64> = df.column("volume")?.f64()?.into_no_null_iter().collect();
    let week_keys: Vec<i64> = ts.iter().map(|&t| week_start(t)).collect();

    let (vwap_cols, (ema_vec, (rsi_cols, atr))) = join(
        || calc_vwap_variants(&ts, &high, &low, &close, &volume),
        || {
            join(
                || calc_triple_ema(&close, &week_keys),
                || {
                    join(
                        || calc_rsi(&close, &week_keys),
                        || calc_atr(&high, &low, &close, &week_keys),
                    )
                },
            )
        },
    );
    let (rsi_ema, rsi_wilder) = rsi_cols;
    let (vwap, vwapn, vwapd) = vwap_cols;

    df.with_column(vwap)?;
    df.with_column(vwapn)?;
    df.with_column(vwapd)?;
    for s in ema_vec { df.with_column(s)?; }
    df.with_column(rsi_ema)?;
    df.with_column(rsi_wilder)?;
    df.with_column(atr)?;
    Ok(())
}

// === Tests ===============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    #[test]
    fn test_indicators_added() {
        let mut df = df!(
            "timestamp" => [1i64,2,3,4,5,6,7,8,9,10,11,12,13,14,15],
            "open" => [1.0;15],
            "high" => [2.0;15],
            "low" => [0.5;15],
            "close" => (1..=15).map(|v| v as f64).collect::<Vec<_>>(),
            "volume" => [100.0;15]
        ).unwrap();
        enrich_indicators(&mut df).unwrap();
        for col in ["vwap","vwapn","vwapd","ema_9","ema_14","ema_21","rsi_14_ema","rsi_14_wilder","atr_14"] {
            assert!(df.column(col).is_ok(), "missing column {col}");
            assert_eq!(df.height(), df.column(col).unwrap().len());
        }
    }

    #[test]
    fn test_weekly_reset_and_sessions() {
        use chrono::TimeZone;
        let tz = New_York;
        let ts = vec![
            tz.with_ymd_and_hms(2024,5,17,16,0,0).unwrap().with_timezone(&Utc).timestamp_millis(),
            tz.with_ymd_and_hms(2024,5,17,17,0,0).unwrap().with_timezone(&Utc).timestamp_millis(),
            tz.with_ymd_and_hms(2024,5,19,18,0,0).unwrap().with_timezone(&Utc).timestamp_millis(),
            tz.with_ymd_and_hms(2024,5,19,19,0,0).unwrap().with_timezone(&Utc).timestamp_millis(),
            tz.with_ymd_and_hms(2024,5,20,9,0,0).unwrap().with_timezone(&Utc).timestamp_millis(),
        ];
        let close: Vec<f64> = (1..=5).map(|v| v as f64).collect();
        let mut df = df!(
            "timestamp" => ts.clone(),
            "open" => close.clone(),
            "high" => close.iter().map(|v| v+0.1).collect::<Vec<_>>(),
            "low" => close.iter().map(|v| v-0.1).collect::<Vec<_>>(),
            "close" => close.clone(),
            "volume" => [1.0;5]
        ).unwrap();
        enrich_indicators(&mut df).unwrap();
        let ema = df.column("ema_9").unwrap().f64().unwrap().get(2).unwrap();
        assert!((ema - close[2]).abs() < 1e-9);
        let vwapn = df.column("vwapn").unwrap().f64().unwrap();
        let vwapd = df.column("vwapd").unwrap().f64().unwrap();
        assert!(vwapn.get(2).unwrap().is_finite() && vwapd.get(2).unwrap().is_nan());
        assert!(vwapd.get(4).unwrap().is_finite() && vwapn.get(4).unwrap().is_nan());
    }
}

