use crate::loader::Bar;
use polars::prelude::*;
use chrono::{NaiveDateTime, Utc};
use anyhow::Result;

pub fn bars_to_dataframe(bars: &[Bar]) -> Result<DataFrame> {
    let ts: Vec<_> = bars.iter().map(|b| b.ts_event / 1_000_000).collect(); // nanoseconds → milliseconds
    let open: Vec<_> = bars.iter().map(|b| b.open as f64 / 1e9).collect();   // scale to float prices
    let high: Vec<_> = bars.iter().map(|b| b.high as f64 / 1e9).collect();
    let low: Vec<_> = bars.iter().map(|b| b.low as f64 / 1e9).collect();
    let close: Vec<_> = bars.iter().map(|b| b.close as f64 / 1e9).collect();
    let volume: Vec<_> = bars.iter().map(|b| b.volume as f64).collect();

    let df = df![
        "timestamp" => ts,
        "open" => open,
        "high" => high,
        "low" => low,
        "close" => close,
        "volume" => volume
    ]?;

    Ok(df)
}

pub fn downsample_to_5min(df: &DataFrame) -> Result<DataFrame> {
    let lazy = df.clone().lazy();

    let grouped = lazy
        .with_column(
            col("timestamp")
                .cast(DataType::Datetime(TimeUnit::Milliseconds, None))
                .alias("ts"),
        )
        .group_by_dynamic(
            col("ts"),
            [],
            DynamicGroupOptions {
                every: Duration::parse("5m"),
                period: Duration::parse("5m"),
                offset: Duration::parse("0s"),
                label: Label::Left,
                include_boundaries: false,
                closed_window: ClosedWindow::Left,
                start_by: StartBy::WindowBound,
                ..Default::default()
            },
        )
        .agg([
            col("open").first().alias("open"),
            col("high").max().alias("high"),
            col("low").min().alias("low"),
            col("close").last().alias("close"),
            col("volume").sum().alias("volume"),
        ])
        .sort(["ts"], SortMultipleOptions::default())
        .collect()?;

    Ok(grouped)
}