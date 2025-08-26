use std::fs;
use anyhow::Result;
use std::collections::BTreeMap;
use std::path::Path;
use DataLoader::{loader, resampler, indicators, storage, stitcher::ContractWindow};
use DataLoader::stitcher::stitch_contracts;
use polars::prelude::*;
use chrono::{Duration, Datelike, Utc, DateTime, NaiveDate, TimeZone};
use chrono_tz::America::New_York;

type TsMs = i64;
const RAW_DIR: &str = "raw_data";
const PARQUET_DIR: &str = "parquet_data";



fn main() -> Result<()> {

    // --- Toggle these two lines as needed ---
    //process_and_save_all_contracts()?; // Step 1
    stitch_from_parquet()?;            // Step 2

    Ok(())
}

/// Step 1: Read -> Process -> Save (per contract)
/// Comment out this call in `main` when you just want stitching.
fn process_and_save_all_contracts() -> Result<()> {
    for symbol_dir in fs::read_dir(RAW_DIR)? {
        let symbol_dir = symbol_dir?;
        if !symbol_dir.file_type()?.is_dir() {
            continue;
        }
        let symbol = symbol_dir.file_name().to_string_lossy().to_string();
        let out_dir = Path::new(PARQUET_DIR).join(&symbol);
        fs::create_dir_all(&out_dir)?;

        for file in fs::read_dir(symbol_dir.path())? {
            let file = file?;
            let path = file.path();
            if path.extension().map_or(false, |ext| ext == "json") {
                let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();


                println!("Processing {}/{}", symbol, file_stem);


                let bars = loader::load_bars_from_file(&path)?;
                let df = resampler::bars_to_dataframe(&bars)?;
                let mut df_5m = resampler::downsample_to_5min(&df)?;
                indicators::enrich_indicators(&mut df_5m)?;


                let output_path = out_dir.join(format!("{}.parquet", file_stem));
                storage::write_parquet(&df_5m, output_path.to_str().unwrap())?;


            }
        }
    }
    Ok(())
}



/// Step 2: Read saved Parquet files -> Stitch
fn stitch_from_parquet() -> Result<()> {

    for symbol_dir in fs::read_dir(PARQUET_DIR)? {
        let symbol_dir = symbol_dir?;
        if !symbol_dir.file_type()?.is_dir() {
            continue;



        }
        let symbol = symbol_dir.file_name().to_string_lossy().to_string();
        let mut loaded: Vec<(String, DataFrame)> = Vec::new();
        for file in fs::read_dir(symbol_dir.path())? {
            let file = file?;
            let path = file.path();
            if path.extension().map_or(false, |ext| ext == "parquet") {
                let stem = path.file_stem().unwrap().to_string_lossy().to_string();
                if stem.ends_with("_stitched") {
                    continue;
                }
                let mut df = storage::read_parquet(path.to_str().unwrap())?;
                // Parquet persists `timestamp` as `datetime[ms]`; cast to raw `Int64`
                // so downstream code expecting integer epochs can operate uniformly.
                df.try_apply("timestamp", |s| s.cast(&DataType::Int64))?;
                loaded.push((stem, df));
            }
        }

        if loaded.is_empty() {
            continue;
        }


        loaded.sort_by_key(|(_, df)| {
            df.column("timestamp")
                .unwrap()
                .i64()
                .unwrap()
                .into_iter()
                .flatten()
                .min()
                .unwrap()
        });

        let windows = rollover_windows(&loaded)?;
        let stitched = stitch_contracts(&windows)?;
        let out_path = symbol_dir.path().join(format!("{}_stitched.parquet", symbol));
        storage::write_parquet(&stitched, out_path.to_str().unwrap())?;
        println!(
            "✅ Wrote stitched data for {} with {} rows",
            symbol,
            stitched.height()
        );
    }
    Ok(())
}


/// Determine rollover boundaries between adjacent contracts and build windows
fn rollover_windows(contracts: &[(String, DataFrame)]) -> Result<Vec<ContractWindow>> {
    let mut rollover_points: Vec<(TsMs, TsMs)> = Vec::new();
    for pair in contracts.windows(2) {
        let (front_name, front_df) = &pair[0];
        let (back_name, back_df) = &pair[1];
        let (end_ts, start_ts) = find_volume_rollover(front_df, back_df)?;
        let ny = New_York;
        let end_dt = DateTime::<Utc>::from_timestamp_millis(end_ts).unwrap().with_timezone(&ny);
        let start_dt = DateTime::<Utc>::from_timestamp_millis(start_ts).unwrap().with_timezone(&ny);
        println!("Rolling {} -> {} on {} / {}", front_name, back_name, end_dt, start_dt);
        rollover_points.push((end_ts, start_ts));
    }

    let mut windows = Vec::new();
    for (idx, (name, df)) in contracts.iter().enumerate() {
        let start_ts = if idx == 0 {
            df.column("timestamp")?
                .i64()?
                .into_iter()
                .flatten()
                .min()
                .unwrap()
        } else {
            rollover_points[idx - 1].1
        };
        let end_ts = if idx == contracts.len() - 1 {
            i64::MAX
        } else {
            rollover_points[idx].0
        };
        windows.push(ContractWindow {
            name: name.clone(),
            df: df.clone(),
            start_ts,
            end_ts,
        });
    }
    Ok(windows)
}


/// Identify volume crossover and return Friday close/Sunday open timestamps in ms UTC
fn find_volume_rollover(front: &DataFrame, back: &DataFrame) -> Result<(TsMs, TsMs)> {
    // Use last 1500 bars of front and first 1500 bars of back
    let front_slice = front.select(["timestamp", "volume"])?.tail(Some(1500));
    let back_slice = back.select(["timestamp", "volume"])?.head(Some(1500));

    use std::collections::BTreeMap;
    let mut front_map: BTreeMap<NaiveDate, f64> = BTreeMap::new();
    let mut back_map: BTreeMap<NaiveDate, f64> = BTreeMap::new();

    let ny_tz = New_York;

    let f_ts = front_slice.column("timestamp")?.i64()?;
    let f_vol = front_slice.column("volume")?.f64()?;
    for (t_opt, v_opt) in f_ts.into_iter().zip(f_vol.into_iter()) {
        if let (Some(t), Some(v)) = (t_opt, v_opt) {
            if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(t) {
                let date = dt.with_timezone(&ny_tz).date_naive();
                *front_map.entry(date).or_insert(0.0) = v;
            }
        }
    }

    let b_ts = back_slice.column("timestamp")?.i64()?;
    let b_vol = back_slice.column("volume")?.f64()?;
    for (t_opt, v_opt) in b_ts.into_iter().zip(b_vol.into_iter()) {
        if let (Some(t), Some(v)) = (t_opt, v_opt) {
            if let Some(dt) = DateTime::<Utc>::from_timestamp_millis(t) {
                let date = dt.with_timezone(&ny_tz).date_naive();
                *back_map.entry(date).or_insert(0.0) = v;
            }
        }
    }

    let mut roll_date = *front_map.keys().next().unwrap();
    for (date, fvol) in &front_map {
        if let Some(bvol) = back_map.get(date) {
            if bvol > fvol {
                roll_date = *date;
                break;
            }
        }
    }

    let monday = roll_date - Duration::days(roll_date.weekday().num_days_from_monday() as i64);
    let friday = monday + Duration::days(4);
    let friday_close_local = friday.and_hms_opt(17, 0, 0).unwrap();
    let sunday_open_local = (friday + Duration::days(2)).and_hms_opt(18, 0, 0).unwrap();

    let tz = New_York;
    let friday_utc = tz.from_local_datetime(&friday_close_local).unwrap().with_timezone(&Utc);
    let sunday_utc = tz.from_local_datetime(&sunday_open_local).unwrap().with_timezone(&Utc);

    Ok((friday_utc.timestamp_millis(), sunday_utc.timestamp_millis()))
}