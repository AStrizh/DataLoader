use std::fs;
use anyhow::Result;
use std::collections::BTreeMap;
use DataLoader::{loader, resampler, indicators, storage, stitcher::ContractWindow};
use DataLoader::stitcher::stitch_contracts;

type TsMs = i64;
const RAW_DIR: &str = "raw_data";
const PARQUET_DIR: &str = "parquet_data";

fn rollover_windows() -> BTreeMap<&'static str, (TsMs, TsMs)> {
    BTreeMap::from([
        ("2022-11-11_2022-12-20_CLF3_ohlcv1m", (1668466800000, 1671055140000)), // Nov 14 2022 18:00 - Dec 14, 2022 16:59
        ("2022-12-11_2023-01-20_CLG3_ohlcv1m", (1671058800000, 1673992740000)), // Dec 14, 2022 18:00 - Jan 17 2023 16:59
        ("2023-01-13_2023-02-21_CLH3_ohlcv1m", (1673996400000, 1676411940000)), // Jan 17 2023 18:00 - Feb 14 2023 16:59
        ("2023-02-10_2023-03-21_CLJ3_ohlcv1m", (1676415600000, 1678827540000)), // Feb 14 2023 18:00 - Mar 14 2023 16:59
        // Add more as needed
    ])
}


fn main() -> Result<()> {
    let windows = rollover_windows();

    // --- Toggle these two lines as needed ---
    process_and_save_all_contracts()?;                         // Step 1
    let stitched = stitch_from_parquet()?;          // Step 2

    storage::write_parquet(&stitched, &(format!("{}/stitched.parquet", PARQUET_DIR)))?;
    println!("✅ Wrote stitched data with {} rows", stitched.height());
    Ok(())
}

/// Step 1: Read -> Process -> Save (per contract)
/// Comment out this call in `main` when you just want stitching.
fn process_and_save_all_contracts() -> Result<()> {
    for entry in fs::read_dir(RAW_DIR)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map_or(false, |ext| ext == "json") {
            let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();

            if rollover_windows().contains_key(file_stem.as_str()) {
                println!("Processing {}", file_stem);

                // Load & transform
                let bars = loader::load_bars_from_file(&path)?;
                let df = resampler::bars_to_dataframe(&bars)?;
                let mut df_5m = resampler::downsample_to_5min(&df)?;
                indicators::enrich_indicators(&mut df_5m)?;

                // Save individual contract parquet
                let output_path = format!("{}/{}.parquet", PARQUET_DIR, file_stem);
                storage::write_parquet(&df_5m, &output_path)?;
            } else {
                println!("Skipping file with unknown rollover window: {}", file_stem);
            }
        }
    }
    Ok(())
}

/// Step 2: Read saved Parquet files -> Stitch
fn stitch_from_parquet() -> Result<polars::prelude::DataFrame> {
    let windows = rollover_windows();
    let mut contracts = Vec::with_capacity(windows.len());

    // BTreeMap iterates in key order. Since your keys start with the start-date,
    // this naturally yields chronological order without extra code.
    for (name, (start_ts, end_ts)) in windows.iter() {
        let path = format!("{}/{}.parquet", PARQUET_DIR, name); // plain String
        let df = storage::read_parquet(&path)?;                 // takes &str

        contracts.push(ContractWindow {
            name: (*name).to_string(),
            df,
            start_ts: *start_ts,
            end_ts: *end_ts,
        });
    }

    stitch_contracts(&contracts)
}




















// fn main() -> Result<()> {
//     // --- Define contract rollover windows ---
//     let rollover_windows = BTreeMap::from([
//         ("2022-11-11_2022-12-20_CLF3_ohlcv1m", (1668466800000, 1671055140000)), // Nov 14 2022 18:00 - Dec 14, 2022 16:59
//         ("2022-12-11_2023-01-20_CLG3_ohlcv1m", (1671058800000, 1673992740000)), // Dec 14, 2022 18:00 - Jan 17 2023 16:59
//         ("2023-01-13_2023-02-21_CLH3_ohlcv1m", (1673996400000, 1676411940000)), // Jan 17 2023 18:00 - Feb 14 2023 16:59
//         ("2023-02-10_2023-03-21_CLJ3_ohlcv1m", (1676415600000, 1678827540000)), // Feb 14 2023 18:00 - Mar 14 2023 16:59
//         // Add more as needed
//     ]);
//
//     let mut contracts = Vec::new();
//
//     for entry in fs::read_dir("raw_data")? {
//         let entry = entry?;
//         let path = entry.path();
//         if path.extension().map_or(false, |ext| ext == "json") {
//             let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();
//
//             if let Some((start_ts, end_ts)) = rollover_windows.get(file_stem.as_str()) {
//                 println!("Processing {}", file_stem);
//
//                 // Load & transform
//                 let bars = loader::load_bars_from_file(&path)?;
//                 let df = resampler::bars_to_dataframe(&bars)?;
//                 let mut df_5m = resampler::downsample_to_5min(&df)?;
//                 indicators::enrich_indicators(&mut df_5m)?;
//
//                 // Save individual contract (optional)
//                 let output_path = format!("parquet_data/{}.parquet", file_stem);
//                 storage::write_parquet(&df_5m, &output_path)?;
//
//                 // Add for stitching
//                 contracts.push(ContractWindow {
//                     name: file_stem,
//                     df: df_5m,
//                     start_ts: *start_ts,
//                     end_ts: *end_ts,
//                 });
//             } else {
//                 println!("Skipping file with unknown rollover window: {}", file_stem);
//             }
//         }
//     }
//
//     // --- Stitch into one continuous dataset ---
//     let stitched = stitch_contracts(&contracts)?;
//     storage::write_parquet(&stitched, "parquet_data/stitched.parquet")?;
//     println!("✅ Wrote stitched data with {} rows", stitched.height());
//
//     Ok(())
// }



// fn main() -> anyhow::Result<()> {
//     let bars = loader::load_all_bars_from_folder("raw_data/")?;
//     println!("Loaded {} bars", bars.len());
//
//     // Take a subset for a quick test (up to 10k)
//     const SAMPLE_N: usize = 10_000;
//     let take_n = bars.len().min(SAMPLE_N);
//     let sample = &bars[..take_n];
//     println!("Sampling {} bars for 5m resample test...", take_n);
//
//     // Convert to DataFrame and downsample
//     let df = resampler::bars_to_dataframe(sample)?;
//     let resampled = resampler::downsample_to_5min(&df)?;
//
//     // Print a quick preview
//     println!("Resampled shape: {:?}", resampled.shape());
//     println!("Resampled head:\n{:?}", resampled.head(Some(10)));
//
//     Ok(())
// }