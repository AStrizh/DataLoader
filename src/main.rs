mod loader;
mod resampler;
mod indicators;
mod sticher;
mod storage;

use polars::prelude::*;

use std::fs;
use anyhow::Result;
use std::collections::BTreeMap;
use crate::sticher::{stitch_contracts, ContractWindow};

fn main() -> Result<()> {
    // --- Define contract rollover windows ---
    let rollover_windows = BTreeMap::from([
        ("CLZ3", (1699488000000, 1709683200000)), // Dec 9, 2023 - Mar 6, 2024
        ("CLF4", (1709683200000, 1719964800000)), // Mar 6, 2024 - Jun 3, 2024
        ("CLM4", (1719964800000, 1730239200000)), // Jun 3, 2024 - Aug 30, 2024
        // Add more as needed
    ]);

    let mut contracts = Vec::new();

    for entry in fs::read_dir("raw_data")? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();

            if let Some((start_ts, end_ts)) = rollover_windows.get(file_stem.as_str()) {
                println!("Processing {}", file_stem);

                // Load & transform
                let bars = loader::load_bars_from_file(&path)?;
                let df = resampler::bars_to_dataframe(&bars)?;
                let mut df_5m = resampler::downsample_to_5min(&df)?;
                indicators::enrich_indicators(&mut df_5m)?;

                // Save individual contract (optional)
                let output_path = format!("parquet_data/{}.parquet", file_stem);
                storage::write_parquet(&df_5m, &output_path)?;

                // Add for stitching
                contracts.push(ContractWindow {
                    name: file_stem,
                    df: df_5m,
                    start_ts: *start_ts,
                    end_ts: *end_ts,
                });
            } else {
                println!("Skipping file with unknown rollover window: {}", file_stem);
            }
        }
    }

    // --- Stitch into one continuous dataset ---
    let stitched = stitch_contracts(&contracts)?;
    storage::write_parquet(&stitched, "parquet_data/stitched.parquet")?;
    println!("✅ Wrote stitched data with {} rows", stitched.height());

    Ok(())
}



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