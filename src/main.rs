use std::fs;
use anyhow::Result;
use std::collections::BTreeMap;
use DataLoader::{loader, resampler, indicators, storage, stitcher::ContractWindow};
use DataLoader::stitcher::stitch_contracts;

fn main() -> Result<()> {
    // --- Define contract rollover windows ---
    let rollover_windows = BTreeMap::from([
        ("2022-11-11_2022-12-20_CLF3_ohlcv1m", (1668466800000000000, 1671055140000000000)), // Nov 14 2022 18:00 - Dec 14, 2022 16:59
        ("2022-12-11_2023-01-20_CLG3_ohlcv1m", (1671058800000000000, 1673992740000000000)), // Dec 14, 2022 18:00 - Jan 17 2023 16:59
        ("2023-01-13_2023-02-21_CLH3_ohlcv1m", (1673996400000000000, 1676411940000000000)), // Jan 17 2023 18:00 - Feb 14 2023 16:59
        ("2023-02-10_2023-03-21_CLJ3_ohlcv1m", (1676415600000000000, 1678827540000000000)), // Feb 14 2023 18:00 - Mar 14 2023 16:59
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