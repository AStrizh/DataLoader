mod loader;
mod resampler;

fn main() -> anyhow::Result<()> {
    let bars = loader::load_all_bars_from_folder("raw_data/")?;
    println!("Loaded {} bars", bars.len());

    // Take a subset for a quick test (up to 10k)
    const SAMPLE_N: usize = 10_000;
    let take_n = bars.len().min(SAMPLE_N);
    let sample = &bars[..take_n];
    println!("Sampling {} bars for 5m resample test...", take_n);

    // Convert to DataFrame and downsample
    let df = resampler::bars_to_dataframe(sample)?;
    let resampled = resampler::downsample_to_5min(&df)?;

    // Print a quick preview
    println!("Resampled shape: {:?}", resampled.shape());
    println!("Resampled head:\n{:?}", resampled.head(Some(10)));

    Ok(())
}