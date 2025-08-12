// Write processed DataFrames to Apache Parquet
//
// Load DataFrames from Parquet
//
// Ensure all metadata/columns are preserved (like contract, indicators, timestamp)


use polars::prelude::*;
use std::fs::File;
use anyhow::{Result, Context};
/// Save a DataFrame to a Parquet file
pub fn write_parquet(df: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)
        .with_context(|| format!("Failed to create output file: {path}"))?;

    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd)
        .finish(df)
        .context("Failed to write Parquet data")?;

    Ok(())
}

/// Load a DataFrame from a Parquet file
pub fn read_parquet(path: &str) -> Result<DataFrame> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open Parquet file: {path}"))?;

    let df = ParquetReader::new(file)
        .finish()
        .context("Failed to read Parquet data")?;

    Ok(df)
}