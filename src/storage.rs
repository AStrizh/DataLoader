// Write processed DataFrames to Apache Parquet
//
// Load DataFrames from Parquet
//
// Ensure all metadata/columns are preserved (like contract, indicators, timestamp)


use polars::prelude::*;
use polars::io::parquet::{read::ParquetReader, write::ParquetWriter};
use std::fs::{File, create_dir_all};
use std::path::Path;
use anyhow::{Result, Context};
/// Save a DataFrame to a Parquet file
pub fn write_parquet(df: &DataFrame, path: &str) -> Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        create_dir_all(parent)
            .with_context(|| format!("Failed to create parent directory for {path}"))?;
    }
    let file = File::create(path)
        .with_context(|| format!("Failed to create output file: {path}"))?;

    let mut df = df.clone();
    ParquetWriter::new(file)
        .with_compression(ParquetCompression::Zstd(None))
        .finish(&mut df)
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

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;
    use tempfile::NamedTempFile;

    #[test]
    fn test_parquet_roundtrip() {
        let df = df!(
            "timestamp" => [1i64,2],
            "open" => [1.0,1.0],
            "high" => [1.0,1.0],
            "low" => [1.0,1.0],
            "close" => [1.0,1.0],
            "volume" => [1.0,1.0]
        ).unwrap();
        let file = NamedTempFile::new().unwrap();
        write_parquet(&df, file.path().to_str().unwrap()).unwrap();
        let read = read_parquet(file.path().to_str().unwrap()).unwrap();
        assert_eq!(read.height(), df.height());
    }
}