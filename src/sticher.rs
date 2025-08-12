use polars::prelude::*;
use anyhow::{Result, bail};
use std::collections::HashMap;


// 🧠 Goals for stitcher.rs
// Combine multiple DataFrames (from different contracts)
//
// Allow for explicit rollover date control per contract
//
// Add metadata like contract name and timestamp range
//
// Ensure clean, non-overlapping time windows
//
// You mentioned you want to avoid analyzing two contracts on the same day. So we’ll assume you’ll provide rollover dates per contract, and we’ll:
//
// Truncate each contract's data to that range
//
// Concatenate in correct order



/// Represents a contract and its valid time window
#[derive(Debug, Clone)]
pub struct ContractWindow {
    pub name: String,
    pub df: DataFrame,
    pub start_ts: i64, // UNIX epoch millis
    pub end_ts: i64,   // exclusive
}

/// Truncate contract to its assigned time window
pub fn truncate_contract(contract: &ContractWindow) -> Result<DataFrame> {
    let df = contract.df.lazy();
    let filtered = df
        .filter(
            col("timestamp")
                .gt_eq(lit(contract.start_ts))
                .and(col("timestamp").lt(lit(contract.end_ts))),
        )
        .with_column(lit(&contract.name).alias("contract"))
        .collect()?;

    Ok(filtered)
}

/// Merge all contracts together, preserving order
pub fn stitch_contracts(windows: &[ContractWindow]) -> Result<DataFrame> {
    let mut stitched = Vec::new();

    for contract in windows {
        let df = truncate_contract(contract)?;
        stitched.push(df);
    }

    concat_df(&stitched).map_err(Into::into)
}
