use polars::prelude::*;
use anyhow::Result;
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
    let time_col = if contract.df.get_column_names().iter().any(|c| *c == "timestamp") {
        "timestamp"
    } else {
        "ts"
    };
    let df = contract.df.clone().lazy();
    let filtered = df
        .filter(
            col(time_col)
                .gt_eq(lit(contract.start_ts))
                .and(col(time_col).lt(lit(contract.end_ts))),
        )
        .with_column(lit(contract.name.clone()).alias("contract"))
        .collect()?;

    Ok(filtered)
}

/// Merge all contracts together, preserving order
pub fn stitch_contracts(windows: &[ContractWindow]) -> Result<DataFrame> {
    let mut stitched = windows
        .iter()
        .map(truncate_contract)
        .collect::<Result<Vec<_>>>()?;
    if let Some(mut out) = stitched.pop() {
        while let Some(df) = stitched.pop() {
            out.vstack_mut(&df)?;
        }
        Ok(out)
    } else {
        Ok(DataFrame::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::df;

    #[test]
    fn test_stitch() {
        let df1 = df!(
            "timestamp" => [1i64,2,3],
            "open" => [1.0,1.0,1.0],
            "high" => [1.0,1.0,1.0],
            "low" => [1.0,1.0,1.0],
            "close" => [1.0,1.0,1.0],
            "volume" => [1.0,1.0,1.0]
        ).unwrap();
        let df2 = df!(
            "timestamp" => [4i64,5,6],
            "open" => [1.0,1.0,1.0],
            "high" => [1.0,1.0,1.0],
            "low" => [1.0,1.0,1.0],
            "close" => [1.0,1.0,1.0],
            "volume" => [1.0,1.0,1.0]
        ).unwrap();
        let windows = vec![
            ContractWindow { name: "A".into(), df: df1, start_ts: 1, end_ts: 4 },
            ContractWindow { name: "B".into(), df: df2, start_ts: 4, end_ts: 7 },
        ];
        let stitched = stitch_contracts(&windows).unwrap();
        assert_eq!(stitched.height(), 6);
        assert!(stitched.column("contract").is_ok());
    }
}

