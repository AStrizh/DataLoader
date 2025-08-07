use serde::Deserialize;
use std::fs::File;
use std::io::{BufReader};
use std::path::Path;
use anyhow::{Result, Context};

#[derive(Debug, Deserialize, Clone)]
pub struct Bar {
    pub instrument_name: String,
    pub instrument_id: u32,
    pub ts_event: i64,  // nanoseconds since epoch
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: u64,
}

/// Load all bars from a single JSON file
pub fn load_bars_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<Bar>> {
    let file = File::open(&path)
        .with_context(|| format!("Failed to open file: {}", path.as_ref().display()))?;
    let reader = BufReader::new(file);

    let bars: Vec<Bar> = serde_json::Deserializer::from_reader(reader)
        .into_iter::<Bar>()
        .collect::<Result<_, _>>()
        .with_context(|| format!("Failed to deserialize JSON in file: {}", path.as_ref().display()))?;

    Ok(bars)
}

/// Load bars from all JSON files in a folder
pub fn load_all_bars_from_folder<P: AsRef<Path>>(folder: P) -> Result<Vec<Bar>> {
    let mut all_bars = Vec::new();

    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "json") {
            let mut bars = load_bars_from_file(&path)?;
            all_bars.append(&mut bars);
        }
    }

    Ok(all_bars)
}
