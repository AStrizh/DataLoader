use DataLoader::{loader, resampler, indicators, storage, stitcher, stitcher::ContractWindow};
use tempfile::NamedTempFile;

#[test]
fn pipeline_smoke() {
    let bars = loader::load_bars_from_file("raw_data/sample.json").unwrap();
    let df = resampler::bars_to_dataframe(&bars).unwrap();
    let mut df5 = resampler::downsample_to_5min(&df).unwrap();
    indicators::enrich_indicators(&mut df5).unwrap();
    let tmp = NamedTempFile::new().unwrap();
    storage::write_parquet(&df5, tmp.path().to_str().unwrap()).unwrap();
    let read = storage::read_parquet(tmp.path().to_str().unwrap()).unwrap();
    let window = ContractWindow { name: "test".into(), df: read, start_ts: 0, end_ts: i64::MAX };
    let stitched = stitcher::stitch_contracts(&[window]).unwrap();
    assert_eq!(df5.height(), stitched.height());
}
