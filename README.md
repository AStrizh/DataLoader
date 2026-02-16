# DataLoader

## 1. Purpose
DataLoader processes historical futures OHLCV bars into Parquet datasets for backtesting.  
The executable supports two pipeline stages:

1. Per-contract processing: JSON input -> normalized DataFrame -> timeframe aggregation -> indicator enrichment -> Parquet output.
2. Contract stitching: processed contract files -> rollover window assignment -> continuous stitched Parquet output.

## 2. Runtime Configuration
Runtime configuration is defined in `src/main.rs` constants:

| Constant | Default | Behavior |
| --- | --- | --- |
| `RAW_DIR` | `raw_data` | Root directory scanned for raw JSON contract files. |
| `PARQUET_DIR` | `parquet_data` | Root directory used for per-contract and stitched Parquet output. |
| `TIMEFRAME` | `1m` | Aggregation interval passed to Polars dynamic grouping (examples: `1m`, `5m`, `15m`, `1h`). |
| `SYMBOL_FILTER` | `Some("CL")` | When `Some(symbol)`, only that symbol directory is processed and stitched. Use `None` to process all symbols. |

## 3. Input and Output Conventions
Input and output paths are discovered by directory scan; no symbol list is hardcoded.

### Input
- Location: `raw_data/{SYMBOL}/*.json`
- Encoding: newline-delimited JSON objects
- Expected fields per bar:

| Field | Type | Notes |
| --- | --- | --- |
| `instrument_name` | `String` | Contract identifier in source data. |
| `instrument_id` | `u32` | Source identifier. |
| `ts_event` | `i64` | Epoch nanoseconds. |
| `open`, `high`, `low`, `close` | `i64` | Price scaled by `1e9`. |
| `volume` | `u64` | Bar volume. |

### Per-contract Parquet Output
- Location: `parquet_data/{SYMBOL}/`
- Filename rule: source stem is rewritten from `_ohlcv1m` to `_ohlcv{TIMEFRAME}` before `.parquet` is appended.
- Example:
  - Input stem: `2024-12-10_2025-03-21_ESH5_ohlcv1m`
  - `TIMEFRAME = "15m"` -> `2024-12-10_2025-03-21_ESH5_ohlcv15m.parquet`

### Stitched Output
- Location: `parquet_data/{SYMBOL}/`
- Filename rule: `{SYMBOL}_{TIMEFRAME}_stitched.parquet`
- Example: `CL_1m_stitched.parquet`

## 4. Processing Pipeline

### 4.1 Stage 1: Process and Save Contracts
Function: `process_and_save_all_contracts()` in `src/main.rs`.

Per JSON file, the pipeline performs:
1. Load bars with `loader::load_bars_from_file`.
2. Convert to DataFrame with `resampler::bars_to_dataframe`.
3. Aggregate with `resampler::downsample(df, TIMEFRAME)`.
4. Add indicators with `indicators::enrich_indicators`.
5. Write Parquet with `storage::write_parquet`.

### 4.2 Stage 2: Stitch Contracts
Function: `stitch_from_parquet()` in `src/main.rs`.

Per symbol directory, the pipeline performs:
1. Load all `.parquet` files except stems ending with `_stitched`.
2. Keep only stems ending with `_ohlcv{TIMEFRAME}`.
3. Cast `timestamp` to `Int64` for rollover and truncation logic.
4. Sort contracts by minimum timestamp.
5. Build windows with `rollover_windows`.
6. Stitch windows with `stitch_contracts`.
7. Cast stitched `timestamp` back to `Datetime[ms]`.
8. Write `{SYMBOL}_{TIMEFRAME}_stitched.parquet`.

## 5. Module Responsibilities

| Module | Responsibility |
| --- | --- |
| `src/loader.rs` | Deserialize JSON bar records from files/folders into `Vec<Bar>`. |
| `src/resampler.rs` | Convert bars to DataFrame and aggregate to requested timeframe using OHLCV rules. |
| `src/indicators.rs` | Compute VWAP variants, EMAs, RSI variants, ATR, efficiency ratio, and price density. |
| `src/storage.rs` | Read/write Parquet using Zstd compression. |
| `src/stitcher.rs` | Truncate contracts to assigned windows and append contract label. |
| `src/main.rs` | Orchestrate stage execution, file discovery, rollover detection, and stitched output. |

## 6. Data Transformations

### 6.1 Type Conversion
- Timestamp: `ts_event` nanoseconds -> `timestamp` milliseconds (`i64`).
- Prices: `i64` scaled values -> `f64` by division with `1e9`.
- Volume: `u64` -> `f64`.

### 6.2 Aggregation Rules (`downsample`)
Polars `group_by_dynamic` is used with:
- `every = TIMEFRAME`
- `period = TIMEFRAME`
- `offset = 0s`
- `closed_window = Left`
- `label = Left`
- `start_by = WindowBound`

Per window aggregation:
- `open = first`
- `high = max`
- `low = min`
- `close = last`
- `volume = sum`

`downsample_to_5min()` is retained for compatibility and delegates to `downsample(..., "5m")`.

## 7. Indicator Specification
Indicator logic is implemented in `src/indicators.rs`.

### 7.1 Grouping and Session Keys
- Timezone reference: `America/New_York`.
- Weekly grouping key (`week_start`):
  - Base key is Monday `00:00` ET of the current week.
  - Sunday bars at or after `18:00` ET are assigned to the next week.
- Session key (`session_start`):
  - Session starts at `18:00` ET.
  - Bars before `18:00` ET are assigned to the prior calendar date's session start.
- Night session key (`night_session_id`):
  - Active when ET hour `>= 18` or `< 7`.
- Day session key (`day_session_id`):
  - Active when ET hour is in `[7, 17)`.

### 7.2 Output Columns
The enrichment step appends:
- `vwap`, `vwapn`, `vwapd`
- `ema_9`, `ema_14`, `ema_21`
- `rsi_14_ema`, `rsi_14_wilder`
- `atr_14`
- `er_10`
- `pd_10`

### 7.3 Formulas and Reset Rules
- EMA (`ema_grouped`):
  - Computed with `k = 2 / (period + 1)`.
  - State resets at weekly key boundaries.
- RSI 14:
  - Gain/loss deltas are computed only inside weekly groups.
  - `rsi_14_ema` uses EMA-smoothed gain/loss.
  - `rsi_14_wilder` uses RMA-smoothed gain/loss.
  - If average loss is `0`, RSI is set to `100`.
- ATR 14:
  - True range uses previous close only within same weekly group.
  - Smoothed with grouped RMA(14).
- Efficiency ratio (`er_10`):
  - Rolling 10-bar directional efficiency within each weekly group.
  - `ER = abs(close_t - close_{t-10}) / sum(abs(close_i - close_{i-1}))`.
  - If denominator is `0`, value is `0`.
- Price density (`pd_10`):
  - Rolling 10-bar path density within each weekly group.
  - `PD = sum(high_i - low_i) / (max(high_i) - min(low_i))`.
  - If denominator is `0`, value is `NaN`.
- VWAP variants:
  - `vwap`: cumulative from session start (`18:00` ET) using typical price `(H+L+C)/3`.
  - `vwapn`: same logic during night session only; `NaN` outside night session.
  - `vwapd`: same logic during day session only; `NaN` outside day session.

Indicator branches are executed in parallel with Rayon `join`.

## 8. Rollover and Stitching Logic

### 8.1 Rollover Window Assignment
Function: `rollover_windows()` in `src/main.rs`.

For each adjacent contract pair:
1. Call `find_volume_rollover(front, back)`.
2. Use returned `(front_end, back_start)` as boundaries.
3. Build `ContractWindow` entries:
   - First contract: start = minimum timestamp in contract.
   - Middle contracts: start = previous pair `back_start`, end = current pair `front_end`.
   - Last contract: end = maximum timestamp + `1 ms` (exclusive boundary).

### 8.2 Volume Crossover Detection
Function: `find_volume_rollover()` in `src/main.rs`.

Current implementation steps:
1. Use last `1500` bars from front and first `1500` bars from back.
2. Convert timestamps to New York dates.
3. Build date->volume maps from each slice using the last observed bar volume for each date.
4. Select first date where `back_volume > front_volume`; default is earliest front date if none found.
5. Compute rollover week from selected date:
   - Front end: Friday `17:00` ET.
   - Back start: Sunday `18:00` ET.
6. Return UTC datetimes for both boundaries.

## 9. Resulting Schema

| Column | Type | Source |
| --- | --- | --- |
| `timestamp` | `Datetime[ms]` in persisted outputs | Derived from `ts_event`; cast to `Int64` in stitching internals. |
| `open`, `high`, `low`, `close` | `Float64` | Input prices scaled by `1e9` and aggregated by timeframe. |
| `volume` | `Float64` | Input volume aggregated by timeframe. |
| `vwap`, `vwapn`, `vwapd` | `Float64` | Session VWAP indicators. |
| `ema_9`, `ema_14`, `ema_21` | `Float64` | Weekly-reset EMA indicators. |
| `rsi_14_ema`, `rsi_14_wilder` | `Float64` | Weekly-reset RSI indicators. |
| `atr_14` | `Float64` | Weekly-reset ATR indicator. |
| `er_10` | `Float64` | Weekly-reset efficiency ratio. |
| `pd_10` | `Float64` | Weekly-reset price density. |
| `contract` | `String` | Added during stitching. |

## 10. Build and Test

```bash
cargo check
cargo run
cargo run --release
cargo test
cargo test --lib
```

`src/main.rs` currently runs both stages by default in `main()`:
- `process_and_save_all_contracts()?`
- `stitch_from_parquet()?`

## 11. Current Constraints and Known Behaviors
- `TIMEFRAME` and `SYMBOL_FILTER` are compile-time constants; they are not CLI parameters.
- Stitching input selection is strict: only files ending `_ohlcv{TIMEFRAME}` are used.
- Rollover date maps use the last observed bar volume per date; they do not currently accumulate full daily volume.
- `loader::tests::test_load_file` expects `raw_data/sample.json`; this path is not present in the repository snapshot.
- `stitch_contracts()` appends windows by popping from a vector. Result row order follows reverse traversal of the input windows unless re-sorted afterward.
