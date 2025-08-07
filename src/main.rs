mod loader;

fn main() -> anyhow::Result<()> {
    let bars = loader::load_all_bars_from_folder("raw_data/")?;
    println!("Loaded {} bars", bars.len());
    Ok(())
}