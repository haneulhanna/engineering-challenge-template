use anyhow::{Result, ensure};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "orderbooks.sbe".to_string());
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path.as_str())?;

    let mut snapshots = 0;
    let mut updates = 0;

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoded) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;

                if snapshots <= 5 {
                    println!(
                        "Snapshot: {}, {} bids, {} asks",
                        snapshot.symbol,
                        snapshot.bids.len(),
                        snapshot.asks.len()
                    );
                }
            }

            AnyMessage::PriceLevelUpdate(decoded) => {
                PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                updates += 1;
            }

            _ => {
                anyhow::bail!("Unexpected SBE message type");
            }
        }
    }

    println!("Snapshots decoded: {snapshots}");
    println!("Updates decoded: {updates}");

    ensure!(snapshots > 0, "No snapshots found!");

    println!("SBE verification passed!");
    Ok(())
}
