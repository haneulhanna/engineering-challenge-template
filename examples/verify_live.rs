use anyhow::{Result, ensure};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

fn main() -> Result<()> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open("kalshi-ownership-check.sbe")?;

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
