use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use std::collections::BTreeMap;

pub const SCALE: i64 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "yes" => Ok(Self::Yes),
            "no" => Ok(Self::No),
            _ => bail!("unknown Kalshi outcome: {value}"),
        }
    }
    pub fn output_side(self) -> Side {
        match self {
            Self::Yes => Side::Buy,
            Self::No => Side::Sell,
        }
    }
    pub fn output_price(self, price: i64) -> i64 {
        match self {
            Self::Yes => price,
            Self::No => SCALE - price,
        }
    }
}

/// Convert a decimal string directly to a signed 1e6 fixed-point integer.
/// Reject rather than silently round excess precision.
pub fn fixed(value: &str) -> Result<i64> {
    let value = value.trim();
    if value.is_empty() {
        bail!("empty decimal");
    }
    let (negative, unsigned) = if let Some(s) = value.strip_prefix('-') {
        (true, s)
    } else if let Some(s) = value.strip_prefix('+') {
        (false, s)
    } else {
        (false, value)
    };
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty() {
        bail!("invalid decimal");
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        bail!("invalid decimal characters: {value}");
    }
    if fraction.len() > 6 {
        bail!("more than six fractional digits: {value}");
    }
    let whole: i64 = if whole.is_empty() { 0 } else { whole.parse()? };
    let mut fractional: i64 = if fraction.is_empty() {
        0
    } else {
        fraction.parse()?
    };
    for _ in fraction.len()..6 {
        fractional *= 10;
    }
    let scaled = whole
        .checked_mul(SCALE)
        .and_then(|x| x.checked_add(fractional))
        .context("decimal out of range")?;
    if negative {
        Ok(scaled.checked_neg().context("decimal out of range")?)
    } else {
        Ok(scaled)
    }
}

#[derive(Default, Debug, Clone)]
pub struct OrderBook {
    yes_bids: BTreeMap<i64, i64>,
    no_bids: BTreeMap<i64, i64>,
}

impl OrderBook {
    pub fn from_snapshot(yes: &[(String, String)], no: &[(String, String)]) -> Result<Self> {
        let mut book = Self::default();
        for (price, quantity) in yes {
            book.set(Outcome::Yes, fixed(price)?, fixed(quantity)?)?;
        }
        for (price, quantity) in no {
            book.set(Outcome::No, fixed(price)?, fixed(quantity)?)?;
        }
        Ok(book)
    }

    fn levels(&mut self, outcome: Outcome) -> &mut BTreeMap<i64, i64> {
        match outcome {
            Outcome::Yes => &mut self.yes_bids,
            Outcome::No => &mut self.no_bids,
        }
    }

    pub fn set(&mut self, outcome: Outcome, price: i64, quantity: i64) -> Result<()> {
        if !(0..=SCALE).contains(&price) {
            bail!("price outside [0,1]: {price}");
        }
        if quantity < 0 {
            bail!("negative quantity: {quantity}");
        }
        let levels = self.levels(outcome);
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        Ok(())
    }

    /// Kalshi sends signed deltas; output must be the resulting absolute quantity.
    pub fn apply_delta(&mut self, outcome: Outcome, price: i64, delta: i64) -> Result<i64> {
        let old = *self.levels(outcome).get(&price).unwrap_or(&0);
        let updated = old.checked_add(delta).context("quantity overflow")?;
        self.set(outcome, price, updated)?;
        Ok(updated)
    }

    pub fn snapshot(
        &self,
        symbol: &str,
        receive_time: DateTime<Utc>,
        exchange_time: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at: receive_time,
            exchange_timestamp: exchange_time,
            bids: self
                .yes_bids
                .iter()
                .rev()
                .map(|(&price, &quantity)| SnapshotBidsEntryDomain {
                    price_level: price,
                    quantity,
                })
                .collect(),
            asks: self
                .no_bids
                .iter()
                .rev()
                .map(|(&price, &quantity)| SnapshotAsksEntryDomain {
                    price_level: SCALE - price,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_parsing() {
        assert_eq!(fixed("0.4000").unwrap(), 400_000);
        assert_eq!(fixed("-8.25").unwrap(), -8_250_000);
        assert_eq!(fixed(".5").unwrap(), 500_000);
        assert!(fixed("1.1234567").is_err());
        assert!(fixed("NaN").is_err());
    }
    #[test]
    fn snapshots_deltas_and_conversion() {
        let mut b = OrderBook::from_snapshot(
            &[("0.40".into(), "20.00".into())],
            &[("0.55".into(), "10.00".into())],
        )
        .unwrap();
        assert_eq!(
            b.apply_delta(Outcome::Yes, 400_000, -8_000_000).unwrap(),
            12_000_000
        );
        assert!(b.apply_delta(Outcome::Yes, 400_000, -13_000_000).is_err());
        let t = Utc::now();
        let s = b.snapshot("TEST", t, t);
        assert_eq!(s.bids[0].quantity, 12_000_000);
        assert_eq!(s.asks[0].price_level, 450_000);
        b.apply_delta(Outcome::Yes, 400_000, -12_000_000).unwrap();
        assert!(b.snapshot("TEST", t, t).bids.is_empty());
    }
}
