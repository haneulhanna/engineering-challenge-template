
use std::collections::BTreeMap;

#[derive(Debug, Default)]

// std::map<int64_t. int64_t>

pub struct OrderBook {
    yes_bids: BTreeMap<i64, i64>,
    no_bids: BTreeMap<i64, i64>,
}

#[derive(Clone, Copy)]
pub enum Outcome {
    Yes,
    No,
}

// func of 
impl OrderBook {
    pub fn set_level(
        // this
        &mut self,
        outcome: Outcome,
        price: i64,
        quantity: i64,
    ) -> Result<(), &'static str> {
        if !(0..=1_000_000).contains(&price) {
            return Err("invalid price");
        }

        if quantity < 0 {
            return Err("negative quantity");
        }

        //switch?
        let levels = match outcome {
            Outcome::Yes => &mut self.yes_bids,
            Outcome::No => &mut self.no_bids,
        };

        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }

        Ok(())
    }

    
    pub fn apply_delta(
        &mut self,
        outcome: Outcome,
        price: i64,
        delta: i64,
    ) -> Result<i64, &'static str> {

        let levels = match outcome {
            Outcome::Yes => &self.yes_bids,
            Outcome::No => &self.no_bids,
        };

        let current = levels.get(&price).copied().unwrap_or(0);

        let updated = current
            .checked_add(delta)
            .ok_or("quantity overflow")?;

        self.set_level(outcome, price, updated)?;

        Ok(updated)
    }


}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook() {
        let mut book = OrderBook::default();

        // YES $0.40에 20계약
        book.set_level(Outcome::Yes, 400_000, 20_000_000).unwrap();

        // NO $0.55에 10계약
        book.set_level(Outcome::No, 550_000, 10_000_000).unwrap();

        assert_eq!(
            book.yes_bids.get(&400_000),
            Some(&20_000_000)
        );

        assert_eq!(
            book.no_bids.get(&550_000),
            Some(&10_000_000)
        );

        // 수량이 0이 되면 가격 레벨 삭제
        book.set_level(Outcome::Yes, 400_000, 0).unwrap();

        assert!(!book.yes_bids.contains_key(&400_000));
    }
}

