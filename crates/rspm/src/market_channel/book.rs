use super::{
    event::{Book, MarketEvent, PriceChange, Side},
    frame::Frame,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};

/// Maintained book for one asset. Levels are keyed by exact decimal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AssetBook {
    pub market: String,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
    /// False after a gap or reconnect until this asset's next book snapshot.
    /// Levels are kept but stale while false.
    pub complete: bool,
    pub tick_size: Option<Decimal>,
    /// Top of book as the venue last reported it (a book snapshot resets it to
    /// the level maps), `None` for an empty side. In the live capture 4.8% of
    /// the values on price_change entries differed from the levels at that
    /// instant and agreed again at a later frame, so use [`Self::top_bid`]
    /// when the top must match the levels.
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub last_trade_price: Option<Decimal>,
    pub hash: Option<String>,
    pub timestamp_ms: i64,
}

impl AssetBook {
    /// Highest bid price in the maintained levels.
    pub fn top_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().copied()
    }

    /// Lowest ask price in the maintained levels.
    pub fn top_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().copied()
    }
}

/// One maintained book per asset, fed by [`Frame`]s or single events.
///
/// Size rule (checked against a live capture, 1276 of 1276 touched levels):
/// `price_change.size` is the new absolute size at that level, `0` removes it.
#[derive(Debug, Default)]
pub struct BookSet {
    books: HashMap<String, AssetBook>,
    cursor: Option<(u64, u64)>,
}

fn replace_side(levels: &[super::event::Level]) -> BTreeMap<Decimal, Decimal> {
    levels
        .iter()
        .filter(|level| !level.size.is_zero())
        .map(|level| (level.price, level.size))
        .collect()
}

fn note(changed: &mut Vec<String>, asset: &str) {
    if !changed.iter().any(|seen| seen == asset) {
        changed.push(asset.to_owned());
    }
}

/// The venue reports an empty bid side as 0 and an empty ask side as 1, neither of
/// which is a tradable price.
fn top(price: Option<Decimal>, empty: Decimal) -> Option<Decimal> {
    price.filter(|price| *price != empty)
}

fn update<T: PartialEq>(slot: &mut T, value: T) -> bool {
    let differs = *slot != value;
    *slot = value;
    differs
}

impl BookSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn book(&self, asset_id: &str) -> Option<&AssetBook> {
        self.books.get(asset_id)
    }

    /// Forget an asset, for example after an unsubscribe or a resolution.
    pub fn remove(&mut self, asset_id: &str) -> Option<AssetBook> {
        self.books.remove(asset_id)
    }

    /// Apply every event of one frame, first checking the frame's position in
    /// the stream: a new connection or a sequence that is not the successor of
    /// the last one marks every book incomplete. Returns the assets that
    /// changed, including those just marked incomplete.
    pub fn apply_frame(&mut self, frame: &Frame) -> Vec<String> {
        let mut changed = Vec::new();
        let contiguous = self.cursor.is_none_or(|(conn, seq)| {
            conn == frame.conn_id && seq.checked_add(1) == Some(frame.seq)
        });
        self.cursor = Some((frame.conn_id, frame.seq));
        if !contiguous {
            for (asset, book) in &mut self.books {
                if book.complete {
                    book.complete = false;
                    note(&mut changed, asset);
                }
            }
        }
        for event in frame.events() {
            for asset in self.apply(&event) {
                note(&mut changed, &asset);
            }
        }
        changed
    }

    /// Apply one event. Returns the assets whose book or metadata changed.
    /// Level updates are ignored for an asset without a complete book, since
    /// they have no valid base.
    pub fn apply(&mut self, event: &MarketEvent) -> Vec<String> {
        let mut changed = Vec::new();
        match event {
            MarketEvent::Book(book) => {
                self.apply_book(book);
                note(&mut changed, &book.asset_id);
            }
            MarketEvent::PriceChange(change) => self.apply_price_change(change, &mut changed),
            MarketEvent::BestBidAsk(quote) => {
                if let Some(book) = self.books.get_mut(&quote.asset_id)
                    && (update(&mut book.best_bid, top(quote.best_bid, Decimal::ZERO))
                        | update(&mut book.best_ask, top(quote.best_ask, Decimal::ONE)))
                {
                    note(&mut changed, &quote.asset_id);
                }
            }
            MarketEvent::TickSizeChange(tick) => {
                if let Some(book) = self.books.get_mut(&tick.asset_id)
                    && update(&mut book.tick_size, Some(tick.new_tick_size))
                {
                    note(&mut changed, &tick.asset_id);
                }
            }
            MarketEvent::LastTradePrice(trade) => {
                if let Some(book) = self.books.get_mut(&trade.asset_id)
                    && update(&mut book.last_trade_price, Some(trade.price))
                {
                    note(&mut changed, &trade.asset_id);
                }
            }
            MarketEvent::NewMarket(_)
            | MarketEvent::MarketResolved(_)
            | MarketEvent::Other { .. } => {}
        }
        changed
    }

    fn apply_book(&mut self, snapshot: &Book) {
        let book = self.books.entry(snapshot.asset_id.clone()).or_default();
        book.market.clone_from(&snapshot.market);
        book.bids = replace_side(&snapshot.bids);
        book.asks = replace_side(&snapshot.asks);
        book.complete = true;
        book.best_bid = book.top_bid();
        book.best_ask = book.top_ask();
        book.hash.clone_from(&snapshot.hash);
        book.timestamp_ms = snapshot.timestamp_ms;
        if snapshot.tick_size.is_some() {
            book.tick_size = snapshot.tick_size;
        }
        if snapshot.last_trade_price.is_some() {
            book.last_trade_price = snapshot.last_trade_price;
        }
    }

    fn apply_price_change(&mut self, change: &PriceChange, changed: &mut Vec<String>) {
        for entry in &change.price_changes {
            let Some(book) = self.books.get_mut(&entry.asset_id) else {
                continue;
            };
            if !book.complete {
                continue;
            }
            let side = match entry.side {
                Side::Buy => &mut book.bids,
                Side::Sell => &mut book.asks,
            };
            let mut touched = if entry.size.is_zero() {
                side.remove(&entry.price).is_some()
            } else {
                side.insert(entry.price, entry.size) != Some(entry.size)
            };
            if let Some(bid) = entry.best_bid {
                touched |= update(&mut book.best_bid, top(Some(bid), Decimal::ZERO));
            }
            if let Some(ask) = entry.best_ask {
                touched |= update(&mut book.best_ask, top(Some(ask), Decimal::ONE));
            }
            book.hash.clone_from(&entry.hash);
            book.timestamp_ms = change.timestamp_ms;
            if touched {
                note(changed, &entry.asset_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    fn frame(conn_id: u64, seq: u64, json: &str) -> Frame {
        Frame {
            conn_id,
            seq,
            received_at_ns: 0,
            bytes: json.as_bytes().to_vec(),
        }
    }

    fn book_json(asset: &str, bids: &str, asks: &str) -> String {
        format!(
            r#"{{"market":"0xm","asset_id":"{asset}","bids":{bids},"asks":{asks},"hash":"h","timestamp":"1","event_type":"book"}}"#
        )
    }

    fn change_json(asset: &str, price: &str, size: &str, side: &str) -> String {
        format!(
            r#"{{"market":"0xm","price_changes":[{{"asset_id":"{asset}","price":"{price}","size":"{size}","side":"{side}","hash":"h2","best_bid":"0.4","best_ask":"0.6"}}],"timestamp":"2","event_type":"price_change"}}"#
        )
    }

    fn seeded() -> BookSet {
        let mut set = BookSet::new();
        let changed = set.apply_frame(&frame(
            1,
            1,
            &book_json(
                "A",
                r#"[{"price":"0.40","size":"10"},{"price":"0.39","size":"5"}]"#,
                r#"[{"price":"0.60","size":"7"}]"#,
            ),
        ));
        assert_eq!(changed, ["A"]);
        set
    }

    #[test]
    fn price_change_size_is_absolute_and_zero_removes() {
        let mut set = seeded();
        // 10 -> 25 is a set, not 10 + 25.
        assert_eq!(
            set.apply_frame(&frame(1, 2, &change_json("A", "0.4", "25", "BUY"))),
            ["A"]
        );
        assert_eq!(set.book("A").unwrap().bids[&dec("0.40")], dec("25"));
        // Same size again changes nothing.
        assert!(
            set.apply_frame(&frame(1, 3, &change_json("A", "0.4", "25", "BUY")))
                .is_empty()
        );
        // New ask level, then removal of a bid level.
        set.apply_frame(&frame(1, 4, &change_json("A", "0.61", "3", "SELL")));
        set.apply_frame(&frame(1, 5, &change_json("A", "0.39", "0", "BUY")));
        let book = set.book("A").unwrap();
        assert_eq!(book.asks[&dec("0.61")], dec("3"));
        assert!(!book.bids.contains_key(&dec("0.39")));
        assert_eq!(book.bids.len(), 1);
    }

    #[test]
    fn venue_empty_side_sentinels_are_not_a_top_of_book() {
        let mut set = BookSet::new();
        set.apply_frame(&frame(
            1,
            1,
            &book_json("A", r#"[{"price":"0.40","size":"10"}]"#, "[]"),
        ));
        let emptied = r#"{"market":"0xm","price_changes":[{"asset_id":"A","price":"0.4","size":"0","side":"BUY","hash":"h","best_bid":"0","best_ask":"1"}],"timestamp":"2","event_type":"price_change"}"#;
        assert_eq!(set.apply_frame(&frame(1, 2, emptied)), ["A"]);
        let book = set.book("A").unwrap();
        assert_eq!((book.best_bid, book.best_ask), (None, None));
        assert_eq!((book.top_bid(), book.top_ask()), (None, None));
    }

    #[test]
    fn levels_are_keyed_by_value_not_spelling() {
        let mut set = seeded();
        set.apply_frame(&frame(1, 2, &change_json("A", "0.400", "11", "BUY")));
        let book = set.book("A").unwrap();
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[&dec("0.4")], dec("11"));
    }

    #[test]
    fn book_snapshot_replaces_the_sides() {
        let mut set = seeded();
        set.apply_frame(&frame(
            1,
            2,
            &book_json("A", r#"[{"price":"0.30","size":"1"}]"#, "[]"),
        ));
        let book = set.book("A").unwrap();
        assert_eq!(book.bids.len(), 1);
        assert!(book.asks.is_empty());
        assert_eq!(book.best_bid, Some(dec("0.30")));
    }

    #[test]
    fn metadata_events_update_without_touching_levels() {
        let mut set = seeded();
        let best = r#"{"market":"0xm","asset_id":"A","best_bid":"0.41","best_ask":"0.59","spread":"0.18","timestamp":"3","event_type":"best_bid_ask"}"#;
        let tick = r#"{"market":"0xm","asset_id":"A","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"4","event_type":"tick_size_change"}"#;
        assert_eq!(set.apply_frame(&frame(1, 2, best)), ["A"]);
        assert_eq!(set.apply_frame(&frame(1, 3, tick)), ["A"]);
        let book = set.book("A").unwrap();
        assert_eq!(book.best_bid, Some(dec("0.41")));
        assert_eq!(book.tick_size, Some(dec("0.001")));
        assert_eq!(book.bids.len(), 2);
        // An asset that never had a snapshot is not invented from metadata.
        let other = best.replace("\"A\"", "\"Z\"");
        assert!(set.apply_frame(&frame(1, 4, &other)).is_empty());
        assert!(set.book("Z").is_none());
    }

    #[test]
    fn sequence_jump_marks_books_incomplete_until_next_snapshot() {
        let mut set = seeded();
        set.apply_frame(&frame(1, 2, &book_json("B", "[]", "[]")));
        // seq 3 and 4 were dropped by the reader.
        let changed = set.apply_frame(&frame(1, 5, &change_json("A", "0.4", "99", "BUY")));
        assert_eq!(changed.len(), 2);
        let book = set.book("A").unwrap();
        assert!(!book.complete);
        assert_eq!(
            book.bids[&dec("0.40")],
            dec("10"),
            "no level update on a stale base"
        );
        // The next snapshot of A restores only A.
        set.apply_frame(&frame(
            1,
            6,
            &book_json("A", r#"[{"price":"0.2","size":"1"}]"#, "[]"),
        ));
        assert!(set.book("A").unwrap().complete);
        assert!(!set.book("B").unwrap().complete);
    }

    #[test]
    fn new_connection_marks_books_incomplete_even_with_seq_one() {
        let mut set = seeded();
        let marker = r#"{"event_type":"future_thing"}"#;
        let changed = set.apply_frame(&frame(2, 1, marker));
        assert_eq!(changed, ["A"]);
        assert!(!set.book("A").unwrap().complete);
    }

    #[test]
    fn contiguous_frames_never_invalidate() {
        let mut set = seeded();
        for seq in 2..20 {
            set.apply_frame(&frame(1, seq, r#"{"event_type":"future_thing"}"#));
        }
        assert!(set.book("A").unwrap().complete);
    }
}
