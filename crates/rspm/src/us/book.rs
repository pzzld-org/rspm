/*
    Appellation: book <module>
    Contrib: @FL03
*/
use super::{UsBookLevel, UsMarketData, unix_ms};
use chrono::DateTime;
use rust_decimal::{Decimal, prelude::ToPrimitive as _};

/// Best bid and offer of one side of a binary market, with their sizes.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct UsQuote {
    pub bid: Option<f64>,
    pub bid_size: Option<f64>,
    pub ask: Option<f64>,
    pub ask_size: Option<f64>,
}

/// What the caller last held for a market's YES book: its best bid and offer prices and the
/// last-change stamp it carried.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UsBookStamp {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub stamp_ms: i64,
}

/// The top of one market's book: the YES side, its exact NO complement, and the book's last change.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct UsTopOfBook {
    pub yes: UsQuote,
    pub no: UsQuote,
    /// The `transactTime` of the last YES best bid or offer price change, in epoch milliseconds.
    pub last_change_ms: i64,
}

impl UsMarketData {
    /// The venue `transactTime` in epoch nanoseconds. `None` when absent or not RFC 3339.
    pub fn transact_ns(&self) -> Option<i64> {
        DateTime::parse_from_rfc3339(self.transact_time.as_deref()?)
            .ok()?
            .timestamp_nanos_opt()
    }

    /// The YES top from the highest bid and lowest offer, and the NO top as its complement. The
    /// complement is computed in `Decimal` from the venue strings, so `1 - 0.3000` is exactly `0.7`.
    ///
    /// The venue resends the whole book about every 100 ms, so the stamp is the book's last
    /// change: `prior`'s stamp holds while the YES best bid and offer prices are unchanged, as
    /// `f64`s. A size-only change is not a reprice. Without a `prior`, or once a price moves, the
    /// stamp is this frame's `transactTime`, or the local clock when the frame carries none.
    pub fn top_of_book(&self, prior: Option<UsBookStamp>) -> UsTopOfBook {
        let best_bid = self.bids.iter().max_by_key(|level| level.px.value);
        let best_ask = self.offers.iter().min_by_key(|level| level.px.value);
        let price = |level: Option<&UsBookLevel>| level.and_then(|level| level.px.value.to_f64());
        let size = |level: Option<&UsBookLevel>| level.and_then(|level| level.qty.to_f64());
        let complement = |level: Option<&UsBookLevel>| {
            level.and_then(|level| (Decimal::ONE - level.px.value).to_f64())
        };
        let yes = UsQuote {
            bid: price(best_bid),
            bid_size: size(best_bid),
            ask: price(best_ask),
            ask_size: size(best_ask),
        };
        let no = UsQuote {
            bid: complement(best_ask),
            bid_size: size(best_ask),
            ask: complement(best_bid),
            ask_size: size(best_bid),
        };
        let frame_ms = self.transact_ns().map_or_else(unix_ms, |ns| ns / 1_000_000);
        let last_change_ms = prior
            .filter(|prior| prior.bid == yes.bid && prior.ask == yes.ask)
            .map_or(frame_ms, |prior| prior.stamp_ms);
        UsTopOfBook {
            yes,
            no,
            last_change_ms,
        }
    }
}
