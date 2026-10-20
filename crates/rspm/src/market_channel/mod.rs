//! Native Polymarket public market channel.
//!
//! A reader task stamps every received frame with a per-connection sequence and a
//! receipt time before it is queued, so a gap shows up to the consumer as a
//! sequence jump. Parsing ([`Frame::events`]) and book maintenance ([`BookSet`])
//! happen on the consumer side. The module imports nothing from the rest of the
//! crate so it can move to its own crate unchanged.

mod book;
mod channel;
mod event;
mod frame;

pub use self::book::{AssetBook, BookSet};
pub use self::channel::{
    ClockFn, Closed, ConnectionState, MarketChannelConfig, MarketHandle, RawHook, StatsSnapshot,
    spawn,
};
pub use self::event::{
    BestBidAsk, Book, LastTradePrice, Level, MarketEvent, MarketResolved, NewMarket, PriceChange,
    PriceChangeEntry, Side, TickSizeChange,
};
pub use self::frame::Frame;
