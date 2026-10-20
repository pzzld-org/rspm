use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error as _};
use serde_json::{Value, value::RawValue};
use std::str::FromStr;

/// Aggressor or resting side as the venue spells it.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash)]
pub enum Side {
    #[serde(rename = "BUY")]
    Buy,
    #[serde(rename = "SELL")]
    Sell,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Level {
    pub price: Decimal,
    pub size: Decimal,
}

/// Full book for one asset. The subscribe snapshot also carries `tick_size` and
/// `last_trade_price`; later object-form books do not.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Book {
    pub market: String,
    pub asset_id: String,
    #[serde(default)]
    pub bids: Vec<Level>,
    #[serde(default)]
    pub asks: Vec<Level>,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub tick_size: Option<Decimal>,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub last_trade_price: Option<Decimal>,
}

/// `size` is the new absolute size at `price`; `0` removes the level (checked
/// against the live capture, see `BookSet`).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PriceChangeEntry {
    pub asset_id: String,
    pub price: Decimal,
    pub size: Decimal,
    pub side: Side,
    #[serde(default)]
    pub hash: Option<String>,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub best_ask: Option<Decimal>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct PriceChange {
    pub market: String,
    pub price_changes: Vec<PriceChangeEntry>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct BestBidAsk {
    pub market: String,
    pub asset_id: String,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub best_bid: Option<Decimal>,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub best_ask: Option<Decimal>,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub spread: Option<Decimal>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct TickSizeChange {
    pub market: String,
    pub asset_id: String,
    pub old_tick_size: Decimal,
    pub new_tick_size: Decimal,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct LastTradePrice {
    pub market: String,
    pub asset_id: String,
    pub price: Decimal,
    #[serde(default, deserialize_with = "opt_decimal")]
    pub size: Option<Decimal>,
    // The venue has been seen to send this blank.
    #[serde(default, deserialize_with = "opt_decimal")]
    pub fee_rate_bps: Option<Decimal>,
    #[serde(default)]
    pub side: Option<Side>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
    #[serde(default)]
    pub transaction_hash: Option<String>,
}

/// Only sent under `custom_feature_enabled`. The capture did not contain one, so
/// the shape follows the venue SDK types and everything past the identifiers is
/// optional.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct NewMarket {
    pub id: String,
    pub market: String,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub slug: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, alias = "asset_ids")]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub outcomes: Vec<String>,
    #[serde(default)]
    pub event_message: Option<Value>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct MarketResolved {
    pub id: String,
    pub market: String,
    #[serde(default, alias = "asset_ids")]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub winning_asset_id: Option<String>,
    #[serde(default)]
    pub winning_outcome: Option<String>,
    #[serde(default)]
    pub event_message: Option<Value>,
    #[serde(rename = "timestamp", deserialize_with = "millis")]
    pub timestamp_ms: i64,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// One market event. `Other` keeps the exact raw JSON text of an element whose
/// type is unknown or whose body does not parse, so nothing is dropped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarketEvent {
    Book(Book),
    PriceChange(PriceChange),
    BestBidAsk(BestBidAsk),
    TickSizeChange(TickSizeChange),
    LastTradePrice(LastTradePrice),
    NewMarket(NewMarket),
    MarketResolved(MarketResolved),
    Other { raw: String },
}

/// Timestamps arrive as decimal strings; accept a bare integer too.
fn millis<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Millis {
        Text(String),
        Int(i64),
    }
    match Millis::deserialize(deserializer)? {
        Millis::Int(value) => Ok(value),
        Millis::Text(text) => text.trim().parse().map_err(D::Error::custom),
    }
}

/// Decimal string that may be absent, null or blank.
fn opt_decimal<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Decimal>, D::Error> {
    match Option::<String>::deserialize(deserializer)? {
        None => Ok(None),
        Some(text) if text.trim().is_empty() => Ok(None),
        Some(text) => Decimal::from_str(text.trim())
            .map(Some)
            .map_err(D::Error::custom),
    }
}

#[derive(Deserialize)]
struct Tag {
    event_type: Option<String>,
}

fn other(raw: &RawValue) -> MarketEvent {
    MarketEvent::Other {
        raw: raw.get().to_owned(),
    }
}

fn typed<T: serde::de::DeserializeOwned>(
    raw: &RawValue,
    wrap: fn(T) -> MarketEvent,
) -> MarketEvent {
    serde_json::from_str(raw.get()).map_or_else(|_| other(raw), wrap)
}

fn parse_event(raw: &RawValue) -> MarketEvent {
    let tag = serde_json::from_str::<Tag>(raw.get())
        .ok()
        .and_then(|tag| tag.event_type);
    match tag.as_deref() {
        Some("book") => typed(raw, MarketEvent::Book),
        Some("price_change") => typed(raw, MarketEvent::PriceChange),
        Some("best_bid_ask") => typed(raw, MarketEvent::BestBidAsk),
        Some("tick_size_change") => typed(raw, MarketEvent::TickSizeChange),
        Some("last_trade_price") => typed(raw, MarketEvent::LastTradePrice),
        Some("new_market") => typed(raw, MarketEvent::NewMarket),
        Some("market_resolved") => typed(raw, MarketEvent::MarketResolved),
        _ => other(raw),
    }
}

pub(super) fn parse_frame(bytes: &[u8]) -> Vec<MarketEvent> {
    let whole_frame_other = || {
        vec![MarketEvent::Other {
            raw: String::from_utf8_lossy(bytes).into_owned(),
        }]
    };
    let Ok(root) = serde_json::from_slice::<&RawValue>(bytes) else {
        return whole_frame_other();
    };
    if root.get().starts_with('[') {
        match serde_json::from_str::<Vec<&RawValue>>(root.get()) {
            Ok(elements) => elements.into_iter().map(parse_event).collect(),
            Err(_) => whole_frame_other(),
        }
    } else {
        vec![parse_event(root)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        Decimal::from_str(text).unwrap()
    }

    fn one(frame: &str) -> MarketEvent {
        let mut events = parse_frame(frame.as_bytes());
        assert_eq!(events.len(), 1, "{frame}");
        events.remove(0)
    }

    #[test]
    fn book_parses_levels_as_decimals_and_optional_snapshot_fields() {
        let MarketEvent::Book(book) = one(
            r#"{"market":"0xm1","asset_id":"111","bids":[{"price":"0.01","size":"970"},{"price":"0.02","size":"1158.17"}],"asks":[{"price":"0.99","size":"1118"}],"hash":"h1","timestamp":"1700000000001","event_type":"book"}"#,
        ) else {
            panic!("expected book");
        };
        assert_eq!(book.bids[1].size, dec("1158.17"));
        assert_eq!(book.timestamp_ms, 1_700_000_000_001);
        assert_eq!(book.tick_size, None);

        let MarketEvent::Book(snapshot) = one(
            r#"{"market":"0xm1","asset_id":"111","timestamp":"5","hash":"h","bids":[],"asks":[],"tick_size":"0.001","event_type":"book","last_trade_price":"0.999"}"#,
        ) else {
            panic!("expected book");
        };
        assert_eq!(snapshot.tick_size, Some(dec("0.001")));
        assert_eq!(snapshot.last_trade_price, Some(dec("0.999")));
    }

    #[test]
    fn price_change_keeps_every_entry_with_its_side() {
        let MarketEvent::PriceChange(change) = one(
            r#"{"market":"0xm1","price_changes":[{"asset_id":"111","price":"0.88","size":"31","side":"BUY","hash":"a","best_bid":"0.95","best_ask":"0.96"},{"asset_id":"222","price":"0.12","size":"0","side":"SELL","hash":"b","best_bid":"0.04","best_ask":"0.05"}],"timestamp":"1700000000002","event_type":"price_change"}"#,
        ) else {
            panic!("expected price_change");
        };
        assert_eq!(change.price_changes.len(), 2);
        assert_eq!(change.price_changes[0].side, Side::Buy);
        assert_eq!(change.price_changes[1].size, Decimal::ZERO);
        assert_eq!(change.price_changes[1].best_ask, Some(dec("0.05")));
    }

    #[test]
    fn remaining_event_types_parse() {
        assert!(matches!(
            one(
                r#"{"market":"0xm1","asset_id":"111","best_bid":"0.96","best_ask":"1","spread":"0.04","timestamp":"3","event_type":"best_bid_ask"}"#
            ),
            MarketEvent::BestBidAsk(b) if b.best_ask == Some(dec("1"))
        ));
        assert!(matches!(
            one(
                r#"{"market":"0xm1","asset_id":"111","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"4","event_type":"tick_size_change"}"#
            ),
            MarketEvent::TickSizeChange(t) if t.new_tick_size == dec("0.001")
        ));
        assert!(matches!(
            one(
                r#"{"market":"0xm1","asset_id":"111","price":"0.96","size":"20","fee_rate_bps":"0","side":"BUY","timestamp":"5","event_type":"last_trade_price","transaction_hash":"0xabc"}"#
            ),
            MarketEvent::LastTradePrice(t) if t.side == Some(Side::Buy) && t.fee_rate_bps == Some(Decimal::ZERO)
        ));
        assert!(matches!(
            one(
                r#"{"id":"9","question":"q","market":"0xm1","slug":"s","description":"d","assets_ids":["111","222"],"outcomes":["Up","Down"],"timestamp":"6","event_type":"new_market"}"#
            ),
            MarketEvent::NewMarket(n) if n.assets_ids == ["111", "222"]
        ));
        assert!(matches!(
            one(
                r#"{"id":"9","market":"0xm1","assets_ids":["111","222"],"winning_asset_id":"222","winning_outcome":"Down","event_message":null,"timestamp":"7","event_type":"market_resolved","tags":["a"]}"#
            ),
            MarketEvent::MarketResolved(r) if r.winning_outcome.as_deref() == Some("Down")
        ));
    }

    #[test]
    fn blank_fee_rate_still_parses_as_a_trade() {
        assert!(matches!(
            one(
                r#"{"market":"0xm1","asset_id":"111","price":"0.5","size":"1","fee_rate_bps":"","side":"SELL","timestamp":"5","event_type":"last_trade_price"}"#
            ),
            MarketEvent::LastTradePrice(t) if t.fee_rate_bps.is_none()
        ));
    }

    #[test]
    fn array_frame_with_trailing_newline_yields_one_event_per_element() {
        let frame = "[{\"market\":\"0xm1\",\"asset_id\":\"111\",\"bids\":[],\"asks\":[],\"timestamp\":\"1\",\"event_type\":\"book\"},{\"market\":\"0xm1\",\"asset_id\":\"222\",\"bids\":[],\"asks\":[],\"timestamp\":\"1\",\"event_type\":\"book\"}]\n";
        let events = parse_frame(frame.as_bytes());
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| matches!(e, MarketEvent::Book(_))));
        assert!(parse_frame(b"[]").is_empty());
    }

    #[test]
    fn unknown_type_keeps_exact_raw_text() {
        let raw = r#"{ "event_type" : "future_thing", "x":[1, 2] }"#;
        assert_eq!(
            one(raw),
            MarketEvent::Other {
                raw: raw.to_owned()
            }
        );
    }

    #[test]
    fn known_type_with_a_bad_body_is_kept_as_other_not_dropped() {
        let raw = r#"{"market":"0xm1","asset_id":"111","price":"not-a-number","timestamp":"5","event_type":"last_trade_price"}"#;
        assert_eq!(
            one(raw),
            MarketEvent::Other {
                raw: raw.to_owned()
            }
        );
    }

    #[test]
    fn non_json_and_non_object_frames_are_kept_as_other() {
        assert_eq!(
            parse_frame(b"INVALID OPERATION"),
            [MarketEvent::Other {
                raw: "INVALID OPERATION".to_owned()
            }]
        );
        assert_eq!(
            parse_frame(br#"[1,{"event_type":"book"}]"#).len(),
            2,
            "every array element is accounted for"
        );
    }
}
