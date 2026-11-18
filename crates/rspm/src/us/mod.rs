/*
    Appellation: us <module>
    Contrib: @FL03
*/
//! Client for the Polymarket US Retail API.
//!
//! Gateway reads (markets, books, settlement) need no credentials. Account,
//! order and portfolio calls and both WebSockets carry an Ed25519 signature
//! over `timestamp || METHOD || path`. This module does no automatic retry.
//!
//! [`UsPlane`] is the data plane: market discovery, the markets WebSocket subscriptions and the
//! strike reads, on the venue's window clock.

mod book;
mod client;
mod market;
mod plane;
mod sign;
mod types;
mod ws;

pub use self::{book::*, client::*, market::*, plane::*, sign::*, types::*, ws::*};

/// REST base for account, order and portfolio calls.
pub const US_API_BASE: &str = "https://api.polymarket.us";
/// Gateway base for public market data.
pub const US_GATEWAY_BASE: &str = "https://gateway.polymarket.us";
/// Private WebSocket path.
pub const US_WS_PRIVATE_PATH: &str = "/v1/ws/private";
/// Markets WebSocket path.
pub const US_WS_MARKETS_PATH: &str = "/v1/ws/markets";
/// Prefix for node token ids that name a US market side.
pub const US_TOKEN_PREFIX: &str = "pmus:";

/// The side of a binary US market.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsOutcome {
    Yes,
    No,
}

/// Node token id for one side of a US market: `pmus:yes:<id>` or `pmus:no:<id>`.
pub fn us_token_id(outcome: UsOutcome, market_side_id: &str) -> String {
    let side = match outcome {
        UsOutcome::Yes => "yes",
        UsOutcome::No => "no",
    };
    format!("{US_TOKEN_PREFIX}{side}:{market_side_id}")
}

/// Split a node token id into its outcome and market-side id. `None` for
/// any other prefix, an unknown side, or an empty id.
pub fn parse_us_token_id(token_id: &str) -> Option<(UsOutcome, &str)> {
    let (side, id) = token_id.strip_prefix(US_TOKEN_PREFIX)?.split_once(':')?;
    let outcome = match side {
        "yes" => UsOutcome::Yes,
        "no" => UsOutcome::No,
        _ => return None,
    };
    (!id.is_empty()).then_some((outcome, id))
}

/// Whether `token_id` names a US market side.
pub fn is_us_token_id(token_id: &str) -> bool {
    token_id.starts_with(US_TOKEN_PREFIX)
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| i64::try_from(elapsed.as_millis()).ok())
        .unwrap_or(i64::MAX)
}
