/*
    Appellation: gamma <module>
    Created At: 2026.08.08:02:44:15
    Contrib: @FL03
*/
//! Gamma API client — market discovery, info, and order book.
//! Polymarket's Gamma API exposes metadata about prediction markets.
//! Unlike the CLOB API it does not require authentication.
//! # Endpoints used:
//! - `GET /markets?slug=<slug>` — fetch market by slug
//! - `GET /events?q=<term>` — search events by keyword (replaces dead `/markets?search=`)
//! - `GET /books/<token_id>` — order book summary for a token
#![cfg(feature = "gamma")]
pub use self::prelude::*;

pub mod client;
mod consts;
pub mod sprint;

mod utils {
    #[doc(inline)]
    pub use self::prelude::*;

    mod convert;

    mod prelude {
        pub use super::convert::*;
    }
}

pub(crate) mod prelude {
    pub use super::client::*;
    pub use super::consts::*;
    pub use super::utils::*;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_base_url_is_preserved_without_a_trailing_slash() {
        let client = GammaClient::from_url("https://gamma.example.test/");
        assert_eq!(client.base_url, "https://gamma.example.test");
    }

    #[test]
    fn gamma_client_base_url() {
        let c = GammaClient::new();
        assert_eq!(c.base_url, GAMMA_BASE);
    }

    /// #2027 — live Gamma responses key the venue min-order-size fields as
    /// `orderMinSize` / `orderPriceMinTickSize`, NOT `minimumOrderSize` /
    /// `minimumTickSize` (the latter never existed in the live API and
    /// silently deserialized to 0.0 on every row). Regression guard: a
    /// fixture keyed with the LIVE names must produce non-zero fields.
    #[test]
    fn market_row_from_gamma_value_reads_live_min_size_keys() {
        let v = serde_json::json!({
            "orderMinSize": 5,
            "orderPriceMinTickSize": 0.01,
        });
        let row = market_row_from_gamma_value(&v);
        assert!(
            (row.minimum_order_size - 5.0).abs() < f64::EPSILON,
            "minimum_order_size not parsed from live `orderMinSize` key: got {}",
            row.minimum_order_size
        );
        assert!(
            (row.minimum_tick_size - 0.01).abs() < 1e-12,
            "minimum_tick_size not parsed from live `orderPriceMinTickSize` key: got {}",
            row.minimum_tick_size
        );
    }
}
