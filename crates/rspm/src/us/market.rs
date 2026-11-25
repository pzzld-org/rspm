/*
    Appellation: market <module>
    Contrib: @FL03
*/
//! Up/Down window markets: translation, the strike and fee store, and the discovery clock.
use super::{UsMarket, UsOutcome, unix_ms, us_token_id};
use chrono::DateTime;
use rust_decimal::{Decimal, prelude::ToPrimitive as _};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, PoisonError, RwLock},
};

/// The Up/Down market type in `assetPriceTerms.marketType`.
const UP_DOWN: &str = "ASSET_PRICE_MARKET_TYPE_UP_DOWN";
/// Discovery runs 120 s, 60 s and 5 s around each 300 s boundary, on the window clock.
const DISCOVERY_OFFSETS_MS: [i64; 3] = [-120_000, -60_000, 5_000];
const GRID_MS: i64 = 300_000;
/// While a window opening at a boundary is unlisted, discovery retries every 10 s, up to W+120 s.
const LISTING_RETRY_MS: i64 = 10_000;
const LISTING_RETRY_END_MS: i64 = 120_000;
/// Strikes and fees of windows older than this are dropped from the store.
const RETAIN_SECS: i64 = 3600;

/// A window length the data plane discovers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsHorizon {
    FiveMin,
    FifteenMin,
}

impl UsHorizon {
    pub const ALL: [Self; 2] = [Self::FiveMin, Self::FifteenMin];

    /// The venue `horizon`: `5m` or `15m`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FiveMin => "5m",
            Self::FifteenMin => "15m",
        }
    }

    /// The window length in seconds.
    pub const fn secs(self) -> i64 {
        match self {
            Self::FiveMin => 300,
            Self::FifteenMin => 900,
        }
    }

    /// The horizon the venue calls `label`. `None` for any other.
    pub fn parse(label: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|horizon| horizon.as_str() == label)
    }
}

/// One discovered Up/Down window market, keyed by its venue slug.
#[derive(Clone, Debug, PartialEq)]
pub struct UsWindow {
    /// The venue `marketSlug`.
    pub slug: String,
    /// The asset ticker as the venue sends it, one of the plane's configured assets.
    pub asset: String,
    pub horizon: UsHorizon,
    /// Window open, unix seconds.
    pub window_start: i64,
    /// `pmus:yes:<id>` of the long side.
    pub yes_token: String,
    /// `pmus:no:<id>` of the short side.
    pub no_token: String,
    pub min_trade_qty: Option<f64>,
}

impl UsWindow {
    /// Translate one gateway market. `None` for any market that is not an Up/Down window of a
    /// configured asset and horizon, has no parseable window start, or lacks either side.
    pub(super) fn translate(market: &UsMarket, assets: &[String]) -> Option<Self> {
        let terms = market.asset_price_terms.as_ref()?;
        if terms.market_type != UP_DOWN {
            return None;
        }
        let asset = assets.iter().find(|asset| **asset == terms.asset.symbol)?;
        let horizon = UsHorizon::parse(&terms.horizon)?;
        let window_start = DateTime::parse_from_rfc3339(&terms.window_start)
            .ok()?
            .timestamp();
        let yes = market.market_sides.iter().find(|side| side.long)?;
        let no = market.market_sides.iter().find(|side| !side.long)?;
        Some(Self {
            slug: market.slug.clone(),
            asset: asset.clone(),
            horizon,
            window_start,
            yes_token: us_token_id(UsOutcome::Yes, &yes.id),
            no_token: us_token_id(UsOutcome::No, &no.id),
            min_trade_qty: market.minimum_trade_qty,
        })
    }
}

/// The `priceToBeat` of a market, once the venue stamps it.
pub(super) fn market_strike(market: &UsMarket) -> Option<f64> {
    market
        .asset_price_terms
        .as_ref()?
        .price_to_beat
        .as_ref()?
        .value
        .to_f64()
}

/// Strikes and fee coefficients the data plane has read. Clones share one store, so a caller can
/// keep it for the life of the process and read it with no plane running.
#[derive(Clone, Debug, Default)]
pub struct UsStore {
    strikes: Arc<RwLock<BTreeMap<String, (i64, f64)>>>,
    fees: Arc<RwLock<BTreeMap<String, (i64, Decimal)>>>,
}

impl UsStore {
    /// The `priceToBeat` of the market with venue slug `slug`, once it is stamped.
    pub fn strike(&self, slug: &str) -> Option<f64> {
        self.strikes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(slug)
            .map(|(_, price)| *price)
    }

    /// The taker fee schedule `(theta, exponent)` of a `pmus:` token, from its market's discovered
    /// `feeCoefficient`. The US fee is `theta * C * p * (1 - p)`, so the exponent is 1.
    pub fn fee_schedule(&self, token_id: &str) -> Option<(Decimal, u32)> {
        self.fees
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(token_id)
            .map(|(_, theta)| (*theta, 1))
    }

    /// Record a strike. The first value for a market wins, and strikes older than the retention
    /// window are dropped.
    pub(super) fn record_strike(&self, slug: &str, window_start: i64, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        let cutoff = unix_ms() / 1000 - RETAIN_SECS;
        let mut strikes = self.strikes.write().unwrap_or_else(PoisonError::into_inner);
        strikes.retain(|_, (start, _)| *start > cutoff);
        strikes
            .entry(slug.to_owned())
            .or_insert((window_start, price));
    }

    /// Record a market's fee coefficient under both its tokens, since an entry or exit names one.
    /// The latest value wins.
    pub(super) fn record_fee(&self, window: &UsWindow, coefficient: Option<f64>) {
        // Through the shortest text form, so the payload's 0.095 stays exactly 0.095.
        let Some(theta) = coefficient
            // A zero coefficient fails the Paper fee-evidence check and latches Paper closed fleet-wide,
            // so it refuses that market's entries instead (no fee metadata).
            .filter(|theta| theta.is_finite() && *theta > 0.0)
            .and_then(|theta| Decimal::from_str_exact(&theta.to_string()).ok())
        else {
            return;
        };
        let cutoff = unix_ms() / 1000 - RETAIN_SECS;
        let mut fees = self.fees.write().unwrap_or_else(PoisonError::into_inner);
        fees.retain(|_, (start, _)| *start > cutoff);
        for token in [&window.yes_token, &window.no_token] {
            fees.insert(token.clone(), (window.window_start, theta));
        }
    }
}

/// Milliseconds from `now_ms` to the next discovery point on the window clock.
pub(super) fn next_discovery_delay_ms(now_ms: i64) -> u64 {
    let next = DISCOVERY_OFFSETS_MS
        .iter()
        .map(|offset| ((now_ms - offset).div_euclid(GRID_MS) + 1) * GRID_MS + offset)
        .min()
        .unwrap_or(now_ms);
    u64::try_from((next - now_ms).max(1)).unwrap_or(1)
}

/// Milliseconds to the next retry, or `None` when no retry is due. The venue can list a window only
/// at or after its open, so a retry runs while any asset's window opening at the latest 300 s
/// boundary is unlisted. Retries stop at W+120 s, before the next regular pass at W+180 s.
pub(super) fn listing_retry_delay_ms(
    listed: &HashMap<String, UsWindow>,
    assets: &[String],
    now_ms: i64,
) -> Option<u64> {
    let boundary_ms = now_ms.div_euclid(GRID_MS) * GRID_MS;
    if now_ms - boundary_ms + LISTING_RETRY_MS > LISTING_RETRY_END_MS {
        return None;
    }
    let boundary = boundary_ms / 1000;
    let unlisted = UsHorizon::ALL.into_iter().any(|horizon| {
        boundary % horizon.secs() == 0
            && assets.iter().any(|asset| {
                !listed.values().any(|entry| {
                    entry.asset == *asset
                        && entry.horizon == horizon
                        && entry.window_start == boundary
                })
            })
    });
    unlisted.then(|| u64::try_from(LISTING_RETRY_MS).unwrap_or(1))
}
