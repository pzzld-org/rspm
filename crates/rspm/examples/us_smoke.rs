//! Live read-only smoke for the Polymarket US API. Operators run it by hand.
//!
//! Reads `POLYMARKET_US_KEY_ID` and `POLYMARKET_US_SECRET_KEY` from the
//! environment and never prints them. It reads the current btc-15m Up/Down
//! market, the book, balances, positions, open orders, the HTTP `Date` clock
//! skew, and 10 seconds of the markets WebSocket. With `--preview` it also
//! calls the non-executing preview endpoint for a BUY_LONG at 0.01. This file
//! has no order-placement path.
//!
//! Run: `cargo run --example us_smoke --features us [-- --preview]`

use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rspm::us::{
    Amount, CreateOrderRequest, UsClient, UsMarket, UsMarketsQuery, UsOrderIntent, UsSigner,
    UsSubscription, UsTimeInForce, UsWs, UsWsChannel, UsWsEvent,
};
use rust_decimal::Decimal;
use tokio::time::{Instant, timeout_at};

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let preview = std::env::args().any(|arg| arg == "--preview");
    let key_id = std::env::var("POLYMARKET_US_KEY_ID")?;
    let secret = std::env::var("POLYMARKET_US_SECRET_KEY")?;
    let client = UsClient::authenticated(UsSigner::from_base64(key_id, &secret)?);

    match clock_skew_seconds().await {
        Ok(skew) => println!("clock skew vs HTTP Date header: {skew} s (must be within 30 s)"),
        Err(error) => println!("clock skew: error: {error}"),
    }

    let now = now_epoch();
    let query = UsMarketsQuery {
        categories: vec!["crypto".to_owned()],
        closed: Some(false),
        end_date_min: Some(rfc3339(now)),
        end_date_max: Some(rfc3339(now + 50 * 60)),
        limit: Some(200),
    };
    let rows = step("gateway markets", client.markets(&query).await, |rows| {
        let btc_15m = rows.iter().filter(|market| is_btc_15m(market)).count();
        format!("{} rows, {btc_15m} btc-15m Up/Down", rows.len())
    });
    let Some(market) = rows.and_then(|rows| {
        rows.into_iter()
            .find(|market| is_btc_15m(market) && window_contains(market, now))
    }) else {
        println!("no btc-15m Up/Down market is open now; stopping");
        return Ok(());
    };
    let slug = market.slug.clone();
    println!("current btc-15m market: {slug}");

    step("market", client.market_by_slug(&slug).await, |market| {
        format!(
            "status={} sides={}",
            market.status,
            market.market_sides.len()
        )
    });
    step("book", client.book(&slug).await, |book| {
        let best = book.offers.first().map(|level| level.px.value.to_string());
        format!(
            "bids={} offers={} best_offer={}",
            book.bids.len(),
            book.offers.len(),
            best.unwrap_or_else(|| "none".to_owned())
        )
    });
    step("balances", client.balances().await, |balances| {
        let currencies: Vec<&str> = balances.iter().map(|b| b.currency.as_str()).collect();
        let funded = balances.iter().any(|b| b.buying_power > 0.0);
        format!("currencies={currencies:?} buying_power_positive={funded}")
    });
    step("positions", client.positions(None, None).await, |page| {
        format!("{} rows, eof={:?}", page.positions.len(), page.eof)
    });
    step("open orders", client.open_orders().await, |orders| {
        format!("{} open", orders.len())
    });

    let (ws, mut events) = UsWs::spawn(&client, UsWsChannel::Markets, Duration::from_secs(30))?;
    ws.subscribe(UsSubscription {
        request_id: "smoke-md-1".to_owned(),
        subscription_type: "SUBSCRIPTION_TYPE_MARKET_DATA",
        market_slugs: vec![slug.clone()],
    });
    let mut counts: BTreeMap<&'static str, u64> = BTreeMap::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while let Ok(Some(event)) = timeout_at(deadline, events.recv()).await {
        *counts.entry(event_kind(&event)).or_default() += 1;
    }
    ws.shutdown().await;
    println!("markets WebSocket, 10 s: {counts:?}");

    if preview {
        let request = CreateOrderRequest {
            market_slug: slug.clone(),
            r#type: "ORDER_TYPE_LIMIT",
            price: Amount {
                value: Decimal::new(1, 2),
                currency: "USD".to_owned(),
            },
            quantity: 0.01,
            tif: UsTimeInForce::ImmediateOrCancel,
            intent: UsOrderIntent::BuyLong,
            participate_dont_initiate: false,
            manual_order_indicator: "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            synchronous_execution: None,
            max_block_time: None,
        };
        step(
            "preview (not executed)",
            client.preview_order(&request).await,
            |order| {
                format!(
                    "state={} quantity={:?} price={:?} filled_quantity={:?}",
                    order.state,
                    order.quantity,
                    order.price.as_ref().map(|price| price.value.to_string()),
                    order.filled_quantity
                )
            },
        );
    }
    Ok(())
}

/// Print one step's outcome and return its value. A failed step is reported, not fatal.
fn step<T>(
    label: &str,
    result: rspm::Result<T>,
    summarize: impl FnOnce(&T) -> String,
) -> Option<T> {
    match result {
        Ok(value) => {
            println!("{label}: {}", summarize(&value));
            Some(value)
        }
        Err(error) => {
            println!("{label}: error: {error}");
            None
        }
    }
}

fn is_btc_15m(market: &UsMarket) -> bool {
    market.asset_price_terms.as_ref().is_some_and(|terms| {
        terms.asset.symbol == "btc"
            && terms.horizon == "15m"
            && terms.market_type == "ASSET_PRICE_MARKET_TYPE_UP_DOWN"
    })
}

fn window_contains(market: &UsMarket, now: i64) -> bool {
    market
        .asset_price_terms
        .as_ref()
        .and_then(|terms| {
            Some((
                parse_rfc3339(&terms.window_start)?,
                parse_rfc3339(&terms.window_end)?,
            ))
        })
        .is_some_and(|(start, end)| start <= now && now < end)
}

fn event_kind(event: &UsWsEvent) -> &'static str {
    match event {
        UsWsEvent::Connected { .. } => "Connected",
        UsWsEvent::Disconnected { .. } => "Disconnected",
        UsWsEvent::OrderSnapshot { .. } => "OrderSnapshot",
        UsWsEvent::Execution { .. } => "Execution",
        UsWsEvent::Position { .. } => "Position",
        UsWsEvent::MarketData { .. } => "MarketData",
        UsWsEvent::SubscriptionError { .. } => "SubscriptionError",
        UsWsEvent::Other { .. } => "Other",
    }
}

/// Server time minus local time, in seconds, from the `Date` header of the health endpoint.
async fn clock_skew_seconds() -> anyhow::Result<i64> {
    let response = reqwest::get("https://api.polymarket.us/v1/health").await?;
    let date = response
        .headers()
        .get(reqwest::header::DATE)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("no Date header"))?;
    let server = parse_http_date(date).ok_or_else(|| anyhow::anyhow!("unparseable Date header"))?;
    Ok(server - now_epoch())
}

fn now_epoch() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX)
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Hinnant's `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Inverse of `days_from_civil`: `(year, month, day)`.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `YYYY-MM-DDTHH:MM:SSZ` for an epoch second.
fn rfc3339(epoch: i64) -> String {
    let (year, month, day) = civil_from_days(epoch.div_euclid(86_400));
    let secs = epoch.rem_euclid(86_400);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

/// Parse `YYYY-MM-DDTHH:MM:SSZ`.
fn parse_rfc3339(text: &str) -> Option<i64> {
    let (date, time) = text.strip_suffix('Z')?.split_once('T')?;
    let mut date = date.split('-').map(|part| part.parse::<i64>().ok());
    let (year, month, day) = (date.next()??, date.next()??, date.next()??);
    let mut time = time.split(':').map(|part| part.parse::<i64>().ok());
    let (hour, minute, second) = (time.next()??, time.next()??, time.next()??);
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

/// Parse an IMF-fixdate, `Fri, 30 Oct 2026 16:00:00 GMT`.
fn parse_http_date(text: &str) -> Option<i64> {
    let mut parts = text.split_whitespace();
    parts.next()?;
    let day: i64 = parts.next()?.parse().ok()?;
    let month_name = parts.next()?;
    let month = MONTHS.iter().position(|name| *name == month_name)? as i64 + 1;
    let year: i64 = parts.next()?.parse().ok()?;
    let mut hms = parts
        .next()?
        .split(':')
        .map(|part| part.parse::<i64>().ok());
    let (hour, minute, second) = (hms.next()??, hms.next()??, hms.next()??);
    if parts.next()? != "GMT" {
        return None;
    }
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}
