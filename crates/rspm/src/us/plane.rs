/*
    Appellation: plane <module>
    Contrib: @FL03
*/
//! The Polymarket US data plane.
//!
//! One task discovers the Up/Down window markets on the window clock, keeps the markets
//! WebSocket subscribed to the listed ones, and reads each window's `priceToBeat` into a
//! [`UsStore`]. It reports what it saw as [`UsPlaneEvent`]s: every venue body and frame exactly
//! as received, so a caller can capture them raw, and the typed results.
use super::{
    UsClient, UsMarket, UsMarketData, UsStore, UsSubscription, UsWindow, UsWs, UsWsChannel,
    UsWsEvent,
    market::{listing_retry_delay_ms, market_strike, next_discovery_delay_ms},
    unix_ms,
};
use crate::Result;
use chrono::{DateTime, SecondsFormat};
use core::time::Duration;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
    time::{Instant, sleep, sleep_until},
};

/// Subscription type for book and stats updates on the markets WebSocket.
const MARKET_DATA: &str = "SUBSCRIPTION_TYPE_MARKET_DATA";
/// Windows whose markets list every window within 50 min of now.
const LOOKAHEAD_SECS: i64 = 50 * 60;
const DISCOVERY_LIMIT: u32 = 200;
/// Pages read per discovery pass; the venue lists hourly crypto markets beside the windows.
const DISCOVERY_MAX_PAGES: u32 = 5;
/// After the venue rejects a subscription, discovery runs again within this delay to resubscribe.
const RESUBSCRIBE_AFTER: Duration = Duration::from_secs(10);
/// Strike reads start 2 s after W, poll every 2 s, and stop at W+60 s.
const STRIKE_FIRST_SECS: i64 = 2;
const STRIKE_POLL: Duration = Duration::from_secs(2);
const WS_IDLE: Duration = Duration::from_secs(60);
const EVENT_CAPACITY: usize = 1024;

/// A window's strike is counted absent once it is still unstamped this many seconds after W.
pub const STRIKE_DEADLINE_SECS: i64 = 60;

/// What the data plane reports, in the order it happened.
#[derive(Clone, Debug)]
pub enum UsPlaneEvent {
    /// A markets-WebSocket frame exactly as received: books, rejections and heartbeats alike.
    /// `market_slug` and `venue_ts_ns` are set for a book.
    Frame {
        market_slug: Option<String>,
        venue_ts_ns: Option<i64>,
        raw: String,
    },
    /// A discovery page body exactly as received, before it is decoded. `offset` is the page's
    /// position in the pass.
    Page {
        offset: u32,
        raw: String,
    },
    /// A strike read's body exactly as received, before it is decoded.
    Market {
        slug: String,
        raw: String,
    },
    /// An applied discovery pass: every listed window market, in venue order.
    Listed(Vec<UsWindow>),
    /// A listed market's book. Unlisted markets' books are only captured as frames.
    Book {
        yes_token: String,
        no_token: String,
        data: UsMarketData,
    },
    Connected {
        generation: u64,
    },
    Disconnected {
        generation: u64,
        reason: String,
    },
    /// The venue rejected a subscription. The plane forgets it and resubscribes its markets soon.
    Rejected {
        request_id: String,
        error: String,
    },
    /// A discovery request failed. The last-good listing stays.
    DiscoveryFailed(String),
    /// A discovery body did not decode. The last-good listing stays.
    DiscoveryUndecodable(String),
    /// Discovery stopped at its page limit, so later markets are unlisted.
    DiscoveryTruncated {
        offset: u32,
    },
    StrikeReadFailed {
        slug: String,
        error: String,
    },
    /// A window's `priceToBeat` was still unstamped at W+[`STRIKE_DEADLINE_SECS`].
    StrikeAbsent {
        asset: String,
        window_start: i64,
        slug: String,
    },
    /// A strike watch panicked or was cancelled.
    StrikeWatchEnded(String),
}

/// A running data plane. Dropping the handle stops it.
pub struct UsPlane {
    task: JoinHandle<()>,
}

impl Drop for UsPlane {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl UsPlane {
    /// Spawn the plane and return it with its event receiver. `assets` are the venue tickers to
    /// discover, matched exactly; `store` receives the strikes and fees it reads. The event channel
    /// closes when the markets WebSocket task ends, and the plane stops when the receiver is dropped.
    /// A consumer that stops reading stalls the plane.
    ///
    /// Returns `InvalidParam` when the client has no signer. Must be called inside a Tokio runtime.
    pub fn spawn(
        client: &UsClient,
        assets: Vec<String>,
        store: UsStore,
    ) -> Result<(Self, mpsc::Receiver<UsPlaneEvent>)> {
        let (ws, ws_events) = UsWs::spawn(client, UsWsChannel::Markets, WS_IDLE)?;
        let (events, receiver) = mpsc::channel(EVENT_CAPACITY);
        let plane = Plane {
            client: client.clone(),
            assets,
            store,
            events,
        };
        let task = tokio::spawn(plane.run(ws, ws_events));
        Ok((Self { task }, receiver))
    }
}

/// `GET /v1/markets` body. The venue wraps the list in `markets`. A market that does not decode
/// (hourly range, above/below) is not one this plane trades, so the page keeps them raw.
#[derive(Deserialize)]
struct MarketsPage {
    markets: Vec<serde_json::Value>,
}

/// `GET /v1/market/slug/{slug}` body.
#[derive(Deserialize)]
struct MarketEnvelope {
    market: UsMarket,
}

#[derive(Default)]
struct State {
    listed: HashMap<String, UsWindow>,
    // Live subscriptions on the markets WebSocket: request id and the market slugs it covers.
    subscriptions: Vec<(String, BTreeSet<String>)>,
    // Sequence that keeps a new subscription's ids apart from earlier ones.
    subscription_seq: u64,
    strike_watch: BTreeSet<String>,
    strikes: JoinSet<()>,
}

#[derive(Clone)]
struct Plane {
    client: UsClient,
    assets: Vec<String>,
    store: UsStore,
    events: mpsc::Sender<UsPlaneEvent>,
}

impl Plane {
    async fn emit(&self, event: UsPlaneEvent) {
        let _ = self.events.send(event).await;
    }

    async fn run(self, ws: UsWs, mut ws_events: mpsc::Receiver<UsWsEvent>) {
        let mut state = State::default();
        let mut next = Instant::now();
        loop {
            tokio::select! {
                () = self.events.closed() => return,
                () = sleep_until(next) => {
                    self.discover(&ws, &mut state).await;
                    let now = unix_ms();
                    let delay = listing_retry_delay_ms(&state.listed, &self.assets, now)
                        .unwrap_or_else(|| next_discovery_delay_ms(now));
                    next = Instant::now() + Duration::from_millis(delay);
                    // A failed pass after a rejection leaves listed markets unsubscribed: retry soon.
                    if state.listed.keys().any(|slug| {
                        !state.subscriptions.iter().any(|(_, slugs)| slugs.contains(slug))
                    }) {
                        next = next.min(Instant::now() + RESUBSCRIBE_AFTER);
                    }
                }
                event = ws_events.recv() => match event {
                    Some(event) => {
                        let rejected = self.handle(event, &state.listed).await;
                        // A rejected live subscription leaves its markets unbooked: forget it (so the
                        // socket does not replay it) and let discovery resubscribe them soon.
                        if let Some(request_id) = rejected
                            && let Some(index) =
                                state.subscriptions.iter().position(|(id, _)| *id == request_id)
                        {
                            state.subscriptions.remove(index);
                            ws.unsubscribe(&request_id);
                            next = next.min(Instant::now() + RESUBSCRIBE_AFTER);
                        }
                    }
                    None => return,
                },
                Some(joined) = state.strikes.join_next(), if !state.strikes.is_empty() => {
                    if let Err(error) = joined {
                        self.emit(UsPlaneEvent::StrikeWatchEnded(error.to_string())).await;
                    }
                }
            }
        }
    }

    /// Report one markets-WebSocket event. Returns the request id of a rejected subscription.
    async fn handle(&self, event: UsWsEvent, listed: &HashMap<String, UsWindow>) -> Option<String> {
        let frame = |raw| UsPlaneEvent::Frame {
            market_slug: None,
            venue_ts_ns: None,
            raw,
        };
        match event {
            UsWsEvent::Connected { generation } => {
                self.emit(UsPlaneEvent::Connected { generation }).await;
            }
            UsWsEvent::Disconnected { generation, reason } => {
                self.emit(UsPlaneEvent::Disconnected { generation, reason })
                    .await;
            }
            UsWsEvent::SubscriptionError {
                request_id,
                error,
                raw,
            } => {
                self.emit(UsPlaneEvent::Rejected {
                    request_id: request_id.clone(),
                    error,
                })
                .await;
                self.emit(frame(raw)).await;
                return Some(request_id);
            }
            UsWsEvent::MarketData { data, raw } => {
                self.emit(UsPlaneEvent::Frame {
                    market_slug: Some(data.market_slug.clone()),
                    venue_ts_ns: data.transact_ns(),
                    raw,
                })
                .await;
                if let Some(window) = listed.get(&data.market_slug) {
                    self.emit(UsPlaneEvent::Book {
                        yes_token: window.yes_token.clone(),
                        no_token: window.no_token.clone(),
                        data,
                    })
                    .await;
                }
            }
            UsWsEvent::OrderSnapshot { raw, .. }
            | UsWsEvent::Execution { raw, .. }
            | UsWsEvent::Position { raw }
            | UsWsEvent::Other { raw } => self.emit(frame(raw)).await,
        }
        None
    }

    /// Discover the window markets and keep the WebSocket subscriptions and strike watches in
    /// step. A failed discovery keeps the last-good listing. Each body is reported before it is
    /// decoded, so a body that fails to decode is still kept.
    async fn discover(&self, ws: &UsWs, state: &mut State) {
        let now = unix_ms() / 1000;
        // The pairs `UsClient::markets` sends, plus the page offset, read raw so the body is reported.
        let query = [
            ("categories", "crypto".to_owned()),
            ("closed", "false".to_owned()),
            ("endDateMin", rfc3339(now)),
            ("endDateMax", rfc3339(now + LOOKAHEAD_SECS)),
            ("limit", DISCOVERY_LIMIT.to_string()),
        ];
        let mut markets = Vec::new();
        let mut offset = 0;
        loop {
            let mut paged = query.to_vec();
            paged.push(("offset", offset.to_string()));
            let text = match self
                .client
                .gateway_get_text(&["v1", "markets"], &paged)
                .await
            {
                Ok(text) => text,
                Err(error) => {
                    self.emit(UsPlaneEvent::DiscoveryFailed(error.to_string()))
                        .await;
                    return;
                }
            };
            let parsed = serde_json::from_str::<MarketsPage>(&text);
            self.emit(UsPlaneEvent::Page { offset, raw: text }).await;
            let page = match parsed {
                Ok(page) => page.markets,
                Err(error) => {
                    self.emit(UsPlaneEvent::DiscoveryUndecodable(error.to_string()))
                        .await;
                    return;
                }
            };
            let full = page.len() == DISCOVERY_LIMIT as usize;
            markets.extend(
                page.into_iter()
                    .filter_map(|market| serde_json::from_value::<UsMarket>(market).ok()),
            );
            if !full {
                break;
            }
            if offset + DISCOVERY_LIMIT >= DISCOVERY_LIMIT * DISCOVERY_MAX_PAGES {
                self.emit(UsPlaneEvent::DiscoveryTruncated { offset }).await;
                break;
            }
            offset += DISCOVERY_LIMIT;
        }

        let mut windows = Vec::new();
        for market in &markets {
            let Some(window) = UsWindow::translate(market, &self.assets) else {
                continue;
            };
            if let Some(price) = market_strike(market) {
                self.store
                    .record_strike(&window.slug, window.window_start, price);
            }
            self.store.record_fee(&window, market.fee_coefficient);
            windows.push(window);
        }
        let listed: HashMap<String, UsWindow> = windows
            .iter()
            .map(|window| (window.slug.clone(), window.clone()))
            .collect();
        self.emit(UsPlaneEvent::Listed(windows)).await;

        // The venue caps a connection at about 10 subscriptions and rejects a subscription naming a
        // market another live subscription covers. So each window's markets share one subscription,
        // dropped once none of them is listed (the venue lists only open windows), and only markets no
        // live subscription covers are subscribed. A listed market never leaves its subscription, so
        // its book stays continuous across window rolls, and since unsubscribes go out first, only
        // listed markets are subscribed, whether the cap counts subscriptions or markets.
        state.subscriptions.retain(|(request_id, slugs)| {
            let live = slugs.iter().any(|slug| listed.contains_key(slug));
            if !live {
                ws.unsubscribe(request_id);
            }
            live
        });
        let covered: BTreeSet<&String> = state
            .subscriptions
            .iter()
            .flat_map(|(_, slugs)| slugs)
            .collect();
        let mut uncovered: BTreeMap<(i64, &str), BTreeSet<String>> = BTreeMap::new();
        for (slug, entry) in &listed {
            if !covered.contains(slug) {
                uncovered
                    .entry((entry.window_start, entry.horizon.as_str()))
                    .or_default()
                    .insert(slug.clone());
            }
        }
        if !uncovered.is_empty() {
            state.subscription_seq += 1;
        }
        for (group, market_slugs) in uncovered.into_values().enumerate() {
            let request_id = subscription_id(state.subscription_seq, group);
            ws.subscribe(UsSubscription {
                request_id: request_id.clone(),
                subscription_type: MARKET_DATA,
                market_slugs: market_slugs.iter().cloned().collect(),
            });
            state.subscriptions.push((request_id, market_slugs));
        }

        state.strike_watch.retain(|slug| listed.contains_key(slug));
        for (slug, window) in &listed {
            if !state.strike_watch.contains(slug) && self.store.strike(slug).is_none() {
                state.strike_watch.insert(slug.clone());
                state.strikes.spawn(self.clone().strike_watch(
                    slug.clone(),
                    window.asset.clone(),
                    window.window_start,
                ));
            }
        }
        state.listed = listed;
    }

    /// Re-read one market's `priceToBeat` from W+2 s every 2 s until W+60 s. Reports the window as
    /// absent once it is still unstamped at W+60 s.
    async fn strike_watch(self, slug: String, asset: String, window_start: i64) {
        sleep_until_unix(window_start + STRIKE_FIRST_SECS).await;
        let deadline = window_start + STRIKE_DEADLINE_SECS;
        loop {
            match self.read_market(&slug).await {
                Ok(market) => {
                    if let Some(price) = market_strike(&market) {
                        self.store.record_strike(&slug, window_start, price);
                        return;
                    }
                }
                Err(error) => {
                    self.emit(UsPlaneEvent::StrikeReadFailed {
                        slug: slug.clone(),
                        error,
                    })
                    .await;
                }
            }
            if unix_ms() / 1000 >= deadline {
                self.emit(UsPlaneEvent::StrikeAbsent {
                    asset,
                    window_start,
                    slug,
                })
                .await;
                return;
            }
            sleep(STRIKE_POLL).await;
        }
    }

    /// `GET /v1/market/slug/{slug}`, with the raw body reported before it is decoded.
    async fn read_market(&self, slug: &str) -> std::result::Result<UsMarket, String> {
        let text = self
            .client
            .gateway_get_text(&["v1", "market", "slug", slug], &[])
            .await
            .map_err(|error| error.to_string())?;
        let parsed = serde_json::from_str::<MarketEnvelope>(&text);
        self.emit(UsPlaneEvent::Market {
            slug: slug.to_owned(),
            raw: text,
        })
        .await;
        parsed
            .map(|envelope| envelope.market)
            .map_err(|error| error.to_string())
    }
}

/// Request id of one chunk of subscription set `seq`.
fn subscription_id(seq: u64, chunk: usize) -> String {
    format!("md:set:{seq}:{chunk}")
}

/// `secs` as the venue's RFC 3339 UTC query value.
fn rfc3339(secs: i64) -> String {
    DateTime::from_timestamp(secs, 0)
        .unwrap_or_default()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

async fn sleep_until_unix(unix_secs: i64) {
    if let Ok(delay_ms) = u64::try_from(unix_secs * 1000 - unix_ms()) {
        sleep(Duration::from_millis(delay_ms)).await;
    }
}
