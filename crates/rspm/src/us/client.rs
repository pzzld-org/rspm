/*
    Appellation: client <module>
    Contrib: @FL03
*/
use super::{
    CreateOrderRequest, CreateOrderResponse, US_API_BASE, US_GATEWAY_BASE, UsActivitiesPage,
    UsActivitiesQuery, UsBalance, UsMarket, UsMarketData, UsMarketsQuery, UsOrder, UsPositionsPage,
    UsSigner,
};
use crate::{Error, Result};
use core::time::Duration;
use reqwest::{
    Method, StatusCode,
    header::{CONTENT_TYPE, RETRY_AFTER},
};
use rust_decimal::Decimal;
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IgnoredAny},
};
use std::sync::Arc;
use url::Url;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Latency stopgap message. It is not a throttle, so it never maps to `RateLimited`.
const GLOBAL_RATE_LIMIT: &str = "Global Rate Limit Exceeded";

/// Client for the Polymarket US Retail API.
///
/// Gateway reads need no credentials. Account, order and portfolio calls are
/// signed per request. No request is retried: a POST may have reached the
/// venue, and the API has no idempotency key.
#[derive(Clone, Debug)]
pub struct UsClient {
    http: reqwest::Client,
    api_base: String,
    gateway_base: String,
    signer: Option<Arc<UsSigner>>,
}

impl UsClient {
    /// A client for public gateway reads. Authenticated calls return `InvalidParam`.
    pub fn public() -> Self {
        Self::from_parts(None)
    }

    /// A client that signs every authenticated request with `signer`.
    pub fn authenticated(signer: UsSigner) -> Self {
        Self::from_parts(Some(Arc::new(signer)))
    }

    fn from_parts(signer: Option<Arc<UsSigner>>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .retry(reqwest::retry::never())
            .build()
            .expect("reqwest client with the US request policy");
        Self {
            http,
            api_base: US_API_BASE.to_owned(),
            gateway_base: US_GATEWAY_BASE.to_owned(),
            signer,
        }
    }

    /// Point both hosts at test servers.
    ///
    /// Signed requests stay valid for 30 s, so production never sends them
    /// to a configurable host.
    #[cfg(feature = "test-utils")]
    pub fn with_bases(self, api: Url, gateway: Url) -> Self {
        Self {
            api_base: api.to_string(),
            gateway_base: gateway.to_string(),
            ..self
        }
    }

    // public gateway

    /// `GET /v1/markets`.
    pub async fn markets(&self, q: &UsMarketsQuery) -> Result<Vec<UsMarket>> {
        let text = self
            .gateway_get_text(&["v1", "markets"], &market_query(q))
            .await?;
        Ok(decode::<MarketsPage>(&text)?.markets)
    }

    /// `GET /v1/market/slug/{slug}`.
    pub async fn market_by_slug(&self, slug: &str) -> Result<UsMarket> {
        let text = self
            .gateway_get_text(&["v1", "market", "slug", slug], &[])
            .await?;
        Ok(decode::<MarketEnvelope>(&text)?.market)
    }

    /// `GET /v1/markets/{slug}/book`.
    pub async fn book(&self, slug: &str) -> Result<UsMarketData> {
        let text = self
            .gateway_get_text(&["v1", "markets", slug, "book"], &[])
            .await?;
        Ok(decode::<BookEnvelope>(&text)?.market_data)
    }

    /// `GET /v1/markets/{slug}/settlement`. Returns `None` on 404, which the
    /// API uses for both "not found" and "not settled yet".
    pub async fn settlement(&self, slug: &str) -> Result<Option<Decimal>> {
        let text = match self
            .gateway_get_text(&["v1", "markets", slug, "settlement"], &[])
            .await
        {
            Err(Error::NotFound) => return Ok(None),
            result => result?,
        };
        Ok(decode::<SettlementEnvelope>(&text)?.settlement)
    }

    /// `GET` a gateway path and return the body as text. The body goes through
    /// `Response::text`, so invalid UTF-8 is replaced, not kept byte for byte.
    /// Status and error mapping are the same as the typed gateway calls.
    pub async fn gateway_get_text(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
    ) -> Result<String> {
        let url = endpoint(&self.gateway_base, segments, query)?;
        let response = self.request(Method::GET, &url, false)?.send().await?;
        read_text(response).await
    }

    // authenticated

    /// `GET /v1/account/balances`.
    pub async fn balances(&self) -> Result<Vec<UsBalance>> {
        let url = endpoint(&self.api_base, &["v1", "account", "balances"], &[])?;
        Ok(self.get::<BalancesEnvelope>(url, true).await?.balances)
    }

    /// `GET /v1/portfolio/positions`. `market` and `cursor` go in the query,
    /// which is not signed.
    pub async fn positions(
        &self,
        market: Option<&str>,
        cursor: Option<&str>,
    ) -> Result<UsPositionsPage> {
        let mut query = Vec::new();
        if let Some(market) = market {
            query.push(("market", market.to_owned()));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor.to_owned()));
        }
        let url = endpoint(&self.api_base, &["v1", "portfolio", "positions"], &query)?;
        self.get(url, true).await
    }

    /// `GET /v1/portfolio/activities`. The filters go in the query, which is not signed.
    pub async fn activities(&self, q: &UsActivitiesQuery) -> Result<UsActivitiesPage> {
        let mut query: Vec<(&str, String)> = q
            .types
            .iter()
            .map(|kind| ("types", kind.as_str().to_owned()))
            .collect();
        if let Some(slug) = &q.market_slug {
            query.push(("marketSlug", slug.clone()));
        }
        if let Some(limit) = q.limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(cursor) = &q.cursor {
            query.push(("cursor", cursor.clone()));
        }
        if let Some(order) = q.sort_order {
            query.push(("sortOrder", order.as_str().to_owned()));
        }
        let url = endpoint(&self.api_base, &["v1", "portfolio", "activities"], &query)?;
        self.get(url, true).await
    }

    /// `GET /v1/orders/open`.
    pub async fn open_orders(&self) -> Result<Vec<UsOrder>> {
        let url = endpoint(&self.api_base, &["v1", "orders", "open"], &[])?;
        Ok(self.get::<OrdersEnvelope>(url, true).await?.orders)
    }

    /// `GET /v1/order/{id}`.
    pub async fn order(&self, order_id: &str) -> Result<UsOrder> {
        let url = endpoint(&self.api_base, &["v1", "order", order_id], &[])?;
        Ok(self.get::<OrderEnvelope>(url, true).await?.order)
    }

    /// `POST /v1/order/preview`. The venue validates and prices the order and
    /// does not execute it.
    pub async fn preview_order(&self, req: &CreateOrderRequest) -> Result<UsOrder> {
        let url = endpoint(&self.api_base, &["v1", "order", "preview"], &[])?;
        let body = PreviewBody { request: req };
        Ok(self.post::<_, OrderEnvelope>(url, &body).await?.order)
    }

    /// `POST /v1/orders`. Never retried.
    pub async fn create_order(&self, req: &CreateOrderRequest) -> Result<CreateOrderResponse> {
        let url = endpoint(&self.api_base, &["v1", "orders"], &[])?;
        self.post(url, req).await
    }

    /// `POST /v1/order/{id}/cancel`. Never retried.
    pub async fn cancel_order(&self, order_id: &str, market_slug: &str) -> Result<()> {
        let url = endpoint(&self.api_base, &["v1", "order", order_id, "cancel"], &[])?;
        self.post::<_, IgnoredAny>(url, &CancelBody { market_slug })
            .await?;
        Ok(())
    }

    // crate-internal plumbing for the WebSocket client

    pub(crate) fn signer(&self) -> Option<Arc<UsSigner>> {
        self.signer.clone()
    }

    /// The WebSocket URL for `path`: `https` becomes `wss` and `http` becomes `ws`.
    pub(crate) fn ws_url(&self, path: &str) -> Result<Url> {
        let mut url = Url::parse(&self.api_base).map_err(Error::invalid)?;
        let scheme = match url.scheme() {
            "https" => "wss",
            "http" => "ws",
            other => {
                return Err(Error::invalid(format!(
                    "unsupported API base scheme {other}"
                )));
            }
        };
        url.set_scheme(scheme)
            .map_err(|()| Error::invalid("cannot switch the API base to a WebSocket scheme"))?;
        url.set_path(path);
        Ok(url)
    }

    async fn get<T: DeserializeOwned>(&self, url: Url, signed: bool) -> Result<T> {
        let response = self.request(Method::GET, &url, signed)?.send().await?;
        read(response).await
    }

    async fn post<B, T>(&self, url: Url, body: &B) -> Result<T>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let bytes = serde_json::to_vec(body)?;
        let response = self
            .request(Method::POST, &url, true)?
            .body(bytes)
            .send()
            .await?;
        read(response).await
    }

    /// Build a request. A signed request is signed over the path of `url`,
    /// which is exactly the path that goes on the wire.
    fn request(&self, method: Method, url: &Url, signed: bool) -> Result<reqwest::RequestBuilder> {
        let builder = self
            .http
            .request(method.clone(), url.as_str())
            .header(CONTENT_TYPE, "application/json");
        if !signed {
            return Ok(builder);
        }
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| Error::invalid("authenticated endpoint requires an UsSigner"))?;
        let auth = signer.headers_now(method.as_str(), url.path());
        Ok(builder
            .header("X-PM-Access-Key", auth.access_key)
            .header("X-PM-Timestamp", auth.timestamp)
            .header("X-PM-Signature", auth.signature))
    }
}

/// Build `base` + `segments` (percent-encoded per segment) + `query`.
fn endpoint(base: &str, segments: &[&str], query: &[(&str, String)]) -> Result<Url> {
    let mut url = Url::parse(base).map_err(Error::invalid)?;
    url.path_segments_mut()
        .map_err(|()| Error::invalid("base URL cannot carry path segments"))?
        .pop_if_empty()
        .extend(segments);
    if !query.is_empty() {
        let mut pairs = url.query_pairs_mut();
        for (key, value) in query {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}

/// Query pairs for `GET /v1/markets`. Repeated `categories` keys, lowercase bools.
fn market_query(q: &UsMarketsQuery) -> Vec<(&'static str, String)> {
    let mut query: Vec<(&'static str, String)> = q
        .categories
        .iter()
        .map(|category| ("categories", category.clone()))
        .collect();
    if let Some(closed) = q.closed {
        query.push(("closed", closed.to_string()));
    }
    if let Some(min) = &q.end_date_min {
        query.push(("endDateMin", min.clone()));
    }
    if let Some(max) = &q.end_date_max {
        query.push(("endDateMax", max.clone()));
    }
    if let Some(limit) = q.limit {
        query.push(("limit", limit.to_string()));
    }
    query
}

/// The body text of a successful response, or the mapped error.
async fn read_text(response: reqwest::Response) -> Result<String> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = response.text().await?;
    if status.is_success() {
        return Ok(body);
    }
    Err(status_error(status, retry_after, &body))
}

/// Decode a JSON body. An empty body means `{}`.
fn decode<T: DeserializeOwned>(body: &str) -> Result<T> {
    let json = if body.trim().is_empty() { "{}" } else { body };
    Ok(serde_json::from_str(json)?)
}

/// Read and decode a response body. Success carries JSON.
async fn read<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    decode(&read_text(response).await?)
}

fn status_error(status: StatusCode, retry_after: Option<Duration>, body: &str) -> Error {
    match status {
        StatusCode::NOT_FOUND => Error::NotFound,
        StatusCode::TOO_MANY_REQUESTS if !body.contains(GLOBAL_RATE_LIMIT) => {
            Error::rate_limited(retry_after)
        }
        _ => Error::http(status.as_u16(), error_message(status, body)),
    }
}

/// The SDK rule: JSON `message`, then JSON `error`, then the raw body, then the reason phrase.
fn error_message(status: StatusCode, body: &str) -> String {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        for key in ["message", "error"] {
            if let Some(text) = json
                .get(key)
                .and_then(serde_json::Value::as_str)
                .filter(|text| !text.is_empty())
            {
                return text.to_owned();
            }
        }
    }
    let raw = body.trim();
    if !raw.is_empty() {
        return raw.to_owned();
    }
    status.canonical_reason().unwrap_or_default().to_owned()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketsPage {
    markets: Vec<UsMarket>,
}

#[derive(Deserialize)]
struct MarketEnvelope {
    market: UsMarket,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookEnvelope {
    market_data: UsMarketData,
}

#[derive(Deserialize)]
struct SettlementEnvelope {
    settlement: Option<Decimal>,
}

#[derive(Deserialize)]
struct BalancesEnvelope {
    balances: Vec<UsBalance>,
}

#[derive(Deserialize)]
struct OrdersEnvelope {
    orders: Vec<UsOrder>,
}

#[derive(Deserialize)]
struct OrderEnvelope {
    order: UsOrder,
}

#[derive(Serialize)]
struct PreviewBody<'a> {
    request: &'a CreateOrderRequest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelBody<'a> {
    market_slug: &'a str,
}
