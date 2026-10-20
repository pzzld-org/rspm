/*
    Appellation: sprint <module>
    Contrib: @FL03
*/
//! Sprint-market token discovery: the one place that decides which outcome
//! tokens belong to the current 5m / 15m / 4h `updown` markets.
//!
//! Derived from the worker's `clob_trades` Gamma discovery so the node and the
//! worker can call one function and compare equal token sets. The slug
//! derivation and the response extraction are pure; [`GammaClient::sprint_token_set`]
//! is the thin HTTP layer over them.
use super::client::GammaClient;
use crate::error::Error;
use std::collections::HashMap;
use std::time::Duration;

/// `(window_label, window_seconds)` for every sprint window queried.
pub const SPRINT_WINDOWS: &[(&str, i64)] = &[("5m", 300), ("15m", 900), ("4h", 14_400)];

/// Gamma's `limit` for the sprint query; a response with more markets than
/// this was not produced by our slug filter and is rejected.
const MAX_MARKETS: usize = 50;
/// Longest accepted outcome-token id, in bytes.
const MAX_TOKEN_BYTES: usize = 512;
/// Longest accepted Gamma response body, in bytes.
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// YES and NO outcome tokens of the discovered sprint markets.
///
/// `slug_by_token` keys are the token set; `token_ids` keeps Gamma's order
/// (YES then NO per market) for callers that subscribe in that order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TokenSet {
    pub token_ids: Vec<String>,
    pub slug_by_token: HashMap<String, String>,
    pub markets_returned: usize,
}

/// The `{asset}-updown-{window}-{window_start}` slug for every
/// `(asset, window)` pair, assets lowercased to match Polymarket's slugs.
pub fn sprint_slugs(assets: &[String], now_secs: i64) -> Vec<String> {
    assets
        .iter()
        .flat_map(|asset| {
            let asset = asset.to_lowercase();
            SPRINT_WINDOWS.iter().map(move |(label, window_secs)| {
                let window_start = (now_secs / window_secs) * window_secs;
                format!("{asset}-updown-{label}-{window_start}")
            })
        })
        .collect()
}

#[derive(serde::Deserialize)]
struct SprintMarket {
    slug: String,
    // Gamma encodes the token array as a JSON string inside the JSON object.
    #[serde(rename = "clobTokenIds")]
    clob_token_ids: String,
}

/// Extract the token set from a raw Gamma `/markets` body.
///
/// Fails closed: any market whose slug was not requested (a stale or future
/// window), a token list that is not exactly two numeric ids, or a repeated
/// token rejects the whole response. The `Err` value is a stable reason code.
pub fn parse_token_set(body: &[u8], requested_slugs: &[String]) -> Result<TokenSet, &'static str> {
    let markets: Vec<SprintMarket> =
        serde_json::from_slice(body).map_err(|_| "gamma_response_schema_drift")?;
    if markets.len() > MAX_MARKETS {
        return Err("gamma_response_market_count_oversize");
    }
    let capacity = markets.len() * 2;
    let mut token_ids = Vec::with_capacity(capacity);
    let mut slug_by_token = HashMap::with_capacity(capacity);
    for market in &markets {
        if !requested_slugs.contains(&market.slug) {
            return Err("gamma_response_unrequested_market");
        }
        let tokens: Vec<String> =
            serde_json::from_str(&market.clob_token_ids).map_err(|_| "gamma_token_schema_drift")?;
        if tokens.len() != 2
            || tokens.iter().any(|token| {
                token.is_empty()
                    || token.len() > MAX_TOKEN_BYTES
                    || !token.bytes().all(|byte| byte.is_ascii_digit())
            })
        {
            return Err("gamma_token_schema_drift");
        }
        for token in tokens {
            if slug_by_token
                .insert(token.clone(), market.slug.clone())
                .is_some()
            {
                return Err("gamma_duplicate_token_id");
            }
            token_ids.push(token);
        }
    }
    Ok(TokenSet {
        token_ids,
        slug_by_token,
        markets_returned: markets.len(),
    })
}

impl GammaClient {
    /// Discover the YES and NO tokens of the current sprint markets of every
    /// asset in `assets` at `now_secs`.
    ///
    /// `closed=false` is sent to Gamma, and only the slugs of the windows
    /// containing `now_secs` are requested, so closed and out-of-window markets
    /// are excluded at the source; [`parse_token_set`] rejects any that
    /// appear anyway.
    pub async fn sprint_token_set(
        &self,
        assets: &[String],
        now_secs: i64,
    ) -> Result<TokenSet, Error> {
        let slugs = sprint_slugs(assets, now_secs);
        let mut url = format!("{}/markets?closed=false&limit={MAX_MARKETS}", self.base_url);
        for slug in &slugs {
            url.push_str("&slug=");
            url.push_str(slug);
        }
        let mut response = self
            .http
            .get(&url)
            .header("Accept", "application/json")
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let message = response.text().await.unwrap_or_default();
            return Err(Error::http(status.as_u16(), message));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > MAX_BODY_BYTES - body.len() {
                return Err(Error::Unknown("gamma_response_stream_oversize".into()));
            }
            body.extend_from_slice(&chunk);
        }
        parse_token_set(&body, &slugs).map_err(|reason| Error::Unknown(reason.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    // 5m start 1_783_868_100, 15m start 1_783_867_500, 4h start 1_783_857_600.
    const NOW: i64 = 1_783_868_190;
    const BTC_5M: &str = "btc-updown-5m-1783868100";
    const BTC_15M: &str = "btc-updown-15m-1783867500";
    const BTC_4H: &str = "btc-updown-4h-1783857600";

    fn assets(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn market(slug: &str, yes: &str, no: &str) -> serde_json::Value {
        serde_json::json!({
            "slug": slug,
            "closed": false,
            "clobTokenIds": serde_json::to_string(&[yes, no]).unwrap(),
        })
    }

    fn body(markets: &[serde_json::Value]) -> Vec<u8> {
        serde_json::to_vec(markets).unwrap()
    }

    #[test]
    fn slugs_are_asset_major_lowercase_and_floored_to_each_window() {
        assert_eq!(
            sprint_slugs(&assets(&["BTC", "Sol"]), NOW),
            [
                BTC_5M,
                BTC_15M,
                BTC_4H,
                "sol-updown-5m-1783868100",
                "sol-updown-15m-1783867500",
                "sol-updown-4h-1783857600",
            ]
        );
    }

    #[test]
    fn slugs_roll_to_the_next_window_exactly_on_the_boundary() {
        let before = sprint_slugs(&assets(&["BTC"]), 1_783_868_099);
        let at = sprint_slugs(&assets(&["BTC"]), 1_783_868_100);
        assert_eq!(before[0], "btc-updown-5m-1783867800");
        assert_eq!(at[0], BTC_5M);
        assert_eq!(
            before[1], at[1],
            "15m window is unchanged at the 5m boundary"
        );
    }

    #[test]
    fn extracts_both_tokens_per_market_in_gamma_order() {
        let slugs = sprint_slugs(&assets(&["BTC"]), NOW);
        let set = parse_token_set(
            &body(&[
                market(BTC_15M, "2001", "2002"),
                market(BTC_5M, "1001", "1002"),
            ]),
            &slugs,
        )
        .unwrap();
        assert_eq!(set.token_ids, ["2001", "2002", "1001", "1002"]);
        assert_eq!(set.markets_returned, 2);
        let expected: HashMap<String, String> = [
            ("2001", BTC_15M),
            ("2002", BTC_15M),
            ("1001", BTC_5M),
            ("1002", BTC_5M),
        ]
        .map(|(token, slug)| (token.to_string(), slug.to_string()))
        .into();
        assert_eq!(set.slug_by_token, expected);
    }

    #[test]
    fn a_market_outside_the_current_windows_rejects_the_response() {
        let slugs = sprint_slugs(&assets(&["BTC"]), NOW);
        // The previous 5m window: closed in practice, and not requested.
        let stale = market("btc-updown-5m-1783867800", "3001", "3002");
        assert_eq!(
            parse_token_set(&body(&[market(BTC_5M, "1001", "1002"), stale]), &slugs),
            Err("gamma_response_unrequested_market")
        );
    }

    #[test]
    fn malformed_token_lists_reject_the_response() {
        let slugs = sprint_slugs(&assets(&["BTC"]), NOW);
        let cases = [
            ("non-numeric", market(BTC_5M, "1001", "0xabc")),
            ("empty id", market(BTC_5M, "1001", "")),
            ("oversize id", market(BTC_5M, "1001", &"7".repeat(513))),
            (
                "one token",
                serde_json::json!({"slug": BTC_5M, "clobTokenIds": "[\"1001\"]"}),
            ),
            (
                "not a json array",
                serde_json::json!({"slug": BTC_5M, "clobTokenIds": "1001,1002"}),
            ),
        ];
        for (name, bad) in cases {
            assert_eq!(
                parse_token_set(&body(&[bad]), &slugs),
                Err("gamma_token_schema_drift"),
                "{name}"
            );
        }
        assert_eq!(
            parse_token_set(b"{\"not\":\"a list\"}", &slugs),
            Err("gamma_response_schema_drift")
        );
    }

    #[test]
    fn a_token_shared_by_two_markets_rejects_the_response() {
        let slugs = sprint_slugs(&assets(&["BTC"]), NOW);
        assert_eq!(
            parse_token_set(
                &body(&[
                    market(BTC_5M, "1001", "1002"),
                    market(BTC_15M, "1002", "2002"),
                ]),
                &slugs,
            ),
            Err("gamma_duplicate_token_id")
        );
    }

    #[test]
    fn more_markets_than_the_query_limit_reject_the_response() {
        let slugs = vec![BTC_5M.to_string()];
        let flood: Vec<_> = (0..=MAX_MARKETS)
            .map(|n| {
                market(
                    BTC_5M,
                    &format!("{}", 10 * n + 1),
                    &format!("{}", 10 * n + 2),
                )
            })
            .collect();
        assert_eq!(
            parse_token_set(&body(&flood), &slugs),
            Err("gamma_response_market_count_oversize")
        );
    }

    /// Serves `status`/`payload` once and returns the request line it saw.
    fn serve_once(
        status: &'static str,
        payload: Vec<u8>,
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
            }
            let head = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                payload.len()
            );
            stream.write_all(head.as_bytes()).unwrap();
            stream.write_all(&payload).unwrap();
            String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or_default()
                .to_string()
        });
        (base, handle)
    }

    #[tokio::test]
    async fn client_requests_only_open_current_window_slugs_and_returns_their_tokens() {
        let (base, server) = serve_once(
            "200 OK",
            body(&[
                market(BTC_5M, "1001", "1002"),
                market(BTC_15M, "2001", "2002"),
                market(BTC_4H, "4001", "4002"),
            ]),
        );
        let set = GammaClient::from_url(base)
            .sprint_token_set(&assets(&["BTC"]), NOW)
            .await
            .unwrap();
        assert_eq!(
            server.join().unwrap(),
            format!(
                "GET /markets?closed=false&limit=50&slug={BTC_5M}&slug={BTC_15M}&slug={BTC_4H} HTTP/1.1"
            )
        );
        assert_eq!(set.markets_returned, 3);
        assert_eq!(set.token_ids.len(), 6);
        assert_eq!(set.slug_by_token["4002"], BTC_4H);
    }

    #[tokio::test]
    async fn client_surfaces_http_errors_and_rejected_bodies() {
        let (base, server) = serve_once("503 Service Unavailable", b"down".to_vec());
        let err = GammaClient::from_url(base)
            .sprint_token_set(&assets(&["BTC"]), NOW)
            .await
            .unwrap_err();
        server.join().unwrap();
        assert!(matches!(err, Error::Http { status: 503, .. }), "{err:?}");

        let (base, server) = serve_once(
            "200 OK",
            body(&[market("btc-updown-5m-1783867800", "3001", "3002")]),
        );
        let err = GammaClient::from_url(base)
            .sprint_token_set(&assets(&["BTC"]), NOW)
            .await
            .unwrap_err();
        server.join().unwrap();
        assert!(
            err.to_string()
                .contains("gamma_response_unrequested_market"),
            "{err}"
        );
    }
}
