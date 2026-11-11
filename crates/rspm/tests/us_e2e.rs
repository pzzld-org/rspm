//! End-to-end tests for the Polymarket US client.
//!
//! Every server binds `127.0.0.1:0`, and nothing here reaches Polymarket. The
//! servers verify request signatures with `ring`, independently of the
//! client's signing code. The known-answer vectors come from RFC 8032 and the
//! Polymarket signing reference (`verify/kat.mjs`).

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use base64::prelude::{BASE64_STANDARD, Engine as _};
use chrono::{DateTime, SecondsFormat};
use futures::{SinkExt as _, StreamExt as _};
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey};
use rspm::{
    Error,
    us::{
        Amount, CreateOrderRequest, UsBookStamp, UsClient, UsHorizon, UsMarket, UsMarketsQuery,
        UsOrderIntent, UsOutcome, UsPlane, UsPlaneEvent, UsSigner, UsStore, UsSubscription,
        UsTimeInForce, UsWs, UsWsChannel, UsWsEvent, is_us_token_id, parse_us_token_id,
        us_token_id,
    },
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
        http,
    },
};
use url::Url;

const KEY_ID: &str = "test-key-id";
/// RFC 8032 TEST 1 seed. It is a public test vector, not a Polymarket credential.
const SEED_B64: &str = "nWGxne/9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A=";
/// Seed followed by the derived public key (64-byte form).
const SEED_PUB_B64: &str =
    "nWGxne/9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2DXWpgBgrEKt9VL/tPJZAc6DuFy89qmIyWvAhpo9wdRGg==";
const PUBLIC_KEY_HEX: &str = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a";
const EMPTY_MESSAGE_SIG_HEX: &str = "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b";
const POST_TS: u64 = 1_700_000_000_000;
const POST_ORDERS_SIG_B64: &str =
    "l/ry9TVLSH7BiT8MhbMrgAvrZzz61esR9V567RoRuEXADHNFk1rza0I+erHgAdQjTsT5Hke29D+9cYizzg01DA==";
const POSITIONS_TS: u64 = 1_792_495_800_000;
const POSITIONS_SIG_B64: &str =
    "FuxqzaxC7EHmIZnn39HOCwWWXegC0mcnxy/78FjEBk+0PaY1tyGsOWBZpw5NLd7RGxI2WhMdxDr4BkBwkoLRCg==";

const MARKET_SLUG: &str = "cpc-btc-updown-5m-2026-10-30-1545z";
/// Real gateway captures (`GET /v1/market/slug/...` and `GET /v1/markets/{slug}/book`).
const MARKET_FIXTURE: &str = include_str!("fixtures/us_market_btc_updown.json");
const BOOK_FIXTURE: &str = include_str!("fixtures/us_book_btc_updown.json");

fn signer() -> UsSigner {
    UsSigner::from_base64(KEY_ID, SEED_B64).expect("seed signer")
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// True if `signature_b64` is a valid Ed25519 signature over `timestamp || method || path`.
fn signed_by(
    public_key: &[u8],
    timestamp: &str,
    signature_b64: &str,
    method: &str,
    path: &str,
) -> bool {
    let Ok(signature) = BASE64_STANDARD.decode(signature_b64) else {
        return false;
    };
    let payload = format!("{timestamp}{method}{path}");
    UnparsedPublicKey::new(&ED25519, public_key)
        .verify(payload.as_bytes(), &signature)
        .is_ok()
}

// ---------------------------------------------------------------------------
// Signer known-answer vectors and redaction
// ---------------------------------------------------------------------------

#[test]
fn signer_known_answer_vectors_and_redaction() {
    // The ring derivation must match RFC 8032 before any Polymarket vector counts.
    let seed = BASE64_STANDARD.decode(SEED_B64).expect("seed base64");
    let pair = Ed25519KeyPair::from_seed_unchecked(&seed).expect("RFC seed");
    assert_eq!(hex_encode(pair.public_key().as_ref()), PUBLIC_KEY_HEX);
    assert_eq!(hex_encode(pair.sign(b"").as_ref()), EMPTY_MESSAGE_SIG_HEX);

    let signer = signer();
    assert_eq!(hex_encode(&signer.public_key()), PUBLIC_KEY_HEX);
    assert_eq!(
        signer.sign(POST_TS, "POST", "/v1/orders"),
        POST_ORDERS_SIG_B64
    );
    // The method is uppercased before signing.
    assert_eq!(
        signer.sign(POST_TS, "post", "/v1/orders"),
        POST_ORDERS_SIG_B64
    );
    assert_eq!(
        signer.sign(POSITIONS_TS, "GET", "/v1/portfolio/positions"),
        POSITIONS_SIG_B64
    );

    // The 64-byte form signs identically.
    let long_form = UsSigner::from_base64(KEY_ID, SEED_PUB_B64).expect("64-byte form");
    assert_eq!(hex_encode(&long_form.public_key()), PUBLIC_KEY_HEX);
    assert_eq!(
        long_form.sign(POST_TS, "POST", "/v1/orders"),
        POST_ORDERS_SIG_B64
    );

    let headers = signer.headers(POST_TS, "POST", "/v1/orders");
    assert_eq!(headers.access_key, KEY_ID);
    assert_eq!(headers.timestamp, "1700000000000");
    assert_eq!(headers.signature, POST_ORDERS_SIG_B64);

    // Debug prints no fields and no signature. There is no Display impl to test.
    assert_eq!(format!("{signer:?}"), "UsSigner { .. }");
    let debug = format!("{headers:?}");
    assert!(
        !debug.contains(POST_ORDERS_SIG_B64),
        "signature leaked into Debug: {debug}"
    );

    let now = signer.headers_now("GET", "/v1/account/balances");
    assert_eq!(now.timestamp.len(), 13, "wall clock in milliseconds");
    assert!(now.timestamp.bytes().all(|byte| byte.is_ascii_digit()));

    // The verifier used by every server test must reject a signature that
    // covers a different method, path or timestamp. Otherwise the positive
    // checks prove nothing.
    let public_key = signer.public_key();
    assert!(signed_by(
        &public_key,
        "1700000000000",
        POST_ORDERS_SIG_B64,
        "POST",
        "/v1/orders"
    ));
    assert!(!signed_by(
        &public_key,
        "1700000000000",
        POST_ORDERS_SIG_B64,
        "GET",
        "/v1/orders"
    ));
    assert!(!signed_by(
        &public_key,
        "1700000000000",
        POST_ORDERS_SIG_B64,
        "POST",
        "/v1/order"
    ));
    assert!(!signed_by(
        &public_key,
        "1700000000001",
        POST_ORDERS_SIG_B64,
        "POST",
        "/v1/orders"
    ));
    assert!(!signed_by(
        &public_key,
        "1700000000000",
        POST_ORDERS_SIG_B64,
        "POST",
        "/v1/orders?x=1"
    ));
}

#[test]
fn signer_rejects_bad_secrets() {
    // Flip one bit of the public half. The seed-derived key no longer matches.
    let mut bytes = BASE64_STANDARD
        .decode(SEED_PUB_B64)
        .expect("64-byte base64");
    bytes[63] ^= 0x01;
    let flipped = BASE64_STANDARD.encode(&bytes);
    assert!(matches!(
        UsSigner::from_base64(KEY_ID, &flipped),
        Err(Error::InvalidParam(_))
    ));

    let wrong_length = BASE64_STANDARD.encode([0_u8; 16]);
    assert!(matches!(
        UsSigner::from_base64(KEY_ID, &wrong_length),
        Err(Error::InvalidParam(_))
    ));
    assert!(matches!(
        UsSigner::from_base64(KEY_ID, "not base64!"),
        Err(Error::InvalidParam(_))
    ));
    // Whitespace around the secret is tolerated (env files end in newlines).
    assert!(UsSigner::from_base64(KEY_ID, &format!("  {SEED_B64}\n")).is_ok());
}

#[test]
fn token_ids_round_trip_and_reject_foreign_prefixes() {
    let yes = us_token_id(UsOutcome::Yes, "3917298");
    let no = us_token_id(UsOutcome::No, "3917299");
    assert_eq!(yes, "pmus:yes:3917298");
    assert_eq!(no, "pmus:no:3917299");
    assert_eq!(parse_us_token_id(&yes), Some((UsOutcome::Yes, "3917298")));
    assert_eq!(parse_us_token_id(&no), Some((UsOutcome::No, "3917299")));
    assert!(is_us_token_id(&yes));

    for foreign in [
        "123456789",
        "clob:yes:3917298",
        "PMUS:yes:1",
        "pmus:maybe:1",
        "pmus:yes:",
        "pmus:yes",
    ] {
        assert_eq!(parse_us_token_id(foreign), None, "{foreign}");
    }
    assert!(!is_us_token_id("123456789"));
}

// ---------------------------------------------------------------------------
// Loopback HTTP server
// ---------------------------------------------------------------------------

/// One request as the server received it. Header names are lowercased.
#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or_default()
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.as_str() == name)
            .map(|(_, value)| value.as_str())
    }

    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("JSON request body")
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    headers: Vec<(&'static str, String)>,
    body: String,
}

impl Reply {
    fn json(status: u16, body: Value) -> Self {
        Self::raw(status, "application/json", body.to_string())
    }

    fn json_str(status: u16, body: &str) -> Self {
        Self::raw(status, "application/json", body.to_owned())
    }

    fn text(status: u16, body: &str) -> Self {
        Self::raw(status, "text/plain; charset=utf-8", body.to_owned())
    }

    fn raw(status: u16, content_type: &'static str, body: String) -> Self {
        Self {
            status,
            content_type,
            headers: Vec::new(),
            body,
        }
    }

    fn with_header(mut self, name: &'static str, value: &str) -> Self {
        self.headers.push((name, value.to_owned()));
        self
    }

    fn to_wire(&self) -> String {
        let mut out = format!(
            "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.status,
            reason(self.status),
            self.content_type,
            self.body.len()
        );
        for (name, value) in &self.headers {
            out.push_str(&format!("{name}: {value}\r\n"));
        }
        out.push_str("\r\n");
        out.push_str(&self.body);
        out
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

type Log = Arc<Mutex<Vec<Recorded>>>;

/// Serve `handler` on a loopback port. Each connection carries one request.
async fn serve_http(handler: impl Fn(&Recorded) -> Reply + Send + Sync + 'static) -> (Url, Log) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local address");
    let base = Url::parse(&format!("http://{addr}")).expect("base URL");
    let log: Log = Arc::default();
    let handler = Arc::new(handler);
    let sink = Arc::clone(&log);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let handler = Arc::clone(&handler);
            let sink = Arc::clone(&sink);
            tokio::spawn(async move {
                let Some(request) = read_request(&mut stream).await else {
                    return;
                };
                let reply = handler(&request);
                sink.lock().expect("request log").push(request);
                let _ = stream.write_all(reply.to_wire().as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    (base, log)
}

async fn read_more(stream: &mut TcpStream, buf: &mut Vec<u8>) -> usize {
    let mut chunk = [0_u8; 4096];
    let read = stream.read(&mut chunk).await.unwrap_or(0);
    buf.extend_from_slice(&chunk[..read]);
    read
}

async fn read_request(stream: &mut TcpStream) -> Option<Recorded> {
    let mut buf = Vec::new();
    let head_len = loop {
        if let Some(at) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        if read_more(stream, &mut buf).await == 0 {
            return None;
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_len]).into_owned();
    let mut lines = head.split("\r\n");
    let mut start = lines.next()?.split(' ');
    let method = start.next()?.to_owned();
    let target = start.next()?.to_owned();
    let headers: Vec<(String, String)> = lines
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length = headers
        .iter()
        .find(|(name, _)| name == "content-length")
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    while buf.len() < head_len + length {
        if read_more(stream, &mut buf).await == 0 {
            return None;
        }
    }
    Some(Recorded {
        method,
        target,
        headers,
        body: buf[head_len..head_len + length].to_vec(),
    })
}

fn requests(log: &Log) -> Vec<Recorded> {
    log.lock().expect("request log").clone()
}

/// The body of the last request matching `method` and `path`.
fn last_body(log: &Log, method: &str, path: &str) -> Value {
    requests(log)
        .iter()
        .rev()
        .find(|request| request.method == method && request.path() == path)
        .unwrap_or_else(|| panic!("no {method} {path} recorded"))
        .json()
}

fn base_url(addr: SocketAddr) -> Url {
    Url::parse(&format!("http://{addr}")).expect("base URL")
}

fn create_request(
    price: Decimal,
    quantity: f64,
    intent: UsOrderIntent,
    tif: UsTimeInForce,
) -> CreateOrderRequest {
    CreateOrderRequest {
        market_slug: MARKET_SLUG.to_owned(),
        r#type: "ORDER_TYPE_LIMIT",
        price: Amount {
            value: price,
            currency: "USD".to_owned(),
        },
        quantity,
        tif,
        intent,
        participate_dont_initiate: false,
        manual_order_indicator: "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        synchronous_execution: None,
        max_block_time: None,
    }
}

// ---------------------------------------------------------------------------
// REST: signing, wire shape and bodies
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rest_requests_are_signed_over_path_and_match_the_wire_shape() {
    let (base, log) = serve_http(|request| {
        match (request.method.as_str(), request.path()) {
            ("GET", "/v1/portfolio/positions") => Reply::json(
                200,
                json!({
                    "positions": {
                        MARKET_SLUG: {
                            "netPositionDecimal": "1.5000",
                            "qtyAvailableDecimal": "1.5000",
                            "cost": {"value": "55.00", "currency": "USD"},
                            "expired": false
                        }
                    },
                    "nextCursor": "c2",
                    "eof": false
                }),
            ),
            ("POST", "/v1/orders") => {
                Reply::json(200, json!({"id": "ord-1", "executions": []}))
            }
            ("POST", "/v1/order/preview") => Reply::json(
                200,
                // Preview has no exchange id yet, which the client must tolerate.
                json!({"order": {"marketSlug": MARKET_SLUG, "state": "ORDER_STATE_NEW", "quantity": 0.01, "cumQuantity": 0.0, "leavesQuantity": 0.01}}),
            ),
            ("POST", "/v1/order/ord-1/cancel") => Reply::json(200, json!({})),
            _ => Reply::json(404, json!({"message": "unexpected route"})),
        }
    })
    .await;
    let client = UsClient::authenticated(signer()).with_bases(base.clone(), base);

    let page = client
        .positions(Some(MARKET_SLUG), Some("c1"))
        .await
        .expect("positions");
    let position = &page.positions[MARKET_SLUG];
    assert_eq!(position.net_position_decimal, Decimal::new(15, 1));
    assert_eq!(page.next_cursor.as_deref(), Some("c2"));

    // The S-4.1 example order, with the frozen AUTOMATIC indicator.
    let example = create_request(
        Decimal::new(555, 3),
        0.5,
        UsOrderIntent::BuyLong,
        UsTimeInForce::GoodTillCancel,
    );
    let created = client.create_order(&example).await.expect("create");
    assert_eq!(created.id, "ord-1");
    assert!(created.executions.is_empty());
    assert_eq!(
        last_body(&log, "POST", "/v1/orders"),
        json!({
            "marketSlug": MARKET_SLUG,
            "type": "ORDER_TYPE_LIMIT",
            "price": {"value": "0.555", "currency": "USD"},
            "quantity": 0.5,
            "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "intent": "ORDER_INTENT_BUY_LONG",
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            "participateDontInitiate": false
        })
    );

    // Synchronous execution: maxBlockTime is a JSON string, not a number.
    let mut blocking = example.clone();
    blocking.synchronous_execution = Some(true);
    blocking.max_block_time = Some("2".to_owned());
    client
        .create_order(&blocking)
        .await
        .expect("blocking create");
    let body = last_body(&log, "POST", "/v1/orders");
    assert_eq!(body["synchronousExecution"], json!(true));
    assert_eq!(body["maxBlockTime"], json!("2"));

    // Preview nests the order under `request`, and the quantity is a number.
    let preview_req = create_request(
        Decimal::new(1, 2),
        0.01,
        UsOrderIntent::BuyLong,
        UsTimeInForce::ImmediateOrCancel,
    );
    let preview = client.preview_order(&preview_req).await.expect("preview");
    assert!(preview.id.is_empty(), "preview has no exchange id");
    assert_eq!(preview.state, "ORDER_STATE_NEW");
    let preview_body = last_body(&log, "POST", "/v1/order/preview");
    assert_eq!(preview_body.as_object().map(|map| map.len()), Some(1));
    assert_eq!(preview_body["request"]["quantity"], json!(0.01));
    assert_eq!(
        preview_body["request"]["tif"],
        json!("TIME_IN_FORCE_IMMEDIATE_OR_CANCEL")
    );

    client
        .cancel_order("ord-1", MARKET_SLUG)
        .await
        .expect("cancel returns an empty body");
    assert_eq!(
        last_body(&log, "POST", "/v1/order/ord-1/cancel"),
        json!({"marketSlug": MARKET_SLUG})
    );

    // Every authenticated request carries valid headers over its path. The
    // query is not signed, and the body is not signed.
    let public_key = signer().public_key();
    let sent = requests(&log);
    assert_eq!(sent.len(), 5);
    for request in &sent {
        assert_eq!(request.header("x-pm-access-key"), Some(KEY_ID));
        assert_eq!(request.header("content-type"), Some("application/json"));
        assert!(
            signed_by(
                &public_key,
                request.header("x-pm-timestamp").unwrap_or_default(),
                request.header("x-pm-signature").unwrap_or_default(),
                &request.method,
                request.path(),
            ),
            "bad signature on {} {}",
            request.method,
            request.target
        );
    }
    let positions = &sent[0];
    assert!(positions.target.starts_with("/v1/portfolio/positions?"));
    assert!(positions.target.contains(&format!("market={MARKET_SLUG}")));
    assert!(positions.target.contains("cursor=c1"));
}

#[tokio::test]
async fn rest_errors_map_to_rspm_errors_and_post_is_never_retried() {
    let (base, log) = serve_http(|request| match (request.method.as_str(), request.path()) {
        ("GET", "/v1/account/balances") => Reply::text(401, "Missing required API key headers"),
        ("GET", "/v1/markets/cpc-open/settlement") => {
            Reply::json(404, json!({"code": 5, "message": "market not settled"}))
        }
        ("GET", "/v1/markets/cpc-done/settlement") => {
            Reply::json(200, json!({"slug": "cpc-done", "settlement": 1}))
        }
        ("GET", "/v1/market/slug/missing") => Reply::json(404, json!({"message": "not found"})),
        ("GET", "/v1/portfolio/positions") => {
            Reply::json(429, json!({"status": 429, "message": "Too Many Requests"}))
                .with_header("Retry-After", "2")
        }
        ("GET", "/v1/orders/open") => Reply::json(
            429,
            json!({"status": 429, "message": "Global Rate Limit Exceeded"}),
        ),
        ("POST", "/v1/orders") => Reply::json(500, json!({"message": "internal"})),
        _ => Reply::json(404, json!({"message": "unexpected route"})),
    })
    .await;
    let client = UsClient::authenticated(signer()).with_bases(base.clone(), base.clone());

    match client.balances().await.expect_err("401") {
        Error::Http { status, message } => {
            assert_eq!(status, 401);
            assert_eq!(message, "Missing required API key headers");
        }
        other => panic!("expected Http 401, got {other:?}"),
    }

    assert!(matches!(
        client.market_by_slug("missing").await.expect_err("404"),
        Error::NotFound
    ));

    // Settlement: 404 is "not settled" (Ok(None)), and a 200 decodes the number.
    assert_eq!(
        client.settlement("cpc-open").await.expect("settlement 404"),
        None
    );
    assert_eq!(
        client.settlement("cpc-done").await.expect("settlement 200"),
        Some(Decimal::ONE)
    );

    match client.positions(None, None).await.expect_err("429") {
        Error::RateLimited { retry_after } => {
            assert_eq!(retry_after, Some(Duration::from_secs(2)));
        }
        other => panic!("expected RateLimited, got {other:?}"),
    }

    // The latency stopgap is an order-level reject, not a throttle.
    match client.open_orders().await.expect_err("stopgap") {
        Error::Http { status, message } => {
            assert_eq!(status, 429);
            assert_eq!(message, "Global Rate Limit Exceeded");
        }
        other => panic!("expected Http 429 for the stopgap, got {other:?}"),
    }

    // One POST, one attempt, even on a 500.
    let example = create_request(
        Decimal::new(555, 3),
        0.5,
        UsOrderIntent::BuyLong,
        UsTimeInForce::GoodTillCancel,
    );
    match client.create_order(&example).await.expect_err("500") {
        Error::Http { status, message } => {
            assert_eq!(status, 500);
            assert_eq!(message, "internal");
        }
        other => panic!("expected Http 500, got {other:?}"),
    }
    let posts = requests(&log)
        .iter()
        .filter(|request| request.method == "POST")
        .count();
    assert_eq!(posts, 1, "POST must never be retried");

    // Without a signer, authenticated calls fail before any request is sent.
    let before = requests(&log).len();
    let anonymous = UsClient::public().with_bases(base.clone(), base);
    assert!(matches!(
        anonymous.balances().await.expect_err("no signer"),
        Error::InvalidParam(_)
    ));
    assert_eq!(requests(&log).len(), before);
}

#[tokio::test]
async fn gateway_reads_decode_real_market_and_book_shapes() {
    let market: Value = serde_json::from_str(MARKET_FIXTURE).expect("market fixture");
    let markets_page = json!({"markets": [market["market"].clone()]});
    let (base, log) = serve_http(
        move |request| match (request.method.as_str(), request.path()) {
            ("GET", path) if path == format!("/v1/market/slug/{MARKET_SLUG}") => {
                Reply::json_str(200, MARKET_FIXTURE)
            }
            ("GET", path) if path == format!("/v1/markets/{MARKET_SLUG}/book") => {
                Reply::json_str(200, BOOK_FIXTURE)
            }
            ("GET", "/v1/markets") => Reply::json(200, markets_page.clone()),
            _ => Reply::json(404, json!({"message": "unexpected route"})),
        },
    )
    .await;
    let client = UsClient::public().with_bases(base.clone(), base);

    let found = client.market_by_slug(MARKET_SLUG).await.expect("market");
    let text = client
        .gateway_get_text(&["v1", "market", "slug", MARKET_SLUG], &[])
        .await
        .expect("market text");
    assert_eq!(text, MARKET_FIXTURE);
    let envelope: Value = serde_json::from_str(&text).expect("market envelope");
    let decoded: UsMarket = serde_json::from_value(envelope["market"].clone()).expect("market");
    assert_eq!(format!("{decoded:?}"), format!("{found:?}"));
    assert_eq!(found.slug, MARKET_SLUG);
    assert_eq!(found.status, "MARKET_STATUS_OPEN");
    assert_eq!(found.minimum_trade_qty, Some(0.01));
    assert_eq!(found.fee_coefficient, Some(0.0695));
    assert_eq!(found.order_price_min_tick_size, Some(0.01));
    assert_eq!(found.market_sides.len(), 2);
    let yes = found
        .market_sides
        .iter()
        .find(|side| side.long)
        .expect("yes side");
    let no = found
        .market_sides
        .iter()
        .find(|side| !side.long)
        .expect("no side");
    assert_eq!((yes.id.as_str(), no.id.as_str()), ("3917254", "3917255"));
    let terms = found.asset_price_terms.expect("asset price terms");
    assert_eq!(terms.asset.symbol, "btc");
    assert_eq!(terms.horizon, "5m");
    assert_eq!(terms.window_start, "2026-10-30T15:45:00Z");
    assert_eq!(terms.index_symbol.as_deref(), Some("BRTI"));
    assert_eq!(
        terms.price_to_beat.map(|price| price.value),
        Some(Decimal::new(7_740_414, 2))
    );

    let book = client.book(MARKET_SLUG).await.expect("book");
    assert_eq!(book.market_slug, MARKET_SLUG);
    assert!(book.bids.is_empty());
    assert_eq!(book.offers.len(), 99);
    let best = &book.offers[0];
    assert_eq!(best.px.value, Decimal::new(1, 2));
    assert_eq!(best.px.currency, "USD");
    assert_eq!(best.qty, Decimal::new(3_450_888, 2));
    assert_eq!(book.state.as_deref(), Some("MARKET_STATE_OPEN"));

    let markets = client
        .markets(&UsMarketsQuery {
            categories: vec!["crypto".to_owned()],
            closed: Some(false),
            end_date_min: Some("2026-10-30T16:00:00Z".to_owned()),
            end_date_max: Some("2026-10-30T16:50:00Z".to_owned()),
            limit: Some(200),
        })
        .await
        .expect("markets");
    assert_eq!(markets.len(), 1);
    let query: Vec<(String, String)> = url::form_urlencoded::parse(
        requests(&log)
            .iter()
            .find(|request| request.path() == "/v1/markets")
            .and_then(|request| request.target.split_once('?'))
            .map(|(_, query)| query)
            .unwrap_or_default()
            .as_bytes(),
    )
    .map(|(key, value)| (key.into_owned(), value.into_owned()))
    .collect();
    assert_eq!(
        query,
        [
            ("categories", "crypto"),
            ("closed", "false"),
            ("endDateMin", "2026-10-30T16:00:00Z"),
            ("endDateMax", "2026-10-30T16:50:00Z"),
            ("limit", "200"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .to_vec()
    );
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

/// The signed handshake the server accepted.
#[derive(Debug)]
struct Handshake {
    path: String,
    access_key: String,
    timestamp: String,
    signature: String,
}

fn header_text(request: &Request, name: &str) -> String {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned()
}

/// Accept one WebSocket upgrade. The upgrade is rejected with 401 unless its
/// signature verifies over `<timestamp>GET<path>`.
// The 401 reject type is tungstenite's `ErrorResponse`, fixed by `accept_hdr_async`.
#[allow(clippy::result_large_err)]
async fn accept_signed(
    listener: &TcpListener,
    public_key: [u8; 32],
) -> (WebSocketStream<TcpStream>, Handshake) {
    let (stream, _) = listener.accept().await.expect("accept");
    let mut captured = None;
    let socket = accept_hdr_async(stream, |request: &Request, response: Response| {
        let path = request.uri().path().to_owned();
        let access_key = header_text(request, "x-pm-access-key");
        let timestamp = header_text(request, "x-pm-timestamp");
        let signature = header_text(request, "x-pm-signature");
        if !signed_by(&public_key, &timestamp, &signature, "GET", &path) {
            return Err(http::Response::builder()
                .status(401)
                .body(Some("bad signature".to_owned()))
                .expect("reject response"));
        }
        captured = Some(Handshake {
            path,
            access_key,
            timestamp,
            signature,
        });
        Ok(response)
    })
    .await
    .expect("signed websocket upgrade");
    (socket, captured.expect("handshake callback ran"))
}

async fn next_text(socket: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        match socket.next().await.expect("frame").expect("read frame") {
            Message::Text(text) => return serde_json::from_str(text.as_str()).expect("JSON frame"),
            _ => continue,
        }
    }
}

async fn next_event(events: &mut mpsc::Receiver<UsWsEvent>) -> UsWsEvent {
    timeout(Duration::from_secs(10), events.recv())
        .await
        .expect("event within 10 s")
        .expect("event channel open")
}

fn order_json(id: &str) -> Value {
    json!({
        "id": id,
        "marketSlug": MARKET_SLUG,
        "side": "ORDER_SIDE_BUY",
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": "0.555", "currency": "USD"},
        "quantity": 0.5,
        "cumQuantity": 0.0,
        "leavesQuantity": 0.5,
        "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
        "intent": "ORDER_INTENT_BUY_LONG",
        "state": "ORDER_STATE_NEW"
    })
}

fn execution_json() -> Value {
    json!({
        "id": "exec-1",
        "order": order_json("ord-1"),
        "type": "EXECUTION_TYPE_TRADE",
        "lastShares": "0.50",
        "lastPx": {"value": "0.555", "currency": "USD"},
        "tradeId": "trade-789",
        "aggressor": true,
        "transactTime": "2026-10-30T15:50:00Z"
    })
}

fn order_subscription() -> UsSubscription {
    UsSubscription {
        request_id: "order-sub-1".to_owned(),
        subscription_type: "SUBSCRIPTION_TYPE_ORDER",
        market_slugs: vec![MARKET_SLUG.to_owned()],
    }
}

fn subscribe_frame(request_id: &str) -> Value {
    json!({"subscribe": {
        "requestId": request_id,
        "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
        "marketSlugs": [MARKET_SLUG]
    }})
}

fn snapshot_frame() -> Value {
    json!({
        "requestId": "order-sub-1",
        "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
        "orderSubscriptionSnapshot": {"orders": [order_json("ord-1")], "eof": true}
    })
}

#[tokio::test]
async fn ws_upgrade_is_signed_subscribe_round_trips_and_events_decode() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local address");
    let public_key = signer().public_key();
    let server = tokio::spawn(async move {
        let (mut socket, handshake) = accept_signed(&listener, public_key).await;
        let subscribe = next_text(&mut socket).await;
        for frame in [
            json!({"heartbeat": {}}),
            snapshot_frame(),
            // The SDK alias `orderUpdate` decodes like `orderSubscriptionUpdate`.
            json!({"requestId": "order-sub-1", "subscriptionType": "SUBSCRIPTION_TYPE_ORDER",
                   "orderUpdate": {"execution": execution_json()}}),
            json!({"requestId": "md-sub-1", "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                   "marketData": {"marketSlug": MARKET_SLUG, "bids": [],
                       "offers": [{"px": {"value": "0.560", "currency": "USD"}, "qty": "0.80"}],
                       "state": "MARKET_STATE_OPEN", "transactTime": "2026-10-30T15:50:00Z"}}),
            json!({"requestId": "bad-sub", "error": "unknown market slug",
                   "subscriptionType": "SUBSCRIPTION_TYPE_ORDER"}),
        ] {
            socket
                .send(Message::text(frame.to_string()))
                .await
                .expect("send frame");
        }
        while let Some(Ok(_)) = socket.next().await {}
        (subscribe, handshake)
    });

    let client = UsClient::authenticated(signer()).with_bases(base_url(addr), base_url(addr));
    let (ws, mut events) =
        UsWs::spawn(&client, UsWsChannel::Private, Duration::from_secs(5)).expect("spawn");
    ws.subscribe(order_subscription());

    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::Connected { generation: 1 }
    ));
    match next_event(&mut events).await {
        UsWsEvent::Other { raw } => assert!(raw.contains("heartbeat")),
        other => panic!("expected heartbeat as Other, got {other:?}"),
    }
    match next_event(&mut events).await {
        UsWsEvent::OrderSnapshot { orders, eof, .. } => {
            assert!(eof);
            assert_eq!(orders.len(), 1);
            assert_eq!(orders[0].id, "ord-1");
            assert_eq!(orders[0].state, "ORDER_STATE_NEW");
        }
        other => panic!("expected OrderSnapshot, got {other:?}"),
    }
    match next_event(&mut events).await {
        UsWsEvent::Execution { execution, raw } => {
            assert_eq!(execution.id, "exec-1");
            assert_eq!(execution.r#type, "EXECUTION_TYPE_TRADE");
            assert_eq!(execution.order.id, "ord-1");
            assert_eq!(execution.last_shares, Some(Decimal::new(50, 2)));
            assert!(raw.contains("orderUpdate"));
        }
        other => panic!("expected Execution, got {other:?}"),
    }
    match next_event(&mut events).await {
        UsWsEvent::MarketData { data, .. } => {
            assert_eq!(data.market_slug, MARKET_SLUG);
            assert_eq!(data.offers.len(), 1);
            assert_eq!(data.offers[0].qty, Decimal::new(80, 2));
        }
        other => panic!("expected MarketData, got {other:?}"),
    }
    match next_event(&mut events).await {
        UsWsEvent::SubscriptionError {
            request_id, error, ..
        } => {
            assert_eq!(request_id, "bad-sub");
            assert_eq!(error, "unknown market slug");
        }
        other => panic!("expected SubscriptionError, got {other:?}"),
    }

    ws.shutdown().await;
    let (subscribe, handshake) = server.await.expect("server task");
    assert_eq!(subscribe, subscribe_frame("order-sub-1"));
    assert_eq!(handshake.path, "/v1/ws/private");
    assert_eq!(handshake.access_key, KEY_ID);
    assert!(handshake.timestamp.parse::<u64>().is_ok());
    assert!(!handshake.signature.is_empty());
}

#[tokio::test]
async fn ws_reconnects_after_drop_with_fresh_signature_and_replays_subscription() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local address");
    let public_key = signer().public_key();
    let server = tokio::spawn(async move {
        // Connection 1 sends the subscribe frame and then drops without a close.
        let (mut first, first_handshake) = accept_signed(&listener, public_key).await;
        let first_subscribe = next_text(&mut first).await;
        drop(first);

        // Connection 2 must carry a fresh signature and the same subscription.
        let (mut second, second_handshake) = accept_signed(&listener, public_key).await;
        let replayed = next_text(&mut second).await;
        second
            .send(Message::text(snapshot_frame().to_string()))
            .await
            .expect("snapshot");
        while let Some(Ok(_)) = second.next().await {}
        (first_handshake, first_subscribe, second_handshake, replayed)
    });

    let client = UsClient::authenticated(signer()).with_bases(base_url(addr), base_url(addr));
    let (ws, mut events) =
        UsWs::spawn(&client, UsWsChannel::Private, Duration::from_secs(5)).expect("spawn");
    ws.subscribe(order_subscription());

    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::Connected { generation: 1 }
    ));
    match next_event(&mut events).await {
        UsWsEvent::Disconnected { generation, .. } => assert_eq!(generation, 1),
        other => panic!("expected Disconnected for generation 1, got {other:?}"),
    }
    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::Connected { generation: 2 }
    ));
    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::OrderSnapshot { .. }
    ));

    ws.shutdown().await;
    let (first, first_subscribe, second, replayed) = server.await.expect("server task");
    let first_ts: u64 = first.timestamp.parse().expect("timestamp");
    let second_ts: u64 = second.timestamp.parse().expect("timestamp");
    assert!(
        second_ts > first_ts,
        "reconnect must sign a fresh timestamp"
    );
    assert_ne!(first.signature, second.signature);
    assert_eq!(first_subscribe, subscribe_frame("order-sub-1"));
    assert_eq!(
        replayed, first_subscribe,
        "subscriptions replay on reconnect"
    );
}

#[tokio::test]
async fn ws_silence_past_idle_triggers_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local address");
    let public_key = signer().public_key();
    let server = tokio::spawn(async move {
        // Connection 1 stays open and silent until the client gives up on it.
        let (mut silent, _) = accept_signed(&listener, public_key).await;
        let _ = next_text(&mut silent).await;
        let gave_up = timeout(Duration::from_secs(10), async {
            while let Some(Ok(_)) = silent.next().await {}
        })
        .await;
        assert!(gave_up.is_ok(), "client never closed the idle socket");

        let (mut fresh, _) = accept_signed(&listener, public_key).await;
        let replayed = next_text(&mut fresh).await;
        fresh
            .send(Message::text(snapshot_frame().to_string()))
            .await
            .expect("snapshot");
        while let Some(Ok(_)) = fresh.next().await {}
        replayed
    });

    let client = UsClient::authenticated(signer()).with_bases(base_url(addr), base_url(addr));
    let (ws, mut events) =
        UsWs::spawn(&client, UsWsChannel::Private, Duration::from_millis(300)).expect("spawn");
    ws.subscribe(order_subscription());

    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::Connected { generation: 1 }
    ));
    match next_event(&mut events).await {
        UsWsEvent::Disconnected { generation, reason } => {
            assert_eq!(generation, 1);
            assert!(reason.contains("idle"), "reason: {reason}");
        }
        other => panic!("expected idle Disconnected, got {other:?}"),
    }
    assert!(matches!(
        next_event(&mut events).await,
        UsWsEvent::Connected { generation: 2 }
    ));

    ws.shutdown().await;
    let replayed = server.await.expect("server task");
    assert_eq!(replayed, subscribe_frame("order-sub-1"));
}

// ---------------------------------------------------------------------------
// Data plane
// ---------------------------------------------------------------------------

const PLANE_A: &str = "cpc-btc-updown-15m-test-a";
const PLANE_B: &str = "cpc-btc-updown-5m-test-b";
const PLANE_B2: &str = "cpc-btc-updown-5m-test-b2";
const PLANE_C: &str = "cpc-btc-updown-5m-test-c";
const PLANE_DOGE: &str = "cpc-doge-updown-5m-test-d";
const PLANE_RESENT: &str = "2026-10-30T15:49:00.887703145Z";

fn rfc3339(secs: i64) -> String {
    DateTime::from_timestamp(secs, 0)
        .expect("representable timestamp")
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// A gateway market in the `GET /v1/markets` shape.
fn window_market(
    slug: &str,
    asset: &str,
    horizon: &str,
    start: i64,
    sides: (&str, &str),
    strike: Option<&str>,
) -> Value {
    let width = if horizon == "5m" { 300 } else { 900 };
    json!({
        "id": format!("id-{slug}"),
        "slug": slug,
        "status": "MARKET_STATUS_OPEN",
        "minimumTradeQty": 0.01,
        "feeCoefficient": 0.095,
        "marketSides": [
            {"id": sides.0, "long": true},
            {"id": sides.1, "long": false}
        ],
        "assetPriceTerms": {
            "marketType": "ASSET_PRICE_MARKET_TYPE_UP_DOWN",
            "asset": {"symbol": asset},
            "horizon": horizon,
            "windowStart": rfc3339(start),
            "windowEnd": rfc3339(start + width),
            "priceToBeat": strike.map(|value| json!({"value": value, "currency": "USD"})),
        }
    })
}

/// A markets-WebSocket `marketData` frame.
fn book_frame(
    slug: &str,
    bids: &[(&str, &str)],
    offers: &[(&str, &str)],
    transact: &str,
) -> String {
    let levels = |levels: &[(&str, &str)]| -> Vec<Value> {
        levels
            .iter()
            .map(|(px, qty)| json!({"px": {"value": px, "currency": "USD"}, "qty": qty}))
            .collect()
    };
    json!({
        "marketData": {
            "marketSlug": slug,
            "bids": levels(bids),
            "offers": levels(offers),
            "state": "MARKET_STATE_OPEN",
            "transactTime": transact,
        }
    })
    .to_string()
}

/// A markets WebSocket that answers a subscribe with a heartbeat and the `books` of each market it
/// names, except that it rejects the first subscription naming exactly `reject_once`. Every client
/// frame is logged.
async fn serve_plane_ws(
    listener: TcpListener,
    books: Vec<(&'static str, Vec<String>)>,
    reject_once: &'static str,
) -> Arc<Mutex<Vec<Value>>> {
    let public_key = signer().public_key();
    let log: Arc<Mutex<Vec<Value>>> = Arc::default();
    let sink = Arc::clone(&log);
    tokio::spawn(async move {
        let (mut socket, _) = accept_signed(&listener, public_key).await;
        let mut rejected = false;
        while let Some(Ok(Message::Text(text))) = socket.next().await {
            let frame: Value = serde_json::from_str(text.as_str()).expect("client frame");
            sink.lock().expect("frame log").push(frame.clone());
            let Some(subscribe) = frame.get("subscribe") else {
                continue;
            };
            let slugs: Vec<&str> = subscribe["marketSlugs"]
                .as_array()
                .expect("market slugs")
                .iter()
                .filter_map(Value::as_str)
                .collect();
            if !rejected && slugs == [reject_once] {
                rejected = true;
                let rejection = json!({
                    "requestId": subscribe["requestId"],
                    "error": "subscription temporarily unavailable",
                });
                socket
                    .send(Message::text(rejection.to_string()))
                    .await
                    .expect("rejection");
                continue;
            }
            let mut replies = vec![json!({"heartbeat": {}}).to_string()];
            for (slug, frames) in &books {
                if slugs.contains(slug) {
                    replies.extend(frames.iter().cloned());
                }
            }
            for reply in replies {
                socket.send(Message::text(reply)).await.expect("book frame");
            }
        }
    });
    log
}

/// The data plane against loopback venue servers: discovery keeps Up/Down windows of the
/// configured assets and skips markets it cannot decode, reports every body and frame raw, groups
/// subscriptions by window, books only listed markets with a last-change stamp that holds across
/// size-only resends, and reads strikes and fees into the store. The first pass lists the 15m
/// market A, 5m market B and a later 5m window C, and the venue rejects C's lone subscription once.
/// The next pass fails, and the one after lists A, B's successor B2 and C: B's subscription is
/// dropped, A's rides through untouched, no subscribe overlaps a live one, and the rejected C and
/// the new B2 are booked.
#[tokio::test]
async fn data_plane_discovers_subscribes_books_and_resubscribes_after_a_rejection() {
    let start = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_secs(),
    )
    .expect("seconds fit")
        - 10;
    let a = window_market(PLANE_A, "btc", "15m", start, ("1", "2"), Some("77404.14"));
    let b_open = window_market(PLANE_B, "btc", "5m", start, ("3", "4"), None);
    let b_stamped = window_market(PLANE_B, "btc", "5m", start, ("3", "4"), Some("2.5"));
    let b2 = window_market(PLANE_B2, "btc", "5m", start + 300, ("9", "10"), Some("3.5"));
    let c = window_market(PLANE_C, "btc", "5m", start + 900, ("5", "6"), Some("1.5"));
    let doge = window_market(PLANE_DOGE, "doge", "5m", start, ("7", "8"), Some("0.1"));
    // An hourly range market has no window terms, so it does not decode as a market.
    let hourly = json!({"slug": "cpc-btc-range-hr-test", "assetPriceTerms": {"windowStart": null}});
    let page = json!({"markets": [hourly.clone(), a.clone(), b_open, c.clone(), doge.clone()]})
        .to_string();
    let rolled = json!({"markets": [hourly, a, b2, c, doge]}).to_string();
    let b_body = json!({"market": b_stamped}).to_string();

    let passes = Arc::new(AtomicUsize::new(0));
    let (gateway, _) = {
        let (page, rolled, b_body, passes) = (
            page.clone(),
            rolled.clone(),
            b_body.clone(),
            Arc::clone(&passes),
        );
        serve_http(move |request| match request.path() {
            "/v1/markets" => match passes.fetch_add(1, Ordering::SeqCst) {
                0 => Reply::json_str(200, &page),
                // The second pass fails, so the rejected market waits for the third.
                1 => Reply::text(500, "gateway down"),
                _ => Reply::json_str(200, &rolled),
            },
            path if path == format!("/v1/market/slug/{PLANE_B}") => Reply::json_str(200, &b_body),
            _ => Reply::json(404, json!({"message": "unexpected route"})),
        })
        .await
    };
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let api = base_url(listener.local_addr().expect("local address"));
    let a_frames = vec![
        book_frame(
            PLANE_A,
            &[("0.2800", "40"), ("0.2900", "100")],
            &[("0.3100", "50"), ("0.3000", "25")],
            "2026-10-30T15:49:00.787703145Z",
        ),
        // The venue resends the whole book about every 100 ms: same prices, one size changed.
        book_frame(
            PLANE_A,
            &[("0.2800", "40"), ("0.2900", "60")],
            &[("0.3100", "50"), ("0.3000", "25")],
            PLANE_RESENT,
        ),
        book_frame(
            PLANE_A,
            &[("0.2900", "100")],
            &[("0.3200", "7")],
            "2026-10-30T15:49:05Z",
        ),
    ];
    let book = |slug| book_frame(slug, &[("0.4500", "5")], &[("0.5500", "5")], PLANE_RESENT);
    let frames = serve_plane_ws(
        listener,
        vec![
            (PLANE_A, a_frames),
            (PLANE_B, vec![book(PLANE_B)]),
            (PLANE_B2, vec![book(PLANE_B2)]),
            (PLANE_C, vec![book(PLANE_C)]),
        ],
        PLANE_C,
    )
    .await;

    let client = UsClient::authenticated(signer()).with_bases(api, gateway);
    let store = UsStore::default();
    let (plane, mut events) =
        UsPlane::spawn(&client, vec!["btc".to_owned()], store.clone()).expect("spawn");
    let mut seen = Vec::new();
    timeout(Duration::from_secs(40), async {
        let booked = |seen: &[UsPlaneEvent], slug| {
            seen.iter().any(
                |event| matches!(event, UsPlaneEvent::Book { data, .. } if data.market_slug == slug),
            )
        };
        while let Some(event) = events.recv().await {
            seen.push(event);
            if booked(&seen, PLANE_C) && booked(&seen, PLANE_B2) {
                return;
            }
        }
        panic!("the plane ended before C and B2 were booked");
    })
    .await
    .expect("C and B2 booked within 40 s");

    // Discovery: both bodies are reported raw, in venue order, and only btc windows are listed.
    let passes: Vec<&Vec<_>> = seen
        .iter()
        .filter_map(|event| match event {
            UsPlaneEvent::Listed(windows) => Some(windows),
            _ => None,
        })
        .collect();
    let slugs = |windows: &[_]| -> Vec<String> {
        windows
            .iter()
            .map(|window: &rspm::us::UsWindow| window.slug.clone())
            .collect()
    };
    assert_eq!(
        slugs(passes[0]),
        [PLANE_A, PLANE_B, PLANE_C],
        "decodable btc windows"
    );
    assert_eq!(
        slugs(passes.last().expect("a pass")),
        [PLANE_A, PLANE_B2, PLANE_C],
        "the roll"
    );
    let first = &passes[0][0];
    assert_eq!(first.asset, "btc");
    assert_eq!(first.horizon, UsHorizon::FifteenMin);
    assert_eq!(first.window_start, start);
    assert_eq!(first.yes_token, "pmus:yes:1");
    assert_eq!(first.no_token, "pmus:no:2");
    assert_eq!(first.min_trade_qty, Some(0.01));
    assert!(
        seen.iter()
            .any(|event| matches!(event, UsPlaneEvent::Page { offset: 0, raw } if *raw == page)),
        "the page body is reported as received, undecodable market included"
    );
    assert!(
        seen.iter()
            .any(|event| matches!(event, UsPlaneEvent::Page { offset: 0, raw } if *raw == rolled)),
        "the rolled page is reported too"
    );
    assert!(
        seen.iter().any(|event| matches!(event, UsPlaneEvent::DiscoveryFailed(error) if error.contains("gateway down"))),
        "the failed pass is reported"
    );

    // Strikes and fees: A's strike came with the listing, B's from its own read, kept raw.
    assert_eq!(
        store.strike(PLANE_A).map(f64::to_bits),
        Some(77404.14_f64.to_bits())
    );
    assert_eq!(store.strike(PLANE_B), Some(2.5));
    assert!(
        seen.iter().any(|event| matches!(event, UsPlaneEvent::Market { slug, raw } if slug == PLANE_B && *raw == b_body)),
        "the strike read is reported as received"
    );
    assert_eq!(
        store.fee_schedule("pmus:no:2"),
        Some((Decimal::new(95, 3), 1)),
        "the fee coefficient is exact, under both tokens"
    );
    assert_eq!(
        store.fee_schedule("pmus:yes:7"),
        None,
        "doge was never listed"
    );

    // Frames: every frame is reported raw, books with their slug and venue time.
    assert!(
        seen.iter().any(|event| matches!(event, UsPlaneEvent::Frame { market_slug: None, venue_ts_ns: None, raw } if raw.contains("heartbeat"))),
        "a heartbeat is a frame with no market"
    );
    assert!(
        seen.iter().any(|event| matches!(event, UsPlaneEvent::Frame { market_slug: Some(slug), venue_ts_ns: Some(1_793_375_340_787_703_145), .. } if slug == PLANE_A)),
        "a book frame carries its venue time in ns"
    );

    // Books: YES from the highest bid and lowest offer, NO its exact complement. The stamp holds
    // across the resend, which moved a size only, and restamps when a price moves.
    let mut prior = None;
    let tops: Vec<_> = seen
        .iter()
        .filter_map(|event| match event {
            UsPlaneEvent::Book {
                yes_token,
                no_token,
                data,
            } if data.market_slug == PLANE_A => {
                assert_eq!(
                    (yes_token.as_str(), no_token.as_str()),
                    ("pmus:yes:1", "pmus:no:2")
                );
                let top = data.top_of_book(prior);
                prior = Some(UsBookStamp {
                    bid: top.yes.bid,
                    ask: top.yes.ask,
                    stamp_ms: top.last_change_ms,
                });
                Some(top)
            }
            _ => None,
        })
        .collect();
    assert_eq!(tops.len(), 3, "A's three frames are booked");
    assert_eq!(
        (
            tops[0].yes.bid,
            tops[0].yes.ask,
            tops[0].yes.bid_size,
            tops[0].yes.ask_size
        ),
        (Some(0.29), Some(0.3), Some(100.0), Some(25.0))
    );
    assert_eq!(
        (
            tops[0].no.bid,
            tops[0].no.ask,
            tops[0].no.bid_size,
            tops[0].no.ask_size
        ),
        (Some(0.7), Some(0.71), Some(25.0), Some(100.0)),
        "NO bid is exactly 1 - 0.3000"
    );
    assert_eq!(tops[0].last_change_ms, 1_793_375_340_787);
    assert_eq!(tops[1].yes.bid_size, Some(60.0));
    assert_eq!(
        tops[1].last_change_ms, 1_793_375_340_787,
        "a size-only resend keeps the stamp"
    );
    assert_eq!(tops[2].yes.ask, Some(0.32));
    assert_eq!(
        tops[2].last_change_ms, 1_793_375_345_000,
        "a price move restamps"
    );

    // Subscriptions: one per window, the 15m group before the 5m one at the same start, a fresh
    // set for the rejected market, and A is never dropped or resubscribed.
    let frames = frames.lock().expect("frame log").clone();
    let subscribes: Vec<(&str, Vec<&str>)> = frames
        .iter()
        .filter_map(|frame| frame.get("subscribe"))
        .map(|subscribe| {
            let slugs = subscribe["marketSlugs"].as_array().expect("slugs");
            (
                subscribe["requestId"].as_str().expect("request id"),
                slugs.iter().filter_map(Value::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(
        subscribes,
        [
            ("md:set:1:0", vec![PLANE_A]),
            ("md:set:1:1", vec![PLANE_B]),
            ("md:set:1:2", vec![PLANE_C]),
            ("md:set:2:0", vec![PLANE_B2]),
            ("md:set:2:1", vec![PLANE_C]),
        ]
    );
    let rejected = seen.iter().find_map(|event| match event {
        UsPlaneEvent::Rejected { request_id, .. } => Some(request_id.as_str()),
        _ => None,
    });
    assert_eq!(
        rejected,
        Some("md:set:1:2"),
        "the venue rejected C's lone subscription"
    );
    let unsubscribed: Vec<&str> = frames
        .iter()
        .filter_map(|frame| frame["unsubscribe"]["requestId"].as_str())
        .collect();
    assert_eq!(
        unsubscribed,
        ["md:set:1:2", "md:set:1:1"],
        "the rejected subscription and B's, once unlisted; A's rides through"
    );

    // Dropping the handle stops the plane and closes the channel.
    drop(plane);
    timeout(Duration::from_secs(5), async {
        while events.recv().await.is_some() {}
    })
    .await
    .expect("the channel closes once the plane is dropped");
}
