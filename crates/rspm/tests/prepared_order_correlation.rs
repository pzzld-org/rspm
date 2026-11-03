//! Runtime proof for the prepared-order correlation boundary.
#![cfg(feature = "clob")]

use core::str::FromStr as _;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rspm::polymarket::{
    self, POLYGON,
    auth::{Credentials, LocalSigner, Signer as _, Uuid},
    clob::types::SignatureType,
    types::Signature,
};
use rspm::{
    AuthenticatedProtocolAuthority, ClobClient, ClobConfig, ClobOperationError, ClobSide,
    PreparedClobOrder, SubmissionController,
};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const TEST_PRIVATE_KEY: &str = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
const TOKEN_ID: &str = "1";
const CLIENT_MARKER: &str = "s1-runtime-marker";
const EXPECTED_MAKER_AMOUNT: &str = "500000";
const EXPECTED_TAKER_AMOUNT: &str = "1000000";
const EXPECTED_SIDE: &str = "BUY";
const EXPECTED_ORDER_TYPE: &str = "FAK";
const EXPECTED_SIGNATURE_TYPE: u8 = 0;
const EXPECTED_EXPIRATION: &str = "0";
const ZERO_B256: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, Debug, Eq, PartialEq)]
struct CapturedRequest {
    head: Vec<u8>,
    method: String,
    path: String,
    body: Vec<u8>,
}

struct ResponsePlan {
    status: u16,
    body: String,
    retry_after_zero: bool,
    provider_oracle: bool,
}

impl ResponsePlan {
    fn accepted(order_id: &str) -> Self {
        Self {
            status: 200,
            body: format!(
                r#"{{"error_msg":"","makingAmount":"","orderID":"{order_id}","status":"live","success":true,"takingAmount":""}}"#
            ),
            retry_after_zero: false,
            provider_oracle: false,
        }
    }

    fn accepted_by_provider_oracle() -> Self {
        Self {
            status: 200,
            body: String::new(),
            retry_after_zero: false,
            provider_oracle: true,
        }
    }

    fn rate_limited() -> Self {
        Self {
            status: 429,
            body: r#"{"error_msg":"rate limited"}"#.to_owned(),
            retry_after_zero: true,
            provider_oracle: false,
        }
    }
}

#[derive(Clone)]
struct ServerState {
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    responses: Arc<Mutex<VecDeque<ResponsePlan>>>,
    balance_body: String,
}

struct FakeClobServer {
    endpoint: String,
    state: ServerState,
    task: JoinHandle<()>,
}

impl FakeClobServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local fake CLOB server");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("fake CLOB server address")
        );
        let state = ServerState {
            requests: Arc::new(Mutex::new(Vec::new())),
            responses: Arc::new(Mutex::new(VecDeque::new())),
            balance_body: balance_body(),
        };
        let task_state = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                serve_connection(socket, &task_state).await;
            }
        });
        Self {
            endpoint,
            state,
            task,
        }
    }

    fn push_response(&self, response: ResponsePlan) {
        self.state
            .responses
            .lock()
            .expect("response queue lock")
            .push_back(response);
    }

    fn order_requests(&self) -> Vec<CapturedRequest> {
        self.state
            .requests
            .lock()
            .expect("request capture lock")
            .iter()
            .filter(|request| request.method == "POST" && request.path == "/order")
            .cloned()
            .collect()
    }
}

impl Drop for FakeClobServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_connection(mut socket: TcpStream, state: &ServerState) {
    let Some(request) = read_request(&mut socket).await else {
        return;
    };
    state
        .requests
        .lock()
        .expect("request capture lock")
        .push(request.clone());
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/version") => ResponsePlan {
            status: 200,
            body: r#"{"version":2}"#.to_owned(),
            retry_after_zero: false,
            provider_oracle: false,
        },
        ("GET", "/balance-allowance/update") | ("GET", "/balance-allowance") => ResponsePlan {
            status: 200,
            body: state.balance_body.clone(),
            retry_after_zero: false,
            provider_oracle: false,
        },
        ("GET", "/tick-size") => ResponsePlan {
            status: 200,
            body: r#"{"minimum_tick_size":"0.01"}"#.to_owned(),
            retry_after_zero: false,
            provider_oracle: false,
        },
        ("GET", "/neg-risk") => ResponsePlan {
            status: 200,
            body: r#"{"neg_risk":false}"#.to_owned(),
            retry_after_zero: false,
            provider_oracle: false,
        },
        ("GET", "/fee-rate") => ResponsePlan {
            status: 200,
            body: r#"{"base_fee":0}"#.to_owned(),
            retry_after_zero: false,
            provider_oracle: false,
        },
        ("POST", "/order") => {
            let response = state
                .responses
                .lock()
                .expect("response queue lock")
                .pop_front()
                .unwrap_or(ResponsePlan {
                    status: 500,
                    body: r#"{"error_msg":"missing test response"}"#.to_owned(),
                    retry_after_zero: false,
                    provider_oracle: false,
                });
            if response.provider_oracle {
                ResponsePlan::accepted(&manual_provider_order_id(&request.body))
            } else {
                response
            }
        }
        _ => ResponsePlan {
            status: 404,
            body: r#"{"error_msg":"unexpected test request"}"#.to_owned(),
            retry_after_zero: false,
            provider_oracle: false,
        },
    };
    write_response(&mut socket, response).await;
}

async fn read_request(socket: &mut TcpStream) -> Option<CapturedRequest> {
    let mut bytes = Vec::with_capacity(8 * 1024);
    let header_end = loop {
        let mut chunk = [0_u8; 8 * 1024];
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(end) = find_bytes(&bytes, b"\r\n\r\n") {
            break end;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let body_start = header_end + 4;
    while bytes.len() < body_start + content_length {
        let mut chunk = [0_u8; 8 * 1024];
        let read = socket.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let mut first_line = headers.lines().next()?.split_whitespace();
    let method = first_line.next()?.to_owned();
    let target = first_line.next()?;
    let path = target.split('?').next().unwrap_or(target).to_owned();
    Some(CapturedRequest {
        head: bytes[..header_end].to_vec(),
        method,
        path,
        body: bytes[body_start..body_start + content_length].to_vec(),
    })
}

async fn write_response(socket: &mut TcpStream, response: ResponsePlan) {
    let reason = match response.status {
        200 => "OK",
        404 => "Not Found",
        429 => "Too Many Requests",
        _ => "Internal Server Error",
    };
    let retry_after = if response.retry_after_zero {
        "Retry-After: 0\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n",
        response.status,
        reason,
        response.body.len(),
        retry_after,
    );
    socket
        .write_all(head.as_bytes())
        .await
        .expect("write fake response head");
    socket
        .write_all(response.body.as_bytes())
        .await
        .expect("write fake response body");
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn balance_body() -> String {
    let normal = polymarket::contract_config(POLYGON, false).expect("Polygon contract config");
    let neg_risk = polymarket::contract_config(POLYGON, true).expect("Polygon contract config");
    let spenders = [
        normal.exchange_v2.expect("Polygon V2 exchange"),
        neg_risk.exchange_v2.expect("Polygon V2 neg-risk exchange"),
        neg_risk.neg_risk_adapter.expect("Polygon neg-risk adapter"),
    ];
    format!(
        r#"{{"balance":"100000000","allowances":{{"{}":"100000000","{}":"100000000","{}":"100000000"}}}}"#,
        spenders[0], spenders[1], spenders[2]
    )
}

async fn activated_client(
    server: &FakeClobServer,
) -> (
    ClobClient,
    SubmissionController,
    AuthenticatedProtocolAuthority,
) {
    let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
        .expect("test signer")
        .with_chain_id(Some(POLYGON));
    let credentials = Credentials::new(
        Uuid::nil(),
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned(),
        "test-only-passphrase".to_owned(),
    );
    let (client, controller) = ClobClient::from_authenticated_authority_with_submission_controller(
        ClobConfig::new(server.endpoint.clone(), None),
        credentials,
        signer,
        None,
        SignatureType::Eoa,
    )
    .await
    .expect("build authenticated loopback client");
    let authority = client
        .refresh_authenticated_capital_authority()
        .await
        .expect("refresh protocol and capital authority");
    assert!(
        client
            .activate_authenticated_protocol_authority_if(authority, || true)
            .await
            .expect("activate protocol authority")
    );
    let epoch = controller.revocation_epoch();
    let activation = controller.acquire_exclusive().await;
    assert!(activation.activate(epoch, authority));
    drop(activation);
    (client, controller, authority)
}

async fn submit_prepared(
    client: &ClobClient,
    controller: &SubmissionController,
    authority: AuthenticatedProtocolAuthority,
    order: PreparedClobOrder,
) -> Result<Option<String>, ClobOperationError> {
    let controller = controller.clone();
    client
        .submit_prepared(order, move || {
            let controller = controller.clone();
            async move { controller.authorize_if(authority, || true).await }
        })
        .await
}

#[derive(Clone, Debug)]
struct ParsedWireOrder {
    salt: u64,
    maker: String,
    signer: String,
    token_id: String,
    maker_amount: String,
    taker_amount: String,
    side: String,
    expiration: String,
    signature_type: u8,
    timestamp: String,
    metadata: String,
    builder: String,
    signature: String,
    order_type: String,
    owner: String,
    post_only: bool,
}

#[derive(Serialize)]
struct WireOrderFixture<'a> {
    salt: u64,
    maker: &'a str,
    signer: &'a str,
    #[serde(rename = "tokenId")]
    token_id: &'a str,
    #[serde(rename = "makerAmount")]
    maker_amount: &'a str,
    #[serde(rename = "takerAmount")]
    taker_amount: &'a str,
    side: &'a str,
    expiration: &'a str,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    timestamp: &'a str,
    metadata: &'a str,
    builder: &'a str,
    signature: &'a str,
}

#[derive(Serialize)]
struct WireRequestFixture<'a> {
    order: WireOrderFixture<'a>,
    #[serde(rename = "orderType")]
    order_type: &'a str,
    owner: &'a str,
    #[serde(rename = "postOnly")]
    post_only: bool,
}

fn assert_exact_keys(object: &Map<String, Value>, expected: &[&str]) {
    let mut actual: Vec<&str> = object.keys().map(String::as_str).collect();
    actual.sort_unstable();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    assert!(actual == expected, "wire field set mismatch");
}

fn named_string_field(object: &Map<String, Value>, name: &str) -> String {
    object
        .get(name)
        .and_then(Value::as_str)
        .expect("wire string field missing")
        .to_owned()
}

fn parse_wire_order(body: &[u8]) -> ParsedWireOrder {
    let root: Value = serde_json::from_slice(body).expect("wire JSON parse");
    let root = root.as_object().expect("wire object missing");
    assert_exact_keys(root, &["order", "orderType", "owner", "postOnly"]);
    let order = root
        .get("order")
        .and_then(Value::as_object)
        .expect("wire order object missing");
    assert_exact_keys(
        order,
        &[
            "salt",
            "maker",
            "signer",
            "tokenId",
            "makerAmount",
            "takerAmount",
            "side",
            "expiration",
            "signatureType",
            "timestamp",
            "metadata",
            "builder",
            "signature",
        ],
    );
    let salt = order
        .get("salt")
        .and_then(Value::as_u64)
        .expect("wire salt missing");
    let signature_type = order
        .get("signatureType")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .expect("wire signature type missing");
    let post_only = root
        .get("postOnly")
        .and_then(Value::as_bool)
        .expect("wire post-only field missing");
    ParsedWireOrder {
        salt,
        maker: named_string_field(order, "maker"),
        signer: named_string_field(order, "signer"),
        token_id: named_string_field(order, "tokenId"),
        maker_amount: named_string_field(order, "makerAmount"),
        taker_amount: named_string_field(order, "takerAmount"),
        side: named_string_field(order, "side"),
        expiration: named_string_field(order, "expiration"),
        signature_type,
        timestamp: named_string_field(order, "timestamp"),
        metadata: named_string_field(order, "metadata"),
        builder: named_string_field(order, "builder"),
        signature: named_string_field(order, "signature"),
        order_type: named_string_field(root, "orderType"),
        owner: named_string_field(root, "owner"),
        post_only,
    }
}

const DOMAIN_TYPE: &str =
    "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)";
const ORDER_TYPE: &str = "Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)";
const ORDER_NAME: &str = "Polymarket CTF Exchange";
const ORDER_VERSION: &str = "2";
const VERIFYING_CONTRACT: &str = "0xe111180000d2663c0091e4f400237545b87b996b";
const FIXED_PROVIDER_ORDER_ID: &str =
    "0xa2cf8393e1314c7d25a39643b623b42fda1b59bc61dc34087450bc239407b553";
const KECCAK_ROTATION: [[u32; 5]; 5] = [
    [0, 36, 3, 41, 18],
    [1, 44, 10, 45, 2],
    [62, 6, 43, 15, 61],
    [28, 55, 25, 21, 56],
    [27, 20, 39, 8, 14],
];
const KECCAK_ROUND_CONSTANTS: [u64; 24] = [
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808a,
    0x8000000080008000,
    0x000000000000808b,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008a,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000a,
    0x000000008000808b,
    0x800000000000008b,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800a,
    0x800000008000000a,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
];

fn keccak_permutation(state: &mut [u64; 25]) {
    for &round_constant in &KECCAK_ROUND_CONSTANTS {
        let mut column_parity = [0_u64; 5];
        for x in 0..5 {
            for y in 0..5 {
                column_parity[x] ^= state[x + 5 * y];
            }
        }
        let mut column_delta = [0_u64; 5];
        for x in 0..5 {
            column_delta[x] =
                column_parity[(x + 4) % 5] ^ column_parity[(x + 1) % 5].rotate_left(1);
        }
        for x in 0..5 {
            for y in 0..5 {
                state[x + 5 * y] ^= column_delta[x];
            }
        }

        let mut rotated = [0_u64; 25];
        for x in 0..5 {
            for y in 0..5 {
                let destination_x = y;
                let destination_y = (2 * x + 3 * y) % 5;
                rotated[destination_x + 5 * destination_y] =
                    state[x + 5 * y].rotate_left(KECCAK_ROTATION[x][y]);
            }
        }
        for x in 0..5 {
            for y in 0..5 {
                state[x + 5 * y] = rotated[x + 5 * y]
                    ^ ((!rotated[(x + 1) % 5 + 5 * y]) & rotated[(x + 2) % 5 + 5 * y]);
            }
        }
        state[0] ^= round_constant;
    }
}

fn manual_keccak256(input: &[u8]) -> [u8; 32] {
    const RATE: usize = 136;
    let mut padded = input.to_vec();
    padded.push(0x01);
    while padded.len() % RATE != RATE - 1 {
        padded.push(0);
    }
    padded.push(0x80);

    let mut state = [0_u64; 25];
    for block in padded.as_chunks::<RATE>().0 {
        for (lane, bytes) in block.as_chunks::<8>().0.iter().enumerate() {
            let mut lane_bytes = [0_u8; 8];
            lane_bytes.copy_from_slice(bytes);
            state[lane] ^= u64::from_le_bytes(lane_bytes);
        }
        keccak_permutation(&mut state);
    }

    let mut output = [0_u8; 32];
    for (lane, bytes) in output.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        bytes.copy_from_slice(&state[lane].to_le_bytes());
    }
    output
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("non-canonical hexadecimal value"),
    }
}

fn hex_bytes(value: &str, byte_len: usize, field: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    assert!(bytes.len() == 2 + byte_len * 2, "{field} length mismatch");
    assert!(bytes.starts_with(b"0x"), "{field} prefix mismatch");
    let digits = &bytes[2..];
    assert!(
        digits
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{field} hexadecimal form mismatch"
    );
    digits
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn decimal_word(value: &str, field: &str) -> [u8; 32] {
    assert!(!value.is_empty(), "{field} is empty");
    assert!(
        value == "0" || !value.starts_with('0'),
        "{field} has non-canonical leading zero"
    );
    let mut word = [0_u8; 32];
    for digit in value.bytes() {
        assert!(digit.is_ascii_digit(), "{field} is not decimal");
        let mut carry = u16::from(digit - b'0');
        for byte in word.iter_mut().rev() {
            let product = u16::from(*byte) * 10 + carry;
            *byte = (product & 0xff) as u8;
            carry = product >> 8;
        }
        assert!(carry == 0, "{field} exceeds uint256");
    }
    word
}

fn u64_word(value: u64) -> [u8; 32] {
    let mut word = [0_u8; 32];
    word[24..].copy_from_slice(&value.to_be_bytes());
    word
}

fn address_word(value: &str, field: &str) -> [u8; 32] {
    let address = hex_bytes(value, 20, field);
    let mut word = [0_u8; 32];
    word[12..].copy_from_slice(&address);
    word
}

fn bytes32_word(value: &str, field: &str) -> [u8; 32] {
    let bytes = hex_bytes(value, 32, field);
    let mut word = [0_u8; 32];
    word.copy_from_slice(&bytes);
    word
}

fn abi_encode_words(words: &[[u8; 32]]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(words.len() * 32);
    for word in words {
        encoded.extend_from_slice(word);
    }
    encoded
}

fn manual_domain_separator() -> [u8; 32] {
    let words = [
        manual_keccak256(DOMAIN_TYPE.as_bytes()),
        manual_keccak256(ORDER_NAME.as_bytes()),
        manual_keccak256(ORDER_VERSION.as_bytes()),
        decimal_word("137", "chainId"),
        address_word(VERIFYING_CONTRACT, "verifyingContract"),
    ];
    manual_keccak256(&abi_encode_words(&words))
}

fn manual_order_struct_hash(wire: &ParsedWireOrder) -> [u8; 32] {
    assert!(wire.salt < (1_u64 << 53), "wire salt range mismatch");
    assert!(wire.side == EXPECTED_SIDE, "wire side value mismatch");
    assert!(
        wire.signature_type == EXPECTED_SIGNATURE_TYPE,
        "wire signature type mismatch"
    );
    let words = [
        manual_keccak256(ORDER_TYPE.as_bytes()),
        u64_word(wire.salt),
        address_word(&wire.maker, "maker"),
        address_word(&wire.signer, "signer"),
        decimal_word(&wire.token_id, "tokenId"),
        decimal_word(&wire.maker_amount, "makerAmount"),
        decimal_word(&wire.taker_amount, "takerAmount"),
        u64_word(0),
        u64_word(u64::from(wire.signature_type)),
        decimal_word(&wire.timestamp, "timestamp"),
        bytes32_word(&wire.metadata, "metadata"),
        bytes32_word(&wire.builder, "builder"),
    ];
    manual_keccak256(&abi_encode_words(&words))
}

fn manual_provider_order_id_from_wire(wire: &ParsedWireOrder) -> String {
    let domain_separator = manual_domain_separator();
    let order_struct_hash = manual_order_struct_hash(wire);
    let mut encoded = Vec::with_capacity(66);
    encoded.extend_from_slice(&[0x19, 0x01]);
    encoded.extend_from_slice(&domain_separator);
    encoded.extend_from_slice(&order_struct_hash);
    format!("0x{}", hex_lower(&manual_keccak256(&encoded)))
}

fn manual_provider_order_id(body: &[u8]) -> String {
    manual_provider_order_id_from_wire(&parse_wire_order(body))
}

fn body_digest(body: &[u8]) -> String {
    hex_lower(&manual_keccak256(body))
}

fn assert_exact_prepared_wire(
    body: &[u8],
    prepare_started_ms: u128,
    prepare_finished_ms: u128,
) -> String {
    let wire = parse_wire_order(body);
    let signer = LocalSigner::from_str(TEST_PRIVATE_KEY)
        .expect("test signer")
        .with_chain_id(Some(POLYGON));
    let signer_text = format!("{:#x}", signer.address());
    assert!(wire.salt < (1_u64 << 53), "wire salt range mismatch");
    assert!(wire.maker == signer_text, "wire maker value mismatch");
    assert!(wire.signer == signer_text, "wire signer value mismatch");
    assert!(wire.token_id == TOKEN_ID, "wire token value mismatch");
    assert!(
        wire.maker_amount == EXPECTED_MAKER_AMOUNT,
        "wire maker amount mismatch"
    );
    assert!(
        wire.taker_amount == EXPECTED_TAKER_AMOUNT,
        "wire taker amount mismatch"
    );
    assert!(wire.side == EXPECTED_SIDE, "wire side value mismatch");
    assert!(
        wire.expiration == EXPECTED_EXPIRATION,
        "wire expiration value mismatch"
    );
    assert!(
        wire.signature_type == EXPECTED_SIGNATURE_TYPE,
        "wire signature type mismatch"
    );
    assert!(wire.metadata == ZERO_B256, "wire metadata value mismatch");
    assert!(wire.builder == ZERO_B256, "wire builder value mismatch");
    assert!(
        wire.order_type == EXPECTED_ORDER_TYPE,
        "wire order type mismatch"
    );
    assert!(
        wire.owner == Uuid::nil().to_string(),
        "wire owner value mismatch"
    );
    assert!(!wire.post_only, "wire post-only value mismatch");

    let timestamp = wire
        .timestamp
        .parse::<u128>()
        .expect("wire timestamp integer");
    assert!(
        prepare_started_ms <= timestamp && timestamp <= prepare_finished_ms,
        "wire timestamp outside prepare window"
    );
    assert!(
        wire.timestamp == timestamp.to_string(),
        "wire timestamp serialization mismatch"
    );
    assert!(
        wire.signature.len() == 132
            && wire.signature.starts_with("0x")
            && wire.signature.as_bytes()[2..]
                .iter()
                .all(u8::is_ascii_hexdigit),
        "wire signature format mismatch"
    );

    let hash = manual_provider_order_id_from_wire(&wire);
    let signature = wire
        .signature
        .parse::<Signature>()
        .expect("wire signature parse");
    assert!(
        wire.signature == signature.to_string(),
        "wire signature serialization mismatch"
    );
    let recovered = signature
        .recover_address_from_prehash(&hash.parse().expect("provider hash parse"))
        .expect("wire signature recovery");
    assert!(
        recovered == signer.address(),
        "wire signature signer mismatch"
    );

    let expected = serde_json::to_vec(&WireRequestFixture {
        order: WireOrderFixture {
            salt: wire.salt,
            maker: &wire.maker,
            signer: &wire.signer,
            token_id: &wire.token_id,
            maker_amount: &wire.maker_amount,
            taker_amount: &wire.taker_amount,
            side: &wire.side,
            expiration: &wire.expiration,
            signature_type: wire.signature_type,
            timestamp: &wire.timestamp,
            metadata: &wire.metadata,
            builder: &wire.builder,
            signature: &wire.signature,
        },
        order_type: &wire.order_type,
        owner: &wire.owner,
        post_only: wire.post_only,
    })
    .expect("serialize canonical wire fixture");
    assert!(expected == body, "serialized signed order differs");
    hash
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis()
}

fn fixed_provider_vector_wire() -> ParsedWireOrder {
    ParsedWireOrder {
        salt: 1,
        maker: "0x70997970c51812dc3a010c7d01b50e0d17dc79c8".to_owned(),
        signer: "0x70997970c51812dc3a010c7d01b50e0d17dc79c8".to_owned(),
        token_id: "1".to_owned(),
        maker_amount: "500000".to_owned(),
        taker_amount: "1000000".to_owned(),
        side: "BUY".to_owned(),
        expiration: "0".to_owned(),
        signature_type: 0,
        timestamp: "1710000000000".to_owned(),
        metadata: ZERO_B256.to_owned(),
        builder: ZERO_B256.to_owned(),
        signature: "0x".to_owned(),
        order_type: "FAK".to_owned(),
        owner: Uuid::nil().to_string(),
        post_only: false,
    }
}

#[test]
fn manual_keccak_matches_legacy_ethereum_vectors() {
    assert_eq!(
        hex_lower(&manual_keccak256(b"")),
        "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
    );
    assert_eq!(
        hex_lower(&manual_keccak256(b"abc")),
        "4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"
    );
}

#[test]
fn manual_eip712_matches_fixed_provider_vector() {
    assert_eq!(
        hex_lower(&manual_keccak256(DOMAIN_TYPE.as_bytes())),
        "8b73c3c69bb8fe3d512ecc4cf759cc79239f7b179b0ffacaa9a75d522b39400f"
    );
    assert_eq!(
        hex_lower(&manual_keccak256(ORDER_TYPE.as_bytes())),
        "bb86318a2138f5fa8ae32fbe8e659f8fcf13cc6ae4014a707893055433818589"
    );
    assert_eq!(
        hex_lower(&manual_domain_separator()),
        "3264e159346253e26a64e00b69032db0e7d32f94628de3e6eecb50304d7af3d2"
    );
    let wire = fixed_provider_vector_wire();
    assert_eq!(
        hex_lower(&manual_order_struct_hash(&wire)),
        "94458488caec06f31ab1178a35aacb8f7f901a05daaed6df76c7e0b6bd4e89a4"
    );
    assert_eq!(
        manual_provider_order_id_from_wire(&wire),
        FIXED_PROVIDER_ORDER_ID
    );
}

#[tokio::test]
async fn prepare_posts_nothing_and_provider_hash_fixture_is_accepted() {
    let server = FakeClobServer::start().await;
    let (client, controller, authority) = activated_client(&server).await;
    let prepare_started_ms = epoch_millis();
    let prepared = client
        .prepare_fak(TOKEN_ID, ClobSide::Buy, 0.5, 1.0, CLIENT_MARKER)
        .await
        .expect("prepare signed order");
    let prepare_finished_ms = epoch_millis();
    assert!(
        server.order_requests().is_empty(),
        "prepare must not POST /order"
    );
    server.push_response(ResponsePlan::accepted_by_provider_oracle());
    server.push_response(ResponsePlan::accepted_by_provider_oracle());

    let first = submit_prepared(&client, &controller, authority, prepared.clone()).await;
    let first_requests = server.order_requests();
    assert!(first_requests.len() == 1, "first submission count mismatch");
    let expected_provider_id = assert_exact_prepared_wire(
        &first_requests[0].body,
        prepare_started_ms,
        prepare_finished_ms,
    );
    assert!(
        first == Ok(Some(expected_provider_id.clone())),
        "provider-native acceptance mismatch"
    );

    let second = submit_prepared(&client, &controller, authority, prepared).await;
    let requests = server.order_requests();
    assert!(
        second == Ok(Some(expected_provider_id)),
        "second acceptance mismatch"
    );
    assert!(requests.len() == 2, "submission count mismatch");
    assert!(requests[0].body == requests[1].body, "retry body changed");
    assert!(!requests[0].head.is_empty(), "request head missing");
}

#[tokio::test]
async fn mismatched_provider_response_is_typed_and_never_accepted() {
    let server = FakeClobServer::start().await;
    let (client, controller, authority) = activated_client(&server).await;
    let prepare_started_ms = epoch_millis();
    let prepared = client
        .prepare_fak(TOKEN_ID, ClobSide::Buy, 0.5, 1.0, CLIENT_MARKER)
        .await
        .expect("prepare signed order");
    let prepare_finished_ms = epoch_millis();
    server.push_response(ResponsePlan::accepted(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
    ));

    let result = submit_prepared(&client, &controller, authority, prepared.clone()).await;
    assert!(
        result == Err(ClobOperationError::PreparedOrderMismatch),
        "mismatched provider response accepted"
    );
    let requests = server.order_requests();
    assert!(requests.len() == 1, "mismatch submission count mismatch");
    let _ = assert_exact_prepared_wire(&requests[0].body, prepare_started_ms, prepare_finished_ms);
}

#[tokio::test]
async fn retry_reuses_the_exact_prepared_body_and_kills_rebuild_mutations() {
    let server = FakeClobServer::start().await;
    let (client, controller, authority) = activated_client(&server).await;
    let prepare_started_ms = epoch_millis();
    let prepared = client
        .prepare_fak(TOKEN_ID, ClobSide::Buy, 0.5, 1.0, CLIENT_MARKER)
        .await
        .expect("prepare signed order");
    let prepare_finished_ms = epoch_millis();
    server.push_response(ResponsePlan::rate_limited());
    server.push_response(ResponsePlan::accepted_by_provider_oracle());

    let result = submit_prepared(&client, &controller, authority, prepared.clone()).await;
    let requests = server.order_requests();
    assert!(
        requests.len() == 2,
        "MUTATION_RETRY_REQUEST_COUNT expected=2 observed={}",
        requests.len()
    );
    let first_body_digest = body_digest(&requests[0].body);
    let second_body_digest = body_digest(&requests[1].body);
    assert!(
        requests[0].body == requests[1].body,
        "MUTATION_RETRY_BODY_CHANGED request_count={} first_body_digest={} second_body_digest={}",
        requests.len(),
        first_body_digest,
        second_body_digest
    );
    assert!(result.is_ok(), "rate-limited prepared submission failed");
    let expected_body = requests[0].body.clone();
    let expected_provider_id =
        assert_exact_prepared_wire(&expected_body, prepare_started_ms, prepare_finished_ms);
    assert!(
        result == Ok(Some(expected_provider_id)),
        "rate-limited provider acceptance mismatch"
    );
    assert!(requests[1].body == expected_body, "retry body changed");
    assert!(
        requests[0].body == requests[1].body,
        "retry rebuilt signed order"
    );
}
