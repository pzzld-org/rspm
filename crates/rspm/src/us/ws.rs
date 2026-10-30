/*
    Appellation: ws <module>
    Contrib: @FL03
*/
use super::{
    US_WS_MARKETS_PATH, US_WS_PRIVATE_PATH, UsClient, UsExecution, UsMarketData, UsOrder, UsSigner,
};
use crate::{Error, Result};
use futures::{SinkExt as _, StreamExt as _};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::{Instant, sleep_until, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use url::Url;

const EVENT_CAPACITY: usize = 1024;
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Replay = BTreeMap<String, UsSubscription>;

/// Which WebSocket endpoint to open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UsWsChannel {
    Private,
    Markets,
}

impl UsWsChannel {
    fn path(self) -> &'static str {
        match self {
            Self::Private => US_WS_PRIVATE_PATH,
            Self::Markets => US_WS_MARKETS_PATH,
        }
    }
}

/// A subscription. Its `request_id` matches responses and unsubscribes.
/// An empty `market_slugs` means every market on the private channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UsSubscription {
    pub request_id: String,
    pub subscription_type: &'static str,
    pub market_slugs: Vec<String>,
}

/// An event from the WebSocket. Frames keep their raw text, so the node can capture them.
// `Execution` holds a full order. Boxing it would change the frozen public API.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum UsWsEvent {
    /// The socket is up. `generation` counts successful connections from 1.
    Connected {
        generation: u64,
    },
    /// The socket went down, or a connection attempt failed. Subscriptions are
    /// replayed on the next connection.
    Disconnected {
        generation: u64,
        reason: String,
    },
    OrderSnapshot {
        orders: Vec<UsOrder>,
        eof: bool,
        raw: String,
    },
    Execution {
        execution: UsExecution,
        raw: String,
    },
    /// Positions are seeded from REST, so the payload stays raw here.
    Position {
        raw: String,
    },
    MarketData {
        data: UsMarketData,
        raw: String,
    },
    SubscriptionError {
        request_id: String,
        error: String,
        raw: String,
    },
    /// Heartbeats and any frame this client does not route.
    Other {
        raw: String,
    },
}

enum Command {
    Subscribe(UsSubscription),
    Unsubscribe(String),
    Shutdown,
}

/// A running WebSocket connection task. Dropping the handle stops the task.
pub struct UsWs {
    commands: mpsc::UnboundedSender<Command>,
    task: JoinHandle<()>,
}

impl UsWs {
    /// Spawn the connection task and return it with its event receiver.
    ///
    /// The URL comes from the client's API base. Each (re)connect signs
    /// `<ts>GET<path>` afresh and replays every current subscription. No
    /// frame for `idle` triggers a reconnect. Reconnects back off from 1 s,
    /// doubling to 30 s. The backoff resets after a session that delivered a frame.
    ///
    /// The event channel is bounded. On overflow the task drops the
    /// connection and emits `Disconnected`, so a consumer knows to reconcile
    /// from REST. Must be called inside a Tokio runtime.
    ///
    /// Returns `InvalidParam` when the client has no signer.
    pub fn spawn(
        client: &UsClient,
        channel: UsWsChannel,
        idle: Duration,
    ) -> Result<(Self, mpsc::Receiver<UsWsEvent>)> {
        let signer = client
            .signer()
            .ok_or_else(|| Error::invalid("websocket requires an authenticated client"))?;
        let url = client.ws_url(channel.path())?;
        let (events, receiver) = mpsc::channel(EVENT_CAPACITY);
        let (commands, command_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run(Link { signer, url, idle }, command_rx, events));
        Ok((Self { commands, task }, receiver))
    }

    /// Add a subscription. Sent now if connected, else on the next connection.
    /// The caller splits sets larger than 100 market slugs.
    pub fn subscribe(&self, sub: UsSubscription) {
        let _ = self.commands.send(Command::Subscribe(sub));
    }

    /// Remove a subscription by request id.
    pub fn unsubscribe(&self, request_id: &str) {
        let _ = self
            .commands
            .send(Command::Unsubscribe(request_id.to_owned()));
    }

    /// Close the socket and wait up to 5 s for the task to finish. A task still
    /// blocked on a full event channel is aborted after that.
    pub async fn shutdown(mut self) {
        let _ = self.commands.send(Command::Shutdown);
        if timeout(SHUTDOWN_GRACE, &mut self.task).await.is_err() {
            self.task.abort();
        }
    }
}

struct Link {
    signer: Arc<UsSigner>,
    url: Url,
    idle: Duration,
}

/// Why a session ended.
enum Ended {
    Shutdown,
    Lost(String),
}

async fn run(
    link: Link,
    mut commands: mpsc::UnboundedReceiver<Command>,
    events: mpsc::Sender<UsWsEvent>,
) {
    let mut subs = Replay::new();
    let mut generation = 0_u64;
    let mut delay = BACKOFF_MIN;
    let mut pause = None;
    loop {
        if let Some(duration) = pause.take()
            && !hold(duration, &mut commands, &mut subs).await
        {
            return;
        }
        let socket = match dial(&link).await {
            Ok(socket) => socket,
            Err(reason) => {
                if events
                    .send(UsWsEvent::Disconnected { generation, reason })
                    .await
                    .is_err()
                {
                    return;
                }
                pause = Some(delay);
                delay = (delay * 2).min(BACKOFF_MAX);
                continue;
            }
        };
        generation += 1;
        if events
            .send(UsWsEvent::Connected { generation })
            .await
            .is_err()
        {
            return;
        }
        let mut healthy = false;
        match session(
            socket,
            link.idle,
            &mut subs,
            &mut commands,
            &events,
            &mut healthy,
        )
        .await
        {
            Ended::Shutdown => return,
            Ended::Lost(reason) => {
                if events
                    .send(UsWsEvent::Disconnected { generation, reason })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        // A server that accepts and then closes must not reset the backoff.
        if healthy {
            delay = BACKOFF_MIN;
        }
        pause = Some(delay);
        delay = (delay * 2).min(BACKOFF_MAX);
    }
}

/// Sleep for `duration` while applying subscription changes. False on shutdown.
async fn hold(
    duration: Duration,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    subs: &mut Replay,
) -> bool {
    let until = Instant::now() + duration;
    loop {
        tokio::select! {
            () = sleep_until(until) => return true,
            command = commands.recv() => match command {
                None | Some(Command::Shutdown) => return false,
                Some(Command::Subscribe(sub)) => {
                    subs.insert(sub.request_id.clone(), sub);
                }
                Some(Command::Unsubscribe(request_id)) => {
                    subs.remove(&request_id);
                }
            },
        }
    }
}

/// Open one signed connection. The signature is taken now, so each dial is fresh.
async fn dial(link: &Link) -> std::result::Result<Socket, String> {
    let mut request = link
        .url
        .as_str()
        .into_client_request()
        .map_err(|error| error.to_string())?;
    let auth = link.signer.headers_now("GET", link.url.path());
    let headers = request.headers_mut();
    headers.insert("x-pm-access-key", header_value(&auth.access_key)?);
    headers.insert("x-pm-timestamp", header_value(&auth.timestamp)?);
    headers.insert("x-pm-signature", header_value(&auth.signature)?);
    match timeout(CONNECT_TIMEOUT, connect_async(request)).await {
        Ok(Ok((socket, _response))) => Ok(socket),
        Ok(Err(error)) => Err(format!("connect failed: {error}")),
        Err(_elapsed) => Err("connect timed out".to_owned()),
    }
}

fn header_value(value: &str) -> std::result::Result<HeaderValue, String> {
    HeaderValue::from_str(value).map_err(|error| error.to_string())
}

/// Run one connection until it ends. Replays subscriptions first. Sets `healthy`
/// once a data frame arrives.
async fn session(
    socket: Socket,
    idle: Duration,
    subs: &mut Replay,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    events: &mpsc::Sender<UsWsEvent>,
    healthy: &mut bool,
) -> Ended {
    let (mut sink, mut stream) = socket.split();
    for sub in subs.values() {
        if let Err(error) = sink.send(Message::text(subscribe_frame(sub))).await {
            return Ended::Lost(format!("replay failed: {error}"));
        }
    }
    let mut deadline = Instant::now() + idle;
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                None | Some(Command::Shutdown) => {
                    let _ = sink.close().await;
                    return Ended::Shutdown;
                }
                Some(Command::Subscribe(sub)) => {
                    // Record before sending, so a failed send still replays on reconnect.
                    let frame = subscribe_frame(&sub);
                    subs.insert(sub.request_id.clone(), sub);
                    if let Err(error) = sink.send(Message::text(frame)).await {
                        return Ended::Lost(format!("send failed: {error}"));
                    }
                }
                Some(Command::Unsubscribe(request_id)) => {
                    if subs.remove(&request_id).is_some()
                        && let Err(error) = sink.send(Message::text(unsubscribe_frame(&request_id))).await
                    {
                        return Ended::Lost(format!("send failed: {error}"));
                    }
                }
            },
            () = sleep_until(deadline) => {
                return Ended::Lost(format!("no frame within the {idle:?} idle window"));
            }
            frame = stream.next() => {
                deadline = Instant::now() + idle;
                let message = match frame {
                    None => return Ended::Lost("stream ended".to_owned()),
                    Some(Err(error)) => return Ended::Lost(format!("read failed: {error}")),
                    Some(Ok(message)) => message,
                };
                let event = match message {
                    Message::Text(text) => decode(text.as_str().to_owned()),
                    Message::Binary(bytes) => UsWsEvent::Other {
                        raw: String::from_utf8_lossy(&bytes).into_owned(),
                    },
                    Message::Close(_) => return Ended::Lost("server closed the socket".to_owned()),
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                };
                *healthy = true;
                if let Err(end) = deliver(events, event) {
                    return end;
                }
            }
        }
    }
}

/// Hand an event to the consumer without blocking. A full channel ends the session.
fn deliver(events: &mpsc::Sender<UsWsEvent>, event: UsWsEvent) -> std::result::Result<(), Ended> {
    match events.try_send(event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            Err(Ended::Lost("event channel overflow".to_owned()))
        }
        Err(mpsc::error::TrySendError::Closed(_)) => Err(Ended::Shutdown),
    }
}

/// Route one frame by its top-level key. An `error` key means a subscription failure.
/// A keyed body that fails to decode becomes `Other`, with the raw text kept.
fn decode(raw: String) -> UsWsEvent {
    let Ok(frame) = serde_json::from_str::<Value>(&raw) else {
        return UsWsEvent::Other { raw };
    };
    if let Some(error) = frame.get("error").filter(|error| !error.is_null()) {
        return UsWsEvent::SubscriptionError {
            request_id: frame
                .get("requestId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            error: error
                .as_str()
                .map_or_else(|| error.to_string(), str::to_owned),
            raw,
        };
    }
    if let Some(body) = first(&frame, &["orderSubscriptionSnapshot", "ordersSnapshot"]) {
        return match OrdersSnapshot::deserialize(body) {
            Ok(snapshot) => UsWsEvent::OrderSnapshot {
                orders: snapshot.orders,
                eof: snapshot.eof,
                raw,
            },
            Err(_) => UsWsEvent::Other { raw },
        };
    }
    if let Some(body) = first(&frame, &["orderSubscriptionUpdate", "orderUpdate"]) {
        return match ExecutionUpdate::deserialize(body) {
            Ok(update) => UsWsEvent::Execution {
                execution: update.execution,
                raw,
            },
            Err(_) => UsWsEvent::Other { raw },
        };
    }
    if first(
        &frame,
        &[
            "positionSubscription",
            "positionSubscriptionUpdate",
            "positionUpdate",
        ],
    )
    .is_some()
    {
        return UsWsEvent::Position { raw };
    }
    if let Some(body) = first(&frame, &["marketData"]) {
        return match UsMarketData::deserialize(body) {
            Ok(data) => UsWsEvent::MarketData { data, raw },
            Err(_) => UsWsEvent::Other { raw },
        };
    }
    UsWsEvent::Other { raw }
}

/// The first non-null value among `keys`.
fn first<'a>(frame: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter()
        .filter_map(|key| frame.get(*key))
        .find(|value| !value.is_null())
}

fn subscribe_frame(sub: &UsSubscription) -> String {
    let mut body = json!({
        "requestId": sub.request_id,
        "subscriptionType": sub.subscription_type,
    });
    if !sub.market_slugs.is_empty() {
        body["marketSlugs"] = json!(sub.market_slugs);
    }
    json!({ "subscribe": body }).to_string()
}

fn unsubscribe_frame(request_id: &str) -> String {
    json!({ "unsubscribe": { "requestId": request_id } }).to_string()
}

#[derive(Deserialize)]
struct OrdersSnapshot {
    orders: Vec<UsOrder>,
    #[serde(default)]
    eof: bool,
}

#[derive(Deserialize)]
struct ExecutionUpdate {
    execution: UsExecution,
}
