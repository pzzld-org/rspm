use super::frame::Frame;
use futures::{SinkExt as _, StreamExt as _};
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, MissedTickBehavior, interval, sleep, sleep_until, timeout},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        Message,
        protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
    },
};

/// Receipt clock in nanoseconds since the Unix epoch.
pub type ClockFn = Arc<dyn Fn() -> i64 + Send + Sync>;
/// Called by the reader with `(conn_id, seq, received_at_ns, bytes)` for every
/// received data frame, including frames then dropped on a full queue. It runs
/// on the reader task, so it must not block.
pub type RawHook = Arc<dyn Fn(u64, u64, i64, &[u8]) + Send + Sync>;

#[derive(Clone)]
pub struct MarketChannelConfig {
    pub url: String,
    /// Bounded frame queue between the reader and the consumer.
    pub queue_capacity: usize,
    pub ping_interval: Duration,
    /// No inbound message for this long (PONG replies count) reconnects.
    pub read_idle_timeout: Duration,
    /// No PONG for this long reconnects even while data frames still arrive,
    /// as the worker does.
    pub pong_timeout: Duration,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub close_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub max_message_bytes: usize,
    pub clock: ClockFn,
    pub raw_hook: Option<RawHook>,
}

impl Default for MarketChannelConfig {
    fn default() -> Self {
        Self {
            url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned(),
            queue_capacity: 16_384,
            // The venue answers PING with PONG; the worker pings every 10 s and
            // gives up after 20 s without a PONG.
            ping_interval: Duration::from_secs(10),
            read_idle_timeout: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(20),
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
            backoff_min: Duration::from_millis(500),
            backoff_max: Duration::from_secs(30),
            max_message_bytes: 8 * 1024 * 1024,
            clock: Arc::new(|| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |elapsed| {
                        i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)
                    })
            }),
            raw_hook: None,
        }
    }
}

/// Caller-visible connection state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionState {
    /// Id of the live connection, `None` while connecting or backing off.
    pub connected: Option<u64>,
    /// Close code of the most recent close. A stream end or error without a
    /// close frame is recorded as 1006.
    pub last_close_code: Option<u16>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub frames_received: u64,
    pub frames_enqueued: u64,
    pub frames_dropped: u64,
    pub reconnects: u64,
    /// Read-idle and missing-PONG reconnects.
    pub idle_timeouts: u64,
    pub close_1013: u64,
    pub close_1006: u64,
    pub close_other: u64,
    /// Application PONG replies. They are not frames and take no sequence.
    pub pongs: u64,
}

#[derive(Default)]
struct Counters {
    frames_received: AtomicU64,
    frames_enqueued: AtomicU64,
    frames_dropped: AtomicU64,
    reconnects: AtomicU64,
    idle_timeouts: AtomicU64,
    close_1013: AtomicU64,
    close_1006: AtomicU64,
    close_other: AtomicU64,
    pongs: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            frames_received: self.frames_received.load(Relaxed),
            frames_enqueued: self.frames_enqueued.load(Relaxed),
            frames_dropped: self.frames_dropped.load(Relaxed),
            reconnects: self.reconnects.load(Relaxed),
            idle_timeouts: self.idle_timeouts.load(Relaxed),
            close_1013: self.close_1013.load(Relaxed),
            close_1006: self.close_1006.load(Relaxed),
            close_other: self.close_other.load(Relaxed),
            pongs: self.pongs.load(Relaxed),
        }
    }
}

/// The channel is shut down.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("market channel is shut down")]
pub struct Closed;

enum Cmd {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

struct Shared {
    counters: Arc<Counters>,
    state: watch::Receiver<ConnectionState>,
    cmds: mpsc::UnboundedSender<Cmd>,
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}

/// Cloneable control handle. Dropping every handle also stops the channel.
#[derive(Clone)]
pub struct MarketHandle {
    shared: Arc<Shared>,
}

impl MarketHandle {
    /// Add assets. Takes effect on the live socket and on every reconnect.
    pub fn subscribe(&self, asset_ids: Vec<String>) -> Result<(), Closed> {
        self.send(Cmd::Subscribe(asset_ids))
    }

    pub fn unsubscribe(&self, asset_ids: Vec<String>) -> Result<(), Closed> {
        self.send(Cmd::Unsubscribe(asset_ids))
    }

    fn send(&self, cmd: Cmd) -> Result<(), Closed> {
        if *self.shared.stop.borrow() {
            return Err(Closed);
        }
        self.shared.cmds.send(cmd).map_err(|_| Closed)
    }

    pub fn stats(&self) -> StatsSnapshot {
        self.shared.counters.snapshot()
    }

    pub fn state(&self) -> watch::Receiver<ConnectionState> {
        self.shared.state.clone()
    }

    /// Stop reconnecting, send a Close frame best effort, end the tasks and
    /// close the frame queue. Idempotent and safe to call from several clones;
    /// every call returns once the connection task has ended.
    pub async fn shutdown(&self) {
        self.shared.stop.send_replace(true);
        let mut done = self.shared.done.clone();
        // An Err means the task is gone already, which is the state we wait for.
        let _ = done.wait_for(|finished| *finished).await;
    }
}

/// Start the channel on the current tokio runtime and subscribe `asset_ids`.
/// With no assets the task stays idle until the first [`MarketHandle::subscribe`].
/// The receiver yields `None` once the channel has shut down.
pub fn spawn(
    config: MarketChannelConfig,
    asset_ids: Vec<String>,
) -> (MarketHandle, mpsc::Receiver<Frame>) {
    let (frames_tx, frames_rx) = mpsc::channel(config.queue_capacity.max(1));
    let (cmds_tx, cmds_rx) = mpsc::unbounded_channel();
    let (stop_tx, stop_rx) = watch::channel(false);
    let (state_tx, state_rx) = watch::channel(ConnectionState::default());
    let (done_tx, done_rx) = watch::channel(false);
    let counters = Arc::new(Counters::default());
    let mut task = Task {
        config,
        counters: Arc::clone(&counters),
        frames: frames_tx,
        state: state_tx,
        cmds: cmds_rx,
        stop: stop_rx,
        assets: asset_ids.into_iter().collect(),
        conn_id: 0,
    };
    tokio::spawn(async move {
        task.run().await;
        // The queue must be closed before `shutdown` callers are released.
        drop(task);
        done_tx.send_replace(true);
    });
    let handle = MarketHandle {
        shared: Arc::new(Shared {
            counters,
            state: state_rx,
            cmds: cmds_tx,
            stop: stop_tx,
            done: done_rx,
        }),
    };
    (handle, frames_rx)
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

enum Ended {
    Stop,
    Lost,
}

struct Task {
    config: MarketChannelConfig,
    counters: Arc<Counters>,
    frames: mpsc::Sender<Frame>,
    state: watch::Sender<ConnectionState>,
    cmds: mpsc::UnboundedReceiver<Cmd>,
    stop: watch::Receiver<bool>,
    assets: BTreeSet<String>,
    conn_id: u64,
}

// Same json! calls as the worker, so the wire text is the same under the same
// serde_json features.
fn subscribe_frame(assets: &[String]) -> String {
    serde_json::json!({
        "assets_ids": assets,
        "type": "market",
        "custom_feature_enabled": true
    })
    .to_string()
}

fn operation_frame(assets: &[String], operation: &str) -> String {
    serde_json::json!({
        "assets_ids": assets,
        "operation": operation,
        "custom_feature_enabled": true,
    })
    .to_string()
}

impl Task {
    async fn run(&mut self) {
        let mut backoff = self.config.backoff_min;
        loop {
            if *self.stop.borrow() {
                break;
            }
            if self.assets.is_empty() {
                tokio::select! {
                    _ = self.stop.changed() => break,
                    cmd = self.cmds.recv() => match cmd {
                        Some(cmd) => { self.record(cmd); }
                        None => break,
                    },
                }
                continue;
            }
            let socket_config = self.socket_config();
            let socket = tokio::select! {
                _ = self.stop.changed() => break,
                connected = timeout(
                    self.config.connect_timeout,
                    connect_async_with_config(&self.config.url, Some(socket_config), false),
                ) => connected.ok().and_then(Result::ok).map(|(socket, _)| socket),
            };
            if let Some(socket) = socket {
                match self.session(socket, &mut backoff).await {
                    Ended::Stop => break,
                    Ended::Lost => {}
                }
                self.state.send_modify(|state| state.connected = None);
            }
            tokio::select! {
                _ = self.stop.changed() => break,
                () = sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(self.config.backoff_max);
        }
        self.state.send_modify(|state| state.connected = None);
    }

    fn socket_config(&self) -> WebSocketConfig {
        WebSocketConfig::default()
            .max_message_size(Some(self.config.max_message_bytes))
            .max_frame_size(Some(self.config.max_message_bytes))
    }

    /// Commands that arrive while no socket is live only edit the asset set; the
    /// next connect subscribes the whole set.
    fn record(&mut self, cmd: Cmd) -> Vec<String> {
        match cmd {
            Cmd::Subscribe(ids) => ids
                .into_iter()
                .filter(|id| self.assets.insert(id.clone()))
                .collect(),
            Cmd::Unsubscribe(ids) => ids
                .into_iter()
                .filter(|id| self.assets.remove(id))
                .collect(),
        }
    }

    async fn send(&self, socket: &mut Socket, text: String) -> bool {
        matches!(
            timeout(
                self.config.write_timeout,
                socket.send(Message::Text(text.into()))
            )
            .await,
            Ok(Ok(()))
        )
    }

    fn record_close(&self, code: Option<u16>) {
        let counter = match code {
            Some(1013) => &self.counters.close_1013,
            Some(1006) => &self.counters.close_1006,
            _ => &self.counters.close_other,
        };
        counter.fetch_add(1, Relaxed);
        self.state.send_modify(|state| state.last_close_code = code);
    }

    async fn close(&self, socket: &mut Socket) {
        let frame = CloseFrame {
            code: CloseCode::Normal,
            reason: "".into(),
        };
        let _ = timeout(self.config.close_timeout, socket.close(Some(frame))).await;
    }

    async fn session(&mut self, mut socket: Socket, backoff: &mut Duration) -> Ended {
        while let Ok(cmd) = self.cmds.try_recv() {
            self.record(cmd);
        }
        let initial: Vec<String> = self.assets.iter().cloned().collect();
        if !self.send(&mut socket, subscribe_frame(&initial)).await {
            return Ended::Lost;
        }
        self.conn_id += 1;
        let conn_id = self.conn_id;
        if conn_id > 1 {
            self.counters.reconnects.fetch_add(1, Relaxed);
        }
        self.state
            .send_modify(|state| state.connected = Some(conn_id));

        let mut ping = interval(self.config.ping_interval);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ping.tick().await;
        let idle = sleep_until(Instant::now() + self.config.read_idle_timeout);
        tokio::pin!(idle);
        let mut last_pong = Instant::now();
        let mut seq = 0_u64;

        loop {
            tokio::select! {
                biased;
                _ = self.stop.changed() => {
                    self.close(&mut socket).await;
                    return Ended::Stop;
                }
                cmd = self.cmds.recv() => {
                    let Some(cmd) = cmd else {
                        self.close(&mut socket).await;
                        return Ended::Stop;
                    };
                    let operation = if matches!(cmd, Cmd::Subscribe(_)) { "subscribe" } else { "unsubscribe" };
                    let ids = self.record(cmd);
                    if !ids.is_empty()
                        && !self.send(&mut socket, operation_frame(&ids, operation)).await
                    {
                        return Ended::Lost;
                    }
                }
                _ = ping.tick() => {
                    if last_pong.elapsed() > self.config.pong_timeout {
                        self.counters.idle_timeouts.fetch_add(1, Relaxed);
                        return Ended::Lost;
                    }
                    if !self.send(&mut socket, "PING".to_owned()).await {
                        return Ended::Lost;
                    }
                }
                () = &mut idle => {
                    self.counters.idle_timeouts.fetch_add(1, Relaxed);
                    return Ended::Lost;
                }
                message = socket.next() => {
                    idle.as_mut().reset(Instant::now() + self.config.read_idle_timeout);
                    let bytes: &[u8] = match &message {
                        Some(Ok(Message::Text(text))) if text.as_bytes() == b"PONG" => {
                            self.counters.pongs.fetch_add(1, Relaxed);
                            last_pong = Instant::now();
                            continue;
                        }
                        Some(Ok(Message::Text(text))) => text.as_bytes(),
                        Some(Ok(Message::Binary(bytes))) => bytes,
                        // tungstenite answers protocol pings itself.
                        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                        Some(Ok(Message::Close(frame))) => {
                            self.record_close(Some(frame.as_ref().map_or(1005, |f| u16::from(f.code))));
                            return Ended::Lost;
                        }
                        // Stream end or error without a close frame.
                        None | Some(Err(_)) => {
                            self.record_close(Some(1006));
                            return Ended::Lost;
                        }
                    };
                    seq += 1;
                    let received_at_ns = (self.config.clock)();
                    self.counters.frames_received.fetch_add(1, Relaxed);
                    *backoff = self.config.backoff_min;
                    if let Some(hook) = &self.config.raw_hook {
                        hook(conn_id, seq, received_at_ns, bytes);
                    }
                    match self.frames.try_reserve() {
                        Ok(permit) => {
                            permit.send(Frame { conn_id, seq, received_at_ns, bytes: bytes.to_vec() });
                            self.counters.frames_enqueued.fetch_add(1, Relaxed);                        }
                        Err(mpsc::error::TrySendError::Full(())) => {
                            self.counters.frames_dropped.fetch_add(1, Relaxed);
                        }
                        // The consumer is gone; nothing left to feed.
                        Err(mpsc::error::TrySendError::Closed(())) => {
                            self.close(&mut socket).await;
                            return Ended::Stop;
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicI64};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_tungstenite::{WebSocketStream, accept_async};

    type Server = WebSocketStream<TcpStream>;
    type Seen = Arc<Mutex<Vec<(u64, u64, i64, Vec<u8>)>>>;

    /// Compared as JSON: key order follows serde_json's features, not the venue.
    fn subscribe_a1_a2() -> serde_json::Value {
        serde_json::json!({"assets_ids": ["A1", "A2"], "type": "market", "custom_feature_enabled": true})
    }

    fn json(text: String) -> serde_json::Value {
        serde_json::from_str(&text).unwrap()
    }

    async fn listen() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        (listener, url)
    }

    async fn accept(listener: &TcpListener) -> Server {
        let (stream, _) = listener.accept().await.unwrap();
        accept_async(stream).await.unwrap()
    }

    async fn next_text(server: &mut Server) -> String {
        loop {
            match server.next().await {
                Some(Ok(Message::Text(text))) if text.as_bytes() != b"PING" => {
                    return text.as_str().to_owned();
                }
                Some(Ok(_)) => {}
                other => panic!("server socket ended: {other:?}"),
            }
        }
    }

    async fn send_text(server: &mut Server, text: &str) {
        server.send(Message::Text(text.into())).await.unwrap();
    }

    fn config(url: &str) -> (MarketChannelConfig, Seen) {
        let seen = Seen::default();
        let sink = Arc::clone(&seen);
        let tick = AtomicI64::new(0);
        let config = MarketChannelConfig {
            url: url.to_owned(),
            backoff_min: Duration::from_millis(20),
            backoff_max: Duration::from_millis(80),
            clock: Arc::new(move || tick.fetch_add(1, Relaxed) * 1_000),
            raw_hook: Some(Arc::new(move |conn, seq, ns, bytes| {
                sink.lock().unwrap().push((conn, seq, ns, bytes.to_vec()));
            })),
            ..MarketChannelConfig::default()
        };
        (config, seen)
    }

    async fn within<T>(future: impl Future<Output = T>) -> T {
        timeout(Duration::from_secs(10), future)
            .await
            .expect("timed out")
    }

    #[tokio::test]
    async fn frames_are_stamped_hooked_and_ordered_and_pong_is_not_a_frame() {
        let (listener, url) = listen().await;
        let (config, seen) = config(&url);
        let (handle, mut frames) = spawn(config, vec!["A2".into(), "A1".into()]);
        let mut server = accept(&listener).await;
        assert_eq!(
            json(within(next_text(&mut server)).await),
            subscribe_a1_a2()
        );
        for text in ["one", "PONG", "two", "three"] {
            send_text(&mut server, text).await;
        }

        for want in 1..=3_u64 {
            let frame = within(frames.recv()).await.unwrap();
            assert_eq!((frame.conn_id, frame.seq), (1, want));
            assert_eq!(
                frame.received_at_ns,
                (i64::try_from(want).unwrap() - 1) * 1_000
            );
        }
        let stats = handle.stats();
        assert_eq!(
            (stats.frames_received, stats.frames_enqueued, stats.pongs),
            (3, 3, 1)
        );
        let hooked = seen.lock().unwrap().clone();
        assert_eq!(hooked.len(), 3);
        assert_eq!(hooked[2], (1, 3, 2_000, b"three".to_vec()));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn full_queue_drops_counts_and_shows_as_a_seq_jump() {
        let (listener, url) = listen().await;
        let (mut config, seen) = config(&url);
        config.queue_capacity = 2;
        let (handle, mut frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut server = accept(&listener).await;
        next_text(&mut server).await;
        for n in 1..=5 {
            send_text(&mut server, &format!("f{n}")).await;
        }
        within(async {
            while handle.stats().frames_received < 5 {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await;

        let stats = handle.stats();
        assert_eq!((stats.frames_enqueued, stats.frames_dropped), (2, 3));
        assert_eq!(
            seen.lock().unwrap().len(),
            5,
            "the hook sees dropped frames too"
        );
        assert_eq!(within(frames.recv()).await.unwrap().seq, 1);
        assert_eq!(within(frames.recv()).await.unwrap().seq, 2);
        send_text(&mut server, "f6").await;
        assert_eq!(within(frames.recv()).await.unwrap().seq, 6);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn dynamic_subscribe_then_shutdown_is_ordered_idempotent_and_final() {
        let (listener, url) = listen().await;
        let (config, _) = config(&url);
        let (handle, mut frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut server = accept(&listener).await;
        next_text(&mut server).await;

        handle.subscribe(vec!["A3".into(), "A1".into()]).unwrap();
        assert_eq!(
            json(within(next_text(&mut server)).await),
            serde_json::json!({"assets_ids": ["A3"], "operation": "subscribe", "custom_feature_enabled": true})
        );

        let other = handle.clone();
        within(async { tokio::join!(handle.shutdown(), other.shutdown()) }).await;
        handle.shutdown().await;
        assert!(within(frames.recv()).await.is_none());
        assert_eq!(handle.subscribe(vec!["A4".into()]), Err(Closed));
        let close = within(async {
            loop {
                match server.next().await {
                    Some(Ok(Message::Close(frame))) => return frame.map(|f| u16::from(f.code)),
                    Some(Ok(_)) => {}
                    _ => return None,
                }
            }
        })
        .await;
        assert_eq!(close, Some(1000));
    }

    #[tokio::test]
    async fn close_1013_reconnects_and_resubscribes_the_same_assets() {
        let (listener, url) = listen().await;
        let (config, _) = config(&url);
        let (handle, mut frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut state = handle.state();

        let mut first = accept(&listener).await;
        next_text(&mut first).await;
        send_text(&mut first, "before").await;
        assert_eq!(within(frames.recv()).await.unwrap().conn_id, 1);
        first
            .close(Some(CloseFrame {
                code: CloseCode::from(1013),
                reason: "slow consumer".into(),
            }))
            .await
            .unwrap();

        let mut second = accept(&listener).await;
        assert_eq!(
            json(within(next_text(&mut second)).await),
            subscribe_a1_a2()
        );
        send_text(&mut second, "after").await;
        let frame = within(frames.recv()).await.unwrap();
        assert_eq!((frame.conn_id, frame.seq), (2, 1));
        let stats = handle.stats();
        assert_eq!(
            (stats.close_1013, stats.close_1006, stats.reconnects),
            (1, 0, 1)
        );
        let seen = *within(state.wait_for(|s| s.connected == Some(2)))
            .await
            .unwrap();
        assert_eq!(seen.last_close_code, Some(1013));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn dropped_socket_without_close_frame_counts_as_1006() {
        let (listener, url) = listen().await;
        let (config, _) = config(&url);
        let (handle, _frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut first = accept(&listener).await;
        next_text(&mut first).await;
        drop(first);
        let mut second = accept(&listener).await;
        next_text(&mut second).await;
        assert_eq!(handle.stats().close_1006, 1);
        assert_eq!(handle.state().borrow().last_close_code, Some(1006));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn silent_socket_hits_the_idle_timeout_and_reconnects() {
        let (listener, url) = listen().await;
        let (mut config, _) = config(&url);
        config.read_idle_timeout = Duration::from_millis(100);
        let (handle, _frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut first = accept(&listener).await;
        next_text(&mut first).await;
        let mut second = accept(&listener).await;
        assert_eq!(
            json(within(next_text(&mut second)).await),
            subscribe_a1_a2()
        );
        let stats = handle.stats();
        assert_eq!((stats.idle_timeouts, stats.reconnects), (1, 1));
        drop(first);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn data_without_pong_reconnects_on_the_pong_timeout() {
        let (listener, url) = listen().await;
        let (mut config, _) = config(&url);
        config.ping_interval = Duration::from_millis(20);
        config.pong_timeout = Duration::from_millis(60);
        let (handle, _frames) = spawn(config, vec!["A1".into(), "A2".into()]);
        let mut first = accept(&listener).await;
        next_text(&mut first).await;
        // Data keeps the read-idle timer fresh, so only the missing PONG can end it.
        let second = tokio::select! {
            server = accept(&listener) => server,
            () = async {
                loop {
                    let _ = first.send(Message::Text("tick".into())).await;
                    sleep(Duration::from_millis(10)).await;
                }
            } => unreachable!(),
        };
        assert_eq!(handle.stats().idle_timeouts, 1);
        drop(second);
        handle.shutdown().await;
    }
}
