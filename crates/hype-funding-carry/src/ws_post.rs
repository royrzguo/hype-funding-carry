//! Persistent post channel. Each accepted request is sent at most once; an uncertain
//! acknowledgement is returned to the caller for account/order reconciliation, never replayed.
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore},
    time::{timeout, timeout_at, Instant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

pub const MAINNET_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// This request never entered socket.send. Only this error permits another transport.
#[derive(Debug)]
pub struct NotSent(&'static str);

impl std::fmt::Display for NotSent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "WS post {} before send", self.0)
    }
}

impl std::error::Error for NotSent {}

#[derive(Default)]
struct PostTrace {
    enabled: bool,
    queued_at: Option<Instant>,
    picked_at: Option<Instant>,
    sent_at: Option<(i64, Instant)>,
    flushed_at: Option<Instant>,
    received_at: Option<Instant>,
    parsed_at: Option<Instant>,
    action_type: String,
    outcome: &'static str,
}

type PostReply = (Result<Value>, Option<(u64, Instant)>);

struct Request {
    kind: &'static str,
    payload: Value,
    deadline: Instant,
    queued_at: Option<Instant>,
    result: oneshot::Sender<PostReply>,
    _permit: OwnedSemaphorePermit,
}

pub struct WsPost {
    requests: mpsc::Sender<Request>,
    connected: Arc<AtomicBool>,
    permit: Arc<Semaphore>,
}

impl WsPost {
    pub async fn connect(url: &str) -> Result<Self> {
        let socket = connect(url).await?;
        let (requests, rx) = mpsc::channel(1);
        let connected = Arc::new(AtomicBool::new(true));
        let permit = Arc::new(Semaphore::new(1));
        tokio::spawn(run(
            url.to_string(),
            socket,
            rx,
            connected.clone(),
            permit.clone(),
        ));
        Ok(Self {
            requests,
            connected,
            permit,
        })
    }

    pub async fn post(&self, kind: &'static str, payload: Value) -> Result<Value> {
        self.post_with_timeout(kind, payload, REQUEST_TIMEOUT).await
    }

    async fn post_with_timeout(
        &self,
        kind: &'static str,
        payload: Value,
        wait: Duration,
    ) -> Result<Value> {
        if !self.connected.load(Ordering::Acquire) {
            return Err(NotSent("disconnected").into());
        }
        let permit = self
            .permit
            .clone()
            .try_acquire_owned()
            .map_err(|_| NotSent("busy"))?;
        let (result, response) = oneshot::channel();
        let deadline = Instant::now() + wait;
        let queued_at = (std::env::var("HFC_TRACE_IO").as_deref() == Ok("1")).then(Instant::now);
        self.requests
            .try_send(Request {
                kind,
                payload,
                deadline,
                queued_at,
                result,
                _permit: permit,
            })
            .map_err(|_| NotSent("unavailable (closed or full queue)"))?;
        let (result, ready_at) = timeout_at(deadline, response)
            .await
            .context("WS post deadline exceeded; outcome unknown, reconcile before retry")?
            .context("WS post worker stopped; outcome unknown, reconcile before retry")?;
        if let Some((id, ready_at)) = ready_at {
            eprintln!(
                "{}",
                json!({"event":"hfc_io_dispatch","transport":"ws_post","id":id,"kind":kind,"dispatch_us":ready_at.elapsed().as_micros()})
            );
        }
        result
    }
}

async fn connect(url: &str) -> Result<Socket> {
    Ok(timeout(REQUEST_TIMEOUT, connect_async(url))
        .await
        .context("WS post connect timeout before send")??
        .0)
}

async fn run(
    url: String,
    socket: Socket,
    mut rx: mpsc::Receiver<Request>,
    connected: Arc<AtomicBool>,
    permit: Arc<Semaphore>,
) {
    let mut socket = Some(socket);
    let mut id = 0u64;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut reconnect_at = Instant::now() + Duration::from_secs(1);
    loop {
        let Some(ws) = socket.as_mut() else {
            // Reconnect the transport only. Failed or queued actions are never replayed.
            tokio::select! {
                command = rx.recv() => {
                    let Some(command) = command else {return};
                    let _ = command.result.send((Err(NotSent("disconnected").into()), None));
                }
                result = async {tokio::time::sleep_until(reconnect_at).await; connect(&url).await} => {
                    match result {
                        Ok(ws) => socket = Some(ws),
                        Err(error) => eprintln!("ws_post_reconnect_failed={error:#}"),
                    }
                    connected.store(socket.is_some(), Ordering::Release);
                    reconnect_at = Instant::now() + Duration::from_secs(1);
                }
            }
            continue;
        };
        tokio::select! {
            biased;
            command = rx.recv() => {
                let Some(command) = command else {return};
                if command.result.is_closed() {continue;}
                if command.deadline <= Instant::now() {
                    let _ = command.result.send((Err(NotSent("expired").into()), None));
                    continue;
                }
                id += 1;
                let mut trace = PostTrace {
                    enabled: command.queued_at.is_some(),
                    queued_at: command.queued_at,
                    picked_at: command.queued_at.map(|_| Instant::now()),
                    ..PostTrace::default()
                };
                let result = match timeout_at(command.deadline, round_trip(ws, id, command.kind, command.payload, command.deadline, &mut trace)).await {
                    Ok(result) => result,
                    Err(_) => Err(anyhow::anyhow!("WS post acknowledgement timeout; outcome unknown, reconcile before retry")),
                };
                let finished_at = Instant::now();
                let finish_ts_ms = crate::api::now_ms();
                if result.is_err() {
                    socket = None;
                    connected.store(false, Ordering::Release);
                    reconnect_at = Instant::now() + Duration::from_secs(1);
                }
                // A resumed caller may immediately hedge a fill. Release capacity
                // before waking it, including when diagnostic output is enabled.
                drop(command._permit);
                let _ = command.result.send((result, trace.enabled.then(|| (id, Instant::now()))));
                // Emit only after the socket operation and reply handoff. Synchronous
                // diagnostic output must not sit between the send timestamp and write.
                if let Some((start_ts_ms, started)) = trace.sent_at {
                    eprintln!("{}", json!({"event":"hfc_io_send","transport":"ws_post","id":id,"kind":command.kind,"action_type":trace.action_type,"start_ts_ms":start_ts_ms}));
                    eprintln!("{}", json!({"event":"hfc_io_finish","transport":"ws_post","id":id,"kind":command.kind,"action_type":trace.action_type,"start_ts_ms":start_ts_ms,"finish_ts_ms":finish_ts_ms,"elapsed_ms":finished_at.duration_since(started).as_millis(),"elapsed_us":finished_at.duration_since(started).as_micros(),"queue_us":trace.picked_at.zip(trace.queued_at).map(|(end,start)|end.duration_since(start).as_micros()),"encode_us":trace.picked_at.map(|start|started.duration_since(start).as_micros()),"write_us":trace.flushed_at.map(|end|end.duration_since(started).as_micros()),"reply_wait_us":trace.received_at.zip(trace.flushed_at).map(|(end,start)|end.duration_since(start).as_micros()),"parse_us":trace.parsed_at.zip(trace.received_at).map(|(end,start)|end.duration_since(start).as_micros()),"finish_us":trace.parsed_at.map(|start|finished_at.duration_since(start).as_micros()),"outcome":trace.outcome}));
                }
            }
            frame = ws.next() => {
                match frame {
                    Some(Ok(Message::Ping(bytes))) => {
                        let Ok(_permit) = permit.clone().try_acquire_owned() else {continue;};
                        if !matches!(timeout(REQUEST_TIMEOUT, ws.send(Message::Pong(bytes))).await, Ok(Ok(()))) {socket = None;}
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => socket = None,
                    _ => {}
                }
            }
            _ = ping.tick() => {
                let Ok(_permit) = permit.clone().try_acquire_owned() else {continue;};
                if !matches!(timeout(REQUEST_TIMEOUT, ws.send(Message::Text(json!({"method":"ping"}).to_string()))).await, Ok(Ok(()))) {socket = None;}
            }
        }
        if socket.is_none() {
            connected.store(false, Ordering::Release);
        }
    }
}

async fn round_trip(
    ws: &mut Socket,
    id: u64,
    kind: &str,
    payload: Value,
    deadline: Instant,
    trace: &mut PostTrace,
) -> Result<Value> {
    if trace.enabled {
        trace.action_type = payload["action"]["type"]
            .as_str()
            .unwrap_or("unknown")
            .to_string();
    }
    let message = Message::Text(
        json!({"method":"post","id":id,
        "request":{"type":kind,"payload":payload}})
        .to_string(),
    );
    if deadline <= Instant::now() {
        return Err(NotSent("expired").into());
    }
    if trace.enabled {
        let start_ts_ms = crate::api::now_ms();
        trace.sent_at = Some((start_ts_ms, Instant::now()));
        trace.outcome = "unknown";
    }
    ws.send(message)
        .await
        .context("WS post send failed; outcome unknown, reconcile before retry")?;
    trace.flushed_at = trace.enabled.then(Instant::now);
    while let Some(frame) = ws.next().await {
        let received_at = trace.enabled.then(Instant::now);
        match frame.context("WS post read failed; outcome unknown, reconcile before retry")? {
            Message::Text(text) => {
                let response: Value = serde_json::from_str(&text)
                    .context("invalid WS post response; outcome unknown, reconcile before retry")?;
                if response["channel"] != "post" || response["data"]["id"].as_u64() != Some(id) {
                    continue;
                }
                trace.received_at = received_at;
                trace.parsed_at = trace.enabled.then(Instant::now);
                let response = &response["data"]["response"];
                if response["type"] == "error" {
                    trace.outcome = "rejected";
                    bail!("WS post rejected: {}", response["payload"]);
                }
                if response["type"] != kind {
                    bail!(
                        "WS post response type mismatch; outcome unknown, reconcile before retry"
                    );
                }
                let payload = response.get("payload").cloned().context(
                    "WS post response missing payload; outcome unknown, reconcile before retry",
                )?;
                trace.outcome = "ack";
                return Ok(payload);
            }
            Message::Ping(bytes) => ws.send(Message::Pong(bytes)).await?,
            Message::Close(_) => break,
            _ => {}
        }
    }
    bail!("WS post disconnected awaiting acknowledgement; outcome unknown, reconcile before retry")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

    async fn next_post<S>(socket: &mut WebSocketStream<S>) -> Value
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            let Message::Text(text) = socket.next().await.unwrap().unwrap() else {
                continue;
            };
            let value: Value = serde_json::from_str(&text).unwrap();
            if value["method"] == "post" {
                return value;
            }
        }
    }

    #[tokio::test]
    async fn response_ids_and_typed_exchange_payload() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            for expected in [1, 2] {
                let request = next_post(&mut ws).await;
                assert_eq!(request["id"], expected);
                let payload = json!({"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":88383}}]}}});
                for id in [999, expected] {
                    ws.send(Message::Text(json!({"channel":"post","data":{"id":id,"response":{"type":"action","payload":payload}}}).to_string())).await.unwrap();
                }
            }
        });
        let client = WsPost::connect(&url).await?;
        for _ in 0..2 {
            let value = client.post("action", json!({})).await?;
            #[cfg(feature = "live-sdk")]
            assert!(matches!(
                serde_json::from_value::<hyperliquid_rust_sdk::ExchangeResponseStatus>(value)?,
                hyperliquid_rust_sdk::ExchangeResponseStatus::Ok(_)
            ));
            #[cfg(not(feature = "live-sdk"))]
            assert_eq!(value["status"], "ok");
        }
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn timeout_and_disconnect_never_replay() -> Result<()> {
        for close in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("ws://{}", listener.local_addr()?);
            let server = tokio::spawn(async move {
                let mut ws = accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();
                let request = next_post(&mut ws).await;
                if close {
                    ws.close(None).await.unwrap();
                }
                let mut ws = accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();
                let next = next_post(&mut ws).await;
                assert_eq!(request["request"]["payload"]["nonce"], 1);
                assert_eq!(next["request"]["payload"]["nonce"], 2);
                ws.send(Message::Text(json!({"channel":"post","data":{"id":next["id"],"response":{"type":"action","payload":{}}}}).to_string())).await.unwrap();
            });
            let client = WsPost::connect(&url).await?;
            let started = Instant::now();
            let error = client
                .post_with_timeout("action", json!({"nonce":1}), Duration::from_millis(100))
                .await
                .unwrap_err();
            assert!(error.to_string().contains("outcome unknown"));
            assert!(error.downcast_ref::<NotSent>().is_none());
            assert!(started.elapsed() < Duration::from_millis(500));
            tokio::time::sleep(Duration::from_millis(1300)).await;
            client.post("action", json!({"nonce":2})).await?;
            server.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn slow_reconnect_rejects_actions_without_queueing() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (connecting, started) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            assert_eq!(next_post(&mut ws).await["request"]["payload"]["nonce"], 1);
            ws.close(None).await.unwrap();
            let stream = listener.accept().await.unwrap().0;
            connecting.send(()).unwrap();
            released.await.unwrap();
            let mut ws = accept_async(stream).await.unwrap();
            let request = next_post(&mut ws).await;
            assert_eq!(request["request"]["payload"]["nonce"], 2);
            ws.send(Message::Text(json!({"channel":"post","data":{"id":request["id"],"response":{"type":"action","payload":{}}}}).to_string())).await.unwrap();
        });
        let client = WsPost::connect(&url).await?;
        assert!(client.post("action", json!({"nonce":1})).await.is_err());
        timeout(Duration::from_secs(3), started).await??;
        for _ in 0..20 {
            let error = timeout(
                Duration::from_millis(50),
                client.post("action", json!({"nonce":999})),
            )
            .await?
            .unwrap_err();
            assert!(error.to_string().contains("disconnected before send"));
            assert!(error.downcast_ref::<NotSent>().is_some());
        }
        release.send(()).unwrap();
        timeout(Duration::from_secs(1), async {
            while !client.connected.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        client.post("action", json!({"nonce":2})).await?;
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn pending_action_rejects_second_action_and_expired_action_never_sends() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (received, receipt) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            assert_eq!(next_post(&mut ws).await["request"]["payload"]["nonce"], 1);
            received.send(()).unwrap();
            // No acknowledgement: first request has a deadline, second must never be queued.
            let _ = timeout(Duration::from_millis(300), async {
                while let Some(Ok(frame)) = ws.next().await {
                    if let Message::Text(text) = frame {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        assert_ne!(value["method"], "post");
                    }
                }
            })
            .await;
        });
        let client = Arc::new(WsPost::connect(&url).await?);
        let first_client = client.clone();
        let first = tokio::spawn(async move {
            first_client
                .post_with_timeout("action", json!({"nonce":1}), Duration::from_millis(100))
                .await
        });
        receipt.await?;
        let error = client.post("action", json!({"nonce":2})).await.unwrap_err();
        assert!(error.to_string().contains("busy before send"));
        assert!(error.downcast_ref::<NotSent>().is_some());
        assert!(first.await?.is_err());
        server.await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            assert!(timeout(Duration::from_millis(100), next_post(&mut ws))
                .await
                .is_err());
        });
        let client = WsPost::connect(&url).await?;
        assert!(client
            .post_with_timeout("action", json!({"nonce":3}), Duration::ZERO)
            .await
            .is_err());
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn unavailable_queue_and_pre_send_expiry_are_typed_unsent() -> Result<()> {
        let (requests, rx) = mpsc::channel(1);
        drop(rx);
        let client = WsPost {
            requests,
            connected: Arc::new(AtomicBool::new(true)),
            permit: Arc::new(Semaphore::new(1)),
        };
        let error = client.post("action", json!({})).await.unwrap_err();
        assert!(error.downcast_ref::<NotSent>().is_some());

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            assert!(timeout(Duration::from_millis(100), next_post(&mut ws))
                .await
                .is_err());
        });
        let mut socket = connect(&url).await?;
        let mut trace = PostTrace {
            enabled: true,
            ..PostTrace::default()
        };
        let error = round_trip(
            &mut socket,
            1,
            "action",
            json!({}),
            Instant::now(),
            &mut trace,
        )
        .await
        .unwrap_err();
        assert!(error.downcast_ref::<NotSent>().is_some());
        assert!(
            trace.sent_at.is_none(),
            "an unsent request must not be counted as socket I/O"
        );
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn trace_counts_actual_send_and_distinguishes_unknown_ack() -> Result<()> {
        for acknowledge in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let url = format!("ws://{}", listener.local_addr()?);
            let server = tokio::spawn(async move {
                let mut ws = accept_async(listener.accept().await.unwrap().0)
                    .await
                    .unwrap();
                let request = next_post(&mut ws).await;
                if acknowledge {
                    ws.send(Message::Text(json!({"channel":"post","data":{"id":request["id"],"response":{"type":"action","payload":{}}}}).to_string())).await.unwrap();
                } else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            });
            let mut socket = connect(&url).await?;
            let mut trace = PostTrace {
                enabled: true,
                ..PostTrace::default()
            };
            let deadline = Instant::now() + Duration::from_millis(50);
            let result = timeout_at(
                deadline,
                round_trip(
                    &mut socket,
                    1,
                    "action",
                    json!({"action":{"type":"order"}}),
                    deadline,
                    &mut trace,
                ),
            )
            .await;
            assert!(trace.sent_at.is_some());
            assert_eq!(trace.outcome, if acknowledge { "ack" } else { "unknown" });
            assert_eq!(result.is_ok(), acknowledge);
            server.await?;
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn traced_ack_releases_capacity_before_waking_next_action() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            for _ in 0..8 {
                let request = next_post(&mut ws).await;
                ws.send(Message::Text(json!({"channel":"post","data":{"id":request["id"],"response":{"type":"action","payload":{}}}}).to_string())).await.unwrap();
            }
        });
        let client = WsPost::connect(&url).await?;
        for _ in 0..8 {
            let permit = client.permit.clone().try_acquire_owned()?;
            let (result, response) = oneshot::channel();
            client
                .requests
                .try_send(Request {
                    kind: "action",
                    payload: json!({}),
                    deadline: Instant::now() + Duration::from_secs(1),
                    queued_at: Some(Instant::now()),
                    result,
                    _permit: permit,
                })
                .map_err(|_| anyhow::anyhow!("test queue unavailable"))?;
            let (result, _) = timeout(Duration::from_secs(1), response).await??;
            result?;
            assert_eq!(
                client.permit.available_permits(),
                1,
                "acknowledged order must leave capacity for immediate hedge"
            );
        }
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn trace_separates_flush_reply_wait_and_json_parse() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = next_post(&mut ws).await;
            ws.send(Message::Text(
                json!({"channel":"post","data":{"id":999,"response":{"type":"info","payload":{}}}})
                    .to_string(),
            ))
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            ws.send(Message::Text(json!({"channel":"post","data":{"id":request["id"],"response":{"type":"info","payload":{"ok":true}}}}).to_string())).await.unwrap();
        });
        let mut socket = connect(&url).await?;
        let mut trace = PostTrace {
            enabled: true,
            ..PostTrace::default()
        };
        assert_eq!(
            round_trip(
                &mut socket,
                1,
                "info",
                json!({}),
                Instant::now() + Duration::from_secs(1),
                &mut trace
            )
            .await?["ok"],
            true
        );
        let sent = trace.sent_at.unwrap().1;
        let flushed = trace.flushed_at.unwrap();
        let received = trace.received_at.unwrap();
        let parsed = trace.parsed_at.unwrap();
        assert!(sent <= flushed && flushed <= received && received <= parsed);
        assert!(received.duration_since(flushed) >= Duration::from_millis(20));
        assert_eq!(
            parsed.duration_since(sent),
            flushed.duration_since(sent)
                + received.duration_since(flushed)
                + parsed.duration_since(received)
        );
        server.await?;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "unsigned public network probe; never sends an action"]
    async fn mainnet_info() -> Result<()> {
        let client = WsPost::connect(MAINNET_WS_URL).await?;
        for _ in 0..12 {
            let started = Instant::now();
            let response = client
                .post("info", json!({"type":"l2Book","coin":"HYPE"}))
                .await?;
            assert_eq!(response["type"], "l2Book");
            assert_eq!(response["data"]["coin"], "HYPE");
            assert!(!response["data"]["levels"][0]
                .as_array()
                .context("missing bids")?
                .is_empty());
            println!("unsigned WS info RTT={}ms", started.elapsed().as_millis());
        }
        Ok(())
    }
}
