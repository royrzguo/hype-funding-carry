use super::*;

type CachedExchange = Option<(String, String, Arc<ExchangeClient>)>;

/// Constructing an `ExchangeClient` pulls `meta` and `spotMeta` from the exchange, about
/// 130KB over two round trips, and `open_s3` builds one twice per entry. The payload is the
/// asset listing, which only changes when Hyperliquid lists a coin, so the client is reused.
/// The local credential is cached for one process; key rotation takes effect on restart.
/// Account and expected-agent identity checks remain outside the cache.
pub(super) async fn exchange(account: &str) -> Result<Arc<ExchangeClient>> {
    static CACHE: OnceLock<TokioMutex<CachedExchange>> = OnceLock::new();
    let key = load_private_key()?;
    let fingerprint = credential_fingerprint(&key);
    let expected = std::env::var("HFC_AGENT_ADDRESS").context("HFC_AGENT_ADDRESS required")?;
    validate_confirmation(&std::env::var("HFC_ACCOUNT_ADDRESS")?, account)?;
    let mut cache = CACHE.get_or_init(|| TokioMutex::new(None)).lock().await;
    if let Some((cached_fingerprint, cached_agent, client)) = cache.as_ref() {
        if cached_fingerprint == &fingerprint && cached_agent.eq_ignore_ascii_case(&expected) {
            return Ok(client.clone());
        }
    }

    let wallet: PrivateKeySigner = key
        .parse()
        .context("parse HYPE Funding Carry agent private key")?;
    if !wallet.address().to_string().eq_ignore_ascii_case(&expected) {
        bail!("agent credential does not derive HFC_AGENT_ADDRESS");
    }
    wallet
        .sign_message(b"hype-funding-carry-live-preflight")
        .await
        .context("offline live preflight signature")?;

    let client = Arc::new(
        ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None)
            .await
            .context("initialize pinned Hyperliquid SDK")?,
    );
    assert_pinned_asset_indices(&client)?;
    *cache = Some((fingerprint, expected.to_ascii_lowercase(), client.clone()));
    Ok(client)
}

pub async fn warm_exchange(account: &str) -> Result<()> {
    exchange(account).await?;
    ws_post().await?;
    Ok(())
}

async fn ws_post() -> Result<Arc<crate::ws_post::WsPost>> {
    static CACHE: OnceLock<TokioMutex<Option<Arc<crate::ws_post::WsPost>>>> = OnceLock::new();
    let mut cache = CACHE.get_or_init(|| TokioMutex::new(None)).lock().await;
    if let Some(client) = cache.as_ref() {
        return Ok(client.clone());
    }
    let client = Arc::new(crate::ws_post::WsPost::connect(crate::ws_post::MAINNET_WS_URL).await?);
    *cache = Some(client.clone());
    Ok(client)
}

pub(super) async fn cancel_ws(
    exchange: &ExchangeClient,
    request: ClientCancelRequest,
) -> Result<ExchangeResponseStatus> {
    let client = ws_post().await?;
    let payload = crate::order_signing::cancel_payload(exchange, &request.asset, request.oid)?;
    decode_ws_ack(client.post("action", payload).await?)
}

fn decode_ws_ack(value: Value) -> Result<ExchangeResponseStatus> {
    let started =
        (std::env::var("HFC_TRACE_IO").as_deref() == Ok("1")).then(std::time::Instant::now);
    let response = serde_json::from_value(value)
        .context("decode WS acknowledgement; outcome unknown, reconcile before retry");
    if let Some(started) = started {
        eprintln!(
            "{}",
            json!({"event":"hfc_io_decode","transport":"ws_post","decode_us":started.elapsed().as_micros(),"ok":response.is_ok()})
        );
    }
    response
}

pub(super) async fn cancel_http(
    exchange: &ExchangeClient,
    request: ClientCancelRequest,
) -> Result<ExchangeResponseStatus> {
    let payload = crate::order_signing::cancel_payload(exchange, &request.asset, request.oid)?;
    post_http(exchange, payload).await
}

async fn post_http(exchange: &ExchangeClient, payload: Value) -> Result<ExchangeResponseStatus> {
    // WS and recovery HTTP must use the same nonce allocator. The SDK's private
    // allocator is separate, so reuse its HTTP client with our already-signed payload.
    let body = serde_json::to_string(&payload)?;
    let trace = (std::env::var("HFC_TRACE_IO").as_deref() == Ok("1"))
        .then(|| (now_ms(), std::time::Instant::now()));
    let action_type = payload["action"]["type"].as_str().unwrap_or("unknown");
    if let Some((start_ts_ms, _)) = trace {
        eprintln!(
            "{}",
            json!({"event":"hfc_io_send","transport":"http_recovery","kind":"action","action_type":action_type,"start_ts_ms":start_ts_ms})
        );
    }
    let result = async {
        let response = exchange.http_client.post("/exchange", body).await.context(
            "HTTP recovery acknowledgement failed; outcome unknown, reconcile before retry",
        )?;
        serde_json::from_str(&response).context(
            "decode HTTP recovery acknowledgement; outcome unknown, reconcile before retry",
        )
    }
    .await;
    if let Some((start_ts_ms, started)) = trace {
        eprintln!(
            "{}",
            json!({"event":"hfc_io_finish","transport":"http_recovery","kind":"action","action_type":action_type,"start_ts_ms":start_ts_ms,"finish_ts_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis(),"outcome":if result.is_ok() {"ack"} else {"unknown"}})
        );
    }
    result
}

pub(super) fn credential_fingerprint(key: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(key.as_bytes()))
}

/// The SDK freezes its name-to-asset-index map at construction, so a cached client would keep
/// submitting against whatever index the listing had when the process started. Pin the two
/// names the order path actually uses, taken from the exchange listing: HYPE sits at 159 in
/// the perp universe and the spot pair @107 maps to 10000 + 107.
///
/// This catches a listing that moved before this process started. Catching one that moves
/// while it runs needs a fresh meta read off the trading path, which is not built yet.
pub(super) fn assert_pinned_asset_indices(client: &ExchangeClient) -> Result<()> {
    for name in [PERP, SPOT] {
        let index = client
            .coin_to_asset
            .get(name)
            .copied()
            .with_context(|| format!("pinned SDK has no asset index for {name}"))?;
        let expected = match name {
            PERP => HYPE_PERP_ASSET_INDEX,
            _ => HYPE_SPOT_ASSET_INDEX,
        };
        if index != expected {
            bail!("asset index for {name} moved from {expected} to {index}");
        }
    }
    Ok(())
}

pub(super) fn request(spec: &OrderSpec) -> ClientOrderRequest {
    ClientOrderRequest {
        asset: spec.asset.to_string(),
        is_buy: spec.is_buy,
        reduce_only: spec.reduce_only,
        limit_px: spec.px,
        sz: spec.qty,
        cloid: Some(spec.cloid),
        order_type: ClientOrder::Limit(ClientLimit {
            tif: spec.tif.to_string(),
        }),
    }
}

pub(super) fn outcomes(
    response: &ExchangeResponseStatus,
    request_ts_ms: i64,
    response_ts_ms: i64,
) -> Vec<OrderOutcome> {
    let raw = format!("{response:?}");
    match response {
        ExchangeResponseStatus::Err(error) => vec![OrderOutcome {
            status: "error".to_string(),
            oid: 0,
            filled_qty: 0.0,
            avg_fill_px: 0.0,
            request_ts_ms,
            response_ts_ms,
            error: error.clone(),
            raw,
        }],
        ExchangeResponseStatus::Ok(response) => response
            .data
            .as_ref()
            .map(|data| {
                data.statuses
                    .iter()
                    .map(|status| match status {
                        ExchangeDataStatus::Success => ("success", 0, 0.0, 0.0, ""),
                        ExchangeDataStatus::WaitingForFill => ("waiting_for_fill", 0, 0.0, 0.0, ""),
                        ExchangeDataStatus::WaitingForTrigger => {
                            ("waiting_for_trigger", 0, 0.0, 0.0, "")
                        }
                        ExchangeDataStatus::Error(error) => ("error", 0, 0.0, 0.0, error.as_str()),
                        ExchangeDataStatus::Resting(order) => ("resting", order.oid, 0.0, 0.0, ""),
                        ExchangeDataStatus::Filled(order) => (
                            "filled",
                            order.oid,
                            order.total_sz.parse().unwrap_or(0.0),
                            order.avg_px.parse().unwrap_or(0.0),
                            "",
                        ),
                    })
                    .map(
                        |(status, oid, filled_qty, avg_fill_px, error)| OrderOutcome {
                            status: status.to_string(),
                            oid,
                            filled_qty,
                            avg_fill_px,
                            request_ts_ms,
                            response_ts_ms,
                            error: error.to_string(),
                            raw: raw.clone(),
                        },
                    )
                    .collect()
            })
            .unwrap_or_else(|| {
                vec![OrderOutcome {
                    status: "ok_no_data".to_string(),
                    oid: 0,
                    filled_qty: 0.0,
                    avg_fill_px: 0.0,
                    request_ts_ms,
                    response_ts_ms,
                    error: String::new(),
                    raw,
                }]
            }),
    }
}

pub(super) async fn send_one(exchange: &ExchangeClient, spec: &OrderSpec) -> Result<OrderOutcome> {
    let request_ts_ms = now_ms();
    let payload = crate::order_signing::order_payload(exchange, request(spec))?;
    let response = post_http(exchange, payload)
        .await
        .with_context(|| format!("submit recovery {}", spec.label))?;
    order_outcome(response, request_ts_ms, spec)
}

async fn send_one_fast(exchange: &ExchangeClient, spec: &OrderSpec) -> Result<OrderOutcome> {
    let request_ts_ms = now_ms();
    let client = ws_post().await?;
    let payload = crate::order_signing::order_payload(exchange, request(spec))?;
    let response = client
        .post("action", payload)
        .await
        .with_context(|| format!("submit {}", spec.label))?;
    let response = decode_ws_ack(response)?;
    order_outcome(response, request_ts_ms, spec)
}

fn order_outcome(
    response: ExchangeResponseStatus,
    request_ts_ms: i64,
    spec: &OrderSpec,
) -> Result<OrderOutcome> {
    let response_ts_ms = now_ms();
    let outcome = outcomes(&response, request_ts_ms, response_ts_ms)
        .into_iter()
        .next()
        .context("missing order status")?;
    println!(
        "{}",
        json!({
            "test":spec.label,
            "cloid":cloid_hex(spec.cloid),
            "status":outcome.status,
            "oid":outcome.oid,
            "error":outcome.error
        })
    );
    Ok(outcome)
}

pub(super) async fn submit_one(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    decision_id: &str,
    spec: &OrderSpec,
) -> Result<OrderOutcome> {
    let outcome = send_one(exchange, spec).await?;
    audit_order_background(audit, decision_id, spec, &outcome);
    Ok(outcome)
}

pub(super) async fn submit_one_fast(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    decision_id: &str,
    spec: &OrderSpec,
) -> Result<OrderOutcome> {
    let outcome = send_one_fast(exchange, spec).await?;
    audit_order_background(audit, decision_id, spec, &outcome);
    Ok(outcome)
}

pub(super) fn audit_hedge_latency_background(audit: &AuditClient, row: HedgeLatencyRow) {
    if let Err(error) = audit.enqueue_best_effort("hfc_exec_hedge_latency", &row) {
        eprintln!("audit_hedge_latency_queue_failed={error:#}");
    }
}

pub(super) async fn cancel_oid(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    decision_id: &str,
    spec: &OrderSpec,
    oid: u64,
) -> Result<OrderOutcome> {
    let request_ts_ms = now_ms();
    let response = cancel_ws(
        exchange,
        ClientCancelRequest {
            asset: spec.asset.to_string(),
            oid,
        },
    )
    .await
    .with_context(|| format!("cancel {} oid={oid}", spec.label))?;
    let response_ts_ms = now_ms();
    let outcome = outcomes(&response, request_ts_ms, response_ts_ms)
        .into_iter()
        .next()
        .context("missing cancel status")?;
    audit_order_background(audit, decision_id, spec, &outcome);
    Ok(outcome)
}
