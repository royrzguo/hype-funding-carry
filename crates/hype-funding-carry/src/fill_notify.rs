use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Serialize;
use serde_json::Value;

use crate::api::{now_ms, InfoClient};
use crate::audit::AuditClient;
use crate::notify::send_dedup;

#[derive(Serialize)]
struct FillRow {
    decision_id: String,
    cloid: String,
    oid: u64,
    fill_ts_ms: i64,
    venue: String,
    side: String,
    liquidity: String,
    px: f64,
    qty: f64,
    fee_usd: f64,
    fee_native: f64,
    fee_token: String,
    tid: u64,
    order_submit_ts_ms: i64,
    initial_queue_qty: f64,
    eligible_tape_qty: Option<f64>,
    queue_multiplier: Option<f64>,
    queue_metric_ready: u8,
    live_basis_at_fill_bps: Option<f64>,
    raw_fill: String,
    created_ts_ms: i64,
}

#[derive(Clone)]
struct AuditedOrder {
    decision_id: String,
    cloid: String,
    submit_ts_ms: i64,
    side: String,
    order_label: String,
    initial_queue_qty: f64,
}

#[derive(Clone, Default)]
struct DecisionMeta {
    action: String,
    run_tag: String,
    intent_type: String,
    hypercore_account: String,
}

#[derive(Clone, Copy)]
struct HedgeExecution {
    lead_tid: u64,
    hedge_vwap_px: f64,
}

fn parse_number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .or_else(|| value[key].as_str().and_then(|item| item.parse().ok()))
        .unwrap_or(0.0)
}

fn venue_name(venue: &str) -> &str {
    match venue {
        "spot" | "@107" => "现货",
        "perp" | "HYPE" => "永续",
        value => value,
    }
}

fn side_name(side: &str) -> &str {
    match side {
        "buy" | "B" => "买入",
        "sell" | "A" => "卖出",
        value => value,
    }
}

fn liquidity_name(liquidity: &str) -> &str {
    match liquidity {
        "maker" => "Maker",
        "taker" => "Taker",
        value => value,
    }
}

fn action_name(order: Option<&AuditedOrder>, meta: Option<&DecisionMeta>) -> &'static str {
    let label = order.map_or("", |value| value.order_label.as_str());
    let action = meta.map_or("", |value| value.action.as_str());
    let run_tag = meta.map_or("", |value| value.run_tag.as_str());
    let intent = meta.map_or("", |value| value.intent_type.as_str());
    let joined = format!("{label} {action} {run_tag} {intent}").to_ascii_lowercase();
    if joined.contains("restore") || joined.contains("acceptance") {
        "恢复"
    } else if joined.contains("pmr") {
        "PMR减仓"
    } else if joined.contains("repair")
        || joined.contains("recover")
        || joined.contains("hedge_residual")
        || joined.contains("delta_hedge") && meta.is_none()
    {
        "残差修复"
    } else if joined.contains("close")
        || joined.contains("exit")
        || joined.contains("reduce")
        || joined.contains("redeem")
    {
        "减仓"
    } else if joined.contains("open") || joined.contains("entry") || joined.contains("add") {
        "加仓"
    } else if joined.contains("smoke") || joined.contains("micro") || joined.contains("ab_") {
        "执行检查"
    } else if order.is_some() {
        "手动/执行器"
    } else {
        "外部/未识别"
    }
}

pub(crate) fn fill_fee_bps(fee_usd: f64, px: f64, qty: f64) -> f64 {
    let notional = px * qty;
    if notional > 0.0 {
        fee_usd / notional * 10_000.0
    } else {
        0.0
    }
}

struct FillNotification<'a> {
    action: &'a str,
    venue: &'a str,
    side: &'a str,
    liquidity: &'a str,
    qty: f64,
    px: f64,
    fee_usd: f64,
    trigger_basis_bps: Option<f64>,
    actual_hedge_basis_bps: Option<f64>,
}

fn format_fill_message(fill: FillNotification<'_>) -> String {
    let mut message = format!(
        "HYPE Funding Carry 成交｜{action}\n{}{} {:.4} HYPE（${:.2}）｜{}\n价格 ${:.4}｜手续费 ${:.4}（{:.2}bp）",
        venue_name(fill.venue),
        side_name(fill.side),
        fill.qty,
        fill.qty * fill.px,
        liquidity_name(fill.liquidity),
        fill.px,
        fill.fee_usd,
        fill_fee_bps(fill.fee_usd, fill.px, fill.qty),
        action = fill.action,
    );
    if let Some(basis) = fill.trigger_basis_bps {
        message.push_str(&format!("\n触发 basis {basis:+.2}bp"));
    }
    if let Some(basis) = fill.actual_hedge_basis_bps {
        message.push_str(&format!("\n实际对冲 basis {basis:+.2}bp"));
    }
    message
}

static LAST_SEND: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
static NOTIFICATION_THREADS: OnceLock<Mutex<Vec<std::thread::JoinHandle<()>>>> = OnceLock::new();

pub(crate) fn notify_background(dedupe_key: String, message: String) {
    let handle = std::thread::spawn(move || {
        let Ok(mut last_send) = LAST_SEND.get_or_init(|| Mutex::new(None)).lock() else {
            eprintln!("fill_queue_lock_poisoned");
            return;
        };
        if let Some(last_send_at) = *last_send {
            if let Some(wait) = Duration::from_millis(1_100).checked_sub(last_send_at.elapsed()) {
                std::thread::sleep(wait);
            }
        }
        send_dedup(&dedupe_key, &message);
        *last_send = Some(Instant::now());
    });
    if let Ok(mut threads) = NOTIFICATION_THREADS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
    {
        threads.retain(|thread| !thread.is_finished());
        threads.push(handle);
    } else {
        eprintln!("fill_thread_registry_poisoned");
    }
}

pub fn flush_background(timeout: Duration) -> Result<()> {
    let handles = {
        let mut threads = NOTIFICATION_THREADS
            .get_or_init(|| Mutex::new(Vec::new()))
            .lock()
            .map_err(|_| anyhow::anyhow!("fill thread registry poisoned"))?;
        std::mem::take(&mut *threads)
    };
    join_handles(handles, timeout)
}

fn join_handles(handles: Vec<std::thread::JoinHandle<()>>, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    for handle in handles {
        while !handle.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if !handle.is_finished() {
            bail!("fill notification flush timed out");
        }
        if handle.join().is_err() {
            bail!("fill notification thread panicked");
        }
    }
    Ok(())
}

async fn load_orders(audit: &AuditClient, oids: &[u64]) -> Result<HashMap<u64, AuditedOrder>> {
    let mut result = HashMap::new();
    for oid in oids {
        if let Some(row) = audit.order(*oid)? {
            result.insert(
                *oid,
                AuditedOrder {
                    decision_id: row["decision_id"].as_str().unwrap_or("").to_string(),
                    cloid: row["cloid"].as_str().unwrap_or("").to_string(),
                    submit_ts_ms: parse_number(&row, "submit_ts_ms") as i64,
                    side: row["side"].as_str().unwrap_or("").to_string(),
                    order_label: row["order_label"].as_str().unwrap_or("").to_string(),
                    initial_queue_qty: parse_number(&row, "initial_queue_qty"),
                },
            );
        }
    }
    Ok(result)
}

async fn load_decisions(
    audit: &AuditClient,
    ids: &[String],
) -> Result<HashMap<String, DecisionMeta>> {
    let mut result = HashMap::new();
    for id in ids {
        if let Some(row) = audit.decision(id)? {
            result.insert(
                id.clone(),
                DecisionMeta {
                    action: row["action"].as_str().unwrap_or("").to_string(),
                    run_tag: row["run_tag"].as_str().unwrap_or("").to_string(),
                    intent_type: row["intent_type"].as_str().unwrap_or("").to_string(),
                    hypercore_account: row["hypercore_account"].as_str().unwrap_or("").to_string(),
                },
            );
        }
    }
    Ok(result)
}

async fn load_hedges(audit: &AuditClient, oids: &[u64]) -> Result<HashMap<u64, HedgeExecution>> {
    let mut result = HashMap::new();
    for oid in oids {
        if let Some(row) = audit.hedge(*oid)? {
            result.insert(
                *oid,
                HedgeExecution {
                    lead_tid: row["lead_tid"].as_u64().unwrap_or(0),
                    hedge_vwap_px: parse_number(&row, "hedge_vwap_px"),
                },
            );
        }
    }
    Ok(result)
}

struct KnownExecTids {
    tids: HashSet<u64>,
    reliable: bool,
}

async fn known_exec_tids(audit: &AuditClient, tids: &[u64]) -> KnownExecTids {
    if tids.is_empty() {
        return KnownExecTids {
            tids: HashSet::new(),
            reliable: true,
        };
    }
    match audit.known_tids(tids) {
        Ok(tids) => KnownExecTids {
            tids,
            reliable: true,
        },
        Err(error) => {
            eprintln!("fill_notification_dedupe_failed={error:#}");
            KnownExecTids {
                tids: HashSet::new(),
                reliable: false,
            }
        }
    }
}

fn should_notify_fill(
    attributed: bool,
    already_known: bool,
    dedupe_reliable: bool,
    fill_ts_ms: i64,
    notify_start_ms: i64,
) -> bool {
    if attributed && dedupe_reliable {
        !already_known
    } else {
        !already_known && fill_ts_ms >= notify_start_ms
    }
}

pub async fn reconcile_fills(info: &InfoClient, account: &str) -> Result<(usize, f64)> {
    static UNATTRIBUTED_NOTIFY_START_MS: OnceLock<i64> = OnceLock::new();
    let unattributed_start_ms = *UNATTRIBUTED_NOTIFY_START_MS.get_or_init(now_ms);
    let audit = AuditClient::from_env()?;
    let fills = info.user_fills(account).await?;
    reconcile_fill_records(&audit, account, &fills, unattributed_start_ms).await
}

async fn reconcile_fill_records(
    audit: &AuditClient,
    account: &str,
    fills: &Value,
    unattributed_start_ms: i64,
) -> Result<(usize, f64)> {
    let fills = fills
        .as_array()
        .context("userFills response is not an array")?;
    let oids = fills
        .iter()
        .filter_map(|fill| fill["oid"].as_u64())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut orders = load_orders(audit, &oids).await?;
    let decision_ids = orders
        .values()
        .map(|order| order.decision_id.clone())
        .collect::<Vec<_>>();
    let decisions = load_decisions(audit, &decision_ids).await?;
    let mut hedges = load_hedges(audit, &oids).await?;
    orders.retain(|_, order| {
        decisions
            .get(&order.decision_id)
            .is_some_and(|meta| meta.hypercore_account.eq_ignore_ascii_case(account))
    });
    hedges.retain(|oid, _| orders.contains_key(oid));
    let fill_by_tid = fills
        .iter()
        .map(|fill| (parse_number(fill, "tid") as u64, fill))
        .collect::<HashMap<_, _>>();
    let candidate_tids = fills
        .iter()
        .filter(|fill| orders.contains_key(&(parse_number(fill, "oid") as u64)))
        .map(|fill| parse_number(fill, "tid") as u64)
        .filter(|tid| *tid > 0)
        .collect::<Vec<_>>();
    let known_tids = known_exec_tids(audit, &candidate_tids).await;
    let mut count = 0;
    let mut fees = 0.0;
    for fill in fills {
        let fill_ts = parse_number(fill, "time") as i64;
        let oid = parse_number(fill, "oid") as u64;
        let tid = parse_number(fill, "tid") as u64;
        if tid == 0 {
            continue;
        }
        let order = orders.get(&oid);
        // Skip fills already persisted by an earlier reconciliation.
        let already_known = known_tids.tids.contains(&tid);
        let fee_native = parse_number(fill, "fee");
        let fee_token = fill["feeToken"].as_str().unwrap_or("").to_string();
        let px = parse_number(fill, "px");
        let qty = parse_number(fill, "sz");
        let fee_usd = if fee_token == "HYPE" {
            fee_native * px
        } else {
            fee_native
        };
        let coin = fill["coin"].as_str().unwrap_or("");
        let venue = if coin == "HYPE" {
            "perp"
        } else if coin == "@107" {
            "spot"
        } else {
            coin
        };
        let side = order.map_or_else(
            || fill["side"].as_str().unwrap_or("").to_string(),
            |value| value.side.clone(),
        );
        let liquidity = if fill["crossed"].as_bool().unwrap_or(false) {
            "taker"
        } else {
            "maker"
        };
        let meta = order.and_then(|value| decisions.get(&value.decision_id));
        let action = action_name(order, meta);
        let actual_basis = (!already_known)
            .then(|| {
                hedges.get(&oid).and_then(|hedge| {
                    let spot_px = fill_by_tid
                        .get(&hedge.lead_tid)
                        .map(|lead| parse_number(lead, "px"))?;
                    (spot_px > 0.0 && hedge.hedge_vwap_px > 0.0)
                        .then_some((hedge.hedge_vwap_px / spot_px - 1.0) * 10_000.0)
                })
            })
            .flatten();

        let trigger_basis = None;
        let mut row = None;
        if let Some(order) = order.filter(|_| !already_known) {
            // Historical tape/quote enrichment is not available in a standalone account.
            // Ready=0 explicitly marks queue estimates unavailable; actual hedge basis is
            // reconstructed above from the two real fills.
            let (eligible_tape_qty, queue_multiplier, queue_metric_ready) = (None, None, 0);
            let live_basis_at_fill_bps = None;
            row = Some(FillRow {
                decision_id: order.decision_id.clone(),
                cloid: order.cloid.clone(),
                oid,
                fill_ts_ms: fill_ts,
                venue: venue.to_string(),
                side: side.clone(),
                liquidity: liquidity.to_string(),
                px,
                qty,
                fee_usd,
                fee_native,
                fee_token: fee_token.clone(),
                tid,
                order_submit_ts_ms: order.submit_ts_ms,
                initial_queue_qty: order.initial_queue_qty,
                eligible_tape_qty,
                queue_multiplier,
                queue_metric_ready,
                live_basis_at_fill_bps,
                raw_fill: serde_json::to_string(fill)?,
                created_ts_ms: now_ms(),
            });
        }

        let should_notify = should_notify_fill(
            order.is_some(),
            already_known,
            known_tids.reliable,
            fill_ts,
            unattributed_start_ms,
        );
        if should_notify {
            notify_background(
                format!("fill:{tid}"),
                format_fill_message(FillNotification {
                    action,
                    venue,
                    side: &side,
                    liquidity,
                    qty,
                    px,
                    fee_usd,
                    trigger_basis_bps: trigger_basis,
                    actual_hedge_basis_bps: actual_basis,
                }),
            );
        }
        if let Some(row) = row {
            audit.insert("hfc_exec_fill", &row).await?;
        }
        if order.is_some() {
            count += 1;
            fees += fee_usd;
        }
    }
    Ok((count, fees))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn attributed_rebates_preserve_signed_audit_fees_and_deduplicate() {
        let directory = std::env::temp_dir().join(format!("hfc-rebates-{}", uuid::Uuid::new_v4()));
        let audit = AuditClient::open(&directory).unwrap();
        audit
            .insert(
                "hfc_exec_decision",
                &serde_json::json!({
                    "decision_id":"rebates","hypercore_account":"a","action":"open"
                }),
            )
            .await
            .unwrap();
        for oid in [1, 2] {
            audit
                .insert(
                    "hfc_exec_order",
                    &serde_json::json!({
                        "oid":oid,"decision_id":"rebates","cloid":oid.to_string(),
                        "event_ts_ms":100,"created_ts_ms":100,"order_label":"entry"
                    }),
                )
                .await
                .unwrap();
        }
        let fills = serde_json::json!([
            {"coin":"@107","px":"50","sz":"1000","side":"B","time":150,"tid":1,"oid":1,"fee":"-0.02","feeToken":"HYPE"},
            {"coin":"HYPE","px":"50","sz":"1000","side":"A","time":150,"tid":2,"oid":2,"fee":"-0.1","feeToken":"USDC"}
        ]);
        let (count, fees) = reconcile_fill_records(&audit, "a", &fills, 0)
            .await
            .unwrap();
        assert_eq!(count, 2);
        assert!((fees + 1.1).abs() < 1e-9);
        let attributed = audit.decision_fills("rebates").unwrap();
        assert_eq!(attributed.len(), 2);
        assert_eq!(parse_number(&attributed[0], "fee_native"), -0.02);
        assert_eq!(parse_number(&attributed[1], "fee_native"), -0.1);
        let attributed_fees: f64 = attributed
            .iter()
            .map(|row| parse_number(row, "fee_usd"))
            .sum();
        assert!((attributed_fees + 1.1).abs() < 1e-9);
        let journal_len = std::fs::metadata(directory.join("events.jsonl"))
            .unwrap()
            .len();
        reconcile_fill_records(&audit, "a", &fills, 0)
            .await
            .unwrap();
        assert_eq!(audit.decision_fills("rebates").unwrap().len(), 2);
        assert_eq!(
            std::fs::metadata(directory.join("events.jsonl"))
                .unwrap()
                .len(),
            journal_len
        );
        flush_background(Duration::from_secs(5)).unwrap();
        let replay_directory = directory.with_extension("replay");
        std::fs::create_dir_all(&replay_directory).unwrap();
        std::fs::copy(
            directory.join("events.jsonl"),
            replay_directory.join("events.jsonl"),
        )
        .unwrap();
        let replayed = AuditClient::open(&replay_directory).unwrap();
        assert_eq!(replayed.decision_fills("rebates").unwrap(), attributed);
    }

    #[test]
    fn notification_is_human_readable_and_keeps_actual_basis() {
        let message = format_fill_message(FillNotification {
            action: "加仓",
            venue: "perp",
            side: "sell",
            liquidity: "taker",
            qty: 0.23,
            px: 52.412,
            fee_usd: 0.0052,
            trigger_basis_bps: None,
            actual_hedge_basis_bps: Some(-5.15),
        });
        assert!(message.contains("HYPE Funding Carry 成交｜加仓"));
        assert!(message.contains("永续卖出 0.2300 HYPE"));
        assert!(message.contains("实际对冲 basis -5.15bp"));
        assert!(!message.contains("decision="));
    }

    #[test]
    fn action_classification_distinguishes_recovery_from_normal_exit() {
        let restore = AuditedOrder {
            decision_id: String::new(),
            cloid: String::new(),
            submit_ts_ms: 0,
            side: "sell".to_string(),
            order_label: "ab_restore_spot_sell".to_string(),
            initial_queue_qty: 0.0,
        };
        assert_eq!(action_name(Some(&restore), None), "恢复");
        let exit = DecisionMeta {
            run_tag: "controller_band_reduce".to_string(),
            ..DecisionMeta::default()
        };
        assert_eq!(action_name(Some(&restore), Some(&exit)), "恢复");
    }

    #[test]
    fn fee_basis_points_remain_exact() {
        assert!((fill_fee_bps(4.0, 50.0, 200.0) - 4.0).abs() < 1e-9);
    }

    #[test]
    fn dedupe_failure_allows_only_new_fills() {
        assert!(should_notify_fill(true, false, false, 101, 100));
        assert!(!should_notify_fill(true, false, false, 99, 100));
        assert!(should_notify_fill(false, false, true, 101, 100));
        assert!(!should_notify_fill(false, false, true, 99, 100));
        assert!(!should_notify_fill(true, true, true, 101, 100));
    }

    #[test]
    fn notification_threads_are_joined_before_process_exit() {
        let completed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_state = completed.clone();
        let handle = std::thread::spawn(move || {
            worker_state.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        join_handles(vec![handle], Duration::from_secs(1)).unwrap();
        assert!(completed.load(std::sync::atomic::Ordering::SeqCst));
    }
}
