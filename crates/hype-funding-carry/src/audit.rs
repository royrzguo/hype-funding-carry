use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use fs2::FileExt;
use serde::Serialize;
use serde_json::{json, Value};

use crate::state_io::state_path;

type FillKey = (i64, u64, String);
type EventKey = (i64, String);

#[derive(Clone)]
pub struct AuditClient {
    store: Arc<Mutex<Store>>,
}

struct Store {
    journal: File,
    _lock: File,
    index: Index,
}

#[derive(Default)]
struct Index {
    orders: HashMap<u64, Value>,
    decisions: HashMap<String, Value>,
    hedges: HashMap<u64, Value>,
    fills: HashMap<String, BTreeMap<String, Value>>,
    tids: HashSet<u64>,
    account_fills: HashMap<String, BTreeMap<FillKey, Value>>,
    ledgers: HashMap<String, Ledger>,
    funding: HashMap<String, Funding>,
    borrow: HashMap<String, Value>,
    accounting_baselines: HashMap<String, Value>,
}

#[derive(Default)]
struct Ledger {
    rows: BTreeMap<EventKey, Value>,
    net_external_usdc: f64,
    fees_usd: f64,
    totals: HashMap<i64, (f64, f64)>,
}

#[derive(Default)]
struct Funding {
    rows: BTreeMap<EventKey, Value>,
    totals: HashMap<i64, f64>,
}

fn number(row: &Value, field: &str) -> f64 {
    row[field]
        .as_f64()
        .or_else(|| row[field].as_str()?.parse().ok())
        .unwrap_or(0.0)
}
fn text(row: &Value, field: &str) -> String {
    row[field].as_str().unwrap_or("").to_string()
}
fn timestamp(row: &Value, field: &str) -> i64 {
    row[field]
        .as_i64()
        .unwrap_or_else(|| number(row, field) as i64)
}
fn id(row: &Value, field: &str) -> u64 {
    row[field]
        .as_u64()
        .unwrap_or_else(|| number(row, field) as u64)
}
fn account(row: &Value) -> String {
    text(row, "hypercore_account").to_ascii_lowercase()
}

impl Index {
    fn apply(&mut self, table: &str, row: Value) {
        match table {
            "hfc_exec_order" if id(&row, "oid") > 0 => {
                let oid = id(&row, "oid");
                let submit = self
                    .orders
                    .get(&oid)
                    .map(|old| timestamp(old, "submit_ts_ms").min(timestamp(&row, "event_ts_ms")))
                    .unwrap_or_else(|| timestamp(&row, "event_ts_ms"));
                let mut latest = match self.orders.get(&oid) {
                    Some(old)
                        if timestamp(old, "created_ts_ms") > timestamp(&row, "created_ts_ms") =>
                    {
                        old.clone()
                    }
                    _ => row,
                };
                latest["submit_ts_ms"] = json!(submit);
                self.orders.insert(oid, latest);
            }
            "hfc_exec_decision" => {
                self.decisions.insert(text(&row, "decision_id"), row);
            }
            "hfc_exec_hedge_latency" if id(&row, "hedge_oid") > 0 => {
                self.hedges.insert(id(&row, "hedge_oid"), row);
            }
            "hfc_exec_fill" => {
                self.tids.insert(id(&row, "tid"));
                let key = json!([row["cloid"], row["fill_ts_ms"], row["tid"]]).to_string();
                self.fills
                    .entry(text(&row, "decision_id"))
                    .or_default()
                    .insert(key, row);
            }
            "hfc_account_fill" => {
                let key = (
                    timestamp(&row, "fill_ts_ms"),
                    id(&row, "tid"),
                    text(&row, "coin"),
                );
                self.account_fills
                    .entry(account(&row))
                    .or_default()
                    .insert(key, row);
            }
            "hfc_account_ledger" => {
                let ledger = self.ledgers.entry(account(&row)).or_default();
                let key = (timestamp(&row, "event_ts_ms"), text(&row, "hash"));
                let ts = key.0;
                let old = ledger.rows.insert(key, row.clone());
                if let Some(old) = &old {
                    ledger.net_external_usdc -= number(old, "net_external_usdc");
                    ledger.fees_usd -= number(old, "fee_usd");
                }
                ledger.net_external_usdc += number(&row, "net_external_usdc");
                ledger.fees_usd += number(&row, "fee_usd");
                for (since, (external, fees)) in &mut ledger.totals {
                    if ts >= *since {
                        *external += number(&row, "net_external_usdc")
                            - old
                                .as_ref()
                                .map(|row| number(row, "net_external_usdc"))
                                .unwrap_or(0.0);
                        *fees += number(&row, "fee_usd")
                            - old
                                .as_ref()
                                .map(|row| number(row, "fee_usd"))
                                .unwrap_or(0.0);
                    }
                }
            }
            "hfc_accounting_baseline" => {
                self.accounting_baselines
                    .entry(account(&row))
                    .or_insert(row);
            }
            "hfc_account_funding" => {
                let funding = self.funding.entry(account(&row)).or_default();
                let ts = timestamp(&row, "funding_ts_ms");
                let old = funding.rows.insert((ts, text(&row, "hash")), row.clone());
                let change = number(&row, "actual_usdc")
                    - old
                        .as_ref()
                        .map(|old| number(old, "actual_usdc"))
                        .unwrap_or(0.0);
                for (since, total) in &mut funding.totals {
                    if ts >= *since {
                        *total += change;
                    }
                }
            }
            "hfc_borrow_snapshot" => {
                let key = account(&row);
                if self.borrow.get(&key).is_none_or(|old| {
                    timestamp(old, "snapshot_ts_ms") <= timestamp(&row, "snapshot_ts_ms")
                }) {
                    self.borrow.insert(key, row);
                }
            }
            _ => {}
        }
    }

    fn duplicate(&self, table: &str, row: &Value) -> bool {
        let previous = match table {
            "hfc_account_fill" => self.account_fills.get(&account(row)).and_then(|rows| {
                rows.get(&(
                    timestamp(row, "fill_ts_ms"),
                    id(row, "tid"),
                    text(row, "coin"),
                ))
            }),
            "hfc_account_ledger" => self.ledgers.get(&account(row)).and_then(|ledger| {
                ledger
                    .rows
                    .get(&(timestamp(row, "event_ts_ms"), text(row, "hash")))
            }),
            "hfc_account_funding" => self.funding.get(&account(row)).and_then(|funding| {
                funding
                    .rows
                    .get(&(timestamp(row, "funding_ts_ms"), text(row, "hash")))
            }),
            _ => None,
        };
        previous.is_some_and(|old| {
            let mut old = old.clone();
            let mut row = row.clone();
            old.as_object_mut().map(|obj| obj.remove("created_ts_ms"));
            row.as_object_mut().map(|obj| obj.remove("created_ts_ms"));
            old == row
        })
    }
}

impl Store {
    fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory).context("create audit directory")?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("writer.lock"))?;
        lock.try_lock_exclusive()
            .context("another process holds this audit directory")?;
        let mut journal = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("events.jsonl"))?;
        let mut index = Index::default();
        let mut reader = BufReader::new(&journal);
        let mut valid_len = 0;
        loop {
            let mut line = Vec::new();
            let size = reader
                .read_until(b'\n', &mut line)
                .context("read audit journal")?;
            if size == 0 {
                break;
            }
            if !line.ends_with(b"\n") {
                break;
            }
            let record: Value = serde_json::from_slice(&line).context("decode audit journal")?;
            let table = record["table"]
                .as_str()
                .context("audit record missing table")?;
            index.apply(table, record["row"].clone());
            valid_len += size as u64;
        }
        drop(reader);
        // An interrupted append cannot leave a partial record ahead of future records.
        if journal.metadata()?.len() != valid_len {
            journal.set_len(valid_len)?;
            journal.sync_all()?;
        }
        journal.seek(SeekFrom::End(0))?;
        journal.sync_all()?;
        #[cfg(unix)]
        File::open(directory)?.sync_all()?;
        Ok(Self {
            journal,
            _lock: lock,
            index,
        })
    }

    fn insert(&mut self, table: &str, row: Value) -> Result<()> {
        if self.index.duplicate(table, &row) {
            return Ok(());
        }
        let mut bytes = serde_json::to_vec(&json!({"table": table, "row": row}))?;
        bytes.push(b'\n');
        let start = self.journal.stream_position()?;
        if let Err(error) = self
            .journal
            .write_all(&bytes)
            .and_then(|_| self.journal.sync_data())
        {
            self.journal
                .set_len(start)
                .context("rollback incomplete audit append")?;
            self.journal.seek(SeekFrom::End(0))?;
            return Err(error).context("append audit record");
        }
        self.index.apply(table, row);
        Ok(())
    }
}

static STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<Store>>>>> = OnceLock::new();
struct QueuedInsert {
    client: AuditClient,
    table: String,
    row: Value,
}
struct QueueProgress {
    pending: Mutex<usize>,
    drained: Condvar,
}
static QUEUE_PROGRESS: QueueProgress = QueueProgress {
    pending: Mutex::new(0),
    drained: Condvar::new(),
};
static AUDIT_QUEUE: OnceLock<mpsc::Sender<QueuedInsert>> = OnceLock::new();

impl AuditClient {
    pub fn from_env() -> Result<Self> {
        Self::open(&state_path("audit"))
    }

    pub(crate) fn open(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        let mut stores = STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|_| anyhow::anyhow!("audit registry poisoned"))?;
        let store = match stores.get(&directory) {
            Some(store) => store.clone(),
            None => {
                let store = Arc::new(Mutex::new(Store::open(&directory)?));
                stores.insert(directory, store.clone());
                store
            }
        };
        Ok(Self { store })
    }

    fn with_index<T>(&self, read: impl FnOnce(&mut Index) -> T) -> Result<T> {
        let mut store = self
            .store
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store poisoned"))?;
        Ok(read(&mut store.index))
    }

    pub async fn insert<T: Serialize>(&self, table: &str, row: &T) -> Result<()> {
        let row = serde_json::to_value(row)?;
        self.store
            .lock()
            .map_err(|_| anyhow::anyhow!("audit store poisoned"))?
            .insert(table, row)
    }

    pub async fn insert_best_effort<T: Serialize>(&self, table: &str, row: &T) {
        if let Err(error) = self.insert(table, row).await {
            eprintln!("audit_write_failed table={table} error={error:#}");
        }
    }

    pub fn enqueue_best_effort<T: Serialize>(&self, table: &str, row: &T) -> Result<()> {
        let row = serde_json::to_value(row)?;
        let sender = AUDIT_QUEUE.get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<QueuedInsert>();
            std::thread::Builder::new()
                .name("hfc-audit-writer".to_string())
                .spawn(move || {
                    for work in receiver {
                        let result = work
                            .client
                            .store
                            .lock()
                            .map_err(|_| anyhow::anyhow!("audit store poisoned"))
                            .and_then(|mut store| store.insert(&work.table, work.row));
                        if let Err(error) = result {
                            eprintln!("audit_write_failed table={} error={error:#}", work.table);
                        }
                        let mut pending =
                            QUEUE_PROGRESS.pending.lock().expect("audit queue poisoned");
                        *pending = pending.saturating_sub(1);
                        if *pending == 0 {
                            QUEUE_PROGRESS.drained.notify_all();
                        }
                    }
                })
                .expect("spawn audit writer");
            sender
        });
        *QUEUE_PROGRESS.pending.lock().expect("audit queue poisoned") += 1;
        if sender
            .send(QueuedInsert {
                client: self.clone(),
                table: table.to_string(),
                row,
            })
            .is_err()
        {
            let mut pending = QUEUE_PROGRESS.pending.lock().expect("audit queue poisoned");
            *pending = pending.saturating_sub(1);
            bail!("audit writer queue unavailable");
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn enqueue_entry_stage(
        &self,
        trigger_id: &str,
        decision_id: &str,
        hypercore_account: &str,
        run_tag: &str,
        stage: &str,
        event_ts_ms: i64,
        trigger_ts_ms: i64,
        quoted_basis_bps: f64,
        detail: &str,
    ) -> Result<()> {
        self.enqueue_best_effort("hfc_exec_entry_latency", &json!({
            "trigger_id":trigger_id,"decision_id":decision_id,"hypercore_account":hypercore_account.to_ascii_lowercase(),
            "run_tag":run_tag,"stage":stage,"event_ts_ms":event_ts_ms,"trigger_ts_ms":trigger_ts_ms,
            "elapsed_from_trigger_ms":event_ts_ms.saturating_sub(trigger_ts_ms).max(0) as u64,
            "quoted_basis_bps":quoted_basis_bps,"detail":detail,"created_ts_ms":crate::api::now_ms(),
        }))
    }

    pub fn flush_background(timeout: Duration) -> Result<()> {
        let pending = QUEUE_PROGRESS.pending.lock().expect("audit queue poisoned");
        let (pending, _) = QUEUE_PROGRESS
            .drained
            .wait_timeout_while(pending, timeout, |pending| *pending > 0)
            .expect("audit flush poisoned");
        if *pending > 0 {
            bail!("audit flush timed out with {} pending rows", *pending);
        }
        Ok(())
    }

    pub fn order(&self, oid: u64) -> Result<Option<Value>> {
        self.with_index(|index| index.orders.get(&oid).cloned())
    }
    pub fn decision(&self, decision_id: &str) -> Result<Option<Value>> {
        self.with_index(|index| index.decisions.get(decision_id).cloned())
    }
    pub fn hedge(&self, oid: u64) -> Result<Option<Value>> {
        self.with_index(|index| index.hedges.get(&oid).cloned())
    }
    pub fn known_tids(&self, tids: &[u64]) -> Result<HashSet<u64>> {
        self.with_index(|index| {
            tids.iter()
                .copied()
                .filter(|tid| index.tids.contains(tid))
                .collect()
        })
    }
    pub fn decision_fills(&self, decision_id: &str) -> Result<Vec<Value>> {
        self.with_index(|index| {
            index
                .fills
                .get(decision_id)
                .map(|rows| rows.values().cloned().collect())
                .unwrap_or_default()
        })
    }

    pub fn latest_account_fill(&self, account: &str) -> Result<i64> {
        self.with_index(|index| {
            index
                .account_fills
                .get(&account.to_ascii_lowercase())
                .and_then(|rows| rows.last_key_value())
                .map(|(key, _)| key.0)
                .unwrap_or(0)
        })
    }
    pub fn has_account_fill(&self, account: &str, key: &FillKey) -> Result<bool> {
        self.with_index(|index| {
            index
                .account_fills
                .get(&account.to_ascii_lowercase())
                .is_some_and(|rows| rows.contains_key(key))
        })
    }
    pub fn account_fills_after(&self, account: &str, cursor: &FillKey) -> Result<Vec<Value>> {
        self.with_index(|index| {
            index
                .account_fills
                .get(&account.to_ascii_lowercase())
                .map(|rows| {
                    rows.range((
                        std::ops::Bound::Excluded(cursor),
                        std::ops::Bound::Unbounded,
                    ))
                    .map(|(_, row)| row.clone())
                    .collect()
                })
                .unwrap_or_default()
        })
    }
    pub fn ledger_summary(&self, account: &str) -> Result<Value> {
        self.with_index(|index| {
            index.ledgers.get(&account.to_ascii_lowercase()).map(|ledger| json!({
            "first_event_ts_ms":ledger.rows.first_key_value().map(|(key, _)| key.0).unwrap_or(0),
            "latest":ledger.rows.last_key_value().map(|(key, _)| key.0).unwrap_or(0),
            "net_external_usdc":ledger.net_external_usdc,"fees_usd":ledger.fees_usd,
        })).unwrap_or_else(|| json!({}))
        })
    }
    pub fn ledger_totals_since(&self, account: &str, since: i64) -> Result<(f64, f64)> {
        self.with_index(|index| {
            let ledger = index
                .ledgers
                .entry(account.to_ascii_lowercase())
                .or_default();
            *ledger.totals.entry(since).or_insert_with(|| {
                ledger.rows.range((since, String::new())..).fold(
                    (0.0, 0.0),
                    |(external, fees), (_, row)| {
                        (
                            external + number(row, "net_external_usdc"),
                            fees + number(row, "fee_usd"),
                        )
                    },
                )
            })
        })
    }
    pub fn accounting_baseline(&self, account: &str) -> Result<Option<Value>> {
        self.with_index(|index| {
            index
                .accounting_baselines
                .get(&account.to_ascii_lowercase())
                .cloned()
        })
    }
    pub fn latest_funding(&self, account: &str) -> Result<i64> {
        self.with_index(|index| {
            index
                .funding
                .get(&account.to_ascii_lowercase())
                .and_then(|funding| funding.rows.last_key_value())
                .map(|(key, _)| key.0)
                .unwrap_or(0)
        })
    }
    pub fn funding_total(&self, account: &str, since: i64) -> Result<f64> {
        self.with_index(|index| {
            let funding = index
                .funding
                .entry(account.to_ascii_lowercase())
                .or_default();
            *funding.totals.entry(since).or_insert_with(|| {
                funding
                    .rows
                    .range((since, String::new())..)
                    .map(|(_, row)| number(row, "actual_usdc"))
                    .sum()
            })
        })
    }
    pub fn previous_borrow(&self, account: &str) -> Result<Option<Value>> {
        self.with_index(|index| index.borrow.get(&account.to_ascii_lowercase()).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn directory() -> PathBuf {
        std::env::temp_dir().join(format!("hfc-audit-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn journal_replays_deduplicates_and_preserves_account_totals() {
        let path = directory();
        let client = AuditClient::open(&path).unwrap();
        let ledger = json!({"hypercore_account":"0xA","event_ts_ms":10,"hash":"deposit","net_external_usdc":1000.0,"fee_usd":0.2,"created_ts_ms":1});
        client.insert("hfc_account_ledger", &ledger).await.unwrap();
        let length = std::fs::metadata(path.join("events.jsonl")).unwrap().len();
        let mut replay = ledger.clone();
        replay["created_ts_ms"] = json!(2);
        client.insert("hfc_account_ledger", &replay).await.unwrap();
        assert_eq!(
            std::fs::metadata(path.join("events.jsonl")).unwrap().len(),
            length
        );
        for (time, value) in [(5, 0.5), (15, 1.5)] {
            client.insert("hfc_account_funding", &json!({"hypercore_account":"0xa","funding_ts_ms":time,"hash":format!("f{time}"),"actual_usdc":value})).await.unwrap();
        }
        assert_eq!(client.funding_total("0xA", 10).unwrap(), 1.5);
        client.insert("hfc_account_funding", &json!({"hypercore_account":"0xa","funding_ts_ms":20,"hash":"f20","actual_usdc":-0.3})).await.unwrap();
        assert!((client.funding_total("0xa", 10).unwrap() - 1.2).abs() < 1e-9);
        STORES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&path.canonicalize().unwrap());
        drop(client);
        let reopened = AuditClient::open(&path).unwrap();
        assert_eq!(
            number(
                &reopened.ledger_summary("0xA").unwrap(),
                "net_external_usdc"
            ),
            1000.0
        );
        assert!((reopened.funding_total("0xa", 10).unwrap() - 1.2).abs() < 1e-9);
    }

    #[tokio::test]
    async fn accounting_window_excludes_old_rows_and_preserves_baseline_on_replay() {
        let path = directory();
        let client = AuditClient::open(&path).unwrap();
        let baseline =
            json!({"hypercore_account":"a", "snapshot_ts_ms":100,"account_value_usd":1099.96});
        client
            .insert("hfc_accounting_baseline", &baseline)
            .await
            .unwrap();
        for (ts, capital, fee) in [
            (10, 1000.0, 0.0),
            (90, 100.0, 0.04),
            (110, 250.0, 0.0),
            (120, -100.0, 0.01),
        ] {
            client.insert("hfc_account_ledger", &json!({"hypercore_account":"a", "event_ts_ms":ts,"hash":ts.to_string(),"net_external_usdc":capital,"fee_usd":fee})).await.unwrap();
        }
        assert_eq!(client.ledger_totals_since("a", 101).unwrap(), (150.0, 0.01));
        client.insert("hfc_account_ledger", &json!({"hypercore_account":"a", "event_ts_ms":80,"hash":"late-historical","net_external_usdc":500.0,"fee_usd":1.0})).await.unwrap();
        assert_eq!(client.ledger_totals_since("a", 101).unwrap(), (150.0, 0.01));
        client.insert("hfc_account_ledger", &json!({"hypercore_account":"a", "event_ts_ms":130,"hash":"new","net_external_usdc":20.0,"fee_usd":0.0})).await.unwrap();
        assert_eq!(client.ledger_totals_since("a", 101).unwrap(), (170.0, 0.01));
        STORES
            .get()
            .unwrap()
            .lock()
            .unwrap()
            .remove(&path.canonicalize().unwrap());
        drop(client);
        let reopened = AuditClient::open(&path).unwrap();
        assert_eq!(
            reopened.accounting_baseline("a").unwrap().unwrap(),
            baseline
        );
        assert_eq!(
            reopened.ledger_totals_since("a", 101).unwrap(),
            (170.0, 0.01)
        );
    }

    #[test]
    fn journal_recovers_partial_tail_and_excludes_a_second_writer() {
        let path = directory();
        let mut store = Store::open(&path).unwrap();
        store
            .insert("hfc_exec_decision", json!({"decision_id":"d1"}))
            .unwrap();
        assert!(Store::open(&path).is_err());
        store.journal.write_all(b"{\"table\":\"\xe4\xb8").unwrap();
        drop(store);
        let mut recovered = Store::open(&path).unwrap();
        assert!(recovered.index.decisions.contains_key("d1"));
        recovered
            .insert("hfc_exec_decision", json!({"decision_id":"d2"}))
            .unwrap();
        drop(recovered);
        let replayed = Store::open(&path).unwrap();
        assert_eq!(replayed.index.decisions.len(), 2);
    }

    #[tokio::test]
    async fn steady_state_appends_and_reads_only_new_account_fills() {
        let mut append_sizes = Vec::new();
        for history in [1_000_u64, 10_000] {
            let path = directory();
            std::fs::create_dir_all(&path).unwrap();
            let fill = |tid| {
                json!({
                    "hypercore_account":"a","fill_ts_ms":tid,"tid":tid,
                    "coin":"HYPE","raw_fill":"{}","created_ts_ms":1
                })
            };
            let mut bytes = Vec::new();
            for tid in 1..=history {
                serde_json::to_writer(
                    &mut bytes,
                    &json!({"table":"hfc_account_fill","row":fill(tid)}),
                )
                .unwrap();
                bytes.push(b'\n');
            }
            std::fs::write(path.join("events.jsonl"), &bytes).unwrap();
            let client = AuditClient::open(&path).unwrap();
            let previous_size = std::fs::metadata(path.join("events.jsonl")).unwrap().len();
            for tid in 100_001..=100_003 {
                client.insert("hfc_account_fill", &fill(tid)).await.unwrap();
            }
            let next_size = std::fs::metadata(path.join("events.jsonl")).unwrap().len();
            append_sizes.push(next_size - previous_size);
            assert_eq!(
                client
                    .account_fills_after("a", &(history as i64, history, "HYPE".to_string()))
                    .unwrap()
                    .len(),
                3
            );
            for tid in 100_001..=100_003 {
                client.insert("hfc_account_fill", &fill(tid)).await.unwrap();
            }
            assert_eq!(
                std::fs::metadata(path.join("events.jsonl")).unwrap().len(),
                next_size
            );
        }
        assert_eq!(append_sizes[0], append_sizes[1]);
    }

    #[tokio::test]
    async fn fill_lookup_returns_only_changes_and_keeps_u64_identity() {
        let client = AuditClient::open(&directory()).unwrap();
        let large_tid = 9_007_199_254_740_993_u64;
        for (time, tid) in [(1, 1), (2, large_tid)] {
            client.insert("hfc_account_fill", &json!({"hypercore_account":"a","fill_ts_ms":time,"tid":tid,"coin":"HYPE","raw_fill":"{}"})).await.unwrap();
        }
        let rows = client
            .account_fills_after("a", &(1, 1, "HYPE".to_string()))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(id(&rows[0], "tid"), large_tid);
        client
            .enqueue_best_effort(
                "hfc_exec_fill",
                &json!({"decision_id":"d","cloid":"c","fill_ts_ms":2,"tid":large_tid}),
            )
            .unwrap();
        AuditClient::flush_background(Duration::from_secs(5)).unwrap();
        assert!(client
            .known_tids(&[large_tid])
            .unwrap()
            .contains(&large_tid));
        assert_eq!(client.decision_fills("d").unwrap().len(), 1);
    }
}
