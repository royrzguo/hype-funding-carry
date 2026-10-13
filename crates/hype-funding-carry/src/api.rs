use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::future::join_all;
use reqwest::Client;
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::asset_domain::{
    evaluate_asset_domain, parse_perp_dexes, AssetDomainInput, AssetDomainScan, DexDomainInput,
};
use crate::model::Book;

const INFO_URL: &str = "https://api.hyperliquid.xyz/info";
const DOMAIN_SCAN_CACHE_MS: i64 = 300_000;
/// Incomplete scans cannot establish a clean asset domain. Retry them sooner than complete
/// scans while retaining a short cache to limit repeated requests during rate limiting.
const INCOMPLETE_DOMAIN_SCAN_CACHE_MS: i64 = 30_000;
const AGENT_CACHE_MS: i64 = 300_000;
const FUNDING_CACHE_MS: i64 = 300_000;
const BORROW_RESERVE_CACHE_MS: i64 = 300_000;
const FEE_CACHE_MS: i64 = 6 * 3_600_000;
/// Resting-entry risk monitor only: it checks PMR, agent, orders, delta and carry regime and
/// never reads account value, so its `portfolio` (weight 20, every 15 s) can be a minute old.
/// The controller cycle keeps a fresh read: account value feeds sizing and accounting.
const RISK_MONITOR_PORTFOLIO_CACHE_MS: i64 = 60_000;
const INFO_MAX_IN_FLIGHT: usize = 4;
const INFO_MAX_ATTEMPTS: usize = 4;

#[derive(Clone)]
pub struct InfoClient {
    http: Client,
    url: String,
    permits: Arc<Semaphore>,
    domain_scan_cache: Arc<Mutex<Option<(String, AssetDomainScan)>>>,
    slow_cache: Arc<Mutex<HashMap<String, TimedValue>>>,
}

#[derive(Clone)]
struct TimedValue {
    observed_at_ms: i64,
    value: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccountSnapshot {
    pub snapshot_ts_ms: i64,
    pub account_value_usd: f64,
    pub account_value_ts_ms: i64,
    pub portfolio_margin_enabled: bool,
    pub portfolio_margin_ratio: f64,
    pub available_after_maintenance_usdc: f64,
    pub usdc_total: f64,
    pub usdc_borrowed: f64,
    pub hype_spot_qty: f64,
    pub hype_perp_qty: f64,
    pub liquidation_px: f64,
    pub funding_24h_bps: f64,
    pub funding_24h_all_negative: bool,
    pub predicted_next_funding_rate: f64,
    pub next_funding_time_ms: i64,
    pub open_orders: u32,
    pub unknown_exposure: bool,
    pub asset_domain: AssetDomainScan,
    pub agent_authorized: bool,
    pub agent_valid_until_ms: i64,
    pub perp_taker_fee_bps: f64,
    pub perp_maker_fee_bps: f64,
    pub spot_taker_fee_bps: f64,
    pub spot_maker_fee_bps: f64,
    pub usdc_borrow_apr: f64,
    pub raw_account_state: String,
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub account: AccountSnapshot,
    pub spot: Book,
    pub perp: Book,
}

#[derive(Clone, Debug)]
pub struct ExecutionState {
    pub hype_spot_qty: f64,
    pub hype_perp_qty: f64,
    pub open_order_oids: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct BorrowAccountingState {
    pub snapshot_ts_ms: i64,
    pub spot_borrowed_usdc: f64,
    pub borrow_value_usdc: f64,
    pub borrow_basis_usdc: f64,
    pub borrow_apr: f64,
    pub health: String,
    pub health_factor: f64,
    pub raw_accounting_state: String,
}

#[derive(Clone, Debug)]
pub struct OpenOrder {
    pub dex: String,
    pub coin: String,
    pub oid: u64,
}

impl InfoClient {
    pub fn new() -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("build HTTP client")?;
        Ok(Self {
            http,
            url: INFO_URL.to_string(),
            permits: Arc::new(Semaphore::new(INFO_MAX_IN_FLIGHT)),
            domain_scan_cache: Arc::new(Mutex::new(None)),
            slow_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[cfg(test)]
    fn for_test(url: String) -> Result<Self> {
        let mut client = Self::new()?;
        client.url = url;
        Ok(client)
    }

    async fn post(&self, payload: Value) -> Result<Value> {
        for attempt in 0..INFO_MAX_ATTEMPTS {
            let permit = self
                .permits
                .acquire()
                .await
                .context("Hyperliquid info request limiter closed")?;
            let trace = (std::env::var("HFC_TRACE_IO").as_deref() == Ok("1")).then(|| {
                static REQUEST_ID: std::sync::atomic::AtomicU64 =
                    std::sync::atomic::AtomicU64::new(1);
                (
                    REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    now_ms(),
                    Instant::now(),
                )
            });
            let request_type = payload["type"].as_str().unwrap_or("unknown");
            if let Some((id, start_ts_ms, _)) = trace {
                eprintln!(
                    "{}",
                    json!({"event":"hfc_io_send","transport":"http_info","id":id,"request_type":request_type,"attempt":attempt + 1,"start_ts_ms":start_ts_ms})
                );
            }
            let mut status_code = None;
            let response: Result<_> = async {
                let response = self
                    .http
                    .post(&self.url)
                    .json(&payload)
                    .send()
                    .await
                    .context("Hyperliquid info request")?;
                let status = response.status();
                status_code = Some(status.as_u16());
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(Duration::from_secs);
                let body = response
                    .text()
                    .await
                    .context("read Hyperliquid info response")?;
                Ok((status, retry_after, body))
            }
            .await;
            if let Some((id, start_ts_ms, started)) = trace {
                eprintln!(
                    "{}",
                    json!({"event":"hfc_io_finish","transport":"http_info","id":id,"request_type":request_type,"attempt":attempt + 1,"start_ts_ms":start_ts_ms,"finish_ts_ms":now_ms(),"elapsed_ms":started.elapsed().as_millis(),"http_status":status_code,"outcome":if response.is_err() {"transport_error"} else if status_code.is_some_and(|code| !(200..300).contains(&code)) {"http_error"} else {"response"}})
                );
            }
            let (status, retry_after, body) = response?;
            if status.is_success() {
                return serde_json::from_str(&body).context("decode Hyperliquid info JSON");
            }
            if retryable_info_status(status.as_u16()) && attempt + 1 < INFO_MAX_ATTEMPTS {
                // Log transient failures that a successful retry would otherwise hide.
                eprintln!(
                    "hl_info_retry status={} attempt={} request_type={}",
                    status.as_u16(),
                    attempt,
                    payload["type"].as_str().unwrap_or("unknown"),
                );
                drop(permit);
                tokio::time::sleep(retry_after.unwrap_or_else(|| info_retry_delay(attempt))).await;
                continue;
            }
            bail!("Hyperliquid info HTTP {status}: {body}");
        }
        unreachable!("bounded Hyperliquid info retry loop always returns")
    }

    pub async fn open_orders(&self, account: &str) -> Result<Value> {
        self.post(json!({"type":"openOrders","user":account})).await
    }

    pub async fn all_open_orders(&self, account: &str) -> Result<Vec<OpenOrder>> {
        let dex_value = self.post(json!({"type":"perpDexs"})).await?;
        let dexes = parse_perp_dexes(&dex_value).map_err(anyhow::Error::msg)?;
        let responses = join_all(
            dexes
                .iter()
                .map(|dex| self.post(json!({"type":"openOrders","user":account,"dex":dex}))),
        )
        .await;
        let mut output = Vec::new();
        for (dex, response) in dexes.into_iter().zip(responses) {
            let value = response.with_context(|| {
                format!(
                    "openOrders failed for dex {}",
                    if dex.is_empty() { "main" } else { &dex }
                )
            })?;
            let rows = value.as_array().with_context(|| {
                format!(
                    "openOrders response is not an array for dex {}",
                    if dex.is_empty() { "main" } else { &dex }
                )
            })?;
            for row in rows {
                output.push(OpenOrder {
                    dex: dex.clone(),
                    coin: row["coin"]
                        .as_str()
                        .context("open order missing coin")?
                        .to_string(),
                    oid: row["oid"]
                        .as_u64()
                        .or_else(|| row["oid"].as_str().and_then(|value| value.parse().ok()))
                        .context("open order missing oid")?,
                });
            }
        }
        Ok(output)
    }

    pub async fn extra_agents(&self, account: &str) -> Result<Value> {
        self.post(json!({"type":"extraAgents","user":account}))
            .await
    }

    pub async fn user_fills(&self, account: &str) -> Result<Value> {
        self.post(json!({
            "type":"userFills",
            "user":account,
            "aggregateByTime":false
        }))
        .await
    }

    pub async fn user_fills_by_time(&self, account: &str, start_ms: i64) -> Result<Value> {
        self.post(json!({
            "type":"userFillsByTime",
            "user":account,
            "startTime":start_ms,
            "aggregateByTime":false
        }))
        .await
    }

    pub async fn user_funding(&self, account: &str, start_ms: i64) -> Result<Value> {
        self.post(json!({
            "type":"userFunding",
            "user":account,
            "startTime":start_ms
        }))
        .await
    }

    pub async fn user_non_funding_ledger_updates(
        &self,
        account: &str,
        start_ms: i64,
    ) -> Result<Value> {
        self.post(json!({
            "type":"userNonFundingLedgerUpdates",
            "user":account,
            "startTime":start_ms
        }))
        .await
    }

    pub async fn order_status(&self, account: &str, cloid: &str) -> Result<Value> {
        self.post(json!({"type":"orderStatus","user":account,"oid":cloid}))
            .await
    }

    pub async fn book(&self, coin: &str) -> Result<Book> {
        let coin = if coin == "HYPE/USDC" { "@107" } else { coin };
        let value = self.post(json!({"type":"l2Book","coin":coin})).await?;
        parse_book(&value, now_ms())
    }

    pub async fn execution_state(&self, account: &str) -> Result<ExecutionState> {
        let (spot_state, perp_state, orders) = tokio::try_join!(
            self.post(json!({"type":"spotClearinghouseState","user":account})),
            self.post(json!({"type":"clearinghouseState","user":account})),
            self.post(json!({"type":"openOrders","user":account})),
        )?;
        let hype_spot_qty = optional_balance_total(&spot_state, "HYPE")?;
        let (hype_perp_qty, _) = hype_position(&perp_state)?;
        Ok(ExecutionState {
            hype_spot_qty,
            hype_perp_qty,
            open_order_oids: open_order_oids(&orders)?,
        })
    }

    pub async fn borrow_accounting_state(&self, account: &str) -> Result<BorrowAccountingState> {
        let (spot_state, borrow_lend_state, borrow_reserve) = tokio::try_join!(
            self.post(json!({"type":"spotClearinghouseState","user":account})),
            self.post(json!({"type":"borrowLendUserState","user":account})),
            self.post(json!({"type":"borrowLendReserveState","token":0})),
        )?;
        let user_state = borrow_lend_state
            .get("userState")
            .unwrap_or(&borrow_lend_state);
        let token_state = borrow_lend_usdc_state(user_state)?;
        let borrow = &token_state["borrow"];
        let spot_borrowed_usdc = optional_number_at(&balance(&spot_state, "USDC"), "borrowed")?;
        let borrow_value_usdc = optional_number_at(borrow, "value")?;
        let borrow_basis_usdc = optional_number_at(borrow, "basis")?;
        let health_factor = optional_number_at(user_state, "healthFactor")?;
        let health = user_state["health"].as_str().unwrap_or("").to_string();
        let snapshot_ts_ms = now_ms();
        Ok(BorrowAccountingState {
            snapshot_ts_ms,
            spot_borrowed_usdc,
            borrow_value_usdc,
            borrow_basis_usdc,
            borrow_apr: required_number_at(&borrow_reserve, "borrowYearlyRate")?,
            health,
            health_factor,
            raw_accounting_state: serde_json::to_string(&json!({
                "spot": spot_state,
                "borrow_lend": borrow_lend_state,
                "reserve": borrow_reserve,
            }))?,
        })
    }

    pub async fn snapshot(&self, account: &str, agent: &str) -> Result<Snapshot> {
        let asset_domain = self.cached_asset_domain_scan(account).await;
        self.snapshot_with_domain(account, agent, Some(asset_domain), false, false)
            .await
    }

    /// Snapshot for the resting-entry risk monitor; see RISK_MONITOR_PORTFOLIO_CACHE_MS.
    pub async fn snapshot_for_resting_risk(
        &self,
        account: &str,
        agent: &str,
    ) -> Result<(Snapshot, ExecutionState)> {
        let asset_domain = self.cached_asset_domain_scan(account).await;
        let (snapshot, orders) = tokio::try_join!(
            self.snapshot_with_domain(account, agent, Some(asset_domain), false, true),
            self.post(json!({"type":"openOrders","user":account})),
        )?;
        let execution = ExecutionState {
            hype_spot_qty: snapshot.account.hype_spot_qty,
            hype_perp_qty: snapshot.account.hype_perp_qty,
            open_order_oids: open_order_oids(&orders)?,
        };
        Ok((snapshot, execution))
    }

    pub async fn snapshot_with_fresh_domain(&self, account: &str, agent: &str) -> Result<Snapshot> {
        self.snapshot_with_domain(account, agent, None, true, false)
            .await
    }

    async fn cached_post(
        &self,
        key: String,
        ttl_ms: i64,
        payload: Value,
        bypass_cache: bool,
    ) -> Result<Value> {
        if !bypass_cache {
            let cached = self
                .slow_cache
                .lock()
                .expect("slow info cache mutex poisoned")
                .get(&key)
                .cloned();
            if let Some(cached) = cached {
                if slow_cache_fresh(&key, cached.observed_at_ms, now_ms(), ttl_ms) {
                    return Ok(cached.value);
                }
            }
        }
        let value = self.post(payload).await?;
        self.slow_cache
            .lock()
            .expect("slow info cache mutex poisoned")
            .insert(
                key,
                TimedValue {
                    observed_at_ms: now_ms(),
                    value: value.clone(),
                },
            );
        Ok(value)
    }

    pub async fn asset_domain_scan(&self, account: &str) -> AssetDomainScan {
        let scan_started_at_ms = now_ms();
        let perp_dexes = self
            .post(json!({"type":"perpDexs"}))
            .await
            .map_err(|error| format!("{error:#}"));
        let dex_names = perp_dexes
            .as_ref()
            .ok()
            .and_then(|value| parse_perp_dexes(value).ok())
            .unwrap_or_default();
        let dex_requests = dex_names.iter().map(|dex| async move {
            let clearinghouse_state = self
                .post(json!({
                    "type":"clearinghouseState",
                    "user":account,
                    "dex":dex,
                }))
                .await
                .map_err(|error| format!("{error:#}"));
            let open_orders = self
                .post(json!({"type":"openOrders","user":account,"dex":dex}))
                .await
                .map_err(|error| format!("{error:#}"));
            DexDomainInput {
                name: dex.clone(),
                clearinghouse_state,
                open_orders,
            }
        });
        let (dex_states, spot_state, borrow_lend_state, delegator_summary, delegations) = tokio::join!(
            join_all(dex_requests),
            capture(self.post(json!({
                "type":"spotClearinghouseState",
                "user":account,
            }))),
            capture(self.post(json!({
                "type":"borrowLendUserState",
                "user":account,
            }))),
            capture(self.post(json!({"type":"delegatorSummary","user":account}))),
            capture(self.post(json!({"type":"delegations","user":account}))),
        );
        let scan = evaluate_asset_domain(AssetDomainInput {
            perp_dexes,
            dex_states,
            spot_state,
            borrow_lend_state,
            delegator_summary,
            delegations,
            scan_started_at_ms,
            observed_at_ms: now_ms(),
        });
        *self
            .domain_scan_cache
            .lock()
            .expect("asset domain cache mutex poisoned") =
            Some((account.to_ascii_lowercase(), scan.clone()));
        scan
    }

    async fn cached_asset_domain_scan(&self, account: &str) -> AssetDomainScan {
        let cached = self
            .domain_scan_cache
            .lock()
            .expect("asset domain cache mutex poisoned")
            .clone();
        if let Some((cached_account, scan)) = cached {
            if cached_account.eq_ignore_ascii_case(account)
                && domain_scan_cache_fresh(&scan, now_ms())
            {
                return scan;
            }
        }
        self.asset_domain_scan(account).await
    }

    async fn snapshot_with_domain(
        &self,
        account: &str,
        agent: &str,
        asset_domain: Option<AssetDomainScan>,
        fresh_slow_inputs: bool,
        cached_portfolio: bool,
    ) -> Result<Snapshot> {
        let now = now_ms();
        let start = now - 30 * 60 * 60 * 1000;
        let (
            spot_state,
            perp_state,
            portfolio,
            funding,
            agents,
            fees,
            borrow_reserve,
            predicted_fundings,
        ) = tokio::try_join!(
            self.post(json!({"type":"spotClearinghouseState","user":account})),
            self.post(json!({"type":"clearinghouseState","user":account})),
            async {
                let payload = json!({"type":"portfolio","user":account});
                if cached_portfolio {
                    self.cached_post(
                        format!("portfolio:{}", account.to_ascii_lowercase()),
                        RISK_MONITOR_PORTFOLIO_CACHE_MS,
                        payload,
                        false,
                    )
                    .await
                } else {
                    self.post(payload).await
                }
            },
            self.cached_post(
                "funding:HYPE".to_string(),
                FUNDING_CACHE_MS,
                json!({"type":"fundingHistory","coin":"HYPE","startTime":start}),
                fresh_slow_inputs,
            ),
            self.cached_post(
                format!("agents:{}", account.to_ascii_lowercase()),
                AGENT_CACHE_MS,
                json!({"type":"extraAgents","user":account}),
                fresh_slow_inputs,
            ),
            self.cached_post(
                format!("fees:{}", account.to_ascii_lowercase()),
                FEE_CACHE_MS,
                json!({"type":"userFees","user":account}),
                fresh_slow_inputs,
            ),
            self.cached_post(
                "borrow-reserve:0".to_string(),
                BORROW_RESERVE_CACHE_MS,
                json!({"type":"borrowLendReserveState","token":0}),
                fresh_slow_inputs,
            ),
            self.cached_post(
                "predicted-fundings".to_string(),
                FUNDING_CACHE_MS,
                json!({"type":"predictedFundings"}),
                fresh_slow_inputs,
            ),
        )?;
        let asset_domain = match asset_domain {
            Some(scan) => scan,
            None => self.asset_domain_scan(account).await,
        };
        // Fetch books after account reads so queueing does not consume their freshness
        // budget before planning. All requests share the same concurrency limiter.
        let (spot_l2, perp_l2) = tokio::try_join!(
            self.post(json!({"type":"l2Book","coin":"@107"})),
            self.post(json!({"type":"l2Book","coin":"HYPE"})),
        )?;
        let recv_ts_ms = now_ms();
        let spot = parse_book(&spot_l2, recv_ts_ms)?;
        let perp = parse_book(&perp_l2, recv_ts_ms)?;
        let usdc = balance(&spot_state, "USDC");
        let (hype_perp_qty, liquidation_px) = hype_position(&perp_state)?;
        let unknown_exposure = asset_domain.has_unknown_exposure();
        let (funding_24h_bps, funding_24h_all_negative) = funding_24h(&funding)?;
        let (predicted_next_funding_rate, next_funding_time_ms) =
            predicted_hype_funding(&predicted_fundings, now)?;
        let (agent_authorized, agent_valid_until_ms) = agent_status(&agents, agent)?;
        let portfolio_margin_enabled = spot_state["portfolioMarginEnabled"]
            .as_bool()
            .context("spot state missing portfolioMarginEnabled")?;
        let portfolio_margin_ratio = required_number_at(&spot_state, "portfolioMarginRatio")?;
        let raw_account_state = serde_json::to_string(&json!({
            "spot": spot_state,
            "perp": perp_state,
            "asset_domain": &asset_domain,
        }))?;
        let (account_value_ts_ms, account_value_usd) = portfolio_observation(&portfolio)?;
        Ok(Snapshot {
            account: AccountSnapshot {
                snapshot_ts_ms: now,
                account_value_usd,
                account_value_ts_ms,
                portfolio_margin_enabled,
                portfolio_margin_ratio,
                available_after_maintenance_usdc: available_usdc(&spot_state)?,
                usdc_total: required_number_at(&usdc, "total")?,
                // HyperCore omits `borrowed` while the balance has no loan.
                usdc_borrowed: optional_number_at(&usdc, "borrowed")?,
                hype_spot_qty: optional_balance_total(&spot_state, "HYPE")?,
                hype_perp_qty,
                liquidation_px,
                funding_24h_bps,
                funding_24h_all_negative,
                predicted_next_funding_rate,
                next_funding_time_ms,
                open_orders: asset_domain.open_orders_total,
                unknown_exposure,
                asset_domain,
                agent_authorized,
                agent_valid_until_ms,
                perp_taker_fee_bps: required_number_at(&fees, "userCrossRate")? * 10_000.0,
                perp_maker_fee_bps: required_number_at(&fees, "userAddRate")? * 10_000.0,
                spot_taker_fee_bps: required_number_at(&fees, "userSpotCrossRate")? * 10_000.0,
                spot_maker_fee_bps: required_number_at(&fees, "userSpotAddRate")? * 10_000.0,
                usdc_borrow_apr: required_number_at(&borrow_reserve, "borrowYearlyRate")?,
                raw_account_state,
            },
            spot,
            perp,
        })
    }
}

fn open_order_oids(orders: &Value) -> Result<Vec<u64>> {
    orders
        .as_array()
        .context("openOrders response is not an array")?
        .iter()
        .map(|order| required_number_at(order, "oid").map(|value| value as u64))
        .collect()
}

fn borrow_lend_usdc_state(value: &Value) -> Result<&Value> {
    if let Some(rows) = value["tokenToState"].as_array() {
        return rows
            .iter()
            .find_map(|row| {
                let pair = row.as_array()?;
                if pair.first().map(parse_number) == Some(0.0) {
                    pair.get(1)
                } else {
                    None
                }
            })
            .context("borrowLendUserState missing USDC tokenToState row");
    }
    value["tokenStates"]
        .as_array()
        .and_then(|rows| {
            rows.iter()
                .find(|row| required_number_at(row, "token").ok() == Some(0.0))
        })
        .context("borrowLendUserState missing USDC token state")
}

fn retryable_info_status(status: u16) -> bool {
    status == 429 || (500..=599).contains(&status)
}

fn info_retry_delay(attempt: usize) -> Duration {
    Duration::from_millis(250_u64.saturating_mul(1_u64 << attempt.min(3)))
}

async fn capture(
    future: impl std::future::Future<Output = Result<Value>>,
) -> Result<Value, String> {
    future.await.map_err(|error| format!("{error:#}"))
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis() as i64
}

fn parse_number(value: &Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|x| x.parse().ok()))
        .unwrap_or(0.0)
}

fn number_at(value: &Value, key: &str) -> f64 {
    parse_number(&value[key])
}

fn required_number_at(value: &Value, key: &str) -> Result<f64> {
    value[key]
        .as_f64()
        .or_else(|| value[key].as_str().and_then(|x| x.parse().ok()))
        .filter(|number: &f64| number.is_finite())
        .with_context(|| format!("missing or invalid numeric field {key}"))
}

fn optional_number_at(value: &Value, key: &str) -> Result<f64> {
    if value.get(key).is_none_or(Value::is_null) {
        Ok(0.0)
    } else {
        required_number_at(value, key)
    }
}

fn balance(spot: &Value, coin: &str) -> Value {
    spot["balances"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["coin"] == coin))
        .cloned()
        .unwrap_or(Value::Null)
}

fn optional_balance_total(spot: &Value, coin: &str) -> Result<f64> {
    let balances = spot["balances"]
        .as_array()
        .context("spot state missing balances")?;
    match balances
        .iter()
        .find(|row| row["coin"].as_str() == Some(coin))
    {
        Some(row) => required_number_at(row, "total"),
        None => Ok(0.0),
    }
}

fn available_usdc(spot: &Value) -> Result<f64> {
    let rows = spot["tokenToAvailableAfterMaintenance"]
        .as_array()
        .context("spot state missing tokenToAvailableAfterMaintenance")?;
    for row in rows {
        let pair = row
            .as_array()
            .context("available-after-maintenance row is not a pair")?;
        let token = pair.first().context("available row missing token")?;
        let token = token
            .as_u64()
            .or_else(|| token.as_str().and_then(|value| value.parse().ok()))
            .context("available row has invalid token")?;
        if token == 0 {
            let value = pair.get(1).context("USDC available row missing value")?;
            return value
                .as_f64()
                .or_else(|| value.as_str().and_then(|number| number.parse().ok()))
                .filter(|number: &f64| number.is_finite())
                .context("USDC available value is invalid");
        }
    }
    bail!("spot state missing USDC available after maintenance")
}

fn hype_position(perp: &Value) -> Result<(f64, f64)> {
    let positions = perp["assetPositions"]
        .as_array()
        .context("clearinghouse state missing assetPositions")?;
    let position = positions
        .iter()
        .map(|row| &row["position"])
        .find(|position| position["coin"] == "HYPE");
    match position {
        Some(value) => Ok((
            required_number_at(value, "szi")?,
            number_at(value, "liquidationPx"),
        )),
        None => Ok((0.0, 0.0)),
    }
}

fn portfolio_observation(portfolio: &Value) -> Result<(i64, f64)> {
    let row = portfolio
        .as_array()
        .and_then(|periods| {
            periods.iter().find(|row| {
                row.as_array()
                    .and_then(|pair| pair.first())
                    .and_then(Value::as_str)
                    == Some("allTime")
            })
        })
        .and_then(Value::as_array)
        .and_then(|pair| pair.get(1))
        .and_then(|data| data["accountValueHistory"].as_array())
        .and_then(|history| history.last())
        .and_then(Value::as_array)
        .context("portfolio missing allTime account observation")?;
    let timestamp = row
        .first()
        .and_then(Value::as_i64)
        .filter(|ts| *ts > 0)
        .context("portfolio missing valid account value timestamp")?;
    let value = row
        .get(1)
        .map(parse_number)
        .filter(|value| value.is_finite() && *value > 0.0)
        .context("portfolio missing positive allTime account value")?;
    Ok((timestamp, value))
}

fn funding_rows(value: &Value) -> Result<Vec<(i64, f64)>> {
    value
        .as_array()
        .context("fundingHistory response is not an array")?
        .iter()
        .map(|row| {
            Ok((
                required_number_at(row, "time")? as i64,
                required_number_at(row, "fundingRate")?,
            ))
        })
        .collect()
}

fn funding_24h(value: &Value) -> Result<(f64, bool)> {
    let mut rows = funding_rows(value)?;
    rows.sort_by_key(|row| row.0);
    let last: Vec<_> = rows.into_iter().rev().take(24).collect();
    let sum_bps = last.iter().map(|row| row.1).sum::<f64>() * 10_000.0;
    let all_negative = last.len() == 24
        && last.iter().all(|row| row.1 < 0.0)
        && last
            .windows(2)
            .all(|pair| (pair[0].0 - pair[1].0).abs() <= 5_400_000);
    Ok((sum_bps, all_negative))
}

fn predicted_hype_funding(value: &Value, observed_ms: i64) -> Result<(f64, i64)> {
    let venues = value
        .as_array()
        .context("predictedFundings response is not an array")?
        .iter()
        .find_map(|row| {
            let pair = row.as_array()?;
            (pair.first()?.as_str()? == "HYPE")
                .then(|| pair.get(1))
                .flatten()
        })
        .and_then(Value::as_array)
        .context("predictedFundings missing HYPE")?;
    let prediction = venues
        .iter()
        .find_map(|row| {
            let pair = row.as_array()?;
            (pair.first()?.as_str()? == "HlPerp")
                .then(|| pair.get(1))
                .flatten()
        })
        .context("predictedFundings missing HYPE HlPerp")?;
    let rate = required_number_at(prediction, "fundingRate")?;
    let next_ms = required_number_at(prediction, "nextFundingTime")? as i64;
    let interval_hours = required_number_at(prediction, "fundingIntervalHours")?;
    if (interval_hours - 1.0).abs() > 1e-9 {
        bail!("predicted HYPE funding interval is not one hour");
    }
    let publication_grace_ms = 5 * 60_000;
    let interval_ms = (interval_hours * 3_600_000.0) as i64;
    if next_ms < observed_ms - interval_ms - publication_grace_ms
        || next_ms > observed_ms + 2 * interval_ms
    {
        bail!(
            "predicted HYPE funding timestamp is stale or implausible: next_funding_ms={next_ms} observed_ms={observed_ms}"
        );
    }
    Ok((rate, next_ms))
}

fn agent_status(value: &Value, expected: &str) -> Result<(bool, i64)> {
    let agents = value
        .as_array()
        .context("extraAgents response is not an array")?;
    match agents.iter().find(|agent| {
        agent["address"]
            .as_str()
            .map(|address| address.eq_ignore_ascii_case(expected))
            .unwrap_or(false)
    }) {
        Some(agent) => Ok((true, required_number_at(agent, "validUntil")? as i64)),
        None => Ok((false, 0)),
    }
}

pub(crate) fn parse_book(value: &Value, recv_ts_ms: i64) -> Result<Book> {
    let levels = value["levels"]
        .as_array()
        .ok_or_else(|| anyhow!("l2Book missing levels"))?;
    let side = |index: usize, field: &str| -> Result<Vec<f64>> {
        levels
            .get(index)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|row| required_number_at(row, field))
            .collect()
    };
    let book = Book {
        exchange_ts_ms: value["time"]
            .as_i64()
            .ok_or_else(|| anyhow!("l2Book missing exchange time"))?,
        recv_ts_ms,
        bid_px: side(0, "px")?,
        bid_sz: side(0, "sz")?,
        ask_px: side(1, "px")?,
        ask_sz: side(1, "sz")?,
    };
    if book.bid_px.is_empty()
        || book.ask_px.is_empty()
        || book.bid_px.iter().any(|value| *value <= 0.0)
        || book.ask_px.iter().any(|value| *value <= 0.0)
        || book.bid_sz.iter().any(|value| *value < 0.0)
        || book.ask_sz.iter().any(|value| *value < 0.0)
        || book.best_bid()? >= book.best_ask()?
    {
        bail!("l2Book missing a side");
    }
    Ok(book)
}

/// Funding cache entries expire at the hour boundary. Early reads may still carry the
/// previous funding period, so entries read before the rollover buffer expires live only
/// until the end of that buffer.
fn slow_cache_fresh(key: &str, observed_at_ms: i64, now_ms: i64, ttl_ms: i64) -> bool {
    const HOUR_MS: i64 = 3_600_000;
    const ROLLOVER_MS: i64 = 60_000;
    let hourly = key.starts_with("funding:") || key == "predicted-fundings";
    let hour = observed_at_ms.div_euclid(HOUR_MS);
    let rolled = |ts: i64| ts.rem_euclid(HOUR_MS) >= ROLLOVER_MS;
    now_ms.saturating_sub(observed_at_ms) <= ttl_ms
        && (!hourly
            || (now_ms.div_euclid(HOUR_MS) == hour && (rolled(observed_at_ms) || !rolled(now_ms))))
}

fn domain_scan_cache_fresh(scan: &AssetDomainScan, now_ms: i64) -> bool {
    let ttl_ms = if scan.incomplete_reads.is_empty() {
        DOMAIN_SCAN_CACHE_MS
    } else {
        INCOMPLETE_DOMAIN_SCAN_CACHE_MS
    };
    now_ms.saturating_sub(scan.observed_at_ms) <= ttl_ms
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    #[tokio::test]
    async fn resting_risk_shares_fresh_account_reads_and_keeps_live_orders() -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let client = InfoClient::for_test(format!("http://{}", listener.local_addr()?))?;
        *client.domain_scan_cache.lock().unwrap() = Some((
            "account".to_string(),
            AssetDomainScan {
                observed_at_ms: now_ms(),
                domain_clean: true,
                ..AssetDomainScan::default()
            },
        ));
        let server = tokio::spawn(async move {
            let mut counts = HashMap::<String, usize>::new();
            for _ in 0..11 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let request = loop {
                    let mut chunk = [0u8; 2048];
                    let size = socket.read(&mut chunk).await.unwrap();
                    assert!(size > 0);
                    bytes.extend_from_slice(&chunk[..size]);
                    if let Some(boundary) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..boundary]);
                        let length: usize = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|value| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        if bytes.len() >= boundary + 4 + length {
                            break serde_json::from_slice::<Value>(
                                &bytes[boundary + 4..boundary + 4 + length],
                            )
                            .unwrap();
                        }
                    }
                };
                let kind = request["type"].as_str().unwrap();
                *counts.entry(kind.to_string()).or_default() += 1;
                let body = match kind {
                    "spotClearinghouseState" => json!({"balances":[{"coin":"USDC","total":"1000"},{"coin":"HYPE","total":"2"}],"portfolioMarginEnabled":true,"portfolioMarginRatio":"0.4","tokenToAvailableAfterMaintenance":[[0,"500"]]}),
                    "clearinghouseState" => json!({"assetPositions":[{"position":{"coin":"HYPE","szi":"-2"}}]}),
                    "openOrders" => json!([{"oid":42}]),
                    "portfolio" => json!([["allTime",{"accountValueHistory":[[now_ms(),"1000"]]}]]),
                    "fundingHistory" => json!([]),
                    "extraAgents" => json!([{"address":"agent","validUntil":now_ms()+600_000}]),
                    "userFees" => json!({"userCrossRate":"0.00045","userAddRate":"0.00015","userSpotCrossRate":"0.0007","userSpotAddRate":"0.0004"}),
                    "borrowLendReserveState" => json!({"borrowYearlyRate":"0.05"}),
                    "predictedFundings" => json!([["HYPE",[["HlPerp",{"fundingRate":"0.00001","nextFundingTime":now_ms(),"fundingIntervalHours":1}]]]]),
                    "l2Book" => json!({"time":now_ms(),"levels":[[{"px":"100","sz":"10"}],[{"px":"101","sz":"10"}]]}),
                    _ => panic!("unexpected request {kind}"),
                }.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            counts
        });
        let (snapshot, state) = tokio::time::timeout(
            Duration::from_secs(3),
            client.snapshot_for_resting_risk("account", "agent"),
        )
        .await??;
        let counts = server.await?;
        assert_eq!(counts["spotClearinghouseState"], 1);
        assert_eq!(counts["clearinghouseState"], 1);
        assert_eq!(counts["openOrders"], 1);
        assert_eq!(state.open_order_oids, vec![42]);
        assert_eq!(state.hype_spot_qty, snapshot.account.hype_spot_qty);
        assert_eq!(state.hype_perp_qty, snapshot.account.hype_perp_qty);
        assert_eq!(snapshot.account.portfolio_margin_ratio, 0.4);
        assert!(snapshot.account.agent_authorized);
        assert!(open_order_oids(&json!({})).is_err());
        assert!(open_order_oids(&json!([{"coin":"HYPE"}])).is_err());
        Ok(())
    }

    #[test]
    fn funding_cache_expires_at_the_hour_but_other_inputs_keep_their_ttl() {
        let hour = 1_789_498_800_000_i64; // Fixed UTC hour boundary.
        let read = hour - 1_000;
        assert!(!slow_cache_fresh(
            "predicted-fundings",
            read,
            hour + 5_000,
            300_000
        ));
        assert!(!slow_cache_fresh(
            "funding:HYPE",
            read,
            hour + 5_000,
            300_000
        ));
        assert!(slow_cache_fresh(
            "predicted-fundings",
            hour + 61_000,
            hour + 200_000,
            300_000
        ));
        assert!(slow_cache_fresh(
            "borrow-reserve:0",
            read,
            hour + 5_000,
            300_000
        ));
    }

    #[test]
    fn a_funding_read_before_hl_rolls_the_hour_expires_at_one_minute_past() {
        let hour = 1_789_970_400_000_i64; // Fixed UTC hour boundary.
        let early = hour + 10_000; // HL may still report the previous hour here
        assert!(slow_cache_fresh(
            "predicted-fundings",
            early,
            hour + 50_000,
            300_000
        ));
        assert!(!slow_cache_fresh(
            "predicted-fundings",
            early,
            hour + 61_000,
            300_000
        ));
        assert!(!slow_cache_fresh(
            "predicted-fundings",
            early,
            hour + 305_000,
            300_000
        ));
        assert!(slow_cache_fresh(
            "predicted-fundings",
            hour + 61_000,
            hour + 300_000,
            300_000
        ));
        assert!(slow_cache_fresh(
            "borrow-reserve:0",
            early,
            hour + 200_000,
            300_000
        ));
    }

    #[test]
    fn an_incomplete_domain_scan_expires_long_before_a_complete_one() {
        let complete = AssetDomainScan {
            observed_at_ms: 1_000,
            domain_clean: true,
            ..AssetDomainScan::default()
        };
        assert!(domain_scan_cache_fresh(&complete, 1_000 + 299_000));
        assert!(!domain_scan_cache_fresh(&complete, 1_000 + 301_000));
        let incomplete = AssetDomainScan {
            incomplete_reads: vec!["openOrders[io]: HTTP 429".to_string()],
            ..complete
        };
        assert!(domain_scan_cache_fresh(&incomplete, 1_000 + 29_000));
        assert!(!domain_scan_cache_fresh(&incomplete, 1_000 + 31_000));
    }

    #[test]
    fn info_retry_policy_is_bounded_to_rate_limits_and_server_errors() {
        assert!(retryable_info_status(429));
        assert!(retryable_info_status(503));
        assert!(!retryable_info_status(400));
        assert_eq!(info_retry_delay(0), Duration::from_millis(250));
        assert_eq!(info_retry_delay(3), Duration::from_secs(2));
        assert_eq!(info_retry_delay(99), Duration::from_secs(2));
    }

    #[tokio::test]
    async fn info_client_retries_429_and_decodes_the_following_json() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for (index, stream) in listener.incoming().take(2).enumerate() {
                let mut stream = stream.unwrap();
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).unwrap();
                let (status, body) = if index == 0 {
                    ("429 Too Many Requests", "null")
                } else {
                    ("200 OK", "{\"ok\":true}")
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let client = InfoClient::for_test(format!("http://{address}")).unwrap();
        assert_eq!(
            client.post(json!({"type":"test"})).await.unwrap(),
            json!({"ok":true})
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn spot_book_uses_the_info_api_asset_symbol() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0_u8; 2048];
            let size = stream.read(&mut request).unwrap();
            let body = r#"{"time":1,"levels":[[{"px":"10","sz":"1"}],[{"px":"11","sz":"1"}]]}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
            String::from_utf8_lossy(&request[..size]).to_string()
        });
        let client = InfoClient::for_test(format!("http://{address}")).unwrap();
        client.book("HYPE/USDC").await.unwrap();
        let request = server.join().unwrap();
        assert!(request.contains(r#""coin":"@107""#), "{request}");
    }

    #[tokio::test]
    async fn slow_cache_is_reused_but_fresh_preflight_bypasses_it() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for (index, stream) in listener.incoming().take(2).enumerate() {
                let mut stream = stream.unwrap();
                let mut request = [0_u8; 2048];
                let _ = stream.read(&mut request).unwrap();
                let body = format!("{{\"request\":{}}}", index + 1);
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let client = InfoClient::for_test(format!("http://{address}")).unwrap();
        let first = client
            .cached_post("test".to_string(), 60_000, json!({"type":"test"}), false)
            .await
            .unwrap();
        let cached = client
            .cached_post("test".to_string(), 60_000, json!({"type":"test"}), false)
            .await
            .unwrap();
        let fresh = client
            .cached_post("test".to_string(), 60_000, json!({"type":"test"}), true)
            .await
            .unwrap();
        assert_eq!(first, json!({"request":1}));
        assert_eq!(cached, first);
        assert_eq!(fresh, json!({"request":2}));
        server.join().unwrap();
    }

    #[test]
    fn funding_requires_24_negative_hourly_rows() {
        let rows: Vec<Value> = (0..24)
            .map(|i| json!({"time": i * 3_600_000, "fundingRate":"-0.00001"}))
            .collect();
        let (bps, negative) = funding_24h(&Value::Array(rows)).unwrap();
        assert!((bps + 2.4).abs() < 1e-9);
        assert!(negative);
    }

    #[test]
    fn portfolio_observation_preserves_the_actual_nav_timestamp() {
        let portfolio = json!([["allTime", {"accountValueHistory":[[100,"1000"],[150,"1100"]]}]]);
        assert_eq!(portfolio_observation(&portfolio).unwrap(), (150, 1100.0));
        let invalid = json!([["allTime", {"accountValueHistory":[["invalid","1100"]]}]]);
        assert!(portfolio_observation(&invalid).is_err());
    }

    #[test]
    fn predicted_funding_selects_hyperliquid_hype_row() {
        let observed_ms = 1_000_000;
        let response = json!([
            ["BTC", [["HlPerp", {"fundingRate":"0.1","nextFundingTime":4600000,"fundingIntervalHours":1}]]],
            ["HYPE", [
                ["Other", {"fundingRate":"0.2","nextFundingTime":4600000,"fundingIntervalHours":1}],
                ["HlPerp", {"fundingRate":"-0.00002","nextFundingTime":4600000,"fundingIntervalHours":1}]
            ]]
        ]);
        assert_eq!(
            predicted_hype_funding(&response, observed_ms).unwrap(),
            (-0.00002, 4_600_000)
        );
    }

    #[test]
    fn predicted_funding_fails_closed_on_missing_or_stale_data() {
        assert!(predicted_hype_funding(&json!([]), 1_000_000).is_err());
        let stale = json!([["HYPE", [["HlPerp", {
            "fundingRate":"0.00001",
            "nextFundingTime":1,
            "fundingIntervalHours":1
        }]]]]);
        assert!(predicted_hype_funding(&stale, 3_900_001).is_ok());
        assert!(predicted_hype_funding(&stale, 5_000_000).is_err());
    }

    #[test]
    fn malformed_funding_and_present_balance_fail_closed() {
        assert!(funding_rows(&json!([{"time": 1, "fundingRate": "bad"}])).is_err());
        assert!(optional_balance_total(
            &json!({"balances":[{"coin":"HYPE","total":"bad"}]}),
            "HYPE"
        )
        .is_err());
        assert_eq!(
            optional_balance_total(&json!({"balances":[]}), "HYPE").unwrap(),
            0.0
        );
    }

    #[test]
    fn borrow_lend_state_reads_the_exchange_token_to_state_shape() {
        let state = json!({
            "tokenToState":[
                [0,{"borrow":{"basis":"1198.2","value":"1198.3"}}],
                [150,{"borrow":{"basis":"0","value":"0"}}]
            ],
            "health":"healthy",
            "healthFactor":"4.2"
        });
        let usdc = borrow_lend_usdc_state(&state).unwrap();
        assert_eq!(
            required_number_at(&usdc["borrow"], "basis").unwrap(),
            1198.2
        );
        assert_eq!(
            required_number_at(&usdc["borrow"], "value").unwrap(),
            1198.3
        );
    }

    #[test]
    fn book_requires_exchange_time_and_numeric_uncrossed_levels() {
        let valid = json!({
            "time": 1_000,
            "levels": [
                [{"px":"58.0","sz":"1.0"}],
                [{"px":"58.1","sz":"2.0"}]
            ]
        });
        assert_eq!(parse_book(&valid, 1_010).unwrap().exchange_ts_ms, 1_000);

        let mut missing_time = valid.clone();
        missing_time.as_object_mut().unwrap().remove("time");
        assert!(parse_book(&missing_time, 1_010).is_err());

        let crossed = json!({
            "time": 1_000,
            "levels": [
                [{"px":"58.2","sz":"1.0"}],
                [{"px":"58.1","sz":"2.0"}]
            ]
        });
        assert!(parse_book(&crossed, 1_010).is_err());
    }
}
