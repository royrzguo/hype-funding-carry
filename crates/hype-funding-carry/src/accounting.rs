use std::collections::HashSet;

use crate::state_io::{durable_write, state_path};
use anyhow::{Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::{now_ms, InfoClient, Snapshot};
use crate::audit::AuditClient;

const YEAR_MS: f64 = 365.0 * 86_400_000.0;
const APR_MIN_SAMPLE_MS: i64 = 86_400_000;

#[derive(Serialize)]
struct FundingRow {
    funding_ts_ms: i64,
    hash: String,
    hypercore_account: String,
    coin: String,
    funding_rate: f64,
    position_qty: f64,
    actual_usdc: f64,
    reference_oracle_px: Option<f64>,
    reference_age_ms: Option<u64>,
    expected_usdc: Option<f64>,
    difference_usdc: Option<f64>,
    direction_ok: Option<u8>,
    raw_update: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct AccountLedgerRow {
    event_ts_ms: i64,
    hash: String,
    hypercore_account: String,
    event_type: String,
    token: String,
    amount: f64,
    usdc_value: f64,
    net_external_usdc: f64,
    fee_usd: f64,
    raw_update: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct AccountFillRow {
    hypercore_account: String,
    fill_ts_ms: i64,
    tid: u64,
    oid: u64,
    coin: String,
    side: String,
    liquidity: &'static str,
    px: f64,
    qty: f64,
    fee_native: f64,
    fee_token: String,
    raw_fill: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct BorrowSnapshotRow {
    hypercore_account: String,
    snapshot_ts_ms: i64,
    spot_borrowed_usdc: f64,
    borrow_value_usdc: f64,
    borrow_basis_usdc: f64,
    accrued_unindexed_usdc: f64,
    borrow_apr: f64,
    modeled_cumulative_interest_usd: f64,
    reconciliation_difference_usd: f64,
    health: String,
    health_factor: f64,
    raw_accounting_state: String,
    initialization_source: String,
    created_ts_ms: i64,
    accounting_start_ts_ms: i64,
    borrow_observed_ts_ms: i64,
    accounting_debt_usdc: f64,
    accounting_borrow_apr: f64,
}

#[derive(Serialize)]
struct PnlRow {
    accounting_start_ts_ms: i64,
    opening_account_value_usd: f64,
    snapshot_ts_ms: i64,
    run_tag: &'static str,
    hypercore_account: String,
    entry_ts_ms: i64,
    matched_qty: f64,
    entry_notional_usd: f64,
    spot_entry_vwap: f64,
    perp_entry_vwap: f64,
    entry_basis_bps: f64,
    spot_mid_px: f64,
    perp_mid_px: f64,
    current_basis_bps: f64,
    fees_realized_usd: f64,
    funding_realized_usd: f64,
    borrow_cost_est_usd: f64,
    basis_mtm_usd: f64,
    net_pnl_ex_mtm_usd: f64,
    net_pnl_incl_mtm_usd: f64,
    net_apr_ex_mtm_pct: f64,
    net_apr_incl_mtm_pct: f64,
    account_value_usd: f64,
    portfolio_margin_ratio: f64,
    usdc_borrowed: f64,
    residual_delta_usd: f64,
    /// Denominator for reported return. Now the HyperCore account, matching what the band
    /// is sized against; the column keeps its name so its history stays comparable.
    /// Account equity used by this personal-account reference.
    strategy_nav_usd: f64,
    /// The HyperCore account, which is what return is measured against and what the band is
    /// sized on.
    core_equity_usd: f64,
    net_external_usdc: f64,
    trading_realized_pnl_usd: f64,
    account_nav_pnl_usd: f64,
    reconciliation_difference_usd: f64,
    accounting_age_ms: u64,
    apr_eligible: u8,
    account_apr_pct: f64,
    created_ts_ms: i64,
}

pub struct AccountingResult {
    pub pnl_written: bool,
}

struct PnlCalc {
    basis_mtm: f64,
    net_ex_mtm: f64,
    net_incl_mtm: f64,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct Inventory {
    qty: f64,
    avg_px: f64,
    opened_ts_ms: i64,
    realized_pnl: f64,
}

#[derive(Clone, Default, Deserialize, Serialize)]
struct FillSummary {
    spot: Inventory,
    perp: Inventory,
    fees_usd: f64,
}

#[derive(Deserialize, Serialize)]
struct AccountingBaseline {
    hypercore_account: String,
    snapshot_ts_ms: i64,
    account_value_usd: f64,
    spot: Inventory,
    perp: Inventory,
    spot_borrowed_usdc: f64,
    borrow_apr: f64,
}

impl AccountingBaseline {
    fn from_snapshot(account: &str, snapshot: &Snapshot) -> Result<Self> {
        let ts = snapshot.account.account_value_ts_ms;
        anyhow::ensure!(
            ts > 0 && snapshot.account.account_value_usd.is_finite(),
            "invalid accounting baseline"
        );
        let inventory = |qty, avg_px| Inventory {
            qty,
            avg_px,
            opened_ts_ms: if qty != 0.0 { ts } else { 0 },
            realized_pnl: 0.0,
        };
        Ok(Self {
            hypercore_account: canonical_account(account),
            snapshot_ts_ms: ts,
            account_value_usd: snapshot.account.account_value_usd,
            spot: inventory(
                snapshot.account.hype_spot_qty,
                (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0,
            ),
            perp: inventory(
                snapshot.account.hype_perp_qty,
                (snapshot.perp.best_bid()? + snapshot.perp.best_ask()?) / 2.0,
            ),
            spot_borrowed_usdc: snapshot.account.usdc_borrowed,
            borrow_apr: snapshot.account.usdc_borrow_apr,
        })
    }

    fn seed_fills(&self) -> FillCheckpoint {
        FillCheckpoint {
            accounting_start_ts_ms: self.snapshot_ts_ms,
            // Events in the baseline millisecond are absorbed in the opening snapshot.
            cursor: (self.snapshot_ts_ms, u64::MAX, char::MAX.to_string()),
            summary: FillSummary {
                spot: self.spot.clone(),
                perp: self.perp.clone(),
                fees_usd: 0.0,
            },
        }
    }

    fn nav_pnl(&self, account_value_usd: f64, net_external_usdc: f64) -> f64 {
        account_value_usd - self.account_value_usd - net_external_usdc
    }
}

async fn require_quiet_baseline(
    info: &InfoClient,
    account: &str,
    snapshot: &Snapshot,
) -> Result<()> {
    let start = snapshot
        .account
        .account_value_ts_ms
        .min(snapshot.account.snapshot_ts_ms)
        .saturating_sub(1);
    let (fills, ledger, funding) = tokio::try_join!(
        info.user_fills_by_time(account, start),
        info.user_non_funding_ledger_updates(account, start),
        info.user_funding(account, start),
    )?;
    for response in [fills, ledger, funding] {
        let rows = response
            .as_array()
            .context("baseline account history is not an array")?;
        require_synchronized_history(rows, snapshot)?;
    }
    Ok(())
}

fn require_synchronized_history(rows: &[Value], snapshot: &Snapshot) -> Result<()> {
    let start = snapshot
        .account
        .account_value_ts_ms
        .min(snapshot.account.snapshot_ts_ms)
        .saturating_sub(1);
    let end = snapshot
        .spot
        .recv_ts_ms
        .max(snapshot.perp.recv_ts_ms)
        .max(snapshot.account.snapshot_ts_ms)
        .max(snapshot.account.account_value_ts_ms)
        .saturating_add(1);
    anyhow::ensure!(!rows.iter().any(|row| {
        let ts = number(row, "time") as i64;
        ts >= start && ts <= end
    }), "account activity occurred between NAV observation and account snapshot; wait for a synchronized NAV observation");
    Ok(())
}

fn canonical_account(account: &str) -> String {
    account.to_ascii_lowercase()
}

#[allow(clippy::too_many_arguments)]
fn calculate_pnl(
    matched_qty: f64,
    spot_entry: f64,
    perp_entry: f64,
    spot_mid: f64,
    perp_mid: f64,
    funding_usd: f64,
    fees_usd: f64,
    borrow_cost_usd: f64,
) -> PnlCalc {
    let basis_mtm = matched_qty * ((spot_mid - spot_entry) + (perp_entry - perp_mid));
    let net_ex_mtm = funding_usd - fees_usd - borrow_cost_usd;
    PnlCalc {
        basis_mtm,
        net_ex_mtm,
        net_incl_mtm: net_ex_mtm + basis_mtm,
    }
}

fn number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .or_else(|| value[key].as_str().and_then(|x| x.parse().ok()))
        .unwrap_or(0.0)
}

fn fill_fee_usd(fill: &Value) -> Result<f64> {
    let fee = number(fill, "fee");
    match text(fill, "feeToken") {
        "" | "USDC" => Ok(fee),
        "HYPE" => Ok(fee * number(fill, "px")),
        token => anyhow::bail!("unsupported HYPE Funding Carry fill fee token {token}"),
    }
}

fn apply_spot_fill(inventory: &mut Inventory, fill: &Value) -> Result<()> {
    let px = number(fill, "px");
    let qty = number(fill, "sz");
    let ts_ms = number(fill, "time") as i64;
    if px <= 0.0 || qty <= 0.0 || ts_ms <= 0 {
        anyhow::bail!("invalid spot fill")
    }
    match text(fill, "side") {
        "B" => {
            if inventory.qty <= 1e-12 {
                inventory.avg_px = px;
                inventory.opened_ts_ms = ts_ms;
            } else {
                inventory.avg_px =
                    (inventory.qty * inventory.avg_px + qty * px) / (inventory.qty + qty);
            }
            inventory.qty += qty;
        }
        "A" => {
            if qty > inventory.qty + 1e-9 {
                anyhow::bail!("spot fill history implies a short HYPE balance")
            }
            inventory.realized_pnl += qty * (px - inventory.avg_px);
            inventory.qty = (inventory.qty - qty).max(0.0);
            if inventory.qty <= 1e-12 {
                inventory.avg_px = 0.0;
                inventory.opened_ts_ms = 0;
            }
        }
        side => anyhow::bail!("unsupported spot fill side {side}"),
    }
    if text(fill, "feeToken") == "HYPE" {
        let fee = number(fill, "fee");
        if fee > inventory.qty + 1e-9 {
            anyhow::bail!("HYPE fee exceeds spot inventory")
        }
        if fee < 0.0 {
            // Rebate inventory enters at fill price; its value is already fee income.
            inventory.avg_px =
                (inventory.qty * inventory.avg_px - fee * px) / (inventory.qty - fee);
            if inventory.qty <= 1e-12 {
                inventory.opened_ts_ms = ts_ms;
            }
        } else {
            inventory.realized_pnl += fee * (px - inventory.avg_px);
        }
        inventory.qty = (inventory.qty - fee).max(0.0);
    }
    Ok(())
}

fn apply_perp_fill(inventory: &mut Inventory, fill: &Value) -> Result<()> {
    let px = number(fill, "px");
    let qty = number(fill, "sz");
    let ts_ms = number(fill, "time") as i64;
    let delta = match text(fill, "side") {
        "B" => qty,
        "A" => -qty,
        side => anyhow::bail!("unsupported perp fill side {side}"),
    };
    if px <= 0.0 || qty <= 0.0 || ts_ms <= 0 {
        anyhow::bail!("invalid perp fill")
    }
    if inventory.qty.abs() <= 1e-12 || inventory.qty.signum() == delta.signum() {
        if inventory.qty.abs() <= 1e-12 {
            inventory.avg_px = px;
            inventory.opened_ts_ms = ts_ms;
        } else {
            inventory.avg_px =
                (inventory.qty.abs() * inventory.avg_px + qty * px) / (inventory.qty.abs() + qty);
        }
        inventory.qty += delta;
        return Ok(());
    }
    let closed = inventory.qty.abs().min(qty);
    inventory.realized_pnl += if inventory.qty > 0.0 {
        closed * (px - inventory.avg_px)
    } else {
        closed * (inventory.avg_px - px)
    };
    let remaining = inventory.qty + delta;
    if remaining.abs() <= 1e-12 {
        inventory.qty = 0.0;
        inventory.avg_px = 0.0;
        inventory.opened_ts_ms = 0;
    } else if remaining.signum() == inventory.qty.signum() {
        inventory.qty = remaining;
    } else {
        inventory.qty = remaining;
        inventory.avg_px = px;
        inventory.opened_ts_ms = ts_ms;
    }
    Ok(())
}

#[derive(Default, Deserialize, Serialize)]
struct FillCheckpoint {
    #[serde(default)]
    accounting_start_ts_ms: i64,
    cursor: (i64, u64, String),
    summary: FillSummary,
}

impl FillCheckpoint {
    fn apply(&mut self, fills: &[Value]) -> Result<usize> {
        let mut processed = 0;
        let mut ordered = unique_fills(fills);
        ordered.sort_by_key(|fill| fill_identity(fill));
        for fill in ordered {
            let identity = fill_identity(fill);
            if identity <= self.cursor {
                continue;
            }
            match text(fill, "coin") {
                "@107" => apply_spot_fill(&mut self.summary.spot, fill)?,
                "HYPE" => apply_perp_fill(&mut self.summary.perp, fill)?,
                coin => anyhow::bail!("unexpected account fill asset {coin}"),
            }
            self.summary.fees_usd += fill_fee_usd(fill)?;
            self.cursor = identity;
            processed += 1;
        }
        Ok(processed)
    }
}

#[cfg(test)]
fn summarize_fills(value: &Value) -> Result<FillSummary> {
    let mut checkpoint = FillCheckpoint::default();
    checkpoint.apply(
        value
            .as_array()
            .context("userFills response is not an array")?,
    )?;
    Ok(checkpoint.summary)
}

fn fill_identity(fill: &Value) -> (i64, u64, String) {
    (
        number(fill, "time") as i64,
        number(fill, "tid") as u64,
        text(fill, "coin").to_string(),
    )
}

fn unique_fills(fills: &[Value]) -> Vec<&Value> {
    let mut seen = HashSet::new();
    let mut result: Vec<_> = fills
        .iter()
        .filter(|fill| seen.insert(fill_identity(fill)))
        .collect();
    result.sort_by_key(|fill| fill_identity(fill));
    result
}

fn account_fill_row(account: &str, fill: &Value) -> Result<AccountFillRow> {
    let (fill_ts_ms, tid, coin) = fill_identity(fill);
    let px = number(fill, "px");
    let qty = number(fill, "sz");
    if fill_ts_ms <= 0 || tid == 0 || px <= 0.0 || qty <= 0.0 || coin.is_empty() {
        anyhow::bail!("invalid account fill")
    }
    Ok(AccountFillRow {
        hypercore_account: canonical_account(account),
        fill_ts_ms,
        tid,
        oid: number(fill, "oid") as u64,
        coin,
        side: text(fill, "side").to_string(),
        liquidity: if fill["crossed"].as_bool().unwrap_or(false) {
            "taker"
        } else {
            "maker"
        },
        px,
        qty,
        fee_native: number(fill, "fee"),
        fee_token: text(fill, "feeToken").to_string(),
        raw_fill: serde_json::to_string(fill)?,
        created_ts_ms: now_ms(),
    })
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or("")
}

fn account_ledger_row(account: &str, update: &Value) -> Option<AccountLedgerRow> {
    let delta = update.get("delta")?;
    let event_type = text(delta, "type");
    let is_transfer = matches!(
        event_type,
        "send" | "spotTransfer" | "internalTransfer" | "subAccountTransfer"
    );
    let amount = number(delta, "amount");
    let usdc = number(delta, "usdc");
    let usdc_value = if delta.get("usdcValue").is_some() {
        number(delta, "usdcValue")
    } else {
        usdc.abs()
    };
    let account = canonical_account(account);
    let source = text(delta, "user").to_ascii_lowercase();
    let destination = text(delta, "destination").to_ascii_lowercase();
    let fee_usd = if event_type == "accountActivationGas" {
        amount.abs()
    } else if is_transfer && destination == account && source != account {
        0.0
    } else if matches!(text(delta, "feeToken"), "" | "USDC") {
        number(delta, "fee").abs()
    } else {
        0.0
    };
    let net_external_usdc = match event_type {
        "deposit" => usdc.abs(),
        "withdraw" => -usdc.abs(),
        _ if is_transfer && destination == account && source != account => usdc_value,
        _ if is_transfer && source == account && destination != account => -usdc_value,
        _ => 0.0,
    };
    let raw_hash = text(update, "hash");
    let hash = if matches!(event_type, "send" | "spotTransfer" | "deposit" | "withdraw") {
        raw_hash.to_string()
    } else {
        format!("{raw_hash}:{event_type}")
    };
    Some(AccountLedgerRow {
        event_ts_ms: number(update, "time") as i64,
        hash,
        hypercore_account: account.to_string(),
        event_type: event_type.to_string(),
        token: if event_type == "accountActivationGas" {
            "USDC".to_string()
        } else {
            text(delta, "token").to_string()
        },
        amount: if amount != 0.0 { amount } else { usdc },
        usdc_value,
        net_external_usdc,
        fee_usd,
        raw_update: serde_json::to_string(update).ok()?,
        created_ts_ms: now_ms(),
    })
}

async fn sync_account_ledger(
    info: &InfoClient,
    audit: &AuditClient,
    account: &str,
    since: i64,
    snapshot: &Snapshot,
) -> Result<()> {
    let until = snapshot.account.account_value_ts_ms;
    let latest_ts = number(&audit.ledger_summary(account)?, "latest") as i64;
    let mut cursor = latest_ts
        .saturating_sub(1)
        .min(until.min(snapshot.account.snapshot_ts_ms).saturating_sub(1))
        .max(since.saturating_sub(2));
    for _ in 0..20 {
        let updates = info
            .user_non_funding_ledger_updates(account, cursor)
            .await?;
        let updates = updates
            .as_array()
            .context("userNonFundingLedgerUpdates response is not an array")?;
        require_synchronized_history(updates, snapshot)?;
        for update in updates {
            let ts = number(update, "time") as i64;
            if ts < since || ts > until {
                continue;
            }
            if let Some(row) = account_ledger_row(account, update) {
                audit.insert("hfc_account_ledger", &row).await?;
            }
        }
        if updates.len() < 500
            || updates
                .iter()
                .any(|update| number(update, "time") as i64 > until)
        {
            return Ok(());
        }
        let next = updates
            .iter()
            .map(|update| number(update, "time") as i64)
            .max()
            .context("ledger page missing final timestamp")?;
        anyhow::ensure!(
            next > cursor,
            "ledger page saturated one timestamp; history is incomplete"
        );
        cursor = next;
    }
    anyhow::bail!("account ledger pagination exceeded 20 pages")
}

async fn sync_account_fills(
    info: &InfoClient,
    audit: &AuditClient,
    account: &str,
    baseline: &AccountingBaseline,
    snapshot: &Snapshot,
) -> Result<FillSummary> {
    let until = snapshot.account.account_value_ts_ms;
    let account_key = canonical_account(account);
    let checkpoint_path = state_path(format!("hfc_fill_summary_{account_key}.json"));
    let mut save_seed = false;
    let mut checkpoint: FillCheckpoint = match std::fs::read(&checkpoint_path) {
        Ok(bytes) => serde_json::from_slice(&bytes).context("decode fill checkpoint")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            save_seed = true;
            baseline.seed_fills()
        }
        Err(error) => return Err(error).context("read fill checkpoint"),
    };
    if checkpoint.accounting_start_ts_ms != baseline.snapshot_ts_ms {
        save_seed = true;
        checkpoint = baseline.seed_fills();
    }
    let latest_ts = audit.latest_account_fill(account)?;
    {
        let since = baseline.snapshot_ts_ms.saturating_add(1);
        let mut cursor = latest_ts
            .saturating_sub(1)
            .min(until.min(snapshot.account.snapshot_ts_ms).saturating_sub(1))
            .max(since.saturating_sub(2));
        let mut complete = false;
        let mut seen = HashSet::new();
        let mut pending = Vec::new();
        for _ in 0..20 {
            let fills = info.user_fills_by_time(account, cursor).await?;
            let fills = fills
                .as_array()
                .context("userFillsByTime response is not an array")?;
            require_synchronized_history(fills, snapshot)?;
            for fill in unique_fills(fills) {
                let identity = fill_identity(fill);
                seen.insert(identity.clone());
                anyhow::ensure!(seen.len() < 10_000, "fill backfill reached the 10000-recent-fill API retention limit; completeness is unknown");
                let ts = number(fill, "time") as i64;
                if ts < since || ts > until {
                    continue;
                }
                require_ordered_fill(
                    &checkpoint,
                    &identity,
                    audit.has_account_fill(account, &identity)?,
                )?;
                pending.push(account_fill_row(account, fill)?);
            }
            if fills.len() < 2_000 || fills.iter().any(|fill| number(fill, "time") as i64 > until) {
                complete = true;
                break;
            }
            let next = fills
                .iter()
                .map(|fill| number(fill, "time") as i64)
                .max()
                .context("userFillsByTime page missing final timestamp")?;
            if next <= cursor {
                anyhow::bail!("userFillsByTime cursor did not advance")
            }
            cursor = next;
        }
        if !complete {
            anyhow::bail!("userFillsByTime pagination exceeded 20 pages")
        }
        for row in pending {
            audit.insert("hfc_account_fill", &row).await?;
        }
    }
    let fills = audit
        .account_fills_after(account, &checkpoint.cursor)?
        .iter()
        .filter(|row| number(row, "fill_ts_ms") as i64 <= until)
        .map(|row| {
            serde_json::from_str::<Value>(text(row, "raw_fill"))
                .context("decode persisted account fill")
        })
        .collect::<Result<Vec<_>>>()?;
    if checkpoint.apply(&fills)? > 0 || save_seed {
        durable_write(&checkpoint_path, &serde_json::to_vec(&checkpoint)?)?;
    }
    Ok(checkpoint.summary)
}

fn require_ordered_fill(
    checkpoint: &FillCheckpoint,
    identity: &(i64, u64, String),
    known: bool,
) -> Result<()> {
    anyhow::ensure!(*identity > checkpoint.cursor || known,
        "newly observed fill precedes the inventory checkpoint; accounting history needs reconciliation");
    Ok(())
}

fn require_pnl_for_position(
    pnl_written: bool,
    hype_spot_qty: f64,
    hype_perp_qty: f64,
) -> Result<()> {
    if !pnl_written && (hype_spot_qty.abs() > 1e-9 || hype_perp_qty.abs() > 1e-9) {
        anyhow::bail!("open HYPE Funding Carry position has no writable account PnL baseline")
    }
    Ok(())
}

async fn sync_funding(
    info: &InfoClient,
    audit: &AuditClient,
    account: &str,
    since: i64,
    snapshot: &Snapshot,
) -> Result<()> {
    let until = snapshot.account.account_value_ts_ms;
    let account_key = canonical_account(account);
    let latest_ts = audit.latest_funding(account)?;
    let mut cursor = latest_ts
        .saturating_sub(1)
        .min(until.min(snapshot.account.snapshot_ts_ms).saturating_sub(1))
        .max(since.saturating_sub(2));
    for _ in 0..20 {
        let updates = info.user_funding(account, cursor).await?;
        let updates = updates
            .as_array()
            .context("userFunding response is not an array")?;
        require_synchronized_history(updates, snapshot)?;
        for update in updates {
            let delta = &update["delta"];
            if delta["coin"].as_str() != Some("HYPE") {
                continue;
            }
            let ts_ms = number(update, "time") as i64;
            if ts_ms < since || ts_ms > until {
                continue;
            }
            let rate = number(delta, "fundingRate");
            let qty = number(delta, "szi");
            let actual = number(delta, "usdc");
            // Exchange funding cash is authoritative. Historical oracle checks require
            // a separate collector; unavailable estimates are NULL, never fabricated.
            audit
                .insert(
                    "hfc_account_funding",
                    &FundingRow {
                        funding_ts_ms: ts_ms,
                        hash: update["hash"].as_str().unwrap_or("").to_string(),
                        hypercore_account: account_key.clone(),
                        coin: "HYPE".to_string(),
                        funding_rate: rate,
                        position_qty: qty,
                        actual_usdc: actual,
                        reference_oracle_px: None,
                        reference_age_ms: None,
                        expected_usdc: None,
                        difference_usdc: None,
                        direction_ok: None,
                        raw_update: serde_json::to_string(update)?,
                        created_ts_ms: now_ms(),
                    },
                )
                .await?;
        }
        if updates.len() < 500
            || updates
                .iter()
                .any(|update| number(update, "time") as i64 > until)
        {
            return Ok(());
        }
        let next = updates
            .iter()
            .map(|update| number(update, "time") as i64)
            .max()
            .context("userFunding page missing final timestamp")?;
        anyhow::ensure!(
            next > cursor,
            "funding page saturated one timestamp; history is incomplete"
        );
        cursor = next;
    }
    anyhow::bail!("userFunding pagination exceeded 20 pages")
}

fn interest_increment_usd(
    start_ts_ms: i64,
    end_ts_ms: i64,
    start_debt_usd: f64,
    end_debt_usd: f64,
    start_apr: f64,
    end_apr: f64,
) -> f64 {
    let elapsed_ms = end_ts_ms.saturating_sub(start_ts_ms).max(0) as f64;
    let average_debt = (start_debt_usd.max(0.0) + end_debt_usd.max(0.0)) / 2.0;
    let average_apr = (start_apr.max(0.0) + end_apr.max(0.0)) / 2.0;
    average_debt * average_apr * elapsed_ms / YEAR_MS
}

async fn sync_borrow_snapshot(
    info: &InfoClient,
    audit: &AuditClient,
    account: &str,
    snapshot: &Snapshot,
    baseline: &AccountingBaseline,
) -> Result<f64> {
    let current = &snapshot.account;
    let observed = info.borrow_accounting_state(account).await?;
    let previous = audit
        .previous_borrow(account)?
        .filter(|row| number(row, "accounting_start_ts_ms") as i64 == baseline.snapshot_ts_ms);
    let (modeled_cumulative_interest_usd, initialization_source) = match previous {
        Some(previous) => (
            number(&previous, "modeled_cumulative_interest_usd")
                + interest_increment_usd(
                    number(&previous, "snapshot_ts_ms") as i64,
                    current.account_value_ts_ms,
                    number(&previous, "accounting_debt_usdc"),
                    current.usdc_borrowed,
                    number(&previous, "accounting_borrow_apr"),
                    current.usdc_borrow_apr,
                ),
            "previous_borrow_snapshot".to_string(),
        ),
        None => (
            interest_increment_usd(
                baseline.snapshot_ts_ms,
                current.account_value_ts_ms,
                baseline.spot_borrowed_usdc,
                current.usdc_borrowed,
                baseline.borrow_apr,
                current.usdc_borrow_apr,
            ),
            "accounting_baseline".to_string(),
        ),
    };
    audit
        .insert(
            "hfc_borrow_snapshot",
            &BorrowSnapshotRow {
                hypercore_account: canonical_account(account),
                snapshot_ts_ms: current.account_value_ts_ms,
                spot_borrowed_usdc: observed.spot_borrowed_usdc,
                borrow_value_usdc: observed.borrow_value_usdc,
                borrow_basis_usdc: observed.borrow_basis_usdc,
                accrued_unindexed_usdc: observed.borrow_value_usdc - observed.borrow_basis_usdc,
                borrow_apr: observed.borrow_apr,
                modeled_cumulative_interest_usd,
                reconciliation_difference_usd: observed.spot_borrowed_usdc
                    - observed.borrow_value_usdc,
                health: observed.health,
                health_factor: observed.health_factor,
                raw_accounting_state: observed.raw_accounting_state,
                initialization_source,
                created_ts_ms: now_ms(),
                accounting_start_ts_ms: baseline.snapshot_ts_ms,
                borrow_observed_ts_ms: observed.snapshot_ts_ms,
                accounting_debt_usdc: current.usdc_borrowed,
                accounting_borrow_apr: current.usdc_borrow_apr,
            },
        )
        .await?;
    Ok(modeled_cumulative_interest_usd)
}

#[allow(clippy::too_many_arguments)]
async fn write_pnl(
    audit: &AuditClient,
    account: &str,
    snapshot: &Snapshot,
    strategy_nav_usd: f64,
    core_equity_usd: f64,
    fills: FillSummary,
    borrow_cost: f64,
    baseline: &AccountingBaseline,
) -> Result<bool> {
    let account_key = canonical_account(account);
    if core_equity_usd <= 0.0 {
        return Ok(false);
    }
    let since = baseline.snapshot_ts_ms.saturating_add(1);
    let (net_external_usdc, ledger_fees) = audit.ledger_totals_since(account, since)?;
    let spot_mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let perp_mid = (snapshot.perp.best_bid()? + snapshot.perp.best_ask()?) / 2.0;
    if (fills.spot.qty - snapshot.account.hype_spot_qty).abs() * spot_mid > 1.0
        || (fills.perp.qty - snapshot.account.hype_perp_qty).abs() * perp_mid > 1.0
    {
        anyhow::bail!("account fill history does not reconcile to current HYPE positions")
    }
    let entry_ts = match (fills.spot.opened_ts_ms > 0, fills.perp.opened_ts_ms > 0) {
        (true, true) => fills.spot.opened_ts_ms.min(fills.perp.opened_ts_ms),
        (true, false) => fills.spot.opened_ts_ms,
        (false, true) => fills.perp.opened_ts_ms,
        (false, false) => 0,
    };
    let spot_entry = fills.spot.avg_px;
    let perp_entry = fills.perp.avg_px;
    let fees = fills.fees_usd + ledger_fees;
    let funding_usd = audit.funding_total(account, since)?;
    let matched_qty = snapshot
        .account
        .hype_spot_qty
        .max(0.0)
        .min((-snapshot.account.hype_perp_qty).max(0.0));
    let accounting_age_ms = (snapshot.account.account_value_ts_ms - baseline.snapshot_ts_ms).max(0);
    let trading_realized = fills.spot.realized_pnl + fills.perp.realized_pnl;
    let pnl = calculate_pnl(
        matched_qty,
        spot_entry,
        perp_entry,
        spot_mid,
        perp_mid,
        funding_usd + trading_realized,
        fees,
        borrow_cost,
    );
    let entry_notional = matched_qty * spot_entry;
    let account_nav_pnl = baseline.nav_pnl(snapshot.account.account_value_usd, net_external_usdc);
    let reconciliation_difference = account_nav_pnl - pnl.net_incl_mtm;
    let apr_eligible = accounting_age_ms >= APR_MIN_SAMPLE_MS;
    let annualizer = if apr_eligible {
        YEAR_MS / accounting_age_ms as f64 / core_equity_usd * 100.0
    } else {
        0.0
    };
    let residual_delta =
        (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * spot_mid;
    audit
        .insert(
            "hfc_live_pnl",
            &PnlRow {
                accounting_start_ts_ms: baseline.snapshot_ts_ms,
                opening_account_value_usd: baseline.account_value_usd,
                snapshot_ts_ms: snapshot.account.account_value_ts_ms,
                run_tag: "hfc_total",
                hypercore_account: account_key,
                entry_ts_ms: entry_ts,
                matched_qty,
                entry_notional_usd: entry_notional,
                spot_entry_vwap: spot_entry,
                perp_entry_vwap: perp_entry,
                entry_basis_bps: if spot_entry > 0.0 && perp_entry > 0.0 {
                    (perp_entry / spot_entry - 1.0) * 10_000.0
                } else {
                    0.0
                },
                spot_mid_px: spot_mid,
                perp_mid_px: perp_mid,
                current_basis_bps: (perp_mid / spot_mid - 1.0) * 10_000.0,
                fees_realized_usd: fees,
                funding_realized_usd: funding_usd,
                borrow_cost_est_usd: borrow_cost,
                basis_mtm_usd: pnl.basis_mtm,
                net_pnl_ex_mtm_usd: pnl.net_ex_mtm,
                net_pnl_incl_mtm_usd: pnl.net_incl_mtm,
                net_apr_ex_mtm_pct: pnl.net_ex_mtm * annualizer,
                net_apr_incl_mtm_pct: pnl.net_incl_mtm * annualizer,
                account_value_usd: snapshot.account.account_value_usd,
                portfolio_margin_ratio: snapshot.account.portfolio_margin_ratio,
                usdc_borrowed: snapshot.account.usdc_borrowed,
                residual_delta_usd: residual_delta,
                strategy_nav_usd,
                core_equity_usd,
                net_external_usdc,
                trading_realized_pnl_usd: trading_realized,
                account_nav_pnl_usd: account_nav_pnl,
                reconciliation_difference_usd: reconciliation_difference,
                accounting_age_ms: accounting_age_ms as u64,
                apr_eligible: apr_eligible as u8,
                account_apr_pct: account_nav_pnl * annualizer,
                created_ts_ms: now_ms(),
            },
        )
        .await?;
    Ok(true)
}

pub async fn sync_accounting(
    info: &InfoClient,
    audit: &AuditClient,
    account: &str,
    snapshot: &Snapshot,
    strategy_nav_usd: f64,
    core_equity_usd: f64,
) -> Result<AccountingResult> {
    let lock_path = state_path(format!(
        "hfc_accounting_{}.lock",
        canonical_account(account)
    ));
    std::fs::create_dir_all(lock_path.parent().context("accounting state directory")?)?;
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock.try_lock_exclusive()
        .context("another accounting sync is active for this account")?;
    let baseline: AccountingBaseline = match audit.accounting_baseline(account)? {
        Some(row) => serde_json::from_value(row).context("decode accounting baseline")?,
        None => {
            require_quiet_baseline(info, account, snapshot).await?;
            let baseline = AccountingBaseline::from_snapshot(account, snapshot)?;
            audit.insert("hfc_accounting_baseline", &baseline).await?;
            baseline
        }
    };
    let until = snapshot.account.account_value_ts_ms;
    anyhow::ensure!(
        until >= baseline.snapshot_ts_ms,
        "accounting snapshot precedes baseline"
    );
    // Previously fetched facts cannot be compared against an older account snapshot.
    anyhow::ensure!(
        audit.latest_account_fill(account)? <= until
            && number(&audit.ledger_summary(account)?, "latest") as i64 <= until
            && audit.latest_funding(account)? <= until,
        "accounting snapshot precedes stored account events"
    );
    if let Some(previous) = audit.previous_borrow(account)? {
        anyhow::ensure!(
            number(&previous, "accounting_start_ts_ms") as i64 != baseline.snapshot_ts_ms
                || number(&previous, "snapshot_ts_ms") as i64 <= until,
            "accounting snapshot precedes stored borrow estimate"
        );
    }
    let since = baseline.snapshot_ts_ms.saturating_add(1);
    let fills = sync_account_fills(info, audit, account, &baseline, snapshot).await?;
    sync_account_ledger(info, audit, account, since, snapshot).await?;
    sync_funding(info, audit, account, since, snapshot).await?;
    let borrow_cost = sync_borrow_snapshot(info, audit, account, snapshot, &baseline).await?;
    let pnl_written = write_pnl(
        audit,
        account,
        snapshot,
        strategy_nav_usd,
        core_equity_usd,
        fills,
        borrow_cost,
        &baseline,
    )
    .await?;
    require_pnl_for_position(
        pnl_written,
        snapshot.account.hype_spot_qty,
        snapshot.account.hype_perp_qty,
    )?;
    Ok(AccountingResult { pnl_written })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn baseline(equity: f64) -> AccountingBaseline {
        AccountingBaseline {
            hypercore_account: "a".to_string(),
            snapshot_ts_ms: 100,
            account_value_usd: equity,
            spot: Inventory {
                qty: 1.0,
                avg_px: 50.0,
                opened_ts_ms: 100,
                realized_pnl: 0.0,
            },
            perp: Inventory {
                qty: -1.0,
                avg_px: 51.0,
                opened_ts_ms: 100,
                realized_pnl: 0.0,
            },
            spot_borrowed_usdc: 0.0,
            borrow_apr: 0.0,
        }
    }

    #[test]
    fn local_baseline_excludes_old_principal_and_treats_transfers_as_capital() {
        // Old principal (1000) and recent deposit (100) are both already in opening NAV.
        let baseline = baseline(1099.96);
        assert!(baseline.nav_pnl(1099.96, 0.0).abs() < 1e-9);
        assert!((baseline.nav_pnl(1349.98, 250.0) - 0.02).abs() < 1e-9);
        assert!((baseline.nav_pnl(1249.97, 150.0) - 0.01).abs() < 1e-9);
        assert!((baseline.nav_pnl(1099.92, 0.0) + 0.04).abs() < 1e-9);
    }

    #[test]
    fn existing_inventory_is_marked_at_baseline_and_survives_restart() {
        let baseline = baseline(1099.96);
        let mut checkpoint = baseline.seed_fills();
        let pre_baseline =
            serde_json::json!({"coin":"@107", "px":"30", "sz":"1", "side":"B", "time":99,"tid":1});
        let same_millisecond =
            serde_json::json!({"coin":"@107", "px":"50", "sz":"1", "side":"B", "time":100,"tid":2});
        assert_eq!(
            checkpoint.apply(&[pre_baseline, same_millisecond]).unwrap(),
            0
        );
        let fills = vec![
            serde_json::json!({"coin":"@107", "px":"52", "sz":"1", "side":"A", "time":101,"tid":3,"fee":"0.01","feeToken":"USDC"}),
            serde_json::json!({"coin":"HYPE", "px":"52.1", "sz":"1", "side":"B", "time":101,"tid":4,"fee":"0.02","feeToken":"USDC"}),
        ];
        assert_eq!(checkpoint.apply(&fills).unwrap(), 2);
        let mut restored: FillCheckpoint =
            serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();
        assert_eq!(restored.apply(&fills).unwrap(), 0);
        assert_eq!(restored.accounting_start_ts_ms, 100);
        assert!(
            (restored.summary.spot.realized_pnl + restored.summary.perp.realized_pnl - 0.9).abs()
                < 1e-9
        );
        assert!((restored.summary.fees_usd - 0.03).abs() < 1e-9);
        let restored_baseline: AccountingBaseline =
            serde_json::from_slice(&serde_json::to_vec(&baseline).unwrap()).unwrap();
        assert_eq!(restored_baseline.account_value_usd, 1099.96);
    }

    #[tokio::test]
    async fn reported_pnl_uses_observation_window_not_visible_historical_deposits() {
        use crate::api::AccountSnapshot;
        use crate::asset_domain::AssetDomainScan;
        use crate::model::Book;
        let directory = std::env::temp_dir().join(format!("hfc-pnl-{}", uuid::Uuid::new_v4()));
        let audit = AuditClient::open(&directory).unwrap();
        // The 60-day-old 1000 deposit is absent, but a recent 100 deposit is visible.
        audit.insert("hfc_account_ledger", &serde_json::json!({
            "hypercore_account":"a", "event_ts_ms":90, "hash":"recent-deposit", "net_external_usdc":100.0, "fee_usd":0.0
        })).await.unwrap();
        let baseline = baseline(1099.96);
        let book = |price| Book {
            exchange_ts_ms: 100,
            recv_ts_ms: 100,
            bid_px: vec![price],
            ask_px: vec![price],
            bid_sz: vec![10.0],
            ask_sz: vec![10.0],
        };
        let mut snapshot = Snapshot {
            account: AccountSnapshot {
                snapshot_ts_ms: 100,
                account_value_usd: 1099.96,
                account_value_ts_ms: 100,
                portfolio_margin_enabled: true,
                portfolio_margin_ratio: 0.1,
                available_after_maintenance_usdc: 1000.0,
                usdc_total: 1049.96,
                usdc_borrowed: 0.0,
                hype_spot_qty: 1.0,
                hype_perp_qty: -1.0,
                liquidation_px: 0.0,
                funding_24h_bps: 0.0,
                funding_24h_all_negative: false,
                predicted_next_funding_rate: 0.0,
                next_funding_time_ms: 3600000,
                open_orders: 0,
                unknown_exposure: false,
                asset_domain: AssetDomainScan::default(),
                agent_authorized: true,
                agent_valid_until_ms: 100000000,
                perp_taker_fee_bps: 4.5,
                perp_maker_fee_bps: 1.5,
                spot_taker_fee_bps: 7.0,
                spot_maker_fee_bps: 4.0,
                usdc_borrow_apr: 0.0,
                raw_account_state: "{}".to_string(),
            },
            spot: book(50.0),
            perp: book(51.0),
        };
        let fills = baseline.seed_fills().summary;
        // A deposit during REST reads must not appear as profit, even with unchanged positions.
        snapshot.account.snapshot_ts_ms = 200;
        snapshot.account.account_value_ts_ms = 200;
        snapshot.spot.recv_ts_ms = 225;
        snapshot.perp.recv_ts_ms = 225;
        let during_reads = serde_json::json!({"time":201,"delta":{"type":"deposit","usdc":"100"}});
        assert!(require_synchronized_history(&[during_reads], &snapshot).is_err());
        // A stale NAV point cannot seed a baseline after intervening capital movement.
        snapshot.account.account_value_ts_ms = 100;
        let after_old_nav = serde_json::json!({"time":150,"delta":{"type":"deposit","usdc":"100"}});
        assert!(
            require_synchronized_history(std::slice::from_ref(&after_old_nav), &snapshot).is_err()
        );
        snapshot.account.account_value_ts_ms = 210;
        assert!(require_synchronized_history(&[after_old_nav], &snapshot).is_ok());
        snapshot.account.snapshot_ts_ms = 100;
        snapshot.account.account_value_ts_ms = 100;
        assert!(write_pnl(
            &audit,
            "a",
            &snapshot,
            1099.96,
            1099.96,
            fills.clone(),
            0.0,
            &baseline
        )
        .await
        .unwrap());
        // After a 250 deposit and a 100 withdrawal, actual funding earns 0.02.
        for (ts, capital) in [(110, 250.0), (120, -100.0)] {
            audit.insert("hfc_account_ledger", &serde_json::json!({"hypercore_account":"a","event_ts_ms":ts,"hash":ts.to_string(),"net_external_usdc":capital,"fee_usd":0.0})).await.unwrap();
        }
        audit.insert("hfc_account_funding", &serde_json::json!({"hypercore_account":"a","funding_ts_ms":125,"hash":"f","actual_usdc":0.02})).await.unwrap();
        snapshot.account.snapshot_ts_ms = 100 + APR_MIN_SAMPLE_MS;
        snapshot.account.account_value_ts_ms = 100 + APR_MIN_SAMPLE_MS;
        snapshot.account.account_value_usd = 1249.98;
        assert!(
            write_pnl(&audit, "a", &snapshot, 1249.98, 1249.98, fills, 0.0, &baseline)
                .await
                .unwrap()
        );
        let rows: Vec<Value> = std::fs::read_to_string(directory.join("events.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .filter(|event| event["table"] == "hfc_live_pnl")
            .map(|event| event["row"].clone())
            .collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(number(&rows[0], "account_nav_pnl_usd"), 0.0);
        assert_eq!(number(&rows[0], "apr_eligible"), 0.0);
        assert!((number(&rows[1], "account_nav_pnl_usd") - 0.02).abs() < 1e-9);
        assert!((number(&rows[1], "net_pnl_incl_mtm_usd") - 0.02).abs() < 1e-9);
        assert!(number(&rows[1], "reconciliation_difference_usd").abs() < 1e-9);
    }

    #[test]
    fn incremental_inventory_survives_restart_and_ignores_overlap() {
        for history_size in [1_000_u64, 10_000] {
            let fill = |tid: u64| {
                serde_json::json!({
                    "coin":"@107", "px":"100", "sz":"0.01", "side":"B",
                    "time":1_000 + tid, "tid":tid, "fee":"0.001", "feeToken":"USDC"
                })
            };
            let history = (1..=history_size).map(fill).collect::<Vec<_>>();
            let mut checkpoint = FillCheckpoint::default();
            assert_eq!(checkpoint.apply(&history).unwrap(), history_size as usize);
            let mut restored: FillCheckpoint =
                serde_json::from_slice(&serde_json::to_vec(&checkpoint).unwrap()).unwrap();
            let next = (history_size - 1..=history_size + 3)
                .map(fill)
                .collect::<Vec<_>>();
            assert_eq!(restored.apply(&next).unwrap(), 3);
            assert_eq!(restored.apply(&next).unwrap(), 0);
            let full = (1..=history_size + 3).map(fill).collect::<Vec<_>>();
            let expected = summarize_fills(&Value::Array(full)).unwrap();
            assert!((restored.summary.spot.qty - expected.spot.qty).abs() < 1e-10);
            assert!((restored.summary.fees_usd - expected.fees_usd).abs() < 1e-10);
        }
    }

    #[test]
    fn pnl_separates_realized_carry_from_basis_mtm() {
        let pnl = calculate_pnl(2.0, 100.0, 100.1, 101.0, 101.0, 0.05, 0.02, 0.01);
        assert!((pnl.basis_mtm - 0.2).abs() < 1e-9);
        assert!((pnl.net_ex_mtm - 0.02).abs() < 1e-9);
        assert!((pnl.net_incl_mtm - 0.22).abs() < 1e-9);
    }

    #[test]
    fn a_late_new_fill_cannot_be_silently_skipped_by_the_checkpoint() {
        let mut checkpoint = baseline(1000.0).seed_fills();
        checkpoint.cursor = (200, 10, "HYPE".to_string());
        let late = (199, 9, "HYPE".to_string());
        assert!(require_ordered_fill(&checkpoint, &late, false).is_err());
        assert!(require_ordered_fill(&checkpoint, &late, true).is_ok());
        assert!(require_ordered_fill(&checkpoint, &(201, 11, "HYPE".to_string()), false).is_ok());
    }

    #[test]
    fn account_ledger_distinguishes_external_and_internal_flows() {
        let account = "0xAbC";
        let inbound = serde_json::json!({
            "time":1,"hash":"in","delta":{
                "type":"send","user":"0xdef","destination":"0xAbC",
                "token":"USDC","amount":"16.995","usdcValue":"16.995","fee":"1","feeToken":"USDC"
            }
        });
        let outbound = serde_json::json!({
            "time":2,"hash":"out","delta":{
                "type":"spotTransfer","user":"0xABC","destination":"0xdef",
                "token":"USDC","amount":"5","usdcValue":"5","fee":"0.1"
            }
        });
        let internal = serde_json::json!({
            "time":3,"hash":"internal","delta":{
                "type":"accountClassTransfer","usdc":"10","toPerp":true
            }
        });
        let activation = serde_json::json!({
            "time":4,"hash":"in","delta":{
                "type":"accountActivationGas","token":"USDC","amount":"1"
            }
        });
        assert_eq!(
            account_ledger_row(account, &inbound)
                .unwrap()
                .net_external_usdc,
            16.995
        );
        assert_eq!(
            account_ledger_row(account, &outbound)
                .unwrap()
                .net_external_usdc,
            -5.0
        );
        assert_eq!(
            account_ledger_row(account, &internal)
                .unwrap()
                .net_external_usdc,
            0.0
        );
        assert_eq!(account_ledger_row(account, &inbound).unwrap().fee_usd, 0.0);
        assert_eq!(
            account_ledger_row(account, &activation).unwrap().fee_usd,
            1.0
        );
        assert_eq!(
            account_ledger_row(account, &inbound)
                .unwrap()
                .hypercore_account,
            "0xabc"
        );
    }

    #[test]
    fn account_transfers_adjust_capital_and_charge_only_the_sender() {
        for event_type in ["internalTransfer", "subAccountTransfer"] {
            let transfer = |time: i64, user: &str, destination: &str, amount: &str| {
                serde_json::json!({
                    "time":time,"hash":time.to_string(),"delta":{
                        "type":event_type,"user":user,"destination":destination,
                        "usdc":amount,"fee":if event_type == "internalTransfer" {"1"} else {"0"}
                    }
                })
            };
            let incoming =
                account_ledger_row("0xAbC", &transfer(150_i64, "0xdef", "0xABC", "100")).unwrap();
            let outgoing =
                account_ledger_row("0xABC", &transfer(151_i64, "0xabc", "0xdef", "40")).unwrap();
            let self_transfer =
                account_ledger_row("0xabc", &transfer(152_i64, "0xABC", "0xabc", "20")).unwrap();
            assert_eq!(incoming.net_external_usdc, 100.0);
            assert_eq!(incoming.fee_usd, 0.0);
            assert_eq!(outgoing.net_external_usdc, -40.0);
            assert_eq!(self_transfer.net_external_usdc, 0.0);
            let capital = incoming.net_external_usdc + outgoing.net_external_usdc;
            let nav = 1000.0 + capital - outgoing.fee_usd;
            assert_eq!(baseline(1000.0).nav_pnl(nav, capital), -outgoing.fee_usd);
            assert_eq!(
                outgoing.fee_usd,
                if event_type == "internalTransfer" {
                    1.0
                } else {
                    0.0
                }
            );
        }
    }

    #[test]
    fn fill_rebates_increase_inventory_and_offset_trading_costs() {
        let fills = serde_json::json!([
            {"coin":"@107","px":"50","sz":"1000","side":"B","time":150,"tid":1,"fee":"-0.02","feeToken":"HYPE"},
            {"coin":"HYPE","px":"50","sz":"1000","side":"A","time":150,"tid":2,"fee":"0.5","feeToken":"USDC"}
        ]);
        let summary = summarize_fills(&fills).unwrap();
        assert!((summary.spot.qty - 1000.02).abs() < 1e-9);
        let stored = account_fill_row("a", &fills[0]).unwrap();
        assert_eq!(stored.fee_native, -0.02);
        assert_eq!(stored.fee_token, "HYPE");
        assert_eq!(summary.perp.qty, -1000.0);
        assert_eq!(summary.fees_usd, -0.5);
        let rebate = serde_json::json!({"fee":"-0.01","feeToken":"USDC"});
        assert_eq!(fill_fee_usd(&rebate).unwrap(), -0.01);
        let pnl = calculate_pnl(1000.0, 50.0, 50.0, 50.0, 50.0, 0.0, summary.fees_usd, 0.0);
        assert_eq!(pnl.net_incl_mtm, 0.5);
    }

    #[test]
    fn selling_rebate_dust_does_not_count_the_rebate_twice() {
        let fills = serde_json::json!([
            {"coin":"@107","px":"50","sz":"1000","side":"B","time":150,"tid":1,"fee":"0","feeToken":"USDC"},
            {"coin":"@107","px":"50","sz":"1000","side":"A","time":151,"tid":2,"fee":"-0.02","feeToken":"HYPE"},
            {"coin":"@107","px":"50","sz":"0.02","side":"A","time":152,"tid":3,"fee":"0","feeToken":"USDC"}
        ]);
        let summary = summarize_fills(&fills).unwrap();
        assert_eq!(summary.spot.qty, 0.0);
        assert_eq!(summary.spot.realized_pnl, 0.0);
        assert_eq!(summary.fees_usd, -1.0);
    }

    #[test]
    fn rebate_inventory_is_marked_at_fill_price_alongside_existing_inventory() {
        let mut inventory = Inventory {
            qty: 10.0,
            avg_px: 50.0,
            opened_ts_ms: 100,
            realized_pnl: 0.0,
        };
        let fill = serde_json::json!({"px":"60","sz":"5","side":"A",
            "time":150,"fee":"-0.02","feeToken":"HYPE"});
        apply_spot_fill(&mut inventory, &fill).unwrap();
        assert!((inventory.qty - 5.02).abs() < 1e-9);
        let pnl = inventory.realized_pnl + inventory.qty * (60.0 - inventory.avg_px)
            - fill_fee_usd(&fill).unwrap();
        assert!((pnl - 101.2).abs() < 1e-9);
    }

    #[test]
    fn hype_fee_inventory_is_removed_at_fill_price_alongside_existing_inventory() {
        let mut inventory = Inventory {
            qty: 10.0,
            avg_px: 50.0,
            opened_ts_ms: 100,
            realized_pnl: 0.0,
        };
        let fill = serde_json::json!({"px":"60","sz":"5","side":"B",
            "time":150,"fee":"0.02","feeToken":"HYPE"});
        apply_spot_fill(&mut inventory, &fill).unwrap();
        assert!((inventory.qty - 14.98).abs() < 1e-9);
        let pnl = inventory.realized_pnl + inventory.qty * (60.0 - inventory.avg_px)
            - fill_fee_usd(&fill).unwrap();
        assert!((pnl - 98.8).abs() < 1e-9);
    }

    #[test]
    fn fills_reconstruct_inventory_realized_pnl_and_hype_fees() {
        let fills = serde_json::json!([
            {"coin":"HYPE","px":"101","sz":"1","side":"B","time":4,"tid":4,"fee":"0.02","feeToken":"USDC"},
            {"coin":"@107","px":"102","sz":"1","side":"A","time":3,"tid":3,"fee":"0.01","feeToken":"USDC"},
            {"coin":"HYPE","px":"100","sz":"2","side":"A","time":2,"tid":2,"fee":"0.02","feeToken":"USDC"},
            {"coin":"@107","px":"100","sz":"2","side":"B","time":1,"tid":1,"fee":"0.01","feeToken":"HYPE"}
        ]);
        let summary = summarize_fills(&fills).unwrap();
        assert!((summary.spot.qty - 0.99).abs() < 1e-9);
        assert!((summary.perp.qty + 1.0).abs() < 1e-9);
        assert!((summary.spot.realized_pnl - 2.0).abs() < 1e-9);
        assert!((summary.perp.realized_pnl + 1.0).abs() < 1e-9);
        assert!((summary.fees_usd - 1.05).abs() < 1e-9);
    }

    #[test]
    fn incremental_fill_overlap_is_deduplicated_by_account_fact_key() {
        let fills = vec![
            serde_json::json!({"coin":"HYPE","px":"100","sz":"1","side":"A","time":2,"tid":7}),
            serde_json::json!({"coin":"HYPE","px":"100","sz":"1","side":"A","time":2,"tid":7}),
            serde_json::json!({"coin":"@107","px":"100","sz":"1","side":"B","time":2,"tid":7}),
        ];
        assert_eq!(unique_fills(&fills).len(), 2);
    }

    #[test]
    fn borrow_interest_uses_time_weighted_debt_and_rate() {
        let interest = interest_increment_usd(0, 365 * 86_400_000, 1_000.0, 2_000.0, 0.04, 0.06);
        assert!((interest - 75.0).abs() < 1e-9);
    }

    #[test]
    fn missing_pnl_is_an_error_only_when_a_position_exists() {
        assert!(require_pnl_for_position(false, 1.0, -1.0).is_err());
        assert!(require_pnl_for_position(false, 0.0, 0.0).is_ok());
        assert!(require_pnl_for_position(true, 1.0, -1.0).is_ok());
    }
}
