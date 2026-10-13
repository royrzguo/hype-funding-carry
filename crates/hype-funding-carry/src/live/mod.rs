use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex as TokioMutex;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{bail, Context, Result};
use hyperliquid_rust_sdk::{
    BaseUrl, ClientCancelRequest, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient,
    ExchangeDataStatus, ExchangeResponseStatus,
};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::api::{now_ms, InfoClient, Snapshot};
use crate::asset_domain::DomainAction;
use crate::audit::AuditClient;
use crate::config::{AccountMode, StrategyConfig};
use crate::entry_policy::evaluate_entry;
use crate::fill_notify::notify_background as notify_fill_background;
use crate::model::{basis_limited_capacity_qty, round_hype_qty, walk};
use crate::notify::{alert, resolve};
use crate::order_policy::{
    create_allowed, create_gate_bps, entry_touch_basis_bps, exit_touch_basis_bps, resting_action,
    OrderFlow, OrderPolicy, RestingAction,
};
use crate::portfolio::uniquely_identifiable_missing_leg;
use crate::risk::NakedTracker;
use crate::secret::load_private_key;
use crate::ws::{bbo_book, spawn_feed, FeedEvent, MarketBook, Touch};
use crate::EXECUTOR_VERSION;

pub use crate::fill_notify::reconcile_fills;

mod audit;
mod entry;
mod exit;
mod orders;
mod recovery;

use audit::*;
pub use entry::{open_s3, open_s3_permitted};
pub use exit::close_s3;
pub use orders::warm_exchange;
use orders::*;
pub use recovery::{cancel_all, recover, recover_live, repair};

const SPOT: &str = "HYPE/USDC";
const PERP: &str = "HYPE";
/// Pinned so a cached SDK client cannot silently submit against a relisted index.
const HYPE_PERP_ASSET_INDEX: u32 = 159;
const HYPE_SPOT_ASSET_INDEX: u32 = 10_107;
/// Hyperliquid rejects a price carrying more than five significant figures, and separately
/// caps decimals at `MAX_DECIMALS - szDecimals` (4 on the HYPE perp, 6 on HYPE spot). A fixed
/// 0.001 grid satisfies both only below $100 -- at $100.123 the price carries six figures and
/// the order is refused, which would strand the perp IOC hedge after a spot fill and block the
/// deleveraging paths. Derive the grid from the price instead.
const PRICE_SIGNIFICANT_FIGURES: i32 = 5;
const MAX_PRICE_DECIMALS: i32 = 4;
/// `px / tick` lands just under an integer for prices the exchange already quotes (52.081 gives
/// 52080.99999999999), so a bare floor drops a tick. Same guard `floor_qty` already carries.
const TICK_EPSILON: f64 = 1e-9;
const EXECUTION_WS_READY_TIMEOUT_SECS: u64 = 15;
pub const ENTRY_PERMIT_TTL_MS: i64 = 10_000;
/// REST reconciliation backs up WebSocket-driven fills, hedges and cancels. Keep its
/// cadence bounded to leave API capacity for account and risk reads.
pub(crate) const RESTING_REST_RECONCILE_MS: i64 = 15_000;
const ENTRY_PERMIT_MAX_ACCOUNT_AGE_MS: i64 = 30_000;

fn managed_resting_action(
    policy: &OrderPolicy,
    enforce_basis: bool,
    flow: OrderFlow,
    live_basis_bps: f64,
    age_ms: u64,
    at_touch: bool,
) -> RestingAction {
    if enforce_basis {
        resting_action(policy, flow, live_basis_bps, age_ms, at_touch)
    } else if age_ms >= policy.maker_hard_timeout_ms {
        RestingAction::CancelHardTimeout
    } else if age_ms >= policy.maker_soft_timeout_ms && !at_touch {
        RestingAction::CancelOffTouch
    } else {
        RestingAction::Keep
    }
}

#[derive(Debug)]
struct BasisGateNotMet {
    quoted_bps: f64,
    gate_bps: f64,
}

#[derive(Debug)]
struct AloWouldMatch {
    flow: &'static str,
}

#[derive(Debug)]
struct HedgeCapacityChanged {
    requested_usd: f64,
    available_usd: f64,
}

#[derive(Debug)]
struct EntryPermitInvalid(&'static str);

impl fmt::Display for EntryPermitInvalid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "entry permit invalid: {}", self.0)
    }
}

impl std::error::Error for EntryPermitInvalid {}

pub fn is_entry_permit_invalid(error: &anyhow::Error) -> bool {
    error.downcast_ref::<EntryPermitInvalid>().is_some()
}

#[derive(Clone, Debug)]
pub struct EntryPermit {
    snapshot: Snapshot,
    market: Option<PermittedMarketState>,
    config_hash: String,
    issued_ts_ms: i64,
    expires_ts_ms: i64,
}

impl EntryPermit {
    pub fn issue(
        snapshot: Snapshot,
        context: &ExecutionContext<'_>,
        issued_ts_ms: i64,
    ) -> Result<Self> {
        let account_age_ms = issued_ts_ms.saturating_sub(snapshot.account.snapshot_ts_ms);
        if issued_ts_ms < snapshot.account.snapshot_ts_ms
            || account_age_ms > ENTRY_PERMIT_MAX_ACCOUNT_AGE_MS
        {
            bail!("entry permit account state is stale or from the future");
        }
        validate_open_preflight_limits(
            &snapshot,
            context.delta_tolerance_usd,
            context.order_policy.maker_hard_timeout_ms + ENTRY_PERMIT_TTL_MS as u64,
        )?;
        if snapshot.account.portfolio_margin_ratio >= context.warning_pmr {
            bail!("entry permit PMR is at or above the warning threshold");
        }
        Ok(Self {
            snapshot,
            market: None,
            config_hash: context.config_hash.to_string(),
            issued_ts_ms,
            expires_ts_ms: issued_ts_ms + ENTRY_PERMIT_TTL_MS,
        })
    }

    pub fn expires_ts_ms(&self) -> i64 {
        self.expires_ts_ms
    }

    pub fn issued_ts_ms(&self) -> i64 {
        self.issued_ts_ms
    }

    pub fn state_ts_ms(&self) -> i64 {
        self.snapshot.account.snapshot_ts_ms
    }

    pub fn with_books(mut self, spot: MarketBook, perp: MarketBook) -> Self {
        self.snapshot.spot = spot.book.clone();
        self.snapshot.perp = perp.book.clone();
        self.market = Some(PermittedMarketState { spot, perp });
        self
    }

    fn validate(&self, context: &ExecutionContext<'_>, now: i64) -> Result<()> {
        if self.config_hash != context.config_hash {
            return Err(EntryPermitInvalid("config changed").into());
        }
        if now < self.issued_ts_ms || now > self.expires_ts_ms {
            return Err(EntryPermitInvalid("expired").into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ExitMode {
    Patient,
    Normal,
    Emergency,
}

#[derive(Clone, Copy)]
pub struct ExecutionContext<'a> {
    pub run_tag: &'static str,
    pub parent_intent_id: &'a str,
    pub enforce_basis: bool,
    pub force_taker_exit: bool,
    pub strategy_config: Option<&'a StrategyConfig>,
    pub order_policy: OrderPolicy,
    pub parameter_version: &'a str,
    pub config_hash: &'a str,
    pub git_sha: &'a str,
    pub account_mode: &'static str,
    pub hypercore_account: &'a str,
    pub entry_trigger_id: &'a str,
    pub entry_trigger_ts_ms: i64,
    pub entry_trigger_basis_bps: f64,
    pub audit: Option<&'a AuditClient>,
    pub delta_tolerance_usd: f64,
    pub delta_emergency_usd: f64,
    pub max_single_order_notional_usd: f64,
    pub max_order_qty_hype: Option<f64>,
    pub hedge_depth_utilization: f64,
    pub warning_pmr: f64,
    pub maker_reduce_pmr: f64,
    pub taker_reduce_pmr: f64,
    pub max_market_age_ms: u64,
    pub max_market_clock_skew_ms: u64,
    pub max_leg_timestamp_skew_ms: u64,
}

impl ExecutionContext<'static> {
    pub const fn standalone(run_tag: &'static str) -> Self {
        Self {
            run_tag,
            parent_intent_id: "",
            enforce_basis: true,
            force_taker_exit: false,
            strategy_config: None,
            order_policy: OrderPolicy::standalone_default(),
            parameter_version: "manual-probe",
            config_hash: "manual-probe",
            git_sha: "unknown",
            account_mode: "eoa",
            hypercore_account: "",
            entry_trigger_id: "",
            entry_trigger_ts_ms: 0,
            entry_trigger_basis_bps: 0.0,
            audit: None,
            delta_tolerance_usd: 10.0,
            delta_emergency_usd: 200.0,
            max_single_order_notional_usd: 500.0,
            max_order_qty_hype: None,
            hedge_depth_utilization: 0.2,
            warning_pmr: 0.65,
            maker_reduce_pmr: 0.75,
            taker_reduce_pmr: 0.80,
            max_market_age_ms: 5_000,
            max_market_clock_skew_ms: 2_000,
            max_leg_timestamp_skew_ms: 1_500,
        }
    }
}

impl<'a> ExecutionContext<'a> {
    pub fn configured(
        run_tag: &'static str,
        parent_intent_id: &'a str,
        enforce_basis: bool,
        config: &'a StrategyConfig,
        config_hash: &'a str,
        git_sha: &'a str,
        hypercore_account: &'a str,
    ) -> Self {
        Self {
            run_tag,
            parent_intent_id,
            enforce_basis,
            force_taker_exit: false,
            strategy_config: Some(config),
            order_policy: config.order_policy(),
            parameter_version: &config.parameter_version,
            config_hash,
            git_sha,
            account_mode: match config.account_mode {
                AccountMode::Eoa => "eoa",
            },
            hypercore_account,
            entry_trigger_id: "",
            entry_trigger_ts_ms: 0,
            entry_trigger_basis_bps: 0.0,
            audit: None,
            delta_tolerance_usd: config.min_trade_notional_usd,
            delta_emergency_usd: config.delta_emergency_usd,
            max_single_order_notional_usd: config.max_single_order_notional_usd,
            max_order_qty_hype: None,
            hedge_depth_utilization: config.hedge_depth_utilization,
            warning_pmr: config.risk.warning_pmr,
            maker_reduce_pmr: config.risk.maker_reduce_pmr,
            taker_reduce_pmr: config.risk.taker_reduce_pmr,
            max_market_age_ms: config.max_market_age_ms,
            max_market_clock_skew_ms: config.max_market_clock_skew_ms,
            max_leg_timestamp_skew_ms: config.max_leg_timestamp_skew_ms,
        }
    }
}

impl ExitMode {
    fn flow(self) -> Option<OrderFlow> {
        match self {
            Self::Patient => Some(OrderFlow::PatientExit),
            Self::Normal => Some(OrderFlow::NormalExit),
            Self::Emergency => None,
        }
    }

    pub fn run_tag(self) -> &'static str {
        match self {
            Self::Patient => "s3_exit_patient",
            Self::Normal => "s3_exit_normal",
            Self::Emergency => "emergency_exit",
        }
    }
}

impl fmt::Display for BasisGateNotMet {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "basis gate not met: quoted={:.3}bp gate={:.3}bp",
            self.quoted_bps, self.gate_bps
        )
    }
}

impl std::error::Error for BasisGateNotMet {}

pub fn is_basis_gate_not_met(error: &anyhow::Error) -> bool {
    error.downcast_ref::<BasisGateNotMet>().is_some()
}

impl fmt::Display for AloWouldMatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} ALO would immediately match; refresh market state before retry",
            self.flow
        )
    }
}

impl std::error::Error for AloWouldMatch {}

pub fn is_retryable_alo_reject(error: &anyhow::Error) -> bool {
    error.downcast_ref::<AloWouldMatch>().is_some()
}

impl fmt::Display for HedgeCapacityChanged {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "hedge capacity changed: requested=${:.2} available=${:.2}",
            self.requested_usd, self.available_usd
        )
    }
}

impl std::error::Error for HedgeCapacityChanged {}

pub fn is_retryable_capacity_change(error: &anyhow::Error) -> bool {
    error.downcast_ref::<HedgeCapacityChanged>().is_some()
}

#[derive(Clone)]
struct OrderSpec {
    label: &'static str,
    venue: &'static str,
    asset: &'static str,
    is_buy: bool,
    reduce_only: bool,
    tif: &'static str,
    px: f64,
    qty: f64,
    initial_queue_qty: f64,
    cloid: Uuid,
}

#[derive(Clone, Debug)]
struct OrderOutcome {
    status: String,
    oid: u64,
    filled_qty: f64,
    avg_fill_px: f64,
    request_ts_ms: i64,
    response_ts_ms: i64,
    error: String,
    raw: String,
}

fn alo_would_immediately_match(outcome: &OrderOutcome) -> bool {
    outcome.status == "error"
        && outcome
            .error
            .to_ascii_lowercase()
            .contains("post only order would have immediately matched")
}

#[derive(Serialize)]
struct HedgeLatencyRow {
    decision_id: String,
    attempt_id: String,
    path: String,
    lead_oid: u64,
    lead_tid: u64,
    lead_exchange_ts_ms: i64,
    lead_ws_recv_ts_ms: i64,
    hedge_request_ts_ms: i64,
    hedge_response_ts_ms: i64,
    hedge_oid: u64,
    requested_qty: f64,
    filled_qty: f64,
    hedge_touch_px: f64,
    hedge_vwap_px: f64,
    hedge_loss_bps: f64,
    status: String,
    error: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct DecisionRow<'a> {
    decision_id: &'a str,
    parameter_version: &'a str,
    executor_version: &'static str,
    mode: &'static str,
    run_tag: &'a str,
    action: &'a str,
    decision_ts_ms: i64,
    notional_usd: f64,
    target_hype_qty: f64,
    gate_bps: f64,
    arrival_mid_basis_bps: f64,
    quoted_basis_bps: f64,
    executable_basis_bps: f64,
    eligible: u8,
    blocked: u8,
    reason: &'a str,
    account_mode: &'a str,
    hypercore_account: &'a str,
    config_hash: &'a str,
    git_sha: &'a str,
    spot_bid_px: f64,
    spot_ask_px: f64,
    perp_bid_px: f64,
    perp_ask_px: f64,
    intent_id: &'a str,
    intent_type: &'a str,
    entry_policy_version: &'static str,
    predicted_next_funding_rate: f64,
    predicted_next_funding_bps: f64,
    next_funding_time_ms: i64,
    timing_penalty_bps: f64,
    timing_entry_eligible: u8,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct OrderRow<'a> {
    decision_id: &'a str,
    attempt_id: &'a str,
    cloid: String,
    event_ts_ms: i64,
    request_ts_ms: i64,
    response_ts_ms: i64,
    mode: &'static str,
    venue: &'a str,
    asset: &'a str,
    order_label: &'a str,
    side: &'static str,
    tif: &'a str,
    reduce_only: u8,
    requested_px: f64,
    requested_qty: f64,
    initial_queue_qty: f64,
    order_age_ms: u64,
    live_basis_bps: f64,
    remaining_queue_estimate: f64,
    cancel_reason: &'a str,
    ws_reconnect_count: u32,
    rest_reconcile_count: u32,
    status: &'a str,
    oid: u64,
    error: &'a str,
    raw_response: &'a str,
    created_ts_ms: i64,
}

fn audit_entry_stage(
    audit: &AuditClient,
    context: &ExecutionContext<'_>,
    decision_id: &str,
    stage: &str,
    event_ts_ms: i64,
    quoted_basis_bps: f64,
    detail: &str,
) {
    let trigger_ts_ms = if context.entry_trigger_ts_ms > 0 {
        context.entry_trigger_ts_ms
    } else {
        event_ts_ms
    };
    if let Err(error) = audit.enqueue_entry_stage(
        context.entry_trigger_id,
        decision_id,
        context.hypercore_account,
        context.run_tag,
        stage,
        event_ts_ms,
        trigger_ts_ms,
        quoted_basis_bps,
        detail,
    ) {
        eprintln!("audit_entry_stage_queue_failed stage={stage} error={error:#}");
    }
}

fn price_tick(px: f64) -> f64 {
    // clamp keeps a zero or non-finite price from saturating the cast into an overflow.
    let magnitude = px.abs().log10().floor().clamp(-9.0, 9.0) as i32;
    let decimals = (PRICE_SIGNIFICANT_FIGURES - 1 - magnitude).clamp(0, MAX_PRICE_DECIMALS);
    10f64.powi(-decimals)
}

fn floor_tick(px: f64) -> f64 {
    let tick = price_tick(px);
    (px / tick + TICK_EPSILON).floor() * tick
}

fn ceil_tick(px: f64) -> f64 {
    let tick = price_tick(px);
    (px / tick - TICK_EPSILON).ceil() * tick
}

fn floor_qty(qty: f64) -> f64 {
    (qty * 100.0 + 1e-9).floor() / 100.0
}

fn ceil_qty(qty: f64) -> f64 {
    (qty * 100.0 - 1e-9).ceil() / 100.0
}

/// Direction, touch price and quantity for a delta repair, or `None` while the residual is
/// still inside the tolerance the risk layer carries.
///
/// The exchange applies its minimum notional to the order we send, not to the raw residual,
/// and `NakedTracker` starts its clock on the raw residual at the same $10. Flooring to the
/// lot between those two checks opens a dead band exactly one lot wide: a residual just over
/// $10 is worth less than $10 once floored, so every hedge is rejected for being under the
/// minimum while the risk layer keeps counting it as unhedged.
///
/// Round up instead of down once flooring would drop us under the minimum. Overshoot is
/// bounded by one lot, the residual it leaves points the other way and is far inside the
/// tolerance, so the next cycle settles instead of repeating.
fn plan_delta_hedge(delta_qty: f64, best_bid: f64, best_ask: f64) -> Option<(bool, f64, f64)> {
    let (is_buy, touch_px) = if delta_qty > 0.0 {
        (false, best_bid)
    } else {
        (true, best_ask)
    };
    let residual_qty = delta_qty.abs();
    if residual_qty * touch_px < 10.0 {
        return None;
    }
    let floored = floor_qty(residual_qty);
    let qty = if floored * touch_px < 10.0 {
        ceil_qty(residual_qty)
    } else {
        floored
    };
    Some((is_buy, touch_px, qty))
}

fn activation_qty(
    requested_qty: f64,
    capacity_qty: f64,
    spot_px: f64,
    min_notional_usd: f64,
) -> Result<f64> {
    let qty = requested_qty.min(floor_qty(capacity_qty));
    if qty * spot_px + 1e-9 < min_notional_usd {
        return Err(HedgeCapacityChanged {
            requested_usd: requested_qty * spot_px,
            available_usd: capacity_qty * spot_px,
        }
        .into());
    }
    Ok(qty)
}

fn hedge_loss_bps(is_buy: bool, touch_px: f64, vwap_px: f64) -> f64 {
    if touch_px <= 0.0 || vwap_px <= 0.0 {
        return 0.0;
    }
    if is_buy {
        (vwap_px / touch_px - 1.0) * 10_000.0
    } else {
        (touch_px / vwap_px - 1.0) * 10_000.0
    }
}

fn cloid(decision_id: &str, label: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("{decision_id}:{label}").as_bytes(),
    )
}

fn cloid_hex(value: Uuid) -> String {
    format!("0x{}", value.simple())
}

fn parse_number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .or_else(|| value[key].as_str().and_then(|x| x.parse().ok()))
        .unwrap_or(0.0)
}

fn validate_confirmation(account: &str, confirmation: &str) -> Result<()> {
    if !confirmation.eq_ignore_ascii_case(account) {
        bail!("--confirm-account must exactly match HFC_ACCOUNT_ADDRESS");
    }
    Ok(())
}

#[cfg(test)]
fn validate_flat_preflight(snapshot: &Snapshot) -> Result<()> {
    require_domain(snapshot, DomainAction::IncreaseRisk, "flat preflight")?;
    if !snapshot.account.portfolio_margin_enabled {
        bail!("portfolio margin is disabled");
    }
    if !snapshot.account.agent_authorized
        || snapshot.account.agent_valid_until_ms <= snapshot.account.snapshot_ts_ms
    {
        bail!("HYPE Funding Carry agent is missing or expired");
    }
    if snapshot.account.open_orders != 0 {
        bail!("preflight requires zero open orders");
    }
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    if snapshot.account.hype_spot_qty.abs() * mid >= 10.0
        || snapshot.account.hype_perp_qty.abs() * mid >= 10.0
    {
        bail!("preflight permits only sub-$10 HYPE dust");
    }
    Ok(())
}

#[cfg(test)]
fn validate_open_preflight(snapshot: &Snapshot) -> Result<()> {
    validate_open_preflight_limits(snapshot, 10.0, 0)
}

fn validate_open_preflight_limits(
    snapshot: &Snapshot,
    delta_tolerance_usd: f64,
    min_agent_life_ms: u64,
) -> Result<()> {
    require_domain(snapshot, DomainAction::IncreaseRisk, "open preflight")?;
    if !snapshot.account.portfolio_margin_enabled {
        bail!("portfolio margin is disabled");
    }
    // A resting maker order outlives this check by up to maker_hard_timeout_ms. An agent that
    // expires while the order rests leaves a spot fill that cannot be hedged and an order
    // that cannot be cancelled, so require the agent to outlive the order it authorises.
    if !snapshot.account.agent_authorized
        || snapshot.account.agent_valid_until_ms - snapshot.account.snapshot_ts_ms
            <= min_agent_life_ms as i64
    {
        bail!(
            "HYPE Funding Carry agent is missing, expired, or expires before a resting order would"
        );
    }
    if snapshot.account.open_orders != 0 {
        bail!("preflight requires zero open orders");
    }
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    if delta_usd >= delta_tolerance_usd {
        bail!("preflight found unhedged delta ${delta_usd:.2}");
    }
    if snapshot.account.hype_spot_qty < -1e-9 || snapshot.account.hype_perp_qty > 1e-9 {
        bail!("preflight found an invalid HYPE Funding Carry position direction");
    }
    Ok(())
}

fn validate_open_activation(snapshot: &Snapshot, context: &ExecutionContext<'_>) -> Result<()> {
    validate_open_preflight_limits(
        snapshot,
        context.delta_tolerance_usd,
        context.order_policy.maker_hard_timeout_ms,
    )?;
    if snapshot.account.portfolio_margin_ratio >= context.warning_pmr {
        bail!("activation PMR is at or above the warning threshold");
    }
    let observed_ms = now_ms();
    let valid = |book: &crate::model::Book| {
        book.exchange_ts_ms > 0
            && book.recv_ts_ms > 0
            && book.exchange_ts_ms <= book.recv_ts_ms + context.max_market_clock_skew_ms as i64
            && book.recv_ts_ms <= observed_ms + context.max_market_clock_skew_ms as i64
            && observed_ms.saturating_sub(book.recv_ts_ms) as u64 <= context.max_market_age_ms
            && observed_ms.saturating_sub(book.exchange_ts_ms) as u64 <= context.max_market_age_ms
    };
    if !valid(&snapshot.spot)
        || !valid(&snapshot.perp)
        || snapshot
            .spot
            .exchange_ts_ms
            .abs_diff(snapshot.perp.exchange_ts_ms)
            > context.max_leg_timestamp_skew_ms
    {
        bail!("activation market snapshot is stale or timestamp-inconsistent");
    }
    Ok(())
}

fn validate_resting_entry_risk(
    snapshot: &Snapshot,
    active_oid_count: u32,
    warning_pmr: f64,
    delta_emergency_usd: f64,
    min_agent_life_ms: u64,
) -> Result<()> {
    require_domain(snapshot, DomainAction::IncreaseRisk, "resting entry risk")?;
    if !snapshot.account.portfolio_margin_enabled {
        bail!("portfolio margin is disabled");
    }
    if !snapshot.account.agent_authorized
        || snapshot.account.agent_valid_until_ms - snapshot.account.snapshot_ts_ms
            <= min_agent_life_ms as i64
    {
        bail!("agent is missing or expired");
    }
    if snapshot.account.portfolio_margin_ratio >= warning_pmr {
        bail!("PMR is at or above the warning threshold");
    }
    if snapshot.account.open_orders > active_oid_count {
        bail!("unexpected open order appeared while entry was resting");
    }
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    if delta_usd > delta_emergency_usd {
        bail!("material delta appeared while entry was resting");
    }
    Ok(())
}

#[derive(Clone)]
struct RestingEntryRiskPolicy {
    warning_pmr: f64,
    delta_emergency_usd: f64,
    min_agent_life_ms: u64,
}

fn spawn_entry_risk_monitor(
    info: InfoClient,
    account: String,
    agent: String,
    policy: RestingEntryRiskPolicy,
    expected_oid: u64,
) -> (
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(ENTRY_PERMIT_TTL_MS as u64)).await;
        loop {
            let result: Result<()> = async {
                let (snapshot, execution) =
                    info.snapshot_for_resting_risk(&account, &agent).await?;
                if execution
                    .open_order_oids
                    .iter()
                    .any(|oid| *oid != expected_oid)
                {
                    bail!("unexpected open order appeared while entry was resting");
                }
                let active_oid_count = execution.open_order_oids.contains(&expected_oid) as u32;
                validate_resting_entry_risk(
                    &snapshot,
                    active_oid_count,
                    policy.warning_pmr,
                    policy.delta_emergency_usd,
                    policy.min_agent_life_ms,
                )?;

                Ok(())
            }
            .await;
            if let Err(error) = result {
                let _ = sender.send(error.to_string());
                return;
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    });
    (task, receiver)
}

#[derive(Clone, Debug)]
struct PermittedMarketState {
    spot: MarketBook,
    perp: MarketBook,
}

impl PermittedMarketState {
    #[cfg(test)]
    fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        Ok(Self {
            spot: MarketBook::new(snapshot.spot.clone()),
            perp: MarketBook::new(snapshot.perp.clone()),
        })
    }

    fn observe(&mut self, event: FeedEvent) -> Result<()> {
        match event {
            FeedEvent::Disconnected(_) => Err(EntryPermitInvalid("websocket disconnected").into()),
            FeedEvent::UserFill { .. } | FeedEvent::OrderUpdate { .. } => {
                Err(EntryPermitInvalid("user state changed").into())
            }
            FeedEvent::Bbo {
                coin,
                exchange_ts_ms,
                recv_ts_ms,
                bid,
                ask,
            } if coin == "@107" || coin == "HYPE" => {
                let bbo = bbo_book(exchange_ts_ms, recv_ts_ms, bid, ask)
                    .map_err(|_| EntryPermitInvalid("market side missing or invalid"))?;
                if coin == "@107" {
                    self.spot.update(bbo, true);
                } else {
                    self.perp.update(bbo, true);
                }
                Ok(())
            }
            FeedEvent::Book { coin, book } if coin == "@107" || coin == "HYPE" => {
                if coin == "@107" {
                    self.spot.update(book, false);
                } else {
                    self.perp.update(book, false);
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn drain_permitted_feed(
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    market: &mut PermittedMarketState,
) -> Result<()> {
    loop {
        match feed.try_recv() {
            Ok(event) => market.observe(event)?,
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => return Ok(()),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                return Err(EntryPermitInvalid("websocket ended").into())
            }
        }
    }
}

fn validate_permitted_pre_submit(
    permit: &EntryPermit,
    context: &ExecutionContext<'_>,
    market: &PermittedMarketState,
    spot_order_px: f64,
    qty: f64,
    gate_bps: f64,
) -> Result<f64> {
    let observed_ms = now_ms();
    permit.validate(context, observed_ms)?;
    let valid = |book: &crate::model::Book| {
        book.exchange_ts_ms > 0
            && book.recv_ts_ms > 0
            && book.exchange_ts_ms <= book.recv_ts_ms + context.max_market_clock_skew_ms as i64
            && book.recv_ts_ms <= observed_ms + context.max_market_clock_skew_ms as i64
            && observed_ms.saturating_sub(book.recv_ts_ms) as u64 <= context.max_market_age_ms
            && observed_ms.saturating_sub(book.exchange_ts_ms) as u64 <= context.max_market_age_ms
    };
    let spot = &market.spot.book;
    let perp = &market.perp.book;
    if !valid(spot)
        || !valid(perp)
        || market
            .spot
            .book
            .exchange_ts_ms
            .abs_diff(perp.exchange_ts_ms)
            > context.max_leg_timestamp_skew_ms
    {
        return Err(EntryPermitInvalid("market state stale or inconsistent").into());
    }
    if spot.best_ask()? <= spot_order_px + TICK_EPSILON {
        return Err(AloWouldMatch { flow: "entry" }.into());
    }
    let top_basis_bps = (perp.best_bid()? / spot_order_px - 1.0) * 10_000.0;
    if context.enforce_basis && top_basis_bps + 1e-10 < gate_bps {
        return Err(BasisGateNotMet {
            quoted_bps: top_basis_bps,
            gate_bps,
        }
        .into());
    }
    let capacity = basis_limited_capacity_qty(
        perp,
        "sell",
        spot_order_px,
        context.order_policy.entry_keep_bps,
    )? * context.hedge_depth_utilization;
    if qty > capacity + 1e-9 {
        return Err(HedgeCapacityChanged {
            requested_usd: qty * spot_order_px,
            available_usd: capacity * spot_order_px,
        }
        .into());
    }
    let basis_bps = (walk(perp, "sell", qty)? / spot_order_px - 1.0) * 10_000.0;
    if context.enforce_basis && basis_bps + 1e-10 < gate_bps {
        return Err(BasisGateNotMet {
            quoted_bps: basis_bps,
            gate_bps,
        }
        .into());
    }
    Ok(basis_bps)
}

#[cfg(test)]
fn validate_open_request(notional: f64) -> Result<()> {
    if !(10.0..=500.0).contains(&notional) {
        bail!("S3 open notional must be between $10 and $500");
    }
    Ok(())
}

#[cfg(test)]
fn validate_close_preflight(snapshot: &Snapshot) -> Result<()> {
    validate_close_preflight_limits(snapshot, 10.0)
}

fn validate_close_preflight_limits(snapshot: &Snapshot, delta_tolerance_usd: f64) -> Result<()> {
    require_domain(snapshot, DomainAction::Reduce, "close preflight")?;
    if !snapshot.account.agent_authorized
        || snapshot.account.agent_valid_until_ms <= snapshot.account.snapshot_ts_ms
    {
        bail!("HYPE Funding Carry agent is missing or expired");
    }
    if snapshot.account.open_orders != 0 {
        bail!("close preflight requires zero open orders");
    }
    if snapshot.account.hype_spot_qty < -1e-9 || snapshot.account.hype_perp_qty > 1e-9 {
        bail!("close preflight found an invalid HYPE Funding Carry position direction");
    }
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    if delta_usd >= delta_tolerance_usd {
        bail!("close preflight found unhedged delta ${delta_usd:.2}");
    }
    let matched_qty = snapshot
        .account
        .hype_spot_qty
        .min(-snapshot.account.hype_perp_qty)
        .max(0.0);
    if matched_qty * mid < delta_tolerance_usd {
        bail!("close preflight found no material matched HYPE Funding Carry position");
    }
    Ok(())
}

fn validate_maker_close_agent_lifetime(
    snapshot: &Snapshot,
    hard_timeout_ms: u64,
    observed_ms: i64,
) -> Result<()> {
    // Keep authorization through the scheduled maker deadline and late-fill grace.
    let min_life_ms = hard_timeout_ms.saturating_add(EXIT_FILL_GRACE_MS);
    if !snapshot.account.agent_authorized
        || snapshot
            .account
            .agent_valid_until_ms
            .saturating_sub(observed_ms)
            <= min_life_ms as i64
    {
        bail!("agent expires before the maker exit can be released; use an immediate exit");
    }
    Ok(())
}

fn validate_emergency_close_preflight(snapshot: &Snapshot) -> Result<()> {
    require_domain(snapshot, DomainAction::Reduce, "emergency close")?;
    if !snapshot.account.agent_authorized
        || snapshot.account.agent_valid_until_ms <= snapshot.account.snapshot_ts_ms
    {
        bail!("HYPE Funding Carry agent is missing or expired");
    }
    if snapshot.account.hype_spot_qty < -1e-9 || snapshot.account.hype_perp_qty > 1e-9 {
        bail!("emergency close found an invalid HYPE Funding Carry position direction");
    }
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    if snapshot.account.hype_spot_qty * mid < 10.0 && -snapshot.account.hype_perp_qty * mid < 10.0 {
        bail!("emergency close found no material HYPE Funding Carry position");
    }
    Ok(())
}

fn require_domain(snapshot: &Snapshot, action: DomainAction, label: &str) -> Result<()> {
    if snapshot.account.asset_domain.allows(action) {
        return Ok(());
    }
    bail!(
        "{label} blocked by asset domain guard: {}",
        serde_json::to_string(&snapshot.account.asset_domain)
            .unwrap_or_else(|_| "asset-domain-serialization-failed".to_string())
    )
}

/// A post-trade domain scan failing must not turn a completed trade into an error: that
/// persists the attempt file and wedges the next recovery. One rate-limited sub-request is
/// enough to make the scan unclean. The scan still writes the domain cache, so the next cycle sees the
/// unclean state through `unknown_exposure` and halts there. The alert carries its own key:
/// the cycle-level one is resolved as soon as the cycle succeeds.
static POST_ACTION_DOMAIN_ALERTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Resolve a post-action domain alert from the next clean controller scan.
pub fn clear_post_action_domain_alert(scan: &crate::asset_domain::AssetDomainScan) {
    if scan.domain_clean
        && POST_ACTION_DOMAIN_ALERTED.swap(false, std::sync::atomic::Ordering::Relaxed)
    {
        std::thread::spawn(|| resolve("hfc_post_action_domain", "HYPE Funding Carry 成交后资产域"));
    }
}

async fn report_post_action_domain(result: Result<()>, label: &str) {
    match result {
        Ok(()) => {
            POST_ACTION_DOMAIN_ALERTED.store(false, std::sync::atomic::Ordering::Relaxed);
            resolve("hfc_post_action_domain", "HYPE Funding Carry 成交后资产域")
        }
        Err(error) => {
            POST_ACTION_DOMAIN_ALERTED.store(true, std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "{}",
                json!({"post_action_domain_failed": error.to_string()})
            );
            let message = format!("HYPE Funding Carry {label} 成交后资产域检查未通过（交易已完成，下一周期由 unknown_exposure 接手）: {error:#}");
            std::thread::spawn(move || alert("hfc_post_action_domain", &message));
        }
    }
}

async fn require_clean_post_action_domain(
    info: &InfoClient,
    account: &str,
    label: &str,
) -> Result<()> {
    let mut scan = info.asset_domain_scan(account).await;
    if !scan.domain_clean && scan.only_incomplete_reads() {
        // Nothing unexpected was seen, some venue just could not be read (a 429 burst on
        // the shared IP). One spaced retry separates that from a real violation.
        tokio::time::sleep(Duration::from_secs(2)).await;
        scan = info.asset_domain_scan(account).await;
    }
    if scan.domain_clean {
        return Ok(());
    }
    bail!(
        "{label} post-action asset domain scan failed: {}",
        serde_json::to_string(&scan)
            .unwrap_or_else(|_| "asset-domain-serialization-failed".to_string())
    )
}

async fn require_no_new_post_action_violation(
    info: &InfoClient,
    account: &str,
    before: &crate::asset_domain::AssetDomainScan,
    label: &str,
) -> Result<()> {
    let scan = info.asset_domain_scan(account).await;
    if !scan.introduced_violation_since(before) {
        return Ok(());
    }
    bail!(
        "{label} post-action asset domain scan found a new or unreadable exposure: {}",
        serde_json::to_string(&scan)
            .unwrap_or_else(|_| "asset-domain-serialization-failed".to_string())
    )
}

fn close_target_qty(snapshot: &Snapshot, notional: Option<f64>) -> Result<f64> {
    let ask = snapshot.spot.best_ask()?;
    let available = floor_qty(
        snapshot
            .account
            .hype_spot_qty
            .min(-snapshot.account.hype_perp_qty)
            .max(0.0),
    );
    let qty = match notional {
        Some(value) => {
            if value < 10.0 {
                bail!("S3 close notional must be at least $10");
            }
            if value > available * ask + 0.01 {
                bail!("S3 close notional exceeds the matched HYPE Funding Carry position");
            }
            floor_qty(value / ask).min(available)
        }
        None => available,
    };
    if qty * ask < 10.0 {
        bail!("rounded S3 close quantity is below minimum notional");
    }
    Ok(qty)
}

async fn wait_snapshot(info: &InfoClient, account: &str, agent: &str) -> Result<Snapshot> {
    tokio::time::sleep(Duration::from_millis(900)).await;
    info.snapshot(account, agent).await
}

async fn cleanup_to_flat(
    info: &InfoClient,
    exchange: &ExchangeClient,
    audit: &AuditClient,
    account: &str,
    agent: &str,
    decision_id: &str,
) -> Result<Snapshot> {
    let snapshot = info.snapshot(account, agent).await?;
    let spot_qty = floor_qty(snapshot.account.hype_spot_qty.max(0.0));
    let perp_qty = floor_qty((-snapshot.account.hype_perp_qty).max(0.0));
    let mut specs = Vec::new();
    if spot_qty * snapshot.spot.best_bid()? >= 10.0 {
        specs.push(OrderSpec {
            label: "cleanup_spot_sell",
            venue: "spot",
            asset: SPOT,
            is_buy: false,
            reduce_only: false,
            tif: "Ioc",
            px: floor_tick(snapshot.spot.best_bid()? * 0.99),
            qty: spot_qty,
            initial_queue_qty: 0.0,
            cloid: cloid(decision_id, "cleanup_spot_sell"),
        });
    }
    if perp_qty * snapshot.perp.best_ask()? >= 10.0 {
        specs.push(OrderSpec {
            label: "cleanup_perp_buy",
            venue: "perp",
            asset: PERP,
            is_buy: true,
            reduce_only: true,
            tif: "Ioc",
            px: ceil_tick(snapshot.perp.best_ask()? * 1.01),
            qty: perp_qty,
            initial_queue_qty: 0.0,
            cloid: cloid(decision_id, "cleanup_perp_buy"),
        });
    }
    let mut completed = Vec::new();
    for spec in &specs {
        match send_one(exchange, spec).await {
            Ok(outcome) => completed.push((spec, outcome)),
            Err(error) => {
                eprintln!(
                    "{}",
                    json!({"cleanup_error":error.to_string(),"test":spec.label})
                );
            }
        }
    }
    for (spec, outcome) in completed {
        audit_order_background(audit, decision_id, spec, &outcome);
    }
    let final_snapshot = wait_snapshot(info, account, agent).await?;
    let spot_residual =
        final_snapshot.account.hype_spot_qty.abs() * final_snapshot.spot.best_bid()?;
    let perp_residual =
        final_snapshot.account.hype_perp_qty.abs() * final_snapshot.perp.best_ask()?;
    if spot_residual >= 10.0 || perp_residual >= 10.0 {
        bail!("cleanup left material position: spot=${spot_residual:.2} perp=${perp_residual:.2}");
    }
    Ok(final_snapshot)
}

async fn close_matched_taker(
    info: &InfoClient,
    exchange: &ExchangeClient,
    audit: &AuditClient,
    account: &str,
    agent: &str,
    decision_id: &str,
    target_qty: f64,
) -> Result<Snapshot> {
    let snapshot = info.snapshot(account, agent).await?;
    let qty = floor_qty(
        target_qty.min(
            snapshot
                .account
                .hype_spot_qty
                .min(-snapshot.account.hype_perp_qty)
                .max(0.0),
        ),
    );
    if qty * snapshot.spot.best_bid()? < 10.0 {
        return Ok(snapshot);
    }
    let spot = OrderSpec {
        label: "risk_exit_spot_sell",
        venue: "spot",
        asset: SPOT,
        is_buy: false,
        reduce_only: false,
        tif: "Ioc",
        px: floor_tick(snapshot.spot.best_bid()? * 0.99),
        qty,
        initial_queue_qty: 0.0,
        cloid: cloid(decision_id, "risk_exit_spot_sell"),
    };
    let outcome = submit_one(exchange, audit, decision_id, &spot).await?;
    if outcome.filled_qty <= 1e-9 {
        bail!(
            "risk exit spot IOC made no progress: status={} error={}",
            outcome.status,
            outcome.error
        );
    }
    let after_spot = wait_snapshot(info, account, agent).await?;
    let reduced_spot_qty = snapshot.account.hype_spot_qty - after_spot.account.hype_spot_qty;
    if reduced_spot_qty <= 1e-9 {
        bail!("risk exit spot position did not decrease after a reported fill");
    }
    hedge_live_delta(info, exchange, audit, account, agent, decision_id, 10_000).await
}

async fn decision_fill_totals(audit: &AuditClient, decision_id: &str) -> Result<(usize, f64)> {
    let fills = audit.decision_fills(decision_id)?;
    Ok((
        fills.len(),
        fills.iter().map(|row| parse_number(row, "fee_usd")).sum(),
    ))
}

#[derive(Clone, Copy, Debug)]
struct DecisionFillMetrics {
    spot_qty: f64,
    perp_qty: f64,
    spot_vwap: f64,
    perp_vwap: f64,
    fees_usd: f64,
}

impl DecisionFillMetrics {
    fn from_signed_totals(
        spot_qty: f64,
        perp_qty: f64,
        spot_notional: f64,
        perp_notional: f64,
        fees_usd: f64,
    ) -> Self {
        let spot_qty = spot_qty.abs();
        let perp_qty = perp_qty.abs();
        Self {
            spot_qty,
            perp_qty,
            spot_vwap: if spot_qty > 0.0 {
                spot_notional.abs() / spot_qty
            } else {
                0.0
            },
            perp_vwap: if perp_qty > 0.0 {
                perp_notional.abs() / perp_qty
            } else {
                0.0
            },
            fees_usd,
        }
    }

    fn basis_bps(self) -> f64 {
        if self.spot_vwap > 0.0 && self.perp_vwap > 0.0 {
            (self.perp_vwap / self.spot_vwap - 1.0) * 10_000.0
        } else {
            0.0
        }
    }

    fn fee_bps(self) -> f64 {
        let matched_notional = self.spot_qty.min(self.perp_qty) * self.spot_vwap;
        if matched_notional > 0.0 {
            self.fees_usd / matched_notional * 10_000.0
        } else {
            0.0
        }
    }
}

async fn decision_fill_metrics(
    audit: &AuditClient,
    decision_id: &str,
) -> Result<DecisionFillMetrics> {
    let rows = audit.decision_fills(decision_id)?;
    let mut spot_qty = 0.0;
    let mut perp_qty = 0.0;
    let mut spot_notional = 0.0;
    let mut perp_notional = 0.0;
    let mut fees = 0.0;
    for row in rows {
        let qty = parse_number(&row, "qty");
        let px = parse_number(&row, "px");
        let side = row["side"].as_str().unwrap_or("");
        match row["venue"].as_str() {
            Some("spot") => {
                let signed = match side {
                    "buy" => qty,
                    "sell" => -qty,
                    _ => 0.0,
                };
                spot_qty += signed;
                spot_notional += signed * px;
            }
            Some("perp") => {
                let signed = match side {
                    "sell" => qty,
                    "buy" => -qty,
                    _ => 0.0,
                };
                perp_qty += signed;
                perp_notional += signed * px;
            }
            _ => {}
        }
        fees += parse_number(&row, "fee_usd");
    }
    Ok(DecisionFillMetrics::from_signed_totals(
        spot_qty,
        perp_qty,
        spot_notional,
        perp_notional,
        fees,
    ))
}

fn spot_position_change_qty(is_buy: bool, qty: f64, fee: f64, fee_token: &str) -> f64 {
    let hype_fee = if fee_token.eq_ignore_ascii_case("HYPE") {
        fee
    } else {
        0.0
    };
    if is_buy {
        (qty - hype_fee).max(0.0)
    } else {
        (qty + hype_fee).max(0.0)
    }
}

fn watchdog_requires_cancel(maker_open: bool, delta_qty: f64, reference_px: f64) -> bool {
    maker_open && delta_qty.abs() * reference_px >= 10.0
}

#[allow(clippy::too_many_arguments)]
async fn submit_known_perp_hedge(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    decision_id: &str,
    sequence: u32,
    is_buy: bool,
    qty: f64,
    touch_px: f64,
    via_ws: bool,
) -> Result<OrderOutcome> {
    let spec = OrderSpec {
        label: "s3_perp_delta_hedge",
        venue: "perp",
        asset: PERP,
        is_buy,
        reduce_only: is_buy,
        tif: "Ioc",
        px: if is_buy {
            ceil_tick(touch_px * 1.005)
        } else {
            floor_tick(touch_px * 0.995)
        },
        qty,
        initial_queue_qty: 0.0,
        cloid: cloid(decision_id, &format!("s3_perp_delta_hedge_{sequence}")),
    };
    if via_ws {
        confirmed_hedge_after_ws(
            submit_one_fast(exchange, audit, decision_id, &spec).await,
            submit_one(exchange, audit, decision_id, &spec),
        )
        .await
    } else {
        submit_one(exchange, audit, decision_id, &spec).await
    }
}

/// A known spot fill still needs its hedge while a WS cancel waits for acknowledgement.
/// Only a definitive unsent result allows HTTP; unknown results must be reconciled.
async fn confirmed_hedge_after_ws<T>(
    ws_result: Result<T>,
    http: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    match ws_result {
        Err(error) if error.downcast_ref::<crate::ws_post::NotSent>().is_some() => http.await,
        result => result,
    }
}

#[allow(clippy::too_many_arguments)]
async fn hedge_spot_fill_fast(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    decision_id: &str,
    sequence: u32,
    lead_oid: u64,
    lead_tid: u64,
    lead_exchange_ts_ms: i64,
    lead_ws_recv_ts_ms: i64,
    spot_is_buy: bool,
    fill_qty: f64,
    fee: f64,
    fee_token: &str,
    perp_touch_px: f64,
    pending_hedge_qty: &mut f64,
) -> Result<Option<OrderOutcome>> {
    *pending_hedge_qty += spot_position_change_qty(spot_is_buy, fill_qty, fee, fee_token);
    let requested_qty = floor_qty(*pending_hedge_qty);
    if requested_qty * perp_touch_px < 10.0 {
        return Ok(None);
    }

    let outcome = submit_known_perp_hedge(
        exchange,
        audit,
        decision_id,
        sequence,
        !spot_is_buy,
        requested_qty,
        perp_touch_px,
        true,
    )
    .await?;
    audit_hedge_latency_background(
        audit,
        HedgeLatencyRow {
            decision_id: decision_id.to_string(),
            attempt_id: decision_id.to_string(),
            path: "ws_fill_fast".to_string(),
            lead_oid,
            lead_tid,
            lead_exchange_ts_ms,
            lead_ws_recv_ts_ms,
            hedge_request_ts_ms: outcome.request_ts_ms,
            hedge_response_ts_ms: outcome.response_ts_ms,
            hedge_oid: outcome.oid,
            requested_qty,
            filled_qty: outcome.filled_qty,
            hedge_touch_px: perp_touch_px,
            hedge_vwap_px: outcome.avg_fill_px,
            hedge_loss_bps: hedge_loss_bps(!spot_is_buy, perp_touch_px, outcome.avg_fill_px),
            status: outcome.status.clone(),
            error: outcome.error.clone(),
            created_ts_ms: now_ms(),
        },
    );

    let filled_qty = outcome.filled_qty.min(requested_qty).max(0.0);
    *pending_hedge_qty = (*pending_hedge_qty - filled_qty).max(0.0);
    if outcome.status != "filled" || filled_qty + 1e-9 < requested_qty {
        bail!(
            "fast hedge incomplete status={} requested={requested_qty:.4} filled={filled_qty:.4}: {}",
            outcome.status,
            outcome.error,
        );
    }
    Ok(Some(outcome))
}

/// Keep listening briefly after a maker stops to hedge fills still in flight.
const EXIT_FILL_GRACE_MS: u64 = 1_000;

/// Stop managing a resting spot maker without dropping fills still in flight on the feed.
///
/// A cancel can race with a fill, and REST may report the order gone before its WebSocket
/// fill arrives. Keep hedging this order's fills during cancel and for a short grace after
/// the cancel returns; the caller's
/// REST repair stays as the backstop. Returns the cancel outcome when one was sent.
#[allow(clippy::too_many_arguments)]
async fn release_maker_hedging_fills(
    exchange: &ExchangeClient,
    audit: &AuditClient,
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    decision_id: &str,
    spot: &OrderSpec,
    oid: u64,
    cancel: bool,
    perp_touch: &mut Touch,
    sequence: &mut u32,
    seen_fill_tids: &mut HashSet<u64>,
    filled_gross_qty: &mut f64,
    pending_hedge_qty: &mut f64,
) -> Option<Result<OrderOutcome>> {
    let cancel_request = async {
        if cancel {
            Some(cancel_oid(exchange, audit, decision_id, spot, oid).await)
        } else {
            None
        }
    };
    tokio::pin!(cancel_request);
    let grace = tokio::time::sleep(Duration::from_millis(EXIT_FILL_GRACE_MS));
    tokio::pin!(grace);
    let mut cancel_outcome = None;
    let mut cancel_done = false;
    // No early exit on quantity: the REST watchdog also raises filled_gross_qty, so a full
    // count does not prove every WS fill has taken its hedge.
    loop {
        tokio::select! {
            biased;
            outcome = &mut cancel_request, if !cancel_done => {
                cancel_outcome = outcome;
                cancel_done = true;
                grace.as_mut().reset(
                    tokio::time::Instant::now() + Duration::from_millis(EXIT_FILL_GRACE_MS),
                );
            }
            event = feed.recv() => match event {
                Some(FeedEvent::UserFill {
                    oid: fill_oid,
                    tid,
                    qty,
                    fee,
                    fee_token,
                    exchange_ts_ms,
                    recv_ts_ms,
                    ..
                }) if fill_oid == oid => {
                    if seen_fill_tids.insert(tid) {
                        *filled_gross_qty += qty;
                        *sequence += 1;
                        if let Err(error) = hedge_spot_fill_fast(
                            exchange,
                            audit,
                            decision_id,
                            *sequence,
                            oid,
                            tid,
                            exchange_ts_ms,
                            recv_ts_ms,
                            spot.is_buy,
                            qty,
                            fee,
                            &fee_token,
                            perp_touch.px,
                            pending_hedge_qty,
                        )
                        .await
                        {
                            eprintln!(
                                "{}",
                                json!({"release_fill_hedge_failed": format!("{error:#}"), "oid": oid})
                            );
                            break;
                        }
                    }
                }
                Some(FeedEvent::Bbo { coin, bid, ask, .. }) if coin == "HYPE" => {
                    // Buying spot hedges into the perp bid; selling spot into the ask.
                    if let Some(touch) = if spot.is_buy { bid } else { ask } {
                        *perp_touch = touch;
                    }
                }
                Some(_) => {}
                None => break,
            },
            _ = &mut grace, if cancel_done => break,
        }
    }
    if !cancel_done {
        cancel_outcome = cancel_request.await;
    }
    cancel_outcome
}

async fn hedge_live_delta(
    info: &InfoClient,
    exchange: &ExchangeClient,
    audit: &AuditClient,
    account: &str,
    agent: &str,
    decision_id: &str,
    sequence: u32,
) -> Result<Snapshot> {
    let (state, perp_book) = tokio::try_join!(info.execution_state(account), info.book(PERP))?;
    let delta_qty = state.hype_spot_qty + state.hype_perp_qty;
    let Some((is_buy, touch_px, qty)) =
        plan_delta_hedge(delta_qty, perp_book.best_bid()?, perp_book.best_ask()?)
    else {
        return wait_snapshot(info, account, agent).await;
    };
    let outcome = match submit_known_perp_hedge(
        exchange,
        audit,
        decision_id,
        sequence,
        is_buy,
        qty,
        touch_px,
        false,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            eprintln!(
                "{}",
                json!({"hedge_submit_uncertain":error.to_string(),"qty":qty})
            );
            return wait_snapshot(info, account, agent).await;
        }
    };
    audit_hedge_latency_background(
        audit,
        HedgeLatencyRow {
            decision_id: decision_id.to_string(),
            attempt_id: decision_id.to_string(),
            path: "state_repair".to_string(),
            lead_oid: 0,
            lead_tid: 0,
            lead_exchange_ts_ms: 0,
            lead_ws_recv_ts_ms: 0,
            hedge_request_ts_ms: outcome.request_ts_ms,
            hedge_response_ts_ms: outcome.response_ts_ms,
            hedge_oid: outcome.oid,
            requested_qty: qty,
            filled_qty: outcome.filled_qty,
            hedge_touch_px: touch_px,
            hedge_vwap_px: outcome.avg_fill_px,
            hedge_loss_bps: hedge_loss_bps(is_buy, touch_px, outcome.avg_fill_px),
            status: outcome.status.clone(),
            error: outcome.error.clone(),
            created_ts_ms: now_ms(),
        },
    );
    if outcome.status != "filled" {
        eprintln!(
            "{}",
            json!({"hedge_not_filled":outcome.status,"error":outcome.error,"qty":qty})
        );
    }
    wait_snapshot(info, account, agent).await
}

#[cfg(test)]
include!("tests.rs");
