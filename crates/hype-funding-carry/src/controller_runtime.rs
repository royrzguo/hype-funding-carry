use anyhow::Result;
use serde::Serialize;
use uuid::Uuid;

use crate::api::{now_ms, InfoClient, Snapshot};
use crate::asset_domain::DomainAction;
use crate::audit::AuditClient;
use crate::config::{AccountMode, StrategyConfig};
use crate::controller::{desired_trade, next_intent, ControllerInput, DesiredTrade, Intent};
use crate::entry_policy::{evaluate_entry, EntryPolicyEstimate};
use crate::model::{basis_limited_capacity_qty, round_hype_qty, walk};
use crate::portfolio::{
    calculate, uniquely_identifiable_missing_leg, PortfolioInput, PortfolioState,
};
use crate::risk::{evaluate_action, ActionRiskInput, NakedTracker, RiskAction};

#[derive(Clone, Copy, Debug, Default)]
pub struct RebalancePhase {
    pub add_to_target: bool,
    pub reduce_to_target: bool,
}

const CONTROLLER_MARKET_AGE_GRACE_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum ControllerExecution {
    OpenS3 {
        notional_usd: f64,
        gate_bps: f64,
        keep_bps: f64,
    },
    CloseS3 {
        notional_usd: f64,
    },
    CancelUnexpected,
    HedgeResidual,
    PmrMakerReduce {
        notional_usd: f64,
    },
    PmrTakerReduce {
        notional_usd: f64,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct ControllerPlan {
    pub decision_id: String,
    pub decision_ts_ms: i64,
    pub parameter_version: String,
    pub config_hash: String,
    pub git_sha: String,
    pub account_mode: AccountMode,
    pub hypercore_account: String,

    pub account_value_usd: f64,
    pub operational_total_assets_usd: f64,
    pub target_strategy_equity_usd: f64,
    /// Equity the band is sized against and the denominator for reported return.
    pub core_equity_usd: f64,

    pub portfolio_margin_ratio: f64,
    pub portfolio: PortfolioState,
    pub add_to_target: bool,
    pub reduce_to_target: bool,
    pub lower_deployment_ratio: f64,
    pub target_deployment_ratio: f64,
    pub upper_deployment_ratio: f64,
    pub entry_basis_bps: f64,
    pub entry_gate_bps: f64,
    pub entry_gate_eligible: bool,
    pub entry_hedge_capacity_usd: f64,
    pub exit_hedge_capacity_usd: f64,
    pub entry_policy: EntryPolicyEstimate,
    pub normal_exit_basis_bps: f64,
    pub spot_market_age_ms: u64,
    pub perp_market_age_ms: u64,
    pub market_state_complete: bool,
    pub agent_expiry_warning: bool,
    pub risk_action: RiskAction,
    pub allow_new_risk: bool,
    pub intent: Intent,
    pub desired_trade: Option<DesiredTrade>,
    pub open_orders: u32,
    pub unknown_exposure: bool,
    pub naked_ms: u64,
    pub created_ts_ms: i64,
}

#[derive(Clone, Copy, Debug)]
pub struct ExternalState {
    pub operational_total_assets_usd: f64,
    /// HyperCore net equity used to size the matched position.
    pub target_strategy_equity_usd: f64,
    pub core_equity_usd: f64,
}

impl ExternalState {
    pub fn eoa(snapshot: &Snapshot) -> Self {
        Self {
            operational_total_assets_usd: snapshot.account.account_value_usd,
            target_strategy_equity_usd: snapshot.account.account_value_usd,
            core_equity_usd: snapshot.account.account_value_usd,
        }
    }
}

#[derive(Serialize)]
struct ConfigRow<'a> {
    config_hash: &'a str,
    parameter_version: &'a str,
    git_sha: &'a str,
    account_mode: &'a str,
    hypercore_account: &'a str,

    canonical_config_json: &'a str,
    loaded_ts_ms: i64,
    activated_ts_ms: i64,
    valid: u8,
    error: &'a str,
}

#[derive(Serialize)]
struct DecisionRow<'a> {
    decision_id: &'a str,
    parameter_version: &'a str,
    executor_version: &'static str,
    mode: &'static str,
    run_tag: &'static str,
    action: &'a str,
    decision_ts_ms: i64,
    notional_usd: f64,
    target_hype_qty: f64,
    gate_bps: f64,
    arrival_mid_basis_bps: f64,
    quoted_basis_bps: f64,
    executable_basis_bps: f64,
    required_quote_basis_bps: f64,
    keep_quote_basis_bps: f64,
    spot_maker_fee_bps: f64,
    spot_taker_fee_bps: f64,
    perp_maker_fee_bps: f64,
    perp_taker_fee_bps: f64,
    usdc_borrow_apr: f64,
    entry_policy_version: &'static str,
    predicted_next_funding_rate: f64,
    predicted_next_funding_bps: f64,
    next_funding_time_ms: i64,
    timing_penalty_bps: f64,
    timing_entry_eligible: u8,
    eligible: u8,
    blocked: u8,
    reason: &'a str,
    spot_bid_px: f64,
    spot_ask_px: f64,
    perp_bid_px: f64,
    perp_ask_px: f64,
    account_mode: &'a str,
    hypercore_account: &'a str,

    config_hash: &'a str,
    git_sha: &'a str,
    intent_id: &'a str,

    intent_type: &'a str,
    target_strategy_equity_usd: f64,
    core_equity_usd: f64,
    matched_notional_usd: f64,
    deployment_ratio: f64,
    target_deployment_ratio: f64,
    lower_deployment_ratio: f64,
    upper_deployment_ratio: f64,

    risk_action: &'a str,
    raw_controller_state: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct RiskRow<'a> {
    snapshot_ts_ms: i64,
    mode: &'static str,
    account_value_usd: f64,
    portfolio_margin_ratio: f64,
    liquidation_px: f64,
    available_after_maintenance_usdc: f64,
    usdc_total: f64,
    usdc_borrowed: f64,
    hype_spot_qty: f64,
    hype_perp_qty: f64,
    hype_mid_px: f64,
    residual_delta_usd: f64,
    naked_ms: u64,
    funding_24h_bps: f64,
    open_orders: u32,
    agent_authorized: u8,
    agent_valid_until_ms: i64,
    risk_level: &'a str,
    allow_new_entry: u8,
    kill_switch: u8,
    reasons: Vec<&'a str>,
    raw_account_state: &'a str,
    created_ts_ms: i64,
    account_mode: &'a str,
    hypercore_account: &'a str,

    parameter_version: &'a str,
    config_hash: &'a str,
    git_sha: &'a str,
    intent_id: &'a str,

    intent_type: &'a str,
    target_strategy_equity_usd: f64,
    core_equity_usd: f64,
    matched_notional_usd: f64,
    deployment_ratio: f64,
    target_deployment_ratio: f64,
    lower_deployment_ratio: f64,
    upper_deployment_ratio: f64,
}

/// A band rebalance smaller than this share of one clip is not worth the fixed cost of an
/// entry: the request burst it generates is the same whatever the size.
const BAND_TRADE_FLOOR_FRACTION: f64 = 0.1;

/// Baked in by build.rs. Reading HEAD at runtime described the working tree rather than the
/// running binary, so a pull that had not been restarted made audit rows claim the new commit.
pub fn git_sha() -> String {
    env!("GIT_SHA").to_string()
}

pub fn bounded_controller_execution(
    plan: &ControllerPlan,
    config: &StrategyConfig,
    max_notional_usd: f64,
) -> Result<Option<ControllerExecution>> {
    let configured_cap = config.max_single_order_notional_usd;
    if max_notional_usd + 1e-10 < config.min_trade_notional_usd {
        anyhow::bail!("live max notional is below the configured minimum");
    }
    let is_open = match plan.intent {
        Intent::Idle => return Ok(None),
        Intent::BandAdd { .. } => true,
        Intent::BandReduce { .. } => {
            if plan.normal_exit_basis_bps > config.normal_exit_create_bps + 1e-10 {
                return Ok(None);
            }
            false
        }
        Intent::CancelUnexpected => return Ok(Some(ControllerExecution::CancelUnexpected)),
        Intent::HedgeResidual { .. } => return Ok(Some(ControllerExecution::HedgeResidual)),
        Intent::PmrMakerReduce { .. } => {
            return Ok(Some(ControllerExecution::PmrMakerReduce {
                notional_usd: plan.portfolio.matched_notional_usd.min(configured_cap),
            }))
        }
        Intent::PmrTakerReduce { .. } => {
            return Ok(Some(ControllerExecution::PmrTakerReduce {
                notional_usd: plan.portfolio.matched_notional_usd.min(configured_cap),
            }))
        }
        Intent::ManualFullClose => anyhow::bail!("manual full close is not a controller action"),
        Intent::ReconcileUnknown => return Ok(None),
    };
    let market_capacity = if is_open {
        plan.entry_hedge_capacity_usd
    } else {
        plan.exit_hedge_capacity_usd
    };
    let bounded = plan
        .intent
        .amount_usd()
        .min(max_notional_usd)
        .min(configured_cap)
        .min(market_capacity);
    if bounded + 1e-10 < config.min_trade_notional_usd {
        return Ok(None);
    }
    if is_open {
        Ok(Some(ControllerExecution::OpenS3 {
            notional_usd: bounded,
            gate_bps: plan.entry_gate_bps,
            keep_bps: plan.entry_policy.keep_gate_bps,
        }))
    } else {
        Ok(Some(ControllerExecution::CloseS3 {
            notional_usd: bounded,
        }))
    }
}

fn account_mode(mode: AccountMode) -> &'static str {
    match mode {
        AccountMode::Eoa => "eoa",
    }
}

fn executable_bases(
    snapshot: &Snapshot,
    entry_notional_usd: f64,
    exit_notional_usd: f64,
) -> Result<(f64, f64)> {
    let reference = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let entry_qty = if entry_notional_usd >= 10.0 {
        round_hype_qty(entry_notional_usd, reference)
    } else {
        0.01
    };
    let exit_qty = if exit_notional_usd >= 10.0 {
        round_hype_qty(exit_notional_usd, reference)
    } else {
        0.01
    };
    let entry =
        (walk(&snapshot.perp, "sell", entry_qty)? / snapshot.spot.best_bid()? - 1.0) * 10_000.0;
    let exit =
        (walk(&snapshot.perp, "buy", exit_qty)? / snapshot.spot.best_ask()? - 1.0) * 10_000.0;
    Ok((entry, exit))
}

pub fn build_plan(
    config: &StrategyConfig,
    snapshot: &Snapshot,
    account: &str,
    git_sha: &str,
    naked_ms: u64,
) -> Result<ControllerPlan> {
    build_plan_with_external(
        config,
        snapshot,
        account,
        git_sha,
        naked_ms,
        ExternalState::eoa(snapshot),
    )
}

pub fn build_plan_with_external(
    config: &StrategyConfig,
    snapshot: &Snapshot,
    account: &str,
    git_sha: &str,
    naked_ms: u64,
    external: ExternalState,
) -> Result<ControllerPlan> {
    build_plan_with_external_rebalance(
        config,
        snapshot,
        account,
        git_sha,
        naked_ms,
        external,
        RebalancePhase::default(),
    )
}

pub fn build_plan_with_external_rebalance(
    config: &StrategyConfig,
    snapshot: &Snapshot,
    account: &str,
    git_sha: &str,
    naked_ms: u64,
    external: ExternalState,
    rebalance: RebalancePhase,
) -> Result<ControllerPlan> {
    config.validate()?;
    if !external.operational_total_assets_usd.is_finite()
        || external.operational_total_assets_usd <= 0.0
        || !external.target_strategy_equity_usd.is_finite()
        || external.target_strategy_equity_usd <= 0.0
        || !external.core_equity_usd.is_finite()
        || external.core_equity_usd <= 0.0
    {
        anyhow::bail!("invalid external controller state");
    }
    let observed_ms = now_ms();
    let spot_market_age_ms = observed_ms.saturating_sub(snapshot.spot.exchange_ts_ms) as u64;
    let perp_market_age_ms = observed_ms.saturating_sub(snapshot.perp.exchange_ts_ms) as u64;
    let timestamp_valid = |book: &crate::model::Book| {
        book.exchange_ts_ms > 0
            && book.recv_ts_ms > 0
            && book.exchange_ts_ms <= book.recv_ts_ms + config.max_market_clock_skew_ms as i64
            && book.recv_ts_ms <= observed_ms + config.max_market_clock_skew_ms as i64
            && observed_ms.saturating_sub(book.recv_ts_ms) as u64
                <= config.max_market_age_ms + CONTROLLER_MARKET_AGE_GRACE_MS
            && observed_ms.saturating_sub(book.exchange_ts_ms) as u64
                <= config.max_market_age_ms + CONTROLLER_MARKET_AGE_GRACE_MS
    };
    let leg_skew_ms = snapshot
        .spot
        .exchange_ts_ms
        .abs_diff(snapshot.perp.exchange_ts_ms);
    let market_state_complete = timestamp_valid(&snapshot.spot)
        && timestamp_valid(&snapshot.perp)
        && leg_skew_ms <= config.max_leg_timestamp_skew_ms;
    let agent_expiry_warning = snapshot.account.agent_authorized
        && snapshot.account.agent_valid_until_ms > snapshot.account.snapshot_ts_ms
        && snapshot.account.agent_valid_until_ms - snapshot.account.snapshot_ts_ms
            <= config.agent_expiry_warning_ms as i64;
    let portfolio = calculate(&PortfolioInput {
        hype_spot_qty: snapshot.account.hype_spot_qty,
        hype_perp_qty: snapshot.account.hype_perp_qty,
        spot_bid_px: snapshot.spot.best_bid()?,
        spot_ask_px: snapshot.spot.best_ask()?,
        perp_bid_px: snapshot.perp.best_bid()?,
        perp_ask_px: snapshot.perp.best_ask()?,
        sizing_equity_usd: external.core_equity_usd,
        target_deployment_ratio: config.target_deployment_ratio,
    })?;
    let entry_hedge_capacity_usd = (snapshot.perp.bid_sz.first().copied().unwrap_or(0.0)
        * config.hedge_depth_utilization
        * snapshot.spot.best_bid()?)
    .min(config.max_single_order_notional_usd);
    let exit_hedge_capacity_usd = (basis_limited_capacity_qty(
        &snapshot.perp,
        "buy",
        snapshot.spot.best_ask()?,
        config.normal_exit_keep_bps,
    )? * config.hedge_depth_utilization
        * snapshot.spot.best_ask()?)
    .min(config.max_single_order_notional_usd);
    let entry_candidate = portfolio
        .target_delta_usd
        .max(0.0)
        .min(entry_hedge_capacity_usd)
        .min(config.max_single_order_notional_usd);
    let exit_candidate = (-portfolio.target_delta_usd)
        .max(0.0)
        .min(exit_hedge_capacity_usd)
        .min(config.max_single_order_notional_usd);
    let (entry_basis_bps, normal_exit_basis_bps) =
        executable_bases(snapshot, entry_candidate, exit_candidate)?;
    let entry_policy = evaluate_entry(config, &snapshot.account, entry_basis_bps);
    let entry_gate_bps = entry_policy.create_gate_bps;
    let entry_gate_eligible = entry_policy.eligible;
    let unique_hedge = uniquely_identifiable_missing_leg(
        snapshot.account.hype_spot_qty,
        snapshot.account.hype_perp_qty,
        portfolio.reference_hype_px,
        config.min_trade_notional_usd,
        snapshot.account.open_orders == 0,
        snapshot.account.asset_domain.allows(DomainAction::Hedge {
            uniquely_identified: true,
        }),
    );
    let risk_action = evaluate_action(
        &ActionRiskInput {
            portfolio_margin_enabled: snapshot.account.portfolio_margin_enabled,
            portfolio_margin_ratio: snapshot.account.portfolio_margin_ratio,
            residual_delta_usd: portfolio.residual_delta_usd,
            delta_emergency_usd: config.delta_emergency_usd,
            naked_ms,
            naked_timeout_ms: config.naked_timeout_ms,
            agent_authorized: snapshot.account.agent_authorized,
            agent_valid_until_ms: snapshot.account.agent_valid_until_ms,
            now_ms: snapshot.account.snapshot_ts_ms,
            config_valid: true,
            market_state_complete,
            unknown_exposure: snapshot.account.unknown_exposure,
        },
        &config.risk,
    );
    let allow_new_risk = risk_action.allows_new_risk();
    let intent = next_intent(&ControllerInput {
        risk_action,
        state_unknown: false,
        unique_hedge,
        hedge_execution_available: snapshot.account.agent_authorized,
        unexpected_orders: snapshot.account.open_orders > 0,
        manual_full_close: false,

        deployment_ratio: Some(portfolio.deployment_ratio),
        add_to_target: rebalance.add_to_target,
        reduce_to_target: rebalance.reduce_to_target,
        matched_notional_usd: portfolio.matched_notional_usd,
        sizing_equity_usd: external.core_equity_usd,
        band: config.deployment_band,
        entry_gate_eligible,

        dust_usd: config.min_trade_notional_usd,
        min_band_trade_usd: config.max_single_order_notional_usd * BAND_TRADE_FLOOR_FRACTION,
    });
    let desired_trade = desired_trade(intent, config.min_trade_notional_usd);
    let config_hash = config.config_hash()?;
    let (lower_deployment_ratio, target_deployment_ratio, upper_deployment_ratio) = config
        .deployment_band
        .map(|band| (band.lower, band.target, band.upper))
        .unwrap_or((0.0, config.target_deployment_ratio, 0.0));
    let add_to_target = config.deployment_band.is_some_and(|band| {
        portfolio.deployment_ratio < band.lower
            || (rebalance.add_to_target && portfolio.deployment_ratio < band.target)
    });
    let reduce_to_target = config.deployment_band.is_some_and(|band| {
        portfolio.deployment_ratio > band.upper
            || (rebalance.reduce_to_target && portfolio.deployment_ratio > band.target)
    });
    let id_key = format!(
        "{}:{}:{}:{}:{}:{}:{:.8}:{:.8}",
        config_hash,
        account.to_lowercase(),
        snapshot.account.snapshot_ts_ms,
        intent.as_str(),
        add_to_target,
        reduce_to_target,
        intent.amount_usd(),
        external.target_strategy_equity_usd,
    );
    Ok(ControllerPlan {
        decision_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, id_key.as_bytes()).to_string(),
        decision_ts_ms: snapshot.account.snapshot_ts_ms,
        parameter_version: config.parameter_version.clone(),
        config_hash,
        git_sha: git_sha.to_string(),
        account_mode: config.account_mode,
        hypercore_account: account.to_string(),

        account_value_usd: snapshot.account.account_value_usd,
        operational_total_assets_usd: external.operational_total_assets_usd,
        target_strategy_equity_usd: external.target_strategy_equity_usd,
        core_equity_usd: external.core_equity_usd,

        portfolio_margin_ratio: snapshot.account.portfolio_margin_ratio,
        portfolio,
        add_to_target,
        reduce_to_target,
        lower_deployment_ratio,
        target_deployment_ratio,
        upper_deployment_ratio,
        entry_basis_bps,
        entry_gate_bps,
        entry_gate_eligible,
        entry_hedge_capacity_usd,
        exit_hedge_capacity_usd,
        entry_policy,
        normal_exit_basis_bps,
        spot_market_age_ms,
        perp_market_age_ms,
        market_state_complete,
        agent_expiry_warning,
        risk_action,
        allow_new_risk,
        intent,
        desired_trade,
        open_orders: snapshot.account.open_orders,
        unknown_exposure: snapshot.account.unknown_exposure,
        naked_ms,
        created_ts_ms: now_ms(),
    })
}

pub async fn fetch_plan(
    info: &InfoClient,
    config: &StrategyConfig,
    account: &str,
    agent: &str,
    git_sha: &str,
    naked_tracker: &mut NakedTracker,
    rebalance: RebalancePhase,
) -> Result<(ControllerPlan, Snapshot)> {
    let snapshot = info.snapshot(account, agent).await?;
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    let naked_ms = naked_tracker.observe_with_threshold(
        snapshot.account.snapshot_ts_ms,
        delta_usd,
        config.min_trade_notional_usd,
    );
    let plan = build_plan_with_external_rebalance(
        config,
        &snapshot,
        account,
        git_sha,
        naked_ms,
        ExternalState::eoa(&snapshot),
        rebalance,
    )?;
    Ok((plan, snapshot))
}

pub async fn audit_config(
    audit: &AuditClient,
    config: &StrategyConfig,
    account: &str,
    git_sha: &str,
    background: bool,
) -> Result<()> {
    let canonical = config.canonical_json()?;
    let hash = config.config_hash()?;
    let ts = now_ms();
    let row = ConfigRow {
        config_hash: &hash,
        parameter_version: &config.parameter_version,
        git_sha,
        account_mode: account_mode(config.account_mode),
        hypercore_account: account,

        canonical_config_json: &canonical,
        loaded_ts_ms: ts,
        activated_ts_ms: ts,
        valid: 1,
        error: "",
    };
    if background {
        audit.enqueue_best_effort("hfc_exec_config", &row)?;
    } else {
        audit.insert_best_effort("hfc_exec_config", &row).await;
    }
    Ok(())
}

pub async fn audit_plan(
    audit: &AuditClient,
    plan: &ControllerPlan,
    snapshot: &Snapshot,
    mode: &'static str,
) {
    let mid_basis = (((snapshot.perp.best_bid().unwrap_or(0.0)
        + snapshot.perp.best_ask().unwrap_or(0.0))
        / 2.0)
        / ((snapshot.spot.best_bid().unwrap_or(1.0) + snapshot.spot.best_ask().unwrap_or(1.0))
            / 2.0)
        - 1.0)
        * 10_000.0;
    let raw_controller_state = serde_json::to_string(plan).unwrap_or_else(|_| "{}".to_string());
    let reason = if plan.risk_action != RiskAction::Normal {
        plan.risk_action.as_str()
    } else if plan.intent == Intent::Idle && !plan.entry_policy.timing_eligible {
        "entry_timing_gate"
    } else {
        plan.intent.as_str()
    };
    let eligible = matches!(
        plan.intent,
        Intent::BandAdd { .. }
            | Intent::BandReduce { .. }
            | Intent::PmrMakerReduce { .. }
            | Intent::PmrTakerReduce { .. }
            | Intent::HedgeResidual { .. }
    );
    let row = DecisionRow {
        decision_id: &plan.decision_id,
        parameter_version: &plan.parameter_version,
        executor_version: crate::EXECUTOR_VERSION,
        mode,
        run_tag: mode,
        action: plan.intent.as_str(),
        decision_ts_ms: plan.decision_ts_ms,
        notional_usd: plan.intent.amount_usd(),
        target_hype_qty: plan.intent.amount_usd() / plan.portfolio.reference_hype_px,
        gate_bps: plan.entry_gate_bps,
        arrival_mid_basis_bps: mid_basis,
        quoted_basis_bps: plan.entry_basis_bps,
        executable_basis_bps: plan.entry_basis_bps,
        required_quote_basis_bps: plan.entry_policy.create_gate_bps,
        keep_quote_basis_bps: plan.entry_policy.keep_gate_bps,
        spot_maker_fee_bps: snapshot.account.spot_maker_fee_bps,
        spot_taker_fee_bps: snapshot.account.spot_taker_fee_bps,
        perp_maker_fee_bps: snapshot.account.perp_maker_fee_bps,
        perp_taker_fee_bps: snapshot.account.perp_taker_fee_bps,
        usdc_borrow_apr: snapshot.account.usdc_borrow_apr,
        entry_policy_version: "timing_v2",
        predicted_next_funding_rate: plan.entry_policy.predicted_next_funding_rate,
        predicted_next_funding_bps: plan.entry_policy.predicted_next_funding_bps,
        next_funding_time_ms: plan.entry_policy.next_funding_time_ms,
        timing_penalty_bps: plan.entry_policy.timing_penalty_bps,
        timing_entry_eligible: plan.entry_policy.timing_eligible as u8,
        eligible: eligible as u8,
        blocked: matches!(
            plan.risk_action,
            RiskAction::HaltUnknown | RiskAction::HedgeOnly
        ) as u8,
        reason,
        spot_bid_px: snapshot.spot.best_bid().unwrap_or(0.0),
        spot_ask_px: snapshot.spot.best_ask().unwrap_or(0.0),
        perp_bid_px: snapshot.perp.best_bid().unwrap_or(0.0),
        perp_ask_px: snapshot.perp.best_ask().unwrap_or(0.0),
        account_mode: account_mode(plan.account_mode),
        hypercore_account: &plan.hypercore_account,

        config_hash: &plan.config_hash,
        git_sha: &plan.git_sha,
        intent_id: &plan.decision_id,

        intent_type: plan.intent.as_str(),
        target_strategy_equity_usd: plan.target_strategy_equity_usd,
        core_equity_usd: plan.core_equity_usd,
        matched_notional_usd: plan.portfolio.matched_notional_usd,
        deployment_ratio: plan.portfolio.deployment_ratio,
        target_deployment_ratio: plan.target_deployment_ratio,
        lower_deployment_ratio: plan.lower_deployment_ratio,
        upper_deployment_ratio: plan.upper_deployment_ratio,

        risk_action: plan.risk_action.as_str(),
        raw_controller_state,
        created_ts_ms: plan.created_ts_ms,
    };
    if mode == "controller_live" {
        if let Err(error) = audit.enqueue_best_effort("hfc_exec_decision", &row) {
            eprintln!("controller_decision_audit_queue_failed={error:#}");
        }
    } else {
        audit.insert_best_effort("hfc_exec_decision", &row).await;
    }

    let risk_reason = plan.risk_action.as_str();
    let risk_row = RiskRow {
        snapshot_ts_ms: plan.decision_ts_ms,
        mode,
        account_value_usd: plan.account_value_usd,
        portfolio_margin_ratio: plan.portfolio_margin_ratio,
        liquidation_px: snapshot.account.liquidation_px,
        available_after_maintenance_usdc: snapshot.account.available_after_maintenance_usdc,
        usdc_total: snapshot.account.usdc_total,
        usdc_borrowed: snapshot.account.usdc_borrowed,
        hype_spot_qty: snapshot.account.hype_spot_qty,
        hype_perp_qty: snapshot.account.hype_perp_qty,
        hype_mid_px: plan.portfolio.reference_hype_px,
        residual_delta_usd: plan.portfolio.residual_delta_usd,
        naked_ms: plan.naked_ms,
        funding_24h_bps: snapshot.account.funding_24h_bps,
        open_orders: plan.open_orders,
        agent_authorized: snapshot.account.agent_authorized as u8,
        agent_valid_until_ms: snapshot.account.agent_valid_until_ms,
        risk_level: risk_reason,
        allow_new_entry: plan.allow_new_risk as u8,
        kill_switch: matches!(
            plan.risk_action,
            RiskAction::HaltUnknown | RiskAction::HedgeOnly
        ) as u8,
        reasons: {
            let mut reasons = if plan.risk_action == RiskAction::Normal {
                Vec::new()
            } else {
                vec![risk_reason]
            };
            if snapshot.account.funding_24h_all_negative {
                reasons.push("funding_24h_all_negative");
            }
            reasons
        },
        raw_account_state: &snapshot.account.raw_account_state,
        created_ts_ms: plan.created_ts_ms,
        account_mode: account_mode(plan.account_mode),
        hypercore_account: &plan.hypercore_account,

        parameter_version: &plan.parameter_version,
        config_hash: &plan.config_hash,
        git_sha: &plan.git_sha,
        intent_id: &plan.decision_id,

        intent_type: plan.intent.as_str(),
        target_strategy_equity_usd: plan.target_strategy_equity_usd,
        core_equity_usd: plan.core_equity_usd,
        matched_notional_usd: plan.portfolio.matched_notional_usd,
        deployment_ratio: plan.portfolio.deployment_ratio,
        target_deployment_ratio: plan.target_deployment_ratio,
        lower_deployment_ratio: plan.lower_deployment_ratio,
        upper_deployment_ratio: plan.upper_deployment_ratio,
    };
    if mode == "controller_live" {
        if let Err(error) = audit.enqueue_best_effort("hfc_exec_risk", &risk_row) {
            eprintln!("controller_risk_audit_queue_failed={error:#}");
        }
    } else {
        audit.insert_best_effort("hfc_exec_risk", &risk_row).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::AccountSnapshot;
    use crate::asset_domain::AssetDomainScan;
    use crate::model::Book;

    fn book(bid: f64, ask: f64) -> crate::model::Book {
        Book {
            exchange_ts_ms: now_ms(),
            recv_ts_ms: now_ms(),
            bid_px: vec![bid],
            bid_sz: vec![1_000.0],
            ask_px: vec![ask],
            ask_sz: vec![1_000.0],
        }
    }

    fn snapshot() -> Snapshot {
        Snapshot {
            account: AccountSnapshot {
                snapshot_ts_ms: 1,
                account_value_usd: 10_100.0,
                account_value_ts_ms: 1,
                portfolio_margin_enabled: true,
                portfolio_margin_ratio: 0.01,
                available_after_maintenance_usdc: 9_900.0,
                usdc_total: 9_950.0,
                usdc_borrowed: 0.0,
                hype_spot_qty: 2.0,
                hype_perp_qty: -2.0,
                liquidation_px: 0.0,
                funding_24h_bps: 2.8,
                funding_24h_all_negative: false,
                predicted_next_funding_rate: 0.0000125,
                next_funding_time_ms: 3_600_000,
                open_orders: 0,
                unknown_exposure: false,
                asset_domain: AssetDomainScan {
                    domain_clean: true,
                    ..AssetDomainScan::default()
                },
                agent_authorized: true,
                agent_valid_until_ms: 100_000,
                perp_taker_fee_bps: 4.5,
                perp_maker_fee_bps: 1.5,
                spot_taker_fee_bps: 7.0,
                spot_maker_fee_bps: 4.0,
                usdc_borrow_apr: 0.05,
                raw_account_state: "{}".to_string(),
            },
            spot: book(50.0, 50.01),
            perp: book(50.02, 50.03),
        }
    }

    #[test]
    fn git_sha_is_baked_in_and_well_formed() {
        let sha = git_sha();
        assert!(!sha.is_empty(), "build script injected an empty sha");
        let core = sha.strip_suffix("-dirty").unwrap_or(&sha);
        assert!(
            core == "unknown" || (core.len() == 40 && core.chars().all(|c| c.is_ascii_hexdigit())),
            "unexpected git sha {sha}"
        );
    }

    #[test]
    fn eoa_fixture_emits_full_band_delta() {
        let plan = build_plan(
            &StrategyConfig::eoa_fixture(),
            &snapshot(),
            "0xabc",
            "sha",
            0,
        )
        .unwrap();
        assert_eq!(
            plan.intent,
            Intent::BandAdd {
                amount_usd: 13_029.97
            }
        );
        assert_eq!(plan.account_mode, AccountMode::Eoa);
    }

    #[test]
    fn negative_funding_and_expensive_borrow_do_not_veto_band_adds() {
        let mut config = StrategyConfig::eoa_fixture();
        config.target_deployment_ratio = 2.0;
        config.deployment_band.as_mut().unwrap().target = 2.0;
        config.deployment_band.as_mut().unwrap().upper = 2.5;
        let mut state = snapshot();
        state.account.funding_24h_all_negative = true;
        state.account.funding_24h_bps = -5.0;
        state.account.predicted_next_funding_rate = -0.00002;
        state.account.usdc_borrow_apr = 0.5;

        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.risk_action, RiskAction::Normal);
        assert!(plan.allow_new_risk);
        assert!(matches!(plan.intent, Intent::BandAdd { .. }));
        assert!((plan.entry_gate_bps - 0.2).abs() < 1e-12);
        assert!(matches!(
            bounded_controller_execution(&plan, &config, 500.0).unwrap(),
            Some(ControllerExecution::OpenS3 { .. })
        ));

        state.perp = book(49.99, 50.0);
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.intent, Intent::Idle);
        assert!(!plan.entry_gate_eligible);

        state.perp = book(50.02, 50.03);
        state.account.portfolio_margin_ratio = config.risk.warning_pmr;
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.risk_action, RiskAction::StopNewRisk);
        assert_eq!(plan.intent, Intent::Idle);
    }

    #[test]
    fn latched_plan_keeps_adding_between_lower_and_target() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.account.hype_spot_qty = 220.0;
        state.account.hype_perp_qty = -220.0;
        let external = ExternalState::eoa(&state);

        let idle = build_plan_with_external(&config, &state, "0xabc", "sha", 0, external).unwrap();
        assert_eq!(idle.intent, Intent::Idle);

        let active = build_plan_with_external_rebalance(
            &config,
            &state,
            "0xabc",
            "sha",
            0,
            external,
            RebalancePhase {
                add_to_target: true,
                reduce_to_target: false,
            },
        )
        .unwrap();
        assert!(active.add_to_target);
        assert!(matches!(active.intent, Intent::BandAdd { .. }));
    }

    #[test]
    fn latched_plan_keeps_reducing_between_upper_and_target() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.account.hype_spot_qty = 300.0;
        state.account.hype_perp_qty = -300.0;
        let external = ExternalState::eoa(&state);

        let idle = build_plan_with_external(&config, &state, "0xabc", "sha", 0, external).unwrap();
        assert_eq!(idle.intent, Intent::Idle);

        let active = build_plan_with_external_rebalance(
            &config,
            &state,
            "0xabc",
            "sha",
            0,
            external,
            RebalancePhase {
                add_to_target: false,
                reduce_to_target: true,
            },
        )
        .unwrap();
        assert!(active.reduce_to_target);
        assert!(matches!(active.intent, Intent::BandReduce { .. }));
    }

    #[test]
    fn entry_capacity_uses_fresh_perp_bid_one() {
        let mut config = StrategyConfig::eoa_fixture();
        config.max_single_order_notional_usd = 10_000.0;
        config.hedge_depth_utilization = 0.2;
        let mut state = snapshot();
        state.perp.bid_px = vec![50.02, 50.01];
        state.perp.bid_sz = vec![10.0, 100_000.0];

        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert!((plan.entry_hedge_capacity_usd - 100.0).abs() < 1e-9);
    }

    #[test]
    fn core_equity_at_or_below_zero_is_rejected_with_a_named_error() {
        let state = snapshot();
        let mut external = ExternalState::eoa(&state);
        external.core_equity_usd = 0.0;
        let error = build_plan_with_external(
            &StrategyConfig::eoa_fixture(),
            &state,
            "0xabc",
            "sha",
            0,
            external,
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("invalid external controller state"),
            "{error}"
        );
    }

    #[test]
    fn unknown_asset_halts_instead_of_adding() {
        let mut state = snapshot();
        state.account.unknown_exposure = true;
        let plan = build_plan(&StrategyConfig::eoa_fixture(), &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.intent, Intent::ReconcileUnknown);
        assert_eq!(plan.risk_action, RiskAction::HaltUnknown);
    }

    #[test]
    fn stage_c_bounds_band_actions_to_fifty_dollars() {
        let config = StrategyConfig::eoa_fixture();
        let plan = build_plan(&config, &snapshot(), "0xabc", "sha", 0).unwrap();
        assert_eq!(
            bounded_controller_execution(&plan, &config, 50.0).unwrap(),
            Some(ControllerExecution::OpenS3 {
                notional_usd: 50.0,
                gate_bps: plan.entry_gate_bps,
                keep_bps: plan.entry_policy.keep_gate_bps,
            })
        );

        let mut high = snapshot();
        high.account.hype_spot_qty = 400.0;
        high.account.hype_perp_qty = -400.0;
        high.perp = book(49.98, 49.99);
        let plan = build_plan(&config, &high, "0xabc", "sha", 0).unwrap();
        assert_eq!(
            bounded_controller_execution(&plan, &config, 25.0).unwrap(),
            Some(ControllerExecution::CloseS3 { notional_usd: 25.0 })
        );
    }

    #[test]
    fn configured_absolute_cap_bounds_the_operator_cap() {
        let config = StrategyConfig::eoa_fixture();
        let plan = build_plan(&config, &snapshot(), "0xabc", "sha", 0).unwrap();
        assert_eq!(
            bounded_controller_execution(&plan, &config, 5_000.0).unwrap(),
            Some(ControllerExecution::OpenS3 {
                notional_usd: 500.0,
                gate_bps: plan.entry_gate_bps,
                keep_bps: plan.entry_policy.keep_gate_bps,
            })
        );
    }

    #[test]
    fn pmr_taker_reduce_ignores_normal_operator_cap_but_keeps_absolute_cap() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.account.hype_spot_qty = 250.0;
        state.account.hype_perp_qty = -250.0;
        let mut plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        plan.intent = Intent::PmrTakerReduce { target_pmr: 0.72 };
        assert_eq!(
            bounded_controller_execution(&plan, &config, 50.0).unwrap(),
            Some(ControllerExecution::PmrTakerReduce {
                notional_usd: plan
                    .account_value_usd
                    .min(config.max_single_order_notional_usd)
            })
        );
    }

    #[test]
    fn agent_expiry_warning_uses_configured_window() {
        let mut state = snapshot();
        state.account.agent_valid_until_ms = state.account.snapshot_ts_ms + 1_000;
        let plan = build_plan(&StrategyConfig::eoa_fixture(), &state, "0xabc", "sha", 0).unwrap();
        assert!(plan.agent_expiry_warning);
    }

    #[test]
    fn stage_c_waits_for_normal_exit_gate() {
        let config = StrategyConfig::eoa_fixture();
        let mut high = snapshot();
        high.account.hype_spot_qty = 400.0;
        high.account.hype_perp_qty = -400.0;
        let plan = build_plan(&config, &high, "0xabc", "sha", 0).unwrap();
        assert!(matches!(plan.intent, Intent::BandReduce { .. }));
        assert_eq!(
            bounded_controller_execution(&plan, &config, 50.0).unwrap(),
            None
        );
    }

    #[test]
    fn unknown_state_waits_without_stopping_the_controller() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.account.unknown_exposure = true;
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(
            bounded_controller_execution(&plan, &config, 50.0).unwrap(),
            None
        );
    }

    #[test]
    fn unknown_state_keeps_cancel_and_unique_hedge_recovery() {
        let config = StrategyConfig::eoa_fixture();
        let mut unexpected_order = snapshot();
        unexpected_order.account.open_orders = 1;
        unexpected_order.account.unknown_exposure = true;
        let cancel = build_plan(&config, &unexpected_order, "0xabc", "sha", 0).unwrap();
        assert_eq!(cancel.intent, Intent::CancelUnexpected);
        assert_eq!(
            bounded_controller_execution(&cancel, &config, 50.0).unwrap(),
            Some(ControllerExecution::CancelUnexpected)
        );

        let mut missing_leg = snapshot();
        missing_leg.account.hype_spot_qty = 3.0;
        missing_leg.account.hype_perp_qty = -2.0;
        missing_leg.account.unknown_exposure = true;
        let hedge = build_plan(&config, &missing_leg, "0xabc", "sha", 0).unwrap();
        assert!(matches!(hedge.intent, Intent::HedgeResidual { .. }));
        assert_eq!(
            bounded_controller_execution(&hedge, &config, 50.0).unwrap(),
            Some(ControllerExecution::HedgeResidual)
        );
    }

    #[test]
    fn stale_market_halts_new_risk() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.spot.exchange_ts_ms = 0;
        state.perp.exchange_ts_ms = 0;
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.risk_action, RiskAction::HaltUnknown);
        assert!(!plan.market_state_complete);
        assert_eq!(plan.intent, Intent::ReconcileUnknown);
        assert_eq!(
            bounded_controller_execution(&plan, &config, 50.0).unwrap(),
            None
        );
    }

    #[test]
    fn controller_tolerates_one_second_of_rest_book_timestamp_jitter() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        let accepted_ts = now_ms() - config.max_market_age_ms as i64 - 500;
        state.spot.exchange_ts_ms = accepted_ts;
        state.spot.recv_ts_ms = accepted_ts;
        state.perp.exchange_ts_ms = accepted_ts;
        state.perp.recv_ts_ms = accepted_ts;
        assert!(
            build_plan(&config, &state, "0xabc", "sha", 0)
                .unwrap()
                .market_state_complete
        );

        let stale_ts =
            now_ms() - config.max_market_age_ms as i64 - CONTROLLER_MARKET_AGE_GRACE_MS as i64 - 1;
        state.spot.exchange_ts_ms = stale_ts;
        state.spot.recv_ts_ms = stale_ts;
        state.perp.exchange_ts_ms = stale_ts;
        state.perp.recv_ts_ms = stale_ts;
        assert!(
            !build_plan(&config, &state, "0xabc", "sha", 0)
                .unwrap()
                .market_state_complete
        );
    }

    #[test]
    fn future_market_timestamp_halts_new_risk() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.spot.recv_ts_ms = now_ms() + config.max_market_clock_skew_ms as i64 + 1;
        state.spot.exchange_ts_ms = state.spot.recv_ts_ms;
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.risk_action, RiskAction::HaltUnknown);
        assert_eq!(plan.intent, Intent::ReconcileUnknown);
    }

    #[test]
    fn excessive_leg_timestamp_skew_halts_new_risk() {
        let config = StrategyConfig::eoa_fixture();
        let mut state = snapshot();
        state.spot.exchange_ts_ms = now_ms();
        state.spot.recv_ts_ms = state.spot.exchange_ts_ms;
        state.perp.exchange_ts_ms =
            state.spot.exchange_ts_ms - config.max_leg_timestamp_skew_ms as i64 - 1;
        state.perp.recv_ts_ms = state.perp.exchange_ts_ms;
        let plan = build_plan(&config, &state, "0xabc", "sha", 0).unwrap();
        assert_eq!(plan.risk_action, RiskAction::HaltUnknown);
        assert_eq!(plan.intent, Intent::ReconcileUnknown);
    }
}
