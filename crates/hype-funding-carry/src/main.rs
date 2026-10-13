use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use hype_funding_carry::accounting::sync_accounting;
use hype_funding_carry::api::{now_ms, InfoClient};
use hype_funding_carry::audit::AuditClient;
use hype_funding_carry::config::StrategyConfig;
#[cfg(feature = "live-sdk")]
use hype_funding_carry::controller::Intent;
use hype_funding_carry::controller_runtime::{
    audit_config, audit_plan, fetch_plan, git_sha, RebalancePhase,
};
#[cfg(feature = "live-sdk")]
use hype_funding_carry::controller_runtime::{bounded_controller_execution, ControllerExecution};
#[cfg(feature = "live-sdk")]
use hype_funding_carry::deployment_phase::DeploymentPhase;
use hype_funding_carry::model::{decide, Action, Decision};
use hype_funding_carry::notify::{alert, resolve};
#[cfg(feature = "live-sdk")]
use hype_funding_carry::risk::RiskAction;
use hype_funding_carry::risk::{evaluate, NakedTracker, RiskInput};
#[cfg(feature = "live-sdk")]
use hype_funding_carry::ws::{bbo_book, spawn_feed, FeedEvent, MarketBook};
use hype_funding_carry::{DRY_RUN_PARAMETER_VERSION, EXECUTOR_VERSION};
use serde::Serialize;

mod cli;
mod controller_service;

use cli::{Cli, Command};
use controller_service::run_controller_plan;
#[cfg(feature = "live-sdk")]
use controller_service::{
    run_controller_live_serve, run_controller_open_once, run_controller_rebalance_once,
};

#[cfg(feature = "live-sdk")]
const GATE_WAKE_MIN_INTERVAL_MS: i64 = 30_000;

#[cfg(feature = "live-sdk")]
#[derive(Debug, Default)]
struct LiveCycleState {
    urgent: bool,
    watch_entry_gate: bool,
    entry_create_bps: f64,
    entry_keep_bps: f64,
    accounting_snapshot: Option<hype_funding_carry::api::Snapshot>,
    strategy_nav_usd: f64,
    core_equity_usd: f64,
    prepared_entry: Option<PreparedEntry>,
}

#[cfg(feature = "live-sdk")]
#[derive(Debug)]
struct PreparedEntry {
    run_tag: &'static str,
    permit: hype_funding_carry::live::EntryPermit,
    parent_intent_id: String,
    notional_usd: f64,
    gate_bps: f64,
    keep_bps: f64,
    config: StrategyConfig,
    config_hash: String,
    git_sha: String,
}

#[cfg(feature = "live-sdk")]
struct ControllerRuntimeState {
    naked_tracker: NakedTracker,
    deployment_phase: DeploymentPhase,
    prepare_entry: bool,
}

#[cfg(feature = "live-sdk")]
impl ControllerRuntimeState {
    fn load(account: &str, mode: &str) -> Result<Self> {
        Ok(Self {
            naked_tracker: NakedTracker::default(),
            deployment_phase: DeploymentPhase::load(account, mode)?,
            prepare_entry: false,
        })
    }
}

#[cfg(feature = "live-sdk")]
#[derive(Clone, Debug, PartialEq)]
struct EntryTrigger {
    id: String,
    recv_ts_ms: i64,
    basis_bps: f64,
    spot: MarketBook,
    perp: MarketBook,
}

#[cfg(feature = "live-sdk")]
#[derive(Debug)]
struct EntryGateWatcher {
    connected: bool,
    armed: bool,
    spot_book: Option<MarketBook>,
    perp_book: Option<MarketBook>,
    last_trigger_recv_ts_ms: i64,
    permit_state_ts_ms: i64,
    permit_issued_ts_ms: i64,
    entry_state_changed: bool,
    fill_seen: bool,
}

#[cfg(feature = "live-sdk")]
impl Default for EntryGateWatcher {
    fn default() -> Self {
        Self {
            connected: false,
            armed: true,
            spot_book: None,
            perp_book: None,
            last_trigger_recv_ts_ms: 0,
            permit_state_ts_ms: 0,
            permit_issued_ts_ms: 0,
            entry_state_changed: false,
            fill_seen: false,
        }
    }
}

#[cfg(feature = "live-sdk")]
impl EntryGateWatcher {
    fn begin_permit(&mut self, state_ts_ms: i64, issued_ts_ms: i64) {
        self.permit_state_ts_ms = state_ts_ms;
        self.permit_issued_ts_ms = issued_ts_ms;
        self.entry_state_changed = false;
        self.fill_seen = false;
        self.armed = true;
        self.spot_book = None;
        self.perp_book = None;
    }

    fn observe(
        &mut self,
        event: FeedEvent,
        create_bps: f64,
        keep_bps: f64,
    ) -> Option<EntryTrigger> {
        let recv_ts_ms = match event {
            FeedEvent::Connected => {
                self.connected = true;
                self.armed = true;
                self.spot_book = None;
                self.perp_book = None;
                return None;
            }
            FeedEvent::Disconnected(_) => {
                self.connected = false;
                self.spot_book = None;
                self.perp_book = None;
                self.entry_state_changed = true;
                return None;
            }
            FeedEvent::Bbo {
                coin,
                bid,
                ask,
                exchange_ts_ms,
                recv_ts_ms,
            } if self.connected
                && recv_ts_ms >= self.permit_issued_ts_ms
                && (coin == "@107" || coin == "HYPE") =>
            {
                let book = match bbo_book(exchange_ts_ms, recv_ts_ms, bid, ask) {
                    Ok(book) => book,
                    Err(_) => {
                        self.entry_state_changed = true;
                        return None;
                    }
                };
                self.update_book(&coin, book, true);
                recv_ts_ms
            }
            FeedEvent::Book { coin, book }
                if self.connected
                    && book.recv_ts_ms >= self.permit_issued_ts_ms
                    && (coin == "@107" || coin == "HYPE") =>
            {
                let recv_ts_ms = book.recv_ts_ms;
                self.update_book(&coin, book, false);
                recv_ts_ms
            }
            FeedEvent::UserFill { recv_ts_ms, .. } => {
                self.fill_seen = true;
                if recv_ts_ms > self.permit_state_ts_ms {
                    self.entry_state_changed = true;
                }
                return None;
            }
            FeedEvent::OrderUpdate { recv_ts_ms, .. } => {
                if recv_ts_ms > self.permit_state_ts_ms {
                    self.entry_state_changed = true;
                }
                return None;
            }
            _ => return None,
        };
        let (Some(spot), Some(perp)) = (&self.spot_book, &self.perp_book) else {
            return None;
        };
        let basis_bps = (perp.book.best_bid().ok()? / spot.book.best_bid().ok()? - 1.0) * 10_000.0;
        if basis_bps < keep_bps {
            self.armed = true;
        }
        let trigger_interval_elapsed = self.last_trigger_recv_ts_ms == 0
            || recv_ts_ms.saturating_sub(self.last_trigger_recv_ts_ms) >= GATE_WAKE_MIN_INTERVAL_MS;
        if !self.entry_state_changed
            && self.armed
            && basis_bps >= create_bps
            && trigger_interval_elapsed
        {
            self.armed = false;
            self.last_trigger_recv_ts_ms = recv_ts_ms;
            Some(EntryTrigger {
                id: uuid::Uuid::new_v4().to_string(),
                recv_ts_ms,
                basis_bps,
                spot: spot.clone(),
                perp: perp.clone(),
            })
        } else {
            None
        }
    }

    fn update_book(&mut self, coin: &str, book: hype_funding_carry::model::Book, is_bbo: bool) {
        let cached = if coin == "@107" {
            &mut self.spot_book
        } else {
            &mut self.perp_book
        };
        if let Some(cached) = cached {
            cached.update(book, is_bbo);
        } else {
            *cached = Some(MarketBook::new(book));
        }
    }
}

#[derive(Clone, Serialize)]
struct RiskRow {
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
    risk_level: String,
    allow_new_entry: u8,
    kill_switch: u8,
    reasons: Vec<String>,
    raw_account_state: String,
    created_ts_ms: i64,
}

#[derive(Serialize)]
struct DecisionRow<'a> {
    decision_id: &'a str,
    parameter_version: &'static str,
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
    eligible: u8,
    blocked: u8,
    reason: &'a str,
    spot_bid_px: f64,
    spot_ask_px: f64,
    perp_bid_px: f64,
    perp_ask_px: f64,
    created_ts_ms: i64,
}

fn account_address() -> Result<String> {
    let address =
        std::env::var("HFC_ACCOUNT_ADDRESS").context("HFC_ACCOUNT_ADDRESS is required")?;
    if address.len() != 42
        || !address.starts_with("0x")
        || !address[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("HFC_ACCOUNT_ADDRESS is malformed");
    }
    Ok(address)
}

fn agent_address() -> Result<String> {
    let address = std::env::var("HFC_AGENT_ADDRESS").context("HFC_AGENT_ADDRESS is required")?;
    if address.len() != 42
        || !address.starts_with("0x")
        || !address[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("HFC_AGENT_ADDRESS is malformed");
    }
    Ok(address)
}

#[cfg(feature = "live-sdk")]
fn validate_confirmed_account(account: &str, confirmation: &str) -> Result<()> {
    if !account.eq_ignore_ascii_case(confirmation) {
        bail!("--confirm-account must exactly confirm the HYPE Funding Carry account");
    }
    Ok(())
}

#[cfg(feature = "live-sdk")]
fn accounting_snapshot_safe(intent: Intent, execution: Option<ControllerExecution>) -> bool {
    !matches!(intent, Intent::ReconcileUnknown) && execution.is_none()
}

#[cfg(feature = "live-sdk")]
struct FillNotificationFlushGuard;

#[cfg(feature = "live-sdk")]
impl Drop for FillNotificationFlushGuard {
    fn drop(&mut self) {
        if let Err(error) =
            hype_funding_carry::fill_notify::flush_background(Duration::from_secs(10))
        {
            eprintln!("fill_notification_flush_failed={error:#}");
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("HFC_DISABLE_DOTENV").as_deref() != Ok("1") {
        dotenvy::dotenv().ok();
    }
    let cli = Cli::parse();
    #[cfg(feature = "live-sdk")]
    let _fill_notification_flush = FillNotificationFlushGuard;
    let info = InfoClient::new()?;
    let account = account_address()?;
    let agent = agent_address()?;
    #[cfg(feature = "live-sdk")]
    let _writer_lock = {
        let writes = match &cli.command {
            Command::RebalanceOnce { execute, .. }
            | Command::OpenOnce { execute, .. }
            | Command::Open { execute, .. }
            | Command::Close { execute, .. }
            | Command::Repair { execute, .. }
            | Command::CancelAll { execute, .. } => *execute,
            Command::Serve { execute, .. } => *execute,
            _ => false,
        };
        if writes {
            Some(hype_funding_carry::writer_lock::WriterLock::acquire(
                &account,
            )?)
        } else {
            None
        }
    };
    let result = match cli.command {
        Command::Status { audit } => run_status(&info, &account, &agent, audit).await,
        Command::FeedCheck { seconds } => run_feed_check(&account, seconds).await,
        Command::DryRun {
            notional,
            action,
            interval_seconds,
            audit,
        } => {
            if !(10.0..=500.0).contains(&notional) {
                bail!("v0.1 dry-run notional must be between $10 and $500");
            }
            let mut tracker = NakedTracker::default();
            let mut next_accounting_ms = 0;
            loop {
                let (risk, snapshot) = run_decision(
                    &info,
                    &account,
                    &agent,
                    action.into(),
                    notional,
                    audit,
                    &mut tracker,
                )
                .await?;
                if audit {
                    if risk.risk_level != "ok" || risk.kill_switch != 0 {
                        alert(
                            "hfc_live_risk",
                            &format!(
                                "HYPE Funding Carry 风控: level={} kill={} PMR={:.3} delta=${:.2} reasons={:?}",
                                risk.risk_level,
                                risk.kill_switch,
                                risk.portfolio_margin_ratio,
                                risk.residual_delta_usd,
                                risk.reasons
                            ),
                        );
                    }
                    if now_ms() >= next_accounting_ms {
                        if risk.risk_level == "ok" && risk.kill_switch == 0 {
                            resolve("hfc_live_risk", "HYPE Funding Carry 实时风控");
                        }
                        let accounting = AuditClient::from_env();
                        match accounting {
                            Ok(client) => {
                                match sync_accounting(
                                    &info,
                                    &client,
                                    &account,
                                    &snapshot,
                                    snapshot.account.account_value_usd,
                                    snapshot.account.account_value_usd,
                                )
                                .await
                                {
                                    Ok(result) => {
                                        resolve(
                                            "hfc_accounting_error",
                                            "HYPE Funding Carry 账户会计同步",
                                        );
                                        if result.pnl_written {
                                            resolve(
                                                "hfc_accounting_missing_pnl",
                                                "HYPE Funding Carry PnL 写入",
                                            );
                                        } else {
                                            alert(
                                                "hfc_accounting_missing_pnl",
                                                "HYPE Funding Carry 账户会计未写入 PnL 快照",
                                            );
                                        }
                                    }
                                    Err(error) => alert(
                                        "hfc_accounting_error",
                                        &format!("HYPE Funding Carry 账户会计同步失败: {error:#}"),
                                    ),
                                }
                            }
                            Err(error) => alert(
                                "hfc_accounting_error",
                                &format!("HYPE Funding Carry 账户会计初始化失败: {error:#}"),
                            ),
                        }
                        next_accounting_ms = now_ms() + 60_000;
                    }
                }
                if interval_seconds == 0 {
                    break Ok(());
                }
                tokio::time::sleep(Duration::from_secs(interval_seconds.max(5))).await;
            }
        }
        Command::Plan { config, audit } => {
            run_controller_plan(&info, &account, &agent, config.as_deref(), audit).await
        }
        Command::Serve {
            config,
            execute,
            confirm_account,
            max_notional,
            interval_seconds,
        } => {
            #[cfg(not(feature = "live-sdk"))]
            let _ = (&config, &confirm_account, max_notional, interval_seconds);
            if !execute {
                bail!("serve requires explicit --execute");
            }
            #[cfg(not(feature = "live-sdk"))]
            bail!("serve --execute requires the live-sdk feature");
            #[cfg(feature = "live-sdk")]
            {
                hype_funding_carry::authorization::require_production_go_live()?;
                let confirmation = confirm_account
                    .as_deref()
                    .context("serve --execute requires --confirm-account")?;
                return run_controller_live_serve(
                    &info,
                    &account,
                    &agent,
                    config.as_deref(),
                    interval_seconds,
                    max_notional,
                    confirmation,
                )
                .await;
            }
        }
        #[cfg(feature = "live-sdk")]
        Command::RebalanceOnce {
            config,
            max_notional,
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("rebalance-once requires explicit --execute");
            }
            hype_funding_carry::authorization::require_production_go_live()?;
            let mut runtime_state = ControllerRuntimeState::load(&account, "live")?;
            run_controller_rebalance_once(
                &info,
                &account,
                &agent,
                config.as_deref(),
                max_notional,
                &confirm_account,
                &mut runtime_state,
            )
            .await
            .map(|_| ())
        }
        #[cfg(feature = "live-sdk")]
        Command::OpenOnce {
            config,
            notional,
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("open-once requires explicit --execute");
            }
            hype_funding_carry::authorization::require_production_go_live()?;
            validate_confirmed_account(&account, &confirm_account)?;
            run_controller_open_once(
                &info,
                &account,
                &agent,
                config.as_deref(),
                notional,
                &confirm_account,
            )
            .await
        }
        #[cfg(feature = "live-sdk")]
        Command::SignerCheck => signer_check().await,
        #[cfg(feature = "live-sdk")]
        Command::Open {
            config,
            notional,
            gate_bps,
            keep_bps,
            max_qty,
            depth_utilization,
            wait,
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("open requires explicit --execute");
            }
            if max_qty.is_some_and(|value| !value.is_finite() || value <= 0.0) {
                bail!("--max-qty must be a positive finite number");
            }
            if depth_utilization
                .is_some_and(|value| !value.is_finite() || value <= 0.0 || value > 1.0)
            {
                bail!("--depth-utilization must be in (0, 1]");
            }
            hype_funding_carry::authorization::require_production_go_live()?;
            let loaded_config = config.as_deref().map(StrategyConfig::load).transpose()?;
            let config_hash = loaded_config
                .as_ref()
                .map(StrategyConfig::config_hash)
                .transpose()?
                .unwrap_or_else(|| "manual-probe".to_string());
            let sha = git_sha();
            let mut context = loaded_config.as_ref().map_or_else(
                || hype_funding_carry::live::ExecutionContext::standalone("s3"),
                |config| {
                    hype_funding_carry::live::ExecutionContext::configured(
                        "manual_s3",
                        "manual_s3",
                        true,
                        config,
                        &config_hash,
                        &sha,
                        &account,
                    )
                },
            );
            context.strategy_config = None;
            context.order_policy.entry_create_bps = gate_bps;
            context.order_policy.entry_keep_bps = keep_bps.unwrap_or(gate_bps);
            context.max_order_qty_hype = max_qty;
            if let Some(value) = depth_utilization {
                context.hedge_depth_utilization = value;
            }
            loop {
                let result = hype_funding_carry::live::open_s3(
                    &info,
                    &account,
                    &agent,
                    &confirm_account,
                    notional,
                    gate_bps,
                    context,
                )
                .await;
                if !wait || result.is_ok() {
                    break result;
                }
                let error = result.as_ref().expect_err("checked above");
                if hype_funding_carry::live::is_basis_gate_not_met(error)
                    || hype_funding_carry::live::is_retryable_capacity_change(error)
                {
                    tokio::time::sleep(Duration::from_secs(15)).await;
                } else if hype_funding_carry::live::is_retryable_alo_reject(error) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    break result;
                }
            }
        }
        #[cfg(feature = "live-sdk")]
        Command::Close {
            mode,
            notional,
            wait,
            execution_probe_ignore_basis,
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("close requires explicit --execute");
            }
            let mode: hype_funding_carry::live::ExitMode = mode.into();
            if wait {
                if mode != hype_funding_carry::live::ExitMode::Normal
                    || notional.is_some()
                    || execution_probe_ignore_basis
                {
                    bail!("close --wait requires normal mode, full close, and basis enforcement");
                }
                run_close_wait_with_shutdown(&info, &account, &agent, &confirm_account).await
            } else {
                hype_funding_carry::live::close_s3(
                    &info,
                    &account,
                    &agent,
                    &confirm_account,
                    mode,
                    notional,
                    hype_funding_carry::live::ExecutionContext {
                        enforce_basis: !execution_probe_ignore_basis,
                        ..hype_funding_carry::live::ExecutionContext::standalone(mode.run_tag())
                    },
                )
                .await
            }
        }
        #[cfg(feature = "live-sdk")]
        Command::Repair {
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("repair requires explicit --execute");
            }
            hype_funding_carry::live::repair(&info, &account, &agent, &confirm_account).await
        }
        #[cfg(feature = "live-sdk")]
        Command::ReconcileFills => {
            let (fills, fees_usd) =
                hype_funding_carry::live::reconcile_fills(&info, &account).await?;
            println!("{{\"reconciled_fills\":{fills},\"fees_usd\":{fees_usd}}}");
            Ok(())
        }
        #[cfg(feature = "live-sdk")]
        Command::CancelAll {
            execute,
            confirm_account,
        } => {
            if !execute {
                bail!("cancel-all requires explicit --execute");
            }
            let canceled =
                hype_funding_carry::live::cancel_all(&info, &account, &confirm_account).await?;
            println!("{{\"canceled_orders\":{canceled}}}");
            Ok(())
        }
        #[cfg(feature = "live-sdk")]
        Command::Recover => hype_funding_carry::live::recover(&info, &account, &agent).await,
    };
    if let Err(error) = AuditClient::flush_background(Duration::from_secs(10)) {
        eprintln!("audit_queue_flush_failed={error:#}");
    }
    result
}

#[cfg(feature = "live-sdk")]
fn shutdown_signal() -> Result<impl std::future::Future<Output = Result<()>>> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    Ok(async move {
        #[cfg(unix)]
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        Ok(())
    })
}

#[cfg(feature = "live-sdk")]
async fn run_close_wait_with_shutdown(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
) -> Result<()> {
    validate_confirmed_account(account, confirmation)?;
    let shutdown = shutdown_signal()?;
    tokio::pin!(shutdown);
    let close_info = info.clone();
    let close_account = account.to_string();
    let close_agent = agent.to_string();
    let close_confirmation = confirmation.to_string();
    let mut close = tokio::spawn(async move {
        loop {
            let snapshot = close_info.snapshot(&close_account, &close_agent).await?;
            let spot_ask = snapshot.spot.best_ask()?;
            let perp_ask = snapshot.perp.best_ask()?;
            if snapshot.account.hype_spot_qty.abs() * spot_ask < 10.0
                && snapshot.account.hype_perp_qty.abs() * perp_ask < 10.0
                && snapshot.account.open_orders == 0
            {
                println!(
                    "{}",
                    serde_json::json!({
                        "status":"completed",
                        "remaining_spot_qty":snapshot.account.hype_spot_qty,
                        "remaining_perp_qty":snapshot.account.hype_perp_qty,
                        "open_orders":snapshot.account.open_orders
                    })
                );
                return Ok(());
            }
            let result = hype_funding_carry::live::close_s3(
                &close_info,
                &close_account,
                &close_agent,
                &close_confirmation,
                hype_funding_carry::live::ExitMode::Normal,
                None,
                hype_funding_carry::live::ExecutionContext::standalone("close_wait"),
            )
            .await;
            if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_basis_gate_not_met)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"normal_exit_gate_not_met"})
                );
                tokio::time::sleep(Duration::from_secs(15)).await;
            } else if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_retryable_alo_reject)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"exit_alo_would_match"})
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                result?;
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    });
    tokio::select! {
        result = &mut close => match result {
            Ok(result) => result,
            Err(join_error) => {
                recover_interrupted_close(info, account, agent, confirmation).await?;
                Err(anyhow::anyhow!("close wait task failed before normal return: {join_error}"))
            }
        },
        signal = &mut shutdown => {
            signal?;
            close.abort();
            let _ = close.await;
            recover_interrupted_close(info, account, agent, confirmation).await?;
            bail!("close wait interrupted; recovery completed")
        }
    }
}

#[cfg(feature = "live-sdk")]
async fn recover_interrupted_close(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
) -> Result<()> {
    hype_funding_carry::live::recover_live(
        info,
        account,
        agent,
        confirmation,
        &hype_funding_carry::live::ExecutionContext::standalone("close_wait_signal_recovery"),
    )
    .await
}

async fn run_feed_check(account: &str, seconds: u64) -> Result<()> {
    use hype_funding_carry::ws::{spawn_feed, FeedEvent};

    let mut feed = spawn_feed(account.to_string());
    let deadline = tokio::time::sleep(Duration::from_secs(seconds.max(1)));
    tokio::pin!(deadline);
    let mut connected = 0;
    let mut disconnected = 0;
    let mut spot_bbo = 0;
    let mut perp_bbo = 0;
    let mut fills = 0;
    let mut order_updates = 0;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            event = feed.recv() => match event {
                Some(FeedEvent::Connected) => connected += 1,
                Some(FeedEvent::Disconnected(_)) => disconnected += 1,
                Some(FeedEvent::Bbo { coin, .. }) if coin == "@107" => spot_bbo += 1,
                Some(FeedEvent::Bbo { coin, .. }) if coin == "HYPE" => perp_bbo += 1,
                Some(FeedEvent::UserFill { .. }) => fills += 1,
                Some(FeedEvent::OrderUpdate { .. }) => order_updates += 1,
                Some(_) => {}
                None => break,
            }
        }
    }
    println!(
        "{}",
        serde_json::json!({
            "connected":connected,
            "disconnected":disconnected,
            "spot_bbo":spot_bbo,
            "perp_bbo":perp_bbo,
            "user_fills":fills,
            "order_updates":order_updates,
            "sent_action":false
        })
    );
    if connected == 0 || disconnected > 0 || spot_bbo == 0 || perp_bbo == 0 {
        bail!("websocket feed check incomplete");
    }
    Ok(())
}

#[cfg(feature = "live-sdk")]
async fn signer_check() -> Result<()> {
    use alloy::signers::local::PrivateKeySigner;
    use alloy::signers::Signer;
    use hyperliquid_rust_sdk::{
        BaseUrl, ClientLimit, ClientOrder, ClientOrderRequest, ExchangeClient,
    };

    let key = hype_funding_carry::secret::load_private_key()?;
    let wallet: PrivateKeySigner = key
        .parse()
        .context("parse HYPE Funding Carry agent private key")?;
    let expected = std::env::var("HFC_AGENT_ADDRESS").context("HFC_AGENT_ADDRESS required")?;
    if wallet.address().to_string().to_lowercase() != expected.to_lowercase() {
        bail!("agent credential does not derive HFC_AGENT_ADDRESS");
    }
    wallet
        .sign_message(b"hype-funding-carry-offline-signer-check")
        .await
        .context("offline agent signature")?;
    let client = ExchangeClient::new(None, wallet, Some(BaseUrl::Mainnet), None, None)
        .await
        .context("initialize pinned Hyperliquid SDK")?;
    if !client.coin_to_asset.contains_key("HYPE") || !client.coin_to_asset.contains_key("HYPE/USDC")
    {
        bail!("pinned SDK metadata is missing HYPE assets");
    }
    let cloid = uuid::Uuid::parse_str("12345678-1234-5678-1234-567812345678")?;
    let _spot_alo = ClientOrderRequest {
        asset: "HYPE/USDC".to_string(),
        is_buy: true,
        reduce_only: false,
        limit_px: 1.0,
        sz: 0.01,
        cloid: Some(cloid),
        order_type: ClientOrder::Limit(ClientLimit {
            tif: "Alo".to_string(),
        }),
    };
    let _perp_reduce_only = ClientOrderRequest {
        asset: "HYPE".to_string(),
        is_buy: true,
        reduce_only: true,
        limit_px: 1.0,
        sz: 0.01,
        cloid: Some(cloid),
        order_type: ClientOrder::Limit(ClientLimit {
            tif: "Ioc".to_string(),
        }),
    };
    println!(
        "{{\"signer_match\":true,\"offline_signature\":true,\"sdk_commit\":\"aac75585daf1\",\"spot_alo_type\":true,\"perp_reduce_only_type\":true,\"cloid_type\":true,\"sent_action\":false}}"
    );
    Ok(())
}

async fn run_status(info: &InfoClient, account: &str, agent: &str, audit: bool) -> Result<()> {
    let mut tracker = NakedTracker::default();
    let (row, _, asset_domain) = snapshot_and_risk(info, account, agent, &mut tracker).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "snapshot_ts_ms": row.snapshot_ts_ms,
            "account_value_usd": row.account_value_usd,
            "portfolio_margin_ratio": row.portfolio_margin_ratio,
            "liquidation_px": row.liquidation_px,
            "available_after_maintenance_usdc": row.available_after_maintenance_usdc,
            "usdc_total": row.usdc_total,
            "usdc_borrowed": row.usdc_borrowed,
            "hype_spot_qty": row.hype_spot_qty,
            "hype_perp_qty": row.hype_perp_qty,
            "residual_delta_usd": row.residual_delta_usd,
            "funding_24h_bps": row.funding_24h_bps,
            "open_orders": row.open_orders,
            "agent_authorized": row.agent_authorized,
            "agent_valid_until_ms": row.agent_valid_until_ms,
            "risk_level": row.risk_level,
            "allow_new_entry": row.allow_new_entry,
            "kill_switch": row.kill_switch,
            "reasons": row.reasons,
            "asset_domain": asset_domain,
        }))?
    );
    if audit {
        AuditClient::from_env()?
            .insert("hfc_exec_risk", &row)
            .await?;
    }
    Ok(())
}

async fn run_decision(
    info: &InfoClient,
    account: &str,
    agent: &str,
    action: Action,
    notional: f64,
    audit: bool,
    tracker: &mut NakedTracker,
) -> Result<(RiskRow, hype_funding_carry::api::Snapshot)> {
    let snapshot = info.snapshot(account, agent).await?;
    let (risk_row, blocked_reason) = risk_row(&snapshot, tracker)?;
    let decision = decide(
        action,
        snapshot.account.snapshot_ts_ms,
        notional,
        &snapshot.spot,
        &snapshot.perp,
        !risk_row.allow_new_entry.eq(&1) && action == Action::Entry,
        &blocked_reason,
    )?;
    println!("{}", serde_json::to_string_pretty(&decision)?);
    if audit {
        let ch = AuditClient::from_env()?;
        ch.insert("hfc_exec_risk", &risk_row).await?;
        ch.insert("hfc_exec_decision", &decision_row(&decision))
            .await?;
    }
    Ok((risk_row, snapshot))
}

async fn snapshot_and_risk(
    info: &InfoClient,
    account: &str,
    agent: &str,
    tracker: &mut NakedTracker,
) -> Result<(
    RiskRow,
    String,
    hype_funding_carry::asset_domain::AssetDomainScan,
)> {
    let snapshot = info.snapshot(account, agent).await?;
    let row = risk_row(&snapshot, tracker)?;
    Ok((row.0, row.1, snapshot.account.asset_domain))
}

fn risk_row(
    snapshot: &hype_funding_carry::api::Snapshot,
    tracker: &mut NakedTracker,
) -> Result<(RiskRow, String)> {
    let spot_bid = snapshot.spot.best_bid()?;
    let spot_ask = snapshot.spot.best_ask()?;
    let mid = (spot_bid + spot_ask) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    let naked_ms = tracker.observe(snapshot.account.snapshot_ts_ms, delta_usd);
    let risk = evaluate(&RiskInput {
        portfolio_margin_enabled: snapshot.account.portfolio_margin_enabled,
        portfolio_margin_ratio: snapshot.account.portfolio_margin_ratio,
        residual_delta_usd: delta_usd,
        naked_ms,
        funding_24h_all_negative: snapshot.account.funding_24h_all_negative,
        agent_authorized: snapshot.account.agent_authorized,
        agent_valid_until_ms: snapshot.account.agent_valid_until_ms,
        now_ms: snapshot.account.snapshot_ts_ms,
        unknown_exposure: snapshot.account.unknown_exposure,
    });
    let blocked_reason = risk.reasons.join(",");
    Ok((
        RiskRow {
            snapshot_ts_ms: snapshot.account.snapshot_ts_ms,
            mode: "dry_run",
            account_value_usd: snapshot.account.account_value_usd,
            portfolio_margin_ratio: snapshot.account.portfolio_margin_ratio,
            liquidation_px: snapshot.account.liquidation_px,
            available_after_maintenance_usdc: snapshot.account.available_after_maintenance_usdc,
            usdc_total: snapshot.account.usdc_total,
            usdc_borrowed: snapshot.account.usdc_borrowed,
            hype_spot_qty: snapshot.account.hype_spot_qty,
            hype_perp_qty: snapshot.account.hype_perp_qty,
            hype_mid_px: mid,
            residual_delta_usd: delta_usd,
            naked_ms,
            funding_24h_bps: snapshot.account.funding_24h_bps,
            open_orders: snapshot.account.open_orders,
            agent_authorized: snapshot.account.agent_authorized as u8,
            agent_valid_until_ms: snapshot.account.agent_valid_until_ms,
            risk_level: risk.level,
            allow_new_entry: risk.allow_new_entry as u8,
            kill_switch: risk.kill_switch as u8,
            reasons: risk.reasons,
            raw_account_state: snapshot.account.raw_account_state.clone(),
            created_ts_ms: now_ms(),
        },
        blocked_reason,
    ))
}

fn decision_row(decision: &Decision) -> DecisionRow<'_> {
    DecisionRow {
        decision_id: &decision.decision_id,
        parameter_version: DRY_RUN_PARAMETER_VERSION,
        executor_version: EXECUTOR_VERSION,
        mode: "dry_run",
        run_tag: "",
        action: &decision.action,
        decision_ts_ms: decision.decision_ts_ms,
        notional_usd: decision.notional_usd,
        target_hype_qty: decision.target_hype_qty,
        gate_bps: decision.gate_bps,
        arrival_mid_basis_bps: (((decision.perp_bid_px + decision.perp_ask_px) / 2.0)
            / ((decision.spot_bid_px + decision.spot_ask_px) / 2.0)
            - 1.0)
            * 10_000.0,
        quoted_basis_bps: decision.executable_basis_bps,
        executable_basis_bps: decision.executable_basis_bps,
        eligible: decision.eligible as u8,
        blocked: decision.blocked as u8,
        reason: &decision.reason,
        spot_bid_px: decision.spot_bid_px,
        spot_ask_px: decision.spot_ask_px,
        perp_bid_px: decision.perp_bid_px,
        perp_ask_px: decision.perp_ask_px,
        created_ts_ms: now_ms(),
    }
}

#[cfg(all(test, feature = "live-sdk"))]
mod main_tests {
    use super::*;
    use hype_funding_carry::ws::Touch;

    #[test]
    fn open_once_requires_explicit_notional_and_confirmation_without_gate_overrides() {
        let args = [
            "hype-funding-carry",
            "open-once",
            "--notional",
            "25",
            "--execute",
            "--confirm-account",
            "0xabc",
        ];
        assert!(matches!(
            Cli::try_parse_from(args).unwrap().command,
            Command::OpenOnce {
                notional: 25.0,
                execute: true,
                ..
            }
        ));
        assert!(Cli::try_parse_from([
            "hype-funding-carry",
            "open-once",
            "--notional",
            "25",
            "--execute"
        ])
        .is_err());
        assert!(Cli::try_parse_from([
            "hype-funding-carry",
            "open-once",
            "--execute",
            "--confirm-account",
            "0xabc"
        ])
        .is_err());
        let mut unsafe_args = args.to_vec();
        unsafe_args.extend(["--gate-bps", "0"]);
        assert!(Cli::try_parse_from(unsafe_args).is_err());
    }

    #[cfg(feature = "live-sdk")]
    fn connected() -> FeedEvent {
        FeedEvent::Connected
    }

    #[cfg(feature = "live-sdk")]
    fn bid(coin: &str, px: f64) -> FeedEvent {
        bid_at(coin, px, 100_000)
    }

    #[cfg(feature = "live-sdk")]
    fn bid_at(coin: &str, px: f64, recv_ts_ms: i64) -> FeedEvent {
        FeedEvent::Bbo {
            coin: coin.to_string(),
            exchange_ts_ms: recv_ts_ms,
            recv_ts_ms,
            bid: Some(Touch { px, sz: 10.0 }),
            ask: Some(Touch {
                px: px + 0.01,
                sz: 10.0,
            }),
        }
    }

    #[test]
    fn confirmed_account_is_checked_before_any_live_fast_path() {
        assert!(validate_confirmed_account("0xAbC", "0xaBc").is_ok());
        assert!(validate_confirmed_account("0xabc", "0xdef").is_err());
    }

    #[cfg(feature = "live-sdk")]
    #[cfg(feature = "live-sdk")]
    #[test]
    fn entry_gate_wakes_on_crossing_and_rearms_below_keep() {
        let mut watcher = EntryGateWatcher::default();
        assert_eq!(watcher.observe(connected(), 1.0, 0.0), None);
        assert_eq!(watcher.observe(bid("@107", 100.0), 1.0, 0.0), None);
        assert!(watcher.observe(bid("HYPE", 100.02), 1.0, 0.0).is_some());
        assert_eq!(watcher.observe(bid("HYPE", 100.03), 1.0, 0.0), None);
        assert_eq!(watcher.observe(bid("HYPE", 99.99), 1.0, 0.0), None);
        assert_eq!(
            watcher.observe(bid_at("HYPE", 100.02, 129_999), 1.0, 0.0),
            None
        );
        assert!(watcher
            .observe(bid_at("HYPE", 100.02, 130_000), 1.0, 0.0)
            .is_some());
    }

    #[test]
    fn entry_trigger_carries_ws_books_and_newer_bbo_does_not_reuse_old_depth() {
        let mut watcher = EntryGateWatcher::default();
        watcher.observe(connected(), 1.0, 0.0);
        let mut spot = bbo_book(
            100_000,
            100_000,
            Some(Touch {
                px: 100.0,
                sz: 10.0,
            }),
            Some(Touch {
                px: 100.01,
                sz: 10.0,
            }),
        )
        .unwrap();
        spot.bid_px.push(99.99);
        spot.bid_sz.push(20.0);
        watcher.observe(
            FeedEvent::Book {
                coin: "@107".into(),
                book: spot.clone(),
            },
            1.0,
            0.0,
        );
        let trigger = watcher.observe(bid("HYPE", 100.02), 1.0, 0.0).unwrap();
        assert_eq!(trigger.spot.book, spot);
        assert_eq!(trigger.perp.book.bid_px, vec![100.02]);
        watcher.observe(bid_at("HYPE", 100.03, 100_100), 1.0, 0.0);
        let old = bbo_book(
            100_050,
            100_101,
            Some(Touch {
                px: 100.01,
                sz: 20.0,
            }),
            Some(Touch {
                px: 100.02,
                sz: 20.0,
            }),
        )
        .unwrap();
        watcher.observe(
            FeedEvent::Book {
                coin: "HYPE".into(),
                book: old,
            },
            1.0,
            0.0,
        );
        assert_eq!(watcher.perp_book.unwrap().book.bid_px, vec![100.03]);
    }

    #[cfg(feature = "live-sdk")]
    #[test]
    fn new_permit_rearms_entry_while_basis_remains_high() {
        let mut watcher = EntryGateWatcher::default();
        watcher.observe(connected(), 1.0, 0.0);
        watcher.begin_permit(1, 100_000);
        watcher.observe(bid_at("@107", 100.0, 100_000), 1.0, 0.0);
        assert!(watcher
            .observe(bid_at("HYPE", 100.02, 100_000), 1.0, 0.0)
            .is_some());

        watcher.begin_permit(2, 130_000);
        watcher.observe(bid_at("@107", 100.0, 130_000), 1.0, 0.0);
        assert!(watcher
            .observe(bid_at("HYPE", 100.02, 130_000), 1.0, 0.0)
            .is_some());
    }

    #[cfg(feature = "live-sdk")]
    #[test]
    fn entry_gate_requires_fresh_books_after_reconnect() {
        let mut watcher = EntryGateWatcher::default();
        watcher.observe(connected(), 1.0, 0.0);
        watcher.observe(bid("@107", 100.0), 1.0, 0.0);
        watcher.observe(FeedEvent::Disconnected("test".to_string()), 1.0, 0.0);
        assert!(watcher.entry_state_changed);
        watcher.begin_permit(1, 0);
        watcher.observe(connected(), 1.0, 0.0);
        assert_eq!(watcher.observe(bid("HYPE", 100.02), 1.0, 0.0), None);
        assert!(watcher.observe(bid("@107", 100.0), 1.0, 0.0).is_some());
    }

    #[cfg(feature = "live-sdk")]
    #[test]
    fn entry_gate_ignores_bbo_queued_before_permit_issue() {
        let mut watcher = EntryGateWatcher::default();
        watcher.observe(connected(), 1.0, 0.0);
        watcher.begin_permit(100, 200);
        assert_eq!(watcher.observe(bid_at("@107", 100.0, 199), 1.0, 0.0), None);
        assert_eq!(watcher.observe(bid_at("HYPE", 100.02, 199), 1.0, 0.0), None);
        assert_eq!(watcher.observe(bid_at("HYPE", 100.02, 200), 1.0, 0.0), None);
        assert!(watcher
            .observe(bid_at("@107", 100.0, 201), 1.0, 0.0)
            .is_some());
    }

    #[cfg(feature = "live-sdk")]
    #[test]
    fn entry_gate_invalidates_permit_on_user_state_change() {
        let mut watcher = EntryGateWatcher::default();
        watcher.begin_permit(100, 0);
        watcher.observe(connected(), 1.0, 0.0);
        watcher.observe(bid("@107", 100.0), 1.0, 0.0);
        watcher.observe(
            FeedEvent::OrderUpdate {
                oid: 7,
                status: "open".to_string(),
                status_ts_ms: 101,
                recv_ts_ms: 101,
            },
            1.0,
            0.0,
        );
        assert!(watcher.entry_state_changed);
        assert_eq!(watcher.observe(bid("HYPE", 100.02), 1.0, 0.0), None);

        watcher.fill_seen = false;
        watcher.observe(
            FeedEvent::UserFill {
                oid: 7,
                tid: 8,
                coin: "HYPE".to_string(),
                px: 100.0,
                qty: 0.1,
                fee: 0.001,
                fee_token: "USDC".to_string(),
                exchange_ts_ms: 102,
                recv_ts_ms: 102,
            },
            1.0,
            0.0,
        );
        assert!(watcher.fill_seen);
    }
}
