use std::path::PathBuf;

use super::*;

/// A live cycle failure must persist this long before it pages anyone.
#[cfg(feature = "live-sdk")]
const CYCLE_ALERT_GRACE_S: u64 = 180;

#[cfg(feature = "live-sdk")]
#[derive(Debug, PartialEq)]
enum PreparedEntryOutcome {
    Retry,
    Finished,
}

#[cfg(feature = "live-sdk")]
fn audit_gate_wake(audit: &AuditClient, account: &str, run_tag: &str, trigger: &EntryTrigger) {
    if let Err(error) = audit.enqueue_entry_stage(
        &trigger.id,
        "",
        account,
        run_tag,
        "gate_wake",
        trigger.recv_ts_ms,
        trigger.recv_ts_ms,
        trigger.basis_bps,
        "",
    ) {
        eprintln!("audit_gate_wake_queue_failed={error:#}");
    }
    println!(
        "{}",
        serde_json::json!({"status":"gate_wake","trigger_id":trigger.id,
        "quoted_basis_bps":trigger.basis_bps,"next_action":"permitted_ws_activation"})
    );
}

#[cfg(feature = "live-sdk")]
fn validate_open_once_notional(config: &StrategyConfig, notional: f64) -> Result<()> {
    if !(10.0..=50.0).contains(&notional)
        || notional < config.min_trade_notional_usd
        || notional > config.max_single_order_notional_usd
    {
        bail!("open-once notional must be $10-$50 and inside configured order limits");
    }
    Ok(())
}

#[cfg(feature = "live-sdk")]
fn validate_open_once_plan(
    plan: &hype_funding_carry::controller_runtime::ControllerPlan,
    notional: f64,
) -> Result<()> {
    if !plan.allow_new_risk
        || plan.open_orders != 0
        || !matches!(plan.intent, Intent::Idle | Intent::BandAdd { .. })
    {
        bail!(
            "open-once requires healthy new-risk state and no prior intent: {:?}",
            plan.intent
        );
    }
    if plan.upper_deployment_ratio <= 0.0
        || plan.portfolio.matched_notional_usd + notional
            > plan.core_equity_usd * plan.upper_deployment_ratio
    {
        bail!("open-once would exceed the configured upper deployment band");
    }
    Ok(())
}

/// An explicit small increase uses the controller's account and risk checks and permit hot path,
/// but does not advance a deployment phase or dispatch any matched-position reduction.
#[cfg(feature = "live-sdk")]
pub(super) async fn run_controller_open_once(
    info: &InfoClient,
    account: &str,
    agent: &str,
    config_path: Option<&Path>,
    notional: f64,
    confirmation: &str,
) -> Result<()> {
    validate_confirmed_account(account, confirmation)?;
    let config = controller_config(config_path)?;
    validate_open_once_notional(&config, notional)?;
    if hype_funding_carry::attempt_state::load(account)?.is_some() {
        bail!("open-once requires the previous attempt to be recovered first");
    }
    let hash = config.config_hash()?;
    let sha = git_sha();
    let audit = AuditClient::from_env()?;
    let mut runtime = ControllerRuntimeState::load(account, "live")?;
    let shutdown = shutdown_signal()?;
    tokio::pin!(shutdown);
    hype_funding_carry::live::warm_exchange(account).await?;
    audit_config(&audit, &config, account, &sha, true).await?;
    let mut feed = spawn_feed(account.to_string());
    let mut watcher = EntryGateWatcher::default();
    let run = async {
        loop {
            let (plan, snapshot) = fetch_plan(
                info,
                &config,
                account,
                agent,
                &sha,
                &mut runtime.naked_tracker,
                RebalancePhase {
                    add_to_target: runtime.deployment_phase.add_active(),
                    reduce_to_target: runtime.deployment_phase.reduce_active(),
                },
            )
            .await?;
            audit_plan(&audit, &plan, &snapshot, "controller_open_once").await;
            validate_open_once_plan(&plan, notional)?;
            if hype_funding_carry::risk_phase::effective_action(
                account,
                plan.risk_action,
                plan.portfolio_margin_ratio,
                config.risk.maker_target_pmr,
            )?
            .is_some_and(|action| !action.allows_new_risk())
            {
                bail!("open-once cannot interrupt a latched risk reduction");
            }
            let mut context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_open_once",
                &plan.decision_id,
                true,
                &config,
                &hash,
                &sha,
                account,
            );
            context.order_policy.entry_create_bps = plan.entry_gate_bps;
            context.order_policy.entry_keep_bps = plan.entry_policy.keep_gate_bps;
            let permit =
                hype_funding_carry::live::EntryPermit::issue(snapshot, &context, now_ms())?;
            watcher.begin_permit(permit.state_ts_ms(), permit.issued_ts_ms());
            let wait = Duration::from_millis(
                permit.expires_ts_ms().saturating_sub(now_ms()).max(0) as u64,
            );
            let prepared = PreparedEntry {
                run_tag: "controller_open_once",
                permit,
                parent_intent_id: plan.decision_id,
                notional_usd: notional,
                gate_bps: plan.entry_gate_bps,
                keep_bps: plan.entry_policy.keep_gate_bps,
                config: config.clone(),
                config_hash: hash.clone(),
                git_sha: sha.clone(),
            };
            let Some(trigger) = wait_for_gate_or_timeout(
                &mut feed,
                &mut watcher,
                true,
                prepared.gate_bps,
                prepared.keep_bps,
                wait,
            )
            .await
            else {
                continue;
            };
            audit_gate_wake(&audit, account, prepared.run_tag, &trigger);
            match run_prepared_entry(
                info,
                account,
                agent,
                confirmation,
                &audit,
                &mut feed,
                prepared,
                &trigger,
            )
            .await?
            {
                PreparedEntryOutcome::Retry => continue,
                PreparedEntryOutcome::Finished => {
                    println!(
                        "{}",
                        serde_json::json!({"status":"entry_management_finished",
                        "run_tag":"controller_open_once","trigger_id":trigger.id,
                        "detail":"one maker management completed; fill quantities are in the open result"})
                    );
                    return Ok(());
                }
            }
        }
    };
    let result = tokio::select! {
        result = run => result,
        signal = &mut shutdown => {
            signal?;
            return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
        }
    };
    if let Err(error) = result {
        if hype_funding_carry::attempt_state::load(account)?.is_some() {
            let context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_open_once_recovery",
                "recovery",
                false,
                &config,
                &hash,
                &sha,
                account,
            );
            if let Err(recovery) =
                hype_funding_carry::live::recover_live(info, account, agent, confirmation, &context)
                    .await
            {
                return Err(error)
                    .context(format!("open-once stopped; recovery failed: {recovery:#}"));
            }
        }
        return Err(error);
    }
    result
}

#[cfg(feature = "live-sdk")]
pub(super) async fn run_controller_live_serve(
    info: &InfoClient,
    account: &str,
    agent: &str,
    config_path: Option<&Path>,
    interval_seconds: u64,
    max_notional: f64,
    confirmation: &str,
) -> Result<()> {
    let shutdown = shutdown_signal()?;
    tokio::pin!(shutdown);
    let startup_config = controller_config(config_path)?;
    let startup_hash = startup_config.config_hash()?;
    let startup_sha = git_sha();
    let startup_context = hype_funding_carry::live::ExecutionContext::configured(
        "controller_startup_recovery",
        "startup",
        false,
        &startup_config,
        &startup_hash,
        &startup_sha,
        account,
    );
    let startup_recovery = hype_funding_carry::live::recover_live(
        info,
        account,
        agent,
        confirmation,
        &startup_context,
    );
    let startup_result = tokio::select! {
        result = startup_recovery => result,
        signal = &mut shutdown => {
            signal?;
            return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
        }
    };
    if let Err(error) = startup_result {
        alert(
            "hfc_controller_live",
            &format!("HYPE Funding Carry live controller startup recovery failed: {error:#}"),
        );
        return Err(error);
    }
    hype_funding_carry::live::warm_exchange(account).await?;
    let mut gate_feed = spawn_feed(account.to_string());
    let mut gate_watcher = EntryGateWatcher::default();
    let gate_audit = AuditClient::from_env()?;
    let mut runtime_state = ControllerRuntimeState::load(account, "live")?;
    runtime_state.prepare_entry = true;
    let mut next_accounting_ms = 0;
    let mut consecutive_cycle_errors: u32 = 0;
    let mut recovery_required = false;
    loop {
        let recovery = async {
            let config = controller_config(config_path)?;
            let hash = config.config_hash()?;
            let sha = git_sha();
            let context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_error_recovery",
                "recovery",
                false,
                &config,
                &hash,
                &sha,
                account,
            );
            hype_funding_carry::live::recover_live(info, account, agent, confirmation, &context)
                .await
        };
        let cycle = run_cycle_after_recovery(
            &mut recovery_required,
            recovery,
            run_controller_rebalance_once(
                info,
                account,
                agent,
                config_path,
                max_notional,
                confirmation,
                &mut runtime_state,
            ),
        );
        let result = tokio::select! {
            result = cycle => result,
            signal = &mut shutdown => {
                signal?;
                return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
            }
        };
        let mut cycle_state = match result {
            Ok(state) => state,
            Err(error) => {
                // Incomplete upstream state blocks this cycle's actions. Keep polling
                // and report persistent failures without abandoning position monitoring.
                report_live_cycle_error(&error, &mut consecutive_cycle_errors);
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(interval_seconds.max(5))) => {}
                    signal = &mut shutdown => {
                        signal?;
                        return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
                    }
                }
                continue;
            }
        };
        if let Some(snapshot) = cycle_state.accounting_snapshot.as_ref() {
            hype_funding_carry::live::clear_post_action_domain_alert(
                &snapshot.account.asset_domain,
            );
        }
        if now_ms() >= next_accounting_ms {
            if let Some(snapshot) = cycle_state.accounting_snapshot.take() {
                spawn_controller_accounting(
                    info.clone(),
                    account.to_string(),
                    snapshot,
                    cycle_state.strategy_nav_usd,
                    cycle_state.core_equity_usd,
                );
                next_accounting_ms = now_ms() + 300_000;
            }
        }
        let cycle_wait = Duration::from_secs(if cycle_state.urgent {
            1
        } else {
            interval_seconds.max(5)
        });
        let permit_wait = cycle_state
            .prepared_entry
            .as_ref()
            .map_or(cycle_wait, |entry| {
                Duration::from_millis(
                    entry.permit.expires_ts_ms().saturating_sub(now_ms()).max(0) as u64
                )
                .min(cycle_wait)
            });
        gate_watcher.begin_permit(
            cycle_state
                .prepared_entry
                .as_ref()
                .map_or(0, |entry| entry.permit.state_ts_ms()),
            cycle_state
                .prepared_entry
                .as_ref()
                .map_or(0, |entry| entry.permit.issued_ts_ms()),
        );
        let gate_trigger = {
            let wait = wait_for_gate_or_timeout(
                &mut gate_feed,
                &mut gate_watcher,
                cycle_state.watch_entry_gate,
                cycle_state.entry_create_bps,
                cycle_state.entry_keep_bps,
                permit_wait,
            );
            tokio::pin!(wait);
            tokio::select! {
                gate_trigger = &mut wait => gate_trigger,
                signal = &mut shutdown => {
                    signal?;
                    return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
                }
            }
        };
        if gate_watcher.fill_seen {
            spawn_fill_reconciliation(info.clone(), account.to_string());
            gate_watcher.fill_seen = false;
        }
        if let Some(trigger) = gate_trigger {
            audit_gate_wake(&gate_audit, account, "controller_band_add", &trigger);
            if let Some(prepared) = cycle_state.prepared_entry.take() {
                let result = await_entry_or_shutdown(
                    run_prepared_entry(
                        info,
                        account,
                        agent,
                        confirmation,
                        &gate_audit,
                        &mut gate_feed,
                        prepared,
                        &trigger,
                    ),
                    &mut shutdown,
                    graceful_live_shutdown(info, account, agent, config_path, confirmation),
                )
                .await?;
                let Some(result) = result else {
                    return Ok(());
                };
                if let Err(error) = result {
                    recovery_required = true;
                    report_live_cycle_error(&error, &mut consecutive_cycle_errors);
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(interval_seconds.max(5))) => {}
                        signal = &mut shutdown => {
                            signal?;
                            return graceful_live_shutdown(info, account, agent, config_path, confirmation).await;
                        }
                    }
                    continue;
                }
            }
        }
        consecutive_cycle_errors = 0;
        resolve("hfc_controller_live", "HYPE Funding Carry live controller");
    }
}

#[cfg(feature = "live-sdk")]
async fn await_entry_or_shutdown<T>(
    entry: impl std::future::Future<Output = Result<T>>,
    shutdown: impl std::future::Future<Output = Result<()>>,
    recovery: impl std::future::Future<Output = Result<()>>,
) -> Result<Option<Result<T>>> {
    // Own and drop the interrupted entry before recovery clears its checkpoint.
    let result = tokio::select! {
        result = entry => Some(result),
        signal = shutdown => {
            signal?;
            None
        }
    };
    if result.is_none() {
        recovery.await?;
    }
    Ok(result)
}

#[cfg(feature = "live-sdk")]
async fn run_cycle_after_recovery<T>(
    recovery_required: &mut bool,
    recovery: impl std::future::Future<Output = Result<()>>,
    cycle: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    // Never issue another entry permit while an interrupted attempt is unresolved.
    if *recovery_required {
        recovery.await?;
        *recovery_required = false;
    }
    let result = cycle.await;
    *recovery_required = result.is_err();
    result
}

#[cfg(feature = "live-sdk")]
fn report_live_cycle_error(error: &anyhow::Error, consecutive_errors: &mut u32) {
    *consecutive_errors += 1;
    hype_funding_carry::notify::alert_after(
        "hfc_controller_live",
        &format!(
            "HYPE Funding Carry live controller cycle failed \
             (连续 {consecutive_errors} 次): {error:#}"
        ),
        CYCLE_ALERT_GRACE_S,
    );
    println!(
        "{}",
        serde_json::json!({
            "status":"cycle_failed",
            "consecutive_errors":consecutive_errors,
            "error":format!("{error:#}"),
        })
    );
}

#[cfg(feature = "live-sdk")]
#[allow(clippy::too_many_arguments)]
async fn run_prepared_entry(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    audit: &AuditClient,
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    prepared: PreparedEntry,
    trigger: &EntryTrigger,
) -> Result<PreparedEntryOutcome> {
    let mut context = hype_funding_carry::live::ExecutionContext::configured(
        prepared.run_tag,
        &prepared.parent_intent_id,
        true,
        &prepared.config,
        &prepared.config_hash,
        &prepared.git_sha,
        account,
    );
    context.order_policy.entry_create_bps = prepared.gate_bps;
    context.order_policy.entry_keep_bps = prepared.keep_bps;
    context.entry_trigger_id = &trigger.id;
    context.entry_trigger_ts_ms = trigger.recv_ts_ms;
    context.entry_trigger_basis_bps = trigger.basis_bps;
    context.audit = Some(audit);
    let result = hype_funding_carry::live::open_s3_permitted(
        info,
        account,
        agent,
        confirmation,
        prepared.notional_usd,
        prepared.gate_bps,
        context,
        feed,
        prepared
            .permit
            .with_books(trigger.spot.clone(), trigger.perp.clone()),
    )
    .await;
    if result
        .as_ref()
        .is_err_and(hype_funding_carry::live::is_entry_permit_invalid)
    {
        println!(
            "{}",
            serde_json::json!({"status":"waiting","reason":"entry_permit_expired_or_changed"})
        );
        Ok(PreparedEntryOutcome::Retry)
    } else if result
        .as_ref()
        .is_err_and(hype_funding_carry::live::is_basis_gate_not_met)
    {
        println!(
            "{}",
            serde_json::json!({"status":"waiting","reason":"entry_gate_changed_before_activation"})
        );
        Ok(PreparedEntryOutcome::Retry)
    } else if result
        .as_ref()
        .is_err_and(hype_funding_carry::live::is_retryable_alo_reject)
    {
        println!(
            "{}",
            serde_json::json!({"status":"waiting","reason":"entry_alo_would_match"})
        );
        Ok(PreparedEntryOutcome::Retry)
    } else if result
        .as_ref()
        .is_err_and(hype_funding_carry::live::is_retryable_capacity_change)
    {
        println!(
            "{}",
            serde_json::json!({"status":"waiting","reason":"hedge_depth_changed_before_activation"})
        );
        Ok(PreparedEntryOutcome::Retry)
    } else {
        result.map(|()| PreparedEntryOutcome::Finished)
    }
}

#[cfg(feature = "live-sdk")]
fn spawn_controller_accounting(
    info: InfoClient,
    account: String,
    snapshot: hype_funding_carry::api::Snapshot,
    strategy_nav_usd: f64,
    core_equity_usd: f64,
) {
    tokio::spawn(async move {
        let result = match AuditClient::from_env() {
            Ok(audit) => {
                sync_accounting(
                    &info,
                    &audit,
                    &account,
                    &snapshot,
                    strategy_nav_usd,
                    core_equity_usd,
                )
                .await
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(accounting) => {
                resolve("hfc_accounting_error", "HYPE Funding Carry 账户会计同步");
                if accounting.pnl_written {
                    resolve("hfc_accounting_missing_pnl", "HYPE Funding Carry PnL 写入");
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
    });
}

#[cfg(feature = "live-sdk")]
fn spawn_fill_reconciliation(info: InfoClient, account: String) {
    tokio::spawn(async move {
        if let Err(error) = hype_funding_carry::live::reconcile_fills(&info, &account).await {
            eprintln!("background_reconcile_fills_failed={error:#}");
        }
    });
}

#[cfg(feature = "live-sdk")]
pub(super) async fn wait_for_gate_or_timeout(
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    watcher: &mut EntryGateWatcher,
    enabled: bool,
    create_bps: f64,
    keep_bps: f64,
    timeout: Duration,
) -> Option<EntryTrigger> {
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return None,
            event = feed.recv() => {
                let event = event?;
                let trigger = watcher.observe(event, create_bps, keep_bps);
                if watcher.entry_state_changed {
                    return None;
                }
                if enabled && trigger.is_some() {
                    return trigger;
                }
            }
        }
    }
}

#[cfg(feature = "live-sdk")]
async fn graceful_live_shutdown(
    info: &InfoClient,
    account: &str,
    agent: &str,
    config_path: Option<&Path>,
    confirmation: &str,
) -> Result<()> {
    let config = controller_config(config_path)?;
    let config_hash = config.config_hash()?;
    let sha = git_sha();
    let context = hype_funding_carry::live::ExecutionContext::configured(
        "controller_shutdown",
        "shutdown",
        false,
        &config,
        &config_hash,
        &sha,
        account,
    );
    hype_funding_carry::live::recover_live(info, account, agent, confirmation, &context).await?;
    resolve(
        "hfc_controller_live",
        "HYPE Funding Carry live controller 已安全停止",
    );
    Ok(())
}

pub(super) fn controller_config(path: Option<&Path>) -> Result<StrategyConfig> {
    let path = match path {
        Some(path) => path.to_path_buf(),
        None => std::env::var("HFC_CONFIG_PATH")
            .map(PathBuf::from)
            .context("--config or HFC_CONFIG_PATH is required")?,
    };
    StrategyConfig::load(&path)
}

pub(super) async fn run_controller_plan(
    info: &InfoClient,
    account: &str,
    agent: &str,
    config_path: Option<&Path>,
    audit: bool,
) -> Result<()> {
    let config = controller_config(config_path)?;
    let sha = git_sha();
    let mut naked_tracker = NakedTracker::default();
    let (plan, snapshot) = fetch_plan(
        info,
        &config,
        account,
        agent,
        &sha,
        &mut naked_tracker,
        RebalancePhase::default(),
    )
    .await?;
    if audit {
        let client = AuditClient::from_env()?;
        audit_config(&client, &config, account, &sha, false).await?;
        audit_plan(&client, &plan, &snapshot, "controller_plan").await;
    }
    println!("{}", serde_json::to_string_pretty(&plan)?);
    Ok(())
}

#[cfg(feature = "live-sdk")]
pub(super) async fn run_controller_rebalance_once(
    info: &InfoClient,
    account: &str,
    agent: &str,
    config_path: Option<&Path>,
    max_notional: f64,
    confirmation: &str,
    runtime_state: &mut ControllerRuntimeState,
) -> Result<LiveCycleState> {
    let config = controller_config(config_path)?;
    let sha = git_sha();
    let (mut plan, snapshot) = fetch_plan(
        info,
        &config,
        account,
        agent,
        &sha,
        &mut runtime_state.naked_tracker,
        RebalancePhase {
            add_to_target: runtime_state.deployment_phase.add_active(),
            reduce_to_target: runtime_state.deployment_phase.reduce_active(),
        },
    )
    .await?;
    runtime_state
        .deployment_phase
        .observe(plan.portfolio.deployment_ratio, config.deployment_band)?;
    let latched_risk = hype_funding_carry::risk_phase::effective_action(
        account,
        plan.risk_action,
        plan.portfolio_margin_ratio,
        config.risk.maker_target_pmr,
    )?;
    if let Some(action) = latched_risk {
        if matches!(
            plan.intent,
            Intent::Idle
                | Intent::BandAdd { .. }
                | Intent::BandReduce { .. }
                | Intent::PmrMakerReduce { .. }
                | Intent::PmrTakerReduce { .. }
        ) {
            plan.risk_action = action;
            plan.intent = match action {
                RiskAction::MakerReduce { target_pmr } => Intent::PmrMakerReduce { target_pmr },
                RiskAction::TakerReduce { target_pmr } => Intent::PmrTakerReduce { target_pmr },
                _ => plan.intent,
            };
        }
    }
    if plan.agent_expiry_warning {
        alert(
            "hfc_agent_expiry",
            "HYPE Funding Carry agent expires inside the configured warning window",
        );
    } else {
        resolve("hfc_agent_expiry", "HYPE Funding Carry agent expiry");
    }
    if snapshot.account.funding_24h_all_negative {
        alert(
            "hfc_funding_negative",
            "过去 24 小时 funding 全为负；仅记录收益变化，不限制新增仓位",
        );
    } else {
        resolve("hfc_funding_negative", "24 小时 funding");
    }

    let audit = AuditClient::from_env()?;
    audit_config(&audit, &config, account, &sha, true).await?;
    audit_plan(&audit, &plan, &snapshot, "controller_live").await;
    let watch_entry_gate = plan.allow_new_risk
        && matches!(plan.intent, Intent::Idle | Intent::BandAdd { .. })
        && plan.open_orders == 0
        && plan.lower_deployment_ratio > 0.0
        && (plan.portfolio.deployment_ratio < plan.lower_deployment_ratio
            || (plan.add_to_target
                && plan.portfolio.deployment_ratio < plan.target_deployment_ratio))
        && plan.portfolio.target_delta_usd >= config.min_trade_notional_usd;

    let planned_execution = bounded_controller_execution(&plan, &config, max_notional)?;
    let prepared_entry = if runtime_state.prepare_entry && watch_entry_gate {
        let notional_usd = plan
            .portfolio
            .target_delta_usd
            .min(max_notional)
            .min(config.max_single_order_notional_usd);
        let mut context = hype_funding_carry::live::ExecutionContext::configured(
            "controller_band_add",
            &plan.decision_id,
            true,
            &config,
            &plan.config_hash,
            &sha,
            account,
        );
        context.order_policy.entry_create_bps = plan.entry_gate_bps;
        context.order_policy.entry_keep_bps = plan.entry_policy.keep_gate_bps;
        Some(PreparedEntry {
            run_tag: "controller_band_add",
            permit: hype_funding_carry::live::EntryPermit::issue(
                snapshot.clone(),
                &context,
                now_ms(),
            )?,
            parent_intent_id: plan.decision_id.clone(),
            notional_usd,
            gate_bps: plan.entry_gate_bps,
            keep_bps: plan.entry_policy.keep_gate_bps,
            config: config.clone(),
            config_hash: plan.config_hash.clone(),
            git_sha: sha.clone(),
        })
    } else {
        None
    };
    let execution = if prepared_entry.is_some() {
        None
    } else {
        planned_execution
    };
    let accounting_safe = accounting_snapshot_safe(plan.intent, execution);
    let urgent = matches!(
        execution,
        Some(
            ControllerExecution::PmrMakerReduce { .. } | ControllerExecution::PmrTakerReduce { .. },
        )
    );
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "controller_intent_id": plan.decision_id,
            "intent": plan.intent,
            "execution": execution,
        }))?
    );
    let result = match execution {
        Some(ControllerExecution::OpenS3 {
            notional_usd,
            gate_bps,
            keep_bps,
        }) => {
            let mut context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_band_add",
                &plan.decision_id,
                true,
                &config,
                &plan.config_hash,
                &sha,
                account,
            );
            context.order_policy.entry_create_bps = gate_bps;
            context.order_policy.entry_keep_bps = keep_bps;
            context.audit = Some(&audit);
            let result = hype_funding_carry::live::open_s3(
                info,
                account,
                agent,
                confirmation,
                notional_usd,
                gate_bps,
                context,
            )
            .await;
            if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_basis_gate_not_met)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"entry_gate_changed_before_activation"})
                );
                Ok(())
            } else if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_retryable_alo_reject)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"entry_alo_would_match"})
                );
                Ok(())
            } else if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_retryable_capacity_change)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"hedge_depth_changed_before_activation"})
                );
                Ok(())
            } else {
                result
            }
        }
        Some(ControllerExecution::CloseS3 { notional_usd }) => {
            let result = hype_funding_carry::live::close_s3(
                info,
                account,
                agent,
                confirmation,
                hype_funding_carry::live::ExitMode::Normal,
                Some(notional_usd),
                hype_funding_carry::live::ExecutionContext::configured(
                    "controller_band_reduce",
                    &plan.decision_id,
                    true,
                    &config,
                    &plan.config_hash,
                    &sha,
                    account,
                ),
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
                Ok(())
            } else if result
                .as_ref()
                .is_err_and(hype_funding_carry::live::is_retryable_alo_reject)
            {
                println!(
                    "{}",
                    serde_json::json!({"status":"waiting","reason":"exit_alo_would_match"})
                );
                Ok(())
            } else {
                result
            }
        }
        Some(ControllerExecution::CancelUnexpected) => {
            let canceled =
                hype_funding_carry::live::cancel_all(info, account, confirmation).await?;
            println!("{}", serde_json::json!({"canceled_orders":canceled}));
            Ok(())
        }
        Some(ControllerExecution::HedgeResidual) => {
            let context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_hedge_residual",
                &plan.decision_id,
                false,
                &config,
                &plan.config_hash,
                &sha,
                account,
            );
            hype_funding_carry::live::recover_live(info, account, agent, confirmation, &context)
                .await
        }
        Some(ControllerExecution::PmrMakerReduce { notional_usd }) => {
            let context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_pmr_reduce",
                &plan.decision_id,
                false,
                &config,
                &plan.config_hash,
                &sha,
                account,
            );
            hype_funding_carry::live::close_s3(
                info,
                account,
                agent,
                confirmation,
                hype_funding_carry::live::ExitMode::Normal,
                Some(notional_usd),
                context,
            )
            .await
        }
        Some(ControllerExecution::PmrTakerReduce { notional_usd }) => {
            let mut context = hype_funding_carry::live::ExecutionContext::configured(
                "controller_pmr_taker_reduce",
                &plan.decision_id,
                false,
                &config,
                &plan.config_hash,
                &sha,
                account,
            );
            context.force_taker_exit = true;
            hype_funding_carry::live::close_s3(
                info,
                account,
                agent,
                confirmation,
                hype_funding_carry::live::ExitMode::Normal,
                Some(notional_usd),
                context,
            )
            .await
        }
        None => Ok(()),
    };
    result?;
    Ok(LiveCycleState {
        urgent,
        watch_entry_gate,
        entry_create_bps: plan.entry_gate_bps,
        entry_keep_bps: plan.entry_policy.keep_gate_bps,
        accounting_snapshot: accounting_safe.then_some(snapshot),
        strategy_nav_usd: plan.target_strategy_equity_usd,
        core_equity_usd: plan.core_equity_usd,
        prepared_entry,
    })
}

#[cfg(all(test, feature = "live-sdk"))]
mod tests {
    use super::*;
    use std::cell::Cell;

    fn open_once_config() -> StrategyConfig {
        let config = StrategyConfig::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/strategy.example.json"),
        )
        .unwrap();
        config
    }

    fn open_once_plan(
        config: &StrategyConfig,
        funding: f64,
    ) -> hype_funding_carry::controller_runtime::ControllerPlan {
        let now = now_ms();
        let book = hype_funding_carry::model::Book {
            exchange_ts_ms: now,
            recv_ts_ms: now,
            bid_px: vec![50.0],
            bid_sz: vec![100.0],
            ask_px: vec![50.01],
            ask_sz: vec![100.0],
        };
        let snapshot = hype_funding_carry::api::Snapshot {
            spot: book.clone(),
            perp: book,
            account: hype_funding_carry::api::AccountSnapshot {
                snapshot_ts_ms: now,
                account_value_usd: 1000.0,
                account_value_ts_ms: now,
                portfolio_margin_enabled: true,
                portfolio_margin_ratio: 0.1,
                available_after_maintenance_usdc: 900.0,
                usdc_total: 1000.0,
                usdc_borrowed: 0.0,
                hype_spot_qty: 30.0,
                hype_perp_qty: -30.0,
                liquidation_px: 0.0,
                funding_24h_bps: 0.0,
                funding_24h_all_negative: false,
                predicted_next_funding_rate: funding,
                next_funding_time_ms: now + 3_600_000,
                open_orders: 0,
                unknown_exposure: false,
                asset_domain: hype_funding_carry::asset_domain::AssetDomainScan {
                    domain_clean: true,
                    ..Default::default()
                },
                agent_authorized: true,
                agent_valid_until_ms: now + 7 * 86_400_000,
                perp_taker_fee_bps: 4.5,
                perp_maker_fee_bps: 1.5,
                spot_taker_fee_bps: 7.0,
                spot_maker_fee_bps: 4.0,
                usdc_borrow_apr: 0.05,
                raw_account_state: "{}".into(),
            },
        };
        hype_funding_carry::controller_runtime::build_plan(config, &snapshot, "0xabc", "sha", 0)
            .unwrap()
    }

    #[test]
    fn open_once_is_hard_capped_and_never_lowers_configured_gates() {
        let mut config = open_once_config();
        config.deployment_band = Some(hype_funding_carry::config::DeploymentBand {
            lower: 1.0,
            target: config.target_deployment_ratio,
            upper: 1.8,
        });
        for notional in [10.0, 25.0, 50.0] {
            validate_open_once_notional(&config, notional).unwrap();
        }
        for notional in [9.99, 50.01, f64::NAN, f64::INFINITY] {
            assert!(validate_open_once_notional(&config, notional).is_err());
        }
        config.max_single_order_notional_usd = 20.0;
        assert!(validate_open_once_notional(&config, 25.0).is_err());
        let plan = open_once_plan(&config, -0.0001);
        assert!(plan.entry_gate_bps >= config.entry_create_bps);
        assert!(plan.entry_policy.keep_gate_bps >= config.entry_keep_bps);
        assert!(plan.entry_policy.timing_penalty_bps > 0.0);
    }

    #[test]
    fn open_once_can_increase_an_idle_healthy_position_but_respects_upper_band() {
        let mut config = open_once_config();
        config.deployment_band = Some(hype_funding_carry::config::DeploymentBand {
            lower: 1.0,
            target: config.target_deployment_ratio,
            upper: 1.8,
        });
        let mut plan = open_once_plan(&config, 0.0001);
        assert_eq!(plan.intent, Intent::Idle);
        assert!(plan.portfolio.target_delta_usd < 0.0);
        validate_open_once_plan(&plan, 50.0).unwrap();
        plan.portfolio.matched_notional_usd = 1790.0;
        validate_open_once_plan(&plan, 10.0).unwrap();
        assert!(validate_open_once_plan(&plan, 25.0).is_err());
    }

    #[test]
    fn open_once_refuses_higher_priority_intents_or_new_risk_revocation() {
        let mut config = open_once_config();
        config.deployment_band = Some(hype_funding_carry::config::DeploymentBand {
            lower: 1.0,
            target: config.target_deployment_ratio,
            upper: 1.8,
        });
        let mut plan = open_once_plan(&config, 0.0001);
        for intent in [
            Intent::ReconcileUnknown,
            Intent::CancelUnexpected,
            Intent::ManualFullClose,
            Intent::BandReduce { amount_usd: 20.0 },
            Intent::PmrMakerReduce { target_pmr: 0.65 },
            Intent::PmrTakerReduce { target_pmr: 0.72 },
        ] {
            plan.intent = intent;
            assert!(validate_open_once_plan(&plan, 25.0).is_err());
        }
        plan.intent = Intent::Idle;
        plan.allow_new_risk = false;
        assert!(validate_open_once_plan(&plan, 25.0).is_err());
        plan.allow_new_risk = true;
        plan.open_orders = 1;
        assert!(validate_open_once_plan(&plan, 25.0).is_err());
    }

    #[tokio::test]
    async fn stopped_entry_drops_checkpoint_guard_before_shutdown_recovery() {
        struct CheckpointGuard<'a>(&'a Cell<bool>);
        impl Drop for CheckpointGuard<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }
        let checkpoint_exists = Cell::new(false);
        let (started, entry_started) = tokio::sync::oneshot::channel();
        let entry = async {
            let _guard = CheckpointGuard(&checkpoint_exists);
            started.send(()).unwrap();
            std::future::pending::<Result<()>>().await
        };
        let shutdown = async {
            entry_started.await.unwrap();
            Ok(())
        };
        let recovery = async {
            assert!(checkpoint_exists.get());
            checkpoint_exists.set(false);
            Ok(())
        };
        assert!(await_entry_or_shutdown(entry, shutdown, recovery)
            .await
            .unwrap()
            .is_none());
        assert!(!checkpoint_exists.get());
    }

    #[tokio::test]
    async fn interrupted_entry_blocks_new_cycle_until_recovery_succeeds() {
        let mut recovery_required = true;
        let cycle_calls = Cell::new(0);
        let recovery_calls = Cell::new(0);
        let result = run_cycle_after_recovery(
            &mut recovery_required,
            async {
                recovery_calls.set(recovery_calls.get() + 1);
                anyhow::bail!("temporary API failure while recovering partial spot fill")
            },
            async {
                cycle_calls.set(cycle_calls.get() + 1);
                Ok(())
            },
        )
        .await;
        assert!(result.is_err());
        assert!(recovery_required);
        assert_eq!(cycle_calls.get(), 0);

        run_cycle_after_recovery(
            &mut recovery_required,
            async {
                recovery_calls.set(recovery_calls.get() + 1);
                Ok(())
            },
            async {
                assert_eq!(recovery_calls.get(), 2);
                cycle_calls.set(cycle_calls.get() + 1);
                Ok(())
            },
        )
        .await
        .unwrap();
        assert!(!recovery_required);
        assert_eq!(cycle_calls.get(), 1);
    }

    #[tokio::test]
    async fn failed_cycle_requires_recovery_but_healthy_cycles_skip_it() {
        let mut recovery_required = false;
        let recovery_calls = Cell::new(0);
        let result = run_cycle_after_recovery::<()>(
            &mut recovery_required,
            async {
                recovery_calls.set(recovery_calls.get() + 1);
                Ok(())
            },
            async { anyhow::bail!("spot submission failed after attempt checkpoint") },
        )
        .await;
        assert!(result.is_err());
        assert!(recovery_required);
        assert_eq!(recovery_calls.get(), 0);

        run_cycle_after_recovery(
            &mut recovery_required,
            async {
                recovery_calls.set(recovery_calls.get() + 1);
                Ok(())
            },
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert!(!recovery_required);
        run_cycle_after_recovery(
            &mut recovery_required,
            async { panic!("healthy monitoring cycle must not rerun recovery") },
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(recovery_calls.get(), 1);
    }
}
