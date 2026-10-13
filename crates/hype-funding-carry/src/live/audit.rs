use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn audit_decision(
    audit: &AuditClient,
    decision_id: &str,
    action: &str,
    notional: f64,
    qty: f64,
    snapshot: &Snapshot,
    context: ExecutionContext<'_>,
    gate_bps: f64,
    reason: &str,
    quoted_basis_override: Option<f64>,
) -> Result<()> {
    let spot_bid = snapshot.spot.best_bid()?;
    let spot_ask = snapshot.spot.best_ask()?;
    let perp_bid = snapshot.perp.best_bid()?;
    let perp_ask = snapshot.perp.best_ask()?;
    let quoted_basis_bps = quoted_basis_override.unwrap_or((perp_bid / spot_bid - 1.0) * 10_000.0);
    let entry_policy = context
        .strategy_config
        .map(|config| evaluate_entry(config, &snapshot.account, quoted_basis_bps));
    if let Err(error) = audit.enqueue_best_effort(
        "hfc_exec_decision",
        &DecisionRow {
            decision_id,
            parameter_version: context.parameter_version,
            executor_version: EXECUTOR_VERSION,
            mode: "live",
            run_tag: context.run_tag,
            action,
            decision_ts_ms: snapshot.account.snapshot_ts_ms,
            notional_usd: notional,
            target_hype_qty: qty,
            gate_bps,
            arrival_mid_basis_bps: (((perp_bid + perp_ask) / 2.0) / ((spot_bid + spot_ask) / 2.0)
                - 1.0)
                * 10_000.0,
            quoted_basis_bps,
            executable_basis_bps: quoted_basis_bps,
            eligible: 1,
            blocked: 0,
            reason,
            account_mode: context.account_mode,
            hypercore_account: context.hypercore_account,
            config_hash: context.config_hash,
            git_sha: context.git_sha,
            spot_bid_px: spot_bid,
            spot_ask_px: spot_ask,
            perp_bid_px: perp_bid,
            perp_ask_px: perp_ask,
            intent_id: context.parent_intent_id,
            intent_type: if context.parent_intent_id.is_empty() {
                ""
            } else {
                context.run_tag
            },
            entry_policy_version: entry_policy.map_or("", |_| "timing_v2"),
            predicted_next_funding_rate: entry_policy
                .map_or(0.0, |value| value.predicted_next_funding_rate),
            predicted_next_funding_bps: entry_policy
                .map_or(0.0, |value| value.predicted_next_funding_bps),
            next_funding_time_ms: entry_policy.map_or(0, |value| value.next_funding_time_ms),
            timing_penalty_bps: entry_policy.map_or(0.0, |value| value.timing_penalty_bps),
            timing_entry_eligible: entry_policy.is_some_and(|value| value.timing_eligible) as u8,
            created_ts_ms: now_ms(),
        },
    ) {
        eprintln!("decision_audit_queue_failed decision_id={decision_id} error={error:#}");
    }
    Ok(())
}

pub(super) async fn audit_risk(audit: &AuditClient, snapshot: &Snapshot) -> Result<()> {
    audit_risk_with_context(audit, snapshot, None).await
}

pub(super) async fn audit_risk_configured(
    audit: &AuditClient,
    snapshot: &Snapshot,
    context: &ExecutionContext<'_>,
) -> Result<()> {
    audit_risk_with_context(audit, snapshot, Some(context)).await
}

pub(super) async fn audit_risk_with_context(
    audit: &AuditClient,
    snapshot: &Snapshot,
    context: Option<&ExecutionContext<'_>>,
) -> Result<()> {
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    static NAKED_TRACKER: OnceLock<Mutex<NakedTracker>> = OnceLock::new();
    let naked_ms = NAKED_TRACKER
        .get_or_init(|| Mutex::new(NakedTracker::default()))
        .lock()
        .map_err(|_| anyhow::anyhow!("HYPE Funding Carry naked tracker mutex poisoned"))?
        .observe(snapshot.account.snapshot_ts_ms, delta_usd);
    let delta_emergency_usd = context.map_or(200.0, |value| value.delta_emergency_usd);
    let warning_pmr = context.map_or(0.65, |value| value.warning_pmr);
    let maker_reduce_pmr = context.map_or(0.75, |value| value.maker_reduce_pmr);
    let taker_reduce_pmr = context.map_or(0.85, |value| value.taker_reduce_pmr);
    let emergency = delta_usd > delta_emergency_usd
        || snapshot.account.portfolio_margin_ratio >= taker_reduce_pmr;
    let risk_level = if emergency {
        "emergency"
    } else if snapshot.account.portfolio_margin_ratio >= maker_reduce_pmr {
        "reduce"
    } else if snapshot.account.portfolio_margin_ratio >= warning_pmr {
        "warning"
    } else {
        "ok"
    };
    audit.enqueue_best_effort(
            "hfc_exec_risk",
            &json!({
                "snapshot_ts_ms":snapshot.account.snapshot_ts_ms,
                "mode":"live",
                "account_value_usd":snapshot.account.account_value_usd,
                "portfolio_margin_ratio":snapshot.account.portfolio_margin_ratio,
                "liquidation_px":snapshot.account.liquidation_px,
                "available_after_maintenance_usdc":snapshot.account.available_after_maintenance_usdc,
                "usdc_total":snapshot.account.usdc_total,
                "usdc_borrowed":snapshot.account.usdc_borrowed,
                "hype_spot_qty":snapshot.account.hype_spot_qty,
                "hype_perp_qty":snapshot.account.hype_perp_qty,
                "hype_mid_px":mid,
                "residual_delta_usd":delta_usd,
                "naked_ms":naked_ms,
                "funding_24h_bps":snapshot.account.funding_24h_bps,
                "open_orders":snapshot.account.open_orders,
                "agent_authorized":snapshot.account.agent_authorized as u8,
                "agent_valid_until_ms":snapshot.account.agent_valid_until_ms,
                "risk_level":risk_level,
                "allow_new_entry":(risk_level == "ok") as u8,
                "kill_switch":emergency as u8,
                "reasons":if emergency {vec!["configured_emergency_threshold"]} else {Vec::<&str>::new()},
                "raw_account_state":snapshot.account.raw_account_state,
                "account_mode":context.map_or("eoa", |value| value.account_mode),
                "hypercore_account":context.map_or("", |value| value.hypercore_account),

                "parameter_version":context.map_or("manual-probe", |value| value.parameter_version),
                "config_hash":context.map_or("manual-probe", |value| value.config_hash),
                "git_sha":context.map_or("unknown", |value| value.git_sha),
                "intent_id":context.map_or("", |value| value.parent_intent_id),
                "intent_type":context.map_or("", |value| value.run_tag),
                "created_ts_ms":now_ms()
            }),
    )
}

pub(super) fn audit_order_background(
    audit: &AuditClient,
    decision_id: &str,
    spec: &OrderSpec,
    outcome: &OrderOutcome,
) {
    let row = OrderRow {
        decision_id,
        attempt_id: decision_id,
        cloid: cloid_hex(spec.cloid),
        event_ts_ms: outcome.request_ts_ms,
        request_ts_ms: outcome.request_ts_ms,
        response_ts_ms: outcome.response_ts_ms,
        mode: "live",
        venue: spec.venue,
        asset: spec.asset,
        order_label: spec.label,
        side: if spec.is_buy { "buy" } else { "sell" },
        tif: spec.tif,
        reduce_only: spec.reduce_only as u8,
        requested_px: spec.px,
        requested_qty: spec.qty,
        initial_queue_qty: spec.initial_queue_qty,
        order_age_ms: 0,
        live_basis_bps: 0.0,
        remaining_queue_estimate: spec.initial_queue_qty,
        cancel_reason: "",
        ws_reconnect_count: 0,
        rest_reconcile_count: 0,
        status: &outcome.status,
        oid: outcome.oid,
        error: &outcome.error,
        raw_response: &outcome.raw,
        created_ts_ms: now_ms(),
    };
    if let Err(error) = audit.enqueue_best_effort("hfc_exec_order", &row) {
        eprintln!("audit_order_queue_failed={error:#}");
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn audit_manager_event(
    audit: &AuditClient,
    decision_id: &str,
    spec: &OrderSpec,
    oid: u64,
    submit_ts_ms: i64,
    status: &str,
    live_basis_bps: f64,
    eligible_tape_qty: f64,
    cancel_reason: &str,
    ws_reconnect_count: u32,
    rest_reconcile_count: u32,
) {
    let age_ms = now_ms().saturating_sub(submit_ts_ms) as u64;
    audit
        .insert_best_effort(
            "hfc_exec_order",
            &OrderRow {
                decision_id,
                attempt_id: decision_id,
                cloid: cloid_hex(spec.cloid),
                event_ts_ms: now_ms(),
                request_ts_ms: 0,
                response_ts_ms: 0,
                mode: "live",
                venue: spec.venue,
                asset: spec.asset,
                order_label: spec.label,
                side: if spec.is_buy { "buy" } else { "sell" },
                tif: spec.tif,
                reduce_only: spec.reduce_only as u8,
                requested_px: spec.px,
                requested_qty: spec.qty,
                initial_queue_qty: spec.initial_queue_qty,
                order_age_ms: age_ms,
                live_basis_bps,
                remaining_queue_estimate: (spec.initial_queue_qty - eligible_tape_qty).max(0.0),
                cancel_reason,
                ws_reconnect_count,
                rest_reconcile_count,
                status,
                oid,
                error: "",
                raw_response: "",
                created_ts_ms: now_ms(),
            },
        )
        .await;
}
