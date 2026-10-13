use super::*;
use crate::{api::ExecutionState, model::Book};
use std::{future::Future, pin::Pin};

type EntryReconcile<'a> = Pin<Box<dyn Future<Output = Result<(ExecutionState, Book)>> + Send + 'a>>;

#[derive(Default)]
struct EntryWatchdog<'a> {
    pending: Option<EntryReconcile<'a>>,
    fill_revision: u32,
}

impl<'a> EntryWatchdog<'a> {
    fn start(
        &mut self,
        revision: u32,
        read: impl Future<Output = Result<(ExecutionState, Book)>> + Send + 'a,
    ) {
        assert!(self.pending.is_none());
        self.fill_revision = revision;
        self.pending = Some(Box::pin(read));
    }

    async fn ready(&mut self) -> Result<(ExecutionState, Book)> {
        match self.pending.as_mut() {
            Some(read) => read.await,
            None => std::future::pending().await,
        }
    }

    fn finish(
        &mut self,
        result: Result<(ExecutionState, Book)>,
        revision: u32,
    ) -> Result<Option<(ExecutionState, Book)>> {
        self.pending = None;
        // A read begun before a processed fill/hedge can still show its unhedged
        // account state. Discard that observation; errors still revoke the order.
        result.map(|value| (revision == self.fill_revision).then_some(value))
    }
}

pub async fn open_s3(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    notional: f64,
    gate_bps: f64,
    context: ExecutionContext<'_>,
) -> Result<()> {
    let mut feed = spawn_feed(account.to_string());
    open_s3_inner(
        info,
        account,
        agent,
        confirmation,
        notional,
        gate_bps,
        context,
        &mut feed,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn open_s3_permitted(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    notional: f64,
    gate_bps: f64,
    context: ExecutionContext<'_>,
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    permit: EntryPermit,
) -> Result<()> {
    open_s3_inner(
        info,
        account,
        agent,
        confirmation,
        notional,
        gate_bps,
        context,
        feed,
        Some(permit),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn open_s3_inner(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    mut notional: f64,
    gate_bps: f64,
    mut context: ExecutionContext<'_>,
    feed: &mut tokio::sync::mpsc::UnboundedReceiver<FeedEvent>,
    permit: Option<EntryPermit>,
) -> Result<()> {
    let audit = match context.audit {
        Some(audit) => audit.clone(),
        None => AuditClient::from_env()?,
    };
    audit_entry_stage(
        &audit,
        &context,
        "",
        "open_start",
        now_ms(),
        context.entry_trigger_basis_bps,
        "",
    );
    validate_confirmation(account, confirmation)?;
    let permitted = permit.is_some();
    let before = if let Some(permit) = permit.as_ref() {
        permit.validate(&context, now_ms())?;
        permit.snapshot.clone()
    } else {
        // Recovery and full account state remain mandatory for manual/direct opens. The
        // controller fast path reaches this function only with a single-use permit issued by
        // a completed cycle.
        recover_live(info, account, agent, confirmation, &context).await?;
        audit_entry_stage(
            &audit,
            &context,
            "",
            "recovery_complete",
            now_ms(),
            context.entry_trigger_basis_bps,
            "",
        );
        let snapshot = info.snapshot(account, agent).await?;
        audit_entry_stage(
            &audit,
            &context,
            "",
            "preflight_snapshot_complete",
            now_ms(),
            context.entry_trigger_basis_bps,
            "",
        );
        snapshot
    };
    validate_open_preflight_limits(
        &before,
        context.delta_tolerance_usd,
        context.order_policy.maker_hard_timeout_ms,
    )?;
    if notional + 1e-9 < context.delta_tolerance_usd
        || notional > context.max_single_order_notional_usd + 1e-9
    {
        bail!("S3 open notional is outside configured live limits");
    }
    audit_risk_configured(&audit, &before, &context).await?;
    if !permitted {
        tokio::time::timeout(
            Duration::from_secs(EXECUTION_WS_READY_TIMEOUT_SECS),
            async {
                let mut connected = false;
                let mut spot_bbo = false;
                let mut perp_bbo = false;
                loop {
                    match feed.recv().await {
                        Some(FeedEvent::Connected) => connected = true,
                        Some(FeedEvent::Bbo { coin, .. }) if coin == "@107" => spot_bbo = true,
                        Some(FeedEvent::Bbo { coin, .. }) if coin == "HYPE" => perp_bbo = true,
                        Some(FeedEvent::Disconnected(error)) => {
                            bail!("execution websocket failed before submit: {error}")
                        }
                        Some(_) => {}
                        None => bail!("execution websocket ended before submit"),
                    }
                    if connected && spot_bbo && perp_bbo {
                        return Ok(());
                    }
                }
            },
        )
        .await
        .context("execution websocket connect timeout")??;
    }
    audit_entry_stage(
        &audit,
        &context,
        "",
        "execution_ws_ready",
        now_ms(),
        context.entry_trigger_basis_bps,
        "",
    );
    let mut permitted_market = None;
    let activation = if let Some(permit) = permit.as_ref() {
        let mut market = permit
            .market
            .clone()
            .ok_or(EntryPermitInvalid("missing WS activation books"))?;
        drain_permitted_feed(feed, &mut market)?;
        permit.validate(&context, now_ms())?;
        let activation = Snapshot {
            account: before.account.clone(),
            spot: market.spot.book.clone(),
            perp: market.perp.book.clone(),
        };
        permitted_market = Some(market);
        activation
    } else {
        info.snapshot_with_fresh_domain(account, agent).await?
    };
    audit_entry_stage(
        &audit,
        &context,
        "",
        "activation_snapshot_complete",
        now_ms(),
        context.entry_trigger_basis_bps,
        "",
    );
    if permitted {
        validate_open_activation(&activation, &context)
            .map_err(|_| EntryPermitInvalid("activation state stale or changed"))?;
    } else {
        validate_open_activation(&activation, &context)?;
    }
    let spot_px = floor_tick(activation.spot.best_bid()?);
    let requested_qty = round_hype_qty(notional, spot_px);
    let mut qty = context
        .max_order_qty_hype
        .map_or(requested_qty, |limit| requested_qty.min(floor_qty(limit)));
    if qty * spot_px < 10.0 {
        bail!("rounded S3 quantity is below minimum notional");
    }
    if let Some(config) = context.strategy_config {
        let top_basis = (activation.perp.best_bid()? / spot_px - 1.0) * 10_000.0;
        let estimate = evaluate_entry(config, &activation.account, top_basis);
        context.order_policy.entry_keep_bps = estimate.keep_gate_bps;
    }
    let activation_capacity_qty = basis_limited_capacity_qty(
        &activation.perp,
        "sell",
        spot_px,
        context.order_policy.entry_keep_bps,
    )? * context.hedge_depth_utilization;
    let activation_requested_qty = qty;
    qty = activation_qty(
        activation_requested_qty,
        activation_capacity_qty,
        spot_px,
        context.delta_tolerance_usd,
    )?;
    notional = qty * spot_px;
    if qty + 1e-9 < activation_requested_qty {
        println!(
            "{}",
            json!({
                "status":"activation_qty_resized",
                "requested_qty":activation_requested_qty,
                "resized_qty":qty,
                "notional_usd":notional
            })
        );
    }
    let quoted_basis = (walk(&activation.perp, "sell", qty)? / spot_px - 1.0) * 10_000.0;
    let mut effective_gate_bps = gate_bps;
    if context.enforce_basis {
        if let Some(config) = context.strategy_config {
            let estimate = evaluate_entry(config, &activation.account, quoted_basis);
            effective_gate_bps = estimate.create_gate_bps;
            context.order_policy.entry_create_bps = estimate.create_gate_bps;
            context.order_policy.entry_keep_bps = estimate.keep_gate_bps;
            if !estimate.eligible {
                return Err(BasisGateNotMet {
                    quoted_bps: quoted_basis,
                    gate_bps: effective_gate_bps,
                }
                .into());
            }
        }
    }
    if context.enforce_basis && quoted_basis + 1e-10 < effective_gate_bps {
        return Err(BasisGateNotMet {
            quoted_bps: quoted_basis,
            gate_bps: effective_gate_bps,
        }
        .into());
    }
    let decision_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!(
            "s3-open:{}:{notional:.2}:{gate_bps:.3}:{}",
            before.account.snapshot_ts_ms, context.run_tag,
        )
        .as_bytes(),
    )
    .to_string();
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "activation_validation_complete",
        now_ms(),
        quoted_basis,
        "",
    );
    let exchange = exchange(account).await?;
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "exchange_ready",
        now_ms(),
        quoted_basis,
        "",
    );
    let spot = OrderSpec {
        label: "s3_spot_bid_alo",
        venue: "spot",
        asset: SPOT,
        is_buy: true,
        reduce_only: false,
        tif: "Alo",
        px: spot_px,
        qty,
        initial_queue_qty: activation.spot.bid_sz.first().copied().unwrap_or(0.0),
        cloid: cloid(&decision_id, "s3_spot_bid_alo"),
    };
    audit_decision(
        &audit,
        &decision_id,
        "open",
        notional,
        qty,
        &activation,
        context,
        effective_gate_bps,
        if context.enforce_basis {
            "basis_gate_passed_at_activation"
        } else {
            "execution_probe_basis_observed_only"
        },
        Some(quoted_basis),
    )
    .await?;
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "decision_audit_queued",
        now_ms(),
        quoted_basis,
        "",
    );
    let mut active_attempt = crate::attempt_state::ActiveAttempt::begin(
        account,
        &decision_id,
        "open_s3",
        context.config_hash,
        activation.account.hype_spot_qty,
        activation.account.hype_perp_qty,
        notional,
        now_ms(),
    )?;
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "attempt_state_ready",
        now_ms(),
        quoted_basis,
        "",
    );
    if let (Some(permit), Some(market)) = (permit.as_ref(), permitted_market.as_mut()) {
        if let Err(error) = drain_permitted_feed(feed, market).and_then(|()| {
            validate_permitted_pre_submit(
                permit,
                &context,
                market,
                spot_px,
                qty,
                effective_gate_bps,
            )
        }) {
            active_attempt.finish()?;
            return Err(error);
        }
    }
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "pre_submit_revalidated",
        now_ms(),
        quoted_basis,
        "",
    );
    let outcome = match submit_one_fast(&exchange, &audit, &decision_id, &spot).await {
        Ok(outcome) => outcome,
        Err(error) => {
            tokio::time::sleep(Duration::from_millis(900)).await;
            let _ = cancel_all(info, account, confirmation).await;
            for sequence in 1..=3 {
                let _ = hedge_live_delta(
                    info,
                    &exchange,
                    &audit,
                    account,
                    agent,
                    &decision_id,
                    sequence,
                )
                .await;
            }
            return Err(error).context("spot ALO submission was uncertain; recovery attempted");
        }
    };
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "order_request",
        outcome.request_ts_ms,
        quoted_basis,
        "",
    );
    audit_entry_stage(
        &audit,
        &context,
        &decision_id,
        "order_response",
        outcome.response_ts_ms,
        quoted_basis,
        &outcome.status,
    );
    if let Err(error) = active_attempt.submitted(outcome.oid) {
        eprintln!(
            "persist_submitted_oid_failed oid={} error={error:#}",
            outcome.oid
        );
    }
    if outcome.status != "resting" && outcome.status != "filled" {
        if alo_would_immediately_match(&outcome) {
            active_attempt.finish()?;
            return Err(AloWouldMatch { flow: "entry" }.into());
        }
        bail!(
            "spot ALO was not accepted: {} {}",
            outcome.status,
            outcome.error
        );
    }
    let (risk_monitor, mut risk_revocations) = spawn_entry_risk_monitor(
        info.clone(),
        account.to_string(),
        agent.to_string(),
        RestingEntryRiskPolicy {
            warning_pmr: context.warning_pmr,
            delta_emergency_usd: context.delta_emergency_usd,
            min_agent_life_ms: context.order_policy.maker_hard_timeout_ms,
        },
        outcome.oid,
    );
    let submit_ts_ms = now_ms();
    let mut latest_spot_bid = Touch {
        px: activation.spot.best_bid()?,
        sz: activation.spot.bid_sz.first().copied().unwrap_or(0.0),
    };
    let mut latest_perp_bid = Touch {
        px: activation.perp.best_bid()?,
        sz: activation.perp.bid_sz.first().copied().unwrap_or(0.0),
    };
    let mut latest_perp_ts_ms = activation.perp.exchange_ts_ms;
    let mut live_basis_bps = quoted_basis;
    let mut filled_gross_qty: f64 = 0.0;
    let mut pending_hedge_qty: f64 = 0.0;
    let mut eligible_tape_qty: f64 = 0.0;
    let mut seen_fill_tids = HashSet::new();
    let mut seen_trade_tids = HashSet::new();
    let mut ws_reconnect_count = 0;
    let mut rest_reconcile_count = 0;
    let mut order_update_recorded = false;
    let mut next_rest_ms = submit_ts_ms + RESTING_REST_RECONCILE_MS;
    let mut reconcile_not_before_ms = 0;
    let mut cancel_reason = String::new();
    let mut sequence = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    let mut watchdog = EntryWatchdog::default();
    heartbeat.tick().await;
    'manage: loop {
        let mut reconciliation = None;
        tokio::select! {
            biased;
            revocation = risk_revocations.recv() => {
                cancel_reason = revocation.map_or_else(
                    || "risk_monitor_ended".to_string(),
                    |reason| format!("risk_revoked:{reason}"),
                );
                break 'manage;
            }
            result = watchdog.ready() => {
                rest_reconcile_count += 1;
                match watchdog.finish(result, sequence) {
                    Ok(value) => reconciliation = value,
                    Err(error) => {
                        cancel_reason = format!("rest_watchdog_failed:{error}");
                        break 'manage;
                    }
                }
            }
            event = feed.recv() => match event {
                Some(FeedEvent::Connected) => {
                    ws_reconnect_count += 1;
                }
                Some(FeedEvent::Disconnected(error)) => {
                    cancel_reason = format!("ws_disconnected:{error}");
                    break 'manage;
                }
                Some(FeedEvent::Bbo { coin, bid, exchange_ts_ms, .. }) => {
                    if let Some(bid) = bid {
                        if coin == "@107" {
                            latest_spot_bid = bid;
                        } else if coin == "HYPE" && exchange_ts_ms >= latest_perp_ts_ms {
                            latest_perp_bid = bid;
                            latest_perp_ts_ms = exchange_ts_ms;
                        }
                    }
                }
                Some(FeedEvent::UserFill {
                    oid,
                    tid,
                    qty,
                    fee,
                    fee_token,
                    exchange_ts_ms,
                    recv_ts_ms,
                    ..
                }) if oid == outcome.oid => {
                    if seen_fill_tids.insert(tid) {
                        filled_gross_qty += qty;
                        sequence += 1;
                        match hedge_spot_fill_fast(
                            &exchange,
                            &audit,
                            &decision_id,
                            sequence,
                            oid,
                            tid,
                            exchange_ts_ms,
                            recv_ts_ms,
                            true,
                            qty,
                            fee,
                            &fee_token,
                            latest_perp_bid.px,
                            &mut pending_hedge_qty,
                        )
                        .await
                        {
                            Ok(Some(hedge)) => {
                                reconcile_not_before_ms = hedge.response_ts_ms + 900;
                            }
                            Ok(None) => {}
                            Err(error) => {
                                cancel_reason = format!("hedge_failed:{error}");
                                break 'manage;
                            }
                        }
                    }
                }
                Some(FeedEvent::OrderUpdate {
                    oid,
                    status,
                    recv_ts_ms,
                    ..
                }) if oid == outcome.oid => {
                    if !order_update_recorded {
                        audit_entry_stage(
                            &audit,
                            &context,
                            &decision_id,
                            "order_update",
                            recv_ts_ms,
                            quoted_basis,
                            &status,
                        );
                        order_update_recorded = true;
                    }
                    next_rest_ms = next_rest_ms.min(recv_ts_ms + 250);
                }
                Some(FeedEvent::SpotTrade { tid, side, px, qty, .. }) => {
                    if side == "A" && px <= spot.px + 1e-10 && seen_trade_tids.insert(tid) {
                        eligible_tape_qty += qty;
                    }
                }
                Some(_) => {}
                None => {
                    cancel_reason = "ws_channel_ended".to_string();
                    break 'manage;
                }
            },
            _ = heartbeat.tick() => {}
        }

        let now = now_ms();
        let mut touch_depth_unknown = false;
        let remaining_qty = (spot.qty - filled_gross_qty).max(0.0);
        if remaining_qty > 0.0 {
            match entry_touch_basis_bps(
                &context.order_policy,
                spot.px,
                remaining_qty,
                latest_perp_bid.px,
                latest_perp_bid.sz,
            ) {
                Some(basis_bps) => live_basis_bps = basis_bps,
                None => touch_depth_unknown = true,
            }
        }

        let mut reconciled_book = false;
        if let Some((state, perp_book)) = reconciliation {
            let delta_qty = state.hype_spot_qty + state.hype_perp_qty;
            if watchdog_requires_cancel(
                state.open_order_oids.contains(&outcome.oid),
                delta_qty,
                spot.px,
            ) {
                cancel_reason = "watchdog_unhedged_delta".to_string();
                break 'manage;
            }
            if !state.open_order_oids.contains(&outcome.oid) {
                break;
            }
            filled_gross_qty = filled_gross_qty
                .max((state.hype_spot_qty - activation.account.hype_spot_qty).max(0.0));
            if perp_book.exchange_ts_ms >= latest_perp_ts_ms {
                reconciled_book = true;
                let remaining = (spot.qty - filled_gross_qty).max(0.01);
                live_basis_bps = match walk(&perp_book, "sell", remaining) {
                    Ok(px) => (px / spot.px - 1.0) * 10_000.0,
                    Err(error) => {
                        cancel_reason = format!("insufficient_hedge_depth:{error}");
                        break 'manage;
                    }
                };
                // A newer REST observation must also advance the WS touch watermark;
                // otherwise the next older BBO/tick would undo its price check.
                latest_perp_ts_ms = perp_book.exchange_ts_ms;
                latest_perp_bid = Touch {
                    px: perp_book.bid_px[0],
                    sz: perp_book.bid_sz[0],
                };
            }
        }
        if watchdog.pending.is_none() && now >= next_rest_ms && now >= reconcile_not_before_ms {
            watchdog.start(sequence, async {
                tokio::try_join!(info.execution_state(account), info.book(PERP))
            });
            next_rest_ms = now + RESTING_REST_RECONCILE_MS;
        }
        if touch_depth_unknown
            && !reconciled_book
            && now.saturating_sub(submit_ts_ms) < context.order_policy.maker_hard_timeout_ms as i64
        {
            continue;
        }

        let at_touch = (latest_spot_bid.px - spot.px).abs() < price_tick(spot.px) / 2.0;
        let age_ms = now.saturating_sub(submit_ts_ms) as u64;
        let action = managed_resting_action(
            &context.order_policy,
            context.enforce_basis,
            OrderFlow::Entry,
            live_basis_bps,
            age_ms,
            at_touch,
        );
        if action != RestingAction::Keep {
            cancel_reason = match action {
                RestingAction::CancelBasis => "live_basis_below_keep_floor",
                RestingAction::CancelOffTouch => "soft_review_off_touch",
                RestingAction::CancelHardTimeout => "hard_timeout",
                RestingAction::Keep => unreachable!(),
            }
            .to_string();
            break;
        }
    }
    // Dropping the pending read cancels it before cancellation/recovery starts.
    drop(watchdog);
    risk_monitor.abort();

    // oid is zero only on a malformed accept, which the status check above already
    // rejects; cancelling it cannot reach an order. close_s3 has carried this guard
    // since it was written.
    if outcome.oid > 0 {
        if let Some(Err(error)) = release_maker_hedging_fills(
            &exchange,
            &audit,
            feed,
            &decision_id,
            &spot,
            outcome.oid,
            !cancel_reason.is_empty(),
            &mut latest_perp_bid,
            &mut sequence,
            &mut seen_fill_tids,
            &mut filled_gross_qty,
            &mut pending_hedge_qty,
        )
        .await
        {
            eprintln!("spot_cancel_uncertain oid={} error={error:#}", outcome.oid);
        }
    }
    let mut maker_closed = cancel_reason.is_empty();
    if !cancel_reason.is_empty() && outcome.oid > 0 {
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let state = match info.execution_state(account).await {
                Ok(state) => state,
                Err(error) => {
                    eprintln!(
                        "cancel_reconcile_failed oid={} error={error:#}",
                        outcome.oid
                    );
                    continue;
                }
            };
            rest_reconcile_count += 1;
            if !state.open_order_oids.contains(&outcome.oid) {
                maker_closed = true;
                break;
            }
            let _ = cancel_oid(&exchange, &audit, &decision_id, &spot, outcome.oid).await;
        }
        if !maker_closed {
            bail!(
                "spot maker order {} remained open after cancellation retries",
                outcome.oid
            );
        }
        sequence += 1;
        if let Err(error) = hedge_live_delta(
            info,
            &exchange,
            &audit,
            account,
            agent,
            &decision_id,
            sequence,
        )
        .await
        {
            eprintln!(
                "post_cancel_hedge_failed oid={} error={error:#}",
                outcome.oid
            );
        }
    }
    let mut final_snapshot = info.snapshot(account, agent).await?;
    for _ in 0..3 {
        let mid = (final_snapshot.spot.best_bid()? + final_snapshot.spot.best_ask()?) / 2.0;
        let delta_usd =
            (final_snapshot.account.hype_spot_qty + final_snapshot.account.hype_perp_qty).abs()
                * mid;
        if delta_usd < 10.0 {
            break;
        }
        sequence += 1;
        match hedge_live_delta(
            info,
            &exchange,
            &audit,
            account,
            agent,
            &decision_id,
            sequence,
        )
        .await
        {
            Ok(snapshot) => final_snapshot = snapshot,
            Err(error) => {
                eprintln!("final_hedge_retry_failed={error:#}");
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Ok(snapshot) = info.snapshot(account, agent).await {
                    final_snapshot = snapshot;
                }
            }
        }
    }
    if final_snapshot.account.open_orders != 0 {
        let _ = cancel_all(info, account, confirmation).await;
        final_snapshot = wait_snapshot(info, account, agent).await?;
        if final_snapshot.account.open_orders != 0 {
            bail!("S3 open left an open order after cancel-all");
        }
    }
    let final_mid = (final_snapshot.spot.best_bid()? + final_snapshot.spot.best_ask()?) / 2.0;
    let residual_delta =
        (final_snapshot.account.hype_spot_qty + final_snapshot.account.hype_perp_qty).abs()
            * final_mid;
    if residual_delta >= 10.0 {
        bail!("S3 open left material delta ${residual_delta:.2}");
    }
    report_post_action_domain(
        require_clean_post_action_domain(info, account, "S3 open").await,
        "S3 open",
    )
    .await;
    audit_manager_event(
        &audit,
        &decision_id,
        &spot,
        outcome.oid,
        submit_ts_ms,
        if cancel_reason.is_empty() {
            "manager_closed"
        } else {
            "manager_cancel"
        },
        live_basis_bps,
        eligible_tape_qty,
        &cancel_reason,
        ws_reconnect_count,
        rest_reconcile_count,
    )
    .await;
    if let Err(error) = audit_risk_configured(&audit, &final_snapshot, &context).await {
        eprintln!("final_audit_risk_failed={error:#}");
    }
    // Audit-store failures must not fail an open that already traded: the error would
    // persist the attempt and wedge the next recover_live on dust, which blocks the
    // reduce path too. Mirrors the post-close handling below.
    if let Err(error) = reconcile_fills(info, account).await {
        eprintln!("post_open_reconcile_fills_failed={error:#}");
    }
    let (fill_count, fees_usd) = match decision_fill_totals(&audit, &decision_id).await {
        Ok(value) => value,
        Err(error) => {
            eprintln!("open_fill_totals_failed={error:#}");
            (0, 0.0)
        }
    };
    let fill_metrics = decision_fill_metrics(&audit, &decision_id).await.ok();
    let added_spot = final_snapshot.account.hype_spot_qty - activation.account.hype_spot_qty;
    let added_short = activation.account.hype_perp_qty - final_snapshot.account.hype_perp_qty;
    let dust_qty = (added_spot - added_short).max(0.0);
    audit
        .insert_best_effort(
            "hfc_exec_dust",
            &json!({
                "event_id":format!("{decision_id}:s3_entry_dust"),
                "decision_id":decision_id,
                "event_ts_ms":now_ms(),
                "asset":"HYPE",
                "reason":"spot_fee_net_receipt",
                "qty":dust_qty,
                "usd_value":dust_qty * final_mid,
                "status":"carried",
                "created_ts_ms":now_ms()
            }),
        )
        .await;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "decision_id":decision_id,
            "run_tag":context.run_tag,
            "quoted_basis_bps":quoted_basis,
            "requested_notional_usd":notional,
            "requested_qty":qty,
            "added_spot_qty":added_spot,
            "added_perp_short_qty":added_short,
            "residual_delta_usd":residual_delta,
            "fill_count":fill_count,
            "fees_usd":fees_usd,
            "open_orders":final_snapshot.account.open_orders
        }))?
    );
    if let Some(metrics) =
        fill_metrics.filter(|metrics| metrics.spot_qty > 0.0 && metrics.perp_qty > 0.0)
    {
        let basis_bps = metrics.basis_bps();
        let fee_bps = metrics.fee_bps();
        notify_fill_background(format!("complete:{decision_id}:open"), format!(
            "HYPE Funding Carry 加仓完成\n本次：现货买入 {:.4} HYPE @ ${:.4}｜永续卖出 {:.4} HYPE @ ${:.4}\n实际成交 basis {basis_bps:+.2}bp｜手续费 ${:.4}（{fee_bps:.2}bp）｜入场总成本 {:.2}bp\n动作后净敞口 ${residual_delta:.2}｜挂单 {}",
            metrics.spot_qty,
            metrics.spot_vwap,
            metrics.perp_qty,
            metrics.perp_vwap,
            metrics.fees_usd,
            fee_bps - basis_bps,
            final_snapshot.account.open_orders,
        ));
    }
    active_attempt.finish()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn pending_entry_rest_keeps_receiving_fills_and_discards_prehedge_state() -> Result<()> {
        use tokio::{
            sync::{mpsc, oneshot},
            time::timeout,
        };
        let (release, released) = oneshot::channel();
        let mut watchdog = EntryWatchdog::default();
        watchdog.start(0, async { released.await? });
        let (sender, mut feed) = mpsc::unbounded_channel();
        sender.send(FeedEvent::UserFill {
            oid: 42,
            tid: 1,
            coin: "@107".into(),
            px: 100.0,
            qty: 0.5,
            fee: 0.0,
            fee_token: "USDC".into(),
            exchange_ts_ms: 1,
            recv_ts_ms: 2,
        })?;
        let fill = timeout(Duration::from_millis(100), async {
            tokio::select! {
                result = watchdog.ready() => panic!("REST must still be pending: {result:?}"),
                fill = feed.recv() => fill,
            }
        })
        .await?
        .unwrap();
        assert!(matches!(fill, FeedEvent::UserFill { qty: 0.5, .. }));
        assert!(
            watchdog.pending.is_some(),
            "WS must not cancel the in-flight read"
        );
        let book = Book {
            exchange_ts_ms: 1,
            recv_ts_ms: 1,
            bid_px: vec![58.0],
            bid_sz: vec![1.0],
            ask_px: vec![58.1],
            ask_sz: vec![1.0],
        };
        release
            .send(Ok((
                crate::api::ExecutionState {
                    hype_spot_qty: 0.5,
                    hype_perp_qty: 0.0,
                    open_order_oids: vec![42],
                },
                book,
            )))
            .unwrap();
        let result = watchdog.ready().await;
        assert!(
            watchdog.finish(result, 1)?.is_none(),
            "prehedge delta must not revoke a now-hedged order"
        );
        assert!(watchdog.pending.is_none());
        watchdog.start(1, async { Err(anyhow::anyhow!("REST unavailable")) });
        let result = watchdog.ready().await;
        assert!(
            watchdog.finish(result, 2).is_err(),
            "stale reads must not hide watchdog failures"
        );
        Ok(())
    }

    #[tokio::test]
    async fn entry_rest_without_intervening_fill_keeps_account_and_depth_checks() -> Result<()> {
        let mut watchdog = EntryWatchdog::default();
        let book = Book {
            exchange_ts_ms: 1,
            recv_ts_ms: 1,
            bid_px: vec![58.0],
            bid_sz: vec![1.0],
            ask_px: vec![58.1],
            ask_sz: vec![1.0],
        };
        watchdog.start(0, async {
            Ok((
                crate::api::ExecutionState {
                    hype_spot_qty: 0.5,
                    hype_perp_qty: 0.0,
                    open_order_oids: vec![42],
                },
                book,
            ))
        });
        let result = watchdog.ready().await;
        let (state, book) = watchdog.finish(result, 0)?.unwrap();
        assert!(watchdog_requires_cancel(
            true,
            state.hype_spot_qty + state.hype_perp_qty,
            100.0
        ));
        assert_eq!(walk(&book, "sell", 0.5)?, 58.0);
        Ok(())
    }
}
