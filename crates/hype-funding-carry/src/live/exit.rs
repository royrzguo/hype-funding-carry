use super::*;

pub async fn close_s3(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    mode: ExitMode,
    notional: Option<f64>,
    context: ExecutionContext<'_>,
) -> Result<()> {
    validate_confirmation(account, confirmation)?;
    if mode == ExitMode::Emergency && notional.is_some() {
        bail!("emergency close always closes the full HYPE Funding Carry position");
    }

    if let Err(error) = reconcile_fills(info, account).await {
        eprintln!("pre_close_reconcile_fills_failed={error:#}");
    }
    recover_live(info, account, agent, confirmation, &context).await?;
    let mut before = info.snapshot(account, agent).await?;
    if mode == ExitMode::Emergency && before.account.open_orders != 0 {
        cancel_all(info, account, confirmation).await?;
        before = wait_snapshot(info, account, agent).await?;
    }

    let audit = AuditClient::from_env()?;
    if let Err(error) = audit_risk_configured(&audit, &before, &context).await {
        eprintln!("pre_close_audit_risk_failed={error:#}");
    }
    if mode == ExitMode::Emergency {
        validate_emergency_close_preflight(&before)?;
        let qty = floor_qty(
            before
                .account
                .hype_spot_qty
                .max(-before.account.hype_perp_qty),
        );
        let decision_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("emergency-close:{}", before.account.snapshot_ts_ms).as_bytes(),
        )
        .to_string();
        if let Err(error) = audit_decision(
            &audit,
            &decision_id,
            "close",
            qty * before.spot.best_bid()?,
            qty,
            &before,
            ExecutionContext {
                run_tag: mode.run_tag(),
                ..context
            },
            0.0,
            "explicit_emergency_exit",
            Some((before.perp.best_ask()? / before.spot.best_bid()? - 1.0) * 10_000.0),
        )
        .await
        {
            eprintln!("emergency_close_audit_decision_failed={error:#}");
        }
        let exchange = exchange(account).await?;
        let active_attempt = crate::attempt_state::ActiveAttempt::begin(
            account,
            &decision_id,
            "emergency_close",
            context.config_hash,
            before.account.hype_spot_qty,
            before.account.hype_perp_qty,
            qty * before.spot.best_bid()?,
            now_ms(),
        )?;
        let after = cleanup_to_flat(info, &exchange, &audit, account, agent, &decision_id).await?;
        if let Err(error) = audit_risk_configured(&audit, &after, &context).await {
            eprintln!("final_audit_risk_failed={error:#}");
        }
        if let Err(error) = reconcile_fills(info, account).await {
            eprintln!("post_emergency_reconcile_fills_failed={error:#}");
        }
        let (fill_count, fees_usd) = match decision_fill_totals(&audit, &decision_id).await {
            Ok(value) => value,
            Err(error) => {
                eprintln!("emergency_fill_totals_failed={error:#}");
                (0, 0.0)
            }
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "decision_id":decision_id,
                "mode":mode.run_tag(),
                "closed_spot_qty":before.account.hype_spot_qty - after.account.hype_spot_qty,
                "reduced_perp_short_qty":after.account.hype_perp_qty - before.account.hype_perp_qty,
                "remaining_spot_qty":after.account.hype_spot_qty,
                "remaining_perp_qty":after.account.hype_perp_qty,
                "fill_count":fill_count,
                "fees_usd":fees_usd,
                "open_orders":after.account.open_orders
            }))?
        );
        report_post_action_domain(
            require_no_new_post_action_violation(
                info,
                account,
                &before.account.asset_domain,
                "emergency close",
            )
            .await,
            "emergency close",
        )
        .await;
        active_attempt.finish()?;
        return Ok(());
    }

    validate_close_preflight_limits(&before, context.delta_tolerance_usd)?;
    let flow = mode.flow().context("missing maker exit flow")?;
    let qty = close_target_qty(&before, notional)?;
    if context.force_taker_exit || before.account.portfolio_margin_ratio >= context.taker_reduce_pmr
    {
        let decision_id = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("risk-close:{}:{qty:.2}", before.account.snapshot_ts_ms).as_bytes(),
        )
        .to_string();
        if let Err(error) = audit_decision(
            &audit,
            &decision_id,
            "close",
            qty * before.spot.best_bid()?,
            qty,
            &before,
            ExecutionContext {
                run_tag: "risk_exit",
                ..context
            },
            0.0,
            "pmr_at_or_above_emergency_threshold",
            Some((before.perp.best_ask()? / before.spot.best_bid()? - 1.0) * 10_000.0),
        )
        .await
        {
            eprintln!("risk_close_audit_decision_failed={error:#}");
        }
        let exchange = exchange(account).await?;
        let active_attempt = crate::attempt_state::ActiveAttempt::begin(
            account,
            &decision_id,
            "pmr_taker_reduce",
            context.config_hash,
            before.account.hype_spot_qty,
            before.account.hype_perp_qty,
            qty * before.spot.best_bid()?,
            now_ms(),
        )?;
        let after =
            close_matched_taker(info, &exchange, &audit, account, agent, &decision_id, qty).await?;
        let residual_delta = (after.account.hype_spot_qty + after.account.hype_perp_qty).abs()
            * after.spot.best_bid()?;
        if residual_delta >= 10.0 {
            bail!("risk close left material delta ${residual_delta:.2}");
        }
        if let Err(error) = reconcile_fills(info, account).await {
            eprintln!("post_risk_close_reconcile_fills_failed={error:#}");
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "decision_id":decision_id,
                "mode":"risk_exit",
                "trigger":format!("pmr_at_or_above_{:.2}", context.taker_reduce_pmr),
                "requested_qty":qty,
                "closed_spot_qty":before.account.hype_spot_qty - after.account.hype_spot_qty,
                "reduced_perp_short_qty":after.account.hype_perp_qty - before.account.hype_perp_qty,
                "remaining_spot_qty":after.account.hype_spot_qty,
                "remaining_perp_qty":after.account.hype_perp_qty,
                "residual_delta_usd":residual_delta,
                "open_orders":after.account.open_orders
            }))?
        );
        report_post_action_domain(
            require_no_new_post_action_violation(
                info,
                account,
                &before.account.asset_domain,
                "PMR risk close",
            )
            .await,
            "PMR risk close",
        )
        .await;
        active_attempt.finish()?;
        return Ok(());
    }
    validate_maker_close_agent_lifetime(
        &before,
        context.order_policy.maker_hard_timeout_ms,
        now_ms(),
    )?;
    let spot_limit_px = ceil_tick(before.spot.best_ask()?);
    let mut quoted_basis = (walk(&before.perp, "buy", qty)? / spot_limit_px - 1.0) * 10_000.0;
    let gate_bps = create_gate_bps(&context.order_policy, flow);
    if context.enforce_basis && !create_allowed(&context.order_policy, flow, quoted_basis) {
        return Err(BasisGateNotMet {
            quoted_bps: quoted_basis,
            gate_bps,
        }
        .into());
    }

    let decision_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!(
            "s3-close:{}:{qty:.2}:{}",
            before.account.snapshot_ts_ms,
            mode.run_tag(),
        )
        .as_bytes(),
    )
    .to_string();
    let exchange = exchange(account).await?;
    let mut spot = OrderSpec {
        label: "s3_exit_spot_ask_alo",
        venue: "spot",
        asset: SPOT,
        is_buy: false,
        reduce_only: false,
        tif: "Alo",
        px: spot_limit_px,
        qty,
        initial_queue_qty: before.spot.ask_sz.first().copied().unwrap_or(0.0),
        cloid: cloid(&decision_id, "s3_exit_spot_ask_alo"),
    };
    let mut feed = spawn_feed(account.to_string());
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
    let activation = info.snapshot(account, agent).await?;
    validate_close_preflight_limits(&activation, context.delta_tolerance_usd)?;
    let activation_spot_px = ceil_tick(activation.spot.best_ask()?);
    if (activation_spot_px - spot.px).abs() >= price_tick(spot.px) / 2.0 {
        spot.px = activation_spot_px;
    }
    quoted_basis = (walk(&activation.perp, "buy", qty)? / spot.px - 1.0) * 10_000.0;
    if context.enforce_basis && !create_allowed(&context.order_policy, flow, quoted_basis) {
        return Err(BasisGateNotMet {
            quoted_bps: quoted_basis,
            gate_bps,
        }
        .into());
    }
    spot.initial_queue_qty = activation.spot.ask_sz.first().copied().unwrap_or(0.0);
    if let Err(error) = audit_decision(
        &audit,
        &decision_id,
        "close",
        qty * spot_limit_px,
        qty,
        &activation,
        context,
        gate_bps,
        if context.enforce_basis {
            "basis_gate_passed_at_activation"
        } else {
            "execution_probe_basis_observed_only"
        },
        Some(quoted_basis),
    )
    .await
    {
        eprintln!("close_audit_decision_failed={error:#}");
    }
    let submit_ts_ms = now_ms();
    validate_maker_close_agent_lifetime(
        &activation,
        context.order_policy.maker_hard_timeout_ms,
        submit_ts_ms,
    )?;
    let mut active_attempt = crate::attempt_state::ActiveAttempt::begin(
        account,
        &decision_id,
        "close_s3",
        context.config_hash,
        before.account.hype_spot_qty,
        before.account.hype_perp_qty,
        qty * spot_limit_px,
        now_ms(),
    )?;
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
            return Err(error)
                .context("exit spot ALO submission was uncertain; recovery attempted");
        }
    };
    if let Err(error) = active_attempt.submitted(outcome.oid) {
        eprintln!(
            "persist_submitted_oid_failed oid={} error={error:#}",
            outcome.oid
        );
    }
    if outcome.status != "resting" && outcome.status != "filled" {
        if alo_would_immediately_match(&outcome) {
            active_attempt.finish()?;
            return Err(AloWouldMatch { flow: "exit" }.into());
        }
        bail!(
            "exit spot ALO was not accepted: {} {}",
            outcome.status,
            outcome.error
        );
    }

    let mut latest_spot_ask = Touch {
        px: activation.spot.best_ask()?,
        sz: activation.spot.ask_sz.first().copied().unwrap_or(0.0),
    };
    let mut latest_perp_ask = Touch {
        px: activation.perp.best_ask()?,
        sz: activation.perp.ask_sz.first().copied().unwrap_or(0.0),
    };
    let mut live_basis_bps = quoted_basis;
    let mut filled_gross_qty: f64 = 0.0;
    let mut pending_hedge_qty: f64 = 0.0;
    let mut eligible_tape_qty: f64 = 0.0;
    let mut seen_fill_tids = HashSet::new();
    let mut seen_trade_tids = HashSet::new();
    let mut ws_reconnect_count = 0;
    let mut rest_reconcile_count = 0;
    let mut next_rest_ms = submit_ts_ms + RESTING_REST_RECONCILE_MS;
    let mut next_risk_ms = submit_ts_ms + 30_000;
    let mut reconcile_not_before_ms = 0;
    let mut cancel_reason = String::new();
    let mut emergency_escalation = false;
    let mut sequence = 0;
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    heartbeat.tick().await;
    'manage: loop {
        let mut force_reconcile = false;
        tokio::select! {
            _ = heartbeat.tick() => {}
            event = feed.recv() => match event {
                Some(FeedEvent::Connected) => ws_reconnect_count += 1,
                Some(FeedEvent::Disconnected(error)) => {
                    cancel_reason = format!("ws_disconnected:{error}");
                    break 'manage;
                }
                Some(FeedEvent::Bbo { coin, ask, .. }) => {
                    if let Some(ask) = ask {
                        if coin == "@107" {
                            latest_spot_ask = ask;
                        } else if coin == "HYPE" {
                            latest_perp_ask = ask;
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
                            false,
                            qty,
                            fee,
                            &fee_token,
                            latest_perp_ask.px,
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
                Some(FeedEvent::OrderUpdate { oid, .. }) if oid == outcome.oid => {
                    force_reconcile = true;
                }
                Some(FeedEvent::SpotTrade { tid, side, px, qty, .. }) => {
                    if side == "B" && px + 1e-10 >= spot.px && seen_trade_tids.insert(tid) {
                        eligible_tape_qty += qty;
                    }
                }
                Some(_) => {}
                None => {
                    cancel_reason = "ws_channel_ended".to_string();
                    break 'manage;
                }
            }
        }

        let now = now_ms();
        if now.saturating_sub(submit_ts_ms) as u64 >= context.order_policy.maker_hard_timeout_ms {
            cancel_reason = "hard_timeout".to_string();
            break;
        }
        let mut touch_depth_unknown = false;
        let remaining_qty = (spot.qty - filled_gross_qty).max(0.0);
        if remaining_qty > 0.0 {
            match exit_touch_basis_bps(
                &context.order_policy,
                flow,
                spot.px,
                remaining_qty,
                latest_perp_ask.px,
                latest_perp_ask.sz,
            ) {
                Some(basis_bps) => live_basis_bps = basis_bps,
                None => touch_depth_unknown = true,
            }
        }

        let mut reconciled_book = false;
        if (force_reconcile || now >= next_rest_ms) && now >= reconcile_not_before_ms {
            let (mut state, perp_book) =
                match tokio::try_join!(info.execution_state(account), info.book(PERP),) {
                    Ok(value) => value,
                    Err(error) => {
                        cancel_reason = format!("rest_watchdog_failed:{error}");
                        break 'manage;
                    }
                };
            reconciled_book = true;
            rest_reconcile_count += 1;
            next_rest_ms = now + RESTING_REST_RECONCILE_MS;
            let delta_qty = state.hype_spot_qty + state.hype_perp_qty;
            if delta_qty.abs() * spot.px >= 10.0 {
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
                    cancel_reason = format!("watchdog_hedge_failed:{error}");
                    break 'manage;
                }
                state = match info.execution_state(account).await {
                    Ok(state) => state,
                    Err(error) => {
                        cancel_reason = format!("post_hedge_reconcile_failed:{error}");
                        break 'manage;
                    }
                };
                rest_reconcile_count += 1;
            }
            if !state.open_order_oids.contains(&outcome.oid) {
                break;
            }
            filled_gross_qty = filled_gross_qty
                .max((activation.account.hype_spot_qty - state.hype_spot_qty).max(0.0));
            let remaining = (spot.qty - filled_gross_qty).max(0.01);
            live_basis_bps = match walk(&perp_book, "buy", remaining) {
                Ok(px) => (px / spot.px - 1.0) * 10_000.0,
                Err(error) => {
                    cancel_reason = format!("insufficient_hedge_depth:{error}");
                    break 'manage;
                }
            };
        }
        if touch_depth_unknown && !reconciled_book {
            continue;
        }

        if now >= next_risk_ms {
            let risk_snapshot = match info.snapshot(account, agent).await {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    cancel_reason = format!("risk_snapshot_failed:{error}");
                    break 'manage;
                }
            };
            if let Err(error) = audit_risk_configured(&audit, &risk_snapshot, &context).await {
                eprintln!("audit_risk_failed={error:#}");
            }
            if !risk_snapshot.account.agent_authorized {
                cancel_reason = "agent_unauthorized".to_string();
                break;
            }
            if risk_snapshot.account.portfolio_margin_ratio >= context.taker_reduce_pmr {
                cancel_reason = "emergency_pmr".to_string();
                emergency_escalation = true;
                break;
            }
            next_risk_ms = now + 30_000;
        }

        let at_touch = (latest_spot_ask.px - spot.px).abs() < price_tick(spot.px) / 2.0;
        let age_ms = now.saturating_sub(submit_ts_ms) as u64;
        let action = managed_resting_action(
            &context.order_policy,
            context.enforce_basis,
            flow,
            live_basis_bps,
            age_ms,
            at_touch,
        );
        if action != RestingAction::Keep {
            cancel_reason = match action {
                RestingAction::CancelBasis => "live_basis_above_keep_ceiling",
                RestingAction::CancelOffTouch => "soft_review_off_touch",
                RestingAction::CancelHardTimeout => "hard_timeout",
                RestingAction::Keep => unreachable!(),
            }
            .to_string();
            break;
        }
    }

    if outcome.oid > 0 {
        if let Some(Err(error)) = release_maker_hedging_fills(
            &exchange,
            &audit,
            &mut feed,
            &decision_id,
            &spot,
            outcome.oid,
            !cancel_reason.is_empty(),
            &mut latest_perp_ask,
            &mut sequence,
            &mut seen_fill_tids,
            &mut filled_gross_qty,
            &mut pending_hedge_qty,
        )
        .await
        {
            eprintln!(
                "exit_spot_cancel_uncertain oid={} error={error:#}",
                outcome.oid
            );
        }
    }
    if !cancel_reason.is_empty() && outcome.oid > 0 {
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
                "post_exit_cancel_hedge_failed oid={} error={error:#}",
                outcome.oid
            );
        }
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let state = match info.execution_state(account).await {
                Ok(state) => state,
                Err(error) => {
                    eprintln!(
                        "exit_cancel_reconcile_failed oid={} error={error:#}",
                        outcome.oid
                    );
                    continue;
                }
            };
            rest_reconcile_count += 1;
            if !state.open_order_oids.contains(&outcome.oid) {
                break;
            }
            let _ = cancel_oid(&exchange, &audit, &decision_id, &spot, outcome.oid).await;
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
                eprintln!("final_exit_hedge_retry_failed={error:#}");
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
            bail!("S3 close left an open order after cancel-all");
        }
    }
    if emergency_escalation {
        let closed_spot = before.account.hype_spot_qty - final_snapshot.account.hype_spot_qty;
        let remaining_target = floor_qty((qty - closed_spot).max(0.0));
        final_snapshot = close_matched_taker(
            info,
            &exchange,
            &audit,
            account,
            agent,
            &decision_id,
            remaining_target,
        )
        .await?;
    }
    let final_mid = (final_snapshot.spot.best_bid()? + final_snapshot.spot.best_ask()?) / 2.0;
    let residual_delta =
        (final_snapshot.account.hype_spot_qty + final_snapshot.account.hype_perp_qty).abs()
            * final_mid;
    if residual_delta >= 10.0 {
        bail!("S3 close left material delta ${residual_delta:.2}");
    }
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
    if let Err(error) = reconcile_fills(info, account).await {
        eprintln!("post_close_reconcile_fills_failed={error:#}");
    }
    let (fill_count, fees_usd) = match decision_fill_totals(&audit, &decision_id).await {
        Ok(value) => value,
        Err(error) => {
            eprintln!("close_fill_totals_failed={error:#}");
            (0, 0.0)
        }
    };
    let fill_metrics = decision_fill_metrics(&audit, &decision_id).await.ok();
    let closed_spot = before.account.hype_spot_qty - final_snapshot.account.hype_spot_qty;
    let reduced_short = final_snapshot.account.hype_perp_qty - before.account.hype_perp_qty;
    audit
        .insert_best_effort(
            "hfc_exec_dust",
            &json!({
                "event_id":format!("{decision_id}:s3_exit_residual"),
                "decision_id":decision_id,
                "event_ts_ms":now_ms(),
                "asset":"HYPE",
                "reason":"below_minimum_order",
                "qty":final_snapshot.account.hype_spot_qty + final_snapshot.account.hype_perp_qty,
                "usd_value":residual_delta,
                "status":"carried",
                "created_ts_ms":now_ms()
            }),
        )
        .await;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "decision_id":decision_id,
            "mode":mode.run_tag(),
            "quoted_basis_bps":quoted_basis,
            "gate_bps":gate_bps,
            "requested_qty":qty,
            "closed_spot_qty":closed_spot,
            "reduced_perp_short_qty":reduced_short,
            "remaining_spot_qty":final_snapshot.account.hype_spot_qty,
            "remaining_perp_qty":final_snapshot.account.hype_perp_qty,
            "residual_delta_usd":residual_delta,
            "cancel_reason":cancel_reason,
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
        notify_fill_background(format!("complete:{decision_id}:close"), format!(
            "HYPE Funding Carry 减仓完成\n本次：现货卖出 {:.4} HYPE @ ${:.4}｜永续买入 {:.4} HYPE @ ${:.4}\n实际成交 basis {basis_bps:+.2}bp｜手续费 ${:.4}（{fee_bps:.2}bp）｜退出总成本 {:.2}bp\n动作后净敞口 ${residual_delta:.2}｜挂单 {}",
            metrics.spot_qty,
            metrics.spot_vwap,
            metrics.perp_qty,
            metrics.perp_vwap,
            metrics.fees_usd,
            fee_bps + basis_bps,
            final_snapshot.account.open_orders,
        ));
    }
    report_post_action_domain(
        require_no_new_post_action_violation(
            info,
            account,
            &before.account.asset_domain,
            "S3 close",
        )
        .await,
        "S3 close",
    )
    .await;
    active_attempt.finish()?;
    Ok(())
}
