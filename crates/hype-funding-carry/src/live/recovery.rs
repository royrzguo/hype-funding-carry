use super::*;

#[allow(clippy::too_many_arguments)]
pub async fn repair(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
) -> Result<()> {
    validate_confirmation(account, confirmation)?;
    let before = info.snapshot(account, agent).await?;
    if !before.account.agent_authorized || before.account.open_orders != 0 {
        bail!("repair requires authorized agent and zero open orders");
    }
    let audit = AuditClient::from_env()?;
    audit_risk(&audit, &before).await?;
    let exchange = exchange(account).await?;
    let decision_id = Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("repair:{}", before.account.snapshot_ts_ms).as_bytes(),
    )
    .to_string();
    let qty = floor_qty(
        before
            .account
            .hype_spot_qty
            .max(-before.account.hype_perp_qty),
    );
    audit_decision(
        &audit,
        &decision_id,
        "repair",
        qty * before.spot.best_bid()?,
        qty,
        &before,
        ExecutionContext::standalone("repair"),
        0.0,
        "explicit_repair",
        None,
    )
    .await?;
    let after = cleanup_to_flat(info, &exchange, &audit, account, agent, &decision_id).await?;
    audit_risk(&audit, &after).await?;
    if let Err(error) = reconcile_fills(info, account).await {
        eprintln!("post_repair_reconcile_fills_failed={error:#}");
    }
    let residual_qty = after.account.hype_spot_qty + after.account.hype_perp_qty;
    audit
        .insert_best_effort(
            "hfc_exec_dust",
            &json!({
                "event_id":format!("{decision_id}:repair_residual"),
                "decision_id":decision_id,
                "event_ts_ms":now_ms(),
                "asset":"HYPE",
                "reason":"below_minimum_order",
                "qty":residual_qty,
                "usd_value":residual_qty.abs() * after.spot.best_bid()?,
                "status":"carried",
                "created_ts_ms":now_ms()
            }),
        )
        .await;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "decision_id":decision_id,
            "spot_qty":after.account.hype_spot_qty,
            "perp_qty":after.account.hype_perp_qty,
            "residual_delta_usd":
                (after.account.hype_spot_qty + after.account.hype_perp_qty).abs()
                    * after.spot.best_bid()?,
            "open_orders":after.account.open_orders
        }))?
    );
    report_post_action_domain(
        require_no_new_post_action_violation(info, account, &before.account.asset_domain, "repair")
            .await,
        "repair",
    )
    .await;
    Ok(())
}

pub async fn cancel_all(info: &InfoClient, account: &str, confirmation: &str) -> Result<usize> {
    validate_confirmation(account, confirmation)?;
    let orders = info.all_open_orders(account).await?;
    let exchange = exchange(account).await?;
    let mut canceled = 0;
    for order in &orders {
        let asset = if order.coin == "@107" {
            SPOT
        } else {
            &order.coin
        };
        // Recovery has already reconciled the real OID. Keep this independent of the
        // possibly failed trading WS; this cancels an observed order, never replays it.
        let response = cancel_http(
            &exchange,
            ClientCancelRequest {
                asset: asset.to_string(),
                oid: order.oid,
            },
        )
        .await?;
        println!(
            "{}",
            json!({
                "cancel_all":format!("{response:?}"),
                "dex":if order.dex.is_empty() { "main" } else { &order.dex },
                "coin":order.coin,
                "oid":order.oid,
            })
        );
        canceled += 1;
    }
    if !info.all_open_orders(account).await?.is_empty() {
        bail!("cancel-all left open orders");
    }
    let scan = info.asset_domain_scan(account).await;
    if !scan.incomplete_reads.is_empty() || !scan.unexpected_orders.is_empty() {
        bail!(
            "cancel-all could not prove the full order domain is empty: {}",
            serde_json::to_string(&scan)
                .unwrap_or_else(|_| "asset-domain-serialization-failed".to_string())
        );
    }
    Ok(canceled)
}

pub async fn recover(info: &InfoClient, account: &str, agent: &str) -> Result<()> {
    let snapshot = info.snapshot(account, agent).await?;
    let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
    let delta_usd = (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
    let state = if snapshot.account.open_orders > 0 {
        "open_orders"
    } else if snapshot.account.hype_spot_qty.abs() * mid < 10.0
        && snapshot.account.hype_perp_qty.abs() * mid < 10.0
    {
        "flat_or_dust"
    } else if delta_usd < 10.0 {
        "hedged"
    } else {
        "unhedged"
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "state":state,
            "spot_qty":snapshot.account.hype_spot_qty,
            "perp_qty":snapshot.account.hype_perp_qty,
            "residual_delta_usd":delta_usd,
            "open_orders":snapshot.account.open_orders,
            "action":"none"
        }))?
    );
    Ok(())
}

async fn reconcile_recovery_fills(info: &InfoClient, account: &str) {
    if let Err(error) = AuditClient::flush_background(Duration::from_secs(10)) {
        eprintln!("recovery_audit_flush_failed={error:#}");
    }
    if let Err(error) = reconcile_fills(info, account).await {
        eprintln!("recovery_reconcile_fills_failed={error:#}");
    }
}

pub async fn recover_live(
    info: &InfoClient,
    account: &str,
    agent: &str,
    confirmation: &str,
    context: &ExecutionContext<'_>,
) -> Result<()> {
    validate_confirmation(account, confirmation)?;
    reconcile_recovery_fills(info, account).await;
    let active = crate::attempt_state::load(account)?;
    let audit = AuditClient::from_env()?;
    let exchange = exchange(account).await?;
    let initial_orders = info.open_orders(account).await?;
    if !initial_orders
        .as_array()
        .context("openOrders response is not an array")?
        .is_empty()
    {
        cancel_all(info, account, confirmation).await?;
    }
    if let Some(attempt) = &active {
        let objective = crate::attempt_state::recovery_objective(&attempt.action)?;
        if attempt.config_hash != context.config_hash {
            eprintln!(
                "active_attempt_config_changed old={} new={}",
                attempt.config_hash, context.config_hash
            );
        }
        if objective == crate::attempt_state::RecoveryObjective::Flat {
            for _ in 0..3 {
                let snapshot = info.snapshot(account, agent).await?;
                let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
                if snapshot.account.hype_spot_qty.abs() * mid < context.delta_tolerance_usd
                    && snapshot.account.hype_perp_qty.abs() * mid < context.delta_tolerance_usd
                {
                    crate::attempt_state::clear_recovered(account)?;
                    reconcile_recovery_fills(info, account).await;
                    return Ok(());
                }
                cleanup_to_flat(info, &exchange, &audit, account, agent, &attempt.attempt_id)
                    .await?;
            }
            bail!("emergency close recovery did not reach flat state");
        }
    }
    let mut hedge_sequence = 0;
    for _ in 0..6 {
        let state = info.execution_state(account).await?;
        if !state.open_order_oids.is_empty() {
            cancel_all(info, account, confirmation).await?;
            continue;
        }
        let snapshot = info.snapshot(account, agent).await?;
        let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
        let delta_usd =
            (snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty).abs() * mid;
        if snapshot.account.unknown_exposure && active.is_none() && state.open_order_oids.is_empty()
        {
            bail!("live recovery found unknown exposure without a known active attempt");
        }
        match crate::attempt_state::recovery_step(
            active.is_some(),
            state.open_order_oids.len(),
            delta_usd,
            context.delta_tolerance_usd,
        ) {
            crate::attempt_state::RecoveryStep::CancelOrders => unreachable!(),
            crate::attempt_state::RecoveryStep::HedgeDelta => {
                let snapshot = info.snapshot_with_fresh_domain(account, agent).await?;
                let mid = (snapshot.spot.best_bid()? + snapshot.spot.best_ask()?) / 2.0;
                let unique = uniquely_identifiable_missing_leg(
                    snapshot.account.hype_spot_qty,
                    snapshot.account.hype_perp_qty,
                    mid,
                    context.delta_tolerance_usd,
                    state.open_order_oids.is_empty(),
                    snapshot.account.asset_domain.allows(DomainAction::Hedge {
                        uniquely_identified: true,
                    }),
                )
                .is_some();
                require_domain(
                    &snapshot,
                    DomainAction::Hedge {
                        uniquely_identified: unique,
                    },
                    "live recovery hedge",
                )?;
                hedge_sequence += 1;
                if hedge_sequence > 3 {
                    bail!("live recovery exceeded three hedge attempts");
                }
                hedge_live_delta(
                    info,
                    &exchange,
                    &audit,
                    account,
                    agent,
                    active
                        .as_ref()
                        .map_or("startup_recovery", |attempt| attempt.attempt_id.as_str()),
                    hedge_sequence,
                )
                .await?;
            }
            crate::attempt_state::RecoveryStep::HaltDust => {
                audit
                    .insert_best_effort(
                        "hfc_exec_dust",
                        &json!({
                            "event_id":format!("{}:recovery_dust", active.as_ref().map_or("startup", |attempt| attempt.attempt_id.as_str())),
                            "decision_id":active.as_ref().map_or("startup", |attempt| attempt.attempt_id.as_str()),
                            "event_ts_ms":now_ms(),
                            "asset":"HYPE",
                            "reason":"interrupted_below_minimum_order",
                            "qty":snapshot.account.hype_spot_qty + snapshot.account.hype_perp_qty,
                            "usd_value":delta_usd,
                            "status":"carried_after_interrupted_attempt",
                            "created_ts_ms":now_ms()
                        }),
                    )
                    .await;
                // Residual under the exchange minimum cannot be traded away, so refusing to
                // continue fixes nothing and takes the PMR ladder and the reduce path down
                // with it. Anything that grows to the minimum is hedged by the branch above.
                // A completed open already carries the same dust forward.
                crate::attempt_state::clear_recovered(account)?;
                reconcile_recovery_fills(info, account).await;
                return Ok(());
            }
            crate::attempt_state::RecoveryStep::ClearAttempt => {
                crate::attempt_state::clear_recovered(account)?;
                reconcile_recovery_fills(info, account).await;
                return Ok(());
            }
            crate::attempt_state::RecoveryStep::Ready => {
                reconcile_recovery_fills(info, account).await;
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(900)).await;
    }
    bail!("live recovery did not converge")
}
