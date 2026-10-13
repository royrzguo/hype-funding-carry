#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::AccountSnapshot;
    use crate::asset_domain::AssetDomainScan;

    #[tokio::test]
    async fn confirmed_fill_hedge_uses_http_once_while_ws_cancel_is_pending() -> Result<()> {
        use futures_util::{SinkExt, StreamExt};
        use tokio::{net::TcpListener, sync::oneshot, time::timeout};
        use tokio_tungstenite::{accept_async, tungstenite::Message};

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let (received, receipt) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0).await.unwrap();
            let request = loop {
                let Message::Text(text) = ws.next().await.unwrap().unwrap() else {continue};
                let request:Value = serde_json::from_str(&text).unwrap();
                if request["method"] == "post" {break request;}
            };
            assert_eq!(request["request"]["payload"]["type"], "cancel");
            received.send(()).unwrap();
            released.await.unwrap();
            ws.send(Message::Text(json!({"channel":"post","data":{"id":request["id"],"response":{"type":"action","payload":{"cancel":"accepted"}}}}).to_string())).await.unwrap();
            let _ = timeout(Duration::from_millis(100), async {
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let request:Value = serde_json::from_str(&text).unwrap();
                    assert_ne!(request["method"], "post", "unsent hedge must not reach WS later");
                }
            }).await;
        });
        let client = Arc::new(crate::ws_post::WsPost::connect(&url).await?);
        let cancel_client = client.clone();
        let cancel = tokio::spawn(async move {cancel_client.post("action", json!({"type":"cancel"})).await});
        receipt.await?;
        let ws_result = client.post("action", json!({"type":"order","tif":"Ioc"})).await.context("submit confirmed perp hedge");
        assert!(ws_result.as_ref().unwrap_err().downcast_ref::<crate::ws_post::NotSent>().is_some());
        let mut http_calls = 0;
        let outcome = timeout(Duration::from_millis(50), confirmed_hedge_after_ws(ws_result, async {
            http_calls += 1;
            Ok(json!({"status":"filled"}))
        })).await??;
        assert_eq!(outcome["status"], "filled");
        assert_eq!(http_calls, 1);
        // HTTP finished before the cancellation was acknowledged.
        release.send(()).unwrap();
        assert_eq!(cancel.await??["cancel"], "accepted");
        server.await?;
        Ok(())
    }

    #[tokio::test]
    async fn confirmed_hedge_unknown_ack_and_timeout_never_use_http() -> Result<()> {
        use futures_util::StreamExt;
        use tokio::{net::TcpListener, time::timeout};
        use tokio_tungstenite::{accept_async, tungstenite::Message};

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("ws://{}", listener.local_addr()?);
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0).await.unwrap();
            loop {
                let Message::Text(text) = ws.next().await.unwrap().unwrap() else {continue};
                let request:Value = serde_json::from_str(&text).unwrap();
                if request["method"] == "post" {break;}
            }
            // Order bytes arrived, but no acknowledgement: its outcome is unknown.
        });
        let client = crate::ws_post::WsPost::connect(&url).await?;
        let unknown = client.post("action", json!({"type":"order"})).await;
        let error = unknown.as_ref().unwrap_err();
        assert!(error.downcast_ref::<crate::ws_post::NotSent>().is_none());
        let mut http_calls = 0;
        assert!(confirmed_hedge_after_ws(unknown, async {http_calls += 1; Ok(json!({}))}).await.is_err());
        let elapsed = timeout(Duration::ZERO, std::future::pending::<()>()).await.unwrap_err();
        assert!(confirmed_hedge_after_ws::<Value>(Err(elapsed.into()), async {http_calls += 1; Ok(json!({}))}).await.is_err());
        assert_eq!(confirmed_hedge_after_ws(Ok(json!({"status":"filled"})), async {http_calls += 1; Ok(json!({}))}).await?["status"], "filled");
        assert_eq!(http_calls, 0);
        server.await?;
        Ok(())
    }


    #[tokio::test]
    async fn local_audit_preserves_partial_fill_basis_and_fee_totals() {
        let directory = std::env::temp_dir().join(format!("hfc-execution-{}", Uuid::new_v4()));
        let audit = AuditClient::open(&directory).unwrap();
        for (tid, venue, side, qty, px) in [
            (1, "spot", "buy", 0.4, 100.0),
            (2, "spot", "buy", 0.6, 100.02),
            (3, "perp", "sell", 0.4, 100.1),
            (4, "perp", "sell", 0.6, 100.12),
        ] {
            let row = serde_json::json!({
                "decision_id":"paired-entry","cloid":format!("c{tid}"),
                "fill_ts_ms":tid,"tid":tid,"venue":venue,"side":side,
                "qty":qty,"px":px,"fee_usd":0.01
            });
            audit.insert("hfc_exec_fill", &row).await.unwrap();
            audit.insert("hfc_exec_fill", &row).await.unwrap();
        }
        audit.insert("hfc_exec_fill", &serde_json::json!({
            "decision_id":"other","cloid":"other","fill_ts_ms":5,"tid":5,
            "venue":"spot","side":"sell","qty":10.0,"px":50.0,"fee_usd":9.0
        })).await.unwrap();
        let metrics = decision_fill_metrics(&audit, "paired-entry").await.unwrap();
        assert!((metrics.spot_qty - 1.0).abs() < 1e-9);
        assert!((metrics.perp_qty - 1.0).abs() < 1e-9);
        assert!((metrics.spot_vwap - 100.012).abs() < 1e-9);
        assert!((metrics.perp_vwap - 100.112).abs() < 1e-9);
        assert!((metrics.basis_bps() - 0.1 / 100.012 * 10_000.0).abs() < 1e-9);
        let (count, fees) = decision_fill_totals(&audit, "paired-entry").await.unwrap();
        assert_eq!(count, 4);
        assert!((fees - 0.04).abs() < 1e-9);
    }

    // `close --execute` runs without a config file, so these defaults decide real manual
    // exits. They may be stricter than production but must never be looser.
    #[test]
    fn standalone_defaults_are_not_looser_than_production_risk_policy() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../config/strategy.example.json");
        let config = crate::config::StrategyConfig::load(&path).unwrap();
        let context = ExecutionContext::standalone("test");
        assert!(context.warning_pmr <= config.risk.warning_pmr);
        assert!(context.maker_reduce_pmr <= config.risk.maker_reduce_pmr);
        assert!(context.taker_reduce_pmr <= config.risk.taker_reduce_pmr);
        assert!(context.hedge_depth_utilization <= config.hedge_depth_utilization);
        assert!(context.max_single_order_notional_usd <= config.max_single_order_notional_usd);
    }

    fn snapshot(spot_qty: f64, perp_qty: f64, open_orders: u32) -> Snapshot {
        let book = crate::model::Book {
            exchange_ts_ms: 1_000,
            recv_ts_ms: 1_000,
            bid_px: vec![58.0],
            bid_sz: vec![100.0],
            ask_px: vec![58.01],
            ask_sz: vec![100.0],
        };
        Snapshot {
            account: AccountSnapshot {
                snapshot_ts_ms: 1_000,
                account_value_usd: 10_100.0,
                account_value_ts_ms: 1_000,
                portfolio_margin_enabled: true,
                portfolio_margin_ratio: 0.0,
                available_after_maintenance_usdc: 10_000.0,
                usdc_total: 10_000.0,
                usdc_borrowed: 0.0,
                hype_spot_qty: spot_qty,
                hype_perp_qty: perp_qty,
                liquidation_px: 0.0,
                funding_24h_bps: 2.0,
                funding_24h_all_negative: false,
                predicted_next_funding_rate: 0.0000125,
                next_funding_time_ms: 3_600_000,
                open_orders,
                unknown_exposure: false,
                asset_domain: AssetDomainScan {
                    domain_clean: true,
                    ..AssetDomainScan::default()
                },
                agent_authorized: true,
                agent_valid_until_ms: 10_000,
                perp_taker_fee_bps: 4.5,
                perp_maker_fee_bps: 1.5,
                spot_taker_fee_bps: 7.0,
                spot_maker_fee_bps: 4.0,
                usdc_borrow_apr: 0.05,
                raw_account_state: String::new(),
            },
            spot: book.clone(),
            perp: book,
        }
    }

    #[test]
    fn credential_fingerprint_distinguishes_keys() {
        let a = credential_fingerprint("0xaaa");
        assert_eq!(a, credential_fingerprint("0xaaa"));
        assert_ne!(a, credential_fingerprint("0xbbb"));
        assert!(!a.contains("0xaaa"), "fingerprint must not carry the key");
    }

    #[test]
    fn open_preflight_requires_the_agent_to_outlive_a_resting_order() {
        // Fixture leaves 9_000ms of agent life (valid_until 10_000, snapshot_ts 1_000).
        let state = snapshot(0.0, 0.0, 0);
        assert!(validate_open_preflight_limits(&state, 10.0, 0).is_ok());
        assert!(validate_open_preflight_limits(&state, 10.0, 8_999).is_ok());
        assert!(validate_open_preflight_limits(&state, 10.0, 9_000).is_err());
        assert!(validate_open_preflight_limits(&state, 10.0, 300_000).is_err());
    }

    #[test]
    fn maker_exit_keeps_authorization_through_release_without_blocking_immediate_exit() {
        let mut state = snapshot(1.0, -1.0, 0);
        // A valid agent with only nine seconds left can authorize an immediate exit.
        assert!(validate_close_preflight_limits(&state, 10.0).is_ok());
        assert!(validate_emergency_close_preflight(&state).is_ok());
        assert!(validate_maker_close_agent_lifetime(&state, 300_000, 1_000).is_err());

        state.account.agent_valid_until_ms = 302_001;
        assert!(validate_maker_close_agent_lifetime(&state, 300_000, 1_000).is_ok());
        // The final pre-submit check uses current time, after activation and audit work.
        assert!(validate_maker_close_agent_lifetime(&state, 300_000, 1_001).is_err());
        state.account.agent_authorized = false;
        assert!(validate_maker_close_agent_lifetime(&state, 300_000, 1_000).is_err());
    }

    #[test]
    fn maker_exit_response_latency_does_not_extend_the_authorized_deadline() {
        let policy = OrderPolicy::standalone_default();
        let submit_ts_ms = 1_000_i64;
        let response_ts_ms = submit_ts_ms + 1_500;
        let mut state = snapshot(1.0, -1.0, 0);
        state.account.agent_valid_until_ms = submit_ts_ms
            + policy.maker_hard_timeout_ms as i64
            + EXIT_FILL_GRACE_MS as i64
            + 1;
        validate_maker_close_agent_lifetime(&state, policy.maker_hard_timeout_ms, submit_ts_ms)
            .unwrap();
        let deadline = submit_ts_ms + policy.maker_hard_timeout_ms as i64;
        assert_eq!(
            managed_resting_action(
                &policy,
                true,
                OrderFlow::NormalExit,
                0.0,
                (deadline - submit_ts_ms) as u64,
                true,
            ),
            RestingAction::CancelHardTimeout
        );
        // Anchoring to the response would keep the order past the checked lifetime.
        assert_eq!(
            managed_resting_action(
                &policy,
                true,
                OrderFlow::NormalExit,
                0.0,
                (deadline - response_ts_ms) as u64,
                true,
            ),
            RestingAction::Keep
        );
        assert!(deadline + (EXIT_FILL_GRACE_MS as i64) < state.account.agent_valid_until_ms);
        assert!(
            response_ts_ms + policy.maker_hard_timeout_ms as i64
                > state.account.agent_valid_until_ms
        );
    }

    #[test]
    fn interrupted_dust_below_the_minimum_order_does_not_halt_recovery() {
        use crate::attempt_state::{recovery_step, RecoveryStep};
        // Under the minimum it cannot be traded away, so recovery records it and continues.
        assert_eq!(recovery_step(true, 0, 0.50, 10.0), RecoveryStep::HaltDust);
        // At or above the minimum the hedge branch fixes it by trading instead.
        assert_eq!(recovery_step(true, 0, 10.0, 10.0), RecoveryStep::HedgeDelta);
        // Open orders still take precedence over both.
        assert_eq!(
            recovery_step(true, 1, 0.50, 10.0),
            RecoveryStep::CancelOrders
        );
    }

    #[test]
    fn tick_rounding_is_directional() {
        assert!((floor_tick(58.2489) - 58.248).abs() < 1e-9);
        assert!((ceil_tick(58.2481) - 58.249).abs() < 1e-9);
    }

    fn significant_figures(px: f64) -> usize {
        let rendered = format!("{px:.8}");
        let digits = rendered.replace('.', "");
        let digits = digits.trim_start_matches('0').trim_end_matches('0');
        digits.len()
    }

    #[test]
    fn prices_stay_inside_the_five_significant_figure_limit_at_every_magnitude() {
        // The exchange refuses a sixth figure. Walk each decade an order could be priced in.
        for px in [
            9.87654, 52.0817, 99.9994, 100.1234, 152.3456, 999.987, 1860.35, 62657.5,
        ] {
            for rounded in [floor_tick(px), ceil_tick(px)] {
                assert!(
                    significant_figures(rounded) <= 5,
                    "{px} rounded to {rounded}, which carries too many figures"
                );
            }
        }
    }

    #[test]
    fn the_grid_is_unchanged_for_prices_hype_actually_trades_at() {
        // Guards the deploy: below $100 the derived grid must reproduce the old fixed 0.001.
        for step in 0..40_000 {
            let px = 20.0 + f64::from(step) * 0.002;
            assert!(px < 100.0);
            assert!((price_tick(px) - 0.001).abs() < 1e-12, "grid moved at {px}");
        }
    }

    #[test]
    fn the_grid_coarsens_exactly_where_the_figure_limit_binds() {
        assert!((price_tick(99.999) - 0.001).abs() < 1e-12);
        assert!((price_tick(100.0) - 0.01).abs() < 1e-12);
        assert!((price_tick(999.99) - 0.01).abs() < 1e-12);
        assert!((price_tick(1000.0) - 0.1).abs() < 1e-12);
    }

    #[test]
    fn rounding_an_already_aligned_price_is_a_no_op() {
        // floor_tick prices our resting bid off the book's best bid, which the exchange has
        // already aligned. Dropping a tick there posts us behind the touch for no reason.
        for step in 0..11_000 {
            let px = 50.0 + f64::from(step) * 0.001;
            let floored = floor_tick(px);
            assert!(
                (floored - px).abs() < 1e-9,
                "floor_tick({px}) moved it to {floored}"
            );
            let ceiled = ceil_tick(px);
            assert!(
                (ceiled - px).abs() < 1e-9,
                "ceil_tick({px}) moved it to {ceiled}"
            );
        }
    }

    #[test]
    fn a_degenerate_price_does_not_overflow_the_exponent() {
        for px in [0.0, f64::INFINITY, f64::NAN] {
            let tick = price_tick(px);
            assert!(tick > 0.0 && tick <= 1.0, "{px} produced tick {tick}");
        }
    }

    #[test]
    fn quantity_floor_tolerates_binary_subtraction_noise() {
        assert!((floor_qty(2.88 - 2.56) - 0.32).abs() < 1e-9);
        assert!((floor_qty(0.329_778) - 0.32).abs() < 1e-9);
        assert!((floor_qty(0.319) - 0.31).abs() < 1e-9);
    }

    #[test]
    fn delta_hedge_rounds_up_out_of_the_one_lot_dead_band() {
        // The raw residual clears the minimum, but rounding down one lot produces an
        // order below the minimum. Rounding up must leave a sub-minimum residual.
        let residual = 0.115_144_119_999_968_1;
        let bid = 87.61;
        assert!(residual * bid > 10.0, "raw residual must clear the minimum");
        assert!(
            floor_qty(residual) * bid < 10.0,
            "floored residual must fall under it"
        );

        let (is_buy, touch_px, qty) = plan_delta_hedge(residual, bid, bid + 0.01).unwrap();
        assert!(!is_buy);
        assert!((touch_px - bid).abs() < 1e-9);
        assert!((qty - 0.12).abs() < 1e-9, "rounds up one lot, got {qty}");
        assert!(qty * touch_px >= 10.0, "the order we send clears the minimum");
        assert!(
            (qty - residual) * touch_px < 1.0,
            "overshoot stays inside one lot"
        );
    }

    #[test]
    fn delta_hedge_carries_a_residual_inside_the_risk_tolerance() {
        // Below $10 the risk layer resets its clock, so there is nothing to repair and no
        // order to place.
        assert!(plan_delta_hedge(0.05, 87.61, 87.62).is_none());
        assert!(plan_delta_hedge(-0.05, 87.61, 87.62).is_none());
    }

    #[test]
    fn delta_hedge_plans_both_directions_once_the_floored_order_clears_the_minimum() {
        let (is_buy, touch_px, qty) = plan_delta_hedge(0.5, 87.61, 87.62).unwrap();
        assert!(!is_buy, "a long spot delta is repaired by selling perp");
        assert!((touch_px - 87.61).abs() < 1e-9, "sells quote the bid");
        assert!((qty - 0.5).abs() < 1e-9);

        let (is_buy, touch_px, qty) = plan_delta_hedge(-0.5, 87.61, 87.62).unwrap();
        assert!(is_buy, "a short spot delta is repaired by buying perp");
        assert!((touch_px - 87.62).abs() < 1e-9, "buys quote the ask");
        assert!((qty - 0.5).abs() < 1e-9);
    }

    #[test]
    fn activation_quantity_shrinks_to_fresh_hedge_capacity() {
        assert!((activation_qty(100.0, 99.987, 58.0, 10.0).unwrap() - 99.98).abs() < 1e-9);
        assert!((activation_qty(100.0, 120.0, 58.0, 10.0).unwrap() - 100.0).abs() < 1e-9);
        assert!(activation_qty(1.0, 0.1, 58.0, 10.0).is_err());
    }

    #[test]
    fn fill_metrics_report_fee_and_net_entry_cost_bps() {
        assert!((crate::fill_notify::fill_fee_bps(4.0, 50.0, 200.0) - 4.0).abs() < 1e-9);
        let metrics = DecisionFillMetrics {
            spot_qty: 200.0,
            perp_qty: 200.0,
            spot_vwap: 50.0,
            perp_vwap: 50.01,
            fees_usd: 8.0,
        };
        assert!((metrics.basis_bps() - 2.0).abs() < 1e-9);
        assert!((metrics.fee_bps() - 8.0).abs() < 1e-9);
        assert!((metrics.fee_bps() - metrics.basis_bps() - 6.0).abs() < 1e-9);
        assert!((hedge_loss_bps(true, 50.0, 50.01) - 2.0).abs() < 1e-9);
        assert!((hedge_loss_bps(false, 50.0, 49.99) - 2.0004).abs() < 1e-3);
    }

    #[test]
    fn fill_metrics_net_corrective_hedges_by_side() {
        let metrics = DecisionFillMetrics::from_signed_totals(
            10.0,
            20.0 - 10.0,
            1_000.0,
            2_020.0 - 1_000.0,
            1.0,
        );
        assert!((metrics.spot_qty - 10.0).abs() < 1e-9);
        assert!((metrics.perp_qty - 10.0).abs() < 1e-9);
        assert!((metrics.perp_vwap - 102.0).abs() < 1e-9);
        assert!((metrics.basis_bps() - 200.0).abs() < 1e-9);
    }

    #[test]
    fn watchdog_cancels_live_maker_before_repairing_delta() {
        assert!(watchdog_requires_cancel(true, 1.0, 50.0));
        assert!(!watchdog_requires_cancel(false, 1.0, 50.0));
        assert!(!watchdog_requires_cancel(true, 0.1, 50.0));
    }

    #[test]
    fn execution_probe_ignores_basis_but_keeps_lifecycle_limits() {
        assert_eq!(
            managed_resting_action(
                &OrderPolicy::standalone_default(),
                false,
                OrderFlow::Entry,
                -10.0,
                1_000,
                true,
            ),
            RestingAction::Keep
        );
        assert_eq!(
            managed_resting_action(
                &OrderPolicy::standalone_default(),
                false,
                OrderFlow::NormalExit,
                10.0,
                60_000,
                false,
            ),
            RestingAction::CancelOffTouch
        );
        assert_eq!(
            managed_resting_action(
                &OrderPolicy::standalone_default(),
                false,
                OrderFlow::Entry,
                10.0,
                300_000,
                true,
            ),
            RestingAction::CancelHardTimeout
        );
    }

    #[test]
    fn spot_fill_position_change_accounts_for_hype_fee() {
        assert!((spot_position_change_qty(true, 0.85, 0.00057, "HYPE") - 0.84943).abs() < 1e-9);
        assert!((spot_position_change_qty(false, 0.85, 0.00057, "HYPE") - 0.85057).abs() < 1e-9);
        assert!((spot_position_change_qty(true, 0.85, 0.03, "USDC") - 0.85).abs() < 1e-9);
        assert!((spot_position_change_qty(false, 0.85, 0.03, "USDC") - 0.85).abs() < 1e-9);
    }

    #[test]
    fn order_response_preserves_actual_filled_quantity_and_timing() {
        let response: ExchangeResponseStatus = serde_json::from_value(json!({
            "status":"ok",
            "response":{
                "type":"order",
                "data":{"statuses":[{"filled":{"totalSz":"0.84","avgPx":"58","oid":7}}]}
            }
        }))
        .unwrap();
        let rows = outcomes(&response, 100, 250);
        assert_eq!(rows[0].status, "filled");
        assert_eq!(rows[0].oid, 7);
        assert!((rows[0].filled_qty - 0.84).abs() < 1e-9);
        assert!((rows[0].avg_fill_px - 58.0).abs() < 1e-9);
        assert_eq!(rows[0].request_ts_ms, 100);
        assert_eq!(rows[0].response_ts_ms, 250);
    }

    #[test]
    fn post_only_cross_rejection_is_retryable_but_other_errors_are_not() {
        let mut outcome = OrderOutcome {
            status: "error".to_string(),
            oid: 0,
            filled_qty: 0.0,
            avg_fill_px: 0.0,
            request_ts_ms: 100,
            response_ts_ms: 200,
            error: "Post only order would have immediately matched, bbo was 57.866@57.888"
                .to_string(),
            raw: String::new(),
        };
        assert!(alo_would_immediately_match(&outcome));
        let error: anyhow::Error = AloWouldMatch { flow: "entry" }.into();
        assert!(is_retryable_alo_reject(&error));

        outcome.error = "Insufficient margin".to_string();
        assert!(!alo_would_immediately_match(&outcome));
    }

    #[test]
    fn open_preflight_allows_existing_hedged_position() {
        validate_open_preflight(&snapshot(2.0, -2.0, 0)).unwrap();
    }

    #[test]
    fn open_preflight_does_not_infer_pm_state_from_account_value() {
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.account_value_usd = 1.0;
        validate_open_preflight(&state).unwrap();
    }

    #[test]
    fn flat_preflight_does_not_infer_pm_state_from_account_value() {
        let mut state = snapshot(0.0, 0.0, 0);
        state.account.account_value_usd = 1.0;
        validate_flat_preflight(&state).unwrap();
    }

    #[test]
    fn entry_permit_is_single_window_and_bound_to_config() {
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.account_value_usd = 1.0;
        state.account.agent_valid_until_ms = 1_000_000;
        let context = ExecutionContext::standalone("permit_test");
        let permit = EntryPermit::issue(state, &context, 2_000).unwrap();
        permit.validate(&context, permit.expires_ts_ms()).unwrap();

        let expired = permit
            .validate(&context, permit.expires_ts_ms() + 1)
            .unwrap_err();
        assert!(is_entry_permit_invalid(&expired));

        let mut changed = context;
        changed.config_hash = "changed";
        let changed = permit.validate(&changed, 2_000).unwrap_err();
        assert!(is_entry_permit_invalid(&changed));
    }

    #[test]
    fn entry_permit_keeps_pm_and_order_guards() {
        let context = ExecutionContext::standalone("permit_test");
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.agent_valid_until_ms = 1_000_000;
        state.account.portfolio_margin_enabled = false;
        assert!(EntryPermit::issue(state, &context, 2_000).is_err());

        let mut state = snapshot(2.0, -2.0, 1);
        state.account.agent_valid_until_ms = 1_000_000;
        assert!(EntryPermit::issue(state, &context, 2_000).is_err());

        let mut state = snapshot(2.0, -2.0, 0);
        state.account.agent_valid_until_ms = 1_000_000;
        assert!(EntryPermit::issue(state, &context, 31_001).is_err());
    }

    #[test]
    fn resting_entry_risk_revokes_adds_without_blocking_recovery_code() {
        let config = crate::config::StrategyConfig::reference_fixture();
        let mut state = snapshot(2.0, -2.0, 1);
        state.account.usdc_borrow_apr = 0.5;
        state.account.funding_24h_all_negative = true;
        validate_resting_entry_risk(
            &state,
            1,
            config.risk.warning_pmr,
            config.delta_emergency_usd,
            0,
        )
        .unwrap();

        state.account.portfolio_margin_enabled = false;
        assert!(validate_resting_entry_risk(
            &state,
            1,
            config.risk.warning_pmr,
            config.delta_emergency_usd,
            0,
        )
        .is_err());
        state.account.portfolio_margin_enabled = true;
        state.account.open_orders = 2;
        assert!(validate_resting_entry_risk(
            &state,
            1,
            config.risk.warning_pmr,
            config.delta_emergency_usd,
            0,
        )
        .is_err());
    }

    #[test]
    fn permitted_submit_rejects_ws_or_user_state_changes() {
        let mut market = PermittedMarketState::from_snapshot(&snapshot(2.0, -2.0, 0)).unwrap();
        let (sender, mut feed) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(FeedEvent::OrderUpdate {
                oid: 7,
                status: "open".to_string(),
                status_ts_ms: 1,
                recv_ts_ms: 2,
            })
            .unwrap();
        let error = drain_permitted_feed(&mut feed, &mut market).unwrap_err();
        assert!(is_entry_permit_invalid(&error));

        let (sender, mut feed) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(FeedEvent::Disconnected("test".to_string()))
            .unwrap();
        let error = drain_permitted_feed(&mut feed, &mut market).unwrap_err();
        assert!(is_entry_permit_invalid(&error));
    }

    #[test]
    fn permitted_submit_rechecks_latest_ws_basis() {
        let now = now_ms();
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.snapshot_ts_ms = now;
        state.account.agent_valid_until_ms = now + 600_000;
        state.spot.exchange_ts_ms = now;
        state.spot.recv_ts_ms = now;
        state.perp.exchange_ts_ms = now;
        state.perp.recv_ts_ms = now;
        state.perp.bid_px[0] = 58.10;
        state.perp.ask_px[0] = 58.11;
        let context = ExecutionContext::standalone("permit_test");
        let permit = EntryPermit::issue(state.clone(), &context, now).unwrap();
        let mut market = PermittedMarketState::from_snapshot(&state).unwrap();
        assert!(validate_permitted_pre_submit(&permit, &context, &market, 58.0, 1.0, 5.0).is_ok());

        let (sender, mut feed) = tokio::sync::mpsc::unbounded_channel();
        sender
            .send(FeedEvent::Bbo {
                coin: "HYPE".to_string(),
                exchange_ts_ms: now,
                recv_ts_ms: now,
                bid: Some(Touch { px: 57.9, sz: 1.0 }),
                ask: Some(Touch { px: 57.91, sz: 1.0 }),
            })
            .unwrap();
        drain_permitted_feed(&mut feed, &mut market).unwrap();
        let error =
            validate_permitted_pre_submit(&permit, &context, &market, 58.0, 1.0, 5.0).unwrap_err();
        assert!(is_basis_gate_not_met(&error));
    }

    #[test]
    fn permitted_submit_checks_whole_quantity_and_latest_capacity() {
        let now = now_ms();
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.snapshot_ts_ms = now;
        state.account.agent_valid_until_ms = now + 600_000;
        for book in [&mut state.spot, &mut state.perp] {
            book.exchange_ts_ms = now;
            book.recv_ts_ms = now;
        }
        state.perp.bid_px = vec![58.10, 58.0];
        state.perp.bid_sz = vec![0.1, 100.0];
        state.perp.ask_px[0] = 58.11;
        let mut context = ExecutionContext::standalone("permit_test");
        context.order_policy.entry_keep_bps = 0.0;
        let permit = EntryPermit::issue(state.clone(), &context, now).unwrap();
        let mut market = PermittedMarketState::from_snapshot(&state).unwrap();
        // Top basis is >5bp, but the entire 1 HYPE order is <5bp.
        let error = validate_permitted_pre_submit(&permit, &context, &market, 58.0, 1.0, 5.0).unwrap_err();
        assert!(is_basis_gate_not_met(&error));
        assert!(validate_permitted_pre_submit(&permit, &context, &market, 58.0, 0.1, 5.0).is_ok());
        market.observe(FeedEvent::Bbo {
            coin:"HYPE".into(), exchange_ts_ms:now, recv_ts_ms:now,
            bid:Some(Touch {px:58.10,sz:0.05}), ask:Some(Touch {px:58.11,sz:100.0}),
        }).unwrap();
        // The updated top proves only 0.05 HYPE; 20% utilization permits 0.01.
        let error = validate_permitted_pre_submit(&permit, &context, &market, 58.0, 0.1, 5.0).unwrap_err();
        assert!(is_retryable_capacity_change(&error), "{error:#}");
    }

    #[test]
    fn permitted_submit_rechecks_expiry_and_market_freshness() {
        let now = now_ms();
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.snapshot_ts_ms = now;
        state.account.agent_valid_until_ms = now + 600_000;
        state.spot.exchange_ts_ms = now;
        state.spot.recv_ts_ms = now;
        state.perp.exchange_ts_ms = now;
        state.perp.recv_ts_ms = now;
        state.perp.bid_px[0] = 58.10;
        state.perp.ask_px[0] = 58.11;
        let context = ExecutionContext::standalone("permit_test");
        let mut permit = EntryPermit::issue(state.clone(), &context, now).unwrap();
        let mut market = PermittedMarketState::from_snapshot(&state).unwrap();

        permit.expires_ts_ms = now - 1;
        let error =
            validate_permitted_pre_submit(&permit, &context, &market, 58.0, 1.0, 5.0).unwrap_err();
        assert!(is_entry_permit_invalid(&error));

        permit.expires_ts_ms = now + ENTRY_PERMIT_TTL_MS;
        market.perp.book.recv_ts_ms = now - context.max_market_age_ms as i64 - 1;
        let error =
            validate_permitted_pre_submit(&permit, &context, &market, 58.0, 1.0, 5.0).unwrap_err();
        assert!(is_entry_permit_invalid(&error));
    }

    #[test]
    fn permitted_submit_avoids_a_known_post_only_cross() {
        let now = now_ms();
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.snapshot_ts_ms = now;
        state.account.agent_valid_until_ms = now + 600_000;
        state.spot.exchange_ts_ms = now;
        state.spot.recv_ts_ms = now;
        state.perp.exchange_ts_ms = now;
        state.perp.recv_ts_ms = now;
        state.perp.bid_px[0] = 58.10;
        state.perp.ask_px[0] = 58.11;
        let context = ExecutionContext::standalone("permit_test");
        let permit = EntryPermit::issue(state.clone(), &context, now).unwrap();
        let mut market = PermittedMarketState::from_snapshot(&state).unwrap();
        market.spot.book.ask_px[0] = 58.01;

        let error =
            validate_permitted_pre_submit(&permit, &context, &market, 58.01, 1.0, 5.0).unwrap_err();
        assert!(is_retryable_alo_reject(&error));
    }

    #[test]
    fn open_preflight_rejects_orders_and_material_delta() {
        assert!(validate_open_preflight(&snapshot(2.0, -2.0, 1)).is_err());
        assert!(validate_open_preflight(&snapshot(2.0, -1.0, 0)).is_err());
    }

    #[test]
    fn dirty_domain_blocks_add_but_not_known_hfc_reduction() {
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.asset_domain.domain_clean = false;
        state
            .account
            .asset_domain
            .unexpected_positions
            .push(crate::asset_domain::DomainFinding {
                location: "perp:xyz".to_string(),
                asset: "xyz:XYZ100".to_string(),
                detail: "szi=1".to_string(),
            });
        state.account.unknown_exposure = true;
        assert!(validate_open_preflight(&state).is_err());
        validate_close_preflight(&state).unwrap();
    }

    #[test]
    fn activation_preflight_rejects_warning_pmr_and_bad_market_clock() {
        let mut state = snapshot(2.0, -2.0, 0);
        let now = now_ms();
        state.account.snapshot_ts_ms = now;
        // Must outlive maker_hard_timeout_ms; this test is about PMR and market clocks.
        state.account.agent_valid_until_ms = now + 600_000;
        state.spot.exchange_ts_ms = now;
        state.spot.recv_ts_ms = now;
        state.perp.exchange_ts_ms = now;
        state.perp.recv_ts_ms = now;
        let context = ExecutionContext::standalone("test");
        validate_open_activation(&state, &context).unwrap();

        state.account.portfolio_margin_ratio = context.warning_pmr;
        assert!(validate_open_activation(&state, &context).is_err());
        state.account.portfolio_margin_ratio = 0.0;
        state.perp.exchange_ts_ms = now + context.max_market_clock_skew_ms as i64 + 1;
        state.perp.recv_ts_ms = state.perp.exchange_ts_ms;
        assert!(validate_open_activation(&state, &context).is_err());
    }

    #[test]
    fn open_request_enforces_live_notional_bounds() {
        assert!(validate_open_request(9.99).is_err());
        validate_open_request(10.0).unwrap();
        validate_open_request(500.0).unwrap();
        assert!(validate_open_request(500.01).is_err());
    }

    #[test]
    fn close_preflight_allows_risk_reduction_below_entry_thresholds() {
        let mut state = snapshot(2.0, -2.0, 0);
        state.account.account_value_usd = 9_000.0;
        state.account.portfolio_margin_enabled = false;
        validate_close_preflight(&state).unwrap();
    }

    #[test]
    fn close_preflight_rejects_orders_delta_and_flat_state() {
        assert!(validate_close_preflight(&snapshot(2.0, -2.0, 1)).is_err());
        assert!(validate_close_preflight(&snapshot(2.0, -1.0, 0)).is_err());
        assert!(validate_close_preflight(&snapshot(0.01, 0.0, 0)).is_err());
    }

    #[test]
    fn close_target_is_bounded_by_matched_position() {
        let state = snapshot(2.0, -2.0, 0);
        assert!((close_target_qty(&state, None).unwrap() - 2.0).abs() < 1e-9);
        assert!((close_target_qty(&state, Some(58.01 * 1.5)).unwrap() - 1.5).abs() < 1e-9);
        assert!(close_target_qty(&state, Some(200.0)).is_err());
    }

    #[test]
    fn emergency_close_accepts_material_delta_for_cleanup() {
        validate_emergency_close_preflight(&snapshot(2.0, -1.0, 0)).unwrap();
    }
}
