use serde::Serialize;

use crate::api::AccountSnapshot;
use crate::config::StrategyConfig;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct EntryPolicyEstimate {
    pub predicted_next_funding_rate: f64,
    pub predicted_next_funding_bps: f64,
    pub next_funding_time_ms: i64,
    pub timing_penalty_bps: f64,
    pub create_gate_bps: f64,
    pub keep_gate_bps: f64,
    pub timing_eligible: bool,
    pub eligible: bool,
}

pub fn evaluate_entry(
    config: &StrategyConfig,
    account: &AccountSnapshot,
    quoted_basis_bps: f64,
) -> EntryPolicyEstimate {
    let policy = config.entry_policy;
    let predicted_next_funding_bps = account.predicted_next_funding_rate * 10_000.0;
    let timing_penalty_bps =
        (-predicted_next_funding_bps).max(0.0) * policy.negative_next_funding_weight;
    let create_gate_bps = config.entry_create_bps + timing_penalty_bps;
    let keep_gate_bps = config.entry_keep_bps + timing_penalty_bps;
    let timing_eligible = quoted_basis_bps + 1e-10 >= create_gate_bps;
    EntryPolicyEstimate {
        predicted_next_funding_rate: account.predicted_next_funding_rate,
        predicted_next_funding_bps,
        next_funding_time_ms: account.next_funding_time_ms,
        timing_penalty_bps,
        create_gate_bps,
        keep_gate_bps,
        timing_eligible,
        eligible: timing_eligible,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset_domain::AssetDomainScan;

    fn account(predicted_rate: f64) -> AccountSnapshot {
        AccountSnapshot {
            snapshot_ts_ms: 1,
            account_value_usd: 10_100.0,
            account_value_ts_ms: 1,
            portfolio_margin_enabled: true,
            portfolio_margin_ratio: 0.0,
            available_after_maintenance_usdc: 10_000.0,
            usdc_total: 10_000.0,
            usdc_borrowed: 0.0,
            hype_spot_qty: 0.0,
            hype_perp_qty: 0.0,
            liquidation_px: 0.0,
            funding_24h_bps: 0.0,
            funding_24h_all_negative: false,
            predicted_next_funding_rate: predicted_rate,
            next_funding_time_ms: 3_600_000,
            open_orders: 0,
            unknown_exposure: false,
            asset_domain: AssetDomainScan {
                domain_clean: true,
                ..AssetDomainScan::default()
            },
            agent_authorized: true,
            agent_valid_until_ms: i64::MAX,
            perp_taker_fee_bps: 4.5,
            perp_maker_fee_bps: 1.5,
            spot_taker_fee_bps: 7.0,
            spot_maker_fee_bps: 4.0,
            usdc_borrow_apr: 0.05,
            raw_account_state: "{}".to_string(),
        }
    }

    #[test]
    fn positive_next_funding_does_not_relax_zero_basis_floor() {
        let config = StrategyConfig::eoa_fixture();
        let estimate = evaluate_entry(&config, &account(0.0000125), -0.01);
        assert_eq!(estimate.create_gate_bps, 0.0);
        assert_eq!(estimate.keep_gate_bps, -2.0);
        assert!(!estimate.eligible);
    }

    #[test]
    fn negative_next_funding_is_added_to_create_and_keep_gates() {
        let config = StrategyConfig::eoa_fixture();
        let estimate = evaluate_entry(&config, &account(-0.00002), 0.2);
        assert!((estimate.predicted_next_funding_bps + 0.2).abs() < 1e-12);
        assert!((estimate.create_gate_bps - 0.2).abs() < 1e-12);
        assert!((estimate.keep_gate_bps + 1.8).abs() < 1e-12);
        assert!(estimate.eligible);
    }

    #[test]
    fn leverage_and_borrow_rate_do_not_veto_entry() {
        let mut config = StrategyConfig::eoa_fixture();
        let baseline = evaluate_entry(&config, &account(0.0000125), 10.0);
        config.target_deployment_ratio = 2.0;
        let mut state = account(0.0000125);
        state.usdc_borrow_apr = 0.5;
        let estimate = evaluate_entry(&config, &state, 10.0);
        assert!(estimate.eligible);
        assert_eq!(estimate, baseline);
    }
}
