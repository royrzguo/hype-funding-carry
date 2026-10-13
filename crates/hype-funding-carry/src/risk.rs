use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RiskThresholds {
    pub warning_pmr: f64,
    pub maker_reduce_pmr: f64,
    pub taker_reduce_pmr: f64,
    pub maker_target_pmr: f64,
    pub taker_target_pmr: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RiskAction {
    Normal,
    StopNewRisk,
    MakerReduce { target_pmr: f64 },
    TakerReduce { target_pmr: f64 },
    HedgeOnly,
    HaltUnknown,
}

impl RiskAction {
    pub fn allows_new_risk(self) -> bool {
        self == Self::Normal
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::StopNewRisk => "stop_new_risk",
            Self::MakerReduce { .. } => "maker_reduce",
            Self::TakerReduce { .. } => "taker_reduce",
            Self::HedgeOnly => "hedge_only",
            Self::HaltUnknown => "halt_unknown",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ActionRiskInput {
    pub portfolio_margin_enabled: bool,
    pub portfolio_margin_ratio: f64,
    pub residual_delta_usd: f64,
    pub delta_emergency_usd: f64,
    pub naked_ms: u64,
    pub naked_timeout_ms: u64,
    pub agent_authorized: bool,
    pub agent_valid_until_ms: i64,
    pub now_ms: i64,
    pub config_valid: bool,
    pub market_state_complete: bool,
    pub unknown_exposure: bool,
}

pub fn evaluate_action(input: &ActionRiskInput, thresholds: &RiskThresholds) -> RiskAction {
    if !input.config_valid
        || !input.market_state_complete
        || input.unknown_exposure
        || !input.portfolio_margin_enabled
        || !input.agent_authorized
        || input.agent_valid_until_ms <= input.now_ms
    {
        return RiskAction::HaltUnknown;
    }
    if input.residual_delta_usd > input.delta_emergency_usd
        || input.naked_ms > input.naked_timeout_ms
    {
        return RiskAction::HedgeOnly;
    }
    if input.portfolio_margin_ratio >= thresholds.taker_reduce_pmr {
        return RiskAction::TakerReduce {
            target_pmr: thresholds.taker_target_pmr,
        };
    }
    if input.portfolio_margin_ratio >= thresholds.maker_reduce_pmr {
        return RiskAction::MakerReduce {
            target_pmr: thresholds.maker_target_pmr,
        };
    }
    if input.portfolio_margin_ratio >= thresholds.warning_pmr {
        return RiskAction::StopNewRisk;
    }
    RiskAction::Normal
}

#[derive(Clone, Debug, Default)]
pub struct RiskInput {
    pub portfolio_margin_enabled: bool,
    pub portfolio_margin_ratio: f64,
    pub residual_delta_usd: f64,
    pub naked_ms: u64,
    pub funding_24h_all_negative: bool,
    pub agent_authorized: bool,
    pub agent_valid_until_ms: i64,
    pub now_ms: i64,
    pub unknown_exposure: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct RiskDecision {
    pub level: String,
    pub allow_new_entry: bool,
    pub kill_switch: bool,
    pub reasons: Vec<String>,
}

pub fn evaluate(input: &RiskInput) -> RiskDecision {
    let mut reasons = Vec::new();

    if !input.portfolio_margin_enabled {
        reasons.push("portfolio_margin_disabled".to_string());
    }
    if input.unknown_exposure {
        reasons.push("asset_domain_unclean".to_string());
    }
    if input.funding_24h_all_negative {
        reasons.push("funding_24h_all_negative".to_string());
    }
    if input.portfolio_margin_ratio >= 0.65 {
        reasons.push("pmr_at_or_above_0_65".to_string());
    }
    if input.portfolio_margin_ratio >= 0.75 {
        reasons.push("pmr_at_or_above_0_75".to_string());
    }
    if input.portfolio_margin_ratio >= 0.85 {
        reasons.push("pmr_at_or_above_0_85".to_string());
    }
    if input.residual_delta_usd > 200.0 {
        reasons.push("unhedged_delta_over_200".to_string());
    }
    if input.naked_ms > 30_000 {
        reasons.push("unhedged_delta_over_30s".to_string());
    }
    if !input.agent_authorized || input.agent_valid_until_ms <= input.now_ms {
        reasons.push("agent_missing_or_expired".to_string());
    } else if input.agent_valid_until_ms - input.now_ms <= 7 * 86_400_000 {
        reasons.push("agent_expires_within_7d".to_string());
    }
    let emergency = !input.portfolio_margin_enabled
        || input.unknown_exposure
        || input.portfolio_margin_ratio >= 0.85
        || input.residual_delta_usd > 200.0
        || input.naked_ms > 30_000
        || !input.agent_authorized
        || input.agent_valid_until_ms <= input.now_ms;
    let level = if emergency {
        "emergency"
    } else if input.portfolio_margin_ratio >= 0.75 {
        "reduce"
    } else if input.portfolio_margin_ratio >= 0.65
        || input.agent_valid_until_ms - input.now_ms <= 7 * 86_400_000
    {
        "warning"
    } else {
        "ok"
    };
    RiskDecision {
        level: level.to_string(),
        allow_new_entry: level == "ok",
        kill_switch: emergency,
        reasons,
    }
}

#[derive(Default)]
pub struct NakedTracker {
    since_ms: Option<i64>,
}

impl NakedTracker {
    pub fn observe(&mut self, ts_ms: i64, residual_delta_usd: f64) -> u64 {
        self.observe_with_threshold(ts_ms, residual_delta_usd, 10.0)
    }

    pub fn observe_with_threshold(
        &mut self,
        ts_ms: i64,
        residual_delta_usd: f64,
        tolerance_usd: f64,
    ) -> u64 {
        if residual_delta_usd < tolerance_usd {
            self.since_ms = None;
            return 0;
        }
        let since = *self.since_ms.get_or_insert(ts_ms);
        ts_ms.saturating_sub(since) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> RiskInput {
        RiskInput {
            portfolio_margin_enabled: true,
            agent_authorized: true,
            agent_valid_until_ms: 10_000_000_000,
            now_ms: 0,
            ..RiskInput::default()
        }
    }

    #[test]
    fn pmr_ladder_is_enforced() {
        let mut input = base();
        input.portfolio_margin_ratio = 0.65;
        assert_eq!(evaluate(&input).level, "warning");
        input.portfolio_margin_ratio = 0.75;
        assert_eq!(evaluate(&input).level, "reduce");
        input.portfolio_margin_ratio = 0.85;
        assert!(evaluate(&input).kill_switch);
    }

    #[test]
    fn naked_tracker_resets() {
        let mut tracker = NakedTracker::default();
        assert_eq!(tracker.observe(1_000, 250.0), 0);
        assert_eq!(tracker.observe(32_000, 250.0), 31_000);
        assert_eq!(tracker.observe(33_000, 9.99), 0);
    }

    #[test]
    fn unknown_asset_domain_stops_new_risk() {
        let mut input = base();
        input.unknown_exposure = true;
        let result = evaluate(&input);
        assert!(result.kill_switch);
        assert!(!result.allow_new_entry);
        assert!(result.reasons.contains(&"asset_domain_unclean".to_string()));
    }

    #[test]
    fn negative_funding_is_recorded_without_blocking_entry() {
        let mut input = base();
        input.funding_24h_all_negative = true;
        let result = evaluate(&input);
        assert_eq!(result.level, "ok");
        assert!(result.allow_new_entry);
        assert!(!result.kill_switch);
        assert!(result
            .reasons
            .contains(&"funding_24h_all_negative".to_string()));
    }

    fn action_input() -> ActionRiskInput {
        ActionRiskInput {
            portfolio_margin_enabled: true,
            portfolio_margin_ratio: 0.0,
            residual_delta_usd: 0.0,
            delta_emergency_usd: 200.0,
            naked_ms: 0,
            naked_timeout_ms: 30_000,
            agent_authorized: true,
            agent_valid_until_ms: 100_000,
            now_ms: 0,
            config_valid: true,
            market_state_complete: true,
            unknown_exposure: false,
        }
    }

    fn production_thresholds() -> RiskThresholds {
        RiskThresholds {
            warning_pmr: 0.65,
            maker_reduce_pmr: 0.75,
            taker_reduce_pmr: 0.80,
            maker_target_pmr: 0.65,
            taker_target_pmr: 0.72,
        }
    }

    #[test]
    fn production_action_ladder_uses_new_thresholds() {
        let mut input = action_input();
        input.portfolio_margin_ratio = 0.65;
        assert_eq!(
            evaluate_action(&input, &production_thresholds()),
            RiskAction::StopNewRisk
        );
        input.portfolio_margin_ratio = 0.75;
        assert_eq!(
            evaluate_action(&input, &production_thresholds()),
            RiskAction::MakerReduce { target_pmr: 0.65 }
        );
        input.portfolio_margin_ratio = 0.80;
        assert_eq!(
            evaluate_action(&input, &production_thresholds()),
            RiskAction::TakerReduce { target_pmr: 0.72 }
        );
    }

    #[test]
    fn unknown_state_precedes_pmr_reduction() {
        let mut input = action_input();
        input.portfolio_margin_ratio = 0.90;
        input.market_state_complete = false;
        assert_eq!(
            evaluate_action(&input, &production_thresholds()),
            RiskAction::HaltUnknown
        );
    }

    #[test]
    fn portfolio_margin_state_is_checked_directly() {
        let input = action_input();
        assert_eq!(
            evaluate_action(&input, &production_thresholds()),
            RiskAction::Normal
        );

        let mut disabled = input;
        disabled.portfolio_margin_enabled = false;
        assert_eq!(
            evaluate_action(&disabled, &production_thresholds()),
            RiskAction::HaltUnknown
        );
    }
}
