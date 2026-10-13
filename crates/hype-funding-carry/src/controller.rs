use serde::Serialize;

use crate::config::DeploymentBand;
use crate::portfolio::HedgePlan;
use crate::risk::RiskAction;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeDirection {
    Add,
    Reduce,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct DesiredTrade {
    pub direction: TradeDirection,
    pub notional_usd: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RetargetAction {
    Keep,
    Cancel,
    CancelThenReconcile,
    CancelThenReplace { desired: DesiredTrade },
}

pub fn desired_trade(intent: Intent, dust_usd: f64) -> Option<DesiredTrade> {
    let (direction, notional_usd) = match intent {
        Intent::BandAdd { amount_usd } => (TradeDirection::Add, amount_usd),
        Intent::BandReduce { amount_usd } => (TradeDirection::Reduce, amount_usd),
        _ => return None,
    };
    (notional_usd + 1e-10 >= dust_usd).then_some(DesiredTrade {
        direction,
        notional_usd,
    })
}

pub fn retarget_action(
    resting: DesiredTrade,
    desired: Option<DesiredTrade>,
    dust_usd: f64,
) -> RetargetAction {
    let Some(desired) = desired.filter(|value| value.notional_usd + 1e-10 >= dust_usd) else {
        return RetargetAction::Cancel;
    };
    if resting.direction != desired.direction {
        return RetargetAction::CancelThenReconcile;
    }
    if resting.notional_usd > desired.notional_usd + dust_usd {
        return RetargetAction::CancelThenReplace { desired };
    }
    RetargetAction::Keep
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(tag = "intent", rename_all = "snake_case")]
pub enum Intent {
    ReconcileUnknown,
    HedgeResidual { plan: HedgePlan },
    CancelUnexpected,
    ManualFullClose,
    PmrTakerReduce { target_pmr: f64 },
    PmrMakerReduce { target_pmr: f64 },
    BandReduce { amount_usd: f64 },
    BandAdd { amount_usd: f64 },
    Idle,
}

impl Intent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReconcileUnknown => "reconcile_unknown",
            Self::HedgeResidual { .. } => "hedge_residual",
            Self::CancelUnexpected => "cancel_unexpected",
            Self::ManualFullClose => "manual_full_close",
            Self::PmrTakerReduce { .. } => "pmr_taker_reduce",
            Self::PmrMakerReduce { .. } => "pmr_maker_reduce",
            Self::BandReduce { .. } => "band_reduce",
            Self::BandAdd { .. } => "band_add",
            Self::Idle => "idle",
        }
    }

    pub fn amount_usd(self) -> f64 {
        match self {
            Self::BandReduce { amount_usd } | Self::BandAdd { amount_usd } => amount_usd,
            Self::HedgeResidual { plan } => plan.notional_usd,
            _ => 0.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ControllerInput {
    pub risk_action: RiskAction,
    pub state_unknown: bool,
    pub unique_hedge: Option<HedgePlan>,
    pub hedge_execution_available: bool,
    pub unexpected_orders: bool,
    pub manual_full_close: bool,

    pub deployment_ratio: Option<f64>,
    pub add_to_target: bool,
    pub reduce_to_target: bool,
    pub matched_notional_usd: f64,
    /// Equity the band is sized against. See ExternalState::core_equity_usd.
    pub sizing_equity_usd: f64,
    pub band: Option<DeploymentBand>,
    pub entry_gate_eligible: bool,

    pub dust_usd: f64,
    /// Floor for band rebalancing specifically. A clip costs the same ~125 info requests and
    /// attempt-file cycle whatever its size, so closing the last few dollars to target is not
    /// worth one: it was a $26 clip's post-trade domain scan that tripped the exchange rate
    /// limit on 08-01. Ordinary dust still uses `dust_usd`.
    pub min_band_trade_usd: f64,
}

pub fn next_intent(input: &ControllerInput) -> Intent {
    if input.unexpected_orders {
        return Intent::CancelUnexpected;
    }
    if input.state_unknown || input.risk_action == RiskAction::HaltUnknown {
        return if input.hedge_execution_available {
            input
                .unique_hedge
                .map(|plan| Intent::HedgeResidual { plan })
                .unwrap_or(Intent::ReconcileUnknown)
        } else {
            Intent::ReconcileUnknown
        };
    }
    if let Some(plan) = input
        .unique_hedge
        .filter(|_| input.hedge_execution_available)
    {
        return Intent::HedgeResidual { plan };
    }
    if input.manual_full_close {
        return Intent::ManualFullClose;
    }
    match input.risk_action {
        RiskAction::TakerReduce { target_pmr } => {
            return Intent::PmrTakerReduce { target_pmr };
        }
        RiskAction::MakerReduce { target_pmr } => {
            return Intent::PmrMakerReduce { target_pmr };
        }
        RiskAction::HedgeOnly => return Intent::Idle,
        RiskAction::Normal | RiskAction::StopNewRisk | RiskAction::HaltUnknown => {}
    }

    let (Some(ratio), Some(band)) = (input.deployment_ratio, input.band) else {
        return Intent::Idle;
    };
    let target_notional = input.sizing_equity_usd * band.target;
    if ratio > band.upper || (input.reduce_to_target && ratio > band.target) {
        let amount_usd = (input.matched_notional_usd - target_notional).max(0.0);
        if amount_usd + 1e-10 >= input.min_band_trade_usd {
            return Intent::BandReduce { amount_usd };
        }
        return Intent::Idle;
    }

    let add_phase_active = ratio < band.lower || (input.add_to_target && ratio < band.target);
    if add_phase_active && input.risk_action.allows_new_risk() && input.entry_gate_eligible {
        let amount_usd = (target_notional - input.matched_notional_usd).max(0.0);
        if amount_usd + 1e-10 >= input.min_band_trade_usd {
            return Intent::BandAdd { amount_usd };
        }
    }
    Intent::Idle
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::portfolio::HedgeLeg;

    fn base() -> ControllerInput {
        ControllerInput {
            risk_action: RiskAction::Normal,
            state_unknown: false,
            unique_hedge: None,
            hedge_execution_available: true,
            unexpected_orders: false,
            manual_full_close: false,

            deployment_ratio: Some(1.30),
            add_to_target: false,
            reduce_to_target: false,
            matched_notional_usd: 26_000.0,
            sizing_equity_usd: 20_000.0,
            band: Some(DeploymentBand {
                lower: 1.00,
                target: 1.30,
                upper: 1.60,
            }),
            entry_gate_eligible: true,

            dust_usd: 10.0,
            min_band_trade_usd: 0.0,
        }
    }

    #[test]
    fn band_skips_clips_too_small_to_be_worth_an_entry() {
        // equity 20_000 × target 1.30 = 26_000 of notional.
        fn at(input: &mut ControllerInput, matched: f64) {
            input.matched_notional_usd = matched;
            input.deployment_ratio = Some(matched / input.sizing_equity_usd);
        }
        let mut input = base();
        input.min_band_trade_usd = 100.0;
        input.add_to_target = true;

        // A small gap below the configured trade floor does not trigger an entry.
        at(&mut input, 26_000.0 - 31.0);
        assert_eq!(next_intent(&input), Intent::Idle);
        // A gap worth an entry still trades.
        at(&mut input, 26_000.0 - 500.0);
        assert!(matches!(next_intent(&input), Intent::BandAdd { .. }));

        // The same floor applies to reducing.
        input.add_to_target = false;
        input.reduce_to_target = true;
        at(&mut input, 26_000.0 + 31.0);
        assert_eq!(next_intent(&input), Intent::Idle);
        at(&mut input, 26_000.0 + 500.0);
        assert!(matches!(next_intent(&input), Intent::BandReduce { .. }));
    }

    #[test]
    fn unique_hedge_is_allowed_during_unknown_state() {
        let mut input = base();
        input.state_unknown = true;
        input.unique_hedge = Some(HedgePlan {
            leg: HedgeLeg::SellPerp,
            qty: 1.0,
            notional_usd: 50.0,
        });
        assert!(matches!(next_intent(&input), Intent::HedgeResidual { .. }));
        input.unique_hedge = None;
        assert_eq!(next_intent(&input), Intent::ReconcileUnknown);

        input.unique_hedge = Some(HedgePlan {
            leg: HedgeLeg::SellPerp,
            qty: 1.0,
            notional_usd: 50.0,
        });
        input.hedge_execution_available = false;
        assert_eq!(next_intent(&input), Intent::ReconcileUnknown);
    }

    #[test]
    fn missing_deployment_band_disables_rebalancing_but_keeps_pmr() {
        let mut input = base();
        input.deployment_ratio = Some(0.70);
        input.band = None;
        assert_eq!(next_intent(&input), Intent::Idle);

        input.risk_action = RiskAction::MakerReduce { target_pmr: 0.65 };
        assert_eq!(
            next_intent(&input),
            Intent::PmrMakerReduce { target_pmr: 0.65 }
        );
    }

    #[test]
    fn resting_order_retargets_without_opposite_live_orders() {
        let reduce = DesiredTrade {
            direction: TradeDirection::Reduce,
            notional_usd: 500.0,
        };
        let smaller = DesiredTrade {
            direction: TradeDirection::Reduce,
            notional_usd: 200.0,
        };
        assert_eq!(
            retarget_action(reduce, Some(smaller), 10.0),
            RetargetAction::CancelThenReplace { desired: smaller }
        );

        let add = DesiredTrade {
            direction: TradeDirection::Add,
            notional_usd: 300.0,
        };
        assert_eq!(
            retarget_action(reduce, Some(add), 10.0),
            RetargetAction::CancelThenReconcile
        );
        assert_eq!(retarget_action(reduce, None, 10.0), RetargetAction::Cancel);
    }

    #[test]
    fn larger_same_direction_target_keeps_queue_position() {
        let resting = DesiredTrade {
            direction: TradeDirection::Add,
            notional_usd: 200.0,
        };
        let desired = DesiredTrade {
            direction: TradeDirection::Add,
            notional_usd: 500.0,
        };
        assert_eq!(
            retarget_action(resting, Some(desired), 10.0),
            RetargetAction::Keep
        );
        assert_eq!(
            desired_trade(Intent::BandAdd { amount_usd: 10.0 }, 10.0,),
            Some(DesiredTrade {
                direction: TradeDirection::Add,
                notional_usd: 10.0,
            })
        );
    }

    #[test]
    fn band_reports_the_full_delta_to_the_approved_target() {
        let mut input = base();
        input.deployment_ratio = Some(1.70);
        input.matched_notional_usd = 34_000.0;
        assert_eq!(
            next_intent(&input),
            Intent::BandReduce {
                amount_usd: 8_000.0
            }
        );

        input.deployment_ratio = Some(0.90);
        input.matched_notional_usd = 18_000.0;
        assert_eq!(
            next_intent(&input),
            Intent::BandAdd {
                amount_usd: 8_000.0
            }
        );
        input.risk_action = RiskAction::StopNewRisk;
        assert_eq!(next_intent(&input), Intent::Idle);
    }

    #[test]
    fn latched_add_continues_above_lower_until_target() {
        let mut input = base();
        input.add_to_target = true;
        input.deployment_ratio = Some(1.10);
        input.matched_notional_usd = 22_000.0;
        assert_eq!(
            next_intent(&input),
            Intent::BandAdd {
                amount_usd: 4_000.0
            }
        );

        input.deployment_ratio = Some(1.30);
        input.matched_notional_usd = 26_000.0;
        assert_eq!(next_intent(&input), Intent::Idle);
    }

    #[test]
    fn latched_reduce_continues_below_upper_until_target() {
        let mut input = base();
        input.reduce_to_target = true;
        input.deployment_ratio = Some(1.50);
        input.matched_notional_usd = 30_000.0;
        assert_eq!(
            next_intent(&input),
            Intent::BandReduce {
                amount_usd: 4_000.0
            }
        );

        input.deployment_ratio = Some(1.30);
        input.matched_notional_usd = 26_000.0;
        assert_eq!(next_intent(&input), Intent::Idle);
    }

    #[test]
    fn exact_band_boundaries_do_not_rebalance() {
        let mut input = base();
        input.deployment_ratio = Some(1.00);
        input.matched_notional_usd = 20_000.0;
        assert_eq!(next_intent(&input), Intent::Idle);
        input.deployment_ratio = Some(1.30);
        input.matched_notional_usd = 26_000.0;
        assert_eq!(next_intent(&input), Intent::Idle);
        input.deployment_ratio = Some(1.60);
        input.matched_notional_usd = 32_000.0;
        assert_eq!(next_intent(&input), Intent::Idle);
    }
}
