use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
pub struct OrderPolicy {
    pub entry_create_bps: f64,
    pub entry_keep_bps: f64,
    pub patient_exit_create_bps: f64,
    pub patient_exit_keep_bps: f64,
    pub normal_exit_create_bps: f64,
    pub normal_exit_keep_bps: f64,
    pub maker_soft_timeout_ms: u64,
    pub maker_hard_timeout_ms: u64,
}

impl OrderPolicy {
    pub const fn standalone_default() -> Self {
        Self {
            entry_create_bps: 0.0,
            entry_keep_bps: -2.0,
            patient_exit_create_bps: -2.0,
            patient_exit_keep_bps: 0.0,
            normal_exit_create_bps: 0.0,
            normal_exit_keep_bps: 2.0,
            maker_soft_timeout_ms: 60_000,
            maker_hard_timeout_ms: 300_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderFlow {
    Entry,
    PatientExit,
    NormalExit,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RestingAction {
    Keep,
    CancelBasis,
    CancelOffTouch,
    CancelHardTimeout,
}

pub fn entry_touch_basis_bps(
    policy: &OrderPolicy,
    spot_limit_px: f64,
    remaining_qty: f64,
    perp_bid_px: f64,
    perp_bid_qty: f64,
) -> Option<f64> {
    let basis_bps = (perp_bid_px / spot_limit_px - 1.0) * 10_000.0;
    if !keep_allowed(policy, OrderFlow::Entry, basis_bps) || perp_bid_qty + 1e-10 >= remaining_qty {
        Some(basis_bps)
    } else {
        None
    }
}

pub fn exit_touch_basis_bps(
    policy: &OrderPolicy,
    flow: OrderFlow,
    spot_limit_px: f64,
    remaining_qty: f64,
    perp_ask_px: f64,
    perp_ask_qty: f64,
) -> Option<f64> {
    let basis_bps = (perp_ask_px / spot_limit_px - 1.0) * 10_000.0;
    if !keep_allowed(policy, flow, basis_bps) || perp_ask_qty + 1e-10 >= remaining_qty {
        Some(basis_bps)
    } else {
        None
    }
}

pub fn create_gate_bps(policy: &OrderPolicy, flow: OrderFlow) -> f64 {
    match flow {
        OrderFlow::Entry => policy.entry_create_bps,
        OrderFlow::PatientExit => policy.patient_exit_create_bps,
        OrderFlow::NormalExit => policy.normal_exit_create_bps,
    }
}

pub fn create_allowed(policy: &OrderPolicy, flow: OrderFlow, basis_bps: f64) -> bool {
    match flow {
        OrderFlow::Entry => basis_bps + 1e-10 >= create_gate_bps(policy, flow),
        OrderFlow::PatientExit | OrderFlow::NormalExit => {
            basis_bps <= create_gate_bps(policy, flow) + 1e-10
        }
    }
}

pub fn keep_allowed(policy: &OrderPolicy, flow: OrderFlow, live_basis_bps: f64) -> bool {
    match flow {
        OrderFlow::Entry => live_basis_bps + 1e-10 >= policy.entry_keep_bps,
        OrderFlow::PatientExit => live_basis_bps <= policy.patient_exit_keep_bps + 1e-10,
        OrderFlow::NormalExit => live_basis_bps <= policy.normal_exit_keep_bps + 1e-10,
    }
}

pub fn resting_action(
    policy: &OrderPolicy,
    flow: OrderFlow,
    live_basis_bps: f64,
    age_ms: u64,
    at_touch: bool,
) -> RestingAction {
    if age_ms >= policy.maker_hard_timeout_ms {
        RestingAction::CancelHardTimeout
    } else if !keep_allowed(policy, flow, live_basis_bps) {
        RestingAction::CancelBasis
    } else if age_ms >= policy.maker_soft_timeout_ms && !at_touch {
        RestingAction::CancelOffTouch
    } else {
        RestingAction::Keep
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> OrderPolicy {
        OrderPolicy::standalone_default()
    }

    #[test]
    fn entry_uses_zero_to_create_minus_two_to_keep() {
        let policy = policy();
        assert!(!create_allowed(&policy, OrderFlow::Entry, -0.01));
        assert!(create_allowed(&policy, OrderFlow::Entry, 0.0));
        assert!(keep_allowed(&policy, OrderFlow::Entry, -2.0));
        assert!(!keep_allowed(&policy, OrderFlow::Entry, -2.01));
    }

    #[test]
    fn exit_modes_have_distinct_deadbands() {
        let policy = policy();
        assert!(create_allowed(&policy, OrderFlow::PatientExit, -2.0));
        assert!(!create_allowed(&policy, OrderFlow::PatientExit, -1.99));
        assert!(keep_allowed(&policy, OrderFlow::PatientExit, 0.0));
        assert!(!keep_allowed(&policy, OrderFlow::PatientExit, 0.01));
        assert!(create_allowed(&policy, OrderFlow::NormalExit, 0.0));
        assert!(!create_allowed(&policy, OrderFlow::NormalExit, 0.01));
        assert!(keep_allowed(&policy, OrderFlow::NormalExit, 2.0));
        assert!(!keep_allowed(&policy, OrderFlow::NormalExit, 2.01));
    }

    #[test]
    fn sixty_seconds_only_cancels_an_off_touch_order() {
        assert_eq!(
            resting_action(&policy(), OrderFlow::Entry, 1.0, 60_000, true),
            RestingAction::Keep
        );
        assert_eq!(
            resting_action(&policy(), OrderFlow::Entry, 1.0, 60_000, false),
            RestingAction::CancelOffTouch
        );
    }

    #[test]
    fn basis_and_hard_timeout_override_queue_preservation() {
        assert_eq!(
            resting_action(&policy(), OrderFlow::Entry, -2.01, 5_000, true),
            RestingAction::CancelBasis
        );
        assert_eq!(
            resting_action(&policy(), OrderFlow::Entry, 3.0, 300_000, true),
            RestingAction::CancelHardTimeout
        );
    }

    #[test]
    fn negative_top_basis_is_actionable_even_when_top_depth_is_short() {
        assert!(entry_touch_basis_bps(&policy(), 100.0, 10.0, 99.97, 1.0).unwrap() < -2.0);
        assert_eq!(
            entry_touch_basis_bps(&policy(), 100.0, 10.0, 100.01, 1.0),
            None
        );
        assert!(entry_touch_basis_bps(&policy(), 100.0, 10.0, 100.01, 10.0).is_some());
        let mut dynamic = policy();
        dynamic.entry_keep_bps = 2.0;
        assert!(entry_touch_basis_bps(&dynamic, 100.0, 10.0, 100.01, 1.0).is_some());
    }

    #[test]
    fn bad_exit_basis_is_actionable_even_when_top_depth_is_short() {
        assert!(
            exit_touch_basis_bps(&policy(), OrderFlow::NormalExit, 100.0, 10.0, 100.03, 1.0)
                .unwrap()
                > 2.0
        );
        assert_eq!(
            exit_touch_basis_bps(&policy(), OrderFlow::NormalExit, 100.0, 10.0, 100.01, 1.0),
            None
        );
        assert!(
            exit_touch_basis_bps(&policy(), OrderFlow::NormalExit, 100.0, 10.0, 100.01, 10.0)
                .is_some()
        );
        assert!(
            exit_touch_basis_bps(&policy(), OrderFlow::PatientExit, 100.0, 10.0, 100.01, 1.0)
                .unwrap()
                > 0.0
        );
    }

    #[test]
    fn policy_changes_take_effect_without_code_changes() {
        let mut changed = policy();
        changed.entry_keep_bps = 1.0;
        changed.normal_exit_keep_bps = 1.0;
        changed.maker_soft_timeout_ms = 10_000;
        assert!(!keep_allowed(&changed, OrderFlow::Entry, 0.5));
        assert!(!keep_allowed(&changed, OrderFlow::NormalExit, 1.5));
        assert_eq!(
            resting_action(&changed, OrderFlow::Entry, 2.0, 10_000, false),
            RestingAction::CancelOffTouch
        );
    }
}
