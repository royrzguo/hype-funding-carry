use anyhow::{bail, Result};
use serde::Serialize;

#[derive(Clone, Copy, Debug)]
pub struct PortfolioInput {
    pub hype_spot_qty: f64,
    pub hype_perp_qty: f64,
    pub spot_bid_px: f64,
    pub spot_ask_px: f64,
    pub perp_bid_px: f64,
    pub perp_ask_px: f64,
    /// Equity the band is sized against. See ExternalState::core_equity_usd.
    pub sizing_equity_usd: f64,
    pub target_deployment_ratio: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct PortfolioState {
    pub reference_hype_px: f64,
    pub matched_hype_qty: f64,
    pub matched_notional_usd: f64,
    pub residual_delta_usd: f64,
    pub deployment_ratio: f64,
    pub target_matched_notional_usd: f64,
    pub target_delta_usd: f64,
}

pub fn calculate(input: &PortfolioInput) -> Result<PortfolioState> {
    for value in [
        input.spot_bid_px,
        input.spot_ask_px,
        input.perp_bid_px,
        input.perp_ask_px,
        input.sizing_equity_usd,
        input.target_deployment_ratio,
    ] {
        if !value.is_finite() || value <= 0.0 {
            bail!("portfolio inputs must be finite and positive");
        }
    }
    if input.hype_spot_qty < 0.0 || input.hype_perp_qty > 0.0 {
        bail!("HYPE Funding Carry portfolio must be spot long and perp short");
    }
    let spot_mid = (input.spot_bid_px + input.spot_ask_px) / 2.0;
    let perp_mid = (input.perp_bid_px + input.perp_ask_px) / 2.0;
    let reference_hype_px = (spot_mid + perp_mid) / 2.0;
    let matched_hype_qty = input.hype_spot_qty.min(input.hype_perp_qty.abs());
    let matched_notional_usd = matched_hype_qty * reference_hype_px;
    let residual_delta_usd = (input.hype_spot_qty + input.hype_perp_qty).abs() * reference_hype_px;
    let deployment_ratio = matched_notional_usd / input.sizing_equity_usd;
    let target_matched_notional_usd = input.sizing_equity_usd * input.target_deployment_ratio;
    Ok(PortfolioState {
        reference_hype_px,
        matched_hype_qty,
        matched_notional_usd,
        residual_delta_usd,
        deployment_ratio,
        target_matched_notional_usd,
        target_delta_usd: target_matched_notional_usd - matched_notional_usd,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HedgeLeg {
    BuySpot,
    SellPerp,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct HedgePlan {
    pub leg: HedgeLeg,
    pub qty: f64,
    pub notional_usd: f64,
}

pub fn uniquely_identifiable_missing_leg(
    hype_spot_qty: f64,
    hype_perp_qty: f64,
    reference_hype_px: f64,
    dust_usd: f64,
    open_orders_fully_known_and_empty: bool,
    no_unknown_assets_or_positions: bool,
) -> Option<HedgePlan> {
    if !open_orders_fully_known_and_empty
        || !no_unknown_assets_or_positions
        || !reference_hype_px.is_finite()
        || reference_hype_px <= 0.0
        || hype_spot_qty < 0.0
        || hype_perp_qty > 0.0
    {
        return None;
    }
    let delta_qty = hype_spot_qty + hype_perp_qty;
    let notional_usd = delta_qty.abs() * reference_hype_px;
    if notional_usd <= dust_usd {
        return None;
    }
    Some(HedgePlan {
        leg: if delta_qty > 0.0 {
            HedgeLeg::SellPerp
        } else {
            HedgeLeg::BuySpot
        },
        qty: delta_qty.abs(),
        notional_usd,
    })
}

pub fn projected_core_equity(current_equity_usd: f64, outflow_usd: f64) -> f64 {
    current_equity_usd - outflow_usd.max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_notional_uses_equal_quantity() {
        let state = calculate(&PortfolioInput {
            hype_spot_qty: 30.0,
            hype_perp_qty: -29.5,
            spot_bid_px: 49.9,
            spot_ask_px: 50.1,
            perp_bid_px: 50.0,
            perp_ask_px: 50.2,
            sizing_equity_usd: 1_000.0,
            target_deployment_ratio: 1.30,
        })
        .unwrap();
        assert!((state.reference_hype_px - 50.05).abs() < 1e-12);
        assert!((state.matched_hype_qty - 29.5).abs() < 1e-12);
        assert!((state.residual_delta_usd - 25.025).abs() < 1e-9);
        assert!(state.deployment_ratio > 1.47 && state.deployment_ratio < 1.48);
    }

    #[test]
    fn unique_hedge_requires_known_empty_orders() {
        assert_eq!(
            uniquely_identifiable_missing_leg(2.0, -1.0, 50.0, 10.0, true, true),
            Some(HedgePlan {
                leg: HedgeLeg::SellPerp,
                qty: 1.0,
                notional_usd: 50.0,
            })
        );
        assert_eq!(
            uniquely_identifiable_missing_leg(2.0, -1.0, 50.0, 10.0, false, true),
            None
        );
    }

    #[test]
    fn dust_and_wrong_direction_are_not_auto_hedged() {
        assert_eq!(
            uniquely_identifiable_missing_leg(1.01, -1.0, 50.0, 10.0, true, true),
            None
        );
        assert_eq!(
            uniquely_identifiable_missing_leg(1.0, 1.0, 50.0, 10.0, true, true),
            None
        );
    }
}
