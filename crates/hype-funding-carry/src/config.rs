use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::order_policy::OrderPolicy;
use crate::risk::RiskThresholds;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountMode {
    Eoa,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentBand {
    pub lower: f64,
    pub target: f64,
    pub upper: f64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EntryPolicyConfig {
    pub negative_next_funding_weight: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub parameter_version: String,
    pub account_mode: AccountMode,
    pub target_deployment_ratio: f64,
    pub deployment_band: Option<DeploymentBand>,
    pub entry_create_bps: f64,
    pub entry_keep_bps: f64,
    pub patient_exit_create_bps: f64,
    pub patient_exit_keep_bps: f64,
    pub normal_exit_create_bps: f64,
    pub normal_exit_keep_bps: f64,
    pub maker_soft_timeout_ms: u64,
    pub maker_hard_timeout_ms: u64,
    pub delta_emergency_usd: f64,
    pub naked_timeout_ms: u64,
    pub min_trade_notional_usd: f64,
    pub max_single_order_notional_usd: f64,
    pub hedge_depth_utilization: f64,
    pub max_market_age_ms: u64,
    pub max_market_clock_skew_ms: u64,
    pub max_leg_timestamp_skew_ms: u64,
    pub agent_expiry_warning_ms: u64,
    pub entry_policy: EntryPolicyConfig,
    pub risk: RiskThresholds,
}

impl StrategyConfig {
    pub fn order_policy(&self) -> OrderPolicy {
        OrderPolicy {
            entry_create_bps: self.entry_create_bps,
            entry_keep_bps: self.entry_keep_bps,
            patient_exit_create_bps: self.patient_exit_create_bps,
            patient_exit_keep_bps: self.patient_exit_keep_bps,
            normal_exit_create_bps: self.normal_exit_create_bps,
            normal_exit_keep_bps: self.normal_exit_keep_bps,
            maker_soft_timeout_ms: self.maker_soft_timeout_ms,
            maker_hard_timeout_ms: self.maker_hard_timeout_ms,
        }
    }
    #[cfg(test)]
    pub fn reference_fixture() -> Self {
        // Original decision-test profile; separate from the production example.
        serde_json::from_str(include_str!("../../../config/decision-test.json")).unwrap()
    }

    #[cfg(test)]
    pub fn eoa_test_default() -> Self {
        let mut config = Self::reference_fixture();
        config.parameter_version = "hype-funding-carry-eoa-test-v1".to_string();
        config.account_mode = AccountMode::Eoa;
        config.target_deployment_ratio = 1.20;
        config.deployment_band = None;
        config.risk.taker_reduce_pmr = 0.85;
        config
    }

    #[cfg(test)]
    pub fn eoa_fixture() -> Self {
        let mut config = Self::reference_fixture();
        config.parameter_version = "hype-funding-carry-eoa-timing-v3".to_string();
        config.account_mode = AccountMode::Eoa;
        config
    }

    pub fn load(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("read HYPE Funding Carry config {}", path.display()))?;
        let config: Self = serde_json::from_str(&raw)
            .with_context(|| format!("parse HYPE Funding Carry config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.parameter_version.trim().is_empty() {
            bail!("parameter_version is required");
        }

        if !self.target_deployment_ratio.is_finite() || self.target_deployment_ratio <= 0.0 {
            bail!("target_deployment_ratio must be positive");
        }
        if let Some(band) = self.deployment_band {
            if !band.lower.is_finite()
                || !band.target.is_finite()
                || !band.upper.is_finite()
                || !(0.0 < band.lower && band.lower < band.target && band.target < band.upper)
            {
                bail!("deployment band must satisfy 0 < lower < target < upper");
            }
            if (band.target - self.target_deployment_ratio).abs() > 1e-12 {
                bail!("deployment band target must match target_deployment_ratio");
            }
        }
        for basis in [
            self.entry_create_bps,
            self.entry_keep_bps,
            self.patient_exit_create_bps,
            self.patient_exit_keep_bps,
            self.normal_exit_create_bps,
            self.normal_exit_keep_bps,
        ] {
            if !basis.is_finite() {
                bail!("basis thresholds must be finite");
            }
        }
        if self.entry_keep_bps > self.entry_create_bps
            || self.patient_exit_create_bps > self.patient_exit_keep_bps
            || self.normal_exit_create_bps > self.normal_exit_keep_bps
        {
            bail!("basis create/keep hysteresis is invalid");
        }
        if !(0.0 < self.risk.warning_pmr
            && self.risk.warning_pmr < self.risk.maker_reduce_pmr
            && self.risk.maker_reduce_pmr < self.risk.taker_reduce_pmr
            && self.risk.taker_reduce_pmr < 0.95)
        {
            bail!("PMR thresholds must increase and remain below liquidation");
        }
        if !(self.risk.maker_target_pmr < self.risk.maker_reduce_pmr
            && self.risk.taker_target_pmr < self.risk.taker_reduce_pmr)
        {
            bail!("PMR reduction targets must be below their triggers");
        }
        if self.maker_soft_timeout_ms == 0
            || self.maker_soft_timeout_ms >= self.maker_hard_timeout_ms
        {
            bail!("maker timeout must satisfy 0 < soft < hard");
        }
        if self.min_trade_notional_usd <= 0.0
            || self.max_single_order_notional_usd < self.min_trade_notional_usd
            || !(0.0..=1.0).contains(&self.hedge_depth_utilization)
            || self.hedge_depth_utilization == 0.0
            || self.max_market_age_ms == 0
            || self.max_market_clock_skew_ms == 0
            || self.max_leg_timestamp_skew_ms == 0
            || self.agent_expiry_warning_ms == 0
        {
            bail!("invalid notional, age, or cycle adjustment limit");
        }
        let policy = self.entry_policy;
        if !policy.negative_next_funding_weight.is_finite()
            || !(0.0..=5.0).contains(&policy.negative_next_funding_weight)
        {
            bail!("invalid entry policy configuration");
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(self).context("serialize canonical HYPE Funding Carry config")
    }

    pub fn config_hash(&self) -> Result<String> {
        let digest = Sha256::digest(self.canonical_json()?.as_bytes());
        Ok(format!("{digest:x}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_example_keeps_the_recorded_production_parameters() {
        let config: StrategyConfig =
            serde_json::from_str(include_str!("../../../config/strategy.example.json")).unwrap();
        config.validate().unwrap();
        assert_eq!(config.account_mode, AccountMode::Eoa);
        assert_eq!(config.target_deployment_ratio, 1.4);
        assert_eq!(config.entry_create_bps, 3.0);
        assert_eq!(config.entry_keep_bps, 1.5);
        assert_eq!(config.risk.taker_reduce_pmr, 0.8);
    }

    #[test]
    fn manual_test_profile_can_disable_the_band() {
        let config = StrategyConfig::eoa_test_default();
        config.validate().unwrap();
        assert_eq!(config.target_deployment_ratio, 1.20);
        assert_eq!(config.deployment_band, None);
        assert_eq!(config.risk.taker_reduce_pmr, 0.85);
    }

    #[test]
    fn account_fixture_uses_the_production_strategy_rules() {
        let config = StrategyConfig::eoa_fixture();
        config.validate().unwrap();
        assert_eq!(config.account_mode, AccountMode::Eoa);
        assert_eq!(config.target_deployment_ratio, 1.30);
        assert!(config.deployment_band.is_some());
        assert_eq!(config.risk.taker_reduce_pmr, 0.80);
    }

    #[test]
    fn config_hash_is_stable_and_tracks_changes() {
        let first = StrategyConfig::reference_fixture();
        let mut second = first.clone();
        assert_eq!(first.config_hash().unwrap(), second.config_hash().unwrap());
        second.entry_create_bps = 3.0;
        assert_ne!(first.config_hash().unwrap(), second.config_hash().unwrap());
    }

    #[test]
    fn rejects_mismatched_band_target() {
        let mut config = StrategyConfig::reference_fixture();
        config.deployment_band.as_mut().unwrap().target = 1.31;
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_unknown_fields_in_json() {
        let mut value = serde_json::to_value(StrategyConfig::reference_fixture()).unwrap();
        value["entry_creat_bps"] = serde_json::json!(2.0);
        assert!(serde_json::from_value::<StrategyConfig>(value).is_err());

        let mut value = serde_json::to_value(StrategyConfig::reference_fixture()).unwrap();
        value["min_core_equity_usd"] = serde_json::json!(10_050.0);
        assert!(serde_json::from_value::<StrategyConfig>(value).is_err());
    }

    #[test]
    fn rejects_unknown_fields_inside_nested_config() {
        let mut value = serde_json::to_value(StrategyConfig::reference_fixture()).unwrap();
        value["entry_policy"]["negative_next_funding_weight_typo"] = serde_json::json!(1.0);
        assert!(serde_json::from_value::<StrategyConfig>(value).is_err());

        let mut value = serde_json::to_value(StrategyConfig::reference_fixture()).unwrap();
        value["risk"]["warning_typo"] = serde_json::json!(0.65);
        assert!(serde_json::from_value::<StrategyConfig>(value).is_err());
    }

    #[test]
    fn rejects_retired_carry_assumptions() {
        for field in [
            "long_term_funding_apr",
            "funding_confidence",
            "min_net_carry_apr",
            "perp_initial_margin_ratio",
        ] {
            let mut value = serde_json::to_value(StrategyConfig::reference_fixture()).unwrap();
            value["entry_policy"][field] = serde_json::json!(0.1);
            assert!(serde_json::from_value::<StrategyConfig>(value).is_err());
        }
    }
}
