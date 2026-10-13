use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::state_io::{durable_remove, durable_write, state_path};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActiveAttemptState {
    pub attempt_id: String,
    pub account: String,
    pub action: String,
    pub config_hash: String,
    pub baseline_spot_qty: f64,
    pub baseline_perp_qty: f64,
    pub requested_notional_usd: f64,
    pub submitted_oid: u64,
    pub started_ts_ms: i64,
}

pub struct ActiveAttempt {
    path: PathBuf,
    state: ActiveAttemptState,
    finished: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecoveryStep {
    CancelOrders,
    HedgeDelta,
    HaltDust,
    ClearAttempt,
    Ready,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RecoveryObjective {
    Hedged,
    Flat,
}

pub fn recovery_objective(action: &str) -> Result<RecoveryObjective> {
    match action {
        "open_s3" | "close_s3" | "pmr_taker_reduce" => Ok(RecoveryObjective::Hedged),
        "emergency_close" => Ok(RecoveryObjective::Flat),
        _ => bail!("unknown active HYPE Funding Carry attempt action {action}"),
    }
}

pub fn recovery_step(
    active_attempt: bool,
    open_order_count: usize,
    residual_delta_usd: f64,
    delta_tolerance_usd: f64,
) -> RecoveryStep {
    if open_order_count > 0 {
        RecoveryStep::CancelOrders
    } else if residual_delta_usd >= delta_tolerance_usd {
        RecoveryStep::HedgeDelta
    } else if active_attempt && residual_delta_usd > 0.01 {
        RecoveryStep::HaltDust
    } else if active_attempt {
        RecoveryStep::ClearAttempt
    } else {
        RecoveryStep::Ready
    }
}

impl ActiveAttempt {
    #[allow(clippy::too_many_arguments)]
    pub fn begin(
        account: &str,
        attempt_id: &str,
        action: &str,
        config_hash: &str,
        baseline_spot_qty: f64,
        baseline_perp_qty: f64,
        requested_notional_usd: f64,
        started_ts_ms: i64,
    ) -> Result<Self> {
        let path = path(account);
        if path.exists() {
            bail!("an HYPE Funding Carry attempt is already active; live recovery is required");
        }
        let state = ActiveAttemptState {
            attempt_id: attempt_id.to_string(),
            account: account.to_string(),
            action: action.to_string(),
            config_hash: config_hash.to_string(),
            baseline_spot_qty,
            baseline_perp_qty,
            requested_notional_usd,
            submitted_oid: 0,
            started_ts_ms,
        };
        save(&path, &state)?;
        Ok(Self {
            path,
            state,
            finished: false,
        })
    }

    pub fn submitted(&mut self, oid: u64) -> Result<()> {
        self.state.submitted_oid = oid;
        save(&self.path, &self.state)
    }

    pub fn finish(mut self) -> Result<()> {
        durable_remove(&self.path).context("clear completed HYPE Funding Carry active attempt")?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for ActiveAttempt {
    fn drop(&mut self) {
        if !self.finished {
            let _ = save(&self.path, &self.state);
        }
    }
}

pub fn load(account: &str) -> Result<Option<ActiveAttemptState>> {
    let path = path(account);
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(Some(
            serde_json::from_str(&raw).context("parse HYPE Funding Carry active attempt")?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("read HYPE Funding Carry active attempt"),
    }
}

pub fn clear_recovered(account: &str) -> Result<()> {
    let path = path(account);
    durable_remove(&path).context("clear recovered HYPE Funding Carry attempt")
}

fn path(account: &str) -> PathBuf {
    if let Ok(path) = std::env::var("HFC_ACTIVE_ATTEMPT_PATH") {
        return PathBuf::from(path);
    }
    let account = account.trim_start_matches("0x").to_ascii_lowercase();
    state_path(format!("hfc_active_attempt_{account}.json"))
}

fn save(path: &Path, state: &ActiveAttemptState) -> Result<()> {
    durable_write(path, &serde_json::to_vec_pretty(state)?)
        .context("write HYPE Funding Carry attempt state")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn active_attempt_survives_drop_until_recovered() {
        let file =
            std::env::temp_dir().join(format!("hype-funding-carry-attempt-{}", Uuid::new_v4()));
        std::env::set_var("HFC_ACTIVE_ATTEMPT_PATH", &file);
        let attempt =
            ActiveAttempt::begin("0xabc", "id", "open", "hash", 1.0, -1.0, 20.0, 1).unwrap();
        drop(attempt);
        assert_eq!(load("0xabc").unwrap().unwrap().attempt_id, "id");
        clear_recovered("0xabc").unwrap();
        std::env::remove_var("HFC_ACTIVE_ATTEMPT_PATH");
    }

    #[test]
    fn recovery_always_cancels_then_hedges_before_clearing() {
        assert_eq!(
            recovery_step(true, 1, 200.0, 10.0),
            RecoveryStep::CancelOrders
        );
        assert_eq!(
            recovery_step(true, 0, 200.0, 10.0),
            RecoveryStep::HedgeDelta
        );
        assert_eq!(recovery_step(true, 0, 9.99, 10.0), RecoveryStep::HaltDust);
        assert_eq!(
            recovery_step(true, 0, 0.0, 10.0),
            RecoveryStep::ClearAttempt
        );
        assert_eq!(recovery_step(false, 0, 9.99, 10.0), RecoveryStep::Ready);
    }

    #[test]
    fn recovery_objective_is_action_aware() {
        assert_eq!(
            recovery_objective("open_s3").unwrap(),
            RecoveryObjective::Hedged
        );
        assert_eq!(
            recovery_objective("pmr_taker_reduce").unwrap(),
            RecoveryObjective::Hedged
        );
        assert_eq!(
            recovery_objective("emergency_close").unwrap(),
            RecoveryObjective::Flat
        );
        assert!(recovery_objective("typo").is_err());
    }
}
