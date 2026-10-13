use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::risk::RiskAction;
use crate::state_io::{durable_remove, durable_write, state_path};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Maker,
    Taker,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    mode: Mode,
    target_pmr: f64,
}

pub fn effective_action(
    account: &str,
    observed: RiskAction,
    pmr: f64,
    maker_target_pmr: f64,
) -> Result<Option<RiskAction>> {
    effective_action_path(path(account), observed, pmr, maker_target_pmr)
}

fn effective_action_path(
    path: PathBuf,
    observed: RiskAction,
    pmr: f64,
    maker_target_pmr: f64,
) -> Result<Option<RiskAction>> {
    let mut state = load(&path)?;
    match observed {
        RiskAction::TakerReduce { target_pmr } => {
            state = Some(State {
                mode: Mode::Taker,
                target_pmr,
            });
        }
        RiskAction::MakerReduce { target_pmr } if state.is_none() => {
            state = Some(State {
                mode: Mode::Maker,
                target_pmr,
            });
        }
        _ => {}
    }

    let Some(mut phase) = state else {
        return Ok(None);
    };
    if phase.mode == Mode::Taker && pmr <= phase.target_pmr {
        phase = State {
            mode: Mode::Maker,
            target_pmr: maker_target_pmr,
        };
    }
    if pmr <= phase.target_pmr {
        clear(&path)?;
        return Ok(None);
    }
    save(&path, &phase)?;
    Ok(Some(match phase.mode {
        Mode::Maker => RiskAction::MakerReduce {
            target_pmr: phase.target_pmr,
        },
        Mode::Taker => RiskAction::TakerReduce {
            target_pmr: phase.target_pmr,
        },
    }))
}

fn path(account: &str) -> PathBuf {
    let account = account.trim_start_matches("0x").to_ascii_lowercase();
    state_path(format!("hfc_risk_phase_{account}.json"))
}

fn load(path: &Path) -> Result<Option<State>> {
    match std::fs::read_to_string(path) {
        Ok(raw) => Ok(Some(
            serde_json::from_str(&raw).context("parse HYPE Funding Carry risk phase")?,
        )),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).context("read HYPE Funding Carry risk phase"),
    }
}

fn save(path: &Path, state: &State) -> Result<()> {
    durable_write(path, &serde_json::to_vec_pretty(state)?)
        .context("write HYPE Funding Carry risk phase")
}

fn clear(path: &Path) -> Result<()> {
    durable_remove(path).context("clear HYPE Funding Carry risk phase")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn taker_phase_latches_to_072_then_maker_to_065() {
        let path =
            std::env::temp_dir().join(format!("hype-funding-carry-risk-phase-{}", Uuid::new_v4()));
        assert_eq!(
            effective_action_path(
                path.clone(),
                RiskAction::TakerReduce { target_pmr: 0.72 },
                0.80,
                0.65,
            )
            .unwrap(),
            Some(RiskAction::TakerReduce { target_pmr: 0.72 })
        );
        assert_eq!(
            effective_action_path(path.clone(), RiskAction::Normal, 0.74, 0.65).unwrap(),
            Some(RiskAction::TakerReduce { target_pmr: 0.72 })
        );
        assert_eq!(
            effective_action_path(path.clone(), RiskAction::Normal, 0.70, 0.65).unwrap(),
            Some(RiskAction::MakerReduce { target_pmr: 0.65 })
        );
        assert_eq!(
            effective_action_path(path.clone(), RiskAction::Normal, 0.64, 0.65).unwrap(),
            None
        );
        assert!(!path.exists());
    }
}
