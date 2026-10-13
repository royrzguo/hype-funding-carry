use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::config::DeploymentBand;
use crate::state_io::{durable_remove, durable_write, state_path};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Add,
    Reduce,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct State {
    mode: Mode,
    #[serde(default)]
    band: Option<DeploymentBand>,
}

#[derive(Debug)]
pub struct DeploymentPhase {
    path: PathBuf,
    mode: Option<Mode>,
    band: Option<DeploymentBand>,
}

impl DeploymentPhase {
    pub fn load(account: &str, mode: &str) -> Result<Self> {
        Self::load_path(path(account, mode))
    }

    fn load_path(path: PathBuf) -> Result<Self> {
        let (mode, band) = match std::fs::read_to_string(&path) {
            Ok(raw) => {
                let state = serde_json::from_str::<State>(&raw)
                    .context("parse HYPE Funding Carry deployment phase")?;
                (Some(state.mode), state.band)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, None),
            Err(error) => return Err(error).context("read HYPE Funding Carry deployment phase"),
        };
        Ok(Self { path, mode, band })
    }

    pub fn add_active(&self) -> bool {
        self.mode == Some(Mode::Add)
    }

    pub fn reduce_active(&self) -> bool {
        self.mode == Some(Mode::Reduce)
    }

    pub fn observe(&mut self, deployment_ratio: f64, band: Option<DeploymentBand>) -> Result<()> {
        let Some(band) = band else {
            self.band = None;
            return self.set_mode(None);
        };
        if self.mode.is_some() && self.band != Some(band) {
            self.band = Some(band);
            self.set_mode(None)?;
        } else {
            self.band = Some(band);
        }
        match self.mode {
            Some(Mode::Add) if deployment_ratio >= band.target => self.set_mode(None)?,
            Some(Mode::Reduce) if deployment_ratio <= band.target => self.set_mode(None)?,
            _ => {}
        }
        if self.mode.is_none() && deployment_ratio < band.lower {
            self.set_mode(Some(Mode::Add))?;
        } else if self.mode.is_none() && deployment_ratio > band.upper {
            self.set_mode(Some(Mode::Reduce))?;
        }
        Ok(())
    }

    fn set_mode(&mut self, mode: Option<Mode>) -> Result<()> {
        if self.mode == mode {
            return Ok(());
        }
        if let Some(mode) = mode {
            save(
                &self.path,
                &State {
                    mode,
                    band: self.band,
                },
            )?;
        } else {
            clear(&self.path)?;
        }
        self.mode = mode;
        Ok(())
    }
}

fn path(account: &str, mode: &str) -> PathBuf {
    let account = account.trim_start_matches("0x").to_ascii_lowercase();
    state_path(format!("hfc_deployment_phase_{mode}_{account}.json"))
}

fn save(path: &Path, state: &State) -> Result<()> {
    durable_write(path, &serde_json::to_vec_pretty(state)?)
        .context("write HYPE Funding Carry deployment phase")
}

fn clear(path: &Path) -> Result<()> {
    durable_remove(path).context("clear HYPE Funding Carry deployment phase")
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn band() -> Option<DeploymentBand> {
        Some(DeploymentBand {
            lower: 1.0,
            target: 1.3,
            upper: 1.6,
        })
    }

    #[test]
    fn crossing_lower_latches_until_target_and_survives_restart() {
        let path = std::env::temp_dir().join(format!(
            "hype-funding-carry-deployment-phase-{}",
            Uuid::new_v4()
        ));
        let mut phase = DeploymentPhase::load_path(path.clone()).unwrap();
        phase.observe(0.99, band()).unwrap();
        assert!(phase.add_active());

        let mut restarted = DeploymentPhase::load_path(path.clone()).unwrap();
        restarted.observe(1.10, band()).unwrap();
        assert!(restarted.add_active());
        restarted.observe(1.30, band()).unwrap();
        assert!(!restarted.add_active());
        assert!(!path.exists());
    }

    #[test]
    fn crossing_upper_latches_reduce_until_target_and_survives_restart() {
        let path = std::env::temp_dir().join(format!(
            "hype-funding-carry-deployment-phase-{}",
            Uuid::new_v4()
        ));
        let mut phase = DeploymentPhase::load_path(path.clone()).unwrap();
        phase.observe(1.61, band()).unwrap();
        assert!(phase.reduce_active());

        let mut restarted = DeploymentPhase::load_path(path.clone()).unwrap();
        restarted.observe(1.50, band()).unwrap();
        assert!(restarted.reduce_active());
        restarted.observe(1.30, band()).unwrap();
        assert!(!restarted.reduce_active());
        assert!(!path.exists());
    }

    #[test]
    fn changing_the_band_clears_a_stale_phase() {
        let path = std::env::temp_dir().join(format!(
            "hype-funding-carry-deployment-phase-{}",
            Uuid::new_v4()
        ));
        let mut phase = DeploymentPhase::load_path(path.clone()).unwrap();
        phase.observe(1.61, band()).unwrap();
        assert!(phase.reduce_active());

        let changed = Some(DeploymentBand {
            lower: 1.0,
            target: 1.4,
            upper: 1.8,
        });
        phase.observe(1.61, changed).unwrap();
        assert!(!phase.reduce_active());
        assert!(!path.exists());
    }
}
