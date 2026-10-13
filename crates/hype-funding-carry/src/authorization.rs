use anyhow::{bail, Result};

const PRODUCTION_TOKEN: &str = "HYPE_FUNDING_CARRY_LIVE";

pub fn require_production_go_live() -> Result<()> {
    if std::env::var("HFC_PRODUCTION_GO_LIVE_AUTHORIZATION").as_deref() != Ok(PRODUCTION_TOKEN) {
        bail!("production HYPE Funding Carry execution requires the explicit go-live authorization token");
    }
    Ok(())
}
