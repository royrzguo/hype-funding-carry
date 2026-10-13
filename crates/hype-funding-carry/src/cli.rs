use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use hype_funding_carry::model::Action;

#[derive(Parser)]
#[command(about = "HYPE Funding Carry on Hyperliquid")]
pub(super) struct Cli {
    #[command(subcommand)]
    pub(super) command: Command,
}

#[derive(Subcommand)]
pub(super) enum Command {
    /// Read account, PM, position, funding, order, and market state.
    Status {
        #[arg(long)]
        audit: bool,
    },
    /// Verify raw public/user websocket parsing without loading a signer.
    FeedCheck {
        #[arg(long, default_value_t = 5)]
        seconds: u64,
    },
    /// Evaluate one or more decisions without loading a signer.
    DryRun {
        #[arg(long, default_value_t = 500.0)]
        notional: f64,
        #[arg(long, value_enum, default_value_t = DryAction::Entry)]
        action: DryAction,
        #[arg(long, default_value_t = 0)]
        interval_seconds: u64,
        #[arg(long)]
        audit: bool,
    },
    /// Compute one account controller intent without loading a signer.
    Plan {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        audit: bool,
    },
    /// Run the account strategy controller.
    Serve {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: Option<String>,
        #[arg(long, default_value_t = 50.0)]
        max_notional: f64,
        #[arg(long, default_value_t = 30)]
        interval_seconds: u64,
    },
    /// Execute at most one explicitly confirmed deployment-band adjustment.
    #[cfg(feature = "live-sdk")]
    RebalanceOnce {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, default_value_t = 50.0)]
        max_notional: f64,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Wait for the configured gate and manage at most one $10-$50 permitted entry.
    #[cfg(feature = "live-sdk")]
    OpenOnce {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        notional: f64,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Validate the local agent key and pinned SDK without sending an action.
    #[cfg(feature = "live-sdk")]
    SignerCheck,
    /// Place one incremental spot-maker/perp-taker entry.
    #[cfg(feature = "live-sdk")]
    Open {
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long, default_value_t = 100.0)]
        notional: f64,
        #[arg(long, default_value_t = 0.0)]
        gate_bps: f64,
        #[arg(long)]
        keep_bps: Option<f64>,
        #[arg(long)]
        max_qty: Option<f64>,
        #[arg(long)]
        depth_utilization: Option<f64>,
        #[arg(long)]
        wait: bool,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Reduce a matched position using maker execution or an emergency taker exit.
    #[cfg(feature = "live-sdk")]
    Close {
        #[arg(long, value_enum, default_value_t = CloseMode::Normal)]
        mode: CloseMode,
        /// USD notional to reduce; omit to close the full matched position.
        #[arg(long)]
        notional: Option<f64>,
        /// Keep waiting and retrying until the full matched position is closed.
        #[arg(long)]
        wait: bool,
        /// Test-only: observe basis without blocking a maker exit.
        #[arg(long)]
        execution_probe_ignore_basis: bool,
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Flatten all HYPE spot and perpetual positions, leaving only sub-minimum dust.
    #[cfg(feature = "live-sdk")]
    Repair {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Rebuild normalized fills from Hyperliquid and audited order ids.
    #[cfg(feature = "live-sdk")]
    ReconcileFills,
    /// Cancel every open order in the account, requiring explicit live confirmation.
    #[cfg(feature = "live-sdk")]
    CancelAll {
        #[arg(long)]
        execute: bool,
        #[arg(long)]
        confirm_account: String,
    },
    /// Reconstruct startup state without taking an action.
    #[cfg(feature = "live-sdk")]
    Recover,
}

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum DryAction {
    Entry,
    Exit,
}

#[derive(Clone, Copy, ValueEnum)]
#[cfg(feature = "live-sdk")]
pub(super) enum CloseMode {
    Patient,
    Normal,
    Emergency,
}

#[cfg(feature = "live-sdk")]
impl From<CloseMode> for hype_funding_carry::live::ExitMode {
    fn from(value: CloseMode) -> Self {
        match value {
            CloseMode::Patient => Self::Patient,
            CloseMode::Normal => Self::Normal,
            CloseMode::Emergency => Self::Emergency,
        }
    }
}

impl From<DryAction> for Action {
    fn from(value: DryAction) -> Self {
        match value {
            DryAction::Entry => Action::Entry,
            DryAction::Exit => Action::Exit,
        }
    }
}
