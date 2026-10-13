pub mod accounting;
pub mod api;
pub mod asset_domain;
pub mod attempt_state;
pub mod audit;
pub mod authorization;
pub mod config;
pub mod controller;
pub mod controller_runtime;
pub mod deployment_phase;
pub mod entry_policy;
#[cfg(feature = "live-sdk")]
pub mod fill_notify;
#[cfg(feature = "live-sdk")]
pub mod live;
pub mod model;
pub mod notify;
pub mod order_policy;
#[cfg(feature = "live-sdk")]
mod order_signing;
pub mod portfolio;
pub mod risk;
pub mod risk_phase;
#[cfg(feature = "live-sdk")]
pub mod secret;
pub mod state_io;
pub mod writer_lock;
pub mod ws;
pub mod ws_post;

pub const EXECUTOR_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const DRY_RUN_PARAMETER_VERSION: &str = "hype-funding-carry-dry-run-v1";
