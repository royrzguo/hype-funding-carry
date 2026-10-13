# HYPE Funding Carry on Hyperliquid

[简体中文](README.zh-CN.md)

A Rust reference implementation of a HYPE funding carry strategy on Hyperliquid,
using delta-neutral spot and perpetual positions with Portfolio Margin.
Adapted from live trading code, it covers entry and exit, position management, risk controls,
recovery, and accounting. Strategy parameters and execution conditions can be adjusted
for account size and capital allocation.

## Architecture

A single process uses WebSocket for live market data, entry and hedging, and REST
for account checks and recovery. Execution state and audit logs are stored locally.

Source code is in `crates/hype-funding-carry/src/`:

| Module | Main files | Responsibility |
| --- | --- | --- |
| Market and account data | `api.rs`, `ws.rs` | Order books, account state, PM, borrowing, open orders, and fills |
| Entry conditions | `entry_policy.rs`, `model.rs` | Basis thresholds, depth, and next-period funding adjustments |
| Position management | `portfolio.rs`, `deployment_phase.rs` | Paired positions, target allocation, and incremental adjustments |
| Risk controls | `risk.rs`, `risk_phase.rs`, `asset_domain.rs` | PM risk, deleveraging, residual exposure, and asset-domain checks |
| Execution | `order_policy.rs`, `live/` | Maker orders, partial-fill hedging, IOC orders, cancellation, and exit |
| Recovery | `attempt_state.rs`, `writer_lock.rs`, `state_io.rs` | Execution checkpoints, restart recovery, and writer locks |
| Accounting | `accounting.rs`, `audit.rs`, `fill_notify.rs` | Account ledger, inventory, fees, and PnL |

`controller.rs`, `controller_runtime.rs`, and `controller_service.rs` coordinate these modules.
Recovery and risk controls take priority over increasing positions. Spot maker fills are
hedged with perpetuals based on the actual net filled quantity. Resting orders are managed
by basis, order-book, and timeout conditions. Funding, borrowing interest, and fees are
included in monitoring and accounting.

## Build and configuration

Requires Rust 1.91+. Linux also requires `pkg-config` and OpenSSL development packages
(`pkg-config libssl-dev` on Debian/Ubuntu). Dependencies are pinned by `Cargo.lock`
and a fixed SDK commit.

```bash
cargo build --release --locked
cp .env.example .env
```

Set the account address, API agent address, `HFC_PRIVATE_KEY_FILE`, and `HFC_STATE_DIR`
in `.env`. The signing key is read from a local file; on Unix, group and other permissions
must be disabled. The account must have Portfolio Margin enabled and authorize the API agent.

Strategy parameters are in [config/strategy.example.json](config/strategy.example.json):

- `target_deployment_ratio`, `deployment_band`: paired position notional divided by net equity, and the adjustment band.
- `entry_create_bps`, `entry_keep_bps`: thresholds for placing and retaining entry orders.
- `entry_policy`: adjustment of basis thresholds for negative next-period funding.
- `risk`: PMR thresholds and deleveraging targets.
- Execution notional, order-book utilization, residual exposure, and timeout settings: order and risk limits.

The implementation currently supports HYPE/USDC spot and HYPE perpetuals. Supporting
another asset requires changes to the trading pair, asset IDs, precision, and asset-domain checks.
The program manages all balances and positions in the supported asset within the account;
use a dedicated strategy account. Instances managing the same account must share a state
directory. The writer lock coordinates those instances only; it does not coordinate with
other trading programs.

## Running

Read-only checks do not load signing keys or submit orders:

```bash
./target/release/hype-funding-carry status
./target/release/hype-funding-carry plan --config config/strategy.example.json
./target/release/hype-funding-carry feed-check --seconds 5
```

Trading commands require `--execute` and a matching `--confirm-account`.
Set `HFC_PRODUCTION_GO_LIVE_AUTHORIZATION=HYPE_FUNDING_CARRY_LIVE` in `.env`
before starting the controller:

```bash
./target/release/hype-funding-carry serve \
  --config config/strategy.example.json \
  --execute \
  --confirm-account "0xYourHyperCoreAccountAddress" \
  --max-notional 50
```

`--confirm-account` must match the configured account. `--max-notional` limits the notional
of each ordinary position adjustment. Total allocation is governed by the deployment band;
risk deleveraging, residual handling, and recovery use their respective risk limits.

| Command | Purpose |
| --- | --- |
| `serve` | Run the controller continuously |
| `open` / `rebalance-once` | Perform a single entry or position adjustment; the authorization token above is also required |
| `open-once` | Wait for the configured gate and manage one $10–50 entry |
| `close` | Use `--notional` for a specified exit amount; omit it to target the entire paired position; `--wait` continues the exit |
| `recover` | Inspect recovery state without trading |
| `repair` | Flatten all HYPE spot and perpetual positions in the account, including paired positions |
| `cancel-all` | Cancel all open orders in the account |
| `signer-check` | Load the key and check the signing identity without submitting orders |

See `--help` for command options. `close --wait` cannot be combined with `--notional`.
An ordinary single exit may complete partially and then return. Exit and cleanup commands
do not require the authorization token for increasing exposure. On Ctrl-C or SIGTERM,
the controller attempts to cancel orders and reconcile residual exposure before stopping.
Use `close` to exit paired positions.

## Local state

`HFC_STATE_DIR` defaults to `state/` and contains execution checkpoints, accounting
baselines, process locks, and `audit/events.jsonl`. Preserve this directory across restarts.
The exchange is the source of truth for orders and positions. Logs are append-only;
runtime processing is incremental using fill, order, and account cursors. Logs are replayed
at startup to build indexes.

Accounting starts from the first local observation baseline. NAV changes are adjusted
for net deposits and withdrawals; existing positions are valued at baseline market prices.
Funding and fees come from exchange records. Borrowing interest is estimated from debt
and APR. Deleting state resets the observation period. Unsynchronized snapshots,
incomplete history, or inventory reconciliation failures produce accounting errors;
hedging of known positions continues.

## Development

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --no-default-features
```

## Agent skill

[`skills/hype-funding-carry`](skills/hype-funding-carry/SKILL.md) guides Claude Code and Codex
through setup, read-only checks, and operation. It does not place orders unless you ask.

- Claude Code: `mkdir -p ~/.claude/skills && cp -r skills/hype-funding-carry ~/.claude/skills/`
- Codex: ask `$skill-installer` to install
  `https://github.com/royrzguo/hype-funding-carry/tree/main/skills/hype-funding-carry`

## Disclaimer

This project is provided as open-source reference code. It is not investment advice
or a managed trading service. Trading commands place real orders using your account
and funds. Exchange rules, market conditions, liquidity, and software defects may cause
losses. Review the code, adjust parameters, and test with small amounts before use.
You are responsible for its use and any resulting losses.

## License

[MIT](LICENSE) · barker.money
