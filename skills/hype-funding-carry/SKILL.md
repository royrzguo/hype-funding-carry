---
name: hype-funding-carry
description: Set up, operate, and troubleshoot the HYPE Funding Carry Rust reference (delta-neutral HYPE spot + perpetual on Hyperliquid Portfolio Margin). Use when a user wants to build it, configure an account and API agent, run read-only checks, start or stop the controller, exit positions, or understand its state and logs.
---

# HYPE Funding Carry

Source: https://github.com/royrzguo/hype-funding-carry

A single Rust process that holds HYPE spot against a short HYPE perpetual on a Hyperliquid
Portfolio Margin account. Spot is bought with maker orders when the executable basis clears
the configured gate; every spot fill is hedged on the perpetual by its net filled quantity.
State and audit logs are local files.

## Safety rules for the agent

- Trading commands place real orders with the user's funds. Never add `--execute`, set
  `HFC_PRODUCTION_GO_LIVE_AUTHORIZATION`, or run `serve`, `open`, `open-once`,
  `rebalance-once`, `close`, `repair`, or `cancel-all` unless the user explicitly asks for
  that exact action in this conversation.
- Never print, copy, or move the agent private key. Only reference it through
  `HFC_PRIVATE_KEY_FILE`.
- The program manages all HYPE balances, HYPE positions, and orders in the account.
  Use a dedicated account. If the account holds other strategies, stay with read-only commands.
- `repair` flattens all HYPE spot and perpetual positions; `cancel-all` cancels every order
  in the account. Explain this before the user runs either.
- Do not delete `HFC_STATE_DIR`. It holds execution checkpoints and the accounting baseline.
- After a failed or interrupted trade, run `recover` and `status` first. Do not rerun the
  trading command until the reported state is understood.

## Setup

1. Requirements: Rust 1.91+. On Linux also `pkg-config` and OpenSSL headers
   (`pkg-config libssl-dev` on Debian/Ubuntu).
2. Build: `cargo build --release --locked`
3. Configure: `cp .env.example .env`, then set `HFC_ACCOUNT_ADDRESS`, `HFC_AGENT_ADDRESS`,
   `HFC_PRIVATE_KEY_FILE`, and `HFC_STATE_DIR`. The key file must not be readable by group
   or others (`chmod 600`).
4. Account: enable Portfolio Margin and authorize the API agent on Hyperliquid.
5. Parameters: copy `config/strategy.example.json` and adjust deployment ratio and band,
   entry thresholds, PMR limits, and per-order notional for the account size.

## Read-only checks (no key loaded, no orders)

```bash
./target/release/hype-funding-carry status
./target/release/hype-funding-carry plan --config config/strategy.example.json
./target/release/hype-funding-carry feed-check --seconds 5
./target/release/hype-funding-carry recover
./target/release/hype-funding-carry signer-check   # loads the key, sends nothing
```

Explain the output to the user: PM state and PMR, HYPE spot and perpetual positions, open
orders, the asset-domain scan (other assets block new risk), and the controller intent.

## Trading (only on explicit user request)

All trading commands require `--execute` and `--confirm-account <account address>`.
Commands that add exposure also require `HFC_PRODUCTION_GO_LIVE_AUTHORIZATION=HYPE_FUNDING_CARRY_LIVE`.

- Small first entry: `open-once --notional <10-50> --execute --confirm-account <addr>`
- Continuous controller: `serve --config <file> --execute --confirm-account <addr> --max-notional 50`
  (Ctrl-C or SIGTERM cancels orders and reconciles before stopping; positions stay open.)
- Exit: `close --execute --confirm-account <addr>` with `--notional <usd>` for a partial exit,
  `--wait` to keep going until fully closed, or `--mode emergency` for an immediate taker exit.

## Logs and state

- `HFC_STATE_DIR/audit/events.jsonl`: append-only orders, fills, decisions, and accounting.
- Set `HFC_TRACE_IO=1` for request timing.
- Accounting starts from the first local observation; deleting state resets it.

This is reference code, not investment advice. The user is responsible for its use and any losses.
