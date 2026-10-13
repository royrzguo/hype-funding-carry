# HYPE Funding Carry on Hyperliquid reference

Read README.md before editing. This is a personal-account reference
implementation, not a managed trading service.

- Keep personal-account scope explicit; avoid operator-specific deployment details.
- Preserve partial-fill hedging, PM checks, asset-domain checks, account confirmation,
  the single-writer lock, and recovery semantics when changing execution.
- Never fetch the operator's keys or trade to test a change without explicit authorization.
- Do not commit credentials, runtime state or account snapshots.
- Keep trade records and account-specific reports outside this repository.
- Keep HYPE-specific assumptions visible. BTC is not supported by replacing a config ticker.
- Preserve the seven responsibilities in the README without adding services or frameworks.
- Validation: cargo fmt --check; cargo test --locked --all-features;
  cargo test --locked --no-default-features. New financial calculations need realistic vectors.
- Keep the runtime a single process with local files. Do not require a database service,
  systemd credentials or fixed operating-system directories.
- Use relevant local tests for code changes.
