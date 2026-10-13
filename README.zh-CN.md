# HYPE Funding Carry on Hyperliquid

[English](README.md)

在 Hyperliquid 上构建 HYPE Funding Carry 策略的 Rust 参考实现，
通过 Portfolio Margin 配对现货与永续空单，形成 delta-neutral 仓位。
由实盘代码整理，覆盖入场与退出、仓位管理、风控、恢复和核算。
策略参数与执行条件可按账户和资金规模调整。

## 架构

单进程通过 WebSocket 获取实时行情、执行建仓与对冲，REST 用于账户检查和恢复对账。
执行状态与审计日志保存在本地。

代码位于 `crates/hype-funding-carry/src/`：

| 模块 | 主要文件 | 职责 |
| --- | --- | --- |
| 行情与账户 | `api.rs`, `ws.rs` | 盘口、账户、PM、借款、挂单和成交 |
| 入场条件 | `entry_policy.rs`, `model.rs` | Basis 门槛、深度和下一期 Funding 调整 |
| 仓位管理 | `portfolio.rs`, `deployment_phase.rs` | 配对仓位、目标比例和分批调整 |
| 风控 | `risk.rs`, `risk_phase.rs`, `asset_domain.rs` | PM 风险、去杠杆、残差和资产域检查 |
| 执行 | `order_policy.rs`, `live/` | Maker 挂单、部分成交对冲、IOC、撤单和退出 |
| 恢复 | `attempt_state.rs`, `writer_lock.rs`, `state_io.rs` | 执行检查点、重启恢复和写锁 |
| 核算 | `accounting.rs`, `audit.rs`, `fill_notify.rs` | 账户流水、库存、费用和收益 |

`controller.rs`、`controller_runtime.rs` 与 `controller_service.rs` 组合并调度上述模块。
恢复与风控优先于新增仓位。现货 Maker 按实际成交净数量对冲永续，
挂单按 Basis、盘口和超时条件管理；Funding、借息和费用进入监控与核算。

## 构建与配置

需要 Rust 1.91+。Linux 另需 `pkg-config` 和 OpenSSL 开发包
（Debian/Ubuntu：`pkg-config libssl-dev`）。依赖由 `Cargo.lock` 和固定 SDK commit 锁定。

```bash
cargo build --release --locked
cp .env.example .env
```

在 `.env` 中设置账户地址、API agent 地址、`HFC_PRIVATE_KEY_FILE` 和 `HFC_STATE_DIR`。
密钥从本地文件读取，Unix 文件权限须为仅本人可读。账户需启用 Portfolio Margin 并授权 API agent。

策略参数见 [config/strategy.example.json](config/strategy.example.json)：

- `target_deployment_ratio`、`deployment_band`：配对仓位名义金额／净权益及调整区间。
- `entry_create_bps`、`entry_keep_bps`：创建与保留入场挂单的门槛。
- `entry_policy`：下一期负 Funding 对 Basis 门槛的调整。
- `risk`：PMR 触发值与去杠杆目标。
- 执行金额、盘口利用率、残差和超时参数：订单与风险边界。

当前适配 HYPE/USDC 现货与 HYPE 永续。更换资产需同步调整交易对、资产编号、精度和资产域检查。
程序管理账户内该资产的全部余额和仓位，应使用独立策略账户。
同一账户的本程序实例须共用状态目录；写锁仅协调这些实例，不与其他交易程序自动互斥。

## 运行

只读检查不加载签名密钥或提交订单：

```bash
./target/release/hype-funding-carry status
./target/release/hype-funding-carry plan --config config/strategy.example.json
./target/release/hype-funding-carry feed-check --seconds 5
```

交易命令要求 `--execute` 与匹配的 `--confirm-account`。
在 `.env` 中设置 `HFC_PRODUCTION_GO_LIVE_AUTHORIZATION=HYPE_FUNDING_CARRY_LIVE` 后启动控制器：

```bash
./target/release/hype-funding-carry serve \
  --config config/strategy.example.json \
  --execute \
  --confirm-account "0xYourHyperCoreAccountAddress" \
  --max-notional 50
```

`--confirm-account` 须与配置账户一致。`--max-notional` 限制普通仓位调整的单次金额；
累计仓位由 Deployment band 控制，风险去杠杆、残差和恢复使用各自的风控边界。

| 命令 | 用途 |
| --- | --- |
| `serve` | 连续运行控制器 |
| `open` / `rebalance-once` | 单次入场／仓位调整，同样要求上述授权令牌 |
| `open-once` | 等待配置门槛，管理一笔 $10–50 建仓 |
| `close` | `--notional` 指定退出金额；省略时以全部配对仓位为目标，`--wait` 持续退出 |
| `recover` | 只读检查恢复状态 |
| `repair` | 清理账户内全部 HYPE 现货与永续仓位，包括配对仓位 |
| `cancel-all` | 撤销账户内全部挂单 |
| `signer-check` | 加载密钥校验签名身份，不提交订单 |

各命令参数见 `--help`。`close --wait` 不与 `--notional` 同用；普通单次退出可部分完成后结束。
退出和清理命令不要求新增风险授权令牌。控制器收到 Ctrl-C 或 SIGTERM 后尝试撤单、
核对残差后停止；配对仓位通过 `close` 退出。

## 本地状态

`HFC_STATE_DIR` 默认是 `state/`，保存执行检查点、核算基线、进程锁和 `audit/events.jsonl`。
重启时保留该目录；订单与仓位以交易所状态为准。
日志追加写入，运行期间按成交、订单和账户游标增量处理；启动时重放日志建立索引。

核算以首次本地观察基线为起点，净值变化扣除净入出金；已有仓位按基线市价计量。
Funding 和费用来自交易所记录，借息按债务与 APR 估算。删除状态会重置观察期。
快照不同步、历史不完整或库存对账失败时报告核算错误，已知仓位对冲继续执行。

## 开发

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --no-default-features
```

## Agent skill

[`skills/hype-funding-carry`](skills/hype-funding-carry/SKILL.md) 引导 Claude Code 和 Codex
完成配置、只读检查和运行操作；除非你明确要求，不会下单。

- Claude Code：`mkdir -p ~/.claude/skills && cp -r skills/hype-funding-carry ~/.claude/skills/`
- Codex：让 `$skill-installer` 安装
  `https://github.com/royrzguo/hype-funding-carry/tree/main/skills/hype-funding-carry`

## 免责声明

本项目仅作为开源参考代码提供，不构成投资建议，也不是托管交易服务。
程序会使用你的账户和资金真实下单；交易所规则、行情、流动性和程序缺陷都可能造成损失。
使用前请自行审阅代码、调整参数并小额测试，风险与后果由使用者自行承担。

## License

[MIT](LICENSE) · barker.money
