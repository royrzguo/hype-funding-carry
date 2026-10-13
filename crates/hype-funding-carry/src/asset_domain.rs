use std::collections::HashSet;

use serde::Serialize;
use serde_json::Value;

const ZERO_EPSILON: f64 = 1e-12;
const USDC_TOKEN: u64 = 0;
const HYPE_TOKEN: u64 = 150;

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct DomainFinding {
    pub location: String,
    pub asset: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct AssetDomainScan {
    pub scanned_perp_dexes: Vec<String>,
    pub open_orders_total: u32,
    pub unexpected_positions: Vec<DomainFinding>,
    pub unexpected_orders: Vec<DomainFinding>,
    pub unexpected_spot_assets: Vec<DomainFinding>,
    pub unexpected_borrow_lend_assets: Vec<DomainFinding>,
    pub delegation_or_staking_nonzero: bool,
    pub incomplete_reads: Vec<String>,
    pub domain_clean: bool,
    pub scan_started_at_ms: i64,
    pub observed_at_ms: i64,
}

impl AssetDomainScan {
    pub fn has_unknown_exposure(&self) -> bool {
        !self.domain_clean
    }

    pub fn allows(&self, action: DomainAction) -> bool {
        match action {
            DomainAction::Cancel | DomainAction::Reduce | DomainAction::Recover => true,
            DomainAction::Hedge {
                uniquely_identified,
            } => {
                uniquely_identified
                    && self.incomplete_reads.is_empty()
                    && self.unexpected_positions.is_empty()
                    && self.unexpected_orders.is_empty()
                    && self.unexpected_spot_assets.is_empty()
                    && self.unexpected_borrow_lend_assets.is_empty()
                    && !self.delegation_or_staking_nonzero
            }
            DomainAction::IncreaseRisk
            | DomainAction::FlowConfirm
            | DomainAction::NavSync
            | DomainAction::Settle => self.domain_clean,
        }
    }

    /// Not clean only because a read failed: no unexpected position, order, asset or stake.
    pub fn only_incomplete_reads(&self) -> bool {
        !self.incomplete_reads.is_empty()
            && self.unexpected_positions.is_empty()
            && self.unexpected_orders.is_empty()
            && self.unexpected_spot_assets.is_empty()
            && self.unexpected_borrow_lend_assets.is_empty()
            && !self.delegation_or_staking_nonzero
    }

    pub fn introduced_violation_since(&self, before: &Self) -> bool {
        !self.incomplete_reads.is_empty()
            || self
                .unexpected_positions
                .iter()
                .any(|finding| !before.unexpected_positions.contains(finding))
            || self
                .unexpected_orders
                .iter()
                .any(|finding| !before.unexpected_orders.contains(finding))
            || self
                .unexpected_spot_assets
                .iter()
                .any(|finding| !before.unexpected_spot_assets.contains(finding))
            || self
                .unexpected_borrow_lend_assets
                .iter()
                .any(|finding| !before.unexpected_borrow_lend_assets.contains(finding))
            || (self.delegation_or_staking_nonzero && !before.delegation_or_staking_nonzero)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DomainAction {
    IncreaseRisk,
    FlowConfirm,
    NavSync,
    Settle,
    Cancel,
    Reduce,
    Recover,
    Hedge { uniquely_identified: bool },
}

#[derive(Clone, Debug)]
pub struct DexDomainInput {
    pub name: String,
    pub clearinghouse_state: Result<Value, String>,
    pub open_orders: Result<Value, String>,
}

#[derive(Clone, Debug)]
pub struct AssetDomainInput {
    pub perp_dexes: Result<Value, String>,
    pub dex_states: Vec<DexDomainInput>,
    pub spot_state: Result<Value, String>,
    pub borrow_lend_state: Result<Value, String>,
    pub delegator_summary: Result<Value, String>,
    pub delegations: Result<Value, String>,
    pub scan_started_at_ms: i64,
    pub observed_at_ms: i64,
}

pub fn parse_perp_dexes(value: &Value) -> Result<Vec<String>, String> {
    let rows = value
        .as_array()
        .ok_or_else(|| "perpDexs response is not an array".to_string())?;
    if rows.is_empty() {
        return Err("perpDexs response is empty".to_string());
    }
    let mut names = Vec::with_capacity(rows.len());
    let mut seen = HashSet::new();
    for row in rows {
        let name = if row.is_null() {
            String::new()
        } else {
            row.get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| "perpDexs entry is missing a non-empty name".to_string())?
                .to_string()
        };
        if !seen.insert(name.clone()) {
            return Err(format!(
                "perpDexs contains duplicate dex {}",
                display_dex(&name)
            ));
        }
        names.push(name);
    }
    if names.iter().filter(|name| name.is_empty()).count() != 1 {
        return Err("perpDexs must contain exactly one main dex".to_string());
    }
    Ok(names)
}

pub fn evaluate_asset_domain(input: AssetDomainInput) -> AssetDomainScan {
    let mut scan = AssetDomainScan {
        scanned_perp_dexes: Vec::new(),
        open_orders_total: 0,
        unexpected_positions: Vec::new(),
        unexpected_orders: Vec::new(),
        unexpected_spot_assets: Vec::new(),
        unexpected_borrow_lend_assets: Vec::new(),
        delegation_or_staking_nonzero: false,
        incomplete_reads: Vec::new(),
        domain_clean: false,
        scan_started_at_ms: input.scan_started_at_ms,
        observed_at_ms: input.observed_at_ms,
    };

    let expected_dexes = match input.perp_dexes {
        Ok(value) => match parse_perp_dexes(&value) {
            Ok(names) => names,
            Err(error) => {
                scan.incomplete_reads.push(error);
                Vec::new()
            }
        },
        Err(error) => {
            scan.incomplete_reads.push(format!("perpDexs: {error}"));
            Vec::new()
        }
    };
    scan.scanned_perp_dexes = expected_dexes
        .iter()
        .map(|name| display_dex(name))
        .collect();
    let expected: HashSet<_> = expected_dexes.iter().cloned().collect();
    let actual: HashSet<_> = input
        .dex_states
        .iter()
        .map(|state| state.name.clone())
        .collect();
    for name in expected.difference(&actual) {
        scan.incomplete_reads
            .push(format!("missing dex scan {}", display_dex(name)));
    }
    for name in actual.difference(&expected) {
        scan.incomplete_reads
            .push(format!("unexpected dex scan {}", display_dex(name)));
    }
    if actual.len() != input.dex_states.len() {
        scan.incomplete_reads
            .push("duplicate dex scan result".to_string());
    }

    for dex in input.dex_states {
        scan_perp_state(&dex.name, dex.clearinghouse_state, &mut scan);
        scan_orders(&dex.name, dex.open_orders, &mut scan);
    }
    scan_spot(input.spot_state, &mut scan);
    scan_borrow_lend(input.borrow_lend_state, &mut scan);
    scan_staking(input.delegator_summary, input.delegations, &mut scan);

    scan.domain_clean = scan.incomplete_reads.is_empty()
        && scan.unexpected_positions.is_empty()
        && scan.unexpected_orders.is_empty()
        && scan.unexpected_spot_assets.is_empty()
        && scan.unexpected_borrow_lend_assets.is_empty()
        && !scan.delegation_or_staking_nonzero;
    scan
}

fn scan_perp_state(dex: &str, state: Result<Value, String>, scan: &mut AssetDomainScan) {
    let value = match state {
        Ok(value) => value,
        Err(error) => {
            scan.incomplete_reads
                .push(format!("clearinghouseState[{}]: {error}", display_dex(dex)));
            return;
        }
    };
    let Some(positions) = value.get("assetPositions").and_then(Value::as_array) else {
        scan.incomplete_reads.push(format!(
            "clearinghouseState[{}] missing assetPositions",
            display_dex(dex)
        ));
        return;
    };
    for (index, row) in positions.iter().enumerate() {
        let Some(position) = row.get("position").and_then(Value::as_object) else {
            scan.incomplete_reads.push(format!(
                "clearinghouseState[{}].assetPositions[{index}] malformed",
                display_dex(dex)
            ));
            continue;
        };
        let Some(coin) = position.get("coin").and_then(Value::as_str) else {
            scan.incomplete_reads.push(format!(
                "clearinghouseState[{}].assetPositions[{index}] missing coin",
                display_dex(dex)
            ));
            continue;
        };
        let size = match numeric_field(position.get("szi")) {
            Ok(value) => value,
            Err(error) => {
                scan.incomplete_reads.push(format!(
                    "clearinghouseState[{}].assetPositions[{index}].szi: {error}",
                    display_dex(dex)
                ));
                continue;
            }
        };
        let allowed_main_hype = dex.is_empty() && coin == "HYPE" && size <= ZERO_EPSILON;
        if size.abs() > ZERO_EPSILON && !allowed_main_hype {
            scan.unexpected_positions.push(DomainFinding {
                location: format!("perp:{}", display_dex(dex)),
                asset: coin.to_string(),
                detail: format!("szi={size}"),
            });
        }
    }
}

fn scan_orders(dex: &str, orders: Result<Value, String>, scan: &mut AssetDomainScan) {
    let value = match orders {
        Ok(value) => value,
        Err(error) => {
            scan.incomplete_reads
                .push(format!("openOrders[{}]: {error}", display_dex(dex)));
            return;
        }
    };
    let Some(orders) = value.as_array() else {
        scan.incomplete_reads
            .push(format!("openOrders[{}] is not an array", display_dex(dex)));
        return;
    };
    for (index, order) in orders.iter().enumerate() {
        scan.open_orders_total = scan.open_orders_total.saturating_add(1);
        let Some(coin) = order.get("coin").and_then(Value::as_str) else {
            scan.incomplete_reads.push(format!(
                "openOrders[{}][{index}] missing coin",
                display_dex(dex)
            ));
            continue;
        };
        let oid = match order.get("oid").and_then(integer_value) {
            Some(oid) => oid,
            None => {
                scan.incomplete_reads.push(format!(
                    "openOrders[{}][{index}] missing oid",
                    display_dex(dex)
                ));
                continue;
            }
        };
        if !dex.is_empty() || !matches!(coin, "HYPE" | "@107") {
            scan.unexpected_orders.push(DomainFinding {
                location: format!("orders:{}", display_dex(dex)),
                asset: coin.to_string(),
                detail: format!("oid={oid}"),
            });
        }
    }
}

fn scan_spot(state: Result<Value, String>, scan: &mut AssetDomainScan) {
    let value = match state {
        Ok(value) => value,
        Err(error) => {
            scan.incomplete_reads
                .push(format!("spotClearinghouseState: {error}"));
            return;
        }
    };
    let Some(balances) = value.get("balances").and_then(Value::as_array) else {
        scan.incomplete_reads
            .push("spotClearinghouseState missing balances".to_string());
        return;
    };
    let mut seen = HashSet::new();
    for (index, row) in balances.iter().enumerate() {
        let Some(object) = row.as_object() else {
            scan.incomplete_reads
                .push(format!("spot balance[{index}] malformed"));
            continue;
        };
        let Some(token) = object.get("token").and_then(integer_value) else {
            scan.incomplete_reads
                .push(format!("spot balance[{index}] missing numeric token"));
            continue;
        };
        if !seen.insert(token) {
            scan.incomplete_reads
                .push(format!("duplicate spot token {token}"));
            continue;
        }
        let allowed = matches!(token, USDC_TOKEN | HYPE_TOKEN);
        for field in ["total", "hold", "entryNtl"] {
            let number = match numeric_field(object.get(field)) {
                Ok(value) => value,
                Err(error) => {
                    scan.incomplete_reads
                        .push(format!("spot token {token}.{field}: {error}"));
                    continue;
                }
            };
            let material = if field == "hold" {
                number > ZERO_EPSILON
            } else {
                number.abs() > ZERO_EPSILON
            };
            if !allowed && material {
                scan.unexpected_spot_assets.push(DomainFinding {
                    location: "spot".to_string(),
                    asset: format!("token:{token}"),
                    detail: format!("{field}={number}"),
                });
            }
        }
        for field in ["spotHold", "supplied", "borrowed"] {
            let Some(value) = object.get(field) else {
                continue;
            };
            let number = match numeric_field(Some(value)) {
                Ok(value) => value,
                Err(error) => {
                    scan.incomplete_reads
                        .push(format!("spot token {token}.{field}: {error}"));
                    continue;
                }
            };
            if !allowed && number.abs() > ZERO_EPSILON {
                scan.unexpected_spot_assets.push(DomainFinding {
                    location: "spot".to_string(),
                    asset: format!("token:{token}"),
                    detail: format!("{field}={number}"),
                });
            }
        }
        if let Some(value) = object.get("ltv") {
            if let Err(error) = numeric_field(Some(value)) {
                scan.incomplete_reads
                    .push(format!("spot token {token}.ltv: {error}"));
            }
        }
        for field in object.keys() {
            if !matches!(
                field.as_str(),
                "coin"
                    | "token"
                    | "total"
                    | "hold"
                    | "entryNtl"
                    | "spotHold"
                    | "supplied"
                    | "borrowed"
                    | "ltv"
            ) {
                scan.incomplete_reads
                    .push(format!("spot token {token} has unknown field {field}"));
            }
        }
    }
    let Some(available_rows) = value
        .get("tokenToAvailableAfterMaintenance")
        .and_then(Value::as_array)
    else {
        scan.incomplete_reads
            .push("spotClearinghouseState missing tokenToAvailableAfterMaintenance".to_string());
        return;
    };
    let mut available_tokens = HashSet::new();
    for (index, row) in available_rows.iter().enumerate() {
        let Some(pair) = row.as_array() else {
            scan.incomplete_reads.push(format!(
                "available-after-maintenance row[{index}] is not a pair"
            ));
            continue;
        };
        let Some(token) = pair.first().and_then(integer_value) else {
            scan.incomplete_reads.push(format!(
                "available-after-maintenance row[{index}] missing token"
            ));
            continue;
        };
        if !available_tokens.insert(token) {
            scan.incomplete_reads.push(format!(
                "duplicate available-after-maintenance token {token}"
            ));
        }
        if let Err(error) = numeric_field(pair.get(1)) {
            scan.incomplete_reads.push(format!(
                "available-after-maintenance token {token}: {error}"
            ));
        }
    }
}

fn scan_borrow_lend(state: Result<Value, String>, scan: &mut AssetDomainScan) {
    let value = match state {
        Ok(value) => value,
        Err(error) => {
            scan.incomplete_reads
                .push(format!("borrowLendUserState: {error}"));
            return;
        }
    };
    let Some(rows) = value.get("tokenToState").and_then(Value::as_array) else {
        scan.incomplete_reads
            .push("borrowLendUserState missing tokenToState".to_string());
        return;
    };
    let mut seen = HashSet::new();
    for (index, row) in rows.iter().enumerate() {
        let Some(pair) = row.as_array() else {
            scan.incomplete_reads
                .push(format!("borrow/lend row[{index}] is not a pair"));
            continue;
        };
        let Some(token) = pair.first().and_then(integer_value) else {
            scan.incomplete_reads
                .push(format!("borrow/lend row[{index}] missing token"));
            continue;
        };
        let Some(token_state) = pair.get(1) else {
            scan.incomplete_reads
                .push(format!("borrow/lend token {token} missing state"));
            continue;
        };
        if !seen.insert(token) {
            scan.incomplete_reads
                .push(format!("duplicate borrow/lend token {token}"));
            continue;
        }
        let mut fields = Vec::new();
        if let Err(error) = collect_numeric_leaves(token_state, "", &mut fields) {
            scan.incomplete_reads
                .push(format!("borrow/lend token {token}: {error}"));
            continue;
        }
        if !matches!(token, USDC_TOKEN | HYPE_TOKEN) {
            for (field, value) in fields {
                if value.abs() > ZERO_EPSILON {
                    scan.unexpected_borrow_lend_assets.push(DomainFinding {
                        location: "borrow_lend".to_string(),
                        asset: format!("token:{token}"),
                        detail: format!("{field}={value}"),
                    });
                }
            }
        }
    }
}

fn scan_staking(
    summary: Result<Value, String>,
    delegations: Result<Value, String>,
    scan: &mut AssetDomainScan,
) {
    match summary {
        Ok(value) => {
            let Some(object) = value.as_object() else {
                scan.incomplete_reads
                    .push("delegatorSummary is not an object".to_string());
                return;
            };
            for field in [
                "delegated",
                "undelegated",
                "totalPendingWithdrawal",
                "nPendingWithdrawals",
            ] {
                match numeric_field(object.get(field)) {
                    Ok(value) if value.abs() > ZERO_EPSILON => {
                        scan.delegation_or_staking_nonzero = true;
                    }
                    Ok(_) => {}
                    Err(error) => scan
                        .incomplete_reads
                        .push(format!("delegatorSummary.{field}: {error}")),
                }
            }
        }
        Err(error) => scan
            .incomplete_reads
            .push(format!("delegatorSummary: {error}")),
    }
    match delegations {
        Ok(value) => {
            let Some(rows) = value.as_array() else {
                scan.incomplete_reads
                    .push("delegations response is not an array".to_string());
                return;
            };
            for (index, row) in rows.iter().enumerate() {
                match numeric_field(row.get("amount")) {
                    Ok(value) if value.abs() > ZERO_EPSILON => {
                        scan.delegation_or_staking_nonzero = true;
                    }
                    Ok(_) => {}
                    Err(error) => scan
                        .incomplete_reads
                        .push(format!("delegations[{index}].amount: {error}")),
                }
            }
        }
        Err(error) => scan.incomplete_reads.push(format!("delegations: {error}")),
    }
}

fn collect_numeric_leaves(
    value: &Value,
    path: &str,
    output: &mut Vec<(String, f64)>,
) -> Result<(), String> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                collect_numeric_leaves(value, &path, output)?;
            }
            Ok(())
        }
        Value::Array(_) | Value::Bool(_) | Value::Null => {
            Err(format!("{path} is not a numeric leaf"))
        }
        Value::Number(_) | Value::String(_) => {
            output.push((path.to_string(), numeric_field(Some(value))?));
            Ok(())
        }
    }
}

fn numeric_field(value: Option<&Value>) -> Result<f64, String> {
    value
        .and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .filter(|value: &f64| value.is_finite())
        .ok_or_else(|| "missing or invalid number".to_string())
}

fn integer_value(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn display_dex(name: &str) -> String {
    if name.is_empty() {
        "main".to_string()
    } else {
        name.to_string()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn clean_input() -> AssetDomainInput {
        AssetDomainInput {
            perp_dexes: Ok(json!([null, {"name":"xyz"}])),
            dex_states: vec![
                DexDomainInput {
                    name: String::new(),
                    clearinghouse_state: Ok(json!({"assetPositions":[
                        {"position":{"coin":"HYPE","szi":"-2"}}
                    ]})),
                    open_orders: Ok(json!([
                        {"coin":"HYPE","oid":1},
                        {"coin":"@107","oid":2}
                    ])),
                },
                DexDomainInput {
                    name: "xyz".to_string(),
                    clearinghouse_state: Ok(json!({"assetPositions":[]})),
                    open_orders: Ok(json!([])),
                },
            ],
            spot_state: Ok(json!({"balances":[
                {"coin":"USDC","token":0,"total":"100","hold":"0","entryNtl":"0"},
                {"coin":"HYPE","token":150,"total":"2","hold":"0","entryNtl":"100"},
                {"coin":"USDE","token":166,"total":"0","hold":"-1.5","entryNtl":"0","ltv":"0.5"}
            ],"tokenToAvailableAfterMaintenance":[[0,"100"],[150,"2"],[166,"0"]]})),
            borrow_lend_state: Ok(json!({"tokenToState":[
                [0,{"borrow":{"basis":"0","value":"0"},"supply":{"basis":"1","value":"1"}}],
                [150,{"borrow":{"basis":"0","value":"0"},"supply":{"basis":"0","value":"0"}}]
            ]})),
            delegator_summary: Ok(json!({
                "delegated":"0","undelegated":"0","totalPendingWithdrawal":"0",
                "nPendingWithdrawals":0
            })),
            delegations: Ok(json!([])),
            scan_started_at_ms: 1,
            observed_at_ms: 2,
        }
    }

    #[test]
    fn clean_hfc_domain_passes_and_zero_third_token_is_record_only() {
        let result = evaluate_asset_domain(clean_input());
        assert!(result.domain_clean, "{result:?}");
        assert_eq!(result.scanned_perp_dexes, ["main", "xyz"]);
    }

    #[test]
    fn builder_position_and_order_fail() {
        let mut input = clean_input();
        input.dex_states[1].clearinghouse_state = Ok(json!({"assetPositions":[
            {"position":{"coin":"xyz:XYZ100","szi":"1"}}
        ]}));
        input.dex_states[1].open_orders = Ok(json!([{"coin":"xyz:XYZ100","oid":3}]));
        let result = evaluate_asset_domain(input);
        assert!(!result.domain_clean);
        assert_eq!(result.unexpected_positions.len(), 1);
        assert_eq!(result.unexpected_orders.len(), 1);
    }

    #[test]
    fn third_spot_and_unapproved_pair_fail_when_nonzero() {
        let mut input = clean_input();
        input.spot_state = Ok(json!({"balances":[
            {"coin":"PURR","token":1,"total":"0.1","hold":"0","entryNtl":"1"}
        ],"tokenToAvailableAfterMaintenance":[[1,"0.1"]]}));
        input.dex_states[0]
            .open_orders
            .clone_from(&Ok(json!([{"coin":"@1","oid":4}])));
        let result = evaluate_asset_domain(input);
        assert!(!result.domain_clean);
        assert!(!result.unexpected_spot_assets.is_empty());
        assert_eq!(result.unexpected_orders.len(), 1);
    }

    #[test]
    fn third_borrow_lend_token_fails_when_nonzero() {
        let mut input = clean_input();
        input.borrow_lend_state = Ok(json!({"tokenToState":[
            [166,{"borrow":{"basis":"0.2","value":"3"},"supply":{"basis":"0","value":"0"}}]
        ]}));
        let result = evaluate_asset_domain(input);
        assert!(!result.domain_clean);
        assert_eq!(result.unexpected_borrow_lend_assets.len(), 2);
    }

    #[test]
    fn nonzero_staking_fails() {
        let mut input = clean_input();
        input.delegator_summary = Ok(json!({
            "delegated":"1","undelegated":"0","totalPendingWithdrawal":"0",
            "nPendingWithdrawals":0
        }));
        input.delegations = Ok(json!([{"amount":"1"}]));
        let result = evaluate_asset_domain(input);
        assert!(!result.domain_clean);
        assert!(result.delegation_or_staking_nonzero);
    }

    #[test]
    fn partial_malformed_or_missing_dex_scan_fails_closed() {
        let mut input = clean_input();
        input.dex_states.pop();
        input.borrow_lend_state = Err("timeout".to_string());
        input.delegations = Ok(json!({}));
        let result = evaluate_asset_domain(input);
        assert!(!result.domain_clean);
        assert!(result.incomplete_reads.len() >= 3, "{result:?}");
    }

    #[test]
    fn newly_listed_empty_dex_is_scanned_and_passes() {
        let mut input = clean_input();
        input.perp_dexes = Ok(json!([null, {"name":"xyz"}, {"name":"newdex"}]));
        input.dex_states.push(DexDomainInput {
            name: "newdex".to_string(),
            clearinghouse_state: Ok(json!({"assetPositions":[]})),
            open_orders: Ok(json!([])),
        });
        let result = evaluate_asset_domain(input);
        assert!(result.domain_clean, "{result:?}");
        assert_eq!(result.scanned_perp_dexes, ["main", "xyz", "newdex"]);
    }

    #[test]
    fn duplicate_or_empty_dex_list_fails_closed() {
        assert!(parse_perp_dexes(&json!([])).is_err());
        assert!(parse_perp_dexes(&json!([null, null])).is_err());
        assert!(parse_perp_dexes(&json!([null, {"name":"xyz"}, {"name":"xyz"}])).is_err());
    }

    #[test]
    fn dirty_domain_blocks_risk_and_settlement_but_preserves_recovery_actions() {
        let mut result = evaluate_asset_domain(clean_input());
        result.unexpected_positions.push(DomainFinding {
            location: "perp:xyz".to_string(),
            asset: "xyz:XYZ100".to_string(),
            detail: "szi=1".to_string(),
        });
        result.domain_clean = false;
        assert!(!result.allows(DomainAction::IncreaseRisk));
        assert!(!result.allows(DomainAction::FlowConfirm));
        assert!(!result.allows(DomainAction::NavSync));
        assert!(!result.allows(DomainAction::Settle));
        assert!(result.allows(DomainAction::Cancel));
        assert!(result.allows(DomainAction::Reduce));
        assert!(result.allows(DomainAction::Recover));
        assert!(!result.allows(DomainAction::Hedge {
            uniquely_identified: true
        }));
    }

    #[test]
    fn hedge_requires_complete_known_domain_and_unique_missing_leg() {
        let mut result = evaluate_asset_domain(clean_input());
        assert!(result.allows(DomainAction::Hedge {
            uniquely_identified: true
        }));
        assert!(!result.allows(DomainAction::Hedge {
            uniquely_identified: false
        }));
        result.delegation_or_staking_nonzero = true;
        result.domain_clean = false;
        assert!(!result.allows(DomainAction::Hedge {
            uniquely_identified: true
        }));
        result.incomplete_reads.push("spot timeout".to_string());
        assert!(!result.allows(DomainAction::Hedge {
            uniquely_identified: true
        }));
    }

    #[test]
    fn recovery_may_leave_existing_violation_but_must_not_create_another() {
        let mut before = evaluate_asset_domain(clean_input());
        before.unexpected_positions.push(DomainFinding {
            location: "perp:xyz".to_string(),
            asset: "xyz:XYZ100".to_string(),
            detail: "szi=1".to_string(),
        });
        before.domain_clean = false;
        assert!(!before.introduced_violation_since(&before));
        let mut after = before.clone();
        after.unexpected_orders.push(DomainFinding {
            location: "orders:xyz".to_string(),
            asset: "xyz:XYZ100".to_string(),
            detail: "oid=9".to_string(),
        });
        assert!(after.introduced_violation_since(&before));
    }
}
