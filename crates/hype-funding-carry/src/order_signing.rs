//! The pinned SDK's public action type preserves its MessagePack field order. Only its
//! private wire-number conversion and L1 signer are reproduced here, checked against SDK fixtures.
use std::sync::atomic::{AtomicU64, Ordering};

use alloy::{
    primitives::{keccak256, Address},
    signers::SignerSync,
    sol,
    sol_types::{eip712_domain, SolStruct},
};
use anyhow::{Context, Result};
use hyperliquid_rust_sdk::{Actions, ClientOrder, ClientOrderRequest, ExchangeClient};
use serde_json::{json, Value};

sol! {
    struct Agent {
        string source;
        bytes32 connectionId;
    }
}

fn next_nonce() -> u64 {
    static NONCE: AtomicU64 = AtomicU64::new(0);
    let now = crate::api::now_ms() as u64;
    NONCE.fetch_max(now, Ordering::SeqCst);
    NONCE.fetch_add(1, Ordering::SeqCst)
}

fn wire_number(value: f64) -> String {
    let text = format!("{value:.8}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    if text == "-0" { "0" } else { text }.to_string()
}

pub(crate) fn order_payload(exchange: &ExchangeClient, order: ClientOrderRequest) -> Result<Value> {
    let action = order_action(&exchange.coin_to_asset, order)?;
    signed_payload(exchange, &action, next_nonce())
}

fn order_action(
    assets: &std::collections::HashMap<String, u32>,
    order: ClientOrderRequest,
) -> Result<Actions> {
    let asset = assets.get(&order.asset).context("unknown order asset")?;
    let ClientOrder::Limit(limit) = order.order_type else {
        anyhow::bail!("WS execution only supports limit orders");
    };
    let mut wire = json!({"a":asset,"b":order.is_buy,"p":wire_number(order.limit_px),
        "s":wire_number(order.sz),"r":order.reduce_only,"t":{"limit":{"tif":limit.tif}}});
    if let Some(cloid) = order.cloid {
        wire["c"] = json!(format!("0x{}", cloid.simple()));
    }
    Ok(serde_json::from_value(
        json!({"type":"order", "orders":[wire], "grouping":"na"}),
    )?)
}

pub(crate) fn cancel_payload(exchange: &ExchangeClient, asset: &str, oid: u64) -> Result<Value> {
    let asset = exchange
        .coin_to_asset
        .get(asset)
        .context("unknown cancel asset")?;
    let action: Actions =
        serde_json::from_value(json!({"type":"cancel", "cancels":[{"a":asset,"o":oid}]}))?;
    signed_payload(exchange, &action, next_nonce())
}

fn signed_payload(exchange: &ExchangeClient, action: &Actions, nonce: u64) -> Result<Value> {
    let signature = sign_action(
        &exchange.wallet,
        action,
        nonce,
        exchange.vault_address,
        exchange.http_client.base_url == "https://api.hyperliquid.xyz",
    )?;
    Ok(json!({"action": action, "nonce":nonce,
        "signature":{"r":signature.r(),"s":signature.s(),"v":27 + signature.v() as u64},
        "vaultAddress":exchange.vault_address}))
}

fn sign_action(
    wallet: &alloy::signers::local::PrivateKeySigner,
    action: &Actions,
    nonce: u64,
    vault: Option<Address>,
    mainnet: bool,
) -> Result<alloy::primitives::Signature> {
    let mut bytes = rmp_serde::to_vec_named(action)?;
    bytes.extend(nonce.to_be_bytes());
    if let Some(vault) = vault {
        bytes.push(1);
        bytes.extend(vault);
    } else {
        bytes.push(0);
    }
    let agent = Agent {
        source: if mainnet { "a" } else { "b" }.into(),
        connectionId: keccak256(bytes),
    };
    let domain = eip712_domain! {name:"Exchange", version:"1", chain_id:1337, verifying_contract:Address::ZERO,};
    Ok(wallet.sign_hash_sync(&agent.eip712_signing_hash(&domain))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_action_preserves_exchange_wire_fields() -> Result<()> {
        let assets =
            std::collections::HashMap::from([("HYPE".into(), 159), ("HYPE/USDC".into(), 10107)]);
        let cloid = uuid::Uuid::parse_str("1e60610f-0b3d-4205-97c8-8c1fed2ad5ee")?;
        for (asset, index, tif, is_buy, reduce_only) in [
            ("HYPE", 159, "Ioc", false, true),
            ("HYPE/USDC", 10107, "Alo", true, false),
        ] {
            let action = order_action(
                &assets,
                ClientOrderRequest {
                    asset: asset.into(),
                    is_buy,
                    reduce_only,
                    limit_px: 52.0810000001,
                    sz: 0.00076000001,
                    cloid: Some(cloid),
                    order_type: ClientOrder::Limit(hyperliquid_rust_sdk::ClientLimit {
                        tif: tif.into(),
                    }),
                },
            )?;
            assert_eq!(
                serde_json::to_value(action)?,
                json!({"type":"order","orders":[{
                "a":index,"b":is_buy,"p":"52.081","s":"0.00076","r":reduce_only,
                "t":{"limit":{"tif":tif}},"c":"0x1e60610f0b3d420597c88c1fed2ad5ee"}],"grouping":"na"})
            );
        }
        Ok(())
    }

    #[test]
    fn pinned_sdk_signatures_match() -> Result<()> {
        // Public test key and expected signatures from pinned upstream exchange_client.rs.
        let wallet = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e".parse()?;
        for (action, mainnet, testnet) in [
            (json!({"type":"order","orders":[{"a":1,"b":true,"p":"2000.0","s":"3.5","r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}),
             "0x77957e58e70f43b6b68581f2dc42011fc384538a2e5b7bf42d5b936f19fbb67360721a8598727230f67080efee48c812a6a4442013fd3b0eed509171bef9f23f1c",
             "0xcd0925372ff1ed499e54883e9a6205ecfadec748f80ec463fe2f84f1209648776377961965cb7b12414186b1ea291e95fd512722427efcbcfb3b0b2bcd4d79d01c"),
            (json!({"type":"order","orders":[{"a":1,"b":true,"p":"2000.0","s":"3.5","r":false,"t":{"limit":{"tif":"Ioc"}},"c":"0x1e60610f0b3d420597c88c1fed2ad5ee"}],"grouping":"na"}),
             "0xd3e894092eb27098077145714630a77bbe3836120ee29df7d935d8510b03a08f456de5ec1be82aa65fc6ecda9ef928b0445e212517a98858cfaa251c4cd7552b1c",
             "0x3768349dbb22a7fd770fc9fc50c7b5124a7da342ea579b309f58002ceae49b4357badc7909770919c45d850aabb08474ff2b7b3204ae5b66d9f7375582981f111c"),
            (json!({"type":"cancel","cancels":[{"a":1,"o":82382}]}),
             "0x02f76cc5b16e0810152fa0e14e7b219f49c361e3325f771544c6f54e157bf9fa17ed0afc11a98596be85d5cd9f86600aad515337318f7ab346e5ccc1b03425d51b",
             "0x6ffebadfd48067663390962539fbde76cfa36f53be65abe2ab72c9db6d0db44457720db9d7c4860f142a484f070c84eb4b9694c3a617c83f0d698a27e55fd5e01c"),
        ] {
            let action = serde_json::from_value(action)?;
            assert_eq!(sign_action(&wallet, &action, 1583838, None, true)?.to_string(), mainnet);
            assert_eq!(sign_action(&wallet, &action, 1583838, None, false)?.to_string(), testnet);
        }
        Ok(())
    }

    #[test]
    fn wire_format_and_nonce() {
        assert_eq!(wire_number(-0.0), "0");
        assert_eq!(wire_number(0.000760), "0.00076");
        assert_eq!(wire_number(52.081), "52.081");
        assert!(next_nonce() < next_nonce());
    }
}
