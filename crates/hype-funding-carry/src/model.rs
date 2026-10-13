use anyhow::{bail, Result};
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Action {
    Entry,
    Exit,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Entry => "entry",
            Self::Exit => "exit",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Book {
    pub exchange_ts_ms: i64,
    pub recv_ts_ms: i64,
    pub bid_px: Vec<f64>,
    pub bid_sz: Vec<f64>,
    pub ask_px: Vec<f64>,
    pub ask_sz: Vec<f64>,
}

impl Book {
    pub fn best_bid(&self) -> Result<f64> {
        self.bid_px
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing bid"))
    }

    pub fn best_ask(&self) -> Result<f64> {
        self.ask_px
            .first()
            .copied()
            .ok_or_else(|| anyhow::anyhow!("missing ask"))
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Decision {
    pub decision_id: String,
    pub action: String,
    pub decision_ts_ms: i64,
    pub notional_usd: f64,
    pub target_hype_qty: f64,
    pub gate_bps: f64,
    pub executable_basis_bps: f64,
    pub eligible: bool,
    pub blocked: bool,
    pub reason: String,
    pub spot_bid_px: f64,
    pub spot_ask_px: f64,
    pub perp_bid_px: f64,
    pub perp_ask_px: f64,
}

pub fn round_hype_qty(notional_usd: f64, reference_px: f64) -> f64 {
    ((notional_usd / reference_px) * 100.0).floor() / 100.0
}

pub fn make_cloid(decision_id: &str, venue: &str, sequence: u32) -> String {
    let key = format!("{decision_id}:{venue}:{sequence}");
    format!(
        "0x{}",
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).simple()
    )
}

pub fn walk(book: &Book, side: &str, qty: f64) -> Result<f64> {
    let (prices, sizes) = match side {
        "buy" => (&book.ask_px, &book.ask_sz),
        "sell" => (&book.bid_px, &book.bid_sz),
        _ => bail!("invalid side {side}"),
    };
    let mut remaining = qty;
    let mut notional = 0.0;
    for (px, available) in prices.iter().zip(sizes) {
        let take = remaining.min(*available);
        notional += take * px;
        remaining -= take;
        if remaining <= 1e-9 {
            return Ok(notional / qty);
        }
    }
    bail!("insufficient {side} depth for {qty} HYPE")
}

/// Maximum displayed quantity whose hedge VWAP stays inside the basis limit.
/// Sell hedges require basis >= limit; buy hedges require basis <= limit.
pub fn basis_limited_capacity_qty(
    book: &Book,
    side: &str,
    spot_limit_px: f64,
    basis_limit_bps: f64,
) -> Result<f64> {
    if spot_limit_px <= 0.0 || !basis_limit_bps.is_finite() {
        bail!("invalid basis-capacity input");
    }
    let sizes = match side {
        "sell" => &book.bid_sz,
        "buy" => &book.ask_sz,
        _ => bail!("invalid side {side}"),
    };
    let total: f64 = sizes.iter().sum();
    if total <= 0.0 {
        return Ok(0.0);
    }
    let eligible = |qty: f64| -> Result<bool> {
        let basis = (walk(book, side, qty)? / spot_limit_px - 1.0) * 10_000.0;
        Ok(match side {
            "sell" => basis + 1e-10 >= basis_limit_bps,
            "buy" => basis <= basis_limit_bps + 1e-10,
            _ => unreachable!(),
        })
    };
    let probe = total.min(0.01);
    if !eligible(probe)? {
        return Ok(0.0);
    }
    if eligible(total)? {
        return Ok(total);
    }
    let (mut low, mut high) = (probe, total);
    for _ in 0..48 {
        let mid = (low + high) / 2.0;
        if eligible(mid)? {
            low = mid;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

pub fn decide(
    action: Action,
    ts_ms: i64,
    notional_usd: f64,
    spot: &Book,
    perp: &Book,
    blocked: bool,
    block_reason: &str,
) -> Result<Decision> {
    let spot_bid = spot.best_bid()?;
    let spot_ask = spot.best_ask()?;
    let perp_bid = perp.best_bid()?;
    let perp_ask = perp.best_ask()?;
    let reference = (spot_bid + spot_ask) / 2.0;
    let qty = round_hype_qty(notional_usd, reference);
    if qty <= 0.0 {
        bail!("target quantity rounds to zero");
    }

    let (basis, gate, basis_ok) = match action {
        Action::Entry => {
            let perp_sell_vwap = walk(perp, "sell", qty)?;
            let basis = (perp_sell_vwap / spot_bid - 1.0) * 10_000.0;
            (basis, 2.0, basis + 1e-10 >= 2.0)
        }
        Action::Exit => {
            let perp_buy_vwap = walk(perp, "buy", qty)?;
            let basis = (perp_buy_vwap / spot_ask - 1.0) * 10_000.0;
            (basis, 0.0, basis <= 1e-10)
        }
    };
    let reason = if blocked {
        block_reason.to_string()
    } else if !basis_ok {
        "basis_gate".to_string()
    } else {
        String::new()
    };
    let key = format!("{}:{ts_ms}:{notional_usd:.2}", action.as_str());
    let decision_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()).to_string();
    Ok(Decision {
        decision_id,
        action: action.as_str().to_string(),
        decision_ts_ms: ts_ms,
        notional_usd,
        target_hype_qty: qty,
        gate_bps: gate,
        executable_basis_bps: basis,
        eligible: basis_ok && !blocked,
        blocked,
        reason,
        spot_bid_px: spot_bid,
        spot_ask_px: spot_ask,
        perp_bid_px: perp_bid,
        perp_ask_px: perp_ask,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(bid: f64, ask: f64) -> Book {
        Book {
            exchange_ts_ms: 1,
            recv_ts_ms: 1,
            bid_px: vec![bid],
            bid_sz: vec![100.0],
            ask_px: vec![ask],
            ask_sz: vec![100.0],
        }
    }

    #[test]
    fn fixed_entry_vector_passes_at_two_bps() {
        let spot = book(100.0, 100.02);
        let perp = book(100.02, 100.03);
        let d = decide(Action::Entry, 1, 500.0, &spot, &perp, false, "").unwrap();
        assert!((d.executable_basis_bps - 2.0).abs() < 1e-9);
        assert!(d.eligible);
        assert_eq!(d.target_hype_qty, 4.99);
    }

    #[test]
    fn fixed_exit_vector_uses_spot_ask_and_perp_ask() {
        let spot = book(100.0, 100.02);
        let perp = book(99.99, 100.01);
        let d = decide(Action::Exit, 1, 500.0, &spot, &perp, false, "").unwrap();
        assert!(d.executable_basis_bps < 0.0);
        assert!(d.eligible);
    }

    #[test]
    fn risk_block_overrides_basis() {
        let spot = book(100.0, 100.02);
        let perp = book(100.03, 100.04);
        let d = decide(Action::Entry, 1, 500.0, &spot, &perp, true, "pmr_warning").unwrap();
        assert!(!d.eligible);
        assert_eq!(d.reason, "pmr_warning");
    }

    #[test]
    fn cloid_is_deterministic_128_bit_hex() {
        let a = make_cloid("decision", "spot", 1);
        let b = make_cloid("decision", "spot", 1);
        assert_eq!(a, b);
        assert_eq!(a.len(), 34);
        assert!(a.starts_with("0x"));
    }

    #[test]
    fn basis_capacity_uses_only_depth_inside_vwap_limit() {
        let book = Book {
            exchange_ts_ms: 1,
            recv_ts_ms: 1,
            bid_px: vec![100.0, 99.9, 99.0],
            bid_sz: vec![10.0, 10.0, 100.0],
            ask_px: vec![100.1, 100.2, 101.0],
            ask_sz: vec![10.0, 10.0, 100.0],
        };
        let sell = basis_limited_capacity_qty(&book, "sell", 100.0, -5.0).unwrap();
        let buy = basis_limited_capacity_qty(&book, "buy", 100.0, 15.0).unwrap();
        assert!((sell - 20.0).abs() < 1e-6);
        assert!((buy - 20.0).abs() < 1e-6);
    }

    #[test]
    fn walk_accepts_the_floating_sum_of_many_depth_levels() {
        let sizes = vec![310.0615; 20];
        let total: f64 = sizes.iter().sum();
        let book = Book {
            exchange_ts_ms: 1,
            recv_ts_ms: 1,
            bid_px: vec![100.0; 20],
            bid_sz: sizes.clone(),
            ask_px: vec![100.1; 20],
            ask_sz: sizes,
        };
        assert!((walk(&book, "sell", total).unwrap() - 100.0).abs() < 1e-9);
    }
}
