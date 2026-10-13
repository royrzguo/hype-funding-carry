use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

// Local diagnostics. External alert delivery is the operator's integration.
static ALERTS: OnceLock<Mutex<HashMap<String, (Instant, bool)>>> = OnceLock::new();

pub fn send(message: &str) {
    eprintln!("{message}");
}

pub fn send_dedup(key: &str, message: &str) {
    eprintln!("event={key} {message}");
}

pub fn alert(key: &str, message: &str) {
    alert_after(key, message, 0);
}

pub fn alert_after(key: &str, message: &str, grace_s: u64) {
    let mut alerts = ALERTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap();
    let (since, emitted) = alerts
        .entry(key.to_string())
        .or_insert((Instant::now(), false));
    if !*emitted && since.elapsed() >= Duration::from_secs(grace_s) {
        eprintln!("alert={key} {message}");
        *emitted = true;
    }
}

pub fn resolve(key: &str, message: &str) {
    if let Some((_, true)) = ALERTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap()
        .remove(key)
    {
        eprintln!("resolved={key} {message}");
    }
}
