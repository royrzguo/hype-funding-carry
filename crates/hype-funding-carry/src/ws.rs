use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::api::now_ms;
use crate::model::Book;

const WS_URL: &str = "wss://api.hyperliquid.xyz/ws";

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Touch {
    pub px: f64,
    pub sz: f64,
}

/// Latest provable depth for one leg; ordering and depth freshness are distinct.
#[derive(Clone, Debug, PartialEq)]
pub struct MarketBook {
    pub book: Book,
    latest_event_ts_ms: i64,
}

impl MarketBook {
    pub fn new(book: Book) -> Self {
        Self {
            latest_event_ts_ms: book.exchange_ts_ms,
            book,
        }
    }

    pub fn update(&mut self, book: Book, is_bbo: bool) {
        if book.exchange_ts_ms < self.latest_event_ts_ms {
            return;
        }
        self.latest_event_ts_ms = book.exchange_ts_ms;
        if is_bbo
            && self.book.bid_px.first() == book.bid_px.first()
            && self.book.bid_sz.first() == book.bid_sz.first()
            && self.book.ask_px.first() == book.ask_px.first()
            && self.book.ask_sz.first() == book.ask_sz.first()
        {
            // An unchanged top does not refresh the observation time of deeper levels.
            return;
        }
        // A changed BBO proves only one level. Never attach old depth to a new top.
        self.book = book;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FeedEvent {
    Connected,
    Disconnected(String),
    Book {
        coin: String,
        book: Book,
    },
    Bbo {
        coin: String,
        exchange_ts_ms: i64,
        recv_ts_ms: i64,
        bid: Option<Touch>,
        ask: Option<Touch>,
    },
    UserFill {
        oid: u64,
        tid: u64,
        coin: String,
        px: f64,
        qty: f64,
        fee: f64,
        fee_token: String,
        exchange_ts_ms: i64,
        recv_ts_ms: i64,
    },
    OrderUpdate {
        oid: u64,
        status: String,
        status_ts_ms: i64,
        recv_ts_ms: i64,
    },
    SpotTrade {
        tid: u64,
        side: String,
        px: f64,
        qty: f64,
        recv_ts_ms: i64,
    },
}

fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|x| x.parse().ok()))
        .unwrap_or(0.0)
}

fn touch(value: Option<&Value>) -> Option<Touch> {
    let value = value?.as_object()?;
    Some(Touch {
        px: number(value.get("px")?),
        sz: number(value.get("sz")?),
    })
}

pub fn bbo_book(
    exchange_ts_ms: i64,
    recv_ts_ms: i64,
    bid: Option<Touch>,
    ask: Option<Touch>,
) -> anyhow::Result<Book> {
    let (Some(bid), Some(ask)) = (bid, ask) else {
        anyhow::bail!("BBO missing market side");
    };
    if !bid.px.is_finite()
        || !ask.px.is_finite()
        || !bid.sz.is_finite()
        || !ask.sz.is_finite()
        || bid.px <= 0.0
        || ask.px <= bid.px
        || bid.sz < 0.0
        || ask.sz < 0.0
    {
        anyhow::bail!("BBO invalid market side");
    }
    Ok(Book {
        exchange_ts_ms,
        recv_ts_ms,
        bid_px: vec![bid.px],
        bid_sz: vec![bid.sz],
        ask_px: vec![ask.px],
        ask_sz: vec![ask.sz],
    })
}

fn parse_events(value: &Value, recv_ts_ms: i64) -> anyhow::Result<Vec<FeedEvent>> {
    Ok(match value["channel"].as_str().unwrap_or("") {
        "l2Book" => vec![FeedEvent::Book {
            coin: value["data"]["coin"].as_str().unwrap_or("").to_string(),
            book: crate::api::parse_book(&value["data"], recv_ts_ms)?,
        }],
        "bbo" => {
            let data = &value["data"];
            let sides = data["bbo"].as_array();
            vec![FeedEvent::Bbo {
                coin: data["coin"].as_str().unwrap_or("").to_string(),
                exchange_ts_ms: number(&data["time"]) as i64,
                recv_ts_ms,
                bid: sides.and_then(|x| touch(x.first())),
                ask: sides.and_then(|x| touch(x.get(1))),
            }]
        }
        "userFills" => value["data"]["fills"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|fill| FeedEvent::UserFill {
                oid: number(&fill["oid"]) as u64,
                tid: number(&fill["tid"]) as u64,
                coin: fill["coin"].as_str().unwrap_or("").to_string(),
                px: number(&fill["px"]),
                qty: number(&fill["sz"]),
                fee: number(&fill["fee"]),
                fee_token: fill["feeToken"].as_str().unwrap_or("").to_string(),
                exchange_ts_ms: number(&fill["time"]) as i64,
                recv_ts_ms,
            })
            .collect(),
        "orderUpdates" => value["data"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|update| FeedEvent::OrderUpdate {
                oid: number(&update["order"]["oid"]) as u64,
                status: update["status"].as_str().unwrap_or("").to_string(),
                status_ts_ms: number(&update["statusTimestamp"]) as i64,
                recv_ts_ms,
            })
            .collect(),
        "trades" => value["data"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|trade| FeedEvent::SpotTrade {
                tid: number(&trade["tid"]) as u64,
                side: trade["side"].as_str().unwrap_or("").to_string(),
                px: number(&trade["px"]),
                qty: number(&trade["sz"]),
                recv_ts_ms,
            })
            .collect(),
        _ => Vec::new(),
    })
}

async fn run_connection(account: &str, sender: &UnboundedSender<FeedEvent>) -> anyhow::Result<()> {
    let (stream, _) = connect_async(WS_URL).await?;
    let (mut writer, mut reader) = stream.split();
    for subscription in [
        json!({"type":"userFills","user":account}),
        json!({"type":"orderUpdates","user":account}),
        json!({"type":"trades","coin":"@107"}),
        json!({"type":"bbo","coin":"@107"}),
        json!({"type":"bbo","coin":"HYPE"}),
        json!({"type":"l2Book","coin":"@107","fast":true}),
        json!({"type":"l2Book","coin":"HYPE","fast":true}),
    ] {
        writer
            .send(Message::Text(
                json!({"method":"subscribe","subscription":subscription}).to_string(),
            ))
            .await?;
    }
    let _ = sender.send(FeedEvent::Connected);
    let mut ping = tokio::time::interval(Duration::from_secs(40));
    ping.tick().await;
    loop {
        tokio::select! {
            _ = ping.tick() => {
                writer.send(Message::Text(json!({"method":"ping"}).to_string())).await?;
            }
            message = reader.next() => {
                let Some(message) = message else {
                    anyhow::bail!("websocket ended");
                };
                match message? {
                    Message::Text(text) => {
                        if let Ok(value) = serde_json::from_str::<Value>(&text) {
                            for event in parse_events(&value, now_ms())? {
                                if sender.send(event).is_err() {
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Message::Ping(payload) => writer.send(Message::Pong(payload)).await?,
                    Message::Close(_) => anyhow::bail!("websocket closed"),
                    _ => {}
                }
            }
        }
    }
}

pub fn spawn_feed(account: String) -> UnboundedReceiver<FeedEvent> {
    let (sender, receiver) = unbounded_channel();
    tokio::spawn(async move {
        let mut backoff_s = 1;
        loop {
            match run_connection(&account, &sender).await {
                Ok(()) => return,
                Err(error) => {
                    if sender
                        .send(FeedEvent::Disconnected(error.to_string()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(backoff_s)).await;
            backoff_s = (backoff_s * 2).min(10);
        }
    });
    receiver
}

#[cfg(test)]
mod tests {
    use super::*;

    fn depth(ts: i64) -> Book {
        Book {
            exchange_ts_ms: ts,
            recv_ts_ms: ts,
            bid_px: vec![58.0, 57.99],
            bid_sz: vec![10.0, 20.0],
            ask_px: vec![58.01, 58.02],
            ask_sz: vec![10.0, 20.0],
        }
    }

    #[test]
    fn newer_bbo_drops_old_depth_and_old_snapshots_cannot_restore_it() {
        let mut market = MarketBook::new(depth(100));
        let top = bbo_book(
            130,
            130,
            Some(Touch {
                px: 58.005,
                sz: 2.0,
            }),
            Some(Touch {
                px: 58.015,
                sz: 3.0,
            }),
        )
        .unwrap();
        market.update(top.clone(), true);
        market.update(depth(120), false);
        assert_eq!(market.book, top);
        let mut fresh = depth(140);
        fresh.bid_px[0] = 58.005;
        market.update(fresh.clone(), false);
        assert_eq!(market.book, fresh);
    }

    #[test]
    fn same_top_does_not_refresh_depth_or_allow_older_depth_to_replace_it() {
        let mut market = MarketBook::new(depth(100));
        let top = bbo_book(
            130,
            130,
            Some(Touch { px: 58.0, sz: 10.0 }),
            Some(Touch {
                px: 58.01,
                sz: 10.0,
            }),
        )
        .unwrap();
        market.update(top, true);
        let mut old = depth(120);
        old.bid_px[0] = 57.0;
        market.update(old, false);
        assert_eq!(market.book, depth(100));
    }

    #[test]
    fn same_block_depth_and_bbo_keep_the_same_depth_in_either_order() {
        let top = bbo_book(
            100,
            101,
            Some(Touch { px: 58.0, sz: 10.0 }),
            Some(Touch {
                px: 58.01,
                sz: 10.0,
            }),
        )
        .unwrap();
        let mut depth_first = MarketBook::new(depth(100));
        depth_first.update(top.clone(), true);
        let mut bbo_first = MarketBook::new(top);
        bbo_first.update(depth(100), false);
        assert_eq!(depth_first.book, bbo_first.book);
    }

    #[test]
    fn parses_five_level_snapshot_and_rejects_invalid_depth() {
        let mut levels = json!({"channel":"l2Book","data":{
            "coin":"HYPE","time":100,"fast":true,
            "levels":[
                (0..5).map(|i| json!({"px":58.0 - i as f64 * 0.01,"sz":10.0,"n":1})).collect::<Vec<_>>(),
                (0..5).map(|i| json!({"px":58.01 + i as f64 * 0.01,"sz":10.0,"n":1})).collect::<Vec<_>>()
            ]
        }});
        let rows = parse_events(&levels, 110).unwrap();
        let FeedEvent::Book { coin, book } = &rows[0] else {
            panic!("missing depth")
        };
        assert_eq!(coin, "HYPE");
        assert_eq!(book.bid_px.len(), 5);
        assert_eq!(book.ask_px.len(), 5);
        assert_eq!((book.exchange_ts_ms, book.recv_ts_ms), (100, 110));
        levels["data"]["levels"][0][0]["sz"] = json!(-1);
        assert!(parse_events(&levels, 110).is_err());
        assert!(bbo_book(
            100,
            110,
            Some(Touch {
                px: 58.0,
                sz: f64::NAN
            }),
            Some(Touch {
                px: 58.01,
                sz: 10.0
            })
        )
        .is_err());
    }

    #[test]
    fn parses_bbo_without_strong_sdk_types() {
        let rows = parse_events(
            &json!({
                "channel":"bbo",
                "data":{"coin":"HYPE","time":123,"bbo":[
                    {"px":"58.1","sz":"20","n":2},
                    {"px":"58.2","sz":"30","n":3}
                ]}
            }),
            456,
        )
        .unwrap();
        assert_eq!(
            rows[0],
            FeedEvent::Bbo {
                coin: "HYPE".to_string(),
                exchange_ts_ms: 123,
                recv_ts_ms: 456,
                bid: Some(Touch { px: 58.1, sz: 20.0 }),
                ask: Some(Touch { px: 58.2, sz: 30.0 }),
            }
        );
    }

    #[test]
    fn parses_fill_and_order_update_by_tid_and_oid() {
        let fills = parse_events(
            &json!({"channel":"userFills","data":{"fills":[{
                "oid":7,"tid":8,"coin":"@107","px":"58","sz":"1.2",
                "fee":"0.0004","feeToken":"HYPE","time":9
            }]}}),
            10,
        )
        .unwrap();
        assert!(matches!(
            fills[0],
            FeedEvent::UserFill {
                oid: 7,
                tid: 8,
                exchange_ts_ms: 9,
                recv_ts_ms: 10,
                ..
            }
        ));
        let orders = parse_events(
            &json!({"channel":"orderUpdates","data":[{
                "order":{"oid":7},"status":"filled","statusTimestamp":9
            }]}),
            10,
        )
        .unwrap();
        assert!(matches!(orders[0], FeedEvent::OrderUpdate { oid: 7, .. }));
    }
}
