use crate::kalshi::{BookEvent, Credentials, parse_message};
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Every socket owns one Kalshi orderbook subscription. A fresh subscription
/// emits fresh initial snapshots; never attempt to carry old books across reconnects.
pub async fn run_shard(
    shard: usize,
    mut target: watch::Receiver<Vec<String>>,
    mut reset: mpsc::Receiver<()>,
    mut shutdown: watch::Receiver<bool>,
    output: mpsc::Sender<(usize, BookEvent)>,
    credentials: Credentials,
    ws_url: String,
) {
    let mut backoff = 1u64;
    loop {
        if *shutdown.borrow() {
            break;
        }
        let current = target.borrow().clone();
        if current.is_empty() {
            tokio::select! {
                _ = target.changed() => {},
                _ = shutdown.changed() => {},
            }
            continue;
        }
        // Drop any existing book state before connecting/reconnecting.
        if output
            .send((shard, BookEvent::Invalidate(current.clone())))
            .await
            .is_err()
        {
            break;
        }
        let session = run_session(
            shard,
            &current,
            &mut target,
            &mut reset,
            &mut shutdown,
            &output,
            &credentials,
            &ws_url,
        )
        .await;
        // The socket no longer owns valid state, including symbols removed or
        // moved to another shard by a refresh.
        if output
            .send((shard, BookEvent::Invalidate(current.clone())))
            .await
            .is_err()
        {
            break;
        }
        if *shutdown.borrow() {
            break;
        }
        if let Err(error) = session {
            eprintln!("shard {shard} reconnecting: {error:#}");
            let delay = Duration::from_secs(backoff);
            backoff = (backoff * 2).min(30);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {},
                _ = target.changed() => { backoff = 1; },
                _ = shutdown.changed() => {},
            }
        } else {
            backoff = 1;
        }
    }
}

async fn run_session(
    shard: usize,
    markets: &[String],
    target: &mut watch::Receiver<Vec<String>>,
    reset: &mut mpsc::Receiver<()>,
    shutdown: &mut watch::Receiver<bool>,
    output: &mpsc::Sender<(usize, BookEvent)>,
    credentials: &Credentials,
    ws_url: &str,
) -> Result<()> {
    let request = credentials.websocket_request(ws_url)?;
    let (mut socket, _) = connect_async(request)
        .await
        .context("Kalshi websocket handshake")?;
    eprintln!("shard {shard} connected ({} markets)", markets.len());
    let subscription = json!({
        "id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":markets}
    });
    socket
        .send(Message::Text(subscription.to_string().into()))
        .await?;
    let mut last_seq: Option<i64> = None;
    let mut sid: Option<i64> = None;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            changed = target.changed() => {
                if changed.is_err() || *target.borrow() != markets { break; }
            }
            _ = reset.recv() => { bail!("orderbook failed validation; resubscribing"); }
            incoming = socket.next() => {
                let Some(incoming) = incoming else { bail!("WebSocket ended"); };
                match incoming? {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(text.as_ref())?;
                        let kind = value["type"].as_str().unwrap_or("");
                        if kind == "error" { bail!("Kalshi returned error: {}", value["msg"]); }
                        if kind == "subscribed" {
                            sid = value["msg"]["sid"].as_i64();
                            eprintln!("shard {shard} subscribed, sid={sid:?}");
                        }
                        // seq is scoped to a Kalshi subscription, not a market clock.
                        if let Some(seq) = value["seq"].as_i64() {
                            if sid.is_none() || value["sid"].as_i64() == sid {
                                if let Some(last) = last_seq {
                                    if seq != last + 1 { bail!("sequence gap: {last} -> {seq}"); }
                                }
                                last_seq = Some(seq);
                            }
                        }
                        if let Some(event) = parse_message(&value)? {
                            let symbol = match &event {
                                BookEvent::Snapshot { symbol, .. } | BookEvent::Delta { symbol, .. } => symbol,
                                BookEvent::Invalidate(_) => unreachable!(),
                            };
                            if markets.iter().any(|m| m == symbol) {
                                output.send((shard, event)).await.context("collector event channel closed")?;
                            }
                        }
                    }
                    Message::Ping(payload) => { socket.send(Message::Pong(payload)).await?; }
                    Message::Close(_) => { bail!("WebSocket closed by server"); }
                    _ => {}
                }
            }
        }
    }
    let _ = socket.send(Message::Close(None)).await;
    Ok(())
}
