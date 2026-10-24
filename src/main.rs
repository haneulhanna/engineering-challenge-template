mod collector;
mod kalshi;
mod orderbook;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use kalshi::{BookEvent, Credentials, DEFAULT_REST, DEFAULT_WS};
use orderbook::OrderBook;
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{Instant, interval_at},
};

const WRITER_CHUNK: usize = 64 << 20;
const DEFAULT_MARKETS_PER_SOCKET: usize = 60;

struct State {
    book: OrderBook,
    exchange: DateTime<Utc>,
}

struct Worker {
    target: watch::Sender<Vec<String>>,
    reset: mpsc::Sender<()>,
    join: JoinHandle<()>,
}

struct Engine {
    states: HashMap<String, State>,
    writer: AlignedSbeWriter,
}

impl Engine {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            states: HashMap::new(),
            writer: AlignedSbeWriter::create(path, WRITER_CHUNK)?,
        })
    }
    fn process(&mut self, event: BookEvent) -> Result<()> {
        match event {
            BookEvent::Invalidate(symbols) => {
                for symbol in symbols {
                    self.states.remove(&symbol);
                }
            }
            BookEvent::Snapshot {
                symbol,
                yes,
                no,
                received,
                exchange,
            } => {
                let book = OrderBook::from_snapshot(&yes, &no)?;
                // Initial and recovery snapshots are essential for replay correctness.
                self.writer
                    .write_msg(&book.snapshot(&symbol, received, exchange))?;
                self.states.insert(symbol, State { book, exchange });
            }
            BookEvent::Delta {
                symbol,
                outcome,
                price,
                delta,
                received,
                exchange,
            } => {
                // Never apply a delta until the socket has sent a valid initial snapshot.
                let Some(state) = self.states.get_mut(&symbol) else {
                    return Ok(());
                };
                let updated = state.book.apply_delta(outcome, price, delta)?;
                state.exchange = exchange;
                self.writer.write_msg(&PriceLevelUpdateDomain {
                    backtest_timestamp: received,
                    exchange_timestamp: exchange,
                    price_level: outcome.output_price(price),
                    quantity: updated,
                    side: outcome.output_side(),
                    symbol,
                })?;
            }
        }
        Ok(())
    }
    fn minute_snapshots(&mut self) -> Result<()> {
        let receive = Utc::now();
        for (symbol, state) in &self.states {
            self.writer
                .write_msg(&state.book.snapshot(symbol, receive, state.exchange))?;
        }
        self.writer.flush()?;
        Ok(())
    }
}

fn shard_sets(all: &[String], per_socket: usize) -> Vec<Vec<String>> {
    all.chunks(per_socket).map(|part| part.to_vec()).collect()
}

fn update_workers(
    chunks: Vec<Vec<String>>,
    workers: &mut Vec<Worker>,
    event_tx: &mpsc::Sender<(usize, BookEvent)>,
    shutdown: &watch::Receiver<bool>,
    auth: &Credentials,
    ws_url: &str,
) {
    // Workers are reused for existing chunk indices; membership changes cause
    // a clean resubscribe and a new snapshot (not stale incremental state).
    for (i, chunk) in chunks.iter().enumerate() {
        if i >= workers.len() {
            let (target, receiver) = watch::channel(chunk.clone());
            let (reset, reset_rx) = mpsc::channel(4);
            let join = tokio::spawn(collector::run_shard(
                i,
                receiver,
                reset_rx,
                shutdown.clone(),
                event_tx.clone(),
                auth.clone(),
                ws_url.to_owned(),
            ));
            workers.push(Worker {
                target,
                reset,
                join,
            });
        } else if *workers[i].target.borrow() != *chunk {
            workers[i].target.send_replace(chunk.clone());
        }
    }
    for worker in workers.iter().skip(chunks.len()) {
        worker.target.send_replace(Vec::new());
    }
}

/// Reject queued events from WebSocket shards that no longer own the market.
/// Shard ownership is established by the most recent market discovery.
fn filter_event_by_owner(
    shard: usize,
    event: BookEvent,
    owners: &HashMap<String, usize>,
) -> Option<BookEvent> {
    match event {
        BookEvent::Invalidate(symbols) => {
            let owned: Vec<String> = symbols
                .into_iter()
                .filter(|symbol| owners.get(symbol).copied() == Some(shard))
                .collect();
            if owned.is_empty() {
                None
            } else {
                Some(BookEvent::Invalidate(owned))
            }
        }
        other => {
            let symbol = match &other {
                BookEvent::Snapshot { symbol, .. } | BookEvent::Delta { symbol, .. } => symbol,
                BookEvent::Invalidate(_) => unreachable!(),
            };
            if owners.get(symbol).copied() == Some(shard) {
                Some(other)
            } else {
                None
            }
        }
    }
}

fn apply_output(engine: &mut Engine, shard: usize, event: BookEvent, workers: &[Worker]) {
    let symbol = match &event {
        BookEvent::Snapshot { symbol, .. } | BookEvent::Delta { symbol, .. } => {
            Some(symbol.clone())
        }
        BookEvent::Invalidate(_) => None,
    };
    if let Err(error) = engine.process(event) {
        eprintln!("shard {shard} invalid data: {error:#}; rebuilding from fresh snapshot");
        if let Some(symbol) = symbol {
            engine.states.remove(&symbol);
        }
        if let Some(worker) = workers.get(shard) {
            let _ = worker.reset.try_send(());
        }
    }
}

async fn replay(path: &str, engine: &mut Engine) -> Result<()> {
    let text = std::fs::read_to_string(path)?;
    for (line_no, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("invalid JSON at line {}", line_no + 1))?;
        if let Some(event) = kalshi::parse_message(&value)? {
            engine.process(event)?;
        }
    }
    engine.minute_snapshots()?;
    eprintln!("Offline replay finished: {} books", engine.states.len());
    Ok(())
}

// Probe a single connection before spawning any market collection workers.
async fn verify_kalshi_auth(auth: &Credentials, ws_url: &str) -> Result<()> {
    use futures_util::SinkExt;
    eprintln!("Checking Kalshi WebSocket authentication (one connection only)...");
    let request = auth.websocket_request(ws_url)?;
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("Kalshi WebSocket authentication preflight")?;
    let _ = socket.close(None).await;
    eprintln!("Kalshi WebSocket handshake succeeded.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().collect();
    if arguments.iter().any(|argument| argument == "--auth-check") {
        let auth = Credentials::from_env()?;
        let ws_url = std::env::var("KALSHI_WS_URL").unwrap_or_else(|_| DEFAULT_WS.to_owned());
        verify_kalshi_auth(&auth, &ws_url).await?;
        return Ok(());
    }

    let is_replay = arguments.len() >= 3 && arguments[1] == "--replay";
    let output = std::env::var("ORDERBOOK_OUTPUT").unwrap_or_else(|_| {
        if is_replay {
            "orderbooks.sbe".into()
        } else {
            "/dev/stdout".into()
        }
    });
    let mut engine = Engine::new(Path::new(&output))?;
    if arguments.len() >= 3 && arguments[1] == "--replay" {
        replay(&arguments[2], &mut engine).await?;
        engine.writer.flush()?;
        return Ok(());
    }
    let auth = Credentials::from_env()?;
    let rest = std::env::var("KALSHI_REST_URL").unwrap_or_else(|_| DEFAULT_REST.to_owned());
    let ws_url = std::env::var("KALSHI_WS_URL").unwrap_or_else(|_| DEFAULT_WS.to_owned());

    verify_kalshi_auth(&auth, &ws_url).await?;
    let per_socket = std::env::var("MARKETS_PER_SOCKET")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MARKETS_PER_SOCKET);
    // For a small live test, export TEST_MAX_MARKETS=5. Unset to collect all markets.
    let test_max_markets = std::env::var("TEST_MAX_MARKETS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0);
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(25))
        .build()?;
    let (event_tx, mut event_rx) = mpsc::channel::<(usize, BookEvent)>(16_384);
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut workers: Vec<Worker> = Vec::new();
    let mut current: HashSet<String> = HashSet::new();
    let mut owners: HashMap<String, usize> = HashMap::new();

    // Initial discovery is retried in the main loop if the REST API is unavailable.
    let mut refresh = interval_at(Instant::now(), Duration::from_secs(60));
    let mut snapshot_timer = interval_at(
        Instant::now() + Duration::from_secs(60),
        Duration::from_secs(60),
    );
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => { eprintln!("Ctrl-C: stopping collector"); break; }
            _ = refresh.tick() => {
                match kalshi::live_markets(&rest, &http).await {
                    Ok(names) => {
                        let mut names = names;
                        if let Some(limit) = test_max_markets {
                            names.truncate(limit);
                            eprintln!("TEST MODE: collecting only {} markets", names.len());
                        }

                        let desired: HashSet<String> = names.iter().cloned().collect();
                        for removed in current.difference(&desired) { engine.states.remove(removed); }
                        if desired != current {
                            eprintln!("discovered {} open regular markets", names.len());
                            let chunks = shard_sets(&names, per_socket);
                            let mut next_owners = HashMap::new();
                            for (shard, symbols) in chunks.iter().enumerate() {
                                for symbol in symbols {
                                    next_owners.insert(symbol.clone(), shard);
                                }
                            }
                            // A book is invalid if its market moved to another shard.
                            for (symbol, previous_shard) in &owners {
                                if next_owners.get(symbol) != Some(previous_shard) {
                                    engine.states.remove(symbol);
                                }
                            }
                            owners = next_owners;
                            update_workers(chunks, &mut workers, &event_tx,
                                &stop_rx, &auth, &ws_url);
                            current = desired;
                        }
                    }
                    Err(error) => eprintln!("market refresh failed; preserving subscriptions: {error:#}"),
                }
            }
            maybe_event = event_rx.recv() => {
                if let Some((shard, event)) = maybe_event {
                    if let Some(event) = filter_event_by_owner(shard, event, &owners) {
                        if let BookEvent::Snapshot { symbol, .. } = &event {
                            eprintln!("DEBUG: received snapshot for {symbol}");
                        }
                        apply_output(&mut engine, shard, event, &workers);
                    }
                }
            }
            _ = snapshot_timer.tick() => engine.minute_snapshots()?,
        }
    }
    stop_tx.send_replace(true);
    // Allow each worker to close its socket before the last flush. Timeout guards
    // against a connection stuck during handshake or IO.
    for worker in workers {
        let _ = tokio::time::timeout(Duration::from_secs(3), worker.join).await;
    }
    drop(event_tx);
    while let Ok((shard, event)) = event_rx.try_recv() {
        if let Some(event) = filter_event_by_owner(shard, event, &owners) {
            apply_output(&mut engine, shard, event, &[]);
        }
    }
    engine.writer.flush()?;
    eprintln!(
        "closed {} with {} remaining active order books",
        output,
        engine.states.len()
    );
    Ok(())
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::orderbook::Outcome;

    #[test]
    fn test_recovery_after_disconnect() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let mut engine = Engine::new(file.path())?;
        let now = Utc::now();
        let symbol = "TEST-MARKET".to_string();

        // Initial connection: 20 contracts
        engine.process(BookEvent::Snapshot {
            symbol: symbol.clone(),
            yes: vec![("0.40".into(), "20.00".into())],
            no: vec![],
            received: now,
            exchange: now,
        })?;

        assert_eq!(engine.states.len(), 1);

        // Connection becomes invalid
        engine.process(BookEvent::Invalidate(vec![symbol.clone()]))?;
        assert!(engine.states.is_empty());

        // Delta without a valid snapshot must be ignored
        engine.process(BookEvent::Delta {
            symbol: symbol.clone(),
            outcome: Outcome::Yes,
            price: 400_000,
            delta: 5_000_000,
            received: now,
            exchange: now,
        })?;

        assert!(engine.states.is_empty());

        // Reconnection provides a fresh snapshot
        engine.process(BookEvent::Snapshot {
            symbol: symbol.clone(),
            yes: vec![("0.40".into(), "12.00".into())],
            no: vec![],
            received: now,
            exchange: now,
        })?;

        let state = engine.states.get(&symbol).unwrap();
        let snapshot = state.book.snapshot(&symbol, now, now);

        assert_eq!(snapshot.bids[0].quantity, 12_000_000);

        Ok(())
    }
}

#[cfg(test)]
mod shard_ownership_regression {
    use super::*;
    use crate::orderbook::Outcome;

    #[test]
    fn old_shard_cannot_erase_or_update_new_shard_book() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let mut engine = Engine::new(file.path())?;
        let now = Utc::now();
        let symbol = "TEST-MOVED-MARKET".to_string();
        let owners = HashMap::from([(symbol.clone(), 1_usize)]);

        // The new shard has installed a fresh order-book snapshot.
        let fresh = BookEvent::Snapshot {
            symbol: symbol.clone(),
            yes: vec![("0.40".into(), "12.00".into())],
            no: vec![],
            received: now,
            exchange: now,
        };
        engine.process(filter_event_by_owner(1, fresh, &owners).unwrap())?;

        // Delayed invalidate and delta messages from the previous shard are stale.
        let stale_invalidate = BookEvent::Invalidate(vec![symbol.clone()]);
        assert!(filter_event_by_owner(0, stale_invalidate, &owners).is_none());
        let stale_delta = BookEvent::Delta {
            symbol: symbol.clone(),
            outcome: Outcome::Yes,
            price: 400_000,
            delta: 5_000_000,
            received: now,
            exchange: now,
        };
        assert!(filter_event_by_owner(0, stale_delta, &owners).is_none());

        let current = engine.states.get(&symbol).unwrap();
        assert_eq!(
            current.book.snapshot(&symbol, now, now).bids[0].quantity,
            12_000_000
        );

        // The correct owner may still invalidate its own book when it disconnects.
        let valid_invalidate =
            filter_event_by_owner(1, BookEvent::Invalidate(vec![symbol.clone()]), &owners).unwrap();
        engine.process(valid_invalidate)?;
        assert!(!engine.states.contains_key(&symbol));
        Ok(())
    }

    #[test]
    fn partial_invalidation_keeps_only_markets_owned_by_sender() {
        let owners = HashMap::from([("A".to_string(), 0_usize), ("B".to_string(), 1_usize)]);
        let event = BookEvent::Invalidate(vec!["A".into(), "B".into()]);
        let Some(BookEvent::Invalidate(symbols)) = filter_event_by_owner(0, event, &owners) else {
            panic!("expected owned invalidation");
        };
        assert_eq!(symbols, vec!["A".to_string()]);
    }
}
