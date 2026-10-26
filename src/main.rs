mod collector;
mod kalshi;
mod orderbook;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use engineering_challenge::util::AlignedSbeWriter;
use engineering_challenge::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use kalshi::{BookEvent, Credentials, DEFAULT_REST, DEFAULT_WS};
use orderbook::OrderBook;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
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
    epoch: Arc<AtomicU64>,
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

/// Plan subscription changes and invalidate books whose socket membership changes.
/// A retained market still needs a fresh snapshot when its shard resubscribes.
fn reconcile_market_refresh(
    names: &[String],
    per_socket: usize,
    previous_chunks: &[Vec<String>],
    states: &mut HashMap<String, State>,
    owners: &mut HashMap<String, usize>,
) -> Option<Vec<Vec<String>>> {
    let mut sorted = names.to_vec();
    sorted.sort();
    sorted.dedup();
    let chunks = shard_sets(&sorted, per_socket);
    // update_workers keeps empty workers at the end when market counts shrink.
    let active_previous: Vec<Vec<String>> = previous_chunks
        .iter()
        .filter(|chunk| !chunk.is_empty())
        .cloned()
        .collect();
    if chunks == active_previous {
        return None;
    }

    let mut next_owners = HashMap::new();
    for (shard, symbols) in chunks.iter().enumerate() {
        for symbol in symbols {
            next_owners.insert(symbol.clone(), shard);
        }
    }

    // If the contents of a shard changed, its socket will resubscribe.
    // Invalidate every old book on that shard, including markets that
    // stay on the same shard, so no stale state can be snapshotted.
    for (shard, previous) in active_previous.iter().enumerate() {
        if chunks.get(shard) != Some(previous) {
            for symbol in previous {
                states.remove(symbol);
            }
        }
    }
    // Never retain a book for a market that is no longer active.
    states.retain(|symbol, _| next_owners.contains_key(symbol));
    *owners = next_owners;
    Some(chunks)
}

fn update_workers(
    chunks: Vec<Vec<String>>,
    workers: &mut Vec<Worker>,
    event_tx: &mpsc::Sender<(usize, u64, BookEvent)>,
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
            let epoch = Arc::new(AtomicU64::new(0));
            let join = tokio::spawn(collector::run_shard(
                i,
                receiver,
                reset_rx,
                shutdown.clone(),
                event_tx.clone(),
                auth.clone(),
                ws_url.to_owned(),
                epoch.clone(),
            ));
            workers.push(Worker {
                target,
                reset,
                join,
                epoch,
            });
        } else if *workers[i].target.borrow() != *chunk {
            // Immediately reject queued data from the old subscription.
            workers[i].epoch.fetch_add(1, Ordering::SeqCst);
            workers[i].target.send_replace(chunk.clone());
        }
    }
    for worker in workers.iter().skip(chunks.len()) {
        if !worker.target.borrow().is_empty() {
            worker.epoch.fetch_add(1, Ordering::SeqCst);
            worker.target.send_replace(Vec::new());
        }
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

/// A shard index alone is insufficient: a restarted connection may produce
/// delayed queued messages from the previous subscription on that same shard.
fn filter_event_by_session(
    shard: usize,
    session_epoch: u64,
    event: BookEvent,
    owners: &HashMap<String, usize>,
    workers: &[Worker],
) -> Option<BookEvent> {
    let current_epoch = workers.get(shard)?.epoch.load(Ordering::SeqCst);
    if session_epoch != current_epoch {
        return None;
    }
    filter_event_by_owner(shard, event, owners)
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
    let (event_tx, mut event_rx) = mpsc::channel::<(usize, u64, BookEvent)>(16_384);
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
                        if desired != current {
                            eprintln!("discovered {} open regular markets", names.len());
                            let previous_chunks: Vec<Vec<String>> = workers
                                .iter()
                                .map(|worker| worker.target.borrow().clone())
                                .collect();
                            if let Some(chunks) = reconcile_market_refresh(
                                &names,
                                per_socket,
                                &previous_chunks,
                                &mut engine.states,
                                &mut owners,
                            ) {
                                update_workers(
                                    chunks,
                                    &mut workers,
                                    &event_tx,
                                    &stop_rx,
                                    &auth,
                                    &ws_url,
                                );
                            }
                            current = desired;
                        }
                    }
                    Err(error) => eprintln!("market refresh failed; preserving subscriptions: {error:#}"),
                }
            }
            maybe_event = event_rx.recv() => {
                if let Some((shard, session_epoch, event)) = maybe_event {
                    if let Some(event) = filter_event_by_session(shard, session_epoch, event, &owners, &workers) {
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
    for worker in &mut workers {
        let _ = tokio::time::timeout(Duration::from_secs(3), &mut worker.join).await;
    }
    drop(event_tx);
    while let Ok((shard, session_epoch, event)) = event_rx.try_recv() {
        if let Some(event) = filter_event_by_session(shard, session_epoch, event, &owners, &workers)
        {
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

#[cfg(test)]
mod dynamic_market_regression {
    use super::*;

    fn state() -> State {
        State {
            book: OrderBook::from_snapshot(&[("0.40".to_string(), "1.00".to_string())], &[])
                .unwrap(),
            exchange: Utc::now(),
        }
    }

    #[test]
    fn adding_market_preserves_unchanged_shards() {
        let old = vec![vec!["A".to_string(), "B".to_string()]];
        let mut states = HashMap::from([("A".to_string(), state()), ("B".to_string(), state())]);
        let mut owners = HashMap::from([("A".to_string(), 0usize), ("B".to_string(), 0usize)]);
        let markets = ["A".to_string(), "B".to_string(), "C".to_string()];
        let next = reconcile_market_refresh(&markets, 2, &old, &mut states, &mut owners)
            .expect("added market should change subscription plan");
        assert_eq!(
            next,
            vec![
                vec!["A".to_string(), "B".to_string()],
                vec!["C".to_string()],
            ]
        );
        assert!(states.contains_key("A"));
        assert!(states.contains_key("B"));
        assert_eq!(owners.get("C"), Some(&1));
    }

    #[test]
    fn removing_market_invalidates_even_same_shard_survivors() {
        let old = vec![
            vec!["A".to_string(), "B".to_string()],
            vec!["C".to_string()],
        ];
        let mut states = HashMap::from([
            ("A".to_string(), state()),
            ("B".to_string(), state()),
            ("C".to_string(), state()),
        ]);
        let mut owners = HashMap::from([
            ("A".to_string(), 0usize),
            ("B".to_string(), 0usize),
            ("C".to_string(), 1usize),
        ]);
        // B remains on shard 0 but shard 0 must resubscribe after A goes away.
        let markets = ["B".to_string(), "C".to_string()];
        let next = reconcile_market_refresh(&markets, 2, &old, &mut states, &mut owners)
            .expect("removal should change subscription plan");
        assert_eq!(next, vec![vec!["B".to_string(), "C".to_string()]]);
        assert!(
            states.is_empty(),
            "all books on changing shards must be invalidated"
        );
        assert!(!owners.contains_key("A"));
        assert_eq!(owners.get("B"), Some(&0));
        assert_eq!(owners.get("C"), Some(&0));
    }

    #[test]
    fn unchanged_membership_does_not_clear_books() {
        let old = vec![vec!["A".to_string()], vec![]];
        let mut states = HashMap::from([("A".to_string(), state())]);
        let mut owners = HashMap::from([("A".to_string(), 0usize)]);
        let markets = ["A".to_string()];
        let next = reconcile_market_refresh(&markets, 2, &old, &mut states, &mut owners);
        assert!(next.is_none());
        assert!(states.contains_key("A"));
        assert_eq!(owners.get("A"), Some(&0));
    }
}

#[cfg(test)]
mod session_generation_regression {
    use super::*;

    fn workers_for_test(epoch: Arc<AtomicU64>) -> Vec<Worker> {
        let (target, _receiver) = watch::channel(vec!["A".to_string()]);
        let (reset, _reset_receiver) = mpsc::channel(1);
        let join = tokio::spawn(async {});
        vec![Worker {
            target,
            reset,
            join,
            epoch,
        }]
    }

    #[tokio::test]
    async fn old_connection_cannot_invalidate_reconnected_book() {
        let epoch = Arc::new(AtomicU64::new(1));
        let workers = workers_for_test(epoch.clone());
        let owners = HashMap::from([("A".to_owned(), 0usize)]);

        let previous = BookEvent::Invalidate(vec!["A".into()]);
        assert!(filter_event_by_session(0, 1, previous, &owners, &workers).is_some());

        // A new connection on the SAME shard replaces the prior subscription.
        epoch.store(2, Ordering::SeqCst);
        let stale = BookEvent::Invalidate(vec!["A".into()]);
        assert!(filter_event_by_session(0, 1, stale, &owners, &workers).is_none());
        let current = BookEvent::Invalidate(vec!["A".into()]);
        assert!(filter_event_by_session(0, 2, current, &owners, &workers).is_some());
    }

    #[tokio::test]
    async fn old_connection_snapshot_and_delta_are_rejected() {
        let epoch = Arc::new(AtomicU64::new(7));
        let workers = workers_for_test(epoch);
        let owners = HashMap::from([("A".to_owned(), 0usize)]);
        let now = Utc::now();
        let old_snapshot = BookEvent::Snapshot {
            symbol: "A".into(),
            yes: vec![],
            no: vec![],
            received: now,
            exchange: now,
        };
        assert!(filter_event_by_session(0, 6, old_snapshot, &owners, &workers).is_none());
        let old_delta = BookEvent::Delta {
            symbol: "A".into(),
            outcome: crate::orderbook::Outcome::Yes,
            price: 400_000,
            delta: 1_000_000,
            received: now,
            exchange: now,
        };
        assert!(filter_event_by_session(0, 6, old_delta, &owners, &workers).is_none());
        let new_snapshot = BookEvent::Snapshot {
            symbol: "A".into(),
            yes: vec![],
            no: vec![],
            received: now,
            exchange: now,
        };
        assert!(filter_event_by_session(0, 7, new_snapshot, &owners, &workers).is_some());
    }
}
