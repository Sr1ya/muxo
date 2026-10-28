//! Fetches a download over HTTP: streams, each one connection to the server through one network,
//! taking blocks from the scheduler and writing them into the staging file at their own offsets.
//!
//! A stream's life is a loop: ask the scheduler for work, run it as an attempt, then apply what
//! became of it. Everything that changes the download happens with the engine locked, between the
//! awaits, so a stream never sees another's half-finished change.

use crate::core::Core;
use crate::download::chunk::{download_chunk, fetch_range, ChunkHooks, ChunkRequest};
use crate::download::concurrency::{
    Action, ConcurrencyController, DiskReading, DiskSnapshot, DiskWatch, NetworkSnapshot, Snapshot, DISK_PATIENCE_MS,
    DISK_RECOVER_MS,
};
use crate::download::download_file::DownloadFile;
use crate::download::file_version::{compare_version, FileVersion, VersionCheck};
use crate::download::plan::{interleave, starting_streams, MAX_STREAMS_PER_NETWORK};
use crate::download::runtime::{Download, Transfer};
use crate::download::scheduler::{pick_work, AttemptView, Requester, SchedulerPolicy, SchedulerState, WorkKind};
use crate::download::units::{advance, recompute_aggregates, retract};
use crate::network::{FetchError, StreamConnection};
use crate::types::{
    BrowserContext, DownloadStatus, HttpStreamState, NetworkStatus, StartDownloadRequest, StreamStatus, UnitStatus,
};
use crate::util::now_ms;
use parking_lot::Mutex;
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

const MAX_CHUNK_RETRIES: u32 = 5;
const RETRY_BASE_DELAY_MS: f64 = 1000.0;
const RETRY_MAX_DELAY_MS: f64 = 15_000.0;
/// A network that can't reach the server keeps one stream asking, often enough to find out soon.
const UNREACHABLE_RETRY_MS: f64 = 5_000.0;
/// A server that answers busy is waited out this long before a stream gives up on it.
const SERVER_BUSY_FOR_MS: f64 = 5.0 * 60_000.0;
const RETRY_AFTER_MAX_MS: f64 = 120_000.0;
// A single TCP stream can get stuck at a crawl while its siblings on the same network run fine;
// a fresh connection usually lands somewhere healthy. Only relative thresholds.
const SLOW_RATIO: f64 = 0.1;
const SLOW_WARMUP_MS: f64 = 5_000.0;
const SLOW_FOR_MS: f64 = 10_000.0;
/// Nothing received this long, while the file is being served to others: dead rather than slow.
const SILENT_AFTER_MS: f64 = 5_000.0;
const MAX_REFRESHES_PER_BLOCK: u32 = 2;
const SCHEDULER_POLICY: SchedulerPolicy =
    SchedulerPolicy { hedge_after_ms: 2_000.0, max_hedges_per_block: 2, startup_ms: 1000.0 };
/// How often a stream with nothing to do looks for work again.
const IDLE_POLL_MS: u64 = 250;
/// Just longer than an idle stream takes to look for work, so a refreshed block goes to a
/// connection already proven fast, if there is one.
const REFRESH_HANDOFF_MS: u64 = IDLE_POLL_MS + 50;
const SAMPLE_BYTES: u64 = 16 * 1024;
const MAX_SAMPLES: usize = 8;
const SAMPLE_TRIES: usize = 4;

/// Doubling from RETRY_BASE_DELAY_MS, with ±20% jitter: a network that drops fails all its
/// streams at once, and they shouldn't all come back at the same instant.
fn retry_delay_ms(attempt: u32) -> f64 {
    let exact = (RETRY_BASE_DELAY_MS * 2f64.powi(attempt.saturating_sub(1).min(30) as i32)).min(RETRY_MAX_DELAY_MS);
    exact * (0.8 + 0.4 * rand::thread_rng().gen::<f64>())
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.total_cmp(b));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 { values[mid] } else { (values[mid - 1] + values[mid]) / 2.0 }
}

/// Whether the file can be fetched in parts. Otherwise one request carries all of it.
pub fn splittable(request: &StartDownloadRequest) -> bool {
    request.supports_ranges && request.total_bytes > 0
}

/// The streams per network the user picked, if they didn't leave it on Auto.
pub fn picked_streams(request: &StartDownloadRequest) -> Option<usize> {
    request.streams_per_network.map(|n| n as usize).filter(|n| (1..=MAX_STREAMS_PER_NETWORK).contains(n))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortReason {
    Refresh,
    Lost,
}

/// One request for one block, by one stream. Near the end a second (hedge) one may race the
/// primary; the block is finished by whichever gets there first.
struct Attempt {
    kind: WorkKind,
    block: usize,
    stream_id: u32,
    network_id: String,
    start_offset: Option<u64>,
    /// Bytes the file has taken: safe to resume from.
    received: u64,
    /// Bytes the network delivered, whatever the disk is doing.
    network_received: u64,
    last_network_at: f64,
    write_waiting: bool,
    started_at: f64,
    abort: CancellationToken,
    abort_reason: Option<AbortReason>,
}

struct ChunkRt {
    /// Stops the whole stream: pause, cancel, retirement. A child of the run's stop token.
    controller: CancellationToken,
    /// Cuts a backoff short without stopping the stream. Replaced once used.
    nudge: CancellationToken,
    connection: Arc<StreamConnection>,
    attempt: Option<u64>,
    warm_since: f64,
    slow_since: Option<f64>,
    last_block_speed: f64,
    received_bytes: u64,
    /// Failed attempts in a row, for backoff.
    failures: u32,
    /// Of those, the ones the server answered wrongly: MAX_CHUNK_RETRIES and it gives up.
    strikes: u32,
    busy_since: Option<f64>,
    refused: bool,
    served: bool,
    retiring: bool,
}

#[derive(Default)]
struct Traffic {
    received: u64,
    alive_at: f64,
}

pub struct HttpTransfer {
    chunks: HashMap<u32, ChunkRt>,
    workers: HashSet<u32>,
    reconciled: HashMap<String, NetworkStatus>,
    accepted: Vec<FileVersion>,
    refreshes_by_block: HashMap<usize, u32>,
    avoid: HashMap<usize, String>,
    attempts_by_block: HashMap<usize, Vec<u64>>,
    attempts: HashMap<u64, Attempt>,
    next_attempt: u64,
    hedges_by_block: HashMap<usize, u32>,
    traffic: HashMap<String, Traffic>,
    hold_until: HashMap<String, f64>,
    concurrency: Option<ConcurrencyController>,
    /// Running Auto downloads per network, kept current by the manager (see Core::shares).
    pub shares: Arc<Mutex<HashMap<String, usize>>>,
    disk: DiskWatch,
    pub file: Option<Arc<std::fs::File>>,
    pub worker_ended: Arc<Notify>,
}

macro_rules! http {
    ($dl:expr) => {
        match &mut $dl.transfer {
            Transfer::Http(transfer) => transfer,
            Transfer::Torrent(_) => unreachable!("an HTTP download"),
        }
    };
}

impl HttpTransfer {
    pub fn new(request: &StartDownloadRequest) -> Self {
        let shares: Arc<Mutex<HashMap<String, usize>>> = Arc::default();
        let concurrency = splittable(request).then(|| match picked_streams(request) {
            Some(picked) => ConcurrencyController::new(picked, false, Arc::new(|_: &str| usize::MAX)),
            None => {
                let shares = Arc::clone(&shares);
                ConcurrencyController::new(
                    MAX_STREAMS_PER_NETWORK,
                    true,
                    Arc::new(move |id: &str| {
                        MAX_STREAMS_PER_NETWORK / shares.lock().get(id).copied().unwrap_or(1).max(1)
                    }),
                )
            }
        });
        HttpTransfer {
            chunks: HashMap::new(),
            workers: HashSet::new(),
            reconciled: HashMap::new(),
            accepted: vec![FileVersion {
                etag: request.etag.clone(),
                last_modified: request.last_modified.clone(),
                total_bytes: request.total_bytes,
            }],
            refreshes_by_block: HashMap::new(),
            avoid: HashMap::new(),
            attempts_by_block: HashMap::new(),
            attempts: HashMap::new(),
            next_attempt: 0,
            hedges_by_block: HashMap::new(),
            traffic: HashMap::new(),
            hold_until: HashMap::new(),
            concurrency,
            shares,
            disk: DiskWatch::default(),
            file: None,
            worker_ended: Arc::new(Notify::new()),
        }
    }

    pub fn running(&self) -> bool {
        !self.workers.is_empty()
    }

    pub fn abort(&mut self) {
        for chunk in self.chunks.values() {
            chunk.controller.cancel();
        }
    }

    pub fn reset(&mut self) {
        self.avoid.clear();
        self.reconciled.clear();
        self.disk.reset();
    }

    fn traffic(&mut self, network_id: &str) -> &mut Traffic {
        self.traffic.entry(network_id.to_owned()).or_default()
    }
}

fn stream_index(dl: &Download, stream_id: u32) -> Option<usize> {
    dl.state.http()?.streams.iter().position(|stream| stream.id == stream_id)
}

fn stream_mut(dl: &mut Download, stream_id: u32) -> Option<&mut HttpStreamState> {
    dl.state.http_mut()?.streams.iter_mut().find(|stream| stream.id == stream_id)
}

fn network_status(dl: &Download, id: &str) -> Option<NetworkStatus> {
    dl.state.networks.iter().find(|network| network.id == id).map(|network| network.status)
}

/// A network's streams that aren't on their way out.
fn live_streams(dl: &mut Download, network_id: &str) -> Vec<u32> {
    let ids: Vec<u32> = dl
        .state
        .http()
        .map(|detail| detail.streams.iter().filter(|s| s.interface_id == network_id).map(|s| s.id).collect())
        .unwrap_or_default();
    let transfer = http!(dl);
    ids.into_iter().filter(|id| !transfer.chunks.get(id).is_some_and(|chunk| chunk.retiring)).collect()
}

/// The stream holds no block: it says so, rather than keeping the last one's numbers on show.
fn go_idle(dl: &mut Download, stream_id: u32) {
    if let Some(stream) = stream_mut(dl, stream_id) {
        stream.status = StreamStatus::Pending;
        stream.current_block_index = None;
        stream.hedge = None;
        stream.speed_bytes_per_sec = 0.0;
    }
}

impl Core {
    /// Each network's streams in line with what it can do now:
    /// - A network that has just come into use starts a set of them; from there concurrency.rs
    ///   sizes it.
    /// - One that can't reach the server keeps a single stream trying.
    /// - One that is off, offline or failed runs none.
    ///
    /// A download that can't be split runs one stream, on the first network able to carry it.
    pub(crate) fn http_reconcile(self: &Arc<Self>, dl: &mut Download) {
        let can_split = splittable(&dl.request);
        let usable: Vec<(String, NetworkStatus)> = dl
            .state
            .networks
            .iter()
            .filter(|n| matches!(n.status, NetworkStatus::On | NetworkStatus::Unreachable))
            .map(|n| (n.id.clone(), n.status))
            .collect();
        let running: Vec<(String, NetworkStatus)> =
            if can_split { usable } else { usable.into_iter().take(1).collect() };
        let mut joining = Vec::new();
        for (id, status) in &running {
            if *status != NetworkStatus::On {
                continue;
            }
            let fresh = http!(dl).reconciled.get(id) != Some(&NetworkStatus::On);
            if fresh || live_streams(dl, id).is_empty() {
                joining.push(id.clone());
            }
        }
        let waiting = dl.units.iter().filter(|unit| unit.status == UnitStatus::Pending).count();
        let starting = if can_split { starting_streams(waiting, joining.len(), picked_streams(&dl.request)) } else { 1 };

        let now = now_ms();
        let mut to_start: Vec<(u32, String)> = Vec::new();
        let networks: Vec<(String, NetworkStatus)> =
            dl.state.networks.iter().map(|n| (n.id.clone(), n.status)).collect();
        for (id, status) in networks {
            let live = live_streams(dl, &id);
            let transfer = http!(dl);
            let target = if !running.iter().any(|(running, _)| *running == id) {
                0
            } else if status == NetworkStatus::Unreachable {
                1
            } else if joining.contains(&id) {
                let limit = transfer.concurrency.as_ref().map_or(starting, |c| c.limit(&id));
                live.len().max(starting.min(limit))
            } else {
                live.len()
            };
            if joining.contains(&id) {
                transfer.traffic(&id).alive_at = now;
            }
            if live.len() > target {
                self.retire_streams(dl, &id, live.len() - target);
            }
            // Streams kept from before a pause start again.
            for stream_id in live.iter().take(target) {
                if !http!(dl).workers.contains(stream_id) {
                    to_start.push((*stream_id, id.clone()));
                }
            }
            if target > live.len() {
                for stream_id in add_streams(dl, &id, target - live.len()) {
                    to_start.push((stream_id, id.clone()));
                }
            }
            http!(dl).reconciled.insert(id, status);
        }
        // Each stream claims a block the moment it starts, so every network gets its first before
        // any gets a second.
        let ordered = interleave(to_start, |(_, network)| network.clone());
        self.start_streams(dl, ordered.into_iter().map(|(id, _)| id).collect());
    }

    pub(crate) fn http_tick(self: &Arc<Self>, dl: &mut Download, now: f64) {
        self.refresh_stuck_connections(dl, now);
        self.reconcile_download(dl);
        if dl.state.status != DownloadStatus::Downloading {
            return;
        }
        let snapshot = self.concurrency_snapshot(dl, now);
        let actions = http!(dl).concurrency.as_mut().map(|controller| controller.tick(&snapshot)).unwrap_or_default();
        for action in actions {
            match action {
                Action::Add { network_id, count } => {
                    let added = add_streams(dl, &network_id, count);
                    self.start_streams(dl, added);
                }
                Action::Retire { network_id, count } => self.retire_streams(dl, &network_id, count),
            }
        }
        for chunk in http!(dl).chunks.values_mut() {
            chunk.refused = false;
            chunk.served = false;
        }
    }

    /// Gets streams going again now, rather than when their backoff runs out: what held them back
    /// has changed. The ones `reconnect` picks also drop their sockets, and what they were
    /// fetching is taken up again on a new one: a refresh, so nothing is lost.
    pub(crate) fn http_wake(&self, dl: &mut Download, pick: &dyn Fn(&str) -> bool, reconnect: &dyn Fn(&str) -> bool) {
        let streams: Vec<(u32, String)> = dl
            .state
            .http()
            .map(|d| d.streams.iter().map(|s| (s.id, s.interface_id.clone())).collect())
            .unwrap_or_default();
        let transfer = http!(dl);
        for (id, network) in streams {
            let Some(chunk) = transfer.chunks.get_mut(&id) else { continue };
            if chunk.retiring || !pick(&network) {
                continue;
            }
            chunk.failures = 0;
            if reconnect(&network) {
                if let Some(attempt) = chunk.attempt.and_then(|a| transfer.attempts.get_mut(&a)) {
                    abort_attempt(attempt, AbortReason::Refresh);
                }
                chunk.connection.reconnect();
            }
            chunk.nudge.cancel();
        }
    }

    pub(crate) fn http_system_resumed(&self, dl: &mut Download, now: f64) {
        let transfer = http!(dl);
        for traffic in transfer.traffic.values_mut() {
            traffic.alive_at = now;
        }
        for chunk in transfer.chunks.values_mut() {
            chunk.warm_since = now;
            chunk.slow_since = None;
        }
        self.http_wake(dl, &|_| true, &|_| true);
    }

    /// Reconnects a connection that isn't carrying its weight:
    /// - Silent: nothing received for SILENT_AFTER_MS while the file is being served to others.
    /// - Crawling: under SLOW_RATIO of its network's reference speed for SLOW_FOR_MS. Networks are
    ///   never compared with each other: cellular is expected to be slower than Wi-Fi.
    fn refresh_stuck_connections(self: &Arc<Self>, dl: &mut Download, now: f64) {
        // Without ranges a reconnect restarts the whole file from byte 0.
        if !dl.request.supports_ranges {
            return;
        }
        let mut unreachable = false;
        let streams: Vec<(u32, String, f64)> = dl
            .state
            .http()
            .map(|d| d.streams.iter().map(|s| (s.id, s.interface_id.clone(), s.speed_bytes_per_sec)).collect())
            .unwrap_or_default();
        for (stream_id, network_id, _) in &streams {
            let transfer = http!(dl);
            let Some(chunk) = transfer.chunks.get(stream_id) else { continue };
            let Some(attempt_id) = chunk.attempt else { continue };
            let Some(attempt) = transfer.attempts.get(&attempt_id) else { continue };
            if attempt.abort_reason.is_some() {
                continue;
            }
            // A paused reader or final flush says nothing about the connection: the disk set
            // its pace.
            if attempt.write_waiting {
                transfer.chunks.get_mut(stream_id).unwrap().slow_since = None;
                continue;
            }
            let silent = attempt.network_received == 0
                && now - attempt.started_at >= SILENT_AFTER_MS
                && dl.units.iter().any(|unit| unit.index != attempt.block && unit.bytes_downloaded > 0);
            let block = attempt.block;
            let primary = attempt.kind == WorkKind::Primary;
            if silent && mark_unreachable(dl, network_id, now) {
                unreachable = true;
            }
            let transfer = http!(dl);
            // Silent while its network's other connections are served: the server took the
            // connection and left it waiting, which is how some turn away one too many.
            if silent {
                transfer.chunks.get_mut(stream_id).unwrap().refused = true;
            }
            let refreshes = transfer.refreshes_by_block.get(&block).copied().unwrap_or(0);
            if primary && refreshes >= MAX_REFRESHES_PER_BLOCK {
                continue;
            }
            if silent || (primary && is_crawling(transfer, &streams, *stream_id, network_id, now)) {
                if primary {
                    transfer.refreshes_by_block.insert(block, refreshes + 1);
                }
                abort_attempt(transfer.attempts.get_mut(&attempt_id).unwrap(), AbortReason::Refresh);
            }
        }
        if unreachable {
            self.reconcile_download(dl);
        }
    }

    /// Looks at whether the disk is what holds the download back, and tells the user.
    fn note_disk(self: &Arc<Self>, dl: &mut Download, now: f64, writing: usize, held: usize) {
        let limited_now = dl.state.http().and_then(|d| d.disk_limited).unwrap_or(false);
        let transfer = http!(dl);
        transfer.disk.sample(now, writing, held);
        let limited = writing > 0
            && (transfer.disk.lasted(DiskReading::Behind, now) >= DISK_PATIENCE_MS
                || (limited_now && transfer.disk.lasted(DiskReading::KeepingUp, now) < DISK_RECOVER_MS));
        if limited == limited_now {
            return;
        }
        if let Some(detail) = dl.state.http_mut() {
            detail.disk_limited = limited.then_some(true);
        }
        self.schedule_update(dl);
    }

    fn concurrency_snapshot(self: &Arc<Self>, dl: &mut Download, now: f64) -> Snapshot {
        let in_use: Vec<String> =
            dl.state.networks.iter().filter(|n| n.status == NetworkStatus::On).map(|n| n.id.clone()).collect();
        let mut writing = 0;
        let mut held = 0;
        let mut networks = Vec::new();
        for id in in_use {
            let live = live_streams(dl, &id);
            let transfer = http!(dl);
            let mut snapshot = NetworkSnapshot { id, streams: 0, answered: 0, refused: 0, served: 0 };
            for stream_id in live {
                snapshot.streams += 1;
                let Some(chunk) = transfer.chunks.get(&stream_id) else { continue };
                snapshot.answered += usize::from(chunk.received_bytes > 0);
                snapshot.refused += usize::from(chunk.refused);
                snapshot.served += usize::from(chunk.served);
                if let Some(attempt) = chunk.attempt.and_then(|a| transfer.attempts.get(&a)) {
                    writing += 1;
                    held += usize::from(attempt.write_waiting);
                }
            }
            networks.push(snapshot);
        }
        self.note_disk(dl, now, writing, held);
        let spare_work = dl.units.iter().filter(|unit| unit.status == UnitStatus::Pending).count();
        let disk = &http!(dl).disk;
        Snapshot { now, networks, spare_work, disk: Some(DiskSnapshot { reading: disk.reading, since: disk.since }) }
    }

    fn start_streams(self: &Arc<Self>, dl: &mut Download, ids: Vec<u32>) {
        if ids.is_empty() {
            return;
        }
        let download_id = dl.state.id.clone();
        for stream_id in ids {
            let Some(network_id) = stream_mut(dl, stream_id).map(|s| s.interface_id.clone()) else { continue };
            let networks = Arc::clone(&self.networks);
            let lookup_id = network_id.clone();
            let connection = Arc::new(StreamConnection::new(
                Arc::new(move || networks.find(&lookup_id)),
                self.knobs.stall_timeout,
                self.knobs.connect_timeout,
            ));
            let now = now_ms();
            let controller = dl.stop.child_token();
            let transfer = http!(dl);
            transfer.chunks.insert(
                stream_id,
                ChunkRt {
                    controller,
                    nudge: CancellationToken::new(),
                    connection,
                    attempt: None,
                    warm_since: now,
                    slow_since: None,
                    last_block_speed: 0.0,
                    received_bytes: 0,
                    failures: 0,
                    strikes: 0,
                    busy_since: None,
                    refused: false,
                    served: false,
                    retiring: false,
                },
            );
            transfer.workers.insert(stream_id);
            tokio::spawn(run_worker(Arc::clone(self), download_id.clone(), stream_id));
        }
        self.schedule_update(dl);
    }

    /// Stops `count` streams on `network_id` for good. What a stream wrote stays, and whoever
    /// takes its block next resumes from there.
    fn retire_streams(self: &Arc<Self>, dl: &mut Download, network_id: &str, count: usize) {
        if count < 1 {
            return;
        }
        let mut order = live_streams(dl, network_id);
        let transfer = http!(dl);
        // Streams the server just refused go first, then ones whose last request failed, then
        // the newest.
        let rank = |id: &u32| match transfer.chunks.get(id) {
            Some(chunk) if chunk.refused => 2,
            Some(chunk) if chunk.failures > 0 => 1,
            _ => 0,
        };
        order.sort_by(|a, b| rank(b).cmp(&rank(a)).then(b.cmp(a)));
        for stream_id in order.into_iter().take(count) {
            let transfer = http!(dl);
            if transfer.workers.contains(&stream_id) {
                if let Some(chunk) = transfer.chunks.get_mut(&stream_id) {
                    // Its worker takes it off the list once it has stopped.
                    chunk.retiring = true;
                    chunk.controller.cancel();
                }
            } else {
                self.remove_stream(dl, stream_id);
            }
        }
    }

    fn remove_stream(self: &Arc<Self>, dl: &mut Download, stream_id: u32) {
        http!(dl).chunks.remove(&stream_id);
        dl.meters.connections.remove(&stream_id);
        if let Some(index) = stream_index(dl, stream_id) {
            dl.state.http_mut().unwrap().streams.remove(index);
            self.schedule_update(dl);
        }
    }
}

/// New streams on `network_id`, put on the download's list for the caller to start.
fn add_streams(dl: &mut Download, network_id: &str, count: usize) -> Vec<u32> {
    let Some(detail) = dl.state.http_mut() else { return Vec::new() };
    let mut next = detail.streams.iter().map(|s| s.id + 1).max().unwrap_or(0);
    let mut added = Vec::new();
    for _ in 0..count {
        detail.streams.push(HttpStreamState {
            id: next,
            interface_id: network_id.to_owned(),
            range_start: 0,
            range_end: None,
            bytes_downloaded: 0,
            speed_bytes_per_sec: 0.0,
            status: StreamStatus::Pending,
            current_block_index: None,
            hedge: None,
        });
        added.push(next);
        next += 1;
    }
    detail.peak_streams = detail.peak_streams.max(detail.streams.len());
    added
}

fn abort_attempt(attempt: &mut Attempt, reason: AbortReason) {
    if attempt.abort_reason.is_some() {
        return;
    }
    attempt.abort_reason = Some(reason);
    attempt.abort.cancel();
}

/// Marks a network in use that has received nothing for SILENT_AFTER_MS as unable to reach the
/// server. Whether it did.
fn mark_unreachable(dl: &mut Download, network_id: &str, now: f64) -> bool {
    if network_status(dl, network_id) != Some(NetworkStatus::On) {
        return false;
    }
    if now - http!(dl).traffic(network_id).alive_at < SILENT_AFTER_MS {
        return false;
    }
    if let Some(network) = dl.state.networks.iter_mut().find(|n| n.id == network_id) {
        network.status = NetworkStatus::Unreachable;
    }
    true
}

fn is_crawling(transfer: &mut HttpTransfer, streams: &[(u32, String, f64)], stream_id: u32, network_id: &str, now: f64) -> bool {
    let warm = |chunk: &ChunkRt| now - chunk.warm_since >= SLOW_WARMUP_MS;
    if !transfer.chunks.get(&stream_id).is_some_and(warm) {
        return false;
    }
    let mut reference = Vec::new();
    let mut own_speed = 0.0;
    for (other, network, speed) in streams {
        if network != network_id {
            continue;
        }
        if *other == stream_id {
            own_speed = *speed;
        }
        let Some(peer) = transfer.chunks.get(other) else { continue };
        if peer.last_block_speed > 0.0 {
            reference.push(peer.last_block_speed);
        }
        if *other != stream_id && peer.attempt.is_some() && warm(peer) {
            reference.push(*speed);
        }
    }
    let chunk = transfer.chunks.get_mut(&stream_id).unwrap();
    if reference.is_empty() || own_speed >= median(&mut reference) * SLOW_RATIO {
        chunk.slow_since = None;
        return false;
    }
    let since = *chunk.slow_since.get_or_insert(now);
    now - since >= SLOW_FOR_MS
}

// --- attempts ---------------------------------------------------------------------------------

fn attempt_position(attempt: &Attempt) -> u64 {
    attempt.start_offset.unwrap_or(0) + attempt.received
}

/// How far the block's other attempts have got: what is safe to keep counted when one lets go.
fn other_attempts_position(transfer: &HttpTransfer, attempt_id: u64, block: usize) -> u64 {
    transfer
        .attempts_by_block
        .get(&block)
        .into_iter()
        .flatten()
        .filter(|id| **id != attempt_id)
        .filter_map(|id| transfer.attempts.get(id))
        .map(attempt_position)
        .max()
        .unwrap_or(0)
}

/// Takes the attempt off the books. Safe to repeat.
fn end_attempt(dl: &mut Download, stream_id: u32, attempt_id: u64) {
    let transfer = http!(dl);
    let Some(block) = transfer.attempts.get(&attempt_id).map(|a| a.block) else { return };
    if let Some(in_flight) = transfer.attempts_by_block.get_mut(&block) {
        in_flight.retain(|id| *id != attempt_id);
        if in_flight.is_empty() {
            transfer.attempts_by_block.remove(&block);
        }
    }
    transfer.attempts.remove(&attempt_id);
    if let Some(chunk) = transfer.chunks.get_mut(&stream_id) {
        if chunk.attempt == Some(attempt_id) {
            chunk.attempt = None;
        }
    }
}

/// The stream stops working on the attempt's block without having finished it. A primary hands
/// the block back to the queue; a hedge just drops out. When it delivered nothing, its network is
/// remembered, so the block goes to another one first (see scheduler.rs).
fn let_go(dl: &mut Download, stream_id: u32, attempt_id: u64, delivered_nothing: bool) {
    let Some((kind, block, network)) =
        http!(dl).attempts.get(&attempt_id).map(|a| (a.kind, a.block, a.network_id.clone()))
    else {
        return;
    };
    end_attempt(dl, stream_id, attempt_id);
    if kind == WorkKind::Primary && dl.units[block].status == UnitStatus::Downloading {
        dl.units[block].status = UnitStatus::Pending;
    }
    if delivered_nothing {
        http!(dl).avoid.insert(block, network);
    }
    go_idle(dl, stream_id);
}

/// What a worker does next, decided with the engine locked.
enum Step {
    Exit,
    /// Wait for `ms`, cut short by the stream stopping or a nudge.
    BackOff { ms: f64, wait: CancellationToken, nudge: CancellationToken },
    Idle { wait: CancellationToken },
    Fetch(Box<FetchJob>),
    /// An attempt already decided before it was sent (the file already holds the block).
    Decided { attempt: u64, outcome: Outcome },
}

struct FetchJob {
    attempt: u64,
    network_id: String,
    url: String,
    browser: Option<BrowserContext>,
    range_start: u64,
    range_end: Option<u64>,
    whole_file: bool,
    connection: Arc<StreamConnection>,
    file: Arc<std::fs::File>,
    cancel: CancellationToken,
    accepted: Vec<FileVersion>,
}

enum Outcome {
    Completed,
    /// The download was paused or cancelled underneath it.
    Stopped,
    Aborted(AbortReason),
    Failed(FetchError),
}

enum FollowUp {
    Continue,
    Stop,
    /// A refreshed primary waits a moment before asking again.
    Handoff { wait: CancellationToken },
    BackOff { ms: f64, wait: CancellationToken, nudge: CancellationToken },
    /// Whether a new label is the same bytes has to be settled by fetching some.
    Confirm(Box<ConfirmJob>),
}

struct ConfirmJob {
    error: FetchError,
    seen: FileVersion,
    samples: Vec<(u64, u64)>,
    file: DownloadFile,
    url: String,
    browser: Option<BrowserContext>,
    connection: Arc<StreamConnection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Same,
    Different,
    Unknown,
}

fn next_step(dl: &mut Download, stream_id: u32) -> Step {
    if dl.state.status != DownloadStatus::Downloading {
        return Step::Exit;
    }
    let Some(network_id) = stream_mut(dl, stream_id).map(|s| s.interface_id.clone()) else { return Step::Exit };
    let transfer = http!(dl);
    let Some(chunk) = transfer.chunks.get(&stream_id) else { return Step::Exit };
    if chunk.controller.is_cancelled() || chunk.retiring {
        return Step::Exit;
    }
    let (wait, nudge, speed) = (chunk.controller.clone(), chunk.nudge.clone(), chunk.last_block_speed);

    // The server asked this network to hold off: no new request over it until then.
    let hold_for = transfer.hold_until.get(&network_id).copied().unwrap_or(0.0) - now_ms();
    if hold_for > 0.0 {
        go_idle(dl, stream_id);
        if let Some(stream) = stream_mut(dl, stream_id) {
            stream.status = StreamStatus::Retrying;
        }
        let jitter = rand::thread_rng().gen::<f64>() * 1000f64.min(hold_for / 10.0);
        return Step::BackOff { ms: hold_for + jitter, wait, nudge };
    }

    let views: HashMap<usize, Vec<AttemptView>> = transfer
        .attempts_by_block
        .iter()
        .map(|(block, ids)| {
            let views = ids
                .iter()
                .filter_map(|id| transfer.attempts.get(id))
                .map(|a| AttemptView {
                    kind: a.kind,
                    stream_id: a.stream_id,
                    network_id: a.network_id.clone(),
                    started_at: a.started_at,
                    write_waiting: a.write_waiting,
                })
                .collect();
            (*block, views)
        })
        .collect();
    let work = {
        let streams = dl.state.http().map(|d| d.streams.as_slice()).unwrap_or_default();
        let transfer = match &dl.transfer {
            Transfer::Http(transfer) => transfer,
            Transfer::Torrent(_) => unreachable!(),
        };
        pick_work(
            &SchedulerState {
                blocks: &dl.units,
                streams,
                attempts: &views,
                avoid: &transfer.avoid,
                hedges_used: &transfer.hedges_by_block,
                disk_behind: transfer.disk.reading == DiskReading::Behind,
            },
            &Requester { id: stream_id, network_id: &network_id, speed_bytes_per_sec: (speed > 0.0).then_some(speed) },
            now_ms(),
            &SCHEDULER_POLICY,
        )
    };
    let Some(work) = work else {
        go_idle(dl, stream_id);
        // While blocks are in flight, stay available in case one is handed back, or turns out
        // slow enough to be worth racing.
        if dl.units.iter().any(|u| matches!(u.status, UnitStatus::Pending | UnitStatus::Downloading)) {
            return Step::Idle { wait };
        }
        if let Some(stream) = stream_mut(dl, stream_id) {
            stream.status = StreamStatus::Completed;
        }
        return Step::Exit;
    };

    // Begin the attempt.
    let block = work.block;
    let previous_writer = dl.units[block].interface_id.clone();
    let transfer = http!(dl);
    let attempt_id = transfer.next_attempt;
    transfer.next_attempt += 1;
    let abort = wait.child_token();
    transfer.attempts.insert(
        attempt_id,
        Attempt {
            kind: work.kind,
            block,
            stream_id,
            network_id: network_id.clone(),
            start_offset: None,
            received: 0,
            network_received: 0,
            last_network_at: 0.0,
            write_waiting: false,
            started_at: now_ms(),
            abort: abort.clone(),
            abort_reason: None,
        },
    );
    transfer.attempts_by_block.entry(block).or_default().push(attempt_id);
    if work.kind == WorkKind::Hedge {
        *transfer.hedges_by_block.entry(block).or_insert(0) += 1;
    } else {
        transfer.avoid.remove(&block);
    }
    let chunk = transfer.chunks.get_mut(&stream_id).unwrap();
    chunk.attempt = Some(attempt_id);
    let connection = Arc::clone(&chunk.connection);
    let file = transfer.file.clone();
    let accepted = transfer.accepted.clone();
    if work.kind == WorkKind::Primary {
        dl.units[block].status = UnitStatus::Downloading;
        dl.units[block].interface_id = Some(network_id.clone());
    }
    let (range_start, range_end) = (dl.units[block].range_start, dl.units[block].range_end);
    if let Some(stream) = stream_mut(dl, stream_id) {
        stream.status = StreamStatus::Downloading;
        stream.hedge = (work.kind == WorkKind::Hedge).then_some(true);
        stream.range_start = range_start;
        stream.range_end = range_end;
        stream.current_block_index = Some(block);
    }

    // Where the request begins. A failed range leaves its written prefix in the staging file;
    // without range support the server can only restart from byte zero.
    let start_offset = if work.kind == WorkKind::Primary {
        let offset = if dl.request.supports_ranges { dl.units[block].bytes_downloaded } else { 0 };
        let keep = offset.max(other_attempts_position(http!(dl), attempt_id, block));
        if retract(&mut dl.units[block], keep, previous_writer.as_deref()) > 0 {
            recompute_aggregates(&mut dl.state, &dl.units);
        }
        offset
    } else {
        dl.units[block].bytes_downloaded
    };
    let attempt = http!(dl).attempts.get_mut(&attempt_id).unwrap();
    attempt.start_offset = Some(start_offset);
    attempt.started_at = now_ms();
    if dl.units[block].length().is_some_and(|length| start_offset >= length) {
        // The staging file already holds the whole block.
        let outcome = if work.kind == WorkKind::Primary { Outcome::Completed } else { Outcome::Aborted(AbortReason::Lost) };
        return Step::Decided { attempt: attempt_id, outcome };
    }
    let Some(file) = file else {
        return Step::Decided { attempt: attempt_id, outcome: Outcome::Stopped };
    };
    Step::Fetch(Box::new(FetchJob {
        attempt: attempt_id,
        network_id,
        url: dl.request.url.clone(),
        browser: dl.request.browser.clone(),
        range_start: range_start + start_offset,
        range_end,
        whole_file: !dl.request.supports_ranges,
        connection,
        file,
        cancel: abort,
        accepted,
    }))
}

/// Feeds an attempt's progress into the download as its bytes arrive and land.
struct AttemptHooks {
    core: Arc<Core>,
    download: String,
    stream_id: u32,
    attempt: u64,
    network_id: String,
}

impl AttemptHooks {
    fn with<R>(&self, f: impl FnOnce(&Arc<Core>, &mut Download) -> R) -> Option<R> {
        let mut engine = self.core.engine.lock();
        let dl = engine.downloads.get_mut(&self.download)?;
        if !matches!(dl.transfer, Transfer::Http(_)) {
            return None;
        }
        Some(f(&self.core, dl))
    }
}

impl ChunkHooks for AttemptHooks {
    fn on_network_progress(&self, received: u64) {
        self.with(|core, dl| {
            let now = now_ms();
            let transfer = http!(dl);
            let Some(attempt) = transfer.attempts.get_mut(&self.attempt) else { return };
            if received <= attempt.network_received {
                return;
            }
            let delta = received - attempt.network_received;
            attempt.network_received = received;
            attempt.last_network_at = now;
            if let Some(chunk) = transfer.chunks.get_mut(&self.stream_id) {
                chunk.received_bytes += delta;
                chunk.served = true;
            }
            let traffic = transfer.traffic(&self.network_id);
            traffic.received += delta;
            traffic.alive_at = now;
            if let Some(network) = dl.state.networks.iter_mut().find(|n| n.id == self.network_id) {
                if network.status == NetworkStatus::Unreachable {
                    // Through again: the next reconcile gives it back its streams.
                    network.status = NetworkStatus::On;
                    if let Some(chunk) = http!(dl).chunks.get_mut(&self.stream_id) {
                        chunk.failures = 0;
                    }
                }
            }
            dl.meters.add(self.stream_id, &self.network_id, delta);
            core.schedule_update(dl);
        });
    }

    fn on_progress(&self, written: u64) {
        self.with(|core, dl| {
            let transfer = http!(dl);
            let Some(attempt) = transfer.attempts.get_mut(&self.attempt) else { return };
            if written <= attempt.received {
                return;
            }
            attempt.received = written;
            let (block, position) = (attempt.block, attempt_position(attempt));
            transfer.disk.landed(now_ms());
            // Only what gets the block further than it already was counts: a racing attempt
            // re-fetches bytes the other already has.
            let gained = advance(&mut dl.units[block], &self.network_id, position);
            if let Some(stream) = stream_mut(dl, self.stream_id) {
                stream.bytes_downloaded += gained;
            }
            if let Some(network) = dl.state.networks.iter_mut().find(|n| n.id == self.network_id) {
                network.bytes_downloaded += gained;
            }
            dl.state.bytes_downloaded += gained;
            core.schedule_update(dl);
        });
    }

    fn on_write_wait(&self, waiting: bool) {
        self.with(|_, dl| {
            if let Some(attempt) = http!(dl).attempts.get_mut(&self.attempt) {
                attempt.write_waiting = waiting;
            }
        });
    }

    fn throttle(&self, bytes: u64) -> f64 {
        self.core.limits.take(&self.network_id, bytes)
    }
}

impl Core {
    /// Applies what became of an attempt to the download.
    fn finish_attempt(self: &Arc<Self>, dl: &mut Download, stream_id: u32, attempt_id: u64, outcome: Outcome) -> FollowUp {
        let follow = match outcome {
            Outcome::Completed => {
                self.finish_completed(dl, stream_id, attempt_id);
                FollowUp::Continue
            }
            Outcome::Stopped => {
                finish_stopped(dl, stream_id, attempt_id);
                FollowUp::Stop
            }
            Outcome::Aborted(reason) => {
                let attempt = http!(dl).attempts.get(&attempt_id);
                let (primary, nothing) =
                    attempt.map_or((false, false), |a| (a.kind == WorkKind::Primary, a.network_received == 0));
                // 'lost': another attempt decided the block. 'refresh': not a failure, so no retry
                // counted and no backoff.
                let_go(dl, stream_id, attempt_id, reason == AbortReason::Refresh && nothing);
                if reason == AbortReason::Refresh && primary {
                    self.schedule_update(dl);
                    let wait = http!(dl).chunks.get(&stream_id).map(|c| c.controller.clone());
                    wait.map_or(FollowUp::Continue, |wait| FollowUp::Handoff { wait })
                } else {
                    FollowUp::Continue
                }
            }
            Outcome::Failed(error) => self.finish_failed(dl, stream_id, attempt_id, error, None),
        };
        if !matches!(follow, FollowUp::Confirm(_)) {
            end_attempt(dl, stream_id, attempt_id);
        }
        follow
    }

    fn finish_completed(self: &Arc<Self>, dl: &mut Download, stream_id: u32, attempt_id: u64) {
        let Some(attempt) = http!(dl).attempts.get(&attempt_id) else { return };
        let (block, network, network_received, started_at, last_network_at) =
            (attempt.block, attempt.network_id.clone(), attempt.network_received, attempt.started_at, attempt.last_network_at);
        // Another attempt already finished this block: these bytes are surplus.
        if dl.units[block].status == UnitStatus::Completed {
            let_go(dl, stream_id, attempt_id, false);
            return;
        }
        end_attempt(dl, stream_id, attempt_id);
        dl.units[block].status = UnitStatus::Completed;
        dl.units[block].interface_id = Some(network.clone());
        if let Some(length) = dl.units[block].length() {
            // Progress can lag the final write: square the block up to its exact size.
            let gained = advance(&mut dl.units[block], &network, length);
            if let Some(stream) = stream_mut(dl, stream_id) {
                stream.bytes_downloaded += gained;
            }
            let elapsed = (if last_network_at > 0.0 { last_network_at } else { now_ms() }) - started_at;
            if let Some(chunk) = http!(dl).chunks.get_mut(&stream_id) {
                chunk.last_block_speed = network_received as f64 / elapsed.max(1.0) * 1000.0;
            }
        }
        let transfer = http!(dl);
        if let Some(chunk) = transfer.chunks.get_mut(&stream_id) {
            chunk.failures = 0;
            chunk.strikes = 0;
        }
        // Whoever is still racing for the block has lost.
        for rival in transfer.attempts_by_block.get(&block).cloned().unwrap_or_default() {
            if let Some(rival) = transfer.attempts.get_mut(&rival) {
                abort_attempt(rival, AbortReason::Lost);
            }
        }
        recompute_aggregates(&mut dl.state, &dl.units);
        self.schedule_update(dl);
    }

    fn finish_failed(
        self: &Arc<Self>,
        dl: &mut Download,
        stream_id: u32,
        attempt_id: u64,
        error: FetchError,
        verdict: Option<Verdict>,
    ) -> FollowUp {
        let Some(attempt) = http!(dl).attempts.get(&attempt_id) else { return FollowUp::Continue };
        let (kind, network_id, network_received, received) =
            (attempt.kind, attempt.network_id.clone(), attempt.network_received, attempt.received);
        let delivered_nothing = network_received == 0;
        let message = error.to_string();

        if let FetchError::RemoteChanged { size, seen, .. } = &error {
            let verdict = match verdict {
                Some(verdict) => verdict,
                None if *size => Verdict::Different,
                None if compare_version(&http!(dl).accepted, seen) == VersionCheck::Same => Verdict::Same,
                None => {
                    let samples = dl
                        .units
                        .iter()
                        .filter(|unit| unit.bytes_downloaded > 0)
                        .map(|unit| (unit.range_start, unit.bytes_downloaded))
                        .collect();
                    let connection = http!(dl).chunks.get(&stream_id).map(|c| Arc::clone(&c.connection));
                    let Some(connection) = connection else { return FollowUp::Stop };
                    return FollowUp::Confirm(Box::new(ConfirmJob {
                        seen: seen.clone(),
                        error: error.clone(),
                        samples,
                        file: DownloadFile::new(dl.partial.clone()),
                        url: dl.request.url.clone(),
                        browser: dl.request.browser.clone(),
                        connection,
                    }));
                }
            };
            // The check takes a moment; the download may have been paused or stopped meanwhile.
            if dl.state.status != DownloadStatus::Downloading {
                finish_stopped(dl, stream_id, attempt_id);
                return FollowUp::Stop;
            }
            match verdict {
                Verdict::Same => {
                    // Same bytes under another label: accept it and fetch again straight away;
                    // nothing from the rejected response was written.
                    let transfer = http!(dl);
                    if compare_version(&transfer.accepted, seen) != VersionCheck::Same {
                        transfer.accepted.push(seen.clone());
                    }
                    let_go(dl, stream_id, attempt_id, false);
                    return FollowUp::Continue;
                }
                Verdict::Different => {
                    // Every other worker would hit the same new version; what's on disk is of the
                    // old one, so no use.
                    self.fail_download(dl, &message, true);
                    finish_stopped(dl, stream_id, attempt_id);
                    return FollowUp::Stop;
                }
                Verdict::Unknown => {}
            }
        }

        if matches!(error, FetchError::NoRoute(_)) {
            // A redirect moved this worker to a host its network cannot reach.
            let_go(dl, stream_id, attempt_id, delivered_nothing);
            self.fail_network(dl, &network_id, &message);
            return FollowUp::Stop;
        }

        let now = now_ms();
        let connection_failed = matches!(error, FetchError::Connection(_));
        // A connection failing while the whole network has gone quiet: the network can't reach
        // the server. It stops being used, bar one stream that keeps trying.
        if connection_failed && mark_unreachable(dl, &network_id, now) {
            self.reconcile_download(dl);
        }
        // A hedge is optional work: its failure isn't retried, and what it wrote stays.
        if kind == WorkKind::Hedge {
            let_go(dl, stream_id, attempt_id, delivered_nothing);
            return FollowUp::Continue;
        }
        if let Some(network) = dl.state.networks.iter_mut().find(|n| n.id == network_id) {
            if network.status == NetworkStatus::On {
                network.retries += 1;
            }
        }
        let Some(chunk) = http!(dl).chunks.get_mut(&stream_id) else { return FollowUp::Stop };
        // What arrived shows the network works, and what was written shows the server does.
        if network_received > 0 {
            chunk.failures = 0;
        }
        if received > 0 {
            chunk.strikes = 0;
            chunk.busy_since = None;
        }
        chunk.failures += 1;
        if matches!(error, FetchError::Status { status: 403 | 429 | 503, .. }) {
            chunk.refused = true;
        }
        let busy = error.transient();
        if busy {
            chunk.busy_since.get_or_insert(now);
        }
        let waiting_out = busy && now - chunk.busy_since.unwrap_or(now) < SERVER_BUSY_FOR_MS;
        let failures = chunk.failures;
        let_go(dl, stream_id, attempt_id, delivered_nothing);

        // A connection that failed is the network's doing, tried for as long as the network is
        // there. A server that answered wrongly gets MAX_CHUNK_RETRIES in a row.
        if !connection_failed {
            let chunk = http!(dl).chunks.get_mut(&stream_id).unwrap();
            chunk.strikes += 1;
            if chunk.strikes > MAX_CHUNK_RETRIES && !waiting_out {
                chunk.retiring = true;
                if live_streams(dl, &network_id).is_empty() {
                    self.fail_network(dl, &network_id, &message);
                }
                return FollowUp::Stop;
            }
        }

        let mut wait = retry_delay_ms(failures);
        if network_status(dl, &network_id) == Some(NetworkStatus::Unreachable) {
            wait = wait.min(UNREACHABLE_RETRY_MS);
        }
        if let FetchError::Status { retry_after_ms: Some(asked), .. } = &error {
            if busy {
                // Asked of this network's address, not of this one request: none of the
                // network's streams sends another until then.
                let asked = (*asked as f64).min(RETRY_AFTER_MAX_MS);
                wait = wait.max(asked);
                let hold = http!(dl).hold_until.entry(network_id.clone()).or_insert(0.0);
                *hold = hold.max(now + asked);
            }
        }
        if let Some(stream) = stream_mut(dl, stream_id) {
            stream.status = StreamStatus::Retrying;
        }
        self.schedule_update(dl);
        let chunk = http!(dl).chunks.get(&stream_id).unwrap();
        FollowUp::BackOff { ms: wait, wait: chunk.controller.clone(), nudge: chunk.nudge.clone() }
    }
}

/// The stream was stopped while its request was in flight.
fn finish_stopped(dl: &mut Download, stream_id: u32, attempt_id: u64) {
    let_go(dl, stream_id, attempt_id, false);
    let paused = dl.state.status == DownloadStatus::Paused;
    if let Some(stream) = stream_mut(dl, stream_id) {
        stream.status = if paused { StreamStatus::Paused } else { StreamStatus::Cancelled };
    }
}

/// Waits `ms`, cut short by either token.
async fn sleep_unless(ms: f64, wait: &CancellationToken, nudge: Option<&CancellationToken>) {
    let sleep = tokio::time::sleep(Duration::from_secs_f64(ms.max(0.0) / 1000.0));
    match nudge {
        Some(nudge) => tokio::select! { _ = sleep => {}, _ = wait.cancelled() => {}, _ = nudge.cancelled() => {} },
        None => tokio::select! { _ = sleep => {}, _ = wait.cancelled() => {} },
    }
}

/// Re-fetches a spread of the bytes already on disk and compares them. Unknown when there's
/// nothing to compare yet, or the samples couldn't be fetched. With `label`, only a reply from a
/// server presenting it counts.
pub(crate) async fn compare_samples(
    samples: &[(u64, u64)],
    file: &DownloadFile,
    url: &str,
    browser: Option<&BrowserContext>,
    connection: &StreamConnection,
    label: Option<&FileVersion>,
    stall: Duration,
) -> &'static str {
    let step = (samples.len() as f64 / MAX_SAMPLES as f64).max(1.0);
    let picks: Vec<(u64, u64)> =
        (0..MAX_SAMPLES.min(samples.len())).map(|i| samples[(i as f64 * step) as usize]).collect();
    let mut compared = 0;
    for (start, bytes) in picks {
        let Ok(local) = file.read(start, SAMPLE_BYTES.min(bytes) as usize).await else { continue };
        if local.is_empty() {
            continue;
        }
        for _ in 0..SAMPLE_TRIES {
            match fetch_range(url, start, start + local.len() as u64 - 1, connection, browser, stall).await {
                Err(_) => return "unknown",
                Ok((body, version)) => {
                    if label.is_some_and(|label| compare_version(std::slice::from_ref(label), &version) != VersionCheck::Same) {
                        continue;
                    }
                    if body != local {
                        return "different";
                    }
                    compared += 1;
                    break;
                }
            }
        }
    }
    if compared > 0 { "same" } else { "unknown" }
}

async fn run_worker(core: Arc<Core>, download: String, stream_id: u32) {
    loop {
        let step = {
            let mut engine = core.engine.lock();
            let Some(dl) = engine.downloads.get_mut(&download) else { return };
            let step = next_step(dl, stream_id);
            core.schedule_update(dl);
            step
        };
        let (attempt, outcome) = match step {
            Step::Exit => break,
            Step::Idle { wait } => {
                sleep_unless(IDLE_POLL_MS as f64, &wait, None).await;
                continue;
            }
            Step::BackOff { ms, wait, nudge } => {
                sleep_unless(ms, &wait, Some(&nudge)).await;
                renew_nudge(&core, &download, stream_id);
                continue;
            }
            Step::Decided { attempt, outcome } => (attempt, outcome),
            Step::Fetch(job) => {
                let hooks = Arc::new(AttemptHooks {
                    core: Arc::clone(&core),
                    download: download.clone(),
                    stream_id,
                    attempt: job.attempt,
                    network_id: job.network_id.clone(),
                });
                let result = download_chunk(ChunkRequest {
                    url: &job.url,
                    browser: job.browser.as_ref(),
                    range_start: job.range_start,
                    range_end: job.range_end,
                    whole_file: job.whole_file,
                    connection: &job.connection,
                    file: Arc::clone(&job.file),
                    cancel: &job.cancel,
                    accepted: &job.accepted,
                    stall_timeout: core.knobs.stall_timeout,
                    hooks,
                })
                .await;
                let outcome = {
                    let mut engine = core.engine.lock();
                    let Some(dl) = engine.downloads.get_mut(&download) else { return };
                    let transfer = http!(dl);
                    let stopped = transfer.chunks.get(&stream_id).is_none_or(|c| c.controller.is_cancelled());
                    let reason = transfer.attempts.get(&job.attempt).and_then(|a| a.abort_reason);
                    match result {
                        Ok(()) => Outcome::Completed,
                        Err(_) if stopped || dl.state.status != DownloadStatus::Downloading => Outcome::Stopped,
                        Err(_) if reason.is_some() => Outcome::Aborted(reason.unwrap()),
                        Err(error) => Outcome::Failed(error),
                    }
                };
                (job.attempt, outcome)
            }
        };

        let mut follow = {
            let mut engine = core.engine.lock();
            let Some(dl) = engine.downloads.get_mut(&download) else { return };
            core.finish_attempt(dl, stream_id, attempt, outcome)
        };
        loop {
            match follow {
                FollowUp::Continue => break,
                FollowUp::Stop => {
                    finish_worker(&core, &download, stream_id);
                    return;
                }
                FollowUp::Handoff { wait } => {
                    sleep_unless(REFRESH_HANDOFF_MS as f64, &wait, None).await;
                    let mut engine = core.engine.lock();
                    if let Some(chunk) = engine.downloads.get_mut(&download).and_then(|dl| match &mut dl.transfer {
                        Transfer::Http(transfer) => transfer.chunks.get_mut(&stream_id),
                        Transfer::Torrent(_) => None,
                    }) {
                        chunk.warm_since = now_ms();
                        chunk.slow_since = None;
                    }
                    break;
                }
                FollowUp::BackOff { ms, wait, nudge } => {
                    sleep_unless(ms, &wait, Some(&nudge)).await;
                    renew_nudge(&core, &download, stream_id);
                    let mut engine = core.engine.lock();
                    let Some(dl) = engine.downloads.get_mut(&download) else { return };
                    let stopped = http!(dl).chunks.get(&stream_id).is_none_or(|c| c.controller.is_cancelled());
                    let status = dl.state.status;
                    if stopped || status != DownloadStatus::Downloading {
                        if let Some(stream) = stream_mut(dl, stream_id) {
                            stream.status =
                                if status == DownloadStatus::Paused { StreamStatus::Paused } else { StreamStatus::Cancelled };
                            stream.speed_bytes_per_sec = 0.0;
                        }
                        drop(engine);
                        finish_worker(&core, &download, stream_id);
                        return;
                    }
                    go_idle(dl, stream_id);
                    if let Some(chunk) = http!(dl).chunks.get_mut(&stream_id) {
                        chunk.warm_since = now_ms();
                        chunk.slow_since = None;
                    }
                    break;
                }
                FollowUp::Confirm(job) => {
                    let verdict = compare_samples(
                        &job.samples,
                        &job.file,
                        &job.url,
                        job.browser.as_ref(),
                        &job.connection,
                        Some(&job.seen),
                        core.knobs.stall_timeout,
                    )
                    .await;
                    let verdict = match verdict {
                        "same" => Verdict::Same,
                        "different" => Verdict::Different,
                        _ => Verdict::Unknown,
                    };
                    let mut engine = core.engine.lock();
                    let Some(dl) = engine.downloads.get_mut(&download) else { return };
                    follow = core.finish_failed(dl, stream_id, attempt, job.error, Some(verdict));
                    end_attempt(dl, stream_id, attempt);
                }
            }
        }
    }
    finish_worker(&core, &download, stream_id);
}

fn renew_nudge(core: &Arc<Core>, download: &str, stream_id: u32) {
    let mut engine = core.engine.lock();
    if let Some(Transfer::Http(transfer)) = engine.downloads.get_mut(download).map(|dl| &mut dl.transfer) {
        if let Some(chunk) = transfer.chunks.get_mut(&stream_id) {
            if chunk.nudge.is_cancelled() {
                chunk.nudge = CancellationToken::new();
            }
        }
    }
}

/// Its sockets would otherwise stay open, kept alive for requests that will never come.
fn finish_worker(core: &Arc<Core>, download: &str, stream_id: u32) {
    let mut engine = core.engine.lock();
    let Some(dl) = engine.downloads.get_mut(download) else { return };
    let Transfer::Http(transfer) = &mut dl.transfer else { return };
    transfer.workers.remove(&stream_id);
    let retiring = transfer.chunks.get(&stream_id).is_some_and(|chunk| {
        chunk.connection.reconnect();
        chunk.retiring
    });
    transfer.worker_ended.notify_one();
    if retiring {
        core.remove_stream(dl, stream_id);
    }
    core.schedule_update(dl);
}
