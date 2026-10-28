//! Speeds: bytes counted as they arrive, and read as a rate only on the download's clock (its
//! tick). Arrivals come in bursts, so a window that starts or ends on one is off by up to a burst;
//! between two clock readings, where the bursts fall doesn't matter.

use crate::types::{Detail, DownloadState, StreamStatus};
use std::collections::{HashMap, VecDeque};

/// Raw deltas are too noisy; averaging over a few seconds tracks reality without jumping around.
const SPEED_WINDOW_MS: f64 = 3000.0;

#[derive(Default)]
pub struct Meter {
    total: u64,
    readings: VecDeque<(u64, f64)>,
}

impl Meter {
    pub fn add(&mut self, bytes: u64) {
        self.total += bytes;
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    /// The rate over the last SPEED_WINDOW_MS, as of `now`; over at least a second, so one burst
    /// over a few ms doesn't read as a speed the connection never had.
    pub fn read(&mut self, now: f64) -> f64 {
        self.readings.push_back((self.total, now));
        while self.readings.len() > 2 && self.readings[1].1 <= now - SPEED_WINDOW_MS {
            self.readings.pop_front();
        }
        let (bytes, time) = self.readings[0];
        (self.total - bytes) as f64 / ((now - time) / 1000.0).max(1.0)
    }
}

/// A download's meters in one direction: each connection's, and each network's.
#[derive(Default)]
pub struct Meters {
    pub connections: HashMap<u32, Meter>,
    pub networks: HashMap<String, Meter>,
}

impl Meters {
    pub fn add(&mut self, connection: u32, network_id: &str, bytes: u64) {
        self.connections.entry(connection).or_default().add(bytes);
        self.networks.entry(network_id.to_owned()).or_default().add(bytes);
    }

    pub fn clear(&mut self) {
        self.connections.clear();
        self.networks.clear();
    }
}

fn set(target: &mut f64, value: f64, changed: &mut bool) {
    if *target != value {
        *changed = true;
        *target = value;
    }
}

/// Reads every meter as of `now` into the download's speeds. Says whether any of them changed.
pub fn update_speeds(state: &mut DownloadState, meters: &mut Meters, uploads: Option<&mut Meters>, now: f64) -> bool {
    let mut changed = false;
    match &mut state.detail {
        Detail::Http(detail) => {
            for stream in &mut detail.streams {
                let speed = meters.connections.get_mut(&stream.id).map_or(0.0, |meter| meter.read(now));
                let moving = stream.status == StreamStatus::Downloading;
                set(&mut stream.speed_bytes_per_sec, if moving { speed } else { 0.0 }, &mut changed);
            }
        }
        Detail::Torrent(detail) => {
            for peer in &mut detail.peers {
                let speed = meters.connections.get_mut(&peer.id).map_or(0.0, |meter| meter.read(now));
                set(&mut peer.speed_bytes_per_sec, speed, &mut changed);
            }
        }
    }
    let mut total = 0.0;
    for network in &mut state.networks {
        let speed = meters.networks.get_mut(&network.id).map_or(0.0, |meter| meter.read(now));
        set(&mut network.speed_bytes_per_sec, speed, &mut changed);
        total += speed;
    }
    set(&mut state.speed_bytes_per_sec, total, &mut changed);

    if let (Some(uploads), Detail::Torrent(detail)) = (uploads, &mut state.detail) {
        for peer in &mut detail.peers {
            let speed = uploads.connections.get_mut(&peer.id).map_or(0.0, |meter| meter.read(now));
            set(&mut peer.upload_speed_bytes_per_sec, speed, &mut changed);
        }
        let mut uploaded = 0.0;
        for network in &mut state.networks {
            let speed = uploads.networks.get_mut(&network.id).map_or(0.0, |meter| meter.read(now));
            let mut current = network.upload_speed_bytes_per_sec.unwrap_or(0.0);
            set(&mut current, speed, &mut changed);
            network.upload_speed_bytes_per_sec = Some(current);
            uploaded += speed;
        }
        set(&mut detail.upload_speed_bytes_per_sec, uploaded, &mut changed);
    }
    changed
}

// How far time left moves toward a new estimate in a second: further down than up, so a speed-up
// shows at once while a wobble doesn't add minutes (Firefox's shares, per second).
const TIME_LEFT_DOWN: f64 = 0.3;
const TIME_LEFT_UP: f64 = 0.1;

pub fn smooth_time_left(previous: Option<f64>, elapsed_seconds: f64, estimate: f64) -> f64 {
    let expected = previous.map_or(0.0, |previous| previous - elapsed_seconds);
    if expected <= 0.0 || estimate <= expected / 2.0 {
        return estimate;
    }
    let share = if estimate < expected { TIME_LEFT_DOWN } else { TIME_LEFT_UP };
    expected + (estimate - expected) * (1.0 - (1.0 - share).powf(elapsed_seconds))
}

/// Brings time left up to date from the speed. Says whether it changed.
pub fn update_time_left(state: &mut DownloadState, elapsed_seconds: f64) -> bool {
    let wanted = state.total_bytes.saturating_sub(state.skipped_bytes());
    let before = state.time_left_seconds;
    state.time_left_seconds = (state.total_bytes > 0 && state.speed_bytes_per_sec > 0.0).then(|| {
        smooth_time_left(
            before,
            elapsed_seconds,
            wanted.saturating_sub(state.bytes_downloaded) as f64 / state.speed_bytes_per_sec,
        )
    });
    state.time_left_seconds != before
}

/// Nothing is moving: a paused or stopped download reads 0 everywhere.
pub fn clear_speeds(state: &mut DownloadState) {
    state.speed_bytes_per_sec = 0.0;
    state.time_left_seconds = None;
    for network in &mut state.networks {
        network.speed_bytes_per_sec = 0.0;
        if network.upload_speed_bytes_per_sec.is_some() {
            network.upload_speed_bytes_per_sec = Some(0.0);
        }
    }
    match &mut state.detail {
        Detail::Http(detail) => {
            detail.disk_limited = None;
            for stream in &mut detail.streams {
                stream.speed_bytes_per_sec = 0.0;
            }
        }
        Detail::Torrent(detail) => {
            detail.upload_speed_bytes_per_sec = 0.0;
            for peer in &mut detail.peers {
                peer.speed_bytes_per_sec = 0.0;
                peer.upload_speed_bytes_per_sec = 0.0;
            }
        }
    }
}
