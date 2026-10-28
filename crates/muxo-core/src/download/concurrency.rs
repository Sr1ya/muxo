//! How many streams each network runs. Deliberately simple, after Gopeed and aria2: no speed
//! comparisons, so noise on a flaky network can't move it.
//!
//! - A network starts with START_STREAMS_PER_NETWORK, or the count the user picked, which it then
//!   keeps rather than grows.
//! - Once every one of its streams has received data, it doubles, up to the limit.
//! - A server that refuses some of a network's streams (503, 429, 403, or leaves them unanswered)
//!   while sending data down others wants fewer connections from this address: the refused
//!   streams close, and the network stays at the ones left. After RECOVER_MS without another
//!   refusal it may have one more, and so on back up to the limit.
//! - A disk that can't keep up holds each stream's reading until its writes catch up. While the
//!   disk has been behind for DISK_PATIENCE_MS no network grows, and every network is cut to half
//!   the most any runs; each DISK_RECOVER_MS it keeps up at the cap, the cap doubles.
//! - Downloads running at once share a network's limit evenly (see Host::downloads_on).

use std::collections::HashMap;
use std::sync::Arc;

pub const RECOVER_MS: f64 = 60_000.0;
pub const DISK_PATIENCE_MS: f64 = 500.0;
pub const DISK_RECOVER_MS: f64 = 10_000.0;
/// No write landed for this long: the disk has stopped rather than slowed down.
pub const DISK_STALLED_MS: f64 = 500.0;

#[derive(Debug, Clone)]
pub struct NetworkSnapshot {
    pub id: String,
    /// Its streams, not counting any retiring.
    pub streams: usize,
    /// Its streams that have received data.
    pub answered: usize,
    /// Its streams the server refused a request from since the last snapshot.
    pub refused: usize,
    /// Its streams that received data since the last snapshot.
    pub served: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskReading {
    Behind,
    KeepingUp,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub struct DiskSnapshot {
    pub reading: DiskReading,
    pub since: f64,
}

pub struct Snapshot {
    pub now: f64,
    /// The networks in use: the ones it may grow.
    pub networks: Vec<NetworkSnapshot>,
    /// How many more streams the waiting blocks could keep busy.
    pub spare_work: usize,
    pub disk: Option<DiskSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Add { network_id: String, count: usize },
    Retire { network_id: String, count: usize },
}

/// Whether the disk is what holds a download back, judged by the clock rather than by how often
/// it is looked at.
#[derive(Debug, Clone)]
pub struct DiskWatch {
    pub reading: DiskReading,
    pub since: f64,
    landed_at: f64,
}

impl Default for DiskWatch {
    fn default() -> Self {
        DiskWatch { reading: DiskReading::Unknown, since: 0.0, landed_at: 0.0 }
    }
}

impl DiskWatch {
    pub fn landed(&mut self, now: f64) {
        self.landed_at = now;
    }

    /// One look: the streams fetching a block, and how many of those hold bytes the disk hasn't
    /// taken. Held while nothing lands is a disk that stopped, which fewer streams wouldn't help.
    pub fn sample(&mut self, now: f64, writing: usize, held: usize) {
        let reading = if writing == 0 {
            DiskReading::Unknown
        } else if held * 2 <= writing {
            DiskReading::KeepingUp
        } else if now - self.landed_at < DISK_STALLED_MS {
            DiskReading::Behind
        } else {
            DiskReading::Unknown
        };
        if reading != self.reading {
            self.reading = reading;
            self.since = now;
        }
    }

    /// How long it has read `reading` without a break; 0 if it doesn't now.
    pub fn lasted(&self, reading: DiskReading, now: f64) -> f64 {
        if self.reading == reading { now - self.since } else { 0.0 }
    }

    pub fn reset(&mut self) {
        self.reading = DiskReading::Unknown;
        self.since = 0.0;
    }
}

struct Ceiling {
    limit: usize,
    since: f64,
}

pub struct ConcurrencyController {
    max_per_network: usize,
    grows: bool,
    share: Arc<dyn Fn(&str) -> usize + Send + Sync>,
    ceilings: HashMap<String, Ceiling>,
    disk_cap: Option<usize>,
    disk_from: f64,
}

impl ConcurrencyController {
    pub fn new(max_per_network: usize, grows: bool, share: Arc<dyn Fn(&str) -> usize + Send + Sync>) -> Self {
        ConcurrencyController { max_per_network, grows, share, ceilings: HashMap::new(), disk_cap: None, disk_from: 0.0 }
    }

    pub fn tick(&mut self, snapshot: &Snapshot) -> Vec<Action> {
        let mut actions = Vec::new();
        let now = snapshot.now;
        let mut spare = snapshot.spare_work;
        if self.grows {
            self.judge_disk(snapshot);
        }
        let disk_holds =
            self.grows && snapshot.disk.is_some_and(|disk| disk.reading != DiskReading::KeepingUp);
        for network in &snapshot.networks {
            let refused_now = network.refused > 0 && network.served > 0;
            if refused_now {
                let previous = self.ceilings.get(&network.id).map_or(usize::MAX, |entry| entry.limit);
                let limit = network.streams.saturating_sub(network.refused).min(previous).max(1);
                self.ceilings.insert(network.id.clone(), Ceiling { limit, since: now });
            } else if let Some(entry) = self.ceilings.get_mut(&network.id) {
                if network.refused == 0 && now - entry.since >= RECOVER_MS {
                    entry.limit += 1;
                    entry.since = now;
                    if entry.limit >= self.max_per_network {
                        self.ceilings.remove(&network.id);
                    }
                }
            }
            let ceiling = self.limit(&network.id);
            if network.streams > ceiling {
                actions.push(Action::Retire { network_id: network.id.clone(), count: network.streams - ceiling });
                continue;
            }
            if network.refused > 0 || disk_holds || network.streams == 0 || network.answered < network.streams {
                continue;
            }
            let growth = if self.grows { network.streams } else { usize::MAX };
            let count = growth.min(ceiling - network.streams).min(spare);
            if count < 1 {
                continue;
            }
            spare -= count;
            actions.push(Action::Add { network_id: network.id.clone(), count });
        }
        actions
    }

    /// The most streams a network may run now: the lowest of a server's ceiling, the disk's cap
    /// and this download's share, and never more than the limit.
    pub fn limit(&self, network_id: &str) -> usize {
        self.max_per_network
            .min((self.share)(network_id))
            .min(self.ceilings.get(network_id).map_or(usize::MAX, |entry| entry.limit))
            .min(self.disk_cap.unwrap_or(usize::MAX))
    }

    fn judge_disk(&mut self, snapshot: &Snapshot) {
        let Some(disk) = snapshot.disk else { return };
        let streams = snapshot.networks.iter().map(|network| network.streams).max().unwrap_or(0);
        let lasted = snapshot.now - disk.since.max(self.disk_from);
        match (disk.reading, self.disk_cap) {
            (DiskReading::Behind, _) if lasted >= DISK_PATIENCE_MS && streams > 1 => {
                self.disk_cap = Some(streams.div_ceil(2));
            }
            // Keeping up below the cap says nothing about it: the wait starts once the streams
            // are back at it.
            (DiskReading::KeepingUp, Some(cap)) if streams < cap => return,
            (DiskReading::KeepingUp, Some(cap)) if lasted >= DISK_RECOVER_MS => {
                self.disk_cap = if cap * 2 >= self.max_per_network { None } else { Some(cap * 2) };
            }
            _ => return,
        }
        self.disk_from = snapshot.now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn net(streams: usize, answered: usize, refused: usize, served: usize) -> NetworkSnapshot {
        NetworkSnapshot { id: "a".into(), streams, answered, refused, served }
    }

    #[test]
    fn doubles_once_every_stream_is_answered_then_backs_off_on_refusal() {
        let mut controller = ConcurrencyController::new(32, true, Arc::new(|_| usize::MAX));
        let grow = controller.tick(&Snapshot { now: 0.0, networks: vec![net(8, 8, 0, 8)], spare_work: 100, disk: None });
        assert_eq!(grow, vec![Action::Add { network_id: "a".into(), count: 8 }]);
        let cut = controller.tick(&Snapshot { now: 1.0, networks: vec![net(16, 16, 3, 13)], spare_work: 100, disk: None });
        assert_eq!(cut, vec![Action::Retire { network_id: "a".into(), count: 3 }]);
        assert_eq!(controller.limit("a"), 13);
        controller.tick(&Snapshot { now: 1.0 + RECOVER_MS, networks: vec![net(13, 13, 0, 13)], spare_work: 0, disk: None });
        assert_eq!(controller.limit("a"), 14);
    }

    #[test]
    fn halves_while_the_disk_is_behind() {
        let mut controller = ConcurrencyController::new(32, true, Arc::new(|_| usize::MAX));
        let disk = Some(DiskSnapshot { reading: DiskReading::Behind, since: 0.0 });
        let actions = controller.tick(&Snapshot { now: 600.0, networks: vec![net(16, 16, 0, 16)], spare_work: 100, disk });
        assert_eq!(actions, vec![Action::Retire { network_id: "a".into(), count: 8 }]);
    }
}
