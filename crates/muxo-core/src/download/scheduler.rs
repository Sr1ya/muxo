//! Who should fetch what, decided from a snapshot of the download and nothing else, so every rule
//! here can be checked against exact situations.
//!
//! A free stream is given one of two kinds of work:
//!
//!   primary  A block nobody is fetching. The normal case, and the only one until the queue runs dry.
//!   hedge    A second attempt at a block someone else is fetching too slowly. Only handed out once
//!            no block is left waiting, so it never takes bandwidth from work that still needs
//!            doing. Whichever attempt finishes first wins; the others are dropped.

use crate::types::{HttpStreamState, StreamStatus, UnitState, UnitStatus};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkKind {
    Primary,
    Hedge,
}

#[derive(Debug, Clone)]
pub struct AttemptView {
    pub kind: WorkKind,
    pub stream_id: u32,
    pub network_id: String,
    /// When its request was sent.
    pub started_at: f64,
    /// The disk, not the connection, sets its pace.
    pub write_waiting: bool,
}

pub struct SchedulerState<'a> {
    pub blocks: &'a [UnitState],
    pub streams: &'a [HttpStreamState],
    /// Attempts in flight, by block index.
    pub attempts: &'a HashMap<usize, Vec<AttemptView>>,
    /// For a block whose last attempt on some network delivered nothing, that network.
    pub avoid: &'a HashMap<usize, String>,
    pub hedges_used: &'a HashMap<usize, u32>,
    /// The disk can't keep up: a hedge would only add writes.
    pub disk_behind: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SchedulerPolicy {
    /// A block is only raced once every attempt at it has been going this long.
    pub hedge_after_ms: f64,
    pub max_hedges_per_block: u32,
    /// What a new request costs before its first byte arrives.
    pub startup_ms: f64,
}

pub struct Requester<'a> {
    pub id: u32,
    pub network_id: &'a str,
    /// How fast this stream fetched its last block, if it has finished one.
    pub speed_bytes_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Work {
    pub kind: WorkKind,
    pub block: usize,
}

/// The first waiting block, except that one whose last attempt on this network delivered nothing
/// is left to another network for as long as a stream there is free to take it.
fn next_waiting_block(state: &SchedulerState, network_id: &str) -> Option<usize> {
    let other_network_free = state
        .streams
        .iter()
        .any(|stream| stream.interface_id != network_id && stream.status == StreamStatus::Pending);
    state
        .blocks
        .iter()
        .find(|block| {
            block.status == UnitStatus::Pending
                && !(other_network_free
                    && state.avoid.get(&block.index).map(String::as_str) == Some(network_id))
        })
        .map(|block| block.index)
}

/// The block most worth another attempt: the one that will be longest yet, if the requester
/// would clearly beat it. A block is as slow as its fastest attempt.
fn next_hedge_target(state: &SchedulerState, who: &Requester, now: f64, policy: &SchedulerPolicy) -> Option<usize> {
    if state.disk_behind || state.blocks.iter().any(|block| block.status == UnitStatus::Pending) {
        return None;
    }
    let speed_of = |stream_id: u32| {
        state.streams.iter().find(|stream| stream.id == stream_id).map_or(0.0, |s| s.speed_bytes_per_sec)
    };
    let mut target = None;
    let mut latest = 0.0;
    let mut indexes: Vec<&usize> = state.attempts.keys().collect();
    indexes.sort_unstable();
    for &index in indexes {
        let attempts = &state.attempts[&index];
        let Some(block) = state.blocks.get(index) else { continue };
        let Some(range_end) = block.range_end else { continue };
        if block.status != UnitStatus::Downloading {
            continue;
        }
        if attempts.is_empty() || !attempts.iter().any(|attempt| attempt.kind == WorkKind::Primary) {
            continue;
        }
        if attempts.iter().any(|attempt| attempt.stream_id == who.id) {
            continue;
        }
        if state.hedges_used.get(&index).copied().unwrap_or(0) >= policy.max_hedges_per_block {
            continue;
        }
        if state.avoid.get(&index).map(String::as_str) == Some(who.network_id) {
            continue;
        }
        if attempts.iter().any(|attempt| now - attempt.started_at < policy.hedge_after_ms) {
            continue;
        }
        if attempts.iter().any(|attempt| attempt.write_waiting) {
            continue;
        }
        let length = range_end - block.range_start + 1;
        if block.bytes_downloaded >= length {
            continue;
        }
        let remaining = (length - block.bytes_downloaded) as f64;
        let eta = attempts
            .iter()
            .map(|attempt| {
                let speed = speed_of(attempt.stream_id);
                if speed > 0.0 { remaining / speed * 1000.0 } else { f64::INFINITY }
            })
            .fold(f64::INFINITY, f64::min);
        let worth_it = match who.speed_bytes_per_sec.filter(|speed| *speed > 0.0) {
            Some(speed) => eta >= 2.0 * (policy.startup_ms + remaining / speed * 1000.0),
            None => eta >= policy.hedge_after_ms,
        };
        if !worth_it {
            continue;
        }
        // The holders' network may be what is slow, so a stream on another network gets the
        // first go, unless none of those would take it either.
        let holders: Vec<&str> = attempts.iter().map(|attempt| attempt.network_id.as_str()).collect();
        let other_network_free = state.streams.iter().any(|stream| {
            !holders.contains(&stream.interface_id.as_str())
                && stream.status == StreamStatus::Pending
                && state.avoid.get(&index) != Some(&stream.interface_id)
        });
        if holders.contains(&who.network_id) && other_network_free {
            continue;
        }
        if eta > latest {
            target = Some(index);
            latest = eta;
        }
    }
    target
}

pub fn pick_work(state: &SchedulerState, who: &Requester, now: f64, policy: &SchedulerPolicy) -> Option<Work> {
    if let Some(block) = next_waiting_block(state, who.network_id) {
        return Some(Work { kind: WorkKind::Primary, block });
    }
    next_hedge_target(state, who, now, policy).map(|block| Work { kind: WorkKind::Hedge, block })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::plan::plan_blocks;

    fn stream(id: u32, network: &str, status: StreamStatus, speed: f64) -> HttpStreamState {
        HttpStreamState {
            id,
            interface_id: network.into(),
            range_start: 0,
            range_end: None,
            bytes_downloaded: 0,
            speed_bytes_per_sec: speed,
            status,
            current_block_index: None,
            hedge: None,
        }
    }

    const POLICY: SchedulerPolicy = SchedulerPolicy { hedge_after_ms: 3000.0, max_hedges_per_block: 2, startup_ms: 1000.0 };

    #[test]
    fn hands_out_waiting_blocks_first_and_avoids_a_silent_network() {
        let blocks = plan_blocks(4 << 20, 1 << 20);
        let streams = vec![stream(0, "a", StreamStatus::Pending, 0.0), stream(1, "b", StreamStatus::Pending, 0.0)];
        let attempts = HashMap::new();
        let avoid = HashMap::from([(0usize, "a".to_string())]);
        let hedges = HashMap::new();
        let state = SchedulerState { blocks: &blocks, streams: &streams, attempts: &attempts, avoid: &avoid, hedges_used: &hedges, disk_behind: false };
        let work = pick_work(&state, &Requester { id: 0, network_id: "a", speed_bytes_per_sec: None }, 0.0, &POLICY);
        assert_eq!(work, Some(Work { kind: WorkKind::Primary, block: 1 }));
    }

    #[test]
    fn races_a_slow_block_once_the_queue_is_dry() {
        let mut blocks = plan_blocks(2 << 20, 1 << 20);
        blocks[0].status = UnitStatus::Completed;
        blocks[1].status = UnitStatus::Downloading;
        let streams = vec![stream(0, "a", StreamStatus::Downloading, 1000.0), stream(1, "b", StreamStatus::Pending, 0.0)];
        let attempts = HashMap::from([(1usize, vec![AttemptView { kind: WorkKind::Primary, stream_id: 0, network_id: "a".into(), started_at: 0.0, write_waiting: false }])]);
        let avoid = HashMap::new();
        let hedges = HashMap::new();
        let state = SchedulerState { blocks: &blocks, streams: &streams, attempts: &attempts, avoid: &avoid, hedges_used: &hedges, disk_behind: false };
        let who = Requester { id: 1, network_id: "b", speed_bytes_per_sec: Some(10_000_000.0) };
        assert_eq!(pick_work(&state, &who, 5000.0, &POLICY), Some(Work { kind: WorkKind::Hedge, block: 1 }));
        // Too early to tell.
        assert_eq!(pick_work(&state, &who, 1000.0, &POLICY), None);
    }
}
