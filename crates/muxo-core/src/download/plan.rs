//! How a download is cut into blocks, and how many streams a network starts on them. Pure, so
//! every rule here can be checked against exact situations. How many streams a network ends up
//! with is decided while the download runs (see concurrency.rs).

use crate::types::{TransferKind, UnitState, UnitStatus};
use std::collections::BTreeMap;

const MIB: u64 = 1024 * 1024;

/// The largest block. A racing second attempt re-fetches a block rather than splitting it (see
/// scheduler.rs), so the last block's size is how long the slowest connection can hold the
/// download up.
pub const DEFAULT_MAX_BLOCK_BYTES: u64 = 8 * MIB;
/// Below about a megabyte the round trips cost more than splitting the file saves.
pub const MIN_BLOCK_BYTES: u64 = MIB;
/// Streams each network starts with, as download managers do (IDM, XDM: 8).
pub const START_STREAMS_PER_NETWORK: usize = 8;
/// The most streams a network can grow to (IDM's limit).
pub const MAX_STREAMS_PER_NETWORK: usize = 32;
/// Blocks planned per stream a network could grow to: a fast network takes more of them, but
/// only if there are more blocks than streams.
const BLOCKS_PER_STREAM: u64 = 2;

/// Bytes per block; the last block holds the remainder.
pub fn plan_block_size(total_bytes: u64, splittable: bool, network_count: usize, max_block: Option<u64>) -> u64 {
    if !splittable || total_bytes == 0 {
        return total_bytes;
    }
    let max_block = max_block.unwrap_or(DEFAULT_MAX_BLOCK_BYTES);
    let target_blocks = network_count.max(1) as u64 * MAX_STREAMS_PER_NETWORK as u64 * BLOCKS_PER_STREAM;
    total_bytes.div_ceil(target_blocks).clamp(MIN_BLOCK_BYTES.min(max_block), max_block)
}

/// Lays `items` out one from each group in turn (a, b, a, b, …), keeping each group's own order.
pub fn interleave<T, K: PartialEq>(items: Vec<T>, group_of: impl Fn(&T) -> K) -> Vec<T> {
    let mut lanes: Vec<(K, Vec<T>)> = Vec::new();
    for item in items {
        let key = group_of(&item);
        match lanes.iter_mut().find(|(lane, _)| *lane == key) {
            Some((_, lane)) => lane.push(item),
            None => lanes.push((key, vec![item])),
        }
    }
    let mut iterators: Vec<_> = lanes.into_iter().map(|(_, lane)| lane.into_iter()).collect();
    let mut out = Vec::new();
    loop {
        let mut any = false;
        for lane in iterators.iter_mut() {
            if let Some(item) = lane.next() {
                out.push(item);
                any = true;
            }
        }
        if !any {
            return out;
        }
    }
}

/// Streams to start on each of `network_count` networks joining a download with `waiting` blocks
/// nobody has taken. Near the end, or on a small file, fewer — but never none, so a network that
/// joins can still race a slow block.
pub fn starting_streams(waiting: usize, network_count: usize, requested: Option<usize>) -> usize {
    let requested = requested.unwrap_or(START_STREAMS_PER_NETWORK).clamp(1, MAX_STREAMS_PER_NETWORK);
    let per_network = waiting.div_ceil(network_count.max(1));
    requested.min(per_network).max(1)
}

fn fresh(kind: TransferKind, index: usize, range_start: u64, range_end: Option<u64>) -> UnitState {
    UnitState {
        kind,
        index,
        range_start,
        range_end,
        status: UnitStatus::Pending,
        interface_id: None,
        bytes_downloaded: 0,
        bytes_by_interface: BTreeMap::new(),
        provisional_bytes: (kind == TransferKind::Torrent).then_some(0),
    }
}

/// The blocks a file of `total_bytes` is cut into. An unknown size is one open-ended block.
pub fn plan_blocks(total_bytes: u64, block_size: u64) -> Vec<UnitState> {
    if total_bytes == 0 || block_size == 0 {
        return vec![fresh(TransferKind::Http, 0, 0, None)];
    }
    (0..total_bytes.div_ceil(block_size))
        .map(|index| {
            let start = index * block_size;
            fresh(TransferKind::Http, index as usize, start, Some((start + block_size).min(total_bytes) - 1))
        })
        .collect()
}

/// A torrent's pieces. Received bytes are provisional until the piece's hash verifies.
pub fn plan_pieces(total_bytes: u64, piece_length: u64) -> Vec<UnitState> {
    if total_bytes == 0 || piece_length == 0 {
        return Vec::new();
    }
    (0..total_bytes.div_ceil(piece_length))
        .map(|index| {
            let start = index * piece_length;
            fresh(TransferKind::Torrent, index as usize, start, Some((start + piece_length).min(total_bytes) - 1))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_blocks_between_the_bounds() {
        assert_eq!(plan_block_size(10 * MIB, true, 1, None), MIB);
        assert_eq!(plan_block_size(10_000 * MIB, true, 1, None), 8 * MIB);
        assert_eq!(plan_block_size(500, false, 2, None), 500);
        let blocks = plan_blocks(10 * MIB + 1, MIB);
        assert_eq!(blocks.len(), 11);
        assert_eq!(blocks[10].range_end, Some(10 * MIB));
        assert_eq!(plan_blocks(0, 0)[0].range_end, None);
    }

    #[test]
    fn interleaves_by_group() {
        let out = interleave(vec![("a", 1), ("a", 2), ("b", 3), ("b", 4), ("a", 5)], |item| item.0);
        assert_eq!(out.iter().map(|item| item.1).collect::<Vec<_>>(), vec![1, 3, 2, 4, 5]);
    }

    #[test]
    fn starts_fewer_streams_near_the_end() {
        assert_eq!(starting_streams(100, 2, None), 8);
        assert_eq!(starting_streams(3, 2, None), 2);
        assert_eq!(starting_streams(0, 2, None), 1);
    }
}
