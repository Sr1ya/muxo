//! A unit's `bytes_downloaded` is its frontier: how much of it, from its first byte, is secured.
//! Two attempts can race for one block, so the frontier is the furthest either has reached, not
//! the sum of what they received. `bytes_by_interface` splits it by network and always adds up to
//! it. Also how units are checkpointed and read back.

use crate::download::plan::{plan_blocks, plan_pieces};
use crate::types::{DownloadState, UnitState, UnitStatus};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// Moves the frontier forward to `position`, crediting the bytes it gains to `network_id`. Does
/// nothing if something already got further. Returns the gain.
pub fn advance(unit: &mut UnitState, network_id: &str, position: u64) -> u64 {
    let next = unit.length().map_or(position, |length| position.min(length));
    if next <= unit.bytes_downloaded {
        return 0;
    }
    let gained = next - unit.bytes_downloaded;
    unit.bytes_downloaded = next;
    *unit.bytes_by_interface.entry(network_id.to_owned()).or_insert(0) += gained;
    gained
}

/// Pulls the frontier back to `position`, for bytes that turned out not to be there. The lost
/// bytes are at the tail, so they come off `blamed` (the network that wrote last) first.
pub fn retract(unit: &mut UnitState, position: u64, blamed: Option<&str>) -> u64 {
    if unit.bytes_downloaded <= position {
        return 0;
    }
    let removed = unit.bytes_downloaded - position;
    let attributed: u64 = unit.bytes_by_interface.values().sum();
    let mut excess = attributed.saturating_sub(position);
    let mut order: Vec<String> = unit.bytes_by_interface.keys().cloned().collect();
    order.sort_by_key(|id| Some(id.as_str()) != blamed);
    for id in order {
        if excess == 0 {
            break;
        }
        let bytes = unit.bytes_by_interface[&id];
        let taken = bytes.min(excess);
        excess -= taken;
        if bytes - taken > 0 {
            unit.bytes_by_interface.insert(id, bytes - taken);
        } else {
            unit.bytes_by_interface.remove(&id);
        }
    }
    unit.bytes_downloaded = position;
    removed
}

/// Re-derives verified byte counts from the units.
pub fn recompute_aggregates(state: &mut DownloadState, units: &[UnitState]) {
    let mut total = 0;
    let mut by_network: HashMap<&str, u64> = HashMap::new();
    for unit in units {
        total += unit.bytes_downloaded;
        for (id, bytes) in &unit.bytes_by_interface {
            *by_network.entry(id).or_insert(0) += bytes;
        }
    }
    state.bytes_downloaded = total;
    for network in &mut state.networks {
        network.bytes_downloaded = by_network.get(network.id.as_str()).copied().unwrap_or(0);
    }
}

/// How units are checkpointed: per network, the bytes it delivered of each unit, by index.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SavedUnits {
    pub progress: BTreeMap<String, Vec<u64>>,
    /// Every unit is complete: needed where a unit's length is unknown.
    pub complete: bool,
}

pub fn save_units(units: &[UnitState]) -> SavedUnits {
    let mut progress: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for unit in units {
        for (id, bytes) in &unit.bytes_by_interface {
            progress.entry(id.clone()).or_insert_with(|| vec![0; units.len()])[unit.index] = *bytes;
        }
    }
    SavedUnits { progress, complete: units.iter().all(|unit| unit.status == UnitStatus::Completed) }
}

/// Lays saved progress over freshly planned units. Read back from disk, it's checked like any
/// input: progress that doesn't fit the plan is dropped and the units start over.
fn restore(mut units: Vec<UnitState>, saved: &SavedUnits) -> Vec<UnitState> {
    let fresh = units.clone();
    for (id, column) in &saved.progress {
        if column.len() != units.len() {
            return fresh;
        }
        for (index, bytes) in column.iter().enumerate() {
            if *bytes > 0 {
                units[index].bytes_by_interface.insert(id.clone(), *bytes);
                units[index].bytes_downloaded += bytes;
            }
        }
    }
    for unit in &mut units {
        let length = unit.length();
        if length.is_some_and(|length| unit.bytes_downloaded > length) {
            return fresh;
        }
        if saved.complete || Some(unit.bytes_downloaded) == length {
            unit.status = UnitStatus::Completed;
        }
    }
    units
}

pub fn restore_blocks(total_bytes: u64, block_size: u64, total_blocks: usize, saved: &SavedUnits) -> Option<Vec<UnitState>> {
    let expected = if total_bytes > 0 { total_bytes.div_ceil(block_size.max(1)) as usize } else { 1 };
    (expected == total_blocks).then(|| restore(plan_blocks(total_bytes, block_size), saved))
}

pub fn restore_pieces(total_bytes: u64, piece_length: u64, total_pieces: usize, saved: &SavedUnits) -> Option<Vec<UnitState>> {
    if piece_length == 0 || total_bytes.div_ceil(piece_length) as usize != total_pieces {
        return None;
    }
    Some(restore(plan_pieces(total_bytes, piece_length), saved))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advance_and_retract_keep_attribution_summed() {
        let mut unit = plan_blocks(100, 100).remove(0);
        assert_eq!(advance(&mut unit, "a", 40), 40);
        assert_eq!(advance(&mut unit, "b", 70), 30);
        assert_eq!(advance(&mut unit, "a", 50), 0);
        assert_eq!(retract(&mut unit, 50, Some("b")), 20);
        assert_eq!(unit.bytes_by_interface["a"], 40);
        assert_eq!(unit.bytes_by_interface["b"], 10);
        assert_eq!(advance(&mut unit, "a", 1000), 50);
    }

    #[test]
    fn restores_what_it_saved() {
        let mut units = plan_blocks(300, 100);
        advance(&mut units[0], "a", 100);
        units[0].status = UnitStatus::Completed;
        advance(&mut units[1], "b", 30);
        let saved = save_units(&units);
        let restored = restore_blocks(300, 100, 3, &saved).unwrap();
        assert_eq!(restored[0].status, UnitStatus::Completed);
        assert_eq!(restored[1].bytes_downloaded, 30);
        assert!(restore_blocks(300, 100, 4, &saved).is_none());
    }
}
