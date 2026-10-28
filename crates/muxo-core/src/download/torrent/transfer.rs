//! Fetches a torrent download through librqbit. The engine runs it as it runs an HTTP download:
//! reconcile starts or stops it, and each tick reads what librqbit has done into the download's
//! peers, pieces and speeds.
//!
//! librqbit opens its own peer connections and can't pin each to a network, so a torrent's
//! traffic is credited to the first network the download has on.

use crate::core::Core;
use crate::download::runtime::{Download, Transfer};
use crate::download::torrent::storage::{PieceObserver, MuxoStorageFactory};
use crate::download::units::recompute_aggregates;
use crate::types::{DownloadStatus, NetworkStatus, TorrentInfo, TorrentPeerState, UnitStatus};
use bytes::Bytes;
use librqbit::limits::LimitsConfig;
use librqbit::{AddTorrent, AddTorrentOptions, ManagedTorrent, Session};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default)]
struct Engine {
    handle: Option<Arc<ManagedTorrent>>,
    session: Option<Arc<Session>>,
    /// Adding or removing is under way.
    busy: bool,
    /// The last reconcile's wish: whether it should be running.
    wanted: bool,
}

pub struct TorrentTransfer {
    pub torrent_bytes: Bytes,
    pub info: TorrentInfo,
    /// Where each of its files is written, in the torrent's order.
    pub targets: Arc<Vec<PathBuf>>,
    /// The torrent's files not chosen, as the torrent names them.
    pub unwanted: Vec<String>,
    pub observer: Arc<PieceObserver>,
    engine: Arc<Mutex<Engine>>,
    fetched: u64,
    uploaded: u64,
    peer_ids: HashMap<String, u32>,
    peer_fetched: HashMap<String, u64>,
}

/// Where a torrent's file lands: under its claimed folder (the torrent's own folder first, so
/// that part is swapped for the claimed name), or as its claimed file.
pub fn targets_for(destination: &Path, info: &TorrentInfo, folder: bool) -> Vec<PathBuf> {
    info.files
        .iter()
        .map(|file| {
            if folder {
                let mut path = destination.to_path_buf();
                path.extend(file.path.split(std::path::MAIN_SEPARATOR).skip(1));
                path
            } else {
                destination.to_path_buf()
            }
        })
        .collect()
}

impl TorrentTransfer {
    pub fn new(torrent_bytes: Bytes, info: TorrentInfo, destination: &Path, folder: bool, unwanted: Vec<String>) -> Self {
        let pieces = info.files.iter().map(|file| file.length).sum::<u64>().div_ceil(info.piece_length.max(1)) as usize;
        TorrentTransfer {
            targets: Arc::new(targets_for(destination, &info, folder)),
            observer: PieceObserver::new(pieces, info.piece_length),
            torrent_bytes,
            info,
            unwanted,
            engine: Arc::default(),
            fetched: 0,
            uploaded: 0,
            peer_ids: HashMap::new(),
            peer_fetched: HashMap::new(),
        }
    }

    /// Stops the torrent's connections. Its files stay; the next run checks them and goes on.
    pub fn abort(&mut self) {
        let engine = Arc::clone(&self.engine);
        let mut state = engine.lock();
        state.wanted = false;
        let (Some(handle), Some(session)) = (state.handle.take(), state.session.clone()) else { return };
        drop(state);
        tokio::spawn(async move {
            let _ = session.delete(handle.id().into(), false).await;
        });
    }

    pub fn reset(&mut self) {
        self.fetched = 0;
        self.uploaded = 0;
        self.peer_fetched.clear();
        self.observer.reset_provisional();
    }

    pub fn running(&self) -> bool {
        let engine = self.engine.lock();
        engine.handle.is_some() || engine.busy
    }

    fn handle(&self) -> Option<Arc<ManagedTorrent>> {
        self.engine.lock().handle.clone()
    }
}

macro_rules! torrent {
    ($dl:expr) => {
        match &mut $dl.transfer {
            Transfer::Torrent(transfer) => transfer,
            Transfer::Http(_) => unreachable!("a torrent download"),
        }
    };
}

impl Core {
    /// Runs the torrent while it has a network on, and stops it when none is.
    pub(crate) fn torrent_reconcile(self: &Arc<Self>, dl: &mut Download) {
        let on = dl.state.networks.iter().find(|n| n.status == NetworkStatus::On).map(|n| n.id.clone());
        let want = dl.state.status == DownloadStatus::Downloading && on.is_some();
        let only_files: Option<Vec<usize>> = dl.request.selected_files.clone();
        let rate = on.as_deref().and_then(|id| self.limits.rate_for(id));
        let transfer = torrent!(dl);
        let mut engine = transfer.engine.lock();
        let was = engine.wanted;
        engine.wanted = want;
        if !want {
            if was {
                if let (Some(handle), Some(session)) = (engine.handle.take(), engine.session.clone()) {
                    tokio::spawn(async move {
                        let _ = session.delete(handle.id().into(), false).await;
                    });
                }
            }
            return;
        }
        if engine.handle.is_some() || engine.busy {
            return;
        }
        engine.busy = true;
        drop(engine);
        let factory = MuxoStorageFactory { files: Arc::clone(&transfer.targets), observer: Arc::clone(&transfer.observer) };
        let bytes = transfer.torrent_bytes.clone();
        let state = Arc::clone(&transfer.engine);
        let output = dl.partial.parent().map(Path::to_path_buf).unwrap_or_default();
        let core = Arc::clone(self);
        let id = dl.state.id.clone();
        tokio::spawn(async move {
            let added = async {
                let session = core.torrents.session().await?;
                state.lock().session = Some(Arc::clone(&session));
                let response = session
                    .add_torrent(
                        AddTorrent::from_bytes(bytes),
                        Some(AddTorrentOptions {
                            only_files,
                            overwrite: true,
                            output_folder: Some(output.to_string_lossy().into_owned()),
                            storage_factory: Some(factory.boxed_factory()),
                            ratelimits: LimitsConfig {
                                download_bps: rate.and_then(|rate| NonZeroU32::new(rate.min(u32::MAX as u64) as u32)),
                                upload_bps: None,
                            },
                            ..Default::default()
                        }),
                    )
                    .await?;
                response.into_handle().ok_or_else(|| anyhow::anyhow!("Invalid torrent metadata"))
            }
            .await;
            // Still wanted: kept. Not any more (paused while it was added): let go again.
            let unwanted = {
                let mut engine = state.lock();
                engine.busy = false;
                match added {
                    Ok(handle) if engine.wanted => {
                        engine.handle = Some(handle);
                        Ok(None)
                    }
                    Ok(handle) => Ok(engine.session.clone().map(|session| (session, handle))),
                    Err(error) => Err(error),
                }
            };
            match unwanted {
                Ok(None) => {}
                Ok(Some((session, handle))) => {
                    let _ = session.delete(handle.id().into(), false).await;
                }
                Err(error) => {
                    let mut engine = core.engine.lock();
                    if let Some(dl) = engine.downloads.get_mut(&id) {
                        core.fail_download(dl, &error.to_string(), false);
                    }
                }
            }
        });
    }

    /// Reads what librqbit has done into the download.
    pub(crate) fn torrent_tick(self: &Arc<Self>, dl: &mut Download, _now: f64) {
        self.reconcile_download(dl);
        if dl.state.status != DownloadStatus::Downloading {
            return;
        }
        let network = dl.state.networks.iter().find(|n| n.status == NetworkStatus::On).map(|n| n.id.clone());
        let Some(handle) = torrent!(dl).handle() else { return };
        let stats = handle.stats();
        if let Some(error) = stats.error.clone() {
            self.fail_download(dl, &error, false);
            return;
        }
        let Some(network) = network else { return };

        // Bytes in and out, counted against the network's data and shown as its speed.
        let fetched = stats.live.as_ref().map_or(0, |live| live.snapshot.fetched_bytes);
        let transfer = torrent!(dl);
        let fetched_delta = fetched.saturating_sub(transfer.fetched);
        transfer.fetched = fetched;
        let uploaded_delta = stats.uploaded_bytes.saturating_sub(transfer.uploaded);
        transfer.uploaded = stats.uploaded_bytes;
        if fetched_delta > 0 {
            self.limits.take(&network, fetched_delta);
        }

        // Its peers, as it sees them now.
        let mut peers = Vec::new();
        if let Some(live) = handle.live() {
            let snapshot = live.per_peer_stats_snapshot(Default::default());
            let mut addresses: Vec<_> = snapshot.peers.into_iter().filter(|(_, peer)| peer.state == "live").collect();
            addresses.sort_by(|a, b| a.0.cmp(&b.0));
            for (address, peer) in addresses {
                let next = transfer.peer_ids.len() as u32;
                let id = *transfer.peer_ids.entry(address.clone()).or_insert(next);
                let before = transfer.peer_fetched.insert(address, peer.counters.fetched_bytes).unwrap_or(peer.counters.fetched_bytes);
                let delta = peer.counters.fetched_bytes.saturating_sub(before);
                dl.meters.connections.entry(id).or_default().add(delta);
                peers.push(TorrentPeerState {
                    id,
                    interface_id: network.clone(),
                    status: if delta > 0 { "receiving".into() } else { "connected".into() },
                    client: None,
                    bytes_downloaded: peer.counters.fetched_bytes,
                    speed_bytes_per_sec: 0.0,
                    bytes_uploaded: 0,
                    upload_speed_bytes_per_sec: 0.0,
                });
            }
        }
        dl.meters.networks.entry(network.clone()).or_default().add(fetched_delta);
        dl.upload_meters.networks.entry(network.clone()).or_default().add(uploaded_delta);
        if let Some(detail) = dl.state.torrent_mut() {
            // Peers keep the speeds the meters last read until the next read replaces them.
            for peer in &mut peers {
                if let Some(old) = detail.peers.iter().find(|old| old.id == peer.id) {
                    peer.speed_bytes_per_sec = old.speed_bytes_per_sec;
                }
            }
            detail.peak_peers = detail.peak_peers.max(peers.len());
            detail.peers = peers;
            detail.bytes_uploaded += uploaded_delta;
        }
        if let Some(net) = dl.network_mut(&network) {
            net.bytes_uploaded = Some(net.bytes_uploaded.unwrap_or(0) + uploaded_delta);
        }

        // Its pieces: verified ones are in, and bytes written to the rest show as provisional.
        let observer = Arc::clone(&torrent!(dl).observer);
        let mut changed = false;
        observer.read(|observed| {
            for unit in &mut dl.units {
                if unit.status == UnitStatus::Skipped || unit.status == UnitStatus::Completed {
                    continue;
                }
                if observed.completed.get(unit.index).copied().unwrap_or(false) {
                    let length = unit.length().unwrap_or(0);
                    unit.status = UnitStatus::Completed;
                    unit.bytes_by_interface.clear();
                    unit.bytes_by_interface.insert(network.clone(), length);
                    unit.bytes_downloaded = length;
                    unit.interface_id = Some(network.clone());
                    unit.provisional_bytes = Some(0);
                    changed = true;
                    continue;
                }
                let provisional = observed.provisional.get(unit.index).copied().unwrap_or(0);
                if unit.provisional_bytes != Some(provisional) {
                    unit.provisional_bytes = Some(provisional);
                    unit.status = if provisional > 0 { UnitStatus::Downloading } else { UnitStatus::Pending };
                    unit.interface_id = (provisional > 0).then(|| network.clone());
                    changed = true;
                }
            }
        });
        if changed {
            recompute_aggregates(&mut dl.state, &dl.units);
        }
        self.schedule_update(dl);
    }

    /// The choice of files changed while it may be running.
    pub(crate) fn torrent_files_chosen(&self, dl: &mut Download) {
        let selected: std::collections::HashSet<usize> = match &dl.request.selected_files {
            Some(selected) => selected.iter().copied().collect(),
            None => (0..dl.state.torrent().map_or(0, |t| t.files.total)).collect(),
        };
        let engine = torrent!(dl).engine.lock();
        let (Some(handle), Some(session)) = (engine.handle.clone(), engine.session.clone()) else { return };
        drop(engine);
        tokio::spawn(async move {
            let _ = session.update_only_files(&handle, &selected).await;
        });
    }
}

/// Already in place: it drops the files not chosen (the pieces at a chosen file's edges wrote
/// part of them), and any folder that leaves empty.
pub async fn remove_unwanted(destination: &Path, unwanted: &[String], folder: bool) {
    if unwanted.is_empty() || !folder {
        return;
    }
    for path in unwanted {
        let mut target = destination.to_path_buf();
        target.extend(path.split(std::path::MAIN_SEPARATOR).skip(1));
        let _ = tokio::fs::remove_file(&target).await;
    }
    let mut folders = Vec::new();
    let mut stack = vec![destination.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(mut entries) = tokio::fs::read_dir(&dir).await else { continue };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_type().await.is_ok_and(|t| t.is_dir()) {
                folders.push(entry.path());
                stack.push(entry.path());
            }
        }
    }
    folders.sort_by_key(|folder| std::cmp::Reverse(folder.as_os_str().len()));
    for folder in folders {
        let _ = tokio::fs::remove_dir(&folder).await;
    }
}
