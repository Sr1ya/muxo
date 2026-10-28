//! A download as the engine keeps it while it's listed, and what it does that doesn't depend on
//! how its bytes are fetched: its networks, telling the window, checkpointing, failing.

use crate::core::Core;
use crate::download::http::HttpTransfer;
use crate::download::meters::{clear_speeds, Meters};
use crate::download::torrent::transfer::TorrentTransfer;
use crate::errors::describe_error;
use crate::types::{
    DownloadNetwork, DownloadState, DownloadStatus, DownloadUpdate, NetworkStatus, StartDownloadRequest, TransferKind,
    UnitState, UnitStatus,
};
use futures::future::{BoxFuture, Shared};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const UI_UPDATE: Duration = Duration::from_millis(200);
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(15);

pub enum Transfer {
    Http(HttpTransfer),
    Torrent(TorrentTransfer),
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileIdentity {
    pub dev: u64,
    pub ino: u64,
}

impl FileIdentity {
    pub fn of(metadata: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(FileIdentity { dev: metadata.dev(), ino: metadata.ino() })
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            None
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct SentUnit {
    status: UnitStatus,
    interface_id: Option<String>,
    bytes_downloaded: u64,
    provisional_bytes: Option<u64>,
}

pub type Done = Shared<BoxFuture<'static, ()>>;

pub struct Download {
    pub state: DownloadState,
    pub units: Vec<UnitState>,
    pub request: StartDownloadRequest,
    /// The `.muxo` staging file of an HTTP download; a torrent's destination itself.
    pub partial: PathBuf,
    pub publication_path: Option<String>,
    pub publication_identity: Option<FileIdentity>,
    /// Cancelled once the current run is over: stopped, or every unit in.
    pub stop: CancellationToken,
    pub run: Option<Done>,
    pub cancelling: Option<Done>,
    pub schedule_stopping: bool,
    pub publishing: bool,
    pub push_scheduled: bool,
    pub persistence_armed: bool,
    pub checkpoint_queued: bool,
    pub persist_lock: Arc<tokio::sync::Mutex<()>>,
    pub removed: bool,
    pub sent_updates: u64,
    sent_units: Vec<Option<SentUnit>>,
    /// The network most recently switched off: what a resume switches back on if none is on.
    pub last_switched_off: Option<String>,
    /// Paused by switching off its last network: switching one back on resumes it.
    pub paused_for_no_network: bool,
    pub speed_sampled_at: f64,
    /// The byte count, read once a second while downloading (see sample_peak).
    pub peak_readings: VecDeque<(f64, u64)>,
    pub file_choices: u64,
    pub meters: Meters,
    pub upload_meters: Meters,
    pub transfer: Transfer,
}

impl Download {
    pub fn new(state: DownloadState, units: Vec<UnitState>, request: StartDownloadRequest, partial: PathBuf, transfer: Transfer) -> Self {
        Download {
            state,
            units,
            request,
            partial,
            publication_path: None,
            publication_identity: None,
            stop: CancellationToken::new(),
            run: None,
            cancelling: None,
            schedule_stopping: false,
            publishing: false,
            push_scheduled: false,
            persistence_armed: false,
            checkpoint_queued: false,
            persist_lock: Arc::default(),
            removed: false,
            sent_updates: 0,
            sent_units: Vec::new(),
            last_switched_off: None,
            paused_for_no_network: false,
            speed_sampled_at: 0.0,
            peak_readings: VecDeque::new(),
            file_choices: 0,
            meters: Meters::default(),
            upload_meters: Meters::default(),
            transfer,
        }
    }

    pub fn kind(&self) -> TransferKind {
        self.state.kind()
    }

    pub fn all_done(&self) -> bool {
        self.units.iter().all(UnitState::is_done)
    }

    /// The units that changed since the window was last sent them.
    fn changed_units(&mut self) -> Vec<UnitState> {
        self.sent_units.resize(self.units.len(), None);
        let mut changed = Vec::new();
        for unit in &self.units {
            let now = SentUnit {
                status: unit.status,
                interface_id: unit.interface_id.clone(),
                bytes_downloaded: unit.bytes_downloaded,
                provisional_bytes: unit.provisional_bytes,
            };
            if self.sent_units[unit.index].as_ref() == Some(&now) {
                continue;
            }
            self.sent_units[unit.index] = Some(now);
            changed.push(unit.clone());
        }
        changed
    }

    pub fn update(&mut self, units: Vec<UnitState>) -> DownloadUpdate {
        let torrent = self.kind() == TransferKind::Torrent;
        DownloadUpdate {
            seq: self.sent_updates,
            state: self.state.clone(),
            blocks: (!torrent).then(|| units.clone()),
            pieces: torrent.then_some(units),
        }
    }

    /// Everything, for a window that is just starting to listen.
    pub fn snapshot(&mut self) -> DownloadUpdate {
        let units = self.units.clone();
        self.update(units)
    }

    pub fn network_mut(&mut self, id: &str) -> Option<&mut DownloadNetwork> {
        self.state.networks.iter_mut().find(|network| network.id == id)
    }
}

impl Core {
    /// Tells the window about `dl` within UI_UPDATE, once however often it's asked.
    pub(crate) fn schedule_update(self: &Arc<Self>, dl: &mut Download) {
        if dl.push_scheduled {
            return;
        }
        dl.push_scheduled = true;
        let core = Arc::clone(self);
        let id = dl.state.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(UI_UPDATE).await;
            let mut engine = core.engine.lock();
            if let Some(dl) = engine.downloads.get_mut(&id) {
                dl.push_scheduled = false;
                core.push_update(dl, true);
            }
        });
    }

    pub(crate) fn push_update(self: &Arc<Self>, dl: &mut Download, persist: bool) {
        if persist {
            self.schedule_persistence(dl);
        }
        if matches!(dl.state.status, DownloadStatus::Paused | DownloadStatus::Cancelled) {
            clear_speeds(&mut dl.state);
        }
        dl.sent_updates += 1;
        let changed = dl.changed_units();
        let update = dl.update(changed);
        self.host.download_updated(&update);
    }

    fn schedule_persistence(self: &Arc<Self>, dl: &mut Download) {
        if self.schedule.lock().suspending || dl.removed || dl.persistence_armed || dl.checkpoint_queued {
            return;
        }
        dl.persistence_armed = true;
        let core = Arc::clone(self);
        let id = dl.state.id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(CHECKPOINT_INTERVAL).await;
            {
                let mut engine = core.engine.lock();
                let Some(dl) = engine.downloads.get_mut(&id) else { return };
                if !dl.persistence_armed {
                    return;
                }
                dl.persistence_armed = false;
                dl.checkpoint_queued = true;
            }
            let _ = core.persist_now(&id, true).await;
        });
    }

    pub(crate) fn notify(&self, title: &str, body: &str) {
        self.host.notify(title, body);
    }

    pub(crate) fn stop_run(&self, dl: &mut Download) {
        dl.stop.cancel();
        match &mut dl.transfer {
            Transfer::Http(transfer) => transfer.abort(),
            Transfer::Torrent(transfer) => transfer.abort(),
        }
    }

    pub(crate) fn fail_download(self: &Arc<Self>, dl: &mut Download, message: &str, discard: bool) {
        if dl.state.status != DownloadStatus::Downloading {
            return;
        }
        dl.state.status = DownloadStatus::Error;
        dl.state.error = Some(message.to_owned());
        dl.state.resumable = Some(!discard);
        self.notify("Download failed", &format!("{}: {}", dl.state.file_name, describe_error(message)));
        self.stop_run(dl);
    }

    pub(crate) fn fail_network(self: &Arc<Self>, dl: &mut Download, network_id: &str, message: &str) {
        if let Some(network) = dl.network_mut(network_id) {
            network.status = NetworkStatus::Failed;
            network.error = Some(message.to_owned());
        }
        self.reconcile_download(dl);
    }

    /// Brings the download's networks up to date with the computer's, and each one's status with
    /// what came of it; then, while it runs, its connections.
    pub(crate) fn reconcile_download(self: &Arc<Self>, dl: &mut Download) {
        let present = self.networks.current();
        if let Some(present) = &present {
            let streams: Vec<String> = match (&dl.state.http(), &dl.state.torrent()) {
                (Some(http), _) => http.streams.iter().map(|s| s.interface_id.clone()).collect(),
                (_, Some(torrent)) => torrent.peers.iter().map(|p| p.interface_id.clone()).collect(),
                _ => Vec::new(),
            };
            // A network that's gone and did nothing for this download leaves its list.
            dl.state.networks.retain(|network| {
                network.enabled
                    || network.bytes_downloaded > 0
                    || network.bytes_uploaded.unwrap_or(0) > 0
                    || present.iter().any(|iface| iface.id == network.id)
                    || streams.contains(&network.id)
            });
            let kind = dl.kind();
            for iface in present {
                if !dl.state.networks.iter().any(|network| network.id == iface.id) {
                    // One that turns up mid-download starts off.
                    dl.state.networks.push(DownloadNetwork::new(iface, false, kind));
                }
            }
        }
        let known = present.is_some();
        for network in &mut dl.state.networks {
            let iface = self.networks.find(&network.id);
            if let Some(iface) = &iface {
                network.label = iface.display_name.clone();
                network.kind = iface.kind;
            }
            let status = if !network.enabled {
                NetworkStatus::Off
            } else if iface.is_none() && known {
                NetworkStatus::Offline
            } else if self.limits.limit_reached(&network.id) {
                NetworkStatus::Limit
            } else if matches!(network.status, NetworkStatus::Off | NetworkStatus::Offline | NetworkStatus::Limit) {
                NetworkStatus::On
            } else {
                network.status
            };
            if status != network.status {
                network.status = status;
                network.error = None;
            }
        }
        if dl.state.status != DownloadStatus::Downloading || dl.stop.is_cancelled() {
            return;
        }
        if !self.schedule_allows() {
            let core = Arc::clone(self);
            let id = dl.state.id.clone();
            tokio::spawn(async move { core.pause_inner(&id, true).await });
            return;
        }
        let enabled: Vec<&DownloadNetwork> = dl.state.networks.iter().filter(|n| n.enabled).collect();
        if !enabled.is_empty() && enabled.iter().all(|n| n.status == NetworkStatus::Failed) {
            let message = enabled[0].error.clone().unwrap_or_else(|| "No network could reach the server".into());
            self.fail_download(dl, &message, false);
            return;
        }
        match &dl.transfer {
            Transfer::Http(_) => self.http_reconcile(dl),
            Transfer::Torrent(_) => self.torrent_reconcile(dl),
        }
    }
}
