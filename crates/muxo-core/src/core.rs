//! The download manager: every download the window lists, from start to history. It runs what a
//! download does whatever fetches it (its networks, the queue, pause and resume, saving it,
//! telling the window) and hands the fetching to its transfer (HTTP streams or a torrent).

use crate::download::browser_context::browser_context_from;
use crate::download::download_file::DownloadFile;
use crate::download::file_version::normalize_etag;
use crate::download::history::History;
use crate::download::http::{compare_samples, picked_streams, splittable, HttpTransfer};
use crate::download::meters::{clear_speeds, update_speeds, update_time_left};
use crate::download::paths::{claim_destination_path, ensure_directory, partial_path, reserve_destination_path};
use crate::download::plan::{plan_block_size, plan_blocks, plan_pieces};
use crate::download::probe::probe_url;
use crate::download::runtime::{Done, Download, FileIdentity, Transfer};
use crate::download::torrent::files::{chosen_files, finished_files, wanted_pieces};
use crate::download::torrent::metadata::TorrentProbe;
use crate::download::torrent::transfer::{remove_unwanted, TorrentTransfer};
use crate::download::units::{recompute_aggregates, restore_blocks, restore_pieces, save_units, SavedUnits};
use crate::errors::describe_error;
use crate::links::{Links, Offer};
use crate::network::interfaces::NetworkMonitor;
use crate::network::routes::{compatible_interfaces, resolve_within, target_host};
use crate::network::{Limits, StreamConnection};
use crate::schedule::schedule_window;
use crate::settings::Settings;
use crate::types::*;
use crate::util::{free_space, io_message, now_ms, path_exists};
use futures::FutureExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// What the engine asks of the app around it: the window, and the OS.
pub trait Host: Send + Sync {
    fn download_updated(&self, update: &DownloadUpdate);
    fn history_changed(&self);
    fn networks_changed(&self, networks: &[NetworkInterfaceInfo]);
    fn magnet_check_status(&self, status: &MagnetCheckStatus);
    /// pending_links() has a new one; bring the window forward.
    fn link_received(&self);
    fn notify(&self, title: &str, body: &str);
    /// Keep the computer from sleeping while a download runs.
    fn keep_awake(&self, _awake: bool) {}
    fn trash(&self, path: &Path) -> Result<(), String>;
    fn reveal(&self, path: &Path) -> bool;
    fn open(&self, path: &Path) -> bool;
    fn app_version(&self) -> String;
}

pub struct CoreConfig {
    pub data_dir: PathBuf,
    pub downloads_dir: PathBuf,
    pub home_dir: PathBuf,
}

pub struct Knobs {
    pub stall_timeout: Duration,
    pub connect_timeout: Duration,
}

pub(crate) struct ScheduleState {
    pub schedule: Option<DownloadSchedule>,
    pub loaded: bool,
    pub suspending: bool,
    pub downloads_at_once: u32,
}

#[derive(Default)]
pub(crate) struct Engine {
    pub downloads: HashMap<String, Download>,
    relinking: HashSet<String>,
    seen_addresses: HashMap<String, Vec<String>>,
    awake: bool,
}

pub struct Core {
    pub(crate) engine: Mutex<Engine>,
    pub(crate) schedule: Mutex<ScheduleState>,
    pub networks: Arc<NetworkMonitor>,
    pub limits: Arc<Limits>,
    pub settings: Settings,
    pub history: History,
    pub torrents: Arc<TorrentProbe>,
    pub links: Links,
    pub(crate) host: Arc<dyn Host>,
    pub(crate) knobs: Knobs,
    pub config: CoreConfig,
    initialized: tokio::sync::watch::Receiver<bool>,
}

const TICK: Duration = Duration::from_millis(500);
const PEAK_SECONDS: f64 = 5.0;
const MANIFEST_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    version: u32,
    saved_at: f64,
    partial_path: PathBuf,
    #[serde(default)]
    publication_path: Option<String>,
    #[serde(default)]
    publication_identity: Option<FileIdentity>,
    /// Without the browser's sign-in: cookies are never written to disk, so a download resumed
    /// after a restart goes without them.
    request: StartDownloadRequest,
    state: DownloadState,
    #[serde(flatten)]
    saved: SavedUnits,
}

fn format_gigabytes(bytes: u64) -> String {
    format!("{:.2} GB", bytes as f64 / 1024f64.powi(3))
}

/// The staging file is on the destination volume and becomes the final file by rename.
fn ensure_disk_space(dir: &Path, required: u64) -> anyhow::Result<()> {
    if required == 0 {
        return Ok(());
    }
    let Some(available) = free_space(dir) else { return Ok(()) };
    if available < required {
        anyhow::bail!(
            "Not enough disk space: this download needs {} but only {} is free",
            format_gigabytes(required),
            format_gigabytes(available)
        );
    }
    Ok(())
}

/// Sets each piece by whether a chosen file needs it: skipped when none does, waiting again when
/// one does. A piece that's in stays in. Returns the skipped pieces' bytes.
fn choose_pieces(pieces: &mut [UnitState], torrent: &TorrentInfo, chosen: Option<&BTreeSet<usize>>) -> u64 {
    let wanted = chosen.map(|chosen| wanted_pieces(&torrent.files, torrent.piece_length, chosen));
    let mut skipped = 0;
    for piece in pieces.iter_mut() {
        if piece.status == UnitStatus::Completed {
            continue;
        }
        let Some(length) = piece.length() else { continue };
        if wanted.as_ref().is_none_or(|wanted| wanted[piece.index]) {
            if piece.status == UnitStatus::Skipped {
                piece.status = UnitStatus::Pending;
            }
            continue;
        }
        piece.status = UnitStatus::Skipped;
        piece.provisional_bytes = Some(0);
        skipped += length;
    }
    skipped
}

fn choice_of(chosen: Option<&BTreeSet<usize>>, total: usize) -> TorrentFiles {
    let selected = chosen.map(|chosen| chosen.iter().copied().collect::<Vec<_>>());
    TorrentFiles { chosen: selected.as_ref().map_or(total, Vec::len), total, selected }
}

fn unchosen_paths(torrent: &TorrentInfo, chosen: Option<&BTreeSet<usize>>) -> Vec<String> {
    match chosen {
        Some(chosen) => torrent
            .files
            .iter()
            .enumerate()
            .filter(|(index, _)| !chosen.contains(index))
            .map(|(_, file)| file.path.clone())
            .collect(),
        None => Vec::new(),
    }
}

/// Adds each network's speed to the download's history, so every series lines up in time.
fn sample_speeds(state: &mut DownloadState) {
    let history = state.speed_history.get_or_insert_with(Default::default);
    let length = history.values().next().map_or(0, Vec::len);
    for network in &state.networks {
        history.entry(network.id.clone()).or_insert_with(|| vec![0.0; length]);
    }
    for (id, series) in history.iter_mut() {
        series.push(state.networks.iter().find(|n| n.id == *id).map_or(0.0, |n| n.speed_bytes_per_sec));
        if series.len() > SPEED_HISTORY_SECONDS {
            series.remove(0);
        }
    }
}

/// Raises the peak to the last PEAK_SECONDS of downloading, read from the bytes themselves, so
/// the peak can't read below the average.
fn sample_peak(dl: &mut Download, now: f64) {
    dl.peak_readings.push_back((now, dl.state.bytes_downloaded));
    while dl.peak_readings.len() > 1 && dl.peak_readings[1].0 <= now - PEAK_SECONDS * 1000.0 {
        dl.peak_readings.pop_front();
    }
    let (first_time, first_bytes) = dl.peak_readings[0];
    if now - first_time < PEAK_SECONDS * 1000.0 {
        return;
    }
    let held = dl.state.bytes_downloaded.saturating_sub(first_bytes) as f64 / ((now - first_time) / 1000.0);
    let downloading = (now - dl.state.started_at - dl.state.total_paused_ms.unwrap_or(0.0)) / 1000.0;
    let average = if downloading >= PEAK_SECONDS { dl.state.bytes_downloaded as f64 / downloading } else { 0.0 };
    dl.state.peak_speed_bytes_per_sec = Some(dl.state.peak_speed_bytes_per_sec.unwrap_or(0.0).max(held).max(average));
}

async fn wait(done: Option<Done>) {
    if let Some(done) = done {
        done.await;
    }
}

impl Core {
    pub async fn start(config: CoreConfig, host: Arc<dyn Host>) -> Arc<Core> {
        let limits = Limits::load(&config.data_dir).await;
        let networks = NetworkMonitor::start();
        let (ready, initialized) = tokio::sync::watch::channel(false);
        let core = Arc::new(Core {
            engine: Mutex::new(Engine::default()),
            schedule: Mutex::new(ScheduleState {
                schedule: None,
                loaded: false,
                suspending: false,
                downloads_at_once: DOWNLOADS_AT_ONCE_DEFAULT,
            }),
            networks,
            limits,
            settings: Settings::new(&config.data_dir),
            history: History::new(&config.data_dir),
            torrents: TorrentProbe::new(&config.data_dir),
            links: Links::default(),
            host,
            knobs: Knobs { stall_timeout: Duration::from_secs(20), connect_timeout: Duration::from_secs(10) },
            config,
            initialized,
        });

        let weak = Arc::downgrade(&core);
        core.networks.set_listener(Arc::new(move |list| {
            let Some(core) = weak.upgrade() else { return };
            core.host.networks_changed(list);
            tokio::spawn(async move { core.networks_changed() });
        }));
        let weak = Arc::downgrade(&core);
        core.limits.set_on_limit_reached(Arc::new(move || {
            if let Some(core) = weak.upgrade() {
                core.limits_changed();
            }
        }));
        let host = Arc::clone(&core.host);
        core.torrents.set_listener(Arc::new(move |status| host.magnet_check_status(&status)));

        core.restore_persisted_downloads().await;
        let settings = core.settings.load().await;
        core.configure_settings(&settings).await;
        let _ = ready.send(true);

        // The schedule is looked at every second while it's on: a window opening or closing.
        let weak = Arc::downgrade(&core);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(core) = weak.upgrade() else { return };
                let enabled = core.schedule.lock().schedule.as_ref().is_some_and(|s| s.enabled);
                if enabled {
                    core.refresh_schedule().await;
                }
            }
        });
        core
    }

    async fn initialized(&self) {
        let mut ready = self.initialized.clone();
        let _ = ready.wait_for(|ready| *ready).await;
    }

    fn downloads_root(&self) -> PathBuf {
        self.config.data_dir.join("downloads")
    }

    fn download_dir(&self, id: &str) -> PathBuf {
        self.downloads_root().join(id)
    }

    fn manifest_path(&self, id: &str) -> PathBuf {
        self.download_dir(id).join("manifest.json")
    }

    fn torrent_file_path(&self, id: &str) -> PathBuf {
        self.download_dir(id).join("metadata.torrent")
    }

    pub(crate) fn schedule_allows(&self) -> bool {
        let schedule = self.schedule.lock();
        schedule.loaded && schedule_window(schedule.schedule.as_ref(), now_ms()).allowed
    }

    fn schedule_enabled(&self) -> Option<bool> {
        self.schedule.lock().schedule.as_ref().is_some_and(|s| s.enabled).then_some(true)
    }

    // --- settings -----------------------------------------------------------------------------

    pub async fn initial_state(&self, dark_by_default: bool) -> InitialState {
        let settings = self.settings.load().await;
        let destination_dir = match &settings.destination_dir {
            // Capped: a folder on a dropped network share can take many seconds to answer.
            Some(dir) => tokio::time::timeout(Duration::from_millis(300), tokio::fs::metadata(dir))
                .await
                .ok()
                .and_then(Result::ok)
                .filter(|meta| meta.is_dir())
                .map(|_| dir.clone()),
            None => None,
        };
        InitialState {
            download_schedule: settings.download_schedule,
            home_dir: self.config.home_dir.to_string_lossy().into_owned(),
            downloads_dir: self.config.downloads_dir.to_string_lossy().into_owned(),
            theme_source: settings
                .theme_source
                .unwrap_or(if dark_by_default { ThemeSource::Dark } else { ThemeSource::Light }),
            network_preferences: settings.network_preferences.unwrap_or_default(),
            downloads_at_once: settings.downloads_at_once.unwrap_or(DOWNLOADS_AT_ONCE_DEFAULT),
            speed_limit: settings.speed_limit,
            slow_mode: settings.slow_mode.unwrap_or(false),
            slow_mode_speed: settings.slow_mode_speed.unwrap_or(DEFAULT_SLOW_MODE_SPEED),
            speed_unit: settings.speed_unit.unwrap_or(SpeedUnit::Bytes),
            destination_dir,
        }
    }

    pub async fn update_settings(self: &Arc<Self>, patch: Value) -> anyhow::Result<()> {
        self.settings.save(patch).await?;
        self.initialized().await;
        // Read back rather than taken from the patch: what was saved is what passed the checks.
        let settings = self.settings.load().await;
        self.configure_settings(&settings).await;
        Ok(())
    }

    /// Takes up what the user set: how many run at once, the schedule, and the limits.
    async fn configure_settings(self: &Arc<Self>, settings: &AppSettings) {
        {
            let mut schedule = self.schedule.lock();
            schedule.schedule = settings.download_schedule.clone();
            schedule.loaded = true;
            schedule.downloads_at_once = settings.downloads_at_once.unwrap_or(DOWNLOADS_AT_ONCE_DEFAULT);
        }
        let scheduled = self.schedule_enabled();
        let mut changed = Vec::new();
        {
            let mut engine = self.engine.lock();
            for dl in engine.downloads.values_mut() {
                if matches!(dl.state.status, DownloadStatus::Queued | DownloadStatus::Downloading)
                    && dl.state.scheduled != scheduled
                {
                    dl.state.scheduled = scheduled;
                    changed.push(dl.state.id.clone());
                    self.push_update(dl, true);
                }
            }
        }
        self.limits.configure(settings);
        self.refresh_schedule().await;
        self.limits_changed();
        for id in changed {
            let _ = self.persist_now(&id, false).await;
        }
    }

    /// A network ran out of data, or a limit changed: each download's networks are looked at
    /// again, so one at its limit stops being used, and one no longer at it is used again.
    fn limits_changed(self: &Arc<Self>) {
        let mut engine = self.engine.lock();
        for dl in engine.downloads.values_mut() {
            if matches!(dl.state.status, DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued) {
                self.reconcile_download(dl);
                self.schedule_update(dl);
            }
        }
    }

    pub async fn reset_network_usage(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        self.limits.reset_usage(id).await?;
        self.limits_changed();
        Ok(())
    }

    /// Stops transfers before the queue can start anything else, while the schedule disallows.
    async fn refresh_schedule(self: &Arc<Self>) {
        if self.schedule.lock().suspending {
            return;
        }
        if !self.schedule_allows() {
            let ids: Vec<String> = self
                .engine
                .lock()
                .downloads
                .values()
                .filter(|dl| dl.state.status == DownloadStatus::Downloading || dl.schedule_stopping)
                .map(|dl| dl.state.id.clone())
                .collect();
            futures::future::join_all(ids.iter().map(|id| self.pause_inner(id, true))).await;
        } else {
            let mut engine = self.engine.lock();
            self.pump(&mut engine);
        }
    }

    // --- the queue ----------------------------------------------------------------------------

    /// Running Auto downloads per network: how many share its streams (see concurrency.rs).
    fn update_shares(engine: &mut Engine) {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for dl in engine.downloads.values() {
            if dl.kind() == TransferKind::Http
                && dl.state.status == DownloadStatus::Downloading
                && splittable(&dl.request)
                && picked_streams(&dl.request).is_none()
            {
                for network in dl.state.networks.iter().filter(|n| n.status == NetworkStatus::On) {
                    *counts.entry(network.id.clone()).or_insert(0) += 1;
                }
            }
        }
        for dl in engine.downloads.values_mut() {
            if let Transfer::Http(transfer) = &dl.transfer {
                *transfer.shares.lock() = counts.clone();
            }
        }
    }

    fn running_count(engine: &Engine) -> usize {
        engine.downloads.values().filter(|dl| dl.state.status == DownloadStatus::Downloading).count()
    }

    fn keep_awake(&self, engine: &mut Engine) {
        let running = Self::running_count(engine) > 0;
        if running != engine.awake {
            engine.awake = running;
            self.host.keep_awake(running);
        }
    }

    /// Starts queued downloads, first queued first, while fewer than downloads-at-once run.
    fn pump(self: &Arc<Self>, engine: &mut Engine) {
        let (suspending, at_once) = {
            let schedule = self.schedule.lock();
            (schedule.suspending, schedule.downloads_at_once as usize)
        };
        if suspending || !self.schedule_allows() {
            self.keep_awake(engine);
            return;
        }
        let mut queued: Vec<(f64, String)> = engine
            .downloads
            .values()
            .filter(|dl| dl.state.status == DownloadStatus::Queued && !dl.schedule_stopping)
            .map(|dl| (dl.state.queued_at.unwrap_or(0.0), dl.state.id.clone()))
            .collect();
        queued.sort_by(|a, b| a.0.total_cmp(&b.0));
        for (_, id) in queued {
            if Self::running_count(engine) >= at_once {
                break;
            }
            self.begin(engine, &id);
        }
        self.keep_awake(engine);
    }

    /// Puts a download in the queue: at its end, or at its front when the user asked for it.
    fn enqueue(&self, engine: &mut Engine, id: &str, front: bool) {
        let places: Vec<f64> = engine
            .downloads
            .values()
            .filter(|dl| dl.state.id != id && dl.state.status == DownloadStatus::Queued)
            .map(|dl| dl.state.queued_at.unwrap_or(0.0))
            .collect();
        let scheduled = self.schedule_enabled();
        let Some(dl) = engine.downloads.get_mut(id) else { return };
        let now = now_ms();
        dl.state.status = DownloadStatus::Queued;
        dl.state.scheduled = scheduled;
        dl.state.paused_at.get_or_insert(now);
        dl.state.queued_at = Some(if front {
            places.iter().copied().fold(now, f64::min) - 1.0
        } else {
            places.iter().map(|place| place + 1.0).fold(now, f64::max)
        });
    }

    fn begin(self: &Arc<Self>, engine: &mut Engine, id: &str) {
        if self.schedule.lock().suspending || !self.schedule_allows() {
            self.enqueue(engine, id, false);
            if let Some(dl) = engine.downloads.get_mut(id) {
                self.push_update(dl, true);
            }
            let core = Arc::clone(self);
            let id = id.to_owned();
            tokio::spawn(async move { core.persist_now(&id, false).await });
            return;
        }
        let scheduled = self.schedule_enabled();
        let Some(dl) = engine.downloads.get_mut(id) else { return };
        dl.state.scheduled = scheduled;
        dl.state.status = DownloadStatus::Downloading;
        dl.state.queued_at = None;
        dl.state.error = None;
        dl.state.resumable = None;
        dl.paused_for_no_network = false;
        // None on: the one switched off last, if it's there, or the first that is.
        if !dl.state.networks.iter().any(|n| n.enabled) {
            let present = |id: &str| self.networks.find(id).is_some();
            let last = dl.last_switched_off.clone();
            let index = dl
                .state
                .networks
                .iter()
                .position(|n| Some(&n.id) == last.as_ref() && present(&n.id))
                .or_else(|| dl.state.networks.iter().position(|n| present(&n.id)))
                .or_else(|| dl.state.networks.iter().position(|n| Some(&n.id) == last.as_ref()))
                .or((!dl.state.networks.is_empty()).then_some(0));
            if let Some(index) = index {
                dl.state.networks[index].enabled = true;
            }
        }
        for network in &mut dl.state.networks {
            if matches!(network.status, NetworkStatus::Failed | NetworkStatus::Unreachable) {
                network.status = NetworkStatus::On;
                network.error = None;
            }
        }
        if let Some(paused_at) = dl.state.paused_at.take() {
            dl.state.total_paused_ms = Some(dl.state.total_paused_ms.unwrap_or(0.0) + now_ms() - paused_at);
        }
        for unit in &mut dl.units {
            if unit.status == UnitStatus::Downloading {
                unit.status = UnitStatus::Pending;
            }
            if unit.provisional_bytes.is_some() {
                unit.provisional_bytes = Some(0);
            }
        }
        match &mut dl.state.detail {
            Detail::Http(detail) => {
                for stream in &mut detail.streams {
                    if stream.status != StreamStatus::Completed {
                        stream.status = StreamStatus::Pending;
                    }
                    stream.speed_bytes_per_sec = 0.0;
                }
            }
            Detail::Torrent(detail) => detail.peers.clear(),
        }
        match &mut dl.transfer {
            Transfer::Http(transfer) => transfer.reset(),
            Transfer::Torrent(transfer) => transfer.reset(),
        }
        self.push_update(dl, true);
        let core = Arc::clone(self);
        let run_id = id.to_owned();
        let run = async move {
            core.run(&run_id).await;
            let mut engine = core.engine.lock();
            core.pump(&mut engine);
        };
        dl.run = Some(run.boxed().shared());
        tokio::spawn(dl.run.clone().unwrap());
        Self::update_shares(engine);
        self.keep_awake(engine);
    }

    // --- public API -----------------------------------------------------------------------------

    /// Every download, oldest first, each as a snapshot.
    pub async fn list_downloads(&self) -> Vec<DownloadUpdate> {
        self.initialized().await;
        let mut engine = self.engine.lock();
        let mut downloads: Vec<&mut Download> = engine.downloads.values_mut().collect();
        downloads.sort_by(|a, b| a.state.started_at.total_cmp(&b.state.started_at));
        downloads.into_iter().map(Download::snapshot).collect()
    }

    pub async fn list_history(&self) -> Vec<FinishedDownload> {
        self.history.list().await
    }

    pub async fn probe(&self, url: &str, browser: Option<&BrowserContext>) -> anyhow::Result<ProbeResult> {
        probe_url(&self.torrents, url, browser, self.knobs.stall_timeout).await
    }

    pub async fn start_download(self: &Arc<Self>, mut request: StartDownloadRequest) -> anyhow::Result<String> {
        self.initialized().await;
        if let Some(browser) = &request.browser {
            let value = serde_json::to_value(browser)?;
            request.browser = Some(
                browser_context_from(&value)
                    .ok_or_else(|| anyhow::anyhow!("The browser’s details for this download couldn’t be read."))?,
            );
        }
        let available = self.networks.refresh().await;
        let selected: Vec<NetworkInterfaceInfo> =
            available.iter().filter(|iface| request.interface_ids.contains(&iface.id)).cloned().collect();
        if selected.is_empty() {
            anyhow::bail!("Select at least one network");
        }
        let destination_dir = PathBuf::from(&request.destination_dir);
        ensure_directory(&destination_dir).await.map_err(|e| anyhow::anyhow!(io_message(&e)))?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();

        let dl = if request.kind == TransferKind::Torrent {
            let info_hash = request.info_hash.clone().unwrap_or_default();
            let bytes = self
                .torrents
                .probed_torrent_file(&info_hash)
                .ok_or_else(|| anyhow::anyhow!("Add the torrent again to start downloading."))?;
            let (torrent, _) = self.torrents.info(&bytes)?;
            let total_bytes: u64 = torrent.files.iter().map(|file| file.length).sum();
            let chosen = chosen_files(request.selected_files.as_deref(), torrent.files.len())?;
            let mut pieces = plan_pieces(total_bytes, torrent.piece_length);
            let skipped_bytes = choose_pieces(&mut pieces, &torrent, chosen.as_ref());
            ensure_disk_space(&destination_dir, total_bytes - skipped_bytes)?;
            let folder = torrent.files[0].path.contains(std::path::MAIN_SEPARATOR);
            let destination = claim_destination_path(&destination_dir, &request.suggested_file_name, folder)
                .await
                .map_err(|e| anyhow::anyhow!(io_message(&e)))?;
            tokio::fs::create_dir_all(self.download_dir(&id)).await?;
            tokio::fs::write(self.torrent_file_path(&id), &bytes).await?;
            let networks =
                available.iter().map(|iface| DownloadNetwork::new(iface, selected.contains(iface), TransferKind::Torrent)).collect();
            let state = DownloadState {
                id: id.clone(),
                url: request.url.clone(),
                file_name: destination.file_name().unwrap_or_default().to_string_lossy().into_owned(),
                destination_path: destination.to_string_lossy().into_owned(),
                total_bytes,
                bytes_downloaded: 0,
                speed_bytes_per_sec: 0.0,
                time_left_seconds: None,
                status: DownloadStatus::Downloading,
                error: None,
                resumable: None,
                started_at: now,
                scheduled: None,
                queued_at: None,
                paused_at: None,
                total_paused_ms: None,
                completed_at: None,
                speed_history: None,
                peak_speed_bytes_per_sec: None,
                seq: None,
                networks,
                detail: Detail::Torrent(TorrentDetail {
                    peers: Vec::new(),
                    peak_peers: 0,
                    total_pieces: pieces.len(),
                    piece_length: torrent.piece_length,
                    files: choice_of(chosen.as_ref(), torrent.files.len()),
                    folder,
                    skipped_bytes,
                    bytes_uploaded: 0,
                    upload_speed_bytes_per_sec: 0.0,
                }),
            };
            let unwanted = unchosen_paths(&torrent, chosen.as_ref());
            let transfer = TorrentTransfer::new(bytes, torrent, &destination, folder, unwanted);
            Download::new(state, pieces, request, destination, Transfer::Torrent(transfer))
        } else {
            let target = url::Url::parse(&request.url).map_err(|_| anyhow::anyhow!("Invalid URL"))?;
            let host = target_host(&target);
            let remote = resolve_within(&host, target.port_or_known_default().unwrap_or(80), self.knobs.stall_timeout).await?;
            let usable = compatible_interfaces(&selected, &remote);
            if usable.is_empty() {
                anyhow::bail!("No selected network has an address compatible with {host}");
            }
            let block_size = plan_block_size(request.total_bytes, request.supports_ranges, usable.len(), None);
            let blocks = plan_blocks(request.total_bytes, block_size);
            ensure_disk_space(&destination_dir, request.total_bytes)?;
            let destination = reserve_destination_path(&destination_dir, &request.suggested_file_name)
                .await
                .map_err(|e| anyhow::anyhow!(io_message(&e)))?;
            let mut networks: Vec<DownloadNetwork> =
                available.iter().map(|iface| DownloadNetwork::new(iface, usable.contains(iface), TransferKind::Http)).collect();
            // One that can't be split runs on one network.
            if !splittable(&request) {
                for network in &mut networks {
                    network.enabled &= network.id == usable[0].id;
                    network.status = if network.enabled { NetworkStatus::On } else { NetworkStatus::Off };
                }
            }
            let from_browser =
                request.browser.as_ref().map(|b| b.name.clone().unwrap_or_else(|| "your browser".into()));
            let state = DownloadState {
                id: id.clone(),
                url: request.url.clone(),
                file_name: destination.file_name().unwrap_or_default().to_string_lossy().into_owned(),
                destination_path: destination.to_string_lossy().into_owned(),
                total_bytes: request.total_bytes,
                bytes_downloaded: 0,
                speed_bytes_per_sec: 0.0,
                time_left_seconds: None,
                status: DownloadStatus::Downloading,
                error: None,
                resumable: None,
                started_at: now,
                scheduled: None,
                queued_at: None,
                paused_at: None,
                total_paused_ms: None,
                completed_at: None,
                speed_history: None,
                peak_speed_bytes_per_sec: None,
                seq: None,
                networks,
                detail: Detail::Http(HttpDetail {
                    streams: Vec::new(),
                    peak_streams: 0,
                    total_blocks: blocks.len(),
                    block_size_bytes: block_size,
                    disk_limited: None,
                    from_browser,
                }),
            };
            let transfer = HttpTransfer::new(&request);
            Download::new(state, blocks, request, partial_path(&destination), Transfer::Http(transfer))
        };

        let room = {
            let mut engine = self.engine.lock();
            let at_once = self.schedule.lock().downloads_at_once as usize;
            let room = Self::running_count(&engine) < at_once && self.schedule_allows();
            engine.downloads.insert(id.clone(), dl);
            if !room {
                self.enqueue(&mut engine, &id, false);
            }
            let scheduled = self.schedule_enabled();
            if let Some(dl) = engine.downloads.get_mut(&id) {
                dl.state.scheduled = scheduled;
            }
            room
        };
        self.persist_now(&id, false).await?;
        let mut engine = self.engine.lock();
        if let Some(dl) = engine.downloads.get_mut(&id) {
            self.push_update(dl, true);
        }
        if room {
            self.begin(&mut engine, &id);
        }
        Ok(id)
    }

    pub async fn pause(self: &Arc<Self>, id: &str) {
        self.pause_inner(id, false).await;
    }

    /// Stops a download's run. `for_schedule`: it goes back in the queue, to go on when the
    /// schedule next allows.
    pub(crate) fn pause_inner<'a>(self: &'a Arc<Self>, id: &'a str, for_schedule: bool) -> futures::future::BoxFuture<'a, ()> {
        async move {
            enum Queued {
                Running,
                Wait(Option<Done>),
                Paused(Option<Done>),
                Nothing,
            }
            let queued = {
                let mut engine = self.engine.lock();
                match engine.downloads.get_mut(id) {
                    None => Queued::Nothing,
                    Some(dl) if dl.state.status == DownloadStatus::Queued && for_schedule => Queued::Wait(dl.run.clone()),
                    Some(dl) if dl.state.status == DownloadStatus::Queued => {
                        dl.state.scheduled = None;
                        dl.state.status = DownloadStatus::Paused;
                        dl.state.queued_at = None;
                        dl.state.paused_at.get_or_insert(now_ms());
                        self.push_update(dl, true);
                        Queued::Paused(dl.run.clone())
                    }
                    Some(dl) if dl.state.status != DownloadStatus::Downloading || dl.publishing => Queued::Nothing,
                    Some(_) => Queued::Running,
                }
            };
            match queued {
                Queued::Running => {}
                Queued::Nothing => return,
                Queued::Wait(run) => return wait(run).await,
                Queued::Paused(run) => {
                    wait(run).await;
                    let _ = self.persist_now(id, false).await;
                    return;
                }
            }
            let run = {
                let mut engine = self.engine.lock();
                if engine.downloads.get(id).is_none_or(|dl| dl.state.status != DownloadStatus::Downloading) {
                    return;
                }
                if for_schedule {
                    engine.downloads.get_mut(id).unwrap().schedule_stopping = true;
                    self.enqueue(&mut engine, id, false);
                } else {
                    let dl = engine.downloads.get_mut(id).unwrap();
                    dl.state.status = DownloadStatus::Paused;
                    dl.state.scheduled = None;
                }
                let dl = engine.downloads.get_mut(id).unwrap();
                dl.state.paused_at = Some(now_ms());
                clear_speeds(&mut dl.state);
                if let Some(detail) = dl.state.http_mut() {
                    for stream in &mut detail.streams {
                        if stream.status != StreamStatus::Completed {
                            stream.status = StreamStatus::Paused;
                        }
                        stream.current_block_index = None;
                        stream.hedge = None;
                    }
                }
                for unit in &mut dl.units {
                    if unit.status == UnitStatus::Downloading {
                        unit.status = UnitStatus::Pending;
                    }
                    if unit.provisional_bytes.is_some() {
                        unit.provisional_bytes = Some(0);
                    }
                }
                match &mut dl.transfer {
                    Transfer::Http(transfer) => transfer.reset(),
                    Transfer::Torrent(transfer) => transfer.reset(),
                }
                self.stop_run(dl);
                self.push_update(dl, true);
                dl.run.clone()
            };
            wait(run).await;
            let _ = self.persist_now(id, false).await;
            {
                let mut engine = self.engine.lock();
                if let Some(dl) = engine.downloads.get_mut(id) {
                    dl.schedule_stopping = false;
                }
                Self::update_shares(&mut engine);
                self.pump(&mut engine);
            }
        }
        .boxed()
    }

    pub fn resume(self: &Arc<Self>, id: &str) {
        {
            let engine = self.engine.lock();
            let Some(dl) = engine.downloads.get(id) else { return };
            if dl.cancelling.is_some() {
                return;
            }
            let status = dl.state.status;
            if status != DownloadStatus::Paused && !(status == DownloadStatus::Error && dl.state.resumable != Some(false)) {
                return;
            }
        }
        let core = Arc::clone(self);
        let id = id.to_owned();
        tokio::spawn(async move { core.resume_after_verifying(&id, false).await });
    }

    async fn resume_after_verifying(self: &Arc<Self>, id: &str, by_network: bool) {
        let (run, partial) = {
            let engine = self.engine.lock();
            let Some(dl) = engine.downloads.get(id) else { return };
            (dl.run.clone(), dl.partial.clone())
        };
        wait(run).await;
        self.networks.refresh().await;
        let missing = tokio::fs::metadata(&partial).await.is_err();
        let queued = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            if missing {
                dl.state.error =
                    Some("The partial download file is unavailable. Reconnect the destination drive and try again.".into());
                self.push_update(dl, true);
                return;
            }
            if dl.cancelling.is_some() || !matches!(dl.state.status, DownloadStatus::Paused | DownloadStatus::Error) {
                return;
            }
            if by_network && !dl.state.networks.iter().any(|n| n.enabled) {
                return;
            }
            let at_once = self.schedule.lock().downloads_at_once as usize;
            if Self::running_count(&engine) >= at_once || !self.schedule_allows() {
                self.enqueue(&mut engine, id, true);
                let dl = engine.downloads.get_mut(id).unwrap();
                dl.state.error = None;
                dl.state.resumable = None;
                dl.paused_for_no_network = false;
                self.push_update(dl, true);
                true
            } else {
                self.begin(&mut engine, id);
                false
            }
        };
        if queued {
            let _ = self.persist_now(id, false).await;
        }
    }

    /// A fresh link to the same file, for a download whose link stopped working; it resumes.
    pub async fn relink(
        self: &Arc<Self>,
        id: &str,
        url: &str,
        given_probe: Option<ProbeResult>,
        given_browser: Option<BrowserContext>,
    ) -> anyhow::Result<()> {
        let replaceable = |core: &Core| {
            let engine = core.engine.lock();
            engine.downloads.get(id).is_some_and(|dl| {
                dl.state.resumable != Some(false)
                    && matches!(dl.state.status, DownloadStatus::Error | DownloadStatus::Paused)
            })
        };
        let browser = {
            let engine = self.engine.lock();
            let Some(dl) = engine.downloads.get(id).filter(|dl| dl.kind() == TransferKind::Http) else {
                anyhow::bail!("Only web downloads support replacing a link.");
            };
            if dl.state.resumable == Some(false) {
                anyhow::bail!("This download can’t resume. Start it again.");
            }
            given_browser.or_else(|| dl.request.browser.clone())
        };
        if !replaceable(self) {
            anyhow::bail!("Pause the download before replacing its link.");
        }
        if !self.engine.lock().relinking.insert(id.to_owned()) {
            anyhow::bail!("A new link for this download is being checked.");
        }
        let result = async {
            let probe = match given_probe {
                Some(probe) => probe,
                None => self.probe(url, browser.as_ref()).await?,
            };
            if probe.kind != TransferKind::Http {
                anyhow::bail!("That isn’t a link to a file");
            }
            let (total, can_split) = {
                let engine = self.engine.lock();
                let dl = engine.downloads.get(id).ok_or_else(|| anyhow::anyhow!("That download is gone."))?;
                (dl.state.total_bytes, splittable(&dl.request))
            };
            if probe.total_bytes.unwrap_or(0) != total {
                anyhow::bail!(
                    "That link is to a different file: {}, not {}",
                    format_gigabytes(probe.total_bytes.unwrap_or(0)),
                    format_gigabytes(total)
                );
            }
            if can_split && !probe.supports_ranges {
                anyhow::bail!("This server doesn’t support resuming downloads. Try another link to the same file.");
            }
            match self.compare_with_disk(id, &probe.final_url, browser.as_ref()).await {
                "different" => anyhow::bail!("That link is to a different file: its bytes don’t match what’s downloaded."),
                "unknown" => anyhow::bail!("Muxo couldn’t check that link against what’s downloaded. Try again."),
                _ => {}
            }
            if !replaceable(self) {
                anyhow::bail!("The download changed while its new link was checked.");
            }
            {
                let mut engine = self.engine.lock();
                let dl = engine.downloads.get_mut(id).unwrap();
                dl.request.url = probe.final_url.clone();
                dl.request.browser = browser.clone();
                dl.state.url = probe.final_url.clone();
            }
            self.persist_now(id, false).await?;
            self.resume(id);
            Ok(())
        }
        .await;
        self.engine.lock().relinking.remove(id);
        result
    }

    async fn compare_with_disk(&self, id: &str, url: &str, browser: Option<&BrowserContext>) -> &'static str {
        let (samples, partial, enabled) = {
            let engine = self.engine.lock();
            let Some(dl) = engine.downloads.get(id) else { return "unknown" };
            let samples: Vec<(u64, u64)> =
                dl.units.iter().filter(|u| u.bytes_downloaded > 0).map(|u| (u.range_start, u.bytes_downloaded)).collect();
            let enabled: Vec<String> = dl.state.networks.iter().filter(|n| n.enabled).map(|n| n.id.clone()).collect();
            (samples, dl.partial.clone(), enabled)
        };
        if samples.is_empty() {
            return "same";
        }
        let available = self.networks.refresh().await;
        let Some(network) = available.iter().find(|iface| enabled.contains(&iface.id)).or(available.first()).cloned() else {
            return "unknown";
        };
        let connection =
            StreamConnection::new(Arc::new(move || Some(network.clone())), self.knobs.stall_timeout, self.knobs.connect_timeout);
        compare_samples(&samples, &DownloadFile::new(partial), url, browser, &connection, None, self.knobs.stall_timeout).await
    }

    /// A failed download this probe looks like the same file as, for the user to resume instead.
    pub fn failed_match(&self, probe: &ProbeResult) -> Option<LinkResumes> {
        let strong = |etag: Option<&String>| etag.filter(|etag| !etag.to_lowercase().starts_with("w/")).cloned();
        let etag = normalize_etag(&strong(probe.etag.as_ref())?);
        let total = probe.total_bytes.filter(|total| *total > 0)?;
        let engine = self.engine.lock();
        engine
            .downloads
            .values()
            .find(|dl| {
                dl.kind() == TransferKind::Http
                    && dl.state.status == DownloadStatus::Error
                    && dl.state.resumable != Some(false)
                    && dl.state.total_bytes == total
                    && dl.request.suggested_file_name == probe.suggested_file_name
                    && strong(dl.request.etag.as_ref()).is_some_and(|ours| normalize_etag(&ours) == etag)
            })
            .map(|dl| LinkResumes { id: dl.state.id.clone(), file_name: dl.state.file_name.clone() })
    }

    pub async fn set_network_enabled(self: &Arc<Self>, id: &str, network_id: &str, enabled: bool) {
        enum Then {
            Nothing,
            Resume,
            PauseForNoNetwork,
        }
        let then = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            let status = dl.state.status;
            if dl.cancelling.is_some()
                || !matches!(status, DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued)
            {
                return;
            }
            let Some(index) = dl.state.networks.iter().position(|n| n.id == network_id) else { return };
            if dl.state.networks[index].enabled == enabled {
                return;
            }
            let was_last = !enabled && !dl.state.networks.iter().enumerate().any(|(i, n)| i != index && n.enabled);
            // One that can't be split runs on one network: switching one on switches the rest off.
            if enabled && dl.kind() == TransferKind::Http && !splittable(&dl.request) {
                dl.state.networks.iter_mut().for_each(|n| n.enabled = false);
            }
            dl.state.networks[index].enabled = enabled;
            if !enabled {
                dl.last_switched_off = Some(network_id.to_owned());
            }
            if was_last && status == DownloadStatus::Downloading {
                dl.paused_for_no_network = true;
                Then::PauseForNoNetwork
            } else {
                self.reconcile_download(dl);
                self.push_update(dl, true);
                let resume = enabled && status == DownloadStatus::Paused && dl.paused_for_no_network;
                Self::update_shares(&mut engine);
                if resume { Then::Resume } else { Then::Nothing }
            }
        };
        match then {
            Then::Nothing => {}
            Then::Resume => self.resume_after_verifying(id, true).await,
            Then::PauseForNoNetwork => {
                self.pause(id).await;
                let mut engine = self.engine.lock();
                if let Some(dl) = engine.downloads.get_mut(id) {
                    if dl.state.status != DownloadStatus::Paused {
                        dl.paused_for_no_network = false;
                    }
                }
            }
        }
    }

    /// Called whenever the computer's networks change: a network that appeared, dropped or got a
    /// new address is looked at by every download, and one whose address went has its sockets
    /// replaced.
    fn networks_changed(self: &Arc<Self>) {
        let mut engine = self.engine.lock();
        let mut changed = HashSet::new();
        let mut moved = HashSet::new();
        let mut seen = HashMap::new();
        for iface in self.networks.current().unwrap_or_default() {
            let addresses: Vec<String> = iface.addresses.iter().map(|a| a.address.clone()).collect();
            let before = engine.seen_addresses.get(&iface.id);
            let same = before.is_some_and(|b| b.len() == addresses.len() && b.iter().all(|a| addresses.contains(a)));
            if !same {
                changed.insert(iface.id.clone());
                if before.is_some_and(|b| b.iter().any(|a| !addresses.contains(a))) {
                    moved.insert(iface.id.clone());
                }
            }
            seen.insert(iface.id.clone(), addresses);
        }
        engine.seen_addresses = seen;
        for dl in engine.downloads.values_mut() {
            if !matches!(dl.state.status, DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued) {
                continue;
            }
            self.reconcile_download(dl);
            if matches!(dl.transfer, Transfer::Http(_)) {
                self.http_wake(dl, &|id| changed.contains(id), &|id| moved.contains(id));
            }
            self.schedule_update(dl);
        }
    }

    /// The computer woke from sleep: every judgement made by the clock starts over.
    pub fn system_resumed(self: &Arc<Self>) {
        let core = Arc::clone(self);
        tokio::spawn(async move {
            core.refresh_schedule().await;
            core.networks.refresh().await;
        });
        let now = now_ms();
        let mut engine = self.engine.lock();
        for dl in engine.downloads.values_mut() {
            if dl.state.status == DownloadStatus::Downloading && matches!(dl.transfer, Transfer::Http(_)) {
                self.http_system_resumed(dl, now);
            }
        }
    }

    async fn torrent_info(&self, id: &str) -> Option<TorrentInfo> {
        let bytes = tokio::fs::read(self.torrent_file_path(id)).await.ok()?;
        self.torrents.info(&bytes.into()).ok().map(|(info, _)| info)
    }

    /// A torrent download's files, in the torrent's order; empty for any other download.
    pub async fn torrent_files(&self, id: &str) -> Vec<TorrentFileEntry> {
        let selected = {
            let engine = self.engine.lock();
            match engine.downloads.get(id) {
                Some(dl) if dl.kind() == TransferKind::Torrent => dl.request.selected_files.clone(),
                _ => return Vec::new(),
            }
        };
        let Some(torrent) = self.torrent_info(id).await else { return Vec::new() };
        let chosen = chosen_files(selected.as_deref(), torrent.files.len()).ok().flatten();
        torrent
            .files
            .into_iter()
            .enumerate()
            .map(|(index, file)| TorrentFileEntry {
                path: file.path,
                length: file.length,
                chosen: chosen.as_ref().is_none_or(|chosen| chosen.contains(&index)),
            })
            .collect()
    }

    /// Which of a torrent download's files to fetch, as it runs or not. Refused for one already
    /// downloaded, or with no room on disk for what's added.
    pub async fn choose_torrent_files(self: &Arc<Self>, id: &str, selected: Vec<usize>) -> anyhow::Result<()> {
        let turn = {
            let mut engine = self.engine.lock();
            let dl = engine
                .downloads
                .get_mut(id)
                .filter(|dl| dl.kind() == TransferKind::Torrent)
                .ok_or_else(|| anyhow::anyhow!("This download has no files to choose"))?;
            dl.file_choices += 1;
            dl.file_choices
        };
        let torrent = self.torrent_info(id).await.ok_or_else(|| anyhow::anyhow!("This download has no files to choose"))?;
        let settled = |core: &Core| {
            let engine = core.engine.lock();
            engine.downloads.get(id).is_none_or(|dl| {
                dl.publishing || matches!(dl.state.status, DownloadStatus::Completed | DownloadStatus::Cancelled)
            })
        };
        if settled(self) {
            anyhow::bail!("This download’s files can’t change now");
        }
        let chosen = chosen_files(Some(&selected), torrent.files.len())?;
        let (adding, destination_dir) = {
            let engine = self.engine.lock();
            let dl = engine.downloads.get(id).unwrap();
            let completed: Vec<bool> = dl.units.iter().map(|u| u.status == UnitStatus::Completed).collect();
            let finished = finished_files(&torrent.files, torrent.piece_length, &completed);
            if let Some(chosen) = &chosen {
                if finished.iter().any(|index| !chosen.contains(index)) {
                    anyhow::bail!("A file that’s already downloaded stays");
                }
            }
            let wanted = chosen.as_ref().map(|chosen| wanted_pieces(&torrent.files, torrent.piece_length, chosen));
            let adding: u64 = dl
                .units
                .iter()
                .filter(|u| u.status == UnitStatus::Skipped && wanted.as_ref().is_none_or(|w| w[u.index]))
                .filter_map(UnitState::length)
                .sum();
            (adding, PathBuf::from(&dl.request.destination_dir))
        };
        ensure_disk_space(&destination_dir, adding)?;
        if settled(self) {
            anyhow::bail!("This download’s files can’t change now");
        }
        let mut engine = self.engine.lock();
        let dl = engine.downloads.get_mut(id).unwrap();
        // Only the latest choice applies.
        if turn != dl.file_choices {
            return Ok(());
        }
        let files = choice_of(chosen.as_ref(), torrent.files.len());
        dl.request.selected_files = files.selected.clone();
        let skipped = choose_pieces(&mut dl.units, &torrent, chosen.as_ref());
        if let Some(detail) = dl.state.torrent_mut() {
            detail.files = files;
            detail.skipped_bytes = skipped;
        }
        if let Transfer::Torrent(transfer) = &mut dl.transfer {
            transfer.unwanted = unchosen_paths(&torrent, chosen.as_ref());
        }
        self.torrent_files_chosen(dl);
        self.push_update(dl, true);
        Ok(())
    }

    pub async fn cancel(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        let pending = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return Ok(()) };
            if let Some(cancelling) = dl.cancelling.clone() {
                Some(cancelling)
            } else {
                if dl.publishing
                    || !matches!(
                        dl.state.status,
                        DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued | DownloadStatus::Error
                    )
                {
                    return Ok(());
                }
                None
            }
        };
        if let Some(pending) = pending {
            pending.await;
            return Ok(());
        }
        let result = Arc::new(Mutex::new(Ok(())));
        let core = Arc::clone(self);
        let task_id = id.to_owned();
        let outcome = Arc::clone(&result);
        let task: Done = async move {
            let result = core.cancel_runtime(&task_id).await;
            *outcome.lock() = result;
        }
        .boxed()
        .shared();
        if let Some(dl) = self.engine.lock().downloads.get_mut(id) {
            dl.cancelling = Some(task.clone());
        }
        task.await;
        if let Some(dl) = self.engine.lock().downloads.get_mut(id) {
            dl.cancelling = None;
        }
        let result = std::mem::replace(&mut *result.lock(), Ok(()));
        result
    }

    async fn cancel_runtime(self: &Arc<Self>, id: &str) -> anyhow::Result<()> {
        self.pause(id).await;
        let (run, partial, kind) = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return Ok(()) };
            if dl.state.status != DownloadStatus::Error {
                dl.state.status = DownloadStatus::Paused;
                dl.state.queued_at = None;
                dl.state.paused_at.get_or_insert(now_ms());
            }
            clear_speeds(&mut dl.state);
            self.stop_run(dl);
            self.push_update(dl, false);
            (dl.run.clone(), dl.partial.clone(), dl.kind())
        };
        wait(run).await;
        let discarded = if kind == TransferKind::Http {
            DownloadFile::new(partial).discard().await
        } else {
            Ok(())
        };
        let result = match discarded {
            Ok(()) => self.remove_persisted(id, false).await,
            Err(error) => Err(error),
        };
        {
        let mut engine = self.engine.lock();
        if let Some(dl) = engine.downloads.get_mut(id) {
            match &result {
                Ok(()) => {
                    dl.state.status = DownloadStatus::Cancelled;
                    if let Some(detail) = dl.state.http_mut() {
                        detail.streams.iter_mut().for_each(|s| s.status = StreamStatus::Cancelled);
                    }
                    self.push_update(dl, false);
                }
                Err(error) => {
                    dl.removed = false;
                    dl.state.status = DownloadStatus::Error;
                    dl.state.error = Some(io_message(error));
                    dl.state.resumable = Some(false);
                    self.push_update(dl, false);
                }
            }
        }
        self.pump(&mut engine);
        }
        if let Err(error) = result {
            let _ = self.persist_now(id, false).await;
            anyhow::bail!(io_message(&error));
        }
        Ok(())
    }

    /// Removes a download, cancelling one under way. A finished one's file stays, unless
    /// `trash_file`: then it goes to the Trash.
    pub async fn remove(self: &Arc<Self>, id: &str, trash_file: bool) -> anyhow::Result<()> {
        self.initialized().await;
        let status = self.engine.lock().downloads.get(id).map(|dl| dl.state.status);
        if let Some(status) = status {
            if matches!(
                status,
                DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued | DownloadStatus::Error
            ) {
                self.cancel(id).await?;
            }
            let removed = self.engine.lock().downloads.remove(id);
            if let Some(dl) = removed {
                self.remove_persisted_dir(id).await;
                if dl.state.status == DownloadStatus::Completed {
                    if trash_file {
                        self.trash(&dl.state, None).await?;
                    }
                    self.history.remove(Some(vec![id.to_owned()])).await?;
                    self.host.history_changed();
                }
                return Ok(());
            }
        }
        if trash_file {
            if let Some(entry) = self.history.find(id).await.filter(|entry| entry.missing != Some(true)) {
                self.trash(&entry.state, entry.downloaded_files.as_deref()).await?;
            }
        }
        self.history.remove(Some(vec![id.to_owned()])).await?;
        self.host.history_changed();
        Ok(())
    }

    /// Trash only a torrent's own files; never move unrelated files added to its folder.
    async fn trash(&self, state: &DownloadState, owned: Option<&[String]>) -> anyhow::Result<()> {
        let destination = PathBuf::from(&state.destination_path);
        let folder = state.torrent().is_some_and(|t| t.folder);
        if !folder {
            return self.host.trash(&destination).map_err(anyhow::Error::msg);
        }
        let Some(files) = owned.filter(|files| !files.is_empty()) else {
            anyhow::bail!("Muxo can’t identify the files in this older torrent. Remove it from the list or manage its files in your file browser.");
        };
        for file in files {
            let target = destination.join(file);
            if target.starts_with(&destination) && tokio::fs::metadata(&target).await.is_ok() {
                self.host.trash(&target).map_err(anyhow::Error::msg)?;
            }
        }
        remove_unwanted(&destination, &[], true).await;
        let _ = tokio::fs::remove_dir(&destination).await;
        Ok(())
    }

    /// Forgets every finished download; their files stay.
    pub async fn clear_history(&self) -> anyhow::Result<()> {
        self.initialized().await;
        self.history.remove(None).await?;
        self.host.history_changed();
        Ok(())
    }

    async fn find_path(&self, id: &str, partial_first: bool) -> Option<PathBuf> {
        let (partial, destination) = {
            let engine = self.engine.lock();
            match engine.downloads.get(id) {
                Some(dl) => (
                    (partial_first && dl.state.status != DownloadStatus::Completed).then(|| dl.partial.clone()),
                    Some(PathBuf::from(&dl.state.destination_path)),
                ),
                None => (None, None),
            }
        };
        let destination = match destination {
            Some(destination) => Some(destination),
            None => self.history.find(id).await.map(|entry| PathBuf::from(entry.state.destination_path)),
        };
        for path in [partial, destination].into_iter().flatten() {
            if path_exists(&path).await.unwrap_or(false) {
                return Some(path);
            }
        }
        None
    }

    /// Shows a download's file in its folder. False when nothing is there any more.
    pub async fn reveal(&self, id: &str) -> bool {
        match self.find_path(id, true).await {
            Some(path) => self.host.reveal(&path),
            None => {
                self.host.history_changed();
                false
            }
        }
    }

    /// Opens a download's file (a torrent's folder) with the OS default.
    pub async fn open(&self, id: &str) -> bool {
        match self.find_path(id, false).await {
            Some(path) => self.host.open(&path),
            None => {
                self.host.history_changed();
                false
            }
        }
    }

    /// Before quitting: every running download is paused and saved, to go on next launch.
    pub async fn suspend_all(self: &Arc<Self>) {
        self.initialized().await;
        self.schedule.lock().suspending = true;
        let downloads: Vec<(String, DownloadStatus, bool)> = self
            .engine
            .lock()
            .downloads
            .values()
            .map(|dl| (dl.state.id.clone(), dl.state.status, dl.state.scheduled == Some(true)))
            .collect();
        futures::future::join_all(downloads.into_iter().map(|(id, status, scheduled)| async move {
            if status == DownloadStatus::Downloading {
                self.pause_inner(&id, scheduled).await;
            } else {
                let run = self.engine.lock().downloads.get(&id).and_then(|dl| dl.run.clone());
                wait(run).await;
                let _ = self.persist_now(&id, false).await;
            }
        }))
        .await;
        self.limits.save().await;
    }

    // --- running --------------------------------------------------------------------------------

    async fn run(self: &Arc<Self>, id: &str) {
        let (stop, ended, partial, kind) = {
            let mut engine = self.engine.lock();
            Self::update_shares(&mut engine);
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            if dl.stop.is_cancelled() {
                dl.stop = CancellationToken::new();
            }
            dl.meters.clear();
            dl.upload_meters.clear();
            let ended = match &mut dl.transfer {
                Transfer::Http(transfer) => {
                    transfer.reset();
                    Some(Arc::clone(&transfer.worker_ended))
                }
                Transfer::Torrent(transfer) => {
                    transfer.reset();
                    None
                }
            };
            (dl.stop.clone(), ended, dl.partial.clone(), dl.kind())
        };
        // Every stream writes through one handle, each at its own offsets.
        if kind == TransferKind::Http {
            let opened = DownloadFile::new(partial.clone()).open_write();
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            match opened {
                Ok(file) => {
                    if let Transfer::Http(transfer) = &mut dl.transfer {
                        transfer.file = Some(Arc::new(file));
                    }
                }
                Err(error) => self.fail_download(dl, &io_message(&error), false),
            }
        }
        let mut ticked_at = now_ms();
        {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            // Readings run on across a pause, so short runs between pauses count too.
            let paused = dl.peak_readings.back().map_or(0.0, |(time, _)| ticked_at - time);
            for reading in dl.peak_readings.iter_mut() {
                reading.0 += paused;
            }
            if dl.peak_readings.back().map(|(_, bytes)| *bytes) != Some(dl.state.bytes_downloaded) {
                dl.peak_readings.push_back((ticked_at, dl.state.bytes_downloaded));
            }
            self.reconcile_download(dl);
        }

        loop {
            {
                let engine = self.engine.lock();
                let Some(dl) = engine.downloads.get(id) else { return };
                if dl.state.status != DownloadStatus::Downloading || dl.all_done() {
                    break;
                }
            }
            match &ended {
                Some(ended) => tokio::select! {
                    _ = tokio::time::sleep(TICK) => {}
                    _ = ended.notified() => {}
                    _ = stop.cancelled() => {}
                },
                None => tokio::select! {
                    _ = tokio::time::sleep(TICK) => {}
                    _ = stop.cancelled() => {}
                },
            }
            let mut engine = self.engine.lock();
            Self::update_shares(&mut engine);
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            if dl.state.status != DownloadStatus::Downloading {
                break;
            }
            let now = now_ms();
            if matches!(dl.transfer, Transfer::Torrent(_)) {
                self.torrent_tick(dl, now);
            }
            let Download { state, meters, upload_meters, transfer, .. } = dl;
            let uploads = matches!(transfer, Transfer::Torrent(_)).then_some(upload_meters);
            let speeds_changed = update_speeds(state, meters, uploads, now);
            let time_left_changed = update_time_left(&mut dl.state, (now - ticked_at) / 1000.0);
            ticked_at = now;
            if speeds_changed || time_left_changed {
                self.schedule_update(dl);
            }
            if now - dl.speed_sampled_at >= 1000.0 {
                dl.speed_sampled_at = now;
                sample_speeds(&mut dl.state);
                sample_peak(dl, now);
                self.schedule_update(dl);
            }
            if matches!(dl.transfer, Transfer::Http(_)) {
                self.http_tick(dl, now);
            }
        }

        // Stopped, or every unit in: every connection stops before anything else happens.
        {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            sample_peak(dl, now_ms());
            self.stop_run(dl);
        }
        loop {
            let running = {
                let engine = self.engine.lock();
                engine.downloads.get(id).is_some_and(|dl| match &dl.transfer {
                    Transfer::Http(transfer) => transfer.running(),
                    Transfer::Torrent(_) => false,
                })
            };
            if !running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let discard = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            clear_speeds(&mut dl.state);
            if let Transfer::Http(transfer) = &mut dl.transfer {
                transfer.file = None;
            }
            if dl.state.status != DownloadStatus::Downloading {
                if dl.state.status == DownloadStatus::Error {
                    self.push_update(dl, true);
                    Some(dl.state.resumable == Some(false) && kind == TransferKind::Http)
                } else {
                    Some(false)
                }
            } else {
                dl.publishing = true;
                None
            }
        };
        match discard {
            Some(true) => {
                let _ = DownloadFile::new(partial).discard().await;
                return;
            }
            Some(false) => return,
            None => {}
        }

        let published = self.publish(id, kind, &partial).await;
        let completed = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return };
            match &published {
                Ok(()) => {
                    self.notify("Download complete", &format!("{} has finished downloading.", dl.state.file_name));
                }
                Err(message) => {
                    dl.state.status = DownloadStatus::Error;
                    dl.state.error = Some(message.clone());
                    self.notify("Download failed", &format!("{}: {}", dl.state.file_name, describe_error(message)));
                }
            }
            dl.publishing = false;
            self.push_update(dl, true);
            dl.state.status == DownloadStatus::Completed
        };
        if completed {
            self.move_to_history(id).await;
        }
    }

    async fn publish(self: &Arc<Self>, id: &str, kind: TransferKind, partial: &Path) -> Result<(), String> {
        let (destination, total, unwanted, folder) = {
            let engine = self.engine.lock();
            let dl = engine.downloads.get(id).ok_or("That download is gone.")?;
            if !dl.all_done() {
                return Err("Download is incomplete — refusing to publish the file".into());
            }
            let (unwanted, folder) = match &dl.transfer {
                Transfer::Torrent(transfer) => (transfer.unwanted.clone(), dl.state.torrent().is_some_and(|t| t.folder)),
                Transfer::Http(_) => (Vec::new(), false),
            };
            (PathBuf::from(&dl.state.destination_path), dl.state.total_bytes, unwanted, folder)
        };
        self.persist_now(id, true).await.map_err(|e| io_message(&e))?;
        let published = if kind == TransferKind::Torrent {
            remove_unwanted(&destination, &unwanted, folder).await;
            let identity = tokio::fs::metadata(&destination).await.ok().and_then(|m| FileIdentity::of(&m));
            {
                let mut engine = self.engine.lock();
                if let Some(dl) = engine.downloads.get_mut(id) {
                    dl.publication_path = Some(destination.to_string_lossy().into_owned());
                    dl.publication_identity = identity;
                }
            }
            destination.clone()
        } else {
            let file = DownloadFile::new(partial.to_path_buf());
            file.publish(&destination, total, |candidate| {
                let core = Arc::clone(self);
                let partial = partial.to_path_buf();
                let id = id.to_owned();
                async move {
                    let identity = tokio::fs::metadata(&partial).await.ok().and_then(|m| FileIdentity::of(&m));
                    {
                        let mut engine = core.engine.lock();
                        if let Some(dl) = engine.downloads.get_mut(&id) {
                            dl.publication_path = Some(candidate.to_string_lossy().into_owned());
                            dl.publication_identity = identity;
                        }
                    }
                    core.persist_now(&id, true).await
                }
            })
            .await
            .map_err(|e| io_message(&e))?
        };
        {
            let mut engine = self.engine.lock();
            let dl = engine.downloads.get_mut(id).ok_or("That download is gone.")?;
            dl.state.destination_path = published.to_string_lossy().into_owned();
            dl.state.file_name = published.file_name().unwrap_or_default().to_string_lossy().into_owned();
            dl.state.status = DownloadStatus::Completed;
            dl.state.completed_at = Some(now_ms());
            if let (Some((start_time, start_bytes)), Some((end_time, end_bytes))) =
                (dl.peak_readings.front().copied(), dl.peak_readings.back().copied())
            {
                let ran_for = (end_time - start_time) / 1000.0;
                if ran_for > 0.0 && dl.state.peak_speed_bytes_per_sec.is_none() {
                    dl.state.peak_speed_bytes_per_sec = Some(end_bytes.saturating_sub(start_bytes) as f64 / ran_for);
                }
            }
            let wanted = dl.state.total_bytes.saturating_sub(dl.state.skipped_bytes());
            if wanted > 0 {
                dl.state.bytes_downloaded = wanted;
            }
        }
        let _ = self.persist_now(id, true).await;
        Ok(())
    }

    // --- saving ---------------------------------------------------------------------------------

    /// Writes the download's manifest: what it takes to pick it up again after a restart.
    pub(crate) async fn persist_now(&self, id: &str, required: bool) -> std::io::Result<()> {
        let lock = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return Ok(()) };
            dl.persistence_armed = false;
            Arc::clone(&dl.persist_lock)
        };
        let _turn = lock.lock().await;
        let (manifest, sync) = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return Ok(()) };
            dl.checkpoint_queued = false;
            if dl.removed {
                return Ok(());
            }
            let mut state = dl.state.clone();
            match &mut state.detail {
                Detail::Http(detail) => detail.streams.clear(),
                Detail::Torrent(detail) => detail.peers.clear(),
            }
            let mut request = dl.request.clone();
            request.browser = None;
            let manifest = Manifest {
                version: MANIFEST_VERSION,
                saved_at: now_ms(),
                partial_path: dl.partial.clone(),
                publication_path: dl.publication_path.clone(),
                publication_identity: dl.publication_identity,
                request,
                state,
                saved: save_units(&dl.units),
            };
            let sync = dl.kind() == TransferKind::Http
                && matches!(dl.state.status, DownloadStatus::Downloading | DownloadStatus::Paused | DownloadStatus::Queued);
            (manifest, sync.then(|| dl.partial.clone()))
        };
        let result = async {
            if let Some(partial) = sync {
                DownloadFile::new(partial).sync().await?;
            }
            let dir = self.download_dir(id);
            tokio::fs::create_dir_all(&dir).await?;
            let path = self.manifest_path(id);
            let temporary = path.with_extension("json.tmp");
            tokio::fs::write(&temporary, serde_json::to_vec(&manifest).map_err(std::io::Error::other)?).await?;
            crate::util::rename_with_retry(&temporary, &path).await
        }
        .await;
        if required { result } else { Ok(()) }
    }

    async fn remove_persisted_dir(&self, id: &str) {
        let _ = tokio::fs::remove_dir_all(self.download_dir(id)).await;
    }

    async fn remove_persisted(&self, id: &str, discard_partial: bool) -> std::io::Result<()> {
        let lock = {
            let mut engine = self.engine.lock();
            let Some(dl) = engine.downloads.get_mut(id) else { return Ok(()) };
            dl.removed = true;
            dl.persistence_armed = false;
            Arc::clone(&dl.persist_lock)
        };
        let _turn = lock.lock().await;
        if discard_partial {
            let partial = self.engine.lock().downloads.get(id).map(|dl| dl.partial.clone());
            if let Some(partial) = partial {
                let _ = DownloadFile::new(partial).discard().await;
            }
        }
        match tokio::fs::remove_dir_all(self.download_dir(id)).await {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }

    /// A finished download leaves the list for history, with its work units: a download that is
    /// done needs none of them. Its saved state goes only once history has it.
    async fn move_to_history(self: &Arc<Self>, id: &str) {
        let files = self.torrent_files(id).await;
        let entry = {
            let engine = self.engine.lock();
            let Some(dl) = engine.downloads.get(id) else { return };
            let mut state = dl.state.clone();
            let (units_written, downloaded_files) = match &mut state.detail {
                Detail::Http(detail) => {
                    detail.streams.clear();
                    (detail.total_blocks, None)
                }
                Detail::Torrent(detail) => {
                    detail.peers.clear();
                    let written = dl.units.iter().filter(|u| u.status != UnitStatus::Skipped).count();
                    let folder = detail.folder;
                    let owned = files
                        .iter()
                        .filter(|file| file.chosen)
                        .map(|file| {
                            if folder {
                                file.path.split(std::path::MAIN_SEPARATOR).skip(1).collect::<Vec<_>>().join(std::path::MAIN_SEPARATOR_STR)
                            } else {
                                Path::new(&file.path).file_name().unwrap_or_default().to_string_lossy().into_owned()
                            }
                        })
                        .collect();
                    (written, Some(owned))
                }
            };
            FinishedDownload { state, units_written, missing: None, downloaded_files }
        };
        if self.history.add(entry).await.is_err() {
            return;
        }
        let _ = self.remove_persisted(id, false).await;
        self.engine.lock().downloads.remove(id);
        self.host.history_changed();
    }

    /// Every download saved when Muxo last ran, as it was: a running one comes back paused (or
    /// queued, when it was waiting for the schedule).
    async fn restore_persisted_downloads(self: &Arc<Self>) {
        let Ok(mut entries) = tokio::fs::read_dir(self.downloads_root()).await else { return };
        let mut restored: Vec<Download> = Vec::new();
        while let Ok(Some(entry)) = entries.next_entry().await {
            let id = entry.file_name().to_string_lossy().into_owned();
            let manifest = tokio::fs::read(self.manifest_path(&id))
                .await
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Manifest>(&bytes).ok())
                .filter(|manifest| manifest.version == MANIFEST_VERSION && manifest.state.id == id);
            match manifest {
                Some(manifest) => match self.restore_one(&id, manifest).await {
                    Some(dl) => restored.push(dl),
                    None => self.remove_persisted_dir(&id).await,
                },
                None => self.remove_persisted_dir(&id).await,
            }
        }
        // Two saved downloads claiming one file: the newer keeps it.
        restored.sort_by(|a, b| b.state.started_at.total_cmp(&a.state.started_at));
        let mut claimed = HashSet::new();
        let mut completed = Vec::new();
        for dl in restored {
            if !claimed.insert(dl.partial.clone()) {
                self.remove_persisted_dir(&dl.state.id).await;
                continue;
            }
            let id = dl.state.id.clone();
            if dl.state.status == DownloadStatus::Completed {
                completed.push(id.clone());
            }
            self.engine.lock().downloads.insert(id.clone(), dl);
        }
        for id in completed {
            self.move_to_history(&id).await;
        }
        let ids: Vec<String> = self.engine.lock().downloads.keys().cloned().collect();
        for id in ids {
            let _ = self.persist_now(&id, false).await;
        }
    }

    async fn restore_one(&self, id: &str, manifest: Manifest) -> Option<Download> {
        let Manifest { saved_at, partial_path, publication_path, publication_identity, request, mut state, saved, .. } =
            manifest;
        let (mut units, transfer) = match (&mut state.detail, request.kind) {
            (Detail::Torrent(detail), TransferKind::Torrent) => {
                let bytes = bytes::Bytes::from(tokio::fs::read(self.torrent_file_path(id)).await.ok()?);
                let (torrent, _) = self.torrents.info(&bytes).ok()?;
                let mut pieces = restore_pieces(state.total_bytes, torrent.piece_length, detail.total_pieces, &saved)?;
                let chosen = chosen_files(request.selected_files.as_deref(), torrent.files.len()).ok()?;
                detail.files = choice_of(chosen.as_ref(), torrent.files.len());
                detail.skipped_bytes = choose_pieces(&mut pieces, &torrent, chosen.as_ref());
                detail.peers.clear();
                let unwanted = unchosen_paths(&torrent, chosen.as_ref());
                let folder = detail.folder;
                (pieces, Transfer::Torrent(TorrentTransfer::new(bytes, torrent, &partial_path, folder, unwanted)))
            }
            (Detail::Http(detail), TransferKind::Http) => {
                let blocks = restore_blocks(state.total_bytes, detail.block_size_bytes, detail.total_blocks, &saved)?;
                detail.streams.clear();
                (blocks, Transfer::Http(HttpTransfer::new(&request)))
            }
            _ => return None,
        };
        if matches!(state.status, DownloadStatus::Downloading | DownloadStatus::Queued) {
            state.status = DownloadStatus::Paused;
            state.paused_at = Some(if saved_at > 0.0 { saved_at } else { now_ms() });
            state.queued_at = None;
        }
        clear_speeds(&mut state);
        for unit in &mut units {
            if unit.status == UnitStatus::Downloading {
                unit.status = UnitStatus::Pending;
            }
            if unit.provisional_bytes.is_some() {
                unit.provisional_bytes = Some(0);
            }
        }
        if state.status == DownloadStatus::Paused {
            let size = tokio::fs::metadata(&partial_path).await.ok().map(|m| if m.is_dir() { state.total_bytes } else { m.len() });
            let published_path = publication_path.clone().unwrap_or_else(|| state.destination_path.clone());
            let published = tokio::fs::metadata(&published_path).await.ok();
            let published_size = published.as_ref().map(|m| if m.is_dir() { state.total_bytes } else { m.len() });
            let expected = if state.total_bytes > 0 { state.total_bytes } else { state.bytes_downloaded };
            let same_file = match (&published, size) {
                (Some(published), Some(_)) => tokio::fs::metadata(&partial_path)
                    .await
                    .ok()
                    .and_then(|partial| FileIdentity::of(&partial))
                    .is_some_and(|partial| Some(partial) == FileIdentity::of(published)),
                (Some(published), None) => publication_identity.is_some() && publication_identity == FileIdentity::of(published),
                _ => false,
            };
            if units.iter().all(UnitState::is_done) && published_size == Some(expected) && same_file {
                // Published just before Muxo last quit.
                state.status = DownloadStatus::Completed;
                state.file_name =
                    Path::new(&published_path).file_name().unwrap_or_default().to_string_lossy().into_owned();
                state.destination_path = published_path;
                state.error = None;
                state.completed_at.get_or_insert(saved_at);
            } else if size.is_none() {
                state.status = DownloadStatus::Error;
                state.error = Some("The partial download file is missing. Remove this download and start again.".into());
                state.resumable = Some(false);
            } else if let Some(size) = size {
                // Progress past what the file holds can't be trusted.
                for unit in &mut units {
                    let length = unit.length().unwrap_or(0);
                    if unit.range_start + unit.bytes_downloaded > size
                        || (unit.status == UnitStatus::Completed && unit.bytes_downloaded != length)
                    {
                        unit.status = UnitStatus::Pending;
                        unit.bytes_downloaded = 0;
                        unit.bytes_by_interface.clear();
                    }
                }
            }
        }
        if state.status == DownloadStatus::Paused && state.scheduled == Some(true) {
            state.status = DownloadStatus::Queued;
            state.queued_at = Some(state.queued_at.unwrap_or(state.started_at));
        }
        if state.status != DownloadStatus::Completed {
            state.speed_history = None;
        }
        recompute_aggregates(&mut state, &units);
        let mut dl = Download::new(state, units, request, partial_path, transfer);
        dl.publication_path = publication_path;
        dl.publication_identity = publication_identity;
        Some(dl)
    }

    // --- links handed to Muxo --------------------------------------------------------------------

    /// A link the OS or the browser extension handed over: it waits for the window.
    pub fn offer(&self, offer: Offer) {
        self.links.add(offer);
        self.host.link_received();
    }

    pub async fn start_from_link(self: &Arc<Self>, mut request: StartDownloadRequest, link_id: Option<&str>) -> anyhow::Result<String> {
        // The browser's sign-in is attached here, so it never passes through the window.
        if let Some(browser) = link_id.and_then(|id| self.links.browser(id)) {
            if request.kind == TransferKind::Http {
                request.browser = Some(browser);
            }
        }
        let id = self.start_download(request).await?;
        if let Some(link_id) = link_id {
            self.links.release(link_id);
        }
        Ok(id)
    }

    pub async fn resume_from_link(self: &Arc<Self>, link_id: &str, download_id: &str) -> anyhow::Result<()> {
        let offer = self.links.get(link_id).ok_or_else(|| anyhow::anyhow!("That download is no longer waiting."))?;
        let probe = offer.probe.clone().filter(|probe| probe.kind == TransferKind::Http);
        let Some(probe) = probe else { anyhow::bail!("That download is no longer waiting.") };
        self.relink(download_id, &probe.final_url.clone(), Some(probe), offer.browser.clone()).await?;
        self.links.release(link_id);
        Ok(())
    }

    pub fn free_space(&self, dir: &str) -> Option<u64> {
        free_space(Path::new(dir))
    }
}
