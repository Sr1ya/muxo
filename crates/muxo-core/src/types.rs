//! The shapes the window and the engine exchange. Each mirrors its namesake in
//! `src/shared/types.ts` field for field (camelCase on the wire), so the renderer reads exactly
//! what the Electron main process used to send.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InterfaceKind {
    Wifi,
    Usb,
    Ethernet,
    Bridge,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkAddress {
    pub address: String,
    pub family: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub netmask: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subnet: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkInterfaceInfo {
    /// Stable identifier for this interface (the OS device name, e.g. "en0").
    pub id: String,
    pub device: String,
    pub display_name: String,
    pub addresses: Vec<NetworkAddress>,
    pub kind: InterfaceKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mac: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferKind {
    Http,
    Torrent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentFile {
    pub path: String,
    pub length: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentInfo {
    pub info_hash: String,
    pub piece_length: u64,
    /// Where each file will be written, relative to the destination folder, already checked to
    /// be safe (see download/torrent/paths.rs).
    pub files: Vec<TorrentFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeResult {
    pub kind: TransferKind,
    pub requested_url: String,
    /// URL after following redirects: what the download should actually fetch.
    pub final_url: String,
    pub supports_ranges: bool,
    /// None when the server did not report a size.
    pub total_bytes: Option<u64>,
    pub suggested_file_name: String,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub torrent: Option<TorrentInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MagnetCheckStatus {
    pub link: String,
    pub no_peers_yet: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentFileEntry {
    pub path: String,
    pub length: u64,
    pub chosen: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DownloadStatus {
    Queued,
    Downloading,
    Paused,
    Completed,
    Error,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StreamStatus {
    Pending,
    Downloading,
    Retrying,
    Paused,
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpStreamState {
    pub id: u32,
    pub interface_id: String,
    pub range_start: u64,
    pub range_end: Option<u64>,
    pub bytes_downloaded: u64,
    pub speed_bytes_per_sec: f64,
    pub status: StreamStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_block_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hedge: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentPeerState {
    pub id: u32,
    pub interface_id: String,
    /// "connected" or "receiving".
    pub status: String,
    pub client: Option<String>,
    pub bytes_downloaded: u64,
    pub speed_bytes_per_sec: f64,
    pub bytes_uploaded: u64,
    pub upload_speed_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnitStatus {
    Pending,
    Downloading,
    Completed,
    /// A torrent piece that none of the chosen files needs.
    Skipped,
}

/// An HTTP block or a torrent piece: one unit of work, as the block grid draws it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnitState {
    pub kind: TransferKind,
    pub index: usize,
    pub range_start: u64,
    pub range_end: Option<u64>,
    pub status: UnitStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface_id: Option<String>,
    pub bytes_downloaded: u64,
    /// Bytes of this unit delivered by each network, summing to `bytes_downloaded`.
    pub bytes_by_interface: BTreeMap<String, u64>,
    /// Torrent only: bytes received this run but not yet hash-verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provisional_bytes: Option<u64>,
}

impl UnitState {
    pub fn length(&self) -> Option<u64> {
        self.range_end.map(|end| end - self.range_start + 1)
    }

    pub fn is_done(&self) -> bool {
        matches!(self.status, UnitStatus::Completed | UnitStatus::Skipped)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkStatus {
    On,
    Off,
    Offline,
    Unreachable,
    Failed,
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadNetwork {
    pub transfer: TransferKind,
    pub id: String,
    pub label: String,
    pub kind: InterfaceKind,
    pub enabled: bool,
    pub status: NetworkStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub bytes_downloaded: u64,
    pub speed_bytes_per_sec: f64,
    pub retries: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_uploaded: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upload_speed_bytes_per_sec: Option<f64>,
}

impl DownloadNetwork {
    pub fn new(iface: &NetworkInterfaceInfo, enabled: bool, transfer: TransferKind) -> Self {
        let torrent = transfer == TransferKind::Torrent;
        DownloadNetwork {
            transfer,
            id: iface.id.clone(),
            label: iface.display_name.clone(),
            kind: iface.kind,
            enabled,
            status: if enabled { NetworkStatus::On } else { NetworkStatus::Off },
            error: None,
            bytes_downloaded: 0,
            speed_bytes_per_sec: 0.0,
            retries: 0,
            bytes_uploaded: torrent.then_some(0),
            upload_speed_bytes_per_sec: torrent.then_some(0.0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentFiles {
    pub chosen: usize,
    pub total: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected: Option<Vec<usize>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpDetail {
    #[serde(default)]
    pub streams: Vec<HttpStreamState>,
    pub peak_streams: usize,
    pub total_blocks: usize,
    pub block_size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_limited: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_browser: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentDetail {
    #[serde(default)]
    pub peers: Vec<TorrentPeerState>,
    pub peak_peers: usize,
    pub total_pieces: usize,
    pub piece_length: u64,
    pub files: TorrentFiles,
    pub folder: bool,
    pub skipped_bytes: u64,
    pub bytes_uploaded: u64,
    pub upload_speed_bytes_per_sec: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Detail {
    Http(HttpDetail),
    Torrent(TorrentDetail),
}

/// A download, without its blocks or pieces (they travel separately; see DownloadUpdate).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadState {
    pub id: String,
    pub url: String,
    pub file_name: String,
    pub destination_path: String,
    /// 0 means the size could not be determined ahead of time.
    pub total_bytes: u64,
    pub bytes_downloaded: u64,
    pub speed_bytes_per_sec: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_left_seconds: Option<f64>,
    pub status: DownloadStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resumable: Option<bool>,
    pub started_at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_paused_ms: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_history: Option<BTreeMap<String, Vec<f64>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_speed_bytes_per_sec: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub networks: Vec<DownloadNetwork>,
    #[serde(flatten)]
    pub detail: Detail,
}

impl DownloadState {
    pub fn http(&self) -> Option<&HttpDetail> {
        match &self.detail {
            Detail::Http(detail) => Some(detail),
            Detail::Torrent(_) => None,
        }
    }

    pub fn http_mut(&mut self) -> Option<&mut HttpDetail> {
        match &mut self.detail {
            Detail::Http(detail) => Some(detail),
            Detail::Torrent(_) => None,
        }
    }

    pub fn torrent(&self) -> Option<&TorrentDetail> {
        match &self.detail {
            Detail::Torrent(detail) => Some(detail),
            Detail::Http(_) => None,
        }
    }

    pub fn torrent_mut(&mut self) -> Option<&mut TorrentDetail> {
        match &mut self.detail {
            Detail::Torrent(detail) => Some(detail),
            Detail::Http(_) => None,
        }
    }

    pub fn kind(&self) -> TransferKind {
        match self.detail {
            Detail::Http(_) => TransferKind::Http,
            Detail::Torrent(_) => TransferKind::Torrent,
        }
    }

    pub fn skipped_bytes(&self) -> u64 {
        self.torrent().map_or(0, |detail| detail.skipped_bytes)
    }
}

/// A finished download as history keeps it: its state without its work units or connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FinishedDownload {
    #[serde(flatten)]
    pub state: DownloadState,
    pub units_written: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub missing: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downloaded_files: Option<Vec<String>>,
}

/// What the engine sends as a download changes: everything but its units, and only the units
/// that changed since it last sent. A snapshot is the same with every unit in it.
#[derive(Debug, Clone, Serialize)]
pub struct DownloadUpdate {
    pub seq: u64,
    pub state: DownloadState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks: Option<Vec<UnitState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pieces: Option<Vec<UnitState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataLimitPeriod {
    Day,
    Week,
    Month,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPreference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub off: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_limit_period: Option<DataLimitPeriod>,
}

pub type NetworkPreferences = BTreeMap<String, NetworkPreference>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateState {
    pub status: String,
    pub auto_update: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub percent: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DownloadSchedule {
    pub enabled: bool,
    /// Minutes since local midnight.
    pub start_minute: u32,
    pub end_minute: u32,
    /// Days on which a window starts, Sunday = 0.
    pub days: Vec<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_date: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThemeSource {
    Light,
    Dark,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpeedUnit {
    Bytes,
    Bits,
}

/// What app-settings.json holds. A missing field was never set.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub download_schedule: Option<DownloadSchedule>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub theme_source: Option<ThemeSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_update: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_preferences: Option<NetworkPreferences>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downloads_at_once: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slow_mode: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slow_mode_speed: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_unit: Option<SpeedUnit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser_extension_used: Option<bool>,
}

pub const DOWNLOADS_AT_ONCE_DEFAULT: u32 = 2;
pub const DOWNLOADS_AT_ONCE_MIN: u32 = 1;
pub const DOWNLOADS_AT_ONCE_MAX: u32 = 4;
/// Seconds of speed history a download keeps (a sample a second).
pub const SPEED_HISTORY_SECONDS: usize = 60;
pub const DEFAULT_SLOW_MODE_SPEED: u64 = 2 * 1024 * 1024;

/// Everything the renderer needs for its first paint.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitialState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_schedule: Option<DownloadSchedule>,
    pub home_dir: String,
    pub downloads_dir: String,
    pub theme_source: ThemeSource,
    pub network_preferences: NetworkPreferences,
    pub downloads_at_once: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed_limit: Option<u64>,
    pub slow_mode: bool,
    pub slow_mode_speed: u64,
    pub speed_unit: SpeedUnit,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_dir: Option<String>,
}

/// The fields of chrome.cookies.Cookie that decide where it's sent.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserCookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub host_only: bool,
    pub path: String,
    pub secure: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserContext {
    pub cookies: Vec<BrowserCookie>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub referer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkResumes {
    pub id: String,
    #[serde(rename = "fileName")]
    pub file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingLink {
    pub id: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe: Option<ProbeResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_in_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumes: Option<LinkResumes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartDownloadRequest {
    pub kind: TransferKind,
    pub url: String,
    pub destination_dir: String,
    pub suggested_file_name: String,
    /// 0 means unknown.
    pub total_bytes: u64,
    pub supports_ranges: bool,
    /// The networks to start on. Every other one starts switched off.
    pub interface_ids: Vec<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// HTTP: streams per network the user picked; left out, the count is decided automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams_per_network: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser: Option<BrowserContext>,
    /// Torrent: the download starts from the .torrent retained when this info hash was probed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub info_hash: Option<String>,
    /// Torrent: the files to download, as indexes into its probe's `files`. Left out: all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_files: Option<Vec<usize>>,
}
