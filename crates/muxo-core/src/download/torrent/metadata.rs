//! What a .torrent holds, and fetching one: from a file, a link, or a magnet link's peers.

use crate::types::{MagnetCheckStatus, ProbeResult, TorrentFile, TorrentInfo, TransferKind};
use bytes::Bytes;
use librqbit::{AddTorrent, AddTorrentOptions, AddTorrentResponse, Session, SessionOptions};
use librqbit::ByteBufOwned;
use parking_lot::Mutex;
use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

/// A .torrent file is metadata only; a link answering with more than this isn't one.
const MAX_TORRENT_FILE_BYTES: u64 = 10 * 1024 * 1024;
const TOO_LARGE: &str = "Muxo supports .torrent files up to 10 MB.";
const NO_PEERS: &str = "No peers responded to this magnet link. Try again later or open a .torrent file.";
const MAGNET_TIMEOUT: Duration = Duration::from_secs(180);
/// How long a magnet link looks before the window is told nobody has answered yet.
const MAGNET_QUIET: Duration = Duration::from_secs(20);
const RECENT_LIMIT: usize = 8;
const MAX_NAME_BYTES: usize = 255;

pub type MagnetListener = Arc<dyn Fn(MagnetCheckStatus) + Send + Sync>;

/// The torrent engine: one librqbit session for the app, and the .torrent of each torrent probed
/// lately, by info hash, which is what a download of it starts from.
pub struct TorrentProbe {
    session_dir: PathBuf,
    session: OnceCell<Arc<Session>>,
    recent: Mutex<VecDeque<(String, Bytes)>>,
    listener: Mutex<Option<MagnetListener>>,
}

/// Where each of a torrent's files will be written, relative to the destination folder (its own
/// folder first), or an error when any could land outside it, collide with another, or can't be
/// written on this system. A torrent's paths come from whoever made it.
pub fn safe_torrent_paths(files: Vec<(Vec<String>, u64)>) -> anyhow::Result<Vec<TorrentFile>> {
    if files.is_empty() {
        anyhow::bail!("This torrent has no files");
    }
    let mut checked = Vec::new();
    for (components, length) in files {
        for segment in &components {
            if segment.is_empty() || segment == "." || segment == ".." || segment.contains(['/', '\\', '\0']) {
                anyhow::bail!("This torrent has a file Muxo can't save safely: {}", components.join("/"));
            }
            if segment.len() > MAX_NAME_BYTES {
                anyhow::bail!("A name in this torrent is too long to save: {segment}");
            }
            if cfg!(windows) {
                let reserved = regex::Regex::new(r"(?i)^(con|prn|aux|nul|com\d|lpt\d)(\..*)?$").unwrap();
                if reserved.is_match(segment) || segment.contains(['<', '>', ':', '"', '|', '?', '*']) || segment.ends_with(['.', ' ']) {
                    anyhow::bail!("Windows can't save a file named {segment}");
                }
            }
        }
        checked.push(TorrentFile { path: components.join(std::path::MAIN_SEPARATOR_STR), length });
    }
    let taken: HashSet<String> = checked.iter().map(|file| file.path.to_lowercase()).collect();
    if taken.len() != checked.len() {
        anyhow::bail!("Two files in this torrent would be saved as the same file");
    }
    for file in &checked {
        let segments: Vec<&str> = file.path.split(std::path::MAIN_SEPARATOR).collect();
        for end in 1..segments.len() {
            if taken.contains(&segments[..end].join(std::path::MAIN_SEPARATOR_STR).to_lowercase()) {
                anyhow::bail!("A file in this torrent has the same name as a folder in it");
            }
        }
    }
    Ok(checked)
}

/// A .torrent file on this computer.
pub async fn read_torrent_file(path: &Path) -> anyhow::Result<Bytes> {
    if tokio::fs::metadata(path).await?.len() > MAX_TORRENT_FILE_BYTES {
        anyhow::bail!(TOO_LARGE);
    }
    Ok(Bytes::from(tokio::fs::read(path).await?))
}

/// A .torrent file a link points at.
pub async fn download_torrent_file(url: &str, timeout: Duration, headers: &[(String, String)]) -> anyhow::Result<Bytes> {
    let client = reqwest::Client::builder().timeout(timeout).no_proxy().build()?;
    let mut request = client.get(url);
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let mut response = request.send().await?;
    if !response.status().is_success() {
        anyhow::bail!("Server responded with status {}", response.status().as_u16());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        body.extend_from_slice(&chunk);
        if body.len() as u64 > MAX_TORRENT_FILE_BYTES {
            anyhow::bail!(TOO_LARGE);
        }
    }
    Ok(Bytes::from(body))
}

impl TorrentProbe {
    pub fn new(data_dir: &Path) -> Arc<Self> {
        Arc::new(TorrentProbe {
            session_dir: data_dir.join("torrents"),
            session: OnceCell::new(),
            recent: Mutex::new(VecDeque::new()),
            listener: Mutex::new(None),
        })
    }

    pub fn set_listener(&self, listener: MagnetListener) {
        *self.listener.lock() = Some(listener);
    }

    /// The app's one torrent session, started on first use.
    pub async fn session(&self) -> anyhow::Result<Arc<Session>> {
        self.session
            .get_or_try_init(|| async {
                tokio::fs::create_dir_all(&self.session_dir).await?;
                Session::new_with_opts(
                    self.session_dir.clone(),
                    SessionOptions {
                        disable_dht_persistence: true,
                        fastresume: false,
                        persistence: None,
                        listen_port_range: Some(6881..6900),
                        enable_upnp_port_forwarding: false,
                        ..Default::default()
                    },
                )
                .await
            })
            .await
            .cloned()
    }

    pub fn probed_torrent_file(&self, info_hash: &str) -> Option<Bytes> {
        self.recent.lock().iter().find(|(hash, _)| hash == info_hash).map(|(_, bytes)| bytes.clone())
    }

    fn remember(&self, info_hash: &str, bytes: &Bytes) {
        let mut recent = self.recent.lock();
        recent.retain(|(hash, _)| hash != info_hash);
        recent.push_back((info_hash.to_owned(), bytes.clone()));
        while recent.len() > RECENT_LIMIT {
            recent.pop_front();
        }
    }

    /// The parsed torrent, its files' safe paths, once they are known to be safe to write.
    pub fn info(&self, bytes: &Bytes) -> anyhow::Result<(TorrentInfo, String)> {
        let parsed = librqbit::torrent_from_bytes::<ByteBufOwned>(bytes)
            .map_err(|_| anyhow::anyhow!("This isn’t a torrent Muxo can read"))?;
        let info_hash = parsed.info_hash.as_string();
        let name = parsed
            .info
            .name
            .as_ref()
            .and_then(|name| std::str::from_utf8(name.as_ref()).ok())
            .map(str::to_owned)
            .unwrap_or_else(|| info_hash.clone());
        let multi = parsed.info.files.is_some();
        let mut files = Vec::new();
        for details in parsed.info.iter_file_details()? {
            let mut components = details.filename.to_vec()?;
            if multi {
                components.insert(0, name.clone());
            }
            files.push((components, details.len));
        }
        let files = safe_torrent_paths(files)?;
        Ok((TorrentInfo { info_hash, piece_length: parsed.info.piece_length as u64, files }, name))
    }

    /// What a .torrent holds, as a probe result.
    pub fn describe(&self, bytes: &Bytes, link: &str) -> anyhow::Result<ProbeResult> {
        let (torrent, name) = self.info(bytes)?;
        self.remember(&torrent.info_hash, bytes);
        Ok(ProbeResult {
            kind: TransferKind::Torrent,
            requested_url: link.to_owned(),
            final_url: link.to_owned(),
            supports_ranges: true,
            total_bytes: Some(torrent.files.iter().map(|file| file.length).sum()),
            suggested_file_name: name,
            content_type: Some("application/x-bittorrent".into()),
            etag: None,
            last_modified: None,
            torrent: Some(torrent),
        })
    }

    /// A magnet link's .torrent, from whichever of its peers answers first.
    pub async fn fetch_magnet_metadata(&self, magnet: &str) -> anyhow::Result<Bytes> {
        if librqbit::Magnet::parse(magnet).is_err() {
            anyhow::bail!("This magnet link isn’t valid");
        }
        let session = self.session().await?;
        let lookup = session.add_torrent(
            AddTorrent::from_url(magnet.to_owned()),
            Some(AddTorrentOptions { list_only: true, ..Default::default() }),
        );
        tokio::pin!(lookup);
        let quiet = tokio::time::sleep(MAGNET_QUIET);
        tokio::pin!(quiet);
        let deadline = tokio::time::sleep(MAGNET_TIMEOUT);
        tokio::pin!(deadline);
        let mut told = false;
        loop {
            tokio::select! {
                result = &mut lookup => {
                    if told {
                        self.tell(magnet, false);
                    }
                    return match result? {
                        AddTorrentResponse::ListOnly(response) => Ok(response.torrent_bytes),
                        _ => anyhow::bail!(NO_PEERS),
                    };
                }
                _ = &mut quiet, if !told => {
                    told = true;
                    self.tell(magnet, true);
                }
                _ = &mut deadline => anyhow::bail!(NO_PEERS),
            }
        }
    }

    fn tell(&self, link: &str, no_peers_yet: bool) {
        if let Some(listener) = self.listener.lock().clone() {
            listener(MagnetCheckStatus { link: link.to_owned(), no_peers_yet });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_unsafe_paths() {
        assert!(safe_torrent_paths(vec![(vec!["a".into(), "..".into()], 1)]).is_err());
        assert!(safe_torrent_paths(vec![(vec!["a".into()], 1), (vec!["A".into()], 1)]).is_err());
        assert!(safe_torrent_paths(vec![(vec!["a".into()], 1), (vec!["a".into(), "b".into()], 1)]).is_err());
        assert_eq!(safe_torrent_paths(vec![(vec!["d".into(), "f".into()], 3)]).unwrap()[0].length, 3);
    }
}
