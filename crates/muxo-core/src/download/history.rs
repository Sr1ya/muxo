//! Finished downloads, newest first: what the window lists under Finished. A download moves here
//! once it completes, leaving its work units behind, so a long history costs launch nothing.

use crate::types::FinishedDownload;
use crate::util::{read_json, update_json};
use serde_json::Value;
use std::path::{Path, PathBuf};

const MAX_ENTRIES: usize = 500;

pub struct History {
    path: PathBuf,
}

/// The file may be hand-edited or from another version: an entry is kept only if it reads.
fn entries_of(parsed: Option<Value>) -> Vec<FinishedDownload> {
    match parsed {
        Some(Value::Array(entries)) => {
            entries.into_iter().filter_map(|entry| serde_json::from_value(entry).ok()).collect()
        }
        _ => Vec::new(),
    }
}

async fn mark_missing(mut entry: FinishedDownload) -> FinishedDownload {
    entry.missing = Some(tokio::fs::metadata(&entry.state.destination_path).await.is_err());
    entry
}

impl History {
    pub fn new(data_dir: &Path) -> Self {
        History { path: data_dir.join("history.json") }
    }

    /// Every finished download, newest first, each marked `missing` when its file is gone.
    pub async fn list(&self) -> Vec<FinishedDownload> {
        let entries = entries_of(read_json(&self.path).await.ok().flatten());
        futures::future::join_all(entries.into_iter().map(mark_missing)).await
    }

    pub async fn find(&self, id: &str) -> Option<FinishedDownload> {
        let entry = entries_of(read_json(&self.path).await.ok().flatten()).into_iter().find(|e| e.state.id == id)?;
        Some(mark_missing(entry).await)
    }

    pub async fn add(&self, mut entry: FinishedDownload) -> std::io::Result<()> {
        entry.missing = None;
        update_json(&self.path, move |current| {
            let id = entry.state.id.clone();
            let mut entries = vec![entry];
            entries.extend(entries_of(current).into_iter().filter(|other| other.state.id != id));
            entries.truncate(MAX_ENTRIES);
            serde_json::to_value(entries).unwrap_or(Value::Array(vec![]))
        })
        .await
    }

    /// Forgets the entries with these ids; none given, every one. The files themselves stay.
    pub async fn remove(&self, ids: Option<Vec<String>>) -> std::io::Result<()> {
        update_json(&self.path, move |current| {
            let kept: Vec<FinishedDownload> = match ids {
                Some(ids) => entries_of(current).into_iter().filter(|e| !ids.contains(&e.state.id)).collect(),
                None => Vec::new(),
            };
            serde_json::to_value(kept).unwrap_or(Value::Array(vec![]))
        })
        .await
    }
}
