//! Where librqbit writes a torrent: each file at the path Muxo claimed for it, with every write
//! and verified piece reported, so the block grid shows real progress.

use crate::download::download_file::{read_at, write_at};
use librqbit::storage::{StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata};
use librqbit_core::lengths::ValidPieceIndex;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// What librqbit has done, for the engine to read on its clock.
pub struct PieceObserver {
    inner: Mutex<Observed>,
    piece_length: u64,
}

pub struct Observed {
    /// Pieces verified this run.
    pub completed: Vec<bool>,
    /// Bytes written this run to pieces not yet verified.
    pub provisional: Vec<u64>,
}

impl PieceObserver {
    pub fn new(pieces: usize, piece_length: u64) -> Arc<Self> {
        Arc::new(PieceObserver {
            inner: Mutex::new(Observed { completed: vec![false; pieces], provisional: vec![0; pieces] }),
            piece_length: piece_length.max(1),
        })
    }

    pub fn wrote(&self, absolute_offset: u64, bytes: u64) {
        let index = (absolute_offset / self.piece_length) as usize;
        let mut inner = self.inner.lock();
        if let Some(provisional) = inner.provisional.get_mut(index) {
            *provisional = (*provisional + bytes).min(self.piece_length);
        }
    }

    pub fn completed(&self, index: usize) {
        let mut inner = self.inner.lock();
        if let Some(done) = inner.completed.get_mut(index) {
            *done = true;
            inner.provisional[index] = 0;
        }
    }

    /// A run starting over: what wasn't verified is forgotten.
    pub fn reset_provisional(&self) {
        self.inner.lock().provisional.iter_mut().for_each(|bytes| *bytes = 0);
    }

    pub fn read<R>(&self, f: impl FnOnce(&Observed) -> R) -> R {
        f(&self.inner.lock())
    }
}

#[derive(Clone)]
pub struct MuxoStorageFactory {
    pub files: Arc<Vec<PathBuf>>,
    pub observer: Arc<PieceObserver>,
}

impl MuxoStorageFactory {
    pub fn boxed_factory(self) -> librqbit::storage::BoxStorageFactory {
        self.boxed()
    }
}

#[derive(Clone)]
pub struct MuxoStorage {
    files: Arc<Vec<PathBuf>>,
    handles: Arc<Vec<Mutex<Option<Arc<File>>>>>,
    offsets: Arc<Vec<u64>>,
    observer: Arc<PieceObserver>,
}

impl StorageFactory for MuxoStorageFactory {
    type Storage = MuxoStorage;

    fn create(&self, _shared: &ManagedTorrentShared, metadata: &TorrentMetadata) -> anyhow::Result<MuxoStorage> {
        let offsets: Vec<u64> = metadata.file_infos.iter().map(|file| file.offset_in_torrent).collect();
        if offsets.len() != self.files.len() {
            anyhow::bail!("Invalid torrent metadata: its files changed");
        }
        Ok(MuxoStorage {
            files: Arc::clone(&self.files),
            handles: Arc::new(self.files.iter().map(|_| Mutex::new(None)).collect()),
            offsets: Arc::new(offsets),
            observer: Arc::clone(&self.observer),
        })
    }

    fn clone_box(&self) -> librqbit::storage::BoxStorageFactory {
        self.clone().boxed()
    }
}

impl MuxoStorage {
    fn handle(&self, file_id: usize, create: bool) -> anyhow::Result<Arc<File>> {
        let mut slot = self.handles.get(file_id).ok_or_else(|| anyhow::anyhow!("no file {file_id}"))?.lock();
        if let Some(file) = slot.as_ref() {
            return Ok(Arc::clone(file));
        }
        let path = &self.files[file_id];
        if create {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = Arc::new(OpenOptions::new().read(true).write(true).create(create).truncate(false).open(path)?);
        *slot = Some(Arc::clone(&file));
        Ok(file)
    }
}

impl TorrentStorage for MuxoStorage {
    fn init(&mut self, _shared: &ManagedTorrentShared, _metadata: &TorrentMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let file = self.handle(file_id, false)?;
        if read_at(&file, buf, offset)? != buf.len() {
            anyhow::bail!("short read");
        }
        Ok(())
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        let file = self.handle(file_id, true)?;
        write_at(&file, buf, offset)?;
        self.observer.wrote(self.offsets[file_id] + offset, buf.len() as u64);
        Ok(())
    }

    fn remove_file(&self, file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        if let Some(slot) = self.handles.get(file_id) {
            slot.lock().take();
        }
        match std::fs::remove_file(&self.files[file_id]) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        let file = self.handle(file_id, true)?;
        if file.metadata()?.len() != length {
            file.set_len(length)?;
        }
        Ok(())
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(self.clone()))
    }

    fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        self.observer.completed(piece_index.get_usize());
        Ok(())
    }
}
