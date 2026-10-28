//! The only large file a download owns. It lives beside the final file so publishing it needs no
//! copy and never a second file's worth of disk space.

use crate::download::paths::{partial_path, split_extension, truncate_bytes};
use crate::util::path_exists;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DownloadFile {
    pub path: PathBuf,
}

/// Positioned writes and reads, never append mode: every writer has its own offset.
pub fn write_at(file: &std::fs::File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let written = std::os::unix::fs::FileExt::write_at(file, buf, offset)?;
        #[cfg(windows)]
        let written = std::os::windows::fs::FileExt::seek_write(file, buf, offset)?;
        if written == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write the download"));
        }
        buf = &buf[written..];
        offset += written as u64;
    }
    Ok(())
}

pub fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        #[cfg(unix)]
        let read = std::os::unix::fs::FileExt::read_at(file, &mut buf[filled..], offset + filled as u64)?;
        #[cfg(windows)]
        let read = std::os::windows::fs::FileExt::seek_read(file, &mut buf[filled..], offset + filled as u64)?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}

impl DownloadFile {
    pub fn new(path: PathBuf) -> Self {
        DownloadFile { path }
    }

    pub fn open_write(&self) -> io::Result<std::fs::File> {
        std::fs::OpenOptions::new().write(true).open(&self.path)
    }

    pub async fn read(&self, position: u64, length: usize) -> io::Result<Vec<u8>> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let file = std::fs::File::open(path)?;
            let mut buf = vec![0; length];
            let read = read_at(&file, &mut buf, position)?;
            buf.truncate(read);
            Ok(buf)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Flushes the staging file before a recovery checkpoint or final publication.
    pub async fn sync(&self) -> io::Result<()> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || std::fs::OpenOptions::new().write(true).open(path)?.sync_all())
            .await
            .map_err(io::Error::other)?
    }

    /// Bytes on disk; an error when the file is gone.
    pub async fn size(&self) -> io::Result<u64> {
        Ok(tokio::fs::metadata(&self.path).await?.len())
    }

    /// Moves the finished file to `destination` or the first free `name (n).ext` beside it.
    /// `before_attempt` runs before each rename (it saves the recovery intent).
    pub async fn publish<F, Fut>(&self, destination: &Path, expected_bytes: u64, before_attempt: F) -> io::Result<PathBuf>
    where
        F: Fn(PathBuf) -> Fut,
        Fut: std::future::Future<Output = io::Result<()>>,
    {
        if expected_bytes > 0 && self.size().await? != expected_bytes {
            return Err(io::Error::other("Download file size does not match the expected size"));
        }
        self.sync().await?;
        let name = destination.file_name().and_then(|n| n.to_str()).unwrap_or("download").to_owned();
        let (stem, extension) = split_extension(&name);
        let directory = destination.parent().unwrap_or(Path::new("."));
        for index in 0..10_000 {
            let suffix = if index == 0 { String::new() } else { format!(" ({index})") };
            if suffix.len() + extension.len() >= 255 {
                return Err(io::Error::other("The file extension is too long"));
            }
            let candidate_stem = truncate_bytes(stem, 255 - suffix.len() - extension.len());
            let candidate = directory.join(format!("{candidate_stem}{suffix}{extension}"));
            if candidate != destination && path_exists(&partial_path(&candidate)).await? {
                continue;
            }
            before_attempt(candidate.clone()).await?;
            // Checked immediately before the rename, after the recovery intent is saved.
            if path_exists(&candidate).await? {
                continue;
            }
            match tokio::fs::rename(&self.path, &candidate).await {
                Ok(()) => return Ok(candidate),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::other("Could not find an unused file name for this download"))
    }

    pub async fn discard(&self) -> io::Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}
