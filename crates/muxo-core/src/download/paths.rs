//! Claiming a destination name for a download.

use crate::util::path_exists;
use regex::Regex;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

// Somewhere to stop rather than spin forever if every candidate is taken.
const MAX_NAME_ATTEMPTS: usize = 10_000;
// Longer than any real extension. Past this, what follows the last dot is part of a title.
const MAX_EXTENSION_BYTES: usize = 32;

/// A server-provided name is one component, never a path or NTFS data stream.
pub fn sanitize_file_name(file_name: &str) -> String {
    let mut safe: String =
        file_name.chars().map(|c| if c == '/' || c == '\\' || c.is_control() { '_' } else { c }).collect();
    if cfg!(windows) {
        static RESERVED: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(?i)^(con|prn|aux|nul|com[1-9¹²³]|lpt[1-9¹²³])(?:\.|$)").unwrap());
        safe = safe.chars().map(|c| if "<>:\"|?*".contains(c) { '_' } else { c }).collect();
        safe = safe.trim_end_matches([' ', '.']).to_owned();
        if RESERVED.is_match(&safe) {
            safe = format!("_{safe}");
        }
    }
    if safe.is_empty() || safe.chars().all(|c| c == '.') { "download".into() } else { safe }
}

pub async fn ensure_directory(directory: &Path) -> io::Result<()> {
    match tokio::fs::create_dir_all(directory).await {
        Ok(()) => Ok(()),
        // Windows refuses to mkdir a drive root.
        Err(error) if tokio::fs::metadata(directory).await.is_ok_and(|meta| meta.is_dir()) => {
            let _ = error;
            Ok(())
        }
        Err(error) => Err(error),
    }
}

/// `name` split into a stem and an extension (with its dot), as path.extname does.
pub fn split_extension(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(dot) if dot > 0 => name.split_at(dot),
        _ => (name, ""),
    }
}

/// Drops characters off the end until `text` fits `max_bytes`.
pub fn truncate_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// The first of `name`, `name (1)`, `name (2)`… in `directory` that is free and that `claim`
/// takes: `claim` returns false to pass on a name. `suffix` is what claiming adds to a name, room
/// left for it within the 255-byte component limit.
async fn claim_name<F, Fut>(directory: &Path, name: &str, suffix: &str, extension: bool, claim: F) -> io::Result<PathBuf>
where
    F: Fn(PathBuf) -> Fut,
    Fut: std::future::Future<Output = io::Result<bool>>,
{
    ensure_directory(directory).await?;
    let name = sanitize_file_name(name);
    let (stem, mut ext) = if extension { split_extension(&name) } else { (name.as_str(), "") };
    if ext.len() > MAX_EXTENSION_BYTES {
        ext = "";
    }
    let stem = if ext.is_empty() { name.as_str() } else { stem };
    let max_base = 255usize.saturating_sub(format!(" (9999){suffix}").len() + ext.len());
    let base = truncate_bytes(stem, max_base);
    for counter in 0..MAX_NAME_ATTEMPTS {
        let candidate = if counter == 0 {
            directory.join(format!("{base}{ext}"))
        } else {
            directory.join(format!("{base} ({counter}){ext}"))
        };
        if path_exists(&candidate).await? {
            continue;
        }
        match claim(candidate.clone()).await {
            Ok(true) => return Ok(candidate),
            Ok(false) => continue,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other(format!("Could not find an unused file name for \"{name}\" in {}", directory.display())))
}

pub fn partial_path(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_owned();
    name.push(".muxo");
    PathBuf::from(name)
}

/// Claims a download by exclusively creating `<final name>.muxo`. The final name itself doesn't
/// appear until the file is complete. An existing partial also claims its final name.
pub async fn reserve_destination_path(directory: &Path, file_name: &str) -> io::Result<PathBuf> {
    claim_name(directory, file_name, ".muxo", true, |candidate| async move {
        let partial = partial_path(&candidate);
        tokio::fs::OpenOptions::new().write(true).create_new(true).open(&partial).await?;
        if !path_exists(&candidate).await? {
            return Ok(true);
        }
        tokio::fs::remove_file(&partial).await?;
        Ok(false)
    })
    .await
}

/// Claims the final name itself, by exclusively creating it empty: a torrent is written in place,
/// as torrent clients do. Being new, it's this download's alone.
pub async fn claim_destination_path(directory: &Path, name: &str, folder: bool) -> io::Result<PathBuf> {
    claim_name(directory, name, "", !folder, |candidate| async move {
        if folder {
            tokio::fs::create_dir(&candidate).await?;
        } else {
            tokio::fs::OpenOptions::new().write(true).create_new(true).open(&candidate).await?;
        }
        Ok(true)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserves_the_next_free_name() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(dir.path().join("a.txt"), b"x").await.unwrap();
        let first = reserve_destination_path(dir.path(), "a.txt").await.unwrap();
        assert_eq!(first, dir.path().join("a (1).txt"));
        assert!(dir.path().join("a (1).txt.muxo").exists());
        let second = reserve_destination_path(dir.path(), "a.txt").await.unwrap();
        assert_eq!(second, dir.path().join("a (2).txt"));
        assert_eq!(sanitize_file_name("../etc/passwd"), ".._etc_passwd");
    }
}
