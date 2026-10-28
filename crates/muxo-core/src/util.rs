use parking_lot::Mutex;
use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Milliseconds since the epoch, as Date.now() reads them.
pub fn now_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as f64)
        .unwrap_or(0.0)
}

/// An I/O error as Node words it ("ENOSPC: …"), so the window's describeError recognizes it and
/// shows its plain-English hint.
pub fn io_message(error: &io::Error) -> String {
    let code = match error.kind() {
        io::ErrorKind::StorageFull => "ENOSPC",
        io::ErrorKind::QuotaExceeded => "EDQUOT",
        io::ErrorKind::PermissionDenied => "EACCES",
        io::ErrorKind::NotFound => "ENOENT",
        io::ErrorKind::ReadOnlyFilesystem => "EROFS",
        io::ErrorKind::AlreadyExists => "EEXIST",
        _ if error.raw_os_error() == Some(5) => "EIO",
        _ => return error.to_string(),
    };
    format!("{code}: {error}")
}

fn write_locks() -> &'static Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(Default::default)
}

fn lock_for(path: &Path) -> Arc<tokio::sync::Mutex<()>> {
    write_locks().lock().entry(path.to_path_buf()).or_default().clone()
}

/// Parsed contents, or None when the file is missing (first run) or corrupt. Any other failure
/// (a lock, too many open files) is an error, so a save never overwrites what it couldn't read.
/// Waits for queued writes first, so a read right after a save sees it.
pub async fn read_json(path: &Path) -> io::Result<Option<Value>> {
    let lock = lock_for(path);
    let _guard = lock.lock().await;
    read_json_now(path).await
}

async fn read_json_now(path: &Path) -> io::Result<Option<Value>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// On Windows, antivirus/indexers/sync clients briefly hold files open, which fails a rename
/// over them: worth a few short retries before giving up on the save.
pub async fn rename_with_retry(from: &Path, to: &Path) -> io::Result<()> {
    let mut attempt = 0;
    loop {
        match tokio::fs::rename(from, to).await {
            Ok(()) => return Ok(()),
            Err(error)
                if attempt < 4
                    && matches!(
                        error.kind(),
                        io::ErrorKind::PermissionDenied | io::ErrorKind::ResourceBusy
                    ) =>
            {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(50 * attempt)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// Read-modify-write, queued per file so two quick saves can't drop each other's change, and
/// written via a temp file + rename so a crash mid-write can't leave a half-written file.
pub async fn update_json<F>(path: &Path, update: F) -> io::Result<()>
where
    F: FnOnce(Option<Value>) -> Value,
{
    let lock = lock_for(path);
    let _guard = lock.lock().await;
    let value = update(read_json_now(path).await?);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    let text = serde_json::to_vec_pretty(&value).map_err(io::Error::other)?;
    tokio::fs::write(&temporary, text).await?;
    rename_with_retry(&temporary, path).await
}

pub fn is_record(value: &Value) -> bool {
    value.is_object()
}

/// A rate or an amount of bytes: a positive whole number, or nothing.
pub fn byte_count(value: Option<&Value>) -> Option<u64> {
    value.and_then(Value::as_u64).filter(|count| *count > 0 && *count <= (1u64 << 53))
}

/// Exists, without following a symlink at the end.
pub async fn path_exists(path: &Path) -> io::Result<bool> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Bytes free on the drive holding `dir`; None when it can't be told.
pub fn free_space(dir: &Path) -> Option<u64> {
    fs4::available_space(dir).ok()
}
