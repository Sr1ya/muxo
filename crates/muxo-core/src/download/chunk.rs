//! Downloads a single byte range of a URL, bound to one network, into the staging file.
//!
//! Succeeding means the *entire* requested range was written, and nothing else was: a block's
//! bytes land at a fixed offset, so a response that is short, starts somewhere else, or overruns
//! the range would corrupt the output rather than just this block. Every one of those is an error,
//! which puts the block back on the queue for a retry instead of marking it done.

use crate::download::browser_context::request_headers;
use crate::download::download_file::write_at;
use crate::download::file_version::{compare_version, FileVersion, VersionCheck};
use crate::network::routes::connection_error;
use crate::network::{FetchError, StreamConnection};
use crate::types::BrowserContext;
use crate::util::io_message;
use bytes::Bytes;
use regex::Regex;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;
use url::Url;

const MAX_REDIRECTS: usize = 5;
/// Per stream: 32 streams hold at most 16 MiB. A full buffer is also what tells Auto the disk is
/// behind (see concurrency.rs): larger, and it hears it later.
const WRITE_BUFFER_BYTES: usize = 512 * 1024;
/// Gathered into one write, so a disk that falls behind gets fewer, larger writes.
const WRITE_COALESCE_BYTES: usize = 128 * 1024;

pub trait ChunkHooks: Send + Sync {
    /// The server answered with usable headers.
    fn on_response(&self, _ttfb_ms: f64) {}
    /// Network bytes received this run, before the disk has them.
    fn on_network_progress(&self, received: u64);
    /// Bytes written to the file this run: safe to count as resumable progress.
    fn on_progress(&self, written: u64);
    /// Whether delivered bytes wait on the disk.
    fn on_write_wait(&self, waiting: bool);
    /// Told of every body byte received; answers how many ms to stop reading for.
    fn throttle(&self, bytes: u64) -> f64;
}

pub struct ChunkRequest<'a> {
    pub url: &'a str,
    pub browser: Option<&'a BrowserContext>,
    pub range_start: u64,
    pub range_end: Option<u64>,
    /// The server doesn't take range requests: ask for the whole file without a Range header.
    pub whole_file: bool,
    pub connection: &'a StreamConnection,
    pub file: Arc<std::fs::File>,
    pub cancel: &'a CancellationToken,
    pub accepted: &'a [FileVersion],
    pub stall_timeout: Duration,
    pub hooks: Arc<dyn ChunkHooks>,
}

#[derive(Debug, Clone, Copy)]
struct ServedRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(value: Option<&str>) -> Option<ServedRange> {
    static PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*$").unwrap());
    let captures = PATTERN.captures(value?)?;
    let start: u64 = captures[1].parse().ok()?;
    let end: u64 = captures[2].parse().ok()?;
    (end >= start).then(|| ServedRange { start, end, total: captures[3].parse().ok() })
}

pub fn header(response: &reqwest::Response, name: &str) -> Option<String> {
    response.headers().get(name).and_then(|value| value.to_str().ok()).map(str::to_owned)
}

/// Retry-After (RFC 9110 §10.2.3), in seconds or as an HTTP date, as milliseconds from now.
pub fn retry_after_ms(value: Option<&str>) -> Option<u64> {
    let trimmed = value?.trim();
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Some(seconds * 1000);
    }
    let at = chrono::DateTime::parse_from_rfc2822(trimmed).ok()?;
    Some((at.timestamp_millis() - chrono::Utc::now().timestamp_millis()).max(0) as u64)
}

/// The version a response says it's from. A 206 states the total size in Content-Range; a 200 is
/// the whole file, so its length is the size.
fn version_of(response: &reqwest::Response, served: Option<ServedRange>) -> FileVersion {
    let length = header(response, "content-length").and_then(|length| length.parse().ok());
    FileVersion {
        etag: header(response, "etag"),
        last_modified: header(response, "last-modified"),
        total_bytes: served
            .and_then(|served| served.total)
            .or(if response.status().as_u16() == 200 { length } else { None })
            .unwrap_or(0),
    }
}

/// Follows redirects for a GET with `range`, the browser's headers re-derived for each hop.
async fn request_following(
    connection: &StreamConnection,
    url: &str,
    browser: Option<&BrowserContext>,
    range: Option<String>,
    cancel: &CancellationToken,
) -> Result<reqwest::Response, FetchError> {
    let mut target = Url::parse(url).map_err(|error| FetchError::Other(format!("Invalid URL: {error}")))?;
    for redirects_left in (0..=MAX_REDIRECTS).rev() {
        let mut headers = request_headers(&target, browser);
        if let Some(range) = &range {
            headers.push(("Range".into(), range.clone()));
        }
        let response = connection.request(&target, &headers, cancel).await?;
        if !response.status().is_redirection() {
            return Ok(response);
        }
        let location = header(&response, "location");
        match location.and_then(|location| target.join(&location).ok()) {
            Some(next) if redirects_left > 0 => target = next,
            _ => return Err(FetchError::Other("Too many redirects for range request".into())),
        }
    }
    unreachable!("the last pass always returns")
}

struct Writer {
    sender: Option<mpsc::UnboundedSender<(Bytes, OwnedSemaphorePermit)>>,
    task: tokio::task::JoinHandle<Result<(), String>>,
    budget: Arc<Semaphore>,
}

impl Writer {
    /// Writes what it's sent at `offset` onward, in order, telling `hooks` of each landed batch.
    fn start(file: Arc<std::fs::File>, offset: u64, hooks: Arc<dyn ChunkHooks>) -> Self {
        let (sender, mut receiver) = mpsc::unbounded_channel::<(Bytes, OwnedSemaphorePermit)>();
        let task = tokio::task::spawn_blocking(move || {
            let mut position = offset;
            let mut written = 0u64;
            while let Some((first, permit)) = receiver.blocking_recv() {
                let mut batch = first.to_vec();
                let mut permits = vec![permit];
                while batch.len() < WRITE_COALESCE_BYTES {
                    match receiver.try_recv() {
                        Ok((more, permit)) => {
                            batch.extend_from_slice(&more);
                            permits.push(permit);
                        }
                        Err(_) => break,
                    }
                }
                write_at(&file, &batch, position).map_err(|error| io_message(&error))?;
                position += batch.len() as u64;
                written += batch.len() as u64;
                drop(permits);
                hooks.on_progress(written);
            }
            Ok(())
        });
        Writer { sender: Some(sender), task, budget: Arc::new(Semaphore::new(WRITE_BUFFER_BYTES)) }
    }

    /// Queues `bytes`, waiting (and saying so) while the writer holds a full buffer.
    async fn write(&self, bytes: Bytes, hooks: &dyn ChunkHooks, cancel: &CancellationToken) -> Result<(), FetchError> {
        let needed = bytes.len().clamp(1, WRITE_BUFFER_BYTES) as u32;
        let permit = match Arc::clone(&self.budget).try_acquire_many_owned(needed) {
            Ok(permit) => permit,
            Err(_) => {
                hooks.on_write_wait(true);
                let acquired = tokio::select! {
                    _ = cancel.cancelled() => return Err(FetchError::Aborted),
                    permit = Arc::clone(&self.budget).acquire_many_owned(needed) => permit,
                };
                hooks.on_write_wait(false);
                acquired.map_err(|_| FetchError::Other("The download's writer closed".into()))?
            }
        };
        let sender = self.sender.as_ref().expect("open until finished");
        sender.send((bytes, permit)).map_err(|_| FetchError::Other("The download's writer failed".into()))
    }

    /// Writes out what it holds and closes. Bytes queued were checked against the range before
    /// they were, so they count like any others.
    async fn finish(mut self) -> Result<(), FetchError> {
        self.sender.take();
        match self.task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(message)) => Err(FetchError::Other(message)),
            Err(error) => Err(FetchError::Other(error.to_string())),
        }
    }
}

pub async fn download_chunk(request: ChunkRequest<'_>) -> Result<(), FetchError> {
    let ChunkRequest { url, browser, range_start, range_end, whole_file, connection, file, cancel, accepted, stall_timeout, hooks } =
        request;
    if cancel.is_cancelled() {
        return Err(FetchError::Aborted);
    }
    let expected = range_end.map(|end| end - range_start + 1);
    // The whole file from the start needs no Range at all, and an empty file would answer
    // `bytes=0-` with 416.
    let range = (!(whole_file && range_start == 0) && (range_start > 0 || range_end.is_some())).then(|| match range_end {
        Some(end) => format!("bytes={range_start}-{end}"),
        None => format!("bytes={range_start}-"),
    });

    let sent_at = std::time::Instant::now();
    let mut response = request_following(connection, url, browser, range, cancel).await?;
    let status = response.status().as_u16();
    // A 200 is only correct when asked from byte 0; otherwise the server ignored the Range.
    if status != 206 && !(status == 200 && range_start == 0) {
        return Err(FetchError::Status { status, retry_after_ms: retry_after_ms(header(&response, "retry-after").as_deref()) });
    }
    // Each range is fetched separately, so a file republished mid-download would otherwise be
    // stitched together from two versions and still pass every length check.
    let served = parse_content_range(header(&response, "content-range").as_deref());
    let seen = version_of(&response, served);
    match compare_version(accepted, &seen) {
        VersionCheck::Same => {}
        VersionCheck::Size(detail) => return Err(FetchError::RemoteChanged { detail, size: true, seen }),
        VersionCheck::Validators(detail) => return Err(FetchError::RemoteChanged { detail, size: false, seen }),
    }
    if status == 206 {
        let Some(served) = served else {
            return Err(FetchError::Other("Server sent a 206 without a usable Content-Range header".into()));
        };
        if served.start != range_start {
            return Err(FetchError::Other(format!(
                "Server returned the wrong range: asked for byte {range_start}, got {}",
                served.start
            )));
        }
        if let Some(end) = range_end.filter(|end| served.end > *end) {
            return Err(FetchError::Other(format!(
                "Server returned more than the requested range: through byte {}, asked through {end}",
                served.end
            )));
        }
    }
    hooks.on_response(sent_at.elapsed().as_secs_f64() * 1000.0);

    let writer = Writer::start(file, range_start, Arc::clone(&hooks));
    let mut received = 0u64;
    let outcome: Result<(), FetchError> = async {
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return Err(FetchError::Aborted),
                next = tokio::time::timeout(stall_timeout, response.chunk()) => next,
            };
            let chunk = match next {
                Err(_) => return Err(FetchError::Connection("Connection stalled: no response from server".into())),
                Ok(Err(error)) => {
                    connection.reconnect();
                    return Err(connection_error(&error));
                }
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => chunk,
            };
            let remaining = expected.map_or(chunk.len() as u64, |expected| expected.saturating_sub(received));
            let usable = if (chunk.len() as u64) <= remaining { chunk.clone() } else { chunk.slice(..remaining as usize) };
            if !usable.is_empty() {
                received += usable.len() as u64;
                hooks.on_network_progress(received);
                let wait = hooks.throttle(usable.len() as u64);
                writer.write(usable.clone(), hooks.as_ref(), cancel).await?;
                if wait > 0.0 {
                    tokio::select! {
                        _ = cancel.cancelled() => return Err(FetchError::Aborted),
                        _ = tokio::time::sleep(Duration::from_secs_f64(wait / 1000.0)) => {}
                    }
                }
            }
            if usable.len() < chunk.len() {
                return Err(FetchError::Other("Server sent more data than the requested range".into()));
            }
        }
        if let Some(expected) = expected.filter(|expected| received != *expected) {
            return Err(FetchError::Connection(format!("Server returned {received} bytes for a {expected}-byte range")));
        }
        Ok(())
    }
    .await;

    // Held until its last bytes are written; closing the file isn't the disk falling behind.
    hooks.on_write_wait(true);
    let flushed = writer.finish().await;
    hooks.on_write_wait(false);
    outcome.and(flushed)
}

/// Fetches `start..=end` into memory, following redirects, and reports which version served it:
/// used to compare a few bytes already on disk against what a server serves now.
pub async fn fetch_range(
    url: &str,
    start: u64,
    end: u64,
    connection: &StreamConnection,
    browser: Option<&BrowserContext>,
    stall_timeout: Duration,
) -> Result<(Vec<u8>, FileVersion), FetchError> {
    let cancel = CancellationToken::new();
    let response = request_following(connection, url, browser, Some(format!("bytes={start}-{end}")), &cancel).await?;
    let served = parse_content_range(header(&response, "content-range").as_deref());
    let status = response.status().as_u16();
    if status != 206 || served.map(|served| served.start) != Some(start) {
        return Err(FetchError::Other(format!("Unexpected response to a sample request (status {status})")));
    }
    let version = version_of(&response, served);
    let body = tokio::time::timeout(stall_timeout, response.bytes())
        .await
        .map_err(|_| FetchError::Connection("Sample request stalled".into()))?
        .map_err(|error| connection_error(&error))?;
    Ok((body.to_vec(), version))
}
