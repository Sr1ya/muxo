//! What a link would download: a file over HTTP(S), or a torrent (a magnet link, a link to a
//! .torrent, or the path of a .torrent on this computer).

use crate::download::browser_context::request_headers;
use crate::download::content_disposition::parse_content_disposition_filename;
use crate::download::file_version::normalize_etag;
use crate::download::torrent::metadata::TorrentProbe;
use crate::network::routes::describe_reqwest;
use crate::types::{BrowserContext, ProbeResult, TransferKind};
use percent_encoding::percent_decode_str;
use regex::Regex;
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use url::Url;

const MAX_REDIRECTS: usize = 5;
const NO_RESPONSE: &str = "The server did not respond — check the link and try again";

struct ProbeResponse {
    status: u16,
    headers: HeaderMap,
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers.get(name).and_then(|value| value.to_str().ok()).map(str::to_owned)
}

fn probe_client() -> &'static reqwest::Client {
    static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .expect("a default client")
    });
    &CLIENT
}

/// GET with a small range: cheaper than fetching the body, and unlike HEAD it also tells (via the
/// 206 status) whether range requests actually work. Not 1 byte: some hosts answer a range ending
/// at byte 0 or 1 with the whole file. The body is dropped once the headers arrive.
async fn request_first_bytes(
    url: &Url,
    deadline: Instant,
    browser: Option<&BrowserContext>,
    range: Option<&str>,
) -> anyhow::Result<ProbeResponse> {
    let mut request = probe_client().get(url.clone());
    for (name, value) in request_headers(url, browser) {
        request = request.header(name, value);
    }
    if let Some(range) = range {
        request = request.header("Range", range);
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    let response = tokio::time::timeout(remaining, request.send())
        .await
        .map_err(|_| anyhow::anyhow!(NO_RESPONSE))?
        .map_err(|error| anyhow::anyhow!(describe_reqwest(&error)))?;
    Ok(ProbeResponse { status: response.status().as_u16(), headers: response.headers().clone() })
}

fn file_name_from_headers(headers: &HeaderMap, url: &Url) -> String {
    if let Some(name) = header_value(headers, "content-disposition").and_then(|d| parse_content_disposition_filename(&d)) {
        return name;
    }
    let path = url.path();
    let decoded = percent_decode_str(path).decode_utf8().map(|p| p.into_owned()).unwrap_or_else(|_| path.to_owned());
    decoded.split('/').filter(|part| !part.is_empty()).last().unwrap_or("download").to_owned()
}

async fn request_following_redirects(
    raw_url: &str,
    browser: Option<&BrowserContext>,
    timeout: Duration,
) -> anyhow::Result<(Url, ProbeResponse)> {
    let mut current = Url::parse(raw_url).map_err(|_| anyhow::anyhow!("Invalid URL"))?;
    let deadline = Instant::now() + timeout;
    let mut ranged = true;
    for redirect in 0..=MAX_REDIRECTS {
        let mut response =
            request_first_bytes(&current, deadline, browser, ranged.then_some("bytes=0-1023")).await?;
        // Some servers refuse any Range with 403 while serving the same URL in full (#131).
        if response.status == 403 && ranged {
            ranged = false;
            response = request_first_bytes(&current, deadline, browser, None).await?;
        }
        if (300..400).contains(&response.status) {
            let Some(location) = header_value(&response.headers, "location") else {
                return Ok((current, response));
            };
            if redirect == MAX_REDIRECTS {
                anyhow::bail!("Too many redirects while checking the link");
            }
            current = current.join(&location)?;
            continue;
        }
        return Ok((current, response));
    }
    unreachable!("the loop returns on its last pass")
}

/// Whether the server labels each byte range with an ETag of its own (pCloud does, #156): then no
/// chunk's ETag ever matches the probe's, and every one would be taken for a changed file.
async fn etag_is_per_range(url: &Url, browser: Option<&BrowserContext>, first: &ProbeResponse, timeout: Duration) -> bool {
    let Some(etag) = header_value(&first.headers, "etag") else { return false };
    let total = header_value(&first.headers, "content-range")
        .and_then(|range| range.rsplit('/').next().and_then(|total| total.parse::<u64>().ok()));
    if first.status != 206 || total.is_none_or(|total| total <= 2048) {
        return false;
    }
    let deadline = Instant::now() + timeout;
    let again = request_first_bytes(url, deadline, browser, Some("bytes=0-1023")).await;
    let next = request_first_bytes(url, deadline, browser, Some("bytes=1024-2047")).await;
    let (Ok(again), Ok(next)) = (again, next) else { return false };
    let etag_of = |r: &ProbeResponse| normalize_etag(&header_value(&r.headers, "etag").unwrap_or_default());
    again.status == 206
        && next.status == 206
        && etag_of(&again) == normalize_etag(&etag)
        && !etag_of(&next).is_empty()
        && etag_of(&next) != normalize_etag(&etag)
}

fn is_torrent_link(headers: &HeaderMap, url: &Url) -> bool {
    let content_type = header_value(headers, "content-type").unwrap_or_default().to_lowercase();
    content_type.starts_with("application/x-bittorrent") || url.path().to_lowercase().ends_with(".torrent")
}

pub async fn probe_url(
    torrents: &TorrentProbe,
    raw_url: &str,
    browser: Option<&BrowserContext>,
    timeout: Duration,
) -> anyhow::Result<ProbeResult> {
    if raw_url.to_lowercase().starts_with("magnet:") {
        let bytes = torrents.fetch_magnet_metadata(raw_url).await?;
        return torrents.describe(&bytes, raw_url);
    }
    if Path::new(raw_url).is_absolute() && raw_url.to_lowercase().ends_with(".torrent") {
        let bytes = crate::download::torrent::metadata::read_torrent_file(Path::new(raw_url)).await?;
        return torrents.describe(&bytes, raw_url);
    }

    let (current, response) = request_following_redirects(raw_url, browser, timeout).await?;

    // An empty file can't satisfy a request for its first bytes: 416 with `bytes */0`.
    static EMPTY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^\s*bytes\s+\*/0\s*$").unwrap());
    let content_range = header_value(&response.headers, "content-range");
    if response.status == 416 && content_range.as_deref().is_some_and(|range| EMPTY.is_match(range)) {
        return Ok(ProbeResult {
            kind: TransferKind::Http,
            requested_url: raw_url.to_owned(),
            final_url: current.to_string(),
            supports_ranges: false,
            total_bytes: Some(0),
            suggested_file_name: file_name_from_headers(&response.headers, &current),
            content_type: header_value(&response.headers, "content-type"),
            etag: header_value(&response.headers, "etag"),
            last_modified: header_value(&response.headers, "last-modified"),
            torrent: None,
        });
    }
    if response.status == 0 || response.status >= 400 {
        anyhow::bail!("Server responded with status {}", response.status);
    }

    if is_torrent_link(&response.headers, &current) {
        let final_url = current.to_string();
        let headers = request_headers(&current, browser);
        let bytes = crate::download::torrent::metadata::download_torrent_file(&final_url, timeout, &headers).await?;
        return torrents.describe(&bytes, &final_url);
    }

    // A server that ignored the Range answered 200, whatever its Accept-Ranges claims.
    let supports_ranges = response.status == 206;
    let mut total_bytes = content_range
        .as_deref()
        .and_then(|range| range.rsplit('/').next().and_then(|total| total.parse::<u64>().ok()));
    if total_bytes.is_none() && response.status == 200 {
        // Only trusted on a full response: on a 206 it describes the bytes asked for.
        total_bytes = header_value(&response.headers, "content-length").and_then(|length| length.parse().ok());
    }
    let per_range = etag_is_per_range(&current, browser, &response, timeout).await;
    Ok(ProbeResult {
        kind: TransferKind::Http,
        requested_url: raw_url.to_owned(),
        final_url: current.to_string(),
        supports_ranges,
        total_bytes,
        suggested_file_name: file_name_from_headers(&response.headers, &current),
        content_type: header_value(&response.headers, "content-type"),
        etag: if per_range { None } else { header_value(&response.headers, "etag") },
        last_modified: header_value(&response.headers, "last-modified"),
        torrent: None,
    })
}
