//! Plain-English versions of common failures, for notifications. The window has the same table
//! (src/shared/errors.ts) for what it shows.

use regex::Regex;
use std::sync::LazyLock;

static HINTS: LazyLock<Vec<(Regex, &'static str)>> = LazyLock::new(|| {
    [
        (r"ENOSPC|EDQUOT", "There isn’t enough space to save this download. Free up space or choose another folder."),
        (r"EACCES|EPERM", "Muxo doesn’t have permission to access this file or folder. Check its permissions or choose another folder."),
        (r"ENOENT", "The file or folder is missing. Check the download location and try again."),
        (r"EROFS", "This drive is read-only. Choose another download folder."),
        (r"EIO", "Muxo couldn’t read or write the download. Check the drive and try again."),
        (r"(?i)Invalid torrent metadata|Invalid torrent|Invalid info hash|Invalid magnet", "Muxo couldn’t read this torrent. Try another magnet link or .torrent file."),
        (r"status (401|403|404|410) for range request", "This link no longer works. Paste a new link to continue."),
        (r"Download is incomplete", "Muxo couldn’t download every part of the file. Try downloading again."),
        (r"Download file size does not match", "The downloaded file has an unexpected size. Muxo couldn’t save the completed file. Try downloading again."),
        (r"ENOTFOUND|EAI_AGAIN", "Muxo couldn’t find the server. Check the link and your connection."),
        (r"ECONNREFUSED", "The server refused the connection. Try again later."),
        (r"ECONNRESET|socket hang up", "The connection was interrupted. Try again in a moment."),
        (r"ETIMEDOUT|ESOCKETTIMEDOUT", "The connection timed out. Check your network and try again."),
        (r"(?i)CERT|SSL|TLS", "The server’s security certificate couldn’t be verified. Check the link or try again later."),
        (r"Invalid URL|ERR_INVALID_URL", "That doesn’t look like a valid link."),
        (r"Server responded with status 401", "This link requires authentication. Try a direct download link."),
        (r"Server responded with status 403", "The server denied access to this file. Check the link or try a new one."),
        (r"Server responded with status 404", "The file couldn’t be found. Check the link and try again."),
        (r"Server responded with status 4\d\d", "The server rejected the download. Check the link and try again."),
        (r"Server responded with status 5\d\d", "The server is having trouble. Try again later."),
    ]
    .into_iter()
    .map(|(pattern, message)| (Regex::new(pattern).unwrap(), message))
    .collect()
});

pub fn describe_error(raw: &str) -> String {
    HINTS
        .iter()
        .find(|(pattern, _)| pattern.is_match(raw))
        .map_or_else(|| raw.to_owned(), |(_, message)| (*message).to_owned())
}
