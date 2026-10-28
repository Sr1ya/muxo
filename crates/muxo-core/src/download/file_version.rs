//! What a server says about which version of the file it's serving, and whether a response is of
//! the version a download started on.

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileVersion {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    /// 0 = unknown.
    pub total_bytes: u64,
}

/// - Same: nothing suggests a different file.
/// - Size: the size differs. Identical bytes can't have a different size, so this is proof.
/// - Validators: the ETag or Last-Modified differs. Only a hint: load-balanced servers label the
///   same bytes differently, so it has to be settled by comparing actual bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionCheck {
    Same,
    Size(String),
    Validators(String),
}

/// Drops what varies between servers without the bytes changing: the weak-validator marker, the
/// quotes, and the suffix some servers add for a compressed variant (Apache's "-gzip").
pub fn normalize_etag(etag: &str) -> String {
    static WEAK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^W/").unwrap());
    static QUOTED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#"^"(.*)"$"#).unwrap());
    static VARIANT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)[-;](gzip|br|deflate|zstd)$").unwrap());
    let trimmed = WEAK.replace(etag.trim(), "").into_owned();
    let unquoted = QUOTED.replace(&trimmed, "$1").into_owned();
    VARIANT.replace(&unquoted, "").into_owned()
}

/// `accepted` is the version the download started on, plus any whose bytes were since confirmed
/// identical to it.
pub fn compare_version(accepted: &[FileVersion], seen: &FileVersion) -> VersionCheck {
    let expected = accepted.first().map_or(0, |version| version.total_bytes);
    if expected > 0 && seen.total_bytes > 0 && seen.total_bytes != expected {
        return VersionCheck::Size(format!("it is now {} bytes instead of {expected}", seen.total_bytes));
    }
    let etags: Vec<String> = accepted.iter().filter_map(|v| v.etag.as_deref()).map(normalize_etag).collect();
    if let Some(etag) = &seen.etag {
        if !etags.is_empty() {
            return if etags.contains(&normalize_etag(etag)) {
                VersionCheck::Same
            } else {
                VersionCheck::Validators(format!("its ETag is now {etag}"))
            };
        }
    }
    let dates: Vec<&str> = accepted.iter().filter_map(|v| v.last_modified.as_deref()).collect();
    if let Some(modified) = &seen.last_modified {
        if !dates.is_empty() && !dates.contains(&modified.as_str()) {
            return VersionCheck::Validators(format!("it was modified at {modified}"));
        }
    }
    VersionCheck::Same
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(etag: Option<&str>, total: u64) -> FileVersion {
        FileVersion { etag: etag.map(Into::into), last_modified: None, total_bytes: total }
    }

    #[test]
    fn compares_versions() {
        assert_eq!(normalize_etag(r#"W/"abc-gzip""#), "abc");
        let accepted = [version(Some("\"abc\""), 10)];
        assert_eq!(compare_version(&accepted, &version(Some("W/\"abc\""), 10)), VersionCheck::Same);
        assert!(matches!(compare_version(&accepted, &version(Some("x"), 10)), VersionCheck::Validators(_)));
        assert!(matches!(compare_version(&accepted, &version(None, 11)), VersionCheck::Size(_)));
    }
}
