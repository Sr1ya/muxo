//! A file's name, as a Content-Disposition header gives it (RFC 6266, 5987 and 2047).

use base64::Engine;
use percent_encoding::percent_decode_str;
use regex::Regex;
use std::collections::HashMap;
use std::sync::LazyLock;

fn decode_uri_component(value: &str) -> Option<String> {
    percent_decode_str(value).decode_utf8().ok().map(|text| text.into_owned())
}

static ENCODED_WORD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^=\?([^?]+)\?([bBqQ])\?([^?]*)\?=$").unwrap());

/// Decodes a single RFC 2047 encoded-word (=?charset?B|Q?…?=), or returns the input unchanged
/// when it is not one or can't be decoded safely.
pub fn decode_rfc2047_word(value: &str) -> String {
    let Some(captures) = ENCODED_WORD.captures(value.trim()) else { return value.to_owned() };
    let charset = &captures[1];
    let text = &captures[3];
    let bytes = if captures[2].eq_ignore_ascii_case("B") {
        if text.len() % 4 != 0 {
            return value.to_owned();
        }
        match base64::engine::general_purpose::STANDARD.decode(text) {
            Ok(bytes) => bytes,
            Err(_) => return value.to_owned(),
        }
    } else {
        // Q-encoding: _ is space; =XX is a byte. A bare '=' that is not =XX is refused.
        let mut bytes = Vec::new();
        let raw = text.as_bytes();
        let mut i = 0;
        while i < raw.len() {
            match raw[i] {
                b'_' => bytes.push(b' '),
                b'=' => {
                    let hex = text.get(i + 1..i + 3).and_then(|hex| u8::from_str_radix(hex, 16).ok());
                    match hex {
                        Some(byte) => {
                            bytes.push(byte);
                            i += 2;
                        }
                        None => return value.to_owned(),
                    }
                }
                byte => bytes.push(byte),
            }
            i += 1;
        }
        bytes
    };
    let Some(encoding) = encoding_rs::Encoding::for_label(charset.as_bytes()) else {
        return value.to_owned();
    };
    // Undecodable bytes must not invent text.
    match encoding.decode_without_bom_handling_and_without_replacement(&bytes) {
        Some(decoded) => decoded.into_owned(),
        None => value.to_owned(),
    }
}

fn is_encoded_word(part: &str) -> bool {
    ENCODED_WORD.is_match(part)
}

/// Decodes a filename made of encoded-words separated by whitespace. Whitespace is discarded only
/// between adjacent encoded-words; when any fails to decode, the original is returned unchanged.
pub fn decode_rfc2047_filename(name: &str) -> String {
    static ANY: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"=\?[^?]+\?[bBqQ]\?[^?]*\?=").unwrap());
    static SPLIT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+").unwrap());
    let trimmed = name.trim();
    if !ANY.is_match(trimmed) {
        return name.to_owned();
    }
    let mut parts: Vec<&str> = Vec::new();
    let mut last = 0;
    for gap in SPLIT.find_iter(trimmed) {
        parts.push(&trimmed[last..gap.start()]);
        parts.push(gap.as_str());
        last = gap.end();
    }
    parts.push(&trimmed[last..]);
    let mut out = String::new();
    let mut saw_encoded = false;
    for (i, part) in parts.iter().enumerate() {
        if part.chars().all(char::is_whitespace) && !part.is_empty() {
            let previous = i.checked_sub(1).map(|j| parts[j]);
            let next = parts.get(i + 1).copied();
            if previous.is_some_and(is_encoded_word) && next.is_some_and(is_encoded_word) {
                continue;
            }
            out.push_str(part);
            continue;
        }
        if is_encoded_word(part) {
            let decoded = decode_rfc2047_word(part);
            if decoded == *part {
                return name.to_owned();
            }
            saw_encoded = true;
            out.push_str(&decoded);
            continue;
        }
        out.push_str(part);
    }
    if saw_encoded { out } else { name.to_owned() }
}

pub fn parse_content_disposition_filename(disposition: &str) -> Option<String> {
    static PARAMETER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r#"(?i)(?:^|;)[ \t]*([!#$%&'*+.^_`|~0-9a-z-]+)[ \t]*=[ \t]*(("(?:[^"\\]|\\.)*")|([^; \t]+))"#).unwrap()
    });
    static EXTENDED: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([a-zA-Z0-9_-]+)'[^']*'([^;\s]+)").unwrap());
    static ESCAPE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\\(.)").unwrap());

    let mut parameters: HashMap<String, String> = HashMap::new();
    let mut first_quoted_filename: Option<String> = None;
    for captures in PARAMETER.captures_iter(disposition) {
        let key = captures[1].to_lowercase();
        let raw = captures[2].to_owned();
        let quoted = captures.get(3).is_some();
        if key == "filename" && quoted && first_quoted_filename.is_none() {
            first_quoted_filename = Some(raw.clone());
        }
        if key == "filename*" && (quoted || !EXTENDED.is_match(&raw)) {
            continue;
        }
        parameters.entry(key).or_insert(raw);
    }

    // filename* takes precedence over filename. RFC 5987 requires both UTF-8 and ISO-8859-1.
    if let Some(captures) = parameters.get("filename*").and_then(|raw| EXTENDED.captures(raw)) {
        let value = &captures[2];
        if captures[1].eq_ignore_ascii_case("iso-8859-1") {
            let bytes: Vec<u8> = percent_decode_str(value).collect();
            return Some(bytes.iter().map(|byte| *byte as char).collect());
        }
        return Some(decode_uri_component(value).unwrap_or_else(|| value.to_owned()));
    }

    if let Some(quoted) = first_quoted_filename {
        let inner = &quoted[1..quoted.len() - 1];
        if !inner.is_empty() {
            let unescaped = ESCAPE.replace_all(inner, "$1").into_owned();
            let decoded = decode_uri_component(&unescaped).unwrap_or(unescaped);
            return Some(decode_rfc2047_filename(&decoded));
        }
    }

    let token = parameters.get("filename")?;
    let decoded = decode_uri_component(token).unwrap_or_else(|| token.clone());
    Some(decode_rfc2047_filename(&decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_names() {
        assert_eq!(parse_content_disposition_filename(r#"attachment; filename="report; final.pdf""#).unwrap(), "report; final.pdf");
        assert_eq!(parse_content_disposition_filename("attachment; filename*=UTF-8''na%C3%AFve.txt; filename=x").unwrap(), "naïve.txt");
        assert_eq!(parse_content_disposition_filename("attachment; filename*=iso-8859-1''%A3.txt").unwrap(), "£.txt");
        assert_eq!(parse_content_disposition_filename("attachment; filename=plain.zip").unwrap(), "plain.zip");
        assert_eq!(parse_content_disposition_filename(r#"attachment; filename="=?UTF-8?B?w6TDtsO8LnR4dA==?=""#).unwrap(), "äöü.txt");
        assert_eq!(parse_content_disposition_filename("inline"), None);
    }
}
