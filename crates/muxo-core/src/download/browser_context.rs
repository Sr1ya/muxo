//! What a browser sends with a link: its cookies, the page it came from and its user agent, so a
//! link that needs a signed-in session works as it does in the browser.

use crate::types::{BrowserContext, BrowserCookie};
use serde_json::Value;
use url::Url;

const USER_AGENT: &str = "Muxo/1.0";
const MAX_COOKIES: usize = 500;
const MAX_TEXT: usize = 8192;

/// Called again for every redirect, so a site's session cookie never reaches the next host (its
/// CDN), while a cookie set for a whole domain still follows a hop within it.
pub fn request_headers(url: &Url, browser: Option<&BrowserContext>) -> Vec<(String, String)> {
    let user_agent = browser.and_then(|b| b.user_agent.clone()).unwrap_or_else(|| USER_AGENT.into());
    let mut headers = vec![("User-Agent".to_owned(), user_agent)];
    if let Some(referer) = browser.and_then(|b| b.referer.clone()) {
        headers.push(("Referer".into(), referer));
    }
    if let Some(cookie) = browser.map(|b| cookie_header(url, &b.cookies)).filter(|c| !c.is_empty()) {
        headers.push(("Cookie".into(), cookie));
    }
    headers
}

/// RFC 6265 §5.4.
fn cookie_header(url: &Url, cookies: &[BrowserCookie]) -> String {
    let host = url.host_str().unwrap_or_default().to_lowercase();
    let path = if url.path().is_empty() { "/" } else { url.path() };
    let mut matching: Vec<&BrowserCookie> = cookies
        .iter()
        .filter(|cookie| {
            let domain = cookie.domain.trim_start_matches('.').to_lowercase();
            let domain_matches = if cookie.host_only {
                host == domain
            } else {
                host == domain || host.ends_with(&format!(".{domain}"))
            };
            let path_matches = path == cookie.path
                || (path.starts_with(&cookie.path)
                    && (cookie.path.ends_with('/') || path.as_bytes().get(cookie.path.len()) == Some(&b'/')));
            domain_matches && path_matches && (!cookie.secure || url.scheme() == "https")
        })
        .collect();
    matching.sort_by(|a, b| b.path.len().cmp(&a.path.len()));
    matching
        .iter()
        .map(|cookie| if cookie.name.is_empty() { cookie.value.clone() } else { format!("{}={}", cookie.name, cookie.value) })
        .collect::<Vec<_>>()
        .join("; ")
}

fn text(value: &Value, cookie: bool) -> Option<String> {
    let text = value.as_str()?;
    let unsafe_char = |c: char| (c as u32) < 0x20 || c == '\x7f' || (cookie && c == ';');
    (text.len() <= MAX_TEXT && !text.chars().any(unsafe_char)).then(|| text.to_owned())
}

/// It comes from outside Muxo (the extension, or a manifest read back from disk), so only known
/// fields are kept, and any line break or control character refuses the whole context.
pub fn browser_context_from(value: &Value) -> Option<BrowserContext> {
    let record = value.as_object()?;
    let cookies = record.get("cookies")?.as_array()?;
    if cookies.len() > MAX_COOKIES {
        return None;
    }
    let optional = |key: &str| -> Option<Option<String>> {
        match record.get(key) {
            None | Some(Value::Null) => Some(None),
            Some(value) => text(value, false).map(Some),
        }
    };
    let referer = optional("referer")?;
    if referer.as_deref().is_some_and(|referer| !(referer.starts_with("http://") || referer.starts_with("https://"))) {
        return None;
    }
    let user_agent = optional("userAgent")?;
    let name = optional("name")?;
    if name.as_deref().is_some_and(|name| name.chars().count() > 40) {
        return None;
    }
    let mut checked = Vec::new();
    for cookie in cookies {
        let cookie = cookie.as_object()?;
        let field = |key: &str| cookie.get(key).unwrap_or(&Value::Null);
        let path = text(field("path"), false)?;
        if !path.starts_with('/') {
            return None;
        }
        checked.push(BrowserCookie {
            name: text(field("name"), true)?,
            value: text(field("value"), true)?,
            domain: text(field("domain"), false)?,
            host_only: field("hostOnly").as_bool()?,
            path,
            secure: field("secure").as_bool()?,
        });
    }
    Some(BrowserContext { cookies: checked, referer, user_agent, name })
}
