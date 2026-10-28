//! Where the browser extension reaches Muxo: HTTP on 127.0.0.1, which works in every browser with
//! nothing to register. There's no pairing: nothing here starts a download without the user, web
//! pages can't get in (see guard), and a program on this computer could already do more. The
//! protocol is shared with the extension (src/shared/browserBridge.ts).

use crate::core::Core;
use crate::download::browser_context::{browser_context_from, request_headers};
use crate::links::Offer;
use crate::types::{BrowserContext, ProbeResult, TransferKind};
use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use parking_lot::Mutex;
use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// Muxo listens on the first that's free and the extension tries them all.
pub const BRIDGE_PORTS: [u16; 3] = [19288, 19289, 19290];
pub const BRIDGE_API: u32 = 1;
/// The extension waits a little longer than this, and Muxo adds nothing once it has given up,
/// so a download never ends up in both.
const BRIDGE_DEADLINE: Duration = Duration::from_millis(8000);
const MAX_BODY_BYTES: usize = 64 * 1024;
/// The extension confirms within milliseconds of an answer; an unconfirmed offer is dropped.
const HANDOFF: Duration = Duration::from_secs(15);

static EXTENSION_ORIGIN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)^(chrome|moz)-extension://[a-z0-9-]+$").unwrap());

pub struct Bridge {
    core: Arc<Core>,
    port: AtomicU16,
    extension_used: AtomicBool,
    handoffs: Mutex<HashMap<String, Offer>>,
}

impl Bridge {
    /// Once an extension has connected, closing the window keeps Muxo in the tray, where the
    /// browser can still reach it.
    pub fn keeps_running_for_browser(&self) -> bool {
        self.port.load(Ordering::Relaxed) != 0 && self.extension_used.load(Ordering::Relaxed)
    }

    fn note_extension(&self) {
        if self.extension_used.swap(true, Ordering::Relaxed) {
            return;
        }
        let core = Arc::clone(&self.core);
        tokio::spawn(async move { core.settings.save(json!({ "browserExtensionUsed": true })).await });
    }
}

pub async fn start_bridge(core: Arc<Core>) -> Arc<Bridge> {
    let used = core.settings.load().await.browser_extension_used == Some(true);
    let bridge = Arc::new(Bridge {
        core,
        port: AtomicU16::new(0),
        extension_used: AtomicBool::new(used),
        handoffs: Mutex::new(HashMap::new()),
    });
    for port in BRIDGE_PORTS {
        let Ok(listener) = tokio::net::TcpListener::bind(("127.0.0.1", port)).await else { continue };
        bridge.port.store(port, Ordering::Relaxed);
        let app = Router::new()
            .route("/ping", get(ping))
            .route("/add", post(add))
            .route("/confirm", post(confirm))
            .layer(middleware::from_fn_with_state(Arc::clone(&bridge), guard))
            .with_state(Arc::clone(&bridge));
        tokio::spawn(async move { axum::serve(listener, app).await });
        return bridge;
    }
    tracing::error!("browser bridge: ports {BRIDGE_PORTS:?} are all taken");
    bridge
}

fn cors_headers(origin: Option<&HeaderValue>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(origin) = origin {
        headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
        headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Content-Type"));
        headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, POST"));
        headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    }
    headers
}

/// Host blocks DNS rebinding: a page whose domain resolves to 127.0.0.1 still sends its own name.
/// A missing Origin is allowed because a page's POST always sends one, and Firefox may leave it
/// off an extension's. JSON only: a page can't send JSON here without a CORS preflight, which
/// only an extension's origin passes.
async fn guard(State(bridge): State<Arc<Bridge>>, request: Request, next: Next) -> Response {
    let port = bridge.port.load(Ordering::Relaxed);
    let host = request.headers().get(header::HOST).and_then(|h| h.to_str().ok()).unwrap_or_default();
    let local = host == format!("127.0.0.1:{port}") || host == format!("localhost:{port}");
    let origin = request.headers().get(header::ORIGIN).cloned();
    let origin_ok = origin.as_ref().is_none_or(|o| o.to_str().is_ok_and(|o| EXTENSION_ORIGIN.is_match(o)));
    if !local || !origin_ok {
        return StatusCode::FORBIDDEN.into_response();
    }
    let cors = cors_headers(origin.as_ref());
    if request.method() == Method::OPTIONS {
        return (StatusCode::NO_CONTENT, cors).into_response();
    }
    let json_body = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_lowercase().starts_with("application/json"));
    if request.method() == Method::POST && !json_body {
        return (StatusCode::UNSUPPORTED_MEDIA_TYPE, cors).into_response();
    }
    let mut response = next.run(request).await;
    response.headers_mut().extend(cors);
    response
}

async fn ping(State(bridge): State<Arc<Bridge>>) -> Response {
    bridge.note_extension();
    Json(json!({ "app": "muxo", "version": bridge.core_version(), "api": BRIDGE_API })).into_response()
}

impl Bridge {
    fn core_version(&self) -> String {
        self.core.host.app_version()
    }
}

fn parse_body(body: &Bytes) -> Option<Value> {
    (body.len() <= MAX_BODY_BYTES).then(|| serde_json::from_slice(body).ok()).flatten()
}

struct AddRequest {
    url: String,
    browser: Option<BrowserContext>,
    page_url: Option<String>,
    min_bytes: Option<u64>,
    file_name: Option<String>,
}

fn add_request_from(body: &Value) -> Option<AddRequest> {
    let url = body.get("url")?.as_str()?;
    let lower = url.to_lowercase();
    if url.len() > 8192 || !(lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("magnet:?")) {
        return None;
    }
    let browser = match body.get("browser") {
        None | Some(Value::Null) => None,
        Some(value) => Some(browser_context_from(value)?),
    };
    let page_url = match body.get("pageUrl") {
        None | Some(Value::Null) => None,
        Some(Value::String(page)) if page.starts_with("http://") || page.starts_with("https://") => Some(page.clone()),
        Some(_) => return None,
    };
    let min_bytes = match body.get("minBytes") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64()?),
    };
    let file_name = match body.get("fileName") {
        None | Some(Value::Null) => None,
        Some(Value::String(name))
            if !name.is_empty() && name.len() <= 255 && !name.chars().any(|c| c.is_control() || c == '/' || c == '\\') =>
        {
            Some(name.clone())
        }
        Some(_) => return None,
    };
    Some(AddRequest { url: url.to_owned(), browser, page_url, min_bytes, file_name })
}

/// The page's site rather than the file's, which is often a CDN's.
fn signed_in_to(request: &AddRequest, probe: &ProbeResult) -> Option<String> {
    let browser = request.browser.as_ref()?;
    let file_url = url::Url::parse(&probe.final_url).ok()?;
    if !request_headers(&file_url, Some(browser)).iter().any(|(name, _)| name == "Cookie") {
        return None;
    }
    let site = request.page_url.as_ref().or(browser.referer.as_ref()).and_then(|url| url::Url::parse(url).ok()).unwrap_or(file_url);
    site.host_str().map(str::to_owned)
}

fn hold(bridge: &Arc<Bridge>, offer: Offer) -> Response {
    let token = uuid::Uuid::new_v4().to_string();
    bridge.handoffs.lock().insert(token.clone(), offer);
    let expire = Arc::clone(bridge);
    let expired = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(HANDOFF).await;
        expire.handoffs.lock().remove(&expired);
    });
    Json(json!({ "token": token })).into_response()
}

/// 200 with a token: Muxo will take it. 422: the browser keeps it.
async fn add(State(bridge): State<Arc<Bridge>>, body: Bytes) -> Response {
    let Some(request) = parse_body(&body).as_ref().and_then(add_request_from) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    bridge.note_extension();
    let keep = |reason: String| (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "reason": reason }))).into_response();
    // A magnet link has nothing to check here: New download looks it up.
    if request.url.to_lowercase().starts_with("magnet:") {
        return hold(&bridge, Offer { url: request.url, ..Default::default() });
    }
    let probe = match tokio::time::timeout(BRIDGE_DEADLINE, bridge.core.probe(&request.url, request.browser.as_ref())).await {
        Err(_) => return keep("The server took too long to answer.".into()),
        Ok(Err(error)) => return keep(error.to_string()),
        Ok(Ok(probe)) => probe,
    };
    let mut probe = probe;
    if let (TransferKind::Http, Some(name)) = (probe.kind, &request.file_name) {
        probe.suggested_file_name = name.clone();
    }
    if probe.kind == TransferKind::Http
        && request.min_bytes.zip(probe.total_bytes).is_some_and(|(min, total)| total < min)
    {
        return keep("smaller than the minimum size".into());
    }
    let resumes = (probe.kind == TransferKind::Http).then(|| bridge.core.failed_match(&probe)).flatten();
    let signed_in = signed_in_to(&request, &probe);
    let from = request.browser.as_ref().and_then(|browser| browser.name.clone());
    hold(
        &bridge,
        Offer { url: request.url, probe: Some(probe), browser: request.browser, from, signed_in_to: signed_in, resumes },
    )
}

/// Muxo shows the download only once the extension confirms the browser's copy is really gone.
async fn confirm(State(bridge): State<Arc<Bridge>>, body: Bytes) -> Response {
    let token = parse_body(&body).and_then(|body| body.get("token").and_then(Value::as_str).map(str::to_owned));
    let offer = token.and_then(|token| bridge.handoffs.lock().remove(&token));
    match offer {
        Some(offer) => {
            bridge.core.offer(offer);
            Json(json!({})).into_response()
        }
        None => StatusCode::GONE.into_response(),
    }
}
