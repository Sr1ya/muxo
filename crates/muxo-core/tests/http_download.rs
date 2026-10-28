//! End to end: a local server that serves byte ranges, and the engine downloading from it over
//! one of this computer's real networks.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use parking_lot::Mutex;
use muxo_core::types::*;
use muxo_core::{Core, CoreConfig, Host};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Default)]
struct TestHost {
    updates: Mutex<Vec<DownloadUpdate>>,
}

impl Host for TestHost {
    fn download_updated(&self, update: &DownloadUpdate) {
        self.updates.lock().push(update.clone());
    }
    fn history_changed(&self) {}
    fn networks_changed(&self, _: &[NetworkInterfaceInfo]) {}
    fn magnet_check_status(&self, _: &MagnetCheckStatus) {}
    fn link_received(&self) {}
    fn notify(&self, _: &str, _: &str) {}
    fn trash(&self, _: &Path) -> Result<(), String> {
        Ok(())
    }
    fn reveal(&self, _: &Path) -> bool {
        true
    }
    fn open(&self, _: &Path) -> bool {
        true
    }
    fn app_version(&self) -> String {
        "test".into()
    }
}

async fn serve(State(body): State<Arc<Vec<u8>>>, headers: HeaderMap) -> Response {
    let total = body.len();
    let range = headers.get("range").and_then(|r| r.to_str().ok()).and_then(|r| r.strip_prefix("bytes="));
    let Some(range) = range else {
        return ([("etag", "\"v1\""), ("accept-ranges", "bytes")], body.to_vec()).into_response();
    };
    let (start, end) = range.split_once('-').unwrap();
    let start: usize = start.parse().unwrap();
    let end: usize = if end.is_empty() { total - 1 } else { end.parse::<usize>().unwrap().min(total - 1) };
    (
        StatusCode::PARTIAL_CONTENT,
        [("etag", "\"v1\"".to_string()), ("content-range", format!("bytes {start}-{end}/{total}"))],
        body[start..=end].to_vec(),
    )
        .into_response()
}

async fn server(body: Vec<u8>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route("/file.bin", get(serve)).with_state(Arc::new(body));
    tokio::spawn(async move { axum::serve(listener, app).await });
    format!("http://{address}/file.bin")
}

fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i * 7 + i / 4096) as u8).collect()
}

async fn engine(data: &Path, downloads: &Path) -> (Arc<Core>, Arc<TestHost>) {
    let host = Arc::new(TestHost::default());
    let core = Core::start(
        CoreConfig { data_dir: data.into(), downloads_dir: downloads.into(), home_dir: downloads.into() },
        Arc::clone(&host) as Arc<dyn Host>,
    )
    .await;
    (core, host)
}

async fn wait_for<F: Fn() -> bool>(timeout: Duration, condition: F) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn last_status(host: &TestHost, id: &str) -> Option<DownloadStatus> {
    host.updates.lock().iter().rev().find(|u| u.state.id == id).map(|u| u.state.status)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_pauses_resumes_and_publishes() {
    let data = tempfile::tempdir().unwrap();
    let downloads = tempfile::tempdir().unwrap();
    let body = payload(12 * 1024 * 1024 + 123);
    let url = server(body.clone()).await;
    let (core, host) = engine(data.path(), downloads.path()).await;

    let networks = core.networks.refresh().await;
    let Some(network) = networks.iter().find(|n| n.addresses.iter().any(|a| a.family == 4)) else {
        eprintln!("no IPv4 network on this computer; skipping");
        return;
    };

    // Slow enough to pause partway: 4 MB/s.
    core.update_settings(serde_json::json!({ "speedLimit": 4 * 1024 * 1024 })).await.unwrap();

    let probe = core.probe(&url, None).await.unwrap();
    assert_eq!(probe.kind, TransferKind::Http);
    assert!(probe.supports_ranges);
    assert_eq!(probe.total_bytes, Some(body.len() as u64));
    assert_eq!(probe.suggested_file_name, "file.bin");

    let id = core
        .start_download(StartDownloadRequest {
            kind: TransferKind::Http,
            url: probe.final_url.clone(),
            destination_dir: downloads.path().to_string_lossy().into(),
            suggested_file_name: probe.suggested_file_name.clone(),
            total_bytes: body.len() as u64,
            supports_ranges: true,
            interface_ids: vec![network.id.clone()],
            etag: probe.etag.clone(),
            last_modified: None,
            streams_per_network: None,
            browser: None,
            info_hash: None,
            selected_files: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(1200)).await;
    core.pause(&id).await;
    assert_eq!(last_status(&host, &id), Some(DownloadStatus::Paused));
    let partial = downloads.path().join("file.bin.muxo");
    assert!(partial.exists(), "the staging file holds the progress");
    let paused_bytes = host.updates.lock().iter().rev().find(|u| u.state.id == id).unwrap().state.bytes_downloaded;
    assert!(paused_bytes > 0 && paused_bytes < body.len() as u64, "paused partway: {paused_bytes}");
    // Manifest saved for a restart.
    assert!(data.path().join("downloads").join(&id).join("manifest.json").exists());

    core.update_settings(serde_json::json!({ "speedLimit": null })).await.unwrap();
    core.resume(&id);

    let finished = wait_for(Duration::from_secs(60), || {
        host.updates.lock().iter().any(|u| u.state.id == id && u.state.status == DownloadStatus::Completed)
    })
    .await;
    assert!(finished, "the download finished: last status {:?}", last_status(&host, &id));
    let published = downloads.path().join("file.bin");
    assert!(wait_for(Duration::from_secs(5), || published.exists()).await);
    assert_eq!(std::fs::read(&published).unwrap(), body, "every byte in its place");
    assert!(!partial.exists());

    let completed = host.updates.lock().iter().rev().find(|u| u.state.id == id).unwrap().clone();
    let streams = completed.state.http().unwrap().peak_streams;
    assert!(streams > 1, "the file came down several streams at once: {streams}");
    assert!(wait_for(Duration::from_secs(5), || futures::executor::block_on(core.list_history()).iter().any(|h| h.state.id == id)).await);
    assert!(core.list_downloads().await.iter().all(|d| d.state.id != id), "finished downloads move to history");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restores_a_paused_download_after_a_restart() {
    let data = tempfile::tempdir().unwrap();
    let downloads = tempfile::tempdir().unwrap();
    let body = payload(6 * 1024 * 1024);
    let url = server(body.clone()).await;
    let id;
    {
        let (core, host) = engine(data.path(), downloads.path()).await;
        let networks = core.networks.refresh().await;
        let Some(network) = networks.iter().find(|n| n.addresses.iter().any(|a| a.family == 4)) else { return };
        core.update_settings(serde_json::json!({ "speedLimit": 2 * 1024 * 1024 })).await.unwrap();
        id = core
            .start_download(StartDownloadRequest {
                kind: TransferKind::Http,
                url: url.clone(),
                destination_dir: downloads.path().to_string_lossy().into(),
                suggested_file_name: "file.bin".into(),
                total_bytes: body.len() as u64,
                supports_ranges: true,
                interface_ids: vec![network.id.clone()],
                etag: Some("\"v1\"".into()),
                last_modified: None,
                streams_per_network: Some(4),
                browser: None,
                info_hash: None,
                selected_files: None,
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1000)).await;
        // What quitting does.
        core.suspend_all().await;
        assert_eq!(last_status(&host, &id), Some(DownloadStatus::Paused));
    }

    let (core, host) = engine(data.path(), downloads.path()).await;
    core.update_settings(serde_json::json!({ "speedLimit": null })).await.unwrap();
    let listed = core.list_downloads().await;
    let restored = listed.iter().find(|d| d.state.id == id).expect("restored");
    assert_eq!(restored.state.status, DownloadStatus::Paused);
    assert!(restored.state.bytes_downloaded > 0);
    core.networks.refresh().await;
    core.resume(&id);
    let finished = wait_for(Duration::from_secs(60), || {
        host.updates.lock().iter().any(|u| u.state.id == id && u.state.status == DownloadStatus::Completed)
    })
    .await;
    assert!(finished, "last status {:?}", last_status(&host, &id));
    let published = downloads.path().join("file.bin");
    assert!(wait_for(Duration::from_secs(5), || published.exists()).await);
    assert_eq!(std::fs::read(published).unwrap(), body);
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_answers_extensions_and_refuses_pages() {
    let data = tempfile::tempdir().unwrap();
    let (core, _host) = engine(data.path(), data.path()).await;
    let bridge = muxo_core::start_bridge(Arc::clone(&core)).await;
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut port = None;
    for candidate in muxo_core::bridge::BRIDGE_PORTS {
        let reply = client.get(format!("http://127.0.0.1:{candidate}/ping")).send().await;
        if let Ok(reply) = reply {
            if reply.status() == 200 {
                let body: serde_json::Value = reply.json().await.unwrap();
                if body["app"] == "muxo" && body["version"] == "test" {
                    port = Some(candidate);
                    break;
                }
            }
        }
    }
    let port = port.expect("the bridge answers on one of its ports");
    assert!(bridge.keeps_running_for_browser(), "a ping counts as the extension being used");
    // A web page's origin is turned away, as is a request under another host name.
    let page = client.get(format!("http://127.0.0.1:{port}/ping")).header("Origin", "https://evil.example").send().await.unwrap();
    assert_eq!(page.status(), 403);
    let rebound = client.get(format!("http://127.0.0.1:{port}/ping")).header("Host", "evil.example").send().await.unwrap();
    assert_eq!(rebound.status(), 403);
    // Not JSON: a page could send that without a preflight.
    let form = client.post(format!("http://127.0.0.1:{port}/add")).header("Content-Type", "text/plain").body("{}").send().await.unwrap();
    assert_eq!(form.status(), 415);
    // A magnet link is handed over as soon as the extension confirms.
    let extension = "chrome-extension://abcdefghijklmnop";
    let add = client
        .post(format!("http://127.0.0.1:{port}/add"))
        .header("Origin", extension)
        .json(&serde_json::json!({ "url": "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" }))
        .send()
        .await
        .unwrap();
    assert_eq!(add.status(), 200);
    assert_eq!(add.headers()["access-control-allow-origin"], extension);
    let token = add.json::<serde_json::Value>().await.unwrap()["token"].as_str().unwrap().to_owned();
    assert!(core.links.pending().is_empty(), "nothing shows before the confirmation");
    let confirm = client
        .post(format!("http://127.0.0.1:{port}/confirm"))
        .header("Origin", extension)
        .json(&serde_json::json!({ "token": token }))
        .send()
        .await
        .unwrap();
    assert_eq!(confirm.status(), 200);
    assert_eq!(core.links.pending().len(), 1);
}
