//! The window's requests, one command per IpcContract entry (src/shared/ipc-contract.ts). Errors
//! go back as their message, which the window's describeError reads.

use crate::AppState;
use muxo_core::types::*;
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tauri::{AppHandle, Manager, State, Theme};
use tauri_plugin_clipboard_manager::ClipboardExt;
use tauri_plugin_dialog::DialogExt;

type Result<T> = std::result::Result<T, String>;

fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}

#[tauri::command]
pub async fn initial_state(state: State<'_, AppState>, app: AppHandle) -> Result<InitialState> {
    let dark = app.get_webview_window("main").and_then(|w| w.theme().ok()) == Some(Theme::Dark);
    let initial = state.core.initial_state(dark).await;
    crate::apply_theme(&app, ThemeSource::Dark);
    Ok(initial)
}

#[tauri::command]
pub async fn list_interfaces(state: State<'_, AppState>) -> Result<Vec<NetworkInterfaceInfo>> {
    Ok(state.core.networks.refresh().await)
}

#[tauri::command]
pub fn device_binding_supported() -> bool {
    muxo_core::device_binding_supported()
}

#[tauri::command]
pub fn open_network_settings() {
    #[cfg(target_os = "windows")]
    let _ = std::process::Command::new("cmd").args(["/C", "start", "ms-settings:network-status"]).spawn();
    #[cfg(target_os = "macos")]
    let _ = std::process::Command::new("open").arg("x-apple.systempreferences:com.apple.preference.network").spawn();
    #[cfg(target_os = "linux")]
    let _ = std::process::Command::new("sh")
        .args(["-c", "gnome-control-center network || nm-connection-editor || true"])
        .spawn();
}

#[tauri::command]
pub async fn update_settings(state: State<'_, AppState>, app: AppHandle, patch: Value) -> Result<()> {
    // The one setting the shell also applies, before saving, so a failed write still switches the
    // window to the theme the toggle now shows.
    if let Some(theme) = patch.get("themeSource").and_then(|t| serde_json::from_value::<ThemeSource>(t.clone()).ok()) {
        crate::apply_theme(&app, theme);
    }
    state.core.update_settings(patch).await.map_err(text)
}

#[tauri::command]
pub async fn probe_url(state: State<'_, AppState>, url: String) -> Result<ProbeResult> {
    state.core.probe(&url, None).await.map_err(text)
}

#[tauri::command]
pub async fn choose_destination_folder(app: AppHandle, default_path: String) -> Result<Option<String>> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let mut dialog = app.dialog().file();
    if !default_path.is_empty() {
        dialog = dialog.set_directory(PathBuf::from(default_path));
    }
    if let Some(window) = app.get_webview_window("main") {
        dialog = dialog.set_parent(&window);
    }
    dialog.pick_folder(move |folder| {
        let _ = send.send(folder.and_then(|f| f.into_path().ok()));
    });
    Ok(receive.await.ok().flatten().map(|path| path.to_string_lossy().into_owned()))
}

#[tauri::command]
pub async fn choose_torrent_file(app: AppHandle) -> Result<Option<String>> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let mut dialog = app.dialog().file().add_filter("Torrent", &["torrent"]);
    if let Some(window) = app.get_webview_window("main") {
        dialog = dialog.set_parent(&window);
    }
    dialog.pick_file(move |file| {
        let _ = send.send(file.and_then(|f| f.into_path().ok()));
    });
    Ok(receive.await.ok().flatten().map(|path| path.to_string_lossy().into_owned()))
}

#[tauri::command]
pub fn read_clipboard_text(app: AppHandle) -> String {
    app.clipboard().read_text().unwrap_or_default()
}

#[tauri::command]
pub async fn reveal_download(state: State<'_, AppState>, id: String) -> Result<bool> {
    Ok(state.core.reveal(&id).await)
}

#[tauri::command]
pub async fn open_download(state: State<'_, AppState>, id: String) -> Result<bool> {
    Ok(state.core.open(&id).await)
}

#[tauri::command]
pub async fn start_download(state: State<'_, AppState>, request: StartDownloadRequest, link_id: Option<String>) -> Result<String> {
    state.core.start_from_link(request, link_id.as_deref()).await.map_err(text)
}

#[tauri::command]
pub async fn list_downloads(state: State<'_, AppState>) -> Result<Vec<DownloadUpdate>> {
    Ok(state.core.list_downloads().await)
}

#[tauri::command]
pub async fn list_history(state: State<'_, AppState>) -> Result<Vec<FinishedDownload>> {
    Ok(state.core.list_history().await)
}

#[tauri::command]
pub async fn clear_history(state: State<'_, AppState>) -> Result<()> {
    state.core.clear_history().await.map_err(text)
}

#[tauri::command]
pub fn network_usage(state: State<'_, AppState>) -> BTreeMap<String, u64> {
    state.core.limits.used_by_period()
}

#[tauri::command]
pub async fn reset_network_usage(state: State<'_, AppState>, id: String) -> Result<()> {
    state.core.reset_network_usage(&id).await.map_err(text)
}

#[tauri::command]
pub fn free_space(state: State<'_, AppState>, dir: String) -> Option<u64> {
    state.core.free_space(&dir)
}

#[tauri::command]
pub async fn torrent_files(state: State<'_, AppState>, id: String) -> Result<Vec<TorrentFileEntry>> {
    Ok(state.core.torrent_files(&id).await)
}

#[tauri::command]
pub async fn choose_torrent_files(state: State<'_, AppState>, id: String, selected: Vec<usize>) -> Result<()> {
    state.core.choose_torrent_files(&id, selected).await.map_err(text)
}

#[tauri::command]
pub async fn pause_download(state: State<'_, AppState>, id: String) -> Result<()> {
    state.core.pause(&id).await;
    Ok(())
}

// Async, though it awaits nothing: a sync command runs on the main thread, outside the Tokio
// runtime, and resume spawns the verify-and-restart task — which panicked there and aborted the app.
#[tauri::command]
pub async fn resume_download(state: State<'_, AppState>, id: String) -> Result<()> {
    state.core.resume(&id);
    Ok(())
}

#[tauri::command]
pub async fn relink_download(state: State<'_, AppState>, id: String, url: String) -> Result<()> {
    state.core.relink(&id, &url, None, None).await.map_err(text)
}

#[tauri::command]
pub async fn set_download_network(state: State<'_, AppState>, id: String, network_id: String, enabled: bool) -> Result<()> {
    state.core.set_network_enabled(&id, &network_id, enabled).await;
    Ok(())
}

#[tauri::command]
pub async fn cancel_download(state: State<'_, AppState>, id: String) -> Result<()> {
    state.core.cancel(&id).await.map_err(text)
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RemoveOptions {
    trash_file: Option<bool>,
}

#[tauri::command]
pub async fn remove_download(state: State<'_, AppState>, id: String, options: Option<RemoveOptions>) -> Result<()> {
    let trash = options.and_then(|o| o.trash_file).unwrap_or(false);
    state.core.remove(&id, trash).await.map_err(text)
}

/// Updates come from the store or package manager in this build: there is nothing to report.
#[tauri::command]
pub async fn update_state(state: State<'_, AppState>) -> Result<UpdateState> {
    let auto_update = state.core.settings.load().await.auto_update != Some(false);
    Ok(UpdateState { status: "idle".into(), auto_update, version: None, percent: None })
}

#[tauri::command]
pub fn show_update_menu() {}

#[tauri::command]
pub fn pending_links(state: State<'_, AppState>) -> Vec<PendingLink> {
    state.core.links.pending()
}

#[tauri::command]
pub async fn resume_from_link(state: State<'_, AppState>, link_id: String, download_id: String) -> Result<()> {
    state.core.resume_from_link(&link_id, &download_id).await.map_err(text)
}

#[tauri::command]
pub fn dismiss_link(state: State<'_, AppState>, id: String) {
    state.core.links.release(&id);
}
