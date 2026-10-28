//! The app around the engine: one window, the OS's links and files handed over, the tray while
//! the browser extension may still need it, and saving every download on the way out.

mod commands;

use muxo_core::types::{DownloadUpdate, MagnetCheckStatus, NetworkInterfaceInfo, ThemeSource};
use muxo_core::{accepted_link, link_from_args, start_bridge, Bridge, Core, CoreConfig, Host, Offer};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tauri::menu::{Menu, MenuItem, PredefinedMenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{AppHandle, DragDropEvent, Emitter, Manager, RunEvent, Theme, WindowEvent};
use tauri_plugin_deep_link::DeepLinkExt;
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_opener::OpenerExt;

/// The channels the window listens on (src/shared/ipc-channels.ts).
mod channels {
    pub const DOWNLOAD_UPDATED: &str = "download:updated";
    pub const HISTORY_CHANGED: &str = "history:changed";
    pub const NETWORKS_CHANGED: &str = "network:changed";
    pub const MAGNET_CHECK_STATUS: &str = "download:magnet-check-status";
    pub const LINK_RECEIVED: &str = "app:link-received";
}

pub struct AppState {
    pub core: Arc<Core>,
    pub bridge: Arc<Bridge>,
}

struct TauriHost {
    app: AppHandle,
}

impl Host for TauriHost {
    fn download_updated(&self, update: &DownloadUpdate) {
        let _ = self.app.emit(channels::DOWNLOAD_UPDATED, update);
    }

    fn history_changed(&self) {
        let _ = self.app.emit(channels::HISTORY_CHANGED, ());
    }

    fn networks_changed(&self, networks: &[NetworkInterfaceInfo]) {
        let _ = self.app.emit(channels::NETWORKS_CHANGED, networks);
    }

    fn magnet_check_status(&self, status: &MagnetCheckStatus) {
        let _ = self.app.emit(channels::MAGNET_CHECK_STATUS, status);
    }

    fn link_received(&self) {
        bring_forward(&self.app);
        let _ = self.app.emit(channels::LINK_RECEIVED, ());
    }

    fn notify(&self, title: &str, body: &str) {
        let _ = self.app.notification().builder().title(title).body(body).show();
    }

    fn trash(&self, path: &Path) -> Result<(), String> {
        trash::delete(path).map_err(|error| error.to_string())
    }

    fn reveal(&self, path: &Path) -> bool {
        self.app.opener().reveal_item_in_dir(path).is_ok()
    }

    fn open(&self, path: &Path) -> bool {
        self.app.opener().open_path(path.to_string_lossy(), None::<&str>).is_ok()
    }

    fn app_version(&self) -> String {
        self.app.package_info().version.to_string()
    }
}

/// Windows and macOS keep a background app from taking focus unless asked firmly.
fn bring_forward(app: &AppHandle) {
    let Some(window) = app.get_webview_window("main") else { return };
    let _ = window.unminimize();
    let _ = window.show();
    #[cfg(target_os = "windows")]
    {
        let _ = window.set_always_on_top(true);
        let _ = window.set_focus();
        let _ = window.set_always_on_top(false);
    }
    let _ = window.set_focus();
}

pub(crate) fn apply_theme(app: &AppHandle, theme: ThemeSource) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.set_theme(Some(match theme {
            ThemeSource::Dark => Theme::Dark,
            ThemeSource::Light => Theme::Light,
        }));
    }
}

/// Links the OS hands over before the engine is up wait here.
static EARLY_LINKS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn offer(app: &AppHandle, candidate: &str) {
    let Some(link) = accepted_link(candidate) else { return };
    match app.try_state::<AppState>() {
        Some(state) => state.core.offer(Offer { url: link, ..Default::default() }),
        None => EARLY_LINKS.lock().unwrap().push(link),
    }
}

fn show_tray(app: &AppHandle) -> tauri::Result<()> {
    if app.tray_by_id("main").is_some() {
        return Ok(());
    }
    let open = MenuItem::with_id(app, "open", "Open Muxo", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit Muxo", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&open, &PredefinedMenuItem::separator(app)?, &quit])?;
    let mut tray = TrayIconBuilder::with_id("main").tooltip("Muxo").menu(&menu).on_menu_event(|app, event| {
        match event.id.as_ref() {
            "open" => bring_forward(app),
            "quit" => app.exit(0),
            _ => {}
        }
    });
    if let Some(icon) = app.default_window_icon() {
        tray = tray.icon(icon.clone());
    }
    tray.build(app)?;
    Ok(())
}

/// Nothing tells a process portably that the computer slept; the wall clock jumping ahead of a
/// timer does.
fn watch_for_wake(core: Arc<Core>) {
    tauri::async_runtime::spawn(async move {
        let mut last = SystemTime::now();
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let now = SystemTime::now();
            if now.duration_since(last).unwrap_or_default() > Duration::from_secs(20) {
                core.system_resumed();
            }
            last = now;
        }
    });
}

pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("MUXO_LOG").unwrap_or_else(|_| "warn".into()))
        .init();
    let quitting = Arc::new(AtomicBool::new(false));

    let app = tauri::Builder::default()
        // One Muxo at a time: a second launch (a magnet link clicked, a .torrent opened) hands
        // its link to the first and exits.
        .plugin(tauri_plugin_single_instance::init(|app, argv, _cwd| match link_from_args(&argv) {
            Some(link) => offer(app, &link),
            None => bring_forward(app),
        }))
        .plugin(tauri_plugin_deep_link::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_notification::init())
        .setup(|app| {
            let handle = app.handle().clone();
            let data_dir = app.path().app_data_dir()?;
            let downloads_dir = app.path().download_dir().unwrap_or_else(|_| data_dir.clone());
            let home_dir = app.path().home_dir().unwrap_or_else(|_| data_dir.clone());
            let host = Arc::new(TauriHost { app: handle.clone() });
            let (core, bridge) = tauri::async_runtime::block_on(async move {
                let core = Core::start(CoreConfig { data_dir, downloads_dir, home_dir }, host).await;
                let bridge = start_bridge(Arc::clone(&core)).await;
                (core, bridge)
            });
            watch_for_wake(Arc::clone(&core));
            for link in EARLY_LINKS.lock().unwrap().drain(..) {
                core.offer(Offer { url: link, ..Default::default() });
            }
            app.manage(AppState { core, bridge });

            // Windows and Linux put the first launch's link on its command line.
            let args: Vec<String> = std::env::args().collect();
            if let Some(link) = link_from_args(&args) {
                offer(&handle, &link);
            }
            let links = handle.clone();
            app.deep_link().on_open_url(move |event| {
                for url in event.urls() {
                    offer(&links, url.as_str());
                }
            });
            #[cfg(any(target_os = "linux", windows))]
            let _ = app.deep_link().register_all();

            if let Some(window) = app.get_webview_window("main") {
                window.show()?;
            }
            Ok(())
        })
        .on_window_event(|window, event| match event {
            // A .torrent dropped on the window goes to New download, like one the OS hands over.
            WindowEvent::DragDrop(DragDropEvent::Drop { paths, .. }) => {
                for path in paths {
                    offer(window.app_handle(), &path.to_string_lossy());
                }
            }
            // Once the browser extension has connected, closing the window keeps Muxo in the
            // tray, where the browser can still reach it.
            WindowEvent::CloseRequested { api, .. } => {
                let app = window.app_handle();
                if app.state::<AppState>().bridge.keeps_running_for_browser() {
                    api.prevent_close();
                    let _ = window.hide();
                    let _ = show_tray(app);
                }
            }
            _ => {}
        })
        .invoke_handler(tauri::generate_handler![
            commands::initial_state,
            commands::list_interfaces,
            commands::device_binding_supported,
            commands::open_network_settings,
            commands::update_settings,
            commands::probe_url,
            commands::choose_destination_folder,
            commands::choose_torrent_file,
            commands::read_clipboard_text,
            commands::reveal_download,
            commands::open_download,
            commands::start_download,
            commands::list_downloads,
            commands::list_history,
            commands::clear_history,
            commands::network_usage,
            commands::reset_network_usage,
            commands::free_space,
            commands::torrent_files,
            commands::choose_torrent_files,
            commands::pause_download,
            commands::resume_download,
            commands::relink_download,
            commands::set_download_network,
            commands::cancel_download,
            commands::remove_download,
            commands::update_state,
            commands::show_update_menu,
            commands::pending_links,
            commands::resume_from_link,
            commands::dismiss_link,
        ])
        .build(tauri::generate_context!())
        .expect("error while building Muxo");

    app.run(move |app, event| match event {
        // Every running download is paused and saved first, to go on next launch; never for
        // longer than a few seconds.
        RunEvent::ExitRequested { api, .. } if !quitting.load(Ordering::SeqCst) => {
            api.prevent_exit();
            quitting.store(true, Ordering::SeqCst);
            let core = Arc::clone(&app.state::<AppState>().core);
            let app = app.clone();
            tauri::async_runtime::spawn(async move {
                let _ = tokio::time::timeout(Duration::from_secs(3), core.suspend_all()).await;
                app.exit(0);
            });
        }
        // macOS hands opened files and links over as events.
        #[cfg(target_os = "macos")]
        RunEvent::Opened { urls } => {
            for url in urls {
                let candidate = url.to_file_path().map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| url.to_string());
                offer(app, &candidate);
            }
        }
        #[cfg(target_os = "macos")]
        RunEvent::Reopen { .. } => bring_forward(app),
        _ => {}
    });
}
