import { invoke as tauriInvoke } from '@tauri-apps/api/core'
import { listen } from '@tauri-apps/api/event'
import { IpcChannels } from '@shared/ipc-channels'
import type { IpcContract } from '@shared/ipc-contract'
import type {
  AppSettings,
  DownloadUpdate,
  InitialState,
  MagnetCheckStatus,
  NetworkInterfaceInfo,
  UpdateState
} from '@shared/types'

// What the Electron preload used to expose as window.muxo, now over Tauri: each request is a
// Rust command (src-tauri/src/commands.rs) and each push event a Tauri event on the same channel
// name as before. The renderer only ever talks to this object.

type Platform = 'darwin' | 'win32' | 'linux'

function detectPlatform(): Platform {
  const agent = navigator.userAgent
  if (/Mac/i.test(agent)) return 'darwin'
  if (/Win/i.test(agent)) return 'win32'
  return 'linux'
}

/** A failed command rejects with its message; the window expects an Error. */
async function invoke<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await tauriInvoke<T>(command, args)
  } catch (error) {
    throw error instanceof Error ? error : new Error(String(error))
  }
}

/** Subscribes now and returns the unsubscribe at once, as ipcRenderer.on did; one called before
 * the subscription lands still takes effect. */
function on<T>(channel: string, callback: (payload: T) => void): () => void {
  let unlisten: (() => void) | null = null
  let gone = false
  void listen<T>(channel, (event) => callback(event.payload)).then((stop) => {
    if (gone) stop()
    else unlisten = stop
  })
  return () => {
    gone = true
    unlisten?.()
  }
}

/** JSON can't carry `undefined`, which is how the window clears a setting: it's sent as null. */
function clearable(patch: AppSettings): Record<string, unknown> {
  return Object.fromEntries(Object.entries(patch).map(([key, value]) => [key, value ?? null]))
}

type Result<K extends keyof IpcContract> = Promise<IpcContract[K]['result']>

function createApi(initialState: InitialState) {
  return {
    platform: detectPlatform(),
    initialState,

    listInterfaces: (): Result<'listInterfaces'> => invoke('list_interfaces'),
    deviceBindingSupported: (): Result<'deviceBindingSupported'> => invoke('device_binding_supported'),
    openNetworkSettings: (): Result<'openNetworkSettings'> => invoke('open_network_settings'),
    updateSettings: (patch: AppSettings): Result<'updateSettings'> =>
      invoke('update_settings', { patch: clearable(patch) }),
    probeUrl: (url: string): Result<'probeUrl'> => invoke('probe_url', { url }),
    chooseDestinationFolder: (defaultPath: string): Result<'chooseDestinationFolder'> =>
      invoke('choose_destination_folder', { defaultPath }),
    chooseTorrentFile: (): Result<'chooseTorrentFile'> => invoke('choose_torrent_file'),
    /** A .torrent dropped on the window is handed over by the shell instead (see lib.rs). */
    pathForFile: (_file: File): string => '',
    readClipboardText: (): Result<'readClipboardText'> => invoke('read_clipboard_text'),
    revealDownload: (id: string): Result<'revealDownload'> => invoke('reveal_download', { id }),
    openDownload: (id: string): Result<'openDownload'> => invoke('open_download', { id }),
    startDownload: (
      request: IpcContract['startDownload']['args'][0],
      linkId?: string
    ): Result<'startDownload'> => invoke('start_download', { request, linkId: linkId ?? null }),
    listDownloads: (): Result<'listDownloads'> => invoke('list_downloads'),
    listHistory: (): Result<'listHistory'> => invoke('list_history'),
    clearHistory: (): Result<'clearHistory'> => invoke('clear_history'),
    networkUsage: (): Result<'networkUsage'> => invoke('network_usage'),
    resetNetworkUsage: (id: string): Result<'resetNetworkUsage'> => invoke('reset_network_usage', { id }),
    freeSpace: (dir: string): Result<'freeSpace'> => invoke('free_space', { dir }),
    torrentFiles: (downloadId: string): Result<'torrentFiles'> => invoke('torrent_files', { id: downloadId }),
    chooseTorrentFiles: (downloadId: string, selected: number[]): Result<'chooseTorrentFiles'> =>
      invoke('choose_torrent_files', { id: downloadId, selected }),
    pauseDownload: (downloadId: string): Result<'pauseDownload'> => invoke('pause_download', { id: downloadId }),
    resumeDownload: (downloadId: string): Result<'resumeDownload'> =>
      invoke('resume_download', { id: downloadId }),
    relinkDownload: (downloadId: string, url: string): Result<'relinkDownload'> =>
      invoke('relink_download', { id: downloadId, url }),
    setDownloadNetwork: (downloadId: string, networkId: string, enabled: boolean): Result<'setDownloadNetwork'> =>
      invoke('set_download_network', { id: downloadId, networkId, enabled }),
    cancelDownload: (downloadId: string): Result<'cancelDownload'> => invoke('cancel_download', { id: downloadId }),
    removeDownload: (downloadId: string, options?: { trashFile?: boolean }): Result<'removeDownload'> =>
      invoke('remove_download', { id: downloadId, options: options ?? null }),
    updateState: (): Result<'updateState'> => invoke('update_state'),
    showUpdateMenu: (): Result<'showUpdateMenu'> => invoke('show_update_menu'),
    pendingLinks: (): Result<'pendingLinks'> => invoke('pending_links'),
    resumeFromLink: (linkId: string, downloadId: string): Result<'resumeFromLink'> =>
      invoke('resume_from_link', { linkId, downloadId }),
    dismissLink: (id: string): Result<'dismissLink'> => invoke('dismiss_link', { id }),

    /** pendingLinks() has the new link. */
    onLinkReceived: (callback: () => void): (() => void) => on(IpcChannels.linkReceived, () => callback()),
    onDownloadUpdated: (callback: (update: DownloadUpdate) => void): (() => void) =>
      on(IpcChannels.downloadUpdated, callback),
    /** A download was added to the finished ones or forgotten: listHistory() has them. */
    onHistoryChanged: (callback: () => void): (() => void) => on(IpcChannels.historyChanged, () => callback()),
    onUpdateState: (callback: (state: UpdateState) => void): (() => void) =>
      on(IpcChannels.updateStateChanged, callback),
    onNetworksChanged: (callback: (networks: NetworkInterfaceInfo[]) => void): (() => void) =>
      on(IpcChannels.networksChanged, callback),
    onMagnetCheckStatus: (callback: (status: MagnetCheckStatus) => void): (() => void) =>
      on(IpcChannels.magnetCheckStatus, callback)
  }
}

export type MuxoApi = ReturnType<typeof createApi>

/** Reads everything the first paint needs, then installs window.muxo. Awaited before the app's
 * modules load, as the preload's synchronous read was. */
export async function installMuxoApi(): Promise<void> {
  const initialState = await invoke<InitialState>('initial_state')
  window.muxo = createApi(initialState)
}
