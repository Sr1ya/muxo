<p align="center">
  <img src="build/icon.png" width="128" alt="Muxo icon">
</p>

<h1 align="center">Muxo</h1>

<p align="center">
  Download files and torrents faster by combining all your internet connections.<br>
  Built on <a href="https://github.com/anmolkapil/plexo">anmolkapil/plexo</a>, rewritten in Rust + Tauri.
</p>

## What it does

Muxo splits a download into blocks and pulls them over every network you have at once (Wi-Fi,
Ethernet, a phone over USB), so the file arrives at the speed of all of them together.

- **Combines networks.** Each connection is pinned to one network; faster networks take more blocks.
- **Pause and resume**, even across restarts and crashes, with checks that the file on the server
  hasn't changed in between.
- **Torrents and magnet links**, with a choice of which files to fetch.
- **Speed and data limits** per network, a slow mode, and a download schedule.
- **Browser extension** for Chrome and Firefox that hands downloads to the app.
- **Pitch-black UI** built for OLED screens, with one blue accent and a color per network.

## How it's built

| Part | Where | What it is |
| --- | --- | --- |
| Engine | `crates/muxo-core` | Pure Rust, no UI: the download manager, per-network HTTP streams, torrents (librqbit), limits, schedule, persistence, history, the browser bridge |
| App | `src-tauri` | Tauri v2 shell: one command per IPC call, events to the window, single instance, `magnet:`/`muxo:` links, `.torrent` files, tray, saving downloads on quit |
| Window | `src/renderer` | React + Tailwind UI; `muxo.ts` is the `window.muxo` API over Tauri. Colors and animations live in `assets/main.css` |
| Shared | `src/shared` | The TypeScript types and IPC contract the Rust types mirror field for field |
| Extension | `extension` | The Chrome/Firefox extension; it speaks the 127.0.0.1 bridge protocol |
| Logo | `build/logo` | The logo as SVG and 1024px PNG: `muxo` (main), `muxo-light`, `muxo-off` (the extension's off icon), `muxo-ios` (square, no transparency) |

The engine pulls blocks from a shared queue so faster networks take more, races slow blocks on
idle connections near the end, grows stream counts until every stream is answered and backs off on
refusals or a slow disk, pins sockets to each network (`SO_BINDTODEVICE` on Linux, `IP_BOUND_IF`
on macOS, the source address on Windows), checks versions so a file changed on the server is never
stitched together, and keeps crash-safe manifests for resuming across restarts.

## Build from source

You need Rust (stable), Node.js 22+, and on Linux the WebKitGTK development packages:

```bash
sudo apt install libwebkit2gtk-4.1-dev libsoup-3.0-dev libjavascriptcoregtk-4.1-dev \
  librsvg2-dev libayatana-appindicator3-dev build-essential libssl-dev
```

Then, from the project folder:

```bash
npm install
npm run dev        # the app, with hot reload
npm run build      # installers for this OS in target/release/bundle
npm test           # the engine's unit and end-to-end tests
```

If `cargo` isn't found, add Rust to your PATH: `export PATH="$HOME/.cargo/bin:$PATH"`.

On Linux, `npm run build` makes a `.deb`, an `.rpm` and an AppImage. Install the `.deb` with:

```bash
sudo apt install ./target/release/bundle/deb/Muxo_1.0.0_amd64.deb
```

| Command | What it does |
| --- | --- |
| `npm run typecheck` | Typechecks the window's code |
| `npm run build:extension` | Builds the Chrome and Firefox extensions into `extension/dist` |
| `npm run icons` | Regenerates `src-tauri/icons` (desktop, Android, iOS) from `build/icon.png` |
| `MUXO_LOG=debug npm run dev` | Logs what the engine does |

After `npm run icons`, regenerate the iOS icons from `build/logo/muxo-ios.png`: the App Store
rejects icons with transparent corners, which the generated ones have.

### Windows and macOS

The code supports both, but each installer has to be built on its own OS: run `npm run build` on
a Windows PC for the `.exe`/`.msi`, and on a Mac for the `.dmg`/`.app` (macOS builds need Xcode,
so they can't be made on Linux). GitHub Actions with
[`tauri-action`](https://github.com/tauri-apps/tauri-action) can build all three platforms at once.

Unsigned builds work but warn: Windows shows SmartScreen, and macOS blocks the app until it's
allowed in System Settings. Signing needs a code-signing certificate on Windows and an Apple
Developer account on macOS.

## Known limitations

- **Torrent peers aren't pinned to networks.** librqbit opens its own peer sockets, so a torrent
  runs over the default route and its traffic is credited to the first network that's on.
  HTTP downloads combine networks.
- **No auto-update.** The update button stays idle; wiring up `tauri-plugin-updater` needs a
  signing key and a release feed.
- **Browser sign-ins aren't saved.** A download resumed after a restart goes without the browser's
  cookies.
- **Keep-awake and Windows sparse staging files** aren't implemented yet (`Host::keep_awake` is
  the hook for the first).
- **Dropping a link's text** on the window doesn't open New download on Windows and Linux, where
  Tauri's file drop takes over the window; dropping a `.torrent` file does.
- **No mobile build yet.** Combining Wi-Fi and mobile data on Android needs a native plugin that
  binds sockets through `ConnectivityManager`.
- **Settings and downloads from Plexo aren't carried over**, since Muxo has its own app id.

## License

[MIT](LICENSE), copyright 2026 Anmol Kapil.
