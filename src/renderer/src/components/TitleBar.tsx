import { getCurrentWindow } from '@tauri-apps/api/window'
import { Minus, Square, X } from 'lucide-react'
import { TITLE_BAR_HEIGHT } from '../theme'

const isMac = window.muxo.platform === 'darwin'

/** The window's title bar, the same on every OS: the logo and name, centered, on a strip the window
 * is dragged by. macOS draws its traffic lights over the left of it; on Windows and Linux the
 * window has no frame of its own, so minimize/maximize/close are drawn here, at the right. */
export function TitleBar(): React.JSX.Element {
  return (
    <div
      data-tauri-drag-region
      style={{ height: TITLE_BAR_HEIGHT }}
      className={`relative flex shrink-0 items-center justify-center border-b-[0.5px] border-border bg-background ${
        // Clear of the controls on either side, so the name stays centered between them.
        isMac ? 'px-[94px]' : 'px-[140px]'
      }`}
    >
      <div
        data-tauri-drag-region
        className="pointer-events-none flex items-center gap-2 truncate font-sans text-[13px] leading-none font-semibold tracking-[0.02em] text-foreground"
      >
        <MuxoMark />
        Muxo
      </div>
      {!isMac && <WindowControls />}
    </div>
  )
}

/** The logo's three strands joining into a download arrow (build/logo/muxo.svg), without its tile. */
function MuxoMark(): React.JSX.Element {
  return (
    <svg width="14" height="14" viewBox="120 80 272 344" fill="none" aria-hidden="true">
      <g stroke="currentColor" strokeWidth="40" strokeLinecap="round" strokeLinejoin="round">
        <path d="M146 104C146 220 256 210 256 300M366 104C366 220 256 210 256 300M256 104V380" />
        <path d="M198 342l58 58 58-58" />
      </g>
    </svg>
  )
}

function WindowControls(): React.JSX.Element {
  const appWindow = getCurrentWindow()
  const button =
    'flex h-full w-[46px] cursor-default items-center justify-center text-[var(--text-secondary)] hover:bg-secondary hover:text-foreground active:scale-100'
  return (
    <div className="absolute top-0 right-0 flex h-full">
      <button aria-label="Minimize" className={button} onClick={() => void appWindow.minimize()}>
        <Minus size={14} />
      </button>
      <button
        aria-label="Maximize"
        className={button}
        onClick={() => void appWindow.toggleMaximize()}
      >
        <Square size={12} />
      </button>
      <button
        aria-label="Close"
        className={`${button} hover:bg-[#e81123] hover:text-white`}
        onClick={() => void appWindow.close()}
      >
        <X size={15} />
      </button>
    </div>
  )
}
