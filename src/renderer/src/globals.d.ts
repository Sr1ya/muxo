import type { MuxoApi } from './muxo'

declare global {
  interface Window {
    muxo: MuxoApi
  }
}
