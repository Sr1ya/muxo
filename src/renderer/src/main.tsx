import './assets/main.css'

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { installMuxoApi } from './muxo'

// The app's modules read window.muxo as they load, so it's in place before they're imported.
await installMuxoApi()
const { default: App } = await import('./App')

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
)
