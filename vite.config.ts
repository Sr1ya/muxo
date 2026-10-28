import { resolve } from 'node:path'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// The window's code, served to Tauri's webview: src/renderer is the root, built into dist/.
export default defineConfig({
  root: resolve(import.meta.dirname, 'src/renderer'),
  clearScreen: false,
  resolve: {
    alias: {
      '@renderer': resolve(import.meta.dirname, 'src/renderer/src'),
      '@shared': resolve(import.meta.dirname, 'src/shared')
    }
  },
  plugins: [react(), tailwindcss()],
  server: { port: 5173, strictPort: true },
  envPrefix: ['VITE_', 'TAURI_ENV_'],
  build: {
    outDir: resolve(import.meta.dirname, 'dist'),
    emptyOutDir: true,
    // main.tsx awaits window.muxo before the app loads.
    target: ['es2022', 'safari16', 'chrome105']
  }
})
