import { defineConfig } from 'vite'

export default defineConfig({
  base: './',
  // 5173 落在 Windows 保留端口范围（如 5141-5240）内会报 EACCES，固定用范围外的端口
  server: {
    port: 5250,
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
