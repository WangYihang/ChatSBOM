import { cloudflare } from '@cloudflare/vite-plugin';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  // index.html sits at the package root, the conventional Vite layout, so
  // /src/app.ts resolves. dist/ is what wrangler serves as static assets.
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    target: 'es2022',
    sourcemap: true,
  },
  // DuckDB-WASM ships its worker and wasm as separate assets; excluding it
  // from dep optimisation keeps those URLs resolvable.
  optimizeDeps: {
    exclude: ['@duckdb/duckdb-wasm'],
  },
  plugins: [react(), cloudflare()],
});
