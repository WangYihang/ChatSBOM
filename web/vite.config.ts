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
    // Written, for reading a stack trace locally, but not pointed at by
    // the bundle; `public/.assetsignore` keeps them out of what is
    // served. `true` published the whole source beside it (#31).
    sourcemap: 'hidden',
    // Fonts stay files. Under the 4 KiB default a subset became a
    // `data:` URL in the stylesheet, which the page's policy refuses
    // (`font-src 'self'`), and which every visitor downloads whether or
    // not a character of it is on the page.
    assetsInlineLimit: (file) => (/\.woff2?$/.test(file) ? false : undefined),
  },
  // DuckDB-WASM ships its worker and wasm as separate assets; excluding it
  // from dep optimisation keeps those URLs resolvable.
  optimizeDeps: {
    exclude: ['@duckdb/duckdb-wasm'],
  },
  plugins: [react(), cloudflare()],
});
