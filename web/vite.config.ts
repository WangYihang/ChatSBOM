import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  // index.html sits at the package root, the conventional Vite layout, so
  // the /src/main.tsx it loads resolves. dist/client is what `chatsbom web
  // serve` serves, and what the web service's image copies (DEPLOY.md).
  build: {
    outDir: 'dist/client',
    emptyOutDir: true,
    target: 'es2022',
    // Written, for reading a stack trace locally, but not pointed at by
    // the bundle, and never served: the service answers a map's path
    // with a 404 (chatsbom/server/app.py). `true` published the whole
    // source beside it (#31).
    sourcemap: 'hidden',
    // Fonts stay files. Under the 4 KiB default a subset became a
    // `data:` URL in the stylesheet, which the page's policy refuses
    // (`font-src 'self'`), and which every visitor downloads whether or
    // not a character of it is on the page.
    assetsInlineLimit: (file) => (/\.woff2?$/.test(file) ? false : undefined),
  },
  plugins: [react()],
  server: {
    // `npm run dev` serves the page from its sources, and hands what it
    // asks to a `chatsbom web serve` on this machine, where that listens
    // unless told otherwise.
    proxy: { '/api': 'http://127.0.0.1:8080' },
  },
});
