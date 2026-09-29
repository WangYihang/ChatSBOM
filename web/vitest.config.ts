import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  // Component tests render JSX, so the test transform needs the same
  // plugin the build uses.
  plugins: [react()],
  resolve: {
    alias: {
      // A module of the Workers runtime, which Node does not have: the
      // spend counter's base class comes from it (test/cloudflare-workers.ts).
      'cloudflare:workers': new URL('./test/cloudflare-workers.ts', import.meta.url)
        .pathname,
    },
  },
  test: {
    // Both extensions. The include pattern listed only `.ts`, which
    // meant a `.tsx` test file was silently not a test: it was written,
    // it passed when run by name, and `npm test` never looked at it.
    include: ['test/**/*.test.ts', 'test/**/*.test.tsx'],
    // Text queries read what is drawn, not a chart's hidden table.
    setupFiles: ['test/setup.ts'],
    // Node's own Web Storage off in the workers, so that `localStorage`
    // is jsdom's. From Node 25 Node has one, a global that is undefined
    // unless --localstorage-file names a file, and it hid jsdom's: every
    // test that clears storage first threw, 50 of them on Node 26. Node
    // 22 takes the flag, and has nothing to turn off.
    execArgv: ['--no-experimental-webstorage'],
    env: {
      // For the test that runs the Worker in workerd: wrangler would
      // otherwise fetch the request metadata it hands a Worker from
      // Cloudflare, and report its usage there.
      CLOUDFLARE_CF_FETCH_ENABLED: 'false',
      WRANGLER_SEND_METRICS: 'false',
    },
  },
});
