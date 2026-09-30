import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  // Component tests render JSX, so the test transform needs the same
  // plugin the build uses.
  plugins: [react()],
  resolve: {
    alias: [
      {
        // A Web Worker's script, which the build makes a file of its own
        // for the page to start: jsdom starts none (test/worker.ts).
        find: /^.+\?worker$/,
        replacement: new URL('./test/worker.ts', import.meta.url).pathname,
      },
    ],
  },
  test: {
    // Both extensions. The include pattern listed only `.ts`, which
    // meant a `.tsx` test file was silently not a test: it was written,
    // it passed when run by name, and `npm test` never looked at it.
    include: ['test/**/*.test.ts', 'test/**/*.test.tsx'],
    // Text queries read what is drawn, not a chart's hidden table.
    setupFiles: ['test/setup.ts'],
  },
});
