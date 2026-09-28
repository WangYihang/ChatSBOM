/**
 * Vite's `?raw` imports, for the tests that read a file as text.
 *
 * Declared here, narrowly, for the reason `src/assets.d.ts` gives: the
 * test tsconfig names its types, and `vite/client` would widen them.
 */
declare module '*?raw' {
  const text: string;
  export default text;
}

/**
 * Vite's glob import, as the markup scan uses it: every matching file,
 * as text. Declared as narrowly as `?raw` above, for the same reason.
 */
interface ImportMeta {
  glob(
    pattern: string,
    options: { query: '?raw'; import: 'default'; eager: true },
  ): Record<string, string>;
}
