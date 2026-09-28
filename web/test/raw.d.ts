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
