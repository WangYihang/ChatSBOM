/**
 * Vite's `?url` imports, declared narrowly.
 *
 * The browser tsconfig sets `types: []` on purpose, so pulling in all of
 * `vite/client` to type one import would widen the ambient surface far
 * more than it needs to. This declares exactly the form used.
 */
declare module '*?url' {
  const url: string;
  export default url;
}
