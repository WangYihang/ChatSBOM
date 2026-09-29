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

/**
 * Stylesheets, which app.tsx imports only for their side effects: the
 * fonts and style.css.
 *
 * From TypeScript 6, `noUncheckedSideEffectImports` is on by default,
 * so such an import must resolve to a module TypeScript knows, and a
 * stylesheet is not one. The declaration is `vite/client`'s own: a
 * module with no exports rather than `any`, because a plain `.css`
 * import has no exports in Vite.
 */
declare module '*.css' {}
