/**
 * The Node built-in the tests use, declared narrowly.
 *
 * For the reason `raw.d.ts` gives: the test tsconfig names its types,
 * and `@types/node` is not installed for the sake of two functions,
 * whose globals the tests of a page have no use for.
 *
 * `readFileSync` reads what a test recorded (`contracturls.test.ts`),
 * which may not be there yet when it is recording, so it cannot be a
 * `?raw` import; and the service's source (`askcodes.test.ts`), which
 * is outside the package.
 */
declare module 'node:fs' {
  export function readFileSync(path: string, encoding: 'utf8'): string;
  export function writeFileSync(path: string, data: string): void;
}
