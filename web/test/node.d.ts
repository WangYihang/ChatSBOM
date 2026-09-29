/**
 * The two Node built-ins the contract suite uses, declared narrowly.
 *
 * For the reason `raw.d.ts` gives: the test tsconfig names its types,
 * and `@types/node` is not installed — nor wanted, since its globals
 * (`fetch`, `Request`, `Response`) are declared again, differently, by
 * `@cloudflare/workers-types`.
 *
 * `readFileSync` reads what the suite recorded (`contractcalls.ts`),
 * which may not be there yet when it is recording, so it cannot be a
 * `?raw` import.
 */
declare module 'node:sqlite' {
  export class StatementSync {
    all(...parameters: unknown[]): Record<string, unknown>[];
  }
  export class DatabaseSync {
    constructor(location: string);
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}

declare module 'node:fs' {
  export function readFileSync(path: string, encoding: 'utf8'): string;
  export function writeFileSync(path: string, data: string): void;
}
