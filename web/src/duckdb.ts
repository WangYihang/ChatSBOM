/**
 * DuckDB-WASM connection, configured to read Parquet over HTTP ranges.
 *
 * The whole dataset is ~21 MB of Parquet, but nothing downloads it all:
 * DuckDB reads the footer, prunes row groups by the statistics it finds
 * there, and fetches only the byte ranges it needs. That is why the
 * artifacts file is written sorted by package name — a "who depends on X"
 * lookup touches a handful of row groups instead of the file.
 *
 * The engine is served from this origin, not from a CDN. Two reasons,
 * one of them fatal:
 *
 *   - `new Worker(url)` refuses a cross-origin script, so DuckDB's own
 *     `getJsDelivrBundles()` cannot be handed straight to `new Worker`.
 *     Measured, not assumed: it fails with "Script at
 *     'https://cdn.jsdelivr.net/.../duckdb-browser-eh.worker.js' cannot
 *     be accessed from origin".
 *   - A dashboard whose query engine lives on a third-party CDN is down
 *     whenever that CDN is unreachable, which for jsDelivr is a routine
 *     condition in some networks.
 *
 * The worker script (~0.7 MB) ships as a static asset; Vite emits it
 * next to the bundle via the `?url` import. The module (~33 MB) exceeds
 * the 25 MiB static-asset cap, so it is served from R2 by the Worker
 * under /wasm/, cached immutably.
 */
import * as duckdb from '@duckdb/duckdb-wasm';
// Vite rewrites these to same-origin asset URLs at build time.
import ehWorkerUrl from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

import type { Queryable } from './queries';

/** Where the Worker serves the WebAssembly module from. */
export const WASM_MODULE_URL = '/wasm/duckdb-eh.wasm';

export interface Manifest {
  schemaVersion: string;
  generator: string;
  rowCounts: Record<string, number>;
  files: { name: string; bytes: number; sha256: string }[];
}

/** Adapts a DuckDB connection to the narrow interface queries.ts needs. */
export class DuckDbConnection implements Queryable {
  constructor(private readonly connection: duckdb.AsyncDuckDBConnection) {}

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    if (params.length === 0) {
      const result = await this.connection.query(sql);
      return result.toArray().map((row) => row.toJSON() as T);
    }

    const statement = await this.connection.prepare(sql);
    try {
      const result = await statement.query(...params);
      return result.toArray().map((row) => row.toJSON() as T);
    } finally {
      await statement.close();
    }
  }

  async close(): Promise<void> {
    await this.connection.close();
  }
}

/**
 * The `eh` build is the only one shipped.
 *
 * DuckDB publishes `mvp`, `eh` and `coi` builds. `coi` needs COOP/COEP
 * headers this deployment does not set, and `mvp` exists for engines
 * without WebAssembly exception handling — a set that no longer includes
 * any browser able to run the rest of this page. Shipping one build
 * halves what has to be uploaded and keeps the cache warm for everyone.
 */
export function ehBundle(): duckdb.DuckDBBundle {
  // pthreadWorker is for the `coi` build only; this one is single-threaded.
  return {
    mainModule: WASM_MODULE_URL,
    mainWorker: ehWorkerUrl,
    pthreadWorker: null,
  };
}

/** A browser too old for the engine should say so, not fail obscurely. */
export async function assertExceptionsSupported(): Promise<void> {
  const features = await duckdb.getPlatformFeatures();
  if (!features.wasmExceptions) {
    throw new Error(
      'This browser lacks WebAssembly exception handling, which the query ' +
        'engine requires. A current Chrome, Firefox or Safari will work.',
    );
  }
}

/** Boot DuckDB-WASM and point it at the dataset. */
export async function connect(
  baseUrl = '/data',
): Promise<{ db: DuckDbConnection; manifest: Manifest }> {
  await assertExceptionsSupported();
  const manifest = await loadManifest(baseUrl);

  const bundle = ehBundle();
  const worker = new Worker(bundle.mainWorker!);
  const database = new duckdb.AsyncDuckDB(
    new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING),
    worker,
  );
  await database.instantiate(bundle.mainModule, bundle.pthreadWorker ?? null);

  const connection = await database.connect();
  // Range requests are how this stays cheap; without them DuckDB falls
  // back to downloading each file whole. This caches the footer and
  // row-group statistics so repeat queries skip re-reading them.
  //
  // `http_keep_alive` is deliberately absent: it belongs to the httpfs
  // extension, which this build never loads — DuckDB-WASM has its own
  // HTTP filesystem — so setting it raises "Extension parameter
  // http_keep_alive was not found after autoloading" and aborts the
  // boot. Connection reuse is the browser fetch stack's business anyway.
  await connection.query(`SET enable_http_metadata_cache = true`);

  return { db: new DuckDbConnection(connection), manifest };
}

async function loadManifest(baseUrl: string): Promise<Manifest> {
  const response = await fetch(`${baseUrl}/manifest.json`);
  if (!response.ok) {
    throw new Error(
      `Could not load dataset manifest (${response.status}). ` +
        `Has \`chatsbom export parquet\` been run and uploaded?`,
    );
  }
  return (await response.json()) as Manifest;
}
