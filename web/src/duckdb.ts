/**
 * DuckDB-WASM connection, configured to read Parquet over HTTP ranges.
 *
 * The whole dataset is ~19 MB of Parquet, but nothing downloads it all:
 * DuckDB reads the footer, prunes row groups by the statistics it finds
 * there, and fetches only the byte ranges it needs. That is why the
 * artifacts file is written sorted by package name — a "who depends on X"
 * lookup touches a handful of row groups instead of the file.
 */
import * as duckdb from '@duckdb/duckdb-wasm';

import type { Queryable } from './queries';

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
 * Boot DuckDB-WASM and point it at the dataset.
 *
 * Uses the bundle DuckDB selects for the current browser. The
 * multi-threaded bundle needs COOP/COEP headers, which the Worker does
 * not set, so this deliberately stays on the single-threaded path.
 */
export async function connect(
  baseUrl = '/data',
): Promise<{ db: DuckDbConnection; manifest: Manifest }> {
  const manifest = await loadManifest(baseUrl);

  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const worker = new Worker(bundle.mainWorker!);
  const database = new duckdb.AsyncDuckDB(
    new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING),
    worker,
  );
  await database.instantiate(bundle.mainModule, bundle.pthreadWorker ?? null);

  const connection = await database.connect();
  // Range requests are how this stays cheap; without them DuckDB falls
  // back to downloading each file whole.
  await connection.query(`SET enable_http_metadata_cache = true`);
  await connection.query(`SET http_keep_alive = true`);

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
