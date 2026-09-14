/**
 * How the dataset is handed to the engine.
 *
 * Measured on the wire: passing a URL straight to `read_parquet` made
 * DuckDB-WASM probe with HEAD and then GET the whole file — 37 requests,
 * 0 of them ranged, 20.6 MB of Parquet on every cold load. Registering
 * each file as an HTTP-protocol handle is what enables partial reads;
 * `read_parquet` then refers to the registered name.
 *
 * Adding `Content-Length` to the HEAD reply was necessary but not
 * sufficient — a Parquet footer is found from the end of the file, so a
 * missing length alone would also defeat ranged reads. Both are needed.
 */
import { describe, expect, it, vi } from 'vitest';

import { registerDataset } from '../src/duckdb';

const FILES = {
  repositories: 'repositories-aaaaaaaa.parquet',
  artifacts: 'artifacts-bbbbbbbb.parquet',
};

function fakeDatabase() {
  return {
    registerFileURL: vi.fn(() => Promise.resolve()),
    calls() {
      return this.registerFileURL.mock.calls;
    },
  };
}

describe('registerDataset', () => {
  it('registers every file as an HTTP handle', async () => {
    const db = fakeDatabase();
    await registerDataset(db, 'https://x.example/data', FILES);
    // 4 is DuckDBDataProtocol.HTTP, read from the package rather than
    // assumed: the enum is BUFFER 0, NODE_FS 1, BROWSER_FILEREADER 2,
    // BROWSER_FSACCESS 3, HTTP 4, S3 5.
    for (const call of db.calls()) {
      expect(call[2]).toBe(4);
    }
    expect(db.calls()).toHaveLength(2);
  });

  it('registers under the bare filename, which the SQL then names', async () => {
    const db = fakeDatabase();
    await registerDataset(db, 'https://x.example/data', FILES);
    const names = db.calls().map((c) => c[0]);
    expect(names).toContain('artifacts-bbbbbbbb.parquet');
    expect(names).toContain('repositories-aaaaaaaa.parquet');
  });

  it('points each handle at the absolute URL', async () => {
    const db = fakeDatabase();
    await registerDataset(db, 'https://x.example/data', FILES);
    const urls = db.calls().map((c) => c[1]);
    expect(urls).toContain('https://x.example/data/artifacts-bbbbbbbb.parquet');
  });

  it('asks for buffered reads, not directIO', async () => {
    const db = fakeDatabase();
    await registerDataset(db, 'https://x.example/data', FILES);
    // directIO bypasses the engine's own range logic; buffered is what
    // lets it fetch a footer and then prune row groups.
    for (const call of db.calls()) {
      expect(call[3]).toBe(false);
    }
  });

  it('skips a table the manifest does not name', async () => {
    const db = fakeDatabase();
    await registerDataset(db, 'https://x.example/data', { artifacts: 'a-1.parquet' });
    expect(db.calls()).toHaveLength(1);
  });
});
