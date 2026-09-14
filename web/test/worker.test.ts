import { describe, expect, it } from 'vitest';

import { isServableData, parseRange } from '../src/worker';

describe('parseRange', () => {
  it('parses a closed range', () => {
    expect(parseRange('bytes=0-1023')).toEqual({ offset: 0, length: 1024 });
  });

  it('parses an open-ended range', () => {
    expect(parseRange('bytes=1024-')).toEqual({ offset: 1024 });
  });

  it('parses a suffix range, which is how Parquet finds its footer', () => {
    expect(parseRange('bytes=-8')).toEqual({ suffix: 8 });
  });

  it('tolerates surrounding whitespace', () => {
    expect(parseRange('  bytes=0-1  ')).toEqual({ offset: 0, length: 2 });
  });

  it.each([
    'bytes=-',
    'bytes=10-5',
    'bytes=abc-def',
    'items=0-10',
    'bytes=0-10, 20-30',
    '',
    'bytes=-0',
  ])('rejects %o', (header) => {
    expect(parseRange(header)).toBeNull();
  });
});

describe('content-addressed data keys', () => {
  // Filenames carry their own content hash now, so the servable set
  // cannot be a fixed list. Validating the shape keeps the guarantee
  // that mattered — a path cannot address arbitrary keys — without
  // pinning it to one export's filenames.
  it('serves a hashed table file', () => {
    expect(isServableData('repositories-659592a2.parquet')).toBe(true);
    expect(isServableData('artifacts-12e8dd23.parquet')).toBe(true);
  });

  it('serves the manifest, whose name must stay stable to be found', () => {
    expect(isServableData('manifest.json')).toBe(true);
  });

  it('refuses a table that is not one of ours', () => {
    expect(isServableData('secrets-12e8dd23.parquet')).toBe(false);
  });

  it('refuses a name without a hash, which an old export would use', () => {
    expect(isServableData('repositories.parquet')).toBe(false);
  });

  it('refuses a hash of the wrong shape', () => {
    expect(isServableData('repositories-zzzz.parquet')).toBe(false);
    expect(isServableData('repositories-659592a2ff.parquet')).toBe(false);
  });

  it('refuses traversal and nested paths', () => {
    expect(isServableData('../wrangler.jsonc')).toBe(false);
    expect(isServableData('a/repositories-659592a2.parquet')).toBe(false);
  });
});

describe('HEAD must advertise the size', () => {
  /**
   * Measured, not theorised. DuckDB-WASM probes a Parquet file with HEAD
   * before reading it: it needs the length to compute where the footer
   * starts, because a Parquet footer is located from the *end* of the
   * file. Our HEAD replied `200` with `Accept-Ranges: bytes` but no
   * `Content-Length`, so the engine could not locate the footer and fell
   * back to downloading all 16,651,228 bytes — measured on the wire as
   * 36 requests, 0 of them ranged, 28 MB on first load.
   *
   * `Accept-Ranges` alone is not enough. Size is the thing.
   */
  it('returns the object size on HEAD', async () => {
    const response = await headData('artifacts-12e8dd23.parquet');
    expect(response.headers.get('content-length')).toBe('4096');
  });

  it('still advertises range support', async () => {
    const response = await headData('artifacts-12e8dd23.parquet');
    expect(response.headers.get('accept-ranges')).toBe('bytes');
  });

  it('sends no body on HEAD', async () => {
    const response = await headData('artifacts-12e8dd23.parquet');
    expect(await response.text()).toBe('');
  });

  it('exposes the headers a cross-origin reader needs to see them', async () => {
    const response = await headData('artifacts-12e8dd23.parquet');
    const exposed = response.headers.get('access-control-expose-headers') ?? '';
    // A browser hides every header but a safelisted few from a
    // cross-origin response, and content-length is not safelisted for
    // reading via the Fetch API in all cases — name it explicitly
    // alongside the range headers.
    for (const name of ['content-length', 'content-range', 'accept-ranges']) {
      expect(exposed).toContain(name);
    }
  });
});

/** A stub R2 bucket holding one 4096-byte object. */
async function headData(key: string): Promise<Response> {
  const { default: worker } = await import('../src/worker');
  const env = {
    DATA: {
      get: (_key: string, options?: { range?: unknown }) => {
        const size = 4096;
        const meta = {
          size,
          httpEtag: '"abc"',
          writeHttpMetadata: (headers: Headers) =>
            headers.set('content-type', 'application/vnd.apache.parquet'),
          ...(options?.range ? { range: options.range } : {}),
          body: new Blob(['x'.repeat(size)]).stream(),
        };
        return Promise.resolve(meta);
      },
    },
    ASSETS: { fetch: () => new Response('asset') },
  } as unknown as Parameters<typeof worker.fetch>[1];

  return worker.fetch(
    new Request(`https://x.example/data/${key}`, { method: 'HEAD' }),
    env,
    {} as ExecutionContext,
  );
}
