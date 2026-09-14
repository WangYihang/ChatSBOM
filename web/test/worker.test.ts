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
