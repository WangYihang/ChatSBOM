import { describe, expect, it } from 'vitest';

import { parseRange } from '../src/worker';

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
