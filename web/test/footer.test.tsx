/**
 * The footer line.
 *
 * It was untested, and that is exactly how it kept a bug the panel
 * above it had already been fixed for: both render the schema version,
 * `Metadata` stopped prefixing it with `v` and this did not, so the
 * footer read "schema vclickhouse (live)" while the panel read
 * "clickhouse (live)".
 *
 * Two places formatting the same field is the shape of that mistake, so
 * the assertions here are the ones `metadata.test.tsx` makes, applied
 * to the other place.
 */
import { describe as group, expect, it } from 'vitest';

import { describe as line } from '../src/app';

const META = {
  generator: 'chatsbom/0.5.4 clickhouse',
  schemaVersion: 'clickhouse (live)',
  observedFrom: '2026-02-11',
  observedTo: '2026-09-13',
};

group('the footer line', () => {
  it('prints the schema version exactly as the backend named it', () => {
    const rendered = line(META);
    expect(rendered).toContain('schema clickhouse (live)');
    expect(rendered).not.toContain('vclickhouse');
  });

  it('adds no prefix to D1\'s contract number either', () => {
    // D1 answers `d1 v5`, which already reads as a version.
    expect(line({ ...META, schemaVersion: 'd1 v5' })).toContain(
      'schema d1 v5',
    );
  });

  it('reports the observation span, which explains a stale row', () => {
    expect(line(META)).toContain('observed 2026-02-11 to 2026-09-13');
  });

  it('says the span is unknown rather than inventing one', () => {
    // A blank date is an observation that never happened; printing it
    // as a range would read as a real one.
    expect(line({ ...META, observedFrom: '', observedTo: '' })).toContain(
      'observation span unknown',
    );
  });

  it('names the build that produced the data', () => {
    expect(line(META)).toContain('chatsbom/0.5.4 clickhouse');
  });
});
