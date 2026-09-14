/**
 * The metadata panel, against a database rather than files.
 *
 * The Parquet path answered "what am I looking at" with a manifest:
 * generator, schema version, freshness, and a checksum per file. That
 * checksum row existed because Parquet is served immutable, so a
 * browser can hold an old copy indefinitely and the digest is how you
 * tell stale data from wrong data.
 *
 * With D1 there are no files and no client-held copy, so that particular
 * question cannot arise and the row is gone. The other three carry over,
 * and the row counts move from the manifest to a query.
 */
// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { Metadata } from '../src/components/Metadata';
import type { DatasetMeta, Totals } from '../src/d1/queries';

beforeEach(() => cleanup());

const META: DatasetMeta = {
  generator: 'chatsbom/0.5.4',
  schemaVersion: 'd1 v5',
  observedFrom: '2026-02-11',
  observedTo: '2026-09-13',
};

const TOTALS: Totals = {
  repositories: 28075,
  dependencies: 6062896,
  packages: 141938,
  classified: 6053469,
};

describe('Metadata', () => {
  it('names the build that produced the dataset', () => {
    render(<Metadata meta={META} totals={TOTALS} />);
    expect(screen.getByText(/chatsbom\/0\.5\.4/)).toBeTruthy();
  });

  it('never rounds the classified share up to a whole 100%', () => {
    /**
     * The real figure is 99.951% — 9,469 of 19,361,638 records carry no
     * known relationship — and both `Math.round` and `toFixed(1)`
     * render that as 100. A panel headed "for debugging what you are
     * looking at" claiming perfect coverage is the one thing it must
     * not do.
     */
    const { container } = render(
      <Metadata
        meta={META}
        totals={{ ...TOTALS, dependencies: 19361638, classified: 19352169 }}
      />,
    );
    expect(container.textContent).toContain('99.95');
    expect(container.textContent).not.toMatch(/\b100(\.0)?%/);
  });

  it('still prints a genuine 100%', () => {
    const { container } = render(
      <Metadata
        meta={META}
        totals={{ ...TOTALS, dependencies: 1000, classified: 1000 }}
      />,
    );
    expect(container.textContent).toContain('100%');
  });

  it('adds decimals until the shortfall shows', () => {
    // One decimal hides anything above 99.95; three catch a single
    // unclassified row in two million.
    const { container } = render(
      <Metadata
        meta={META}
        totals={{ ...TOTALS, dependencies: 2000000, classified: 1999999 }}
      />,
    );
    expect(container.textContent).toMatch(/99\.999/);
  });

  it('shows the schema version exactly as the backend named it', () => {
    // No `v` added here. D1 answers `d1 v5`; ClickHouse answers
    // `clickhouse (live)`, which the old prefix turned into
    // "vclickhouse".
    const { container } = render(<Metadata meta={META} totals={TOTALS} />);
    expect(container.textContent).toContain(META.schemaVersion);
    expect(container.textContent).not.toContain(`v${META.schemaVersion}`);
  });

  it('reports freshness as a span, not a single date', () => {
    // One date invites the reader to assume the whole corpus is that
    // age; on this corpus the ends are seven months apart.
    const { container } = render(<Metadata meta={META} totals={TOTALS} />);
    expect(container.textContent).toContain('2026-02-11');
    expect(container.textContent).toContain('2026-09-13');
  });

  it('says the span is unknown rather than inventing one', () => {
    const { container } = render(
      <Metadata
        meta={{ ...META, observedFrom: '', observedTo: '' }}
        totals={TOTALS}
      />,
    );
    expect(container.textContent).toContain('unknown');
    expect(container.textContent).not.toContain('1970');
  });

  it('shows the row counts, so a truncated import is visible', () => {
    const { container } = render(<Metadata meta={META} totals={TOTALS} />);
    expect(container.textContent).toContain('6,062,896');
    expect(container.textContent).toContain('28,075');
    expect(container.textContent).toContain('141,938');
  });

  it('reports what fraction of records carry a known relationship', () => {
    const { container } = render(<Metadata meta={META} totals={TOTALS} />);
    // 6,053,469 of 6,062,896 — the shortfall is a finding about the
    // data, so it is shown rather than rounded to 100%.
    expect(container.textContent).toMatch(/99\.8/);
  });

  it('explains that star counts and push dates have a different vintage', () => {
    const { container } = render(<Metadata meta={META} totals={TOTALS} />);
    expect(container.textContent).toMatch(/not refreshed|different/i);
  });
});
