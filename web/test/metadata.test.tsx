/**
 * The metadata panel.
 *
 * Exists to answer, without opening a database: which build produced
 * this, which export the browser is actually looking at, how fresh the
 * data is, and how big each file was. All four are questions that come
 * up the moment a number looks wrong.
 *
 * The checksum matters more than it looks: Parquet is served immutable,
 * so a browser can hold an old file indefinitely. A visible checksum
 * prefix is how you tell "the data is wrong" from "this tab has a stale
 * copy of the data".
 */
// @vitest-environment jsdom
import { cleanup, render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { Metadata } from '../src/components/Metadata';
import type { Manifest } from '../src/duckdb';

beforeEach(() => cleanup());

const MANIFEST: Manifest = {
  schemaVersion: '5',
  generator: 'chatsbom/0.5.4',
  rowCounts: { repositories: 28075, artifacts: 6062896, licenses: 500, history: 141938 },
  freshness: { observedFrom: '2026-02-11', observedTo: '2026-09-13' },
  // Content-addressed, as a real manifest is. The fixture used plain
  // names and so the row-count lookup passed here while showing a dash
  // for every file in the browser: a test green against a product
  // broken, because the fixture was not what the export writes.
  files: [
    { name: 'artifacts-abc123de.parquet', bytes: 16700000, sha256: 'abc123def4567890' },
    { name: 'repositories-fedcba09.parquet', bytes: 2800000, sha256: 'fedcba0987654321' },
  ],
};

describe('Metadata', () => {
  it('names the build that produced the dataset', () => {
    render(<Metadata manifest={MANIFEST} />);
    expect(screen.getByText(/chatsbom\/0\.5\.4/)).toBeTruthy();
  });

  it('shows the schema version, which is the contract the page reads', () => {
    const { container } = render(<Metadata manifest={MANIFEST} />);
    expect(container.textContent).toContain('v5');
  });

  it('reports how fresh the data is, as a span', () => {
    const { container } = render(<Metadata manifest={MANIFEST} />);
    // Both ends: one date would hide that some rows are much older.
    expect(container.textContent).toContain('2026-09-13');
    expect(container.textContent).toContain('2026-02-11');
  });

  it('says the observation span is unknown rather than inventing one', () => {
    const { container } = render(
      <Metadata manifest={{ ...MANIFEST, freshness: {} }} />,
    );
    expect(container.textContent).toContain('unknown');
    expect(container.textContent).not.toContain('1970');
  });

  it('lists every file with its size and a checksum prefix', () => {
    const { container } = render(<Metadata manifest={MANIFEST} />);
    expect(container.textContent).toContain('artifacts-abc123de.parquet');
    expect(container.textContent).toContain('16.7 MB');
    // A prefix, not the whole digest: enough to tell two exports apart.
    expect(container.textContent).toContain('abc123de');
    expect(container.textContent).not.toContain('abc123def4567890');
  });

  it('shows row counts per table, so a truncated export is visible', () => {
    const { container } = render(<Metadata manifest={MANIFEST} />);
    expect(container.textContent).toContain('6,062,896');
    expect(container.textContent).toContain('28,075');
  });
});
