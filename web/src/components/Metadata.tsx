/**
 * What this page is actually showing.
 *
 * The questions that come up the moment a number looks wrong: which
 * build produced the data, which contract the queries speak, how fresh
 * the rows are, and how many there are.
 *
 * The Parquet path also showed a checksum per file, because Parquet is
 * served immutable and a browser can hold an old copy for a year — the
 * digest was how you told stale data from wrong data. Queries run
 * against the database now, so there is no client-held copy and that
 * question cannot arise. The row is gone rather than left showing
 * something meaningless.
 */
import { useCallback } from 'react';

import type { DatasetClient } from '../d1/client';
import type { DatasetMeta, Totals } from '../d1/queries';
import { useAsync } from '../hooks';

export function Metadata({
  meta,
  totals,
}: {
  meta: DatasetMeta;
  totals: Totals;
}) {
  const span =
    meta.observedFrom && meta.observedTo
      ? `${meta.observedFrom} → ${meta.observedTo}`
      : 'unknown';

  const classified = totals.dependencies
    ? (totals.classified / totals.dependencies) * 100
    : 0;

  return (
    <div className="panel">
      <h2>
        Dataset metadata
        <span className="qual">for debugging what you are looking at</span>
      </h2>
      <p className="note">
        Observation dates are when <em>this</em> pipeline recorded a
        repository&rsquo;s dependencies. Star counts and push dates come
        from repository metadata collected earlier and are{' '}
        <strong>not refreshed</strong> by a dependency rescan, so a row
        can legitimately show a recent scan beside an older push.
      </p>

      <dl className="meta">
        <div>
          <dt>Generator</dt>
          <dd className="mono">{meta.generator}</dd>
        </div>
        <div>
          <dt>Schema</dt>
          <dd className="mono">v{meta.schemaVersion}</dd>
        </div>
        <div>
          <dt>Observed</dt>
          <dd className="mono">{span}</dd>
        </div>
        <div>
          <dt>Repositories</dt>
          <dd className="mono">{totals.repositories.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Dependency records</dt>
          <dd className="mono">{totals.dependencies.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Distinct packages</dt>
          <dd className="mono">{totals.packages.toLocaleString()}</dd>
        </div>
        <div>
          <dt>Classified</dt>
          {/* One decimal, not a rounded 100%: 9,427 records carry no
              known relationship, and that shortfall is a finding about
              the data rather than noise to hide. */}
          <dd className="mono">{classified.toFixed(1)}%</dd>
        </div>
      </dl>
    </div>
  );
}


/**
 * Fetches the row counts the panel needs.
 *
 * They came off the manifest before, which the page already had in
 * hand. With a database there is nothing to read them from but a query,
 * and `totals` is one precomputed row — the panel is not worth a scan.
 */
export function MetadataPanel({
  dataset,
  meta,
}: {
  dataset: DatasetClient;
  meta: DatasetMeta;
}) {
  const totals = useAsync(
    useCallback(() => dataset.totals(), [dataset]),
    [dataset],
  );

  if (totals.status !== 'ready') {
    return (
      <div className="panel">
        <h2>Dataset metadata</h2>
        <p className="note">
          {totals.status === 'failed'
            ? totals.message
            : 'Reading provenance\u2026'}
        </p>
      </div>
    );
  }

  return <Metadata meta={meta} totals={totals.value} />;
}
