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

/**
 * A percentage that never rounds up to a whole it has not reached.
 *
 * `(19352169 / 19361638) * 100` is 99.951, and both `Math.round` and
 * `toFixed(1)` render that as 100 — so a panel whose whole purpose is
 * to let a reader check the data claimed complete coverage while 9,469
 * records carried no known relationship.
 *
 * Rounds toward the nearest value *below* 100 when that is where the
 * number actually is, and adds decimals until the difference shows.
 * Anything genuinely 100 still prints as 100.
 */
export function shortOfWhole(percent: number): string {
  if (percent >= 100) return '100';
  for (const places of [1, 2, 3]) {
    const rendered = percent.toFixed(places);
    if (Number(rendered) < 100) return rendered;
  }
  // Closer to 100 than three decimals can express, and still not there.
  return '>99.999';
}

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
          {/* No `v` prefix added here. The D1 export's contract version
              is a number, so it wanted one; ClickHouse answers
              `clickhouse`, which rendered as "vclickhouse". Whoever
              supplies the value decides how it reads. */}
          <dd className="mono">{meta.schemaVersion}</dd>
        </div>
        <div>
          <dt>Observed</dt>
          <dd className="mono">{span}</dd>
        </div>
        <div>
          {/*
            Not `Repositories`: the corpus has 28,075 and this counts
            the 24,339 with dependency data. The coverage panel is
            built on the other number — its per-language denominators
            sum to 28,075 — so the bare label put two different
            repository counts on one page under labels that read
            alike. Same wording as the header tile and the coverage
            bars, which is the point.
          */}
          <dt>Repositories with dependency data</dt>
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
          {/* Never rounded up to 100%. One decimal was supposed to
              prevent that and does not: the real figure is 99.951%, and
              `toFixed(1)` renders it as "100.0". 9,469 records carry no
              known relationship, and a panel headed "for debugging what
              you are looking at" claiming perfect coverage is the one
              thing it must not do. */}
          <dd className="mono">{shortOfWhole(classified)}%</dd>
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
