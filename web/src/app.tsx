/**
 * Dashboard root.
 *
 * Renders two views against the query endpoint:
 * an overview that answers standing questions, and a query view for one
 * package. They are peers — a bar in the overview hands its package to
 * the query view, and the segmented control or the Back button returns.
 *
 * All querying happens in the browser. The Worker only serves bytes.
 */
import './style.css';

import { useCallback } from 'react';

import { useAsync, useBoot, useRoute } from './hooks';
import type { DatasetMeta } from './d1/queries';
import type { DatasetClient } from './d1/client';
import { Overview } from './components/Overview';
import { MetadataPanel } from './components/Metadata';
import { QueryView } from './components/QueryView';

export function App() {
  const boot = useBoot();
  const [route, go] = useRoute();

  return (
    <div className="shell">
      <header className="mast">
        <h1>
          Chat<b>SBOM</b>
        </h1>
        <p className="tagline">
          Who <em>actually</em> declares a dependency &mdash; not who merely
          inherits one.
        </p>
        <span className="spacer" />
        {boot.status === 'ready' ? <Counters dataset={boot.dataset} /> : null}
        <nav className="views" role="group" aria-label="View">
          <button
            type="button"
            aria-pressed={route.view === 'overview'}
            onClick={() => go({ view: 'overview' })}
          >
            Overview
          </button>
          <button
            type="button"
            aria-pressed={route.view === 'query'}
            onClick={() => go({ view: 'query' })}
          >
            Query
          </button>
        </nav>
      </header>

      {boot.status === 'loading' ? (
        <p className="note" style={{ padding: '1rem 0' }}>
          Loading the query engine and dataset&hellip; the engine is ~33 MB on
          a first visit and cached immutably afterwards.
        </p>
      ) : null}

      {boot.status === 'failed' ? (
        <p className="answer error" style={{ padding: '1rem 0' }}>
          {boot.message}
        </p>
      ) : null}

      {boot.status === 'ready' ? (
        <Views dataset={boot.dataset} meta={boot.meta} route={route} go={go} />
      ) : null}
    </div>
  );
}

/**
 * Both views plus the footer, once there is a dataset to query.
 *
 * Split out so the hooks below it are only ever called with a dataset in
 * hand: a component cannot call hooks conditionally, and boot is the one
 * genuinely conditional thing on the page.
 */
function Views({
  dataset,
  meta,
  route,
  go,
}: {
  dataset: DatasetClient;
  meta: DatasetMeta;
  route: { view: 'overview' | 'query'; package?: string };
  go: (route: { view: 'overview' | 'query'; package?: string }) => void;
}) {
  // Language options come from the data, never a hard-coded list.
  const coverage = useAsync(
    useCallback(() => dataset.languageCoverage(), [dataset]),
    [dataset],
  );
  const languages =
    coverage.status === 'ready'
      ? coverage.value.map((row) => row.language).filter(Boolean)
      : [];

  return (
    <>
      {/* Both views stay mounted so switching back does not re-run every
          query; `hidden` keeps the inactive one out of the a11y tree. */}
      <section hidden={route.view !== 'overview'}>
        <Overview dataset={dataset} languages={languages} go={go} />
      </section>

      <section hidden={route.view !== 'query'}>
        <QueryView
          dataset={dataset}
          languages={languages}
          route={route}
          go={go}
        />
      </section>

      {/* Metadata sits below both views: it describes the dataset, not
          whichever view is open. */}
      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <MetadataPanel dataset={dataset} meta={meta} />
        </div>
      </div>

      <footer className="end">{describe(meta)}</footer>
    </>
  );
}

function Counters({ dataset }: { dataset: DatasetClient }) {
  const totals = useAsync(
    useCallback(() => dataset.totals(), [dataset]),
    [dataset],
  );
  if (totals.status !== 'ready') return null;
  const t = totals.value;

  const tiles: [number, string][] = [
    [t.repositories, 'repositories'],
    [t.dependencies, 'dependency records'],
    [t.packages, 'distinct packages'],
    [
      t.dependencies ? Math.round((t.classified / t.dependencies) * 100) : 0,
      '% classified',
    ],
  ];

  return (
    <div className="stats" aria-live="polite">
      {tiles.map(([value, label]) => (
        <div className="stat" key={label}>
          <span className="n">{value.toLocaleString()}</span>
          <span className="l">{label}</span>
        </div>
      ))}
    </div>
  );
}

/**
 * The footer line: what this page is showing, in one sentence.
 *
 * The row counts come from a query now rather than from a manifest,
 * because with a database behind the page there are no files to
 * describe. The observation span is the part that matters for reading
 * the numbers, so it stays.
 */
function describe(meta: DatasetMeta): string {
  const span =
    meta.observedFrom && meta.observedTo
      ? `observed ${meta.observedFrom} to ${meta.observedTo}`
      : 'observation span unknown';
  return `${span} · schema v${meta.schemaVersion} · ${meta.generator}`;
}
