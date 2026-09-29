/**
 * Dashboard root.
 *
 * Renders two views against the query endpoint:
 * an overview that answers standing questions, and a query view for one
 * package. They are peers — a bar in the overview hands its package to
 * the query view, and the segmented control or the Back button returns.
 *
 * The page holds no data. Every question is a request to the Worker's
 * `/api/q`, which answers it from ClickHouse or D1 (`d1/client.ts`).
 */
// The fonts are part of the build, so the page loads nothing from
// another origin (#31): the stylesheet and the files were Google's. The
// weights are the ones that stylesheet asked for.
import '@fontsource/ibm-plex-sans-condensed/400.css';
import '@fontsource/ibm-plex-sans-condensed/500.css';
import '@fontsource/ibm-plex-sans-condensed/600.css';
import '@fontsource/ibm-plex-sans-condensed/700.css';
import '@fontsource/jetbrains-mono/400.css';
import '@fontsource/jetbrains-mono/500.css';
import '@fontsource/jetbrains-mono/700.css';
import './style.css';

import { useCallback, useState } from 'react';

import { useAsync, useBoot, useRoute } from './hooks';
import type { DatasetMeta, Totals } from './dataset/types';
import { queryFailure } from './i18n/failure';
import { formatNumber } from './i18n/format';
import type { Locale } from './i18n/locale';
import { LOCALE_NAMES, LOCALES, useDocumentLocale, useLocale } from './i18n/locale';
import { DICTIONARIES } from './i18n/strings';
import type { Dictionary } from './i18n/strings';
import { THEME_CHOICES, useTheme } from './theme';
import { DatasetClient } from './d1/client';
import type { Go, Route } from './router';
import { Overview } from './components/Overview';
import { MetadataPanel } from './components/Metadata';
import { QueryView } from './components/QueryView';

export function App() {
  // One client for the page, so a question two panels ask at once is
  // one request (`DatasetClient`).
  const [dataset] = useState(() => new DatasetClient());
  const boot = useBoot(dataset);
  const [route, go] = useRoute();
  const { locale, setLocale } = useLocale();
  // Held at the root so a change re-renders the tree that draws the
  // charts: `chartTheme()` reads the palette at draw time, and nothing
  // was listening for the OS switching under it.
  const { choice, resolved, setChoice } = useTheme();
  const words = DICTIONARIES[locale];
  // `resolved` is read so an OS theme change re-renders rather than
  // leaving every chart in the previous theme's colours. The charts
  // take their palette from the document, not from this value.
  void resolved;
  // The document's language, title and description, with the page's.
  useDocumentLocale(locale, words.documentTitle, words.documentDescription);

  return (
    <div className="shell">
      <header className="mast">
        <h1>
          Chat<b>SBOM</b>
        </h1>
        <p className="tagline">{words.tagline}</p>
        <span className="spacer" />
        {boot.status !== 'failed' ? (
          <Counters dataset={dataset} words={words} locale={locale} />
        ) : null}
        <nav className="views" role="group" aria-label={words.viewGroup}>
          <button
            type="button"
            aria-pressed={route.view === 'overview'}
            onClick={() => go({ view: 'overview' })}
          >
            {words.viewOverview}
          </button>
          <button
            type="button"
            aria-pressed={route.view === 'query'}
            onClick={() => go({ view: 'query' })}
          >
            {words.viewQuery}
          </button>
        </nav>
        {/*
          Language and theme sit with the view switch rather than in a
          menu: both are one click from any state, and neither is worth
          hiding behind a disclosure a reader has to find first.
        */}
        <nav className="switches" role="group" aria-label={words.localeGroup}>
          {LOCALES.map((option) => (
            <button
              key={option}
              type="button"
              aria-pressed={locale === option}
              onClick={() => setLocale(option)}
            >
              {LOCALE_NAMES[option]}
            </button>
          ))}
        </nav>
        <nav className="switches" role="group" aria-label={words.themeGroup}>
          {THEME_CHOICES.map((option) => (
            <button
              key={option}
              type="button"
              aria-pressed={choice === option}
              onClick={() => setChoice(option)}
            >
              {option === 'light'
                ? words.themeLight
                : option === 'dark'
                  ? words.themeDark
                  : words.themeSystem}
            </button>
          ))}
        </nav>
      </header>

      {boot.status === 'loading' ? (
        <p className="note" style={{ padding: '1rem 0' }}>
          {words.loading}
        </p>
      ) : null}

      {boot.status === 'failed' ? (
        <p className="answer error" style={{ padding: '1rem 0' }}>
          {queryFailure(boot.error, words)}
        </p>
      ) : (
        // Drawn while the provenance is still on its way, so the views
        // ask their questions beside it rather than after it (#42).
        <Views
          dataset={dataset}
          meta={boot.status === 'ready' ? boot.meta : null}
          route={route}
          go={go}
          words={words}
          locale={locale}
        />
      )}
    </div>
  );
}

/**
 * Both views plus the footer, unless the dataset has said it cannot
 * answer.
 *
 * Split out so the hooks below it are called only while there may be a
 * dataset to ask: a component cannot call hooks conditionally, and a
 * failed boot is the one genuinely conditional thing on the page.
 */
function Views({
  dataset,
  meta,
  route,
  go,
  words,
  locale,
}: {
  dataset: DatasetClient;
  /** Null until the provenance has answered. */
  meta: DatasetMeta | null;
  words: Dictionary;
  locale: Locale;
  route: Route;
  go: Go;
}) {
  // Filter options come from the data, never a hard-coded list: the
  // folded GitHub languages (top twelve, `other`, `none`) for the
  // dependants' repository filter, and the ecosystems for the ranking.
  // The overview asks for both as well; the client sends one request
  // for each.
  const coverage = useAsync(
    useCallback((signal: AbortSignal) => dataset.languageCoverage(signal), [dataset]),
    [dataset],
  );
  const languages =
    coverage.status === 'ready'
      ? coverage.value.map((row) => row.language).filter(Boolean)
      : [];
  const ecosystemCoverage = useAsync(
    useCallback((signal: AbortSignal) => dataset.ecosystemCoverage(signal), [dataset]),
    [dataset],
  );
  const ecosystems =
    ecosystemCoverage.status === 'ready'
      ? ecosystemCoverage.value.map((row) => row.ecosystem).filter(Boolean)
      : [];

  return (
    <>
      {/* Both views stay mounted so switching back does not re-run every
          query; `hidden` keeps the inactive one out of the a11y tree. */}
      <section hidden={route.view !== 'overview'}>
        <Overview
            dataset={dataset}
            ecosystems={ecosystems}
            go={go}
            words={words}
            locale={locale}
          />
      </section>

      <section hidden={route.view !== 'query'}>
        <QueryView
          dataset={dataset}
          languages={languages}
          route={route}
          go={go}
          words={words}
          locale={locale}
          visible={route.view === 'query'}
        />
      </section>

      {/* Metadata sits below both views: it describes the dataset, not
          whichever view is open. */}
      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <MetadataPanel
            dataset={dataset}
            meta={meta}
            words={words}
            locale={locale}
          />
        </div>
      </div>

      {meta ? <footer className="end">{describe(meta, words)}</footer> : null}
    </>
  );
}

/**
 * The four headline numbers and what each one is called.
 *
 * Pure and exported so the labels are testable. They were not, and a
 * label was wrong: the first tile read `repositories`, which names the
 * corpus, while the value counts only repositories that have
 * dependency data — 24,339 of 28,075. The coverage panel on the same
 * page uses the other number: its per-language denominators sum to
 * 28,075 and it calls the part `with dependency data`. So the page
 * showed two different repository counts under labels that read alike,
 * and a reader who totalled the coverage bars found a third of a
 * language missing with nothing to explain it.
 *
 * The value is right for its neighbours — records, packages and
 * %-classified are all properties of the analysed set — so the label
 * moves to match, in the panel's own words rather than new ones.
 */
export function counterTiles(
  t: Totals,
  words: Dictionary,
): [number, string][] {
  return [
    [t.repositories, words.tileRepositories],
    [t.dependencies, words.tileRecords],
    [t.packages, words.tilePackages],
    [
      // Not `Math.round`: 99.951% rounds to 100 and the tile then
      // claims every record is classified while 9,469 are not. Floored
      // to the whole percent below, so the tile can say 99 and the
      // metadata panel's decimal explains it.
      t.dependencies
        ? Math.floor((t.classified / t.dependencies) * 100)
        : 0,
      words.tileClassified,
    ],
  ];
}

function Counters({
  dataset,
  words,
  locale,
}: {
  dataset: DatasetClient;
  words: Dictionary;
  locale: Locale;
}) {
  // The metadata panel asks the same, at the same time: one request.
  const totals = useAsync(
    useCallback((signal: AbortSignal) => dataset.totals(signal), [dataset]),
    [dataset],
  );
  if (totals.status !== 'ready') return null;
  const t = totals.value;

  const tiles = counterTiles(t, words);

  return (
    <div className="stats" aria-live="polite">
      {tiles.map(([value, label]) => (
        <div className="stat" key={label}>
          <span className="n">{formatNumber(value, locale)}</span>
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
export function describe(meta: DatasetMeta, words: Dictionary): string {
  const span =
    meta.observedFrom && meta.observedTo
      ? words.observedSpan(meta.observedFrom, meta.observedTo)
      : words.observedUnknown;
  // No `v` prefix. The backend names its own value — `d1 v5` or
  // `clickhouse (live)` — and this line prefixed it a second time,
  // which the panel above had already been fixed for and this had not:
  // the footer read "schema vclickhouse (live)".
  //
  // The schema and generator are not translated: they are the values
  // the backend reports for itself, and a localised copy of an
  // identifier is a different identifier.
  return `${span} · ${words.schemaLabel} ${meta.schemaVersion} · ${meta.generator}`;
}
