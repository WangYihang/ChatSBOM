/**
 * One package, answered.
 *
 * Every string on this view is derived from the query's state on each
 * render. That is the point of the rewrite: the status line used to be
 * an imperative write, which is how it came to sit on "Loading dataset…"
 * indefinitely, and the input used to be a second source of truth
 * alongside the route, which is how typing a name came to update the URL
 * and search for nothing.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import { groupBySource, TimeSeries } from '../charts/Plots';
import { ChartNote, Measured } from '../charts/Frame';
import { DependencyTree } from '../charts/DependencyTree';
import { RankedBars } from '../charts/RankedBars';
import { type Async, useAsync, useDebounced } from '../hooks';
import type { DatasetClient } from '../d1/client';
import type {
  Dependent,
  EdgeAmbiguity,
  VersionSpread,
} from '../dataset/types';
import { formatRoute, type Go, type Route } from '../router';
import { queryFailure } from '../i18n/failure';
import { formatNumber } from '../i18n/format';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { AskPlaceholder } from '../ask/Placeholder';
import { useAsk } from '../ask/useAsk';
import { PackageSearch } from './PackageSearch';
import { Panel } from './Panel';

/** How many rows the table shows. The count is asked separately. */
const SHOWN_LIMIT = 100;
const DEBOUNCE_MS = 250;

/** Rows in the reverse-lookup ranking. */
const PULLERS_LIMIT = 15;

/**
 * How much of the tree to draw.
 *
 * Smaller than the store's own cap. 12 x 3 is 36 leaf rows at a 15px
 * pitch, which is a panel; the store's 14 x 4 is 56 rows and starts to
 * be a scroll.
 */
const TREE_SHAPE = { children: 12, branch: 3 } as const;

/** The shape `versionSpread` returns with nothing to report. */
const EMPTY_SPREAD: VersionSpread = {
  versions: [],
  constrained: 0,
  unversioned: 0,
};

/** Candidates offered under the search box. */
const SUGGEST_LIMIT = 8;

/**
 * Shortest term worth searching.
 *
 * `a` matches roughly ten thousand of the 141,938 names, and ranking by
 * popularity means every match is read before `LIMIT` applies. Two
 * characters keeps the range scan small.
 */
const MIN_SEARCH = 2;

/**
 * What both edge panels cannot tell you, said once.
 *
 * `edges` is keyed on package *name* and has no ecosystem column, and a
 * name is not unique across ecosystems — the reason this page has an
 * ecosystem filter at all. So `bytes` in a drawn tree is the npm
 * package and the Rust crate merged, which is why `serde` turns up
 * under it.
 *
 * Stated rather than hidden. A reader who knows can discount the odd
 * row; a reader who does not would take `bytes -> serde` as a fact
 * about JavaScript.
 *
 * **Measured, not pasted, and measured twice.** These four numbers
 * used to be literals in this sentence, taken before the
 * dependency-graph ingest and never revisited: 2,508 ambiguous names
 * of 141,938 carrying 107,974 of 455,281 edges, or 23.7%.
 *
 * Computing them live first gave 39,186 names and 51.5% of edges,
 * which looked like the caveat had been understating itself. It was
 * not: that count treated `cargo` and `rust-crate` as two ecosystems,
 * and `composer` and `php-composer`, because the two collectors spell
 * one registry two ways. Under canonical names it is 2,730 names
 * (1.2%) and 63,384 edges (10.3%) — 93% of the "ambiguity" was the
 * mapping missing, and the page had gone from understating the
 * problem to overstating it fivefold.
 */
export function edgeCaveat(
  scale: EdgeAmbiguity | null,
  words: Dictionary,
  locale: Locale,
): string {
  // No figures rather than invented ones: a store with no ecosystem
  // column answers null, and the warning stands without them.
  if (!scale || scale.edges === 0) return words.edgeCaveatPlain;
  return words.edgeCaveatMeasured(
    formatNumber(scale.ambiguousNames, locale),
    formatNumber(scale.names, locale),
    formatNumber(scale.ambiguousEdges, locale),
    formatNumber(scale.edges, locale),
    Math.round((scale.ambiguousEdges / scale.edges) * 100),
  );
}

export function QueryView({
  dataset,
  languages,
  route,
  go,
  words,
  locale,
}: {
  dataset: DatasetClient;
  languages: readonly string[];
  route: Route;
  go: Go;
  words: Dictionary;
  locale: Locale;
}) {
  const [typed, setTyped] = useState(route.package ?? '');
  const [directOnly, setDirectOnly] = useState(false);
  const [language, setLanguage] = useState('');
  const [ecosystem, setEcosystem] = useState('');

  // The natural-language slot's only dependency on this page — and the
  // element Turnstile draws in, for a deployment that requires it (#32).
  const challengeHost = useRef<HTMLDivElement>(null);
  const { ask, reset } = useAsk(dataset, challengeHost, locale);

  // The route is the source of truth. An arrival from elsewhere — a bar
  // in the overview, the Back button, a pasted link — sets the field;
  // typing is the reverse direction and is debounced into the route.
  useEffect(() => {
    if (route.package !== undefined && route.package !== typed) {
      setTyped(route.package);
    }
    // Only a route change may overwrite what someone is typing.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [route.package]);

  const settled = useDebounced(typed.trim(), DEBOUNCE_MS);

  useEffect(() => {
    if (settled && settled !== route.package) {
      // In place of the entry, not after it: a name being typed refines
      // where the reader is. Pushed, each pause was an entry of its own,
      // and Back stepped through the half-typed names (#42).
      go({ view: 'query', package: settled }, { replace: true });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [settled]);

  const name = settled;

  const ecosystems = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name ? dataset.ecosystemsFor(name, signal) : Promise.resolve([]),
      [dataset, name],
    ),
    [dataset, name],
  );

  // Not keyed on `name`: the collision scale is a property of the edge
  // table, so this is one read per mount rather than one per package.
  const ambiguity = useAsync(
    useCallback((signal: AbortSignal) => dataset.edgeAmbiguity(signal), [dataset]),
    [dataset],
  );
  const scale = ambiguity.status === 'ready' ? ambiguity.value : null;
  const caveat = edgeCaveat(scale, words, locale);
  // The other figure that used to be a literal in the note below.
  const largest = scale?.largestRepository ?? 0;

  // Offered only when the name is genuinely ambiguous: `mail` is a Ruby
  // gem with 118 dependants and a Maven artifactId with 6, so a single
  // count of 124 would describe something that does not exist.
  const ambiguous =
    ecosystems.status === 'ready' && ecosystems.value.length >= 2
      ? ecosystems.value
      : null;

  // A filter that no longer applies must not keep filtering — but
  // only once there is an answer to judge it against.
  //
  // This used to run while `ecosystemsFor` was still loading, when
  // `ambiguous` is null, and cleared the filter every time the package
  // changed. That made an ecosystem chosen in the search box
  // unsettable: it was wiped before the list it would have matched
  // arrived.
  useEffect(() => {
    if (ecosystems.status !== 'ready') return;
    if (ambiguous && ambiguous.some((row) => row.type === ecosystem)) return;
    if (ecosystem !== '') setEcosystem('');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ambiguous, ecosystems.status]);

  const filters = useMemo(
    () => ({
      name,
      directOnly,
      ...(ecosystem ? { type: ecosystem } : {}),
      ...(language ? { language } : {}),
    }),
    [name, directOnly, ecosystem, language],
  );

  // Rows skipped, kept with the question they are a page of. A new
  // question starts at the first page in the render that asks it — not
  // a page 3 surviving a filter change, which lands the reader on an
  // empty table, and not a reset in an effect after the render, which
  // asked for the old page under the new filters first (#42).
  const [paging, setPaging] = useState({ filters, offset: 0 });
  const offset = paging.filters === filters ? paging.offset : 0;
  const turnTo = (next: number) => setPaging({ filters, offset: next });

  // How many, apart from which: turning a page changes neither count,
  // and asking both again cost two queries a page (#42).
  const counts = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        !name
          ? Promise.resolve(null)
          : Promise.all([
              dataset.countDependents(filters, signal),
              // The row count, which is not the dependant count: 492
              // rows against 326 dependants for `laravel/framework`,
              // and 11,436 against 5,095 for `react`. Paging on the
              // dependant count would run off the end of one package
              // and stop halfway through another.
              dataset.countDependentRows(filters, signal),
            ]).then(([total, totalRows]) => ({ filters, total, totalRows })),
      [dataset, name, filters],
    ),
    [dataset, name, filters],
  );
  const page = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        !name
          ? Promise.resolve(null)
          : dataset
              .dependentsOf({ ...filters, limit: SHOWN_LIMIT, offset }, signal)
              .then((rows) => ({ filters, rows })),
      [dataset, name, filters, offset],
    ),
    [dataset, name, filters, offset],
  );

  // What the table shows: this page, or, while the next one loads, the
  // last one. The table emptied for the length of every request, and the
  // rails under it went with it and came back (#42).
  const shown =
    page.status === 'ready'
      ? page.value
      : page.status === 'loading'
        ? (page.previous ?? null)
        : null;
  const rows = shown?.rows ?? [];
  const hasRows = rows.length > 0;
  const counted =
    counts.status === 'ready'
      ? counts.value
      : counts.status === 'loading'
        ? (counts.previous ?? null)
        : null;
  const totalRows = counted?.totalRows ?? 0;

  // The sentence above the table speaks only of this question. Answers
  // kept from before a filter changed describe a question no longer
  // asked, so it says it is searching until both are this question's —
  // a turned page keeps its counts, and says so from the rows it has.
  const answer: Async<{ rows: Dependent[]; total: number } | null> = !name
    ? { status: 'ready', value: null }
    : page.status === 'failed'
      ? page
      : counts.status === 'failed'
        ? counts
        : counts.status === 'ready' &&
            counts.value?.filters === filters &&
            shown?.filters === filters
          ? { status: 'ready', value: { rows: shown.rows, total: counts.value.total } }
          : { status: 'loading' };

  // Keyed on the name alone: which versions are in use, and since when,
  // is a fact about the package, not about the table's page. Keyed on
  // whether the table had rows, both were asked again every time it
  // emptied while its next page loaded (#42).
  const versions = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name
          ? dataset.versionSpread(name, 10, signal)
          : Promise.resolve(EMPTY_SPREAD),
      [dataset, name],
    ),
    [dataset, name],
  );
  const adoption = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name ? dataset.adoptionOverTime(name, signal) : Promise.resolve([]),
      [dataset, name],
    ),
    [dataset, name],
  );
  const spread =
    versions.status === 'ready'
      ? versions.value
      : versions.status === 'loading'
        ? versions.previous
        : undefined;
  const adopted =
    adoption.status === 'ready'
      ? adoption.value
      : adoption.status === 'loading'
        ? adoption.previous
        : undefined;

  // Candidates for the search box.
  //
  // Gated at two characters: a one-letter term matches thousands of
  // names, and ranking them needs every match read before the limit
  // applies. Two is where the range stops being most of the table.
  const candidates = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name.length >= MIN_SEARCH
          ? dataset.searchPackages(name, SUGGEST_LIMIT, signal)
          : Promise.resolve([]),
      [dataset, name],
    ),
    [dataset, name],
  );

  const pullers = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name ? dataset.pulledInBy(name, PULLERS_LIMIT, signal) : Promise.resolve([]),
      [dataset, name],
    ),
    [dataset, name],
  );
  const tree = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        name
          ? dataset.dependencyTree(name, TREE_SHAPE, signal)
          : Promise.resolve(null),
      [dataset, name],
    ),
    [dataset, name],
  );

  // Hoisted above the conditional markup below, because hooks cannot be
  // called conditionally: with these inline in the `hasRows` branch,
  // React counted a different number of hooks per render and threw
  // "Rendered more hooks than during the previous render". The component
  // tests caught it before a browser did.
  return (
    <>
      <PackageSearch
        value={typed}
        words={words}
        locale={locale}
        onChange={setTyped}
        onChoose={(pkg, picked) => {
          setTyped(pkg);
          // The ecosystem is applied with the name. `mail` is a Ruby
          // gem with 167 dependants, a Maven artifact with 6 and a
          // PyPI package with 1; picking the row means picking one of
          // them, not the name and then a filter.
          setEcosystem(picked ?? '');
          go({ view: 'query', package: pkg });
        }}
        candidates={candidates.status === 'ready' ? candidates.value : []}
        // The dead end this exists for: an exact-name query that found
        // nothing, while candidates with the same prefix do exist.
        deadEnd={
          !!name &&
          page.status === 'ready' &&
          !!page.value &&
          page.value.rows.length === 0
        }
      >
        <label className="field">
          <input
            type="checkbox"
            checked={directOnly}
            onChange={(e) => setDirectOnly(e.target.checked)}
          />
          {words.declaredOnly}
        </label>
        <label className="field">
          {words.languageFilter}
          <select
            value={language}
            onChange={(e) => setLanguage(e.target.value)}
          >
            <option value="">{words.languageAll}</option>
            {languages.map((l) => (
              <option key={l} value={l}>
                {l}
              </option>
            ))}
          </select>
        </label>
        {ambiguous ? (
          <label className="field">
            {words.ecosystemFilter}
            <select
              value={ecosystem}
              onChange={(e) => setEcosystem(e.target.value)}
            >
              <option value="">{words.ecosystemAll(ambiguous.length)}</option>
              {ambiguous.map((row) => (
                <option key={row.type} value={row.type}>
                  {row.type} · {formatNumber(row.repositoryCount, locale)}
                </option>
              ))}
            </select>
          </label>
        ) : null}
      </PackageSearch>

      <div id="status" aria-live="polite">
        {statusLine(name, answer, directOnly, ecosystem, words, locale)}
      </div>

      {hasRows ? (
        <div className="rails">
          <div className="rail">
            {/* Busy while the rows on it are the last answer's, kept so
                the table does not vanish while the next one loads. */}
            <div className="panel tablewrap" aria-busy={page.status === 'loading'}>
              <table>
                <thead>
                  <tr>
                    <th scope="col">{words.tableRepository}</th>
                    <th scope="col" className="num">
                      {words.tableStars}
                    </th>
                    <th scope="col">{words.tableVersion}</th>
                    <th scope="col">{words.tableDepends}</th>
                    <th scope="col">{words.tableEcosystem}</th>
                    <th scope="col">{words.tableLanguage}</th>
                    {/* When we last looked, not when upstream last
                        pushed — the first is what explains a stale row. */}
                    <th scope="col">{words.tableScanned}</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((dep) => (
                    // Keyed on everything that distinguishes a row.
                    // `owner/repo/version` alone repeated for the four
                    // rows one repository produced by declaring the
                    // package in four manifests — identical keys, which
                    // React pairs with whichever element it likes.
                    <tr
                      key={
                        `${dep.owner}/${dep.repo}/${dep.version}` +
                        `/${dep.relationship}/${dep.ecosystem}` +
                        `/${dep.observedAt}`
                      }
                    >
                      <td className="repo">
                        <a href={dep.url} rel="noreferrer noopener">
                          {dep.owner}/{dep.repo}
                        </a>
                      </td>
                      <td className="num">{formatNumber(dep.stars, locale)}</td>
                      <td className="version" title={dep.version || undefined}>
                        {dep.version || '—'}
                      </td>
                      <td>
                        {/* Border style carries the state as well as
                            colour, so the distinction survives
                            colour-vision deficiency and greyscale. */}
                        <span className={`pill ${dep.relationship}`}>
                          {dep.relationship === 'direct'
                            ? words.relationshipDirect
                            : dep.relationship === 'transitive'
                              ? words.relationshipTransitive
                              : words.relationshipUnknown}
                        </span>
                      </td>
                      <td className="nowrap">
                        {dep.ecosystem ? (
                          <span className="tag">{dep.ecosystem}</span>
                        ) : (
                          '—'
                        )}
                      </td>
                      <td className="nowrap">{dep.language || '—'}</td>
                      <td className="mono nowrap">
                        {dep.observedAt || '—'}
                        {/* Said rather than repeated: this row stands
                            for every manifest in the repository that
                            declares the package, and one declares it
                            in 80. */}
                        {dep.manifests > 1 ? (
                          <span className="tag">
                            {words.manifestCount(dep.manifests)}
                          </span>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/*
              Paging over the *rows*, and the range says so. The
              heading above counts dependants — 326 for
              `laravel/framework` — while the table shows one row per
              distinct (repository, version, relationship, ecosystem,
              date), which is 492. Labelling this "of 326" would make
              the last page look broken.
            */}
            {totalRows > SHOWN_LIMIT ? (
              <div className="pager">
                <button
                  type="button"
                  disabled={offset === 0}
                  onClick={() => turnTo(Math.max(0, offset - SHOWN_LIMIT))}
                >
                  {words.pagePrevious}
                </button>
                <span className="note">
                  {words.pageRange(
                    formatNumber(offset + 1, locale),
                    formatNumber(Math.min(offset + rows.length, totalRows), locale),
                    formatNumber(totalRows, locale),
                  )}
                </span>
                <button
                  type="button"
                  disabled={offset + SHOWN_LIMIT >= totalRows}
                  onClick={() => turnTo(offset + SHOWN_LIMIT)}
                >
                  {words.pageNext}
                </button>
              </div>
            ) : null}
          </div>

          <div className="rail">
            <div className="panel">
              <h2>{words.versionsTitle}</h2>
              <p className="note">{words.versionsNote}</p>
              <Measured>
                {(w) => (
                  <RankedBars
                    width={w}
                    words={words}
                    locale={locale}
                    label={words.rankingLabelAll}
                    bars={
                      spread
                        ? spread.versions.map((v) => ({
                            label: v.version,
                            value: v.repositoryCount,
                          }))
                        : []
                    }
                  />
                )}
              </Measured>
              {/* What the list leaves out, said rather than dropped.
                  GitHub's graph reports manifest constraints too, and
                  counted together the constraint `>= 13.0,< 14.0` was
                  this panel's top row for `laravel/framework` — above
                  the real leading version. Excluding them silently
                  would trade one wrong answer for an unexplained
                  one. */}
              {spread && (spread.constrained > 0 || spread.unversioned > 0) ? (
                <ChartNote>
                  {words.versionsNotCounted(
                    spread.constrained > 0
                      ? formatNumber(spread.constrained, locale)
                      : null,
                    spread.unversioned > 0
                      ? formatNumber(spread.unversioned, locale)
                      : null,
                  )}
                </ChartNote>
              ) : null}
              <h2 style={{ marginTop: '.8rem' }}>{words.adoptionTitle}</h2>
              <p className="note">
                {words.adoptionNote}
              </p>
              <Measured>
                {(w) => (
                  <TimeSeries
                    width={w}
                    words={words}
                    locale={locale}
                    snapshotNote={words.adoptionSnapshot}
                  label={words.adoptionLabel(name)}
                  series={adopted ? groupBySource(adopted) : []}
                  />
                )}
              </Measured>
            </div>
          </div>
        </div>
      ) : null}

      {name ? (
        <div className="rails">
          <div className="rail">
            <Panel
              title={words.pullsInTitle}
              qualifier={name}
              note={
                <>
                  {words.pullsInNote}
                </>
              }
            >
              <Measured>
                {(w) =>
                  tree.status === 'ready' && tree.value ? (
                    <DependencyTree
                      tree={tree.value}
                      width={w}
                      words={words}
                      locale={locale}
                      onSelect={(pkg) => go({ view: 'query', package: pkg })}
                      href={(pkg) => formatRoute({ view: 'query', package: pkg })}
                    />
                  ) : (
                    <p className="chart-empty">
                      {tree.status === 'failed'
                        ? queryFailure(tree.error, words)
                        : words.pullsInReading(name)}
                    </p>
                  )
                }
              </Measured>
              <ChartNote>
                {words.pullsInBounded(
                  TREE_SHAPE.children,
                  TREE_SHAPE.branch,
                  largest ? formatNumber(largest, locale) : null,
                )}{' '}
                {caveat}
              </ChartNote>
            </Panel>
          </div>

          <div className="rail">
            <Panel
              title={words.pulledInTitle}
              qualifier={name}
              note={
                words.pulledInNote(name)
              }
            >
              <Measured>
                {(w) => (
                  <RankedBars
                    width={w}
                    words={words}
                    locale={locale}
                    label={words.rankingLabelAll}
                    bars={
                      pullers.status === 'ready'
                        ? pullers.value.map((edge) => ({
                            label: edge.name,
                            value: edge.repositories,
                            onSelect: () =>
                              go({ view: 'query', package: edge.name }),
                            href: formatRoute({ view: 'query', package: edge.name }),
                          }))
                        : []
                    }
                  />
                )}
              </Measured>
              <ChartNote>{caveat}</ChartNote>
            </Panel>
          </div>
        </div>
      ) : null}

      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <Panel
            title={words.askTitle}
            note={
              <>
                {/*
                  This said "the data never leaves your browser", which
                  is not true and is the one kind of claim that has to
                  be. The agent loop runs in the page, but every turn
                  goes through `/api/chat` to Anthropic — and the tool
                  results are posted back as the next user message, so
                  the rows the model reasons over are exactly what gets
                  sent. What is true is narrower and still worth
                  saying: it names typed queries rather than writing
                  SQL, and it never reaches the database itself.
                */}
                {words.askNote}
              </>
            }
          >
            <AskPlaceholder
              ask={ask}
              reset={reset}
              words={words}
              onPackage={(pkg) => go({ view: 'query', package: pkg })}
              suggestions={
                // `mail` when nothing is chosen: a suggestion has to
                // name something, and it is the package the overview
                // used to lead with.
                [
                  words.askSuggestDeclared(name || 'mail'),
                  words.askSuggestVersions(name || 'mail'),
                ]
              }
            />
            {/* Turnstile's widget, drawn while a question is being
                verified and seen only if Cloudflare wants a click. */}
            <div ref={challengeHost} className="challenge" />
          </Panel>
        </div>
      </div>
    </>
  );
}

/**
 * `n` with a noun, singular when `n` is 1.
 *
 * The sentence below read "1 dependants on mail — 0 declare it, 1
 * inherit it", which is three pluralisation faults in one line. A
 * dataset where most packages have a handful of dependants hits the
 * singular constantly, so this is the common case rather than an edge.
 *
 * The number is written for the page's locale, not the browser's (#43).
 */
export function count(
  n: number,
  locale: Locale,
  singular: string,
  plural = `${singular}s`,
): string {
  return `${formatNumber(n, locale)} ${n === 1 ? singular : plural}`;
}

/**
 * The sentence above the table.
 *
 * `total` is the real number of dependants; the rows are a capped page of
 * them. The declared/inherited split is quoted for the rows shown and
 * labelled as such, because it is only known for those — stating it as
 * though it described the total would be a finding the query never made.
 */
export function statusLine(
  name: string,
  result: ReturnType<typeof useAsync<{ rows: Dependent[]; total: number } | null>>,
  directOnly: boolean,
  ecosystem: string,
  words: Dictionary,
  locale: Locale,
): string {
  if (!name) return words.statusPrompt;
  if (result.status === 'loading') return words.statusSearching(name);
  if (result.status === 'failed') return queryFailure(result.error, words);
  if (result.status !== 'ready' || !result.value) return '';

  const { rows, total } = result.value;
  if (rows.length === 0) return words.statusNone(name);

  const direct = rows.filter((d) => d.relationship === 'direct').length;
  const qualified = ecosystem ? `${name} (${ecosystem})` : name;
  // Null when nothing was cut, so the dictionary can drop the clause
  // rather than render "among the 0 shown".
  const shown = total > rows.length ? rows.length : null;

  if (directOnly) {
    return words.statusDeclaredOnly(
      count(total, locale, words.countRepository, words.countRepositoryPlural),
      qualified,
      shown,
      total,
    );
  }
  return words.statusSplit(
    count(total, locale, words.countDependant, words.countDependantPlural),
    qualified,
    direct,
    rows.length - direct,
    shown,
  );
}
