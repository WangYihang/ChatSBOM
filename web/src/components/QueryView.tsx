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
import { useCallback, useEffect, useMemo, useState } from 'react';

import { groupBySource, TimeSeries } from '../charts/Plots';
import { ChartNote, Measured } from '../charts/Frame';
import { DependencyTree } from '../charts/DependencyTree';
import { RankedBars } from '../charts/RankedBars';
import { useAsync, useDebounced } from '../hooks';
import type { DatasetClient } from '../d1/client';
import type {
  Dependent,
  EdgeAmbiguity,
  VersionSpread,
} from '../d1/queries';
import type { Route } from '../router';
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
    scale.ambiguousNames.toLocaleString(locale),
    scale.names.toLocaleString(locale),
    scale.ambiguousEdges.toLocaleString(locale),
    scale.edges.toLocaleString(locale),
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
  go: (route: Route) => void;
  words: Dictionary;
  locale: Locale;
}) {
  const [typed, setTyped] = useState(route.package ?? '');
  const [directOnly, setDirectOnly] = useState(false);
  const [language, setLanguage] = useState('');
  const [ecosystem, setEcosystem] = useState('');

  // The natural-language slot's only dependency on this page.
  const ask = useAsk(dataset);

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
      go({ view: 'query', package: settled });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [settled]);

  const name = settled;

  const ecosystems = useAsync(
    useCallback(
      () => (name ? dataset.ecosystemsFor(name) : Promise.resolve([])),
      [dataset, name],
    ),
    [dataset, name],
  );

  // Not keyed on `name`: the collision scale is a property of the edge
  // table, so this is one read per mount rather than one per package.
  const ambiguity = useAsync(
    useCallback(() => dataset.edgeAmbiguity(), [dataset]),
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

  const result = useAsync(
    useCallback(
      () =>
        !name
          ? Promise.resolve(null)
          : Promise.all([
              dataset.dependentsOf({ ...filters, limit: SHOWN_LIMIT }),
              dataset.countDependents(filters),
            ]).then(([rows, total]) => ({ rows, total })),
      [dataset, name, filters],
    ),
    [dataset, name, filters],
  );

  const rows = result.status === 'ready' && result.value ? result.value.rows : [];
  const hasRows = rows.length > 0;

  const versions = useAsync(
    useCallback(
      () =>
        hasRows
          ? dataset.versionSpread(name, 10)
          : Promise.resolve(EMPTY_SPREAD),
      [dataset, name, hasRows],
    ),
    [dataset, name, hasRows],
  );
  const adoption = useAsync(
    useCallback(
      () => (hasRows ? dataset.adoptionOverTime(name) : Promise.resolve([])),
      [dataset, name, hasRows],
    ),
    [dataset, name, hasRows],
  );

  // Candidates for the search box.
  //
  // Gated at two characters: a one-letter term matches thousands of
  // names, and ranking them needs every match read before the limit
  // applies. Two is where the range stops being most of the table.
  const candidates = useAsync(
    useCallback(
      () =>
        name.length >= MIN_SEARCH
          ? dataset.searchPackages(name, SUGGEST_LIMIT)
          : Promise.resolve([]),
      [dataset, name],
    ),
    [dataset, name],
  );

  const pullers = useAsync(
    useCallback(
      () => (name ? dataset.pulledInBy(name, PULLERS_LIMIT) : Promise.resolve([])),
      [dataset, name],
    ),
    [dataset, name],
  );
  const tree = useAsync(
    useCallback(
      () =>
        name
          ? dataset.dependencyTree(name, TREE_SHAPE)
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
          result.status === 'ready' &&
          !!result.value &&
          result.value.rows.length === 0
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
                  {row.type} · {row.repositoryCount}
                </option>
              ))}
            </select>
          </label>
        ) : null}
      </PackageSearch>

      <div id="status" aria-live="polite">
        {statusLine(name, result, directOnly, ecosystem, words)}
      </div>

      {hasRows ? (
        <div className="rails">
          <div className="rail">
            <div className="panel tablewrap">
              <table>
                <thead>
                  <tr>
                    <th scope="col">{words.tableRepository}</th>
                    <th scope="col" className="num">
                      {words.tableStars}
                    </th>
                    <th scope="col">{words.tableVersion}</th>
                    <th scope="col">{words.tableDepends}</th>
                    {/* When we last looked, not when upstream last
                        pushed — the first is what explains a stale row. */}
                    <th scope="col">{words.tableScanned}</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((dep) => (
                    <tr key={`${dep.owner}/${dep.repo}/${dep.version}`}>
                      <td>
                        <a href={dep.url} rel="noreferrer noopener">
                          {dep.owner}/{dep.repo}
                        </a>
                      </td>
                      <td className="num">{dep.stars.toLocaleString()}</td>
                      <td className="mono">{dep.version || '—'}</td>
                      <td>
                        {/* Border style carries the state as well as
                            colour, so the distinction survives
                            colour-vision deficiency and greyscale. */}
                        <span className={`pill ${dep.relationship}`}>
                          {dep.relationship}
                        </span>
                      </td>
                      <td className="mono">{dep.observedAt || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="rail">
            <div className="panel">
              <h2>{words.versionsTitle}</h2>
              <p className="note">{words.versionsNote}</p>
              <Measured>
                {(w) => (
                  <RankedBars
                    width={w}
                    label={words.rankingLabelAll}
                    bars={
                      versions.status === 'ready'
                        ? versions.value.versions.map((v) => ({
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
              {versions.status === 'ready' &&
              (versions.value.constrained > 0 ||
                versions.value.unversioned > 0) ? (
                <ChartNote>
                  {words.versionsNotCounted(
                    versions.value.constrained > 0
                      ? versions.value.constrained.toLocaleString(locale)
                      : null,
                    versions.value.unversioned > 0
                      ? versions.value.unversioned.toLocaleString(locale)
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
                  label={words.adoptionLabel(name)}
                  series={
                    adoption.status === 'ready'
                      ? groupBySource(adoption.value)
                      : []
                  }
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
                      onSelect={(pkg) => go({ view: 'query', package: pkg })}
                    />
                  ) : (
                    <p className="chart-empty">
                      {tree.status === 'failed'
                        ? tree.message
                        : `Reading the edge table for ${name}…`}
                    </p>
                  )
                }
              </Measured>
              <ChartNote>
                Bounded to {TREE_SHAPE.children} packages and{' '}
                {TREE_SHAPE.branch} per package. The unbounded graph is not
                a smaller version of this
                {largest ? (
                  <>
                    : the largest repository here has{' '}
                    {largest.toLocaleString()} dependencies
                  </>
                ) : null}
                . {caveat}
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
                    label={words.rankingLabelAll}
                    bars={
                      pullers.status === 'ready'
                        ? pullers.value.map((edge) => ({
                            label: edge.name,
                            value: edge.repositories,
                            onSelect: () =>
                              go({ view: 'query', package: edge.name }),
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
 */
export function count(n: number, singular: string, plural = `${singular}s`):
  string {
  return `${n.toLocaleString()} ${n === 1 ? singular : plural}`;
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
): string {
  if (!name) return words.statusPrompt;
  if (result.status === 'loading') return words.statusSearching(name);
  if (result.status === 'failed') return result.message;
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
      count(total, words.countRepository, words.countRepositoryPlural),
      qualified,
      shown,
      total,
    );
  }
  return words.statusSplit(
    count(total, words.countDependant, words.countDependantPlural),
    qualified,
    direct,
    rows.length - direct,
    shown,
  );
}
