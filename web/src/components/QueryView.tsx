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

import { TimeSeries } from '../charts/Plots';
import { Measured } from '../charts/Frame';
import { RankedBars } from '../charts/RankedBars';
import { useAsync, useDebounced } from '../hooks';
import type { Dataset, Dependent } from '../queries';
import type { Route } from '../router';
import { AskPlaceholder } from '../ask/Placeholder';
import { useAsk } from '../ask/useAsk';
import { Panel } from './Panel';

/** How many rows the table shows. The count is asked separately. */
const SHOWN_LIMIT = 100;
const DEBOUNCE_MS = 250;

export function QueryView({
  dataset,
  languages,
  route,
  go,
}: {
  dataset: Dataset;
  languages: readonly string[];
  route: Route;
  go: (route: Route) => void;
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

  // Offered only when the name is genuinely ambiguous: `mail` is a Ruby
  // gem with 118 dependants and a Maven artifactId with 6, so a single
  // count of 124 would describe something that does not exist.
  const ambiguous =
    ecosystems.status === 'ready' && ecosystems.value.length >= 2
      ? ecosystems.value
      : null;

  // A filter that no longer applies must not keep filtering.
  useEffect(() => {
    if (ambiguous && ambiguous.some((row) => row.type === ecosystem)) return;
    if (ecosystem !== '') setEcosystem('');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ambiguous]);

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
      () => (hasRows ? dataset.versionSpread(name, 10) : Promise.resolve([])),
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

  // Hoisted above the conditional markup below, because hooks cannot be
  // called conditionally: with these inline in the `hasRows` branch,
  // React counted a different number of hooks per render and threw
  // "Rendered more hooks than during the previous render". The component
  // tests caught it before a browser did.
  return (
    <>
      <div className="controls" style={{ marginTop: '.5rem' }}>
        <input
          id="package"
          type="search"
          autoComplete="off"
          spellCheck={false}
          placeholder="mail, express, spring-boot-starter-web…"
          aria-label="Package name"
          value={typed}
          onChange={(e) => setTyped(e.target.value)}
        />
        <label className="field">
          <input
            type="checkbox"
            checked={directOnly}
            onChange={(e) => setDirectOnly(e.target.checked)}
          />
          Declared only
        </label>
        <label className="field">
          Language
          <select
            value={language}
            onChange={(e) => setLanguage(e.target.value)}
          >
            <option value="">all</option>
            {languages.map((l) => (
              <option key={l} value={l}>
                {l}
              </option>
            ))}
          </select>
        </label>
        {ambiguous ? (
          <label className="field">
            Ecosystem
            <select
              value={ecosystem}
              onChange={(e) => setEcosystem(e.target.value)}
            >
              <option value="">all {ambiguous.length} ecosystems</option>
              {ambiguous.map((row) => (
                <option key={row.type} value={row.type}>
                  {row.type} · {row.repositoryCount}
                </option>
              ))}
            </select>
          </label>
        ) : null}
      </div>

      <div id="status" aria-live="polite">
        {statusLine(name, result, directOnly, ecosystem)}
      </div>

      {hasRows ? (
        <div className="rails">
          <div className="rail">
            <div className="panel tablewrap">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Repository</th>
                    <th scope="col" className="num">
                      Stars
                    </th>
                    <th scope="col">Version</th>
                    <th scope="col">Depends</th>
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
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="rail">
            <div className="panel">
              <h2>Versions in use</h2>
              <p className="note">Repositories on each resolved version.</p>
              <Measured>
                {(w) => (
                  <RankedBars
                    width={w}
                  label="repositories"
                  bars={
                    versions.status === 'ready'
                      ? versions.value.map((v) => ({
                          label: v.version,
                          value: v.repositoryCount,
                        }))
                      : []
                  }
                  />
                )}
              </Measured>
              <h2 style={{ marginTop: '.8rem' }}>Adoption over time</h2>
              <p className="note">Monthly counts, total and declared.</p>
              <Measured>
                {(w) => (
                  <TimeSeries
                    width={w}
                  label={`Monthly adoption of ${name}`}
                  points={
                    adoption.status === 'ready'
                      ? adoption.value.map((p) => ({
                          label: p.month,
                          total: p.repositoryCount,
                          direct: p.directCount,
                        }))
                      : []
                  }
                  />
                )}
              </Measured>
            </div>
          </div>
        </div>
      ) : null}

      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <Panel
            title="Ask a question"
            note={
              <>
                Answered by a model whose only tools are the same typed
                queries this page uses. It cannot pass SQL, and the data
                never leaves your browser.
              </>
            }
          >
            <AskPlaceholder
              ask={ask}
              onPackage={(pkg) => go({ view: 'query', package: pkg })}
              suggestions={
                name
                  ? [
                      `Which projects declare ${name} rather than inheriting it?`,
                      `What versions of ${name} are in use?`,
                    ]
                  : ['Which projects declare mail rather than inheriting it?']
              }
            />
          </Panel>
        </div>
      </div>
    </>
  );
}

/**
 * The sentence above the table.
 *
 * `total` is the real number of dependants; the rows are a capped page of
 * them. The declared/inherited split is quoted for the rows shown and
 * labelled as such, because it is only known for those — stating it as
 * though it described the total would be a finding the query never made.
 */
function statusLine(
  name: string,
  result: ReturnType<typeof useAsync<{ rows: Dependent[]; total: number } | null>>,
  directOnly: boolean,
  ecosystem: string,
): string {
  if (!name) return 'Type a package name, or pick one from the overview.';
  if (result.status === 'loading') return `Searching for ${name}…`;
  if (result.status === 'failed') return result.message;
  if (result.status !== 'ready' || !result.value) return '';

  const { rows, total } = result.value;
  if (rows.length === 0) {
    return `No repository in the dataset depends on ${name}.`;
  }

  const direct = rows.filter((d) => d.relationship === 'direct').length;
  const qualified = ecosystem ? `${name} (${ecosystem})` : name;
  const capped = total > rows.length;

  if (directOnly) {
    return capped
      ? `${total.toLocaleString()} repositories declare ${qualified}; ` +
          `the ${rows.length} most-starred are shown.`
      : `${total.toLocaleString()} repositories declare ${qualified}.`;
  }
  const split =
    `${direct} declare it, ${rows.length - direct} inherit it` +
    (capped ? ` among the ${rows.length} shown` : '');
  return `${total.toLocaleString()} dependants on ${qualified} — ${split}.`;
}
