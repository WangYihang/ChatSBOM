/**
 * The overview: the standing questions, answered without being asked.
 *
 * Two rails rather than a row-based grid — a grid row is as tall as its
 * tallest cell, so a long ranking beside a short histogram leaves the
 * grid's own background showing. See the note on `.rails` in style.css.
 */
import { useCallback, useMemo, useState } from 'react';

import { Histogram, StackedShare, TimeSeries } from '../charts/Plots';
import { Measured } from '../charts/Frame';
import { RankedBars } from '../charts/RankedBars';
import { SourceShares } from '../charts/SourceShares';
import { useAsync } from '../hooks';
import type { DatasetClient } from '../d1/client';
import type { RelationshipSplit } from '../d1/queries';
import type { Route } from '../router';
import { Panel } from './Panel';

/** Package whose adoption series the overview shows by default. */
const FEATURED = 'mail';

/** The ranking is the answer, so show more of it. */
const TOP_LIMIT = 20;

export function Overview({
  dataset,
  languages,
  go,
}: {
  dataset: DatasetClient;
  languages: readonly string[];
  go: (route: Route) => void;
}) {
  const [directOnly, setDirectOnly] = useState(true);
  const [language, setLanguage] = useState('');

  const split = useAsync(
    useCallback(() => dataset.relationshipSplit(), [dataset]),
    [dataset],
  );
  const coverage = useAsync(
    useCallback(() => dataset.languageCoverage(), [dataset]),
    [dataset],
  );
  const buckets = useAsync(
    useCallback(() => dataset.dependencyDistribution(), [dataset]),
    [dataset],
  );
  const sources = useAsync(
    useCallback(() => dataset.sourceComparison(), [dataset]),
    [dataset],
  );
  const licences = useAsync(
    useCallback(() => dataset.licenseShares(12), [dataset]),
    [dataset],
  );
  const adoption = useAsync(
    useCallback(() => dataset.adoptionOverTime(FEATURED), [dataset]),
    [dataset],
  );
  const top = useAsync(
    useCallback(
      () =>
        dataset.topPackages({
          directOnly,
          ...(language ? { language } : {}),
          limit: TOP_LIMIT,
        }),
      [dataset, directOnly, language],
    ),
    [dataset, directOnly, language],
  );

  return (
    <>
      <Thesis split={split.status === 'ready' ? split.value : null} />

      <div className="rails">
        <div className="rail">
          <Panel
            title="SBOM coverage by language"
            note={
              <>
                The denominators. Coverage is uneven, so a raw
                cross-language count is not a like-for-like comparison
                &mdash; read this before any ranking below.
              </>
            }
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                label="repositories"
                partLabel="with an SBOM"
                bars={
                  coverage.status === 'ready'
                    ? coverage.value.map((row) => ({
                        label: row.language || '(none)',
                        value: row.repositories,
                        part: row.withSbom,
                        detail: {
                          title: row.language || '(none)',
                          lines: [
                            `${row.repositories.toLocaleString()} repositories`,
                            `${row.withSbom.toLocaleString()} with dependency data ` +
                              `(${row.repositories ? Math.round((row.withSbom / row.repositories) * 100) : 0}%)`,
                          ],
                        },
                      }))
                    : []
                }
                />
              )}
            </Measured>
          </Panel>

          <Panel
            title="Most declared packages"
            qualifier="by repositories that declare them"
            note={
              <>
                Unfiltered, this ranking is <code>semver</code>,{' '}
                <code>debug</code>, <code>ms</code> &mdash; npm utilities
                nobody chooses by name.
              </>
            }
            controls={
              <>
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
                    {languages.map((name) => (
                      <option key={name} value={name}>
                        {name}
                      </option>
                    ))}
                  </select>
                </label>
              </>
            }
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                label={directOnly ? 'repositories declaring it' : 'repositories'}
                bars={
                  top.status === 'ready'
                    ? top.value.map((row) => ({
                        label: row.name,
                        value: directOnly ? row.directCount : row.repositoryCount,
                        // Every bar is a way into the query view; the
                        // handler travels with the datum.
                        onSelect: () => go({ view: 'query', package: row.name }),
                        detail: {
                          title: row.name,
                          lines: [
                            `${row.repositoryCount.toLocaleString()} dependants`,
                            `${row.directCount.toLocaleString()} declared it`,
                          ],
                        },
                      }))
                    : []
                }
                />
              )}
            </Measured>
          </Panel>
        </div>

        <div className="rail">
          <Panel
            title="Dependencies per repository"
            note={
              <>
                Bucketed: the spread covers three orders of magnitude, a Go
                module with 80 next to a TypeScript app with 900.
              </>
            }
          >
            <Measured>
              {(w) => (
                <Histogram
                  width={w}
                label="Repositories by dependency count"
                xLabel="dependencies"
                buckets={
                  buckets.status === 'ready'
                    ? buckets.value.map((b) => ({
                        label: b.label,
                        value: b.repositories,
                      }))
                    : []
                }
                />
              )}
            </Measured>
          </Panel>

          <Panel
            title="Licences"
            note={
              <>
                Unknown is shown rather than dropped: &ldquo;we do not
                know&rdquo; is a finding about SBOM quality, and hiding it
                would overstate coverage.
              </>
            }
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                label="repositories"
                bars={
                  licences.status === 'ready'
                    ? licences.value.map((row) => ({
                        label: row.license || '(unknown)',
                        value: row.repositoryCount,
                        detail: {
                          title: row.license || '(unknown)',
                          lines: [
                            `${row.repositoryCount.toLocaleString()} repositories`,
                            `${row.packageCount.toLocaleString()} distinct packages`,
                          ],
                        },
                      }))
                    : []
                }
                />
              )}
            </Measured>
          </Panel>

          <Panel
            title="Adoption over time"
            qualifier={FEATURED}
            note={
              <>
                The question a snapshot cannot answer; it accumulates as
                the collection queue runs.{' '}
                <button
                  type="button"
                  className="drill"
                  onClick={() => go({ view: 'query', package: FEATURED })}
                >
                  Ask about this package &rarr;
                </button>
              </>
            }
          >
            <Measured>
              {(w) => (
                <TimeSeries
                  width={w}
                label={`Monthly adoption of ${FEATURED}`}
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
          </Panel>
        </div>
      </div>

      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <Panel
            title="Where the data came from"
            qualifier="share of rows per language"
            note={
              <>
                Syft reads lockfiles; GitHub&rsquo;s dependency graph parses
                manifests. Shown as each language&rsquo;s own split, with its
                absolute total, because the row counts span four orders of
                magnitude &mdash; on a shared scale every language but
                TypeScript is an invisible sliver.
              </>
            }
          >
            <Measured>
              {(w) => (
                <SourceShares
                  width={w}
                rows={
                  sources.status === 'ready'
                    ? sources.value.map((row) => ({
                        language: row.language || '(none)',
                        syft: row.syft,
                        depgraph: row.depgraph,
                      }))
                    : []
                }
                />
              )}
            </Measured>
          </Panel>
        </div>
      </div>
    </>
  );
}

/**
 * The page's claim, in the data's own numbers.
 *
 * Computed rather than written down, including the comparative, so a
 * corpus that ever inverts does not keep asserting the old direction. A
 * sentence that says "most" beside a figure that says 92.2% invites the
 * reader to work out which one is stale.
 */
function Thesis({ split }: { split: RelationshipSplit | null }) {
  const total = split ? split.direct + split.transitive + split.unknown : 0;
  const inherited = split ? split.transitive > split.direct : true;
  const share = split
    ? ((inherited ? split.transitive : split.direct) / total) * 100
    : 0;

  return (
    <div className="thesis">
      <div>
        <p className="claim">
          {split && total > 0 ? (
            <>
              <b>{share.toFixed(1)}%</b> of {total.toLocaleString()}{' '}
              dependency records are{' '}
              {inherited ? 'inherited, not chosen' : 'declared outright'}.
            </>
          ) : (
            <>&nbsp;</>
          )}
        </p>
        <p className="gloss">
          Which is why an unfiltered &ldquo;most-used package&rdquo; ranking
          measures lockfile size rather than adoption.
        </p>
      </div>
      <Measured>
        {(w) => (
          <StackedShare
            width={w}
          label="How dependencies arrived, across the whole corpus"
          slices={
            split
              ? [
                  { series: 'direct', label: 'declared', value: split.direct },
                  { series: 'transitive', label: 'inherited', value: split.transitive },
                  { series: 'unknown', label: 'undetermined', value: split.unknown },
                ]
              : []
          }
          />
        )}
      </Measured>
    </div>
  );
}
