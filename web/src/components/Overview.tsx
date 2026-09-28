/**
 * The overview: the standing questions, answered without being asked.
 *
 * Two rails rather than a row-based grid — a grid row is as tall as its
 * tallest cell, so a long ranking beside a short histogram leaves the
 * grid's own background showing. See the note on `.rails` in style.css.
 */
import { useCallback, useMemo, useState } from 'react';

import {
  groupBySource,
  Histogram,
  StackedShare,
  TimeSeries,
} from '../charts/Plots';
import { Measured } from '../charts/Frame';
import { RankedBars } from '../charts/RankedBars';
import { SourceShares } from '../charts/SourceShares';
import { useAsync } from '../hooks';
import type { DatasetClient } from '../d1/client';
import type { RelationshipSplit } from '../dataset/types';
import type { Route } from '../router';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { Panel } from './Panel';

/** The ranking is the answer, so show more of it. */
const TOP_LIMIT = 20;

/**
 * Ecosystems drawn in the per-ecosystem panels. The collectors report a
 * long tail — `binary`, `github-action`, `deb` — with a few hundred
 * records each; past this many rows the panel is a list, not a chart.
 */
const ECOSYSTEM_ROWS = 12;

/** `part` of `whole` as a whole percentage, for a bar's detail line. */
function percent(part: number, whole: number): number {
  return whole ? Math.round((part / whole) * 100) : 0;
}

export function Overview({
  dataset,
  ecosystems,
  go,
  words,
  locale,
}: {
  dataset: DatasetClient;
  /** The ranking's filter values: ecosystems, as the data has them. */
  ecosystems: readonly string[];
  go: (route: Route) => void;
  words: Dictionary;
  locale: Locale;
}) {
  const [directOnly, setDirectOnly] = useState(true);
  const [ecosystem, setEcosystem] = useState('');

  const split = useAsync(
    useCallback((signal: AbortSignal) => dataset.relationshipSplit(undefined, signal), [dataset]),
    [dataset],
  );
  const byEcosystem = useAsync(
    useCallback((signal: AbortSignal) => dataset.relationshipByEcosystem(signal), [dataset]),
    [dataset],
  );
  const coverage = useAsync(
    useCallback((signal: AbortSignal) => dataset.languageCoverage(signal), [dataset]),
    [dataset],
  );
  const ecosystemCoverage = useAsync(
    useCallback((signal: AbortSignal) => dataset.ecosystemCoverage(signal), [dataset]),
    [dataset],
  );
  const buckets = useAsync(
    useCallback((signal: AbortSignal) => dataset.dependencyDistribution(signal), [dataset]),
    [dataset],
  );
  const sources = useAsync(
    useCallback((signal: AbortSignal) => dataset.sourceComparison(signal), [dataset]),
    [dataset],
  );
  const licences = useAsync(
    useCallback((signal: AbortSignal) => dataset.licenseShares(12, signal), [dataset]),
    [dataset],
  );
  const top = useAsync(
    useCallback(
      (signal: AbortSignal) =>
        dataset.topPackages(
          {
            directOnly,
            ...(ecosystem ? { ecosystem } : {}),
            limit: TOP_LIMIT,
          },
          signal,
        ),
      [dataset, directOnly, ecosystem],
    ),
    [dataset, directOnly, ecosystem],
  );

  return (
    <>
      <Thesis
        split={split.status === 'ready' ? split.value : null}
        words={words}
        locale={locale}
      />

      <div className="rails">
        <div className="rail">
          <Panel
            title={words.splitTitle}
            qualifier={words.splitQualifier}
            note={words.splitNote}
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                  label={words.splitLabel}
                  valueFormat={(value) => `${value.toFixed(1)}%`}
                  bars={
                    byEcosystem.status === 'ready'
                      ? byEcosystem.value
                        .filter((row) => row.records > 0)
                        .slice(0, ECOSYSTEM_ROWS)
                        .map((row) => ({
                          label: row.ecosystem,
                          // The share, not the count, and no `part`.
                          //
                          // Drawn as `value: records, part: direct`
                          // first, which buried the finding: records
                          // span 8.6M to 2.9K, so TypeScript's 9.2%
                          // was a 50px fill on a 541px track while
                          // Rust's 49.3% was 38px on 78px — the larger
                          // share drawn shorter. Four orders of
                          // magnitude on a shared scale is the same
                          // trap the source panel's note describes.
                          value: (row.direct / row.records) * 100,
                          detail: {
                            title: row.ecosystem,
                            lines: [
                              `${((row.direct / row.records) * 100).toFixed(1)}% declared`,
                              `${row.direct.toLocaleString(locale)} declared`,
                              `${row.transitive.toLocaleString(locale)} inherited`,
                              `${row.records.toLocaleString(locale)} records in total`,
                            ],
                          },
                        }))
                        .sort((a, b) => b.value - a.value)
                      : []
                  }
                />
              )}
            </Measured>
          </Panel>


          {/*
            Title and qualifier follow the filter. They used to be
            fixed, so clearing "declared only" left the panel headed
            "Most declared packages / by repositories that declare
            them" above a ranking of semver, debug and ms — the very
            list the note below calls npm utilities nobody chooses by
            name. That is this page's whole argument stated backwards,
            on the one panel where the distinction is the point.
          */}
          <Panel
            title={
              directOnly ? words.rankingTitleDeclared : words.rankingTitleAll
            }
            qualifier={
              directOnly
                ? words.rankingQualifierDeclared
                : words.rankingQualifierAll
            }
            note={words.rankingNote}
            controls={
              <>
                <label className="field">
                  <input
                    type="checkbox"
                    checked={directOnly}
                    onChange={(e) => setDirectOnly(e.target.checked)}
                  />
                  {words.declaredOnly}
                </label>
                <label className="field">
                  {words.ecosystemFilter}
                  <select
                    value={ecosystem}
                    onChange={(e) => setEcosystem(e.target.value)}
                  >
                    <option value="">{words.ecosystemAny}</option>
                    {ecosystems.map((name) => (
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
                label={directOnly ? words.rankingLabelDeclared : words.rankingLabelAll}
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
                            `${row.repositoryCount.toLocaleString(locale)} dependants`,
                            `${row.directCount.toLocaleString(locale)} declared it`,
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
            title={words.coverageTitle}
            note={words.coverageNote}
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                label={words.coverageLabel}
                partLabel={words.coveragePartLabel}
                bars={
                  coverage.status === 'ready'
                    ? coverage.value.map((row) => ({
                        label: row.language || 'none',
                        value: row.repositories,
                        part: row.withSbom,
                        detail: {
                          title: row.language || 'none',
                          lines: [
                            `${row.repositories.toLocaleString(locale)} repositories`,
                            words.coverageBarTitle(
                              row.withSbom.toLocaleString(locale),
                              percent(row.withSbom, row.repositories),
                            ),
                            ...words.coverageSources(
                              row.withSyft.toLocaleString(locale),
                              row.withDepgraph.toLocaleString(locale),
                              row.withManifest.toLocaleString(locale),
                            ),
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
            title={words.ecosystemCoverageTitle}
            note={words.ecosystemCoverageNote}
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                  label={words.coverageLabel}
                  partLabel={words.ecosystemCoveragePartLabel}
                  bars={
                    ecosystemCoverage.status === 'ready'
                      ? ecosystemCoverage.value
                        .slice(0, ECOSYSTEM_ROWS)
                        .map((row) => ({
                          label: row.ecosystem,
                          value: row.repositories,
                          part: row.withSyft,
                          onSelect: () => setEcosystem(row.ecosystem),
                          detail: {
                            title: row.ecosystem,
                            lines: [
                              `${row.repositories.toLocaleString(locale)} repositories`,
                              words.ecosystemCoverageBarTitle(
                                row.withSyft.toLocaleString(locale),
                                percent(row.withSyft, row.repositories),
                              ),
                              ...words.coverageSources(
                                row.withSyft.toLocaleString(locale),
                                row.withDepgraph.toLocaleString(locale),
                                row.withManifest.toLocaleString(locale),
                              ),
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
            title={words.bucketsTitle}
            note={words.bucketsNote}
          >
            <Measured>
              {(w) => (
                <Histogram
                  width={w}
                label={words.bucketsLabel}
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
            title={words.licencesTitle}
            note={words.licencesNote}
          >
            <Measured>
              {(w) => (
                <RankedBars
                  width={w}
                label={words.coverageLabel}
                bars={
                  licences.status === 'ready'
                    ? licences.value.map((row) => ({
                        label: row.license || words.licenceUnknown,
                        value: row.repositoryCount,
                        detail: {
                          title: row.license || words.licenceUnknown,
                          lines: [
                            `${row.repositoryCount.toLocaleString(locale)} repositories`,
                            `${row.packageCount.toLocaleString(locale)} distinct packages`,
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
      </div>

      <div className="rails">
        <div className="rail" style={{ gridColumn: '1 / -1' }}>
          <Panel
            title={words.sourcesTitle}
            qualifier={words.sourcesQualifier}
            note={words.sourcesNote}
          >
            <Measured>
              {(w) => (
                <SourceShares
                  width={w}
                  label={words.sourcesChartLabel}
                rows={
                  sources.status === 'ready'
                    ? sources.value.slice(0, ECOSYSTEM_ROWS).map((row) => ({
                        label: row.ecosystem,
                        syft: row.syft,
                        depgraph: row.depgraph,
                        manifest: row.manifest,
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
function Thesis({
  split,
  words,
  locale,
}: {
  split: RelationshipSplit | null;
  words: Dictionary;
  locale: Locale;
}) {
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
            words.heroLede(
              <b>{share.toFixed(1)}%</b>,
              total.toLocaleString(locale),
              inherited ? words.heroInherited : words.heroDeclared,
            )
          ) : (
            <>&nbsp;</>
          )}
        </p>
        <p className="gloss">{words.heroWhy}</p>
      </div>
      <Measured>
        {(w) => (
          <StackedShare
            width={w}
            label={words.heroLabel}
          slices={
            split
              ? [
                  { series: 'direct', label: words.heroDeclared, value: split.direct },
                  { series: 'transitive', label: words.heroInherited,
                    value: split.transitive },
                  { series: 'unknown', label: words.heroUndetermined,
                    value: split.unknown },
                ]
              : []
          }
          />
        )}
      </Measured>
    </div>
  );
}
