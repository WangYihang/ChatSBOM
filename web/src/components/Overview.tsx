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
import type { RelationshipSplit } from '../d1/queries';
import type { Route } from '../router';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { Panel } from './Panel';

/** The ranking is the answer, so show more of it. */
const TOP_LIMIT = 20;

export function Overview({
  dataset,
  languages,
  go,
  words,
  locale,
}: {
  dataset: DatasetClient;
  languages: readonly string[];
  go: (route: Route) => void;
  words: Dictionary;
  locale: Locale;
}) {
  const [directOnly, setDirectOnly] = useState(true);
  const [language, setLanguage] = useState('');

  const split = useAsync(
    useCallback(() => dataset.relationshipSplit(), [dataset]),
    [dataset],
  );
  const byLanguage = useAsync(
    useCallback(() => dataset.relationshipByLanguage(), [dataset]),
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
                    byLanguage.status === 'ready'
                      ? byLanguage.value
                        .filter((row) => row.records > 0)
                        .map((row) => ({
                          label: row.language,
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
                            title: row.language,
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
                  {words.languageFilter}
                  <select
                    value={language}
                    onChange={(e) => setLanguage(e.target.value)}
                  >
                    <option value="">{words.languageAll}</option>
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
                        label: row.language || '(none)',
                        value: row.repositories,
                        part: row.withSbom,
                        detail: {
                          title: row.language || '(none)',
                          lines: [
                            `${row.repositories.toLocaleString(locale)} repositories`,
                            words.coverageBarTitle(
                              row.withSbom.toLocaleString(locale),
                              row.repositories
                                ? Math.round(
                                  (row.withSbom / row.repositories) * 100,
                                )
                                : 0,
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
