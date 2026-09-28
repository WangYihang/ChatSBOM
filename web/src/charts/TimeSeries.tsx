/**
 * Adoption over time: one line per instrument, on one timeline.
 *
 * Only the query view draws it, for a package it names, so the page
 * loads it with that view's other charts rather than with the overview
 * (#44). The series it draws are grouped by `groupBySource`, which
 * stays in `Plots.tsx` with its types, for the page to run before this
 * has loaded.
 */
import type { ReactNode } from 'react';
import { scaleLinear } from '@visx/scale';

import {
  ChartFrame,
  ChartNote,
  ChartTable,
  Empty,
  Legend,
  useChartTheme,
  useChartTooltip,
} from './Frame';
import type { TimeSeriesGroup } from './Plots';
import { formatNumber } from '../i18n/format';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { isSeriesName, seriesColor } from '../palette';

export function TimeSeries({
  series,
  label,
  snapshotNote,
  width = 720,
  words,
  locale,
}: {
  series: readonly TimeSeriesGroup[];
  label: string;
  /**
   * Shown when every source has a single observation, so the reader
   * is told the chart is a snapshot rather than a trend.
   *
   * A prop because the words are the caller's: this was hardcoded
   * English — missed by the sweep, which looked at `components/` and
   * at attributes and not at multi-line JSX under `charts/` — and it
   * said "seven months apart", a figure pasted into copy that the next
   * collection makes wrong.
   */
  snapshotNote: ReactNode;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
  words: Dictionary;
  locale: Locale;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  const drawn = series.filter((group) => group.points.length > 0);
  if (drawn.length === 0) return <Empty message={words.adoptionEmpty} />;

  const height = 150;
  const pad = { top: 10, right: 10, bottom: 26, left: 44 };
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;

  // One x axis over every month any series observed, so two lines on
  // the same chart are on the same timeline. Scaling each series to its
  // own months would put February and September at the same x and make
  // the two instruments look simultaneous.
  const months = [
    ...new Set(drawn.flatMap((group) => group.points.map((p) => p.label))),
  ].sort();
  const max =
    Math.max(...drawn.flatMap((group) => group.points.map((p) => p.total))) || 1;

  const x = (month: string) => {
    const index = months.indexOf(month);
    return (
      pad.left +
      (months.length === 1
        ? plotWidth / 2
        : (index / (months.length - 1)) * plotWidth)
    );
  };
  const y = scaleLinear({
    domain: [0, max],
    range: [pad.top + plotHeight, pad.top],
  });

  const colourOf = (source: string) =>
    isSeriesName(source) ? seriesColor(source, theme) : theme.inkMuted;

  // A single observation is a snapshot, not a trend. A line from the
  // origin to one point draws a ramp that reads as "grew from zero",
  // which is a claim one measurement cannot support — and with the
  // series split by instrument, every line currently has one point.
  const single = drawn.filter((group) => group.points.length === 1);

  return (
    <>
      <ChartFrame width={width} height={height} label={label}>
        {[0, 1, 2].map((step) => {
          const gy = pad.top + (plotHeight * step) / 2;
          return (
            <g key={step}>
              <line
                x1={pad.left}
                y1={gy}
                x2={width - pad.right}
                y2={gy}
                stroke={theme.grid}
                strokeWidth={1}
              />
              <text
                x={pad.left - 8}
                y={gy}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.inkMuted}
                fontSize={9}
                fontFamily="var(--f-mono)"
              >
                {formatNumber(Math.round((max * (2 - step)) / 2), locale)}
              </text>
            </g>
          );
        })}

        {drawn.map((group) => {
          const colour = colourOf(group.source);
          const ordered = [...group.points].sort((a, b) =>
            a.label.localeCompare(b.label),
          );
          const line = ordered
            .map((point) => `${x(point.label)},${y(point.total)}`)
            .join(' ');

          return (
            <g key={group.source} data-series={group.source}>
              {ordered.length > 1 ? (
                <polyline
                  points={line}
                  fill="none"
                  stroke={colour}
                  strokeWidth={2}
                  strokeLinejoin="round"
                />
              ) : null}
              {ordered.map((point) => (
                <circle
                  key={point.label}
                  cx={x(point.label)}
                  cy={y(point.total)}
                  r={4}
                  fill={colour}
                  stroke={theme.surface}
                  strokeWidth={2}
                  {...bind({
                    title: `${group.source} · ${point.label}`,
                    lines: words.adoptionPoint(
                      formatNumber(point.total, locale),
                      formatNumber(point.direct, locale),
                    ),
                  })}
                />
              ))}
            </g>
          );
        })}

        {months.map((month, index) =>
          months.length <= 12 ||
          index % Math.ceil(months.length / 8) === 0 ? (
            <text
              key={month}
              x={x(month)}
              y={height - pad.bottom + 14}
              // The end labels anchor inward. A centred label at the
              // last month sits at `width - pad.right`, so half of it
              // — measured 8.8px of `2026-09` — falls outside the
              // frame and is clipped. `pad.right` is 10px and cannot
              // absorb a 38px label, so the anchor moves instead of
              // the padding.
              textAnchor={
                index === 0 && months.length > 1
                  ? 'start'
                  : index === months.length - 1 && months.length > 1
                    ? 'end'
                    : 'middle'
              }
              fill={theme.inkMuted}
              fontSize={9}
              fontFamily="var(--f-mono)"
            >
              {month}
            </text>
          ) : null,
        )}
      </ChartFrame>

      {/* Identity is never colour alone. */}
      <Legend
        entries={drawn.map((group) => ({
          swatch: colourOf(group.source),
          label: group.source,
        }))}
      />

      {single.length === drawn.length ? (
        <ChartNote>{snapshotNote}</ChartNote>
      ) : null}
      <ChartTable
        caption={label}
        head={words.adoptionColumns.source}
        columns={[
          words.adoptionColumns.month,
          words.adoptionColumns.repositories,
          words.adoptionColumns.declared,
        ]}
        rows={drawn.flatMap((group) =>
          [...group.points].sort((a, b) => a.label.localeCompare(b.label)).map((point) => ({
            name: group.source,
            cells: [
              point.label,
              formatNumber(point.total, locale),
              formatNumber(point.direct, locale),
            ],
          })),
        )}
      />
      {tooltip}
    </>
  );
}
