/**
 * The remaining forms: a share bar, a histogram, and the series a time
 * series is drawn from. The time series itself is in `TimeSeries.tsx`,
 * which the page loads with the query view's charts (#44).
 *
 * visx earns more here than it does on the rankings. `scaleBand` and
 * `scaleLinear` replace hand-rolled slot arithmetic, and `AxisBottom`
 * replaces the tick-and-collision logic this project had written itself
 * — the code that decided to "label every other bucket when they would
 * otherwise collide" was a guess at a problem visx has solved properly.
 */
import { AxisBottom } from '@visx/axis';
import { scaleBand, scaleLinear } from '@visx/scale';

import {
  ChartFrame,
  ChartTable,
  Empty,
  Legend,
  useChartTheme,
  useChartTooltip,
} from './Frame';
import { barPath, SPACER } from './geometry';
import { formatNumber } from '../i18n/format';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { rampColor, seriesColor, type SeriesName } from '../palette';

/* ─────────────────────────── share bar ─────────────────────────── */

export interface Slice {
  series: SeriesName;
  label: string;
  value: number;
}

/**
 * One quantity in parts, so one bar rather than three.
 *
 * Taller than the rankings on purpose: this is the page's thesis, and in
 * an earlier revision it was the smallest mark on the page.
 */
export function StackedShare({
  slices,
  label,
  valueLabel,
  width = 720,
  words,
  locale,
}: {
  slices: readonly Slice[];
  label: string;
  /** What the slices count, for the table that gives them (`ChartTable`). */
  valueLabel: string;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
  words: Dictionary;
  locale: Locale;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  const total = slices.reduce((sum, slice) => sum + slice.value, 0);
  if (total === 0) return <Empty message={words.noDataForSelection} />;

  const barHeight = 34;
  const scale = scaleLinear({ domain: [0, total], range: [0, width] });

  let x = 0;

  return (
    <>
      <ChartFrame width={width} height={barHeight + 4} label={label}>
        {slices.map((slice, index) => {
          const raw = scale(slice.value);
          const last = index === slices.length - 1;
          const segment = Math.max(raw - (last ? 0 : SPACER), 1);
          const left = x;
          x += raw;
          const share = (slice.value / total) * 100;

          return (
            <g key={slice.series}>
              <path
                d={barPath(left, 0, segment, barHeight, true)}
                fill={seriesColor(slice.series, theme)}
                {...bind({
                  title: slice.label,
                  lines: [
                    `${formatNumber(slice.value, locale)} (${share.toFixed(1)}%)`,
                  ],
                })}
              />
              {/* Direct-label only segments wide enough to hold the text;
                  a number on every mark is noise, and a number that
                  overflows its mark is worse. */}
              {raw > 84 ? (
                <text
                  x={left + 10}
                  y={barHeight / 2}
                  dominantBaseline="middle"
                  fill={theme.surface}
                  fontSize={12}
                  fontWeight={600}
                >
                  {share.toFixed(1)}%
                </text>
              ) : null}
            </g>
          );
        })}
      </ChartFrame>

      <Legend
        entries={slices.map((slice) => ({
          swatch: seriesColor(slice.series, theme),
          label: `${slice.label} · ${((slice.value / total) * 100).toFixed(1)}%`,
        }))}
      />
      <ChartTable
        caption={label}
        columns={[valueLabel, words.chartShare]}
        rows={slices.map((slice) => ({
          name: slice.label,
          cells: [
            formatNumber(slice.value, locale),
            `${((slice.value / total) * 100).toFixed(1)}%`,
          ],
        }))}
      />
      {tooltip}
    </>
  );
}

/* ─────────────────────────── histogram ─────────────────────────── */

export interface Bucket {
  label: string;
  value: number;
}

/** Counts across ordered buckets. One hue: this is magnitude, not identity. */
export function Histogram({
  buckets,
  label,
  xLabel,
  valueLabel,
  width = 720,
  words,
  locale,
}: {
  buckets: readonly Bucket[];
  label: string;
  /** What the bars count, for the table that gives them (`ChartTable`). */
  valueLabel: string;
  /**
   * Names the x dimension.
   *
   * Spread conditionally onto AxisBottom rather than passed through:
   * this project sets `exactOptionalPropertyTypes`, and visx's prop is
   * `label?: string` rather than `string | undefined`, so handing it an
   * explicit undefined is a type error. Omitting the key is the honest
   * way to say "no label".
   */
  xLabel?: string;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
  words: Dictionary;
  locale: Locale;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (buckets.length === 0) return <Empty message={words.noDataForSelection} />;

  const height = 150;
  const pad = { top: 10, right: 6, bottom: 26, left: 44 };
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const max = Math.max(...buckets.map((bucket) => bucket.value)) || 1;

  const x = scaleBand({
    domain: buckets.map((bucket) => bucket.label),
    range: [pad.left, pad.left + plotWidth],
    padding: 0.12,
  });
  const y = scaleLinear({ domain: [0, max], range: [plotHeight, 0] });

  return (
    <>
      <ChartFrame width={width} height={height} label={label}>
        {/* Recessive gridlines, behind the marks. */}
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

        {buckets.map((bucket) => {
          const barWidth = x.bandwidth();
          const barHeight = Math.max(plotHeight - y(bucket.value), 1);
          const left = x(bucket.label) ?? pad.left;
          return (
            <path
              key={bucket.label}
              d={barPath(
                left,
                pad.top + plotHeight - barHeight,
                barWidth,
                barHeight,
                false,
              )}
              fill={rampColor(bucket.value / max, theme)}
              {...bind({
                title: bucket.label,
                lines: [formatNumber(bucket.value, locale)],
              })}
            />
          );
        })}

        {/* visx places and rotates the tick labels, which is what the
            hand-written "label every other bucket" rule was guessing at. */}
        <AxisBottom
          top={pad.top + plotHeight}
          scale={x}
          stroke={theme.axis}
          tickStroke={theme.axis}
          hideTicks
          tickLabelProps={() => ({
            fill: theme.inkMuted,
            fontSize: 9,
            textAnchor: 'middle',
            fontFamily: 'var(--f-display)',
          })}
          {...(xLabel === undefined ? {} : { label: xLabel })}
          labelProps={{
            fill: theme.inkMuted,
            fontSize: 9,
            textAnchor: 'middle',
            fontFamily: 'var(--f-display)',
          }}
          labelOffset={8}
        />
      </ChartFrame>
      <ChartTable
        caption={label}
        head={xLabel}
        columns={[valueLabel]}
        rows={buckets.map((bucket) => ({
          name: bucket.label,
          cells: [formatNumber(bucket.value, locale)],
        }))}
      />
      {tooltip}
    </>
  );
}

/* ────────────────────────── time series ────────────────────────── */

export interface TimePoint {
  label: string;
  total: number;
  direct: number;
}

/**
 * One instrument's observations of one package.
 *
 * The series are kept apart because the two instruments measure
 * different things. Syft resolves a lockfile's closure; GitHub's
 * dependency graph parses manifests. They ran seven months apart, so a
 * single line over both drew `mail` from February's 124 to September's
 * 149 and read as adoption growing — when the only thing that changed
 * was which tool was looking.
 *
 * One line each says what it actually is: two collections, two
 * measurements. When a second run of the same tool lands, that line
 * gains a second point and becomes a trend the reader can believe.
 */
export interface TimeSeriesGroup {
  /** `syft` or `github-depgraph`. Names the line in the legend. */
  source: string;
  points: readonly TimePoint[];
}

/**
 * Group adoption rows into one series per source.
 *
 * Both call sites need this and both would otherwise write the same
 * reduce — and getting it wrong means merging two instruments back into
 * one line, which is the defect the split exists to remove.
 *
 * Sources come out in a stable order so a colour does not move between
 * renders, and months are sorted because a line drawn in arrival order
 * zigzags.
 */
export function groupBySource(
  rows: readonly {
    source: string;
    month: string;
    repositoryCount: number;
    directCount: number;
  }[],
): TimeSeriesGroup[] {
  const bySource = new Map<string, TimePoint[]>();
  for (const row of rows) {
    const points = bySource.get(row.source) ?? [];
    points.push({
      label: row.month,
      total: row.repositoryCount,
      direct: row.directCount,
    });
    bySource.set(row.source, points);
  }
  return [...bySource.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([source, points]) => ({
      source,
      points: points.sort((a, b) => a.label.localeCompare(b.label)),
    }));
}
