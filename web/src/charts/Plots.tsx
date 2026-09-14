/**
 * The three remaining forms: a share bar, a histogram, and a time
 * series.
 *
 * visx earns more here than it does on the rankings. `scaleBand` and
 * `scaleLinear` replace hand-rolled slot arithmetic, and `AxisBottom`
 * replaces the tick-and-collision logic this project had written itself
 * — the code that decided to "label every other bucket when they would
 * otherwise collide" was a guess at a problem visx has solved properly.
 */
import { AxisBottom } from '@visx/axis';
import { scaleBand, scaleLinear } from '@visx/scale';

import { ChartFrame, ChartNote, Empty, Legend, useChartTheme, useChartTooltip } from './Frame';
import { barPath, SPACER } from './geometry';
import {
  isSeriesName,
  rampColor,
  seriesColor,
  type SeriesName,
} from '../palette';

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
  width = 720,
}: {
  slices: readonly Slice[];
  label: string;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  const total = slices.reduce((sum, slice) => sum + slice.value, 0);
  if (total === 0) return <Empty />;

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
                    `${slice.value.toLocaleString()} (${share.toFixed(1)}%)`,
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
  width = 720,
}: {
  buckets: readonly Bucket[];
  label: string;
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
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (buckets.length === 0) return <Empty />;

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
                {Math.round((max * (2 - step)) / 2).toLocaleString()}
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
                lines: [bucket.value.toLocaleString()],
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

export function TimeSeries({
  series,
  label,
  width = 720,
}: {
  series: readonly TimeSeriesGroup[];
  label: string;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  const drawn = series.filter((group) => group.points.length > 0);
  if (drawn.length === 0) {
    return <Empty message="No history yet — it accumulates as the queue runs." />;
  }

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
                {Math.round((max * (2 - step)) / 2).toLocaleString()}
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
                    lines: [
                      `${point.total.toLocaleString()} repositories`,
                      `${point.direct.toLocaleString()} declared it`,
                    ],
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
        <ChartNote>
          One observation per source, which is a snapshot rather than a
          trend — and the two were taken seven months apart by different
          tools, so the gap between them is not a change in adoption. A
          second run of either gives that line a direction.
        </ChartNote>
      ) : null}
      {tooltip}
    </>
  );
}
