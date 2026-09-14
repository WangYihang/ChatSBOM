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
 * Adoption over time: total repositories and, of those, how many declare
 * the package.
 *
 * Two series on **one** axis — both are repository counts, so a second
 * scale would be the dual-axis mistake.
 */
export function TimeSeries({
  points,
  label,
  width = 720,
}: {
  points: readonly TimePoint[];
  label: string;
  /** Measured panel width, so the type size does not scale with it. */
  width?: number;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (points.length === 0) {
    return <Empty message="No history yet — it accumulates as the queue runs." />;
  }

  const height = 150;
  const pad = { top: 10, right: 10, bottom: 26, left: 44 };
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const max = Math.max(...points.map((point) => point.total)) || 1;

  const x = (index: number) =>
    pad.left +
    (points.length === 1
      ? plotWidth / 2
      : (index / (points.length - 1)) * plotWidth);
  const y = scaleLinear({
    domain: [0, max],
    range: [pad.top + plotHeight, pad.top],
  });

  const totalColor = seriesColor('transitive', theme);
  const directColor = seriesColor('direct', theme);

  // A single observation is a snapshot, not a trend. An area running
  // from the origin to one point draws a ramp that reads as "grew from
  // zero", which is a claim one measurement cannot support.
  const trend = points.length > 1;
  const line = points.map((point, i) => `${x(i)},${y(point.total)}`).join(' ');

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

        {trend ? (
          <>
            <polygon
              points={`${pad.left},${pad.top + plotHeight} ${line} ${x(points.length - 1)},${pad.top + plotHeight}`}
              fill={totalColor}
              opacity={0.14}
            />
            <polyline
              points={line}
              fill="none"
              stroke={totalColor}
              strokeWidth={2}
              strokeLinejoin="round"
            />
            <polyline
              points={points
                .map((point, i) => `${x(i)},${y(point.direct)}`)
                .join(' ')}
              fill="none"
              stroke={directColor}
              strokeWidth={2}
              strokeLinejoin="round"
            />
          </>
        ) : null}

        {points.map((point, index) => (
          <g key={point.label}>
            {/* Markers double as hit targets; a 2px surface ring keeps
                overlapping points readable. */}
            {(
              [
                [point.total, totalColor],
                [point.direct, directColor],
              ] as const
            ).map(([value, color]) => (
              <circle
                key={color}
                cx={x(index)}
                cy={y(value)}
                r={4}
                fill={color}
                stroke={theme.surface}
                strokeWidth={2}
                {...bind({
                  title: point.label,
                  lines: [
                    `total ${point.total.toLocaleString()}`,
                    `direct ${point.direct.toLocaleString()}`,
                  ],
                })}
              />
            ))}
            {points.length <= 12 ||
            index % Math.ceil(points.length / 8) === 0 ? (
              <text
                x={x(index)}
                y={height - pad.bottom + 14}
                textAnchor="middle"
                fill={theme.inkMuted}
                fontSize={9}
                fontFamily="var(--f-mono)"
              >
                {point.label}
              </text>
            ) : null}
          </g>
        ))}
      </ChartFrame>

      {!trend ? (
        <ChartNote>
          Only one observation so far, which is a snapshot rather than a
          trend. A second collection run gives this a direction.
        </ChartNote>
      ) : null}

      <Legend
        entries={[
          { swatch: totalColor, label: 'all dependants' },
          { swatch: directColor, label: 'declared it' },
        ]}
      />
      {tooltip}
    </>
  );
}
