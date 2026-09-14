/**
 * A ranking: one row per item, sorted, with the value beside the bar.
 *
 * visx contributes the scale and nothing else here, which is honest —
 * for a ranking the scale *is* the maths. It earns more on the forms
 * that carry an axis, where its tick algorithm replaces the one this
 * project had hand-rolled.
 *
 * Two encodings in one component:
 *
 *   - a plain magnitude, where the bar takes a step of the validated
 *     sequential ramp;
 *   - a proportion, where the total is a track and the part fills it.
 *
 * The proportion used to be drawn *inside* the bar, inset by a fixed
 * number of pixels. That needs a height budget, and the budget ran out
 * when rows tightened: at a 9px bar the inset mark was 1px and rendered
 * as a glitch line. Full-height marks have no budget to run out of.
 */
import { scaleLinear } from '@visx/scale';

import { ChartFrame, Empty, Legend, useChartTheme, useChartTooltip } from './Frame';
import { barPath, CAP, ROW } from './geometry';
import { rampColor, seriesColor } from '../palette';
import type { TooltipContent } from './tooltip';

export interface RankedBar {
  label: string;
  value: number;
  /** Drawn as a fill over the value's track, for a proportion. */
  part?: number;
  /** Replaces the default tooltip body. Structured, never markup. */
  detail?: TooltipContent;
  /**
   * What to do when this row is chosen.
   *
   * Bound to the row's own marks. The imperative version matched
   * handlers to marks by walking the DOM and pairing element N with
   * datum N, which held only while every row drew exactly one path —
   * and stopped holding the moment a row drew a track and a fill.
   */
  onSelect?: () => void;
}

export function RankedBars({
  bars,
  label,
  partLabel,
  valueFormat = (value: number) => value.toLocaleString(),
  width = ROW.width,
}: {
  bars: readonly RankedBar[];
  label: string;
  partLabel?: string;
  valueFormat?: (value: number) => string;
  /** Measured panel width. Only the plot grows; the gutters are fixed. */
  width?: number;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (bars.length === 0) return <Empty />;

  const plotWidth = Math.max(width - ROW.labelWidth - ROW.valueWidth, 40);
  const height = bars.length * ROW.height + ROW.top;
  const max = Math.max(...bars.map((bar) => bar.value)) || 1;
  const scale = scaleLinear({ domain: [0, max], range: [0, plotWidth] });
  const anyPart = bars.some((bar) => bar.part !== undefined);

  return (
    <>
      <ChartFrame width={width} height={height} label={label}>
        {bars.map((bar, index) => {
          const y = index * ROW.height + ROW.top;
          const barWidth = Math.max(scale(bar.value), CAP);
          const hasPart = bar.part !== undefined;

          const content: TooltipContent =
            bar.detail ?? {
              title: bar.label,
              lines: [
                valueFormat(bar.value),
                ...(hasPart
                  ? [`${partLabel ?? 'part'}: ${valueFormat(bar.part!)}`]
                  : []),
              ],
            };

          const marks = bind(content);
          const select = bar.onSelect
            ? { onClick: bar.onSelect, cursor: 'pointer' as const }
            : {};

          return (
            <g key={bar.label} data-row={bar.label}>
              <text
                x={ROW.labelWidth - 10}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.ink}
                fontSize={11.5}
              >
                {bar.label}
              </text>

              <path
                d={barPath(ROW.labelWidth, y, barWidth, ROW.bar, true)}
                fill={hasPart ? theme.track : rampColor(bar.value / max, theme)}
                {...marks}
                {...select}
              />

              {hasPart ? (
                <path
                  d={barPath(
                    ROW.labelWidth,
                    y,
                    Math.min(Math.max(scale(bar.part!), CAP), barWidth),
                    ROW.bar,
                    true,
                  )}
                  fill={seriesColor('direct', theme)}
                  {...marks}
                  {...select}
                />
              ) : null}

              <text
                x={width - 8}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.inkMuted}
                fontSize={11}
                fontFamily="var(--f-mono)"
              >
                {hasPart
                  ? `${valueFormat(bar.part!)} / ${valueFormat(bar.value)}`
                  : valueFormat(bar.value)}
              </text>
            </g>
          );
        })}
      </ChartFrame>

      {/* A legend for two or more series, so identity is never colour
          alone. A single series needs none — the panel title names it. */}
      {anyPart ? (
        <Legend
          entries={[
            { swatch: theme.track, label },
            { swatch: seriesColor('direct', theme), label: partLabel ?? 'part' },
          ]}
        />
      ) : null}
      {tooltip}
    </>
  );
}
