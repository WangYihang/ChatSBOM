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

import {
  ChartFrame,
  ChartTable,
  Choice,
  Empty,
  Legend,
  type TooltipContent,
  useChartTheme,
  useChartTooltip,
} from './Frame';
import { ADVANCE, barPath, CAP, clipLabel, ROW } from './geometry';
import { formatNumber } from '../i18n/format';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { rampColor, seriesColor } from '../palette';

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
  /**
   * Where choosing the row goes, when it goes somewhere: the row is a
   * link to it (`Choice`). A row with `onSelect` and no address changes
   * the panel instead, and is a button.
   */
  href?: string;
}

export function RankedBars({
  bars,
  label,
  partLabel,
  valueFormat,
  width = ROW.width,
  words,
  locale,
}: {
  bars: readonly RankedBar[];
  label: string;
  partLabel?: string;
  /** How a value is written when it is not a count: a share, say. */
  valueFormat?: (value: number) => string;
  /** Measured panel width. Only the plot grows; the gutters are fixed. */
  width?: number;
  words: Dictionary;
  locale: Locale;
}) {
  const theme = useChartTheme();
  const { bind, focus, tooltip } = useChartTooltip();
  const format = valueFormat ?? ((value: number) => formatNumber(value, locale));
  const partName = partLabel ?? words.chartPart;

  if (bars.length === 0) return <Empty message={words.noDataForSelection} />;

  const plotWidth = Math.max(width - ROW.labelWidth - ROW.valueWidth, 40);
  const height = bars.length * ROW.height + ROW.top;
  const max = Math.max(...bars.map((bar) => bar.value)) || 1;
  const scale = scaleLinear({ domain: [0, max], range: [0, plotWidth] });
  const anyPart = bars.some((bar) => bar.part !== undefined);
  const anyDetail = bars.some((bar) => bar.detail !== undefined);

  return (
    <>
      <ChartFrame
        width={width}
        height={height}
        label={label}
        interactive={bars.some((bar) => bar.onSelect)}
      >
        {bars.map((bar, index) => {
          const y = index * ROW.height + ROW.top;
          const barWidth = Math.max(scale(bar.value), CAP);
          const hasPart = bar.part !== undefined;

          const content: TooltipContent =
            bar.detail ?? {
              title: bar.label,
              lines: [
                format(bar.value),
                ...(hasPart ? [`${partName}: ${format(bar.part!)}`] : []),
              ],
            };

          // The pointer's handlers on the marks; choosing the row is its
          // `Choice`'s, so a click on a track and its fill is one choice.
          const marks = {
            ...bind(content),
            ...(bar.onSelect ? { cursor: 'pointer' as const } : {}),
          };

          const row = (
            <>
              <text
                x={ROW.labelWidth - 10}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.ink}
                fontSize={11.5}
              >
                {/* Trimmed to the gutter. Right-anchored text that
                    outgrows its gutter runs off the left edge of the
                    SVG and is cut at the start, which turns a scoped
                    npm name into a different, non-existent one — this
                    panel drew `@react-native-community/cli-server-api`
                    as `t-native-community/cli-server-api`. The full
                    name is in the tooltip. */}
                {clipLabel(bar.label, ROW.labelWidth - 10, ADVANCE.sans115)}
              </text>

              <path
                d={barPath(ROW.labelWidth, y, barWidth, ROW.bar, true)}
                fill={hasPart ? theme.track : rampColor(bar.value / max, theme)}
                {...marks}
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
                  ? `${format(bar.part!)} / ${format(bar.value)}`
                  : format(bar.value)}
              </text>
            </>
          );

          return (
            <g key={bar.label} data-row={bar.label}>
              {bar.onSelect ? (
                <Choice
                  href={bar.href}
                  name={bar.label}
                  onSelect={bar.onSelect}
                  tip={focus(content)}
                >
                  {row}
                </Choice>
              ) : (
                row
              )}
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
            { swatch: seriesColor('direct', theme), label: partName },
          ]}
        />
      ) : null}
      <ChartTable
        caption={label}
        columns={[
          label,
          ...(anyPart ? [partName] : []),
          ...(anyDetail ? [words.chartDetails] : []),
        ]}
        rows={bars.map((bar) => ({
          name: bar.label,
          cells: [
            format(bar.value),
            ...(anyPart ? [bar.part === undefined ? '' : format(bar.part)] : []),
            ...(anyDetail ? [bar.detail?.lines ?? []] : []),
          ],
        }))}
      />
      {tooltip}
    </>
  );
}
