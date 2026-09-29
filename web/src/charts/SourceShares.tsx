/**
 * Which collector covered each ecosystem.
 *
 * Keyed by the package's ecosystem since #55 §4.12, not the
 * repository's language: a Maven backend under a TypeScript label is
 * Maven's. Three collectors: Syft reads lockfiles, GitHub's graph reads
 * manifests, and the `manifest` source is what Gradle build files
 * declare, which neither of the others reads.
 *
 * The previous form drew both collectors on one shared linear scale, and
 * the row counts span four orders of magnitude — TypeScript 3,549,474
 * rows next to Ruby 30,879. On a shared scale every language but the
 * largest was an invisible sliver, and the dependency-graph series was
 * invisible everywhere, so the panel could not answer the question its
 * own caption posed.
 *
 * Each row is normalised to its own total instead, with the absolute
 * total labelled at the right so the magnitude stays on the page. That
 * makes the actual finding legible: Java is 83% dependency graph, 47,329
 * rows against syft's 9,648, which the old encoding hid completely.
 *
 * A log scale was the other candidate and is worse here: it makes "5x
 * more" and "50x more" look similar, and the question is about shares,
 * not orders of magnitude.
 */
import { scaleLinear } from '@visx/scale';

import { barPath, SPACER } from './geometry';
import {
  ChartFrame,
  ChartTable,
  Empty,
  Legend,
  Mark,
  type TooltipContent,
  useChartTheme,
  useChartTooltip,
} from './Frame';
import { formatNumber } from '../i18n/format';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { seriesColor } from '../palette';

export interface SourceRow {
  /** The ecosystem the row is about. */
  label: string;
  syft: number;
  depgraph: number;
  manifest: number;
}

const ROW = { height: 20, bar: 10, labelWidth: 110, totalWidth: 92, width: 720, top: 6 };

export function SourceShares({
  rows,
  label,
  width = ROW.width,
  words,
  locale,
}: {
  rows: readonly SourceRow[];
  /** The chart's accessible name, so it is not English-only. */
  label: string;
  /** Measured panel width. Only the plot grows; the gutters are fixed. */
  width?: number;
  words: Dictionary;
  locale: Locale;
}) {
  const theme = useChartTheme();
  const { bind, focus, tooltip } = useChartTooltip();

  if (rows.length === 0) return <Empty message={words.noDataForSelection} />;

  const plotWidth = Math.max(width - ROW.labelWidth - ROW.totalWidth, 40);
  const height = rows.length * ROW.height + ROW.top;

  // A collector that contributed nothing is omitted rather than drawn as
  // a zero-width mark — a 1px sliver at the origin reads as "a little",
  // which is the opposite of the truth.
  const drawn = rows.map((row) => ({
    row,
    parts: [
      { key: 'syft' as const, value: row.syft, label: words.sourceNames.syft },
      {
        key: 'github-depgraph' as const,
        value: row.depgraph,
        label: words.sourceNames.depgraph,
      },
      {
        key: 'manifest' as const,
        value: row.manifest,
        label: words.sourceNames.manifest,
      },
    ].filter((part) => part.value > 0),
  }));
  // Each part is read from the keyboard (`Mark`), a row at a time.
  const marks = drawn.reduce((sum, { parts }) => sum + parts.length, 0);
  let read = 0;

  return (
    <>
      <ChartFrame
        width={width}
        height={height}
        label={label}
        marks={marks}
      >
        {drawn.map(({ row, parts }, index) => {
          const total = row.syft + row.depgraph + row.manifest;
          const y = index * ROW.height + ROW.top;

          // Each row gets its own scale, which is the whole point: the
          // comparison is within an ecosystem, not across them.
          const share = scaleLinear({
            domain: [0, total || 1],
            range: [0, plotWidth],
          });

          let x = ROW.labelWidth;

          return (
            <g key={row.label} data-row={row.label}>
              <text
                x={ROW.labelWidth - 10}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.ink}
                fontSize={11.5}
              >
                {row.label}
              </text>

              {parts.map((part, position) => {
                const raw = share(part.value);
                const last = position === parts.length - 1;
                const width = Math.max(raw - (last ? 0 : SPACER), 1);
                const left = x;
                x += raw;
                const content: TooltipContent = {
                  title: row.label,
                  lines: [
                    words.sourceRows(part.label, formatNumber(part.value, locale)),
                    words.sourceShare(((part.value / total) * 100).toFixed(1)),
                  ],
                };
                return (
                  <Mark key={part.key} index={read++} content={content} focus={focus} words={words}>
                    <path
                      d={barPath(left, y, width, ROW.bar, true)}
                      fill={seriesColor(part.key, theme)}
                      {...bind(content)}
                    />
                  </Mark>
                );
              })}

              <text
                x={width - 8}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.inkMuted}
                fontSize={11}
                fontFamily="var(--f-mono)"
              >
                {formatNumber(total, locale)}
              </text>
            </g>
          );
        })}
      </ChartFrame>

      <Legend
        entries={[
          { swatch: seriesColor('syft', theme), label: words.sourceLegend.syft },
          {
            swatch: seriesColor('github-depgraph', theme),
            label: words.sourceLegend.depgraph,
          },
          {
            swatch: seriesColor('manifest', theme),
            label: words.sourceLegend.manifest,
          },
        ]}
      />
      <ChartTable
        caption={label}
        head={words.tableEcosystem}
        columns={[
          words.sourceNames.syft,
          words.sourceNames.depgraph,
          words.sourceNames.manifest,
          words.sourcesLabel,
        ]}
        rows={rows.map((row) => {
          const total = row.syft + row.depgraph + row.manifest;
          // A collector's rows and its share of the ecosystem's, as its
          // tooltip gives them; the total as the chart prints it.
          const cell = (value: number) => [
            formatNumber(value, locale),
            words.sourceShare(((value / (total || 1)) * 100).toFixed(1)),
          ];
          return {
            name: row.label,
            cells: [
              cell(row.syft),
              cell(row.depgraph),
              cell(row.manifest),
              formatNumber(total, locale),
            ],
          };
        })}
      />
      {tooltip}
    </>
  );
}
