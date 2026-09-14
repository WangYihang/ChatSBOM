/**
 * Which collector covered each language.
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
import { ChartFrame, Empty, Legend, useChartTheme, useChartTooltip } from './Frame';
import { seriesColor } from '../palette';

export interface SourceRow {
  language: string;
  syft: number;
  depgraph: number;
}

const ROW = { height: 20, bar: 10, labelWidth: 110, totalWidth: 92, width: 720, top: 6 };

export function SourceShares({
  rows,
  width = ROW.width,
}: {
  rows: readonly SourceRow[];
  /** Measured panel width. Only the plot grows; the gutters are fixed. */
  width?: number;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (rows.length === 0) return <Empty />;

  const plotWidth = Math.max(width - ROW.labelWidth - ROW.totalWidth, 40);
  const height = rows.length * ROW.height + ROW.top;

  return (
    <>
      <ChartFrame
        width={width}
        height={height}
        label="Share of dependency records per language, by collector"
      >
        {rows.map((row, index) => {
          const total = row.syft + row.depgraph;
          const y = index * ROW.height + ROW.top;

          // Each row gets its own scale, which is the whole point: the
          // comparison is within a language, not across them.
          const share = scaleLinear({
            domain: [0, total || 1],
            range: [0, plotWidth],
          });

          // A collector that contributed nothing is omitted rather than
          // drawn as a zero-width mark — a 1px sliver at the origin
          // reads as "a little", which is the opposite of the truth.
          const parts = [
            { key: 'syft' as const, value: row.syft, label: 'Syft' },
            { key: 'github-depgraph' as const, value: row.depgraph, label: 'Dependency graph' },
          ].filter((part) => part.value > 0);

          let x = ROW.labelWidth;

          return (
            <g key={row.language} data-row={row.language}>
              <text
                x={ROW.labelWidth - 10}
                y={y + ROW.bar / 2}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.ink}
                fontSize={11.5}
              >
                {row.language}
              </text>

              {parts.map((part, position) => {
                const raw = share(part.value);
                const last = position === parts.length - 1;
                const width = Math.max(raw - (last ? 0 : SPACER), 1);
                const left = x;
                x += raw;
                return (
                  <path
                    key={part.key}
                    d={barPath(left, y, width, ROW.bar, true)}
                    fill={seriesColor(part.key, theme)}
                    {...bind({
                      title: row.language,
                      lines: [
                        `${part.label}: ${part.value.toLocaleString()} rows`,
                        `${((part.value / total) * 100).toFixed(1)}% of this language`,
                      ],
                    })}
                  />
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
                {total.toLocaleString()}
              </text>
            </g>
          );
        })}
      </ChartFrame>

      <Legend
        entries={[
          { swatch: seriesColor('syft', theme), label: 'Syft · lockfiles' },
          {
            swatch: seriesColor('github-depgraph', theme),
            label: 'Dependency graph · manifests',
          },
        ]}
      />
      {tooltip}
    </>
  );
}
