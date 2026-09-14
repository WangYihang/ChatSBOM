/**
 * The pieces every chart wears: an SVG frame, a legend, a tooltip, and
 * the two kinds of caption.
 *
 * The tooltip is hand-built rather than taken from @visx/tooltip. The
 * house rules say the hit target is the mark plus a 4px halo and the
 * tooltip is a fixed-position element styled by `.chart-tooltip`; wiring
 * visx's positioning in would mean overriding most of it, and the CSS
 * and its assertions already exist.
 */
import { useCallback, useState, type ReactNode } from 'react';

export function ChartFrame({
  width,
  height,
  label,
  children,
}: {
  width: number;
  height: number;
  label: string;
  children: ReactNode;
}) {
  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={label}
      preserveAspectRatio="xMinYMin meet"
    >
      {children}
    </svg>
  );
}

/**
 * Identity is never colour alone: a legend is present for two or more
 * series, and the mark colour sits beside a word.
 */
export function Legend({
  entries,
}: {
  entries: { swatch: string; label: string }[];
}) {
  return (
    <div className="chart-legend">
      {entries.map((entry) => (
        <span key={entry.label}>
          <i style={{ background: entry.swatch }} />
          {entry.label}
        </span>
      ))}
    </div>
  );
}

/** Why a panel is blank, rather than a blank panel. */
export function Empty({ message = 'No data for this selection.' }) {
  return <p className="chart-empty">{message}</p>;
}

/** A caveat about what the marks above can and cannot show. */
export function ChartNote({ children }: { children: ReactNode }) {
  return <p className="chart-note">{children}</p>;
}

/**
 * Tooltip content, structured rather than markup.
 *
 * The imperative version took an HTML string and assigned it to
 * `innerHTML`, and the strings were assembled from dataset values —
 * package names among them. Anyone can publish a package, so that is
 * untrusted input reaching an HTML sink. Structured content has no sink:
 * React escapes every field, and a name containing markup renders as
 * that name.
 */
export interface TooltipContent {
  title: string;
  lines: string[];
}

export interface TooltipState {
  content: TooltipContent;
  x: number;
  y: number;
}

/**
 * A chart that cannot be interrogated is a picture of data rather than a
 * view of it, so every form with a plot gets this.
 */
export function useChartTooltip() {
  const [tip, setTip] = useState<TooltipState | null>(null);

  const bind = useCallback(
    (content: TooltipContent) => ({
      onMouseEnter: (event: React.MouseEvent) =>
        setTip({ content, x: event.clientX, y: event.clientY }),
      onMouseMove: (event: React.MouseEvent) =>
        setTip({ content, x: event.clientX, y: event.clientY }),
      onMouseLeave: () => setTip(null),
    }),
    [],
  );

  const tooltip = tip ? (
    <div
      className="chart-tooltip"
      style={{ left: tip.x + 12, top: tip.y + 12 }}
    >
      <strong>{tip.content.title}</strong>
      {tip.content.lines.map((line) => (
        <div key={line}>{line}</div>
      ))}
    </div>
  ) : null;

  return { bind, tooltip };
}
