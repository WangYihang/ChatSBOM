/**
 * The pieces every chart wears: an SVG frame, a legend, a tooltip, the
 * two kinds of caption, a way in from the keyboard, and its numbers as a
 * table for a reader who cannot see it.
 *
 * The tooltip is hand-built rather than taken from @visx/tooltip. The
 * house rules say the hit target is the mark plus a 4px halo and the
 * tooltip is a fixed-position element styled by `.chart-tooltip`; wiring
 * visx's positioning in would mean overriding most of it, and the CSS
 * and its assertions already exist.
 */
import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type FocusEvent,
  type KeyboardEvent,
  type MouseEvent,
  type ReactNode,
} from 'react';

import { chartTheme, type ChartTheme } from '../palette';

export function ChartFrame({
  width,
  height,
  label,
  interactive = false,
  children,
}: {
  width: number;
  height: number;
  label: string;
  /**
   * Whether marks in it can be chosen (`Choice`).
   *
   * Such a chart is a named group, not an `img`: an image's children
   * are presentational, so the links in one were not there for a
   * screen reader to reach (#43). A chart with nothing to choose stays
   * a picture, its numbers in the table beside it.
   */
  interactive?: boolean;
  children: ReactNode;
}) {
  return (
    // Explicit pixel dimensions as well as a viewBox. The measured
    // width *is* the render width, so there is nothing to scale — and a
    // chart whose size depends on a stylesheet rule matching is a chart
    // that silently overflows its panel the day the markup changes.
    <svg
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role={interactive ? 'group' : 'img'}
      aria-label={label}
      preserveAspectRatio="xMinYMin meet"
    >
      {children}
    </svg>
  );
}

/** The handlers that show a tooltip for as long as a mark has focus. */
export interface FocusTip {
  onFocus(event: FocusEvent<Element>): void;
  onBlur(): void;
  onKeyDown(event: KeyboardEvent<Element>): void;
}

/**
 * A mark that opens something, reachable without a mouse (#43).
 *
 * A bar and a tree's package took `onClick` on a `<path>` or a
 * `<circle>`: no tab stop and no key, so a keyboard reached none of
 * them. A link where choosing it goes somewhere, so it is announced as
 * one and opens in a tab of its own like one; a button where it changes
 * the panel instead. Enter chooses either and Space a button, and
 * focusing it shows what pointing at it shows.
 *
 * It wraps the mark and its label both, so either is a target for the
 * pointer too, and is named by `name` — the full name, which the drawn
 * label may have trimmed.
 */
export function Choice({
  href,
  name,
  onSelect,
  tip,
  children,
}: {
  /** Where choosing it goes. Without one it is a button. */
  href?: string | undefined;
  name: string;
  onSelect: () => void;
  tip: FocusTip;
  children: ReactNode;
}) {
  const onKeyDown = (event: KeyboardEvent<Element>) => {
    tip.onKeyDown(event);
    // With a modifier held, the browser's own meaning stands.
    if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    // Handled here for both kinds rather than left to the browser: a
    // `role="button"` has no key of its own, and one path for both is
    // one path to test.
    if (event.key === 'Enter' || (!href && event.key === ' ')) {
      event.preventDefault();
      onSelect();
    }
  };

  if (href) {
    const onClick = (event: MouseEvent<Element>) => {
      // A click that asks for another tab or window gets one: the
      // address is real. Any other goes through the page's own router.
      if (event.button !== 0 || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) {
        return;
      }
      event.preventDefault();
      onSelect();
    };
    return (
      <a
        href={href}
        aria-label={name}
        onClick={onClick}
        onKeyDown={onKeyDown}
        onFocus={tip.onFocus}
        onBlur={tip.onBlur}
      >
        {children}
      </a>
    );
  }
  return (
    <g
      role="button"
      tabIndex={0}
      aria-label={name}
      onClick={onSelect}
      onKeyDown={onKeyDown}
      onFocus={tip.onFocus}
      onBlur={tip.onBlur}
    >
      {children}
    </g>
  );
}

/**
 * A chart's numbers, as a table only assistive technology is given (#43).
 *
 * The SVG was all there was: a screen reader announced the chart by its
 * name and read nothing it measured, since an image has no parts, and
 * what the tooltips add was for a pointer only. The table says what the
 * marks say and what the tooltips add, in rows a screen reader can walk.
 * `.chart-data` clips it out of sight: the marks are what is drawn.
 *
 * The clip is a block around the table rather than the table itself. A
 * table grows to fit its rows whatever its width and height say, and
 * overflow does not apply to one: clipped as itself, it was hidden, but
 * its box still reached past a phone's edge and scrolled the page
 * sideways, and added a blank strip under the last panel.
 *
 * A cell with several lines — a tooltip's — gives them one under
 * another.
 */
export function ChartTable({
  caption,
  head,
  columns,
  rows,
}: {
  caption: string;
  /** What the first column names, where a word helps. */
  head?: string | undefined;
  columns: readonly string[];
  rows: readonly { name: string; cells: readonly (string | readonly string[])[] }[];
}) {
  return (
    <div className="chart-data">
      <table>
        <caption>{caption}</caption>
        <thead>
          <tr>
            {head ? <th scope="col">{head}</th> : <td />}
            {columns.map((column) => (
              <th key={column} scope="col">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={`${index}:${row.name}`}>
              <th scope="row">{row.name}</th>
              {row.cells.map((cell, column) => (
                <td key={column}>
                  {typeof cell === 'string'
                    ? cell
                    : cell.map((line) => <div key={line}>{line}</div>)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
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

/**
 * Why a panel is blank, rather than a blank panel.
 *
 * The words are the caller's. This had an English default, which four
 * of the six charts used, so a Chinese page said "No data for this
 * selection." under a Chinese heading while the dictionary held the
 * translation (#43).
 */
export function Empty({ message }: { message: string }) {
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
 *
 * `bind` shows a mark's tooltip under the pointer, and `focus` while a
 * mark that can be chosen has focus — it opened for a mouse only
 * (#43) — placed at the mark rather than at a pointer there may not be.
 * Escape puts a focused one away without moving focus, as content that
 * focus shows must let a reader do.
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

  const focus = useCallback(
    (content: TooltipContent): FocusTip => ({
      onFocus: (event) => {
        const mark = event.currentTarget.getBoundingClientRect();
        setTip({ content, x: mark.left, y: mark.bottom });
      },
      onBlur: () => setTip(null),
      onKeyDown: (event) => {
        if (event.key === 'Escape') setTip(null);
      },
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

  return { bind, focus, tooltip };
}

/**
 * The palette the page is actually rendering in, kept current.
 *
 * Reading `chartTheme()` at render time is only half of what a theme
 * switch needs: nothing in the DOM changes when the OS flips, so React
 * is never told to render again and the marks stay in the old palette
 * until something else happens to update them. Subscribing here is the
 * other half.
 *
 * `matchMedia` is absent in some environments, so its absence must mean
 * "no subscription" rather than a throw — a chart that throws while
 * drawing leaves a blank panel instead of a degraded one.
 */
export function useChartTheme(): ChartTheme {
  const [, bump] = useState(0);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const onChange = () => bump((n) => n + 1);
    media.addEventListener('change', onChange);
    return () => media.removeEventListener('change', onChange);
  }, []);

  return chartTheme();
}

/**
 * Lay a chart out in measured pixels rather than a stretched viewBox.
 *
 * A fixed viewBox scaled to its container scales the type with it: the
 * full-width panel drew its labels at 2.5x while the narrow rail drew
 * the same labels at 1.4x, so the page carried two type scales and
 * neither matched the stylesheet. Measuring the panel and laying out in
 * real pixels keeps 11.5px meaning 11.5px everywhere.
 *
 * Measured here rather than with @visx/responsive's ParentSize, after
 * trying it: ParentSize puts its children in an inner
 * `position: absolute; inset: 0` div, which takes them out of flow, so
 * the wrapper is always zero-height. That is correct for its purpose —
 * filling a container that has a definite size — and wrong for these
 * panels, whose height comes *from* the chart. Overriding
 * `parentSizeStyles` does not reach the inner div. Observing a plain
 * block element keeps the chart in flow and the panel sized by it.
 */
export function Measured({
  fallbackWidth = 720,
  children,
}: {
  /** Used until the first measurement, and where measuring is impossible. */
  fallbackWidth?: number;
  children: (width: number) => ReactNode;
}) {
  const host = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const node = host.current;
    if (!node) return;

    const measure = () => {
      // Content width, so panel padding is never counted as plot space.
      const next = Math.round(node.clientWidth);
      setWidth((current) => (current === next ? current : next));
    };
    measure();

    // ResizeObserver catches a panel changing width without the window
    // doing so — a rail reflowing, a filter appearing. Its absence is a
    // reason to fall back, never to throw: a page that cannot measure
    // looks slightly off, a page that throws is blank.
    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', measure);
      return () => window.removeEventListener('resize', measure);
    }
    const observer = new ResizeObserver(measure);
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  return (
    <div className="plot" ref={host}>
      {children(width > 0 ? width : fallbackWidth)}
    </div>
  );
}
