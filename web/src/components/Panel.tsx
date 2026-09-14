/**
 * The page's structural pieces: a panel, and the bridge to a chart.
 *
 * `Chart` exists because the drawing code in charts.ts is imperative —
 * it appends SVG to a host element. Wrapping it rather than rewriting it
 * in one go keeps the house rules (validated palette, 2px surface gaps,
 * track-plus-fill, theme read at draw time) under test throughout the
 * migration to visx, one form at a time, instead of replacing all eight
 * at once and finding out afterwards.
 */
import { useEffect, useRef, type ReactNode } from 'react';

import { useThemeEpoch } from '../hooks';

export function Panel({
  title,
  qualifier,
  note,
  controls,
  children,
}: {
  title: string;
  qualifier?: ReactNode;
  note?: ReactNode;
  controls?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <div className="panel">
      <h2>
        {title}
        {qualifier ? <span className="qual">{qualifier}</span> : null}
      </h2>
      {note ? <p className="note">{note}</p> : null}
      {controls ? <div className="controls">{controls}</div> : null}
      {children}
    </div>
  );
}

/**
 * Draw an imperative chart into a host div.
 *
 * `draw` is re-run whenever the data changes and whenever the OS theme
 * changes, because the palette is read at draw time — a cached palette
 * would leave one panel in the other theme's colours after a switch.
 */
export function Chart({ draw }: { draw: (host: HTMLElement) => void }) {
  const host = useRef<HTMLDivElement>(null);
  const epoch = useThemeEpoch();

  useEffect(() => {
    if (host.current) draw(host.current);
  }, [draw, epoch]);

  return <div className="plot" ref={host} />;
}
