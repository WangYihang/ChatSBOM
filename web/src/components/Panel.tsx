/**
 * The page's structural pieces: a panel, and the bridge to a chart.
 *
 * A panel is a hairline-separated region: an eyebrow title, an optional
 * caveat, optional controls, and whatever answers the question. The
 * bridge that used to live here — a host div for imperative drawing
 * code — is gone now that every form is a component.
 */
import type { ReactNode } from 'react';

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
