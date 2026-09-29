/**
 * The page's structural pieces: a panel, and the bridge to a chart.
 *
 * A panel is a hairline-separated region: an eyebrow title, an optional
 * caveat, optional controls, and whatever answers the question. The
 * bridge that used to live here — a host div for imperative drawing
 * code — is gone now that every form is a component. What stands in for
 * a chart until its question is answered is here instead (`Answered`).
 */
import type { ReactNode } from 'react';

import type { Async } from '../hooks';
import { queryFailure } from '../i18n/failure';
import type { Dictionary } from '../i18n/strings';

/**
 * A chart drawn from its question's answer, or what stands in for it
 * until there is one (#123).
 *
 * The panels drew their charts from the answer or, until there was one,
 * from nothing, and a chart with nothing to draw says "No data for this
 * selection." So a panel whose question had failed said that the
 * selection was empty: a refusal read as a finding, and nothing on the
 * page said that it had not been answered. A failure is said as the
 * page's other failures are, in the reader's language
 * (`queryFailure`), and a question still on its way says so.
 *
 * With `keep`, the last answer is drawn while the next one loads, for a
 * panel that went on showing it rather than emptying (#42).
 */
export function Answered<T>({
  state,
  keep = false,
  words,
  children,
}: {
  state: Async<T>;
  keep?: boolean;
  words: Dictionary;
  /** The chart, for the answer. */
  children: (value: T) => ReactNode;
}) {
  if (state.status === 'ready') return children(state.value);
  if (state.status === 'failed') {
    return <p className="chart-empty error">{queryFailure(state.error, words)}</p>;
  }
  if (keep && state.status === 'loading' && state.previous !== undefined) {
    return children(state.previous);
  }
  return <p className="chart-empty">{words.loadingPart}</p>;
}

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
