/**
 * What every test file starts from.
 *
 * Each chart repeats its numbers in a table that only assistive
 * technology is given (`ChartTable`, #43), so a package's name is on the
 * page twice: drawn, and in the table. A text query looks for what a
 * reader sees, and finds the drawn one; the table is looked for as a
 * screen reader would, by role. Without this, `getByText('typescript')`
 * found the bar's label and the table's row both, and asked which.
 */
import { configure } from '@testing-library/dom';
import { afterEach } from 'vitest';

configure({ defaultIgnore: 'script, style, .chart-data, .chart-data *' });

/**
 * In a file with a DOM, what a test drew is taken down after it (#44).
 *
 * Testing Library does that itself only where `afterEach` is a global,
 * which here it is not, so each file had to. Eight did it before each
 * test instead, and one not at all, so the last test's page was still
 * mounted when its file ended and jsdom was torn down. A state update
 * that arrived after that failed the run on "window is not defined",
 * every test having passed, because React reads `window.event` to
 * schedule one; #43 fixed two files that never unmounted. Unmounted, a
 * page's effects are cleaned up, its requests abandoned and its timers
 * cleared, before the next test starts or the environment goes.
 */
if (typeof document !== 'undefined') {
  const { cleanup } = await import('@testing-library/react');
  afterEach(() => cleanup());
}
