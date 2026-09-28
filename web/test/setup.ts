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

configure({ defaultIgnore: 'script, style, .chart-data, .chart-data *' });
