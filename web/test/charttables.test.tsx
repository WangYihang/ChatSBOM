/**
 * Each chart's numbers, for a reader who cannot see the chart (#43).
 *
 * The SVG was all there was, `role="img"` with a name: a screen reader
 * announced the panel's picture and nothing it measured, since an image
 * has no parts to read. Each chart now carries its numbers as a table,
 * clipped out of sight and there for assistive technology, and says in
 * it what its tooltips add. These find each one the way a screen reader
 * would: by role and by name.
 */
// @vitest-environment jsdom
import { cleanup, render, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { DependencyTree } from '../src/charts/DependencyTree';
import { Histogram, StackedShare, TimeSeries } from '../src/charts/Plots';
import { RankedBars } from '../src/charts/RankedBars';
import { SourceShares } from '../src/charts/SourceShares';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

beforeEach(() => cleanup());

/**
 * The table named `name`, as rows of cell text, headers first: each cell
 * found by the role a screen reader is given for it.
 */
function table(name: string): string[][] {
  const found = screen.getByRole('table', { name });
  return within(found)
    .getAllByRole('row')
    .map((row) =>
      ['columnheader', 'rowheader', 'cell']
        .flatMap((role) => within(row).queryAllByRole(role))
        .sort((a, b) => (a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1))
        .map((cell) => cell.textContent ?? ''),
    );
}

describe('a ranking', () => {
  it('gives its rows and their values as a table', () => {
    render(
      <RankedBars
        words={EN}
        locale="en"
        label="repositories"
        bars={[
          { label: 'python', value: 9_102 },
          { label: 'ruby', value: 1_400 },
        ]}
      />,
    );
    const found = screen.getByRole('table', { name: 'repositories' });
    expect(within(found).getByRole('rowheader', { name: 'python' })).toBeTruthy();
    expect(within(found).getByRole('cell', { name: '9,102' })).toBeTruthy();
    expect(within(found).getByRole('columnheader', { name: 'repositories' })).toBeTruthy();
    // Out of sight: the bars are what is drawn. The clip is a block
    // around the table, not the table: a table grows to fit its rows
    // whatever width and height say, and overflow does not apply to one,
    // so clipped as itself it was hidden but its box still reached 270px
    // past a phone's edge and scrolled the whole page sideways.
    const clip = found.parentElement;
    expect(clip?.tagName).toBe('DIV');
    expect(clip?.classList.contains('chart-data')).toBe(true);
    expect(found.classList.contains('chart-data')).toBe(false);
  });

  it('adds each row’s part, and what its tooltip says', () => {
    render(
      <RankedBars
        words={EN}
        locale="en"
        label="repositories"
        partLabel="with dependency data"
        bars={[
          {
            label: 'rust',
            value: 800,
            part: 700,
            detail: { title: 'rust', lines: ['800 repositories', '600 with a Syft scan'] },
          },
        ]}
      />,
    );
    expect(table('repositories')).toEqual([
      ['', 'repositories', 'with dependency data', EN.chartDetails],
      ['rust', '800', '700', '800 repositories600 with a Syft scan'],
    ]);
  });
});

describe('the sources panel', () => {
  it('gives each ecosystem’s rows by collector, with the share and the total', () => {
    render(
      <SourceShares
        words={EN}
        locale="en"
        label="by collector"
        rows={[{ label: 'java', syft: 9_648, depgraph: 47_329, manifest: 0 }]}
      />,
    );
    expect(table('by collector')).toEqual([
      [
        EN.tableEcosystem,
        EN.sourceNames.syft,
        EN.sourceNames.depgraph,
        EN.sourceNames.manifest,
        EN.sourcesLabel,
      ],
      [
        'java',
        `9,648${EN.sourceShare('16.9')}`,
        `47,329${EN.sourceShare('83.1')}`,
        `0${EN.sourceShare('0.0')}`,
        '56,977',
      ],
    ]);
  });
});

describe('the share bar', () => {
  it('gives each part’s count and share', () => {
    render(
      <StackedShare
        words={EN}
        locale="en"
        label="how dependencies arrived"
        valueLabel="dependency records"
        slices={[
          { series: 'direct', label: 'declared', value: 463_150 },
          { series: 'transitive', label: 'inherited', value: 5_590_319 },
        ]}
      />,
    );
    expect(table('how dependencies arrived')).toEqual([
      ['', 'dependency records', EN.chartShare],
      ['declared', '463,150', '7.7%'],
      ['inherited', '5,590,319', '92.3%'],
    ]);
  });
});

describe('the histogram', () => {
  it('gives each bucket’s count, under the axis’s name', () => {
    render(
      <Histogram
        words={EN}
        locale="en"
        label="repositories by dependency count"
        xLabel="dependencies"
        valueLabel="repositories"
        buckets={[
          { label: '1-9', value: 4_228 },
          { label: '10-24', value: 1_589 },
        ]}
      />,
    );
    expect(table('repositories by dependency count')).toEqual([
      ['dependencies', 'repositories'],
      ['1-9', '4,228'],
      ['10-24', '1,589'],
    ]);
  });
});

describe('the time series', () => {
  it('gives every observation, by source and month', () => {
    render(
      <TimeSeries
        words={EN}
        locale="en"
        label="adoption of mail"
        snapshotNote={EN.adoptionSnapshot}
        series={[
          { source: 'syft', points: [{ label: '2026-02', total: 1_124, direct: 30 }] },
          { source: 'github-depgraph', points: [{ label: '2026-09', total: 149, direct: 149 }] },
        ]}
      />,
    );
    const { source, month, repositories, declared } = EN.adoptionColumns;
    expect(table('adoption of mail')).toEqual([
      [source, month, repositories, declared],
      ['syft', '2026-02', '1,124', '30'],
      ['github-depgraph', '2026-09', '149', '149'],
    ]);
  });
});

describe('the tree', () => {
  it('gives every edge: the package, what pulls it in, in how many repositories', () => {
    render(
      <DependencyTree
        words={EN}
        locale="en"
        width={720}
        tree={{
          root: 'body-parser',
          children: [{ name: 'debug', repositories: 3_580 }],
          grandchildren: [{ parent: 'debug', child: 'ms', repositories: 7_999 }],
        }}
      />,
    );
    const { package: name, parent, repositories } = EN.pullsInColumns;
    expect(table(EN.pullsInLabel('body-parser'))).toEqual([
      [name, parent, repositories],
      ['debug', 'body-parser', '3,580'],
      ['ms', 'debug', '7,999'],
    ]);
  });
});

describe('in Chinese', () => {
  it('heads the tables in Chinese too', () => {
    render(
      <DependencyTree
        words={ZH}
        locale="zh"
        width={720}
        tree={{
          root: 'body-parser',
          children: [{ name: 'debug', repositories: 3_580 }],
          grandchildren: [],
        }}
      />,
    );
    const [head] = table(ZH.pullsInLabel('body-parser'));
    expect(head).toEqual([
      ZH.pullsInColumns.package,
      ZH.pullsInColumns.parent,
      ZH.pullsInColumns.repositories,
    ]);
    expect(head!.join('')).not.toMatch(/[A-Za-z]/);
  });
});
