/**
 * The layered tree.
 *
 * The reason this form was chosen over a force-directed layout is an
 * encoding argument, and the assertions below are that argument made
 * checkable: a column must mean a hop, and stroke width must mean the
 * repository count. A force layout fails the second by construction —
 * position there comes from topology, so the number that matters
 * carries no position, and `debug → ms` at 7,999 repositories draws
 * much like `body-parser → express` at one.
 *
 * The rest are layout invariants that fail silently: two parents on the
 * same y, a parent centred on the frame rather than on its children, a
 * long name drawn across the column beside it.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DependencyTree } from '../src/charts/DependencyTree';
import type { DependencyTree as Tree } from '../src/d1/queries';

beforeEach(() => cleanup());

/**
 * Real rows from `agg_edges`: `body-parser`'s first hop, and the second
 * hop below three of those. Invented numbers would make the encoding
 * assertions vacuous — a spread of 7,999 to 1 is what the form has to
 * survive, and a tidy fixture does not have one.
 */
const TREE: Tree = {
  root: 'body-parser',
  children: [
    { name: 'debug', repositories: 3580 },
    { name: 'http-errors', repositories: 3582 },
    { name: 'express', repositories: 1 },
  ],
  grandchildren: [
    { parent: 'debug', child: 'ms', repositories: 7999 },
    { parent: 'http-errors', child: 'statuses', repositories: 4239 },
    { parent: 'http-errors', child: 'inherits', repositories: 4235 },
    { parent: 'http-errors', child: 'setprototypeof', repositories: 4208 },
  ],
};

const WIDTH = 720;

function draw(tree: Tree = TREE, onSelect?: (name: string) => void) {
  const { container } = render(
    <DependencyTree
      tree={tree}
      width={WIDTH}
      {...(onSelect ? { onSelect } : {})}
    />,
  );
  return container;
}

/** Nodes by name, with the geometry the component computed. */
function nodes(host: HTMLElement) {
  return [...host.querySelectorAll('g[data-node]')].map((g) => {
    const circle = g.querySelector('circle')!;
    return {
      name: g.getAttribute('data-node')!,
      depth: Number(g.getAttribute('data-depth')),
      x: Number(circle.getAttribute('cx')),
      y: Number(circle.getAttribute('cy')),
    };
  });
}

const at = (host: HTMLElement, name: string) =>
  nodes(host).find((node) => node.name === name)!;

const edges = (host: HTMLElement) => [...host.querySelectorAll('path')];

describe('DependencyTree', () => {
  it('draws the root name in full at a realistic length', () => {
    /**
     * The root is the subject of the panel, so it is the one label a
     * reader must not have to guess at. The first gutter was 96px and
     * `safer-buffer` — eleven characters, a perfectly ordinary npm name
     * — drew as `safer-buff…`.
     */
    const host = draw({
      root: 'safer-buffer',
      children: [{ name: 'iconv-lite', repositories: 5956 }],
      grandchildren: [],
    });
    const label = host.querySelector('g[data-depth="0"] text')!;
    expect(label.textContent).toBe('safer-buffer');
  });

  it('keeps a name too long for even the wider gutter reachable', () => {
    const long = '@modelcontextprotocol/sdk-with-an-extra-long-tail';
    const host = draw({
      root: long,
      children: [{ name: 'zod', repositories: 12 }],
      grandchildren: [],
    });
    const label = host.querySelector('g[data-depth="0"] text')!;
    expect(label.textContent).toMatch(/…$/);
    fireEvent.mouseEnter(label);
    expect(document.querySelector('.chart-tooltip')!.textContent).toContain(
      long,
    );
  });

  it('does not spend a fifth of a narrow panel on the root label', () => {
    const { container } = render(
      <DependencyTree tree={TREE} width={360} onSelect={undefined} />,
    );
    const root = container.querySelector('g[data-depth="0"] circle')!;
    expect(Number(root.getAttribute('cx'))).toBeLessThanOrEqual(72);
  });

  it('draws every node exactly once', () => {
    const host = draw();
    expect(nodes(host)).toHaveLength(1 + 3 + 4);
  });

  it('puts a hop in a column', () => {
    const host = draw();
    const root = at(host, 'body-parser');
    const first = at(host, 'debug');
    const second = at(host, 'ms');
    expect(root.x).toBeLessThan(first.x);
    expect(first.x).toBeLessThan(second.x);
    // Every node of one depth shares its column, or depth is not what
    // the horizontal axis means.
    const byDepth = new Map<number, Set<number>>();
    for (const node of nodes(host)) {
      const seen = byDepth.get(node.depth) ?? new Set<number>();
      seen.add(node.x);
      byDepth.set(node.depth, seen);
    }
    for (const seen of byDepth.values()) expect(seen.size).toBe(1);
  });

  it('encodes the repository count as stroke width', () => {
    const host = draw();
    const drawn = edges(host).map((path) => ({
      pair: path.getAttribute('data-edge')!,
      repositories: Number(path.getAttribute('data-repositories')),
      stroke: Number(path.getAttribute('stroke-width')),
    }));
    expect(drawn).toHaveLength(TREE.children.length + TREE.grandchildren.length);

    // Monotonic in the number, which is the claim the chart makes. An
    // encoding that merely varies would satisfy a min/max assertion
    // while carrying no information.
    const byCount = [...drawn].sort((a, b) => a.repositories - b.repositories);
    for (let i = 1; i < byCount.length; i += 1) {
      expect(byCount[i]!.stroke).toBeGreaterThanOrEqual(byCount[i - 1]!.stroke);
    }
    expect(
      drawn.find((edge) => edge.pair === 'debug>ms')!.stroke,
    ).toBeGreaterThan(
      drawn.find((edge) => edge.pair === 'body-parser>express')!.stroke,
    );
  });

  it('keeps the 7,999-to-1 spread visible rather than flattening it', () => {
    /**
     * The whole objection to a force layout. `debug → ms` occurs in
     * 7,999 repositories and `body-parser → express` in one; if those
     * two marks come out within a hair of each other, this form has the
     * same defect and is not worth having.
     */
    const host = draw();
    const widths = edges(host)
      .map((path) => Number(path.getAttribute('stroke-width')))
      .sort((a, b) => a - b);
    const thinnest = widths[0]!;
    const widest = widths[widths.length - 1]!;
    expect(widest / thinnest).toBeGreaterThan(5);
  });

  it('gives no edge a zero width, so a rare pair is still a line', () => {
    // `body-parser → express` is one repository against a maximum of
    // 7,999. Scaled linearly from zero that is 0.0009px: the finding
    // would be that the edge does not exist.
    const host = draw();
    for (const path of edges(host)) {
      expect(Number(path.getAttribute('stroke-width'))).toBeGreaterThan(0.5);
    }
  });

  it('never places two first-hop packages on the same row', () => {
    const host = draw();
    const ys = nodes(host)
      .filter((node) => node.depth === 1)
      .map((node) => node.y);
    expect(new Set(ys).size).toBe(ys.length);
  });

  it('centres a package on its own children, not on the frame', () => {
    /**
     * `http-errors` has three children and `debug` has one, so the
     * blocks are different heights and the frame centre is not where
     * the edges converge. A parent drawn at the frame centre would have
     * its edges crossing its neighbours'.
     */
    const host = draw();
    const parent = at(host, 'http-errors');
    const kids = ['statuses', 'inherits', 'setprototypeof'].map(
      (name) => at(host, name).y,
    );
    const mean = kids.reduce((a, b) => a + b, 0) / kids.length;
    expect(parent.y).toBeCloseTo(mean, 1);
  });

  it('gives a childless package a row of its own', () => {
    // `express` has no second hop here. Without a reserved row it would
    // land on top of whichever package is laid out next.
    const host = draw();
    const express = at(host, 'express');
    const others = nodes(host).filter(
      (node) => node.depth === 1 && node.name !== 'express',
    );
    for (const other of others) {
      expect(Math.abs(other.y - express.y)).toBeGreaterThan(4);
    }
  });

  it('keeps every mark inside the frame it declares', () => {
    const host = draw();
    const svg = host.querySelector('svg')!;
    const height = Number(svg.getAttribute('height'));
    expect(Number(svg.getAttribute('width'))).toBe(WIDTH);
    for (const node of nodes(host)) {
      expect(node.y).toBeGreaterThan(0);
      expect(node.y).toBeLessThan(height);
      expect(node.x).toBeLessThan(WIDTH);
    }
  });

  it('trims a name rather than drawing it over the next column', () => {
    const host = draw({
      root: 'body-parser',
      children: [
        {
          name: '@some-scope/a-package-name-far-longer-than-its-column',
          repositories: 12,
        },
      ],
      grandchildren: [],
    });
    const label = [...host.querySelectorAll('text')].find((text) =>
      text.textContent!.startsWith('@some-scope'),
    )!;
    expect(label.textContent).toMatch(/…$/);
    // Trimmed at the end, so the scope still identifies the package.
    expect(
      '@some-scope/a-package-name-far-longer-than-its-column'.startsWith(
        label.textContent!.slice(0, -1),
      ),
    ).toBe(true);
    expect(label.textContent!.length).toBeLessThan(
      '@some-scope/a-package-name-far-longer-than-its-column'.length,
    );
  });

  it('plates a first-hop row, which sits where its own edges begin', () => {
    /**
     * A first-hop label starts 10px right of the dot its outgoing edges
     * leave from, so the edges were drawn through both the name and the
     * count. Found at full resolution; invisible to a text-against-text
     * check, since the thing crossing them is a stroke.
     *
     * A glyph-outline halo was the first fix and was not enough — an
     * edge showed through the gaps between characters and `iconv-lite`
     * read as `iconv=lite`. Hence a solid plate, and hence this test
     * asserts a rect rather than a paint-order.
     */
    const host = draw();
    const row = host.querySelector('g[data-node="debug"][data-depth="1"]')!;
    const plates = [...row.querySelectorAll('rect')];
    expect(plates).toHaveLength(2);
    for (const plate of plates) {
      expect(Number(plate.getAttribute('width'))).toBeGreaterThan(8);
      expect(Number(plate.getAttribute('height'))).toBeGreaterThan(8);
    }
    // Drawn before the text, or it covers what it is protecting.
    const kids = [...row.children].map((node) => node.tagName);
    expect(kids.indexOf('rect')).toBeLessThan(kids.indexOf('text'));
  });

  it('sizes a plate from the text actually drawn, not the full name', () => {
    // A trimmed name with a plate sized for the untrimmed one would
    // erase the column beside it.
    const host = draw({
      root: 'body-parser',
      children: [
        { name: 'a-package-name-far-longer-than-its-own-column', repositories: 9 },
      ],
      grandchildren: [
        { parent: 'a-package-name-far-longer-than-its-own-column', child: 'x', repositories: 1 },
      ],
    });
    const row = host.querySelector('g[data-depth="1"]')!;
    const drawn = row.querySelector('text')!.textContent!;
    const plate = Number(row.querySelector('rect')!.getAttribute('width'));
    expect(drawn).toMatch(/…$/);
    expect(plate).toBeLessThan(
      'a-package-name-far-longer-than-its-own-column'.length * 7.2,
    );
    expect(plate).toBeCloseTo(drawn.length * 7.2 + 4, 0);
  });

  it('plates no leaf, since nothing crosses one', () => {
    // Edges arriving at a leaf stop 6px short of its dot, so a plate
    // there would be chrome with no defect to fix — and one more mark
    // per row in the densest column.
    const host = draw();
    const leaf = host.querySelector('g[data-node="ms"][data-depth="2"]')!;
    expect(leaf.querySelectorAll('rect')).toHaveLength(0);
  });

  it('labels every node and its count', () => {
    const host = draw();
    const all = [...host.querySelectorAll('text')].map((t) => t.textContent);
    expect(all).toContain('body-parser');
    expect(all).toContain('debug');
    expect(all).toContain('ms');
    expect(all).toContain('3,580');
  });

  it('opens a package when its mark is chosen', () => {
    const onSelect = vi.fn();
    const host = draw(TREE, onSelect);
    fireEvent.click(host.querySelector('g[data-node="ms"] circle')!);
    expect(onSelect).toHaveBeenCalledWith('ms');
  });

  it('says why a panel is blank rather than drawing a blank panel', () => {
    const host = draw({ root: 'left-pad', children: [], grandchildren: [] });
    expect(host.querySelector('svg')).toBeNull();
    expect(host.textContent).toContain('left-pad');
  });

  it('carries a legend, so depth is never colour alone', () => {
    const host = draw();
    const legend = host.querySelector('.chart-legend')!;
    expect(legend.textContent).toContain('body-parser');
    expect(legend.textContent).toContain('second hop');
  });

  it('describes itself for a reader who cannot see it', () => {
    const host = draw();
    const label = host.querySelector('svg')!.getAttribute('aria-label')!;
    expect(label).toContain('body-parser');
    expect(label).toContain('thickness');
  });
});
