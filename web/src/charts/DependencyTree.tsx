/**
 * Two hops of the dependency graph, placed rather than simulated.
 *
 * The obvious form for a graph is a force-directed layout, and it is the
 * wrong one for this data. Position there is decided by topology, so the
 * number that matters carries no position at all — and the number spans
 * three orders of magnitude: of 454,577 edges, 208,211 appear in exactly
 * one repository and 3,886 appear in more than a thousand.
 * `debug → ms` occurs in 7,999 repositories and
 * `body-parser → express` in one, and a force layout gives them
 * comparable visual weight because a line can only get so thin.
 *
 * Here the two encodings do not compete: a column is a hop, and stroke
 * width is the repository count. The long tail stays visible as a
 * hairline instead of averaging away.
 *
 * Bounded on purpose — see `DependencyTree` in `d1/queries.ts`. The
 * largest repository in this dataset declares 6,635 dependencies; the
 * unbounded drawing is not a smaller version of this one, it is an
 * image nobody can read.
 */
import { scaleLinear } from '@visx/scale';

import { ChartFrame, Empty, Legend, useChartTheme, useChartTooltip } from './Frame';
import { ADVANCE, clipLabel } from './geometry';
import type { DependencyTree as Tree } from '../d1/queries';
import { rampColor } from '../palette';
import type { TooltipContent } from './tooltip';

/** Vertical pitch of one leaf row. */
const ROW = 15;
const PAD_TOP = 10;
const PAD_BOTTOM = 6;

/**
 * Left gutter, for the root's own label.
 *
 * 96px was one character too narrow for `safer-buffer`, which drew as
 * `safer-buff…` — and the root is the subject of the whole panel, so it
 * is the one label that must not be guessed at. 150px holds 20
 * characters of the mono face. A scoped name longer than that is still
 * trimmed; the panel heading and the node's tooltip carry it in full.
 *
 * Capped as a fraction of the panel too, so a narrow rail does not
 * spend a fifth of its width on one label.
 */
const ROOT_GUTTER_MAX = 150;
/** Right gutter: a second-hop name plus its count. */
const TAIL_GUTTER = 178;

/** Widest stroke a first-hop edge may take. */
const MAX_STROKE = 7;

interface Node {
  name: string;
  x: number;
  y: number;
  depth: 0 | 1 | 2;
  repositories?: number;
}

export function DependencyTree({
  tree,
  width,
  onSelect,
}: {
  tree: Tree;
  width: number;
  onSelect?: (name: string) => void;
}) {
  const theme = useChartTheme();
  const { bind, tooltip } = useChartTooltip();

  if (tree.children.length === 0) {
    return (
      <Empty message={`No package pulled in by ${tree.root} is recorded.`} />
    );
  }

  /* ---- columns ---------------------------------------------------- */

  const col0 = Math.min(ROOT_GUTTER_MAX, Math.round(width * 0.2));
  const col2 = Math.max(width - TAIL_GUTTER, col0 + 230);
  const col1 = col0 + Math.max(Math.round((col2 - col0) * 0.42), 132);

  /* ---- rows ------------------------------------------------------- */

  // A block of leaf rows per first-hop package, so a package sits
  // beside its own children and a package with none still gets a row.
  // Laying the leaves out globally and then averaging would let two
  // parents land on the same y.
  const grouped = new Map<string, { child: string; repositories: number }[]>();
  for (const edge of tree.grandchildren) {
    const list = grouped.get(edge.parent);
    if (list) list.push(edge);
    else grouped.set(edge.parent, [edge]);
  }

  const nodes: Node[] = [];
  const edges: { from: Node; to: Node; repositories: number; hop: 1 | 2 }[] =
    [];

  let cursor = PAD_TOP;
  const firstHop: Node[] = [];

  for (const child of tree.children) {
    const leaves = grouped.get(child.name) ?? [];
    const block = Math.max(leaves.length, 1);
    const top = cursor;
    cursor += block * ROW;

    const node: Node = {
      name: child.name,
      x: col1,
      y: top + (block * ROW) / 2,
      depth: 1,
      repositories: child.repositories,
    };
    nodes.push(node);
    firstHop.push(node);

    leaves.forEach((leaf, index) => {
      const leafNode: Node = {
        name: leaf.child,
        x: col2,
        y: top + index * ROW + ROW / 2,
        depth: 2,
        repositories: leaf.repositories,
      };
      nodes.push(leafNode);
      edges.push({
        from: node,
        to: leafNode,
        repositories: leaf.repositories,
        hop: 2,
      });
    });
  }

  const height = cursor + PAD_BOTTOM;
  const root: Node = {
    name: tree.root,
    x: col0,
    // Centred on its children rather than on the frame: with a tall
    // block at the top and a short one at the bottom, the frame centre
    // is not where the edges converge.
    y:
      firstHop.reduce((sum, node) => sum + node.y, 0) /
      Math.max(firstHop.length, 1),
    depth: 0,
  };
  for (const node of firstHop) {
    edges.push({
      from: root,
      to: node,
      repositories: node.repositories ?? 0,
      hop: 1,
    });
  }

  /* ---- encodings -------------------------------------------------- */

  const max = Math.max(...edges.map((edge) => edge.repositories), 1);
  const stroke = scaleLinear({ domain: [0, max], range: [0.7, MAX_STROKE] });

  const colour = {
    0: rampColor(1, theme),
    1: rampColor(0.62, theme),
    2: rampColor(0.3, theme),
  } as const;

  const childRoom = col2 - 16 - (col1 + 10) - 46;
  const leafRoom = width - 4 - (col2 + 8) - 46;

  const pick = (name: string) =>
    onSelect ? { onClick: () => onSelect(name), cursor: 'pointer' as const } : {};

  return (
    <>
      <ChartFrame
        width={width}
        height={height}
        label={`Packages ${tree.root} pulls in, two hops, thickness by repository count`}
      >
        {/* Edges first, so a mark is never drawn under a line. */}
        {edges.map((edge) => {
          const mid = (edge.from.x + edge.to.x) / 2;
          const content: TooltipContent = {
            title: `${edge.from.name} → ${edge.to.name}`,
            lines: [
              `${edge.repositories.toLocaleString()} repositories show this pair`,
            ],
          };
          return (
            <path
              key={`${edge.from.name}>${edge.to.name}`}
              // The pair and its count, so the encoding can be checked
              // against the number it claims to carry rather than
              // against the order the paths happen to be emitted in.
              data-edge={`${edge.from.name}>${edge.to.name}`}
              data-repositories={edge.repositories}
              d={
                `M ${edge.from.x + (edge.hop === 1 ? 8 : 6)} ${edge.from.y} ` +
                `C ${mid} ${edge.from.y}, ${mid} ${edge.to.y}, ` +
                `${edge.to.x - 6} ${edge.to.y}`
              }
              fill="none"
              stroke={colour[edge.hop === 1 ? 1 : 2]}
              strokeWidth={stroke(edge.repositories)}
              opacity={edge.hop === 1 ? 0.55 : 0.8}
              {...bind(content)}
            />
          );
        })}

        {/* The root, labelled into its own gutter. */}
        <g data-node={root.name} data-depth="0">
          <circle
            cx={root.x}
            cy={root.y}
            r={7}
            fill={colour[0]}
            {...bind({
              title: root.name,
              lines: [`${tree.children.length} packages pulled in directly`],
            })}
          />
          <text
            x={root.x - 14}
            y={root.y}
            textAnchor="end"
            dominantBaseline="middle"
            fill={theme.ink}
            fontSize={12}
            fontWeight={600}
            fontFamily="var(--f-mono)"
            {...bind({ title: root.name, lines: ['The package asked about'] })}
          >
            {clipLabel(root.name, col0 - 16, ADVANCE.mono115)}
          </text>
        </g>

        {nodes.map((node) => {
          const leaf = node.depth === 2;
          const content: TooltipContent = {
            title: node.name,
            lines: [
              leaf
                ? 'Second hop — pulled in by the package to its left'
                : `Pulled in by ${tree.root} in ` +
                  `${(node.repositories ?? 0).toLocaleString()} repositories`,
              ...(onSelect ? ['Click to open this package'] : []),
            ],
          };
          const marks = { ...bind(content), ...pick(node.name) };

          // Hoisted: the plates behind these are sized from the text
          // that is actually drawn, so a trimmed name gets a trimmed
          // plate rather than one sized for the full name.
          const childLabel = leaf
            ? ''
            : clipLabel(node.name, childRoom, ADVANCE.mono115);
          const leafLabel = leaf
            ? clipLabel(node.name, leafRoom, ADVANCE.mono105)
            : '';
          const count = (node.repositories ?? 0).toLocaleString();
          const countWidth = count.length * ADVANCE.mono105;

          return (
            <g key={`${node.depth}:${node.name}:${node.y}`} data-node={node.name} data-depth={node.depth}>
              <circle
                cx={node.x}
                cy={node.y}
                r={leaf ? 3.2 : 4.6}
                fill={colour[node.depth]}
                {...marks}
              />
              {/* Plates before text, only where an edge crosses.
                  A first-hop row sits in the band its own outgoing
                  edges occupy — the label starts 10px right of the dot
                  the edges leave from — so both the name and the count
                  were drawn over by strokes.

                  A glyph-outline halo (`paint-order: stroke`) was the
                  first fix and is not enough: the halo follows the
                  glyphs, so an edge shows through the gaps *between*
                  characters and `iconv-lite` read as `iconv=lite`. A
                  solid plate has no gaps.

                  A leaf needs neither: edges arriving at it stop 6px
                  short of its dot, clear of both its label and its
                  count. */}
              {leaf ? null : (
                <>
                  <rect
                    x={node.x + 8}
                    y={node.y - 7}
                    width={childLabel.length * ADVANCE.mono115 + 4}
                    height={14}
                    fill={theme.surface}
                  />
                  <rect
                    x={col2 - 16 - countWidth - 3}
                    y={node.y - 6}
                    width={countWidth + 6}
                    height={12}
                    fill={theme.surface}
                  />
                </>
              )}
              <text
                x={node.x + (leaf ? 8 : 10)}
                y={node.y}
                dominantBaseline="middle"
                fill={leaf ? theme.inkMuted : theme.ink}
                fontSize={leaf ? 10.5 : 11.5}
                fontFamily="var(--f-mono)"
                {...marks}
              >
                {leaf ? leafLabel : childLabel}
              </text>
              <text
                x={leaf ? width - 2 : col2 - 16}
                y={node.y}
                textAnchor="end"
                dominantBaseline="middle"
                fill={theme.inkMuted}
                fontSize={10}
                fontFamily="var(--f-mono)"
              >
                {count}
              </text>
            </g>
          );
        })}
      </ChartFrame>

      <Legend
        entries={[
          { swatch: colour[0], label: tree.root },
          { swatch: colour[1], label: 'pulled in directly' },
          { swatch: colour[2], label: 'second hop' },
        ]}
      />
      {tooltip}
    </>
  );
}
