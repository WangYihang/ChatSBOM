/**
 * Chart primitives, hand-authored as inline SVG.
 *
 * No chart library: every form here is bars, stacked bars, an area, or a
 * histogram, and a library would cost more bundle than the whole
 * dashboard's JavaScript. The client bundle is 205 kB today, most of it
 * DuckDB's bindings.
 *
 * Mark specs follow the house rules rather than defaults: thin marks,
 * 4px rounded data-ends anchored to the baseline, a 2px surface gap
 * between adjacent fills so segments read as separate, recessive grid and
 * axes, and selective direct labels — never a number on every mark.
 *
 * Every chart with a plot ships a hover layer. A chart that cannot be
 * interrogated is a picture of data rather than a view of it.
 */
import { chartTheme, rampColor, seriesColor, type SeriesName } from './palette';

const NS = 'http://www.w3.org/2000/svg';

/** Gap between adjacent fills, so two segments never touch. */
const SPACER = 2;
/** Radius on the data end of a bar. */
const CAP = 4;

export interface Bar {
  label: string;
  value: number;
  /** Optional second value drawn as a segment of the same bar. */
  part?: number;
  /** Shown in the tooltip instead of the raw numbers. */
  detail?: string;
}

export interface StackSlice {
  series: SeriesName;
  label: string;
  value: number;
}

export interface TimePoint {
  label: string;
  total: number;
  direct: number;
}

function el<K extends keyof SVGElementTagNameMap>(
  name: K,
  attrs: Record<string, string | number> = {},
): SVGElementTagNameMap[K] {
  const node = document.createElementNS(NS, name);
  for (const [key, value] of Object.entries(attrs)) {
    node.setAttribute(key, String(value));
  }
  return node;
}

function svgRoot(width: number, height: number, label: string): SVGSVGElement {
  const svg = el('svg', {
    viewBox: `0 0 ${width} ${height}`,
    role: 'img',
    'aria-label': label,
    preserveAspectRatio: 'xMinYMin meet',
  });
  svg.style.width = '100%';
  svg.style.height = 'auto';
  return svg;
}

function text(
  content: string,
  x: number,
  y: number,
  opts: { anchor?: string; size?: number; fill: string; weight?: number } = {
    fill: 'currentColor',
  },
): SVGTextElement {
  const node = el('text', {
    x, y,
    'text-anchor': opts.anchor ?? 'start',
    'font-size': opts.size ?? 11,
    fill: opts.fill,
    'dominant-baseline': 'middle',
  });
  if (opts.weight) node.setAttribute('font-weight', String(opts.weight));
  node.textContent = content;
  return node;
}

/** A bar whose data end is rounded and whose baseline end is square. */
function barPath(
  x: number,
  y: number,
  width: number,
  height: number,
  horizontal: boolean,
): string {
  const r = Math.min(CAP, horizontal ? width : height);
  if (horizontal) {
    return [
      `M ${x} ${y}`,
      `H ${x + width - r}`,
      `Q ${x + width} ${y} ${x + width} ${y + r}`,
      `V ${y + height - r}`,
      `Q ${x + width} ${y + height} ${x + width - r} ${y + height}`,
      `H ${x}`,
      'Z',
    ].join(' ');
  }
  return [
    `M ${x} ${y + height}`,
    `V ${y + r}`,
    `Q ${x} ${y} ${x + r} ${y}`,
    `H ${x + width - r}`,
    `Q ${x + width} ${y} ${x + width} ${y + r}`,
    `V ${y + height}`,
    'Z',
  ].join(' ');
}

/** Attach a tooltip to a mark. Hit target is the mark plus a 4px halo. */
function hoverable(
  node: SVGElement,
  tooltip: HTMLElement,
  content: () => string,
): void {
  node.style.cursor = 'default';
  const show = (event: MouseEvent) => {
    tooltip.innerHTML = content();
    tooltip.hidden = false;
    const host = tooltip.parentElement;
    if (!host) return;
    const bounds = host.getBoundingClientRect();
    tooltip.style.left = `${event.clientX - bounds.left + 12}px`;
    tooltip.style.top = `${event.clientY - bounds.top + 12}px`;
  };
  node.addEventListener('mouseenter', show as EventListener);
  node.addEventListener('mousemove', show as EventListener);
  node.addEventListener('mouseleave', () => {
    tooltip.hidden = true;
  });
}

function tooltipFor(host: HTMLElement): HTMLElement {
  let tooltip = host.querySelector<HTMLElement>('.chart-tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.className = 'chart-tooltip';
    tooltip.hidden = true;
    host.append(tooltip);
  }
  return tooltip;
}

function clear(host: HTMLElement): void {
  host.querySelectorAll('svg, .chart-legend').forEach((n) => n.remove());
}

/**
 * Ranked horizontal bars for a magnitude comparison.
 *
 * One hue from the sequential ramp, because this compares *sizes* of one
 * quantity — categorical hues here would imply the rows are different
 * kinds of thing. When `part` is present it is drawn as a darker inset,
 * which is how "of these, how many are direct" is shown without a second
 * axis.
 */
export function rankedBars(
  host: HTMLElement,
  bars: Bar[],
  options: {
    label: string;
    partLabel?: string;
    valueFormat?: (value: number) => string;
  },
): void {
  clear(host);
  if (bars.length === 0) {
    host.append(emptyNote());
    return;
  }

  const theme = chartTheme();
  const tooltip = tooltipFor(host);
  const format = options.valueFormat ?? ((v: number) => v.toLocaleString());

  const rowHeight = 26;
  const barHeight = 12;
  const labelWidth = 168;
  const valueWidth = 76;
  const width = 720;
  const plotWidth = width - labelWidth - valueWidth;
  const height = bars.length * rowHeight + 8;

  const svg = svgRoot(width, height, options.label);
  const max = Math.max(...bars.map((b) => b.value)) || 1;

  bars.forEach((bar, index) => {
    const y = index * rowHeight + 8;
    const fraction = bar.value / max;
    const barWidth = Math.max(fraction * plotWidth, CAP);

    svg.append(
      text(bar.label, labelWidth - 10, y + barHeight / 2, {
        anchor: 'end', fill: theme.ink, size: 11.5,
      }),
    );

    const fill = rampColor(fraction, theme);
    const rect = el('path', {
      d: barPath(labelWidth, y, barWidth, barHeight, true),
      fill,
    });
    hoverable(rect, tooltip, () =>
      bar.detail ??
      `<strong>${bar.label}</strong><br>${format(bar.value)}` +
        (bar.part !== undefined
          ? `<br>${options.partLabel ?? 'part'}: ${format(bar.part)}`
          : ''),
    );
    svg.append(rect);

    // The inset segment is separated by a surface-coloured gap rather
    // than a stroke, so it reads as a slice of the same bar.
    if (bar.part !== undefined && bar.part > 0) {
      const partWidth = Math.max((bar.part / max) * plotWidth, CAP);
      svg.append(
        el('path', {
          d: barPath(labelWidth, y + 3, Math.min(partWidth, barWidth), barHeight - 6, true),
          fill: theme.surface,
          opacity: 0.9,
        }),
        el('path', {
          d: barPath(labelWidth, y + 4, Math.min(partWidth - 1, barWidth - 1), barHeight - 8, true),
          fill: seriesColor('direct', theme),
        }),
      );
    }

    svg.append(
      text(format(bar.value), width - 8, y + barHeight / 2, {
        anchor: 'end', fill: theme.inkMuted, size: 11,
      }),
    );
  });

  host.append(svg);
  if (bars.some((b) => b.part !== undefined)) {
    host.append(
      legend([
        { swatch: rampColor(1, theme), label: options.label },
        { swatch: seriesColor('direct', theme), label: options.partLabel ?? 'direct' },
      ]),
    );
  }
}

/**
 * A single stacked bar showing how a whole divides.
 *
 * Used for the direct/transitive/unknown split, which is one quantity in
 * parts rather than several quantities — so one bar, not three.
 */
export function stackedShare(
  host: HTMLElement,
  slices: StackSlice[],
  options: { label: string },
): void {
  clear(host);
  const total = slices.reduce((sum, s) => sum + s.value, 0);
  if (total === 0) {
    host.append(emptyNote());
    return;
  }

  const theme = chartTheme();
  const tooltip = tooltipFor(host);
  const width = 720;
  const barHeight = 28;
  const svg = svgRoot(width, barHeight + 30, options.label);

  let x = 0;
  slices.forEach((slice, index) => {
    const share = slice.value / total;
    const raw = share * width;
    const last = index === slices.length - 1;
    const segmentWidth = Math.max(raw - (last ? 0 : SPACER), 1);

    const path = el('path', {
      d: barPath(x, 0, segmentWidth, barHeight, true),
      fill: seriesColor(slice.series, theme),
    });
    hoverable(path, tooltip, () =>
      `<strong>${slice.label}</strong><br>` +
      `${slice.value.toLocaleString()} (${(share * 100).toFixed(1)}%)`,
    );
    svg.append(path);

    // Direct-label only segments wide enough to hold the text.
    if (raw > 84) {
      svg.append(
        text(`${(share * 100).toFixed(0)}%`, x + 10, barHeight / 2, {
          fill: theme.surface, size: 12, weight: 600,
        }),
      );
    }
    x += raw;
  });

  host.append(svg);
  host.append(
    legend(
      slices.map((s) => ({
        swatch: seriesColor(s.series, theme),
        label: `${s.label} · ${((s.value / total) * 100).toFixed(1)}%`,
      })),
    ),
  );
}

/** Counts across ordered buckets. One hue: this is magnitude, not identity. */
export function histogram(
  host: HTMLElement,
  buckets: Bar[],
  options: { label: string; xLabel?: string },
): void {
  clear(host);
  if (buckets.length === 0) {
    host.append(emptyNote());
    return;
  }

  const theme = chartTheme();
  const tooltip = tooltipFor(host);
  const width = 720;
  const height = 200;
  const padding = { top: 12, right: 8, bottom: 34, left: 48 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const svg = svgRoot(width, height, options.label);

  const max = Math.max(...buckets.map((b) => b.value)) || 1;
  const slot = plotWidth / buckets.length;

  // Recessive gridlines, drawn behind the marks.
  for (let i = 0; i <= 2; i += 1) {
    const y = padding.top + (plotHeight * i) / 2;
    svg.append(
      el('line', {
        x1: padding.left, y1: y, x2: width - padding.right, y2: y,
        stroke: theme.grid, 'stroke-width': 1,
      }),
      text(
        Math.round((max * (2 - i)) / 2).toLocaleString(),
        padding.left - 8, y,
        { anchor: 'end', fill: theme.inkMuted, size: 10 },
      ),
    );
  }

  buckets.forEach((bucket, index) => {
    const barWidth = Math.max(slot - SPACER * 2, 2);
    const barHeight = Math.max((bucket.value / max) * plotHeight, 1);
    const x = padding.left + index * slot + SPACER;
    const y = padding.top + plotHeight - barHeight;

    const path = el('path', {
      d: barPath(x, y, barWidth, barHeight, false),
      fill: rampColor(bucket.value / max, theme),
    });
    hoverable(path, tooltip, () =>
      `<strong>${bucket.label}</strong><br>${bucket.value.toLocaleString()}`,
    );
    svg.append(path);

    // Label every other bucket when they would otherwise collide.
    if (buckets.length <= 12 || index % 2 === 0) {
      svg.append(
        text(bucket.label, x + barWidth / 2, height - padding.bottom + 14, {
          anchor: 'middle', fill: theme.inkMuted, size: 10,
        }),
      );
    }
  });

  svg.append(
    el('line', {
      x1: padding.left, y1: padding.top + plotHeight,
      x2: width - padding.right, y2: padding.top + plotHeight,
      stroke: theme.axis, 'stroke-width': 1,
    }),
  );
  if (options.xLabel) {
    svg.append(
      text(options.xLabel, width / 2, height - 6, {
        anchor: 'middle', fill: theme.inkMuted, size: 10,
      }),
    );
  }

  host.append(svg);
}

/**
 * Adoption over time: total repositories and, of those, how many declare
 * the package.
 *
 * Two series on **one** axis — both are repository counts, so a second
 * scale would be the dual-axis mistake. The direct line sits inside the
 * total area, which is what makes the relationship readable.
 */
export function timeSeries(
  host: HTMLElement,
  points: TimePoint[],
  options: { label: string },
): void {
  clear(host);
  if (points.length === 0) {
    host.append(emptyNote('No history yet — it accumulates as the queue runs.'));
    return;
  }

  const theme = chartTheme();
  const tooltip = tooltipFor(host);
  const width = 720;
  const height = 220;
  const padding = { top: 14, right: 12, bottom: 34, left: 52 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const svg = svgRoot(width, height, options.label);

  const max = Math.max(...points.map((p) => p.total)) || 1;
  const x = (i: number) =>
    padding.left + (points.length === 1 ? plotWidth / 2 : (i / (points.length - 1)) * plotWidth);
  const y = (v: number) => padding.top + plotHeight - (v / max) * plotHeight;

  for (let i = 0; i <= 2; i += 1) {
    const gy = padding.top + (plotHeight * i) / 2;
    svg.append(
      el('line', {
        x1: padding.left, y1: gy, x2: width - padding.right, y2: gy,
        stroke: theme.grid, 'stroke-width': 1,
      }),
      text(
        Math.round((max * (2 - i)) / 2).toLocaleString(),
        padding.left - 8, gy,
        { anchor: 'end', fill: theme.inkMuted, size: 10 },
      ),
    );
  }

  const totalColor = seriesColor('transitive', theme);
  const directColor = seriesColor('direct', theme);

  const area = points.map((p, i) => `${x(i)},${y(p.total)}`).join(' ');
  svg.append(
    el('polygon', {
      points:
        `${padding.left},${padding.top + plotHeight} ${area} ` +
        `${x(points.length - 1)},${padding.top + plotHeight}`,
      fill: totalColor,
      opacity: 0.14,
    }),
    el('polyline', {
      points: area,
      fill: 'none',
      stroke: totalColor,
      'stroke-width': 2,
      'stroke-linejoin': 'round',
    }),
    el('polyline', {
      points: points.map((p, i) => `${x(i)},${y(p.direct)}`).join(' '),
      fill: 'none',
      stroke: directColor,
      'stroke-width': 2,
      'stroke-linejoin': 'round',
    }),
  );

  // Markers double as hit targets; a 2px surface ring keeps overlapping
  // points readable.
  points.forEach((point, index) => {
    for (const [value, color] of [
      [point.total, totalColor],
      [point.direct, directColor],
    ] as const) {
      const dot = el('circle', {
        cx: x(index), cy: y(value), r: 4,
        fill: color, stroke: theme.surface, 'stroke-width': 2,
      });
      hoverable(dot, tooltip, () =>
        `<strong>${point.label}</strong><br>` +
        `total ${point.total.toLocaleString()}<br>` +
        `direct ${point.direct.toLocaleString()}`,
      );
      svg.append(dot);
    }

    if (points.length <= 12 || index % Math.ceil(points.length / 8) === 0) {
      svg.append(
        text(point.label, x(index), height - padding.bottom + 16, {
          anchor: 'middle', fill: theme.inkMuted, size: 10,
        }),
      );
    }
  });

  host.append(svg);
  host.append(
    legend([
      { swatch: totalColor, label: 'all dependants' },
      { swatch: directColor, label: 'declared it' },
    ]),
  );
}

/** Grouped bars comparing the two SBOM sources per language. */
export function groupedBars(
  host: HTMLElement,
  groups: { label: string; values: { series: SeriesName; value: number }[] }[],
  options: { label: string; seriesLabels: Record<string, string> },
): void {
  clear(host);
  if (groups.length === 0) {
    host.append(emptyNote());
    return;
  }

  const theme = chartTheme();
  const tooltip = tooltipFor(host);
  const width = 720;
  const rowHeight = 34;
  const labelWidth = 110;
  const height = groups.length * rowHeight + 10;
  const plotWidth = width - labelWidth - 70;
  const svg = svgRoot(width, height, options.label);

  const max = Math.max(
    ...groups.flatMap((g) => g.values.map((v) => v.value)),
  ) || 1;

  groups.forEach((group, index) => {
    const top = index * rowHeight + 6;
    svg.append(
      text(group.label, labelWidth - 10, top + rowHeight / 2 - 6, {
        anchor: 'end', fill: theme.ink, size: 11.5,
      }),
    );

    group.values.forEach((entry, row) => {
      const barHeight = 9;
      const y = top + row * (barHeight + SPACER);
      const barWidth = Math.max((entry.value / max) * plotWidth, 1);
      const path = el('path', {
        d: barPath(labelWidth, y, barWidth, barHeight, true),
        fill: seriesColor(entry.series, theme),
      });
      hoverable(path, tooltip, () =>
        `<strong>${group.label}</strong><br>` +
        `${options.seriesLabels[entry.series] ?? entry.series}: ` +
        entry.value.toLocaleString(),
      );
      svg.append(path);
    });
  });

  host.append(svg);
  host.append(
    legend(
      Object.entries(options.seriesLabels).map(([series, label]) => ({
        swatch: seriesColor(series as SeriesName, theme),
        label,
      })),
    ),
  );
}

function legend(entries: { swatch: string; label: string }[]): HTMLElement {
  const box = document.createElement('div');
  box.className = 'chart-legend';
  for (const entry of entries) {
    const item = document.createElement('span');
    const dot = document.createElement('i');
    dot.style.background = entry.swatch;
    item.append(dot, document.createTextNode(entry.label));
    box.append(item);
  }
  return box;
}

function emptyNote(message = 'No data for this selection.'): HTMLElement {
  const note = document.createElement('p');
  note.className = 'chart-empty';
  note.textContent = message;
  return note;
}
