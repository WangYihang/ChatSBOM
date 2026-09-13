/**
 * Dashboard entry point.
 *
 * Boots DuckDB-WASM against the Parquet dataset and renders two views:
 * an overview that answers standing questions, and a query view for one
 * package. They are peers — a bar in the overview hands its package to
 * the query view, and the segmented control or the back button returns.
 *
 * All querying happens here in the browser. The Worker only serves bytes.
 */
import './style.css';

import { Agent, AgentError } from './agent';
import {
  groupedBars,
  histogram,
  rankedBars,
  stackedShare,
  timeSeries,
} from './charts';
import { connect, type Manifest } from './duckdb';
import { Dataset, type Dependent } from './queries';
import { Router, type Route } from './router';

const DEBOUNCE_MS = 250;
/** Package whose adoption series the overview shows by default. */
const FEATURED = 'mail';

function el<T extends HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (!node) throw new Error(`missing element #${id}`);
  return node as T;
}

function statTile(value: number, label: string): HTMLElement {
  const tile = document.createElement('div');
  tile.className = 'stat';
  const n = document.createElement('span');
  n.className = 'n';
  n.textContent = value.toLocaleString();
  const l = document.createElement('span');
  l.className = 'l';
  l.textContent = label;
  tile.append(n, l);
  return tile;
}

async function main(): Promise<void> {
  const status = el('status');

  let dataset: Dataset;
  let manifest: Manifest;
  try {
    const connected = await connect('/data');
    dataset = new Dataset(connected.db, '/data');
    manifest = connected.manifest;
  } catch (error) {
    status.textContent =
      error instanceof Error ? error.message : 'Could not load the dataset.';
    el('stats').textContent = '';
    return;
  }

  describeDataset(manifest);
  await populateLanguages(dataset);

  const router = new Router((route, previous) => {
    void onRoute(dataset, route, previous);
  });

  el('to-overview').addEventListener('click', () =>
    router.go({ view: 'overview' }),
  );
  el('to-query').addEventListener('click', () => router.go({ view: 'query' }));

  wireOverview(dataset, router);
  wireQuery(dataset, router);
  wireAsk(dataset);

  router.start();
  void renderOverview(dataset);
}

/** Language filters are populated from the data, never hard-coded. */
async function populateLanguages(dataset: Dataset): Promise<void> {
  const coverage = await dataset.languageCoverage();
  for (const id of ['top-language', 'query-language']) {
    const select = el<HTMLSelectElement>(id);
    for (const row of coverage) {
      if (!row.language) continue;
      const option = document.createElement('option');
      option.value = row.language;
      option.textContent = row.language;
      select.append(option);
    }
  }
}

async function onRoute(
  dataset: Dataset,
  route: Route,
  previous: Route,
): Promise<void> {
  if (route.view !== 'query') return;

  const search = el<HTMLInputElement>('package');
  if (route.package && route.package !== search.value) {
    search.value = route.package;
    await runPackageQuery(dataset);
  }
  // Only steal focus on a real view change, not on every re-render.
  if (previous.view !== 'query') search.focus();
}

// ---------------------------------------------------------------- overview

function wireOverview(dataset: Dataset, router: Router): void {
  const rerenderTop = () => void renderTopPackages(dataset, router);
  el('top-direct').addEventListener('change', rerenderTop);
  el('top-language').addEventListener('change', rerenderTop);

  el('adoption-drill').addEventListener('click', () =>
    router.go({ view: 'query', package: FEATURED }),
  );

  // Charts read the theme at draw time, so a theme switch must redraw.
  window
    .matchMedia?.('(prefers-color-scheme: dark)')
    .addEventListener('change', () => void renderOverview(dataset));
}

async function renderOverview(dataset: Dataset): Promise<void> {
  const totals = await dataset.totals();
  const stats = el('stats');
  stats.replaceChildren(
    statTile(totals.repositories, 'repositories'),
    statTile(totals.dependencies, 'dependency records'),
    statTile(totals.packages, 'distinct packages'),
    statTile(
      totals.dependencies
        ? Math.round((totals.classified / totals.dependencies) * 100)
        : 0,
      '% classified',
    ),
  );

  const split = await dataset.relationshipSplit();
  stackedShare(
    el('plot-split'),
    [
      { series: 'direct', label: 'declared', value: split.direct },
      { series: 'transitive', label: 'inherited', value: split.transitive },
      { series: 'unknown', label: 'undetermined', value: split.unknown },
    ],
    { label: 'How dependencies arrived, across the whole corpus' },
  );

  const coverage = await dataset.languageCoverage();
  rankedBars(
    el('plot-coverage'),
    coverage.map((row) => ({
      label: row.language || '(none)',
      value: row.repositories,
      part: row.withSbom,
      detail:
        `<strong>${row.language}</strong><br>` +
        `${row.repositories.toLocaleString()} repositories<br>` +
        `${row.withSbom.toLocaleString()} with an SBOM ` +
        `(${row.repositories ? Math.round((row.withSbom / row.repositories) * 100) : 0}%)`,
    })),
    { label: 'repositories', partLabel: 'with an SBOM' },
  );

  const buckets = await dataset.dependencyDistribution();
  histogram(
    el('plot-distribution'),
    buckets.map((b) => ({ label: b.label, value: b.repositories })),
    { label: 'Repositories by dependency count', xLabel: 'dependencies' },
  );

  const sources = await dataset.sourceComparison();
  groupedBars(
    el('plot-sources'),
    sources.map((row) => ({
      label: row.language || '(none)',
      values: [
        { series: 'syft' as const, value: row.syft },
        { series: 'github-depgraph' as const, value: row.depgraph },
      ],
    })),
    {
      label: 'Dependency records per source, by language',
      seriesLabels: { syft: 'Syft', 'github-depgraph': 'Dependency graph' },
    },
  );

  const licenses = await dataset.licenseShares(12);
  rankedBars(
    el('plot-licenses'),
    licenses.map((row) => ({
      label: row.license,
      value: row.repositoryCount,
      detail:
        `<strong>${row.license}</strong><br>` +
        `${row.repositoryCount.toLocaleString()} repositories<br>` +
        `${row.packageCount.toLocaleString()} distinct packages`,
    })),
    { label: 'repositories' },
  );

  el('adoption-package').textContent = `· ${FEATURED}`;
  const adoption = await dataset.adoptionOverTime(FEATURED);
  timeSeries(
    el('plot-adoption'),
    adoption.map((p) => ({
      label: p.month,
      total: p.repositoryCount,
      direct: p.directCount,
    })),
    { label: `Monthly adoption of ${FEATURED}` },
  );

  void renderTopPackages(dataset, undefined);
}

async function renderTopPackages(
  dataset: Dataset,
  router: Router | undefined,
): Promise<void> {
  const directOnly = el<HTMLInputElement>('top-direct').checked;
  const language = el<HTMLSelectElement>('top-language').value;

  const top = await dataset.topPackages({
    directOnly,
    ...(language ? { language } : {}),
    limit: 12,
  });

  const host = el('plot-top');
  rankedBars(
    host,
    top.map((row) => ({
      label: row.name,
      value: directOnly ? row.directCount : row.repositoryCount,
      detail:
        `<strong>${row.name}</strong><br>` +
        `${row.repositoryCount.toLocaleString()} dependants<br>` +
        `${row.directCount.toLocaleString()} declared it`,
    })),
    { label: directOnly ? 'repositories declaring it' : 'repositories' },
  );

  // Every bar is a way into the query view — the path someone takes when
  // a ranking prompts a question about one row.
  if (router) {
    host.querySelectorAll('path').forEach((path, index) => {
      const row = top[index];
      if (!row) return;
      path.style.cursor = 'pointer';
      path.addEventListener('click', () =>
        router.go({ view: 'query', package: row.name }),
      );
    });
  }
}

// ------------------------------------------------------------------- query

function wireQuery(dataset: Dataset, router: Router): void {
  const search = el<HTMLInputElement>('package');
  const directOnly = el<HTMLInputElement>('direct-only');
  const language = el<HTMLSelectElement>('query-language');

  let pending: number | undefined;
  const debounced = () => {
    window.clearTimeout(pending);
    pending = window.setTimeout(() => {
      const name = search.value.trim();
      if (name) router.go({ view: 'query', package: name });
      else void runPackageQuery(dataset);
    }, DEBOUNCE_MS);
  };

  search.addEventListener('input', debounced);
  directOnly.addEventListener('change', () => void runPackageQuery(dataset));
  language.addEventListener('change', () => void runPackageQuery(dataset));
}

async function runPackageQuery(dataset: Dataset): Promise<void> {
  const name = el<HTMLInputElement>('package').value.trim();
  const directOnly = el<HTMLInputElement>('direct-only').checked;
  const language = el<HTMLSelectElement>('query-language').value;

  const status = el('status');
  const results = el('results');
  const charts = el('package-charts');

  if (!name) {
    results.hidden = true;
    charts.hidden = true;
    status.textContent = 'Type a package name, or pick one from the overview.';
    return;
  }

  status.textContent = `Searching for ${name}…`;

  let dependents: Dependent[];
  try {
    dependents = await dataset.dependentsOf({
      name,
      directOnly,
      ...(language ? { language } : {}),
      limit: 100,
    });
  } catch (error) {
    results.hidden = true;
    charts.hidden = true;
    status.textContent =
      error instanceof Error ? error.message : 'Query failed.';
    return;
  }

  renderDependents(dependents);
  results.hidden = dependents.length === 0;
  status.textContent = summarise(name, dependents, directOnly);

  charts.hidden = dependents.length === 0;
  if (dependents.length === 0) return;

  const versions = await dataset.versionSpread(name, 10);
  rankedBars(
    el('plot-versions'),
    versions.map((v) => ({
      label: v.version,
      value: v.repositoryCount,
    })),
    { label: 'repositories' },
  );

  const adoption = await dataset.adoptionOverTime(name);
  timeSeries(
    el('plot-package-adoption'),
    adoption.map((p) => ({
      label: p.month,
      total: p.repositoryCount,
      direct: p.directCount,
    })),
    { label: `Monthly adoption of ${name}` },
  );
}

function renderDependents(dependents: Dependent[]): void {
  const body = el<HTMLTableSectionElement>('rows');
  body.replaceChildren(
    ...dependents.map((dep) => {
      const tr = document.createElement('tr');

      const repo = document.createElement('td');
      const link = document.createElement('a');
      link.href = dep.url;
      link.rel = 'noreferrer noopener';
      link.textContent = `${dep.owner}/${dep.repo}`;
      repo.append(link);

      const stars = document.createElement('td');
      stars.className = 'num';
      stars.textContent = dep.stars.toLocaleString();

      const version = document.createElement('td');
      version.className = 'mono';
      version.textContent = dep.version || '—';

      const relationship = document.createElement('td');
      const pill = document.createElement('span');
      // Border style carries the state as well as colour, so the
      // distinction survives colour-vision deficiency and greyscale.
      pill.className = `pill ${dep.relationship}`;
      pill.textContent = dep.relationship;
      relationship.append(pill);

      tr.append(repo, stars, version, relationship);
      return tr;
    }),
  );
}

function summarise(
  name: string,
  dependents: Dependent[],
  directOnly: boolean,
): string {
  if (dependents.length === 0) {
    return `No repository in the dataset depends on ${name}.`;
  }
  const direct = dependents.filter((d) => d.relationship === 'direct').length;
  if (directOnly) {
    return `${dependents.length} repositories declare ${name}.`;
  }
  return (
    `${dependents.length} dependants on ${name} — ` +
    `${direct} declare it, ${dependents.length - direct} inherit it.`
  );
}

// --------------------------------------------------------------------- ask

function wireAsk(dataset: Dataset): void {
  const form = el<HTMLFormElement>('ask-form');
  const question = el<HTMLInputElement>('question');
  const button = form.querySelector('button');
  const trace = el('trace');
  const answer = el('answer');

  const note = (text: string, className = '') => {
    const line = document.createElement('div');
    if (className) line.className = className;
    line.textContent = text;
    trace.append(line);
  };

  const agent = new Agent(dataset, {
    onThinking: (text) => note(text.split('\n')[0] ?? ''),
    onToolCall: (name, input) => note(`${name}(${JSON.stringify(input)})`),
  });

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const asked = question.value.trim();
    if (!asked) return;

    trace.replaceChildren();
    answer.textContent = '';
    answer.className = 'answer';
    if (button) button.disabled = true;

    void agent
      .ask(asked)
      .then((text) => {
        answer.textContent = text;
      })
      .catch((error: unknown) => {
        answer.className = 'answer error';
        answer.textContent =
          error instanceof AgentError || error instanceof Error
            ? error.message
            : 'The question could not be answered.';
      })
      .finally(() => {
        if (button) button.disabled = false;
      });
  });
}

function describeDataset(manifest: Manifest): void {
  const repos = manifest.rowCounts['repositories'] ?? 0;
  const artifacts = manifest.rowCounts['artifacts'] ?? 0;
  const bytes = manifest.files.reduce((sum, file) => sum + file.bytes, 0);
  el('dataset-info').textContent =
    `${repos.toLocaleString()} repositories · ` +
    `${artifacts.toLocaleString()} dependency records · ` +
    `${(bytes / 1e6).toFixed(1)} MB queried in your browser · ` +
    `schema v${manifest.schemaVersion} · ${manifest.generator}`;
}

void main();
