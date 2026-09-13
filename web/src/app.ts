/**
 * Dashboard entry point.
 *
 * Boots DuckDB-WASM against the Parquet dataset and wires the package
 * search. All querying happens here in the browser; the Worker only ever
 * serves bytes.
 */
import './style.css';

import { Agent, AgentError } from './agent';
import { connect, type Manifest } from './duckdb';
import { Dataset, type Dependent } from './queries';

const DEBOUNCE_MS = 250;

function el<T extends HTMLElement>(id: string): T {
  const node = document.getElementById(id);
  if (!node) throw new Error(`missing element #${id}`);
  return node as T;
}

async function main(): Promise<void> {
  const status = el('status');
  const results = el('results');
  const rows = el<HTMLTableSectionElement>('rows');
  const search = el<HTMLInputElement>('package');
  const directOnly = el<HTMLInputElement>('direct-only');

  let dataset: Dataset;
  let manifest: Manifest;
  try {
    const connected = await connect('/data');
    dataset = new Dataset(connected.db, '/data');
    manifest = connected.manifest;
  } catch (error) {
    status.textContent =
      error instanceof Error ? error.message : 'Could not load the dataset.';
    return;
  }

  describeDataset(manifest);
  status.textContent = 'Type a package name to see which projects depend on it.';
  search.focus();

  let pending: number | undefined;
  const run = () => {
    window.clearTimeout(pending);
    pending = window.setTimeout(() => void update(), DEBOUNCE_MS);
  };

  async function update(): Promise<void> {
    const name = search.value.trim();
    if (!name) {
      results.hidden = true;
      status.textContent = 'Type a package name to see which projects depend on it.';
      return;
    }

    status.textContent = `Searching for ${name}…`;
    let dependents: Dependent[];
    try {
      dependents = await dataset.dependentsOf({
        name,
        directOnly: directOnly.checked,
        limit: 100,
      });
    } catch (error) {
      results.hidden = true;
      status.textContent =
        error instanceof Error ? error.message : 'Query failed.';
      return;
    }

    render(rows, dependents);
    results.hidden = dependents.length === 0;
    status.textContent = summarise(name, dependents, directOnly.checked);
  }

  search.addEventListener('input', run);
  directOnly.addEventListener('change', () => void update());

  wireAsk(dataset);
}

/** The agent loop runs here, in the page, because the data is here. */
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
    onThinking: (text) => note(text.split('\n')[0] ?? '', 'thinking'),
    onToolCall: (name, input) =>
      note(`${name}(${JSON.stringify(input)})`, 'tool'),
  });

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const asked = question.value.trim();
    if (!asked) return;

    trace.replaceChildren();
    answer.textContent = '';
    answer.className = '';
    if (button) button.disabled = true;

    void agent
      .ask(asked)
      .then((text) => {
        answer.textContent = text;
      })
      .catch((error: unknown) => {
        answer.className = 'error';
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

function render(body: HTMLTableSectionElement, dependents: Dependent[]): void {
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
      version.textContent = dep.version || '—';

      const relationship = document.createElement('td');
      relationship.className = `rel-${dep.relationship}`;
      relationship.textContent = dep.relationship;

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
  const scope = directOnly ? 'declared' : 'total';
  return (
    `${dependents.length} ${scope} dependant(s) on ${name}` +
    (directOnly ? '' : ` — ${direct} declare it themselves`)
  );
}

function describeDataset(manifest: Manifest): void {
  const repos = manifest.rowCounts['repositories'] ?? 0;
  const artifacts = manifest.rowCounts['artifacts'] ?? 0;
  const bytes = manifest.files.reduce((sum, file) => sum + file.bytes, 0);
  el('dataset-info').textContent =
    `${repos.toLocaleString()} repositories · ` +
    `${artifacts.toLocaleString()} dependency records · ` +
    `${(bytes / 1e6).toFixed(1)} MB · ` +
    `schema v${manifest.schemaVersion} · ${manifest.generator}`;
}

void main();
