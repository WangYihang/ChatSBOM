/**
 * The tools the model may call.
 *
 * Parameterised functions, never SQL. The model can name a function and
 * its arguments; it cannot ask for a statement to be executed, because
 * no such tool exists. That is the containment: a model that could pass
 * SQL — or a visitor who could talk one into it — could pass any SQL.
 *
 * Each tool is made of methods the query client already has, so the
 * vocabulary here is exactly the vocabulary the dashboard's own controls
 * have. A question cannot reach data the UI could not.
 *
 * The page executes the calls and posts the results back for the next
 * turn, so the loop is client-side. The queries themselves run in the
 * Worker, against ClickHouse or D1, whichever the deployment configures.
 *
 * This is the part of the tools that both sides load. What the model is
 * told about them, and the system prompt, are in `prompt.ts`, which only
 * the Worker loads: it sends them with every turn, and the page, which
 * never sends them, never downloads them (#44).
 */
import type { DatasetClient } from './d1/client';
import type { DependentQuery } from './dataset/types';

/**
 * What the Worker accepts back from the page (`chat.ts`), counted as the
 * page sends it: the length of one tool result's JSON, and of all the
 * text in a conversation together.
 *
 * Declared here rather than there because this is the file the Worker
 * and the page share. A cap the page set against its own copy of these
 * numbers would hold only until someone changed the other copy.
 */
export const MAX_TOOL_RESULT_CHARS = 160 * 1024;
export const MAX_CONVERSATION_CHARS = 200_000;

/**
 * How long one result may be, as the model receives it: a tenth of a
 * conversation.
 *
 * Set against the conversation's bound rather than the result's, because
 * that is the one a real conversation runs into. Every result is sent
 * again with each turn after it, and two `dependents_of` calls at 500
 * rows — over 100,000 characters each — were enough to have the next
 * turn refused. At a tenth, eight results at the cap still leave room
 * for the questions and answers around them. Fifty dependants, the
 * default, come to about 12,000 characters.
 */
export const RESULT_CHARS = MAX_CONVERSATION_CHARS / 10;

/**
 * And no more rows than this, whatever `limit` asks for.
 *
 * Rows are there to be named in an answer, and no answer names a
 * hundred; "how many" is answered by a count, never by rows. A limit is
 * clamped to this before it reaches the store, so the store is not
 * asked for rows only for them to be cut.
 */
export const RESULT_ROWS = 100;

/**
 * The tools there are, by name.
 *
 * Listed here, where the Worker and the page both read it: the Worker
 * checks a conversation's tool calls against it (`chat.ts`), and the
 * page runs them. What the model is told about each is the Worker's
 * alone, in `prompt.ts`, typed against this list, so the two cannot
 * disagree about what exists.
 */
export const TOOL_NAMES = [
  'dependents_of',
  'ecosystems_for',
  'search_packages',
  'top_packages',
  'version_spread',
  'language_coverage',
  'ecosystem_coverage',
] as const;

export type ToolName = (typeof TOOL_NAMES)[number];

export function isToolName(name: string): name is ToolName {
  return (TOOL_NAMES as readonly string[]).includes(name);
}

/**
 * What every tool returns: whatever it counted, then a sample of rows.
 *
 * `rows_shown` is how many rows there are, and `truncated` with
 * `rows_dropped` appear only when some were cut to fit. A tool's counts —
 * `total`, `constrained` — come first, where the model reads first.
 */
export type ToolResult = Record<string, unknown> & {
  rows_shown: number;
  truncated?: true;
  rows_dropped?: number;
  rows: unknown[];
};

/** One tool's answer, before it is bounded. */
interface Answer {
  [field: string]: unknown;
  rows: readonly unknown[];
}

/* Guard rails applied to whatever the model passes. */

function asString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function asBool(value: unknown): boolean {
  return value === true;
}

function asLimit(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value)) return undefined;
  return Math.min(Math.max(Math.floor(value), 1), RESULT_ROWS);
}

/**
 * Run one tool call against the dataset.
 *
 * Inputs arrive from a model, so every field is re-validated here rather
 * than trusted from the schema — `strict: true` constrains shape, not
 * range, and the query layer is the last line before the data. Outputs
 * are bounded on the way back, for every tool alike.
 */
export async function executeTool(
  dataset: DatasetClient,
  name: string,
  input: unknown,
): Promise<ToolResult> {
  if (!isToolName(name)) {
    throw new Error(`unknown tool: ${name}`);
  }
  const { rows, ...counts } = await answer(
    dataset,
    name,
    (input ?? {}) as Record<string, unknown>,
  );
  return bounded(counts, rows);
}

async function answer(
  dataset: DatasetClient,
  name: ToolName,
  args: Record<string, unknown>,
): Promise<Answer> {
  switch (name) {
    case 'dependents_of': {
      const pkg = asString(args['name']);
      if (!pkg) throw new Error('dependents_of requires a package name');
      const filters: DependentQuery = {
        name: pkg,
        ...(asString(args['type']) ? { type: asString(args['type'])! } : {}),
        ...(asString(args['language']) ? { language: asString(args['language'])! } : {}),
        directOnly: asBool(args['direct_only']),
      };
      const limit = asLimit(args['limit']);
      // Counted, never measured. The rows stop at a limit and come one
      // per repository, version and relationship, so their length is
      // neither the dependant count nor bounded by it: 50 for `react`,
      // which 5,095 repositories depend on. The counts take the rows'
      // own filters, as the dashboard's table does, and run beside them.
      const [rows, total, directTotal] = await Promise.all([
        dataset.dependentsOf(limit ? { ...filters, limit } : filters),
        dataset.countDependents(filters),
        filters.directOnly
          ? undefined
          : dataset.countDependents({ ...filters, directOnly: true }),
      ]);
      return {
        total,
        ...(directTotal === undefined ? {} : { direct_total: directTotal }),
        rows,
      };
    }

    case 'ecosystems_for': {
      const pkg = asString(args['name']);
      if (!pkg) throw new Error('ecosystems_for requires a package name');
      return { rows: await dataset.ecosystemsFor(pkg) };
    }

    case 'search_packages': {
      const fragment = asString(args['fragment']);
      if (!fragment) throw new Error('search_packages requires a fragment');
      return { rows: await dataset.searchPackages(fragment, asLimit(args['limit'])) };
    }

    case 'top_packages':
      return {
        rows: await dataset.topPackages({
          ...(asString(args['ecosystem']) ? { ecosystem: asString(args['ecosystem'])! } : {}),
          directOnly: asBool(args['direct_only']),
          ...(asLimit(args['limit']) ? { limit: asLimit(args['limit'])! } : {}),
        }),
      };

    case 'version_spread': {
      const pkg = asString(args['name']);
      if (!pkg) throw new Error('version_spread requires a package name');
      const spread = await dataset.versionSpread(pkg, asLimit(args['limit']));
      // The versions are its rows, like any other tool's, so the same
      // bound applies to them; what was set aside stays beside them.
      return {
        constrained: spread.constrained,
        unversioned: spread.unversioned,
        rows: spread.versions,
      };
    }

    case 'language_coverage':
      return { rows: await dataset.languageCoverage() };

    case 'ecosystem_coverage':
      return { rows: await dataset.ecosystemCoverage() };
  }
}

/**
 * An answer as the model receives it: its counts, then as many rows as
 * fit in RESULT_ROWS and RESULT_CHARS.
 *
 * Applied to every tool rather than to the one that prompted it. Any of
 * them can outgrow a conversation, and not even a limit bounds them all:
 * on ClickHouse a search comes back as a row per ecosystem.
 *
 * Rows are cut from the end, so the head of each ranking survives, and a
 * cut is always declared. A result that lost rows silently would be read
 * as complete, which is the mistake of reading rows as a count, made one
 * step earlier.
 */
function bounded(
  counts: Record<string, unknown>,
  rows: readonly unknown[],
): ToolResult {
  const keep = (shown: number): ToolResult => ({
    ...counts,
    rows_shown: shown,
    ...(shown < rows.length
      ? { truncated: true as const, rows_dropped: rows.length - shown }
      : {}),
    rows: rows.slice(0, shown),
  });
  const fits = (result: ToolResult): boolean =>
    JSON.stringify(result).length <= RESULT_CHARS;

  let most = Math.min(rows.length, RESULT_ROWS);
  const whole = keep(most);
  if (fits(whole)) return whole;

  // The longest head that fits. Another row never shortens the result,
  // so bisection finds it; and the counts beside the rows are a few
  // numbers, so no rows at all always fits.
  let least = 0;
  while (most - least > 1) {
    const middle = Math.floor((least + most) / 2);
    if (fits(keep(middle))) least = middle;
    else most = middle;
  }
  return keep(least);
}
