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
 * Worker against D1.
 */
import type Anthropic from '@anthropic-ai/sdk';

import type { DatasetClient } from './d1/client';
import type { DependentQuery } from './dataset/types';
import { RELATIONSHIPS } from './schema';

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
 * Tool definitions sent to the API. Kept in one place so the Worker and
 * the page cannot disagree about what exists.
 *
 * A description is what the model believes about a result, so it has to
 * say what the store does. A search described as matching substrings,
 * that matched prefixes, let the model conclude a package did not exist
 * when it had only begun the name differently. Each `default` below is
 * the store's own — a tool passes no limit when the model names none —
 * and test/tools.test.ts checks them against both stores.
 */
export const TOOL_DEFINITIONS = [
  {
    name: 'dependents_of',
    description:
      'Count the repositories that depend on a package, and list the most ' +
      'starred of them. `total` is how many repositories depend on it: the ' +
      'real count, whatever the number of rows. `direct_total` is how many ' +
      'of those declare it in their own manifest; it is left out when ' +
      'direct_only already makes `total` that number. `rows` is a sample, ' +
      'most starred first, with a row per repository, version and ' +
      'relationship, so a repository can appear more than once; never ' +
      'report its length as a count. Set direct_only when the question is ' +
      'about projects that chose the package themselves, rather than ' +
      'inheriting it through another dependency — of the 118 repositories ' +
      'whose SBOM lists `mail`, only 17 declare it.',
    input_schema: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
          description: 'Exact package name, as its ecosystem spells it.',
        },
        type: {
          type: 'string',
          description:
            'Ecosystem to scope to, spelled as ecosystems_for reports it, ' +
            'e.g. gem, npm, maven. A name is not unique across ecosystems: ' +
            '`mail` is a Ruby gem with 118 dependants and also a Maven ' +
            'artifactId with 6.',
        },
        language: {
          type: 'string',
          description:
            "Optional filter on the repository's GitHub language, " +
            'lowercased, e.g. ruby: one of the twelve language_coverage ' +
            'lists, or `other`, or `none`. It is not the package\'s ' +
            'ecosystem; use `type` for that.',
        },
        direct_only: {
          type: 'boolean',
          description:
            'Only repositories whose own manifest declares it, in the rows ' +
            'and in `total`.',
        },
        limit: {
          type: 'integer',
          description:
            `Rows to list, default 50, at most ${RESULT_ROWS}. It changes ` +
            'the sample, never `total`.',
        },
      },
      required: ['name'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'ecosystems_for',
    description:
      'Which ecosystems a package name appears in, with counts. Call ' +
      'this before reporting a dependant count: a name shared across ' +
      'ecosystems is two different packages, and summing them reports ' +
      'dependants of something that does not exist.',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Exact package name.' },
      },
      required: ['name'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'search_packages',
    description:
      'Find package names that begin with a prefix, ranked by how many ' +
      'repositories use them. Matching is by prefix, not substring: `mail` ' +
      'would find `mailer` but never `actionmailer`, so a name that does ' +
      'not turn up may only begin differently. A name in several ecosystems ' +
      'can come back as a row for each. Use this first when unsure of the ' +
      'exact name.',
    input_schema: {
      type: 'object',
      properties: {
        fragment: {
          type: 'string',
          description:
            'The start of the name, as its ecosystem spells it; matched as ' +
            'a prefix.',
        },
        limit: {
          type: 'integer',
          description: `Names to return, default 20, at most ${RESULT_ROWS}.`,
        },
      },
      required: ['fragment'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'top_packages',
    description:
      'The most depended-upon packages. Prefer direct_only: the unfiltered ' +
      'ranking is dominated by npm micro-packages (semver, debug, ms) that ' +
      'no project asks for by name. The ranking is stored only so deep, so ' +
      'it can end before `limit` does.',
    input_schema: {
      type: 'object',
      properties: {
        ecosystem: {
          type: 'string',
          description:
            'Optional ecosystem to rank within, e.g. npm, maven, pypi, ' +
            'composer, go, gem, cargo. Omitted, the ranking is the whole ' +
            'corpus, each repository counted once.',
        },
        direct_only: {
          type: 'boolean',
          description: 'Count only declared dependencies.',
        },
        limit: {
          type: 'integer',
          description: `Max rows, default 50, at most ${RESULT_ROWS}.`,
        },
      },
      required: [],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'version_spread',
    description:
      'Which resolved versions of a package are in use, most used first, ' +
      'with the repositories on each. Answers "are projects on the current ' +
      'release". A version recorded as a manifest constraint (`>= 2.0`) or ' +
      'not recorded at all is not listed: `constrained` and `unversioned` ' +
      'count what was set aside. Not scoped to an ecosystem, so a name used ' +
      'in two mixes their versions.',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Exact package name.' },
        limit: {
          type: 'integer',
          description: `Max versions, default 10, at most ${RESULT_ROWS}.`,
        },
      },
      required: ['name'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'language_coverage',
    description:
      "Per GitHub language (the twelve most common, `other` and `none`): " +
      'repositories in the snapshot, and how many have dependency data ' +
      'from each collector. Call this before comparing languages: ' +
      'coverage is uneven, so a raw cross-language count is not a ' +
      'like-for-like comparison.',
    input_schema: {
      type: 'object',
      properties: {},
      required: [],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'ecosystem_coverage',
    description:
      'Per ecosystem (npm, maven, pypi, …): repositories whose manifests ' +
      'or dependencies are of it, and how many each collector covers. A ' +
      'repository counts under every ecosystem it has, so the rows ' +
      'overlap and must not be summed. Call this before comparing ' +
      'ecosystems.',
    input_schema: {
      type: 'object',
      properties: {},
      required: [],
      additionalProperties: false,
    },
    strict: true,
  },
] as const satisfies readonly Anthropic.Tool[];

export type ToolName = (typeof TOOL_DEFINITIONS)[number]['name'];

const TOOL_NAMES: readonly string[] = TOOL_DEFINITIONS.map((t) => t.name);

export function isToolName(name: string): name is ToolName {
  return TOOL_NAMES.includes(name);
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

export const SYSTEM_PROMPT = [
  'You answer questions about open-source dependency data using the tools',
  'provided. You have no other access to the dataset — there is no SQL',
  'escape hatch, so compose the tools instead.',
  '',
  'The dataset is a snapshot of public GitHub repositories: each has an',
  'SBOM, and each dependency is labelled by how it arrived:',
  `  ${RELATIONSHIPS.join(' | ')}`,
  '',
  '`direct` means the project\'s own manifest declares the package;',
  '`transitive` means another dependency pulled it in; `unknown` means no',
  'manifest could be read, so the question is unanswered rather than',
  'answered "no". This distinction usually decides which number a person',
  'actually wants — "who uses X" nearly always means direct.',
  '',
  'Two cautions to pass on rather than hide:',
  '- A package name is not unique across ecosystems. `mail` is a Ruby gem',
  '  with 118 dependants and also a Maven artifactId with 6; summing them',
  '  reports 124 dependants of something that does not exist. Call',
  '  ecosystems_for first when a name could be ambiguous, and say which',
  '  ecosystem a number refers to.',
  '- Versions may be constraints (`>= 0`) rather than resolutions, when',
  '  they came from a manifest instead of a lockfile.',
  '- Coverage differs by language and by ecosystem. Call language_coverage',
  '  or ecosystem_coverage before any such comparison and state the',
  '  denominators: every repository in the snapshot, collected or not.',
  '- A repository can have several ecosystems (an npm front end and a',
  '  Maven back end), so per-ecosystem repository counts overlap: never',
  '  add them up to get a corpus total.',
  '',
  'Rows are samples, never counts. A result holds at most',
  `${RESULT_ROWS} rows and ${RESULT_CHARS} characters: \`rows_shown\` says`,
  'how many it holds, and `truncated` with `rows_dropped` says when some',
  'were cut to fit. A number of dependants comes from `total` or',
  '`direct_total`, never from how many rows came back.',
  '',
  'Be concise. Give the numbers you found, name the repositories when',
  'there are few enough to list, and say plainly when the data cannot',
  'answer the question.',
].join('\n');
