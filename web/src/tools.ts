/**
 * The tool surface the model is given, and the only way it can reach data.
 *
 * The dataset lives in the browser (DuckDB-WASM over Parquet), not on the
 * Worker, so the Worker cannot execute these — it relays `tool_use` blocks
 * to the page, the page runs them against DuckDB, and the results go back.
 * A useful consequence: the Worker never sees query results, and the data
 * never leaves the client.
 *
 * Every tool is a *parameterised function*, never a SQL string. A model
 * that can only choose a function and its arguments cannot turn a prompt
 * into a query plan, which is what made the local TUI unsuitable for a
 * public deployment.
 */
import type Anthropic from '@anthropic-ai/sdk';

import type { Dataset } from './queries';
import { RELATIONSHIPS } from './schema';

/** Tool definitions sent to the API. Kept in one place so the Worker and
 *  the page cannot disagree about what exists. */
export const TOOL_DEFINITIONS = [
  {
    name: 'dependents_of',
    description:
      'List the repositories that depend on a package, most starred first. ' +
      'Set direct_only when the question is about projects that chose the ' +
      'package themselves, rather than inheriting it through another ' +
      'dependency — of the 118 repositories whose SBOM lists `mail`, only ' +
      '17 declare it.',
    input_schema: {
      type: 'object',
      properties: {
        name: {
          type: 'string',
          description: 'Exact package name, as its ecosystem spells it.',
        },
        language: {
          type: 'string',
          description: 'Optional repository language filter, e.g. ruby.',
        },
        direct_only: {
          type: 'boolean',
          description: 'Only repositories whose own manifest declares it.',
        },
        limit: { type: 'integer', description: 'Max rows, default 50.' },
      },
      required: ['name'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'search_packages',
    description:
      'Find package names matching a fragment, ranked by how many ' +
      'repositories use them. Use this first when unsure of the exact name.',
    input_schema: {
      type: 'object',
      properties: {
        fragment: { type: 'string', description: 'Substring to match.' },
        limit: { type: 'integer', description: 'Max rows, default 50.' },
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
      'no project asks for by name.',
    input_schema: {
      type: 'object',
      properties: {
        language: { type: 'string', description: 'Optional language filter.' },
        direct_only: {
          type: 'boolean',
          description: 'Count only declared dependencies.',
        },
        limit: { type: 'integer', description: 'Max rows, default 50.' },
      },
      required: [],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'version_spread',
    description:
      'How a package\'s versions are distributed across repositories, most ' +
      'used first. Answers "are projects on the current release".',
    input_schema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Exact package name.' },
        limit: { type: 'integer', description: 'Max rows, default 50.' },
      },
      required: ['name'],
      additionalProperties: false,
    },
    strict: true,
  },
  {
    name: 'language_coverage',
    description:
      'Per-language repository counts and how many produced a usable SBOM. ' +
      'Call this before comparing languages: coverage is uneven, so a raw ' +
      'cross-language count is not a like-for-like comparison.',
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

/** Guard rails applied to whatever the model passes. */
const MAX_LIMIT = 500;

function asString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function asBool(value: unknown): boolean {
  return value === true;
}

function asLimit(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value)) return undefined;
  return Math.min(Math.max(Math.floor(value), 1), MAX_LIMIT);
}

/**
 * Run one tool call against the dataset.
 *
 * Inputs arrive from a model, so every field is re-validated here rather
 * than trusted from the schema — `strict: true` constrains shape, not
 * range, and the query layer is the last line before the data.
 */
export async function executeTool(
  dataset: Dataset,
  name: string,
  input: unknown,
): Promise<unknown> {
  if (!isToolName(name)) {
    throw new Error(`unknown tool: ${name}`);
  }
  const args = (input ?? {}) as Record<string, unknown>;

  switch (name) {
    case 'dependents_of': {
      const pkg = asString(args['name']);
      if (!pkg) throw new Error('dependents_of requires a package name');
      const rows = await dataset.dependentsOf({
        name: pkg,
        ...(asString(args['language']) ? { language: asString(args['language'])! } : {}),
        directOnly: asBool(args['direct_only']),
        ...(asLimit(args['limit']) ? { limit: asLimit(args['limit'])! } : {}),
      });
      return {
        count: rows.length,
        direct: rows.filter((r) => r.relationship === 'direct').length,
        rows,
      };
    }

    case 'search_packages': {
      const fragment = asString(args['fragment']);
      if (!fragment) throw new Error('search_packages requires a fragment');
      return { rows: await dataset.searchPackages(fragment, asLimit(args['limit'])) };
    }

    case 'top_packages':
      return {
        rows: await dataset.topPackages({
          ...(asString(args['language']) ? { language: asString(args['language'])! } : {}),
          directOnly: asBool(args['direct_only']),
          ...(asLimit(args['limit']) ? { limit: asLimit(args['limit'])! } : {}),
        }),
      };

    case 'version_spread': {
      const pkg = asString(args['name']);
      if (!pkg) throw new Error('version_spread requires a package name');
      return { rows: await dataset.versionSpread(pkg, asLimit(args['limit'])) };
    }

    case 'language_coverage':
      return { rows: await dataset.languageCoverage() };
  }
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
  '- Versions may be constraints (`>= 0`) rather than resolutions, when',
  '  they came from a manifest instead of a lockfile.',
  '- SBOM coverage differs by language. Call language_coverage before any',
  '  cross-language comparison and state the denominators.',
  '',
  'Be concise. Give the numbers you found, name the repositories when',
  'there are few enough to list, and say plainly when the data cannot',
  'answer the question.',
].join('\n');
