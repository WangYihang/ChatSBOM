/**
 * What the page loads first, what it loads when it needs it, and what
 * it never loads (#44).
 *
 * The client was one chunk of 359 kB. The Ask panel's agent loop and
 * the charts only the query view draws were downloaded and parsed
 * before the overview could draw, and the tools' descriptions, which
 * only the Worker sends to the model, went to every visitor. Vite
 * splits the client at each `import()`, so what lands in which chunk
 * follows from the imports: these read them, from the page's entry.
 *
 * An import erased from the build, `import type`, is not followed.
 * Every other one is, whatever it imports: with `verbatimModuleSyntax`
 * an import of types alone, written without `import type`, still loads
 * its module.
 */
import ts from 'typescript';
import { describe, expect, it } from 'vitest';

/** Every module's source, by path. */
const SOURCES = import.meta.glob('../src/**/*.{ts,tsx}', {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>;

const ENTRY = '../src/main.tsx';

/** What a module imports: to run (`now`), with `import()` (`later`), and from packages. */
interface Imports {
  now: string[];
  later: string[];
  packages: string[];
}

/** A relative specifier as the path of the module it names, or null for a stylesheet. */
function resolve(from: string, specifier: string): string | null {
  if (/\.css$/.test(specifier)) return null;
  const base = new URL(specifier, `file:///${from.slice('../'.length)}`).pathname;
  for (const candidate of [base, `${base}.ts`, `${base}.tsx`, `${base}/index.ts`]) {
    if (`..${candidate}` in SOURCES) return `..${candidate}`;
  }
  // A module this cannot find would be a gap in the graph, not a leaf.
  throw new Error(`${from}: cannot resolve ${specifier}`);
}

function importsOf(path: string): Imports {
  const file = ts.createSourceFile(path, SOURCES[path]!, ts.ScriptTarget.Latest, true);
  const found: Imports = { now: [], later: [], packages: [] };
  const add = (specifier: string, when: 'now' | 'later') => {
    if (!specifier.startsWith('.')) {
      found.packages.push(specifier);
      return;
    }
    const target = resolve(path, specifier);
    if (target) found[when].push(target);
  };
  const visit = (node: ts.Node) => {
    if (ts.isImportDeclaration(node) && !node.importClause?.isTypeOnly) {
      add((node.moduleSpecifier as ts.StringLiteral).text, 'now');
    } else if (
      ts.isExportDeclaration(node) &&
      node.moduleSpecifier &&
      !node.isTypeOnly
    ) {
      add((node.moduleSpecifier as ts.StringLiteral).text, 'now');
    } else if (
      ts.isCallExpression(node) &&
      node.expression.kind === ts.SyntaxKind.ImportKeyword &&
      node.arguments[0] &&
      ts.isStringLiteral(node.arguments[0])
    ) {
      add(node.arguments[0].text, 'later');
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return found;
}

/** Every module the page reaches from its entry: before it draws, or ever. */
function reach(later: boolean): { modules: Set<string>; packages: Set<string> } {
  const modules = new Set<string>();
  const packages = new Set<string>();
  const queue = [ENTRY];
  while (queue.length > 0) {
    const path = queue.pop()!;
    if (modules.has(path)) continue;
    modules.add(path);
    const found = importsOf(path);
    for (const name of found.packages) packages.add(name);
    queue.push(...found.now, ...(later ? found.later : []));
  }
  return { modules, packages };
}

/** The modules in `modules` whose source declares an export matching `pattern`. */
const declaring = (modules: Set<string>, pattern: RegExp) =>
  [...modules].filter((path) => pattern.test(SOURCES[path]!));

describe('the page', () => {
  const first = reach(false);
  const ever = reach(true);

  it('is read whole: its entry, its views and the dictionary', () => {
    // A graph that stopped at the entry would pass everything below.
    for (const path of [
      '../src/app.tsx',
      '../src/components/Overview.tsx',
      '../src/components/QueryView.tsx',
      '../src/i18n/strings.tsx',
    ]) {
      expect(first.modules).toContain(path);
    }
  });

  it("never loads the model's instructions or the tools' descriptions", () => {
    // The Worker sends them with every turn; the page has no use for
    // them, and a visitor was sent every word.
    expect(
      declaring(ever.modules, /export const (SYSTEM_PROMPT|TOOL_DEFINITIONS)\b/),
    ).toEqual([]);
    expect(ever.packages).not.toContain('@anthropic-ai/sdk');
  });

  it('loads the agent loop only once the Ask panel is drawn', () => {
    const agent = /export class Agent\b|export (async )?function (useAsk|turnstileSolver|executeTool)\b/;
    expect(declaring(first.modules, agent)).toEqual([]);
    // Loaded later, not lost.
    expect(declaring(ever.modules, agent)).toHaveLength(4);
  });

  it('loads the charts only the query view draws once it draws them', () => {
    const charts = /export function (DependencyTree|TimeSeries)\b/;
    expect(declaring(first.modules, charts)).toEqual([]);
    expect(declaring(ever.modules, charts)).toHaveLength(2);
  });
});
