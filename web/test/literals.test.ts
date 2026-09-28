/**
 * No words written straight into the markup (#43).
 *
 * The dictionary (`i18n/strings.tsx`) is typed so that a word missing in
 * one language is a compile error — but only for words that go through
 * it. A sentence typed into JSX goes around it, and the page then says
 * it in English whichever language was chosen. The loading note, the
 * tree's bound and the empty panels had been translated, and the markup
 * beside them went on saying the same thing in English.
 *
 * So every `.tsx` file outside the dictionary is read here, and any
 * text in its markup is reported: text between tags, a string or
 * template given to a prop that is shown or read out, and a string that
 * is a child expression. Strings built elsewhere in the file — a
 * tooltip's lines, a legend's entries — are not markup, and
 * `chinese.test.tsx` catches those by rendering the page.
 */
import ts from 'typescript';
import { describe, expect, it } from 'vitest';

/** Every component's source, by path. */
const SOURCES = import.meta.glob('../src/**/*.tsx', {
  query: '?raw',
  import: 'default',
  eager: true,
});

/** The props and attributes whose value a reader sees or hears. */
const SHOWN = new Set([
  'alt',
  'aria-description',
  'aria-label',
  'aria-placeholder',
  'aria-roledescription',
  'aria-valuetext',
  'caption',
  'label',
  'message',
  'note',
  'partLabel',
  'placeholder',
  'qualifier',
  'snapshotNote',
  'title',
  'valueLabel',
  'xLabel',
]);

/**
 * Markup text that is not a word in any language, with the reason.
 *
 * Kept short on purpose: an entry is a claim that the text reads the
 * same to every reader.
 */
const ALLOWED: Record<string, string> = {
  // The product's name, drawn as `Chat<b>SBOM</b>`. A name is not
  // translated.
  Chat: 'the product name',
  SBOM: 'the product name',
};

/**
 * Whether `text` holds a word: a letter of the Latin alphabet, once
 * character references — `&hellip;`, `&nbsp;` — are taken out.
 */
const wordy = (text: string) =>
  /[A-Za-z]/.test(text.replace(/&(?:[a-z]+|#\d+|#x[\da-f]+);/gi, ' ')) &&
  !(text.trim() in ALLOWED);

/** The literal text of a string or template, its substitutions left out. */
function literalText(node: ts.Node): string | null {
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
    return node.text;
  }
  if (ts.isTemplateExpression(node)) {
    return [node.head.text, ...node.templateSpans.map((span) => span.literal.text)].join('${…}');
  }
  return null;
}

/**
 * The strings an expression can evaluate to, through the forms markup
 * uses to choose one: `a ? 'x' : 'y'`, `a || 'x'`, `(…)`.
 */
function strings(node: ts.Expression): ts.Node[] {
  if (literalText(node) !== null) return [node];
  if (ts.isParenthesizedExpression(node)) return strings(node.expression);
  if (ts.isConditionalExpression(node)) {
    return [...strings(node.whenTrue), ...strings(node.whenFalse)];
  }
  if (ts.isBinaryExpression(node)) {
    return [...strings(node.left), ...strings(node.right)];
  }
  return [];
}

/** Every piece of markup text in `source`, as `file:line: "text"`. */
function markupText(path: string, source: string): string[] {
  const file = ts.createSourceFile(path, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const found: string[] = [];
  const report = (node: ts.Node, text: string) => {
    const { line } = file.getLineAndCharacterOfPosition(node.getStart(file));
    found.push(`${path.replace('../src/', '')}:${line + 1}: ${JSON.stringify(text.trim())}`);
  };

  const visit = (node: ts.Node) => {
    if (ts.isJsxText(node) && wordy(node.text)) {
      report(node, node.text);
    } else if (ts.isJsxAttribute(node) && SHOWN.has(node.name.getText(file))) {
      const value = node.initializer;
      const candidates =
        value === undefined
          ? []
          : ts.isStringLiteral(value)
            ? [value]
            : ts.isJsxExpression(value) && value.expression
              ? strings(value.expression)
              : [];
      for (const candidate of candidates) {
        const text = literalText(candidate)!;
        if (wordy(text)) report(candidate, text);
      }
    } else if (
      ts.isJsxExpression(node) &&
      node.expression &&
      (ts.isJsxElement(node.parent) || ts.isJsxFragment(node.parent))
    ) {
      for (const candidate of strings(node.expression)) {
        const text = literalText(candidate)!;
        if (wordy(text)) report(candidate, text);
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return found;
}

describe('the markup', () => {
  const components = Object.entries(SOURCES).filter(
    // The dictionary is where the words are meant to be.
    ([path]) => !path.startsWith('../src/i18n/'),
  );

  it('reads every component, so a new one cannot slip past', () => {
    // The files the issue named, at least: a glob that matched nothing
    // would pass everything.
    const paths = components.map(([path]) => path);
    for (const expected of [
      '../src/app.tsx',
      '../src/ask/Placeholder.tsx',
      '../src/charts/DependencyTree.tsx',
      '../src/charts/Frame.tsx',
      '../src/charts/Plots.tsx',
      '../src/charts/RankedBars.tsx',
      '../src/charts/SourceShares.tsx',
      '../src/components/ErrorBoundary.tsx',
      '../src/components/Overview.tsx',
      '../src/components/PackageSearch.tsx',
      '../src/components/QueryView.tsx',
    ]) {
      expect(paths).toContain(expected);
    }
  });

  it('says nothing in words of its own: every word comes from the dictionary', () => {
    const found = components.flatMap(([path, source]) => markupText(path, source));
    expect(found).toEqual([]);
  });

  it('notices a word typed into markup', () => {
    // The scan itself, against the forms it has to catch.
    const source = [
      'const a = <p>Loading the dataset&hellip;</p>;',
      'const b = <Empty message={`No package pulled in by ${root}.`} />;',
      'const c = <Histogram xLabel="dependencies" />;',
      'const d = <p>{failed ? message : `Reading ${name}…`}</p>;',
      'const e = <h1>Chat<b>SBOM</b> {"·"} &mdash;</h1>;',
      'const f = <g className="row" data-row="x" textAnchor="end" />;',
    ].join('\n');
    expect(markupText('../src/example.tsx', source)).toEqual([
      'example.tsx:1: "Loading the dataset&hellip;"',
      'example.tsx:2: "No package pulled in by ${…}."',
      'example.tsx:3: "dependencies"',
      'example.tsx:4: "Reading ${…}…"',
    ]);
  });
});
