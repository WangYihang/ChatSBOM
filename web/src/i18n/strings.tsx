/**
 * Every word the page says, in both languages.
 *
 * One dictionary per locale, both satisfying the same interface, so a
 * missing translation is a type error rather than a blank panel. That
 * is the whole reason for the shape: a `Record<string, string>` would
 * let `zh` drift behind `en` silently, and the failure would surface
 * to a reader rather than to a build.
 *
 * Values are `ReactNode` or functions returning one, not plain
 * strings. The emphasis in this copy carries meaning — *actually*
 * declares, **not refreshed** — and flattening it to text would lose
 * the argument the page is making. The cost is that a translation is
 * markup, which is the honest cost of translating markup.
 *
 * Numbers are formatted by the caller, not here: `formatNumber` needs
 * the locale and the dictionary has no business knowing how a count was
 * rounded. So a count arrives as the string to print.
 */
import type { ReactNode } from 'react';

import type { AskFailure } from '../ask/stream';
import type { Locale } from './locale';

export interface Dictionary {
  /* ---- the document's own ---- */
  /**
   * The page's title, which a tab and a bookmark show, and the
   * description a search result does (#123). `index.html` carries the
   * English, word for word, for a reader the script has not reached yet
   * and a crawler that runs none.
   */
  documentTitle: string;
  documentDescription: string;

  /* ---- the masthead, and the controls in it ---- */
  tagline: ReactNode;
  viewGroup: string;
  viewOverview: string;
  viewQuery: string;
  themeGroup: string;
  themeLight: string;
  themeDark: string;
  themeSystem: string;
  localeGroup: string;

  /* ---- the four headline numbers ---- */
  tileRepositories: string;
  tileRecords: string;
  tilePackages: string;
  tileClassified: string;

  /* ---- boot and failure ---- */
  loading: ReactNode;
  /**
   * What stands in for a part of the page whose code is still on its
   * way: the Ask panel, and the query view's tree and time series,
   * which the page loads when it first draws them (#44). And for a
   * chart whose answer is (`Answered`, #123).
   */
  loadingPart: string;
  /** The error boundary's message, and its one way back. */
  boundaryFailed: string;
  boundaryBack: string;
  /**
   * A question the service refused, by the status it refused it with.
   *
   * `said` is the sentence it was refused with, in English: the
   * service's, or the page's where the service wrote none. English says
   * a failure as it was written where it happened, and it is tested
   * there. Chinese says what the status means, and keeps the English
   * beside it only where one status stands for several of the service's
   * sentences: then only the sentence says which (#43).
   */
  queryRefused: (status: number, said: string) => string;
  /** A question that failed on its way, or in the page. */
  queryFailed: (said: string) => string;

  /* ---- the footer ---- */
  observedSpan: (from: string, to: string) => string;
  observedUnknown: string;
  schemaLabel: string;
  /**
   * The link to the weekly Parquet export's manifest (#154), and what
   * follows it: what the export is, and `query`, which reads one of
   * its files where the service serves it.
   */
  downloadDataset: string;
  downloadNote: (query: ReactNode) => ReactNode;
  /**
   * That query, of a file at `origin`, the page's own address. SQL's
   * words are SQL's in either language; the file is the reader's to
   * name, from the manifest.
   */
  downloadQuery: (origin: string) => string;

  /* ---- the metadata panel ---- */
  metaTitle: string;
  metaQualifier: string;
  metaNote: ReactNode;
  /** The snapshot's id, which a reader cites (#165). */
  metaSnapshot: string;
  metaGenerator: string;
  metaSchema: string;
  metaObserved: string;
  metaRepositories: string;
  metaTracked: string;
  metaRecords: string;
  metaPackages: string;
  metaClassified: string;
  metaReading: string;

  /* ---- the overview's panels ---- */
  splitTitle: string;
  splitQualifier: string;
  splitNote: ReactNode;
  splitLabel: string;
  /** A bar's tooltip: its share, and the three counts behind it. */
  splitDetail: (
    percent: string,
    declared: string,
    inherited: string,
    records: string,
  ) => string[];
  rankingTitleDeclared: string;
  rankingTitleAll: string;
  rankingQualifierDeclared: string;
  rankingQualifierAll: string;
  rankingNote: ReactNode;
  rankingLabelDeclared: string;
  rankingLabelAll: string;
  rankingDetail: (dependants: string, declared: string) => string[];
  /** "8,000 repositories", as a tooltip's first line. */
  repositoryCount: (count: string) => string;
  coverageTitle: string;
  coverageNote: ReactNode;
  coverageLabel: string;
  coveragePartLabel: string;
  coverageBarTitle: (withSbom: string, percent: number) => string;
  /** One detail line per collector, for either coverage panel. */
  coverageSources: (
    syft: string,
    depgraph: string,
    manifest: string,
  ) => string[];
  ecosystemCoverageTitle: string;
  ecosystemCoverageNote: ReactNode;
  ecosystemCoveragePartLabel: string;
  ecosystemCoverageBarTitle: (withSyft: string, percent: number) => string;
  bucketsTitle: string;
  bucketsNote: ReactNode;
  bucketsLabel: string;
  /** The histogram's x axis. */
  bucketsAxis: string;
  licencesTitle: string;
  licencesNote: ReactNode;
  licencesLabel: string;
  licenceUnknown: string;
  licencePackages: (count: string) => string;
  sourcesTitle: string;
  sourcesQualifier: string;
  sourcesNote: ReactNode;
  /** What the panel's totals count: its table's last column. */
  sourcesLabel: string;
  sourcesChartLabel: string;
  /** The three collectors, as a tooltip names them… */
  sourceNames: Readonly<Record<'syft' | 'depgraph' | 'manifest', string>>;
  /** …and as the legend does, with what each one reads. */
  sourceLegend: Readonly<Record<'syft' | 'depgraph' | 'manifest', string>>;
  sourceRows: (collector: string, rows: string) => string;
  sourceShare: (percent: string) => string;
  declaredOnly: string;
  languageFilter: string;
  languageAll: string;
  heroDeclared: string;
  heroInherited: string;
  heroUndetermined: string;
  heroLabel: string;
  /**
   * The hero sentence, assembled by the dictionary rather than by the
   * component.
   *
   * Chinese puts the count first — 「N 条记录中，P 是被动继承的」 —
   * against English's "P of N records are inherited", so a template
   * with holes in fixed positions cannot express both. The emphasis
   * around the percentage is passed in as a node for the same reason:
   * which word carries the weight is a property of the sentence.
   */
  heroLede: (
    percent: ReactNode,
    records: string,
    outcome: string,
  ) => ReactNode;
  heroWhy: ReactNode;

  /* ---- the query view ---- */
  searchPlaceholder: string;
  searchAriaLabel: string;
  searchNothingNamed: (term: string) => string;
  searchExact: string;
  ecosystemFilter: string;
  ecosystemAll: (count: number) => string;
  /** The ranking's ecosystem filter, unset: the whole corpus. */
  ecosystemAny: string;
  statusPrompt: string;
  statusSearching: (name: string) => string;
  statusNone: (name: string) => string;
  /**
   * `total` is passed beside `subject` rather than parsed back out of
   * it. The first version read the count off the formatted string with
   * `/^1 /`, which is a regex against output this same dictionary had
   * just produced — it worked and would have broken the moment the
   * number format changed.
   */
  statusDeclaredOnly: (
    subject: string,
    qualified: string,
    shown: number | null,
    total: number,
  ) => string;
  statusSplit: (
    subject: string,
    qualified: string,
    direct: number,
    inherited: number,
    shown: number | null,
  ) => string;
  countRepository: string;
  countRepositoryPlural: string;
  /**
   * Both forms, always. The default rule appends `s`, which put
   * 「326 个依赖方s」 on the page — and a comment two files away
   * saying Chinese has no plural did not stop it, because the
   * call site simply omitted the argument.
   */
  countDependant: string;
  countDependantPlural: string;
  tableRepository: string;
  tableStars: string;
  tableVersion: string;
  tableDepends: string;
  tableScanned: string;
  tableEcosystem: string;
  tableLanguage: string;
  /** "declared in 4 manifests", on the row that stands for all four. */
  manifestCount: (n: number) => string;
  pagePrevious: string;
  pageNext: string;
  /** "rows 101–200 of 492" — the rows, which are not the dependants. */
  pageRange: (from: string, to: string, total: string) => string;
  relationshipDirect: string;
  relationshipTransitive: string;
  relationshipUnknown: string;
  versionsTitle: string;
  /** The versions chart's name, and its table's caption (#123). */
  versionsLabel: (name: string) => string;
  versionsNote: ReactNode;
  /**
   * Either clause appears only when its count is non-zero, and which
   * of them is present changes the punctuation — so the dictionary
   * assembles the sentence rather than filling holes in a template.
   */
  versionsNotCounted: (
    ranges: string | null,
    none: string | null,
  ) => ReactNode;
  adoptionTitle: string;
  adoptionLabel: (name: string) => string;
  adoptionNote: ReactNode;
  adoptionSnapshot: ReactNode;
  adoptionEmpty: string;
  /** A point's tooltip: repositories, and how many of them declared it. */
  adoptionPoint: (repositories: string, declared: string) => string[];
  /** The heads of the series' table (`ChartTable`). */
  adoptionColumns: Readonly<
    Record<'source' | 'month' | 'repositories' | 'declared', string>
  >;
  pullsInTitle: string;
  pullsInNote: ReactNode;
  /**
   * The tree's bound, and why the unbounded graph is not a bigger copy
   * of it. `largest` is null until the store has said, and the clause
   * that quotes it goes with it.
   */
  pullsInBounded: (
    children: number,
    branch: number,
    largest: string | null,
  ) => string;
  pullsInEmpty: (name: string) => string;
  pullsInReading: (name: string) => string;
  /** The tree's accessible name. */
  pullsInLabel: (root: string) => string;
  pullsInEdge: (repositories: string) => string;
  /** The root's tooltip: what it is, and how much it pulls in. */
  pullsInRoot: string;
  pullsInRootChildren: (packages: string) => string;
  pullsInChild: (root: string, repositories: string) => string;
  pullsInLeaf: string;
  pullsInOpen: string;
  pullsInLegendChild: string;
  pullsInLegendLeaf: string;
  /** The heads of the tree's table: one row an edge. */
  pullsInColumns: Readonly<Record<'package' | 'parent' | 'repositories', string>>;
  pulledInTitle: string;
  /** Its chart's name, and its table's caption (#123). */
  pulledInLabel: (name: string) => string;
  pulledInNote: (name: string) => ReactNode;
  edgeCaveatPlain: string;
  edgeCaveatMeasured: (
    ambiguousNames: string,
    names: string,
    ambiguousEdges: string,
    edges: string,
    share: number,
  ) => string;
  askTitle: string;
  askNote: ReactNode;
  askButton: string;
  askAsking: string;
  /**
   * Why a question got no answer, by the code the service said it with
   * (`chatsbom/server/ask.py`, #144), or the page's own: each in the
   * page's words. The service's English is kept only where it says what
   * the code does not: which part of a question was wrong. A code the
   * page does not know is said as the service said it.
   */
  askUnanswered: (failure: AskFailure) => string;
  /** Any other failure, with what it said, if anything. */
  askFailed: (said: string) => string;
  askQuestionLabel: string;
  askSuggestDeclared: (name: string) => string;
  askSuggestVersions: (name: string) => string;
  askNewConversation: string;
  /** Before the packages an answer looked up, each a way to its view (#123). */
  askPackages: string;
  noDataForSelection: string;
  /** What a bar's part is called when its chart does not say. */
  chartPart: string;
  /** A chart table's column for what the tooltips add. */
  chartDetails: string;
  /** A chart table's column for each part's share of the whole. */
  chartShare: string;
  /**
   * What a mark that opens nothing is called, for a screen reader to
   * announce when it takes focus: its tooltip, as one line (#123).
   */
  chartMark: (title: string, lines: readonly string[]) => string;
  /** Its tooltip's last line, from the keyboard: how to reach the others. */
  chartKeys: string;
}

const EN: Dictionary = {
  documentTitle: 'ChatSBOM · who actually declares a dependency',
  documentDescription:
    'Who actually declares a dependency, across 28,000 open-source repositories.',

  tagline: (
    <>
      Who <em>actually</em> declares a dependency &mdash; not who merely
      inherits one.
    </>
  ),
  viewGroup: 'View',
  viewOverview: 'Overview',
  viewQuery: 'Query',
  themeGroup: 'Theme',
  themeLight: 'Light',
  themeDark: 'Dark',
  themeSystem: 'System',
  localeGroup: 'Language',

  tileRepositories: 'repositories with dependency data',
  tileRecords: 'dependency records',
  tilePackages: 'distinct packages',
  tileClassified: '% classified',

  loading: <>Loading the dataset&hellip;</>,
  loadingPart: 'Loading…',
  boundaryFailed: 'This page could not be drawn.',
  boundaryBack: 'Back to the overview',
  // Each as the service wrote it.
  queryRefused: (_status, said) => said,
  queryFailed: (said) => said || 'The query failed.',

  observedSpan: (from, to) => `observed ${from} to ${to}`,
  observedUnknown: 'observation span unknown',
  schemaLabel: 'schema',
  downloadDataset: 'Download the dataset',
  downloadNote: (query) => (
    <>
      {' '}&mdash; a Parquet file per table, each named in the manifest,
      which DuckDB reads over HTTP without fetching all of it: {query}
    </>
  ),
  downloadQuery: (origin) => `SELECT * FROM '${origin}/export/<file>'`,

  metaTitle: 'Dataset metadata',
  metaQualifier: 'for debugging what you are looking at',
  metaNote: (
    <>
      Observation dates are when <em>this</em> pipeline recorded a
      repository&rsquo;s dependencies. Star counts and push dates come from
      repository metadata collected earlier and are{' '}
      <strong>not refreshed</strong> by a dependency rescan, so a row can
      legitimately show a recent scan beside an older push.
    </>
  ),
  metaSnapshot: 'Snapshot',
  metaGenerator: 'Generator',
  metaSchema: 'Schema',
  metaObserved: 'Observed',
  metaRepositories: 'Repositories with dependency data',
  metaTracked: 'Repositories in the snapshot',
  metaRecords: 'Dependency records',
  metaPackages: 'Distinct packages',
  metaClassified: 'Classified',
  metaReading: 'Reading provenance…',

  splitTitle: 'Declared or inherited, by ecosystem',
  splitQualifier: "share of each ecosystem's dependency records",
  splitNote: (
    <>
      The band above gives one figure for the whole corpus. Asked per
      ecosystem the answer is not one number, and the spread is the
      point: it is the difference between a lockfile that resolves a
      deep npm tree and one that does not. Bars are the declared share,
      so an ecosystem with few records is comparable with one that has
      millions. Keyed by the package&rsquo;s ecosystem, not the
      repository&rsquo;s language: a Maven backend in a repository
      GitHub calls TypeScript is counted under Maven.
    </>
  ),
  splitLabel: 'declared',
  splitDetail: (percent, declared, inherited, records) => [
    `${percent}% declared`,
    `${declared} declared`,
    `${inherited} inherited`,
    `${records} records in total`,
  ],

  rankingTitleDeclared: 'Most declared packages',
  rankingTitleAll: 'Most depended-on packages',
  rankingQualifierDeclared: 'by repositories that declare them',
  rankingQualifierAll:
    'by repositories that depend on them, declared or inherited',
  rankingNote: (
    <>
      Unfiltered, this ranking is <code>semver</code>, <code>debug</code>,{' '}
      <code>ms</code> &mdash; npm utilities nobody chooses by name.
    </>
  ),
  rankingLabelDeclared: 'repositories declaring it',
  rankingLabelAll: 'repositories',
  rankingDetail: (dependants, declared) => [
    `${dependants} dependants`,
    `${declared} declared it`,
  ],
  repositoryCount: (count) => `${count} repositories`,

  coverageTitle: 'Coverage by GitHub language',
  coverageNote: (
    <>
      The denominators: every repository in the current search
      snapshot, collected or not &mdash; not only the ones that were
      scanned. GitHub&rsquo;s language is shown as the twelve most
      common and <code>other</code>. Coverage is uneven, so a raw
      cross-language count is not a like-for-like comparison &mdash;
      read this before any ranking.
    </>
  ),
  coverageLabel: 'repositories',
  coveragePartLabel: 'with dependency data',
  coverageBarTitle: (withSbom, percent) =>
    `${withSbom} with dependency data (${percent}%)`,
  coverageSources: (syft, depgraph, manifest) => [
    `${syft} with a Syft scan`,
    `${depgraph} with a dependency graph`,
    `${manifest} with Gradle declarations`,
  ],
  ecosystemCoverageTitle: 'Coverage by ecosystem',
  ecosystemCoverageNote: (
    <>
      Repositories whose manifests or dependencies are of each
      ecosystem, and how many of them a lockfile scan resolved. A
      repository counts under every ecosystem it has, so these bars
      overlap and do not add up to the snapshot. Choose one to rank its
      packages.
    </>
  ),
  ecosystemCoveragePartLabel: 'resolved by Syft',
  ecosystemCoverageBarTitle: (withSyft, percent) =>
    `${withSyft} resolved by a Syft scan (${percent}%)`,

  bucketsTitle: 'Dependencies per repository',
  bucketsNote: (
    <>
      Bucketed: the spread covers three orders of magnitude, a Go module
      with 80 next to a TypeScript app with 900.
    </>
  ),
  bucketsLabel: 'Repositories by dependency count',
  bucketsAxis: 'dependencies',

  licencesTitle: 'Licences',
  licencesNote: (
    <>
      Unknown is shown rather than dropped: &ldquo;we do not know&rdquo;
      is a finding about SBOM quality, and hiding it would overstate
      coverage.
    </>
  ),
  licencesLabel: 'repositories',
  licenceUnknown: '(unknown)',
  licencePackages: (count) => `${count} distinct packages`,

  sourcesTitle: 'Where the data came from',
  sourcesQualifier: 'share of rows per ecosystem',
  sourcesNote: (
    <>
      Syft reads lockfiles; GitHub&rsquo;s dependency graph parses
      manifests; Gradle build files, which neither reads, are parsed
      for what they declare. Shown as each ecosystem&rsquo;s own split,
      with its absolute total, because the row counts span four orders
      of magnitude &mdash; on a shared scale every ecosystem but npm is
      an invisible sliver.
    </>
  ),
  // This carried the hero chart's name, word for word — nothing about
  // collectors — and nothing asked for it.
  sourcesLabel: 'dependency records',
  sourcesChartLabel:
    'Share of dependency records per ecosystem, by collector',
  sourceNames: {
    syft: 'Syft',
    depgraph: 'Dependency graph',
    manifest: 'Gradle declarations',
  },
  sourceLegend: {
    syft: 'Syft · lockfiles',
    depgraph: 'Dependency graph · manifests',
    manifest: 'Gradle build files · declared',
  },
  sourceRows: (collector, rows) => `${collector}: ${rows} rows`,
  sourceShare: (percent) => `${percent}% of this ecosystem`,

  declaredOnly: 'Declared only',
  languageFilter: 'Language',
  languageAll: 'all',

  heroDeclared: 'declared outright',
  heroInherited: 'inherited, not chosen',
  heroUndetermined: 'undetermined',
  heroLabel: 'How dependencies arrived, across the whole corpus',
  heroLede: (percent, records, outcome) => (
    <>
      {percent} of {records} dependency records are {outcome}.
    </>
  ),
  heroWhy: (
    <>
      Which is why an unfiltered &ldquo;most-used package&rdquo; ranking
      measures lockfile size rather than adoption.
    </>
  ),

  searchPlaceholder: 'laravel, express, spring-boot-starter-web…',
  searchAriaLabel: 'Package name',
  searchNothingNamed: (term) => `Nothing is named ${term}. These are:`,
  searchExact: 'exact',
  ecosystemFilter: 'Ecosystem',
  ecosystemAll: (count) => `all ${count} ecosystems`,
  ecosystemAny: 'all',

  statusPrompt: 'Type a package name, or pick one from the overview.',
  statusSearching: (name) => `Searching for ${name}…`,
  statusNone: (name) => `No repository in the dataset depends on ${name}.`,
  statusDeclaredOnly: (subject, qualified, shown, total) => {
    const declares = total === 1 ? 'declares' : 'declare';
    return shown === null
      ? `${subject} ${declares} ${qualified}.`
      : `${subject} ${declares} ${qualified}; `
        + `the ${shown} most-starred are shown.`;
  },
  statusSplit: (subject, qualified, direct, inherited, shown) => {
    const split =
      `${direct} ${direct === 1 ? 'declares' : 'declare'} it, ` +
      `${inherited} ${inherited === 1 ? 'inherits' : 'inherit'} it`;
    const scope = shown === null ? '' : ` among the ${shown} shown`;
    return `${subject} on ${qualified} — ${split}${scope}.`;
  },
  countRepository: 'repository',
  countRepositoryPlural: 'repositories',
  countDependant: 'dependant',
  countDependantPlural: 'dependants',

  tableRepository: 'Repository',
  tableStars: 'Stars',
  tableVersion: 'Version',
  tableDepends: 'Depends',
  tableScanned: 'Scanned',
  tableEcosystem: 'Ecosystem',
  tableLanguage: 'Language',
  manifestCount: (n) => `×${n} manifests`,
  pagePrevious: 'Previous',
  pageNext: 'Next',
  pageRange: (from, to, total) => `rows ${from}–${to} of ${total}`,
  relationshipDirect: 'direct',
  relationshipTransitive: 'transitive',
  relationshipUnknown: 'unknown',

  versionsTitle: 'Versions in use',
  versionsLabel: (name) => `Versions of ${name} in use`,
  versionsNote: (
    <>
      Resolved versions only, so a bar is a version somebody is actually
      running rather than a range a manifest permits.
    </>
  ),
  versionsNotCounted: (ranges, none) => (
    <>
      Not counted above:{' '}
      {ranges ? `${ranges} rows give a range rather than a version` : null}
      {ranges && none ? ', ' : ranges ? '. ' : null}
      {none ? `${none} give none at all. ` : null}
      GitHub&rsquo;s dependency graph reports what a manifest declares,
      which is not always a resolution.
    </>
  ),

  adoptionTitle: 'Adoption over time',
  adoptionLabel: (name) => `Monthly adoption of ${name}`,
  adoptionNote: (
    <>
      Repositories per collection, per source. Declared counts are in the
      tooltip.
    </>
  ),
  adoptionSnapshot: (
    <>
      One observation per source, which is a snapshot rather than a trend
      &mdash; and the two were taken months apart by different tools, so
      the gap between them is not a change in adoption. A second run of
      either gives that line a direction.
    </>
  ),
  adoptionEmpty: 'No history yet — it accumulates as the queue runs.',
  adoptionPoint: (repositories, declared) => [
    `${repositories} repositories`,
    `${declared} declared it`,
  ],
  adoptionColumns: {
    source: 'source',
    month: 'month',
    repositories: 'repositories',
    declared: 'declaring it',
  },

  pullsInTitle: 'What it pulls in',
  pullsInNote: (
    <>
      Two hops, widest edges first. A column is one hop and stroke width
      is the number of repositories showing that pair.
    </>
  ),
  pullsInBounded: (children, branch, largest) =>
    `Bounded to ${children} packages and ${branch} per package. The `
    + 'unbounded graph is not a smaller version of this'
    + (largest === null
      ? '.'
      : `: the largest repository here has ${largest} dependencies.`),
  pullsInEmpty: (name) => `No package pulled in by ${name} is recorded.`,
  pullsInReading: (name) => `Reading the edge table for ${name}…`,
  pullsInLabel: (root) =>
    `Packages ${root} pulls in, two hops, thickness by repository count`,
  pullsInEdge: (repositories) => `${repositories} repositories show this pair`,
  pullsInRoot: 'The package asked about',
  pullsInRootChildren: (packages) => `${packages} packages pulled in directly`,
  pullsInChild: (root, repositories) =>
    `Pulled in by ${root} in ${repositories} repositories`,
  pullsInLeaf: 'Second hop — pulled in by the package to its left',
  // Said for focus as well as the pointer now, so it names both.
  pullsInOpen: 'Click, or press Enter, to open this package',
  pullsInLegendChild: 'pulled in directly',
  pullsInLegendLeaf: 'second hop',
  pullsInColumns: {
    package: 'package',
    parent: 'pulled in by',
    repositories: 'repositories',
  },

  pulledInTitle: 'What pulls it in',
  pulledInLabel: (name) => `Packages that pull ${name} in`,
  pulledInNote: (name) => (
    <>
      Why {name} is in a lockfile nobody added it to. Repositories in
      which each package pulls it in.
    </>
  ),

  edgeCaveatPlain:
    'Edges are aggregated by package name, which is not unique across '
    + 'ecosystems. The filters above do not reach this panel.',
  edgeCaveatMeasured: (
    ambiguousNames,
    names,
    ambiguousEdges,
    edges,
    share,
  ) =>
    'Edges are aggregated by package name, which is not unique across '
    + `ecosystems: ${ambiguousNames} of ${names} names appear in more than `
    + `one, and they carry ${ambiguousEdges} of ${edges} edges — ${share}%. `
    + 'The filters above do not reach this panel.',

  askTitle: 'Ask a question',
  askNote: (
    <>
      Answered by a model whose only tools are the same typed queries this
      page uses &mdash; it cannot write SQL or reach the database. Your
      question, your earlier questions in this conversation and their
      answers, and the rows those queries return, are sent to DeepSeek to
      produce the answer.
    </>
  ),
  askButton: 'Ask',
  askAsking: 'Asking…',
  askUnanswered: (failure) => {
    switch (failure.code) {
      case 'off':
        return 'AI answers are not configured on this deployment.';
      case 'origin':
        return "Questions must be asked from this site's own page.";
      case 'json':
        return 'The question was not sent as JSON.';
      case 'size':
        return 'The question and the conversation before it are too long. Start a new conversation.';
      case 'rate':
        return 'Too many questions. Wait a moment.';
      case 'invalid':
        // Which part was wrong, as the service said it.
        return failure.said || 'The question was not understood.';
      case 'busy':
        return 'AI answers are busy. Try again in a moment.';
      case 'verification-required':
        return 'Human verification is required. Reload and retry.';
      case 'verification-failed':
        switch (failure.verdict) {
          case 'expired':
            return 'Human verification ran out of time before the question was sent. Ask again.';
          case 'replayed':
            return 'Human verification had been used already. Ask again.';
          case 'other client':
            return 'Human verification was for another address. Ask again.';
          default:
            return 'Human verification failed. Reload and retry.';
        }
      case 'budget':
        return 'The daily budget for AI answers is used up. The dashboard itself still works.';
      case 'unavailable':
        return 'AI answers are unavailable for a moment. Try again shortly.';
      case 'model':
        return 'The model could not be reached. Try again shortly.';
      case 'timeout':
        return 'The model took too long to answer. Try again shortly.';
      case 'cut-off':
        return 'The answer was cut off at its length limit before it finished. Try a narrower question.';
      case 'declined':
        return 'The model declined to answer this question.';
      case 'stopped':
        return `The model stopped without an answer (${failure.reason ?? 'no reason given'}).`;
      case 'garbled':
        return 'The answer was not understood.';
      case 'turns':
        return `Gave up after ${failure.turns ?? 'too many'} turns without a final answer.`;
      case 'failed':
        return 'Something unexpected failed, and the question was not answered. Try again shortly.';
      case 'unverified':
        return 'The human verification check could not be completed. Reload and retry.';
      case 'interrupted':
        return 'The answer stopped arriving before it was finished. Ask again.';
      case 'refused':
        return `The question was refused (${failure.status ?? 'no status'}). Try again shortly.`;
      default:
        return failure.said || 'The question could not be answered.';
    }
  },
  askFailed: (said) => said || 'The question could not be answered.',
  askQuestionLabel: 'Question',
  askSuggestDeclared: (name) =>
    `Which projects declare ${name} rather than inheriting it?`,
  askSuggestVersions: (name) => `What versions of ${name} are in use?`,
  askNewConversation: 'New conversation',
  askPackages: 'Open a package it looked up:',

  noDataForSelection: 'No data for this selection.',
  chartPart: 'part',
  chartDetails: 'details',
  chartShare: 'share',
  chartMark: (title, lines) => [title, lines.join(', ')].filter(Boolean).join(': '),
  chartKeys: 'Arrow keys for the others',
};

const ZH: Dictionary = {
  documentTitle: 'ChatSBOM · 谁真正声明了依赖',
  documentDescription: '在 28,000 个开源仓库中，谁真正声明了一个依赖。',

  tagline: (
    <>
      谁<em>真正</em>声明了一个依赖 &mdash; 而不是谁只是继承了它。
    </>
  ),
  viewGroup: '视图',
  viewOverview: '总览',
  viewQuery: '查询',
  themeGroup: '主题',
  themeLight: '浅色',
  themeDark: '深色',
  themeSystem: '跟随系统',
  localeGroup: '语言',

  // "有依赖数据的仓库" rather than "仓库": the count is the 24,449 that
  // carry dependency rows, not the 28,075 in the corpus, and the bare
  // word was wrong in English for the same reason.
  tileRepositories: '有依赖数据的仓库',
  tileRecords: '依赖记录',
  tilePackages: '去重包数',
  tileClassified: '% 已分类',

  loading: <>正在加载数据集&hellip;</>,
  loadingPart: '正在加载…',
  boundaryFailed: '这个页面没能显示出来。',
  boundaryBack: '回到总览',
  queryRefused: (status, said) => {
    switch (status) {
      case 410:
        // A snapshot gone, and gone again once `meta` was asked for the
        // current one (#144): the page asks nothing more by itself.
        return '数据集刚刚更新了，请刷新页面。';
      case 413:
        return '这个查询太大了。';
      case 429:
        return '查询太频繁了，请稍等片刻再试。';
      case 500:
        return '这个查询没能得到回答。';
      // No dataset configured, or none readable for a moment (#144): a
      // deployment that cannot answer, either way.
      case 503:
        return '这个部署现在无法回答查询。';
      default:
        // A 400 is one of a dozen refusals, each naming what was wrong.
        return `查询被拒绝（${status}）：${said}`;
    }
  },
  queryFailed: (said) => (said ? `查询没能完成：${said}` : '查询没能完成。'),

  observedSpan: (from, to) => `观测区间 ${from} 至 ${to}`,
  observedUnknown: '观测区间未知',
  schemaLabel: '模式',
  downloadDataset: '下载数据集',
  downloadNote: (query) => (
    <>
      {' '}&mdash; 每张表一个 Parquet 文件，文件名见 manifest；DuckDB
      可以通过 HTTP 直接读取，不必整个下载：{query}
    </>
  ),
  downloadQuery: (origin) => `SELECT * FROM '${origin}/export/<文件>'`,

  metaTitle: '数据集元信息',
  metaQualifier: '用于确认你正在看的是什么',
  metaNote: (
    <>
      观测日期是<em>本流水线</em>记录某个仓库依赖的时间。星标数和推送时间来自更早一次的仓库元数据采集，
      <strong>不会</strong>因为重新扫描依赖而刷新 &mdash;
      所以一行里出现「最近扫描」和「较早推送」并存是合理的。
    </>
  ),
  metaSnapshot: '快照',
  metaGenerator: '生成器',
  metaSchema: '模式',
  metaObserved: '观测',
  metaRepositories: '有依赖数据的仓库',
  metaTracked: '快照中的仓库',
  metaRecords: '依赖记录',
  metaPackages: '去重包数',
  metaClassified: '已分类',
  metaReading: '正在读取来源信息…',

  splitTitle: '按生态看：声明还是继承',
  splitQualifier: '各生态依赖记录中主动声明的占比',
  splitNote: (
    <>
      上方色带给出的是全语料库的单一数字。按生态分别提问，答案不是一个数
      &mdash; 差距本身才是重点：它是「lockfile 解析出一整棵 npm 依赖树」
      和「不解析」之间的差别。条形画的是声明占比，所以记录数只有几千的生态
      可以和上百万的生态直接比较。按包所属的生态统计，而不是仓库的语言：
      一个被 GitHub 标为 TypeScript 的仓库里的 Maven 后端，算在 Maven 下。
    </>
  ),
  splitLabel: '主动声明',
  splitDetail: (percent, declared, inherited, records) => [
    `${percent}% 主动声明`,
    `${declared} 条主动声明`,
    `${inherited} 条被动继承`,
    `共 ${records} 条记录`,
  ],

  rankingTitleDeclared: '最常被主动声明的包',
  rankingTitleAll: '最多仓库依赖的包',
  rankingQualifierDeclared: '按主动声明它的仓库数',
  rankingQualifierAll: '按依赖它的仓库数，含主动声明和被动继承',
  rankingNote: (
    <>
      不加过滤时，这个排名是 <code>semver</code>、<code>debug</code>、
      <code>ms</code> &mdash; 没有人按名字挑选的 npm 工具包。
    </>
  ),
  rankingLabelDeclared: '主动声明它的仓库',
  rankingLabelAll: '仓库',
  rankingDetail: (dependants, declared) => [
    `${dependants} 个依赖方`,
    `其中 ${declared} 个主动声明`,
  ],
  repositoryCount: (count) => `${count} 个仓库`,

  coverageTitle: '按 GitHub 语言看覆盖率',
  coverageNote: (
    <>
      这是分母：当前搜索快照中的全部仓库，无论是否已采集 &mdash;
      而不只是扫描过的那些。GitHub 语言只列出最常见的十二种，其余归入
      <code>other</code>。覆盖率并不均匀，所以跨语言直接比较绝对数
      并不是同等条件的比较 &mdash; 请先读这一格，再读排名。
    </>
  ),
  coverageLabel: '仓库',
  coveragePartLabel: '有依赖数据',
  coverageBarTitle: (withSbom, percent) =>
    `${withSbom} 个有依赖数据（${percent}%）`,
  coverageSources: (syft, depgraph, manifest) => [
    `${syft} 个有 Syft 扫描`,
    `${depgraph} 个有依赖图`,
    `${manifest} 个有 Gradle 声明`,
  ],
  ecosystemCoverageTitle: '按生态看覆盖率',
  ecosystemCoverageNote: (
    <>
      manifest 或依赖属于该生态的仓库数，以及其中由 lockfile 扫描解析出的
      数量。一个仓库会计入它拥有的每个生态，所以这些条形互相重叠，
      加起来不等于快照总数。选择一个生态可查看它的包排名。
    </>
  ),
  ecosystemCoveragePartLabel: '由 Syft 解析',
  ecosystemCoverageBarTitle: (withSyft, percent) =>
    `${withSyft} 个由 Syft 扫描解析（${percent}%）`,

  bucketsTitle: '每个仓库的依赖数',
  bucketsNote: (
    <>
      分桶显示：跨度有三个数量级 &mdash; 一个 80 个依赖的 Go 模块，
      和一个 900 个依赖的 TypeScript 应用并列。
    </>
  ),
  bucketsLabel: '按依赖数分布的仓库',
  bucketsAxis: '依赖数',

  licencesTitle: '授权协议',
  licencesNote: (
    <>
      「未知」是显示出来而不是丢弃的：「我们不知道」本身就是关于 SBOM
      质量的一项发现，隐藏它会让覆盖率显得比实际更好。
    </>
  ),
  licencesLabel: '仓库',
  licenceUnknown: '（未知）',
  licencePackages: (count) => `${count} 个不同的包`,

  sourcesTitle: '数据来自哪里',
  sourcesQualifier: '各生态的行数占比',
  sourcesNote: (
    <>
      Syft 读 lockfile；GitHub 的依赖图解析 manifest；两者都不读的 Gradle
      构建文件，则解析其中声明的依赖。这里按每个生态各自的比例显示，
      并标出它的绝对总量 &mdash; 因为行数跨越四个数量级，
      放在同一个刻度上时除 npm 以外的每个生态都会细到看不见。
    </>
  ),
  sourcesLabel: '依赖记录',
  sourcesChartLabel: '各生态的依赖记录占比，按采集器区分',
  sourceNames: {
    syft: 'Syft',
    depgraph: '依赖图',
    manifest: 'Gradle 声明',
  },
  sourceLegend: {
    syft: 'Syft · lockfile',
    depgraph: '依赖图 · manifest',
    manifest: 'Gradle 构建文件 · 声明',
  },
  sourceRows: (collector, rows) => `${collector}：${rows} 行`,
  sourceShare: (percent) => `占该生态的 ${percent}%`,

  declaredOnly: '仅主动声明',
  languageFilter: '语言',
  languageAll: '全部',

  heroDeclared: '主动声明',
  heroInherited: '被动继承，而非选择',
  heroUndetermined: '无法判定',
  heroLabel: '依赖是怎么进来的（全语料库）',
  heroLede: (percent, records, outcome) => (
    <>
      {records} 条依赖记录中，有 {percent} 是{outcome}。
    </>
  ),
  heroWhy: (
    <>
      这就是为什么一个不加过滤的「最常用包」排名，衡量的是 lockfile
      的大小，而不是采纳程度。
    </>
  ),

  searchPlaceholder: 'laravel、express、spring-boot-starter-web…',
  searchAriaLabel: '包名',
  searchNothingNamed: (term) => `没有叫 ${term} 的包。以下是相近的：`,
  searchExact: '精确匹配',
  ecosystemFilter: '生态',
  ecosystemAll: (count) => `全部 ${count} 个生态`,
  ecosystemAny: '全部',

  statusPrompt: '输入包名，或从总览页点一个。',
  statusSearching: (name) => `正在查找 ${name}…`,
  statusNone: (name) => `数据集中没有仓库依赖 ${name}。`,
  // Chinese needs no agreement, so the verb is fixed and the count
  // does not change the sentence — which is why these are functions
  // rather than templates with the plural baked in.
  // Chinese needs no agreement, so `total` is unused here — the
  // signature is shared and the English sentence does need it.
  statusDeclaredOnly: (subject, qualified, shown) =>
    shown === null
      ? `${subject}主动声明了 ${qualified}。`
      : `${subject}主动声明了 ${qualified}；此处显示星标最高的 ${shown} 个。`,
  statusSplit: (subject, qualified, direct, inherited, shown) => {
    const split = `其中 ${direct} 个主动声明、${inherited} 个被动继承`;
    const scope = shown === null ? '' : `（在显示的 ${shown} 个之中）`;
    return `${qualified} 有 ${subject} — ${split}${scope}。`;
  },
  countRepository: '个仓库',
  countRepositoryPlural: '个仓库',
  countDependant: '个依赖方',
  countDependantPlural: '个依赖方',

  tableRepository: '仓库',
  tableStars: '星标',
  tableVersion: '版本',
  tableDepends: '依赖方式',
  tableScanned: '扫描时间',
  tableEcosystem: '生态',
  tableLanguage: '语言',
  manifestCount: (n) => `×${n} 个 manifest`,
  pagePrevious: '上一页',
  pageNext: '下一页',
  pageRange: (from, to, total) => `第 ${from}–${to} 行，共 ${total} 行`,
  relationshipDirect: '主动声明',
  relationshipTransitive: '被动继承',
  relationshipUnknown: '未知',

  versionsTitle: '实际在用的版本',
  versionsLabel: (name) => `${name} 实际在用的版本`,
  versionsNote: (
    <>
      只统计解析出的确定版本，所以每一条都是真的有人在跑的版本，
      而不是 manifest 允许的一个范围。
    </>
  ),
  versionsNotCounted: (ranges, none) => (
    <>
      未计入上图：
      {ranges ? `${ranges} 行给出的是范围而非具体版本` : null}
      {ranges && none ? '，' : ranges ? '。' : null}
      {none ? `${none} 行完全没有版本。` : null}
      GitHub 的依赖图报告的是 manifest 声明的内容，而那并不总是一个解析结果。
    </>
  ),

  adoptionTitle: '随时间的采纳情况',
  adoptionLabel: (name) => `${name} 的月度采纳`,
  adoptionNote: (
    <>
      按采集批次、按来源统计的仓库数。主动声明的数量在悬浮提示里。
    </>
  ),
  adoptionSnapshot: (
    <>
      每个来源只有一次观测，所以这是快照而不是趋势 &mdash;
      而且两次观测相隔数月、由不同工具完成，因此两点之间的落差
      并不是采纳程度的变化。任一来源再跑一次，那条线才有方向。
    </>
  ),
  adoptionEmpty: '还没有历史数据 — 它会随着队列的运行逐渐积累。',
  adoptionPoint: (repositories, declared) => [
    `${repositories} 个仓库`,
    `其中 ${declared} 个主动声明`,
  ],
  adoptionColumns: {
    source: '来源',
    month: '月份',
    repositories: '仓库数',
    declared: '主动声明的仓库数',
  },

  pullsInTitle: '它引入了什么',
  pullsInNote: (
    <>
      两跳，最粗的边在前。每一列是一跳，线宽是出现该「父—子」组合的仓库数。
    </>
  ),
  pullsInBounded: (children, branch, largest) =>
    `限制为 ${children} 个包、每个包 ${branch} 个分支。`
    + '完整的图并不是这张图的放大版'
    + (largest === null ? '。' : `：这里最大的仓库有 ${largest} 个依赖。`),
  pullsInEmpty: (name) => `没有记录到 ${name} 引入的任何包。`,
  pullsInReading: (name) => `正在读取 ${name} 的依赖边…`,
  pullsInLabel: (root) => `${root} 引入的包：两跳，线宽表示仓库数`,
  pullsInEdge: (repositories) => `${repositories} 个仓库中出现这一对`,
  pullsInRoot: '当前查询的包',
  pullsInRootChildren: (packages) => `直接引入了 ${packages} 个包`,
  pullsInChild: (root, repositories) =>
    `在 ${repositories} 个仓库中由 ${root} 引入`,
  pullsInLeaf: '第二跳 — 由左边的包引入',
  pullsInOpen: '点击或按回车键打开这个包',
  pullsInLegendChild: '直接引入',
  pullsInLegendLeaf: '第二跳',
  pullsInColumns: {
    package: '包',
    parent: '引入方',
    repositories: '仓库数',
  },

  pulledInTitle: '什么引入了它',
  pulledInLabel: (name) => `引入 ${name} 的包`,
  pulledInNote: (name) => (
    <>
      为什么没人主动添加的 {name} 会出现在 lockfile 里。
      这里列出每个包在多少个仓库中把它引了进来。
    </>
  ),

  edgeCaveatPlain:
    '边是按包名聚合的，而包名跨生态并不唯一。上方的过滤器不作用于这个面板。',
  edgeCaveatMeasured: (
    ambiguousNames,
    names,
    ambiguousEdges,
    edges,
    share,
  ) =>
    '边是按包名聚合的，而包名跨生态并不唯一：'
    + `${names} 个名字里有 ${ambiguousNames} 个属于多个生态，`
    + `它们承载了 ${edges} 条边中的 ${ambiguousEdges} 条 — ${share}%。`
    + '上方的过滤器不作用于这个面板。',

  askTitle: '提问',
  askNote: (
    <>
      由一个模型回答，它能用的工具就是本页面使用的那组类型化查询
      &mdash; 它不能写 SQL，也接触不到数据库。你的问题、这次对话里之前的问题和回答，
      以及这些查询返回的行，会被发送给 DeepSeek 以生成答案。
    </>
  ),
  askButton: '提问',
  askAsking: '正在提问…',
  askUnanswered: (failure) => {
    switch (failure.code) {
      case 'off':
        return '这个部署没有开启智能问答。';
      case 'origin':
        return '问题必须从本站自己的页面提出。';
      case 'json':
        return '问题的发送格式不对。';
      case 'size':
        return '问题和之前的对话太长了，请开始新对话。';
      case 'rate':
        return '提问太频繁了，请稍等片刻。';
      case 'invalid':
        // Which part was wrong: only the service's sentence says.
        return `这个问题没能被接受：${failure.said}`;
      case 'busy':
        return '智能问答正忙，请稍后再试。';
      case 'verification-required':
        return '需要先通过人机验证，请刷新页面后重试。';
      case 'verification-failed':
        switch (failure.verdict) {
          case 'expired':
            return '人机验证在问题发出之前就过期了，请重新提问。';
          case 'replayed':
            return '这次人机验证已经用过了，请重新提问。';
          case 'other client':
            return '这次人机验证属于另一个网络地址，请重新提问。';
          default:
            return '人机验证没有通过，请刷新页面后重试。';
        }
      case 'budget':
        return '今天用于智能问答的预算已经用完了。仪表板本身仍然可以使用。';
      case 'unavailable':
        return '智能问答暂时不可用，请稍后再试。';
      case 'model':
        return '暂时联系不上模型，请稍后再试。';
      case 'timeout':
        return '模型回答得太久了，请稍后再试。';
      case 'cut-off':
        return '回答写到长度上限时被截断了。请把问题问得更具体一些。';
      case 'declined':
        return '模型拒绝回答这个问题。';
      case 'stopped':
        return `模型没有给出回答就停下了（${failure.reason ?? '没有说明原因'}）。`;
      case 'garbled':
        return '没能读懂服务器返回的回答。';
      case 'turns':
        return `经过 ${failure.turns ?? '太多'} 个回合仍没有得到最终回答，已放弃。`;
      case 'failed':
        return '出了意外的错误，这个问题没能被回答。请稍后再试。';
      case 'unverified':
        return '人机验证没能完成，请刷新页面后重试。';
      case 'interrupted':
        return '回答还没写完就中断了，请重新提问。';
      case 'refused':
        return `这个问题被拒绝了（${failure.status ?? '没有状态码'}），请稍后再试。`;
      default:
        return `这个问题没能被回答：${failure.said}`;
    }
  },
  askFailed: (said) => (said ? `这个问题没能被回答：${said}` : '这个问题没能被回答。'),
  askQuestionLabel: '问题',
  askSuggestDeclared: (name) => `哪些项目是主动声明 ${name} 而不是继承来的？`,
  askSuggestVersions: (name) => `${name} 有哪些版本在使用中？`,
  askNewConversation: '新对话',
  askPackages: '打开它查询过的包：',

  noDataForSelection: '该筛选条件下没有数据。',
  chartPart: '部分',
  chartDetails: '详情',
  chartShare: '占比',
  chartMark: (title, lines) => [title, lines.join('，')].filter(Boolean).join('：'),
  chartKeys: '用方向键查看其他各项',
};

export const DICTIONARIES: Readonly<Record<Locale, Dictionary>> = {
  en: EN,
  zh: ZH,
};
