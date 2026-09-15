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
 * Numbers are formatted by the caller, not here: `toLocaleString()`
 * needs the locale and the dictionary has no business knowing how a
 * count was rounded.
 */
import type { ReactNode } from 'react';

import type { Locale } from './locale';

export interface Dictionary {
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

  /* ---- the footer ---- */
  observedSpan: (from: string, to: string) => string;
  observedUnknown: string;
  schemaLabel: string;

  /* ---- the metadata panel ---- */
  metaTitle: string;
  metaQualifier: string;
  metaNote: ReactNode;
  metaGenerator: string;
  metaSchema: string;
  metaObserved: string;
  metaRepositories: string;
  metaRecords: string;
  metaPackages: string;
  metaClassified: string;
  metaReading: string;

  /* ---- the overview's panels ---- */
  splitTitle: string;
  splitQualifier: string;
  splitNote: ReactNode;
  splitLabel: string;
  rankingTitleDeclared: string;
  rankingTitleAll: string;
  rankingQualifierDeclared: string;
  rankingQualifierAll: string;
  rankingNote: ReactNode;
  rankingLabelDeclared: string;
  rankingLabelAll: string;
  coverageTitle: string;
  coverageNote: ReactNode;
  coverageLabel: string;
  coveragePartLabel: string;
  coverageBarTitle: (withSbom: string, percent: number) => string;
  bucketsTitle: string;
  bucketsNote: ReactNode;
  bucketsLabel: string;
  licencesTitle: string;
  licencesNote: ReactNode;
  licencesLabel: string;
  licenceUnknown: string;
  sourcesTitle: string;
  sourcesQualifier: string;
  sourcesNote: ReactNode;
  sourcesLabel: string;
  sourcesChartLabel: string;
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
  relationshipDirect: string;
  relationshipTransitive: string;
  relationshipUnknown: string;
  versionsTitle: string;
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
  pullsInTitle: string;
  pullsInNote: ReactNode;
  pullsInBounded: (
    children: number,
    branch: number,
    largest: number,
  ) => ReactNode;
  pullsInEmpty: (name: string) => string;
  pulledInTitle: string;
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
  askFailed: string;
  askQuestionLabel: string;
  askSuggestDeclared: (name: string) => string;
  askSuggestVersions: (name: string) => string;
  noDataForSelection: string;
}

const EN: Dictionary = {
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

  loading: <>Connecting to the dataset&hellip;</>,

  observedSpan: (from, to) => `observed ${from} to ${to}`,
  observedUnknown: 'observation span unknown',
  schemaLabel: 'schema',

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
  metaGenerator: 'Generator',
  metaSchema: 'Schema',
  metaObserved: 'Observed',
  metaRepositories: 'Repositories with dependency data',
  metaRecords: 'Dependency records',
  metaPackages: 'Distinct packages',
  metaClassified: 'Classified',
  metaReading: 'Reading provenance…',

  splitTitle: 'Declared or inherited, by language',
  splitQualifier: "share of each language's dependency records",
  splitNote: (
    <>
      The band above gives one figure for the whole corpus. Asked per
      ecosystem the answer is not one number, and the spread is the
      point: it is the difference between a lockfile that resolves a
      deep npm tree and one that does not. Bars are the declared share,
      so a language with few records is comparable with one that has
      millions.
    </>
  ),
  splitLabel: 'declared',

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

  coverageTitle: 'SBOM coverage by language',
  coverageNote: (
    <>
      The denominators. Coverage is uneven, so a raw cross-language count
      is not a like-for-like comparison &mdash; read this before any
      ranking below.
    </>
  ),
  coverageLabel: 'repositories',
  coveragePartLabel: 'with an SBOM',
  coverageBarTitle: (withSbom, percent) =>
    `${withSbom} with dependency data (${percent}%)`,

  bucketsTitle: 'Dependencies per repository',
  bucketsNote: (
    <>
      Bucketed: the spread covers three orders of magnitude, a Go module
      with 80 next to a TypeScript app with 900.
    </>
  ),
  bucketsLabel: 'Repositories by dependency count',

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

  sourcesTitle: 'Where the data came from',
  sourcesQualifier: 'share of rows per language',
  sourcesNote: (
    <>
      Syft reads lockfiles; GitHub&rsquo;s dependency graph parses
      manifests. Shown as each language&rsquo;s own split, with its
      absolute total, because the row counts span four orders of
      magnitude &mdash; on a shared scale every language but TypeScript
      is an invisible sliver.
    </>
  ),
  sourcesLabel: 'How dependencies arrived, across the whole corpus',
  sourcesChartLabel:
    'Share of dependency records per language, by collector',

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
  relationshipDirect: 'direct',
  relationshipTransitive: 'transitive',
  relationshipUnknown: 'unknown',

  versionsTitle: 'Versions in use',
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

  pullsInTitle: 'What it pulls in',
  pullsInNote: (
    <>
      Two hops, widest edges first. A column is one hop and stroke width
      is the number of repositories showing that pair.
    </>
  ),
  pullsInBounded: (children, branch, largest) => (
    <>
      Bounded to {children} packages and {branch} per package. The
      unbounded graph is not a smaller version of this: the largest
      repository here has {largest.toLocaleString()} dependencies.
    </>
  ),
  pullsInEmpty: (name) => `No package pulled in by ${name} is recorded.`,

  pulledInTitle: 'What pulls it in',
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
      question, and the rows those queries return, are sent to Anthropic
      to produce the answer.
    </>
  ),
  askButton: 'Ask',
  askAsking: 'Asking…',
  askFailed: 'The question could not be answered.',
  askQuestionLabel: 'Question',
  askSuggestDeclared: (name) =>
    `Which projects declare ${name} rather than inheriting it?`,
  askSuggestVersions: (name) => `What versions of ${name} are in use?`,

  noDataForSelection: 'No data for this selection.',
};

const ZH: Dictionary = {
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

  loading: <>正在连接数据集&hellip;</>,

  observedSpan: (from, to) => `观测区间 ${from} 至 ${to}`,
  observedUnknown: '观测区间未知',
  schemaLabel: '模式',

  metaTitle: '数据集元信息',
  metaQualifier: '用于确认你正在看的是什么',
  metaNote: (
    <>
      观测日期是<em>本流水线</em>记录某个仓库依赖的时间。星标数和推送时间来自更早一次的仓库元数据采集，
      <strong>不会</strong>因为重新扫描依赖而刷新 &mdash;
      所以一行里出现「最近扫描」和「较早推送」并存是合理的。
    </>
  ),
  metaGenerator: '生成器',
  metaSchema: '模式',
  metaObserved: '观测',
  metaRepositories: '有依赖数据的仓库',
  metaRecords: '依赖记录',
  metaPackages: '去重包数',
  metaClassified: '已分类',
  metaReading: '正在读取来源信息…',

  splitTitle: '按语言看：声明还是继承',
  splitQualifier: '各语言依赖记录中主动声明的占比',
  splitNote: (
    <>
      上方色带给出的是全语料库的单一数字。按生态分别提问，答案不是一个数
      &mdash; 差距本身才是重点：它是「lockfile 解析出一整棵 npm 依赖树」
      和「不解析」之间的差别。条形画的是声明占比，所以记录数只有几千的语言
      可以和上百万的语言直接比较。
    </>
  ),
  splitLabel: '主动声明',

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

  coverageTitle: '各语言的 SBOM 覆盖率',
  coverageNote: (
    <>
      这是下面所有排名的分母。覆盖率并不均匀，所以跨语言直接比较绝对数
      并不是同等条件的比较 &mdash; 请先读这一格，再读下面的排名。
    </>
  ),
  coverageLabel: '仓库',
  coveragePartLabel: '有 SBOM',
  coverageBarTitle: (withSbom, percent) =>
    `${withSbom} 个有依赖数据（${percent}%）`,

  bucketsTitle: '每个仓库的依赖数',
  bucketsNote: (
    <>
      分桶显示：跨度有三个数量级 &mdash; 一个 80 个依赖的 Go 模块，
      和一个 900 个依赖的 TypeScript 应用并列。
    </>
  ),
  bucketsLabel: '按依赖数分布的仓库',

  licencesTitle: '授权协议',
  licencesNote: (
    <>
      「未知」是显示出来而不是丢弃的：「我们不知道」本身就是关于 SBOM
      质量的一项发现，隐藏它会让覆盖率显得比实际更好。
    </>
  ),
  licencesLabel: '仓库',
  licenceUnknown: '（未知）',

  sourcesTitle: '数据来自哪里',
  sourcesQualifier: '各语言的行数占比',
  sourcesNote: (
    <>
      Syft 读 lockfile；GitHub 的依赖图解析 manifest。这里按每个语言
      各自的比例显示，并标出它的绝对总量 &mdash; 因为行数跨越四个数量级，
      放在同一个刻度上时除 TypeScript 以外的每个语言都会细到看不见。
    </>
  ),
  sourcesLabel: '依赖是怎么进来的（全语料库）',
  sourcesChartLabel: '各语言的依赖记录占比，按采集器区分',

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
  relationshipDirect: '主动声明',
  relationshipTransitive: '被动继承',
  relationshipUnknown: '未知',

  versionsTitle: '实际在用的版本',
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

  pullsInTitle: '它引入了什么',
  pullsInNote: (
    <>
      两跳，最粗的边在前。每一列是一跳，线宽是出现该「父—子」组合的仓库数。
    </>
  ),
  pullsInBounded: (children, branch, largest) => (
    <>
      限制为 {children} 个包、每个包 {branch} 个分支。完整的图并不是这张图的放大版：
      这里最大的仓库有 {largest.toLocaleString()} 个依赖。
    </>
  ),
  pullsInEmpty: (name) => `没有记录到 ${name} 引入的任何包。`,

  pulledInTitle: '什么引入了它',
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
      &mdash; 它不能写 SQL，也接触不到数据库。你的问题，以及这些查询返回的行，
      会被发送给 Anthropic 以生成答案。
    </>
  ),
  askButton: '提问',
  askAsking: '正在提问…',
  askFailed: '这个问题没能被回答。',
  askQuestionLabel: '问题',
  askSuggestDeclared: (name) => `哪些项目是主动声明 ${name} 而不是继承来的？`,
  askSuggestVersions: (name) => `${name} 有哪些版本在使用中？`,

  noDataForSelection: '该筛选条件下没有数据。',
};

export const DICTIONARIES: Readonly<Record<Locale, Dictionary>> = {
  en: EN,
  zh: ZH,
};
