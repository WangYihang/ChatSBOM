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
};

export const DICTIONARIES: Readonly<Record<Locale, Dictionary>> = {
  en: EN,
  zh: ZH,
};
