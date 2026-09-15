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
};

export const DICTIONARIES: Readonly<Record<Locale, Dictionary>> = {
  en: EN,
  zh: ZH,
};
