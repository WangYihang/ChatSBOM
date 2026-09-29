/**
 * The page's questions, answered: for the tests that render the whole
 * page and look at every chart on it (#123).
 *
 * Each method answers in the shape it really answers in, with enough
 * that every panel on both views has something to draw: the overview's
 * eight charts, and the query view's four for `mail`.
 */
import { vi } from 'vitest';

/**
 * How long a test waits for the whole page to be drawn: `waitFor`'s
 * option.
 *
 * Its default, a second, holds on an idle machine and not on a busy one.
 * The page draws a dozen charts from as many answers, and the parts it
 * loads when it draws them are compiled on their first import. At a load
 * average of 15 on four cores, a check of the whole page failed about
 * one run in three with nothing wrong.
 */
export const WHOLE_PAGE = { timeout: 5_000 } as const;

export const ANSWERS: Readonly<Record<string, unknown>> = {
  meta: {
    generator: 'chatsbom/test',
    schemaVersion: 'd1 v5',
    observedFrom: '2026-02-11',
    observedTo: '2026-09-13',
  },
  totals: {
    repositories: 24_339, dependencies: 6_062_896, packages: 141_938,
    classified: 6_053_469, tracked: 60_017,
  },
  relationshipSplit: { direct: 463_150, transitive: 5_590_319, unknown: 9_427 },
  relationshipByEcosystem: [
    { ecosystem: 'npm', direct: 3_000, transitive: 9_000, unknown: 10, records: 12_010 },
    { ecosystem: 'cargo', direct: 400, transitive: 410, unknown: 0, records: 810 },
  ],
  languageCoverage: [
    { language: 'rust', repositories: 800, withSbom: 700, withSyft: 600, withDepgraph: 500, withManifest: 0 },
    { language: 'ruby', repositories: 90, withSbom: 20, withSyft: 20, withDepgraph: 0, withManifest: 0 },
  ],
  ecosystemCoverage: [
    { ecosystem: 'npm', repositories: 5_000, withAny: 4_000, withSyft: 3_000, withDepgraph: 2_000, withManifest: 0 },
    { ecosystem: 'gem', repositories: 1_300, withAny: 1_100, withSyft: 900, withDepgraph: 700, withManifest: 0 },
  ],
  dependencyDistribution: [
    { label: '1-9', repositories: 4_228 },
    { label: '10-24', repositories: 1_589 },
  ],
  sourceComparison: [
    { ecosystem: 'maven', syft: 9_648, depgraph: 47_329, manifest: 1_200 },
    { ecosystem: 'npm', syft: 30_000, depgraph: 12_000, manifest: 0 },
  ],
  licenseShares: [
    { license: 'MIT', repositoryCount: 1_200, packageCount: 3_400 },
    { license: 'Apache-2.0', repositoryCount: 300, packageCount: 900 },
  ],
  topPackages: [
    { name: 'serde', repositoryCount: 6_863, directCount: 6_820 },
    { name: 'tokio', repositoryCount: 5_120, directCount: 5_002 },
  ],
  ecosystemsFor: [],
  countDependents: 1_234,
  countDependentRows: 1_500,
  dependentsOf: [
    {
      owner: 'rails', repo: 'rails', stars: 58_182, version: '2.8.1',
      url: 'https://github.com/rails/rails', relationship: 'transitive',
      observedAt: '2026-09-13', ecosystem: 'gem', language: 'ruby', manifests: 3,
    },
  ],
  versionSpread: {
    versions: [
      { version: '2.8.1', repositoryCount: 1_100, kind: 'resolved' },
      { version: '2.7.1', repositoryCount: 240, kind: 'resolved' },
    ],
    constrained: 12,
    unversioned: 3,
  },
  adoptionOverTime: [
    { source: 'syft', month: '2026-02', repositoryCount: 1_124, directCount: 30 },
    { source: 'github-depgraph', month: '2026-09', repositoryCount: 149, directCount: 149 },
  ],
  edgeAmbiguity: {
    names: 225_582, ambiguousNames: 2_730, edges: 614_221,
    ambiguousEdges: 63_384, largestRepository: 5_388,
  },
  pulledInBy: [
    { name: 'actionmailer', repositories: 7_999 },
    { name: 'rails', repositories: 3_100 },
  ],
  dependencyTree: {
    root: 'mail',
    children: [
      { name: 'mini_mime', repositories: 3_580 },
      { name: 'net-smtp', repositories: 2_900 },
    ],
    grandchildren: [
      { parent: 'mini_mime', child: 'net-imap', repositories: 1_200 },
      { parent: 'net-smtp', child: 'net-protocol', repositories: 2_100 },
    ],
  },
  searchPackages: [],
};

/**
 * `/api/q`, answering from `answers`, and refusing each method in
 * `refused` as the Worker refuses one: with a status and its sentence.
 */
export function stubQueries(
  answers: Readonly<Record<string, unknown>> = ANSWERS,
  refused: Readonly<Record<string, readonly [status: number, error: string]>> = {},
): void {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      const { method } = JSON.parse(String(init?.body)) as { method: string };
      const refusal = refused[method];
      return new Response(
        JSON.stringify(
          refusal ? { error: refusal[1] } : method in answers ? answers[method] : [],
        ),
        { status: refusal?.[0] ?? 200, headers: { 'content-type': 'application/json' } },
      );
    }),
  );
}
