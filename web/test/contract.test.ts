/**
 * One contract, held by both stores (#41).
 *
 * The dashboard is served from D1 or from ClickHouse, whichever the
 * deployment binds, and the page cannot tell which: so the same call
 * has to get the same answer from either. Each store used to have its
 * own suite, and both mostly checked that a statement contained some
 * text — which is how D1 came to list every row of a repository under
 * the newer of its two observation dates, ClickHouse to count Composer
 * as three repositories where it is five, and neither suite to notice.
 *
 * So these ask both stores the same questions about one corpus and
 * expect one answer, spelled out below. The corpus is seeded by
 * `fixtures/contract/build.py`, which says why each row is there:
 *
 * - D1 is the real export of it (`fixtures/contract/d1.sql`), loaded
 *   into SQLite, which is what D1 runs.
 * - ClickHouse is the server itself when `CLICKHOUSE_TEST_URL` and
 *   `CLICKHOUSE_TEST_DATABASE` name a seeded database, and otherwise
 *   what that server answered each statement when the fixture was
 *   built (`fixtures/contract/clickhouse.json`). Replayed rather than
 *   required, so the suite stays hermetic; recorded rather than written
 *   by hand, so what is replayed is what the server said. A statement
 *   with no recorded answer fails, and names the command that records
 *   one: `uv run python web/test/fixtures/contract/build.py`.
 *
 * Where the stores still differ it is by design or by what the export
 * can hold, and each such place says so and expects each store's own
 * answer, so a change to either shows up here.
 *
 * What each store's statements look like — which tables, which index,
 * which parameters — is pinned in `d1queries.test.ts` and
 * `clickhouse.test.ts`; the answers are pinned here.
 */
import { writeFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

import { afterAll, describe, expect, it } from 'vitest';

import type { DatasetQueries } from '../src/backend';
import { ClickHouse, type Param } from '../src/clickhouse/client';
import { ClickHouseDataset } from '../src/clickhouse/queries';
import { D1Dataset } from '../src/d1/queries';
import recordedText from './fixtures/contract/clickhouse.json?raw';
import d1Script from './fixtures/contract/d1.sql?raw';

/* ---------------- the two stores ---------------- */

const ENV = (
  globalThis as unknown as { process: { env: Record<string, string | undefined> } }
).process.env;

/** SQLite, holding the export exactly as `wrangler d1 execute` applies it. */
function d1(): DatasetQueries {
  const database = new DatabaseSync(':memory:');
  database.exec(d1Script);
  return new D1Dataset({
    all: async <T>(sql: string, params: unknown[] = []) =>
      database.prepare(sql).all(...params) as T[],
  });
}

/** One statement and what the server answered. */
interface Answer {
  sql: string;
  params: Record<string, Param>;
  rows: unknown[];
}

/** A statement as the recording keys it: whitespace is not meaning. */
function keyOf(sql: string, params: Record<string, Param>): string {
  const bound = Object.keys(params)
    .sort()
    .map((name) => [name, String(params[name])]);
  return JSON.stringify([sql.replace(/\s+/g, ' ').trim(), bound]);
}

const recorded = new Map(
  (JSON.parse(recordedText) as Answer[]).map((answer) => [
    keyOf(answer.sql, answer.params),
    answer,
  ]),
);
const heard = new Map<string, Answer>();

/**
 * ClickHouse's side of `ClickHouseDataset`: the server when there is
 * one, what it answered before when there is not.
 */
class ContractClickHouse {
  constructor(private readonly live: ClickHouse | undefined) {}

  async rows<T>(sql: string, params: Record<string, Param> = {}): Promise<T[]> {
    const key = keyOf(sql, params);
    if (this.live) {
      const rows = await this.live.rows<T>(sql, params);
      heard.set(key, {
        sql: sql.replace(/\s+/g, ' ').trim(),
        params,
        rows: rows as unknown[],
      });
      return rows;
    }
    const answer = recorded.get(key);
    if (!answer) {
      throw new Error(
        'No recorded ClickHouse answer to this statement. Record one with\n'
        + '  uv run python web/test/fixtures/contract/build.py\n'
        + `${sql}\n${JSON.stringify(params)}`,
      );
    }
    return answer.rows as T[];
  }

  async row<T>(sql: string, params: Record<string, Param> = {}): Promise<T | undefined> {
    return (await this.rows<T>(sql, params))[0];
  }
}

function clickhouse(): DatasetQueries {
  const url = ENV['CLICKHOUSE_TEST_URL'];
  const database = ENV['CLICKHOUSE_TEST_DATABASE'];
  const live = url && database
    ? new ClickHouse({
        url,
        database,
        user: ENV['CLICKHOUSE_TEST_USER'] ?? 'admin',
        password: ENV['CLICKHOUSE_TEST_PASSWORD'] ?? 'admin',
      })
    : undefined;
  return new ClickHouseDataset(
    new ContractClickHouse(live) as unknown as ClickHouse,
  );
}

afterAll(() => {
  const path = ENV['CLICKHOUSE_TEST_RECORD'];
  if (!path) return;
  // Sorted, so a statement that did not change keeps its place in the
  // file and a rebuild's diff is what changed.
  const answers = [...heard.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([, answer]) => answer);
  writeFileSync(path, `${JSON.stringify(answers, null, 4)}\n`);
});

type Store = 'D1' | 'ClickHouse';

const STORES: [Store, DatasetQueries][] = [
  ['D1', d1()],
  ['ClickHouse', clickhouse()],
];

/* ---------------- the corpus, as the table shows it ---------------- */

/** A dependants row, one manifest unless said otherwise. */
function dependant(
  owner: string,
  repo: string,
  stars: number,
  language: string,
  version: string,
  relationship: string,
  ecosystem: string,
  observedAt: string,
  manifests = 1,
) {
  return {
    owner,
    repo,
    stars,
    version,
    url: `https://github.com/${owner}/${repo}`,
    language,
    ecosystem,
    relationship,
    observedAt,
    manifests,
  };
}

/**
 * Every current dependant of `mail`, in the table's order: most stars
 * first, then owner and repository, then version, relationship,
 * ecosystem and date — a total order, so a page never repeats or skips
 * a row of the one before.
 *
 * Rails was seen by both collectors, seven months apart, and each row
 * carries its own collector's date; its graph states two constraints,
 * from two manifests. Discourse's 2.8.1 is two rows for the same
 * reason, and its first counts two cataloguers.
 */
const MAIL = [
  dependant('rails', 'rails', 58000, 'Ruby', '2.8.1', 'transitive', 'gem', '2026-02-01'),
  dependant('rails', 'rails', 58000, 'Ruby', '>= 2.7', 'direct', 'gem', '2026-09-13'),
  dependant('rails', 'rails', 58000, 'Ruby', '~> 2.8', 'direct', 'gem', '2026-09-13'),
  dependant('discourse', 'discourse', 47000, 'Ruby', '2.8.1', 'direct', 'gem', '2026-02-11', 2),
  dependant('discourse', 'discourse', 47000, 'Ruby', '2.8.1', 'direct', 'gem', '2026-09-13'),
  dependant('mastodon', 'mastodon', 47000, 'Ruby', '2.8.1', 'direct', 'gem', '2026-02-11'),
  dependant('apache', 'james', 900, 'Java', '1.4.7', 'direct', 'maven', '2026-02-11'),
  dependant('psf', 'app', 500, 'Python', '0.0.1', 'transitive', 'pypi', '2026-02-11'),
];

/**
 * `laravel/framework`: Composer under the graph's spelling in three
 * repositories and Syft's in three, one of them both — five
 * repositories, one ecosystem.
 *
 * `laravel/laravel` declares it in two manifests. ClickHouse keeps a
 * row per manifest and counts them; the D1 export keeps one row per
 * dependency fact and has none to count, so the stores differ there by
 * what they hold, and only there.
 */
function laravel(store: Store) {
  return [
    dependant('laravel', 'laravel', 80000, 'PHP', '^12.0', 'direct', 'composer', '2026-09-13',
      store === 'ClickHouse' ? 2 : 1),
    dependant('monicahq', 'monica', 22000, 'PHP', 'v12.49.0', 'direct', 'composer', '2026-02-11'),
    dependant('firefly-iii', 'firefly-iii', 16000, 'PHP', '^11.0|^12.0', 'direct', 'composer', '2026-09-13'),
    dependant('firefly-iii', 'firefly-iii', 16000, 'PHP', 'v12.49.0', 'direct', 'composer', '2026-02-11'),
    // 23:30 UTC on the 14th: a date made in another zone is the 15th.
    dependant('koel', 'koel', 16000, 'PHP', '', 'transitive', 'composer', '2026-09-14'),
    dependant('koel', 'koel', 16000, 'PHP', '^10.0', 'direct', 'composer', '2026-09-14'),
    dependant('koel', 'koel', 16000, 'PHP', '^9.0', 'direct', 'composer', '2026-09-14'),
    dependant('akaunting', 'akaunting', 9000, 'PHP', 'v10.48.0', 'transitive', 'composer', '2026-02-11'),
    dependant('akaunting', 'akaunting', 9000, 'PHP', 'v11.2.0', 'direct', 'composer', '2026-02-11'),
  ];
}

const edge = (name: string, repositories: number) => ({ name, repositories });
const hop = (parent: string, child: string, repositories: number) => ({
  parent,
  child,
  repositories,
});

describe.each(STORES)('%s', (store, dataset) => {
  describe('dependants', () => {
    it('lists each current row once, with its own ecosystem and date, in a total order', async () => {
      expect(await dataset.dependentsOf({ name: 'mail' })).toEqual(MAIL);
    });

    it('reads an ecosystem as one, whichever collector spelled it', async () => {
      // `composer` is the name the page shows, and what `ecosystemsFor`
      // offers; `php-composer` is Syft's spelling of the same registry.
      for (const type of ['composer', 'php-composer']) {
        expect(
          await dataset.dependentsOf({ name: 'laravel/framework', type }),
        ).toEqual(laravel(store));
      }
      expect(await dataset.dependentsOf({ name: 'mail', type: 'maven' })).toEqual(
        MAIL.filter((row) => row.ecosystem === 'maven'),
      );
    });

    it('filters on the declared relationship and the folded language', async () => {
      expect(await dataset.dependentsOf({ name: 'mail', directOnly: true })).toEqual(
        MAIL.filter((row) => row.relationship === 'direct'),
      );
      expect(await dataset.dependentsOf({ name: 'mail', language: 'Ruby' })).toEqual(
        MAIL.filter((row) => row.language === 'Ruby'),
      );
      const [site] = await dataset.dependentsOf({ name: 'express', language: 'none' });
      expect(site).toMatchObject({ owner: 'expressjs', repo: 'site', language: '' });
    });

    it('pages through the rows without repeating or skipping one', async () => {
      const pages = [];
      for (const offset of [0, 3, 6]) {
        pages.push(
          ...(await dataset.dependentsOf({
            name: 'laravel/framework',
            limit: 3,
            offset,
          })),
        );
      }
      expect(pages).toEqual(laravel(store));
      expect(
        await dataset.dependentsOf({ name: 'laravel/framework', offset: 9 }),
      ).toEqual([]);
    });

    it('counts the rows it pages through, and the repositories they are of', async () => {
      const queries = [
        { name: 'mail' },
        { name: 'mail', directOnly: true },
        { name: 'mail', language: 'ruby' },
        { name: 'laravel/framework', type: 'php-composer' },
      ];
      for (const query of queries) {
        const rows = await dataset.dependentsOf(query);
        expect([query, await dataset.countDependentRows(query)]).toEqual([
          query,
          rows.length,
        ]);
        const repositories = new Set(rows.map((row) => `${row.owner}/${row.repo}`));
        expect([query, await dataset.countDependents(query)]).toEqual([
          query,
          repositories.size,
        ]);
      }
      expect(await dataset.countDependents({ name: 'laravel/framework' })).toBe(5);
      expect(await dataset.countDependents({ name: 'left-pad' })).toBe(0);
    });
  });

  describe('one package', () => {
    it('names its ecosystems once each, counting every repository once', async () => {
      expect(await dataset.ecosystemsFor('laravel/framework')).toEqual([
        { type: 'composer', repositoryCount: 5, directCount: 5 },
      ]);
      // Tied at one, so by name.
      expect(await dataset.ecosystemsFor('mail')).toEqual([
        { type: 'gem', repositoryCount: 3, directCount: 3 },
        { type: 'maven', repositoryCount: 1, directCount: 1 },
        { type: 'pypi', repositoryCount: 1, directCount: 0 },
      ]);
    });

    it('spreads the resolved versions, and counts all of what it set aside', async () => {
      // Three repositories with a constraint, more than the two versions
      // asked for: `constrained` is all three, not the two that would
      // have been listed. And three, not four: koel states two
      // constraint strings and is one repository (#120), as is rails
      // for `mail`.
      expect(await dataset.versionSpread('laravel/framework', 2)).toEqual({
        versions: [
          { kind: 'resolved', version: 'v12.49.0', repositoryCount: 2 },
          { kind: 'resolved', version: 'v10.48.0', repositoryCount: 1 },
        ],
        constrained: 3,
        unversioned: 1,
      });
      expect(await dataset.versionSpread('mail')).toEqual({
        versions: [
          { kind: 'resolved', version: '2.8.1', repositoryCount: 3 },
          { kind: 'resolved', version: '0.0.1', repositoryCount: 1 },
          { kind: 'resolved', version: '1.4.7', repositoryCount: 1 },
        ],
        constrained: 1,
        unversioned: 0,
      });
    });

    it('charts every observation, per collector and month', async () => {
      expect(await dataset.adoptionOverTime('mail')).toEqual([
        { source: 'github-depgraph', month: '2026-09', repositoryCount: 2, directCount: 2 },
        // The scan February's replaced: history keeps it.
        { source: 'syft', month: '2026-01', repositoryCount: 1, directCount: 0 },
        { source: 'syft', month: '2026-02', repositoryCount: 5, directCount: 3 },
      ]);
    });

    it('finds names by prefix, most depended upon first', async () => {
      // ClickHouse splits a name by ecosystem; the D1 export stores one
      // count per name, and splitting it would count the artifacts on
      // every keystroke, so its row stands for the name across all of
      // them. The names, their order and their totals are the same.
      const split = store === 'ClickHouse';
      expect(await dataset.searchPackages('m')).toEqual(
        split
          ? [
              { name: 'mail', ecosystem: 'gem', repositoryCount: 3, nameTotal: 5 },
              { name: 'mail', ecosystem: 'maven', repositoryCount: 1, nameTotal: 5 },
              { name: 'mail', ecosystem: 'pypi', repositoryCount: 1, nameTotal: 5 },
              { name: 'mini_mime', ecosystem: 'gem', repositoryCount: 2, nameTotal: 2 },
              { name: 'ms', ecosystem: 'npm', repositoryCount: 2, nameTotal: 2 },
            ]
          : [
              { name: 'mail', ecosystem: null, repositoryCount: 5, nameTotal: 5 },
              { name: 'mini_mime', ecosystem: null, repositoryCount: 2, nameTotal: 2 },
              { name: 'ms', ecosystem: null, repositoryCount: 2, nameTotal: 2 },
            ],
      );
      expect(await dataset.searchPackages('laravel')).toEqual([
        {
          name: 'laravel/framework',
          ecosystem: split ? 'composer' : null,
          repositoryCount: 5,
          nameTotal: 5,
        },
      ]);
      expect(await dataset.searchPackages('%')).toEqual([]);
    });
  });

  describe('the edges', () => {
    it('reads both directions, widest first', async () => {
      expect(await dataset.dependenciesOf('express')).toEqual([
        edge('body-parser', 5),
        edge('debug', 4),
        edge('qs', 3),
        edge('send', 2),
      ]);
      expect(await dataset.pulledInBy('debug')).toEqual([
        edge('express', 4),
        edge('body-parser', 3),
        edge('send', 3),
      ]);
      expect(await dataset.dependenciesOf('express', 2)).toEqual([
        edge('body-parser', 5),
        edge('debug', 4),
      ]);
    });

    it('keeps an edge only when the export knows both of its packages', async () => {
      // `mail-dev` is in no SBOM. ClickHouse keeps the edges `db edges`
      // counted; the D1 export references packages by id and skips a
      // pair naming one it has not got (`export/d1.py`).
      expect(await dataset.pulledInBy('mail')).toEqual(
        store === 'ClickHouse' ? [edge('mail-dev', 1)] : [],
      );
    });

    it('draws two hops, bounded per parent, in one order', async () => {
      expect(await dataset.dependencyTree('express')).toEqual({
        root: 'express',
        children: [
          edge('body-parser', 5),
          edge('debug', 4),
          edge('qs', 3),
          edge('send', 2),
        ],
        // `debug` at 3 under two parents: by parent.
        grandchildren: [
          hop('debug', 'ms', 4),
          hop('body-parser', 'debug', 3),
          hop('send', 'debug', 3),
          hop('send', 'ms', 3),
          hop('body-parser', 'qs', 3),
          hop('body-parser', 'bytes', 2),
          hop('body-parser', 'raw-body', 2),
          hop('qs', 'side-channel', 2),
        ],
      });
    });

    it('never draws the root as its own grandchild', async () => {
      // `bytes -> body-parser` is an edge too. Excluded before ranking,
      // so `bytes` has no second hop rather than an empty slot.
      expect(await dataset.dependencyTree('body-parser', { branch: 1 })).toEqual({
        root: 'body-parser',
        children: [
          edge('debug', 3),
          edge('qs', 3),
          edge('bytes', 2),
          edge('raw-body', 2),
        ],
        grandchildren: [
          hop('debug', 'ms', 4),
          hop('raw-body', 'bytes', 2),
          hop('qs', 'side-channel', 2),
        ],
      });
      expect(await dataset.dependencyTree('left-pad')).toEqual({
        root: 'left-pad',
        children: [],
        grandchildren: [],
      });
    });
  });

  describe('the overview', () => {
    it('totals the corpus', async () => {
      expect(await dataset.totals()).toEqual({
        repositories: 11,
        dependencies: 34,
        packages: 15,
        classified: 33,
        tracked: 12,
      });
    });

    it('splits the records by relationship, overall and per ecosystem', async () => {
      expect(await dataset.relationshipSplit()).toEqual({
        direct: 17,
        transitive: 16,
        unknown: 1,
      });
      expect(await dataset.relationshipSplit('Composer')).toEqual({
        direct: 7,
        transitive: 2,
        unknown: 0,
      });
      // Tied at nine: by name.
      expect(await dataset.relationshipByEcosystem()).toEqual([
        { ecosystem: 'npm', direct: 1, transitive: 10, unknown: 0, records: 11 },
        { ecosystem: 'composer', direct: 7, transitive: 2, unknown: 0, records: 9 },
        { ecosystem: 'gem', direct: 6, transitive: 3, unknown: 0, records: 9 },
        { ecosystem: 'pypi', direct: 1, transitive: 1, unknown: 1, records: 3 },
        { ecosystem: 'maven', direct: 2, transitive: 0, unknown: 0, records: 2 },
      ]);
    });

    it('compares the collectors per ecosystem', async () => {
      expect(await dataset.sourceComparison()).toEqual([
        { ecosystem: 'npm', syft: 9, depgraph: 2, manifest: 0 },
        { ecosystem: 'composer', syft: 4, depgraph: 5, manifest: 0 },
        { ecosystem: 'gem', syft: 6, depgraph: 3, manifest: 0 },
        { ecosystem: 'pypi', syft: 3, depgraph: 0, manifest: 0 },
        { ecosystem: 'maven', syft: 1, depgraph: 0, manifest: 1 },
      ]);
    });

    it('covers the snapshot per language and per ecosystem', async () => {
      const coverage = (
        language: string,
        repositories: number,
        withSbom: number,
        withSyft: number,
        withDepgraph: number,
        withManifest: number,
      ) => ({ language, repositories, withSbom, withSyft, withDepgraph, withManifest });
      expect(await dataset.languageCoverage()).toEqual([
        coverage('php', 5, 5, 3, 3, 0),
        coverage('ruby', 3, 3, 3, 2, 0),
        // Tracked, never collected.
        coverage('go', 1, 0, 0, 0, 0),
        coverage('java', 1, 1, 1, 0, 1),
        coverage('none', 1, 1, 1, 0, 0),
        coverage('python', 1, 1, 1, 0, 0),
      ]);
      const ecosystem = (
        name: string,
        repositories: number,
        withAny: number,
        withSyft: number,
        withDepgraph: number,
        withManifest: number,
      ) => ({ ecosystem: name, repositories, withAny, withSyft, withDepgraph, withManifest });
      expect(await dataset.ecosystemCoverage()).toEqual([
        ecosystem('composer', 5, 5, 3, 3, 0),
        ecosystem('gem', 3, 3, 3, 2, 0),
        ecosystem('npm', 2, 2, 1, 1, 0),
        ecosystem('maven', 1, 1, 1, 0, 1),
        ecosystem('pypi', 1, 1, 1, 0, 0),
      ]);
    });

    it('ranks the packages under each filter the panel has', async () => {
      const top = (name: string, repositoryCount: number, directCount: number) => ({
        name,
        repositoryCount,
        directCount,
      });
      // Tied at five, and again at two and one: by name.
      expect(await dataset.topPackages({})).toEqual([
        top('laravel/framework', 5, 5),
        top('mail', 5, 4),
        top('debug', 2, 0),
        top('mini_mime', 2, 0),
        top('ms', 2, 0),
        top('body-parser', 1, 0),
        top('bytes', 1, 0),
        top('certifi', 1, 0),
        top('express', 1, 1),
        top('jakarta.mail', 1, 1),
        top('qs', 1, 0),
        top('raw-body', 1, 0),
        top('requests', 1, 1),
        top('send', 1, 0),
        top('side-channel', 1, 0),
      ]);
      expect(await dataset.topPackages({ directOnly: true, limit: 3 })).toEqual([
        top('laravel/framework', 5, 5),
        top('mail', 5, 4),
        top('express', 1, 1),
      ]);
      expect(await dataset.topPackages({ ecosystem: 'Maven' })).toEqual([
        top('jakarta.mail', 1, 1),
        top('mail', 1, 1),
      ]);
    });

    it('shares the licences out, unknown included', async () => {
      const share = (license: string, repositoryCount: number, packageCount: number) => ({
        license,
        repositoryCount,
        packageCount,
      });
      // Unknown and MIT tie at seven: by name, so unknown first.
      expect(await dataset.licenseShares()).toEqual([
        share('', 7, 5),
        share('MIT', 7, 11),
        share('Apache-2.0', 2, 2),
        share('BSD-3-Clause', 1, 1),
        share('MPL-2.0', 1, 1),
      ]);
      expect(await dataset.licenseShares(1)).toEqual([share('', 7, 5)]);
    });

    it('buckets the repositories by how many packages they have', async () => {
      // Not one contract yet: the export and the rollup draw their own
      // buckets (`03-aggregates.sql` counts the tracked repositories
      // with none, `mv_dependency_buckets` only those with some, at
      // other bounds). Stated per store so a change to either shows.
      expect(await dataset.dependencyDistribution()).toEqual(
        store === 'ClickHouse'
          ? [{ label: '1-9', repositories: 11 }]
          : [
              { label: 'none', repositories: 1 },
              { label: '1-9', repositories: 11 },
            ],
      );
    });

    it('measures how ambiguous the name-keyed edges are, where it can', async () => {
      // Answering it in D1 means grouping every artifact by name and
      // ecosystem on request, which that store's rule is not to do; it
      // says it cannot rather than guess.
      expect(await dataset.edgeAmbiguity()).toEqual(
        store === 'ClickHouse'
          ? {
              names: 15,
              ambiguousNames: 1,
              edges: 15,
              ambiguousEdges: 1,
              largestRepository: 9,
            }
          : null,
      );
    });
  });

  describe('provenance', () => {
    it('spans the current observations, by each repository\'s newest', async () => {
      // Not rails' January scan, which February's replaced, nor its
      // February one, which the graph in September came after: the
      // oldest of the repositories' newest observations.
      const meta = await dataset.meta();
      expect([meta.observedFrom, meta.observedTo]).toEqual([
        '2026-02-11',
        '2026-09-14',
      ]);
      expect(meta.schemaVersion).toMatch(
        store === 'ClickHouse' ? /^clickhouse/ : /^d1 v\d+$/,
      );
    });
  });
});
