/**
 * The seam between the dashboard's questions and whatever answers them.
 *
 * It sits at the **method** level, not the SQL level, and that is the
 * whole point. A `runSql(text)` abstraction looks like the obvious seam
 * and is the wrong one, because the two backends do not want the same
 * SQL with different dialects — they want different strategies:
 *
 *     sourceComparison    D1/SQLite   3,122 ms live, so precomputed
 *                         ClickHouse     0.037 s native, no precompute
 *
 * ClickHouse's sparse index answered the `mail` lookup by reading 22,333
 * rows rather than 6,062,896, and its aggregates are fast enough that
 * `agg_*` tables would be pure overhead. A SQL-level seam would force
 * SQLite's compromise onto a backend that does not need it.
 *
 * So each backend implements the questions however suits it, and the
 * endpoint is written against the interface.
 */
import { describe, expect, it, vi } from 'vitest';

import { METHODS } from '../src/d1/api';
import type { DatasetQueries } from '../src/backend';
import { D1Dataset } from '../src/d1/queries';

describe('the backend contract', () => {
  it('is satisfied by the D1 implementation', () => {
    // A compile-time check made runtime-visible: if D1Dataset stops
    // implementing the interface, this file stops type-checking.
    const backend: DatasetQueries = new D1Dataset({
      all: () => Promise.resolve([]),
    });
    expect(backend).toBeInstanceOf(D1Dataset);
  });

  it('covers every method the endpoint exposes', () => {
    /**
     * The registry and the interface must not drift: a method the
     * endpoint accepts but the interface does not declare would compile
     * against D1 and fail against any other backend.
     */
    const stub = new D1Dataset({ all: () => Promise.resolve([]) });
    for (const name of Object.keys(METHODS)) {
      expect(typeof (stub as unknown as Record<string, unknown>)[name]).toBe(
        'function',
      );
    }
  });

  it('lets the endpoint run against a backend that is not D1', async () => {
    /**
     * The actual portability test. A hand-written implementation with no
     * SQL anywhere answers the endpoint's calls — which is what a
     * ClickHouse backend would be: different queries, same questions.
     */
    const elsewhere: DatasetQueries = {
      dependentsOf: vi.fn(async () => []),
      countDependents: vi.fn(async () => 42),
      countDependentRows: vi.fn(async () => 64),
      dependenciesOf: vi.fn(async () => []),
      pulledInBy: vi.fn(async () => []),
      // Null is a legitimate answer: a store without an ecosystem
      // column cannot count cross-ecosystem collisions.
      edgeAmbiguity: vi.fn(async () => null),
      relationshipByLanguage: vi.fn(async () => []),
      versionKindShares: vi.fn(async () => []),
      dependencyTree: vi.fn(async () => ({
        root: 'ms',
        children: [],
        grandchildren: [],
      })),
      relationshipSplit: vi.fn(async () => ({
        direct: 1,
        transitive: 2,
        unknown: 0,
      })),
      totals: vi.fn(async () => ({
        repositories: 1,
        dependencies: 2,
        packages: 3,
        classified: 4,
      })),
      languageCoverage: vi.fn(async () => []),
      topPackages: vi.fn(async () => []),
      dependencyDistribution: vi.fn(async () => []),
      sourceComparison: vi.fn(async () => []),
      searchPackages: vi.fn(async () => []),
      licenseShares: vi.fn(async () => []),
      adoptionOverTime: vi.fn(async () => []),
      versionSpread: vi.fn(async () => ({
        versions: [],
        constrained: 0,
        unversioned: 0,
      })),
      ecosystemsFor: vi.fn(async () => []),
      meta: vi.fn(async () => ({
        generator: 'elsewhere/1.0',
        schemaVersion: '5',
        observedFrom: '',
        observedTo: '',
      })),
    };

    const total = await METHODS['countDependents']!(elsewhere, {
      name: 'mail',
    });
    expect(total).toBe(42);
    expect(elsewhere.countDependents).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'mail' }),
    );
  });

  it('does not leak a SQL-shaped method into the contract', () => {
    /**
     * If the interface ever grows `query(sql)` or similar, the seam has
     * moved back to the SQL level and portability is gone: a caller
     * would start passing statements only one backend understands.
     */
    const stub = new D1Dataset({ all: () => Promise.resolve([]) });
    const surface = Object.getOwnPropertyNames(
      Object.getPrototypeOf(stub) as object,
    );
    for (const name of surface) {
      expect(name).not.toMatch(/^(query|exec|run|sql|raw)$/i);
    }
  });
});
