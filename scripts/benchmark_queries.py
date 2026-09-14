#!/usr/bin/env python3
"""Time every query the dashboard makes, against the live database.

The rollups exist because the overview used to scan the corpus on every
visit: 836 ms across twenty-one queries. Keeping that won is not
automatic — a rebuild changes part sizes, a new rollup can be missing,
and a `dictGet` can quietly become a join. So this measures rather than
assumes, and it was an ad-hoc script until it was needed a third time.

Each query runs `--warmup` times to fill the page cache, then `--runs`
times for the figure: a cold first read is a disk measurement, and the
dashboard's second visitor is the case that matters. Both counts are
reported so the number is never quoted without its method.

The point lookups use real names of deliberately different shapes —
`laravel/framework` has a few hundred rows, `ms` tens of thousands —
because an index that helps the small case can still scan for the
large one.

Usage:

    uv run python scripts/benchmark_queries.py
    uv run python scripts/benchmark_queries.py --runs 10 --budget 50

`--budget` sets the ceiling in milliseconds that makes this exit
non-zero, so it can gate a deploy.
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time

from chatsbom.core.container import get_container

#: Every panel's query, in the order the page issues them. Named for
#: the method on `DatasetQueries` so a slow row points at real code.
QUERIES: tuple[tuple[str, str], ...] = (
    ('totals', 'SELECT * FROM mv_totals'),
    # No WHERE for the unfiltered case: the hero panel sums all nine
    # rows. `WHERE language = ''` looks equivalent and is not — that
    # row only exists if a language-less repository has artifacts, and
    # none does, so this measured a query returning nothing and
    # reported 1.5 ms for it.
    (
        'relationshipSplit',
        'SELECT sum(direct_records) AS direct, '
        'sum(transitive_records) AS transitive, '
        'sum(unknown_records) AS unknown FROM mv_language_totals',
    ),
    (
        'relationshipSplit/php',
        'SELECT sum(direct_records) AS direct, '
        'sum(transitive_records) AS transitive, '
        'sum(unknown_records) AS unknown FROM mv_language_totals '
        "WHERE language = 'php'",
    ),
    (
        'languageCoverage',
        'SELECT * FROM mv_language_coverage ORDER BY repositories DESC',
    ),
    (
        'topPackages/declared',
        'SELECT name, repositories, direct_repositories FROM mv_top_packages '
        "WHERE language = '' AND direct_only = 1 ORDER BY rank LIMIT 20",
    ),
    (
        'topPackages/all',
        'SELECT name, repositories, direct_repositories FROM mv_top_packages '
        "WHERE language = '' AND direct_only = 0 ORDER BY rank LIMIT 20",
    ),
    (
        'topPackages/php',
        'SELECT name, repositories, direct_repositories FROM mv_top_packages '
        "WHERE language = 'php' AND direct_only = 1 ORDER BY rank LIMIT 20",
    ),
    (
        'dependencyDistribution',
        'SELECT * FROM mv_dependency_buckets ORDER BY position',
    ),
    (
        'licenseShares',
        'SELECT license, repositories, packages FROM mv_licenses '
        'ORDER BY repositories DESC LIMIT 12',
    ),
    (
        'sourceComparison',
        'SELECT language, syft_records, depgraph_records '
        'FROM mv_language_totals ORDER BY records DESC',
    ),
    ('edgeAmbiguity', 'SELECT * FROM mv_edge_ambiguity'),
    (
        'meta',
        'SELECT min(observed_at) AS a, max(observed_at) AS b FROM artifacts',
    ),
    # Point lookups: an arbitrary name, so nothing can be precomputed.
    (
        'dependentsOf/laravel',
        'SELECT a.repository_id, '
        "dictGet('dict_repositories', 'owner', a.repository_id) AS owner, "
        "dictGet('dict_repositories', 'stars', a.repository_id) AS stars, "
        'a.version, a.relationship FROM artifacts a '
        "WHERE a.name = 'laravel/framework' "
        "AND dictHas('dict_repositories', a.repository_id) "
        'ORDER BY stars DESC LIMIT 100',
    ),
    (
        'dependentsOf/ms',
        'SELECT a.repository_id, '
        "dictGet('dict_repositories', 'owner', a.repository_id) AS owner, "
        "dictGet('dict_repositories', 'stars', a.repository_id) AS stars, "
        'a.version, a.relationship FROM artifacts a '
        "WHERE a.name = 'ms' "
        "AND dictHas('dict_repositories', a.repository_id) "
        'ORDER BY stars DESC LIMIT 100',
    ),
    (
        'countDependents/ms',
        "SELECT repositories FROM mv_packages WHERE name = 'ms'",
    ),
    (
        'ecosystemsFor/bytes',
        "SELECT type, repositories FROM mv_package_type WHERE name = 'bytes'",
    ),
    (
        'versionSpread/react',
        'SELECT version, version_kind, repositories FROM mv_package_version '
        "WHERE name = 'react' ORDER BY repositories DESC LIMIT 20",
    ),
    (
        'adoptionOverTime/react',
        'SELECT source, month, repositories FROM mv_package_month '
        "WHERE name = 'react' ORDER BY month",
    ),
    (
        'searchPackages/lara',
        "SELECT name, repositories FROM mv_packages WHERE name LIKE '%lara%' "
        'ORDER BY repositories DESC LIMIT 12',
    ),
    (
        'dependenciesOf/express',
        'SELECT child, repositories FROM mv_edges_forward '
        "WHERE parent = 'express' ORDER BY repositories DESC LIMIT 12",
    ),
    (
        'pulledInBy/ms',
        "SELECT parent, repositories FROM edges WHERE child = 'ms' "
        'ORDER BY repositories DESC LIMIT 15',
    ),
    (
        'dependencyTree/express',
        'SELECT parent, child, repositories FROM mv_edges_forward '
        "WHERE parent = 'express' ORDER BY repositories DESC LIMIT 12",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--warmup', type=int, default=2)
    parser.add_argument(
        '--budget', type=float, default=0.0,
        help='Fail if any median exceeds this many milliseconds.',
    )
    args = parser.parse_args()

    client = get_container().get_query_repository().client

    print(f'{args.warmup} warmup, {args.runs} timed runs per query, '
          f'medians in ms')
    print(f"{'query':<28}{'median':>9}{'min':>9}{'max':>9}{'rows':>8}")
    print('-' * 63)

    medians: list[tuple[str, float]] = []
    for name, sql in QUERIES:
        for _ in range(args.warmup):
            client.query(sql)
        times = []
        rows = 0
        for _ in range(args.runs):
            start = time.perf_counter()
            result = client.query(sql)
            times.append((time.perf_counter() - start) * 1000)
            rows = len(result.result_rows)
        median = statistics.median(times)
        medians.append((name, median))
        print(f'{name:<28}{median:>9.1f}{min(times):>9.1f}'
              f'{max(times):>9.1f}{rows:>8,}')

    print('-' * 63)
    total = sum(m for _, m in medians)
    worst = max(medians, key=lambda pair: pair[1])
    print(f'{"total":<28}{total:>9.1f}')
    print(f'slowest: {worst[0]} at {worst[1]:.1f} ms')

    if args.budget:
        over = [(n, m) for n, m in medians if m > args.budget]
        if over:
            print(f'\n{len(over)} over the {args.budget:.0f} ms budget:')
            for name, median in over:
                print(f'  {name} {median:.1f}')
            return len(over)
        print(f'\nAll {len(medians)} within the {args.budget:.0f} ms budget.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
