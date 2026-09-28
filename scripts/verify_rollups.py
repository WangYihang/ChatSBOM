#!/usr/bin/env python3
"""Ask every rollup's question another way, and compare.

A rollup is a cached answer. The only way to know it is still the right
answer is to compute it by another route and diff, which is what this
does — and it has earned its keep: it is how `mv_totals` was caught
storing four numbers computed from an empty source, and how the licence
rollup was caught dropping its largest category.

This was an ad-hoc script until now, re-typed each time it was needed.
It is needed after every `db index --rebuild`, so it lives here.

**Another route, not the same SQL twice.** The checks used to restate
each rollup's query against the base tables, so they shared its bugs.
Every rollup read every observation ever appended, old scans and all,
and so did every check: "14 of 14 agree", on a database where the CLI
listed mail 2.9.1 and the dashboard 2.7.1 beside it. Each answer is now
computed from something the rollup does not read:

  cli        `QueryRepository`, where it has a method for the question.
             It joins each repository's current scan itself, so it
             does not share the views the rollups are built on.
  facts      the `facts` and `current_artifacts` views, where the CLI
             has no such method. They define "current", so a rollup
             that reads anything else disagrees with them.
  artifacts  every observation, for the one rollup that is history.
  edges      the edge table, which records no scans.

Each check names the rollup, the count it holds, the count computed the
other way, and whether they agree. Scope is stated per check rather
than implied:

  full     every row of the rollup compared against the base tables
  agg      totals compared; row-level equality not asserted
  sample   the N rows that matter most, compared row for row

`sample` is used where a full comparison means aggregating twenty
million rows into hundreds of thousands of groups, or where the CLI
answers one name at a time — the check would take longer than the
rebuild it verifies. Where that is the case the script says so instead
of implying it checked everything.

Usage:

    uv run python scripts/verify_rollups.py

Exit status is the number of disagreements, so CI or a shell `&&` can
use it.
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from typing import Any

from chatsbom.core.container import get_container
from chatsbom.core.repository import QueryRepository

Rows = list[tuple[Any, ...]]
#: SQL run as it stands, or a function of the client and the CLI's
#: repository for an answer that is not one statement.
Answer = str | Callable[[Any, QueryRepository], Rows]

#: Rows per `sample` check.
SAMPLE = 20


class Check:
    """One rollup, and the question it caches."""

    def __init__(
        self,
        rollup: str,
        scope: str,
        via: str,
        cached: Answer,
        computed: Answer,
        note: str = '',
    ) -> None:
        self.rollup = rollup
        self.scope = scope
        self.via = via
        self.cached = cached
        self.computed = computed
        self.note = note


def run(client: Any, sql: str, **parameters: Any) -> Rows:
    return [
        tuple(row)
        for row in client.query(sql, parameters=parameters).result_rows
    ]


def answer(source: Answer, client: Any, repo: QueryRepository) -> Rows:
    if isinstance(source, str):
        return run(client, source)
    return source(client, repo)


def top_names(client: Any, limit: int = SAMPLE) -> list[str]:
    """The most depended-upon names, as the rollup ranks them."""
    return [
        str(name) for (name,) in run(
            client,
            'SELECT name FROM mv_packages '
            'ORDER BY repositories DESC, name LIMIT {limit:UInt32}',
            limit=limit,
        )
    ]


def packages_cached(client: Any, repo: QueryRepository) -> Rows:
    return run(
        client,
        'SELECT name, repositories, direct_repositories FROM mv_packages '
        'ORDER BY repositories DESC, name LIMIT {limit:UInt32}',
        limit=SAMPLE,
    )


def packages_computed(client: Any, repo: QueryRepository) -> Rows:
    return [
        (
            name,
            repo.get_dependent_count(name),
            repo.get_dependent_count(name, direct_only=True),
        )
        for name in top_names(client)
    ]


def ranking_computed(client: Any, repo: QueryRepository) -> Rows:
    return [
        (p.name, p.repository_count, p.direct_count)
        for p in repo.get_top_packages(limit=SAMPLE)
    ]


def by_language_cached(client: Any, repo: QueryRepository) -> Rows:
    # The empty language is left out: the CLI reads an empty language
    # filter as no filter, so it has no way to ask about that row.
    return run(
        client,
        'SELECT name, language, repositories, direct_repositories '
        'FROM mv_package_language '
        "WHERE name IN {names:Array(String)} AND language != '' "
        'ORDER BY name, language',
        names=top_names(client, 5),
    )


def by_language_computed(client: Any, repo: QueryRepository) -> Rows:
    return [
        (
            name,
            language,
            repo.get_dependent_count(name, language=language),
            repo.get_dependent_count(
                name, language=language, direct_only=True,
            ),
        )
        for name, language, _, _ in by_language_cached(client, repo)
    ]


def coverage_computed(client: Any, repo: QueryRepository) -> Rows:
    repositories: dict[str, int] = {}
    for count in repo.get_language_stats():
        language = count.language.lower()
        repositories[language] = (
            repositories.get(language, 0) + count.repository_count
        )
    with_sbom: dict[str, int] = {
        str(language): int(total)
        for language, total in run(
            client,
            'SELECT lower(r.language), uniqExact(f.repository_id) '
            'FROM facts AS f '
            'INNER JOIN (SELECT id, language FROM repositories FINAL) AS r '
            'ON r.id = f.repository_id GROUP BY lower(r.language)',
        )
    }
    return sorted(
        (language, total, with_sbom.get(language, 0))
        for language, total in repositories.items()
    )


#: Every rollup, paired with the same question asked another way. Where
#: a rollup holds one row per name the comparison is an aggregate over
#: all of them, which catches a wrong total without grouping twenty
#: million rows twice.
CHECKS: tuple[Check, ...] = (
    Check(
        'mv_package_language', 'sample', 'cli',
        by_language_cached, by_language_computed,
        'the five most used names in each of their languages, against '
        '`db query`',
    ),
    Check(
        'mv_repository_deps', 'agg', 'facts',
        'SELECT count() AS n, sum(packages) AS a, '
        'sum(direct_packages) AS b, sum(records) AS c '
        'FROM mv_repository_deps',
        '''SELECT count() AS n, sum(p_) AS a, sum(d_) AS b, sum(c_) AS c
           FROM (
               SELECT repository_id, uniqExact(name) AS p_,
                      uniqExactIf(name, relationship = 'direct') AS d_,
                      count() AS c_
               FROM facts GROUP BY repository_id)''',
        'records counts current facts, not rows: the dependency graph '
        'reports per manifest, and a repository keeps every scan',
    ),
    Check(
        'mv_licenses', 'full', 'facts',
        'SELECT count() AS n, sum(repositories) AS a, sum(packages) AS b '
        'FROM mv_licenses',
        '''SELECT count() AS n, sum(r_) AS a, sum(p_) AS b FROM (
               SELECT l, uniqExact(repository_id) AS r_, uniqExact(name) AS p_
               FROM current_artifacts ARRAY JOIN licenses AS l GROUP BY l
               UNION ALL
               SELECT '' AS l, uniqExact(repository_id) AS r_,
                      uniqExact(name) AS p_
               FROM current_artifacts WHERE empty(licenses))''',
        'the empty key is the unknown bucket; ARRAY JOIN alone drops it',
    ),
    Check(
        'mv_language_totals', 'full', 'facts',
        'SELECT count() AS n, sum(records) AS a, sum(direct_records) AS b '
        'FROM mv_language_totals',
        '''SELECT count() AS n, sum(c_) AS a, sum(d_) AS b FROM (
               SELECT lower(r.language) AS lang, count() AS c_,
                      countIf(f.relationship = 'direct') AS d_
               FROM facts AS f
               INNER JOIN (SELECT id, language FROM repositories FINAL) AS r
                   ON r.id = f.repository_id
               GROUP BY lang)''',
    ),
    Check(
        'mv_packages', 'sample', 'cli',
        packages_cached, packages_computed,
        'the twenty most used names, each counted by `db query`',
    ),
    Check(
        'mv_edges_forward', 'full', 'edges',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_edges_forward',
        'SELECT count() AS n, sum(repositories) AS a FROM edges',
    ),
    Check(
        'mv_package_month', 'agg', 'artifacts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_month',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, source, toStartOfMonth(observed_at) AS m,
                      uniqExact(repository_id) AS r_
               FROM artifacts GROUP BY name, source, m)''',
        'history: every observation, where every other rollup reads the '
        'current scan; one line per collector, never a line across both',
    ),
    Check(
        'mv_package_type', 'agg', 'facts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_type',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, type, uniqExact(repository_id) AS r_
               FROM facts GROUP BY name, type)''',
    ),
    Check(
        'mv_package_version', 'agg', 'facts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_version',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, version, version_kind,
                      uniqExact(repository_id) AS r_
               FROM facts GROUP BY name, version, version_kind)''',
        'constraints and resolutions kept apart by version_kind',
    ),
    Check(
        'mv_dependency_buckets', 'full', 'facts',
        'SELECT count() AS n, sum(repositories) AS a '
        'FROM mv_dependency_buckets',
        '''SELECT count() AS n, sum(c_) AS a FROM (
               SELECT multiIf(p < 10, 0, p < 25, 1, p < 100, 2,
                              p < 250, 3, p < 1000, 4, 5) AS bucket,
                      count() AS c_
               FROM (SELECT repository_id, uniqExact(name) AS p
                     FROM facts GROUP BY repository_id)
               GROUP BY bucket)''',
        'bucketed on distinct packages, not rows',
    ),
    Check(
        'mv_version_kinds', 'full', 'facts',
        'SELECT version_kind, records FROM mv_version_kinds '
        'ORDER BY version_kind',
        'SELECT version_kind, count() FROM facts '
        'GROUP BY version_kind ORDER BY version_kind',
        'the three add up to the dependencies tile',
    ),
    Check(
        'mv_language_coverage', 'full', 'cli+facts',
        'SELECT language, repositories, with_sbom FROM mv_language_coverage '
        'ORDER BY language',
        coverage_computed,
        'the denominator is the corpus, the numerator what produced an SBOM',
    ),
    Check(
        'mv_totals', 'full', 'facts',
        'SELECT repositories AS n, dependencies AS a, packages AS b, '
        'classified AS c FROM mv_totals',
        '''SELECT
               (SELECT uniqExact(repository_id) FROM facts) AS n,
               (SELECT count() FROM facts) AS a,
               (SELECT uniqExact(name) FROM facts) AS b,
               (SELECT countIf(relationship IN ('direct', 'transitive'))
                FROM facts) AS c''',
        'the four headline numbers; this rollup once stored them from an '
        'empty source because REFRESH only schedules',
    ),
    Check(
        'mv_top_packages', 'sample', 'cli',
        '''SELECT name, repositories, direct_repositories
           FROM mv_top_packages
           WHERE language = '' AND direct_only = 0
           ORDER BY rank LIMIT 20''',
        ranking_computed,
        'the ranking `db query` prints, top 20 row for row',
    ),
    Check(
        'mv_top_packages', 'sample', 'facts',
        # `direct_only = 1` and the empty language are the front page's
        # default: the declared-only ranking across all languages.
        '''SELECT name, direct_repositories FROM mv_top_packages
           WHERE language = '' AND direct_only = 1
           ORDER BY rank LIMIT 20''',
        '''SELECT name, uniqExactIf(repository_id, relationship = 'direct')
                      AS d_
           FROM facts GROUP BY name
           ORDER BY d_ DESC, name LIMIT 20''',
        'the ranking the front page leads with, top 20 row for row',
    ),
    Check(
        'mv_edge_ambiguity', 'full', 'facts',
        'SELECT names AS n, ambiguous_names AS a, edges AS b, '
        'ambiguous_edges AS c, largest_repository AS d FROM mv_edge_ambiguity',
        '''WITH ambiguous AS (
               SELECT name FROM mv_package_type GROUP BY name
               HAVING uniqExact(transform(type,
                   ['rust-crate', 'python', 'golang', 'go-module',
                    'java-archive', 'php-composer'],
                   ['cargo', 'pypi', 'go', 'go', 'maven', 'composer'],
                   type)) > 1)
           SELECT
               (SELECT uniqExact(name) FROM facts) AS n,
               (SELECT count() FROM ambiguous) AS a,
               (SELECT count() FROM edges) AS b,
               (SELECT count() FROM edges
                WHERE child IN (SELECT name FROM ambiguous)
                   OR parent IN (SELECT name FROM ambiguous)) AS c,
               (SELECT max(p) FROM (SELECT repository_id,
                                           uniqExact(name) AS p
                                    FROM facts
                                    GROUP BY repository_id)) AS d''',
        'these four replaced numbers pasted into the copy, which had gone '
        'stale by half',
    ),
)


def show(rows: Rows) -> str:
    if not rows:
        return '(no rows)'
    if len(rows) == 1:
        return ' '.join(
            f'{v:,}' if isinstance(v, int) else str(v)
            for v in rows[0]
        )
    return f'{len(rows)} rows'


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()

    # The export account: the guest profile caps result rows and breaks
    # off silently, which would make a truncated answer look like a
    # disagreement.
    repo = get_container().get_export_repository()
    client = repo.client
    failures = 0

    print(
        f"{'rollup':<24}{'scope':<7}{'via':<10}{'verdict':<11}"
        'cached vs computed',
    )
    print('-' * 88)
    for check in CHECKS:
        cached = answer(check.cached, client, repo)
        computed = answer(check.computed, client, repo)
        agree = cached == computed
        if not agree:
            failures += 1

        mark = 'ok' if agree else 'DISAGREES'
        print(
            f'{check.rollup:<24}{check.scope:<7}{check.via:<10}{mark:<11}'
            f'{show(cached)}',
        )
        if not agree:
            print(f'{"":<52}{show(computed)}  <- computed')
            # Name the rows that differ, not just the fact that some do.
            for got, want in zip(cached, computed):
                if got != want:
                    print(f'      cached {got}')
                    print(f'      computed {want}')
            if len(cached) != len(computed):
                print(f'      row counts differ: {len(cached)} vs '
                      f'{len(computed)}')
        if check.note:
            print(f'      {check.note}')

    print('-' * 88)
    if failures:
        print(f'{failures} of {len(CHECKS)} checks disagree with the '
              f'answers computed another way.')
    else:
        print(f'All {len(CHECKS)} checks agree with the answers computed '
              f'another way.')
    return failures


if __name__ == '__main__':
    sys.exit(main())
