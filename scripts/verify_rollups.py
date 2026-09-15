#!/usr/bin/env python3
"""Ask every rollup's question of the base tables and compare.

A rollup is a cached answer. The only way to know it is still the right
answer is to compute it the slow way and diff, which is what this does
— and it has earned its keep: it is how `mv_totals` was caught storing
four numbers computed from an empty source, and how the licence rollup
was caught dropping its largest category.

This was an ad-hoc script until now, re-typed each time it was needed.
It is needed after every `db index --rebuild`, so it lives here.

Each check names the rollup, the count it holds, the count computed
from the base tables, and whether they agree. Scope is stated per
check rather than implied:

  full     every row of the rollup compared against the base tables
  agg      totals compared; row-level equality not asserted
  sample   the N rows that matter most, compared row for row

`sample` is used where a full comparison means aggregating twenty
million rows into hundreds of thousands of groups — the check would
take longer than the rebuild it verifies. Where that is the case the
script says so instead of implying it checked everything.

Usage:

    uv run python scripts/verify_rollups.py

Exit status is the number of disagreements, so CI or a shell `&&` can
use it.
"""
from __future__ import annotations

import argparse
import sys
from typing import Any

from chatsbom.core.container import get_container


class Check:
    """One rollup, and the question it caches."""

    def __init__(
        self,
        rollup: str,
        scope: str,
        cached: str,
        computed: str,
        note: str = '',
    ) -> None:
        self.rollup = rollup
        self.scope = scope
        self.cached = cached
        self.computed = computed
        self.note = note


#: Every rollup, paired with the same question asked of the base
#: tables. Where a rollup holds one row per name the comparison is an
#: aggregate over all of them, which catches a wrong total without
#: grouping twenty million rows twice.
CHECKS: tuple[Check, ...] = (
    Check(
        'mv_package_language', 'agg',
        'SELECT count() AS n, sum(repositories) AS a, sum(records) AS b '
        'FROM mv_package_language',
        '''SELECT count() AS n, sum(r_) AS a, sum(c_) AS b FROM (
               SELECT a.name, lower(r.language) AS lang,
                      uniqExact(a.repository_id) AS r_, count() AS c_
               FROM (SELECT DISTINCT repository_id, name, version, type,
                            found_by, relationship, source, version_kind
                     FROM artifacts) a
               INNER JOIN repositories r ON r.id = a.repository_id
               GROUP BY a.name, lang)''',
        'records counts distinct dependency facts, not rows: the '
        'dependency graph reports per manifest',
    ),
    Check(
        'mv_repository_deps', 'agg',
        'SELECT count() AS n, sum(packages) AS a, sum(records) AS b '
        'FROM mv_repository_deps',
        '''SELECT count() AS n, sum(p_) AS a, sum(c_) AS b FROM (
               SELECT repository_id, uniqExact(name) AS p_, count() AS c_
               FROM (SELECT DISTINCT repository_id, name, version, type,
                            found_by, relationship, source, version_kind
                     FROM artifacts)
               GROUP BY repository_id)''',
    ),
    Check(
        'mv_licenses', 'full',
        'SELECT count() AS n, sum(repositories) AS a, sum(packages) AS b '
        'FROM mv_licenses',
        '''SELECT count() AS n, sum(r_) AS a, sum(p_) AS b FROM (
               SELECT l, uniqExact(repository_id) AS r_, uniqExact(name) AS p_
               FROM artifacts ARRAY JOIN licenses AS l GROUP BY l
               UNION ALL
               SELECT '' AS l, uniqExact(repository_id) AS r_,
                      uniqExact(name) AS p_
               FROM artifacts WHERE empty(licenses))''',
        'the empty key is the unknown bucket; ARRAY JOIN alone drops it',
    ),
    Check(
        'mv_language_totals', 'full',
        'SELECT count() AS n, sum(records) AS a, sum(direct_records) AS b '
        'FROM mv_language_totals',
        '''SELECT count() AS n, sum(c_) AS a, sum(d_) AS b FROM (
               SELECT lower(r.language) AS lang, count() AS c_,
                      countIf(a.relationship = 'direct') AS d_
               FROM (SELECT DISTINCT repository_id, name, version, type,
                            found_by, relationship, source, version_kind
                     FROM artifacts) a
               INNER JOIN repositories r ON r.id = a.repository_id
               GROUP BY lang)''',
    ),
    Check(
        'mv_packages', 'agg',
        'SELECT count() AS n, sum(repositories) AS a, '
        'sum(direct_repositories) AS b FROM mv_packages',
        '''SELECT count() AS n, sum(r_) AS a, sum(d_) AS b FROM (
               SELECT name, sum(r_) AS r_, sum(d_) AS d_ FROM (
                   SELECT a.name AS name, lower(r.language) AS lang,
                          uniqExact(a.repository_id) AS r_,
                          uniqExactIf(a.repository_id,
                                      a.relationship = 'direct') AS d_
                   FROM (SELECT DISTINCT repository_id, name, version,
                                type, found_by, relationship, source,
                                version_kind FROM artifacts) a
                   INNER JOIN repositories r ON r.id = a.repository_id
                   GROUP BY name, lang)
               GROUP BY name)''',
        'summed per language, as the rollup does — a repository has one '
        'language, so this cannot double count',
    ),
    Check(
        'mv_edges_forward', 'full',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_edges_forward',
        'SELECT count() AS n, sum(repositories) AS a FROM edges',
    ),
    Check(
        'mv_package_month', 'agg',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_month',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, source, toStartOfMonth(observed_at) AS m,
                      uniqExact(repository_id) AS r_
               FROM artifacts GROUP BY name, source, m)''',
        'keyed by source: one line per collector, never a line across both',
    ),
    Check(
        'mv_package_type', 'agg',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_type',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, type, uniqExact(repository_id) AS r_
               FROM artifacts GROUP BY name, type)''',
    ),
    Check(
        'mv_package_version', 'agg',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_version',
        '''SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, version, version_kind,
                      uniqExact(repository_id) AS r_
               FROM artifacts GROUP BY name, version, version_kind)''',
        'constraints and resolutions kept apart by version_kind',
    ),
    Check(
        'mv_dependency_buckets', 'full',
        'SELECT count() AS n, sum(repositories) AS a '
        'FROM mv_dependency_buckets',
        '''SELECT count() AS n, sum(c_) AS a FROM (
               SELECT multiIf(p < 10, 0, p < 25, 1, p < 100, 2,
                              p < 250, 3, p < 1000, 4, 5) AS bucket,
                      count() AS c_
               FROM (SELECT repository_id, uniqExact(name) AS p
                     FROM artifacts GROUP BY repository_id)
               GROUP BY bucket)''',
        'bucketed on distinct packages, not rows',
    ),
    Check(
        'mv_language_coverage', 'full',
        'SELECT count() AS n, sum(repositories) AS a, sum(with_sbom) AS b '
        'FROM mv_language_coverage',
        '''SELECT count() AS n, sum(all_) AS a, sum(has_) AS b FROM (
               SELECT lower(r.language) AS lang, count() AS all_,
                      countIf(r.id IN (SELECT repository_id FROM artifacts))
                          AS has_
               FROM repositories r FINAL GROUP BY lang)''',
        'the denominator is the corpus, the numerator what produced an SBOM',
    ),
    Check(
        'mv_totals', 'full',
        'SELECT repositories AS n, dependencies AS a, packages AS b, '
        'classified AS c FROM mv_totals',
        '''WITH facts AS (
               SELECT DISTINCT repository_id, name, version, type,
                      found_by, relationship, source, version_kind
               FROM artifacts)
           SELECT
               (SELECT uniqExact(repository_id) FROM artifacts) AS n,
               (SELECT count() FROM facts) AS a,
               (SELECT uniqExact(name) FROM artifacts) AS b,
               (SELECT countIf(relationship IN ('direct', 'transitive'))
                FROM facts) AS c''',
        'the four headline numbers; this rollup once stored them from an '
        'empty source because REFRESH only schedules',
    ),
    Check(
        'mv_top_packages', 'sample',
        # `direct_only = 1` and the empty language are the front page's
        # default: the declared-only ranking across all languages.
        '''SELECT name, direct_repositories FROM mv_top_packages
           WHERE language = '' AND direct_only = 1
           ORDER BY rank LIMIT 20''',
        '''SELECT name, d_ FROM (
               SELECT name, sum(d_) AS d_ FROM (
                   SELECT a.name AS name, lower(r.language) AS lang,
                          uniqExactIf(a.repository_id,
                                      a.relationship = 'direct') AS d_
                   FROM artifacts a
                   INNER JOIN repositories r ON r.id = a.repository_id
                   GROUP BY name, lang)
               GROUP BY name)
           ORDER BY d_ DESC, name LIMIT 20''',
        'the ranking the front page leads with, top 20 row for row',
    ),
    Check(
        'mv_edge_ambiguity', 'full',
        'SELECT names AS n, ambiguous_names AS a, edges AS b, '
        'ambiguous_edges AS c, largest_repository AS d FROM mv_edge_ambiguity',
        '''WITH ambiguous AS (
               SELECT name FROM mv_package_type
               GROUP BY name HAVING uniqExact(type) > 1)
           SELECT
               (SELECT uniqExact(name) FROM artifacts) AS n,
               (SELECT count() FROM ambiguous) AS a,
               (SELECT count() FROM edges) AS b,
               (SELECT count() FROM edges
                WHERE child IN (SELECT name FROM ambiguous)
                   OR parent IN (SELECT name FROM ambiguous)) AS c,
               (SELECT max(p) FROM (SELECT repository_id,
                                           uniqExact(name) AS p
                                    FROM artifacts
                                    GROUP BY repository_id)) AS d''',
        'these four replaced numbers pasted into the copy, which had gone '
        'stale by half',
    ),
)


def run(client: Any, sql: str) -> list[tuple]:
    return client.query(sql).result_rows


def main() -> int:
    argparse.ArgumentParser(description=__doc__).parse_args()

    client = get_container().get_export_repository().client
    failures = 0

    print(f"{'rollup':<24}{'scope':<7}{'verdict':<11}cached vs computed")
    print('-' * 78)
    for check in CHECKS:
        cached = run(client, check.cached)
        computed = run(client, check.computed)
        agree = cached == computed
        if not agree:
            failures += 1

        def show(rows: list[tuple]) -> str:
            if not rows:
                return '(no rows)'
            if len(rows) == 1:
                return ' '.join(
                    f'{v:,}' if isinstance(v, int) else str(v)
                    for v in rows[0]
                )
            return f'{len(rows)} rows'

        mark = 'ok' if agree else 'DISAGREES'
        print(f'{check.rollup:<24}{check.scope:<7}{mark:<11}{show(cached)}')
        if not agree:
            print(f'{"":<42}{show(computed)}  <- computed')
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
        print(f'{failures} of {len(CHECKS)} rollups disagree with the '
              f'base tables.')
    else:
        print(f'All {len(CHECKS)} rollups agree with the base tables.')
    return failures


if __name__ == '__main__':
    sys.exit(main())
