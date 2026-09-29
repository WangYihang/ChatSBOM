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
  python     rows fetched at a finer grain and folded here: the corpus
             (the current search snapshot, #55 D2), the top-twelve
             language fold (D7) and the canonical ecosystem of a type
             are decided in Python, not by the SQL the rollups use.
  artifacts  every observation, for the one rollup that is history.
  edges      the edge table, which records no scans.

**Keyed by ecosystem, never summed across it** (#55 §4.12). A
repository has as many ecosystems as it has manifests for, so every
whole-corpus distinct count is checked against a `uniqExact` of its own,
and the per-ecosystem rows against the CLI asked with that ecosystem.
The footer reports what summing across ecosystems would have claimed.

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
import re
import sys
from collections import Counter
from collections.abc import Callable
from typing import Any

from chatsbom.core.container import get_container
from chatsbom.core.ecosystems import canonical
from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import LANGUAGE_BUCKETS

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


# -- the corpus, decided here rather than by the `corpus` view ---------

_DATE = re.compile(r'([0-9]{4}-[0-9]{2}-[0-9]{2})$')


def current_snapshot(names: set[str]) -> str:
    """The corpus's snapshot, by the rule `CURRENT_SNAPSHOT` states in
    SQL: an `all-*` one before any other, then the newest date in its
    name, then the name. '' when no repository names one."""
    if not names:
        return ''

    def key(name: str) -> tuple[bool, str, str]:
        match = _DATE.search(name)
        return (
            name.startswith('all-'), match.group(1) if match else '', name,
        )
    return max(names, key=key)


class Corpus:
    """The corpus and its repositories' attributes, read from
    `repositories FINAL` and selected and folded here."""

    def __init__(self, client: Any) -> None:
        rows = run(
            client,
            'SELECT id, snapshot, github_language, ecosystems '
            'FROM repositories FINAL',
        )
        self.snapshot = current_snapshot({str(r[1]) for r in rows})
        self.rows = [
            (int(r[0]), str(r[2]), [str(e) for e in r[3]])
            for r in rows if str(r[1]) == self.snapshot
        ]
        self.ids = {r[0] for r in self.rows}
        counts: Counter[str] = Counter(
            language.lower() for _, language, _ in self.rows if language
        )
        self.buckets = sorted(
            counts.items(), key=lambda pair: (-pair[1], pair[0]),
        )[:LANGUAGE_BUCKETS]
        self._top = {language for language, _ in self.buckets}

    def bucket(self, language: str) -> str:
        if not language:
            return 'none'
        lowered = language.lower()
        return lowered if lowered in self._top else 'other'


_corpus: Corpus | None = None
_sources: dict[int, dict[str, set[str]]] | None = None


def corpus(client: Any) -> Corpus:
    global _corpus
    if _corpus is None:
        _corpus = Corpus(client)
    return _corpus


def repository_sources(client: Any) -> dict[int, dict[str, set[str]]]:
    """Per repository with a current fact: source -> the ecosystems it
    has facts of, canonicalised here."""
    global _sources
    if _sources is None:
        _sources = {}
        for repository_id, kind, source in run(
            client, 'SELECT DISTINCT repository_id, type, source FROM facts',
        ):
            _sources.setdefault(int(repository_id), {}).setdefault(
                str(source), set(),
            ).add(canonical(str(kind)))
    return _sources


# -- the answers computed another way -----------------------------------

def corpus_computed(client: Any, repo: QueryRepository) -> Rows:
    ids = corpus(client).ids
    return [(len(ids), sum(ids))]


def buckets_computed(client: Any, repo: QueryRepository) -> Rows:
    return [tuple(pair) for pair in corpus(client).buckets]


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


def top_ecosystems(client: Any, limit: int = 4) -> list[str]:
    """The ecosystems with the most records, as the rollup has them."""
    return [
        str(e) for (e,) in run(
            client,
            "SELECT ecosystem FROM mv_ecosystem_totals WHERE ecosystem != '' "
            'ORDER BY records DESC, ecosystem LIMIT {limit:UInt32}',
            limit=limit,
        )
    ]


def by_ecosystem_cached(client: Any, repo: QueryRepository) -> Rows:
    rows: Rows = []
    for ecosystem in top_ecosystems(client):
        rows.extend(
            run(
                client,
                'SELECT ecosystem, name, repositories, direct_repositories '
                'FROM mv_package_ecosystem WHERE ecosystem = {e:String} '
                'ORDER BY repositories DESC, name LIMIT 5',
                e=ecosystem,
            ),
        )
    return rows


def by_ecosystem_computed(client: Any, repo: QueryRepository) -> Rows:
    return [
        (
            ecosystem,
            name,
            repo.get_dependent_count(name, ecosystem=ecosystem),
            repo.get_dependent_count(
                name, ecosystem=ecosystem, direct_only=True,
            ),
        )
        for ecosystem, name, _, _ in by_ecosystem_cached(client, repo)
    ]


def package_ecosystem_computed(client: Any, repo: QueryRepository) -> Rows:
    """Rows, records and the splits, from facts grouped by the raw type
    and folded to ecosystems here."""
    keys: set[tuple[str, str]] = set()
    totals = [0] * 5
    for kind, name, *counts in run(
        client,
        """SELECT type, name, count(), countIf(relationship = 'direct'),
                  countIf(source = 'syft'),
                  countIf(source = 'github-depgraph'),
                  countIf(source = 'manifest')
           FROM facts GROUP BY type, name""",
    ):
        keys.add((canonical(str(kind)), str(name)))
        for position, count in enumerate(counts):
            totals[position] += int(count)
    return [(len(keys), *totals)]


def ecosystem_totals_computed(client: Any, repo: QueryRepository) -> Rows:
    totals: dict[str, list[int]] = {}
    for kind, relationship, source, count in run(
        client,
        'SELECT type, relationship, source, count() FROM facts '
        'GROUP BY type, relationship, source',
    ):
        row = totals.setdefault(canonical(str(kind)), [0] * 7)
        row[{'direct': 0, 'transitive': 1}.get(str(relationship), 2)] += count
        row[3 + {'syft': 0, 'github-depgraph': 1}.get(str(source), 2)] += count
        row[6] += count
    return sorted((ecosystem, *row) for ecosystem, row in totals.items())


def language_coverage_computed(client: Any, repo: QueryRepository) -> Rows:
    the = corpus(client)
    sources = repository_sources(client)
    counts: dict[str, list[int]] = {}
    for repository_id, language, _ in the.rows:
        row = counts.setdefault(the.bucket(language), [0] * 5)
        found = sources.get(repository_id, {})
        row[0] += 1
        row[1] += bool(found)
        row[2] += 'syft' in found
        row[3] += 'github-depgraph' in found
        row[4] += 'manifest' in found
    return sorted((bucket, *row) for bucket, row in counts.items())


def ecosystem_coverage_computed(client: Any, repo: QueryRepository) -> Rows:
    the = corpus(client)
    sources = repository_sources(client)
    counts: dict[str, list[int]] = {}
    for repository_id, _, ecosystems in the.rows:
        found = sources.get(repository_id, {})
        for ecosystem in ecosystems:
            row = counts.setdefault(ecosystem, [0] * 5)
            row[0] += 1
            row[1] += any(ecosystem in held for held in found.values())
            row[2] += ecosystem in found.get('syft', set())
            row[3] += ecosystem in found.get('github-depgraph', set())
            row[4] += ecosystem in found.get('manifest', set())
    return sorted((ecosystem, *row) for ecosystem, row in counts.items())


def ecosystem_coverage_cli(client: Any, repo: QueryRepository) -> Rows:
    return sorted(
        (
            e.ecosystem, e.repository_count, e.syft_count,
            e.depgraph_count, e.manifest_count,
        )
        for e in repo.get_ecosystem_stats()
    )


def totals_computed(client: Any, repo: QueryRepository) -> Rows:
    (repositories, dependencies, packages, classified), = run(
        client,
        """SELECT uniqExact(repository_id), count(), uniqExact(name),
                  countIf(relationship IN ('direct', 'transitive'))
           FROM facts""",
    )
    return [(
        repositories, dependencies, packages, classified,
        len(corpus(client).ids),
    )]


def month_computed(client: Any, repo: QueryRepository) -> Rows:
    # The corpus by the snapshot decided here, not by the view; the
    # month in UTC, as the rollup makes it.
    return run(
        client,
        """SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, source, toStartOfMonth(observed_at, 'UTC') AS m,
                      uniqExact(repository_id) AS r_
               FROM artifacts
               WHERE repository_id IN (
                   SELECT id FROM repositories FINAL
                   WHERE snapshot = {snapshot:String})
               GROUP BY name, source, m)""",
        snapshot=corpus(client).snapshot,
    )


def top_by_ecosystem_cached(client: Any, repo: QueryRepository) -> Rows:
    rows: Rows = []
    for ecosystem in top_ecosystems(client, 3):
        rows.extend(
            run(
                client,
                """SELECT ecosystem, name, repositories, direct_repositories
                   FROM mv_top_packages
                   WHERE ecosystem = {e:String} AND direct_only = 0
                   ORDER BY rank LIMIT 20""",
                e=ecosystem,
            ),
        )
    return rows


def top_by_ecosystem_computed(client: Any, repo: QueryRepository) -> Rows:
    rows: Rows = []
    for ecosystem in top_ecosystems(client, 3):
        rows.extend(
            (ecosystem, p.name, p.repository_count, p.direct_count)
            for p in repo.get_top_packages(limit=20, ecosystem=ecosystem)
        )
    return rows


#: Every rollup, paired with the same question asked another way. Where
#: a rollup holds one row per name the comparison is an aggregate over
#: all of them, which catches a wrong total without grouping twenty
#: million rows twice.
CHECKS: tuple[Check, ...] = (
    Check(
        'corpus (view)', 'full', 'python',
        'SELECT count(), sum(id) FROM corpus',
        corpus_computed,
        'the current search snapshot; every rollup below counts it only',
    ),
    Check(
        'language_buckets (view)', 'full', 'python',
        'SELECT language, repositories FROM language_buckets '
        'ORDER BY repositories DESC, language',
        buckets_computed,
        'the top twelve GitHub languages of the corpus (D7)',
    ),
    Check(
        'mv_package_ecosystem', 'sample', 'cli',
        by_ecosystem_cached, by_ecosystem_computed,
        'the five most used names in each of the four largest '
        'ecosystems, against `db query --ecosystem`',
    ),
    Check(
        'mv_package_ecosystem', 'agg', 'python',
        """SELECT count(), sum(records), sum(direct_records),
                  sum(syft_records), sum(depgraph_records),
                  sum(manifest_records)
           FROM mv_package_ecosystem""",
        package_ecosystem_computed,
        'types folded to ecosystems in Python, not by the SQL transform',
    ),
    Check(
        'mv_repository_deps', 'agg', 'facts',
        'SELECT count() AS n, sum(packages) AS a, '
        'sum(direct_packages) AS b, sum(records) AS c, '
        'sum(syft_records), sum(depgraph_records), sum(manifest_records) '
        'FROM mv_repository_deps',
        """SELECT count() AS n, sum(p_) AS a, sum(d_) AS b, sum(c_) AS c,
                  sum(s_), sum(g_), sum(m_)
           FROM (
               SELECT repository_id, uniqExact(name) AS p_,
                      uniqExactIf(name, relationship = 'direct') AS d_,
                      count() AS c_,
                      countIf(source = 'syft') AS s_,
                      countIf(source = 'github-depgraph') AS g_,
                      countIf(source = 'manifest') AS m_
               FROM facts GROUP BY repository_id)""",
        'records counts current facts, not rows: the dependency graph '
        'reports per manifest, and a repository keeps every scan',
    ),
    Check(
        'mv_licenses', 'full', 'facts',
        'SELECT count() AS n, sum(repositories) AS a, sum(packages) AS b '
        'FROM mv_licenses',
        """SELECT count() AS n, sum(r_) AS a, sum(p_) AS b FROM (
               SELECT l, uniqExact(repository_id) AS r_, uniqExact(name) AS p_
               FROM current_artifacts ARRAY JOIN licenses AS l GROUP BY l
               UNION ALL
               SELECT '' AS l, uniqExact(repository_id) AS r_,
                      uniqExact(name) AS p_
               FROM current_artifacts WHERE empty(licenses))""",
        'the empty key is the unknown bucket; ARRAY JOIN alone drops it',
    ),
    Check(
        'mv_ecosystem_totals', 'full', 'python',
        """SELECT ecosystem, direct_records, transitive_records,
                  unknown_records, syft_records, depgraph_records,
                  manifest_records, records
           FROM mv_ecosystem_totals ORDER BY ecosystem""",
        ecosystem_totals_computed,
        'records partition by ecosystem, so these are the only counts '
        'that may be summed across it',
    ),
    Check(
        'mv_packages', 'sample', 'cli',
        packages_cached, packages_computed,
        'the twenty most used names, each counted by `db query`',
    ),
    Check(
        'mv_packages', 'agg', 'facts',
        'SELECT count(), sum(repositories), sum(direct_repositories) '
        'FROM mv_packages',
        """SELECT count(), sum(r_), sum(d_) FROM (
               SELECT name, uniqExact(repository_id) AS r_,
                      uniqExactIf(repository_id, relationship = 'direct')
                          AS d_
               FROM current_artifacts GROUP BY name)""",
        'uniqExact per name over the corpus, never a sum over ecosystems',
    ),
    Check(
        'mv_edges_forward', 'full', 'edges',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_edges_forward',
        'SELECT count() AS n, sum(repositories) AS a FROM edges',
    ),
    Check(
        'mv_package_month', 'agg', 'artifacts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_month',
        month_computed,
        "history: every observation of the corpus's repositories, where "
        'every other rollup reads the current scan; one line per '
        'collector, never a line across both',
    ),
    Check(
        'mv_package_type', 'agg', 'facts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_type',
        """SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, type, uniqExact(repository_id) AS r_
               FROM facts GROUP BY name, type)""",
    ),
    Check(
        'mv_package_version', 'agg', 'facts',
        'SELECT count() AS n, sum(repositories) AS a FROM mv_package_version',
        """SELECT count() AS n, sum(r_) AS a FROM (
               SELECT name, if(version_kind = 'resolved', version, '') AS v_,
                      version_kind, uniqExact(repository_id) AS r_
               FROM facts GROUP BY name, v_, version_kind)""",
        'constraints and resolutions kept apart by version_kind, and what '
        'is set aside counted once a kind rather than once a string',
    ),
    Check(
        'mv_dependency_buckets', 'full', 'facts',
        'SELECT count() AS n, sum(repositories) AS a '
        'FROM mv_dependency_buckets',
        """SELECT count() AS n, sum(c_) AS a FROM (
               SELECT multiIf(p < 10, 0, p < 25, 1, p < 100, 2,
                              p < 250, 3, p < 1000, 4, 5) AS bucket,
                      count() AS c_
               FROM (SELECT repository_id, uniqExact(name) AS p
                     FROM facts GROUP BY repository_id)
               GROUP BY bucket)""",
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
        'mv_language_coverage', 'full', 'python',
        """SELECT language, repositories, with_sbom, with_syft,
                  with_depgraph, with_manifest
           FROM mv_language_coverage ORDER BY language""",
        language_coverage_computed,
        'the denominator is every repository of the snapshot, collected '
        'or not; languages folded to the top twelve in Python',
    ),
    Check(
        'mv_ecosystem_coverage', 'full', 'python',
        """SELECT ecosystem, repositories, with_any, with_syft,
                  with_depgraph, with_manifest
           FROM mv_ecosystem_coverage ORDER BY ecosystem""",
        ecosystem_coverage_computed,
        'a repository counts under every ecosystem it has: rows overlap',
    ),
    Check(
        'mv_ecosystem_coverage', 'full', 'cli',
        """SELECT ecosystem, repositories, with_syft, with_depgraph,
                  with_manifest
           FROM mv_ecosystem_coverage ORDER BY ecosystem""",
        ecosystem_coverage_cli,
        'against the table `db status` prints',
    ),
    Check(
        'mv_totals', 'full', 'facts',
        'SELECT repositories AS n, dependencies AS a, packages AS b, '
        'classified AS c, tracked AS d FROM mv_totals',
        totals_computed,
        'the headline numbers, each a uniqExact of its own over the '
        'corpus, never a sum of group counts; tracked is the corpus',
    ),
    Check(
        'mv_top_packages', 'sample', 'cli',
        """SELECT name, repositories, direct_repositories
           FROM mv_top_packages
           WHERE ecosystem = '' AND direct_only = 0
           ORDER BY rank LIMIT 20""",
        ranking_computed,
        'the ranking `db query` prints, top 20 row for row',
    ),
    Check(
        'mv_top_packages', 'sample', 'facts',
        # `direct_only = 1` and the empty ecosystem are the front page's
        # default: the declared-only ranking across all ecosystems.
        """SELECT name, direct_repositories FROM mv_top_packages
           WHERE ecosystem = '' AND direct_only = 1
           ORDER BY rank LIMIT 20""",
        """SELECT name, uniqExactIf(repository_id, relationship = 'direct')
                      AS d_
           FROM facts GROUP BY name
           ORDER BY d_ DESC, name LIMIT 20""",
        'the ranking the front page leads with, top 20 row for row',
    ),
    Check(
        'mv_top_packages', 'sample', 'cli',
        top_by_ecosystem_cached, top_by_ecosystem_computed,
        'the top 20 of each of the three largest ecosystems, against '
        '`QueryRepository.get_top_packages(ecosystem=...)`',
    ),
    Check(
        'mv_edge_ambiguity', 'full', 'facts',
        'SELECT names AS n, ambiguous_names AS a, edges AS b, '
        'ambiguous_edges AS c, largest_repository AS d FROM mv_edge_ambiguity',
        # The mapping the rollup reads, not a copy of it: a pasted one
        # went stale, and Syft's `pod` and `dart-pub` were ecosystems of
        # their own here and not there (#120).
        f"""WITH ambiguous AS (
               SELECT name FROM mv_package_type GROUP BY name
               HAVING uniqExact({canonical_sql('type')}) > 1)
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
                                    GROUP BY repository_id)) AS d""",
        'these four replaced numbers pasted into the copy, which had gone '
        'stale by half',
    ),
)


def double_count(client: Any) -> str:
    """What summing per-ecosystem repository counts would have claimed,
    against the distinct counts the rollups store. Reported, not
    checked: it is the error keying by ecosystem has to avoid."""
    (summed, distinct), = run(
        client,
        """SELECT
               (SELECT sum(repositories) FROM mv_package_ecosystem),
               (SELECT sum(repositories) FROM mv_packages)""",
    )
    return (
        f'(package, repository) pairs: {distinct:,} counted once; summed '
        f'across ecosystems they would be {summed:,} '
        f'({summed - distinct:+,})'
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
    global _corpus, _sources
    _corpus = _sources = None

    # The export account: the guest profile caps result rows and breaks
    # off silently, which would make a truncated answer look like a
    # disagreement.
    repo = get_container().get_export_repository()
    client = repo.client
    failures = 0

    print(
        f"{'rollup':<26}{'scope':<7}{'via':<10}{'verdict':<11}"
        'cached vs computed',
    )
    print('-' * 90)
    for check in CHECKS:
        cached = [tuple(row) for row in answer(check.cached, client, repo)]
        computed = [
            tuple(row) for row in answer(check.computed, client, repo)
        ]
        agree = cached == computed
        if not agree:
            failures += 1

        mark = 'ok' if agree else 'DISAGREES'
        print(
            f'{check.rollup:<26}{check.scope:<7}{check.via:<10}{mark:<11}'
            f'{show(cached)}',
        )
        if not agree:
            print(f'{"":<54}{show(computed)}  <- computed')
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

    print('-' * 90)
    print(double_count(client))
    if failures:
        print(f'{failures} of {len(CHECKS)} checks disagree with the '
              f'answers computed another way.')
    else:
        print(f'All {len(CHECKS)} checks agree with the answers computed '
              f'another way.')
    return failures


if __name__ == '__main__':
    sys.exit(main())
