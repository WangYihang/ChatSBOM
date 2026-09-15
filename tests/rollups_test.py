"""The refreshable rollups: order, and what they must not claim.

These assert the declaration, not the numbers — the numbers are checked
against the base tables by a script that needs a populated ClickHouse,
and what can be pinned here is the structure that made them right.
"""
from __future__ import annotations

from chatsbom.core.rollups import REFRESH_ORDER
from chatsbom.core.rollups import REFRESH_SETTINGS
from chatsbom.core.rollups import ROLLUPS


def _without_comments(ddl: str) -> str:
    """The DDL as ClickHouse will read it, with `--` lines removed.

    Every assertion here is a substring check against SQL text, and a
    comment is text too: commenting out the line a test guards passed
    that test. Anything explanatory has to go before the executable
    part is inspected.
    """
    return '\n'.join(
        line for line in ddl.split('\n')
        if not line.strip().startswith('--')
    )


class TestDeclaration:

    def test_every_rollup_is_refreshable(self) -> None:
        """A view without REFRESH is never recomputed, so it answers
        with whatever the corpus looked like when it was created."""
        for name, ddl in ROLLUPS:
            assert 'REFRESH EVERY' in ddl, name

    def test_creating_one_does_not_empty_it(self) -> None:
        """`IF NOT EXISTS`, because the stored rows are the expensive
        part. A plain CREATE on an existing view would drop its contents
        and leave every panel blank until the next refresh."""
        for name, ddl in ROLLUPS:
            assert 'IF NOT EXISTS' in ddl, name

    def test_a_derived_rollup_comes_after_its_source(self) -> None:
        """`mv_totals` and `mv_top_packages` read the rollups above
        them. Declared or refreshed in the wrong order, they summarise
        an empty view or the previous run — `mv_totals` did exactly
        that, storing four numbers computed from a
        `mv_package_language` that had not finished refreshing.
        """
        order = list(REFRESH_ORDER)
        assert order.index('mv_package_language') < order.index('mv_totals')
        assert order.index('mv_repository_deps') < order.index('mv_totals')
        assert (
            order.index('mv_package_language')
            < order.index('mv_top_packages')
        )

    def test_refresh_order_matches_declaration_order(self) -> None:
        assert REFRESH_ORDER == tuple(name for name, _ in ROLLUPS)

    def test_the_experimental_flag_is_carried(self) -> None:
        """Refreshable views are behind a setting in 25.12; without it
        both the CREATE and the REFRESH are rejected."""
        assert REFRESH_SETTINGS[
            'allow_experimental_refreshable_materialized_view'
        ] == 1


class TestExactness:

    def test_the_language_rollup_counts_distinct_repositories(self) -> None:
        """Not `count()`.

        A repository appears once per manifest a package is found in, so
        counting rows would report records and call them repositories.
        """
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_package_language')
        assert 'uniqExact(a.repository_id)' in ddl

    def test_language_is_lowercased_in_the_rollup(self) -> None:
        """The dashboard's filter sends lowercase and
        `repositories.language` is capitalised as GitHub spells it —
        `PHP`, `JavaScript`. Without this the filter matched nothing and
        read as a language with no packages, which is what happened."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_package_language')
        assert 'lower(r.language)' in ddl

    def test_the_licence_rollup_keeps_the_unknown_bucket(self) -> None:
        """`ARRAY JOIN` drops a row whose array is empty.

        Migrating this rollup to ClickHouse therefore deleted the
        largest category in the licences panel: 23,022 of 24,339
        repositories hold at least one package with no licence at all,
        against 16,846 for MIT, so "we do not know" outranks every real
        licence. The panel put MIT on top while its own note promised
        "Unknown is shown rather than dropped ... hiding it would
        overstate coverage", and `Overview.tsx` was already rendering
        the empty key as `(unknown)`.
        """
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_licenses')
        # Comments stripped first. The obvious spelling of this test
        # passed against a `-- UNION ALL`, because a substring check
        # cannot tell SQL from a note about SQL.
        sql = _without_comments(ddl)
        assert 'UNION ALL' in sql
        assert 'WHERE empty(licenses)' in sql
        assert "'' AS license" in sql

    def test_the_ambiguity_rollup_reads_its_three_sources(self) -> None:
        """It replaced four numbers hardcoded in the dashboard's copy,
        which had gone stale by half — 23.7% claimed against 51.5%
        true. Being derived, it has to be created and refreshed after
        every rollup it reads."""
        names = [name for name, _ in ROLLUPS]
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_edge_ambiguity')
        for source in (
            'mv_packages', 'mv_package_type', 'mv_edges_forward',
            'mv_repository_deps',
        ):
            assert source in ddl
            assert names.index(source) < names.index('mv_edge_ambiguity')

    def test_totals_does_not_sum_repositories_across_names(self) -> None:
        """A repository has many packages, so summing a per-name
        distinct count would multiply it. The repository count comes
        from the per-repository rollup, where each appears once."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_totals')
        assert 'count() FROM mv_repository_deps' in ddl

    def test_the_ranking_stores_more_depth_than_the_panel_shows(self) -> None:
        """The panel's limit is a parameter. A rollup holding exactly
        the default would answer a larger request with a short list and
        no sign that it had been truncated."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_top_packages')
        assert 'rank <= 100' in ddl

    def test_the_ranking_carries_both_orderings(self) -> None:
        """`--direct-only` reorders rather than filters, so the rollup
        needs a rank per ordering; one rank plus a re-sort at query time
        would rank the top 100 by total, not the top 100 by direct."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_top_packages')
        assert 'ORDER BY repositories DESC' in ddl
        assert 'ORDER BY direct_repositories DESC' in ddl


class TestArtifactStorage:
    """Storage settings on the fact table, which the rollups cannot help.

    `dependentsOf` takes an arbitrary package name and returns row
    detail, so nothing about it can be precomputed. What is left to tune
    is how much the sparse index has to read to find those rows.
    """

    def test_the_fact_table_reads_in_small_granules(self) -> None:
        """Every point lookup reads a multiple of the granularity, and
        most of it is waste: `laravel/framework` has 299 rows and the
        dependants query read 148,740 of them at the default 8192. At
        1024 it reads 9,216.

        Measured both ways before choosing. Latency gains less than the
        I/O does — 4.3 ms to 3.2 ms, because at this size the query is
        dominated by planning, dictionary lookups and sorting a hundred
        rows rather than by reading from a warm cache — while disk goes
        845 MiB to 933 MiB and a rollup refresh's full scan 142.5 ms to
        149.0 ms.
        """
        from chatsbom.core.schema import ARTIFACTS_DDL
        assert 'index_granularity = 1024' in ARTIFACTS_DDL

    def test_the_sort_key_leads_with_the_column_lookups_filter_on(self) -> None:
        """`name` first, because that is what every point lookup filters
        on and it is the only way the sparse index helps them. A
        repository-first key would make the dashboard's central query a
        full scan.
        """
        from chatsbom.core.schema import ARTIFACTS_DDL
        order = ARTIFACTS_DDL.split('ORDER BY (')[1].split(')')[0]
        assert order.split(',')[0].strip() == 'name'


class TestRecordsCountFactsNotRows:
    """`records` must mean distinct dependency facts in both backends.

    GitHub's dependency graph reports per manifest, so a package
    declared in both `package.json` and `packages/x/package.json` is
    two `artifacts` rows differing only in `artifact_id`. Counting rows
    made the front page say 19,384,165 where the distinct count is
    16,905,915 — and the D1 export has always grouped them away,
    because its schema has no `artifact_id`, so the two stores answered
    `totals().dependencies` with different numbers.

    The repeats are a dependency-graph artefact, not a Syft one: 18% of
    depgraph rows against 1.4% of Syft's.
    """

    KEY = (
        'SELECT DISTINCT repository_id, name, version, type, found_by,\n'
        '                    relationship, source, version_kind'
    )

    def test_the_two_counting_rollups_deduplicate(self) -> None:
        for name in ('mv_package_language', 'mv_repository_deps'):
            _, ddl = next(r for r in ROLLUPS if r[0] == name)
            sql = _without_comments(ddl)
            assert 'SELECT DISTINCT' in sql, name
            assert 'artifact_id' not in sql, name

    def test_the_key_matches_the_export(self) -> None:
        """If these drift the backends disagree again, silently."""
        from chatsbom.export.queries import ARTIFACTS_QUERY
        exported = {
            'repository_id', 'name', 'version', 'type', 'found_by',
            'relationship', 'source', 'version_kind',
        }
        grouped = _without_comments(ARTIFACTS_QUERY)
        grouped = grouped[grouped.index('GROUP BY'):]
        for column in exported:
            assert column in grouped, column
        for name in ('mv_package_language', 'mv_repository_deps'):
            _, ddl = next(r for r in ROLLUPS if r[0] == name)
            distinct = _without_comments(ddl)
            distinct = distinct[distinct.index('SELECT DISTINCT'):]
            for column in exported:
                assert column in distinct, f'{name}: {column}'

    def test_the_totals_read_the_deduplicated_rollups(self) -> None:
        """`mv_totals` sums from these two, so it inherits the fix
        rather than needing its own."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_totals')
        sql = _without_comments(ddl)
        assert 'FROM mv_repository_deps' in sql
        assert 'FROM artifacts' not in sql
