"""The refreshable rollups: order, and what they must not claim.

These assert the declaration, not the numbers — the numbers are checked
against the base tables by a script that needs a populated ClickHouse,
and what can be pinned here is the structure that made them right.
"""
from __future__ import annotations

from chatsbom.core.rollups import REFRESH_ORDER
from chatsbom.core.rollups import REFRESH_SETTINGS
from chatsbom.core.rollups import ROLLUPS


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
