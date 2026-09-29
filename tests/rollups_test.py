"""The refreshable rollups: order, and what they must not claim.

These assert the declaration, not the numbers — the numbers are checked
against the base tables by a script that needs a populated ClickHouse,
and what can be pinned here is the structure that made them right.
"""
from __future__ import annotations

import re

from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.rollups import OBSOLETE_ROLLUPS
from chatsbom.core.rollups import REFRESH_ORDER
from chatsbom.core.rollups import ROLLUPS
from chatsbom.core.schema import language_bucket_sql


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
        for source, derived in (
            ('mv_ecosystem_totals', 'mv_totals'),
            ('mv_repository_deps', 'mv_totals'),
            ('mv_packages', 'mv_totals'),
            ('mv_package_ecosystem', 'mv_ecosystem_totals'),
            ('mv_package_ecosystem', 'mv_top_packages'),
            ('mv_packages', 'mv_top_packages'),
            ('mv_repository_deps', 'mv_language_coverage'),
        ):
            assert order.index(source) < order.index(derived), (
                source, derived,
            )

    def test_every_rollup_names_only_rollups_declared_before_it(self) -> None:
        """The general form of the above: a rollup reading another
        must come after it, whatever the pair."""
        names = [name for name, _ in ROLLUPS]
        for position, (name, ddl) in enumerate(ROLLUPS):
            sql = _without_comments(ddl)
            body = sql[sql.index(' AS'):]
            for other in names:
                if other != name and re.search(rf'\b{other}\b', body):
                    assert names.index(other) < position, (other, name)

    def test_refresh_order_matches_declaration_order(self) -> None:
        assert REFRESH_ORDER == tuple(name for name, _ in ROLLUPS)


class TestExactness:

    def test_the_ecosystem_rollup_counts_distinct_repositories(self) -> None:
        """Not `count()`.

        A repository appears once per manifest a package is found in, so
        counting rows would report records and call them repositories.
        """
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_package_ecosystem')
        assert 'uniqExact(repository_id)' in ddl

    def test_ecosystems_are_canonical_in_the_rollup(self) -> None:
        """Keyed by the canonical ecosystem, so Syft's `java-archive`
        and the graph's `maven` are one filter value, as the dashboard's
        ecosystem names are."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_package_ecosystem')
        assert canonical_sql('type') + ' AS ecosystem' in ddl

    def test_no_rollup_is_keyed_by_the_repositorys_language(self) -> None:
        """A repository's GitHub language is an attribute; it selects no
        dependencies (#55). Only the coverage panel groups by it, folded
        to the top twelve (D7)."""
        for name, ddl in ROLLUPS:
            sql = _without_comments(ddl)
            assert 'r.language' not in sql, name
            assert 'lower(language)' not in sql, name
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_language_coverage')
        assert language_bucket_sql('r.github_language') in ddl

    def test_whole_corpus_counts_are_never_sums_across_ecosystems(
        self,
    ) -> None:
        """A repository has as many ecosystems as it has manifests for,
        so summing a per-ecosystem distinct count counts it once per
        ecosystem. `mv_packages` used to be the sum of the per-language
        rows, which was exact only while each repository had one
        language."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_packages')
        sql = _without_comments(ddl)
        assert 'uniqExact(repository_id)' in sql
        assert 'sum(' not in sql
        assert re.search(r'\bFROM facts\b', sql)
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_totals')
        sql = _without_comments(ddl)
        assert 'mv_package_ecosystem' not in sql
        # Records partition by ecosystem, so that one sum is exact.
        assert 'FROM mv_ecosystem_totals' in sql

    def test_the_ecosystem_totals_carry_records_only(self) -> None:
        """They are summed across ecosystems by every panel that reads
        them unfiltered, which is exact for records and wrong for a
        repository count; so there is none to sum."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_ecosystem_totals')
        assert 'repositories' not in _without_comments(ddl)

    def test_the_corpus_row_of_the_ranking_is_counted_once(self) -> None:
        """The `''` row reads `mv_packages`, not a sum of the
        per-ecosystem rows."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_top_packages')
        sql = _without_comments(ddl)
        corpus_row = sql[sql.index("SELECT '' AS ecosystem"):]
        corpus_row = corpus_row[:corpus_row.index('UNION ALL')]
        assert 'FROM mv_packages' in corpus_row
        assert 'sum(' not in corpus_row

    def test_coverage_is_out_of_the_whole_snapshot(self) -> None:
        """The denominator is every repository of the current snapshot,
        collected or not: a LEFT JOIN from the corpus, not a rollup over
        what was collected."""
        for name in ('mv_language_coverage', 'mv_ecosystem_coverage'):
            _, ddl = next(r for r in ROLLUPS if r[0] == name)
            sql = _without_comments(ddl)
            assert re.search(r'\bcorpus\b', sql), name
            assert 'LEFT JOIN' in sql, name
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_totals')
        assert '(SELECT count() FROM corpus) AS tracked' in ddl

    def test_the_language_rollups_are_dropped(self) -> None:
        """Declared by earlier releases; `ensure_schema` drops them."""
        names = {name for name, _ in ROLLUPS}
        assert set(OBSOLETE_ROLLUPS) == {
            'mv_package_language', 'mv_language_totals',
        }
        assert names.isdisjoint(OBSOLETE_ROLLUPS)

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
        which had gone stale: 23.7% claimed against a canonical 10.3%. Being derived, it has to be created and refreshed after
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

    The key was pasted into each rollup and into the export, and these
    tests compared the copies. It has one home now, the `facts` view,
    and what they check is that everything counting facts reads it.
    """

    KEY = {
        'repository_id', 'name', 'version', 'type', 'found_by',
        'relationship', 'source', 'version_kind',
    }

    def test_facts_are_distinct_on_the_key(self) -> None:
        """Exactly the key: `artifact_id` is the per-manifest
        discriminator, so keeping it would collapse nothing."""
        from chatsbom.core.schema import FACTS_DDL
        sql = _without_comments(FACTS_DDL)
        selected = sql[sql.index('SELECT DISTINCT') + len('SELECT DISTINCT'):]
        selected = selected[:selected.index('FROM')]
        assert {c.strip() for c in selected.split(',')} == self.KEY

    def test_the_two_counting_rollups_deduplicate(self) -> None:
        for name in ('mv_package_ecosystem', 'mv_repository_deps', 'mv_packages'):
            _, ddl = next(r for r in ROLLUPS if r[0] == name)
            sql = _without_comments(ddl)
            assert re.search(r'\bFROM facts\b', sql), name
            assert 'artifact_id' not in sql, name

    def test_the_key_matches_the_export(self) -> None:
        """If these drift the backends disagree again, silently. They
        read the same view, so they cannot."""
        from chatsbom.export.queries import ARTIFACTS_QUERY
        exported = _without_comments(ARTIFACTS_QUERY)
        assert re.search(r'\bFROM facts\b', exported)
        selected = exported[:exported.index('FROM')]
        for column in self.KEY:
            assert column in selected, column

    def test_the_totals_read_the_deduplicated_rollups(self) -> None:
        """`mv_totals` sums from these two, so it inherits the fix
        rather than needing its own."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_totals')
        sql = _without_comments(ddl)
        assert 'FROM mv_repository_deps' in sql
        assert 'FROM artifacts' not in sql


class TestVersionKindsAddUp:
    """The three kinds must total the dependency count on the page.

    `version_kind` separates a resolution from a constraint — Syft
    reads `2.9.1` from a lockfile, the dependency graph may read
    `>= 2.0, < 3.0` from a manifest — and presenting a range as a
    version in use is the thing the column exists to prevent.

    Deduplicated on the same key as the counting rollups, or the three
    would sum to 2.5 million more than `mv_totals.dependencies` and the
    panel would disagree with the tile above it.
    """

    def test_it_deduplicates_like_the_others(self) -> None:
        """From `facts`, which the counting rollups read too."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_version_kinds')
        sql = _without_comments(ddl)
        assert re.search(r'\bFROM facts\b', sql)
        assert 'artifact_id' not in sql

    def test_it_reads_the_fact_table_not_the_version_rollup(self) -> None:
        """`mv_package_version` holds distinct repository counts per
        version, so summing them across versions counts a repository
        once per version it holds. It also has no `records` column —
        the first attempt failed on that before the arithmetic could be
        wrong."""
        _, ddl = next(r for r in ROLLUPS if r[0] == 'mv_version_kinds')
        sql = _without_comments(ddl)
        assert re.search(r'\bFROM facts\b', sql)
        assert 'mv_package_version' not in sql


class TestCurrentStateOrHistory:
    """Each rollup answers about the present or about change over time.

    `artifacts` keeps every scan. The rollups used to read all of it, so
    the overview counted mail 2.7.1 beside 2.9.1 for a repository the
    CLI showed at 2.9.1 alone. `tests/current_state_test.py` checks the
    answers against a database; these check the declarations, so a new
    rollup cannot reach the whole table without saying it is history.
    """

    #: Reads every observation on purpose: the adoption series.
    HISTORY = {'mv_package_month'}

    def test_only_history_reads_every_observation(self) -> None:
        for name, ddl in ROLLUPS:
            reads_all = bool(
                re.search(r'\bFROM artifacts\b', _without_comments(ddl)),
            )
            assert reads_all == (name in self.HISTORY), name

    def test_every_read_of_repositories_is_final(self) -> None:
        """`repositories` is a ReplacingMergeTree, so its recorded
        commit, stars and row count are only right after deduplication,
        and `db index` leaves a second row behind until its OPTIMIZE."""
        from chatsbom.core.dictionaries import DICTIONARIES
        from chatsbom.core.schema import VIEW_DDL
        # Atomic, so an alias is not given back to let the lookahead
        # pass: `repositories AS r FINAL` is final.
        bare = re.compile(
            r'\b(?:FROM|JOIN)\s+(?:\{database\}\.)?repositories\b'
            r'(?>(?:\s+AS\s+\w+)?)(?!\s+FINAL\b)',
        )
        assert bare.search('FROM repositories AS r GROUP BY 1')
        assert not bare.search('FROM repositories AS r FINAL')
        for name, ddl in (*ROLLUPS, *VIEW_DDL, *DICTIONARIES):
            assert not bare.search(_without_comments(ddl)), name
