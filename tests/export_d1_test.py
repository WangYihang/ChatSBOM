"""The D1 export: a normalised SQLite, and SQL that D1 will accept.

Two constraints drive the shape, both measured rather than assumed.

**Size.** A direct translation of the Parquet schema into SQLite is
762.6 MB with the indexes the queries need, and D1's free tier stops at
500 MB. Normalising the repeated strings brings it to 291.5 MB — a 62%
reduction with no rows lost — because the cardinalities are tiny next to
the row count: 6,062,896 artifact rows carry only 141,938 distinct
names, 46,526 versions, and 45 distinct combinations of the five
low-cardinality columns.

**Statement length.** D1 caps a single SQL statement at 100,000 bytes,
and `sqlite3 .dump` emits one INSERT per row — 6,062,896 statements,
which is both enormous and slow to execute over a network. The export
batches rows into multi-row INSERTs sized to stay under the cap.
"""
from __future__ import annotations

from chatsbom.export.d1 import batch_inserts
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import MAX_STATEMENT_BYTES


class TestNormalisedSchema:

    def test_artifacts_reference_lookups_rather_than_repeat_strings(self) -> None:
        artifacts = D1_SCHEMA.table('artifacts')
        assert artifacts.column_names == [
            'repository_id', 'package_id', 'version_id', 'kind_id',
        ]

    def test_the_five_low_cardinality_columns_collapse_into_one_table(self) -> None:
        """type, found_by, relationship, source and version_kind.

        Only 45 distinct combinations exist across 6 million rows, so
        they are one `kinds` row referenced by id rather than five
        strings repeated per artifact.
        """
        kinds = D1_SCHEMA.table('kinds')
        assert kinds.column_names == [
            'id', 'type', 'found_by', 'relationship', 'source', 'version_kind',
        ]

    def test_package_names_are_unique_so_a_lookup_is_a_single_row(self) -> None:
        packages = D1_SCHEMA.table('packages')
        assert packages.column_names == ['id', 'name', 'repositories']
        assert 'name' in packages.unique

    def test_packages_carry_their_own_dependant_count(self) -> None:
        """So the search box can rank by popularity.

        Ranking needs a count for every candidate, not only the ones
        returned, so it cannot be a correlated subquery at query time:
        that is one scan of `artifacts` per candidate name, per
        keystroke. Denormalised onto the row instead, filled once by the
        aggregate script.

        Without it the search orders by name, and `laravel` returns
        forty `laravel-enso/*` packages with one dependant each -- `-`
        is 0x2D and `/` is 0x2F -- never reaching `laravel/framework`,
        which has 98.
        """
        packages = D1_SCHEMA.table('packages')
        column = next(
            c for c in packages.columns if c.name == 'repositories'
        )
        # Defaulted, because the column is written by 03-aggregates.sql
        # and the rows arrive in 02-data.sql. Without a default the
        # data script's INSERTs would not satisfy NOT NULL.
        assert 'DEFAULT 0' in column.type
        assert 'NOT NULL' in column.type

    def test_every_base_table_the_queries_need_is_present(self) -> None:
        """The base tables. Precomputed aggregates are asserted
        separately, in TestPrecomputedAggregates."""
        names = {t.name for t in D1_SCHEMA.tables}
        assert {
            'repositories', 'artifacts', 'packages', 'versions',
            'kinds', 'licenses', 'history',
        } <= names

    def test_derived_aggregates_are_named_apart_from_base_tables(self) -> None:
        """An `agg_` prefix, so a reader can tell derived from source.

        `meta` is neither: it is provenance about the export rather than
        data aggregated from it, so it keeps its own name.
        """
        base = {
            'repositories', 'artifacts', 'packages', 'versions',
            'kinds', 'licenses', 'history',
        }
        for table in D1_SCHEMA.tables:
            if table.name in base or table.name == 'meta':
                continue
            assert table.name.startswith('agg_'), table.name

    def test_indexes_cover_the_query_shapes_the_dashboard_issues(self) -> None:
        """Without these the joins table-scan 6 million rows."""
        indexed = {(i.table, tuple(i.columns)) for i in D1_SCHEMA.indexes}
        assert ('artifacts', ('package_id',)) in indexed
        assert ('artifacts', ('repository_id',)) in indexed
        assert ('repositories', ('language',)) in indexed


class TestBatchedInserts:

    def test_rows_are_grouped_into_multi_row_statements(self) -> None:
        rows = [(i, i, i, i) for i in range(10)]
        statements = list(batch_inserts('artifacts', rows, batch=4))
        # 10 rows at 4 per statement: 4, 4, 2.
        assert len(statements) == 3
        assert statements[0].count('(') == 4
        assert statements[-1].count('(') == 2
        # And every row appears exactly once, in order.
        joined = ''.join(statements)
        assert joined.count('(0,0,0,0)') == 1
        assert joined.index('(0,0,0,0)') < joined.index('(9,9,9,9)')

    def test_no_statement_exceeds_the_d1_limit(self) -> None:
        """A wide row must not be batched into an oversized statement."""
        wide = [('x' * 400,) for _ in range(5000)]
        for statement in batch_inserts('packages', wide):
            assert len(statement.encode()) < MAX_STATEMENT_BYTES

    def test_statements_end_with_a_semicolon(self) -> None:
        for statement in batch_inserts('artifacts', [(1, 2, 3, 4)]):
            assert statement.rstrip().endswith(';')

    def test_strings_are_quoted_and_embedded_quotes_escaped(self) -> None:
        """A package really can be called O'Reilly."""
        statement = next(batch_inserts('packages', [(1, "O'Reilly")]))
        assert "'O''Reilly'" in statement

    def test_no_rows_produces_no_statements(self) -> None:
        assert list(batch_inserts('artifacts', [])) == []


class TestSchemaSql:
    """The DDL D1 receives before any data."""

    def test_emits_create_table_for_every_table(self) -> None:
        from chatsbom.export.d1 import schema_sql
        sql = schema_sql()
        for table in D1_SCHEMA.tables:
            assert f'CREATE TABLE {table.name}' in sql

    def test_carries_no_indexes_at_all(self) -> None:
        """They belong in index_sql, applied after the rows.

        Inserting into an indexed table updates every index per row, so
        the import is markedly faster if the indexes are built once over
        finished data.
        """
        from chatsbom.export.d1 import schema_sql
        assert 'CREATE INDEX' not in schema_sql()

    def test_drops_existing_objects_so_a_reimport_is_clean(self) -> None:
        """D1 keeps whatever was there; a reimport must not double rows."""
        from chatsbom.export.d1 import schema_sql
        sql = schema_sql()
        for table in D1_SCHEMA.tables:
            assert f'DROP TABLE IF EXISTS {table.name}' in sql

    def test_drops_come_before_creates(self) -> None:
        from chatsbom.export.d1 import schema_sql
        sql = schema_sql()
        assert sql.index('DROP TABLE') < sql.index('CREATE TABLE')

    def test_indexes_are_created_after_the_data_is_loaded(self) -> None:
        """Index-then-insert is far slower than insert-then-index.

        The indexes live in their own script for that reason, applied
        after the row data.
        """
        from chatsbom.export.d1 import index_sql
        sql = index_sql()
        assert 'CREATE INDEX' in sql
        assert 'INSERT' not in sql


def _row(
    repository_id: int,
    name: str,
    version: str,
    relationship: str,
) -> dict[str, object]:
    """One artifact row in the shape the export stream yields."""
    return {
        'repository_id': repository_id,
        'name': name,
        'version': version,
        'type': 'gem',
        'found_by': 'gemfile',
        'relationship': relationship,
        'source': 'syft',
        'version_kind': 'exact',
    }


class TestNormalise:
    """Turning wide rows into lookups plus integer references."""

    # Keyed by column name, as the ClickHouse stream yields them. Not
    # positional: this project has already been bitten by positional
    # access, where a column added upstream shifted every later field.
    ROWS = [
        _row(1, 'mail', '2.8.1', 'direct'),
        _row(2, 'mail', '2.9.0', 'transitive'),
        _row(2, 'rails', '7.1.0', 'direct'),
    ]

    def test_each_distinct_name_is_stored_once(self) -> None:
        from chatsbom.export.d1 import normalise
        result = normalise(self.ROWS)
        assert sorted(n for _, n in result.packages) == ['mail', 'rails']

    def test_artifacts_become_four_integers(self) -> None:
        from chatsbom.export.d1 import normalise
        result = normalise(self.ROWS)
        assert len(result.artifacts) == 3
        for row in result.artifacts:
            assert len(row) == 4
            assert all(isinstance(v, int) for v in row)

    def test_the_five_columns_collapse_by_combination_not_per_column(self) -> None:
        """Two rows differing only in relationship are two kinds."""
        from chatsbom.export.d1 import normalise
        result = normalise(self.ROWS)
        assert len(result.kinds) == 2

    def test_references_resolve_back_to_the_original_values(self) -> None:
        """Normalisation must be lossless, so reverse it and compare."""
        from chatsbom.export.d1 import KIND_COLUMNS
        from chatsbom.export.d1 import normalise
        result = normalise(self.ROWS)
        names = dict(result.packages)
        versions = dict(result.versions)
        kinds = {k[0]: k[1:] for k in result.kinds}

        rebuilt = sorted(
            (
                repo, names[pkg], versions[ver],
                *kinds[kind],
            )
            for repo, pkg, ver, kind in result.artifacts
        )
        original = sorted(
            (
                r['repository_id'], r['name'], r['version'],
                *(r[c] for c in KIND_COLUMNS),
            )
            for r in self.ROWS
        )
        assert rebuilt == original

    def test_ids_start_at_one_so_zero_is_never_a_valid_reference(self) -> None:
        from chatsbom.export.d1 import normalise
        result = normalise(self.ROWS)
        assert min(i for i, _ in result.packages) == 1

    def test_no_rows_gives_empty_lookups_rather_than_failing(self) -> None:
        from chatsbom.export.d1 import normalise
        result = normalise([])
        assert result.packages == [] and result.artifacts == []


class TestPrecomputedAggregates:
    """The overview's panels are fixed aggregates; compute them once.

    Measured on the real corpus, against the normalised schema with
    indexes and ANALYZE run:

        sourceComparison     3122 ms   SCAN r SEARCH a SEARCH k
        relationshipSplit    1082 ms   SCAN a SEARCH k
        topPackages           376 ms   SCAN p SEARCH a SEARCH k
        totals                 21 ms   SCAN artifacts

    No index fixes those: they read every one of 6,062,896 artifact rows
    by definition. On D1 that is the bill as well as the latency, since
    it charges for rows read — and the overview is the first thing every
    visitor loads. The panels take no parameters and return tens of
    rows, so they are computed at export time into small tables.

    The point-lookup queries are left alone: `dependentsOf` and
    `countDependents` already run in 4 ms entirely on indexes.
    """

    def test_every_overview_panel_has_a_precomputed_table(self) -> None:
        names = {t.name for t in D1_SCHEMA.tables}
        for panel in (
            'agg_totals',
            'agg_relationship_split',
            'agg_language_coverage',
            'agg_top_packages',
            'agg_dependency_buckets',
            'agg_source_comparison',
        ):
            assert panel in names, panel

    def test_top_packages_is_precomputed_per_filter_combination(self) -> None:
        """The panel has two controls: declared-only, and language.

        So the precomputed rows carry both, and the Worker selects
        rather than aggregates.
        """
        table = D1_SCHEMA.table('agg_top_packages')
        assert 'direct_only' in table.column_names
        assert 'language' in table.column_names
        assert 'rank' in table.column_names

    def test_aggregates_are_indexed_by_what_the_panel_filters_on(self) -> None:
        indexed = {(i.table, tuple(i.columns)) for i in D1_SCHEMA.indexes}
        assert (
            'agg_top_packages', (
                'direct_only',
                'language', 'rank',
            ),
        ) in indexed

    def test_totals_is_a_single_row(self) -> None:
        """Four numbers the footer and the tiles both read."""
        table = D1_SCHEMA.table('agg_totals')
        assert table.column_names == [
            'repositories', 'dependencies', 'packages', 'classified',
        ]


class TestAggregateSql:
    """The SQL that fills the aggregates, run inside SQLite itself."""

    def test_the_package_dependant_count_is_filled(self) -> None:
        """`packages.repositories` is an aggregate wearing a base
        table's clothes, so the `INSERT INTO agg_` check above misses
        it. It is filled by an UPDATE, and if that is ever dropped the
        search box silently ranks every package as equally popular --
        which looks like working software.
        """
        from chatsbom.export.d1 import aggregate_sql
        sql = aggregate_sql()
        assert 'UPDATE packages SET repositories' in sql

    def test_it_counts_repositories_not_artifact_rows(self) -> None:
        """A package appears once per manifest it is found in, so a
        plain count(*) reports rows and not projects -- and the number
        sits beside a name in a list headed "repositories".
        """
        from chatsbom.export.d1 import aggregate_sql
        update = aggregate_sql().split('UPDATE packages SET repositories')[1]
        assert 'count(DISTINCT a.repository_id)' in update.split(';')[0]

    def test_every_derivable_aggregate_gets_filled(self) -> None:
        """Those computable from the base tables, which is most of them.

        `agg_edges` is the exception: package-to-package edges are not in
        the base tables at all — the artifacts table records what a
        repository depends on, not what its packages depend on each
        other. They come from the raw SPDX documents on disk, so they
        are written with the data rather than derived after it.
        """
        from chatsbom.export.d1 import aggregate_sql
        sql = aggregate_sql()
        for table in D1_SCHEMA.tables:
            if table.name.startswith('agg_') and table.name != 'agg_edges':
                assert f'INSERT INTO {table.name}' in sql, table.name

    def test_edges_are_not_derived_from_the_base_tables(self) -> None:
        """Because they cannot be: the information is not there."""
        from chatsbom.export.d1 import aggregate_sql
        assert 'agg_edges' not in aggregate_sql()

    def test_runs_after_the_base_data_since_it_reads_it(self) -> None:
        """It is a separate script applied fourth, not part of the DDL."""
        from chatsbom.export.d1 import aggregate_sql
        from chatsbom.export.d1 import schema_sql
        assert 'INSERT INTO agg_totals' not in schema_sql()
        assert 'CREATE TABLE' not in aggregate_sql()

    def test_top_packages_covers_the_no_filter_case(self) -> None:
        """`language = ''` is the 'all languages' row the panel loads first."""
        from chatsbom.export.d1 import aggregate_sql
        assert "''" in aggregate_sql()


class TestMetaTable:
    """Provenance, for the same debugging the Parquet manifest served.

    The Parquet path answers "what am I looking at" with a manifest:
    generator, schema version, freshness, and a checksum per file. D1 has
    no files, so the file list has no analogue — but the other three do,
    and they are the ones that explain a surprising number.
    """

    def test_meta_is_a_table_like_any_other(self) -> None:
        assert 'meta' in {t.name for t in D1_SCHEMA.tables}

    def test_carries_the_build_and_the_contract_version(self) -> None:
        table = D1_SCHEMA.table('meta')
        assert 'generator' in table.column_names
        assert 'schema_version' in table.column_names

    def test_carries_the_observation_span_as_two_dates(self) -> None:
        """One date invites the reader to assume the whole corpus is
        that age; on this corpus the ends are seven months apart."""
        table = D1_SCHEMA.table('meta')
        assert 'observed_from' in table.column_names
        assert 'observed_to' in table.column_names

    def test_is_filled_by_the_data_script_not_the_ddl(self) -> None:
        from chatsbom.export.d1 import meta_sql
        sql = meta_sql(
            'chatsbom/0.5.4', '5',
            {'observedFrom': 'a', 'observedTo': 'b'},
        )
        assert 'INSERT INTO meta' in sql
        assert 'chatsbom/0.5.4' in sql

    def test_absent_freshness_is_stored_as_empty_not_invented(self) -> None:
        from chatsbom.export.d1 import meta_sql
        sql = meta_sql('x', '5', {})
        assert '1970' not in sql
        assert "''" in sql


def test_meta_records_a_version_string_not_a_module(tmp_path) -> None:
    """`chatsbom.__version__` is a module; the string is inside it.

    Importing the wrong level produced a generator of
    `chatsbom/<module 'chatsbom.__version__' from '...'>` — syntactically
    fine, silently useless, and exactly the sort of thing a metadata
    panel exists to make visible.
    """
    from chatsbom.export.d1 import meta_sql
    from chatsbom.__version__ import __version__
    sql = meta_sql(f'chatsbom/{__version__}', '5', {})
    assert '<module' not in sql
    assert 'chatsbom/0.' in sql or 'chatsbom/1.' in sql


class TestDependencyEdges:
    """Aggregated package-to-package edges.

    GitHub's dependency graph carries real `DEPENDS_ON` edges between
    packages, not just from the repository root — measured across 420
    sampled documents with the root correctly excluded:

        go          94.4% of edges are package -> package
        javascript  93.4%
        java        78.7%
        php         74.8%
        ruby        65.4%
        python      43.3%
        rust        36.7%

    Stored per repository those are 32,377,306 rows and 1,206 MB in
    SQLite. Aggregated by name they are 4,691,332 rows and ~175 MB — 15%
    of the raw count — and they answer the question a reader actually
    has: bringing in `debug` brings in `ms`, in 89 of the sampled
    repositories.

    What aggregation drops is deliberate and has to be stated: which
    repository, and which versions. `mail 2.8.1 -> mini_mime 1.1.5` in
    rails/rails becomes `mail -> mini_mime, seen in N repositories`.
    """

    def test_edges_is_a_table(self) -> None:
        assert 'agg_edges' in {t.name for t in D1_SCHEMA.tables}

    def test_carries_the_pair_and_how_often_it_occurs(self) -> None:
        table = D1_SCHEMA.table('agg_edges')
        assert table.column_names == [
            'parent_id', 'child_id', 'repositories',
        ]

    def test_references_packages_rather_than_repeating_names(self) -> None:
        """4.7M rows: storing two names per row would dwarf the dataset."""
        table = D1_SCHEMA.table('agg_edges')
        for column in table.columns:
            if column.name.endswith('_id'):
                assert column.type.startswith('INTEGER')

    def test_is_indexed_by_parent_so_a_lookup_is_not_a_scan(self) -> None:
        indexed = {(i.table, tuple(i.columns)) for i in D1_SCHEMA.indexes}
        assert ('agg_edges', ('parent_id',)) in indexed

    def test_is_indexed_by_child_so_the_reverse_question_works_too(self) -> None:
        """'What pulls in this package?' is the more useful direction."""
        indexed = {(i.table, tuple(i.columns)) for i in D1_SCHEMA.indexes}
        assert ('agg_edges', ('child_id',)) in indexed


class TestEdgeExtraction:
    """Reading edges out of a dependency-graph SPDX document."""

    DOC = {
        'sbom': {
            'packages': [
                {'SPDXID': 'root', 'name': 'my-app'},
                {'SPDXID': 'p1', 'name': 'debug', 'versionInfo': '4.3.4'},
                {'SPDXID': 'p2', 'name': 'ms', 'versionInfo': '2.1.2'},
            ],
            'relationships': [
                {
                    'spdxElementId': 'doc', 'relatedSpdxElement': 'root',
                    'relationshipType': 'DESCRIBES',
                },
                {
                    'spdxElementId': 'root', 'relatedSpdxElement': 'p1',
                    'relationshipType': 'DEPENDS_ON',
                },
                {
                    'spdxElementId': 'p1', 'relatedSpdxElement': 'p2',
                    'relationshipType': 'DEPENDS_ON',
                },
            ],
        },
    }

    def test_extracts_package_to_package_edges(self) -> None:
        from chatsbom.export.d1 import edges_in
        assert edges_in(self.DOC) == {('debug', 'ms')}

    def test_excludes_edges_out_of_the_repository_root(self) -> None:
        """The root's own dependencies are already the artifacts table.

        Including them would double-count: `my-app -> debug` says the
        repository depends on debug, which is what `artifacts` records.
        This table is about what packages pull in *each other*.
        """
        from chatsbom.export.d1 import edges_in
        assert ('my-app', 'debug') not in edges_in(self.DOC)

    def test_drops_versions_because_the_question_is_about_names(self) -> None:
        from chatsbom.export.d1 import edges_in
        for parent, child in edges_in(self.DOC):
            assert '@' not in parent and '@' not in child

    def test_deduplicates_within_one_document(self) -> None:
        """A repository counts once for a pair, however many times its
        lockfile expresses it."""
        from chatsbom.export.d1 import edges_in
        doc = {
            'sbom': {
                'packages': self.DOC['sbom']['packages'],
                'relationships': self.DOC['sbom']['relationships'] * 3,
            },
        }
        assert edges_in(doc) == {('debug', 'ms')}

    def test_ignores_a_document_with_no_relationships(self) -> None:
        from chatsbom.export.d1 import edges_in
        assert edges_in(
            {'sbom': {'packages': [], 'relationships': []}},
        ) == set()

    def test_ignores_an_edge_naming_an_unknown_element(self) -> None:
        """A dangling SPDXID is a malformed document, not an edge."""
        from chatsbom.export.d1 import edges_in
        doc = {
            'sbom': {
                'packages': [{'SPDXID': 'p1', 'name': 'debug'}],
                'relationships': [
                    {
                        'spdxElementId': 'p1', 'relatedSpdxElement': 'missing',
                        'relationshipType': 'DEPENDS_ON',
                    },
                ],
            },
        }
        assert edges_in(doc) == set()
