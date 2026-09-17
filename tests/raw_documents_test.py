"""The landing zone, and the properties that make it one.

`db index` reads about 80 bytes out of each 820-byte package entry the
collectors write. The rest — `cpes`, `locations`, `metadata`, Syft's
`artifactRelationships` — sits in 31 GB of files and is not queryable,
which has cost this project twice: `06-github-content` stored manifests
and not sources, so Java's lockfiles were never recoverable and its
coverage is still 46%.

Measured, not assumed: these documents compress 12.2x under
`ZSTD(3)`, so the landing zone is about an order of magnitude *smaller*
than the files it copies.
"""
from __future__ import annotations

from chatsbom.core.schema import ddl_column_definitions
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import ddl_engine
from chatsbom.core.schema import RAW_DOCUMENTS_DDL
from chatsbom.core.schema import TABLE_DDL


class TestTheTable:

    def test_it_is_a_managed_table(self) -> None:
        """Otherwise `ensure_schema` never creates it, and the command
        that fills it fails with UNKNOWN_TABLE — which is what happened
        the first time it ran."""
        assert 'raw_documents' in {name for name, _ in TABLE_DDL}

    def test_the_same_document_twice_is_one_row(self) -> None:
        """A ReplacingMergeTree keyed on the content hash, so re-running
        the load inserts nothing new and a second copy of a document
        does not double the table."""
        assert ddl_engine(RAW_DOCUMENTS_DDL) == 'ReplacingMergeTree'
        order = RAW_DOCUMENTS_DDL[RAW_DOCUMENTS_DDL.index('ORDER BY'):]
        for column in ('kind', 'repository_id', 'sha256'):
            assert column in order

    def test_the_body_is_compressed(self) -> None:
        """The whole reason this is cheaper than the files it copies."""
        body = ddl_column_definitions(RAW_DOCUMENTS_DDL)['body']
        assert 'ZSTD' in body

    def test_the_comment_precedes_the_codec(self) -> None:
        """ClickHouse rejects the other order, and the message points at
        the COMMENT rather than at the CODEC: `failed at position 438
        (COMMENT)`."""
        body = ddl_column_definitions(RAW_DOCUMENTS_DDL)['body']
        assert body.index('COMMENT') < body.index('CODEC')

    def test_it_records_where_each_row_came_from(self) -> None:
        """A landing zone nobody can trace back to a file is not
        evidence of anything."""
        for column in ('kind', 'path', 'fetched_at'):
            assert column in ddl_columns(RAW_DOCUMENTS_DDL)


class TestTheLoader:

    def test_it_reports_before_it_writes(self) -> None:
        """A dry run by default, because this rewrites a table.

        Asserted on the source rather than the signature: typer's
        default is an `OptionInfo`, not the `False` inside it, so
        `signature.parameters['apply'].default is False` fails against
        a command that behaves correctly.
        """
        import inspect
        from chatsbom.commands.db import raw
        source = inspect.getsource(raw.main)
        assert "'--apply'" in source
        assert 'if not apply:' in source
        assert 'Dry run' in source

    def test_it_creates_the_schema_first(self) -> None:
        import inspect
        from chatsbom.commands.db import raw
        assert 'ensure_schema' in inspect.getsource(raw.main)

    def test_it_only_lands_documents_about_a_repository(self) -> None:
        """`05-github-tree` and `06-github-content` are inputs to
        collection — a file listing and source files — not documents to
        query. Landing them would triple the table for nothing."""
        from chatsbom.commands.db.raw import SOURCES
        directories = {directory for directory, _, _ in SOURCES}
        assert directories == {'07-sbom', '09-github-depgraph'}

    def test_an_empty_document_is_not_stored(self) -> None:
        """Two zero-byte SBOMs in this corpus were the standing
        `failed=2` on every rebuild. A landing zone that preserves them
        faithfully preserves nothing."""
        import inspect
        from chatsbom.commands.db import raw
        assert 'data.strip()' in inspect.getsource(raw._readable)

    def test_it_dates_a_copy_by_the_file_not_the_clock(self) -> None:
        """`now()` would stamp February's documents as current — the
        same lie `observed_at`'s default told before it was fixed."""
        import inspect
        from chatsbom.commands.db import raw
        source = inspect.getsource(raw._taken_at)
        assert 'st_mtime' in source
