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
        """A tree's file listing is an input to collection, not a document
        to query; landing it would triple the table for nothing. Beside
        it, the discovery list (`manifests.json`) is one: it says why a
        manifest was or was not scanned. The manifests themselves are
        landed file by file, as `content`."""
        from chatsbom.commands.db.raw import SCAN_DOCUMENTS
        from chatsbom.commands.db.raw import SOURCES
        assert dict(SOURCES) == {
            '07-sbom': 'syft',
            '09-github-depgraph': 'github-depgraph',
            '05-github-tree': 'content-index',
        }
        assert SCAN_DOCUMENTS['content-index'] == 'manifests.json'

    def test_an_empty_document_is_not_stored(self) -> None:
        """Two zero-byte SBOMs in this corpus were the standing
        `failed=2` on every rebuild. A landing zone that preserves them
        faithfully preserves nothing."""
        import inspect
        from chatsbom.commands.db import raw
        assert 'data.strip()' in inspect.getsource(raw._readable)

    def test_it_dates_a_copy_by_the_file_not_the_clock(self, tmp_path) -> None:
        """`now()` would stamp February's documents as current — the
        same lie `observed_at`'s default told before it was fixed.

        Asserted against a file with a known mtime rather than against
        the source text: the previous version of this test looked for
        `st_mtime` in `_taken_at`, which kept passing while the value it
        produced was eight hours early.
        """
        import os
        from datetime import datetime, timedelta, timezone
        from chatsbom.commands.db.raw import _taken_at

        document = tmp_path / 'sbom.json'
        document.write_text('{}')
        february = datetime(2026, 2, 11, 11, 14, 39, tzinfo=timezone.utc)
        os.utime(document, (february.timestamp(), february.timestamp()))

        taken = _taken_at(document)
        assert taken == february, 'the file, not the clock'
        assert taken.utcoffset() == timedelta(0), (
            'aware, or the driver reads it as local time and shifts it'
        )


class TestTheRepositoryKeyedWalk:
    """`db raw` finds documents by walking `<stage>/<repository_id>/...`
    (#55): the path is a pure function of the repository and its commit,
    so the directory is the list, and nothing needs a JSONL list's
    record of where a file was."""

    SHA = '0123456789abcdef0123456789abcdef01234567'

    def _tree(self, root):
        from pathlib import Path
        data = Path(root)
        for path, body in {
            f'07-sbom/11/{self.SHA}/sbom.json': '{}',
            f'07-sbom/go/o/r/v1/{self.SHA}/sbom.json': '{}',  # not migrated
            '09-github-depgraph/11/legacy/sbom.spdx.json': '{}',
            f'09-github-depgraph/11/20260920T101010Z-{self.SHA}/sbom.spdx.json': '{}',
            f'09-github-depgraph/11/20260920T101010Z-{self.SHA}/meta.json': (
                '{"ref": "main", "commit_sha": "%s"}' % self.SHA
            ),
            f'06-github-content/11/{self.SHA}/go.mod': 'module x\n',
            f'06-github-content/11/{self.SHA}/sub/go.mod': 'module y\n',
            f'06-github-content/12/{self.SHA}/Gemfile': "gem 'rack'\n",
            f'05-github-tree/11/{self.SHA}/tree.txt': 'go.mod\n',
            f'05-github-tree/11/{self.SHA}/manifests.json': '{"format": 1}',
            f'05-github-tree/12/{self.SHA}/tree.txt': 'Gemfile\n',
        }.items():
            target = data / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body)
        return data

    def test_documents_and_their_stamps(self, tmp_path) -> None:
        from chatsbom.commands.db.raw import _documents
        data = self._tree(tmp_path)
        syft = list(_documents(data / '07-sbom', 'syft', None))
        assert [(r, p.parts[-3:], ref, sha) for r, p, ref, sha in syft] == [
            (11, ('11', self.SHA, 'sbom.json'), '', self.SHA),
        ]
        graphs = list(
            _documents(
                data / '09-github-depgraph', 'github-depgraph', None,
            ),
        )
        stamps = sorted((p.parent.name, ref, sha) for _, p, ref, sha in graphs)
        assert stamps == [
            (f'20260920T101010Z-{self.SHA}', 'main', self.SHA),
            ('legacy', '', ''),
        ]
        index = list(
            _documents(
                data / '05-github-tree', 'content-index', None,
            ),
        )
        assert [(r, p.name, sha) for r, p, _, sha in index] == [
            (11, 'manifests.json', self.SHA),
        ], 'the discovery list, never the tree'

    def test_only_the_repositories_asked_for(self, tmp_path) -> None:
        from chatsbom.commands.db.raw import _content_roots
        data = self._tree(tmp_path)
        assert [
            r for r, _, _ in _content_roots(
                data / '06-github-content', None,
            )
        ] == [11, 12]
        assert [
            r for r, _, _ in _content_roots(
                data / '06-github-content', {12},
            )
        ] == [12]

    def test_paths_are_landed_relative_to_the_data_directory(self, tmp_path) -> None:
        from chatsbom.core.layout import landed
        data = self._tree(tmp_path)
        assert landed(data / f'07-sbom/11/{self.SHA}/sbom.json') == (
            f'07-sbom/11/{self.SHA}/sbom.json'
        )

    def test_the_commit_and_ref_are_columns(self) -> None:
        from chatsbom.core.schema import ddl_columns
        assert {'ref', 'commit_sha'} <= set(ddl_columns(RAW_DOCUMENTS_DDL))
