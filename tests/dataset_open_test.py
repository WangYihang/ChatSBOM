"""How the dataset API opens a snapshot, and what it reads there (#138).

#128 §2.4: the web process reads `snapshots/<id>.sqlite`, a file nothing
writes once it is published, opened `file:<path>?mode=ro&immutable=1`.
Read-only, so what serves the page cannot change what it serves;
immutable, so SQLite takes no lock and leaves no file beside it, and a
thousand readers cost the file nothing. The open is in one place,
`chatsbom/dataset/open.py`, for the web and the CLI alike, and so is
finding which snapshot is current (#132): the one `snapshots/CURRENT`
names on its first line.

And the schema: a snapshot starts as `export d1`'s (`D1_SCHEMA`), so
every method has to answer over that schema with nothing in it, as an
empty dataset rather than a failure.
"""
from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.dataset.open import current
from chatsbom.export.d1 import index_sql
from chatsbom.export.d1 import schema_sql
from tests.dataset_contract_test import corpus


def d1_schema(path: Path) -> Path:
    """`D1_SCHEMA`'s tables and indexes, and no rows."""
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(schema_sql() + index_sql())
        connection.commit()
    return path


class TestTheOpen:

    def test_reads(self, tmp_path: Path) -> None:
        with closing(connect(corpus(tmp_path))) as connection:
            [(count,)] = connection.execute(
                'SELECT count(*) FROM repositories',
            ).fetchall()
        assert count == 12

    def test_writes_nothing(self, tmp_path: Path) -> None:
        path = corpus(tmp_path)
        with closing(connect(path)) as connection:
            with pytest.raises(sqlite3.OperationalError, match='readonly'):
                connection.execute('DELETE FROM repositories')
        with closing(sqlite3.connect(path)) as check:
            [(count,)] = check.execute(
                'SELECT count(*) FROM repositories',
            ).fetchall()
        assert count == 12

    def test_leaves_nothing_beside_the_file(self, tmp_path: Path) -> None:
        # No journal, no -wal, no -shm: a snapshot's directory is the
        # publisher's, and a reader's files in it would be taken for its.
        path = corpus(tmp_path)
        before = sorted(tmp_path.iterdir())
        with open_dataset(path) as dataset:
            dataset.dependents_of('mail')
            assert sorted(tmp_path.iterdir()) == before
        assert sorted(tmp_path.iterdir()) == before

    def test_a_missing_file_is_refused_and_not_made(
        self, tmp_path: Path,
    ) -> None:
        with pytest.raises(sqlite3.OperationalError):
            connect(tmp_path / 'missing.sqlite')
        assert list(tmp_path.iterdir()) == []

    def test_takes_no_lock(self, tmp_path: Path) -> None:
        # Immutable: a lock another connection holds is not waited on,
        # because none is asked for. A snapshot is never written, so there
        # is nothing to wait for.
        path = corpus(tmp_path)
        with closing(sqlite3.connect(path, isolation_level=None)) as writer:
            writer.execute('BEGIN EXCLUSIVE')
            # A read-only open that is not immutable waits, and fails.
            plain = f'file:{path}?mode=ro'
            with closing(sqlite3.connect(plain, uri=True, timeout=0)) as ro:
                with pytest.raises(sqlite3.OperationalError, match='locked'):
                    ro.execute('SELECT count(*) FROM repositories').fetchall()
            with closing(connect(path)) as reader:
                [(count,)] = reader.execute(
                    'SELECT count(*) FROM repositories',
                ).fetchall()
            assert count == 12
            writer.execute('ROLLBACK')

    @pytest.mark.parametrize(
        'directory', ['a?b', 'a#b', 'a%20b', 'a b', 'ünï'],
    )
    def test_a_path_a_uri_would_misread(
        self, tmp_path: Path, directory: str,
    ) -> None:
        # Quoted into the URI: unquoted, `?` starts its query, `#` its
        # fragment, and `%20` is a space.
        place = tmp_path / directory
        place.mkdir()
        with open_dataset(corpus(place)) as dataset:
            assert dataset.totals().tracked == 12

    def test_a_relative_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        corpus(tmp_path)
        monkeypatch.chdir(tmp_path)
        with open_dataset(Path('contract.sqlite')) as dataset:
            assert dataset.totals().tracked == 12

    def test_is_closed_after(self, tmp_path: Path) -> None:
        with open_dataset(corpus(tmp_path)) as dataset:
            dataset.totals()
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            dataset.totals()


#: A snapshot's id, as `CURRENT` names it.
ID = '0123456789abcdef'


def published(directory: Path) -> Path:
    """The contract's file as `publish` leaves a snapshot: named by its
    id, which `CURRENT` names."""
    directory.mkdir(parents=True, exist_ok=True)
    target = corpus(directory).rename(directory / f'{ID}.sqlite')
    (directory / 'CURRENT').write_text(f'{ID}\n')
    return target


class TestTheCurrentSnapshot:

    def test_is_the_one_current_names(self, tmp_path: Path) -> None:
        target = published(tmp_path)
        assert current(tmp_path) == target
        with open_dataset(current(tmp_path)) as dataset:
            assert dataset.totals().tracked == 12

    def test_is_opened_read_only(self, tmp_path: Path) -> None:
        published(tmp_path)
        with closing(connect(current(tmp_path))) as connection:
            with pytest.raises(sqlite3.OperationalError, match='readonly'):
                connection.execute('DELETE FROM repositories')

    def test_by_the_first_line_the_others_being_the_ones_kept(
        self, tmp_path: Path,
    ) -> None:
        target = published(tmp_path)
        (tmp_path / 'CURRENT').write_text(
            f'{ID}\nfedcba9876543210\n00000000000000ff\n',
        )
        assert current(tmp_path) == target

    def test_a_relative_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        published(tmp_path / 'snapshots')
        monkeypatch.chdir(tmp_path)
        with open_dataset(current('snapshots')) as dataset:
            assert dataset.totals().tracked == 12

    def test_none_published_is_said_and_nothing_made(
        self, tmp_path: Path,
    ) -> None:
        with pytest.raises(FileNotFoundError, match='no snapshot'):
            current(tmp_path)
        assert list(tmp_path.iterdir()) == []

    def test_one_named_and_gone_is_said(self, tmp_path: Path) -> None:
        published(tmp_path).unlink()
        with pytest.raises(FileNotFoundError, match=ID):
            current(tmp_path)

    @pytest.mark.parametrize(
        'named', [
            '', '\n', '../contract', '/etc/passwd', f' {ID}', f'{ID} ',
            ID.upper(), ID[:-1], f'{ID}0', f'{ID}.sqlite',
            f'x\n{ID}\n',
        ],
    )
    def test_what_is_not_an_id_is_refused(
        self, tmp_path: Path, named: str,
    ) -> None:
        """Before any file is opened: a name is spliced into a path, and
        `../` would leave the directory."""
        published(tmp_path)
        (tmp_path / 'CURRENT').write_text(named)
        with pytest.raises(ValueError, match='CURRENT'):
            current(tmp_path)

    def test_in_a_read_only_directory_nothing_is_made(
        self, tmp_path: Path,
    ) -> None:
        """Where the web mounts the snapshots read-only: the open makes
        no journal, `-wal` or `-shm`, and so needs nothing it cannot
        have. Root may write the directory all the same; the listing is
        what shows nothing was made."""
        directory = tmp_path / 'snapshots'
        published(directory).chmod(0o444)
        directory.chmod(0o555)
        try:
            before = sorted(directory.iterdir())
            with open_dataset(current(directory)) as dataset:
                assert dataset.totals().tracked == 12
                assert dataset.dependents_of('mail')
                assert sorted(directory.iterdir()) == before
            assert sorted(directory.iterdir()) == before
        finally:
            directory.chmod(0o755)


class TestTheD1Schema:

    def test_every_method_answers_it_empty(self, tmp_path: Path) -> None:
        # Every table and column a method reads is one `D1_SCHEMA`
        # declares; with no rows each answers nothing, or zero, and the
        # provenance it cannot give is empty rather than invented.
        with open_dataset(d1_schema(tmp_path / 'empty.sqlite')) as dataset:
            answers = answer_everything(dataset)
        assert jsonable(answers) == {
            'dependentsOf': [],
            'countDependents': 0,
            'countDependentRows': 0,
            'ecosystemsFor': [],
            'versionSpread': {
                'versions': [], 'constrained': 0, 'unversioned': 0,
            },
            'adoptionOverTime': [],
            'searchPackages': [],
            'dependenciesOf': [],
            'pulledInBy': [],
            'dependencyTree': {
                'root': 'express', 'children': [], 'grandchildren': [],
            },
            'totals': {
                'repositories': 0, 'dependencies': 0, 'packages': 0,
                'classified': 0, 'tracked': 0,
            },
            'relationshipSplit': {'direct': 0, 'transitive': 0, 'unknown': 0},
            'languageCoverage': [],
            'ecosystemCoverage': [],
            'relationshipByEcosystem': [],
            'edgeAmbiguity': None,
            'topPackages': [],
            'dependencyDistribution': [],
            'sourceComparison': [],
            'licenseShares': [],
            'meta': {
                'generator': '', 'schemaVersion': '', 'observedFrom': '',
                'observedTo': '',
            },
        }


def answer_everything(dataset: Dataset) -> dict[str, object]:
    """One answer of each method, keyed as the page names it."""
    return {
        'dependentsOf': dataset.dependents_of(
            'mail', type='gem', language='ruby', direct_only=True,
        ),
        'countDependents': dataset.count_dependents('mail', type='gem'),
        'countDependentRows': dataset.count_dependent_rows('mail'),
        'ecosystemsFor': dataset.ecosystems_for('mail'),
        'versionSpread': dataset.version_spread('mail'),
        'adoptionOverTime': dataset.adoption_over_time('mail'),
        'searchPackages': dataset.search_packages('m'),
        'dependenciesOf': dataset.dependencies_of('express'),
        'pulledInBy': dataset.pulled_in_by('debug'),
        'dependencyTree': dataset.dependency_tree('express'),
        'totals': dataset.totals(),
        'relationshipSplit': dataset.relationship_split('npm'),
        'languageCoverage': dataset.language_coverage(),
        'ecosystemCoverage': dataset.ecosystem_coverage(),
        'relationshipByEcosystem': dataset.relationship_by_ecosystem(),
        'edgeAmbiguity': dataset.edge_ambiguity(),
        'topPackages': dataset.top_packages(direct_only=True),
        'dependencyDistribution': dataset.dependency_distribution(),
        'sourceComparison': dataset.source_comparison(),
        'licenseShares': dataset.license_shares(),
        'meta': dataset.meta(),
    }
