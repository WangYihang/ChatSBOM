"""A snapshot's id changes exactly when what it serves changes (#132).

Q11 on #128: a pass that changed nothing publishes nothing, and the
id, which the web's cache keys are made of, is the dataset's version.
So the id is made of the content, never of when or where it was made:
two warehouses that serve the same rows give one id, however their
rows were loaded, and any row a snapshot serves, changed, gives another.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from chatsbom.snapshot.write import write
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import at
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import FEB
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse

Change = Callable[[Corpus], None]


def identify(directory: Path, corpus: Corpus) -> str:
    directory.mkdir(parents=True, exist_ok=True)
    written = write(
        warehouse(directory / 'warehouse.duckdb', corpus),
        directory / 'snapshots',
    )
    return written.id


@pytest.fixture(scope='module')
def unchanged(tmp_path_factory: pytest.TempPathFactory) -> str:
    return identify(tmp_path_factory.mktemp('unchanged'), shop())


def the_repository(corpus: Corpus, id: int) -> dict[str, Any]:
    [found] = [row for row in corpus.repositories if row['id'] == id]
    return found


def the_artifact(corpus: Corpus, name: str, id: int) -> dict[str, Any]:
    [found] = [
        row for row in corpus.artifacts
        if row['name'] == name and row['repository_id'] == id
    ]
    return found


class TestTheSameContent:

    def test_is_the_same_id(self, tmp_path: Path, unchanged: str) -> None:
        assert identify(tmp_path, shop()) == unchanged

    def test_whatever_order_its_rows_were_loaded_in(
        self, tmp_path: Path, unchanged: str,
    ) -> None:
        corpus = shop()
        corpus.repositories.reverse()
        corpus.artifacts.reverse()
        corpus.edges.reverse()
        assert identify(tmp_path, corpus) == unchanged

    #: What no answer reads: when and where the pass ran, what the
    #: warehouse counted of the whole store, a repository outside the
    #: corpus, a column no table of a snapshot has, and a scan's tool.
    NOT_SERVED: dict[str, Change] = {
        'the pass ran later': lambda c: _build(
            c, built_at=at(2026, 9, 30, 1),
        ),
        'over another store': lambda c: _build(c, store='/elsewhere/data'),
        'and read more of it': lambda c: _build(c, scans=99, observations=999),
        'a repository outside the corpus scanned again': lambda c: (
            c.artifacts.append(
                artifact(
                    4, 'viper', '1.0.0', 'go-module', observed_at=FEB,
                    commit='g2', relationship='direct',
                ),
            )
        ),
        'its topics': lambda c: the_repository(c, 1).update(
            topics=['rails'],
        ),
        'its forks': lambda c: the_repository(c, 2).update(fork_count=7),
        'its creation': lambda c: the_repository(c, 2).update(
            created_at=at(2020, 1, 1),
        ),
        'a purl': lambda c: the_artifact(c, 'lodash', 2).update(
            purl='pkg:npm/lodash@4.17.21',
        ),
    }

    @pytest.mark.parametrize('change', NOT_SERVED, ids=str)
    def test_is_the_same_id_whatever_else_changed(
        self, tmp_path: Path, unchanged: str, change: str,
    ) -> None:
        corpus = shop()
        self.NOT_SERVED[change](corpus)
        assert identify(tmp_path, corpus) == unchanged


def _build(into: Corpus, **fields: Any) -> None:
    assert into.build is not None
    into.build.update(fields)


def _tracked(corpus: Corpus) -> None:
    """A repository of the corpus, never scanned."""
    corpus.repositories.append(repository(5, 'acme', 'new', 10, 'Go'))
    assert corpus.corpus is not None
    corpus.corpus.add(5)


def _a_day_later(corpus: Corpus) -> None:
    """web's scan, dated a day later."""
    for row in corpus.artifacts:
        if row['repository_id'] == 2:
            row['observed_at'] += timedelta(days=1)


class TestAnyServedChange:

    #: A change of a row some table of a snapshot holds, or of `meta`.
    SERVED: dict[str, Change] = {
        'a repository starred once more': lambda c: the_repository(
            c, 1,
        ).update(stars=301),
        'a description': lambda c: the_repository(c, 2).update(
            description='Unicode',
        ),
        'a push': lambda c: the_repository(c, 3).update(
            pushed_at=at(2026, 9, 2),
        ),
        'a repository tracked and not scanned': _tracked,
        'a version': lambda c: the_artifact(c, 'lodash', 2).update(
            version='4.17.22',
        ),
        'a relationship': lambda c: the_artifact(c, 'lodash', 2).update(
            relationship='direct',
        ),
        'a licence': lambda c: the_artifact(c, 'lodash', 2).update(
            licenses=['ISC'],
        ),
        'a scan a day later': _a_day_later,
        'an older scan, which is history': lambda c: c.artifacts.append(
            artifact(
                2, 'lodash', '4.17.20', 'npm', observed_at=at(2025, 12, 1),
                commit='w0', found_by='javascript-lock-cataloger',
            ),
        ),
        'an edge counted again': lambda c: c.edges.__setitem__(
            0, (*c.edges[0][:2], 3, c.edges[0][3]),
        ),
        'the corpus': lambda c: _build(c, corpus='all-2026-09-08'),
    }

    @pytest.mark.parametrize('change', SERVED, ids=str)
    def test_is_another_id(
        self, tmp_path: Path, unchanged: str, change: str,
    ) -> None:
        corpus = shop()
        self.SERVED[change](corpus)
        assert identify(tmp_path, corpus) != unchanged

    def test_another_version_of_the_code_is_another_id(
        self,
        tmp_path: Path,
        unchanged: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`meta` says which build wrote the file, and the page shows it:
        the first pass after an upgrade publishes once."""
        monkeypatch.setattr('chatsbom.snapshot.write.__version__', '9.9.9')
        assert identify(tmp_path, shop()) != unchanged
