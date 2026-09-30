"""The edges' ambiguity, from the warehouse to the page (#165).

The page's two edge panels say how far their edges merge ecosystems:
an edge is keyed by package name, and a name is not unique across
ecosystems (`edgeCaveat`, web/src/components/QueryView.tsx). D1's
schema had nowhere to keep the figure, and counting it on request
groups every artifact, so the dataset answered None and the page said
its caveat without numbers. The warehouse measures it on every pass,
`mv_edge_ambiguity`, one row. `snapshot build` copies that row into the
snapshot, `agg_edge_ambiguity`, and `Dataset.edge_ambiguity()` reads it
there: None only where there is no row, as in a snapshot's tables with
nothing in them.
"""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.dataset.types import EdgeAmbiguity
from chatsbom.snapshot.write import write
from chatsbom.warehouse import connect as warehouse_connect
from tests.dataset_open_test import empty
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import FEB
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse

Row = tuple[Any, ...]

#: The columns, as the warehouse and the snapshot both name them.
COLUMNS = 'names, ambiguous_names, edges, ambiguous_edges, largest_repository'


def built(directory: Path, corpus: Corpus) -> tuple[Path, Path]:
    """A warehouse of `corpus`, and the snapshot written from it."""
    directory.mkdir(parents=True, exist_ok=True)
    store = warehouse(directory / 'warehouse.duckdb', corpus)
    return store, write(store, directory / 'snapshots').path


def measured(store: Path) -> list[Row]:
    """What the warehouse measured: `mv_edge_ambiguity`'s row."""
    with warehouse_connect(store, read_only=True) as con:
        return con.execute(
            f'SELECT {COLUMNS} FROM mv_edge_ambiguity',
        ).fetchall()


def kept(snapshot: Path) -> list[Row]:
    """What the snapshot keeps of it."""
    with closing(connect(snapshot)) as connection:
        return connection.execute(
            f'SELECT {COLUMNS} FROM agg_edge_ambiguity',
        ).fetchall()


def ambiguous(corpus: Corpus) -> None:
    """`rack` an npm package too, in web's scan: a name of two
    ecosystems, at both ends of edges."""
    corpus.artifacts.append(
        artifact(
            2, 'rack', '1.0.0', 'npm', observed_at=FEB, commit='w1',
            found_by='javascript-lock-cataloger',
        ),
    )


@pytest.fixture(scope='module')
def of_shop(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    return built(tmp_path_factory.mktemp('shop'), shop())


class TestTheTable:

    def test_is_the_warehouse_s_row(self, of_shop: tuple[Path, Path]) -> None:
        # SHOP's five names, none of them in two ecosystems: Syft's
        # `php-composer` is Composer. Three edges, `mystery`'s among
        # them, which names no package and which the edge table leaves
        # out: the warehouse counts every edge. web has the most
        # packages, four.
        store, snapshot = of_shop
        assert kept(snapshot) == [(5, 0, 3, 0, 4)]
        assert kept(snapshot) == measured(store)

    def test_counts_a_name_of_two_ecosystems(self, tmp_path: Path) -> None:
        # `rack`, a gem and now an npm package, is one of the five names,
        # and two of the three edges are its.
        corpus = shop()
        ambiguous(corpus)
        store, snapshot = built(tmp_path, corpus)
        assert kept(snapshot) == [(5, 1, 3, 2, 4)]
        assert kept(snapshot) == measured(store)


class TestTheAnswer:

    def test_is_what_the_snapshot_keeps(self, tmp_path: Path) -> None:
        corpus = shop()
        ambiguous(corpus)
        _, snapshot = built(tmp_path, corpus)
        with open_dataset(snapshot) as dataset:
            answer = dataset.edge_ambiguity()
        assert answer == EdgeAmbiguity(
            names=5, ambiguous_names=1, edges=3, ambiguous_edges=2,
            largest_repository=4,
        )
        # As the page reads it.
        assert jsonable(answer) == {
            'names': 5, 'ambiguousNames': 1, 'edges': 3,
            'ambiguousEdges': 2, 'largestRepository': 4,
        }

    def test_is_none_where_there_is_no_row(self, tmp_path: Path) -> None:
        """A snapshot's tables, with nothing in them: the dataset cannot
        say, and says so, rather than zeroes the page would print."""
        with open_dataset(empty(tmp_path / 'empty.sqlite')) as dataset:
            assert dataset.edge_ambiguity() is None

    def test_of_a_warehouse_of_nothing_is_its_zeroes(
        self, tmp_path: Path,
    ) -> None:
        """A warehouse with no repository measures nothing, and says so:
        its row is zeroes, and the page says its caveat without
        figures for an edge table with no edge."""
        store, snapshot = built(tmp_path, Corpus([], []))
        assert measured(store) == [(0, 0, 0, 0, 0)]
        with open_dataset(snapshot) as dataset:
            assert dataset.edge_ambiguity() == EdgeAmbiguity(
                names=0, ambiguous_names=0, edges=0, ambiguous_edges=0,
                largest_repository=0,
            )
