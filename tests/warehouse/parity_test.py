"""Both engines over one input, compared rollup by rollup (#131).

What is left of the live comparison, beside its golden fixtures
(`golden_test.py`): `scripts/warehouse_parity.py`, which goes with
ClickHouse (#153).
"""
from __future__ import annotations

import importlib.util
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.rollups import REFRESH_ORDER
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.warehouse import connect
from chatsbom.warehouse.parity import compare
from chatsbom.warehouse.parity import FOUNDATIONS
from chatsbom.warehouse.parity import RECORDS
from chatsbom.warehouse.parity import report
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse
from tests.warehouse.conftest import synthetic
from tests.warehouse.conftest import with_releases

pytestmark = requires_clickhouse

ROOT = Path(__file__).resolve().parents[2]


def module(relative: str, name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


def config(database: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD, database=database,
    )


def seed(
    database: str,
    repositories: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    edges: list[tuple[str, str, int, datetime]],
    releases: Sequence[dict[str, Any]] = (),
) -> None:
    """The rows into ClickHouse, and the rollups brought up to them, as
    `db index` and `db edges` leave a database."""
    with IngestionRepository(config(database)) as ingest:
        ingest.insert_batch(
            REPOSITORIES.name, REPOSITORIES.rows(repositories),
            REPOSITORIES.column_names,
        )
        ingest.insert_batch(
            ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
        )
        ingest.insert_batch(
            RELEASES.name, RELEASES.rows(list(releases)),
            RELEASES.column_names,
        )
        ingest.insert_batch(
            EDGES.name,
            EDGES.rows([
                {
                    'parent': parent, 'child': child,
                    'repositories': count, 'observed_at': observed_at,
                }
                for parent, child, count, observed_at in edges
            ]),
            EDGES.column_names,
        )
        ingest.optimize()
        ingest.reload_dictionaries()
        ingest.refresh_rollups()


def warehouse_of(
    repositories: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    edges: list[tuple[str, str, int, datetime]],
    corpus: set[int] | None,
    releases: Sequence[dict[str, Any]] = (),
) -> duckdb.DuckDBPyConnection:
    con = connect(':memory:')
    load(con, repositories, artifacts, edges, corpus=corpus, releases=releases)
    derive(con)
    return con


def agree(
    database: str,
    warehouse: duckdb.DuckDBPyConnection,
    empty: tuple[str, ...] = (),
) -> None:
    """Every relation agrees, and every one but those `empty` names has
    rows: not agreement on nothing."""
    with QueryRepository(config(database)) as query:
        verdicts = compare(warehouse, query.client)
    assert [v.name for v in verdicts] == [
        *(name for name, _ in FOUNDATIONS),
        *(relation.name for relation in RECORDS),
        *REFRESH_ORDER,
    ]
    assert all(v.agrees for v in verdicts), report(verdicts)
    assert all(v.rows for v in verdicts if v.name not in empty), report(verdicts)


def test_the_script_compares_a_warehouse_file_with_clickhouse(
    clickhouse_db: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`scripts/warehouse_parity.py`, as an operator runs it beside a
    deployment: a warehouse file, ClickHouse as configured, and the
    number of relations that differ as its status."""
    repositories, artifacts, edges, corpus = synthetic(seed=7, repositories=60)
    releases = with_releases(repositories, seed=7)
    seed(clickhouse_db, repositories, artifacts, edges, releases)
    path = tmp_path / 'warehouse.duckdb'
    with connect(path) as con:
        load(
            con, repositories, artifacts, edges, corpus=corpus,
            releases=releases,
        )
        derive(con)
    script = module('scripts/warehouse_parity.py', 'warehouse_parity')

    def run(argv: list[str]) -> tuple[int, str]:
        with QueryRepository(config(clickhouse_db)) as query:
            monkeypatch.setattr(
                script, 'get_container',
                lambda: SimpleNamespace(get_export_repository=lambda: query),
            )
            monkeypatch.setattr(sys, 'argv', argv)
            status = script.main()
        return status, capsys.readouterr().out

    status, out = run(['warehouse_parity.py', str(path)])
    assert status == 0, out
    assert 'All 22 agree.' in out

    # A warehouse of other rows: the status counts what differs.
    with connect(path) as con:
        con.execute('DELETE FROM mv_totals')
    status, out = run(['warehouse_parity.py', str(path)])
    assert status == 1
    assert 'mv_totals' in out and 'DIFFERS' in out
