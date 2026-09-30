"""Both engines over one input, compared rollup by rollup (#131).

What is left of the live comparison, beside its golden fixtures
(`golden_test.py`): `scripts/warehouse_parity.py`, and the corpus
`chatsbom run` collected into `raw_documents` alone. Each goes with
ClickHouse (#153).
"""
from __future__ import annotations

import importlib.util
import sys
from collections.abc import Iterator
from collections.abc import Sequence
from datetime import date
from datetime import datetime
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import DatabaseConfig
from chatsbom.core.documents import RecordStore
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
from tests.warehouse.conftest import A
from tests.warehouse.conftest import APP
from tests.warehouse.conftest import app_release
from tests.warehouse.conftest import APP_V1
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import B
from tests.warehouse.conftest import BARE
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import DART
from tests.warehouse.conftest import GONE
from tests.warehouse.conftest import GRADLE
from tests.warehouse.conftest import GRAPHED
from tests.warehouse.conftest import PODS
from tests.warehouse.conftest import PODSPEC
from tests.warehouse.conftest import spdx
from tests.warehouse.conftest import Store
from tests.warehouse.conftest import synthetic
from tests.warehouse.conftest import WEB
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


# -- a corpus `chatsbom run` collected: its records in raw_documents alone -


def land(
    records: RecordStore,
    repository_id: int,
    name: str,
    *,
    commit: str,
    pushed: str,
    releases: list[dict[str, Any]],
    taken: datetime,
    paths: Any,
) -> None:
    """A repository's finished record, as `chatsbom run` files it: in
    `raw_documents` (`RecordStore`), and in no `07-sbom` list. Its latest
    stable release is the first of `releases` that is not a pre-release,
    and its commit that release's, or the head's with none."""
    latest = next((r for r in releases if not r.get('prerelease')), None)
    records.remember(
        {
            'id': repository_id, 'owner': 'acme', 'name': name,
            'html_url': f'https://github.com/acme/{name}',
            'stargazers_count': 100, 'default_branch': 'main',
            'pushed_at': pushed, 'all_releases': releases,
            'has_releases': bool(releases), 'total_releases': len(releases),
            'latest_stable_release': latest,
            'download_target': {
                'ref': latest['tag_name'] if latest else 'main',
                'ref_type': 'release' if latest else 'branch',
                'commit_sha': commit, 'commit_sha_short': commit[:7],
            },
            'sbom_path': str(paths.sbom_file(repository_id, commit)),
            'local_content_path': str(paths.content_root(repository_id, commit)),
        },
        paths.sbom_dir / 'index.jsonl',
        taken_at=taken,
    )


DART_BETA = {
    **app_release('3.0.0-beta', '2026-02-05T00:00:00Z', 1),
    'prerelease': True,
}
DART_2 = app_release('2.9.0', '2026-01-05T00:00:00Z', 7)


def run_first(store: Store, records: RecordStore) -> None:
    """What `chatsbom run` had collected by the first `db index`."""
    listed = store.snapshot(
        date(2026, 9, 1), APP, WEB, DART, PODS, GRAPHED, BARE,
    )
    store.seed(listed, APP, WEB, DART, PODS, GRAPHED, BARE)
    older = store.snapshot(date(2026, 3, 1), APP, WEB, GONE)
    store.seed(older, GONE)
    paths = store.paths

    store.sbom(
        1, A, artifact('guava', '32.1.0-jre', 'java-archive'),
        at=at(2026, 2, 11, 9, 30),
    )
    store.content(1, A, {'build.gradle': GRADLE}, at=at(2026, 2, 11, 7, 30))
    land(
        records, 1, 'app', commit=A, pushed='2026-02-10T00:00:00Z',
        releases=[APP_V1], taken=at(2026, 2, 11, 10), paths=paths,
    )
    store.graph(
        1,
        spdx(
            '2026-03-05T08:00:00Z',
            [('com.google.guava:guava', '32.1.0-jre', 'maven')],
            direct=['com.google.guava:guava'],
        ),
        fetched=at(2026, 3, 5, 8, 0),
    )
    store.sbom(
        2, A, artifact('react', '18.2.0', 'npm', licenses=['MIT']),
        artifact('lodash', '4.17.21', 'npm', licenses=['MIT']),
        at=at(2026, 1, 31, 20, 0),
    )
    store.content(
        2, A, {
            'package.json': '{"dependencies": {"react": "18.2.0", '
                            '"lodash": "4.17.21"}}',
        },
        at=at(2026, 1, 31, 18, 0),
    )
    land(
        records, 2, 'web', commit=A, pushed='2026-01-31T00:00:00Z',
        releases=[], taken=at(2026, 1, 31, 21), paths=paths,
    )
    store.sbom(
        3, A, artifact('flutter_bloc', '8.1.3', 'dart-pub', licenses=['MIT']),
        at=at(2026, 2, 6),
    )
    store.content(3, A, {'pubspec.yaml': 'name: app\n'}, at=at(2026, 2, 5, 22))
    land(
        records, 3, 'dart', commit=A, pushed='2026-02-05T12:00:00Z',
        releases=[DART_BETA, DART_2], taken=at(2026, 2, 6, 1), paths=paths,
    )
    store.sbom(4, A, at=at(2026, 2, 4))
    store.content(4, A, {'Pods.podspec': PODSPEC}, at=at(2026, 2, 3, 23, 0))
    land(
        records, 4, 'pods', commit=A, pushed='2026-02-03T00:00:00Z',
        releases=[], taken=at(2026, 2, 4, 1), paths=paths,
    )
    store.graph(
        5,
        spdx(
            '2026-02-20T10:00:00Z',
            [('left-pad', '1.3.0', 'npm'), ('debug', '4.3.4', 'npm')],
            direct=['left-pad'], edges=[('left-pad', 'debug')],
        ),
        fetched=at(2026, 2, 20, 10, 0),
    )
    cobra = artifact('cobra', '1.8.0', 'go-module')
    store.sbom(7, A, cobra, at=at(2026, 2, 5))
    land(
        records, 7, 'gone', commit=A, pushed='2026-02-04T00:00:00Z',
        releases=[APP_V1], taken=at(2026, 2, 5, 1), paths=paths,
    )


def run_second(store: Store, records: RecordStore) -> None:
    """A second walk: `acme/app` pushed and released again."""
    store.sbom(
        1, B, artifact('guava', '33.0.0-jre', 'java-archive'),
        artifact('jsr305', '3.0.2', 'java-archive'),
        at=at(2026, 9, 14, 10, 0),
    )
    store.content(1, B, {'build.gradle': GRADLE}, at=at(2026, 9, 14, 8, 0))
    land(
        records, 1, 'app', commit=B, pushed='2026-09-12T00:00:00Z',
        releases=[
            app_release('v1.1.0', '2026-09-10T00:00:00Z', 3),
            app_release('v1.0.0', '2026-02-01T00:00:00Z', 250),
        ],
        taken=at(2026, 9, 14, 11), paths=store.paths,
    )


@pytest.fixture
def landed(
    store: Store,
    db_command: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Store]:
    """ClickHouse as `db raw --apply` and `db index`, from
    `raw_documents`, left it after each of two walks of `chatsbom run`,
    and `db edges` after the second. The store has the scans and the
    graphs, and none of the records: no decision either, since the
    stages did not keep them yet."""
    monkeypatch.setattr(
        'chatsbom.commands.db.edges.DEPGRAPH_ROOT', store.paths.depgraph_dir,
    )
    for module in (
        'chatsbom.commands.db.raw',
        'chatsbom.commands.data.backfill_decisions',
    ):
        monkeypatch.setattr(
            f'{module}.get_container', lambda: db_command.container,
        )
        monkeypatch.setattr(
            f'{module}.check_clickhouse_connection', lambda **_: True,
        )
    with db_command.repository() as ingest:
        records = RecordStore(ingest.client)
        run_first(store, records)
        db_command('raw', '--apply')
        db_command('index')
        run_second(store, records)
    db_command('raw', '--apply')
    db_command('index')
    db_command('edges')
    yield store


def records_differ(clickhouse_db: str, warehouse: duckdb.DuckDBPyConnection) -> set[str]:
    with QueryRepository(config(clickhouse_db)) as query:
        verdicts = compare(
            warehouse, query.client, [r.name for r in RECORDS],
        )
    return {v.name for v in verdicts if not v.agrees}


def test_without_its_decisions_the_store_lacks_the_releases_and_refs(
    landed: Store, clickhouse_db: str, built: Build,
) -> None:
    """What phase 5 would lose with `raw_documents`, before the backfill."""
    assert records_differ(clickhouse_db, built()) == {
        'releases', 'repository_releases', 'refs',
    }


def test_backfilled_the_store_has_what_raw_documents_had(
    landed: Store, clickhouse_db: str, built: Build,
) -> None:
    """#147's acceptance: the decisions backfilled from `raw_documents`,
    the parity check holds with the releases and refs included, and the
    warehouse built from the store alone has the releases `db index`
    read from `raw_documents`."""
    runner = CliRunner()
    result = runner.invoke(app, ['data', 'backfill-decisions', '--apply'])
    assert result.exit_code == 0, result.output

    warehouse = built()
    agree(clickhouse_db, warehouse)
    assert warehouse.execute(
        'SELECT repository_id, tag_name FROM releases ORDER BY 1, 2',
    ).fetchall() == [
        (1, 'v1.0.0'), (1, 'v1.1.0'), (3, '2.9.0'), (3, '3.0.0-beta'),
        (7, 'v1.0.0'),
    ]

    again = runner.invoke(app, ['data', 'backfill-decisions', '--apply'])
    assert again.exit_code == 0, again.output
    assert 'Nothing to write' in again.output
