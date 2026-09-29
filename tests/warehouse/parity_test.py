"""Both engines over one input, compared rollup by rollup (#131).

Three inputs, each into a `chatsbom_test_*` database of its own, made
and dropped by `clickhouse_db`, and into a warehouse:

- the web's contract corpus, the rows `web/test/fixtures/contract/
  build.py` seeds, which hold each case where two readers of this data
  have disagreed;
- a synthetic corpus of a few hundred repositories, with every source,
  history, per-manifest repeats, names shared across ecosystems,
  constraints and unversioned rows, more than twelve languages, and
  repositories outside the corpus;
- a store on disk, indexed by `db index --from-files` as it stood after
  each of two collections, and counted by `db edges`, against
  `warehouse build` of the store as it stands after both.

On the first two, `scripts/verify_rollups.py`, the oracle, is run on
ClickHouse too: the warehouse agrees with ClickHouse, which agrees with
the answers computed another way.
"""
from __future__ import annotations

import importlib.util
import random
import sys
from collections.abc import Iterator
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.instants import UNSET
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.rollups import REFRESH_ORDER
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.services.db_service import ecosystems_of
from chatsbom.warehouse import connect
from chatsbom.warehouse.parity import compare
from chatsbom.warehouse.parity import FOUNDATIONS
from chatsbom.warehouse.parity import report
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import spdx
from tests.warehouse.conftest import Store

pytestmark = requires_clickhouse

ROOT = Path(__file__).resolve().parents[2]
UTC = timezone.utc


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
) -> duckdb.DuckDBPyConnection:
    con = connect(':memory:')
    load(con, repositories, artifacts, edges, corpus=corpus)
    derive(con)
    return con


def agree(database: str, warehouse: duckdb.DuckDBPyConnection) -> None:
    with QueryRepository(config(database)) as query:
        verdicts = compare(warehouse, query.client)
    assert [v.name for v in verdicts] == [
        *(name for name, _ in FOUNDATIONS), *REFRESH_ORDER,
    ]
    assert all(v.agrees for v in verdicts), report(verdicts)
    # Not agreement on nothing: every relation of the input has rows.
    assert all(v.rows for v in verdicts), report(verdicts)


def oracle_agrees(
    database: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`scripts/verify_rollups.py` on the same ClickHouse database."""
    script = module('scripts/verify_rollups.py', 'verify_rollups')
    with QueryRepository(config(database)) as query:
        monkeypatch.setattr(
            script, 'get_container',
            lambda: SimpleNamespace(get_export_repository=lambda: query),
        )
        monkeypatch.setattr(sys, 'argv', ['verify_rollups.py'])
        failures = script.main()
    out = capsys.readouterr().out
    assert failures == 0, out


# -- the contract corpus ------------------------------------------------


def aware(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The seed's instants, which it writes without a zone, as the UTC
    they are: `clickhouse_connect` would read a naive one as the
    machine's local time (`core/instants.py`)."""
    return [
        {
            key: value.replace(tzinfo=UTC)
            if isinstance(value, datetime) and value.tzinfo is None
            else value
            for key, value in row.items()
        }
        for row in rows
    ]


def test_the_contract_corpus(
    clickhouse_db: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    contract = module(
        'web/test/fixtures/contract/build.py', 'contract_build',
    )
    repositories = aware(contract.REPOSITORIES_SEED)
    artifacts = aware(contract.ARTIFACTS_SEED)
    edges = [
        (parent, child, count, contract.SEP.replace(tzinfo=UTC))
        for parent, child, count in contract.EDGES_SEED
    ]
    seed(clickhouse_db, repositories, artifacts, edges)
    with warehouse_of(repositories, artifacts, edges, None) as warehouse:
        agree(clickhouse_db, warehouse)
    oracle_agrees(clickhouse_db, monkeypatch, capsys)


# -- a synthetic corpus ---------------------------------------------------

CORPUS = 'all-2026-09-01'
NO_GRAPH = UNSET

#: Names shared by several ecosystems, and names of one.
SHARED = ('mail', 'utils', 'core', 'client', 'config')
LANGUAGES = (
    'Ruby', 'Python', 'JavaScript', 'TypeScript', 'Java', 'Go', 'PHP',
    'Rust', 'C#', 'Kotlin', 'Swift', 'Dart', 'C++', 'Scala', 'Elixir',
    'Haskell', '',
)
#: Syft's types, some that are no ecosystem, and the graph's.
SYFT_TYPES = (
    'npm', 'python', 'java-archive', 'gem', 'go-module', 'rust-crate',
    'php-composer', 'dart-pub', 'pod', 'binary', 'github-action',
)
GRAPH_TYPES = ('npm', 'pypi', 'maven', 'gem', 'cargo', 'composer', 'pub')
LICENCES: tuple[list[str], ...] = (
    [], ['MIT'], ['Apache-2.0'], ['MIT', 'Apache-2.0'], ['BSD-3-Clause'],
)


def synthetic(
    seed: int = 131, repositories: int = 300,
) -> tuple[
    list[dict[str, Any]], list[dict[str, Any]],
    list[tuple[str, str, int, datetime]], set[int],
]:
    """Rows as `db index` leaves them: each repository's row names its
    newest scan of each source, and every scan has a row, which is what
    a store yields (a scan that saw nothing has no row to load)."""
    rng = random.Random(seed)
    names = [*SHARED, *(f'pkg-{k:03}' for k in range(400))]
    weights = [1 / (rank + 1) for rank in range(len(names))]

    def name() -> str:
        return rng.choices(names, weights)[0]

    def instant() -> datetime:
        # Late in the UTC day now and then: a month made in another zone
        # would be the next.
        return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(
            days=rng.randrange(260), hours=rng.choice([1, 9, 20, 23]),
            minutes=rng.randrange(60),
        )

    repository_rows: list[dict[str, Any]] = []
    artifacts: list[dict[str, Any]] = []
    for repository_id in range(1, repositories + 1):
        rows: list[dict[str, Any]] = []
        current: list[dict[str, Any]] = []
        commits = sorted(
            {instant() for _ in range(rng.choice([0, 1, 1, 2, 3]))},
        )
        declares = rng.random() < 0.2
        for position, when in enumerate(commits):
            commit = f'{repository_id:04x}{position}'.ljust(40, '0')
            scan = [
                {
                    'repository_id': repository_id,
                    'artifact_id': f'{commit}-{k}',
                    'name': name(),
                    'version': f'{rng.randrange(5)}.{rng.randrange(20)}.0',
                    'type': rng.choice(SYFT_TYPES),
                    'purl': '',
                    'found_by': rng.choice(['a-cataloger', 'b-cataloger']),
                    'licenses': rng.choice(LICENCES),
                    'relationship': rng.choice(
                        ['direct', 'transitive', 'unknown'],
                    ),
                    'source': 'syft',
                    'version_kind': 'resolved',
                    'sbom_ref': 'main',
                    'sbom_commit_sha': commit,
                    'observed_at': when,
                }
                for k in range(rng.randrange(1, 30))
            ]
            if declares:
                scan += [
                    {
                        **scan[0], 'artifact_id': f'{commit}-gradle-{k}',
                        'name': name(), 'type': 'maven', 'purl': '',
                        'licenses': [], 'relationship': 'direct',
                        'source': 'manifest',
                        'version': rng.choice(['', '1.0']),
                        'version_kind': 'constraint',
                        'found_by': 'gradle-literal',
                    }
                    for k in range(rng.randrange(1, 4))
                ]
                for declared in scan:
                    if declared['source'] == 'manifest' and not declared['version']:
                        declared['version_kind'] = 'unversioned'
            rows += scan
            if position == len(commits) - 1:
                current += scan
        graphs = sorted({instant() for _ in range(rng.choice([0, 0, 1, 2]))})
        for position, when in enumerate(graphs):
            fetched: list[dict[str, Any]] = []
            for k in range(rng.randrange(1, 20)):
                kind = rng.choice(
                    ['resolved', 'resolved', 'constraint', 'unversioned'],
                )
                fact = {
                    'repository_id': repository_id,
                    'artifact_id': f'graph-{k}',
                    'name': name(),
                    'version': {
                        'resolved': f'{rng.randrange(3)}.1.0',
                        'constraint': rng.choice(['^1.2', '>= 2.0', '~> 3']),
                        'unversioned': '',
                    }[kind],
                    'type': rng.choice(GRAPH_TYPES),
                    'purl': '',
                    'found_by': 'github-dependency-graph',
                    'licenses': rng.choice(LICENCES[:3]),
                    'relationship': rng.choice(['direct', 'transitive']),
                    'source': 'github-depgraph',
                    'version_kind': kind,
                    'sbom_ref': 'main',
                    'sbom_commit_sha': '',
                    'observed_at': when,
                }
                fetched.append(fact)
                if rng.random() < 0.2:
                    # The same package in a second manifest.
                    fetched.append({**fact, 'artifact_id': f'graph-{k}-again'})
            rows += fetched
            if position == len(graphs) - 1:
                current += fetched
        artifacts += rows
        roll = rng.random()
        snapshot = (
            CORPUS if roll < 0.85 else 'all-2026-03-01' if roll < 0.95 else ''
        )
        language = rng.choice(LANGUAGES)
        repository_rows.append(
            clickhouse_repository(
                repository_id, language, snapshot,
                commit=current[0]['sbom_commit_sha'] if commits else '',
                graph=graphs[-1] if graphs else NO_GRAPH,
                ecosystems=ecosystems_of(current),
            ),
        )
    edges = [
        (name(), name(), rng.randrange(1, 20), datetime(2026, 9, 13, tzinfo=UTC))
        for _ in range(200)
    ]
    unique = {
        (parent, child): (parent, child, n, t)
        for parent, child, n, t in edges
    }
    corpus = {r['id'] for r in repository_rows if r['snapshot'] == CORPUS}
    return repository_rows, artifacts, list(unique.values()), corpus


def clickhouse_repository(
    repository_id: int,
    language: str,
    snapshot: str,
    commit: str,
    graph: datetime,
    ecosystems: list[str],
) -> dict[str, Any]:
    """A `repositories` row as `db index` writes it: its commit and graph
    are the newest scans'."""
    return {
        'id': repository_id, 'owner': f'owner{repository_id % 37}',
        'repo': f'repo{repository_id}',
        'url': f'https://github.com/o/r{repository_id}',
        'stars': 1000 + repository_id, 'description': '',
        'created_at': UNSET, 'language': language, 'topics': [],
        'default_branch': 'main', 'sbom_ref': 'main' if commit else '',
        'sbom_ref_type': 'branch' if commit else '',
        'sbom_commit_sha': commit, 'sbom_commit_sha_short': commit[:7],
        'has_releases': False, 'latest_release_tag': '',
        'latest_release_published_at': UNSET, 'total_releases': 0,
        'pushed_at': UNSET, 'is_archived': False, 'is_fork': False,
        'is_template': False, 'is_mirror': False, 'disk_usage': 0,
        'fork_count': 0, 'watchers_count': 0, 'license_spdx_id': '',
        'license_name': '', 'manifest_sources': [],
        'depgraph_observed_at': graph,
        'depgraph_ref': 'main' if graph != NO_GRAPH else '',
        'depgraph_commit_sha': '', 'github_language': language,
        'ecosystems': ecosystems, 'snapshot': snapshot,
    }


def test_a_synthetic_corpus(
    clickhouse_db: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repositories, artifacts, edges, corpus = synthetic()
    seed(clickhouse_db, repositories, artifacts, edges)
    with warehouse_of(repositories, artifacts, edges, corpus) as warehouse:
        agree(clickhouse_db, warehouse)
        (buckets,), = warehouse.execute(
            'SELECT count(*) FROM mv_language_coverage '
            "WHERE language = 'other'",
        ).fetchall()
        assert buckets == 1
    oracle_agrees(clickhouse_db, monkeypatch, capsys)


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
    seed(clickhouse_db, repositories, artifacts, edges)
    path = tmp_path / 'warehouse.duckdb'
    with connect(path) as con:
        load(con, repositories, artifacts, edges, corpus=corpus)
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
    assert 'All 19 agree.' in out

    # A warehouse of other rows: the status counts what differs.
    with connect(path) as con:
        con.execute('DELETE FROM mv_totals')
    status, out = run(['warehouse_parity.py', str(path)])
    assert status == 1
    assert 'mv_totals' in out and 'DIFFERS' in out


# -- a store, through `db index` and `warehouse build` --------------------

A = 'a' * 40
B = 'b' * 40

APP = Listed(1, 'acme', 'app', stars=500, language='Java')
WEB = Listed(2, 'acme', 'web', stars=900, language='TypeScript')
DART = Listed(3, 'acme', 'dart', stars=1500, language='Dart')
PODS = Listed(4, 'acme', 'pods', stars=700, language='Objective-C')
GRAPHED = Listed(5, 'acme', 'graphed', stars=1200, language='Kotlin')
BARE = Listed(6, 'acme', 'bare', stars=3000, language='C++')
GONE = Listed(7, 'acme', 'gone', stars=800, language='Go')

GRADLE = """
dependencies {
    implementation 'com.google.guava:guava:33.0.0-jre'
    testImplementation 'junit:junit:4.13.2'
}
"""
PODSPEC = """
Pod::Spec.new do |s|
  s.name = 'Pods'
  s.dependency 'AFNetworking', '~> 4.0'
  s.dependency 'SDWebImage'
end
"""


def first_collection(store: Store) -> None:
    """What the collectors had written by the first `db index`."""
    listed = store.snapshot(
        date(2026, 9, 1), APP, WEB, DART, PODS, GRAPHED, BARE,
    )
    store.seed(listed, APP, WEB, DART, PODS, GRAPHED, BARE)
    older = store.snapshot(date(2026, 3, 1), APP, WEB, GONE)
    store.seed(older, GONE)
    # Today's search, still running: not the corpus.
    store.snapshot(date(2026, 9, 29), APP)

    store.sbom(
        1, A, artifact('guava', '32.1.0-jre', 'java-archive'),
        artifact('junit', '4.13.2', 'java-archive', licenses=['EPL-1.0']),
        at=at(2026, 2, 11, 9, 30),
    )
    store.content(1, A, {'build.gradle': GRADLE})
    store.record(1, 'acme', 'app', commit=A, listing='java')
    store.graph(
        1,
        spdx(
            '2026-03-05T08:00:00Z',
            [('com.google.guava:guava', '32.1.0-jre', 'maven')],
            direct=['com.google.guava:guava'],
        ),
        fetched=at(2026, 3, 5, 8, 0),
    )

    npm = [
        artifact('react', '18.2.0', 'npm', licenses=['MIT']),
        artifact('lodash', '4.17.21', 'npm', licenses=['MIT']),
        artifact('js-tokens', '4.0.0', 'npm'),
    ]
    store.sbom(2, A, *npm, at=at(2026, 1, 31, 20, 0))
    store.content(
        2, A, {
            'package.json': '{"dependencies": {"react": "18.2.0", '
                            '"lodash": "4.17.21"}}',
        },
    )
    store.record(2, 'acme', 'web', commit=A, listing='typescript')

    store.sbom(
        3, A, artifact('flutter_bloc', '8.1.3', 'dart-pub', licenses=['MIT']),
        at=at(2026, 2, 3),
    )
    store.content(3, A, {'pubspec.yaml': 'name: app\n'})
    store.record(3, 'acme', 'dart', commit=A, listing='dart')

    store.sbom(4, A, at=at(2026, 2, 4))
    store.content(4, A, {'Pods.podspec': PODSPEC})
    store.record(4, 'acme', 'pods', commit=A)

    store.graph(
        5,
        spdx(
            '2026-02-20T10:00:00Z',
            [
                ('left-pad', '1.3.0', 'npm'), ('debug', '4.3.4', 'npm'),
                ('ms', '2.1.2', 'npm'),
            ],
            direct=['left-pad', 'debug'], edges=[('debug', 'ms')],
        ),
        fetched=at(2026, 2, 20, 10, 0),
    )

    store.sbom(
        7, A, artifact(
            'cobra', '1.8.0',
            'go-module',
        ), at=at(2026, 2, 5),
    )
    store.record(7, 'acme', 'gone', commit=A)


def second_collection(store: Store) -> None:
    """A second walk: new commits, new fetches. The store keeps the
    first's, as it keeps every output."""
    store.sbom(
        1, B, artifact('guava', '33.0.0-jre', 'java-archive'),
        artifact('junit', '4.13.2', 'java-archive', licenses=['EPL-1.0']),
        artifact('jsr305', '3.0.2', 'java-archive'),
        at=at(2026, 9, 14, 10, 0),
    )
    store.content(1, B, {'build.gradle': GRADLE})
    store.record(1, 'acme', 'app', commit=B, listing='java')
    store.graph(
        1,
        spdx(
            '2026-09-13T08:00:00Z',
            [
                ('com.google.guava:guava', '33.0.0-jre', 'maven'),
                ('com.google.code.findbugs:jsr305', '3.0.2', 'maven'),
            ],
            direct=['com.google.guava:guava'],
            edges=[(
                'com.google.guava:guava', 'com.google.code.findbugs:jsr305',
            )],
        ),
        fetched=at(2026, 9, 13, 8, 0),
    )
    # `lodash` is gone from the second commit, and `react` is declared.
    store.sbom(
        2, B, artifact('react', '18.3.1', 'npm', licenses=['MIT']),
        artifact('js-tokens', '4.0.0', 'npm'),
        at=at(2026, 7, 1, 12, 0),
    )
    store.content(
        2, B, {
            'package.json': '{"dependencies": {"react": "18.3.1"}}',
        },
    )
    store.record(2, 'acme', 'web', commit=B, listing='typescript')


@pytest.fixture
def indexed(
    store: Store,
    db_command: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Store]:
    """ClickHouse as `db index --from-files` left it after each
    collection, and `db edges` after the second."""
    monkeypatch.setattr(
        'chatsbom.commands.db.edges.DEPGRAPH_ROOT', store.paths.depgraph_dir,
    )
    first_collection(store)
    db_command('index', '--from-files')
    second_collection(store)
    db_command('index', '--from-files')
    db_command('edges')
    yield store


def test_a_store_through_both_engines(
    indexed: Store,
    clickhouse_db: str,
    built: Build,
) -> None:
    agree(clickhouse_db, built())


def test_the_store_has_what_the_parity_needs(
    indexed: Store, built: Build,
) -> None:
    """The store case is not agreement on nothing: history, a dependency
    that went, every source, and a repository outside the corpus."""
    warehouse = built()
    assert warehouse.execute(
        'SELECT source, count(*) FROM scans GROUP BY source ORDER BY source',
    ).fetchall() == [('github-depgraph', 3), ('manifest', 7), ('syft', 7)]
    assert warehouse.execute(
        "SELECT count(*) FROM facts WHERE source = 'manifest'",
    ).fetchone() == (4,)
    assert warehouse.execute(
        "SELECT name FROM mv_package_month WHERE month = '2026-01' "
        'ORDER BY name',
    ).fetchall() == [('js-tokens',), ('lodash',), ('react',)]
