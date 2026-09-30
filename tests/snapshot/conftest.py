"""Warehouses made from rows, for the snapshots written from them (#132).

A snapshot is written from `warehouse.duckdb` and nothing else, so each
test makes one: ClickHouse-shaped rows loaded as `rows.load` loads the
contract corpus for the golden tests, the current facts and the rollups
derived as a pass derives them, and the `build` row a pass writes last,
which names the corpus.

`SHOP` is the small warehouse the tests read row by row: every case the
snapshot has to get right has a row here, and the comment beside it says
which.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from chatsbom.warehouse import connect
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from chatsbom.warehouse.writer import Scan
from chatsbom.warehouse.writer import Writer
from tests import contract

UTC = timezone.utc


def at(year: int, month: int, day: int, hour: int = 0) -> datetime:
    """An instant in UTC."""
    return datetime(year, month, day, hour, tzinfo=UTC)


def aware(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The seed's instants, which it writes without a zone, as the UTC
    they are."""
    return [
        {
            key: value.replace(tzinfo=UTC)
            if isinstance(value, datetime) and value.tzinfo is None
            else value
            for key, value in row.items()
        }
        for row in rows
    ]


def repository(
    id: int,
    owner: str,
    repo: str,
    stars: int,
    language: str,
    **fields: Any,
) -> dict[str, Any]:
    """A `repositories` row, as the warehouse keeps one."""
    return {
        'id': id, 'owner': owner, 'repo': repo, 'stars': stars,
        'url': f'https://github.com/{owner}/{repo}',
        'github_language': language, 'language': language,
        'pushed_at': at(2026, 9, 1), 'license_spdx_id': '',
        'description': '', 'snapshot': 'all-2026-09-01',
        **fields,
    }


def artifact(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    *,
    observed_at: datetime,
    relationship: str = 'transitive',
    licenses: Iterable[str] = (),
    found_by: str = 'cataloger',
    source: str = 'syft',
    version_kind: str = 'resolved',
    commit: str = '',
    ref: str = 'main',
) -> dict[str, Any]:
    """An `artifacts` row of one scan: a Syft or manifest scan of
    `commit`, or a dependency graph dated `observed_at`."""
    return {
        'repository_id': repository_id,
        'artifact_id': f'{repository_id}-{name}-{version}-{found_by}',
        'name': name, 'version': version, 'type': type, 'purl': '',
        'found_by': found_by, 'licenses': list(licenses),
        'relationship': relationship, 'source': source,
        'version_kind': version_kind,
        'sbom_ref': ref if commit else '', 'sbom_commit_sha': commit,
        'observed_at': observed_at,
    }


def graph(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    *,
    observed_at: datetime,
    relationship: str = 'direct',
    version_kind: str = 'constraint',
) -> dict[str, Any]:
    """A row of a dependency-graph document."""
    return artifact(
        repository_id, name, version, type, observed_at=observed_at,
        relationship=relationship, found_by='github-dependency-graph',
        source='github-depgraph', version_kind=version_kind,
    )


@dataclass
class Corpus:
    """What a warehouse is made from."""

    repositories: list[dict[str, Any]]
    artifacts: list[dict[str, Any]]
    edges: list[tuple[str, str, int, datetime]] = field(default_factory=list)
    #: The corpus's ids; None for every repository.
    corpus: set[int] | None = None
    #: Scans that saw nothing, which rows cannot say: `(repository,
    #: source, key, instant)`, the key a Syft scan's commit.
    empty: list[tuple[int, str, str, datetime]] = field(
        default_factory=list,
    )
    #: The `build` row's fields; None for no row, as `rows.load` leaves
    #: a warehouse.
    build: dict[str, Any] | None = None


def warehouse(path: Path, corpus: Corpus) -> Path:
    """A warehouse file of `corpus`, derived and closed."""
    with connect(path) as con:
        load(
            con, corpus.repositories, corpus.artifacts, corpus.edges,
            corpus=corpus.corpus,
        )
        with Writer(con) as writer:
            for repository_id, source, key, instant in corpus.empty:
                syft = source == 'syft'
                writer.scan(
                    Scan(
                        repository_id=repository_id, source=source,
                        input_key=key, tool='', observed_at=instant,
                        ref='main' if syft else '',
                        commit_sha=key if syft else '',
                    ),
                )
            if corpus.build is not None:
                writer.add('build', corpus.build)
        derive(con)
        con.execute('CHECKPOINT')
    return path


#: What a pass writes last, but the instant and the store: neither is
#: in a snapshot, so no two passes need agree on them.
BUILD: dict[str, Any] = {
    'version': '0.5.4', 'built_at': at(2026, 9, 29, 12),
    'store': '/srv/chatsbom/data', 'corpus': 'all-2026-09-01',
    'repositories': 4, 'scans': 6, 'observations': 10, 'unreadable': 0,
    'unnamed': 0,
}

JAN = at(2026, 1, 15, 9)
FEB = at(2026, 2, 1, 12)
MAR = at(2026, 3, 10, 9)
APR = at(2026, 4, 2, 23)
SEP = at(2026, 9, 13, 8)


def shop() -> Corpus:
    """Four repositories, three of them the corpus."""
    return Corpus(
        repositories=[
            repository(
                1, 'acme', 'app', 300, 'Ruby', license_spdx_id='MIT',
                description='An app',
            ),
            # Its description is not ASCII: the id is made of the rows'
            # values, whatever their characters.
            repository(
                2, 'acme', 'web', 500, 'JavaScript',
                description='Ünïcode — the web 🕸', pushed_at=at(2026, 8, 1),
            ),
            # No language on GitHub, and scanned once, finding nothing.
            repository(3, 'acme', 'idle', 100, ''),
            # Not in the corpus: in no table of a snapshot.
            repository(4, 'other', 'gone', 50, 'Go', snapshot=''),
        ],
        artifacts=[
            # app: a January scan the March one replaced, which shows
            # `rack` too, so the months between them count (Q9); and the
            # graph in September.
            artifact(
                1, 'rack', '3.0.0', 'gem', observed_at=JAN, commit='c1',
                ref='v1.0.0', licenses=['MIT'],
                found_by='gemspec-cataloger',
            ),
            artifact(
                1, 'rack', '3.1.0', 'gem', observed_at=MAR, commit='c2',
                ref='v2.0.0', relationship='direct', licenses=['MIT'],
                found_by='gemfile-lock-cataloger',
            ),
            artifact(
                1, 'puma', '6.4.0', 'gem', observed_at=MAR, commit='c2',
                ref='v2.0.0', found_by='gemfile-lock-cataloger',
            ),
            graph(1, 'rack', '~> 3.1', 'gem', observed_at=SEP),
            # web: every licence case, a name shared with app, and
            # Syft's spelling of Composer, which a snapshot shows as the
            # page does.
            artifact(
                2, 'lodash', '4.17.21', 'npm', observed_at=FEB, commit='w1',
                licenses=['MIT'], found_by='javascript-lock-cataloger',
            ),
            artifact(
                2, 'left-pad', '1.3.0', 'npm', observed_at=FEB, commit='w1',
                relationship='direct', licenses=['WTFPL'],
                found_by='javascript-lock-cataloger',
            ),
            artifact(
                2, 'rack', '3.1.0', 'gem', observed_at=FEB, commit='w1',
                licenses=['MIT', 'Ruby'], found_by='gemfile-lock-cataloger',
            ),
            artifact(
                2, 'laravel/framework', 'v12.0.0', 'php-composer',
                observed_at=FEB, commit='w1', relationship='direct',
                licenses=['MIT'], found_by='php-composer-lock-cataloger',
            ),
            artifact(
                4, 'cobra', '1.8.0', 'go-module', observed_at=FEB,
                commit='g1', relationship='direct', licenses=['Apache-2.0'],
            ),
        ],
        edges=[
            ('rack', 'puma', 2, SEP),
            ('lodash', 'left-pad', 1, SEP),
            # A package no fact names: left out, as `export d1` leaves it.
            ('mystery', 'rack', 1, SEP),
        ],
        corpus={1, 2, 3},
        empty=[
            (3, 'syft', 'i1', APR),
            # web's graph, fetched after its Syft scan and empty: web is
            # still dated by the scan that saw its dependencies, as
            # `export d1` dates it by its newest row, and has no graph
            # date to show beside a row.
            (2, 'github-depgraph', SEP.isoformat(), SEP),
        ],
        build=dict(BUILD),
    )


def contract_corpus() -> Corpus:
    """The contract suite's seed, as `tests/warehouse/golden_test.py`
    loads it: every repository is the corpus, as in ClickHouse while no
    repository named a snapshot."""
    return Corpus(
        repositories=aware(contract.REPOSITORIES_SEED),
        artifacts=aware(contract.ARTIFACTS_SEED),
        edges=[
            (parent, child, count, contract.SEP.replace(tzinfo=UTC))
            for parent, child, count in contract.EDGES_SEED
        ],
        build={**BUILD, 'corpus': ''},
    )
