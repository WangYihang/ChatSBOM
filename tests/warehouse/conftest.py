"""A store on disk, written as the collectors write it, for the warehouse.

`warehouse build` reads `data/` and nothing else (#131), so its tests
write `data/` and nothing else: search snapshots, the ledger, records
in the `07-sbom` lists, Syft documents and manifests under
`<repository_id>/<commit>`, and dependency graphs kept by
`core/depgraph_store`, each fetch beside the one before. Every date a
reader takes from a file is set here, a Syft document's by its mtime.
"""
from __future__ import annotations

import json
import os
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.core import decisions
from chatsbom.core import depgraph_store
from chatsbom.core.config import PathConfig
from chatsbom.core.ledger import Ledger

UTC = timezone.utc

#: The day the tests build on: a snapshot dated before it is complete.
TODAY = date(2026, 9, 29)


def at(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0,
    second: int = 0,
) -> datetime:
    """An instant in UTC."""
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


@dataclass(frozen=True)
class Listed:
    """A repository as a search snapshot lists it."""

    id: int
    owner: str
    repo: str
    stars: int = 1000
    language: str = ''
    branch: str = 'main'
    pushed_at: str = '2026-09-01T00:00:00Z'


def artifact(
    name: str,
    version: str,
    type: str,
    *,
    purl: str | None = None,
    found_by: str = 'cataloger',
    licenses: Iterable[str] = (),
    id: str = '',
) -> dict[str, Any]:
    """One entry of a Syft document's `artifacts`."""
    ecosystem = {
        'java-archive': 'maven', 'python': 'pypi', 'go-module': 'golang',
        'rust-crate': 'cargo', 'php-composer': 'composer',
        'dart-pub': 'pub', 'pod': 'cocoapods',
    }.get(type, type)
    return {
        'id': id or f'{type}-{name}-{version}',
        'name': name,
        'version': version,
        'type': type,
        'foundBy': found_by,
        'purl': purl if purl is not None else (
            f'pkg:{ecosystem}/{name}@{version}'
        ),
        'licenses': [{'value': value} for value in licenses],
    }


def spdx(
    created: str | None,
    packages: Iterable[tuple[str, str, str]],
    *,
    direct: Iterable[str] = (),
    edges: Iterable[tuple[str, str]] = (),
) -> dict[str, Any]:
    """GitHub's dependency-graph answer: `(name, version, purl type)`
    packages, the repository DESCRIBED, a DEPENDS_ON from it to each
    name in `direct`, and one between packages for each of `edges`."""
    root = 'SPDXRef-github-acme-app'
    packages = list(packages)
    ids = {name: f'SPDXRef-{kind}-{name}' for name, _, kind in packages}
    sbom: dict[str, Any] = {
        'SPDXID': 'SPDXRef-DOCUMENT',
        'creationInfo': {'creators': ['Tool: GitHub.com-Dependency-Graph']},
        'packages': [
            {'SPDXID': root, 'name': 'acme/app', 'versionInfo': 'main'},
            *(
                {
                    'SPDXID': ids[name],
                    'name': name,
                    'versionInfo': version,
                    'externalRefs': [{
                        'referenceType': 'purl',
                        'referenceLocator': f'pkg:{kind}/{name}@{version}',
                    }],
                }
                for name, version, kind in packages
            ),
        ],
        'relationships': [
            {
                'relationshipType': 'DESCRIBES',
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relatedSpdxElement': root,
            },
            *(
                {
                    'relationshipType': 'DEPENDS_ON',
                    'spdxElementId': root,
                    'relatedSpdxElement': ids[name],
                }
                for name in direct
            ),
            *(
                {
                    'relationshipType': 'DEPENDS_ON',
                    'spdxElementId': ids[parent],
                    'relatedSpdxElement': ids[child],
                }
                for parent, child in edges
            ),
        ],
    }
    if created is not None:
        sbom['creationInfo']['created'] = created
    return {'sbom': sbom}


def stamp(path: Path, when: datetime) -> None:
    """The file's mtime, which is when a Syft document was made."""
    seconds = when.timestamp()
    os.utime(path, (seconds, seconds))


class Store:
    """`data/`, written a file at a time as the collectors write it."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths = PathConfig(base_data_dir=root)

    # -- the universe ---------------------------------------------------

    def snapshot(
        self,
        day: date,
        *listed: Listed,
        complete: bool = False,
    ) -> str:
        """`01-github-search/all-<day>.jsonl`, as `github search` writes
        it; `complete` also leaves the marker a finished search leaves.
        Its name, as the ledger calls it."""
        path = self.paths.search_snapshot(day)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            ''.join(
                json.dumps({
                    'id': r.id, 'owner': r.owner, 'repo': r.repo,
                    'stars': r.stars, 'language': r.language or None,
                    'default_branch': r.branch, 'pushed_at': r.pushed_at,
                }) + '\n'
                for r in listed
            ),
            encoding='utf-8',
        )
        if complete:
            path.with_name(f'{path.name}.complete').touch()
        return path.stem

    def seed(self, snapshot: str, *listed: Listed) -> None:
        """The ledger seeded from `snapshot`, as `queue track --snapshot`
        seeds it."""
        self.root.mkdir(parents=True, exist_ok=True)
        with Ledger(self.paths.ledger_path) as ledger:
            for r in listed:
                ledger.seed(
                    r.id, r.owner, r.repo, snapshot=snapshot,
                    github_language=r.language, stars=r.stars,
                    default_branch=r.branch,
                )

    # -- records --------------------------------------------------------

    def record(
        self,
        repository_id: int,
        owner: str,
        repo: str,
        *,
        commit: str | None = None,
        ref: str = 'v1.0.0',
        listing: str = 'index',
        **fields: Any,
    ) -> dict[str, Any]:
        """A record appended to `07-sbom/<listing>.jsonl`, as `sbom
        generate` files one: the newest line of a repository is its
        record. With `commit`, the download target and the paths of its
        scan."""
        record: dict[str, Any] = {
            'id': repository_id, 'owner': owner, 'name': repo,
            'stargazers_count': 100,
            'html_url': f'https://github.com/{owner}/{repo}',
            'default_branch': 'main',
            **fields,
        }
        if commit is not None:
            record['download_target'] = {
                'ref': ref, 'ref_type': 'release',
                'commit_sha': commit, 'commit_sha_short': commit[:7],
            }
            record['sbom_path'] = str(
                self.paths.sbom_file(repository_id, commit),
            )
            record['local_content_path'] = str(
                self.paths.content_root(repository_id, commit),
            )
        listing_path = self.paths.sbom_dir / f'{listing}.jsonl'
        listing_path.parent.mkdir(parents=True, exist_ok=True)
        with listing_path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record) + '\n')
        return record

    def decide(
        self,
        repository_id: int,
        *,
        pushed_at: str,
        releases: Iterable[Mapping[str, Any]] = (),
        latest: str | None = None,
        commit: str | None = None,
        ref: str = '',
        ref_type: str = '',
    ) -> dict[str, Any]:
        """The release and commit decisions `chatsbom run` keeps for one
        push (#147): the list `releases`, the tag `latest` chosen from
        it, and with `commit`, what that resolved to. The record they
        were made from."""
        listed = [dict(release) for release in releases]
        chosen = next(
            (r for r in listed if r['tag_name'] == latest), None,
        ) if latest is not None else None
        record: dict[str, Any] = {
            'id': repository_id, 'pushed_at': pushed_at,
            'all_releases': listed, 'has_releases': bool(listed),
            'total_releases': len(listed), 'latest_stable_release': chosen,
        }
        if commit is not None:
            record['download_target'] = {
                'ref': ref, 'ref_type': ref_type, 'commit_sha': commit,
                'commit_sha_short': commit[:7],
            }
        decisions.keep_release(self.paths, record)
        decisions.keep_commit(self.paths, record)
        return record

    # -- documents ------------------------------------------------------

    def sbom(
        self,
        repository_id: int,
        commit: str,
        *artifacts: dict[str, Any],
        at: datetime,
        version: str = '1.52.0',
    ) -> Path:
        """`07-sbom/<id>/<commit>/sbom.json`, as Syft writes it, made at
        `at`."""
        path = self.paths.sbom_file(repository_id, commit)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({
                'artifacts': list(artifacts),
                'artifactRelationships': [],
                'source': {'type': 'directory'},
                'descriptor': {'name': 'syft', 'version': version},
                'schema': {'version': '16.1.0'},
            }),
            encoding='utf-8',
        )
        stamp(path, at)
        return path

    def content(
        self,
        repository_id: int,
        commit: str,
        files: Mapping[str, str],
        at: datetime | None = None,
    ) -> Path:
        """The manifests `github content` fetched at `commit`, each at
        its own path under `06-github-content/<id>/<commit>`."""
        root = self.paths.content_root(repository_id, commit)
        for relative, text in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding='utf-8')
            if at is not None:
                stamp(path, at)
        root.mkdir(parents=True, exist_ok=True)
        return root

    def graph(
        self,
        repository_id: int,
        document: dict[str, Any],
        *,
        fetched: datetime,
        head: str = '',
        ref: str = 'main',
        owner: str = 'acme',
        repo: str = 'app',
    ) -> Path:
        """One fetch of the dependency graph, kept as the depgraph stage
        keeps it: a directory of its own, `meta.json` beside it, and a
        line in `index.jsonl`."""
        stored = depgraph_store.store(
            self.paths.depgraph_dir,
            repository_id=repository_id, owner=owner, repo=repo,
            payload=document, fetched_at=fetched, ref=ref,
            head_sha=head, http_status=200,
        )
        return stored.fetch.document

    def legacy_graph(
        self,
        repository_id: int,
        document: dict[str, Any],
        mtime: datetime | None = None,
    ) -> Path:
        """The one graph a repository had before every fetch was kept,
        where `data migrate-layout` moved it."""
        path = self.paths.legacy_depgraph_file(repository_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(document), encoding='utf-8')
        if mtime is not None:
            stamp(path, mtime)
        return path


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / 'data')


Build = Callable[..., duckdb.DuckDBPyConnection]


@pytest.fixture
def built(store: Store, tmp_path: Path) -> Iterator[Build]:
    """`warehouse build` of `store`, and the file it wrote, opened
    read-only as the operator's DuckDB CLI would open it."""
    opened: list[duckdb.DuckDBPyConnection] = []

    def build(today: date = TODAY) -> duckdb.DuckDBPyConnection:
        from chatsbom.warehouse.build import build as run

        output = tmp_path / 'warehouse.duckdb'
        for connection in opened:
            connection.close()
        run(store.paths, output, today=today)
        connection = duckdb.connect(str(output), read_only=True)
        opened.append(connection)
        return connection

    yield build
    for connection in opened:
        connection.close()


def rows(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    *parameters: Any,
) -> list[tuple[Any, ...]]:
    """What `sql` returns: a test orders it in the SQL, to state it."""
    return con.execute(sql, list(parameters)).fetchall()
