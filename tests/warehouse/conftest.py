"""A store on disk, written as the collectors write it, for the warehouse.

`warehouse build` reads `data/` and nothing else (#131), so its tests
write `data/` and nothing else: search snapshots, records in the
`07-sbom` lists, Syft documents and manifests under
`<repository_id>/<commit>`, and dependency graphs kept by
`core/depgraph_store`, each fetch beside the one before. Every date a
reader takes from a file is set here, a Syft document's by its mtime.

And, at the end, the inputs the golden fixtures were recorded from.
"""
from __future__ import annotations

import json
import os
import random
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.core import decisions
from chatsbom.core import depgraph_store
from chatsbom.core.config import PathConfig
from chatsbom.core.instants import UNSET
from chatsbom.services.db_service import ecosystems_of
from tests import contract

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
        """`01-github-search/all-<day>.jsonl`, as the collector's
        universe writes it; `complete` also leaves the marker a finished
        search leaves. Its name, as a repository's `snapshot` says it."""
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

    def metadata(
        self,
        repository_id: int,
        owner: str,
        repo: str,
        *,
        listing: str = 'go',
        **fields: Any,
    ) -> dict[str, Any]:
        """The repository resource `github repo` appends to
        `02-github-repo/<listing>.jsonl`: the newest line of a
        repository is what GitHub last said of it."""
        record: dict[str, Any] = {
            'id': repository_id, 'owner': owner, 'repo': repo,
            'url': f'https://github.com/{owner}/{repo}',
            **fields,
        }
        listing_path = self.paths.repo_dir / f'{listing}.jsonl'
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

    def tree(
        self,
        repository_id: int,
        commit: str,
        paths: Iterable[str],
        at: datetime | None = None,
    ) -> Path:
        """`05-github-tree/<id>/<commit>/tree.txt`, the paths the tree
        stage listed at `commit` when it was the download target."""
        path = self.paths.tree_file(repository_id, commit)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(''.join(f'{p}\n' for p in paths), encoding='utf-8')
        if at is not None:
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


# == The inputs the golden fixtures were recorded from ====================
#
# Three, as the parity tests fed both engines while they stood side by
# side (#141, #146, #148), and as the golden tests feed the warehouse
# alone now (`tests/golden/`):
#
# - the contract corpus (`tests/contract/`), each of whose rows is a case
#   two readers of this data have disagreed on;
# - a synthetic corpus of a few hundred repositories, with every source,
#   history, per-manifest repeats, names shared across ecosystems,
#   constraints and unversioned rows, more than twelve languages, and
#   repositories outside the corpus;
# - a store on disk as two collections leave it, for `warehouse build`:
#   the parsers' own rows, with the manifests read for each scan's
#   verdicts.
#
# The rows are ClickHouse's shape, as `db index` wrote them, which
# `chatsbom/warehouse/rows.py` loads. Nothing here may change without the
# fixtures being recorded again, which took ClickHouse, and it is gone:
# a test that needs another input adds one.

# -- the contract corpus ------------------------------------------------


def aware(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def contract_rows() -> tuple[
    list[dict[str, Any]], list[dict[str, Any]],
    list[tuple[str, str, int, datetime]],
]:
    """The contract corpus's rows: its repositories, artifacts and
    edges. Every repository is the corpus, as in ClickHouse while no
    repository named a snapshot."""
    return (
        aware(contract.REPOSITORIES_SEED),
        aware(contract.ARTIFACTS_SEED),
        [
            (parent, child, count, contract.SEP.replace(tzinfo=UTC))
            for parent, child, count in contract.EDGES_SEED
        ],
    )


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
    """Rows as `db index` wrote them: each repository's row names its
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
            repository_row(
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


def with_releases(
    repositories: list[dict[str, Any]], seed: int = 147,
) -> list[dict[str, Any]]:
    """`releases` rows for about half the repositories, as `db index`
    wrote them, and each one's row saying how many and which is the
    latest stable one. An asset has a download count, which the store's
    release lists leave out (#147): the releases are compared without
    it."""
    rng = random.Random(seed)
    releases: list[dict[str, Any]] = []
    for row in repositories:
        if rng.random() < 0.5:
            continue
        count = rng.randrange(1, 6)
        listed = []
        for k in range(count):
            published = datetime(2025, 1, 1, tzinfo=UTC) + timedelta(
                days=30 * k + rng.randrange(20), hours=rng.randrange(24),
            )
            prerelease = k == count - 1 and rng.random() < 0.3
            tag = f'v{k}.0.0' + ('-rc1' if prerelease else '')
            assets = [
                {
                    'name': f'app-{k}-{a}.tar.gz', 'size': rng.randrange(10 ** 6),
                    'download_count': rng.randrange(10 ** 4),
                    'content_type': 'application/gzip',
                    'browser_download_url':
                        f'https://github.com/o/r/releases/download/{tag}/{a}',
                    'created_at': published.strftime('%Y-%m-%dT%H:%M:%SZ'),
                }
                for a in range(rng.choice([0, 0, 1, 2]))
            ]
            listed.append({
                'repository_id': row['id'], 'release_id': row['id'] * 100 + k,
                'tag_name': tag, 'name': tag, 'is_prerelease': prerelease,
                'is_draft': False, 'published_at': published,
                'target_commitish': 'main', 'created_at': published,
                'release_assets': json.dumps(assets),
                'source': 'github_release',
            })
        stable = [r for r in listed if not r['is_prerelease']]
        latest = stable[-1] if stable else None
        row.update(
            has_releases=True, total_releases=count,
            latest_release_tag=latest['tag_name'] if latest else '',
            latest_release_published_at=(
                latest['published_at'] if latest else UNSET
            ),
        )
        releases += listed
    return releases


def repository_row(
    repository_id: int,
    language: str,
    snapshot: str,
    commit: str,
    graph: datetime,
    ecosystems: list[str],
) -> dict[str, Any]:
    """A `repositories` row as `db index` wrote it: its commit and graph
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


# -- a store, as two collections leave it --------------------------------

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


def app_release(tag: str, published: str, downloads: int) -> dict[str, Any]:
    """One of `acme/app`'s releases, as its record lists it."""
    return {
        'id': int(published[5:7]), 'tag_name': tag, 'name': tag,
        'published_at': published, 'created_at': published,
        'prerelease': False, 'draft': False, 'target_commitish': 'main',
        'source': 'github_release',
        'assets': [{
            'name': f'app-{tag}.jar', 'size': 2048,
            'download_count': downloads, 'content_type': 'application/java',
            'browser_download_url': f'https://example/{tag}/app.jar',
            'created_at': published, 'uploader': {'login': 'acme'},
        }],
    }


def released(*releases: dict[str, Any]) -> dict[str, Any]:
    """A record's releases, the first the latest stable one."""
    return {
        'all_releases': list(releases), 'latest_stable_release': releases[0],
        'has_releases': True, 'total_releases': len(releases),
    }


APP_V1 = app_release('v1.0.0', '2026-02-01T00:00:00Z', 10)


def first_collection(store: Store) -> None:
    """What the collectors had written by the first collection."""
    store.snapshot(
        date(2026, 9, 1), APP, WEB, DART, PODS, GRAPHED, BARE,
    )
    store.snapshot(date(2026, 3, 1), APP, WEB, GONE)
    # Today's search, still running: not the corpus.
    store.snapshot(date(2026, 9, 29), APP)

    store.sbom(
        1, A, artifact('guava', '32.1.0-jre', 'java-archive'),
        artifact('junit', '4.13.2', 'java-archive', licenses=['EPL-1.0']),
        at=at(2026, 2, 11, 9, 30),
    )
    # Each commit's manifests are fetched before it is scanned, as the
    # content stage runs before Syft: the warehouse dates a commit by
    # them, and ClickHouse dated it by the document: the months agree.
    store.content(1, A, {'build.gradle': GRADLE}, at=at(2026, 2, 11, 7, 30))
    store.record(
        1, 'acme', 'app', commit=A, listing='java', **released(APP_V1),
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
        at=at(2026, 1, 31, 18, 0),
    )
    store.record(2, 'acme', 'web', commit=A, listing='typescript')

    store.sbom(
        3, A, artifact('flutter_bloc', '8.1.3', 'dart-pub', licenses=['MIT']),
        at=at(2026, 2, 3),
    )
    store.content(
        3, A, {'pubspec.yaml': 'name: app\n'}, at=at(2026, 2, 2, 22, 0),
    )
    store.record(3, 'acme', 'dart', commit=A, listing='dart')

    store.sbom(4, A, at=at(2026, 2, 4))
    store.content(4, A, {'Pods.podspec': PODSPEC}, at=at(2026, 2, 3, 23, 0))
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
    store.content(1, B, {'build.gradle': GRADLE}, at=at(2026, 9, 14, 8, 0))
    store.record(
        1, 'acme', 'app', commit=B, ref='v1.1.0', listing='java',
        **released(
            app_release('v1.1.0', '2026-09-10T00:00:00Z', 3),
            app_release('v1.0.0', '2026-02-01T00:00:00Z', 250),
        ),
    )
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
        at=at(2026, 7, 1, 10, 0),
    )
    store.record(2, 'acme', 'web', commit=B, listing='typescript')
