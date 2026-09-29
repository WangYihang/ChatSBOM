"""Build the fixture both query backends are held to (#41).

`test/contract.test.ts` asks D1 and ClickHouse the same questions about
one small corpus and expects the same answers. This writes that corpus
once, into ClickHouse, and gets both stores' copies of it from there:

- `d1.sql` is the real D1 export of it (`chatsbom export d1`), its
  scripts concatenated in the order they are applied. The suite loads
  it into SQLite, which is what D1 runs.
- `clickhouse.json` is what ClickHouse answered each statement the
  ClickHouse backend sent while the suite ran against the seeded
  database. `npm test` replays it, so the suite needs no server; a
  statement it has no answer for fails the test and names this script.
- `calls.json` is what D1 answered each call the suite made of it, in
  the same run, which the Python dataset API is held to (#138). It
  alone can be recorded again without a server, after a change to the
  D1 statements: `CONTRACT_RECORD_CALLS=1 npx vitest run
  test/contract.test.ts`, from `web/`.

Run it again whenever the seed, the D1 export or a ClickHouse statement
changes, with a ClickHouse server up (`docker compose up -d clickhouse`,
or the local one on 127.0.0.1:8123):

    uv run python web/test/fixtures/contract/build.py

It creates a database of its own, `chatsbom_contract_<hex>`, and drops
it afterwards: `--keep` leaves it, for a server to point a Worker at,
and `--no-test` stops after writing `d1.sql`. The server and the admin
account are the ones the Python suite uses (`CLICKHOUSE_TEST_HOST`,
`CLICKHOUSE_TEST_PORT`, `CLICKHOUSE_ADMIN_USER`,
`CLICKHOUSE_ADMIN_PASSWORD`). Admin rather than guest: guest's grants
name `chatsbom` alone, and the statements are the same either way.

The seed is small on purpose, and every row is here for a reason given
beside it: each is a case where the two stores have disagreed.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.d1 import export_d1

HERE = Path(__file__).resolve().parent
WEB = HERE.parents[2]
D1_SQL = HERE / 'd1.sql'
RECORDED = HERE / 'clickhouse.json'

HOST = os.getenv('CLICKHOUSE_TEST_HOST', 'localhost')
PORT = int(os.getenv('CLICKHOUSE_TEST_PORT', '8123'))
USER = os.getenv('CLICKHOUSE_ADMIN_USER', 'admin')
PASSWORD = os.getenv('CLICKHOUSE_ADMIN_PASSWORD', 'admin')

#: Syft's scans. Rails' is earlier than the rest, so the earliest current
#: row (1 February) is not the earliest of the repositories' newest
#: observations (11 February): the two definitions of the span differ.
FEB_EARLY = datetime(2026, 2, 1, 9, 30)
FEB = datetime(2026, 2, 11, 9, 30)
#: A scan of Rails that a later one replaced: history, not current.
JAN = datetime(2026, 1, 20, 9, 30)
#: The dependency graphs, seven months after Syft.
SEP = datetime(2026, 9, 13, 8, 0)
#: A later graph, late in the UTC day: a date made in another zone
#: would be the 15th.
SEP_LATE = datetime(2026, 9, 14, 23, 30)
#: `db index` found no graph (`depgraph_observed_at`'s unset date).
NO_GRAPH = datetime(1970, 1, 2)


def repository(
    id: int,
    owner: str,
    repo: str,
    stars: int,
    language: str,
    ecosystems: list[str],
    commit: str = '',
    graph: datetime = NO_GRAPH,
) -> dict[str, Any]:
    """A `repositories` row, as `db index` writes one."""
    return {
        'id': id, 'owner': owner, 'repo': repo,
        'url': f'https://github.com/{owner}/{repo}', 'stars': stars,
        'description': '', 'created_at': NO_GRAPH, 'language': language,
        'topics': [], 'default_branch': 'main',
        'sbom_ref': 'main' if commit else '', 'sbom_ref_type': 'branch',
        'sbom_commit_sha': commit, 'sbom_commit_sha_short': commit[:7],
        'has_releases': False, 'latest_release_tag': '',
        'latest_release_published_at': NO_GRAPH, 'total_releases': 0,
        'pushed_at': FEB, 'is_archived': False, 'is_fork': False,
        'is_template': False, 'is_mirror': False, 'disk_usage': 0,
        'fork_count': 0, 'watchers_count': 0,
        'license_spdx_id': 'MIT', 'license_name': 'MIT License',
        'manifest_sources': [], 'depgraph_observed_at': graph,
        'depgraph_ref': 'main' if graph != NO_GRAPH else '',
        'depgraph_commit_sha': '', 'github_language': language,
        'ecosystems': ecosystems, 'snapshot': '',
    }


def syft(
    repository_id: int,
    commit: str,
    name: str,
    version: str,
    type: str,
    relationship: str,
    licenses: list[str],
    observed_at: datetime = FEB,
    found_by: str = 'cataloger',
    source: str = 'syft',
    version_kind: str = 'resolved',
    artifact_id: str = '',
) -> dict[str, Any]:
    """An `artifacts` row of a scan at `commit`."""
    return {
        'repository_id': repository_id,
        'artifact_id': artifact_id or f'{repository_id}-{name}-{version}',
        'name': name, 'version': version, 'type': type,
        'purl': f'pkg:{type}/{name}@{version}', 'found_by': found_by,
        'licenses': licenses, 'relationship': relationship,
        'source': source, 'version_kind': version_kind,
        'sbom_ref': 'main', 'sbom_commit_sha': commit,
        'observed_at': observed_at,
    }


def graph(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    relationship: str,
    version_kind: str,
    observed_at: datetime = SEP,
    artifact_id: str = '',
) -> dict[str, Any]:
    """An `artifacts` row of a dependency-graph document: current by the
    instant it states, which its repository records."""
    return syft(
        repository_id, '', name, version, type, relationship, [],
        observed_at=observed_at, found_by='github-dependency-graph',
        source='github-depgraph', version_kind=version_kind,
        artifact_id=artifact_id,
    )


REPOSITORIES_SEED = [
    # Seen by both collectors: every current row of it is not from the
    # same day, and D1 showed the newer date on all of them (#24).
    repository(1, 'rails', 'rails', 58000, 'Ruby', ['gem'], 'r1', SEP),
    # Tied on stars with discourse: an order that stops at the stars
    # cannot say which comes first.
    repository(2, 'mastodon', 'mastodon', 47000, 'Ruby', ['gem'], 'm1'),
    repository(
        3, 'discourse', 'discourse', 47000, 'Ruby', ['gem', 'npm'], 'd1',
        SEP,
    ),
    repository(
        4, 'apache', 'james', 900, 'Java', ['maven'], 'j1',
    ),
    # Composer under both of its spellings: the graph's `composer` here
    # and in firefly and koel, Syft's `php-composer` in monica, firefly
    # and akaunting.
    repository(
        5, 'laravel', 'laravel', 80000, 'PHP', ['composer'], graph=SEP,
    ),
    repository(6, 'monicahq', 'monica', 22000, 'PHP', ['composer'], 'mo1'),
    repository(
        7, 'firefly-iii', 'firefly-iii', 16000, 'PHP', ['composer'], 'f1',
        SEP,
    ),
    repository(
        8, 'koel', 'koel', 16000, 'PHP', ['composer'], graph=SEP_LATE,
    ),
    repository(
        9, 'akaunting', 'akaunting', 9000, 'PHP', ['composer'], 'a1',
    ),
    repository(10, 'psf', 'app', 500, 'Python', ['pypi'], 'p1'),
    # No language on GitHub: the filter's `none`.
    repository(11, 'expressjs', 'site', 100, '', ['npm'], 'e1'),
    # Tracked, never collected: in the denominators, in no row.
    repository(12, 'golang', 'tools', 50, 'Go', []),
]

ARTIFACTS_SEED = [
    # rails: Syft on 1 February, the graph on 13 September, and a scan
    # from January that the February one replaced.
    syft(1, 'r1', 'mail', '2.8.1', 'gem', 'transitive', ['MIT'], FEB_EARLY),
    syft(
        1, 'r1', 'mini_mime', '1.1.5', 'gem', 'transitive', ['MIT'],
        FEB_EARLY,
    ),
    syft(1, 'r0', 'mail', '2.7.0', 'gem', 'transitive', ['MIT'], JAN),
    graph(1, 'mail', '~> 2.8', 'gem', 'direct', 'constraint'),
    # A second constraint string in a second manifest: rails is still
    # one repository with a constraint on `mail`, not two (#120).
    graph(
        1, 'mail', '>= 2.7', 'gem', 'direct', 'constraint',
        artifact_id='1-rails.gemspec',
    ),
    syft(2, 'm1', 'mail', '2.8.1', 'gem', 'direct', ['MIT']),
    syft(2, 'm1', 'mini_mime', '1.1.5', 'gem', 'transitive', ['MIT']),
    # discourse: one version from two cataloguers, and again from the
    # graph. The two Syft rows are one line of the table, the graph's
    # another: they were observed on different days.
    syft(
        3, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemfile-cataloger',
    ),
    syft(
        3, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemspec-cataloger', artifact_id='3-mail-gemspec',
    ),
    graph(3, 'mail', '2.8.1', 'gem', 'direct', 'resolved'),
    graph(3, 'debug', '4.3.4', 'npm', 'transitive', 'resolved'),
    graph(3, 'ms', '2.1.2', 'npm', 'transitive', 'resolved'),
    # `mail` is also a Maven artifact and a PyPI package.
    syft(4, 'j1', 'mail', '1.4.7', 'java-archive', 'direct', ['Apache-2.0']),
    # And a Gradle declaration, for the third source.
    syft(
        4, 'j1', 'jakarta.mail', '2.1.0', 'maven', 'direct', [],
        source='manifest', version_kind='constraint',
    ),
    # The same declaration in two manifests: two rows, one fact.
    graph(
        5, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='5-composer.json',
    ),
    graph(
        5, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='5-packages/app/composer.json',
    ),
    syft(
        6, 'mo1', 'laravel/framework', 'v12.49.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    syft(
        7, 'f1', 'laravel/framework', 'v12.49.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    graph(
        7, 'laravel/framework', '^11.0|^12.0', 'composer', 'direct',
        'constraint',
    ),
    # A third constraint string, so there are more of them than a
    # version spread of two lists.
    graph(
        8, 'laravel/framework', '^10.0', 'composer', 'direct', 'constraint',
        SEP_LATE,
    ),
    # And a fourth, in a second manifest of koel's: four strings, three
    # repositories (#120).
    graph(
        8, 'laravel/framework', '^9.0', 'composer', 'direct', 'constraint',
        SEP_LATE, artifact_id='8-packages/legacy/composer.json',
    ),
    graph(
        8, 'laravel/framework', '', 'composer', 'transitive', 'unversioned',
        SEP_LATE,
    ),
    syft(
        9, 'a1', 'laravel/framework', 'v11.2.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    syft(
        9, 'a1', 'laravel/framework', 'v10.48.0', 'php-composer',
        'transitive', ['MIT'],
    ),
    syft(10, 'p1', 'requests', '2.32.0', 'python', 'direct', ['Apache-2.0']),
    syft(10, 'p1', 'mail', '0.0.1', 'python', 'transitive', []),
    # No manifest said how it arrived: classified is not every record.
    syft(10, 'p1', 'certifi', '2024.2.2', 'python', 'unknown', ['MPL-2.0']),
    # The packages the edges name, so the D1 export can reference them.
    syft(11, 'e1', 'express', '4.19.2', 'npm', 'direct', ['MIT']),
    syft(11, 'e1', 'body-parser', '1.20.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'debug', '2.6.9', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'ms', '2.0.0', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'qs', '6.11.0', 'npm', 'transitive', ['BSD-3-Clause']),
    syft(11, 'e1', 'bytes', '3.1.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'raw-body', '2.5.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'side-channel', '1.0.4', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'send', '0.18.0', 'npm', 'transitive', ['MIT']),
]

#: Ties in the second hop (`debug` at 3 under two parents), an edge back
#: to the root, and a pair naming a package no artifact names, which the
#: D1 export leaves out.
EDGES_SEED = [
    ('express', 'body-parser', 5), ('express', 'debug', 4),
    ('express', 'qs', 3), ('express', 'send', 2),
    ('body-parser', 'debug', 3), ('body-parser', 'qs', 3),
    ('body-parser', 'bytes', 2), ('body-parser', 'raw-body', 2),
    ('debug', 'ms', 4), ('send', 'ms', 3), ('send', 'debug', 3),
    ('qs', 'side-channel', 2), ('raw-body', 'bytes', 2),
    ('bytes', 'body-parser', 1), ('mail-dev', 'mail', 1),
]


def config(database: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=HOST, port=PORT, user=USER, password=PASSWORD, database=database,
    )


def seed(database: str) -> None:
    """The corpus above, with the rollups and the dictionary brought up
    to it, as `db index` and `db edges` leave a database."""
    with IngestionRepository(config(database)) as ingest:
        ingest.ensure_schema()
        # `updated_at` stated rather than defaulted to the insert's time:
        # the export dates a repository with no dependencies by it, and
        # a fixture that changed with the day it was built would be a
        # diff every time.
        columns = [*REPOSITORIES.column_names, 'updated_at']
        ingest.client.insert(
            REPOSITORIES.name,
            [
                [*row, FEB]
                for row in REPOSITORIES.rows(REPOSITORIES_SEED)
            ],
            column_names=columns,
        )
        ingest.insert_batch(
            ARTIFACTS.name, ARTIFACTS.rows(ARTIFACTS_SEED),
            ARTIFACTS.column_names,
        )
        ingest.insert_batch(
            EDGES.name,
            EDGES.rows([
                {
                    'parent': parent, 'child': child,
                    'repositories': count, 'observed_at': SEP,
                }
                for parent, child, count in EDGES_SEED
            ]),
            EDGES.column_names,
        )
        ingest.reload_dictionaries()
        ingest.refresh_rollups()


def write_d1(database: str) -> None:
    """The D1 export of the seeded database, as one script.

    In the order the files are applied, each under a line naming it.
    Trailing blanks are dropped, as the repository's hooks would drop
    them, so the file is the export's byte for byte otherwise.
    """
    with (
        tempfile.TemporaryDirectory() as scratch,
        QueryRepository(config(database)) as query,
    ):
        result = export_d1(query, Path(scratch) / 'd1')
        parts = [
            '-- The D1 export of the contract fixture, generated by\n'
            '-- web/test/fixtures/contract/build.py. Do not edit.\n',
        ]
        for name in sorted(result.files):
            text = (result.directory / name).read_text(encoding='utf-8')
            body = '\n'.join(line.rstrip() for line in text.splitlines())
            parts.append(f'\n-- ==== {name} ====\n{body}\n')
    D1_SQL.write_text(''.join(parts), encoding='utf-8')


def run_suite(database: str) -> int:
    """The contract suite against the seeded server, recording its
    answers into `clickhouse.json`, and D1's, from the `d1.sql` just
    written, into `calls.json`."""
    environment = {
        **os.environ,
        'CLICKHOUSE_TEST_URL': f'http://{HOST}:{PORT}',
        'CLICKHOUSE_TEST_DATABASE': database,
        'CLICKHOUSE_TEST_USER': USER,
        'CLICKHOUSE_TEST_PASSWORD': PASSWORD,
        'CLICKHOUSE_TEST_RECORD': str(RECORDED),
        'CONTRACT_RECORD_CALLS': '1',
    }
    status = subprocess.run(
        ['npx', 'vitest', 'run', 'test/contract.test.ts'],
        cwd=WEB, env=environment, check=False,
    ).returncode
    if RECORDED.exists():
        # As the repository's JSON hook formats it, so the hook has
        # nothing to change in a file this wrote.
        recorded = json.loads(RECORDED.read_text(encoding='utf-8'))
        RECORDED.write_text(
            json.dumps(recorded, indent=4, sort_keys=True) + '\n',
            encoding='utf-8',
        )
    return status


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(__doc__ or '').split('\n')[0],
    )
    parser.add_argument(
        '--keep', action='store_true',
        help='leave the seeded database rather than dropping it',
    )
    parser.add_argument(
        '--no-test', action='store_true',
        help='write d1.sql only; do not run the suite or record',
    )
    args = parser.parse_args()

    import clickhouse_connect

    database = f'chatsbom_contract_{uuid.uuid4().hex[:12]}'
    admin = clickhouse_connect.get_client(
        host=HOST, port=PORT, username=USER, password=PASSWORD,
        database='default',
    )
    admin.command(f'CREATE DATABASE {database}')
    try:
        seed(database)
        write_d1(database)
        status = 0 if args.no_test else run_suite(database)
    finally:
        if args.keep:
            print(f'kept {database}', file=sys.stderr)
        else:
            admin.command(f'DROP DATABASE IF EXISTS {database}')
        admin.close()
    return status


if __name__ == '__main__':
    raise SystemExit(main())
