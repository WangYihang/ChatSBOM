"""`github content` and `run` end to end over the discovered manifests.

A repository labelled TypeScript with a Gradle backend (Stirling-PDF,
appsmith), and one a search snapshot seeded with no language at all:
both get every manifest their tree lists, and Syft is pointed at a
content root holding them at their own paths (#51).

Only the network (git, the raw host) and Syft are faked; the commands,
ledger, services and paths are real, under a fresh working directory.
"""
from __future__ import annotations

import json
import os
import subprocess
import time
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands import run as run_command
from chatsbom.commands.github import content as content_command
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import STAGE_VERSION
from chatsbom.services import sbom_service
from chatsbom.services.content_service import ContentService
from tests.content_test import FakeRaw
from tests.content_test import SHA
from tests.sbom_generate_test import syft_document

runner = CliRunner()

TREE = [
    'README.md',
    'package.json',
    'package-lock.json',
    'app/common/build.gradle',
    'app/core/build.gradle',
    'settings.gradle',
    'build.gradle',
    'frontend/node_modules/x/package.json',
    'testing/e2e/package.json',
]

RAW = {
    'package.json': b'{"name": "root"}',
    'package-lock.json': b'{"lockfileVersion": 3}',
    'app/common/build.gradle': (
        b'dependencies { implementation '
        b"'org.springframework.boot:spring-boot-starter-web' }"
    ),
    'app/core/build.gradle': b'dependencies {}',
    'settings.gradle': b"include 'app:common'",
    'build.gradle': b'plugins {}',
}


class FakeRelease:
    def process_repo(self, repository: Any, stats: Any, language: str):
        return None


class FakeCommit:
    def process_repo(self, repository: Any, stats: Any, language: str):
        return {
            'download_target': {
                'ref': 'main', 'ref_type': 'branch',
                'commit_sha': SHA, 'commit_sha_short': SHA[:7],
            },
        }


class FakeGit:
    def get_repository_tree(self, owner, repo, sha, cache_path=None):
        return list(TREE)


class FakeSyft:
    """Records every file of each tree Syft is pointed at."""

    def __init__(self) -> None:
        self.scans: list[list[str]] = []

    def __call__(self, command: list[str], **kwargs: Any):
        tree = Path(command[1].removeprefix('dir:'))
        self.scans.append(
            sorted(
                p.relative_to(tree).as_posix()
                for p in tree.rglob('*') if p.is_file()
            ),
        )
        return subprocess.CompletedProcess(
            command, 0, stdout=syft_document(), stderr='',
        )


@pytest.fixture
def world(tmp_path, monkeypatch, no_database):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    for module in (content_command, run_command):
        monkeypatch.setattr(
            module, 'verify_github_token', lambda *a, **k: 'octocat',
        )
    raw = FakeRaw(RAW)
    service = ContentService('t')
    service.session = raw
    monkeypatch.setattr(
        Container, 'get_content_service', lambda self, token=None: service,
    )
    monkeypatch.setattr(
        Container, 'get_release_service', lambda self, token=None: FakeRelease(),
    )
    monkeypatch.setattr(
        Container, 'get_commit_service', lambda self, token=None: FakeCommit(),
    )
    monkeypatch.setattr(
        Container, 'get_git_service', lambda self, token=None: FakeGit(),
    )
    syft = FakeSyft()
    monkeypatch.setattr(sbom_service, 'check_syft_installed', lambda: True)
    monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: '1.52.0')
    monkeypatch.setattr(sbom_service.subprocess, 'run', syft)

    Path('data').mkdir()
    pushed = datetime(2026, 9, 1, tzinfo=timezone.utc)
    seen = datetime(2026, 9, 2, tzinfo=timezone.utc)
    with Ledger(Path('data/ledger.sqlite3')) as ledger:
        ledger.track(7, 'Stirling-Tools', 'Stirling-PDF', 'typescript')
        ledger.seed(
            8, 'o', 'unlabelled', snapshot='all',
            github_language='Kotlin',
        )
        for repository_id in (7, 8):
            ledger.record_push(repository_id, pushed, seen)
    return {'raw': raw, 'syft': syft}


EXPECTED = sorted(set(RAW))


def _stored(repository_id: int) -> list[str]:
    root = Path(f'data/06-github-content/{repository_id}/{SHA}')
    return sorted(
        p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file()
    )


def test_github_content_fetches_every_ecosystem_at_every_depth(world):
    result = runner.invoke(
        app, ['github', 'content', '--token', 't'],
    )
    assert result.exit_code == 0, result.output

    for repository_id in (7, 8):
        assert _stored(repository_id) == EXPECTED, repository_id
        document = json.loads(
            Path(
                f'data/05-github-tree/{repository_id}/{SHA}/manifests.json',
            ).read_text(),
        )
        assert document['ecosystems'] == ['maven', 'npm']
        assert document['skipped_by_reason'] == {'excluded-dir': 2}

    with Ledger(Path('data/ledger.sqlite3')) as ledger:
        state = ledger.stage_state(7, Stage.CONTENT)
        assert state is not None
        assert state.stage_version == STAGE_VERSION[Stage.CONTENT]
        assert len(state.output_key) == 64, 'the content digest'
        # Nothing else was recorded: the stage ran alone.
        assert ledger.stage_state(7, Stage.SBOM) is None


def test_run_scans_what_was_discovered(world):
    """The full walk: Syft is pointed at the Gradle backend as well as
    the npm root, for a TypeScript-labelled repository and for one with
    no language."""
    result = runner.invoke(app, ['run', '--token', 't', '--no-depgraph'])
    assert result.exit_code == 0, result.output

    assert world['syft'].scans == [EXPECTED, EXPECTED]
    for repository_id in (7, 8):
        assert Path(f'data/07-sbom/{repository_id}/{SHA}/sbom.json').is_file()


def test_run_writes_to_the_store_alone(world):
    """It kept each finished record in ClickHouse's `raw_documents` too,
    so a walk to the end of the chain needed the database, which the
    warehouse never read (#153). What it collects is in `data/` alone,
    beside Syft's cache: `world` has no database to give it."""
    result = runner.invoke(app, ['run', '--token', 't', '--no-depgraph'])

    assert result.exit_code == 0, result.output
    assert 'recorded' not in result.output
    assert sorted(path.name for path in Path('.').iterdir()) == [
        '.cache', 'data',
    ]
    assert sorted(path.name for path in Path('.cache').iterdir()) == ['syft']


@pytest.mark.parametrize(
    'written_by,regenerated',
    [('1.41.2', True), ('1.52.0', False)],
    ids=['another-syft', 'this-syft'],
)
def test_run_regenerates_an_sbom_another_syft_wrote(
    world, monkeypatch, written_by, regenerated,
):
    """`chatsbom run` asks `process_repo` of each repository it walks, and
    an SBOM another Syft wrote is regenerated there, though its times
    alone would keep it; one this Syft wrote is not. Both repositories
    are walked again: their release stage records nothing, so it stays
    due. Regenerated from the cache here, which the first run filled for
    this Syft: after a real upgrade it holds nothing yet, and Syft
    runs."""
    def run() -> str:
        result = runner.invoke(app, ['run', '--token', 't', '--no-depgraph'])
        assert result.exit_code == 0, result.output
        return result.output

    run()
    assert len(world['syft'].scans) == 2
    stored = Path(f'data/07-sbom/7/{SHA}/sbom.json')
    stored.write_text(
        syft_document('planted', version=written_by), encoding='utf-8',
    )
    # Newer than anything the next walk writes, so that only the version
    # can make either SBOM stale.
    later = time.time() + 3600
    for sbom in Path('data/07-sbom').glob(f'*/{SHA}/sbom.json'):
        os.utime(sbom, (later, later))

    output = run()

    now = json.loads(stored.read_text(encoding='utf-8'))
    assert now['source']['name'] == ('a' if regenerated else 'planted'), output
    assert now['descriptor']['version'] == '1.52.0'
