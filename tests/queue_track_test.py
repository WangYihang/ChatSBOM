"""`queue track --snapshot`: seeding the queue from a search snapshot.

The per-language lists hold 34,621 repositories where the unfiltered
search holds 60,017 (#51). The dependency graph needs only
`owner/repo`, and its endpoint closes after 2026-11-13, so the
repositories no language list has must be in the queue before the rest
of the redesign lands. And `data prune`, which deletes older scans,
must never reach a graph: once the endpoint closes, none can be fetched
again.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.queue.track import snapshot_name
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger

runner = CliRunner()

LEDGER = Path('data/ledger.sqlite3')


@pytest.fixture
def workdir(tmp_path, monkeypatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    return tmp_path


def _snapshot(path: Path, *records: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(json.dumps(record) + '\n' for record in records)
        + 'not json\n',
    )
    return path


def _row(repository_id: int) -> dict:
    with Ledger(LEDGER) as ledger:
        row = ledger._db.execute(
            'SELECT * FROM repository_state WHERE repository_id = ?',
            (repository_id,),
        ).fetchone()
        return dict(row) if row else {}


def test_a_snapshot_seeds_every_repository_whatever_its_language(workdir):
    snapshot = _snapshot(
        Path('data/01-github-search/all-2026-10-01.jsonl'),
        {
            'id': 1, 'owner': 'halo-dev', 'repo': 'halo', 'stars': 35000,
            'language': 'Java', 'default_branch': 'main',
        },
        {
            'id': 2, 'owner': 'o', 'repo': 'cpp', 'stars': 1200,
            'language': 'C++', 'default_branch': 'master',
        },
        {'id': 3, 'owner': 'o', 'name': 'none', 'language': None},
    )

    result = runner.invoke(
        app, ['queue', 'track', '--snapshot', str(snapshot)],
    )

    assert result.exit_code == 0, result.output
    assert '+3' in result.output
    assert '1 unusable' in ' '.join(result.output.split())
    halo = _row(1)
    assert halo['snapshot'] == 'all-2026-10-01'
    assert halo['github_language'] == 'Java'
    assert halo['stars'] == 35000
    assert halo['default_branch'] == 'main'
    assert halo['language'] == '', 'no language-keyed list'
    assert _row(3)['repo'] == 'none'


def test_seeding_again_changes_nothing_but_the_attributes(workdir):
    snapshot = _snapshot(
        Path('data/01-github-search/all.jsonl'),
        {'id': 1, 'owner': 'o', 'repo': 'r', 'stars': 10},
    )
    with Ledger(LEDGER) as ledger:
        ledger.track(1, 'o', 'r', 'java')

    runner.invoke(app, ['queue', 'track', '--snapshot', str(snapshot)])
    result = runner.invoke(
        app, ['queue', 'track', '--snapshot', str(snapshot)],
    )

    assert result.exit_code == 0, result.output
    assert '+0' in result.output
    assert _row(1)['language'] == 'java'
    assert _row(1)['stars'] == 10


def test_an_undated_snapshot_is_named_for_the_day_it_was_written(tmp_path):
    path = tmp_path / 'all.jsonl'
    path.write_text('')
    written = datetime(2026, 3, 9, 12, tzinfo=timezone.utc).timestamp()
    os.utime(path, (written, written))

    assert snapshot_name(path) == 'all-2026-03-09'
    assert snapshot_name(tmp_path / 'all-2026-10-01.jsonl') == 'all-2026-10-01'


def test_prune_never_reaches_the_dependency_graph(workdir):
    """Five levels down, where prune finds scans, a graph would look
    like one; the stage is simply not in its list."""
    old = Path('data/09-github-depgraph/java/o/r/HEAD/old')
    new = Path('data/09-github-depgraph/java/o/r/HEAD/new')
    for directory in (old, new):
        directory.mkdir(parents=True)
        (directory / 'sbom.spdx.json').write_text('{}')

    result = runner.invoke(app, ['data', 'prune', '--keep', '1', '--apply'])

    assert result.exit_code == 0, result.output
    assert old.exists() and new.exists()
    assert '09-github-depgraph' not in result.output
