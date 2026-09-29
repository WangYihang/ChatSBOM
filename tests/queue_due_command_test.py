"""`chatsbom queue due`: the due set derived from the store, beside the
ledger's (#100).

It runs on the collection host while the collector writes the same
ledger, and the collector is redeployed under it: so it reads and never
writes, not the ledger, whose bytes, times and WAL it leaves as they
were, and not beside it. Its report is on stdout and whatever else it
has to say on stderr (#114).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import PathConfig
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from tests.due_test import _collected
from tests.due_test import _fetch
from tests.due_test import _record
from tests.due_test import _sbom
from tests.due_test import _tracked
from tests.sbom_generate_test import syft_document
from tests.sbom_generate_test import SYFT_VERSION

runner = CliRunner()

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
SNAPSHOT = 'all-2026-09-27'


@pytest.fixture
def workdir(tmp_path, monkeypatch) -> Path:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setattr('chatsbom.commands.queue.due._now', lambda: NOW)
    return tmp_path


@pytest.fixture
def paths(workdir) -> PathConfig:
    return PathConfig(base_data_dir=workdir / 'data')


def _snapshot(paths: PathConfig, *ids: int, name: str = SNAPSHOT) -> Path:
    path = paths.search_dir / f'{name}.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(
            json.dumps({
                'id': i, 'owner': 'o', 'repo': f'r{i}', 'stars': 1000 + i,
                'language': 'Go', 'default_branch': 'main',
                'pushed_at': '2026-09-20T00:00:00Z',
            }) + '\n'
            for i in ids
        ),
        encoding='utf-8',
    )
    return path


def _corpus(paths: PathConfig) -> None:
    """Five repositories, one for each way they come to differ:

    1. collected, its graph kept: nothing due;
    2. tracked and never collected: release due, the rest waiting;
    3. collected, its SBOM another Syft's: due for the SBOM alone;
    4. tracked, but only an older snapshot lists it;
    5. only the snapshot lists it.
    """
    _snapshot(paths, 1, 2, 3, 5)
    with Ledger(paths.ledger_path) as ledger:
        _collected(ledger, paths, 1)
        _record(
            ledger, 1, Stage.DEPGRAPH, '', 'x', stage_version=2,
            next_attempt_at=NOW + timedelta(days=20),
        )
        _fetch(paths, 1, NOW - timedelta(days=10))
        _tracked(ledger, 2, snapshot=SNAPSHOT)
        _collected(ledger, paths, 3)
        _sbom(paths, 3, syft_document(version='1.41.2'))
        _tracked(ledger, 4, snapshot='all-2026-09-20')


def due(*arguments: str) -> Any:
    return runner.invoke(
        app, ['queue', 'due', '--syft-version', SYFT_VERSION, *arguments],
    )


def said(text: str) -> str:
    """`text` on one line: Rich wraps a long one."""
    return ' '.join(text.split())


# --- the report -------------------------------------------------------------

def test_it_reports_where_each_stage_stands(workdir, paths):
    _corpus(paths)

    result = due()

    assert result.exit_code == 0, result.output
    out = said(result.stdout)
    assert f'universe {SNAPSHOT}' in out and '4 repositories' in out
    # The derived states, by stage.
    table: dict[str, list[str]] = {}
    for line in result.stdout.splitlines():
        cells = line.split()
        if cells and cells[0] in {str(stage) for stage in Stage}:
            table.setdefault(cells[0], cells[1:])
    #         due waiting blocked deferred present, of which the ledger's
    assert table['release'] == ['2', '0', '0', '0', '2', '2']
    assert table['commit'] == ['0', '2', '0', '0', '2', '2']
    assert table['content'] == ['0', '2', '0', '0', '2', '2']
    assert table['sbom'] == ['1', '2', '0', '0', '1', '0']
    assert table['depgraph'] == ['3', '0', '0', '0', '1', '0']
    assert 'another-syft 1 (3)' in out
    assert 'never-run 2 (2, 5)' in out
    assert 'read from the ledger' in out
    assert 'Elapsed' in out
    assert 'Compared with the ledger' not in out
    assert result.stderr == ''


def test_compare_says_why_the_ledger_differs(workdir, paths):
    _corpus(paths)

    result = due('--compare')

    assert result.exit_code == 0, result.output
    out = said(result.stdout)
    assert 'Compared with the ledger' in out
    for expected in (
        'derived universe:snapshot-only 1 (5)',
        'ledger universe:ledger-only:older-snapshot 1 (4)',
        'ledger upstream-not-run 1 (2)',
        'derived another-syft 1 (3)',
    ):
        assert expected in out, expected
    assert 'Second look' in out
    assert 'Elapsed' in out


def test_the_json_report(workdir, paths):
    _corpus(paths)

    result = due('--compare', '--json', 'report.json', '--samples', '1')

    assert result.exit_code == 0, result.output
    report = json.loads(Path('report.json').read_text(encoding='utf-8'))
    assert report['universe'] == {
        'source': SNAPSHOT, 'repositories': 4, 'unusable': 0,
    }
    assert report['ledger']['tracked'] == 4
    assert report['shard'] is None
    assert report['syft_version'] == SYFT_VERSION
    sbom = report['stages']['sbom']
    assert sbom['derived']['due'] == {
        'count': 1, 'why': {'another-syft': {'count': 1, 'samples': [3]}},
    }
    assert sbom['compared']['derived-only'] == {
        'another-syft': {'count': 1, 'samples': [3]},
    }
    # Present on the ledger's word: release and commit, and content its
    # row vouches for; not the tree or the SBOM, read from the store.
    assert {
        stage: entry['ledger_backed']
        for stage, entry in report['stages'].items()
    } == {
        'release': 2, 'commit': 2, 'tree': 0, 'content': 2, 'sbom': 0,
        'depgraph': 0,
    }
    release = report['stages']['release']['compared']
    assert release['ledger-only'] == {
        'universe:ledger-only:older-snapshot': {'count': 1, 'samples': [4]},
    }
    assert report['stages']['commit']['compared']['ledger-only'] == {
        'upstream-not-run': {'count': 1, 'samples': [2]},
        'universe:ledger-only:older-snapshot': {'count': 1, 'samples': [4]},
    }
    # Repository 4 differs on every stage, 2 on four, 5 on two, 3 on one.
    assert report['second_look'] == {'rechecked': 13, 'converged': 0}
    assert set(report['elapsed_seconds']) >= {'ledger', 'store', 'total'}
    assert report['inventory'] is None


def test_one_stage_alone(workdir, paths):
    _corpus(paths)

    result = due('--stage', 'tree', '--json', 'report.json')

    assert result.exit_code == 0, result.output
    assert list(json.loads(Path('report.json').read_text())['stages']) == [
        'tree',
    ]


def test_a_shard_is_its_ids_alone(workdir, paths):
    _corpus(paths)

    result = due('--compare', '--shard', '1/2', '--json', 'report.json')

    assert result.exit_code == 0, result.output
    report = json.loads(Path('report.json').read_text())
    assert report['shard'] == '1/2'
    assert report['universe']['repositories'] == 3, 'ids 1, 3 and 5'
    assert report['ledger']['tracked'] == 2, 'ids 1 and 3'
    assert report['stages']['release']['compared']['ledger-only'] == {}


@pytest.mark.parametrize('shard', ['2/2', '1', 'a/b', '0/0', '-1/2'])
def test_a_shard_that_is_none_is_refused(workdir, paths, shard):
    _corpus(paths)

    result = due('--shard', shard)

    assert result.exit_code == 2
    assert result.stdout == ''


@pytest.mark.parametrize('stage', ['lock', 'repo', 'nothing'])
def test_a_stage_it_does_not_derive_is_refused(workdir, paths, stage):
    _corpus(paths)

    result = due('--stage', stage)

    assert result.exit_code == 2
    assert result.stdout == ''


def test_the_ledgers_own_set_as_the_universe(workdir, paths):
    _corpus(paths)

    result = due('--compare', '--universe', 'ledger', '--json', 'report.json')

    assert result.exit_code == 0, result.output
    report = json.loads(Path('report.json').read_text())
    assert report['universe']['source'] == 'ledger'
    assert report['universe']['repositories'] == 4
    codes = {
        code
        for stage in report['stages'].values()
        for side in ('derived-only', 'ledger-only')
        for code in stage['compared'][side]
    }
    assert not any(code.startswith('universe') for code in codes)


def test_the_newest_complete_snapshot_is_the_universe(workdir, paths):
    """Today's may still be being written."""
    _corpus(paths)
    _snapshot(paths, 1, name='all-2026-09-28')

    result = due('--json', 'report.json')

    assert result.exit_code == 0, result.output
    assert json.loads(Path('report.json').read_text())['universe'][
        'source'
    ] == SNAPSHOT


def test_with_no_complete_snapshot_it_says_so_and_fails(workdir, paths):
    _snapshot(paths, 1, name='all-2026-09-28')

    result = due()

    assert result.exit_code == 1
    assert result.stdout == ''
    assert '--universe ledger' in said(result.stderr)


def test_unusable_snapshot_lines_are_said_on_stderr(workdir, paths):
    _corpus(paths)
    with (paths.search_dir / f'{SNAPSHOT}.jsonl').open('a') as handle:
        handle.write('not json\n')

    result = due()

    assert result.exit_code == 0, result.output
    assert '1 line' in said(result.stderr)
    assert 'not json' not in result.stdout


def test_with_json_logs_a_diagnostic_is_one_event(
    workdir, paths, json_logs,
):
    """A machine reads stderr then (#114)."""
    _corpus(paths)
    with (paths.search_dir / f'{SNAPSHOT}.jsonl').open('a') as handle:
        handle.write('not json\n')

    result = due()

    assert result.exit_code == 0, result.output
    [event] = [json.loads(line) for line in result.stderr.splitlines()]
    assert event['event'] == 'Snapshot lines left out'
    assert event['lines'] == 1


def test_without_a_ledger_every_release_is_due(workdir, paths):
    """Release is read from the ledger until it has records of its own:
    with none, nothing was ever released, and it says so."""
    _snapshot(paths, 1, 2)

    result = due('--json', 'report.json')

    assert result.exit_code == 0, result.output
    assert 'No ledger' in said(result.stderr)
    report = json.loads(Path('report.json').read_text())
    assert report['ledger']['tracked'] is None
    assert report['stages']['release']['derived']['due']['count'] == 2
    assert not Path('data/ledger.sqlite3').exists()


@pytest.mark.parametrize('arguments', [['--compare'], ['--universe', 'ledger']])
def test_without_a_ledger_there_is_nothing_to_compare(
    workdir, paths, arguments,
):
    _snapshot(paths, 1, 2)

    result = due(*arguments)

    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'No ledger' in said(result.stderr)
    assert not Path('data/ledger.sqlite3').exists()


def test_the_syft_in_force_is_asked_when_not_named(
    workdir, paths, monkeypatch,
):
    _corpus(paths)
    monkeypatch.setattr(
        'chatsbom.commands.queue.due.get_syft_version', lambda: None,
    )

    result = runner.invoke(
        app, ['queue', 'due', '--stage', 'sbom', '--json', 'report.json'],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(Path('report.json').read_text())['syft_version'] is None
    assert '--syft-version' in said(result.stderr)


def test_the_inventory_names_what_nothing_points_to(workdir, paths):
    _corpus(paths)
    stray = paths.tree_file(1, 'c' * 40)
    stray.parent.mkdir(parents=True)
    stray.write_text('README.md\n')
    outside = paths.tree_file(9, 'd' * 40)
    outside.parent.mkdir(parents=True)
    outside.write_text('README.md\n')

    result = due('--inventory', '--json', 'report.json')

    assert result.exit_code == 0, result.output
    trees = {
        entry['root']: entry
        for entry in json.loads(Path('report.json').read_text())['inventory']
    }['05-github-tree']
    assert trees['count'] == 4
    assert trees['pointed'] == 2
    assert trees['superseded'] == {'count': 1, 'samples': [f'1/{"c" * 40}']}
    assert trees['outside'] == {'count': 1, 'samples': [f'9/{"d" * 40}']}
    assert 'Nothing points to' in said(result.stdout)


def test_the_readme_explains_every_reason():
    """Each code the report can give, in the README's table of them."""
    from chatsbom.core.due import REASONS

    readme = (Path(__file__).parent.parent / 'README.md').read_text(
        encoding='utf-8',
    )
    rows = {
        line.split('|')[1].strip().strip('`')
        for line in readme.splitlines()
        if line.startswith('| `') and line.count('|') == 3
    }

    assert set(REASONS) <= rows, set(REASONS) - rows


# --- it writes nothing ------------------------------------------------------

def _ledger_files(paths: PathConfig) -> dict[str, tuple[int, int, bytes]]:
    return {
        path.name: (
            path.stat().st_size, path.stat().st_mtime_ns, path.read_bytes(),
        )
        for path in sorted(paths.base_data_dir.iterdir())
        if path.name.startswith('ledger.') and not path.name.endswith('-shm')
    }


def test_the_ledger_is_left_as_it_was(workdir, paths):
    _corpus(paths)
    before = _ledger_files(paths)
    assert set(before) == {'ledger.sqlite3'}

    result = due('--compare', '--rediscover', '--inventory')

    assert result.exit_code == 0, result.output
    assert _ledger_files(paths) == before


#: A worker holding the ledger open, its last commit still in the WAL.
_WORKER = """
import sqlite3, sys
db = sqlite3.connect(sys.argv[1], isolation_level=None)
db.execute('PRAGMA wal_autocheckpoint=0')
db.execute("UPDATE repository_state SET stars = 1 WHERE repository_id = 1")
print('written', flush=True)
sys.stdin.read()
db.close()
"""


def test_a_ledger_in_use_is_left_as_it_was(workdir, paths):
    _corpus(paths)
    worker = subprocess.Popen(
        [sys.executable, '-c', _WORKER, str(paths.ledger_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    try:
        assert worker.stdout is not None
        assert worker.stdout.readline().strip() == 'written'
        before = _ledger_files(paths)
        assert set(before) == {'ledger.sqlite3', 'ledger.sqlite3-wal'}

        result = due('--compare')

        after = _ledger_files(paths)
    finally:
        assert worker.stdin is not None
        worker.stdin.close()
        worker.wait(timeout=30)
    assert result.exit_code == 0, result.output
    assert after == before


@pytest.mark.skipif(
    os.name != 'posix', reason='directory permissions are POSIX',
)
def test_it_reads_a_ledger_it_may_not_write_beside(workdir, paths):
    """The collector's data directory, read by another user. (As root the
    permissions stop nothing; that nothing appears is checked instead.)"""
    _corpus(paths)
    data = paths.base_data_dir
    before = sorted(path.name for path in data.iterdir())
    data.chmod(0o555)
    try:
        result = due('--compare', '--json', str(workdir / 'report.json'))
    finally:
        data.chmod(0o755)
    assert result.exit_code == 0, result.output
    assert sorted(path.name for path in data.iterdir()) == before
