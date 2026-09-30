"""`chatsbom warehouse build`: one writer, the file held a pass.

The command builds the warehouse from the store beside it (#131). What
it built is its output, on stdout; what else it has to say, on stderr,
where the logs go (#114). A second pass while one runs is refused, a
reader of the last warehouse is never in the way, and a pass that
fails leaves the last warehouse as it was.
"""
from __future__ import annotations

import fcntl
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import Store

runner = CliRunner()

APP = Listed(1, 'acme', 'app', language='Ruby')


@pytest.fixture
def here(
    tmp_path: Path, store: Store, monkeypatch: pytest.MonkeyPatch,
) -> Store:
    """A store in the working directory, as `data/`, with one scanned
    repository in its snapshot; and no server anywhere to reach."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    store.seed(store.snapshot(date(2026, 9, 1), APP), APP)
    store.sbom(
        1, 'a' * 40, artifact('rack', '3.1.0', 'gem', licenses=['MIT']),
        at=at(2026, 9, 14),
    )
    store.record(1, 'acme', 'app', commit='a' * 40)
    return store


def warehouse_rows(path: Path, sql: str) -> list[tuple[object, ...]]:
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute(sql).fetchall()


def test_it_builds_the_warehouse_beside_the_store(here: Store) -> None:
    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 0, result.output
    built = Path('data') / 'warehouse.duckdb'
    assert built.resolve() == here.paths.warehouse_path.resolve()
    assert warehouse_rows(built, 'SELECT name FROM facts') == [('rack',)]
    # Only the warehouse: the pass's own file is renamed into place.
    assert not built.with_name('warehouse.duckdb.building').exists()


def test_what_it_built_is_its_output(here: Store) -> None:
    """On stdout: the corpus, what was read, and each rollup. The
    progress bar is on stderr, with the logs."""
    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 0, result.output
    for said in ('all-2026-09-01', 'mv_package_month_intervals', 'mv_totals'):
        assert said in result.stdout
        assert said not in result.stderr
    assert 'Reading the store' in result.stderr


def test_it_writes_where_it_is_told(here: Store, tmp_path: Path) -> None:
    elsewhere = tmp_path / 'warehouse' / 'w.duckdb'
    result = runner.invoke(
        app, ['warehouse', 'build', '--output', str(elsewhere)],
    )
    assert result.exit_code == 0, result.output
    assert warehouse_rows(elsewhere, 'SELECT count(*) FROM corpus') == [(1,)]


def test_a_second_pass_while_one_runs_is_refused(here: Store) -> None:
    """Said on stderr, status 1, and the warehouse is left alone."""
    lock = Path('data') / 'warehouse.duckdb.lock'
    with lock.open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'another pass' in ' '.join(result.stderr.split()).lower()
    assert not (Path('data') / 'warehouse.duckdb').exists()


def test_a_refusal_is_one_event_when_logs_are_json(
    here: Store, json_logs: None,
) -> None:
    lock = Path('data') / 'warehouse.duckdb.lock'
    with lock.open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 1
    events = [json.loads(line) for line in result.stderr.splitlines()]
    assert [event['level'] for event in events] == ['error']
    assert events[0]['lock'].endswith('warehouse.duckdb.lock')


def test_a_reader_of_the_last_warehouse_is_not_in_the_way(
    here: Store,
) -> None:
    """The operator's DuckDB CLI holds the file open: the pass writes a
    file of its own and renames it over, and the reader goes on reading
    the warehouse it opened."""
    assert runner.invoke(app, ['warehouse', 'build']).exit_code == 0
    built = here.paths.warehouse_path
    here.sbom(
        1, 'b' * 40, artifact('rails', '8.0.0', 'gem'), at=at(2026, 9, 20),
    )
    with duckdb.connect(str(built), read_only=True) as reader:
        result = runner.invoke(app, ['warehouse', 'build'])
        assert result.exit_code == 0, result.output
        assert reader.execute('SELECT name FROM facts').fetchall() == [
            ('rack',),
        ]
    assert warehouse_rows(built, 'SELECT name FROM facts') == [('rails',)]


def test_a_pass_that_fails_leaves_the_last_warehouse(
    here: Store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert runner.invoke(app, ['warehouse', 'build']).exit_code == 0
    built = here.paths.warehouse_path
    before = built.read_bytes()

    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError('the derivation broke')

    monkeypatch.setattr('chatsbom.warehouse.build.derive', broken)
    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code != 0
    assert built.read_bytes() == before
    # What it had written is gone with it, not left for the next pass.
    assert sorted(p.name for p in built.parent.glob('warehouse*')) == [
        'warehouse.duckdb', 'warehouse.duckdb.lock',
    ]
    # And the next pass starts clean from what that one left.
    monkeypatch.undo()
    monkeypatch.chdir(here.root.parent)
    assert runner.invoke(app, ['warehouse', 'build']).exit_code == 0


def test_a_pass_clears_what_a_killed_pass_spilled(here: Store) -> None:
    """DuckDB spills a pass beside the file it writes, in a directory of
    the pass's own (`chatsbom.warehouse.spill`), and removes it when the
    pass closes the file. A pass that was killed leaves it, and only a
    pass writes that file, one at a time: the next removes it."""
    data = here.root
    killed = data / 'warehouse.duckdb.building.tmp-0123456789abcdef'
    killed.mkdir(parents=True)
    (killed / 'duckdb_temp_storage_DEFAULT-0.tmp').write_bytes(bytes(4096))

    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 0, result.output
    assert not killed.exists()


def spilled_by_a_reader(data: Path) -> Path:
    """What a reader of the warehouse that was killed mid-spill leaves
    beside it: `snapshot build` or the export, stopped by the loop's
    stop or by the container's memory limit."""
    left = data / 'warehouse.duckdb.tmp-fedcba9876543210'
    left.mkdir()
    (left / 'duckdb_temp_storage_DEFAULT-0.tmp').write_bytes(bytes(4096))
    return left


def test_a_pass_clears_what_killed_readers_spilled(here: Store) -> None:
    """A reader spills into a directory of its own beside the warehouse,
    which DuckDB removes when the reader closes the file; one killed
    first leaves it, up to the size of what it sorted, every time it is
    killed. A reader spills only while it has the file open, and DuckDB
    locks the file for as long as a process has it open: so while no
    process does, what readers left is nobody's, and a pass removes it,
    as it removes what a killed pass left (#150)."""
    assert runner.invoke(app, ['warehouse', 'build']).exit_code == 0
    left = spilled_by_a_reader(here.root)
    lock = here.root / 'warehouse.duckdb.tmp-fedcba9876543210.lock'
    lock.write_text('not a spill directory')

    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 0, result.output
    assert not left.exists()
    assert lock.read_text() == 'not a spill directory'


#: A reader of the warehouse in a process of its own, as `snapshot build`
#: or the export is beside the loop's pass: it has the file open until
#: its stdin closes.
READER = """
import sys
import duckdb
with duckdb.connect(sys.argv[1], read_only=True) as con:
    con.execute('SELECT count(*) FROM facts').fetchall()
    print('open', flush=True)
    sys.stdin.read()
"""


def test_a_readers_spill_is_left_while_any_reader_has_it_open(
    here: Store,
) -> None:
    """Whose a directory is, the pass cannot tell; that a process has the
    warehouse open, DuckDB's lock says, and one that has may be spilling
    into any of them."""
    assert runner.invoke(app, ['warehouse', 'build']).exit_code == 0
    left = spilled_by_a_reader(here.root)
    with subprocess.Popen(
        [sys.executable, '-c', READER, str(here.paths.warehouse_path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    ) as reader:
        assert reader.stdin is not None and reader.stdout is not None
        assert reader.stdout.readline() == 'open\n'

        result = runner.invoke(app, ['warehouse', 'build'])

        reader.stdin.close()
        assert reader.wait() == 0
    assert result.exit_code == 0, result.output
    assert left.is_dir()


def test_without_a_store_it_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'no store' in ' '.join(result.stderr.split()).lower()


def broken(here: Store, monkeypatch: pytest.MonkeyPatch) -> None:
    """A pass stopped by what the command does not catch."""
    def refuse(*args: object, **kwargs: object) -> None:
        raise RuntimeError('unreadable [/dim] store')

    monkeypatch.setattr('chatsbom.warehouse.build.derive', refuse)


def test_what_it_does_not_catch_is_reported_on_stderr(
    here: Store, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """As `handle_errors` reports it for every command (#124): an exit
    with status 1, not typer's traceback."""
    broken(here, monkeypatch)
    result = runner.invoke(app, ['warehouse', 'build'])

    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'Unexpected Error: unreadable [/dim] store' in result.stderr


def test_what_it_does_not_catch_is_one_json_object(
    here: Store, monkeypatch: pytest.MonkeyPatch, json_logs: None,
) -> None:
    broken(here, monkeypatch)
    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level']) == ('Unexpected error', 'error')
    assert 'RuntimeError: unreadable [/dim] store' in line['exception']
