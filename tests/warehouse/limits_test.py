"""DuckDB's memory limit and threads, from the environment (#148).

Every connection chatsbom makes to DuckDB is `chatsbom.warehouse.connect`'s:
`warehouse build`'s, `snapshot build`'s and the Parquet export's. There
DuckDB is given `CHATSBOM_DUCKDB_MEMORY_LIMIT` and
`CHATSBOM_DUCKDB_THREADS`, or defaults that fit the collector's
container. DuckDB's own are the machine's, 80% of its memory and every
core, and a pass deriving the documented shape held 5.5 GB (#141).
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pytest
import yaml
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.warehouse import connect
from chatsbom.warehouse import MEMORY_LIMIT
from chatsbom.warehouse import THREADS
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import Store

ROOT = Path(__file__).resolve().parents[2]
runner = CliRunner()

SETTINGS = ('CHATSBOM_DUCKDB_MEMORY_LIMIT', 'CHATSBOM_DUCKDB_THREADS')


@pytest.fixture(autouse=True)
def unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neither set, whatever the environment the suite runs in has."""
    for name in SETTINGS:
        monkeypatch.delenv(name, raising=False)


def given(con: duckdb.DuckDBPyConnection) -> tuple[str, int]:
    """The memory limit and the threads a connection runs with, as
    DuckDB says them."""
    [(memory, threads)] = con.execute(
        "SELECT current_setting('memory_limit'), current_setting('threads')",
    ).fetchall()
    return str(memory), int(threads)


def spelt(limit: str) -> str:
    """A memory limit as DuckDB says it back, made by DuckDB itself."""
    with duckdb.connect(config={'memory_limit': limit}) as con:
        return given(con)[0]


GIB = 1024 ** 3


def gibibytes(limit: str) -> float:
    """A limit in DuckDB's spelling, `2GiB` or `1500MB`, in GiB."""
    match = re.fullmatch(r'([0-9.]+)\s*([KMGT])(i?)B', limit, re.IGNORECASE)
    assert match, limit
    base = 1024 if match[3] else 1000
    power = 'KMGT'.index(match[2].upper()) + 1
    return float(match[1]) * base ** power / GIB


class TestTheDefaults:

    def test_are_given_to_every_connection(self) -> None:
        with connect(':memory:') as con:
            assert given(con) == (spelt(MEMORY_LIMIT), THREADS)

    def test_fit_the_collectors_container(self) -> None:
        """Its limits, docker-compose.yaml's: the threads its CPUs, and
        the memory what leaves a GiB and more of it to what is not
        DuckDB's (Python, Arrow's batches, SQLite's page cache), which
        DuckDB's limit does not bound."""
        compose = yaml.safe_load(
            (ROOT / 'docker-compose.yaml').read_text(encoding='utf-8'),
        )
        collector = compose['services']['collector']
        size = re.fullmatch(r'([0-9]+)g', str(collector['mem_limit']))
        assert size, collector['mem_limit']
        assert THREADS == int(float(collector['cpus']))
        assert gibibytes(MEMORY_LIMIT) + 1 <= int(size[1])


class TestTheEnvironment:

    def test_sets_the_memory_limit(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', '1GiB')
        with connect(':memory:') as con:
            assert given(con) == ('1.0 GiB', THREADS)

    def test_sets_the_threads(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '3')
        with connect(':memory:') as con:
            assert given(con) == (spelt(MEMORY_LIMIT), 3)

    @pytest.mark.parametrize(
        ('value', 'spelling'), [
            ('1500MB', '1.3 GiB'), ('512 MiB', '512.0 MiB'),
            (' 2gib ', '2.0 GiB'), ('0.5GiB', '512.0 MiB'),
        ],
    )
    def test_takes_a_limit_as_duckdb_spells_one(
        self, monkeypatch: pytest.MonkeyPatch, value: str, spelling: str,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', value)
        with connect(':memory:') as con:
            assert given(con)[0] == spelling

    def test_empty_is_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """As `.env.example` shows a setting, uncommented and left
        empty."""
        for name in SETTINGS:
            monkeypatch.setenv(name, ' ')
        with connect(':memory:') as con:
            assert given(con) == (spelt(MEMORY_LIMIT), THREADS)

    def test_is_read_as_each_connection_is_made(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Not at import: the CLI loads `.env` after every module is
        imported, in its root callback."""
        with connect(':memory:') as con:
            assert given(con)[1] == THREADS
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '1')
        with connect(':memory:') as con:
            assert given(con)[1] == 1

    def test_reaches_a_file_opened_read_only(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """As the snapshot and the export open the warehouse."""
        path = tmp_path / 'w.duckdb'
        with connect(path) as con:
            con.execute('CREATE TABLE t (n INTEGER)')
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', '768MiB')
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '1')
        with connect(path, read_only=True) as con:
            assert given(con) == ('768.0 MiB', 1)


def spill_directory(path: Path) -> str:
    """Where a connection to `path`, made by `connect` in this process,
    spills what does not fit its memory limit."""
    with connect(path, read_only=True) as con:
        [(directory,)] = con.execute(
            "SELECT current_setting('temp_directory')",
        ).fetchall()
    return str(directory)


@pytest.fixture
def small(tmp_path: Path) -> Path:
    """A file whose one table does not sort in 32 MiB."""
    path = tmp_path / 'w.duckdb'
    with connect(path) as con:
        con.execute(
            'CREATE TABLE t AS SELECT range AS n, md5(range::VARCHAR) AS s '
            'FROM range(600000)',
        )
    return path


class TestWhatDoesNotFit:
    """Is spilled into a directory of the process's own, beside the file.

    DuckDB's own is `<file>.tmp`, one for every process that opens the
    file, and two processes that spill into it read each other's blocks:
    two read-only readers of one file, each sorting 6M rows in 120 MB,
    crashed together (SIGSEGV, or "Corrupt temporary file"), and each
    alone was right. The snapshot and the export both read the
    warehouse, and a limit is what makes a pass spill."""

    def test_beside_the_file_and_not_duckdbs_own(self, small: Path) -> None:
        directory = Path(spill_directory(small))
        assert directory.parent == small.parent
        assert directory.name.startswith('w.duckdb.tmp-')
        assert directory.name != 'w.duckdb.tmp'

    def test_one_a_process(self, small: Path) -> None:
        """Every connection a process makes to the file shares one
        database, which DuckDB refuses to open twice with two configs;
        another process has its own."""
        ours = spill_directory(small)
        assert spill_directory(small) == ours
        with connect(small, read_only=True):
            assert spill_directory(small) == ours
        theirs = subprocess.run(
            [
                sys.executable, '-c',
                'import sys\n'
                'from chatsbom.warehouse import connect\n'
                'with connect(sys.argv[1], read_only=True) as con:\n'
                "    print(con.execute(\"SELECT current_setting("
                "'temp_directory')\").fetchone()[0])\n",
                str(small),
            ],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        assert theirs.startswith(str(small) + '.tmp-')
        assert theirs != ours

    def test_is_made_when_it_spills_and_gone_when_it_closes(
        self, small: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', '32MiB')
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '1')
        directory = Path(spill_directory(small))
        assert not directory.exists()
        with connect(small, read_only=True) as con:
            con.execute(
                'CREATE TEMP TABLE sorted AS SELECT * FROM t ORDER BY s',
            )
            assert directory.is_dir()
            assert con.execute('SELECT count(*) FROM sorted').fetchone() == (
                600000,
            )
        assert not directory.exists()

    def test_a_database_in_memory_spills_where_duckdb_puts_it(self) -> None:
        with connect(':memory:') as con, duckdb.connect() as own:
            assert con.execute(
                "SELECT current_setting('temp_directory')",
            ).fetchone() == own.execute(
                "SELECT current_setting('temp_directory')",
            ).fetchone()


#: A connection made as the collector's container makes one: compose runs
#: it as UID, which the image's /etc/passwd does not know, so its home is
#: `/`, where it can write nothing. Here HOME is a file, where no uid can
#: make DuckDB's extension directory; and the machine is in UTC+8.
HOMELESS = """
import sys
from datetime import datetime, timezone
from chatsbom.warehouse import connect
late = datetime(2026, 1, 31, 20, 0, tzinfo=timezone.utc)
with connect(sys.argv[1]) as con:
    con.execute('CREATE TABLE t (seen TIMESTAMP)')
    con.execute('INSERT INTO t VALUES (?)', [late])
    print(*con.execute(
        "SELECT current_setting('TimeZone'), "
        "strftime(seen, '%Y-%m-%d %H:%M') FROM t"
    ).fetchone())
"""


class TestNothingIsFetched:
    """A connection needs nothing from the network, or from the home
    directory of whoever makes it (#150).

    ICU, which DuckDB's `TimeZone` is, is linked into DuckDB's wheel. But
    given as a setting as the database opened, DuckDB looked for it on
    disk first, before loading its own: where the home was writable it
    fetched 20.7 MB of it from DuckDB's servers into ~/.duckdb, and
    where it was not, as in the collector's container, every command
    that opens DuckDB failed, `warehouse build`, `snapshot build` and
    the export alike. Set once connected, it is the linked one."""

    @pytest.mark.parametrize('database', ['memory', 'file', 'read-only'])
    def test_without_a_home_it_can_write(
        self, tmp_path: Path, database: str,
    ) -> None:
        path = ':memory:' if database == 'memory' else str(
            tmp_path / 'w.duckdb',
        )
        if database == 'read-only':
            with connect(path):
                pass
            code = HOMELESS.replace(
                'connect(sys.argv[1])',
                'connect(sys.argv[1], read_only=True)',
            ).replace(
                "con.execute('CREATE TABLE t (seen TIMESTAMP)')",
                "con.execute('CREATE TEMP TABLE t (seen TIMESTAMP)')",
            )
        else:
            code = HOMELESS
        result = subprocess.run(
            [sys.executable, '-c', code, path],
            env={**os.environ, 'HOME': os.devnull, 'TZ': 'Asia/Shanghai'},
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout == 'UTC 2026-01-31 20:00\n'

    def test_nor_would_it_fetch_an_extension_it_lacks(self) -> None:
        """What the wheel does not link is refused, not fetched."""
        with connect(':memory:') as con:
            assert con.execute(
                "SELECT current_setting('autoinstall_known_extensions'), "
                "current_setting('autoload_known_extensions')",
            ).fetchone() == (False, False)


class TestWhatIsNotALimit:
    """Refused, naming the setting, before DuckDB is given it."""

    @pytest.mark.parametrize(
        'value', ['lots', '8', '80%', '0GB', '0.0 GiB', '-1GB', '2 GB RAM'],
    )
    def test_a_memory_limit(
        self, monkeypatch: pytest.MonkeyPatch, value: str,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', value)
        with pytest.raises(ValueError, match='CHATSBOM_DUCKDB_MEMORY_LIMIT'):
            connect(':memory:')

    @pytest.mark.parametrize('value', ['0', '-1', 'two', '1.5', '²'])
    def test_threads(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', value)
        with pytest.raises(ValueError, match='CHATSBOM_DUCKDB_THREADS'):
            connect(':memory:')

    def test_says_what_it_was_and_what_it_takes(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', 'lots')
        with pytest.raises(ValueError) as refused:
            connect(':memory:')
        said = str(refused.value)
        assert "'lots'" in said and 'GiB' in said


# -- every command that opens DuckDB ----------------------------------------


@pytest.fixture
def opened(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """What each DuckDB connection made from here on was given, as DuckDB
    says it, asked as it is made."""
    real = duckdb.connect
    seen: list[tuple[str, int]] = []

    def connecting(*args: Any, **kwargs: Any) -> duckdb.DuckDBPyConnection:
        con = real(*args, **kwargs)
        seen.append(given(con))
        return con

    monkeypatch.setattr(duckdb, 'connect', connecting)
    return seen


@pytest.fixture
def limited(monkeypatch: pytest.MonkeyPatch) -> tuple[str, int]:
    """Both set, to what no default is."""
    monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', '640MiB')
    monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '1')
    return '640.0 MiB', 1


@pytest.fixture
def here(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """The working directory, where the commands look for `data/`."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    yield tmp_path


APP = Listed(1, 'acme', 'app', language='Ruby')


@pytest.fixture
def stored(here: Path) -> Store:
    """A store in `data/`, with one scanned repository."""
    store = Store(here / 'data')
    store.seed(store.snapshot(date(2026, 9, 1), APP), APP)
    store.sbom(
        1, 'a' * 40, artifact('rack', '3.1.0', 'gem', licenses=['MIT']),
        at=at(2026, 9, 14),
    )
    store.record(1, 'acme', 'app', commit='a' * 40)
    return store


@pytest.fixture
def built(here: Path) -> Path:
    """A warehouse in `data/`, made before the limits are set."""
    (here / 'data').mkdir()
    return warehouse(here / 'data' / 'warehouse.duckdb', shop())


class TestEveryCommandOpensDuckDBWithThem:

    def test_warehouse_build(
        self, stored: Store, limited: tuple[str, int],
        opened: list[tuple[str, int]],
    ) -> None:
        result = runner.invoke(app, ['warehouse', 'build'])
        assert result.exit_code == 0, result.output
        assert opened and set(opened) == {limited}

    def test_snapshot_build(
        self, built: Path, limited: tuple[str, int],
        opened: list[tuple[str, int]],
    ) -> None:
        result = runner.invoke(app, ['snapshot', 'build'])
        assert result.exit_code == 0, result.output
        assert opened and set(opened) == {limited}

    def test_export_parquet(
        self, built: Path, limited: tuple[str, int],
        opened: list[tuple[str, int]],
    ) -> None:
        result = runner.invoke(
            app, ['export', 'parquet'],
        )
        assert result.exit_code == 0, result.output
        assert opened and set(opened) == {limited}


class TestACommandGivenWhatIsNotALimit:
    """Stops before it has built anything, and says which setting."""

    def test_warehouse_build(
        self, stored: Store, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', 'two')
        result = runner.invoke(app, ['warehouse', 'build'])
        assert result.exit_code == 1
        assert result.stdout == ''
        assert 'CHATSBOM_DUCKDB_THREADS' in result.stderr
        assert not stored.paths.warehouse_path.exists()
        assert sorted(p.name for p in stored.root.glob('warehouse*')) == [
            'warehouse.duckdb.lock',
        ]

    def test_snapshot_build(
        self, built: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_MEMORY_LIMIT', 'plenty')
        result = runner.invoke(app, ['snapshot', 'build'])
        assert result.exit_code == 1
        assert result.stdout == ''
        assert 'CHATSBOM_DUCKDB_MEMORY_LIMIT' in result.stderr
        snapshots = built.parent / 'snapshots'
        assert sorted(p.name for p in snapshots.iterdir()) == ['.lock']

    def test_export_parquet(
        self, built: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '0')
        result = runner.invoke(
            app, ['export', 'parquet'],
        )
        assert result.exit_code == 1
        assert result.stdout == ''
        assert 'CHATSBOM_DUCKDB_THREADS' in result.stderr
        assert not (built.parent.parent / 'dist').exists()
