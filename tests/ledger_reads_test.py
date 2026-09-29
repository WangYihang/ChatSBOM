"""What the ledger answers without being written to (#100).

`queue due` compares the due set derived from the store with the
ledger's, while the collector runs and writes the same ledger. So the
ledger must be readable in a way that cannot write: opened read-only,
with none of what opening it for work does (the schema script, the
columns an older ledger lacks, adopting watermarks), and its due sets
read by the same statements the workers claim by.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def _files(directory: Path) -> dict[str, tuple[int, int, bytes]]:
    """Every file in `directory`: its size, mtime and bytes."""
    return {
        path.name: (
            path.stat().st_size, path.stat().st_mtime_ns, path.read_bytes(),
        )
        for path in sorted(directory.iterdir())
    }


# --- the depgraph due set, as `claim_stage` takes it ------------------------

def _varied(ledger: Ledger) -> None:
    """One repository for each way the depgraph stage can be due or not."""
    def seed(repository_id: int, stars: int | None) -> None:
        ledger.seed(
            repository_id, 'o', f'r{repository_id}', snapshot='all-x',
            stars=stars,
        )

    for repository_id, stars in (
        (1, 10), (2, 500), (3, 50), (4, 60), (5, 5), (6, 70), (7, 80),
        (8, 90), (9, 20), (10, 30), (11, None), (12, 500),
    ):
        seed(repository_id, stars)
    # 1, 2, 11: never asked; 12 as well, with more stars than 2 by id.
    old = NOW - timedelta(days=31)
    records = {
        3: StageState(
            3, Stage.DEPGRAPH, outcome='ok', done_at=old,
            next_attempt_at=NOW - timedelta(days=1),
        ),
        4: StageState(
            4, Stage.DEPGRAPH, outcome='absent',
            next_attempt_at=NOW - timedelta(days=1),
        ),
        5: StageState(
            5, Stage.DEPGRAPH, outcome='failed', failure_count=1,
            next_attempt_at=NOW - timedelta(minutes=1),
        ),
        6: StageState(
            6, Stage.DEPGRAPH, outcome='absent',
            next_attempt_at=NOW + timedelta(days=1),
        ),
        8: StageState(
            8, Stage.DEPGRAPH, claimed_by='elsewhere',
            claim_expires_at=NOW + timedelta(minutes=5),
        ),
    }
    for record in records.values():
        ledger.record_stage(record)
    # `record_stage` drops a lease: 8's is put back as a claim leaves it.
    ledger._db.execute(
        "UPDATE stage_state SET claimed_by = 'elsewhere', "
        'claim_expires_at = ? WHERE repository_id = 8',
        ((NOW + timedelta(minutes=5)).isoformat(),),
    )
    ledger.record_absent(7, NOW, retry_at=NOW - timedelta(days=1))
    # 9 and 10: a graph fetched before `stage_state`, only a watermark;
    # 9's is past the refresh, 10's is not.
    for repository_id, fetched in (
        (9, NOW - timedelta(days=40)), (10, NOW - timedelta(days=3)),
    ):
        state = ledger.get(repository_id)
        assert state is not None
        state.stage_watermarks[Stage.DEPGRAPH] = fetched
        ledger.upsert(state)


def test_the_depgraph_due_set_is_what_claim_stage_takes_in_its_order(
    tmp_path,
):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)

        due = ledger.depgraph_due_ids(NOW)
        claimed = ledger.claim_stage(Stage.DEPGRAPH, NOW, None, 'w')

        assert due == [work.repository_id for work in claimed]
        # Never asked first, the most starred first, then the refreshes
        # (a graph and a watermark past it), then the expired negative
        # cache; not the backoff, the absent, the leased or the fresh.
        assert due == [2, 12, 1, 5, 11, 3, 9, 4]


def test_the_depgraph_due_set_takes_nothing(tmp_path):
    """Read, not claimed: no lease is taken and no row is written, so
    it can be asked of a ledger the workers are using."""
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)
        before = ledger._db.execute(
            'SELECT * FROM stage_state ORDER BY repository_id, stage',
        ).fetchall()

        first = ledger.depgraph_due_ids(NOW)
        again = ledger.depgraph_due_ids(NOW)

        after = ledger._db.execute(
            'SELECT * FROM stage_state ORDER BY repository_id, stage',
        ).fetchall()
        assert [tuple(r) for r in after] == [tuple(r) for r in before]
        assert first == again


def test_the_depgraph_due_set_narrows_and_limits_as_the_claim_does(
    tmp_path,
):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)

        assert ledger.depgraph_due_ids(NOW, repos={1, 3, 6}) == [1, 3]
        assert ledger.depgraph_due_ids(NOW, limit=3) == [2, 12, 1]
        assert [
            work.repository_id
            for work in ledger.claim_stage(
                Stage.DEPGRAPH, NOW, 2, 'w', repos={1, 3, 6},
            )
        ] == [1, 3]


def test_a_claimed_repository_leaves_the_depgraph_due_set(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as ledger:
        _varied(ledger)
        [work] = ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'w')

        assert work.repository_id not in ledger.depgraph_due_ids(NOW)


# --- the ledger, read-only --------------------------------------------------

def _written(path: Path) -> None:
    """A ledger with one repository, a stage row, and a watermark that
    was never adopted: written after the last open, which is when
    adopting happens."""
    with Ledger(path) as ledger:
        ledger.track(1, 'o', 'r', 'ruby')
        ledger.record_push(1, NOW - timedelta(days=1), NOW)
        ledger.record_stage_success(1, Stage.RELEASE, NOW, 'p', 'v1')
        state = ledger.get(1)
        assert state is not None
        state.stage_watermarks[Stage.TREE] = NOW
        ledger.upsert(state)


def test_a_read_only_ledger_reads_what_the_ledger_holds(tmp_path):
    path = tmp_path / 'ledger.sqlite3'
    _written(path)

    with Ledger.open_readonly(path) as ledger:
        state = ledger.get(1)
        release = ledger.stage_state(1, Stage.RELEASE)

    assert state is not None and state.full_name == 'o/r'
    assert release is not None and release.output_key == 'v1'


def test_opening_it_read_only_adopts_nothing(tmp_path):
    """Opening a ledger for work adopts every watermark that has no
    `stage_state` row yet. Opened to be read, it writes nothing."""
    path = tmp_path / 'ledger.sqlite3'
    _written(path)

    with Ledger.open_readonly(path) as ledger:
        assert ledger.stage_state(1, Stage.TREE) is None

    with Ledger(path) as ledger:
        assert ledger.stage_state(1, Stage.TREE) is not None, (
            'opened for work, the same ledger adopts it'
        )


def test_it_cannot_write(tmp_path):
    path = tmp_path / 'ledger.sqlite3'
    _written(path)

    with Ledger.open_readonly(path) as ledger:
        with pytest.raises(sqlite3.OperationalError):
            ledger.track(2, 'o', 'x', 'ruby')
        with pytest.raises(sqlite3.OperationalError):
            ledger.claim_stage(Stage.DEPGRAPH, NOW, None, 'w')
        # The due set a claim would take is still readable.
        assert ledger.depgraph_due_ids(NOW) == [1]


def test_it_is_neither_migrated_nor_given_the_schema(tmp_path):
    """A ledger from before `stage_state` and the snapshot columns is
    read as it is: opening it for work would add both."""
    path = tmp_path / 'ledger.sqlite3'
    db = sqlite3.connect(path)
    db.execute(
        'CREATE TABLE repository_state (repository_id INTEGER PRIMARY KEY, '
        'owner TEXT NOT NULL, repo TEXT NOT NULL)',
    )
    db.execute("INSERT INTO repository_state VALUES (1, 'o', 'r')")
    db.commit()
    db.close()
    before = _files(tmp_path)

    with Ledger.open_readonly(path) as ledger:
        assert ledger.count() == 1

    assert _files(tmp_path) == before
    tables = {
        row[0] for row in sqlite3.connect(path).execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'",
        )
    }
    assert tables == {'repository_state'}


def test_a_missing_ledger_is_not_created(tmp_path):
    with pytest.raises(FileNotFoundError):
        Ledger.open_readonly(tmp_path / 'ledger.sqlite3')
    assert list(tmp_path.iterdir()) == []


def test_a_quiet_ledger_is_read_without_a_file_beside_it(tmp_path):
    """No `-wal` or `-shm` is made beside a ledger nothing has open.
    SQLite makes both for a read-only reader of a WAL database, and
    leaves them, owned by whoever read: after a `sudo`, files the
    collector's own user could not write."""
    path = tmp_path / 'ledger.sqlite3'
    _written(path)
    before = _files(tmp_path)
    assert set(before) == {'ledger.sqlite3'}

    with Ledger.open_readonly(path) as ledger:
        assert ledger.count() == 1

    assert _files(tmp_path) == before


#: A worker writing to the ledger and holding it open, its last write
#: in the WAL and not yet in the database file.
_WRITER = """
import sqlite3, sys, time
db = sqlite3.connect(sys.argv[1], isolation_level=None)
db.execute('PRAGMA wal_autocheckpoint=0')
db.execute(
    "INSERT INTO repository_state (repository_id, owner, repo) "
    "VALUES (2, 'o', 'written-by-a-worker')"
)
print('written', flush=True)
sys.stdin.read()
db.close()
"""


def test_a_ledger_in_use_is_read_with_what_its_writer_committed(tmp_path):
    """While the collector runs, its last commits are in the WAL. They
    are read, and neither the database file nor its WAL changes."""
    path = tmp_path / 'ledger.sqlite3'
    _written(path)
    writer = subprocess.Popen(
        [sys.executable, '-c', _WRITER, str(path)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
    )
    try:
        assert writer.stdout is not None
        assert writer.stdout.readline().strip() == 'written'
        wal = Path(f'{path}-wal')
        assert wal.stat().st_size > 0
        before = {
            name: facts for name, facts in _files(tmp_path).items()
            if not name.endswith('-shm')
        }

        with Ledger.open_readonly(path) as ledger:
            names = {state.repo for state in ledger.all()}

        after = {
            name: facts for name, facts in _files(tmp_path).items()
            if not name.endswith('-shm')
        }
    finally:
        assert writer.stdin is not None
        writer.stdin.close()
        writer.wait(timeout=30)
    assert names == {'r', 'written-by-a-worker'}
    assert after == before


@pytest.mark.skipif(
    os.name != 'posix', reason='directory permissions are POSIX',
)
def test_a_ledger_in_a_read_only_directory_is_read(tmp_path):
    """Where the reader may not create a file beside the ledger at all.
    (As root the permissions stop nothing, and the files beside it are
    compared instead.)"""
    directory = tmp_path / 'data'
    directory.mkdir()
    path = directory / 'ledger.sqlite3'
    _written(path)
    before = _files(directory)
    directory.chmod(0o555)
    try:
        with Ledger.open_readonly(path) as ledger:
            assert [state.repo for state in ledger.all()] == ['r']
    finally:
        directory.chmod(0o755)
    assert _files(directory) == before
