"""The daily spend cap's ledger, in web.sqlite (#134).

Ported from the Worker's `SpendCounter` (web/src/spend.ts, #33) and its
use in the chat (web/src/chat.ts, `reserve`), and held to their tests
(web/test/spend.test.ts and the cap's in web/test/chat.test.ts).

The cap was a running total in KV, checked before a call and added to
after it by reading and writing it back: 20 questions at once against a
$5 cap were all admitted, about $44 of calls, of which $2.20 was
recorded. So a call's worst case is held against the cap before it is
made, in one step with the check: here one `BEGIN IMMEDIATE`
transaction, which SQLite gives one writer at a time, whether the
writers are threads or processes.
"""
import json
import math
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest
import structlog

from chatsbom.server.spend import Budget
from chatsbom.server.spend import LedgerUnavailable
from chatsbom.server.spend import OverBudget
from chatsbom.server.spend import spend_day
from chatsbom.server.spend import SpendLedger
from chatsbom.server.spend import Usage
from chatsbom.server.state import WebState

ROOT = Path(__file__).resolve().parents[1]

#: Midday on a day the Worker's tests use.
NOW = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
DAY = '2026-09-14'


@pytest.fixture
def state(tmp_path: Path) -> WebState:
    return WebState(tmp_path / 'state')


@pytest.fixture
def ledger(state: WebState) -> SpendLedger:
    return SpendLedger(state)


def usage(ledger: SpendLedger, day: str = DAY) -> tuple[float, float]:
    """The day's spend and holds, rounded as money is."""
    used = ledger.usage(day)
    return round(used.spent, 10), round(used.held, 10)


class TestTheFile:
    def test_is_web_sqlite_in_the_state_directory(self, tmp_path):
        state = WebState(tmp_path / 'a' / 'b')
        assert state.path == tmp_path / 'a' / 'b' / 'web.sqlite'
        assert state.path.is_file()

    def test_is_in_wal_mode(self, state):
        with closing(sqlite3.connect(state.path)) as db:
            assert db.execute('PRAGMA journal_mode').fetchone() == ('wal',)

    def test_every_connection_syncs_fully(self, state):
        """A hold is on disk before the call it pays for is made; a
        power cut that lost it would lift the cap by as much."""
        with state.connect() as db:
            # 2 is FULL.
            assert db.execute('PRAGMA synchronous').fetchone() == (2,)

    def test_opening_it_again_keeps_what_it_holds(self, tmp_path):
        """What the Worker's test calls a restart."""
        before = SpendLedger(WebState(tmp_path))
        before.reserve('a', 0.4, 1, NOW)
        before.reserve('b', 0.3, 1, NOW)
        before.settle('b', 0.1)

        after = SpendLedger(WebState(tmp_path))
        assert usage(after) == (0.1, 0.4)
        after.settle('a', 0.2)
        assert usage(after) == (0.3, 0)


class TestAReservation:
    def test_is_held_while_it_fits_under_the_cap_and_refused_after(
        self, ledger,
    ):
        assert ledger.reserve('a', 0.6, 1, NOW)
        assert ledger.reserve('b', 0.4, 1, NOW)
        assert not ledger.reserve('c', 0.01, 1, NOW)
        assert ledger.usage(DAY) == Usage(spent=0, held=1)

    def test_is_settled_at_what_the_call_cost_which_frees_the_rest(
        self, ledger,
    ):
        ledger.reserve('a', 0.9, 1, NOW)
        ledger.settle('a', 0.05)
        assert usage(ledger) == (0.05, 0)
        assert ledger.reserve('b', 0.95, 1, NOW)
        assert not ledger.reserve('c', 0.01, 1, NOW)

    def test_is_refunded_when_its_call_was_never_answered(self, ledger):
        ledger.reserve('a', 1, 1, NOW)
        ledger.refund('a')
        assert usage(ledger) == (0, 0)
        assert ledger.reserve('b', 1, 1, NOW)

    def test_is_held_once_however_often_it_is_asked_for(self, ledger):
        assert ledger.reserve('a', 0.6, 1, NOW)
        assert ledger.reserve('a', 0.6, 1, NOW)
        assert usage(ledger) == (0, 0.6)

    def test_is_settled_or_refunded_once_however_often_it_is_asked(
        self, ledger,
    ):
        ledger.reserve('a', 0.5, 1, NOW)
        ledger.settle('a', 0.2)
        ledger.settle('a', 0.2)
        ledger.refund('a')
        ledger.settle('never-held', 0.3)
        assert usage(ledger) == (0.2, 0)

    def test_is_not_held_again_once_settled(self, ledger):
        """A reservation names one call. The Worker kept only what it
        held by name, and would have held a settled one's name again;
        here its row stays, spent, for the rest of the day."""
        ledger.reserve('a', 0.5, 1, NOW)
        ledger.settle('a', 0.2)
        assert not ledger.reserve('a', 0.5, 1, NOW)
        assert usage(ledger) == (0.2, 0)

    @pytest.mark.parametrize('cost', [math.nan, math.inf, -1.0])
    def test_keeps_its_worst_case_when_what_it_cost_cannot_be_read(
        self, ledger, cost,
    ):
        """A usage the ledger cannot add would make every later
        comparison false, and a cap that compares false admits
        anything; SQLite would store NaN as NULL besides."""
        ledger.reserve('a', 0.5, 1, NOW)
        ledger.settle('a', cost)
        assert usage(ledger) == (0.5, 0)

    @pytest.mark.parametrize(
        'usd,cap',
        [
            (math.nan, 1),
            (-1, 1),
            (math.inf, 1),
            (0.1, math.nan),
            (0.1, -1),
        ],
        ids=[
            'a cost that is not a number',
            'a negative cost',
            'an infinite cost',
            'a cap that is not a number',
            'a negative cap',
        ],
    )
    def test_refuses(self, ledger, usd, cap):
        assert not ledger.reserve('a', usd, cap, NOW)
        assert usage(ledger) == (0, 0)


class TestConcurrentCalls:
    """16 × $0.30 is $4.80; a 17th would be $5.10."""

    def test_threads_can_never_take_the_day_past_the_cap(self, ledger):
        start = threading.Barrier(10)
        held: list[bool] = []

        def ask(worker: int) -> None:
            start.wait()
            held.extend(
                ledger.reserve(f'call-{worker}-{n}', 0.3, 5, NOW)
                for n in range(10)
            )

        threads = [
            threading.Thread(target=ask, args=(worker,))
            for worker in range(10)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert sum(held) == 16
        assert usage(ledger) == (0, 4.8)

    def test_processes_can_never_take_the_day_past_the_cap(
        self, state, tmp_path,
    ):
        """Several web processes, or the service and a CLI, on one file."""
        go = tmp_path / 'go'
        workers = [reserving(state, go, f'worker-{n}', 10) for n in range(4)]
        # Each waits for this, having opened the file, so that they all
        # reserve at once.
        time.sleep(0.5)
        go.touch()
        held = [hold for worker in workers for hold in answer(worker)]

        assert len(held) == 40
        assert sum(held) == 16
        assert usage(SpendLedger(state)) == (0, 4.8)

    def test_a_reservation_waits_for_another_process_to_commit(
        self, state, tmp_path,
    ):
        """The check and the hold are one transaction, begun before the
        check: a reservation that read the day, then wrote, would miss
        a hold another process was writing meanwhile, and both would be
        held. Here that other process is this one, holding $4.90 of the
        day's $5 uncommitted while another asks for $0.30."""
        go = tmp_path / 'go'
        worker = reserving(state, go, 'late', 1)
        with closing(
            sqlite3.connect(state.path, isolation_level=None),
        ) as other:
            other.execute('BEGIN IMMEDIATE')
            other.execute(
                'INSERT INTO spend (id, day, usd, settled) '
                "VALUES ('early', ?, 4.9, 0)",
                (DAY,),
            )
            go.touch()
            # Long enough for the other to have read the day, had it not
            # waited for this to commit.
            time.sleep(1)
            other.execute('COMMIT')

        assert answer(worker) == [False]
        assert usage(SpendLedger(state)) == (0, 4.9)

    def test_either_side_of_midnight_each_day_admits_its_own_cap(
        self, ledger,
    ):
        before = datetime(2026, 9, 14, 23, 59, 59, 900_000, timezone.utc)
        after = datetime(2026, 9, 15, 0, 0, 0, 100_000, timezone.utc)
        start = threading.Barrier(8)
        held: dict[str, list[bool]] = {DAY: [], '2026-09-15': []}

        def ask(worker: int) -> None:
            now = before if worker % 2 else after
            start.wait()
            held[spend_day(now)].extend(
                ledger.reserve(f'call-{worker}-{n}', 0.3, 5, now)
                for n in range(10)
            )

        threads = [
            threading.Thread(target=ask, args=(worker,)) for worker in range(8)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert {day: sum(calls) for day, calls in held.items()} == {
            DAY: 16, '2026-09-15': 16,
        }


def reserving(
    state: WebState, go: Path, name: str, count: int,
) -> subprocess.Popen[str]:
    """Another process, which opens the ledger, waits for `go` to exist,
    then makes `count` reservations of $0.30 against a $5 cap."""
    return subprocess.Popen(
        [
            sys.executable, '-c', RESERVE, str(state.path.parent), str(go),
            name, str(count),
        ],
        cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def answer(worker: subprocess.Popen[str]) -> list[bool]:
    """Whether each of `worker`'s reservations was held."""
    out, err = worker.communicate(timeout=120)
    assert worker.returncode == 0, err
    held: list[bool] = json.loads(out)
    return held


#: Opens the ledger in `argv[1]`, waits for `argv[2]` to exist, then
#: makes `argv[4]` reservations of $0.30 against a $5 cap, named after
#: `argv[3]`, and prints whether each was held.
RESERVE = """
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from chatsbom.server.spend import SpendLedger
from chatsbom.server.state import WebState

directory, go, name, count = sys.argv[1:]
ledger = SpendLedger(WebState(Path(directory)))
waited = time.monotonic()
while not Path(go).exists():
    if time.monotonic() - waited > 60:
        sys.exit('never told to go')
    time.sleep(0.001)
now = datetime(2026, 9, 14, 12, tzinfo=timezone.utc)
print(json.dumps([
    ledger.reserve(f'{name}-{n}', 0.3, 5, now) for n in range(int(count))
]))
"""


class TestTheUTCDay:
    def test_names_the_day_a_call_counts_against(self):
        assert spend_day(
            datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc),
        ) == '2026-09-14'
        assert spend_day(
            datetime(2026, 9, 15, 0, 0, 1, tzinfo=timezone.utc),
        ) == '2026-09-15'

    def test_is_utc_wherever_the_clock_is(self):
        tokyo = timezone(timedelta(hours=9))
        assert spend_day(datetime(2026, 9, 15, 8, tzinfo=tokyo)) == DAY

    def test_is_not_guessed_from_a_time_with_no_zone(self):
        with pytest.raises(ValueError):
            spend_day(datetime(2026, 9, 14, 12))

    def test_starts_each_day_at_nothing(self, ledger):
        last_second = datetime(2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc)
        next_day = datetime(2026, 9, 15, 0, 0, 1, tzinfo=timezone.utc)
        ledger.reserve('earlier', 4.9, 5, NOW)
        ledger.settle('earlier', 4.9)

        assert not ledger.reserve('late', 0.3, 5, last_second)
        assert ledger.reserve('early', 0.3, 5, next_day)
        assert usage(ledger, '2026-09-15') == (0, 0.3)

    def test_settles_a_call_against_the_day_that_took_it(self, ledger):
        """The reservation was made against that day's cap, so the
        call's cost belongs to it; the next day starts clean."""
        ledger.reserve(
            'a', 0.5, 1, datetime(
                2026, 9, 14, 23, 59, 59, tzinfo=timezone.utc,
            ),
        )
        # Settled after midnight.
        ledger.settle('a', 0.1)
        assert usage(ledger, DAY) == (0.1, 0)
        assert usage(ledger, '2026-09-15') == (0, 0)


class TestForgettingPastDays:
    """Nothing removed a past day's counter from the Worker, until #115
    had each clear itself with an alarm. Here the service deletes, each
    hour, every day before yesterday: nothing reserves against one, and
    a call reserved just before midnight has long settled."""

    def test_deletes_the_days_before_yesterday(self, ledger):
        for day in (12, 13, 14, 15):
            ledger.reserve(
                f'call-{day}', 0.1, 1,
                datetime(2026, 9, day, 12, tzinfo=timezone.utc),
            )

        deleted = ledger.forget_before_yesterday(
            datetime(2026, 9, 15, 0, 30, tzinfo=timezone.utc),
        )

        assert deleted == 2
        assert usage(ledger, '2026-09-12') == (0, 0)
        assert usage(ledger, '2026-09-13') == (0, 0)
        assert usage(ledger, DAY) == (0, 0.1)
        assert usage(ledger, '2026-09-15') == (0, 0.1)

    def test_a_lost_call_keeps_its_hold_for_its_day(self, ledger):
        """A call that never came back may have been answered, and
        billed: refunding it would be the one way past the cap. Neither
        settled nor refunded, it holds its worst case until its day is
        deleted with the rest."""
        ledger.reserve('lost', 0.3, 1, NOW)
        ledger.forget_before_yesterday(NOW)
        ledger.forget_before_yesterday(
            datetime(2026, 9, 15, 23, tzinfo=timezone.utc),
        )
        assert usage(ledger) == (0, 0.3)

        ledger.forget_before_yesterday(
            datetime(2026, 9, 16, 0, 30, tzinfo=timezone.utc),
        )
        assert usage(ledger) == (0, 0)


class TestABudget:
    """What the chat reserves each turn through (chat.ts, `reserve`):
    the cap, a reservation named for the call, and what to do when the
    ledger cannot be written."""

    def test_holds_a_turn_and_settles_it(self, ledger):
        hold = Budget(ledger, 5).hold(0.3, NOW)
        hold.settle(0.02)
        assert usage(ledger) == (0.02, 0)

    def test_refunds_a_turn_the_model_refused(self, ledger):
        hold = Budget(ledger, 5).hold(0.3, NOW)
        hold.refund()
        assert usage(ledger) == (0, 0)

    def test_holds_each_turn_apart(self, ledger):
        budget = Budget(ledger, 1)
        budget.hold(0.4, NOW)
        budget.hold(0.4, NOW)
        with pytest.raises(OverBudget):
            budget.hold(0.4, NOW)
        assert usage(ledger) == (0, 0.8)

    def test_refuses_a_turn_the_cap_cannot_pay_for(self, ledger):
        with pytest.raises(OverBudget, match='daily budget'):
            Budget(ledger, 0.1).hold(0.3, NOW)
        assert usage(ledger) == (0, 0)

    def test_refuses_rather_than_go_uncounted(self, ledger, monkeypatch):
        """A ledger that cannot be written refuses the turn: an
        uncounted turn is the failure the cap is there to prevent. It
        may have held the turn and failed only to say so, so the hold
        is released."""
        tried: list[str] = []

        def broken(*args: object) -> bool:
            raise sqlite3.OperationalError('database is locked')

        monkeypatch.setattr(ledger, 'reserve', broken)
        monkeypatch.setattr(ledger, 'refund', tried.append)

        with pytest.raises(LedgerUnavailable):
            Budget(ledger, 5).hold(0.3, NOW)
        assert len(tried) == 1

    def test_still_settles_quietly_when_the_ledger_fails(
        self, ledger, monkeypatch,
    ):
        """After the answer, never instead of it: the model is paid for
        either way, and the turn's worst case stays held in its place."""
        hold = Budget(ledger, 5).hold(0.3, NOW)

        def broken(*args: object) -> None:
            raise sqlite3.OperationalError('disk I/O error')

        monkeypatch.setattr(ledger, 'settle', broken)
        monkeypatch.setattr(ledger, 'refund', broken)
        with structlog.testing.capture_logs() as logged:
            hold.settle(0.02)
            hold.refund()

        assert [event['event'] for event in logged] == [
            'spend not settled', 'reservation not refunded',
        ]
        assert usage(ledger) == (0, 0.3)

    @pytest.mark.parametrize('cap', [math.nan, -1.0, math.inf])
    def test_refuses_a_cap_that_is_not_one(self, ledger, cap):
        with pytest.raises(ValueError):
            Budget(ledger, cap)
