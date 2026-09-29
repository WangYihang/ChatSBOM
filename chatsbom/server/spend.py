"""The daily spend cap's ledger (#33), in web.sqlite.

Ported from the Worker's `SpendCounter` (web/src/spend.ts) and the
chat's use of it (`reserve`, web/src/chat.ts), for the chat the service
is to run (#128, section 2.6).

The cap was a running total in KV, checked before a call and added to
after it by reading the total and writing it back. KV reads can be a
minute stale, and of two writes to one key one is kept, so the check
admitted whatever arrived together and the total lost whatever was
added together: 20 questions at once against a $5 cap were all
admitted, about $44 of calls, of which $2.20 was recorded. So the check
and the addition are one step, and it comes before the model call:

  - `reserve` holds the call's worst case against the cap, or refuses.
    The check and the hold are one `BEGIN IMMEDIATE` transaction, which
    SQLite lets one writer at a time begin, threads and processes
    alike: whatever arrives together, what is spent and held never
    passes the cap, so neither can what is paid.
  - `settle` replaces the worst case with what the call cost, once the
    answer says.
  - `refund` releases it, for a call the API refused with an error,
    which it does not bill.
  - A call lost on the way, a timeout or a dropped connection, may have
    been answered and billed all the same: it is neither settled nor
    refunded, and its worst case stays held for the rest of its day.

A day is a UTC day, as the Worker named its counters (`spendDay`), and
a call counts against the day that admitted it, whatever the day is by
the time it settles. A day starts at nothing by having no rows. Days
before yesterday are deleted (`forget_before_yesterday`), which the
service does each hour: nothing reserves against them, and a call
reserved just before midnight has long settled. The Worker's counters
cleared themselves with an alarm an hour after their day (#115).
"""
import math
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import structlog

from chatsbom.server.state import WebState

logger = structlog.get_logger('spend')

#: What a question is told when the day's cap cannot pay for its turn.
USED_UP = (
    'The daily budget for AI answers is used up. '
    'The dashboard itself still works.'
)

#: What it is told when the ledger cannot be written.
UNAVAILABLE = 'AI answers are unavailable for a moment. Try again shortly.'


class OverBudget(Exception):
    """The day's cap cannot pay for the turn: a 429."""

    def __init__(self) -> None:
        super().__init__(USED_UP)


class LedgerUnavailable(Exception):
    """The ledger could not be written, so the turn cannot be counted:
    a 503. An uncounted turn is the failure the cap is there to
    prevent."""

    def __init__(self) -> None:
        super().__init__(UNAVAILABLE)


@dataclass(frozen=True)
class Usage:
    """A day's spending: settled, and held for calls in flight."""

    spent: float
    held: float


def _utc_date(now: datetime) -> date:
    """The UTC date at `now`, which must say its zone: a time without
    one, taken as the machine's local time, could name the wrong day."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError(f'a time with no zone names no UTC day: {now}')
    return now.astimezone(timezone.utc).date()


def spend_day(now: datetime) -> str:
    """The UTC day a call made at `now` counts against, as YYYY-MM-DD."""
    return _utc_date(now).isoformat()


def _readable(usd: float) -> bool:
    """Whether `usd` is an amount: a number, finite and not negative.
    Written so that NaN, which compares false, is not one."""
    return math.isfinite(usd) and usd >= 0


class SpendLedger:
    """Each day's reservations, in web.sqlite."""

    def __init__(self, state: WebState) -> None:
        self._state = state

    def reserve(
        self, reservation: str, usd: float, cap: float, now: datetime,
    ) -> bool:
        """Hold `usd` against `cap` for the call `reservation` names, on
        the UTC day at `now`, or refuse.

        A comparison that is not a number refuses: a cap that compared
        false would admit anything. Asked again for a reservation it
        holds, it says so and holds no more; a reservation already
        settled is not held again.
        """
        day = spend_day(now)
        if not _readable(usd):
            return False
        with self._state.transaction() as db:
            row = db.execute(
                'SELECT settled FROM spend WHERE id = ?', (reservation,),
            ).fetchone()
            if row is not None:
                return bool(row[0] == 0)
            # `total` is 0.0 for a day with no rows, where `sum` is NULL.
            (committed,) = db.execute(
                'SELECT total(usd) FROM spend WHERE day = ?', (day,),
            ).fetchone()
            if not committed + usd <= cap:
                return False
            db.execute(
                'INSERT INTO spend (id, day, usd, settled) '
                'VALUES (?, ?, ?, 0)',
                (reservation, day, usd),
            )
            return True

    def settle(self, reservation: str, usd: float) -> None:
        """Replace a reservation's worst case with what its call cost.

        A cost that cannot be read keeps the worst case, which is known
        to be enough. Settling what is not held, twice or after a
        refund, changes nothing.
        """
        with self._state.connect() as db:
            if _readable(usd):
                db.execute(
                    'UPDATE spend SET usd = ?, settled = 1 '
                    'WHERE id = ? AND settled = 0',
                    (usd, reservation),
                )
            else:
                db.execute(
                    'UPDATE spend SET settled = 1 '
                    'WHERE id = ? AND settled = 0',
                    (reservation,),
                )

    def refund(self, reservation: str) -> None:
        """Release a reservation whose call the API refused, and so did
        not bill. One already settled stays spent."""
        with self._state.connect() as db:
            db.execute(
                'DELETE FROM spend WHERE id = ? AND settled = 0',
                (reservation,),
            )

    def usage(self, day: str) -> Usage:
        """`day`'s spend, and what is held for calls in flight."""
        with self._state.connect() as db:
            spent, held = db.execute(
                'SELECT total(CASE WHEN settled = 1 THEN usd END), '
                'total(CASE WHEN settled = 0 THEN usd END) '
                'FROM spend WHERE day = ?',
                (day,),
            ).fetchone()
        return Usage(spent=spent, held=held)

    def forget_before_yesterday(self, now: datetime) -> int:
        """Delete every day before yesterday's, at `now`, and say how
        many reservations went. A hold among them is a call lost on the
        way; the day it counted against is over all the same."""
        yesterday = (_utc_date(now) - timedelta(days=1)).isoformat()
        with self._state.connect() as db:
            deleted = db.execute(
                'DELETE FROM spend WHERE day < ?', (yesterday,),
            ).rowcount
        return int(deleted)


@dataclass(frozen=True)
class Hold:
    """A turn's worst case, held against its day's cap until it is
    settled or refunded (chat.ts, `Reservation`)."""

    ledger: SpendLedger
    reservation: str

    def settle(self, usd: float) -> None:
        """Replace the worst case with what the turn cost.

        After the answer, never instead of it: the model is paid for
        either way, so a ledger that fails to hear of it is logged, and
        the turn's worst case then stays held in its place.
        """
        try:
            self.ledger.settle(self.reservation, usd)
        except (sqlite3.Error, OSError) as error:
            logger.error(
                'spend not settled',
                reservation=self.reservation, error=str(error),
            )

    def refund(self) -> None:
        """Release it: the API refused the turn, and did not bill it."""
        try:
            self.ledger.refund(self.reservation)
        except (sqlite3.Error, OSError) as error:
            logger.error(
                'reservation not refunded',
                reservation=self.reservation, error=str(error),
            )


class Budget:
    """The day's cap, in dollars, and the ledger that keeps it."""

    def __init__(self, ledger: SpendLedger, cap: float) -> None:
        if not (_readable(cap) and cap != math.inf):
            raise ValueError(f'the cap is not a number of dollars: {cap!r}')
        self.ledger = ledger
        self.cap = cap

    def hold(self, usd: float, now: datetime) -> Hold:
        """Hold a turn's worst case against the cap on the day at `now`,
        before the turn is made, or refuse it.

        A ledger that cannot be written refuses the turn too, since an
        uncounted turn is the failure the cap is there to prevent; it
        may have held the turn and failed only to say so, so the hold
        is released as well as it can be.
        """
        reservation = str(uuid.uuid4())
        try:
            held = self.ledger.reserve(reservation, usd, self.cap, now)
        except (sqlite3.Error, OSError) as error:
            logger.error('spend ledger unavailable', error=str(error))
            Hold(self.ledger, reservation).refund()
            raise LedgerUnavailable() from error
        if not held:
            raise OverBudget()
        return Hold(self.ledger, reservation)
