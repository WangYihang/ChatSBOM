"""Timestamps must land in ClickHouse as the instant they name.

Every `DateTime` column in this project was eight hours early, because
the code computed UTC and then dropped the zone -- and
`clickhouse_connect` reads a naive datetime as local time. The tests
that covered those timestamps all passed: they compared the value to
itself, or asserted `tzinfo is None` as though that were a requirement.

So these assert the property the old tests could not see: an aware
value, and the same instant out as in.
"""
from __future__ import annotations

import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from chatsbom.core.instants import mtime
from chatsbom.core.instants import stated
from chatsbom.core.instants import UNSET
from chatsbom.core.instants import utc

NOON_UTC = datetime(2026, 2, 11, 12, 0, tzinfo=timezone.utc)


def test_an_aware_value_keeps_its_instant():
    """Not its wall clock -- the instant. A value already in UTC+8 is
    the same moment expressed differently, and converting it must not
    move it."""
    shanghai = NOON_UTC.astimezone(timezone(timedelta(hours=8)))
    assert shanghai.hour == 20, 'the fixture is the same instant, not 12:00'
    assert utc(shanghai) == NOON_UTC


def test_a_naive_value_is_read_as_utc_not_local():
    """This is the bug, in one assertion.

    Everything upstream computes UTC. Reading a naive value as local
    time -- which is what the driver does, and what this function
    exists to stop -- shifts it by the machine's offset, so the same
    code produces different data on different machines.
    """
    assert utc(NOON_UTC.replace(tzinfo=None)) == NOON_UTC


def test_every_result_is_aware():
    """The whole point: nothing naive reaches an insert."""
    for value in (utc(None), utc(NOON_UTC), utc(NOON_UTC.replace(tzinfo=None))):
        assert value.tzinfo is not None
        assert value.utcoffset() == timedelta(0)


def test_missing_is_a_day_after_the_epoch():
    """`1970-01-01` is where ClickHouse's DateTime starts, so a
    timezone slip on an unset date clamps to the same value as the
    unset date itself and the two become indistinguishable."""
    assert utc(None) == UNSET
    assert UNSET.date() == datetime(1970, 1, 2).date()


def test_an_mtime_is_the_instant_the_file_was_written(tmp_path):
    path = tmp_path / 'doc.json'
    path.write_text('{}')
    os.utime(path, (NOON_UTC.timestamp(), NOON_UTC.timestamp()))
    assert mtime(path) == NOON_UTC


def test_a_missing_file_is_unset_rather_than_now(tmp_path):
    """A recount must not make an absent document look fresh."""
    assert mtime(tmp_path / 'nope.json') == UNSET


def test_a_stated_timestamp_survives_its_literal_z():
    """SPDX writes RFC 3339 with a literal Z."""
    assert stated('2026-09-14T03:56:20Z') == datetime(
        2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc,
    )


def test_a_stated_offset_is_converted_not_truncated():
    assert stated('2026-09-14T11:56:20+08:00') == datetime(
        2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc,
    )


def test_an_unreadable_statement_is_none_not_an_exception():
    """A document that cannot be dated is still worth ingesting."""
    assert stated('not a timestamp') is None
    assert stated('') is None
    assert stated(None) is None


def test_no_insert_path_strips_a_timezone():
    """A guard on the shape of the mistake, not on one instance of it.

    The bug was `.replace(tzinfo=None)` repeated in four files, each
    reasonable on its own. Anything that needs it again should have to
    justify itself here.
    """
    from pathlib import Path

    allowed = {
        # Reads the ledger to set scheduling watermarks; the ledger
        # stores naive values and is not a ClickHouse column.
        'chatsbom/commands/queue/backfill.py',
        # Names the mistake in its docstring, which is the point of it.
        'chatsbom/core/instants.py',
    }
    offenders = []
    for path in Path('chatsbom').rglob('*.py'):
        if str(path) in allowed:
            continue
        if 'replace(tzinfo=None)' in path.read_text(encoding='utf-8'):
            offenders.append(str(path))
    assert not offenders, (
        f"these strip the timezone before an insert: {offenders}. "
        f"See chatsbom/core/instants.py -- naive values land in the "
        f"database shifted by the machine's UTC offset."
    )
