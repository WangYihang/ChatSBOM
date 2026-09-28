"""Timestamps that survive the trip into ClickHouse.

Every `DateTime` column in this project was eight hours early, and the
reason is one line repeated in four files: `.replace(tzinfo=None)`.

The code computed the right instant and then threw away the only thing
that said which instant it was. `clickhouse_connect` reads a naive
datetime as **local time** and converts it to the column's timezone, so
a value computed as UTC on a UTC+8 machine lands eight hours early.
Measured, against a scratch table on this database:

    inserted naive  2026-02-11 11:14:39  ->  stored 2026-02-11 03:14:39
    inserted aware  2026-02-11 11:14:39  ->  stored 2026-02-11 11:14:39

Nothing in the read path corrects it, so the error is silent and
plausible: `artifacts.observed_at` bottomed out at `2026-02-11 03:04:04`
against a true mtime of `11:04:04`, and the dashboard's "SCANNED"
column, the freshness panel and the export all repeated it. It was
found by comparing two sources of the same document, not by reading
the data.

So: aware datetimes, all the way to the insert. There is no reason to
strip the zone -- the driver accepts aware values and gets them right.
"""
from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path

#: Stand-in for "no date", one day after the epoch.
#:
#: Not `1970-01-01`: ClickHouse's `DateTime` starts there, so a missing
#: date and an off-by-a-few-hours timezone slip on a missing date were
#: indistinguishable -- the second clamps to the same value as the
#: first. A day of headroom makes an unset column look unset.
UNSET = datetime(1970, 1, 2, tzinfo=timezone.utc)


def utc(value: datetime | None, default: datetime = UNSET) -> datetime:
    """`value` as an aware UTC datetime, ready to insert.

    A naive input is *assumed* to be UTC rather than converted from
    local time: everything upstream here computes UTC and the ones that
    lost their zone did it to satisfy a requirement that turned out not
    to exist.
    """
    if value is None:
        return default
    aware = (
        value.replace(tzinfo=timezone.utc) if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
    # Whole seconds, because that is all the destination holds.
    # `DateTime` has no sub-second component, so a file mtime of
    # `11:14:39.924281` is stored as `11:14:39` -- and the microseconds
    # that only exist in memory made the same document read from a file
    # and from `raw_documents` compare unequal while the two rows in the
    # database were identical. Truncating here means "same instant in,
    # same row out" is true rather than nearly true.
    return aware.replace(microsecond=0)


def mtime(path: Path, default: datetime = UNSET) -> datetime:
    """When the file was last written, as an aware UTC datetime.

    The fallback for documents that carry no timestamp of their own --
    Syft's output names its tool and version and nothing else.
    """
    try:
        return utc(datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc))
    except OSError:
        return default


def stated(text: str | None) -> datetime | None:
    """An RFC 3339 timestamp a document states about itself.

    Returns None when it cannot be read, because a document that cannot
    be dated is still a document worth ingesting -- the caller falls
    back to the mtime rather than failing.
    """
    if not text:
        return None
    try:
        # SPDX writes a literal Z, which fromisoformat accepts only
        # from 3.11.
        return utc(datetime.fromisoformat(text.replace('Z', '+00:00')))
    except ValueError:
        return None
