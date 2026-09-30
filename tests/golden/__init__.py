"""Answers recorded from ClickHouse before it was deleted (#153).

The warehouse, the snapshot and the Parquet export were each held to
ClickHouse on the same input while the two engines stood side by side
(#141, #146, #148): an independent engine, whose own rollups
`scripts/verify_rollups.py` held to answers computed another way. With
the server gone, what ClickHouse answered on those inputs is kept here,
beside this module, and the tests hold the warehouse to it.

A relation is kept as JSON: its columns, its rows as `canonical` makes
them, their count, and their SHA-256, so that one too large to read in
a diff is still held to the row. A table compared as a multiset has its
rows sorted; one compared in the order it was written keeps that order.
A row is a line, so that a row that changes is a line of the diff:
pre-commit's JSON formatter, which would put each value on a line of
its own, leaves these files alone. Each test's docstring says how its
fixtures were recorded.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from collections.abc import Sequence
from datetime import date
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from chatsbom.core.instants import utc

GOLDEN = Path(__file__).resolve().parent

#: A relation of more rows than this keeps its count and digest alone.
INLINE = 250

Row = list[Any]


def canonical(value: Any) -> Any:
    """`value` as JSON holds it, and alike whichever engine gave it: a
    flag as its number, an instant as UTC, a whole float as an integer,
    and an array as a list."""
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        return utc(value).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, (list, tuple)):
        return [canonical(item) for item in value]
    if isinstance(value, dict):
        return {str(key): canonical(item) for key, item in value.items()}
    return value


def row(values: Iterable[Any]) -> Row:
    return [canonical(value) for value in values]


def text(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
    )


def ordered(rows: Iterable[Iterable[Any]]) -> list[Row]:
    """Rows in the order they were written."""
    return [row(values) for values in rows]


def multiset(rows: Iterable[Iterable[Any]]) -> list[Row]:
    """Rows as a multiset: sorted, a row twice kept twice."""
    return sorted((row(values) for values in rows), key=text)


def digest(rows: Sequence[Row]) -> str:
    return hashlib.sha256(text(list(rows)).encode('utf-8')).hexdigest()


def relation(columns: Sequence[str], rows: Sequence[Row]) -> dict[str, Any]:
    """A relation as a fixture keeps it."""
    kept: dict[str, Any] = {
        'columns': list(columns),
        'count': len(rows),
        'sha256': digest(rows),
    }
    if len(rows) <= INLINE:
        kept['rows'] = list(rows)
    return kept


def load(name: str) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(
        (GOLDEN / name).read_text(encoding='utf-8'),
    )
    return loaded


def dump(name: str, content: dict[str, Any]) -> Path:
    """For the recorder alone: the fixture, as `load` reads it, a row a
    line."""
    kept: list[list[Row]] = []

    def placed(value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        out: dict[str, Any] = {}
        for key, item in value.items():
            if key == 'rows':
                kept.append(item)
                out[key] = f'<rows {len(kept) - 1}>'
            else:
                out[key] = placed(item)
        return out

    written = json.dumps(
        placed(content), ensure_ascii=False, indent=1, sort_keys=True,
    )
    for index, rows in enumerate(kept):
        lines = ',\n'.join(f'    {text(values)}' for values in rows)
        written = written.replace(
            f'"<rows {index}>"', f'[\n{lines}\n   ]' if rows else '[]',
        )
    path = GOLDEN / name
    path.write_text(written + '\n', encoding='utf-8')
    assert load(name) == content
    return path


def mismatch(
    name: str,
    kept: dict[str, Any],
    rows: Sequence[Row],
    limit: int = 5,
) -> str:
    """What differs between a kept relation and `rows`, for a failure."""
    lines = [
        f'{name}: {len(rows)} rows, {kept["count"]} kept; '
        f'sha256 {digest(rows)[:12]}, {kept["sha256"][:12]} kept',
    ]
    if 'rows' in kept:
        theirs = [text(r) for r in kept['rows']]
        ours = [text(r) for r in rows]
        for label, first, second in (
            ('kept, not here', theirs, ours),
            ('here, not kept', ours, theirs),
        ):
            rest = list(second)
            found = []
            for item in first:
                if item in rest:
                    rest.remove(item)
                else:
                    found.append(item)
            lines += [f'    {label}: {item}' for item in found[:limit]]
    else:
        lines += [f'    here: {text(r)}' for r in rows[:limit]]
    return '\n'.join(lines)


def holds(name: str, kept: dict[str, Any], rows: Sequence[Row]) -> bool:
    return len(rows) == kept['count'] and digest(rows) == kept['sha256']
