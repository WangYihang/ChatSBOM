"""The warehouse against ClickHouse, rollup by rollup, on one input.

Until the cutover (#128 Appendix B, phase 4) the two engines stand side
by side, and every answer the dashboard reads from ClickHouse has to be
the warehouse's too. This asks each engine for every row of every
rollup ClickHouse has (`core/rollups.REFRESH_ORDER`), and of what they
are built on, the corpus, its language buckets and the current facts,
and compares the rows as multisets: order is not an answer, a row twice
is.

ClickHouse's rollups are themselves held to answers computed another
way by `scripts/verify_rollups.py`, which stays the oracle; this holds
the warehouse to ClickHouse. A difference is fixed, or explained where
the two engines are meant to differ.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.core.instants import utc
from chatsbom.core.rollups import REFRESH_ORDER

if TYPE_CHECKING:
    import duckdb

Row = tuple[Any, ...]

#: What the rollups are built on, compared first, so that a rollup that
#: disagrees can be told from the facts it was made of. The columns are
#: ClickHouse's views'.
FOUNDATIONS: tuple[tuple[str, str], ...] = (
    ('corpus', 'id'),
    ('language_buckets', 'language, repositories'),
    (
        'facts',
        'repository_id, name, version, type, found_by, relationship, '
        'source, version_kind',
    ),
)


@dataclass(frozen=True)
class Verdict:
    """One relation, compared."""

    name: str
    columns: str
    #: Rows ClickHouse holds.
    rows: int
    #: Rows ClickHouse holds and the warehouse does not, and the other
    #: way, each as often as it is missing.
    missing: tuple[Row, ...]
    extra: tuple[Row, ...]

    @property
    def agrees(self) -> bool:
        return not self.missing and not self.extra


def compare(
    warehouse: duckdb.DuckDBPyConnection,
    clickhouse: Any,
    names: Iterable[str] | None = None,
) -> list[Verdict]:
    """Every foundation and rollup, or those `names`, in both engines.

    `clickhouse` is a `clickhouse_connect` client of the database the
    same input went into.
    """
    wanted = set(names) if names is not None else None
    relations = [
        *FOUNDATIONS,
        *((name, '') for name in REFRESH_ORDER),
    ]
    verdicts = []
    for name, columns in relations:
        if wanted is not None and name not in wanted:
            continue
        columns = columns or ', '.join(_columns(clickhouse, name))
        theirs = Counter(
            _normal(row) for row in clickhouse.query(
                f'SELECT {columns} FROM {name}',
            ).result_rows
        )
        ours = Counter(
            _normal(row) for row in warehouse.execute(
                f'SELECT {columns} FROM {name}',
            ).fetchall()
        )
        verdicts.append(
            Verdict(
                name=name,
                columns=columns,
                rows=sum(theirs.values()),
                missing=tuple(sorted((theirs - ours).elements(), key=repr)),
                extra=tuple(sorted((ours - theirs).elements(), key=repr)),
            ),
        )
    return verdicts


def report(verdicts: Sequence[Verdict], limit: int = 5) -> str:
    """The verdicts, a line each, and the first rows of each difference."""
    lines = []
    for verdict in verdicts:
        mark = 'ok' if verdict.agrees else 'DIFFERS'
        lines.append(f'{verdict.name:<28} {mark:<8} {verdict.rows:>9,} rows')
        for label, found in (
            ('only in ClickHouse', verdict.missing),
            ('only in the warehouse', verdict.extra),
        ):
            for row in found[:limit]:
                lines.append(f'    {label}: {row}')
            if len(found) > limit:
                lines.append(f'    {label}: {len(found) - limit} more')
    return '\n'.join(lines)


def _columns(clickhouse: Any, name: str) -> list[str]:
    """A table's columns, in ClickHouse's order."""
    return [
        str(column) for (column,) in clickhouse.query(
            'SELECT name FROM system.columns '
            'WHERE database = currentDatabase() AND table = {name:String} '
            'ORDER BY position',
            parameters={'name': name},
        ).result_rows
    ]


def _normal(row: Sequence[Any]) -> Row:
    """A row as both engines' values compare: a flag as its number, an
    instant as an aware UTC one, whichever engine gave it with a zone."""
    return tuple(_value(value) for value in row)


def _value(value: Any) -> Any:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, datetime):
        # The warehouse's are UTC with no zone, and `instants.utc` reads
        # a naive value as UTC.
        return utc(value)
    if isinstance(value, list):
        return tuple(value)
    return value
