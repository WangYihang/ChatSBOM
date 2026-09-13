"""Column contracts for ClickHouse inserts.

ClickHouse's insert API takes positional rows plus a separate column list.
Building those rows by hand couples the producer to the column order, and
a single inserted column silently shifts every downstream index. This
module keeps the order in one place: producers emit mappings keyed by
column name, and `Table.row` projects them.
"""
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Table:
    """An ordered column contract for a single ClickHouse table."""

    name: str
    columns: tuple[str, ...]

    @property
    def column_names(self) -> list[str]:
        """Column list in insert order, as `clickhouse_connect` expects it."""
        return list(self.columns)

    def row(self, values: Mapping[str, Any]) -> list[Any]:
        """Project a column-keyed mapping into a positional row.

        Raises KeyError when the mapping does not cover exactly the
        declared columns, so a schema change fails loudly at the seam
        instead of shifting values into neighbouring columns.
        """
        missing = [c for c in self.columns if c not in values]
        if missing:
            raise KeyError(
                f"{self.name}: missing column(s) {', '.join(missing)}",
            )

        unknown = [k for k in values if k not in self.columns]
        if unknown:
            raise KeyError(
                f"{self.name}: unknown column(s) {', '.join(sorted(unknown))}",
            )

        return [values[c] for c in self.columns]

    def rows(self, records: Sequence[Mapping[str, Any]]) -> list[list[Any]]:
        """Project many mappings, preserving input order."""
        return [self.row(r) for r in records]
