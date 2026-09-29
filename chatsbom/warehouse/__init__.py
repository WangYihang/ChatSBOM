"""The warehouse: an embedded DuckDB file, rebuilt from the store (#131).

Decision Q2 of #128: the analytics move from the ClickHouse server to a
DuckDB file that a pass builds from `data/` alone, and nothing else
writes. It is disposable: delete it and the next pass makes it again,
which is why it is never backed up. Until the cutover it stands beside
ClickHouse, which stays what the dashboard reads, and nothing in the
collector's loop builds it: `chatsbom warehouse build` does, when asked.

What a pass does, in order:

- reads the store with the parsers `db index` uses (`DbService`), so
  one Syft document, manifest or dependency graph is the same rows in
  either engine (`store.py`);
- writes what it read as `scans`, each an input read by one tool, and
  `observations`, what each scan saw, append-only: every scan the store
  holds, not only the newest, with `repositories`, their history,
  `releases` and `edges` (`schema.py`, `writer.py`);
- derives the current facts, the newest scan of each repository and
  source of the corpus, and the rollups from them (`rollups.py`).

It builds into a file of its own and renames it over the last one when
it has finished, so the file is held only for the pass: between passes
the operator's DuckDB CLI can open it, and a reader never sees half a
pass (`build.py`). `parity.py` compares it with ClickHouse on the same
input.

duckdb is imported where a connection is made, not here: the CLI
imports every command at start-up, and only this one needs it.
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

#: The zone every connection works in. Every instant is stored as UTC,
#: in `TIMESTAMP` columns that carry no zone, and each month is made
#: from one (#120). An aware datetime inserted is converted to the
#: session's zone, which was the machine's: on a machine in UTC+8, an
#: instant at 20:00 UTC on 31 January was stored as 1 February.
TIMEZONE = 'UTC'


def connect(
    path: str | Path,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """A connection to the warehouse at `path`, or `:memory:`."""
    import duckdb

    return duckdb.connect(
        str(path), read_only=read_only, config={'TimeZone': TIMEZONE},
    )
