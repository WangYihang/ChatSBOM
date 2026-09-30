"""The serving snapshot: an immutable SQLite file per changed pass (#132).

Decisions Q3 and Q11 of #128: the web serves a read-only SQLite file
that the indexer publishes after each pass that changed the data, and
never the warehouse itself, so that indexing never slows serving and a
snapshot's id is an exact cache key. `chatsbom snapshot build` makes
one from `warehouse.duckdb` (#131), and the collector's loop runs it in
each index pass (#150). The web service, `web`, serves it (phase
3).

- **Its schema is D1's** (`schema.py`): the tables the Worker's D1
  store read, which the Python dataset API (`chatsbom/dataset/`)
  answers every view from, held by the contract suite to what D1
  answered. DuckDB computes its rows from the warehouse (`tables.py`),
  and they are D1's, id for id, where the engines agree
  (`tests/snapshot/parity_test.py`). Two tables are added: where §2.4
  asked and a measurement showed the gain, `dependants`, the
  dependants table's rows in the page's order, which the dataset API
  reads a range of; and `agg_edge_ambiguity`, the warehouse's measure
  of how far the edges merge ecosystems, which D1 had nowhere to keep
  (#165).
- **It is written** (`write.py`) under a name of its own in the
  directory it is published in, from DuckDB's results a batch at a
  time through `sqlite3`, indexed, analysed, closed with no journal
  beside it, and made read-only. Its id is the hash of what it holds,
  table by table and row by row, as it is written: the same content is
  the same id, and any other content another.
- **It is published** (`publish.py`) by a rename, then `CURRENT`, which
  says which snapshot is current and which are kept, by another; the
  last three are kept, and one no longer listed is removed only once
  `CURRENT` has moved. Content that is already current publishes
  nothing.
- **It is read** through `chatsbom/dataset/open.py`: `current()` finds
  the file `CURRENT` names, and `open_dataset` opens it read-only and
  immutable.

`build.py` is one pass: the lock, then what a stopped pass left, the
file written, and published. duckdb is imported where a snapshot is
written, not here: the web reads snapshots without it.
"""
