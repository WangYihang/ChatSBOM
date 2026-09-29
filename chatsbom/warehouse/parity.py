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

On the contract corpus, a synthetic one and a store indexed twice
(`tests/warehouse/parity_test.py`) no relation differs. The engines
decide some things differently, by design, and a store that is not as
the collectors leave it can show it:

- **Which Syft scan is current.** ClickHouse's is the commit the
  repository's newest record names; the warehouse's is the commit with
  a Syft document the store had first most recently. They differ when a
  record names a commit with no document, or when commits' manifests
  were fetched out of their order.
- **When a commit was scanned.** ClickHouse dates a commit's rows by its
  Syft document; the warehouse by the earliest of the document and the
  commit's manifests, which the content stage fetches just before, so
  that `sbom generate` writing an older commit's document again after
  an upgrade of Syft moves nothing (`store._first_had`). The month of
  `mv_package_month` differs where the two fell in different months;
  and a document written again, ClickHouse never reads for an older
  commit, where the warehouse has what the newer Syft saw in it.
- **Which graph is current.** ClickHouse's is the fetch `db index` read,
  the newest by when it was fetched; the warehouse's is the newest by
  the instant GitHub states in it. They differ if GitHub ever states an
  earlier instant for a later fetch.
- **The corpus.** ClickHouse's is the newest-dated snapshot a ledger row
  names; the warehouse's, the newest complete snapshot file
  (`core/catalog.py`). They differ when the ledger was seeded from a
  search still running, or when the file is gone; and a repository the
  snapshot lists and the ledger does not track is in the warehouse's
  corpus alone.
- **History.** ClickHouse keeps what every `db index` read; the
  warehouse, what the store still holds. `data prune` takes scans from
  the second and not the first, and a scan made and replaced between
  two passes of `db index` is in the second only: `mv_package_month`
  follows.
- **A document that cannot be parsed** costs ClickHouse the repository,
  whose record fails, and the warehouse that scan alone.
- **The ref of an older commit** is the one its record had in
  ClickHouse, and empty in the warehouse: only the newest decisions,
  or the newest record, are still read, and the layout names a scan by
  its commit.
- **Edges of the layout before `data migrate-layout`**: `db edges` also
  reads graphs kept under a language; the warehouse reads the
  repository-keyed layout alone.

**The records** (`RECORDS`) are compared too: each repository's
releases, what its row says of them, and the ref, its type and the
commit of its current Syft scan, which ClickHouse keeps on the
repository's row and the warehouse on the scan. Where the warehouse
reads them from the release and commit decisions (#147) rather than
from a record, they differ by design in these:

- **A release asset's download count** is ClickHouse's alone: a store's
  release list leaves it out, since it moves on every fetch, and the
  releases are compared without it.
- **A release withdrawn from GitHub** stays in ClickHouse's `releases`,
  which `db index` only adds to; the warehouse has the newest list.
- **A newest record whose releases could not be fetched** has none in
  ClickHouse; the warehouse has the decision the store kept last.
- **A push decided again differently**, without a push between: the
  record `run` landed says the second decision, and the store kept the
  first, which stands (`core/decisions.py`).
"""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
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


def _without_download_counts(row: Row) -> Row:
    """A `releases` row with its assets as a store's release list keeps
    them: no download count, keys in order. `release_assets` is the
    tenth column of `RELEASE_COLUMNS`."""
    assets = row[9]
    try:
        parsed = json.loads(assets)
    except (TypeError, ValueError):
        return row
    if isinstance(parsed, list):
        parsed = [
            {k: v for k, v in asset.items() if k != 'download_count'}
            if isinstance(asset, dict) else asset
            for asset in parsed
        ]
    return (*row[:9], json.dumps(parsed, sort_keys=True), *row[10:])


@dataclass(frozen=True)
class Relation:
    """What each engine is asked for one relation, in its own words."""

    name: str
    columns: str
    clickhouse: str
    warehouse: str
    #: A row as both engines' are compared, past `_normal`.
    normal: Callable[[Row], Row] | None = None


RELEASE_COLUMNS = (
    'repository_id, release_id, tag_name, name, is_prerelease, is_draft, '
    'published_at, target_commitish, created_at, release_assets, source'
)
REPOSITORY_RELEASES = (
    'id, has_releases, latest_release_tag, latest_release_published_at, '
    'total_releases'
)

#: What the records say beside the facts, of the corpus: every release;
#: what a repository's row says of its releases; and the ref, its type
#: and the commit of its current Syft scan (#147).
RECORDS: tuple[Relation, ...] = (
    Relation(
        'releases', RELEASE_COLUMNS,
        clickhouse=(
            f'SELECT {RELEASE_COLUMNS} FROM releases FINAL '
            'WHERE repository_id IN (SELECT id FROM corpus)'
        ),
        warehouse=(
            f'SELECT {RELEASE_COLUMNS} FROM releases '
            'WHERE repository_id IN (SELECT id FROM corpus)'
        ),
        normal=_without_download_counts,
    ),
    Relation(
        'repository_releases', REPOSITORY_RELEASES,
        clickhouse=f'SELECT {REPOSITORY_RELEASES} FROM corpus',
        warehouse=(
            f'SELECT {REPOSITORY_RELEASES} FROM repositories '
            'WHERE id IN (SELECT id FROM corpus)'
        ),
    ),
    Relation(
        'refs', 'id, ref, ref_type, commit_sha',
        clickhouse=(
            'SELECT id, sbom_ref, sbom_ref_type, sbom_commit_sha '
            "FROM corpus WHERE sbom_commit_sha != ''"
        ),
        warehouse=(
            'SELECT repository_id, ref, ref_type, commit_sha '
            "FROM current_scans WHERE source = 'syft'"
        ),
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
    """Every foundation, record and rollup, or those `names`, in both
    engines.

    `clickhouse` is a `clickhouse_connect` client of the database the
    same input went into.
    """
    wanted = set(names) if names is not None else None
    relations: list[Relation] = [
        *(
            Relation(
                name, columns, f'SELECT {columns} FROM {name}',
                f'SELECT {columns} FROM {name}',
            )
            for name, columns in FOUNDATIONS
        ),
        *RECORDS,
        *(Relation(name, '', '', '') for name in REFRESH_ORDER),
    ]
    verdicts = []
    for relation in relations:
        name = relation.name
        if wanted is not None and name not in wanted:
            continue
        columns = relation.columns or ', '.join(_columns(clickhouse, name))
        normal = relation.normal or (lambda row: row)
        theirs = Counter(
            normal(_normal(row)) for row in clickhouse.query(
                relation.clickhouse or f'SELECT {columns} FROM {name}',
            ).result_rows
        )
        ours = Counter(
            normal(_normal(row)) for row in warehouse.execute(
                relation.warehouse or f'SELECT {columns} FROM {name}',
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
