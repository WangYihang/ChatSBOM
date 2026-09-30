"""ClickHouse's rows, loaded as the warehouse's: for the golden checks.

A seeded corpus such as the contract suite's (`tests/contract`) is
written as `repositories`, `artifacts` and `edges` rows, not as a store.
Both engines were run over such rows, ClickHouse taking them as they
are, and what it answered was recorded before the server went
(`tests/golden/`, #153). The warehouse takes them through this, which
groups `artifacts` rows into the scans ClickHouse told apart: a Syft or
manifest scan by its commit, a dependency graph by the instant its
document states (#22).

What rows cannot say, they do not: a scan that saw nothing has no row,
so it is not here, and the tool that made a scan is unknown. A corpus
meant for both engines has neither. A repository's ecosystems are its
current scans' as `ecosystems_of` reads their rows, which is what `db
index` writes in `repositories.ecosystems` when no manifest names one
the rows do not.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.core.instants import utc
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.services.db_service import ecosystems_of
from chatsbom.warehouse import schema
from chatsbom.warehouse.writer import Scan
from chatsbom.warehouse.writer import Writer

if TYPE_CHECKING:
    import duckdb


def load(
    con: duckdb.DuckDBPyConnection,
    repositories: Iterable[Mapping[str, Any]],
    artifacts: Iterable[Mapping[str, Any]],
    edges: Iterable[tuple[str, str, int, datetime]] = (),
    corpus: Iterable[int] | None = None,
    releases: Iterable[Mapping[str, Any]] = (),
) -> None:
    """The tables, made and filled from ClickHouse-shaped rows.

    `corpus` is the ids of the corpus, None for every repository, as
    ClickHouse's is while no repository names a snapshot. A scan's ref
    type is its repository row's, at the commit the row names: an
    `artifacts` row has no column for it.
    """
    schema.create(con)
    repositories = list(repositories)
    ref_types = {
        (int(row['id']), str(row.get('sbom_commit_sha') or '')):
            str(row.get('sbom_ref_type') or '')
        for row in repositories
    }
    scans: dict[tuple[int, str, str], list[Mapping[str, Any]]] = {}
    for row in artifacts:
        scans.setdefault(_scan_key(row), []).append(row)
    with Writer(con) as writer:
        writer.extend('repositories', repositories)
        writer.extend('releases', releases)
        for (repository_id, source, key), rows in sorted(scans.items()):
            first = rows[0]
            commit = str(first.get('sbom_commit_sha') or '')
            writer.scan(
                Scan(
                    repository_id=repository_id,
                    source=source,
                    input_key=key,
                    tool='',
                    observed_at=utc(first['observed_at']),
                    ref=str(first.get('sbom_ref') or ''),
                    ref_type=(
                        ref_types.get((repository_id, commit), '')
                        if commit and source != DEPGRAPH else ''
                    ),
                    commit_sha=commit,
                    ecosystems=ecosystems_of(rows),
                    rows=rows,
                ),
            )
        writer.extend(
            'edges', (
                {
                    'parent': parent, 'child': child, 'repositories': count,
                    'observed_at': observed_at,
                }
                for parent, child, count, observed_at in edges
            ),
        )
        ids = (
            {int(r['id']) for r in repositories} if corpus is None
            else set(corpus)
        )
        writer.extend('corpus', ({'id': i} for i in sorted(ids)))


def _scan_key(row: Mapping[str, Any]) -> tuple[int, str, str]:
    """Which scan a row is of, as ClickHouse tells them apart; with the
    instant for a commit too, so that one commit's rows written on two
    days, which `forget_scans` keeps from happening, would be two scans
    rather than one with two dates."""
    instant = utc(row['observed_at']).isoformat()
    source = str(row['source'])
    if source == DEPGRAPH:
        return int(row['repository_id']), source, instant
    commit = str(row.get('sbom_commit_sha') or '')
    return int(row['repository_id']), source, f'{commit}@{instant}'
