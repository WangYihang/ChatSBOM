"""The warehouse's tables, and the column each row is written by.

The columns are ClickHouse's, which the parsers make (`DbService`),
rearranged by what the warehouse keeps that ClickHouse did not: the
scan.

- An `artifacts` row is an observation and the scan it belongs to.
  What one scan's rows share, its ref, commit and instant, is the
  scan's (`SCAN_COLUMNS`); the rest is the observation's. `repository_id`
  and `source` are both: every rollup groups or filters on them, and
  kept on the observation they cost a few bytes a part.
- A `repositories` row is the metadata, without the columns that point
  at the scan that is current (`SCAN_POINTERS`). In ClickHouse the
  repository row decided which of its scans was current; here it is the
  newest of each source (`rollups.py`), and nothing points.

`schema_test.py` holds both to the rows the parsers make, so a column
added on one side and not the other fails there.

Each table is declared once, as `Column`s: the DDL is made from them,
and so is what `writer.py` writes for a row that lacks a column.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.core.instants import UNSET

if TYPE_CHECKING:
    import duckdb

#: The columns of a parsed `repositories` row, ClickHouse's, that point
#: at the scan `db index` last read: the Syft target, the manifests read
#: for its verdicts, the graph document, and the ecosystems of those.
#: Each is a scan's here (`scans`), or derived from the current ones.
SCAN_POINTERS: tuple[str, ...] = (
    'sbom_ref', 'sbom_ref_type', 'sbom_commit_sha', 'sbom_commit_sha_short',
    'manifest_sources', 'depgraph_observed_at', 'depgraph_ref',
    'depgraph_commit_sha', 'ecosystems',
)

#: An `artifacts` column that is the scan's, by its name on `scans`:
#: every row of one scan has the same value (`writer.py` checks).
SCAN_COLUMNS: dict[str, str] = {
    'sbom_ref': 'ref',
    'sbom_commit_sha': 'commit_sha',
    'observed_at': 'observed_at',
}

#: What an absent instant is, as everywhere else in the schema.
UNSET_INSTANT: datetime = UNSET


@dataclass(frozen=True)
class Column:
    """One column: its DuckDB type, and what a row without it holds."""

    name: str
    type: str
    default: Any = None
    #: NULL allowed: only where the source may not say, and saying
    #: nothing is different from any value.
    nullable: bool = False

    def declaration(self) -> str:
        return (
            f'{self.name} {self.type}'
            + ('' if self.nullable else ' NOT NULL')
        )


def text(name: str) -> Column:
    return Column(name, 'VARCHAR', '')


def integer(name: str, kind: str = 'UBIGINT') -> Column:
    return Column(name, kind, 0)


def flag(name: str) -> Column:
    return Column(name, 'BOOLEAN', False)


def instant(name: str) -> Column:
    """UTC, stored without a zone (`chatsbom.warehouse.TIMEZONE`)."""
    return Column(name, 'TIMESTAMP', UNSET_INSTANT)


def texts(name: str) -> Column:
    return Column(name, 'VARCHAR[]', ())


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    #: Constraints after the columns: a key, or UNIQUE.
    constraints: tuple[str, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def ddl(self) -> str:
        lines = [column.declaration() for column in self.columns]
        lines += list(self.constraints)
        body = ',\n    '.join(lines)
        return f'CREATE TABLE {self.name} (\n    {body}\n)'


#: The metadata of every repository the store names: its newest record,
#: projected by `DbService.parse_repository`, else what the newest
#: complete snapshot to list it says of it.
REPOSITORIES = Table(
    'repositories', (
        Column('id', 'UBIGINT'),
        text('owner'),
        text('repo'),
        text('url'),
        integer('stars'),
        text('description'),
        instant('created_at'),
        text('language'),
        texts('topics'),
        text('default_branch'),
        flag('has_releases'),
        text('latest_release_tag'),
        instant('latest_release_published_at'),
        integer('total_releases', 'UINTEGER'),
        instant('pushed_at'),
        flag('is_archived'),
        flag('is_fork'),
        flag('is_template'),
        flag('is_mirror'),
        integer('disk_usage', 'UINTEGER'),
        integer('fork_count', 'UINTEGER'),
        integer('watchers_count', 'UINTEGER'),
        text('license_spdx_id'),
        text('license_name'),
        text('github_language'),
        text('snapshot'),
    ), ('PRIMARY KEY (id)',),
)

#: The metadata history: what each dated search snapshot the store keeps
#: said of each repository it listed, on its day. The stars, the name,
#: the language, the default branch and the push, as the search saw
#: them; `complete` as `core/catalog.py` judges it.
REPOSITORY_HISTORY = Table(
    'repository_history', (
        Column('id', 'UBIGINT'),
        text('snapshot'),
        instant('observed_at'),
        flag('complete'),
        text('owner'),
        text('repo'),
        Column('stars', 'UBIGINT', nullable=True),
        text('github_language'),
        text('default_branch'),
        Column('pushed_at', 'TIMESTAMP', nullable=True),
    ), ('PRIMARY KEY (id, snapshot)',),
)

#: One input read by one tool: a commit's Syft document or its
#: manifests, keyed by the commit, or one fetch of the dependency graph,
#: keyed by its directory. As the store keys an output, by input and
#: tool@version (#100).
#:
#: `observed_at` is when the store first had the input. Both of a
#: commit's scans carry the earliest of its Syft document's instant and
#: its manifests' (`store._first_had`), not the document's alone as
#: `db index` had it, because the SBOM stage writes an older commit's
#: document again after an upgrade of Syft. A commit with manifests and
#: no document carries the unset instant (`instants.UNSET`), as `db
#: index` dated its declarations, and a graph the instant it states.
#: `observations` is how many rows it saw, zero included: a scan that
#: saw nothing still replaces the one before it.
SCANS = Table(
    'scans', (
        Column('scan_id', 'UINTEGER'),
        Column('repository_id', 'UBIGINT'),
        text('source'),
        text('input_key'),
        text('tool'),
        instant('observed_at'),
        text('ref'),
        text('ref_type'),
        text('commit_sha'),
        text('document'),
        texts('manifest_sources'),
        texts('ecosystems'),
        integer('observations', 'UINTEGER'),
    ), (
        'PRIMARY KEY (scan_id)',
        'UNIQUE (repository_id, source, input_key, tool)',
    ),
)

#: What a scan saw, one row per artifact it reported: ClickHouse's
#: `artifacts` without the scan's columns. `position` is the row's place
#: in what the parser returned. Append-only, and never keyed: nothing
#: is looked up in it by a key, and an index costs every insert.
OBSERVATIONS = Table(
    'observations', (
        Column('scan_id', 'UINTEGER'),
        integer('position', 'UINTEGER'),
        Column('repository_id', 'UBIGINT'),
        text('source'),
        text('artifact_id'),
        text('name'),
        text('version'),
        text('type'),
        text('purl'),
        text('found_by'),
        texts('licenses'),
        Column('relationship', 'VARCHAR', 'unknown'),
        Column('version_kind', 'VARCHAR', 'resolved'),
    ),
)

#: A repository's releases, as its newest record lists them.
RELEASES = Table(
    'releases', (
        Column('repository_id', 'UBIGINT'),
        integer('release_id'),
        text('tag_name'),
        text('name'),
        flag('is_prerelease'),
        flag('is_draft'),
        instant('published_at'),
        text('target_commitish'),
        instant('created_at'),
        Column('release_assets', 'VARCHAR', '[]'),
        Column('source', 'VARCHAR', 'github_release'),
    ), ('PRIMARY KEY (repository_id, tag_name)',),
)

#: Package pairs, and how many repositories' newest graphs show each:
#: the count `db edges` made, by the same code (`core/edges.py`).
EDGES = Table(
    'edges', (
        text('parent'),
        text('child'),
        integer('repositories'),
        instant('observed_at'),
    ), ('PRIMARY KEY (child, parent)',),
)

#: The repositories of the newest complete search snapshot, or every
#: repository when the store has none (owner decision D2 on #55).
CORPUS = Table(
    'corpus', (
        Column('id', 'UBIGINT'),
    ), ('PRIMARY KEY (id)',),
)

#: One row: what this pass built, from what. `corpus` names the
#: snapshot, '' when the store has none; `unreadable` counts documents
#: that could not be parsed and were left out; `unnamed`, repositories
#: with outputs in the store and no metadata anywhere, left out too.
BUILD = Table(
    'build', (
        text('version'),
        instant('built_at'),
        text('store'),
        text('corpus'),
        integer('repositories'),
        integer('scans'),
        integer('observations'),
        integer('unreadable'),
        integer('unnamed'),
    ),
)

TABLES: tuple[Table, ...] = (
    REPOSITORIES, REPOSITORY_HISTORY, SCANS, OBSERVATIONS, RELEASES,
    EDGES, CORPUS, BUILD,
)

BY_NAME: dict[str, Table] = {table.name: table for table in TABLES}


def create(con: duckdb.DuckDBPyConnection) -> None:
    """Every table, empty. A pass starts from a new file, so there is
    nothing to migrate: a changed table is a changed declaration."""
    for table in TABLES:
        con.execute(table.ddl())
