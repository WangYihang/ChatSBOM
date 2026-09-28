"""Export the dataset as a D1-shaped SQLite database and its SQL.

Three constraints drive the shape here, all measured on the real corpus
rather than assumed.

**Size.** Translating the Parquet schema directly into SQLite produced
762.6 MB once the indexes the queries need were present. Normalising
the repeated strings brought that to 291.5 MB — 62% smaller with no rows
lost — because the cardinalities are tiny next to the row count, as
they were measured then:

    6,062,896 artifact rows
      141,938 distinct package names
       46,526 distinct versions
           45 distinct combinations of type / found_by / relationship /
              source / version_kind

That last line is where most of the saving is. Five strings were stored
on every one of six million rows to express one of forty-five
possibilities. The corpus has grown since: at 16.8 million artifact rows
the normalised database is 831 MB applied, over D1's 500 MB free tier
and inside the 10 GB of its paid plan.

**Statement length.** D1 caps a single SQL statement at 100,000 bytes,
and `sqlite3 .dump` writes one INSERT per row — 6,062,896 statements,
slow to execute over a network even before the size is considered. Rows
are batched into multi-row INSERTs sized to stay under the cap, in
bytes: a character of a Chinese description is three of them.

**Retries.** An import goes over a network, one `wrangler d1 execute`
per file, and the way to recover from a failure — or from a timeout
that had in fact gone through — is to run the file again. So every
script can be applied again without changing the result, and the data
is cut into parts, each of which can be, rather than one file of about
450 MB that a failure sent back to the start. (The SQL is about half
the size of the database it makes: an artifact row is four integers,
some 27 bytes as SQL, and SQLite stores it with its indexes.)

The Parquet export stays, as a copy of the dataset to read directly;
nothing serves it.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import NamedTuple
from typing import TextIO
from typing import TypeVar

import structlog

from chatsbom.__version__ import __version__
from chatsbom.core.ecosystems import canonical
from chatsbom.core.repository import QueryRepository
from chatsbom.export.queries import D1_LICENSES_QUERY
from chatsbom.export.queries import EXPORT_SETTINGS
from chatsbom.export.queries import QUERIES
from chatsbom.export.queries import repository_freshness
from chatsbom.export.queries import whole
from chatsbom.export.schema import SCHEMA_VERSION
from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.provenance import VERSION_KINDS
from chatsbom.models.relationship import RELATIONSHIPS

logger = structlog.get_logger('export_d1')

K = TypeVar('K')

#: D1's documented cap on one SQL statement, in bytes. Batches target a
#: fraction of it so a wide row cannot push a batch over.
MAX_STATEMENT_BYTES = 100_000

#: Leaves room for the column list and a long final row.
_BATCH_BUDGET = MAX_STATEMENT_BYTES // 2

#: Default rows per statement for narrow tables. Wide rows are cut
#: earlier by the byte budget.
DEFAULT_BATCH = 500

#: Bytes a part of the data script is cut at. A failed part is what an
#: import does again, so this bounds what a failure costs: the data was
#: one file of about 450 MB, and a failure anywhere in it meant all of
#: it again.
CHUNK_BYTES = 50_000_000

#: Parts one table may be cut into. Four digits keep the order of their
#: names the order they are applied in: `02-artifacts-10000.sql` would
#: sort before `02-artifacts-9999.sql`.
MAX_PARTS = 9_999


def _one_of(values: Iterable[str]) -> str:
    """A column's description, listing the values it holds.

    Taken from the types that define them. Written out by hand,
    `version_kind` said `exact | range | unknown`, which it never held,
    and `source` did not have `manifest` once schema version 7 added it.
    """
    return ' | '.join(values) + '.'


@dataclass(frozen=True)
class D1Column:
    name: str
    type: str
    description: str


@dataclass(frozen=True)
class D1Table:
    name: str
    description: str
    columns: tuple[D1Column, ...]
    primary_key: str | None = None
    #: Columns that must be unique, so a lookup returns one row.
    unique: tuple[str, ...] = ()

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def ddl(self) -> str:
        lines = [f'  {c.name} {c.type}' for c in self.columns]
        if self.primary_key:
            lines.append(f'  PRIMARY KEY ({self.primary_key})')
        body = ',\n'.join(lines)
        return f'CREATE TABLE {self.name} (\n{body}\n);'


@dataclass(frozen=True)
class D1Index:
    table: str
    columns: tuple[str, ...]
    unique: bool = False

    @property
    def name(self) -> str:
        return f'idx_{self.table}_{"_".join(self.columns)}'

    def ddl(self) -> str:
        # `IF NOT EXISTS`, so that the script can be applied again: a
        # retried `04-indexes.sql` failed on its first line otherwise.
        kind = 'UNIQUE INDEX' if self.unique else 'INDEX'
        cols = ', '.join(self.columns)
        return (
            f'CREATE {kind} IF NOT EXISTS {self.name} '
            f'ON {self.table}({cols});'
        )


@dataclass(frozen=True)
class D1Schema:
    tables: tuple[D1Table, ...] = field(default=())
    indexes: tuple[D1Index, ...] = field(default=())

    def table(self, name: str) -> D1Table:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f'no D1 table named {name!r}')


PACKAGES = D1Table(
    name='packages',
    description='Distinct package names, referenced by artifacts.',
    primary_key='id',
    unique=('name',),
    columns=(
        D1Column('id', 'INTEGER', 'Surrogate key.'),
        D1Column(
            'name', 'TEXT NOT NULL',
            'Package name as the ecosystem spells it.',
        ),
        D1Column(
            'repositories', 'INTEGER NOT NULL DEFAULT 0',
            'Repositories depending on it. Filled by the aggregates.',
        ),
    ),
)

VERSIONS = D1Table(
    name='versions',
    description='Distinct version strings, referenced by artifacts.',
    primary_key='id',
    unique=('version',),
    columns=(
        D1Column('id', 'INTEGER', 'Surrogate key.'),
        D1Column('version', 'TEXT NOT NULL', 'Version as resolved.'),
    ),
)

KINDS = D1Table(
    name='kinds',
    description=(
        'The 45 observed combinations of the five low-cardinality '
        'columns, referenced by artifacts instead of repeated per row.'
    ),
    primary_key='id',
    columns=(
        D1Column('id', 'INTEGER', 'Surrogate key.'),
        D1Column(
            'type', 'TEXT NOT NULL',
            'Canonical ecosystem, e.g. gem, npm or maven: one name per '
            'registry, however the collector spelled it.',
        ),
        D1Column('found_by', 'TEXT NOT NULL', 'Cataloguer that reported it.'),
        D1Column('relationship', 'TEXT NOT NULL', _one_of(RELATIONSHIPS)),
        D1Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        D1Column('version_kind', 'TEXT NOT NULL', _one_of(VERSION_KINDS)),
    ),
)

ARTIFACTS = D1Table(
    name='artifacts',
    description='One row per observed dependency, as four integers.',
    columns=(
        D1Column(
            'repository_id', 'INTEGER NOT NULL',
            'Repository it was found in.',
        ),
        D1Column('package_id', 'INTEGER NOT NULL', 'References packages.id.'),
        D1Column('version_id', 'INTEGER NOT NULL', 'References versions.id.'),
        D1Column('kind_id', 'INTEGER NOT NULL', 'References kinds.id.'),
    ),
)

#: When each source last observed each repository: the date the
#: dependants table shows beside a row (#41).
#:
#: A repository's own `observed_at` is its newest observation from any
#: source, and dating every row by it put September beside a February
#: Syft scan whenever the dependency graph came later — ClickHouse
#: dates each row by its own (#24). The rows reference their source
#: through `kinds`, so the date is a join on `(repository_id, source)`,
#: kept here once per pair rather than on six million rows.
#:
#: Unkeyed, so its rows are placed by rowid and a part of the data
#: script re-applies as every other unkeyed table's does; the unique
#: index on the pair is what the dependants' join looks up.
OBSERVATIONS = D1Table(
    name='observations',
    description=(
        'When each source last observed each repository: the date of its '
        'current observation, per repository and source.'
    ),
    columns=(
        D1Column(
            'repository_id', 'INTEGER NOT NULL',
            'References repositories.id.',
        ),
        D1Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        D1Column(
            'observed_at', 'TEXT NOT NULL',
            'Its current observation by that source, as a UTC date, '
            'YYYY-MM-DD.',
        ),
    ),
)

#: The rows of `observations`. From `current_artifacts`, as the facts
#: are, so a row the export writes always has its date; the newest of a
#: source's current rows, which are one scan's or one document's and so
#: carry one instant. In UTC by name, as every date the export writes
#: (`export/queries.py`).
OBSERVATIONS_QUERY = """
SELECT
    repository_id,
    source,
    formatDateTime(max(observed_at), '%Y-%m-%d', 'UTC') AS observed_at
FROM current_artifacts
GROUP BY repository_id, source
ORDER BY repository_id ASC, source ASC
""".strip()

REPOSITORIES = D1Table(
    name='repositories',
    description=(
        'One row per repository of the current search snapshot, '
        'collected or not.'
    ),
    primary_key='id',
    columns=(
        D1Column('id', 'INTEGER', 'GitHub repository id.'),
        D1Column('owner', 'TEXT NOT NULL', 'Repository owner login.'),
        D1Column('repo', 'TEXT NOT NULL', 'Repository name.'),
        D1Column(
            'stars', 'INTEGER NOT NULL',
            'Star count at collection time.',
        ),
        D1Column(
            'language', 'TEXT NOT NULL',
            "GitHub's primary language, lowercased.",
        ),
        D1Column(
            'github_language', 'TEXT NOT NULL',
            "GitHub's primary language, as GitHub spells it.",
        ),
        D1Column(
            'language_bucket', 'TEXT NOT NULL',
            'The language folded for display: one of the top twelve, '
            "'other' or 'none'. What the language filter matches.",
        ),
        D1Column(
            'ecosystems', 'TEXT NOT NULL',
            'Canonical ecosystems of the current scan, as a JSON array.',
        ),
        D1Column('url', 'TEXT NOT NULL', 'Repository URL.'),
        D1Column('description', 'TEXT NOT NULL', 'Repository description.'),
        D1Column(
            'license_spdx_id', 'TEXT NOT NULL',
            'SPDX licence id, or empty.',
        ),
        D1Column(
            'pushed_at', 'TEXT NOT NULL',
            'Last push upstream, YYYY-MM-DD.',
        ),
        D1Column(
            'observed_at', 'TEXT NOT NULL',
            'When this pipeline last scanned it.',
        ),
        D1Column(
            'sbom_ref', 'TEXT NOT NULL',
            'Tag or branch the SBOM came from.',
        ),
        D1Column(
            'sbom_commit_sha', 'TEXT NOT NULL',
            'Commit the SBOM describes.',
        ),
        D1Column(
            'direct_dependencies',
            'INTEGER NOT NULL', 'Declared packages.',
        ),
        D1Column(
            'total_dependencies', 'INTEGER NOT NULL',
            'Resolved closure size.',
        ),
    ),
)

LICENSES = D1Table(
    name='licenses',
    description='Licence shares, precomputed.',
    columns=(
        D1Column('license', 'TEXT NOT NULL', 'SPDX id, or empty for unknown.'),
        D1Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories carrying it.',
        ),
        D1Column(
            'package_count', 'INTEGER NOT NULL',
            'Distinct packages carrying it.',
        ),
    ),
)

HISTORY = D1Table(
    name='history',
    description=(
        'Monthly adoption series per package, per source. Two sources '
        'measure differently, so a series that mixed them would show a '
        'change of instrument as a change in adoption.'
    ),
    columns=(
        D1Column('name', 'TEXT NOT NULL', 'Package name.'),
        D1Column('month', 'TEXT NOT NULL', 'YYYY-MM.'),
        D1Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        D1Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories depending on it.',
        ),
        D1Column(
            'direct_count', 'INTEGER NOT NULL',
            'Of those, declaring it.',
        ),
    ),
)


# ---------------------------------------------------------------------
# Precomputed aggregates.
#
# The overview's panels are fixed aggregates over the whole corpus, and
# measuring them on the normalised schema with indexes in place gave:
#
#     sourceComparison     3122 ms   SCAN r SEARCH a SEARCH k
#     relationshipSplit    1082 ms   SCAN a SEARCH k
#     topPackages           376 ms   SCAN p SEARCH a SEARCH k
#     totals                 21 ms   SCAN artifacts
#
# No index helps: each reads all 6,062,896 artifact rows by definition.
# On D1 that is the bill as much as the latency — it charges for rows
# read, and the overview is the first thing every visitor loads. These
# panels take no free-text parameters and return tens of rows, so they
# are computed once at export time and selected at request time.
#
# The point lookups are deliberately *not* precomputed: `dependentsOf`
# and `countDependents` already answer in 4 ms straight off the indexes,
# and they take an arbitrary package name, so there is nothing finite to
# precompute.
# ---------------------------------------------------------------------

AGG_TOTALS = D1Table(
    name='agg_totals',
    description='The numbers the tiles and the footer show. One row.',
    columns=(
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories with dependency data.',
        ),
        D1Column('dependencies', 'INTEGER NOT NULL', 'Dependency records.'),
        D1Column('packages', 'INTEGER NOT NULL', 'Distinct packages.'),
        D1Column(
            'classified', 'INTEGER NOT NULL',
            'Records with a known relationship.',
        ),
        D1Column(
            'tracked', 'INTEGER NOT NULL',
            'Repositories in the current search snapshot, collected or '
            'not: the denominator of every coverage ratio.',
        ),
    ),
)

AGG_RELATIONSHIP_SPLIT = D1Table(
    name='agg_relationship_split',
    description=(
        'Declared / inherited / undetermined, per ecosystem and overall.'
    ),
    columns=(
        D1Column(
            'ecosystem', 'TEXT NOT NULL',
            "Canonical ecosystem, or '' for the whole corpus.",
        ),
        D1Column('relationship', 'TEXT NOT NULL', _one_of(RELATIONSHIPS)),
        D1Column('records', 'INTEGER NOT NULL', 'Dependency records.'),
    ),
)

#: Coverage by the repository's GitHub language, folded (D7).
AGG_LANGUAGE_COVERAGE = D1Table(
    name='agg_language_coverage',
    description=(
        'Repositories per GitHub language (top twelve, other, none), and '
        'how many each source covers.'
    ),
    columns=(
        D1Column(
            'language', 'TEXT NOT NULL',
            "Language bucket: a top-twelve language, 'other' or 'none'.",
        ),
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories of the snapshot in that bucket.',
        ),
        D1Column(
            'with_sbom', 'INTEGER NOT NULL',
            'Of those, with dependencies recorded by any source.',
        ),
        D1Column(
            'with_syft', 'INTEGER NOT NULL',
            'Of those, with a Syft scan.',
        ),
        D1Column(
            'with_depgraph', 'INTEGER NOT NULL',
            "Of those, with GitHub's dependency graph.",
        ),
        D1Column(
            'with_manifest', 'INTEGER NOT NULL',
            'Of those, with Gradle build-file declarations.',
        ),
    ),
)

#: Coverage per ecosystem. Rows overlap: a repository counts under
#: every ecosystem it has, so they must not be summed.
AGG_ECOSYSTEM_COVERAGE = D1Table(
    name='agg_ecosystem_coverage',
    description=(
        'Per ecosystem: repositories that have it, and how many of those '
        'each source covers.'
    ),
    columns=(
        D1Column('ecosystem', 'TEXT NOT NULL', 'Canonical ecosystem.'),
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories whose artifacts or manifests are of it.',
        ),
        D1Column(
            'with_any', 'INTEGER NOT NULL',
            'Of those, with a dependency record of it from any source.',
        ),
        D1Column('with_syft', 'INTEGER NOT NULL', 'Of those, from Syft.'),
        D1Column(
            'with_depgraph', 'INTEGER NOT NULL',
            "Of those, from GitHub's dependency graph.",
        ),
        D1Column(
            'with_manifest', 'INTEGER NOT NULL',
            'Of those, from Gradle build files.',
        ),
    ),
)

AGG_TOP_PACKAGES = D1Table(
    name='agg_top_packages',
    description=(
        'The ranking, precomputed per filter combination: the panel has '
        'exactly two controls, so the set of answers is finite.'
    ),
    columns=(
        D1Column(
            'direct_only', 'INTEGER NOT NULL',
            '1 when counting declarations only.',
        ),
        D1Column(
            'ecosystem', 'TEXT NOT NULL',
            "Ecosystem filter, or '' for all.",
        ),
        D1Column('rank', 'INTEGER NOT NULL', '1-based position.'),
        D1Column('name', 'TEXT NOT NULL', 'Package name.'),
        D1Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories depending on it.',
        ),
        D1Column(
            'direct_count', 'INTEGER NOT NULL',
            'Of those, declaring it.',
        ),
    ),
)

AGG_DEPENDENCY_BUCKETS = D1Table(
    name='agg_dependency_buckets',
    description='Repositories per dependency-count bucket.',
    columns=(
        D1Column('bucket', 'TEXT NOT NULL', 'Bucket label, e.g. 100-249.'),
        D1Column(
            'position', 'INTEGER NOT NULL',
            'Sort order, since labels are not ordinal.',
        ),
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories in the bucket.',
        ),
    ),
)

AGG_SOURCE_COMPARISON = D1Table(
    name='agg_source_comparison',
    description='Dependency records per ecosystem, split by collector.',
    columns=(
        D1Column('ecosystem', 'TEXT NOT NULL', 'Canonical ecosystem.'),
        D1Column('syft', 'INTEGER NOT NULL', 'Records from syft.'),
        D1Column(
            'depgraph', 'INTEGER NOT NULL',
            "Records from GitHub's dependency graph.",
        ),
        D1Column(
            'manifest', 'INTEGER NOT NULL',
            'Records from Gradle build files.',
        ),
    ),
)


AGG_EDGES = D1Table(
    name='agg_edges',
    description=(
        'Package-to-package dependency edges, aggregated by name: how '
        'many repositories show this parent pulling in this child.'
    ),
    columns=(
        D1Column('parent_id', 'INTEGER NOT NULL', 'References packages.id.'),
        D1Column('child_id', 'INTEGER NOT NULL', 'References packages.id.'),
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories in which the parent pulls in the child.',
        ),
    ),
)

META = D1Table(
    name='meta',
    description=(
        'Provenance, for the same debugging the Parquet manifest serves: '
        'which build produced this and how fresh the rows are. One row.'
    ),
    columns=(
        D1Column(
            'generator', 'TEXT NOT NULL',
            'Build that produced the data.',
        ),
        D1Column(
            'schema_version', 'TEXT NOT NULL',
            'Export contract version.',
        ),
        D1Column(
            'observed_from', 'TEXT NOT NULL',
            'Earliest observation date.',
        ),
        D1Column('observed_to', 'TEXT NOT NULL', 'Latest observation date.'),
    ),
)

D1_SCHEMA = D1Schema(
    tables=(
        REPOSITORIES, ARTIFACTS, OBSERVATIONS, PACKAGES, VERSIONS, KINDS,
        LICENSES, HISTORY, AGG_TOTALS, AGG_RELATIONSHIP_SPLIT,
        AGG_LANGUAGE_COVERAGE, AGG_ECOSYSTEM_COVERAGE, AGG_TOP_PACKAGES,
        AGG_DEPENDENCY_BUCKETS, AGG_SOURCE_COMPARISON, AGG_EDGES, META,
    ),
    indexes=(
        # Without these the joins table-scan six million rows.
        D1Index('artifacts', ('package_id',)),
        D1Index('artifacts', ('repository_id',)),
        # A dependants row's date: one lookup per row.
        D1Index('observations', ('repository_id', 'source'), unique=True),
        D1Index('packages', ('name',), unique=True),
        D1Index('versions', ('version',), unique=True),
        # What the dependants' language filter matches.
        D1Index('repositories', ('language_bucket',)),
        D1Index('history', ('name',)),
        # Aggregates are indexed by what their panel filters on, so a
        # request is a lookup rather than a scan of the aggregate.
        D1Index('agg_top_packages', ('direct_only', 'ecosystem', 'rank')),
        D1Index('agg_relationship_split', ('ecosystem',)),
        # Both directions. "What does X pull in" and "what pulls in X"
        # are different questions and the second is the more useful one
        # — it is how you find out why a package you never chose is in
        # your lockfile.
        D1Index('agg_edges', ('parent_id',)),
        D1Index('agg_edges', ('child_id',)),
    ),
)


def _d1_value(value: object) -> object:
    """A ClickHouse value as D1 stores it: an array as its JSON text,
    which SQLite's `json_each` reads (`repositories.ecosystems`)."""
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), separators=(',', ':'))
    return value


def sql_literal(value: object) -> str:
    """Render a Python value as a SQLite literal.

    Doubling the quote is SQLite's own escape, and it is needed: a
    package really can be called `O'Reilly`.
    """
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def batch_inserts(
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
    batch: int = DEFAULT_BATCH,
) -> Iterator[str]:
    """Group rows into multi-row INSERTs that D1 will accept.

    `sqlite3 .dump` writes one statement per row, which for the
    artifacts table is 6,062,896 statements. D1 also caps a statement at
    100,000 bytes, so the batch is cut on whichever comes first: the row
    count, or the byte budget. The budget is half the cap, which leaves
    room for the column list and one unusually long final row.

    **Bytes, not characters.** The budget counted characters, and a
    character of a Chinese description is three bytes in UTF-8 and an
    emoji four: batches of them came out at 112–144 KB and D1 refused
    them. A row no statement can hold is refused here, at export time,
    rather than by D1 partway through an import.

    **The columns are named, and checked.** A bare `INSERT INTO t
    VALUES (...)` has to supply every column in declaration order, so
    adding a defaulted column to a table silently invalidates every
    INSERT for it — the export still reports the rows it wrote, and
    SQLite rejects the statement when someone applies it. That is
    exactly what happened when `packages.repositories` was added: the
    summary said 225,400 rows and the table came out empty. Naming the
    columns lets a table carry one the writer does not fill, and
    validating them here turns a mismatch into a loud failure at export
    time rather than a quiet one at import time.
    """
    for statement in _statements(table, columns, rows, batch):
        yield statement.text


class _Statement(NamedTuple):
    """One INSERT, with what writing it into a part needs to know."""

    text: str
    #: Its length in bytes, as written.
    size: int
    #: The first row it inserts.
    first: Sequence[object]
    #: How many rows it inserts.
    rows: int


def _statements(
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
    batch: int,
) -> Iterator[_Statement]:
    """`batch_inserts`, with each statement's size and rows."""
    known = D1_SCHEMA.table(table).column_names
    unknown = [c for c in columns if c not in known]
    if unknown:
        raise ValueError(
            f"{table} has no column(s) {', '.join(unknown)}; "
            f"the schema declares {', '.join(known)}",
        )

    prefix = f"INSERT INTO {table} ({','.join(columns)}) VALUES "
    # The prefix and the `;\n` that ends the statement. Names, so ASCII.
    empty = len(prefix) + 2
    pending: list[str] = []
    first: Sequence[object] = ()
    size = empty

    for row in rows:
        if len(row) != len(columns):
            raise ValueError(
                f'{table} row has {len(row)} value(s) for '
                f'{len(columns)} column(s): {columns}',
            )
        rendered = '(' + ','.join(sql_literal(v) for v in row) + ')'
        # Most rows are integers alone, whose characters are bytes.
        width = (
            len(rendered) if rendered.isascii()
            else len(rendered.encode('utf-8'))
        )
        if empty + width > MAX_STATEMENT_BYTES:
            raise ValueError(
                f'{table} row is {width:,} bytes as SQL, and no statement '
                f"of it fits under D1's cap of {MAX_STATEMENT_BYTES:,} "
                f'bytes: {rendered[:120]}…',
            )
        too_many = len(pending) >= batch
        too_big = size + 1 + width > _BATCH_BUDGET
        if pending and (too_many or too_big):
            yield _Statement(
                prefix + ','.join(pending) + ';\n', size, first, len(pending),
            )
            pending, size = [], empty
        if pending:
            size += 1  # the comma before it
        else:
            first = row
        pending.append(rendered)
        size += width

    if pending:
        yield _Statement(
            prefix + ','.join(pending) + ';\n', size, first, len(pending),
        )


def schema_sql() -> str:
    """DDL for the tables, without indexes.

    Drops first: D1 keeps whatever a previous import left, so a reimport
    against existing tables would double every row rather than replace
    it. Indexes are deliberately absent — see `index_sql`.
    """
    parts = ['-- ChatSBOM D1 schema. Apply before the data.\n']
    for table in reversed(D1_SCHEMA.tables):
        parts.append(f'DROP TABLE IF EXISTS {table.name};')
    parts.append('')
    for table in D1_SCHEMA.tables:
        parts.append(f'-- {table.description}')
        parts.append(table.ddl())
        parts.append('')
    return '\n'.join(parts)


def index_sql() -> str:
    """Indexes, applied *after* the rows are in.

    Inserting into an indexed table updates every index per row;
    building the index once over finished data is markedly faster, which
    matters when the data arrives as batched statements over a network.

    Each is created only if it is not there, so the script can be
    applied again.
    """
    parts = ['-- ChatSBOM D1 indexes. Apply after the data.\n']
    for index in D1_SCHEMA.indexes:
        parts.append(index.ddl())
    return '\n'.join(parts) + '\n'


#: The five columns interned as one combination.
KIND_COLUMNS = ('type', 'found_by', 'relationship', 'source', 'version_kind')


def _intern(interned: dict[K, int], key: K) -> int:
    """`key`'s id, given the next one if it has none yet."""
    found = interned.get(key)
    if found is None:
        found = len(interned) + 1
        interned[key] = found
    return found


class Lookups:
    """The repeated strings of the artifact rows, as lookup tables that
    the rows reference by id. Filled one row at a time, as the rows
    stream past.

    Rows are keyed by column name rather than position. That is not
    incidental: this project has already been bitten by positional
    access, where a column added upstream silently shifted every later
    field. Names fail loudly instead.

    The five trailing columns are interned *as a combination* rather
    than one lookup per column. Only 45 combinations occur across six
    million rows, so one reference replaces five strings; five separate
    lookups would replace five strings with five references and save far
    less.

    Ids start at 1 so that 0 is never a valid reference and a
    zero-initialised integer cannot silently point at a real row.

    Only the lookups are kept: 225,400 names, where `normalise` kept
    every artifact's references in a list until the last was written —
    about 147 bytes a row, 2.3 GiB for 16.8 million rows.
    """

    def __init__(self) -> None:
        self.packages: dict[str, int] = {}
        self.versions: dict[str, int] = {}
        self.kinds: dict[tuple[str, ...], int] = {}

    def reference(
        self,
        row: Mapping[str, object],
    ) -> tuple[int, int, int, int]:
        """An artifact row as its four integers, interning its strings
        as it goes."""
        # The type under its canonical name, as the rollups key it, so
        # `agg_*` by ecosystem agree with ClickHouse's and `php-composer`
        # and `composer` are one filter value.
        kind = tuple(
            canonical(str(row[c])) if c == 'type' else str(row[c])
            for c in KIND_COLUMNS
        )
        return (
            int(str(row['repository_id'])),
            _intern(self.packages, str(row['name'])),
            _intern(self.versions, str(row['version'])),
            _intern(self.kinds, kind),
        )

    def rows(self, table: str) -> Iterator[tuple[object, ...]]:
        """A lookup table's rows, `(id, value, ...)`, in id order.

        Complete once every artifact row has been referenced.
        """
        if table == 'kinds':
            for kind, id in self.kinds.items():
                yield (id, *kind)
            return
        interned = {'packages': self.packages, 'versions': self.versions}
        for value, id in interned[table].items():
            yield (id, value)


def part_name(table: str, part: int) -> str:
    """The file a table's `part`-th part of the data script is in."""
    return f'02-{table}-{part:04d}.sql'


def _increasing(
    table: str,
    column: int,
    rows: Iterable[Sequence[object]],
) -> Iterator[Sequence[object]]:
    """`rows`, refused as soon as the key in `column` fails to increase."""
    last = 0
    for row in rows:
        key = int(str(row[column]))
        if key <= last:
            raise ValueError(
                f'{table} rows must come in increasing id order, and '
                f'{key} came after {last}: each part removes its table '
                f'from its own first id on, which is only its own rows '
                f'and the ones after it if the ids increase',
            )
        last = key
        yield row


def write_chunks(
    directory: Path,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
    *,
    batch: int = DEFAULT_BATCH,
    chunk_bytes: int = CHUNK_BYTES,
) -> tuple[list[Path], int]:
    """Write one table's rows as numbered parts of the data script, and
    return the parts, in order, and the rows written.

    A part is `part_name(table, n)`, of at most `chunk_bytes` bytes
    unless one statement alone is more. Each starts by removing the
    rows its table has from the part's own first row on, so it can be
    applied again, and an import resumed from any part: whatever that
    part, or a part after it, wrote before is gone before its rows go
    in again, and the parts after it write theirs again after it.

    A row's position is its rowid. A table keyed by an `INTEGER` id has
    its ids as rowids, so its rows have to come in increasing id order,
    and they are checked to. Any other table's rowids are given in the
    order its rows arrive, one after another from 1, when its parts are
    applied in order: its first row in part n is one past the rows of
    the parts before it.

    A table with no rows still gets a part, which empties it.
    """
    keyed = D1_SCHEMA.table(table).primary_key
    key = None if keyed is None else list(columns).index(keyed)
    if key is not None:
        rows = _increasing(table, key, rows)

    paths: list[Path] = []
    written = 0
    handle: TextIO | None = None
    size = 0

    def begin(first: int) -> tuple[TextIO, int]:
        if len(paths) == MAX_PARTS:
            raise ValueError(
                f'{table} needs more than {MAX_PARTS:,} parts of '
                f'{chunk_bytes:,} bytes; export with larger ones',
            )
        path = directory / part_name(table, len(paths) + 1)
        paths.append(path)
        opened = path.open('w', encoding='utf-8', newline='\n')
        header = (
            f'-- ChatSBOM D1 data: {table}, part {len(paths)}. Apply after '
            f'01-schema.sql and\n'
            f'-- the parts before it. Safe to apply again: it first removes '
            f'what it and\n'
            f'-- the later parts of {table} wrote, which are then applied '
            f'again.\n'
            f'DELETE FROM {table} WHERE rowid >= {first};\n'
        )
        opened.write(header)
        return opened, len(header)

    try:
        for statement in _statements(table, columns, rows, batch):
            if handle is None or size + statement.size > chunk_bytes:
                if handle is not None:
                    handle.close()
                first = (
                    written + 1 if key is None
                    else int(str(statement.first[key]))
                )
                handle, size = begin(first)
            handle.write(statement.text)
            size += statement.size
            written += statement.rows
        if handle is None:
            handle, size = begin(1)
    finally:
        if handle is not None:
            handle.close()
    return paths, written


#: The scripts around the data's parts. Their numbers put them before
#: and after the parts in the order of their names, which is the order
#: they are applied in.
SCHEMA_SCRIPT = '01-schema.sql'
AGGREGATES_SCRIPT = '03-aggregates.sql'
INDEXES_SCRIPT = '04-indexes.sql'

#: Every row in one file, as an export wrote them before the parts.
_ONE_DATA_FILE = '02-data.sql'

#: A part of the data script (`part_name`).
_PART = re.compile(
    '02-(?:'
    + '|'.join(re.escape(table.name) for table in D1_SCHEMA.tables)
    + r')-\d{4}\.sql',
)

#: Why an export without edges is refused, and what to run instead.
NO_EDGES = (
    'The edges table is empty, so the D1 export would ship a dataset '
    'whose edge panels are empty. Run `chatsbom db edges` first: it '
    'counts the package-to-package edges in the stored dependency '
    'graphs into ClickHouse, where the export reads them.'
)


def _remove_ours(directory: Path) -> None:
    """Delete the files an export writes in `directory`, and no other.

    The parts are applied by name, all of them, so one left by an
    earlier export would be applied with this one's rows: its `0042`
    after this one's `0041`, or the `02-data.sql` of before the split,
    a second copy of every row. Only the names an export writes are
    touched: the directory is one a person chose, as the Parquet export
    learned when it deleted a user's `my-own-analysis.parquet`.
    """
    for path in directory.iterdir():
        name = path.name
        ours = name in (
            SCHEMA_SCRIPT, _ONE_DATA_FILE, AGGREGATES_SCRIPT, INDEXES_SCRIPT,
        ) or _PART.fullmatch(name)
        if ours and path.is_file():
            path.unlink()


@dataclass
class D1ExportResult:
    """What a D1 export produced."""

    directory: Path
    row_counts: dict[str, int] = field(default_factory=dict)
    #: Bytes of each file written, by name. Applied in the order of
    #: their names: `01-schema.sql`, the data's parts, `03-aggregates.sql`
    #: and `04-indexes.sql`.
    files: dict[str, int] = field(default_factory=dict)
    #: Span of observation dates present in the data.
    freshness: dict[str, str] = field(default_factory=dict)

    @property
    def total_bytes(self) -> int:
        return sum(self.files.values())


def export_d1(
    query_repo: QueryRepository,
    directory: Path,
    *,
    batch: int = DEFAULT_BATCH,
    chunk_bytes: int = CHUNK_BYTES,
) -> D1ExportResult:
    """Write the SQL scripts that load this dataset into D1.

    Applied in the order of their names, one `wrangler d1 execute
    --file` each:

      1. `01-schema.sql`              — drops and recreates the tables
      2. `02-<table>-0001.sql`, …     — the rows, in parts of at most
                                        `chunk_bytes` (`write_chunks`)
      3. `03-aggregates.sql`          — the overview's aggregates,
                                        computed from the rows
      4. `04-indexes.sql`             — indexes, built once over
                                        finished data

    Split because the order matters, and so that a failure costs one
    part rather than the whole import. Each file can be applied again
    without changing the result: a failed or timed-out one is run again,
    and the import goes on from there.

    The rows are streamed rather than held. The artifacts become their
    four integer references as they arrive and are written at once; what
    is kept is the lookups they reference (`Lookups`), whose strings are
    what normalising takes out of every row.

    Every query goes out with `EXPORT_SETTINGS`, so a cap on the
    connecting account fails the export rather than cutting a table
    short. The edges are the ones `db edges` keeps in ClickHouse, and an
    export without any is refused before anything is written.

    A failed export removes what it wrote: the parts are applied by
    name, and the ones it had got to would load part of a dataset.
    """
    # Before anything is written, or removed: an export that cannot
    # finish leaves the previous one where it is.
    if not query_repo.has_edges():
        raise RuntimeError(NO_EDGES)

    directory.mkdir(parents=True, exist_ok=True)
    _remove_ours(directory)
    result = D1ExportResult(directory=directory)

    def script(name: str, sql: str) -> None:
        path = directory / name
        path.write_text(sql, encoding='utf-8')
        result.files[name] = path.stat().st_size

    def data(
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[object]],
    ) -> None:
        paths, written = write_chunks(
            directory, table, columns, rows,
            batch=batch, chunk_bytes=chunk_bytes,
        )
        for path in paths:
            result.files[path.name] = path.stat().st_size
        result.row_counts[table] = written

    def read(table: str, sql: str) -> Iterator[Mapping[str, object]]:
        return whole(
            table, query_repo.stream_rows(sql, settings=EXPORT_SETTINGS),
        )

    try:
        script(SCHEMA_SCRIPT, schema_sql())

        # The Parquet artifacts query already returns exactly the eight
        # columns `Lookups.reference` reads. Reusing it means the two
        # exports cannot describe different data.
        lookups = Lookups()
        data(
            'artifacts', ARTIFACTS.column_names,
            (
                lookups.reference(row)
                for row in read('artifacts', QUERIES['artifacts'])
            ),
        )

        # The lookups, complete now that every artifact has been
        # through. Each states the columns it fills. `packages` fills two
        # of its three: `repositories` is written later by the aggregate
        # script, from the artifact rows.
        data('packages', ('id', 'name'), lookups.rows('packages'))
        data('versions', ('id', 'version'), lookups.rows('versions'))
        data('kinds', ('id', *KIND_COLUMNS), lookups.rows('kinds'))

        # The date beside each dependants row: its source's, not the
        # newest of the repository's.
        data(
            'observations', OBSERVATIONS.column_names,
            (
                tuple(row[c] for c in OBSERVATIONS.column_names)
                for row in read('observations', OBSERVATIONS_QUERY)
            ),
        )

        # Provenance, from the rows written: the observation span comes
        # out of the repositories table rather than a clock, so it
        # describes the data's age rather than the export's — the same
        # span the Parquet manifest reports (`repository_freshness`).
        # One pair a repository, whatever the number of artifacts.
        dates: list[tuple[object, object]] = []

        def dated(
            rows: Iterable[Mapping[str, object]],
        ) -> Iterator[Mapping[str, object]]:
            for row in rows:
                dates.append((row['observed_at'], row['total_dependencies']))
                yield row

        # The remaining tables need no normalising: they are small, and
        # their strings do not repeat across millions of rows.
        for name in ('repositories', 'licenses', 'history'):
            columns = D1_SCHEMA.table(name).column_names
            # Ordered by the schema's own column list, so a column added
            # to one side cannot quietly shift the other.
            # `licenses` reads its own query, not the shared one.
            # The shared query is keyed `(license, type)` for the
            # Parquet export, and this table declares one row per
            # licence — shipping those rows verbatim put one row per
            # licence *per ecosystem* in it, so the panel read
            # whichever slice sorted highest as the licence's total:
            # MIT 10,114 against a true 16,846.
            query = D1_LICENSES_QUERY if name == 'licenses' \
                else QUERIES[name]
            if name == 'repositories':
                # By id. Each part names the first id it holds
                # (`write_chunks`), and the shared query sorts by stars
                # for the Parquet file. A table keyed by id is stored in
                # id order whatever order its rows arrive in, so this
                # changes nothing D1 holds.
                rows = dated(
                    read(name, f'SELECT * FROM ({query}) ORDER BY id ASC'),
                )
            else:
                rows = read(name, query)
            data(
                name, columns,
                (tuple(_d1_value(row[c]) for c in columns) for row in rows),
            )
        observed = repository_freshness(
            {'observed_at': at, 'total_dependencies': total}
            for at, total in dates
        )

        # Package-to-package edges, as `db edges` stored them: the
        # table the ClickHouse dashboard reads, so both stores serve one
        # count. They were counted again here from the raw documents,
        # under `data/09-github-depgraph` relative to wherever the export
        # ran — 74 seconds — and a directory that was not there gave an
        # empty `agg_edges` and a warning.
        #
        # They reference `packages` by id, so this comes after the
        # lookup is complete, and a pair naming a package the artifacts
        # never mentioned is skipped rather than inventing a row for it.
        skipped = 0

        def edges() -> Iterator[tuple[int, int, int]]:
            nonlocal skipped
            for parent, child, repositories in whole(
                'edges', query_repo.stream_edges(settings=EXPORT_SETTINGS),
            ):
                parent_id = lookups.packages.get(parent)
                child_id = lookups.packages.get(child)
                if parent_id is None or child_id is None:
                    skipped += 1
                    continue
                yield parent_id, child_id, repositories

        data('agg_edges', AGG_EDGES.column_names, edges())
        if skipped:
            # Expected, not alarming: a transitive package can appear in
            # a dependency graph without appearing in any SBOM we
            # indexed. Counted so the number is visible rather than
            # silently absorbed.
            logger.info(
                'Edges skipped, package not in the dataset', pairs=skipped,
            )

        data(
            'meta', META.column_names,
            [meta_row(f'chatsbom/{__version__}', SCHEMA_VERSION, observed)],
        )
        result.freshness = observed

        # Aggregates before indexes: they read the base tables, and the
        # indexes that speed *them* up are on the aggregate tables, which
        # do not exist as data until this runs.
        script(AGGREGATES_SCRIPT, aggregate_sql())
        script(INDEXES_SCRIPT, index_sql())
    except BaseException:
        _remove_ours(directory)
        raise

    return result


#: How many ranked rows to precompute per filter combination. The panel
#: shows 20; a little headroom costs almost nothing and avoids a
#: re-export if the panel grows.
TOP_PACKAGES_DEPTH = 30


#: The tables `aggregate_sql` fills, all of them computed from the base
#: tables. Not `agg_edges`, which the data script fills from ClickHouse.
AGGREGATED = (
    AGG_TOTALS, AGG_RELATIONSHIP_SPLIT, AGG_LANGUAGE_COVERAGE,
    AGG_ECOSYSTEM_COVERAGE, AGG_TOP_PACKAGES, AGG_DEPENDENCY_BUCKETS,
    AGG_SOURCE_COMPARISON,
)


def aggregate_sql() -> str:
    """Fill the precomputed aggregates, inside SQLite.

    Computed here rather than with more ClickHouse queries for one
    reason: these must agree with the base tables that were just
    written. Deriving them from those same rows makes disagreement
    impossible; a second trip to ClickHouse could pick up rows that
    landed in between.

    Applied as its own script after the data and before the indexes.
    It empties every table it fills before filling it, so that it can
    be applied again: it was `INSERT INTO agg_* SELECT` alone, and a
    retry doubled every aggregate.
    """
    emptied = '\n'.join(f'DELETE FROM {table.name};' for table in AGGREGATED)
    return f"""-- ChatSBOM D1 aggregates. Apply after the data, 02-*.sql.

-- Safe to apply again: every table this script fills is emptied first,
-- so a second run replaces what the first wrote rather than adding a
-- second copy of it. The UPDATE of `packages` sets the same numbers.
{emptied}

-- `WHERE total_dependencies > 0`, because the other three numbers
-- here describe the analysed set and this one has to as well.
--
-- The repositories table holds every repository of the current search
-- snapshot, including those with no dependency row, while ClickHouse's
-- `mv_totals` counts the ones that have one. The dashboard reads
-- this field under the label "repositories with dependency data" — a
-- label made true for one backend and false for the other. Two stores
-- answering the same call differently is how a fallback becomes a
-- different dataset. `tracked` is the snapshot, the denominator.
INSERT INTO agg_totals
  (repositories, dependencies, packages, classified, tracked)
SELECT
  (SELECT count(*) FROM repositories WHERE total_dependencies > 0),
  (SELECT count(*) FROM artifacts),
  (SELECT count(*) FROM packages),
  (SELECT count(*) FROM artifacts a JOIN kinds k ON k.id = a.kind_id
   WHERE k.relationship <> 'unknown'),
  (SELECT count(*) FROM repositories);

-- Per ecosystem and, as the '' row, the whole corpus. The overview reads
-- the '' row; the ecosystem filter reads one of the others. Records
-- partition by ecosystem (a record has one type), so the per-ecosystem
-- rows add up to the '' row.
INSERT INTO agg_relationship_split (ecosystem, relationship, records)
SELECT '', k.relationship, count(*)
FROM artifacts a JOIN kinds k ON k.id = a.kind_id
GROUP BY k.relationship;

INSERT INTO agg_relationship_split (ecosystem, relationship, records)
SELECT k.type, k.relationship, count(*)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
WHERE k.type <> ''
GROUP BY k.type, k.relationship;

-- Denormalised onto `packages` so the search box can rank by it.
--
-- Ordering the search by popularity is the whole point: alphabetically,
-- `laravel` returns forty `laravel-enso/*` packages with one dependant
-- each ('-' is 0x2D, '/' is 0x2F) and never reaches `laravel/framework`
-- with 98. But computing the count per matching row means a correlated
-- subquery over `artifacts` for every candidate, which is the one thing
-- a keystroke-latency query must not do. Stored once here instead.
-- One grouped pass, joined back. Not a correlated subquery: the index
-- that would make one viable, `idx_artifacts_package_id`, is created by
-- `04-indexes.sql` *after* this script, so the subquery form scans the
-- whole artifacts table once per package. Measured on the real export:
-- 225,400 packages against 16,839,566 rows had not finished in 110
-- seconds and would not have; this form takes 3.2 seconds, which also
-- keeps it inside D1's 30-second statement limit.
--
-- Packages the join finds no rows for keep the column's DEFAULT 0.
UPDATE packages SET repositories = counted.n
FROM (
  SELECT package_id, count(DISTINCT repository_id) AS n
  FROM artifacts GROUP BY package_id
) AS counted
WHERE counted.package_id = packages.id;

-- The denominator is every repository of the snapshot, collected or
-- not, folded by GitHub's language (top twelve, other, none).
INSERT INTO agg_language_coverage
  (language, repositories, with_sbom, with_syft, with_depgraph,
   with_manifest)
SELECT r.language_bucket, count(*),
       count(CASE WHEN r.total_dependencies > 0 THEN 1 END),
       count(CASE WHEN s.syft > 0 THEN 1 END),
       count(CASE WHEN s.depgraph > 0 THEN 1 END),
       count(CASE WHEN s.manifest > 0 THEN 1 END)
FROM repositories r
LEFT JOIN (
  SELECT a.repository_id AS id,
         sum(k.source = 'syft') AS syft,
         sum(k.source = 'github-depgraph') AS depgraph,
         sum(k.source = 'manifest') AS manifest
  FROM artifacts a JOIN kinds k ON k.id = a.kind_id
  GROUP BY a.repository_id
) s ON s.id = r.id
GROUP BY r.language_bucket;

-- A repository counts under every ecosystem it has: its artifacts' or
-- its manifests'. These rows overlap and are not to be summed.
INSERT INTO agg_ecosystem_coverage
  (ecosystem, repositories, with_any, with_syft, with_depgraph,
   with_manifest)
SELECT e.value, count(*),
       count(CASE WHEN s.records > 0 THEN 1 END),
       count(CASE WHEN s.syft > 0 THEN 1 END),
       count(CASE WHEN s.depgraph > 0 THEN 1 END),
       count(CASE WHEN s.manifest > 0 THEN 1 END)
FROM repositories r
JOIN json_each(r.ecosystems) e
LEFT JOIN (
  SELECT a.repository_id AS id,
         k.type AS ecosystem,
         count(*) AS records,
         sum(k.source = 'syft') AS syft,
         sum(k.source = 'github-depgraph') AS depgraph,
         sum(k.source = 'manifest') AS manifest
  FROM artifacts a JOIN kinds k ON k.id = a.kind_id
  GROUP BY a.repository_id, k.type
) s ON s.id = r.id AND s.ecosystem = e.value
GROUP BY e.value;

-- The ranking, per filter combination. The panel has exactly two
-- controls -- declared-only, and ecosystem -- so the answer set is
-- finite and can be enumerated.
--
-- The whole-corpus row counts each name's repositories once. It used
-- to sum the per-language counts, which was exact only while every
-- repository had one language; a repository has as many ecosystems as
-- it has manifests for, and `mail` is a gem and a Maven artifact.
INSERT INTO agg_top_packages
  (direct_only, ecosystem, rank, name, repository_count, direct_count)
WITH counted AS (
  SELECT
    k.type AS ecosystem,
    p.name AS name,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT CASE WHEN k.relationship = 'direct'
                        THEN a.repository_id END) AS direct_count
  FROM artifacts a
  JOIN packages p ON p.id = a.package_id
  JOIN kinds k ON k.id = a.kind_id
  GROUP BY k.type, p.name
),
overall AS (
  SELECT
    '' AS ecosystem,
    p.name AS name,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT CASE WHEN k.relationship = 'direct'
                        THEN a.repository_id END) AS direct_count
  FROM artifacts a
  JOIN packages p ON p.id = a.package_id
  JOIN kinds k ON k.id = a.kind_id
  GROUP BY p.name
),
unioned AS (
  SELECT * FROM counted WHERE ecosystem <> ''
  UNION ALL SELECT * FROM overall
),
ranked AS (
  SELECT
    direct_only, ecosystem, name, repository_count, direct_count,
    row_number() OVER (
      PARTITION BY direct_only, ecosystem
      ORDER BY CASE WHEN direct_only = 1 THEN direct_count
                    ELSE repository_count END DESC, name ASC
    ) AS rank
  FROM unioned, (SELECT 0 AS direct_only UNION ALL SELECT 1)
)
SELECT direct_only, ecosystem, rank, name, repository_count, direct_count
FROM ranked
WHERE rank <= {TOP_PACKAGES_DEPTH};

INSERT INTO agg_dependency_buckets (bucket, position, repositories)
SELECT bucket, position, count(*) FROM (
  SELECT
    CASE
      WHEN total_dependencies = 0 THEN 'none'
      WHEN total_dependencies < 10 THEN '1-9'
      WHEN total_dependencies < 25 THEN '10-24'
      WHEN total_dependencies < 50 THEN '25-49'
      WHEN total_dependencies < 100 THEN '50-99'
      WHEN total_dependencies < 250 THEN '100-249'
      WHEN total_dependencies < 500 THEN '250-499'
      WHEN total_dependencies < 1000 THEN '500-999'
      ELSE '1000+'
    END AS bucket,
    CASE
      WHEN total_dependencies = 0 THEN 0
      WHEN total_dependencies < 10 THEN 1
      WHEN total_dependencies < 25 THEN 2
      WHEN total_dependencies < 50 THEN 3
      WHEN total_dependencies < 100 THEN 4
      WHEN total_dependencies < 250 THEN 5
      WHEN total_dependencies < 500 THEN 6
      WHEN total_dependencies < 1000 THEN 7
      ELSE 8
    END AS position
  FROM repositories
) GROUP BY bucket, position;

INSERT INTO agg_source_comparison (ecosystem, syft, depgraph, manifest)
SELECT k.type,
       sum(CASE WHEN k.source = 'syft' THEN 1 ELSE 0 END),
       sum(CASE WHEN k.source = 'github-depgraph' THEN 1 ELSE 0 END),
       sum(CASE WHEN k.source = 'manifest' THEN 1 ELSE 0 END)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
WHERE k.type <> ''
GROUP BY k.type;
"""


def meta_row(
    generator: str,
    schema_version: str,
    freshness: Mapping[str, str],
) -> tuple[str, str, str, str]:
    """The one provenance row, in the order `META` declares.

    Freshness is passed in rather than recomputed: it comes from the
    observation dates present in the data, which the caller has already
    read off the rows it wrote. An absent span is stored as empty
    strings — a default date would read as a real observation.
    """
    return (
        generator,
        schema_version,
        freshness.get('observedFrom', ''),
        freshness.get('observedTo', ''),
    )


def meta_sql(
    generator: str,
    schema_version: str,
    freshness: Mapping[str, str],
) -> str:
    """The provenance row as the data script inserts it.

    With its columns named, as every INSERT here names them. It was
    `INSERT INTO meta VALUES (...)`, which fills the columns by
    position: a column added to `meta` would have failed it on import,
    after the export had reported success.
    """
    return ''.join(
        batch_inserts(
            META.name, META.column_names,
            [meta_row(generator, schema_version, freshness)],
        ),
    )
