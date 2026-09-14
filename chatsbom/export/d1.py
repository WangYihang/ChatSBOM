"""Export the dataset as a D1-shaped SQLite database and its SQL.

Two constraints drive the shape here, both measured on the real corpus
rather than assumed.

**Size.** Translating the Parquet schema directly into SQLite produces
762.6 MB once the indexes the queries need are present, and D1's free
tier stops at 500 MB. Normalising the repeated strings brings that to
291.5 MB — 62% smaller with no rows lost — because the cardinalities are
tiny next to the row count:

    6,062,896 artifact rows
      141,938 distinct package names
       46,526 distinct versions
           45 distinct combinations of type / found_by / relationship /
              source / version_kind

That last line is where most of the saving is. Five strings were stored
on every one of six million rows to express one of forty-five
possibilities.

**Statement length.** D1 caps a single SQL statement at 100,000 bytes,
and `sqlite3 .dump` writes one INSERT per row — 6,062,896 statements,
slow to execute over a network even before the size is considered. Rows
are batched into multi-row INSERTs sized to stay under the cap.

The Parquet export stays: it is what the browser-side engine reads, and
keeping both lets the serving model change without a flag day.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import structlog

from chatsbom.__version__ import __version__
from chatsbom.core.edges import collect_edges
from chatsbom.core.edges import DEPGRAPH_ROOT
from chatsbom.core.repository import QueryRepository
from chatsbom.export.queries import observed_range
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import SCHEMA_VERSION

logger = structlog.get_logger('export_d1')

#: D1's documented cap on one SQL statement. Batches target a fraction of
#: it so a wide row cannot push a batch over.
MAX_STATEMENT_BYTES = 100_000

#: Leaves room for the column list and a long final row.
_BATCH_BUDGET = MAX_STATEMENT_BYTES // 2

#: Default rows per statement for narrow tables. Wide rows are cut
#: earlier by the byte budget.
DEFAULT_BATCH = 500


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
        kind = 'UNIQUE INDEX' if self.unique else 'INDEX'
        cols = ', '.join(self.columns)
        return f'CREATE {kind} {self.name} ON {self.table}({cols});'


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
        D1Column('type', 'TEXT NOT NULL', 'Ecosystem, e.g. gem or npm.'),
        D1Column('found_by', 'TEXT NOT NULL', 'Cataloguer that reported it.'),
        D1Column(
            'relationship', 'TEXT NOT NULL',
            'direct | transitive | unknown.',
        ),
        D1Column('source', 'TEXT NOT NULL', 'syft | github-depgraph.'),
        D1Column('version_kind', 'TEXT NOT NULL', 'exact | range | unknown.'),
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

REPOSITORIES = D1Table(
    name='repositories',
    description='One row per analysed repository.',
    primary_key='id',
    columns=(
        D1Column('id', 'INTEGER', 'GitHub repository id.'),
        D1Column('owner', 'TEXT NOT NULL', 'Repository owner login.'),
        D1Column('repo', 'TEXT NOT NULL', 'Repository name.'),
        D1Column(
            'stars', 'INTEGER NOT NULL',
            'Star count at collection time.',
        ),
        D1Column('language', 'TEXT NOT NULL', 'Primary language, lowercased.'),
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
        D1Column(
            'source', 'TEXT NOT NULL', 'syft | github-depgraph.',
        ),
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
    description='The four numbers the tiles and the footer show. One row.',
    columns=(
        D1Column('repositories', 'INTEGER NOT NULL', 'Repositories analysed.'),
        D1Column('dependencies', 'INTEGER NOT NULL', 'Dependency records.'),
        D1Column('packages', 'INTEGER NOT NULL', 'Distinct packages.'),
        D1Column(
            'classified', 'INTEGER NOT NULL',
            'Records with a known relationship.',
        ),
    ),
)

AGG_RELATIONSHIP_SPLIT = D1Table(
    name='agg_relationship_split',
    description='Declared / inherited / undetermined, per language and overall.',
    columns=(
        D1Column(
            'language', 'TEXT NOT NULL',
            "Language, or '' for the whole corpus.",
        ),
        D1Column(
            'relationship', 'TEXT NOT NULL',
            'direct | transitive | unknown.',
        ),
        D1Column('records', 'INTEGER NOT NULL', 'Dependency records.'),
    ),
)

AGG_LANGUAGE_COVERAGE = D1Table(
    name='agg_language_coverage',
    description='Repositories per language, and how many carry dependency data.',
    columns=(
        D1Column('language', 'TEXT NOT NULL', 'Language, lowercased.'),
        D1Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories in that language.',
        ),
        D1Column(
            'with_sbom', 'INTEGER NOT NULL',
            'Of those, with dependencies recorded.',
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
            'language', 'TEXT NOT NULL',
            "Language filter, or '' for all.",
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
    description='Dependency records per language, split by collector.',
    columns=(
        D1Column('language', 'TEXT NOT NULL', 'Language, lowercased.'),
        D1Column('syft', 'INTEGER NOT NULL', 'Records from syft.'),
        D1Column(
            'depgraph', 'INTEGER NOT NULL',
            "Records from GitHub's dependency graph.",
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
        REPOSITORIES, ARTIFACTS, PACKAGES, VERSIONS, KINDS, LICENSES, HISTORY,
        AGG_TOTALS, AGG_RELATIONSHIP_SPLIT, AGG_LANGUAGE_COVERAGE,
        AGG_TOP_PACKAGES, AGG_DEPENDENCY_BUCKETS, AGG_SOURCE_COMPARISON,
        AGG_EDGES, META,
    ),
    indexes=(
        # Without these the joins table-scan six million rows.
        D1Index('artifacts', ('package_id',)),
        D1Index('artifacts', ('repository_id',)),
        D1Index('packages', ('name',), unique=True),
        D1Index('versions', ('version',), unique=True),
        D1Index('repositories', ('language',)),
        D1Index('history', ('name',)),
        # Aggregates are indexed by what their panel filters on, so a
        # request is a lookup rather than a scan of the aggregate.
        D1Index('agg_top_packages', ('direct_only', 'language', 'rank')),
        D1Index('agg_relationship_split', ('language',)),
        # Both directions. "What does X pull in" and "what pulls in X"
        # are different questions and the second is the more useful one
        # — it is how you find out why a package you never chose is in
        # your lockfile.
        D1Index('agg_edges', ('parent_id',)),
        D1Index('agg_edges', ('child_id',)),
    ),
)


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
    known = D1_SCHEMA.table(table).column_names
    unknown = [c for c in columns if c not in known]
    if unknown:
        raise ValueError(
            f"{table} has no column(s) {', '.join(unknown)}; "
            f"the schema declares {', '.join(known)}",
        )

    prefix = f"INSERT INTO {table} ({','.join(columns)}) VALUES "
    pending: list[str] = []
    size = len(prefix)

    for row in rows:
        if len(row) != len(columns):
            raise ValueError(
                f'{table} row has {len(row)} value(s) for '
                f'{len(columns)} column(s): {columns}',
            )
        rendered = '(' + ','.join(sql_literal(v) for v in row) + ')'
        too_many = len(pending) >= batch
        too_big = size + len(rendered) + 1 > _BATCH_BUDGET
        if pending and (too_many or too_big):
            yield prefix + ','.join(pending) + ';\n'
            pending, size = [], len(prefix)
        pending.append(rendered)
        size += len(rendered) + 1

    if pending:
        yield prefix + ','.join(pending) + ';\n'


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
    """
    parts = ['-- ChatSBOM D1 indexes. Apply after the data.\n']
    for index in D1_SCHEMA.indexes:
        parts.append(index.ddl())
    return '\n'.join(parts) + '\n'


@dataclass
class Normalised:
    """Lookup tables plus a fact table of integer references."""

    packages: list[tuple[int, str]] = field(default_factory=list)
    versions: list[tuple[int, str]] = field(default_factory=list)
    kinds: list[
        tuple[int, str, str, str, str, str]
    ] = field(default_factory=list)
    artifacts: list[tuple[int, int, int, int]] = field(default_factory=list)


#: The five columns interned as one combination.
KIND_COLUMNS = ('type', 'found_by', 'relationship', 'source', 'version_kind')


def normalise(rows: Iterable[Mapping[str, object]]) -> Normalised:
    """Replace the repeated strings in artifact rows with references.

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
    """
    packages: dict[str, int] = {}
    versions: dict[str, int] = {}
    kinds: dict[tuple[str, str, str, str, str], int] = {}
    result = Normalised()

    def intern(table: dict, key):
        found = table.get(key)
        if found is None:
            found = len(table) + 1
            table[key] = found
        return found

    for row in rows:
        repository_id = int(str(row['repository_id']))
        name = str(row['name'])
        version = str(row['version'])
        kind = tuple(str(row[c]) for c in KIND_COLUMNS)

        result.artifacts.append((
            repository_id,
            intern(packages, name),
            intern(versions, version),
            intern(kinds, kind),
        ))

    result.packages = [(i, n) for n, i in packages.items()]
    result.versions = [(i, v) for v, i in versions.items()]
    result.kinds = [(i, *k) for k, i in kinds.items()]
    return result


@dataclass
class D1ExportResult:
    """What a D1 export produced."""

    directory: Path
    row_counts: dict[str, int] = field(default_factory=dict)
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
    depgraph_root: Path | None = None,
) -> D1ExportResult:
    """Write the SQL scripts that load this dataset into D1.

    Three files, in the order they must be applied:

      1. `01-schema.sql`  — drops and recreates the tables
      2. `02-data.sql`    — batched multi-row INSERTs
      3. `03-indexes.sql` — indexes, built once over finished data

    Split because the order matters and because a single file would be
    hundreds of megabytes with no way to resume partway. `wrangler d1
    execute --file` takes one at a time.

    The artifacts rows are normalised on the way through: the repeated
    strings become lookup tables, which took the database from 762.6 MB
    to 291.5 MB — under D1's 500 MB free tier rather than over it.
    """
    directory.mkdir(parents=True, exist_ok=True)

    result = D1ExportResult(directory=directory)

    schema_path = directory / '01-schema.sql'
    schema_path.write_text(schema_sql(), encoding='utf-8')
    result.files[schema_path.name] = schema_path.stat().st_size

    # Artifacts are normalised in memory. At 6 million rows of four
    # integers that is a few hundred megabytes of Python objects, which
    # is affordable on the collector host and much simpler than a
    # streaming two-pass interning scheme. If it ever stops being
    # affordable, the lookups are what to spill to disk first.
    # The Parquet artifacts query already returns exactly the eight
    # columns normalise() expects, in that order. Reusing it means the
    # two exports cannot describe different data.
    normalised = normalise(

        query_repo.stream_rows(QUERIES['artifacts']),
    )

    data_path = directory / '02-data.sql'
    with data_path.open('w', encoding='utf-8') as handle:
        handle.write('-- ChatSBOM D1 data. Apply after 01-schema.sql.\n')

        # Each states the columns it fills. `packages` fills two of
        # its three: `repositories` is written later by the aggregate
        # script, from the artifact rows below.
        for table, cols, rows in (
            ('packages', ('id', 'name'), normalised.packages),
            ('versions', ('id', 'version'), normalised.versions),
            (
                'kinds',
                ('id', *KIND_COLUMNS),
                normalised.kinds,
            ),
            (
                'artifacts',
                D1_SCHEMA.table('artifacts').column_names,
                normalised.artifacts,
            ),
        ):
            result.row_counts[table] = len(rows)
            for statement in batch_inserts(table, cols, rows, batch=batch):
                handle.write(statement)

        # Provenance, from the rows just written: the observation span
        # comes out of the repositories table rather than a clock, so it
        # describes the data's age rather than the export's.
        observed: dict[str, str] = {}

        # The remaining tables need no normalising: they are small, and
        # their strings do not repeat across millions of rows.
        for name in ('repositories', 'licenses', 'history'):
            columns = D1_SCHEMA.table(name).column_names
            # Ordered by the schema's own column list, so a column added
            # to one side cannot quietly shift the other.
            rows = [
                tuple(row[c] for c in columns)
                for row in query_repo.stream_rows(QUERIES[name])
            ]
            result.row_counts[name] = len(rows)
            if name == 'repositories':
                at = columns.index('observed_at')
                observed = observed_range(str(row[at]) for row in rows)
            for statement in batch_inserts(
                name, columns, rows, batch=batch,
            ):
                handle.write(statement)

        # Package-to-package edges, from the raw SPDX documents. They
        # reference `packages` by id, so this has to come after the
        # lookup is interned — and a pair naming a package the artifacts
        # never mentioned is skipped rather than inventing a row for it.
        by_name = {name: pid for pid, name in normalised.packages}
        edge_rows: list[tuple[int, int, int]] = []
        skipped = 0
        edges = collect_edges(depgraph_root or DEPGRAPH_ROOT)
        for (parent, child), repositories in edges.items():
            parent_id = by_name.get(parent)
            child_id = by_name.get(child)
            if parent_id is None or child_id is None:
                skipped += 1
                continue
            edge_rows.append((parent_id, child_id, repositories))

        if skipped:
            # Expected, not alarming: a transitive package can appear in
            # a dependency graph without appearing in any SBOM we
            # indexed. Counted so the number is visible rather than
            # silently absorbed.
            logger.info(
                'Edges skipped, package not in the dataset', pairs=skipped,
            )

        result.row_counts['agg_edges'] = len(edge_rows)
        for statement in batch_inserts(
            'agg_edges',
            D1_SCHEMA.table('agg_edges').column_names,
            edge_rows,
            batch=batch,
        ):
            handle.write(statement)

        handle.write(
            meta_sql(f'chatsbom/{__version__}', SCHEMA_VERSION, observed),
        )
        result.row_counts['meta'] = 1
        result.freshness = observed

    result.files[data_path.name] = data_path.stat().st_size

    # Aggregates before indexes: they read the base tables, and the
    # indexes that speed *them* up are on the aggregate tables, which do
    # not exist as data until this runs.
    agg_path = directory / '03-aggregates.sql'
    agg_path.write_text(aggregate_sql(), encoding='utf-8')
    result.files[agg_path.name] = agg_path.stat().st_size

    index_path = directory / '04-indexes.sql'
    index_path.write_text(index_sql(), encoding='utf-8')
    result.files[index_path.name] = index_path.stat().st_size

    return result


#: How many ranked rows to precompute per filter combination. The panel
#: shows 20; a little headroom costs almost nothing and avoids a
#: re-export if the panel grows.
TOP_PACKAGES_DEPTH = 30


def aggregate_sql() -> str:
    """Fill the precomputed aggregates, inside SQLite.

    Computed here rather than with more ClickHouse queries for one
    reason: these must agree with the base tables that were just
    written. Deriving them from those same rows makes disagreement
    impossible; a second trip to ClickHouse could pick up rows that
    landed in between.

    Applied as its own script after the data and before the indexes.
    """
    return f"""-- ChatSBOM D1 aggregates. Apply after 02-data.sql.

INSERT INTO agg_totals
SELECT
  (SELECT count(*) FROM repositories),
  (SELECT count(*) FROM artifacts),
  (SELECT count(*) FROM packages),
  (SELECT count(*) FROM artifacts a JOIN kinds k ON k.id = a.kind_id
   WHERE k.relationship <> 'unknown');

-- Per language and, as the '' row, the whole corpus. The overview reads
-- the '' row; the language filter reads one of the others.
INSERT INTO agg_relationship_split
SELECT '', k.relationship, count(*)
FROM artifacts a JOIN kinds k ON k.id = a.kind_id
GROUP BY k.relationship;

INSERT INTO agg_relationship_split
SELECT r.language, k.relationship, count(*)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
JOIN repositories r ON r.id = a.repository_id
WHERE r.language <> ''
GROUP BY r.language, k.relationship;

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

INSERT INTO agg_language_coverage
SELECT language, count(*),
       count(CASE WHEN total_dependencies > 0 THEN 1 END)
FROM repositories
GROUP BY language;

-- The ranking, per filter combination. The panel has exactly two
-- controls -- declared-only, and language -- so the answer set is
-- finite and can be enumerated.
INSERT INTO agg_top_packages
WITH counted AS (
  SELECT
    r.language AS language,
    p.name AS name,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT CASE WHEN k.relationship = 'direct'
                        THEN a.repository_id END) AS direct_count
  FROM artifacts a
  JOIN packages p ON p.id = a.package_id
  JOIN kinds k ON k.id = a.kind_id
  JOIN repositories r ON r.id = a.repository_id
  GROUP BY r.language, p.name
),
overall AS (
  SELECT '' AS language, name,
         sum(repository_count) AS repository_count,
         sum(direct_count) AS direct_count
  FROM counted GROUP BY name
),
unioned AS (
  SELECT * FROM counted WHERE language <> ''
  UNION ALL SELECT * FROM overall
),
ranked AS (
  SELECT
    direct_only, language, name, repository_count, direct_count,
    row_number() OVER (
      PARTITION BY direct_only, language
      ORDER BY CASE WHEN direct_only = 1 THEN direct_count
                    ELSE repository_count END DESC, name ASC
    ) AS rank
  FROM unioned, (SELECT 0 AS direct_only UNION ALL SELECT 1)
)
SELECT direct_only, language, rank, name, repository_count, direct_count
FROM ranked
WHERE rank <= {TOP_PACKAGES_DEPTH};

INSERT INTO agg_dependency_buckets
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

INSERT INTO agg_source_comparison
SELECT r.language,
       sum(CASE WHEN k.source = 'syft' THEN 1 ELSE 0 END),
       sum(CASE WHEN k.source = 'github-depgraph' THEN 1 ELSE 0 END)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
JOIN repositories r ON r.id = a.repository_id
WHERE r.language <> ''
GROUP BY r.language;
"""


def meta_sql(
    generator: str,
    schema_version: str,
    freshness: Mapping[str, str],
) -> str:
    """The one provenance row.

    Freshness is passed in rather than recomputed: it comes from the
    observation dates present in the data, which the caller has already
    read off the rows it wrote. An absent span is stored as empty
    strings — a default date would read as a real observation.
    """
    values = ', '.join(
        sql_literal(v) for v in (
            generator,
            schema_version,
            freshness.get('observedFrom', ''),
            freshness.get('observedTo', ''),
        )
    )
    return f'INSERT INTO meta VALUES ({values});\n'
