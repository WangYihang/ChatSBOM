"""A snapshot's tables: D1's, a page table, and a fuller `meta`.

A snapshot is one read-only SQLite file (#132), whose tables are the
ones `chatsbom export d1` wrote for Cloudflare D1, under the same names,
with D1's indexes: the Worker's D1 backend and the Python dataset API
asked them the same questions, and were held to the same answers
(`web/test/fixtures/contract/`). The Worker and `export d1` are gone
(#151), and the tables are the snapshot's own, declared here. It adds
two things to them.

**`meta`**, after the four columns D1's had, which `Dataset` reads as it
read D1's: which snapshot this is, the code that wrote it, the corpus
it describes, and how many rows each table has.

**`dependants`**, the one page-shaped table #128 §2.4 asks for that
measured a gain: the rows of the dependants table, one per repository,
version, relationship, ecosystem and date of a package, stored in the
order the page shows them (`WITHOUT ROWID`, clustered on its key). D1
grouped and sorted every artifact of the package for each page and each
count; this reads a range. At the documented shape (16.1M facts; the
most used package in 16,116 repositories and 25,313 rows) a page and
its counts took 171 ms from D1's tables and 13.6 ms from this one, 0.5
ms for the first page, for 956 MB of the file and 80 s of the build.
A covering index for the filtered counts was measured too, and left
out: it made some counts faster and misled SQLite into sorting pages.

The page's order is a repository's stars, most first, then its owner
and name, then the version, relationship, ecosystem, date and the
repository's id. `place` stands for the first three: a repository's
dense rank by them, which two repositories share only when the three
are the same, so that their rows interleave by version as D1 ordered
them. The date is the row's own source's (`observations`), as D1 dated
it, and the language bucket is the repository's, for the filter.

It is made in SQLite from the other tables, after them, by one
function, `add_dependants`: `snapshot build` makes it so, and so do the
tests, for the contract's corpus, `d1.sql`, applied to a file. Its rows
are a function of theirs, so the snapshot's id, made of theirs, is made
of it too; and a file of D1's tables alone, without it, is not a
snapshot the dataset can serve.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import field

from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.provenance import VERSION_KINDS
from chatsbom.models.relationship import RELATIONSHIPS


def _one_of(values: Iterable[str]) -> str:
    """A column's description, listing the values it holds.

    Taken from the types that define them. Written out by hand,
    `version_kind` said `exact | range | unknown`, which it never held,
    and `source` did not have `manifest` once schema version 7 added it.
    """
    return ' | '.join(values) + '.'


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str


@dataclass(frozen=True)
class Table:
    name: str
    description: str
    columns: tuple[Column, ...]
    primary_key: str | None = None

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
class Index:
    table: str
    columns: tuple[str, ...]
    unique: bool = False

    @property
    def name(self) -> str:
        return f'idx_{self.table}_{"_".join(self.columns)}'

    def ddl(self) -> str:
        kind = 'UNIQUE INDEX' if self.unique else 'INDEX'
        cols = ', '.join(self.columns)
        return (
            f'CREATE {kind} IF NOT EXISTS {self.name} '
            f'ON {self.table}({cols});'
        )


@dataclass(frozen=True)
class Schema:
    tables: tuple[Table, ...] = field(default=())
    indexes: tuple[Index, ...] = field(default=())

    def table(self, name: str) -> Table:
        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f'no table named {name!r}')


PACKAGES = Table(
    name='packages',
    description='Distinct package names, referenced by artifacts.',
    primary_key='id',
    columns=(
        Column('id', 'INTEGER', 'Surrogate key.'),
        Column(
            'name', 'TEXT NOT NULL',
            'Package name as the ecosystem spells it.',
        ),
        Column(
            'repositories', 'INTEGER NOT NULL DEFAULT 0',
            'Repositories depending on it. Filled by the aggregates.',
        ),
    ),
)

VERSIONS = Table(
    name='versions',
    description='Distinct version strings, referenced by artifacts.',
    primary_key='id',
    columns=(
        Column('id', 'INTEGER', 'Surrogate key.'),
        Column('version', 'TEXT NOT NULL', 'Version as resolved.'),
    ),
)

KINDS = Table(
    name='kinds',
    description=(
        'The 45 observed combinations of the five low-cardinality '
        'columns, referenced by artifacts instead of repeated per row.'
    ),
    primary_key='id',
    columns=(
        Column('id', 'INTEGER', 'Surrogate key.'),
        Column(
            'type', 'TEXT NOT NULL',
            'Canonical ecosystem, e.g. gem, npm or maven: one name per '
            'registry, however the collector spelled it.',
        ),
        Column('found_by', 'TEXT NOT NULL', 'Cataloguer that reported it.'),
        Column('relationship', 'TEXT NOT NULL', _one_of(RELATIONSHIPS)),
        Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        Column('version_kind', 'TEXT NOT NULL', _one_of(VERSION_KINDS)),
    ),
)

ARTIFACTS = Table(
    name='artifacts',
    description='One row per observed dependency, as four integers.',
    columns=(
        Column(
            'repository_id', 'INTEGER NOT NULL',
            'Repository it was found in.',
        ),
        Column('package_id', 'INTEGER NOT NULL', 'References packages.id.'),
        Column('version_id', 'INTEGER NOT NULL', 'References versions.id.'),
        Column('kind_id', 'INTEGER NOT NULL', 'References kinds.id.'),
    ),
)

#: When each source last observed each repository: the date the
#: dependants table shows beside a row (#41).
#:
#: A repository's own `observed_at` is its newest observation from any
#: source, and dating every row by it put September beside a February
#: Syft scan whenever the dependency graph came later — ClickHouse
#: dated each row by its own (#24). The rows reference their source
#: through `kinds`, so the date is a join on `(repository_id, source)`,
#: kept here once per pair rather than on six million rows.
#:
#: Unkeyed, its rows placed by rowid; the unique index on the pair is
#: what the dependants' join looks up.
OBSERVATIONS = Table(
    name='observations',
    description=(
        'When each source last observed each repository: the date of its '
        'current observation, per repository and source.'
    ),
    columns=(
        Column(
            'repository_id', 'INTEGER NOT NULL',
            'References repositories.id.',
        ),
        Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        Column(
            'observed_at', 'TEXT NOT NULL',
            'Its current observation by that source, as a UTC date, '
            'YYYY-MM-DD.',
        ),
    ),
)

REPOSITORIES = Table(
    name='repositories',
    description=(
        'One row per repository of the current search snapshot, '
        'collected or not.'
    ),
    primary_key='id',
    columns=(
        Column('id', 'INTEGER', 'GitHub repository id.'),
        Column('owner', 'TEXT NOT NULL', 'Repository owner login.'),
        Column('repo', 'TEXT NOT NULL', 'Repository name.'),
        Column(
            'stars', 'INTEGER NOT NULL',
            'Star count at collection time.',
        ),
        Column(
            'language', 'TEXT NOT NULL',
            "GitHub's primary language, lowercased.",
        ),
        Column(
            'github_language', 'TEXT NOT NULL',
            "GitHub's primary language, as GitHub spells it.",
        ),
        Column(
            'language_bucket', 'TEXT NOT NULL',
            'The language folded for display: one of the top twelve, '
            "'other' or 'none'. What the language filter matches.",
        ),
        Column(
            'ecosystems', 'TEXT NOT NULL',
            'Canonical ecosystems of the current scan, as a JSON array.',
        ),
        Column('url', 'TEXT NOT NULL', 'Repository URL.'),
        Column('description', 'TEXT NOT NULL', 'Repository description.'),
        Column(
            'license_spdx_id', 'TEXT NOT NULL',
            'SPDX licence id, or empty.',
        ),
        Column(
            'pushed_at', 'TEXT NOT NULL',
            'Last push upstream, YYYY-MM-DD.',
        ),
        Column(
            'observed_at', 'TEXT NOT NULL',
            'When this pipeline last scanned it.',
        ),
        Column(
            'sbom_ref', 'TEXT NOT NULL',
            'Tag or branch the SBOM came from.',
        ),
        Column(
            'sbom_commit_sha', 'TEXT NOT NULL',
            'Commit the SBOM describes.',
        ),
        Column(
            'direct_dependencies',
            'INTEGER NOT NULL', 'Declared packages.',
        ),
        Column(
            'total_dependencies', 'INTEGER NOT NULL',
            'Resolved closure size.',
        ),
    ),
)

LICENSES = Table(
    name='licenses',
    description='Licence shares, precomputed.',
    columns=(
        Column('license', 'TEXT NOT NULL', 'SPDX id, or empty for unknown.'),
        Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories carrying it.',
        ),
        Column(
            'package_count', 'INTEGER NOT NULL',
            'Distinct packages carrying it.',
        ),
    ),
)

HISTORY = Table(
    name='history',
    description=(
        'Monthly adoption series per package, per source. Two sources '
        'measure differently, so a series that mixed them would show a '
        'change of instrument as a change in adoption.'
    ),
    columns=(
        Column('name', 'TEXT NOT NULL', 'Package name.'),
        Column('month', 'TEXT NOT NULL', 'YYYY-MM.'),
        Column('source', 'TEXT NOT NULL', _one_of(ARTIFACT_SOURCES)),
        Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories depending on it.',
        ),
        Column(
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

AGG_TOTALS = Table(
    name='agg_totals',
    description='The numbers the tiles and the footer show. One row.',
    columns=(
        Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories with dependency data.',
        ),
        Column('dependencies', 'INTEGER NOT NULL', 'Dependency records.'),
        Column('packages', 'INTEGER NOT NULL', 'Distinct packages.'),
        Column(
            'classified', 'INTEGER NOT NULL',
            'Records with a known relationship.',
        ),
        Column(
            'tracked', 'INTEGER NOT NULL',
            'Repositories in the current search snapshot, collected or '
            'not: the denominator of every coverage ratio.',
        ),
    ),
)

AGG_RELATIONSHIP_SPLIT = Table(
    name='agg_relationship_split',
    description=(
        'Declared / inherited / undetermined, per ecosystem and overall.'
    ),
    columns=(
        Column(
            'ecosystem', 'TEXT NOT NULL',
            "Canonical ecosystem, or '' for the whole corpus.",
        ),
        Column('relationship', 'TEXT NOT NULL', _one_of(RELATIONSHIPS)),
        Column('records', 'INTEGER NOT NULL', 'Dependency records.'),
    ),
)

#: Coverage by the repository's GitHub language, folded (D7).
AGG_LANGUAGE_COVERAGE = Table(
    name='agg_language_coverage',
    description=(
        'Repositories per GitHub language (top twelve, other, none), and '
        'how many each source covers.'
    ),
    columns=(
        Column(
            'language', 'TEXT NOT NULL',
            "Language bucket: a top-twelve language, 'other' or 'none'.",
        ),
        Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories of the snapshot in that bucket.',
        ),
        Column(
            'with_sbom', 'INTEGER NOT NULL',
            'Of those, with dependencies recorded by any source.',
        ),
        Column(
            'with_syft', 'INTEGER NOT NULL',
            'Of those, with a Syft scan.',
        ),
        Column(
            'with_depgraph', 'INTEGER NOT NULL',
            "Of those, with GitHub's dependency graph.",
        ),
        Column(
            'with_manifest', 'INTEGER NOT NULL',
            'Of those, with Gradle build-file declarations.',
        ),
    ),
)

#: Coverage per ecosystem. Rows overlap: a repository counts under
#: every ecosystem it has, so they must not be summed.
AGG_ECOSYSTEM_COVERAGE = Table(
    name='agg_ecosystem_coverage',
    description=(
        'Per ecosystem: repositories that have it, and how many of those '
        'each source covers.'
    ),
    columns=(
        Column('ecosystem', 'TEXT NOT NULL', 'Canonical ecosystem.'),
        Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories whose artifacts or manifests are of it.',
        ),
        Column(
            'with_any', 'INTEGER NOT NULL',
            'Of those, with a dependency record of it from any source.',
        ),
        Column('with_syft', 'INTEGER NOT NULL', 'Of those, from Syft.'),
        Column(
            'with_depgraph', 'INTEGER NOT NULL',
            "Of those, from GitHub's dependency graph.",
        ),
        Column(
            'with_manifest', 'INTEGER NOT NULL',
            'Of those, from Gradle build files.',
        ),
    ),
)

AGG_TOP_PACKAGES = Table(
    name='agg_top_packages',
    description=(
        'The ranking, precomputed per filter combination: the panel has '
        'exactly two controls, so the set of answers is finite.'
    ),
    columns=(
        Column(
            'direct_only', 'INTEGER NOT NULL',
            '1 when counting declarations only.',
        ),
        Column(
            'ecosystem', 'TEXT NOT NULL',
            "Ecosystem filter, or '' for all.",
        ),
        Column('rank', 'INTEGER NOT NULL', '1-based position.'),
        Column('name', 'TEXT NOT NULL', 'Package name.'),
        Column(
            'repository_count', 'INTEGER NOT NULL',
            'Repositories depending on it.',
        ),
        Column(
            'direct_count', 'INTEGER NOT NULL',
            'Of those, declaring it.',
        ),
    ),
)

AGG_DEPENDENCY_BUCKETS = Table(
    name='agg_dependency_buckets',
    description='Repositories per dependency-count bucket.',
    columns=(
        Column('bucket', 'TEXT NOT NULL', 'Bucket label, e.g. 100-249.'),
        Column(
            'position', 'INTEGER NOT NULL',
            'Sort order, since labels are not ordinal.',
        ),
        Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories in the bucket.',
        ),
    ),
)

AGG_SOURCE_COMPARISON = Table(
    name='agg_source_comparison',
    description='Dependency records per ecosystem, split by collector.',
    columns=(
        Column('ecosystem', 'TEXT NOT NULL', 'Canonical ecosystem.'),
        Column('syft', 'INTEGER NOT NULL', 'Records from syft.'),
        Column(
            'depgraph', 'INTEGER NOT NULL',
            "Records from GitHub's dependency graph.",
        ),
        Column(
            'manifest', 'INTEGER NOT NULL',
            'Records from Gradle build files.',
        ),
    ),
)


AGG_EDGES = Table(
    name='agg_edges',
    description=(
        'Package-to-package dependency edges, aggregated by name: how '
        'many repositories show this parent pulling in this child.'
    ),
    columns=(
        Column('parent_id', 'INTEGER NOT NULL', 'References packages.id.'),
        Column('child_id', 'INTEGER NOT NULL', 'References packages.id.'),
        Column(
            'repositories', 'INTEGER NOT NULL',
            'Repositories in which the parent pulls in the child.',
        ),
    ),
)


class Clustered(Table):
    """A table stored as the B-tree of its key (`WITHOUT ROWID`): a
    range of the key is read in the key's order, from the rows
    themselves."""

    def ddl(self) -> str:
        return super().ddl().removesuffix(';') + ' WITHOUT ROWID;'


META = Table(
    name='meta',
    description=(
        'Provenance: which snapshot this is, the build that wrote it and '
        'the corpus it describes, how fresh its rows are and how many '
        'there are. One row.'
    ),
    columns=(
        # The four D1's `meta` had, which `Dataset` reads as it read D1's.
        Column(
            'generator', 'TEXT NOT NULL',
            'Build that produced the data.',
        ),
        Column(
            'schema_version', 'TEXT NOT NULL',
            'Export contract version.',
        ),
        Column(
            'observed_from', 'TEXT NOT NULL',
            'Earliest observation date.',
        ),
        Column('observed_to', 'TEXT NOT NULL', 'Latest observation date.'),
        Column(
            'snapshot', 'TEXT NOT NULL',
            'Its id: the file is `<id>.sqlite`, and the same content has '
            'the same id.',
        ),
        Column(
            'version', 'TEXT NOT NULL',
            "The version of chatsbom that wrote it: the generator's.",
        ),
        Column(
            'corpus', 'TEXT NOT NULL',
            'The search snapshot the corpus is, or empty when the store '
            'has none and the corpus is every repository.',
        ),
        Column(
            'rows', 'TEXT NOT NULL',
            'Rows of each table, this one included, as a JSON object.',
        ),
    ),
)

DEPENDANTS = Clustered(
    name='dependants',
    description=(
        "The dependants table's rows, in its order: one per repository, "
        'version, relationship, ecosystem and date of a package.'
    ),
    primary_key=(
        'package_id, place, version, relationship, type, observed_on, '
        'repository_id'
    ),
    columns=(
        Column('package_id', 'INTEGER NOT NULL', 'References packages.id.'),
        Column(
            'place', 'INTEGER NOT NULL',
            "The repository's place in the order: by stars, most first, "
            'then owner and name, a dense rank.',
        ),
        Column('version', 'TEXT NOT NULL', 'Version as resolved.'),
        Column('relationship', 'TEXT NOT NULL', ' | '.join(RELATIONSHIPS)),
        Column(
            'type', 'TEXT NOT NULL',
            'Canonical ecosystem, as `kinds` has it.',
        ),
        Column(
            'observed_on', 'TEXT NOT NULL',
            "When the row's own source ("
            + ' | '.join(ARTIFACT_SOURCES)
            + ') last observed the repository, YYYY-MM-DD.',
        ),
        Column(
            'repository_id', 'INTEGER NOT NULL',
            'References repositories.id.',
        ),
        Column(
            'language_bucket', 'TEXT NOT NULL',
            "The repository's, which the language filter matches.",
        ),
        Column(
            'manifests', 'INTEGER NOT NULL',
            'The facts the row collapses: one unless two catalogers or '
            'two sources reported it.',
        ),
    ),
)

#: `dependants`, from the other tables: the grouping of a package's
#: artifacts into the rows the table shows, as D1's statements grouped
#: them for one package, for every package at once, grouped by the key
#: so that its rows come in order.
DEPENDANTS_SQL = f"""
INSERT INTO dependants ({', '.join(DEPENDANTS.column_names)})
SELECT a.package_id, r.place, v.version, k.relationship, k.type,
       coalesce(o.observed_at, r.observed_at) AS observed_on,
       r.id, r.language_bucket, count(*)
FROM artifacts AS a
JOIN versions AS v ON v.id = a.version_id
JOIN kinds AS k ON k.id = a.kind_id
JOIN (
    SELECT id, language_bucket, observed_at,
           dense_rank() OVER (ORDER BY stars DESC, owner, repo) AS place
    FROM repositories
) AS r ON r.id = a.repository_id
LEFT JOIN observations AS o
  ON o.repository_id = a.repository_id AND o.source = k.source
GROUP BY a.package_id, r.place, v.version, k.relationship, k.type,
         observed_on, r.id
""".strip()

#: The tables, in the order a snapshot's id hashes them (`write`): D1's,
#: with the page table after the facts it is made of, and `meta` last.
SCHEMA = Schema(
    tables=(
        REPOSITORIES, ARTIFACTS, DEPENDANTS, OBSERVATIONS, PACKAGES,
        VERSIONS, KINDS, LICENSES, HISTORY, AGG_TOTALS,
        AGG_RELATIONSHIP_SPLIT, AGG_LANGUAGE_COVERAGE,
        AGG_ECOSYSTEM_COVERAGE, AGG_TOP_PACKAGES, AGG_DEPENDENCY_BUCKETS,
        AGG_SOURCE_COMPARISON, AGG_EDGES, META,
    ),
    indexes=(
        # Without these the joins table-scan six million rows.
        Index('artifacts', ('package_id',)),
        Index('artifacts', ('repository_id',)),
        # A dependants row's date: one lookup per row.
        Index('observations', ('repository_id', 'source'), unique=True),
        Index('packages', ('name',), unique=True),
        Index('versions', ('version',), unique=True),
        # What the dependants' language filter matches.
        Index('repositories', ('language_bucket',)),
        Index('history', ('name',)),
        # Aggregates are indexed by what their panel filters on, so a
        # request is a lookup rather than a scan of the aggregate.
        Index('agg_top_packages', ('direct_only', 'ecosystem', 'rank')),
        Index('agg_relationship_split', ('ecosystem',)),
        # Both directions. "What does X pull in" and "what pulls in X"
        # are different questions and the second is the more useful one
        # — it is how you find out why a package you never chose is in
        # your lockfile.
        Index('agg_edges', ('parent_id',)),
        Index('agg_edges', ('child_id',)),
    ),
)


def add_dependants(connection: sqlite3.Connection) -> int:
    """Make a file of the other tables, filled, one the dataset can
    serve: add its page table, made from their rows by `DEPENDANTS_SQL`.
    How many rows it has. Committing is the caller's."""
    connection.execute(DEPENDANTS.ddl())
    return connection.execute(DEPENDANTS_SQL).rowcount
