"""A snapshot's tables: `export d1`'s, a page table, and a fuller `meta`.

A snapshot starts from `D1_SCHEMA` (#132): every table and column the
D1 backend reads, under the same names, with D1's indexes, so that the
same statements answer the same questions over either. It adds two
things.

**`meta`**, after the four columns D1's has, which `Dataset` reads as it
reads D1's: which snapshot this is, the code that wrote it, the corpus
it describes, and how many rows each table has.

**`dependants`**, the one page-shaped table #128 §2.4 asks for that
measured a gain: the rows of the dependants table, one per repository,
version, relationship, ecosystem and date of a package, stored in the
order the page shows them (`WITHOUT ROWID`, clustered on its key). D1
groups and sorts every artifact of the package for each page and each
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
are the same, so that their rows interleave by version as D1 orders
them. The date is the row's own source's (`observations`), as D1 dates
it, and the language bucket is the repository's, for the filter.

It is made in SQLite from the D1 tables (`DEPENDANTS_SQL`), after them,
by the one statement that also makes it for `d1.sql` in the contract
tests: its rows are a function of theirs, so the snapshot's id, made of
theirs, is made of it too.
"""
from __future__ import annotations

from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import D1Column
from chatsbom.export.d1 import D1Schema
from chatsbom.export.d1 import D1Table
from chatsbom.export.d1 import META as D1_META
from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.relationship import RELATIONSHIPS


class Clustered(D1Table):
    """A table stored as the B-tree of its key (`WITHOUT ROWID`): a
    range of the key is read in the key's order, from the rows
    themselves."""

    def ddl(self) -> str:
        return super().ddl().removesuffix(';') + ' WITHOUT ROWID;'


META = D1Table(
    name='meta',
    description=(
        'Provenance: which snapshot this is, the build that wrote it and '
        'the corpus it describes, how fresh its rows are and how many '
        'there are. One row.'
    ),
    columns=(
        *D1_META.columns,
        D1Column(
            'snapshot', 'TEXT NOT NULL',
            'Its id: the file is `<id>.sqlite`, and the same content has '
            'the same id.',
        ),
        D1Column(
            'version', 'TEXT NOT NULL',
            "The version of chatsbom that wrote it: the generator's.",
        ),
        D1Column(
            'corpus', 'TEXT NOT NULL',
            'The search snapshot the corpus is, or empty when the store '
            'has none and the corpus is every repository.',
        ),
        D1Column(
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
        D1Column('package_id', 'INTEGER NOT NULL', 'References packages.id.'),
        D1Column(
            'place', 'INTEGER NOT NULL',
            "The repository's place in the order: by stars, most first, "
            'then owner and name, a dense rank.',
        ),
        D1Column('version', 'TEXT NOT NULL', 'Version as resolved.'),
        D1Column('relationship', 'TEXT NOT NULL', ' | '.join(RELATIONSHIPS)),
        D1Column(
            'type', 'TEXT NOT NULL',
            'Canonical ecosystem, as `kinds` has it.',
        ),
        D1Column(
            'observed_on', 'TEXT NOT NULL',
            "When the row's own source ("
            + ' | '.join(ARTIFACT_SOURCES)
            + ') last observed the repository, YYYY-MM-DD.',
        ),
        D1Column(
            'repository_id', 'INTEGER NOT NULL',
            'References repositories.id.',
        ),
        D1Column(
            'language_bucket', 'TEXT NOT NULL',
            "The repository's, which the language filter matches.",
        ),
        D1Column(
            'manifests', 'INTEGER NOT NULL',
            'The facts the row collapses: one unless two catalogers or '
            'two sources reported it.',
        ),
    ),
)

#: `dependants`, from the D1 tables: `web/src/d1/queries.ts`'s grouping
#: of a package's artifacts into the rows the table shows, for every
#: package at once, grouped by the key so that its rows come in order.
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

#: D1's tables in D1's order, `meta` the snapshot's, and the page table
#: after the facts it is made of.
SCHEMA = D1Schema(
    tables=tuple(
        made
        for table in D1_SCHEMA.tables
        for made in (
            (META,) if table.name == META.name
            else (table, DEPENDANTS) if table.name == 'artifacts'
            else (table,)
        )
    ),
    indexes=D1_SCHEMA.indexes,
)
