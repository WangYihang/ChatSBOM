"""The dashboard's questions, answered from a snapshot of the D1 schema.

The port of `web/src/d1/queries.ts`, with what it shares with the other
store in `web/src/dataset/`: one method for each of `DatasetQueries`
(`web/src/backend.ts`), in snake_case, each asking the statement the
TypeScript asks D1, so that the page gets one answer whichever service
it asks (#128 §2.5). `tests/dataset_contract_test.py` holds each method
to what D1 answered the contract suite.

Two things carry over from the TypeScript, and are load-bearing here as
they are there.

**The joins are not optional.** An artifact row is four integers,
`package_id`, `version_id`, `kind_id` and the repository, because the
strings cost 762.6 MB against D1's 500 MB and interning them brought
that to 294.7 MB. So a package is looked up through `packages`;
comparing a name on the fact table is a column that is not there.

**The aggregates are read, never recomputed** (`reads.py`). They read
every artifact row by definition, and are precomputed when the file is
written.

What is new is that each method checks its own arguments (`params.py`)
before it asks anything: there is no endpoint in front of it, and the
web routes, the chat's tools and the CLI all call it directly. No method
takes SQL, or a column's name, and no caller's value is spliced into a
statement: every value is bound.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from typing import Any
from typing import Protocol

from chatsbom.core.ecosystems import canonical
from chatsbom.dataset import params
from chatsbom.dataset import reads
from chatsbom.dataset.reads import Read
from chatsbom.dataset.shape import AnswerT
from chatsbom.dataset.shape import bounded_limit
from chatsbom.dataset.shape import bounded_offset
from chatsbom.dataset.shape import num
from chatsbom.dataset.shape import relationship_of
from chatsbom.dataset.shape import Row
from chatsbom.dataset.shape import shape_dependant
from chatsbom.dataset.shape import shape_edge
from chatsbom.dataset.shape import shape_read
from chatsbom.dataset.shape import shape_spread
from chatsbom.dataset.shape import shape_tree_edge
from chatsbom.dataset.shape import text
from chatsbom.dataset.shape import tree_shape
from chatsbom.dataset.types import AdoptionPoint
from chatsbom.dataset.types import DatasetMeta
from chatsbom.dataset.types import DependencyBucket
from chatsbom.dataset.types import DependencyTree
from chatsbom.dataset.types import Dependent
from chatsbom.dataset.types import EcosystemCoverage
from chatsbom.dataset.types import EcosystemRelationship
from chatsbom.dataset.types import EcosystemShare
from chatsbom.dataset.types import EdgeAmbiguity
from chatsbom.dataset.types import LanguageCoverage
from chatsbom.dataset.types import LicenseShare
from chatsbom.dataset.types import PackageEdge
from chatsbom.dataset.types import PackageMatch
from chatsbom.dataset.types import PackagePopularity
from chatsbom.dataset.types import RelationshipSplit
from chatsbom.dataset.types import SourceComparison
from chatsbom.dataset.types import Totals
from chatsbom.dataset.types import VersionShare
from chatsbom.dataset.types import VersionSpread

#: A value a statement is bound with.
Value = str | int


class Queryable(Protocol):
    """The slice of a SQLite connection the dataset reads through, as
    narrow as `D1Queryable` is, so that a test can watch what it asks."""

    def execute(
        self, sql: str, parameters: Sequence[Any], /,
    ) -> sqlite3.Cursor:
        ...


#: Where a dependants row is read from.
#:
#: Its date is its own source's observation of the repository
#: (`observations`, one row per repository and source). The
#: repository's `observed_at` is the newest of those, and dating every
#: row by it put September beside a February Syft scan whenever the
#: dependency graph came later (#24). It remains the fallback for a row
#: whose date the export did not write, which would otherwise vanish.
_DEPENDANTS = """
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       LEFT JOIN observations AS o
         ON o.repository_id = a.repository_id
        AND o.source = k.source"""

_OBSERVED = 'coalesce(o.observed_at, r.observed_at) AS observed_on'

#: One dependants row per repository, version, relationship, ecosystem
#: and date, which is what the table shows, whatever number of facts it
#: collapses. The count of rows groups by the same keys, so a page never
#: runs past the end.
_ONE_ROW = 'r.id, v.version, k.relationship, k.type, observed_on'

#: What a package pulls in: a lookup by the parent, naming the child.
_PULLS_IN = """
       SELECT c.name AS name, e.repositories AS repositories
       FROM agg_edges AS e
       JOIN packages AS p ON p.id = e.parent_id
       JOIN packages AS c ON c.id = e.child_id
       WHERE p.name = ?
       ORDER BY e.repositories DESC, c.name
       LIMIT ?"""

#: What pulls a package in: a lookup by the child, naming the parent,
#: on the index `idx_agg_edges_child_id` exists for.
_PULLED_IN_BY = """
       SELECT p.name AS name, e.repositories AS repositories
       FROM agg_edges AS e
       JOIN packages AS c ON c.id = e.child_id
       JOIN packages AS p ON p.id = e.parent_id
       WHERE c.name = ?
       ORDER BY e.repositories DESC, p.name
       LIMIT ?"""

#: A LIKE pattern's own characters, which a search escapes.
_WILDCARDS = re.compile(r'[\\%_]')


class _Filters:
    """What "depends on this package" means for one question, checked.

    Shared by the rows and both counts, so the three cannot diverge: a
    count over other filters than the rows beside it is worse than no
    count, because it looks authoritative and disagrees with what the
    reader can see.
    """

    def __init__(
        self,
        name: object,
        type: object,
        language: object,
        direct_only: object,
    ) -> None:
        self.name = params.name('name', name)
        self.type = params.ecosystem('type', type)
        self.language = params.word('language', language)
        self.direct_only = params.flag('direct_only', direct_only)

    def where(self) -> tuple[str, list[Value]]:
        """The WHERE clause's predicates, and what they are bound with."""
        filters = ['p.name = ?']
        bound: list[Value] = [self.name]
        if self.type:
            # Under the name the page shows, which is how `kinds` stores
            # it: Syft's `php-composer`, passed through, matched nothing.
            filters.append('k.type = ?')
            bound.append(canonical(self.type))
        if self.language:
            filters.append('r.language_bucket = ?')
            bound.append(self.language.lower())
        if self.direct_only:
            filters.append('k.relationship = ?')
            bound.append('direct')
        return ' AND '.join(filters), bound


def _or(value: int | None, default: int) -> int:
    """What was asked for, or the method's own default when nothing was:
    each method has its own, as each TypeScript method does."""
    return default if value is None else value


class Dataset:
    """`DatasetQueries`, over a snapshot of the D1 schema.

    Its public methods are those of the interface and nothing else,
    which `tests/dataset_contract_test.py` holds it to: a caller names a
    question and gives its values, and never a statement.
    """

    def __init__(self, db: Queryable) -> None:
        self._db = db

    def _rows(self, sql: str, bound: Sequence[Value] = ()) -> list[Row]:
        with closing(self._db.execute(sql, bound)) as cursor:
            columns = [column[0] for column in cursor.description or ()]
            return [
                dict(zip(columns, row, strict=True))
                for row in cursor.fetchall()
            ]

    def _read(
        self, read: Read[AnswerT], bound: Sequence[Value] = (),
    ) -> list[AnswerT]:
        return [
            shape_read(read.answer, row) for row in self._rows(read.sql, bound)
        ]

    # ---- point lookups: an arbitrary package name, so no precompute --

    def dependents_of(
        self,
        name: str,
        *,
        type: str | None = None,
        language: str | None = None,
        direct_only: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[Dependent]:
        """Repositories depending on a package, most starred first.

        `count(*)` collapses the rows the table shows as one: this schema
        keeps one row per dependency fact, so it counts the cataloguers
        that reported a version. Ordered by every key a row is grouped
        on, ending with the repository, so the order is total: each page
        is its own statement, and a partial order may fall either way in
        each of them.
        """
        where, bound = _Filters(name, type, language, direct_only).where()
        rows = params.whole('limit', limit)
        skip = params.whole('offset', offset, 0)
        return [
            shape_dependant(row)
            for row in self._rows(
                f"""
       SELECT r.owner AS owner, r.repo AS repo, r.stars AS stars,
              v.version AS version, r.url AS url,
              r.github_language AS language, k.type AS ecosystem,
              k.relationship AS relationship, {_OBSERVED},
              count(*) AS manifests
       {_DEPENDANTS}
       WHERE {where}
       GROUP BY {_ONE_ROW}
       ORDER BY r.stars DESC, r.owner, r.repo, v.version, k.relationship,
                k.type, observed_on, r.id
       LIMIT ? OFFSET ?""",
                # Clamped, so no hand-edited URL asks for a page past
                # the end of every package.
                [*bound, bounded_limit(rows), bounded_offset(skip)],
            )
        ]

    def count_dependents(
        self,
        name: str,
        *,
        type: str | None = None,
        language: str | None = None,
        direct_only: bool = False,
    ) -> int:
        """How many repositories depend on a package, unlimited.

        `dependents_of` is capped, so the length of what it returns is a
        display limit rather than a count. A count takes no page: the
        TypeScript interface takes one and drops it.
        """
        where, bound = _Filters(name, type, language, direct_only).where()
        rows = self._rows(
            f"""
       SELECT count(DISTINCT a.repository_id) AS total
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       WHERE {where}""",
            bound,
        )
        return num(rows[0]['total'] if rows else None)

    def count_dependent_rows(
        self,
        name: str,
        *,
        type: str | None = None,
        language: str | None = None,
        direct_only: bool = False,
    ) -> int:
        """The rows `dependents_of` pages through, which is not the
        dependant count: 492 against 326 for `laravel/framework`. Paging
        on the dependant count would stop halfway through a package."""
        where, bound = _Filters(name, type, language, direct_only).where()
        rows = self._rows(
            f"""
       SELECT count(*) AS total FROM (
           SELECT {_OBSERVED}
           {_DEPENDANTS}
           WHERE {where}
           GROUP BY {_ONE_ROW}
       )""",
            bound,
        )
        return num(rows[0]['total'] if rows else None)

    def ecosystems_for(self, name: str) -> list[EcosystemShare]:
        """Which ecosystems a package name appears in.

        Asked before any count is presented as "dependants of X": a name
        shared across ecosystems is two packages, and `mail` is a gem, a
        Maven artifact and a PyPI package. `kinds.type` is already the
        name shown, so a repository holding both collectors' spellings
        of Composer is counted once.
        """
        package = params.name('name', name)
        rows = self._rows(
            """
       SELECT k.type AS type,
              count(DISTINCT a.repository_id) AS repository_count,
              count(DISTINCT CASE WHEN k.relationship = 'direct'
                                  THEN a.repository_id END) AS direct_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       WHERE p.name = ?
       GROUP BY k.type
       ORDER BY repository_count DESC, k.type""",
            [package],
        )
        return [
            EcosystemShare(
                type=text(row['type']),
                repository_count=num(row['repository_count']),
                direct_count=num(row['direct_count']),
            )
            for row in rows
        ]

    def version_spread(
        self, name: str, limit: int | None = None,
    ) -> VersionSpread:
        """Which resolved versions are in use, ten unless asked, and how
        many repositories were set aside for a constraint or no version.

        Every kind is asked for and the list sliced after, since the top
        ten *resolved* versions are not the resolved rows among the top
        ten of everything. What is not a resolution is grouped by its
        kind alone, so each repository counts once however many
        constraint strings it declares (#120).
        """
        package = params.name('name', name)
        shown = bounded_limit(_or(params.whole('limit', limit), 10))
        rows = self._rows(
            """
       SELECT k.version_kind AS version_kind,
              CASE WHEN k.version_kind = 'resolved' THEN v.version ELSE '' END
                AS listed,
              count(DISTINCT a.repository_id) AS repository_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       WHERE p.name = ?
       GROUP BY k.version_kind, listed
       ORDER BY repository_count DESC, listed""",
            [package],
        )
        return shape_spread(
            [
                VersionShare(
                    kind=text(row['version_kind']),
                    version=text(row['listed']),
                    repository_count=num(row['repository_count']),
                )
                for row in rows
            ],
            shown,
        )

    def adoption_over_time(self, name: str) -> list[AdoptionPoint]:
        """One package's monthly series, per collector."""
        return self._read(
            reads.ADOPTION_OVER_TIME, [params.name('name', name)],
        )

    def search_packages(
        self, term: str, limit: int | None = None,
    ) -> list[PackageMatch]:
        """Package names beginning with a term, most depended upon
        first, twenty unless asked.

        Anchored at the start, so it is a range on `idx_packages_name`
        rather than a scan of every name, and escaped, so a `%` or `_` a
        reader typed is not a wildcard. Ranked by the count the export
        stores on `packages`, one row per name across its ecosystems:
        splitting it would count the artifacts on every keystroke.
        """
        start = params.name('term', term)
        shown = bounded_limit(_or(params.whole('limit', limit), 20))
        escaped = _WILDCARDS.sub(lambda match: '\\' + match.group(), start)
        rows = self._rows(
            """
       SELECT p.name AS name, p.repositories AS repository_count
       FROM packages AS p
       WHERE p.name LIKE ? ESCAPE '\\'
       ORDER BY p.repositories DESC, p.name
       LIMIT ?""",
            [f'{escaped}%', shown],
        )
        return [
            PackageMatch(
                name=text(row['name']),
                ecosystem=None,
                repository_count=num(row['repository_count']),
                name_total=num(row['repository_count']),
            )
            for row in rows
        ]

    # ---- the edge table: both directions, and a bounded tree ---------

    def dependencies_of(
        self, name: str, limit: int | None = None,
    ) -> list[PackageEdge]:
        """What a package pulls in, twenty unless asked."""
        package = params.name('name', name)
        shown = bounded_limit(_or(params.whole('limit', limit), 20))
        return self._edges(_PULLS_IN, package, shown)

    def pulled_in_by(
        self, name: str, limit: int | None = None,
    ) -> list[PackageEdge]:
        """What pulls a package in, twenty unless asked: how a reader
        finds out why a package they never chose is in their lockfile."""
        package = params.name('name', name)
        shown = bounded_limit(_or(params.whole('limit', limit), 20))
        return self._edges(_PULLED_IN_BY, package, shown)

    def _edges(self, sql: str, name: str, limit: int) -> list[PackageEdge]:
        return [shape_edge(row) for row in self._rows(sql, [name, limit])]

    def dependency_tree(
        self,
        name: str,
        *,
        children: int | None = None,
        branch: int | None = None,
    ) -> DependencyTree:
        """Two hops around one package, for the tree diagram, bounded at
        both, since a backend that returned every hop would hand the
        page what it cannot draw.

        `branch` is per parent, not a global cap: a global LIMIT would be
        spent on whichever child has the widest edges, and the others
        would draw as leaves, a claim the data does not make.
        """
        root = params.name('name', name)
        first, per_parent = tree_shape(
            params.whole('children', children),
            params.whole('branch', branch),
        )
        hop = self._edges(_PULLS_IN, root, first)
        if not hop:
            return DependencyTree(root=root, children=[], grandchildren=[])
        placeholders = ', '.join('?' for _ in hop)
        rows = self._rows(
            # The window has to be computed before it is filtered, hence
            # the subquery: SQLite takes no ROW_NUMBER() in the WHERE of
            # the SELECT that computes it. And the root is excluded inside
            # it, before it takes a rank: the edges run both ways
            # (`bytes` pulls in `body-parser` in one repository), and
            # excluded after, the row would go and its rank stay spent.
            f"""
       SELECT parent, child, repositories
       FROM (
         SELECT pp.name AS parent,
                cc.name AS child,
                e.repositories AS repositories,
                ROW_NUMBER() OVER (
                  PARTITION BY e.parent_id
                  ORDER BY e.repositories DESC, cc.name
                ) AS branch_rank
         FROM agg_edges AS e
         JOIN packages AS pp ON pp.id = e.parent_id
         JOIN packages AS cc ON cc.id = e.child_id
         WHERE pp.name IN ({placeholders}) AND cc.name <> ?
       )
       WHERE branch_rank <= ?
       ORDER BY repositories DESC, child, parent""",
            [*(edge.name for edge in hop), root, per_parent],
        )
        return DependencyTree(
            root=root,
            children=hop,
            grandchildren=[shape_tree_edge(row) for row in rows],
        )

    # ---- the overview: fixed questions, finite answers ---------------

    def totals(self) -> Totals:
        """The tiles' numbers: one stored row, and none is an empty
        dataset rather than a failure."""
        rows = self._read(reads.TOTALS)
        return rows[0] if rows else shape_read(Totals, {})

    def relationship_split(
        self, ecosystem: str | None = None,
    ) -> RelationshipSplit:
        """The declared/inherited split of the corpus, or of one
        ecosystem: keyed by the package's ecosystem, not the
        repository's language (#55 §4.12). The empty ecosystem is the
        corpus-wide row."""
        wanted = params.ecosystem('ecosystem', ecosystem)
        split = {'direct': 0, 'transitive': 0, 'unknown': 0}
        for row in self._rows(
            """
       SELECT relationship, records
       FROM agg_relationship_split
       WHERE ecosystem = ?""",
            [wanted.lower() if wanted else ''],
        ):
            split[relationship_of(row['relationship'])] += num(row['records'])
        return RelationshipSplit(
            direct=split['direct'],
            transitive=split['transitive'],
            unknown=split['unknown'],
        )

    def language_coverage(self) -> list[LanguageCoverage]:
        """Per GitHub language, folded to the top twelve and `other`."""
        return self._read(reads.LANGUAGE_COVERAGE)

    def ecosystem_coverage(self) -> list[EcosystemCoverage]:
        """Per ecosystem: repositories that have it, and what covers
        them. The rows overlap and must not be summed."""
        return self._read(reads.ECOSYSTEM_COVERAGE)

    def relationship_by_ecosystem(self) -> list[EcosystemRelationship]:
        """The declared/inherited split per ecosystem, all at once.

        `agg_relationship_split` holds it a row per relationship; the
        empty ecosystem is the corpus-wide row and not an ecosystem.
        Largest first, and tied by name as JavaScript compares strings,
        by UTF-16 code unit, so the order is the TypeScript's for any
        name.
        """
        counts: dict[str, dict[str, int]] = {}
        for row in self._rows(
            """
       SELECT ecosystem, relationship, records
       FROM agg_relationship_split
       WHERE ecosystem <> ''""",
        ):
            seen = counts.setdefault(
                text(row['ecosystem']),
                {'direct': 0, 'transitive': 0, 'unknown': 0, 'records': 0},
            )
            records = num(row['records'])
            seen[relationship_of(row['relationship'])] += records
            seen['records'] += records
        split = [
            EcosystemRelationship(
                ecosystem=ecosystem,
                direct=seen['direct'],
                transitive=seen['transitive'],
                unknown=seen['unknown'],
                records=seen['records'],
            )
            for ecosystem, seen in counts.items()
            if seen['records'] > 0
        ]
        return sorted(
            split,
            key=lambda row: (
                -row.records,
                row.ecosystem.encode('utf-16-be', 'surrogatepass'),
            ),
        )

    def edge_ambiguity(self) -> EdgeAmbiguity | None:
        """Not answered, and None rather than approximated.

        The collisions are names with more than one ecosystem among the
        artifacts, and counting them means grouping every artifact row
        by name and kind on request, which this schema's rule is not to
        do. The export could store the figure; until it does, a
        plausible number from the wrong denominator is how the
        hardcoded caveat went wrong in the first place.
        """
        return None

    def top_packages(
        self,
        *,
        direct_only: bool = False,
        ecosystem: str | None = None,
        limit: int | None = None,
    ) -> list[PackagePopularity]:
        """The ranking, under the panel's two controls: declared only,
        and one ecosystem or the whole corpus."""
        wanted = params.ecosystem('ecosystem', ecosystem)
        depth = params.whole('limit', limit)
        direct = params.flag('direct_only', direct_only)
        return self._read(
            reads.TOP_PACKAGES,
            [
                1 if direct else 0,
                # The empty string is the whole-corpus row.
                wanted.lower() if wanted else '',
                bounded_limit(depth),
            ],
        )

    def dependency_distribution(self) -> list[DependencyBucket]:
        """Repositories by how many packages they have, in the buckets'
        own order."""
        return self._read(reads.DEPENDENCY_DISTRIBUTION)

    def source_comparison(self) -> list[SourceComparison]:
        """Dependency records per ecosystem, by the collector that made
        them."""
        return self._read(reads.SOURCE_COMPARISON)

    def license_shares(self, limit: int | None = None) -> list[LicenseShare]:
        """The licences, widest first, twelve unless asked, unknown
        among them."""
        shown = bounded_limit(_or(params.whole('limit', limit), 12))
        return self._read(reads.LICENSE_SHARES, [shown])

    # ---- provenance --------------------------------------------------

    def meta(self) -> DatasetMeta:
        """Which build produced the data and how fresh it is: the one row
        the export writes. None is an unknown span, never an invented
        one."""
        rows = self._rows(
            """
       SELECT generator, schema_version, observed_from, observed_to
       FROM meta""",
        )
        row: Row = rows[0] if rows else {}
        version = text(row.get('schema_version'))
        return DatasetMeta(
            generator=text(row.get('generator')),
            # Prefixed by whoever knows what the value means. The stored
            # value is a contract number, `8`, and reads as nothing on
            # its own; the page shows this as it comes.
            schema_version=f'd1 v{version}' if version else '',
            observed_from=text(row.get('observed_from')),
            observed_to=text(row.get('observed_to')),
        )
