"""The queries that define what an export contains.

Its own module because neither export owns it. `export parquet` and
`export d1` both read these, and a copy per format would let the two
describe different data — which is the failure this prevents rather
than a tidiness preference: the Parquet files and the D1 snapshot are
published as the same dataset.

`observed_range` lives here for the same reason. Both manifests report
freshness, and both must derive it from the rows rather than a clock.

Every table here but `history` describes the present, and reads it from
the `current_artifacts` and `facts` views (`core/schema.py`), the same
definition the ClickHouse rollups use. All of them, `history` included,
cover the corpus: the repositories of the current search snapshot (owner
decision D2 on #55). Each query carried its own copy
of the scan-matching join before, six in all; `history` is the one that
reads every observation, because change over time is what it is for.

Every date is formatted in UTC, by name. The instants are stored right
(`core/instants.py`), but making a date of one takes a zone, and
`formatDateTime` without one takes the server's: a scan after 16:00
UTC would be dated the next day by a server in UTC+8. These files are
published as a dataset, so their dates do not depend on which server
wrote them.

`EXPORT_SETTINGS` and `whole` are here for the same reason as the
queries: how an export reads them decides whether it holds all of it.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from typing import TypeVar

from chatsbom.core.schema import language_bucket_sql
from chatsbom.models.relationship import DIRECT

T = TypeVar('T')

#: Every limit a query can meet stops it with an error, rather than
#: ending its result early.
#:
#: ClickHouse's `*_overflow_mode=break` stops returning rows without an
#: error, so a capped read looks exactly like a complete one: the guest
#: profile caps results that way, and an export read through it lost
#: 6.0M of 6.1M artifact rows and printed "Export Complete". The Parquet
#: export caught that by running each query a second time as a
#: `count()`, which for the artifacts is the most expensive query there
#: is; the D1 export did not catch it at all. Sent with each export
#: query, these make a cap fail the query instead, and a readonly
#: account, which may not change them, fail it before it starts.
EXPORT_SETTINGS: Mapping[str, str] = {
    setting: 'throw'
    for setting in (
        'result_overflow_mode',
        'read_overflow_mode',
        'read_overflow_mode_leaf',
        'timeout_overflow_mode',
        'timeout_overflow_mode_leaf',
        'group_by_overflow_mode',
        'sort_overflow_mode',
        'distinct_overflow_mode',
        'set_overflow_mode',
        'join_overflow_mode',
        'transfer_overflow_mode',
    )
}


class ExportStopped(RuntimeError):
    """An export query ended before its last row."""


def whole(table: str, stream: Iterable[T]) -> Iterator[T]:
    """Every item of an export query's `stream`, or an error that says
    which table the export stopped in.

    Only what reading the stream raises is caught: a cap the query met
    (`EXPORT_SETTINGS`), or a connection that broke. What an exporter
    raises about the rows themselves, a column the contract does not
    declare, is its own and passes through as it is.
    """
    items = iter(stream)
    while True:
        try:
            item = next(items)
        except StopIteration:
            return
        except Exception as error:
            raise ExportStopped(
                f'Export of {table!r} stopped before its last row: '
                f'{error}\n\n'
                f'A limit on the connecting account is the usual cause: '
                f'an export query fails at one rather than return part '
                f'of its result. Export connects as admin for this '
                f'reason; check database/config/users.d/ if you changed '
                f'the profile.',
            ) from error
        yield item


REPOSITORIES_QUERY = f"""
SELECT
    r.id AS id,
    r.owner AS owner,
    r.repo AS repo,
    r.stars AS stars,
    lower(r.github_language) AS language,
    r.github_language AS github_language,
    -- The top-twelve fold (D7), by the `language_buckets` view the
    -- coverage rollup and the dashboard's dictionary read, so all three
    -- fold alike.
    {language_bucket_sql('r.github_language')} AS language_bucket,
    r.ecosystems AS ecosystems,
    r.url AS url,
    r.description AS description,
    r.license_spdx_id AS license_spdx_id,
    formatDateTime(r.pushed_at, '%Y-%m-%d', 'UTC') AS pushed_at,
    -- When *we* last looked, as distinct from when upstream last
    -- pushed. A repository can have been pushed to yesterday and last
    -- scanned six months ago, and only the second explains a stale row.
    --
    -- The newest of its current observations: a Syft row carries its
    -- scan's date and a graph row its document's (#22). This was
    -- `greatest(max(a.observed_at), r.updated_at)`, and `updated_at` is
    -- no observation: it is not in the insert list, so it is the time
    -- of the insert, which `db index` repeats for every repository it
    -- indexes. Every repository read as scanned on the last index day,
    -- and so did the freshness both manifests report.
    --
    -- The 11,840 repositories with no dependencies have no artifact to
    -- carry a date, and still fall back to it. `total_dependencies` is
    -- the count below (ClickHouse resolves an alias anywhere in the
    -- SELECT), and `repository_freshness` leaves these rows out of the
    -- span by the same test.
    formatDateTime(
        if(total_dependencies > 0, max(a.observed_at), r.updated_at),
        '%Y-%m-%d', 'UTC'
    ) AS observed_at,
    r.sbom_ref AS sbom_ref,
    r.sbom_commit_sha AS sbom_commit_sha,
    countDistinctIf(
        a.name, a.name != '' AND a.relationship = '{DIRECT}'
    ) AS direct_dependencies,
    countDistinctIf(a.name, a.name != '') AS total_dependencies,
    r.manifest_sources AS manifest_sources
-- The corpus: the repositories of the current search snapshot (D2),
-- every one of them, collected or not.
FROM corpus AS r
LEFT JOIN current_artifacts AS a ON a.repository_id = r.id
GROUP BY
    r.id, r.owner, r.repo, r.stars, r.github_language, r.ecosystems,
    r.url, r.description, r.license_spdx_id, r.pushed_at, r.updated_at,
    r.sbom_ref, r.sbom_commit_sha,
    r.manifest_sources
ORDER BY r.stars DESC, r.id ASC
"""

# Sorted by name so a "who depends on X" lookup touches few row groups.
#
# One row per fact, which is what the rollups count: the export grouped
# on this key before the rollups did, and now both read it from `facts`.
ARTIFACTS_QUERY = """
SELECT
    repository_id,
    name,
    version,
    type,
    found_by,
    relationship,
    source,
    version_kind
FROM facts
ORDER BY name ASC, repository_id ASC, version ASC
"""

# Monthly adoption per package, straight off the append-only table. Kept
# in its own file so the dashboard's current-state payload stays small —
# only a page asking a temporal question needs to fetch this.
#
# Every observation, not the current scan: a repository scanned in
# February and again in September belongs in both months, as it does in
# `mv_package_month`, which this mirrors.
HISTORY_QUERY = f"""
-- Per source, not merged.
--
-- Syft resolves a lockfile's closure; GitHub's graph parses manifests.
-- They ran seven months apart, so a single series over both drew a line
-- from February's 124 to September's 149 and read as adoption growing
-- when the only thing that changed was the instrument.
SELECT
    a.name AS name,
    formatDateTime(a.observed_at, '%Y-%m', 'UTC') AS month,
    a.source AS source,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
        AS direct_count
FROM artifacts AS a
-- Of the corpus's repositories, as `mv_package_month` counts them.
WHERE a.name != '' AND a.repository_id IN (SELECT id FROM corpus)
GROUP BY a.name, month, a.source
ORDER BY a.name ASC, a.source ASC, month ASC
"""

# Licence distribution. Unknown is kept as an explicit empty string rather
# than dropped: "we do not know" is a finding about SBOM quality, and
# hiding it would overstate how well licences are covered.
LICENSES_QUERY = """
-- `ARRAY JOIN`, not `arrayElement(licenses, 1)`.
--
-- Taking the first element drops every licence after it, and the
-- corpus does carry packages under more than one. Measured against
-- the ClickHouse rollup, which array-joins: 112 licences disappeared
-- entirely and 28 were undercounted, `GPL-2.0-only` by a third — 139
-- against 216. A licence count understated by a third is the wrong
-- kind of wrong for this column to be.
--
-- The empty row is kept deliberately, which is what the column
-- comment means by "or empty for unknown": 23,022 of 24,339
-- repositories hold a package with no licence at all, so dropping it
-- would delete the largest category. `ARRAY JOIN` discards an empty
-- array, hence the second branch.
-- Keyed by `(license, type)`, which is what the Parquet export
-- declares and checks for. The D1 export needs one row per licence
-- and has to fold the type away itself — see `_licence_rows` there.
SELECT
    license,
    type,
    countDistinct(name) AS package_count,
    countDistinct(repository_id) AS repository_count
FROM (
    SELECT l AS license, a.type AS type, a.name AS name,
           a.repository_id AS repository_id
    FROM current_artifacts AS a
    ARRAY JOIN a.licenses AS l
    WHERE a.name != ''
    UNION ALL
    SELECT '' AS license, a.type AS type, a.name AS name,
           a.repository_id AS repository_id
    FROM current_artifacts AS a
    WHERE a.name != '' AND empty(a.licenses)
)
GROUP BY license, type
ORDER BY repository_count DESC, license ASC
LIMIT 500
"""


#: The same licence shares, keyed by licence alone.
#:
#: `LICENSES_QUERY` is keyed `(license, type)` because the Parquet
#: export declares and checks that shape. D1's `licenses` table
#: declares one row per licence and the dashboard reads it as licence
#: totals, so it needs its own grouping rather than a fold of the
#: other's rows: `repository_count` is a distinct count, and summing it
#: across ecosystems would double every repository holding two of them
#: under one licence, while taking the largest slice understates it —
#: MIT 10,114 against 16,846.
#:
#: Checked against `mv_licenses`, which the dashboard's ClickHouse path
#: reads: 500 rows, zero disagreements. The two backends have to answer
#: the same question the same way or the fallback is a different
#: dataset.
D1_LICENSES_QUERY = """
SELECT
    license,
    countDistinct(name) AS package_count,
    countDistinct(repository_id) AS repository_count
FROM (
    SELECT l AS license, a.name AS name,
           a.repository_id AS repository_id
    FROM current_artifacts AS a
    ARRAY JOIN a.licenses AS l
    WHERE a.name != ''
    UNION ALL
    -- `ARRAY JOIN` drops an empty array, and unknown is the largest
    -- category: 23,022 of 24,339 repositories hold a package with no
    -- licence at all. The empty key is what the column means by
    -- "SPDX id, or empty for unknown".
    SELECT '' AS license, a.name AS name,
           a.repository_id AS repository_id
    FROM current_artifacts AS a
    WHERE a.name != '' AND empty(a.licenses)
)
GROUP BY license
ORDER BY repository_count DESC, license ASC
LIMIT 500
""".strip()

QUERIES: dict[str, str] = {
    'repositories': REPOSITORIES_QUERY,
    'artifacts': ARTIFACTS_QUERY,
    'licenses': LICENSES_QUERY,
    'history': HISTORY_QUERY,
}


def observed_range(dates: Iterable[str]) -> dict[str, str]:
    """The span of observation dates actually present in a table.

    Derived from the rows rather than read off a clock, for two reasons.
    The manifest is content-addressed by its checksums, so a wall time
    would make byte-identical exports differ. And an export can run long
    after collection, so a wall time describes when the export ran —
    which is the wrong thing to hold up against a row that looks stale.

    Blank dates are observations that never happened and are excluded;
    including them would report an `observedFrom` of '' for any dataset
    with one unscanned row.
    """
    seen = sorted(d for d in dates if d)
    if not seen:
        return {}
    return {'observedFrom': seen[0], 'observedTo': seen[-1]}


def repository_freshness(
    rows: Iterable[Mapping[str, object]],
) -> dict[str, str]:
    """The observation span of exported `repositories` rows.

    Over the repositories with dependencies. One with none has no
    artifact to date it, so its `observed_at` is the day `db index`
    wrote its row (`REPOSITORIES_QUERY`), which says when the indexer
    ran rather than when anything was seen. In the span it made
    `observedTo` the last index day again whenever one such repository
    existed, and 11,840 of 28,075 do.
    """
    return observed_range(
        str(row['observed_at']) for row in rows if row['total_dependencies']
    )
