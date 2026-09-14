"""Refreshable rollups, so the overview does not scan the corpus.

The dashboard's queries split cleanly in two, and only one half needs
help.

**Point lookups** take an arbitrary package name, so nothing can be
precomputed for them — and nothing needs to be. `artifacts` is sorted by
`name`, so ClickHouse's sparse index answers `WHERE name = ?` by reading
40,000–150,000 rows of 19,361,638. Measured 1.8–7.0 ms. Left alone.

**The overview** asks a fixed set of questions whose answers together
are under a thousand rows, and every one of them was a full scan:

    dependencyDistribution   251.9 ms    19,361,638 rows
    topPackages              180.6 ms    19,361,638
    topPackages(direct)       90.4 ms    19,361,638
    licenseShares             87.3 ms    19,361,638
    sourceComparison          82.4 ms    19,389,713
    totals                    77.8 ms    19,361,638
    languageCoverage          20.6 ms    19,389,713

A `PROJECTION` was tried first and is the wrong tool here. It worked —
rows read fell from 19,361,638 to 624,543 — and the time did not move,
162 ms against 170 ms, because the cost is not I/O: it is merging
624,543 `uniqExact` states, each a hash set of repository ids. Switching
to `uniq` or `uniqCombined` would trade an exact headline number for
half a percent of error, which is not a trade worth making when the page
prints "198 dependants".

So the distinct-counting happens once, at refresh time, and the panels
read plain integers. What makes that exact rather than approximate is a
property of the data: **every repository has exactly one language**
(verified — zero repositories with more than one), so summing a
per-language distinct count across languages double-counts nothing.
Without that, `sum(repositories)` would be wrong and the rollup would
have to store the repository sets themselves.

Result, measured on the same 21 queries: 836.0 ms down to 45.0 ms, with
no query over 7 ms. Refreshing all five costs under a second.

`REFRESH EVERY 1 DAY` is a fallback, not the mechanism. The data changes
only when `db index` or `db edges` runs, and those refresh explicitly —
a daily timer is there so a forgotten refresh is stale by a day rather
than forever.
"""
from __future__ import annotations

#: Package popularity per language, and the relationship and source
#: splits that go with it. The one rollup most panels are derived from.
#:
#: `lower(language)` because the dashboard's filter sends lowercase and
#: `repositories.language` is capitalised as GitHub spells it — `PHP`,
#: `JavaScript`. Lowercasing here rather than per query means a filter
#: that matches nothing cannot be mistaken for a language with no
#: packages, which is exactly what happened while this was being built.
PACKAGE_LANGUAGE = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_language
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, language)
AS SELECT
    a.name AS name,
    lower(r.language) AS language,
    uniqExact(a.repository_id) AS repositories,
    uniqExactIf(a.repository_id, a.relationship = 'direct')
        AS direct_repositories,
    count() AS records,
    countIf(a.relationship = 'direct') AS direct_records,
    countIf(a.relationship = 'transitive') AS transitive_records,
    countIf(a.relationship = 'unknown') AS unknown_records,
    countIf(a.source = 'syft') AS syft_records,
    countIf(a.source = 'github-depgraph') AS depgraph_records
FROM artifacts a
INNER JOIN repositories r ON r.id = a.repository_id
GROUP BY a.name, lower(r.language)
""".strip()

#: One row per repository that has any dependency. Answers the
#: dependency histogram, and the repository count in the totals — which
#: cannot come from PACKAGE_LANGUAGE, since summing distinct repository
#: counts across *names* would count a repository once per package.
REPOSITORY_DEPS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_repository_deps
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY repository_id
AS SELECT
    repository_id,
    uniqExact(name) AS packages,
    uniqExactIf(name, relationship = 'direct') AS direct_packages,
    count() AS records
FROM artifacts
GROUP BY repository_id
""".strip()

#: Licence shares. Its own rollup because the source is an ARRAY JOIN,
#: which a projection cannot express and PACKAGE_LANGUAGE's grain
#: cannot carry: a package row lists several licences, so the counts do
#: not decompose by name.
#:
#: Unknown is a row like any other. "We do not know" is a finding about
#: SBOM quality, and filtering it out would overstate coverage.
LICENSES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_licenses
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY license
AS SELECT
    l AS license,
    uniqExact(repository_id) AS repositories,
    uniqExact(name) AS packages
FROM artifacts
ARRAY JOIN licenses AS l
GROUP BY l
""".strip()

#: The four numbers in the header. One row, so the panel reads one row.
TOTALS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_totals
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS SELECT
    (SELECT count() FROM mv_repository_deps) AS repositories,
    (SELECT sum(records) FROM mv_repository_deps) AS dependencies,
    (SELECT uniqExact(name) FROM mv_package_language) AS packages,
    (SELECT sum(direct_records + transitive_records)
     FROM mv_package_language) AS classified
""".strip()

#: The ranking, per filter combination.
#:
#: The panel has exactly two controls — declared-only and language — so
#: the answer set is finite and can be enumerated: 2 orderings x 10
#: language values x 100 rows. Reading 30 of 2,000 stored rows costs
#: 1.4 ms against 12.6 ms for grouping PACKAGE_LANGUAGE's 371,074.
#:
#: Depth 100, not 30: the panel's limit is a parameter, and a rollup
#: that stored exactly the default would answer a larger request with a
#: short list rather than an error.
TOP_PACKAGES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (language, direct_only, rank)
AS
WITH by_name AS (
    SELECT '' AS language, name,
           sum(repositories) AS repositories,
           sum(direct_repositories) AS direct_repositories
    FROM mv_package_language GROUP BY name
    UNION ALL
    SELECT language, name, repositories, direct_repositories
    FROM mv_package_language
)
SELECT language, direct_only, name, repositories, direct_repositories, rank
FROM (
    SELECT language, 0 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY language
                              ORDER BY repositories DESC, name) AS rank
    FROM by_name
    UNION ALL
    SELECT language, 1 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY language
                              ORDER BY direct_repositories DESC, name) AS rank
    FROM by_name
)
WHERE rank <= 100
""".strip()

#: Creation order is dependency order: TOTALS and TOP_PACKAGES read the
#: two rollups above them, so a fresh database has to build them first.
ROLLUPS: tuple[tuple[str, str], ...] = (
    ('mv_package_language', PACKAGE_LANGUAGE),
    ('mv_repository_deps', REPOSITORY_DEPS),
    ('mv_licenses', LICENSES),
    ('mv_totals', TOTALS),
    ('mv_top_packages', TOP_PACKAGES),
)

#: Refresh order, which is creation order for the same reason: refresh
#: a derived rollup before its source and it summarises the previous
#: run.
REFRESH_ORDER: tuple[str, ...] = tuple(name for name, _ in ROLLUPS)

#: Refreshable materialized views are still behind a flag in 25.12.
REFRESH_SETTINGS = {'allow_experimental_refreshable_materialized_view': 1}
