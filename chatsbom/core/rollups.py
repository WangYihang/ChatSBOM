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
#: **`records` counts distinct dependency facts, not rows.**
#:
#: GitHub's dependency graph reports per manifest, so a package
#: declared in both `package.json` and `packages/x/package.json` is two
#: `artifacts` rows with different `artifact_id`s. Counting rows made
#: "dependency records" 19,384,165 where the distinct count is
#: 16,905,915 — 2,478,250 of them repeats, and 18% of the
#: dependency-graph rows against 1.4% of Syft's, which is what a
#: per-manifest artefact looks like.
#:
#: The D1 export has always grouped them away, because its schema has
#: no `artifact_id` to keep them apart, so the two backends answered
#: the same call with different numbers. Deduplicating here settles it
#: on the meaningful side: `mv_package_language` already counts
#: `uniqExact(repository_id)` for exactly this reason — a repository
#: appears once per manifest too.
#:
#: The key is the one the export uses, so the two cannot drift again.
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
FROM (
    -- The same key `export/queries.py` groups by. `artifact_id` is
    -- deliberately absent: it is the per-manifest discriminator, and
    -- dropping it is the whole point.
    SELECT DISTINCT repository_id, name, version, type, found_by,
                    relationship, source, version_kind
    FROM artifacts
) a
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
FROM (
    -- Deduplicated on the same key as PACKAGE_LANGUAGE, so `records`
    -- means one thing across the rollups. `packages` was already a
    -- distinct count and is unchanged by this.
    SELECT DISTINCT repository_id, name, version, type, found_by,
                    relationship, source, version_kind
    FROM artifacts
)
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
UNION ALL
-- The unknown bucket, keyed empty the way the D1 export keyed it and
-- the way `Overview.tsx` already renders it: `row.license ||
-- '(unknown)'`.
--
-- `ARRAY JOIN` drops a row whose array is empty, so migrating this
-- rollup to ClickHouse silently deleted the largest category in the
-- panel. It is not a rounding error: 23,022 of 24,339 repositories
-- contain at least one package with no licence at all, against
-- 16,846 for MIT, so "we do not know" outranks every real licence
-- and the panel was showing MIT on top.
--
-- The panel's own note says this must not happen — "Unknown is shown
-- rather than dropped ... hiding it would overstate coverage" — so
-- the copy was right and the query had stopped agreeing with it.
SELECT
    '' AS license,
    uniqExact(repository_id) AS repositories,
    uniqExact(name) AS packages
FROM artifacts
WHERE empty(licenses)
""".strip()

#: Per-language totals. Nine rows, so three panels read nine rows.
#:
#: PACKAGE_LANGUAGE could answer all three, and did: it is keyed
#: `(name, language)`, so a language filter cannot use the prefix and
#: `WHERE language = 'php'` read all 371,074 rows — the same cost as no
#: filter at all. Measured: 2.4 ms to 1.1 ms for the split, 3.1 ms to
#: 0.8 ms for the source comparison.
LANGUAGE_TOTALS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_language_totals
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY language
AS SELECT
    language,
    sum(direct_records) AS direct_records,
    sum(transitive_records) AS transitive_records,
    sum(unknown_records) AS unknown_records,
    sum(syft_records) AS syft_records,
    sum(depgraph_records) AS depgraph_records,
    sum(records) AS records
FROM mv_package_language
GROUP BY language
""".strip()

#: One row per package name, ordered by name, for the search box.
#:
#: The search is prefix-matched and runs on every keystroke, so the
#: `GROUP BY name` it needed over PACKAGE_LANGUAGE was work repeated per
#: keypress. Keyed on name alone it is a range scan: a one-letter prefix
#: went 3.3 ms to 2.3 ms and 24,576 rows read to 16,384.
PACKAGES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY name
AS SELECT
    name,
    sum(repositories) AS repositories,
    sum(direct_repositories) AS direct_repositories
FROM mv_package_language
GROUP BY name
""".strip()

#: The four numbers in the header. One row, so the panel reads one row.
TOTALS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_totals
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS SELECT
    (SELECT count() FROM mv_repository_deps) AS repositories,
    (SELECT sum(records) FROM mv_repository_deps) AS dependencies,
    -- From the by-name rollup rather than the per-language one: 225,400
    -- rows against 371,074, and `count()` rather than `uniqExact`,
    -- since that rollup already has one row per name.
    (SELECT count() FROM mv_packages) AS packages,
    (SELECT sum(direct_records + transitive_records)
     FROM mv_language_totals) AS classified
""".strip()

#: The edge table in the other direction.
#:
#: `edges` is `ORDER BY (child, parent)` because the reverse question is
#: the more useful one, so the forward direction had no prefix to use
#: and scanned all 614,221 rows — 2.5 ms, which is fine but is the
#: largest read left among the point lookups.
#:
#: A `PROJECTION` would be the idiomatic fix and ClickHouse refuses it:
#: `ADD PROJECTION is not supported in SummingMergeTree with
#: deduplicate_merge_projection_mode = throw`. Relaxing that setting
#: makes projection upkeep part of every merge on the base table; a
#: rollup keeps the cost in the refresh, where the rest of it already
#: is.
#:
#: `sum()` here, because the source is a SummingMergeTree and a pair can
#: sit in more than one unmerged part.
EDGES_FORWARD = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edges_forward
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (parent, child)
AS SELECT
    parent,
    child,
    sum(repositories) AS repositories
FROM edges
GROUP BY parent, child
""".strip()

#: Per package, source and month, for the adoption series.
#:
#: Keyed by source because the two measure differently and ran seven
#: months apart. Merged, the series drew a line from February's 124 to
#: September's 149 for `mail` and read as adoption growing, when the
#: only thing that changed was the instrument: February is syft's
#: lockfile closure and September is GitHub's manifest parse.
#:
#: The D1 export builds a `history` table for the same reason; here it
#: is a rollup. Grouping the fact table live cost 3.0 ms reading 123,164
#: rows, against a point lookup on 325,190.
#:
#: Two observation dates exist today, so most packages have one or two
#: rows. That is a fact about the collection, not about the rollup — it
#: will grow a row per package per collection month.
PACKAGE_MONTH = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_month
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, source, month)
AS SELECT
    name,
    source,
    formatDateTime(observed_at, '%Y-%m') AS month,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct')
        AS direct_repositories
FROM artifacts
GROUP BY name, source, month
""".strip()

#: Per package and ecosystem.
#:
#: Asked before any count is presented as "dependants of X", because a
#: name shared across ecosystems is two different packages: `mail` is a
#: Ruby gem and a Maven artifactId. 267,101 rows.
PACKAGE_TYPE = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_type
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, type)
AS SELECT
    name,
    type,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct')
        AS direct_repositories
FROM artifacts
GROUP BY name, type
""".strip()

#: Per package and version, keyed by what kind of version it is.
#:
#: `version_kind` matters here and nowhere else. GitHub's graph reports
#: manifest constraints as well as resolutions — 525,899 rows are
#: constraints and 140,731 are unversioned, 3.4% together — and a panel
#: headed "repositories on each resolved version" was counting all
#: three. For `laravel/framework` that put the constraint
#: `>= 13.0,< 14.0` on top with 11 repositories, above the real leading
#: version `v12.49.0` with 7.
#:
#: Keyed rather than filtered, so the query can show the resolved
#: versions *and* say how much it left out. A rollup that dropped the
#: constraints would make that number unavailable.
PACKAGE_VERSION = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_version
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, version_kind, version)
AS SELECT
    name,
    version_kind,
    version,
    uniqExact(repository_id) AS repositories
FROM artifacts
GROUP BY name, version_kind, version
""".strip()

#: Repositories per dependency-count bucket. Six rows.
#:
#: Bucketed here rather than in the query, which is a reversal: the
#: boundaries were left in the query so changing them needed no refresh,
#: and a refresh turns out to cost 0.3 seconds. Six stored rows beat
#: bucketing 24,339 on every page load.
#:
#: `position` travels with the label because '1000+' sorts between
#: '10-24' and '100-249' as a string.
DEPENDENCY_BUCKETS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dependency_buckets
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY position
AS SELECT
    multiIf(packages < 10, 0, packages < 25, 1, packages < 100, 2,
            packages < 250, 3, packages < 1000, 4, 5) AS position,
    multiIf(packages < 10, '1-9', packages < 25, '10-24',
            packages < 100, '25-99', packages < 250, '100-249',
            packages < 1000, '250-999', '1000+') AS bucket,
    count() AS repositories
FROM mv_repository_deps
GROUP BY position, bucket
""".strip()

#: Repositories per language, and how many have any dependency at all.
#:
#: Reads `repositories` rather than a rollup over `artifacts`, because
#: the 3,736 repositories with no dependency row are the finding this
#: panel exists to show and cannot appear in one.
LANGUAGE_COVERAGE = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_language_coverage
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY language
AS SELECT
    lower(r.language) AS language,
    count() AS repositories,
    countIf(d.repository_id != 0) AS with_sbom
FROM repositories AS r
LEFT JOIN mv_repository_deps AS d ON d.repository_id = r.id
GROUP BY language
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
    -- The whole-corpus rows are already grouped in `mv_packages`, so
    -- this reads them rather than repeating the GROUP BY.
    SELECT '' AS language, name, repositories, direct_repositories
    FROM mv_packages
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

#: How badly name-keyed edges are polluted by cross-ecosystem collisions.
#:
#: The dashboard states this as a caveat on both edge panels, and it
#: used to state it from four numbers hardcoded in the copy. They were
#: measured before the dependency-graph ingest and never revisited, so
#: the page claimed 2,508 ambiguous names out of 141,938 carrying
#: 107,974 of 455,281 edges — 23.7% — when the truth had become 39,186
#: of 225,400 carrying 316,546 of 614,221, which is 51.5%. A caveat
#: that understates its own finding by half is worse than none, and a
#: measurement pasted into a sentence will go stale every rebuild.
#:
#: One row, so the panel reads one row.
EDGE_AMBIGUITY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edge_ambiguity
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS WITH ambiguous AS (
    SELECT name FROM mv_package_type GROUP BY name HAVING uniqExact(type) > 1
)
SELECT
    (SELECT count() FROM mv_packages) AS names,
    (SELECT count() FROM ambiguous) AS ambiguous_names,
    (SELECT count() FROM mv_edges_forward) AS edges,
    (SELECT count() FROM mv_edges_forward
     WHERE child IN (SELECT name FROM ambiguous)
        OR parent IN (SELECT name FROM ambiguous)) AS ambiguous_edges,
    -- The other number pasted into the same sentence. It said 6,635
    -- while the real maximum had become 5,388 (`vercel/next.js`), so
    -- the note overstated the graph it was apologising for bounding.
    (SELECT max(packages) FROM mv_repository_deps) AS largest_repository
""".strip()

#: Creation order is dependency order: TOTALS and TOP_PACKAGES read the
#: two rollups above them, so a fresh database has to build them first.
ROLLUPS: tuple[tuple[str, str], ...] = (
    ('mv_package_language', PACKAGE_LANGUAGE),
    ('mv_repository_deps', REPOSITORY_DEPS),
    ('mv_licenses', LICENSES),
    ('mv_language_totals', LANGUAGE_TOTALS),
    ('mv_packages', PACKAGES),
    ('mv_edges_forward', EDGES_FORWARD),
    ('mv_package_month', PACKAGE_MONTH),
    ('mv_package_type', PACKAGE_TYPE),
    ('mv_package_version', PACKAGE_VERSION),
    ('mv_dependency_buckets', DEPENDENCY_BUCKETS),
    ('mv_language_coverage', LANGUAGE_COVERAGE),
    ('mv_totals', TOTALS),
    ('mv_top_packages', TOP_PACKAGES),
    ('mv_edge_ambiguity', EDGE_AMBIGUITY),
)

#: Refresh order, which is creation order for the same reason: refresh
#: a derived rollup before its source and it summarises the previous
#: run.
REFRESH_ORDER: tuple[str, ...] = tuple(name for name, _ in ROLLUPS)

#: Refreshable materialized views are still behind a flag in 25.12.
REFRESH_SETTINGS = {'allow_experimental_refreshable_materialized_view': 1}
