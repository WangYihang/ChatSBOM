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
read plain integers.

**A whole-corpus distinct count is never a sum of group counts.** The
rollups used to be keyed by the repository's language and summed across
languages, which was exact only because every repository had exactly
one. They are keyed by ecosystem now (#55 §4.12), and a repository has
as many ecosystems as it has manifests for: a TypeScript-labelled
repository with a Maven backend is an npm dependant of `react` and a
Maven dependant of `spring-boot-starter-web`, and 14% of repositories
have more than one. Summing per-ecosystem repository counts would count
it twice. So every whole-corpus repository or package count is its own
`uniqExact` over the facts (`mv_packages`, `mv_repository_deps`), and
only record counts, which partition by ecosystem because a record has
one type, are ever summed. `scripts/verify_rollups.py` holds them to
that.

**The corpus** is the current search snapshot (owner decision D2 on
#55): the views these read, `current_artifacts` and `facts`, keep only
repositories in it (`corpus` in `core/schema.py`). History included:
`mv_package_month` reads every observation, of the corpus's
repositories.

Result, measured on the same 21 queries: 836.0 ms down to 45.0 ms, with
no query over 7 ms. Refreshing all five costs under a second.

`REFRESH EVERY 1 DAY` is a fallback, not the mechanism. The data changes
only when `db index` or `db edges` runs, and those refresh explicitly —
a daily timer is there so a forgotten refresh is stale by a day rather
than forever.

**Current state, or history.** `artifacts` keeps every scan of every
repository, and each rollup answers one of two kinds of question:

- *current state* — who depends on X now, at which version, under which
  licence. These read the `facts` and `current_artifacts` views
  (`core/schema.py`), which keep each repository's current scan, as
  the CLI and the exports always have. They used to read the whole
  table, and the moment a repository had a second scan the overview
  counted mail 2.7.1 beside 2.9.1 while the CLI showed 2.9.1 alone.
- *history* — how adoption moved over time. `mv_package_month` is the
  only one, and it reads every observation on purpose: built on the
  current scan, the series would erase itself.

The views are not materialised: each rollup that reads one runs its
join of `repositories FINAL` when it refreshes, and not per request.
"""
from __future__ import annotations

from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.schema import language_bucket_sql
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT

#: A fact's ecosystem: its type under its canonical name
#: (`core/ecosystems.py`), so Syft's `java-archive` and the graph's
#: `maven` are one. A type the table has never seen reads as itself.
ECOSYSTEM = canonical_sql('type')

#: Package popularity per ecosystem, and the relationship and source
#: splits that go with it. The one rollup most panels are derived from.
#:
#: Keyed by ecosystem rather than by the repository's language (#55
#: §4.12). A repository counts under every ecosystem it has a fact in,
#: so `repositories` here must never be summed across ecosystems: that
#: is what `mv_packages` is for.
#:
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
#: The facts come from the `facts` view, which the export reads too, so
#: the two cannot drift again. A fact has one type, so the record
#: counts partition by ecosystem and do sum to the corpus's.
PACKAGE_ECOSYSTEM = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_ecosystem
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (ecosystem, name)
AS SELECT
    {ECOSYSTEM} AS ecosystem,
    name,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct')
        AS direct_repositories,
    count() AS records,
    countIf(relationship = 'direct') AS direct_records,
    countIf(relationship = 'transitive') AS transitive_records,
    countIf(relationship = 'unknown') AS unknown_records,
    countIf(source = '{SYFT}') AS syft_records,
    countIf(source = '{DEPGRAPH}') AS depgraph_records,
    countIf(source = '{MANIFEST}') AS manifest_records
FROM facts
GROUP BY ecosystem, name
""".strip()

#: One row per repository that has any dependency. Answers the
#: dependency histogram, and the repository count in the totals — which
#: cannot come from PACKAGE_ECOSYSTEM, since summing distinct repository
#: counts across *names* or *ecosystems* would count a repository once
#: per package or per ecosystem.
REPOSITORY_DEPS = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_repository_deps
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY repository_id
AS SELECT
    repository_id,
    uniqExact(name) AS packages,
    uniqExactIf(name, relationship = 'direct') AS direct_packages,
    count() AS records,
    -- Which sources cover it, for the coverage panels: a repository
    -- with no Syft scan can still have a graph or Gradle declarations.
    countIf(source = '{SYFT}') AS syft_records,
    countIf(source = '{DEPGRAPH}') AS depgraph_records,
    countIf(source = '{MANIFEST}') AS manifest_records
-- The same facts as PACKAGE_ECOSYSTEM, so `records` means one thing
-- across the rollups: one current fact, however many manifests or
-- scans reported it.
FROM facts
GROUP BY repository_id
""".strip()

#: Licence shares. Its own rollup because the source is an ARRAY JOIN,
#: which a projection cannot express and PACKAGE_ECOSYSTEM's grain
#: cannot carry: a package row lists several licences, so the counts do
#: not decompose by name.
#:
#: Unknown is a row like any other. "We do not know" is a finding about
#: SBOM quality, and filtering it out would overstate coverage.
#:
#: From `current_artifacts` rather than `facts`: licences are not part
#: of a fact's key, and both counts here are distinct anyway.
LICENSES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_licenses
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY license
AS SELECT
    l AS license,
    uniqExact(repository_id) AS repositories,
    uniqExact(name) AS packages
FROM current_artifacts
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
FROM current_artifacts
WHERE empty(licenses)
""".strip()

#: Per-ecosystem totals: a dozen rows, so three panels read a dozen rows.
#:
#: PACKAGE_ECOSYSTEM could answer all three: it is keyed `(ecosystem,
#: name)`, so a filter uses the prefix, but the unfiltered question
#: would sum every row of it on every visit.
#:
#: Records only. They partition by ecosystem, so these sum to the
#: corpus's; a repository count would not, and there is none here.
ECOSYSTEM_TOTALS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ecosystem_totals
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY ecosystem
AS SELECT
    ecosystem,
    sum(direct_records) AS direct_records,
    sum(transitive_records) AS transitive_records,
    sum(unknown_records) AS unknown_records,
    sum(syft_records) AS syft_records,
    sum(depgraph_records) AS depgraph_records,
    sum(manifest_records) AS manifest_records,
    sum(records) AS records
FROM mv_package_ecosystem
GROUP BY ecosystem
""".strip()

#: One row per package name, ordered by name, for the search box and
#: the whole-corpus ranking.
#:
#: Counted from the facts, not summed from PACKAGE_ECOSYSTEM. `ms` is
#: an npm package and nothing else, but `mail` is a gem, a Maven
#: artifact and a PyPI package, and a repository can depend on two of
#: them: the sum over ecosystems counts it once per ecosystem. When
#: every repository had one language the sum over languages was exact;
#: over ecosystems it is not.
#:
#: The search is prefix-matched and runs on every keystroke, and keyed
#: on name alone it is a range scan.
PACKAGES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY name
AS SELECT
    name,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct') AS direct_repositories
FROM facts
GROUP BY name
""".strip()

#: The four numbers in the header, and the corpus they are out of.
#: One row, so the panel reads one row.
#:
#: `tracked` is the denominator every coverage ratio uses: the
#: repositories in the current search snapshot, whether or not anything
#: was collected for them. `repositories` is how many of those have a
#: current dependency fact from any source.
TOTALS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_totals
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS SELECT
    (SELECT count() FROM mv_repository_deps) AS repositories,
    (SELECT sum(records) FROM mv_repository_deps) AS dependencies,
    -- One row per name, and each a distinct count of its own: never a
    -- sum across ecosystems.
    (SELECT count() FROM mv_packages) AS packages,
    -- Records partition by ecosystem, so this sum is exact.
    (SELECT sum(direct_records + transitive_records)
     FROM mv_ecosystem_totals) AS classified,
    (SELECT count() FROM corpus) AS tracked
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
#:
#: **History, so it reads every observation**, where every other rollup
#: over `artifacts` reads the current scan. A repository scanned in
#: February and again in September belongs in both months; restricted
#: to the current scan it would appear in September alone, and the
#: series would chart when repositories were last scanned rather than
#: what they used. `uniqExact` needs no deduplicated facts: a
#: repository reported twice in a month is still one.
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
-- Every observation, of the corpus's repositories (D2): one the current
-- snapshot no longer lists keeps its rows, and they are not counted.
WHERE repository_id IN (SELECT id FROM corpus)
GROUP BY name, source, month
""".strip()

#: Per package and ecosystem.
#:
#: Asked before any count is presented as "dependants of X", because a
#: name shared across ecosystems is two different packages: `mail` is a
#: Ruby gem and a Maven artifactId. 267,101 rows.
#:
#: From `current_artifacts` rather than `facts`: both counts are
#: distinct already, so deduplicating first would be a DISTINCT over
#: every current row for the same answer.
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
FROM current_artifacts
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
#:
#: The versions in use now. Read from every observation, a repository
#: that moved from mail 2.7.1 to 2.9.1 was counted on both. A distinct
#: count, so from `current_artifacts`, as PACKAGE_TYPE is.
PACKAGE_VERSION = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_version
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, version_kind, version)
AS SELECT
    name,
    version_kind,
    version,
    uniqExact(repository_id) AS repositories
FROM current_artifacts
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

#: Repositories per GitHub language, folded to the top twelve and
#: `other` (owner decision D7), and how many have dependency data.
#:
#: Reads the corpus rather than a rollup over `artifacts`, because the
#: repositories with no dependency row are the finding this panel
#: exists to show and cannot appear in one. The denominator is every
#: repository of the current snapshot: 60,017, of which ~32,000 have
#: never been scanned. Measured against only the repositories that have
#: a scan, every ratio would read as near-complete and hide the gap
#: #51 is about.
#:
#: GitHub's language is an attribute of the repository, not of its
#: dependencies: a TypeScript repository's `with_syft` counts its Maven
#: artifacts as well.
LANGUAGE_COVERAGE = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_language_coverage
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY language
AS SELECT
    {language_bucket_sql('r.github_language')} AS language,
    count() AS repositories,
    -- Any current dependency fact, from any source.
    countIf(d.repository_id != 0) AS with_sbom,
    countIf(d.syft_records > 0) AS with_syft,
    countIf(d.depgraph_records > 0) AS with_depgraph,
    countIf(d.manifest_records > 0) AS with_manifest
FROM corpus AS r
LEFT JOIN mv_repository_deps AS d ON d.repository_id = r.id
GROUP BY language
""".strip()

#: Per ecosystem: how many repositories of the corpus have it, and how
#: many of those each source covers (#55 §4.12).
#:
#: A repository has an ecosystem when its current scan's artifacts or
#: its discovered manifests say so (`repositories.ecosystems`), so the
#: denominator includes repositories whose manifests Syft read nothing
#: from. A repository counts under every ecosystem it has: these rows
#: are not to be summed.
#:
#: `with_syft` and the others count repositories with a current fact of
#: that ecosystem from that source.
ECOSYSTEM_COVERAGE = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ecosystem_coverage
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY ecosystem
AS SELECT
    e AS ecosystem,
    count() AS repositories,
    countIf(x.records > 0) AS with_any,
    countIf(x.syft > 0) AS with_syft,
    countIf(x.depgraph > 0) AS with_depgraph,
    countIf(x.manifest > 0) AS with_manifest
FROM (SELECT id, ecosystems FROM corpus) AS r
ARRAY JOIN r.ecosystems AS e
LEFT JOIN (
    SELECT
        repository_id,
        {ECOSYSTEM} AS ecosystem,
        count() AS records,
        countIf(source = '{SYFT}') AS syft,
        countIf(source = '{DEPGRAPH}') AS depgraph,
        countIf(source = '{MANIFEST}') AS manifest
    FROM facts
    GROUP BY repository_id, ecosystem
) AS x ON x.repository_id = r.id AND x.ecosystem = e
GROUP BY e
""".strip()

#: The ranking, per filter combination.
#:
#: The panel has exactly two controls — declared-only and ecosystem — so
#: the answer set is finite and can be enumerated: 2 orderings x (the
#: ecosystems + the corpus) x 100 rows.
#:
#: The corpus row, `''`, reads `mv_packages`, which counts each name's
#: repositories once however many ecosystems it is in. It used to be
#: the sum of the per-language rows.
#:
#: Depth 100, not 30: the panel's limit is a parameter, and a rollup
#: that stored exactly the default would answer a larger request with a
#: short list rather than an error.
TOP_PACKAGES = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (ecosystem, direct_only, rank)
AS
WITH by_name AS (
    SELECT '' AS ecosystem, name, repositories, direct_repositories
    FROM mv_packages
    UNION ALL
    SELECT ecosystem, name, repositories, direct_repositories
    FROM mv_package_ecosystem
    WHERE ecosystem != ''
)
SELECT ecosystem, direct_only, name, repositories, direct_repositories, rank
FROM (
    SELECT ecosystem, 0 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY ecosystem
                              ORDER BY repositories DESC, name) AS rank
    FROM by_name
    UNION ALL
    SELECT ecosystem, 1 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY ecosystem
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
_EDGE_AMBIGUITY_TEMPLATE = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edge_ambiguity
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS WITH ambiguous AS (
    -- Canonical names, not raw types. Counting the spellings apart
    -- made one registry look like two and inflated this fivefold:
    -- 39,658 names and 51.5% of edges against a true 2,730 and 10.3%.
    -- 93% of the ambiguity this warns about was `cargo` beside
    -- `rust-crate` and `composer` beside `php-composer`.
    SELECT name FROM mv_package_type GROUP BY name
    HAVING uniqExact({canonical_type}) > 1
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

#: Filled once, so the canonical mapping has a single home
#: (`core/ecosystems.py`) rather than a second copy written in SQL.
EDGE_AMBIGUITY = _EDGE_AMBIGUITY_TEMPLATE.replace(
    '{canonical_type}', canonical_sql('type'),
)

#: Whether a recorded version is a resolution or a range. Three rows.
#:
#: Syft reads a lockfile and gets `2.9.1`; the dependency graph reads a
#: manifest and may get `>= 2.0, < 3.0`, or nothing at all. Counting
#: them together would present a constraint as a version in use, which
#: is why `version_kind` exists.
#:
#: Its own rollup because the honest source is the facts themselves:
#: `mv_package_version` is keyed `(name, version, version_kind)` and
#: holds distinct *repository* counts, so summing them across versions
#: double counts any repository holding two versions of one package.
#: Asked of the fact table the query is correct and takes 4.4s, which
#: is why it is precomputed rather than run per visit.
#:
#: The same `facts` as PACKAGE_ECOSYSTEM, so these three numbers add up
#: to `mv_totals.dependencies` rather than to something 2.5 million
#: larger.
VERSION_KINDS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_version_kinds
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY version_kind
AS SELECT
    version_kind,
    count() AS records
FROM facts
GROUP BY version_kind
""".strip()

#: Creation order is dependency order: TOTALS and TOP_PACKAGES read the
#: two rollups above them, so a fresh database has to build them first.
ROLLUPS: tuple[tuple[str, str], ...] = (
    ('mv_package_ecosystem', PACKAGE_ECOSYSTEM),
    ('mv_repository_deps', REPOSITORY_DEPS),
    ('mv_licenses', LICENSES),
    ('mv_ecosystem_totals', ECOSYSTEM_TOTALS),
    ('mv_packages', PACKAGES),
    ('mv_edges_forward', EDGES_FORWARD),
    ('mv_package_month', PACKAGE_MONTH),
    ('mv_package_type', PACKAGE_TYPE),
    ('mv_package_version', PACKAGE_VERSION),
    ('mv_dependency_buckets', DEPENDENCY_BUCKETS),
    ('mv_version_kinds', VERSION_KINDS),
    ('mv_language_coverage', LANGUAGE_COVERAGE),
    ('mv_ecosystem_coverage', ECOSYSTEM_COVERAGE),
    ('mv_totals', TOTALS),
    ('mv_top_packages', TOP_PACKAGES),
    ('mv_edge_ambiguity', EDGE_AMBIGUITY),
)

#: Rollups an earlier release declared and this one does not: keyed by
#: the repository's language, which selects nothing any more (#55
#: §4.12). `ensure_schema` drops them, so no refresh keeps computing
#: them and no reader can mistake one for current.
OBSOLETE_ROLLUPS: tuple[str, ...] = (
    'mv_package_language',
    'mv_language_totals',
)

#: Refresh order, which is creation order for the same reason: refresh
#: a derived rollup before its source and it summarises the previous
#: run.
REFRESH_ORDER: tuple[str, ...] = tuple(name for name, _ in ROLLUPS)
