"""ClickHouse schema: DDL plus the insert-column contract for each table.

The DDL and the insert column list are two views of the same truth. They
live together here so they cannot drift apart, and `ddl_columns` lets a
test assert that every declared insert column actually exists in the DDL.
"""
import re

from chatsbom.core.table import Table
from chatsbom.models.provenance import DEPGRAPH

REPOSITORIES_DDL = """
CREATE TABLE IF NOT EXISTS repositories (
    id UInt64 COMMENT 'GitHub Repository ID',
    owner LowCardinality(String) COMMENT 'Owner Name',
    repo String COMMENT 'Repository Name',
    url String COMMENT 'Repository URL',
    stars UInt64 COMMENT 'Star Count',
    description String COMMENT 'Repository Description',
    created_at DateTime COMMENT 'Creation Time',
    language LowCardinality(String) COMMENT 'Programming Language',
    topics Array(LowCardinality(String)) COMMENT 'GitHub Topics',
    default_branch String DEFAULT '' COMMENT 'Default Branch Name',
    sbom_ref String DEFAULT '' COMMENT 'Ref used for SBOM download (tag or branch)',
    sbom_ref_type LowCardinality(String) DEFAULT '' COMMENT 'Ref type: release or branch',
    sbom_commit_sha String DEFAULT '' COMMENT 'Full Commit SHA for SBOM',
    sbom_commit_sha_short String DEFAULT '' COMMENT 'Short Commit SHA (7 chars)',
    has_releases Bool DEFAULT false COMMENT 'Whether repo has any releases',
    latest_release_tag String DEFAULT '' COMMENT 'Latest stable release tag name',
    latest_release_published_at DateTime DEFAULT '1970-01-01' COMMENT 'Latest release publish date',
    total_releases UInt32 DEFAULT 0 COMMENT 'Total number of releases',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time',
    pushed_at DateTime DEFAULT '1970-01-01' COMMENT 'Last Push Time',
    is_archived Bool DEFAULT false COMMENT 'Whether repo is archived',
    is_fork Bool DEFAULT false COMMENT 'Whether repo is a fork',
    is_template Bool DEFAULT false COMMENT 'Whether repo is a template',
    is_mirror Bool DEFAULT false COMMENT 'Whether repo is a mirror',
    disk_usage UInt32 DEFAULT 0 COMMENT 'Disk usage in KB',
    fork_count UInt32 DEFAULT 0 COMMENT 'Number of forks',
    watchers_count UInt32 DEFAULT 0 COMMENT 'Number of watchers',
    license_spdx_id LowCardinality(String) DEFAULT '' COMMENT 'License SPDX ID (e.g., MIT, Apache-2.0)',
    license_name String DEFAULT '' COMMENT 'License full name',
    manifest_sources Array(String) DEFAULT [] COMMENT 'Manifest files read to decide direct vs transitive',
    languages String DEFAULT '{}' COMMENT 'Language distribution as JSON',
    vulnerability_alerts_count Nullable(UInt32) COMMENT 'Number of vulnerability alerts',
    -- The dependency-graph document this row was indexed with, by the
    -- instant it states (`DbService.graph_observed_at`): the repository's
    -- graph rows are current by it, as its Syft rows are by
    -- `sbom_commit_sha`. 1970-01-02, the unset date, when `db index` read
    -- no graph. The default, 1970-01-01, is held only by a row written
    -- before the column existed, and keeps the rule this replaced for it:
    -- see CURRENT_OBSERVATION.
    depgraph_observed_at DateTime DEFAULT toDateTime(0) COMMENT 'When the dependency graph last indexed says it was produced'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
""".strip()

# `artifacts` is append-only. Each row is an *observation*: this package,
# at this version, in this repository, as seen in this scan. Overwriting
# the current state would destroy information on every update — "how long
# did projects take to move off mail 2.7" is unanswerable once the rows
# that knew are gone — and storage is no argument against keeping it:
# 6.1M rows compress to 17 MB, a year of weekly deltas to roughly 220 MB.
#
# Hence MergeTree rather than ReplacingMergeTree, partitioned by month so
# a time-bounded query prunes whole partitions. "Current state" is derived
# by joining on what the repository records now: its `sbom_commit_sha`
# for a Syft row, its `depgraph_observed_at` for a dependency-graph row.
# See `current_artifacts` below.
ARTIFACTS_DDL = """
CREATE TABLE IF NOT EXISTS artifacts (
    repository_id UInt64 COMMENT 'GitHub Repository ID',
    artifact_id String COMMENT 'Artifact ID (from SBOM)',
    name String COMMENT 'Component Name',
    version String COMMENT 'Component Version',
    type LowCardinality(String) COMMENT 'Component Type',
    purl String COMMENT 'Package URL',
    found_by LowCardinality(String) COMMENT 'Detector Name',
    licenses Array(LowCardinality(String)) COMMENT 'License List',
    relationship LowCardinality(String) DEFAULT 'unknown' COMMENT 'direct | transitive | unknown',
    source LowCardinality(String) DEFAULT 'syft' COMMENT 'syft | github-depgraph',
    version_kind LowCardinality(String) DEFAULT 'resolved' COMMENT 'resolved | constraint | unversioned',
    sbom_ref String DEFAULT '' COMMENT 'Ref used for SBOM (tag or branch)',
    sbom_commit_sha String DEFAULT '' COMMENT 'Full Commit SHA for SBOM',
    observed_at DateTime DEFAULT now() COMMENT 'When this observation was recorded',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'
) ENGINE = MergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (name, repository_id, source, sbom_commit_sha, artifact_id, version)
-- 1024 rather than the default 8192.
--
-- Every remaining point lookup reads a multiple of the granularity,
-- and most of it is waste: `laravel/framework` has 299 rows and the
-- dependants query read 148,740. At 1024 it reads 9,216 — sixteen
-- times less I/O for the same answer.
--
-- Measured both sides. Latency improves less than the I/O does, because
-- at this size the query is dominated by planning, dictionary lookups
-- and sorting a hundred rows rather than by reading from a warm page
-- cache: 4.3 ms to 3.2 ms. What it buys beyond that is headroom under
-- concurrency, where the pages are not warm.
--
-- The cost: disk 845 MiB to 933 MiB, the primary index 56 KiB to 577
-- KiB in memory, and the full-table grouping a rollup refresh does
-- 142.5 ms to 149.0 ms. None of those is a reason not to.
--
-- Applies to new parts only, so an existing table keeps 8192 until
-- `db index --rebuild`.
SETTINGS index_granularity = 1024
""".strip()

RELEASES_DDL = """
CREATE TABLE IF NOT EXISTS releases (
    repository_id UInt64 COMMENT 'GitHub Repository ID',
    release_id UInt64 COMMENT 'GitHub Release ID',
    tag_name String COMMENT 'Release Tag Name',
    name String COMMENT 'Release Name',
    is_prerelease Bool COMMENT 'Is Prerelease',
    is_draft Bool COMMENT 'Is Draft',
    published_at DateTime COMMENT 'Publication Time',
    target_commitish String COMMENT 'Target Branch',
    created_at DateTime COMMENT 'Creation Time',
    release_assets String DEFAULT '[]' COMMENT 'Release assets as JSON array',
    source LowCardinality(String) DEFAULT 'github_release' COMMENT 'Data source: github_release or git_tag',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (repository_id, tag_name)
""".strip()


REPOSITORIES = Table(
    name='repositories',
    columns=(
        'id', 'owner', 'repo', 'url', 'stars', 'description', 'created_at',
        'language', 'topics', 'default_branch',
        'sbom_ref', 'sbom_ref_type', 'sbom_commit_sha', 'sbom_commit_sha_short',
        'has_releases', 'latest_release_tag', 'latest_release_published_at',
        'total_releases', 'pushed_at',
        'is_archived', 'is_fork', 'is_template', 'is_mirror',
        'disk_usage', 'fork_count', 'watchers_count',
        'license_spdx_id', 'license_name', 'manifest_sources',
        'depgraph_observed_at',
    ),
)

ARTIFACTS = Table(
    name='artifacts',
    columns=(
        'repository_id', 'artifact_id', 'name', 'version', 'type', 'purl',
        'found_by', 'licenses', 'relationship', 'source', 'version_kind',
        'sbom_ref', 'sbom_commit_sha', 'observed_at',
    ),
)

EDGES = Table(
    name='edges',
    columns=('parent', 'child', 'repositories', 'observed_at'),
)

RELEASES = Table(
    name='releases',
    columns=(
        'repository_id', 'release_id', 'tag_name', 'name', 'is_prerelease',
        'is_draft', 'published_at', 'target_commitish', 'created_at',
        'release_assets', 'source',
    ),
)

# Package-to-package edges, aggregated by pair.
#
# `SummingMergeTree(repositories)` rather than MergeTree: a pair written
# twice is summed on merge instead of leaving two rows, and
# `SELECT sum(repositories) ... GROUP BY` is correct whether or not a
# merge has happened yet. That does not make a recount idempotent — it
# made a second run add every count to itself — so `db edges` replaces
# the whole table each run (`IngestionRepository.rebuilding`).
#
# `ORDER BY (child, parent)`, child first, because the more useful
# question is the reverse one: "what pulls `ms` in" is how a reader
# finds out why a package they never chose is in their lockfile. That
# direction gets the primary-key prefix; the forward direction still
# scans a bounded range.
EDGES_DDL = """
CREATE TABLE IF NOT EXISTS edges (
    parent String COMMENT 'Package that pulls the child in',
    child String COMMENT 'Package pulled in',
    repositories UInt64 COMMENT 'Repositories showing this pair',
    observed_at DateTime COMMENT 'Latest observation behind this count'
) ENGINE = SummingMergeTree(repositories)
ORDER BY (child, parent)
""".strip()

RAW_DOCUMENTS_DDL = """
CREATE TABLE IF NOT EXISTS raw_documents (
    kind LowCardinality(String) COMMENT 'Collector that produced it: syft | github-depgraph',
    repository_id UInt64 COMMENT 'GitHub Repository ID',
    path String COMMENT 'Where it was read from, for tracing a row back',
    sha256 String COMMENT 'Content hash: the same document twice is one row',
    fetched_at DateTime COMMENT 'When this copy was taken',
    body String COMMENT 'The document, verbatim' CODEC(ZSTD(3))
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (kind, repository_id, sha256)
""".strip()

ALL_DDL = (
    REPOSITORIES_DDL, ARTIFACTS_DDL, RELEASES_DDL, EDGES_DDL,
    RAW_DOCUMENTS_DDL,
)

#: An artifact row `a` is current if it belongs to the observation its
#: repository's row `r` records now, and there are two kinds.
#:
#: A Syft row belongs to the scan of `sbom_commit_sha`. A
#: dependency-graph row belongs to the graph document of
#: `depgraph_observed_at`: GitHub builds the graph from the default
#: branch when it is asked, so a graph fetched again while the Syft
#: target stood still is a second document under the same commit. Keyed
#: on the commit, both documents were current, and a package the newer
#: one no longer lists stayed a dependency (#22). The instant is the one
#: the document states, which its rows carry as `observed_at`; both are
#: written by `DbService.graph_observed_at` into `DateTime` columns, so
#: this compares a value with itself, to the second, in no zone.
#:
#: `depgraph_observed_at` is 0 on a row written before the column
#: existed, and there the commit decides for graph rows too, as it did:
#: a deployment keeps its graphs until the next `db index` records a
#: document for each repository. Where `db index` found no graph it
#: records the unset date, which no document states, so none is current.
#:
#: `if` rather than two joins: ClickHouse 25.12 takes the non-equi
#: condition in `ON`, for INNER and LEFT joins alike.
#:
#: `toUnixTimestamp(...) != 0` rather than `!= 0` on the date itself, for
#: the dashboard, which asks this of `dict_repositories`. There, a
#: `dictGet` compared with a constant is rewritten by
#: `optimize_inverse_dictionary_lookup` into a set of keys built from the
#: whole dictionary, on every request: 28,075 rows read where the lookup
#: itself reads 3,072, and 3.0 ms added to it. The wrapped form is not
#: rewritten; `current_state_test.py` holds it to reading nothing more.
CURRENT_OBSERVATION = (
    f"if(a.source = '{DEPGRAPH}' "
    'AND toUnixTimestamp(r.depgraph_observed_at) != 0, '
    'a.observed_at = r.depgraph_observed_at, '
    'a.sbom_commit_sha = r.sbom_commit_sha)'
)

#: The join condition, written once for the view below and for the
#: readers that join `repositories` anyway: the CLI's point lookups need
#: owner, stars and language from it, so they apply this in that join
#: rather than paying for a second one inside the view. `r` has to carry
#: `sbom_commit_sha` and `depgraph_observed_at`.
ON_CURRENT_SCAN = f'a.repository_id = r.id AND {CURRENT_OBSERVATION}'

# The current scan of every repository: the one definition of "current".
#
# `artifacts` keeps every observation, so every question about the
# present has to pick the scan each repository records now. The CLI
# and the exports did, each with its own copy of the join, and the
# rollups and the dashboard did not: once a repository had a second
# scan the overview counted mail 2.7.1 beside 2.9.1 and the CLI showed
# 2.9.1 alone.
#
# `FINAL`, because `repositories` is a ReplacingMergeTree and the
# recorded commit is only the current one after deduplication. `db
# index` writes a fresh row for every repository it touches, so until
# its OPTIMIZE runs a re-scanned repository has two rows naming two
# commits, and a join without `FINAL` counts both scans as current.
#
# A Syft row is current by its commit and a dependency-graph row by its
# document: ON_CURRENT_SCAN above. A repository with no download target
# records an empty commit, as every graph row it ever had carries, so
# only the document tells its graphs apart.
#
# Asking which document costs the join nothing measurable. On the same
# synthetic data, a fifth of it graph rows of an earlier document under
# the same commit, a point lookup through the view took 11.7 ms against
# 12.9 ms keyed on the commit alone, and grouping the whole view by name
# 92 ms against 114 ms (server time, medians of 41 interleaved runs):
# the rows the condition drops are work the join no longer passes on.
#
# A view rather than a table, so there is nothing to keep in step: the
# join is re-run by whoever reads it. A filter on the view still reaches
# `artifacts`' primary key — on 2,000,000 synthetic rows `WHERE name = ?`
# read 1 of 1,954 granules through it, as without it — but the join is
# rebuilt every time: that lookup took 13.6 ms through the view and
# 4.2 ms on the table. Paid once a day by a rollup refresh, or once by
# an export, that is nothing; the dashboard asks per request, so it
# reads the commit from `dict_repositories` instead.
CURRENT_ARTIFACTS_DDL = f"""
CREATE VIEW IF NOT EXISTS current_artifacts AS
SELECT a.*
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha, depgraph_observed_at FROM repositories FINAL
) AS r
    ON {ON_CURRENT_SCAN}
""".strip()

# One row per dependency fact in the current scans.
#
# GitHub's dependency graph reports per manifest, so a package declared
# in both `package.json` and `packages/x/package.json` is two rows that
# differ only in `artifact_id`. Counting rows made "dependency records"
# 19,384,165 where the distinct count is 16,905,915. `artifact_id` is
# deliberately not in the key: it is the per-manifest discriminator,
# and dropping it is the whole point.
#
# The key was pasted into three rollups and the export before it had a
# home here; they read this view now, so they cannot drift apart.
FACTS_DDL = """
CREATE VIEW IF NOT EXISTS facts AS
SELECT DISTINCT repository_id, name, version, type, found_by,
                relationship, source, version_kind
FROM current_artifacts
""".strip()

#: Views over the tables, in dependency order. Declared after the tables
#: and before the rollups, which read them.
VIEW_DDL: tuple[tuple[str, str], ...] = (
    ('current_artifacts', CURRENT_ARTIFACTS_DDL),
    ('facts', FACTS_DDL),
)

# A column line in the DDL: four spaces, a name, then its definition up
# to the trailing comma. Comments are part of the definition ClickHouse
# accepts, so they are carried through to ALTER statements unchanged.
_ENGINE_RE = re.compile(r'ENGINE\s*=\s*(\w+)')


def ddl_engine(ddl: str) -> str:
    """Engine a CREATE TABLE statement asks for."""
    match = _ENGINE_RE.search(ddl)
    return match.group(1) if match else ''


_COLUMN_RE = re.compile(
    r'^\s{4}(\w+)\s+(.+?),?$', re.MULTILINE,
)


def ddl_columns(ddl: str) -> list[str]:
    """Column names declared in a CREATE TABLE statement, in order."""
    return [name for name, _ in _COLUMN_RE.findall(ddl)]


def ddl_column_definitions(ddl: str) -> dict[str, str]:
    """Map each declared column to its type and modifiers.

    Used to bring an existing table up to the current DDL: the type,
    DEFAULT and COMMENT come from one place, so a migrated column is
    defined exactly as a freshly created one would be.
    """
    return {
        name: definition.rstrip().rstrip(',')
        for name, definition in _COLUMN_RE.findall(ddl)
    }


#: DDL paired with the table it creates, for schema reconciliation.
TABLE_DDL: tuple[tuple[str, str], ...] = (
    ('repositories', REPOSITORIES_DDL),
    ('artifacts', ARTIFACTS_DDL),
    ('releases', RELEASES_DDL),
    ('edges', EDGES_DDL),
    ('raw_documents', RAW_DOCUMENTS_DDL),
)
