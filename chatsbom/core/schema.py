"""ClickHouse schema: DDL plus the insert-column contract for each table.

The DDL and the insert column list are two views of the same truth. They
live together here so they cannot drift apart, and `ddl_columns` lets a
test assert that every declared insert column actually exists in the DDL.
"""
import re

from chatsbom.core.table import Table

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
    vulnerability_alerts_count Nullable(UInt32) COMMENT 'Number of vulnerability alerts'
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
# by joining on the repository's recorded `sbom_commit_sha`, which already
# identifies the latest scan.
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
# `SummingMergeTree(repositories)` rather than MergeTree: re-ingesting a
# pair adds to its count on merge instead of leaving two rows, so a
# partial re-run is idempotent in the only sense that matters here —
# `SELECT sum(repositories) ... GROUP BY` is correct whether or not a
# merge has happened yet, and a plain MergeTree would need the caller to
# remember to delete first.
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

ALL_DDL = (REPOSITORIES_DDL, ARTIFACTS_DDL, RELEASES_DDL, EDGES_DDL)

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
)
