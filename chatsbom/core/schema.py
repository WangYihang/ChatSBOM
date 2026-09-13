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
    languages String DEFAULT '{}' COMMENT 'Language distribution as JSON',
    vulnerability_alerts_count Nullable(UInt32) COMMENT 'Number of vulnerability alerts'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (id)
""".strip()

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
    sbom_ref String DEFAULT '' COMMENT 'Ref used for SBOM (tag or branch)',
    sbom_commit_sha String DEFAULT '' COMMENT 'Full Commit SHA for SBOM',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (repository_id, artifact_id, name, version, sbom_commit_sha)
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
        'license_spdx_id', 'license_name',
    ),
)

ARTIFACTS = Table(
    name='artifacts',
    columns=(
        'repository_id', 'artifact_id', 'name', 'version', 'type', 'purl',
        'found_by', 'licenses', 'relationship', 'sbom_ref', 'sbom_commit_sha',
    ),
)

RELEASES = Table(
    name='releases',
    columns=(
        'repository_id', 'release_id', 'tag_name', 'name', 'is_prerelease',
        'is_draft', 'published_at', 'target_commitish', 'created_at',
        'release_assets', 'source',
    ),
)

ALL_DDL = (REPOSITORIES_DDL, ARTIFACTS_DDL, RELEASES_DDL)

# A column line in the DDL: four spaces, a name, then its definition up
# to the trailing comma. Comments are part of the definition ClickHouse
# accepts, so they are carried through to ALTER statements unchanged.
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
)
