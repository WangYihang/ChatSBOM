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
