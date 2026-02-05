REPOSITORIES_DDL = """
CREATE TABLE IF NOT EXISTS repositories (
    id UInt64 COMMENT 'GitHub Repository ID',
    owner String COMMENT 'Owner Name',
    repo String COMMENT 'Repository Name',
    full_name String COMMENT 'Full Name (owner/repo)',
    url String COMMENT 'Repository URL',
    stars UInt64 COMMENT 'Star Count',
    description String COMMENT 'Repository Description',
    created_at DateTime COMMENT 'Creation Time',
    language String COMMENT 'Programming Language',
    topics Array(String) COMMENT 'GitHub Topics',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'
) ENGINE = ReplacingMergeTree(id)
ORDER BY (id)
""".strip()

ARTIFACTS_DDL = """
CREATE TABLE IF NOT EXISTS artifacts (
    repository_id UInt64 COMMENT 'GitHub Repository ID',
    artifact_id String COMMENT 'Artifact ID (from SBOM)',
    name String COMMENT 'Component Name',
    version String COMMENT 'Component Version',
    type String COMMENT 'Component Type',
    purl String COMMENT 'Package URL',
    found_by String COMMENT 'Detector Name',
    licenses Array(String) COMMENT 'License List',
    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'
) ENGINE = ReplacingMergeTree()
ORDER BY (repository_id, artifact_id, name, version)
""".strip()

ALL_DDL = f"""
{REPOSITORIES_DDL};

{ARTIFACTS_DDL};
""".strip()
