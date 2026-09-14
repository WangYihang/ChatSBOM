"""The queries that define what an export contains.

Its own module because neither export owns it. `export parquet` and
`export d1` both read these, and a copy per format would let the two
describe different data — which is the failure this prevents rather
than a tidiness preference: the Parquet files and the D1 snapshot are
published as the same dataset.

`observed_range` lives here for the same reason. Both manifests report
freshness, and both must derive it from the rows rather than a clock.
"""
from __future__ import annotations

from collections.abc import Iterable

from chatsbom.models.relationship import DIRECT


REPOSITORIES_QUERY = f"""
SELECT
    r.id AS id,
    r.owner AS owner,
    r.repo AS repo,
    r.stars AS stars,
    lower(r.language) AS language,
    r.url AS url,
    r.description AS description,
    r.license_spdx_id AS license_spdx_id,
    formatDateTime(r.pushed_at, '%Y-%m-%d') AS pushed_at,
    -- When *we* last looked, as distinct from when upstream last
    -- pushed. A repository can have been pushed to yesterday and last
    -- scanned six months ago, and only the second explains a stale row.
    -- max(observed_at) over its artifacts is the recorded observation;
    -- the 11,840 repositories with no dependencies have no artifact to
    -- carry one, so those fall back to the row's own write time.
    formatDateTime(
        greatest(max(a.observed_at), r.updated_at), '%Y-%m-%d'
    ) AS observed_at,
    r.sbom_ref AS sbom_ref,
    r.sbom_commit_sha AS sbom_commit_sha,
    countDistinctIf(
        a.name, a.name != '' AND a.relationship = '{DIRECT}'
    ) AS direct_dependencies,
    countDistinctIf(a.name, a.name != '') AS total_dependencies,
    r.manifest_sources AS manifest_sources
FROM repositories AS r FINAL
LEFT JOIN artifacts AS a
    ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    r.id, r.owner, r.repo, r.stars, r.language, r.url, r.description,
    r.license_spdx_id, r.pushed_at, r.updated_at, r.sbom_ref,
    r.sbom_commit_sha,
    r.manifest_sources
ORDER BY r.stars DESC, r.id ASC
"""

# Sorted by name so a "who depends on X" lookup touches few row groups.
ARTIFACTS_QUERY = """
SELECT
    a.repository_id AS repository_id,
    a.name AS name,
    a.version AS version,
    a.type AS type,
    a.found_by AS found_by,
    a.relationship AS relationship,
    a.source AS source,
    a.version_kind AS version_kind
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha FROM repositories FINAL
) AS r ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    a.repository_id, a.name, a.version, a.type, a.found_by, a.relationship,
    a.source, a.version_kind
ORDER BY a.name ASC, a.repository_id ASC, a.version ASC
"""

# Monthly adoption per package, straight off the append-only table. Kept
# in its own file so the dashboard's current-state payload stays small —
# only a page asking a temporal question needs to fetch this.
HISTORY_QUERY = f"""
-- Per source, not merged.
--
-- Syft resolves a lockfile's closure; GitHub's graph parses manifests.
-- They ran seven months apart, so a single series over both drew a line
-- from February's 124 to September's 149 and read as adoption growing
-- when the only thing that changed was the instrument.
SELECT
    a.name AS name,
    formatDateTime(a.observed_at, '%Y-%m') AS month,
    a.source AS source,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
        AS direct_count
FROM artifacts AS a
WHERE a.name != ''
GROUP BY a.name, month, a.source
ORDER BY a.name ASC, a.source ASC, month ASC
"""

# Licence distribution. Unknown is kept as an explicit empty string rather
# than dropped: "we do not know" is a finding about SBOM quality, and
# hiding it would overstate how well licences are covered.
LICENSES_QUERY = """
SELECT
    coalesce(arrayElement(a.licenses, 1), '') AS license,
    a.type AS type,
    countDistinct(a.name) AS package_count,
    countDistinct(a.repository_id) AS repository_count
FROM artifacts AS a
INNER JOIN (
    SELECT id, sbom_commit_sha FROM repositories FINAL
) AS r ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
WHERE a.name != ''
GROUP BY license, type
ORDER BY repository_count DESC, license ASC
LIMIT 500
"""

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
