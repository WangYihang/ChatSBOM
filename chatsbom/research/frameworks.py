"""Which frameworks the corpus uses, asked of the warehouse (#153).

The research tools' two questions, which they asked the ClickHouse
server until it went: `openapi candidates` wants the projects of each
framework, with the framework's own version and the OpenAPI tooling
they declare; `classify`, the frameworks each repository it classifies
uses. Both are of the current scan, which is what `facts` holds: each
repository's newest scan of each source, of the corpus, by the one rule
the warehouse derives everything by (`warehouse/rollups.py`). Only they
ask, so the questions are theirs, and left the warehouse's package with
them (#167).

The connection is the caller's, opened read-only (`connect`): nothing
here writes.
"""
from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.models.provenance import RESOLVED
from chatsbom.models.relationship import DIRECT

if TYPE_CHECKING:
    import duckdb

#: The projects of the corpus that use one framework, in one pass over
#: the facts of the names that matter: the framework's own packages, its
#: OpenAPI tooling, and the packages that take a project off its list.
#:
#: The version is the framework's own: of the package a project declares
#: directly where it does, then of the first of the framework's names it
#: has, resolved rather than a constraint where both are there. Any one
#: of them took whichever came first: Starlette's version was FastAPI's,
#: and chi v1's, which a dependency pulled in, that of a project on chi
#: v5 (#47). The commit is the current Syft scan's, else the manifests'.
CANDIDATES = """
WITH asked AS (
    SELECT $packages::VARCHAR[] AS packages,
           $indicators::VARCHAR[] AS indicators,
           $excluded::VARCHAR[] AS excluded
),
scanned AS (
    SELECT repository_id,
           coalesce(
               max(commit_sha) FILTER (WHERE source = 'syft'),
               max(commit_sha) FILTER (WHERE source = 'manifest')
           ) AS commit_sha
    FROM current_scans
    GROUP BY repository_id
)
SELECT
    r.id AS repository_id,
    r.owner AS owner,
    r.repo AS repo,
    r.stars AS stars,
    r.language AS language,
    r.default_branch AS default_branch,
    r.latest_release_tag AS latest_release,
    coalesce(s.commit_sha, '') AS commit_sha,
    arg_min(
        f.version,
        (
            f.relationship != $direct,
            list_position(a.packages, f.name),
            f.version_kind != $resolved,
            f.version
        )
    ) FILTER (WHERE list_contains(a.packages, f.name)) AS framework_version,
    coalesce(
        list_sort(list_distinct(
            list(f.name) FILTER (WHERE list_contains(a.indicators, f.name))
        )),
        []::VARCHAR[]
    ) AS matched_dependencies
FROM facts AS f
CROSS JOIN asked AS a
JOIN repositories AS r ON r.id = f.repository_id
LEFT JOIN scanned AS s ON s.repository_id = r.id
WHERE list_contains(a.packages || a.indicators || a.excluded, f.name)
GROUP BY r.id, r.owner, r.repo, r.stars, r.language, r.default_branch,
         r.latest_release_tag, s.commit_sha
HAVING count(*) FILTER (WHERE list_contains(a.packages, f.name)) > 0
   AND count(*) FILTER (WHERE list_contains(a.excluded, f.name)) = 0
ORDER BY r.stars DESC, r.owner, r.repo
"""

#: Which of the named packages each of the named repositories has, at
#: what version, in its current scan.
USES = """
SELECT DISTINCT repository_id, name, version
FROM facts
WHERE name IN (SELECT unnest($packages::VARCHAR[]))
  AND repository_id IN (SELECT unnest($ids::UBIGINT[]))
ORDER BY repository_id, name, version
"""


def candidates(
    con: duckdb.DuckDBPyConnection,
    packages: Sequence[str],
    indicators: Sequence[str],
    excluded: Sequence[str],
) -> Iterator[dict[str, Any]]:
    """The projects using a framework whose packages are `packages` and
    using none of `excluded`, by stars: each with the framework's
    version, and which of `indicators`, its OpenAPI tooling, it has."""
    cursor = con.execute(
        CANDIDATES,
        {
            'packages': list(packages),
            'indicators': list(indicators),
            'excluded': list(excluded),
            'direct': DIRECT,
            'resolved': RESOLVED,
        },
    )
    columns = [column[0] for column in cursor.description or ()]
    for values in cursor.fetchall():
        yield dict(zip(columns, values))


def frameworks_of(
    con: duckdb.DuckDBPyConnection,
    repository_ids: Sequence[int],
    framework_map: Mapping[str, Sequence[str]],
) -> dict[int, list[tuple[str, str]]]:
    """The frameworks each of `repository_ids` uses, as `(framework,
    version)`, sorted: its current scan's, since `classify` takes the
    first version it is given for the framework it picks, and a version
    of every scan could be one the repository moved off long ago.

    `framework_map` names each framework's packages
    (`FrameworkIndex.as_framework_map`). One query for every
    repository: one a repository was the N+1 in `github classify`.
    """
    package_to_framework = {
        package: framework
        for framework, packages in framework_map.items()
        for package in packages
        if package
    }
    if not repository_ids or not package_to_framework:
        return {}
    rows = con.execute(
        USES,
        {
            'packages': list(package_to_framework),
            'ids': [int(i) for i in repository_ids],
        },
    ).fetchall()
    found: dict[int, list[tuple[str, str]]] = {}
    for repository_id, name, version in rows:
        framework = package_to_framework[str(name)]
        found.setdefault(int(repository_id), []).append(
            (framework, '' if version is None else str(version)),
        )
    for entries in found.values():
        entries.sort()
    return found
