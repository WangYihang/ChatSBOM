"""The store's documents, projected into rows: what the warehouse holds.

The parsers `db index` used to fill ClickHouse, which the warehouse
shares (#131): a repository record, its releases, a commit's Syft
document judged against the same commit's manifests, what its Gradle
builds and podspecs declare, and a dependency-graph document. The
ingest that wrote them into ClickHouse, and the queries that read them
back, went with the server (#153); what reads the store now is
`chatsbom/warehouse/store.py`.
"""
import json
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from chatsbom.core import gradle
from chatsbom.core import podspec
from chatsbom.core.discovery import ecosystem_of
from chatsbom.core.discovery import NAME_ECOSYSTEM
from chatsbom.core.discovery import SUFFIX_ECOSYSTEM
from chatsbom.core.documents import Document
from chatsbom.core.ecosystems import artifact_ecosystem
from chatsbom.core.ecosystems import MEMBERS
from chatsbom.core.instants import utc
from chatsbom.core.manifest import ByEcosystem
from chatsbom.core.manifest import classify
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import sources_of
from chatsbom.models import github_release
from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.relationship import DIRECT
from chatsbom.models.repository import Repository
from chatsbom.services.dependency_graph_service import parse_spdx_document

#: What `release_assets` keeps of an asset, and how; beside the release
#: model, where the store's release lists take them from too (#147).
ASSET_FIELDS = github_release.ASSET_FIELDS
_trimmed_assets = github_release.trimmed_assets


class DbService:
    """The projections of the store's documents into rows, as the
    warehouse's tables take them (`warehouse/schema.py`)."""

    def scan_rows(
        self,
        sbom: Document | None,
        manifests: Sequence[tuple[str, str | None]],
        repo_id: int,
        repo_row: Mapping[str, Any],
        by_ecosystem: ByEcosystem | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """The rows of one commit's scan: Syft's, and the manifests'.

        One commit is read by two sources together, and this is the rule
        that pairs them. Syft's document of the commit's content root is
        judged against the manifests of the same commit, so every
        direct/transitive verdict describes the scan it is stored under.
        What the commit's Gradle builds and podspecs declare is stamped
        with the Syft document's instant, or the unset date when there is
        none: never the time of the ingest.

        One method, so that every commit the store holds is paired the
        same way, as the warehouse reads them all (#131).

        `repo_row` carries the scan's `sbom_ref` and `sbom_commit_sha`.
        `by_ecosystem` is `relationships_from(manifests)`, for a caller
        that has it already.
        """
        if by_ecosystem is None:
            by_ecosystem = relationships_from(manifests) if manifests else {}
        syft_rows = self.parse_artifacts(
            sbom, repo_id, repo_row, direct_deps=by_ecosystem,
        ) if sbom is not None else []
        declared_rows = self.parse_manifests(
            manifests, repo_id, repo_row,
            observed_at=sbom.observed_at if sbom else None,
        )
        return syft_rows, declared_rows

    def parse_repository(
        self,
        repo: Repository,
        direct_deps: ByEcosystem | None = None,
        graph: Document | None = None,
    ) -> dict[str, Any]:
        """Project a Repository into a `repositories` row mapping.

        `direct_deps` carries which manifests were read, which is the
        audit trail behind every direct/transitive verdict: without it a
        `transitive` label is indistinguishable from `unknown`.

        `graph` is the dependency-graph document indexed with it, which
        the row records by `graph_observed_at`: its graph rows are
        current by that document, not by the Syft commit.
        """
        target = repo.download_target
        release = repo.latest_stable_release

        return {
            'id': repo.id,
            'owner': repo.owner,
            'repo': repo.repo,
            'url': repo.url or '',
            'stars': repo.stars,
            'description': repo.description or '',
            'created_at': utc(repo.created_at),
            'language': repo.language or '',
            'topics': repo.topics,
            'default_branch': repo.default_branch,
            'sbom_ref': target.ref if target else '',
            'sbom_ref_type': target.ref_type if target else '',
            'sbom_commit_sha': target.commit_sha if target else '',
            'sbom_commit_sha_short': target.commit_sha_short if target else '',
            'has_releases': bool(repo.has_releases),
            'latest_release_tag': release.tag_name if release else '',
            'latest_release_published_at': utc(
                release.published_at if release else None,
            ),
            'total_releases': repo.total_releases,
            'pushed_at': utc(repo.pushed_at),
            'is_archived': repo.is_archived,
            'is_fork': repo.is_fork,
            'is_template': repo.is_template,
            'is_mirror': repo.is_mirror,
            'disk_usage': repo.disk_usage,
            'fork_count': repo.fork_count,
            'watchers_count': repo.watchers_count,
            'license_spdx_id': repo.license_spdx_id or '',
            'license_name': repo.license_name or '',
            'manifest_sources': sources_of(direct_deps),
            'depgraph_observed_at': graph_observed_at(graph),
            'depgraph_ref': (
                (graph.ref or repo.default_branch) if graph is not None else ''
            ),
            'depgraph_commit_sha': graph.commit_sha if graph is not None else '',
            'github_language': _github_language(repo),
            'snapshot': _snapshot(repo),
            # Filled in once the artifacts are known (`ecosystems_of`).
            'ecosystems': [],
        }

    def parse_releases(self, repo: Repository) -> list[dict[str, Any]]:
        """Project a Repository's releases into `releases` row mappings."""
        return [
            {
                'repository_id': repo.id,
                'release_id': r.id,
                'tag_name': r.tag_name,
                'name': r.name or '',
                'is_prerelease': r.is_prerelease,
                'is_draft': r.is_draft,
                'published_at': utc(r.published_at),
                'target_commitish': r.target_commitish or '',
                'created_at': utc(r.created_at),
                'release_assets': json.dumps(_trimmed_assets(r.assets)),
                'source': r.source,
            }
            for r in (repo.all_releases or [])
        ]

    def parse_artifacts(
        self,
        document: Document,
        repo_id: int,
        repo_row: Mapping[str, Any],
        direct_deps: ByEcosystem | None = None,
    ) -> list[dict[str, Any]]:
        """Project a Syft SBOM into `artifacts` row mappings.

        Takes a document rather than a path: reading one is the
        `DocumentSource`'s job (`core/documents.py`). `observed_at`
        comes with it — when the document was collected is a property of
        the document, not of this call.

        SBOM provenance is carried over from the repository row by column
        name, so the artifact and its repository always agree on which
        commit was scanned.

        `direct_deps` is the declared set per ecosystem. Each artifact is
        judged in its own (`manifest.classify`): a Maven artifact
        against the poms and Gradle builds, an npm one against the
        package.json files, whatever the repository's language.
        """
        data = document.body
        sbom_ref = repo_row['sbom_ref']
        sbom_commit_sha = repo_row['sbom_commit_sha']
        seen_at = document.observed_at

        return [
            {
                'repository_id': repo_id,
                'artifact_id': art.get('id', ''),
                'name': art.get('name', ''),
                'version': art.get('version', ''),
                'type': art.get('type', ''),
                'purl': art.get('purl', ''),
                'found_by': art.get('foundBy', ''),
                'licenses': _licenses(art.get('licenses', [])),
                'relationship': classify(
                    direct_deps, art.get('name') or '',
                    art.get('type') or '', art.get('purl') or '',
                ),
                'source': SYFT,
                'version_kind': classify_version(art.get('version'))[1],
                'sbom_ref': sbom_ref,
                'sbom_commit_sha': sbom_commit_sha,
                'observed_at': seen_at,
            }
            for art in data.get('artifacts', [])
        ]

    def parse_dependency_graph(
        self,
        document: Document,
        repo_id: int,
        repo_row: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Project a stored GitHub dependency-graph document into rows.

        These land in the same table as Syft's output, distinguished by
        `source`. Their versions are classified, since the graph reports
        manifest constraints rather than resolutions.

        A graph is its own observation. GitHub builds it from the default
        branch when it is asked, so the rows are keyed by the document,
        by the instant it states (`graph_observed_at`), which the
        repository row records too; that is what makes a graph fetched
        again, at an unchanged Syft target, replace the one before
        rather than add to it (#22).

        `sbom_ref` and `sbom_commit_sha` are the graph's own: the
        default branch and the HEAD sha the depgraph stage read
        immediately before fetching it (`core/depgraph_store`), not the
        tag and commit the Syft scan read. Which graph rows are current
        is decided by the document's instant, `depgraph_observed_at`,
        which every repository row with a graph records, so a graph row
        whose commit differs from the scan's stays current (#22, #23).

        A legacy document, fetched before the stamp was kept, has no
        commit of its own: it keeps the default branch and the Syft
        scan's commit, as before. Written empty instead, a repository
        row indexed before #22 recorded no instant, and there the commit
        still decides.
        """
        observed = graph_observed_at(document)
        return [
            {
                'repository_id': repo_id,
                'sbom_ref': document.ref or repo_row['default_branch'],
                'sbom_commit_sha': (
                    document.commit_sha or repo_row['sbom_commit_sha']
                ),
                'observed_at': observed,
                **row,
            }
            for row in parse_spdx_document(document.body)
        ]

    def parse_manifests(
        self,
        manifests: Sequence[tuple[str, str | None]],
        repo_id: int,
        repo_row: Mapping[str, Any],
        observed_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """What the repository's Gradle builds and podspecs declare, as
        artifact rows.

        `source = 'manifest'` (owner decision D1 on #55). Only for the
        files neither Syft nor the dependency graph reads:
        `build.gradle(.kts)`, resolved against `settings.gradle(.kts)`,
        `gradle.properties`, the version catalogs and the constants in
        `buildSrc` (`core/gradle.py` says what is and is not resolved);
        and a CocoaPods library's `.podspec` (`core/podspec.py`). A
        `pom.xml` gives none: Syft's `java-pom-cataloger` reports it.

        Every row is `direct`, since the build declares it, and carries
        a *declared* version: `constraint` when it has one and
        `unversioned` when not, never `resolved`. `found_by` says how the
        coordinate was read: a literal, a version-catalog entry, a
        constant, or a podspec.

        Stamped with the Syft scan's ref and commit: the files are the
        content root that scan read, at that commit, so the rows are
        current, and forgotten, with the scan. With no commit there is
        no row. `observed_at` is the scan's, else the unset date: never
        the time of the ingest.
        """
        sha = repo_row['sbom_commit_sha']
        if not sha or not manifests:
            return []
        seen_at = utc(observed_at)
        rows = []
        for path, declared in gradle.declarations(manifests):
            c = declared.coordinate
            rows.append({
                'repository_id': repo_id,
                'artifact_id': f'{path}#{c.group}:{c.name}:{c.version}',
                'name': c.name,
                'version': c.version,
                'type': 'maven',
                'purl': gradle.purl_of(c),
                'found_by': gradle.MANIFEST_FOUND_BY[declared.via],
                'licenses': [],
                'relationship': DIRECT,
                'source': MANIFEST,
                'version_kind': CONSTRAINT if c.version else UNVERSIONED,
                'sbom_ref': repo_row['sbom_ref'],
                'sbom_commit_sha': sha,
                'observed_at': seen_at,
            })
        for path, pod in podspec.declarations(manifests):
            rows.append({
                'repository_id': repo_id,
                'artifact_id': f'{path}#{pod.name}@{pod.requirement}',
                'name': pod.name,
                'version': pod.requirement,
                'type': podspec.ECOSYSTEM,
                'purl': pod.purl,
                'found_by': podspec.FOUND_BY,
                'licenses': [],
                'relationship': DIRECT,
                'source': MANIFEST,
                'version_kind': CONSTRAINT if pod.requirement else UNVERSIONED,
                'sbom_ref': repo_row['sbom_ref'],
                'sbom_commit_sha': sha,
                'observed_at': seen_at,
            })
        return rows

    # -- queries ------------------------------------------------------------


def graph_observed_at(graph: Document | None) -> datetime:
    """Which dependency-graph document a row belongs to.

    GitHub builds the graph from the default branch when it is asked,
    so a graph is an observation of its own, with its own identity: the
    instant it states in `creationInfo.created`. That is the document's
    `observed_at`, which its rows carry, and it is recorded on its
    repository as `depgraph_observed_at`. Both are stamped here and
    nowhere else, so the comparison that decides which rows are current
    is between one value written twice: aware UTC, in whole seconds
    (`instants.utc`), into two `DateTime` columns.

    Without a graph, the unset date, as every absent date in the schema
    is. No document states it, so it selects no row.
    """
    return utc(graph.observed_at if graph is not None else None)


def _licenses(raw: list[Any]) -> list[str]:
    """Flatten Syft's several license shapes into SPDX-ish strings."""
    out = []
    for lic in raw:
        if isinstance(lic, dict):
            value = lic.get('value') or lic.get(
                'spdxExpression',
            ) or lic.get('name')
            if value:
                out.append(value)
        elif isinstance(lic, str):
            out.append(lic)
    return out


def _snapshot(repo: Repository) -> str:
    """The search snapshot the list says names the repository, or ''.

    Only the list says, the search snapshots' (`TrackedRecords`): a
    record carries none, so a repository no snapshot lists is in none,
    and outside the corpus.
    """
    stated = (repo.model_extra or {}).get('snapshot')
    return stated if isinstance(stated, str) else ''


def _github_language(repo: Repository) -> str:
    """GitHub's language, verbatim: the list's, else the record's."""
    stated = (repo.model_extra or {}).get('github_language')
    if isinstance(stated, str) and stated:
        return stated
    return repo.language or ''


#: Ecosystems a repository can be said to have: every one discovery
#: knows a manifest of, and every one an artifact type canonicalises to
#: (`core/ecosystems.py`). Anything else a collector reports -- a
#: `binary`, a `github-action` -- is not an ecosystem of the project.
KNOWN_ECOSYSTEMS: frozenset[str] = frozenset(
    set(NAME_ECOSYSTEM.values()) | set(SUFFIX_ECOSYSTEM.values())
    | set(MEMBERS),
)


def ecosystems_of(
    rows: Iterable[Mapping[str, Any]],
    manifests: Iterable[tuple[str, str | None]] = (),
) -> list[str]:
    """The canonical ecosystems of a repository's current scan.

    From its artifacts, whichever source reported them, and from the
    manifests of its content root, so a repository whose Syft scan
    found nothing still says what it is built with.
    """
    found: set[str] = set()
    for row in rows:
        ecosystem = artifact_ecosystem(
            str(row.get('type') or ''), str(row.get('purl') or ''),
        )
        if ecosystem in KNOWN_ECOSYSTEMS:
            found.add(ecosystem)
    for path, _ in manifests:
        from_path = ecosystem_of(path)
        if from_path in KNOWN_ECOSYSTEMS:
            found.add(from_path)
    return sorted(found)
