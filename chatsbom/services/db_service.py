import json
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core import depgraph_store
from chatsbom.core import gradle
from chatsbom.core.config import get_config
from chatsbom.core.discovery import ecosystem_of
from chatsbom.core.discovery import NAME_ECOSYSTEM
from chatsbom.core.discovery import SUFFIX_ECOSYSTEM
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import Document
from chatsbom.core.documents import DocumentSource
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import ManifestSource
from chatsbom.core.documents import RecordSource
from chatsbom.core.documents import SYFT as SYFT_KIND
from chatsbom.core.ecosystems import artifact_ecosystem
from chatsbom.core.ecosystems import MEMBERS
from chatsbom.core.instants import utc
from chatsbom.core.manifest import ByEcosystem
from chatsbom.core.manifest import classify
from chatsbom.core.manifest import DirectDependencies
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import sources_of
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.stats import BaseStats
from chatsbom.core.table import Table
from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.query import DatabaseStats
from chatsbom.models.query import Dependent
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
from chatsbom.models.relationship import DIRECT
from chatsbom.models.repository import Repository
from chatsbom.services.dependency_graph_service import parse_spdx_document

logger = structlog.get_logger('db_service')

BATCH_SIZE = 1000


#: Asset fields worth keeping, of the sixteen GitHub returns.
#:
#: `release_assets` is written and never read — no query, no rollup, no
#: panel touches it — and it was the largest column in the database:
#: 5.00 GiB uncompressed against about 90 MiB for every other column in
#: `releases` combined, and 9.7 GiB of the ledgers on disk. A single
#: asset averaged 1,555 bytes, of which `uploader` was a complete
#: GitHub user object.
#:
#: Kept rather than dropped entirely, because the column being unread
#: today is not evidence nobody will ask: "which releases ship a
#: binary, how large, and does it carry a checksum" is a reasonable
#: question of a supply-chain dataset, and this project has twice paid
#: for discarding what it had not yet needed.
#:
#: `digest` is on 11.2% of assets and is the checksum, so it stays even
#: though most rows lack it. Measured: 1,555 -> 301 bytes, 81% smaller,
#: which takes the column from 5.00 GiB to about 0.97 GiB.
ASSET_FIELDS: frozenset[str] = frozenset({
    'name',
    'content_type',
    'size',
    'download_count',
    'browser_download_url',
    'created_at',
    'digest',
})


def _trimmed_assets(assets: object) -> list[dict[str, object]]:
    """Release assets, carrying only the fields worth storing.

    Anything that is not a list of mappings is returned as an empty
    list rather than raised on: this runs inside an ingest over 28,000
    repositories, and one oddly-shaped release is not a reason to lose
    the rest.
    """
    if not isinstance(assets, list):
        return []
    trimmed = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        trimmed.append({
            key: value for key, value in asset.items()
            if key in ASSET_FIELDS
        })
    return trimmed


@dataclass(frozen=True, slots=True)
class FrameworkUsage:
    """How widely one framework is used within a language."""

    framework: Framework
    repository_count: int
    direct_count: int
    samples: list[Dependent]


@dataclass(frozen=True, slots=True)
class FrameworkStats:
    """Framework usage for one language."""

    language: Language
    frameworks: list[FrameworkUsage]


@dataclass
class DbStats(BaseStats):
    repos: int = 0
    artifacts: int = 0
    releases: int = 0


class Batch:
    """Accumulates column-keyed records and flushes them in fixed chunks."""

    def __init__(self, table: Table, repo_db: IngestionRepository, size: int = BATCH_SIZE):
        self.table = table
        self.repo_db = repo_db
        self.size = size
        self._pending: list[Mapping[str, Any]] = []

    def add(self, record: Mapping[str, Any]) -> None:
        self._pending.append(record)
        if len(self._pending) >= self.size:
            self.flush()

    def extend(self, records: Sequence[Mapping[str, Any]]) -> None:
        for record in records:
            self.add(record)

    def flush(self) -> None:
        if not self._pending:
            return
        started = datetime.now()
        rows = self.table.rows(self._pending)
        self.repo_db.insert_batch(
            self.table.name, rows, self.table.column_names,
        )
        logger.info(
            'Batch Inserted',
            table=self.table.name,
            count=len(rows),
            elapsed=f"{(datetime.now() - started).total_seconds():.3f}s",
        )
        self._pending = []


class DbService:
    """Service for ingesting repository data and SBOMs into ClickHouse."""

    def __init__(self):
        self.config = get_config()

    # -- ingestion ----------------------------------------------------------

    @staticmethod
    def scans_in(
        records: RecordSource,
        limit: int | None = None,
    ) -> list[tuple[int, str]]:
        """The `(repository_id, sbom_commit_sha)` pairs about to be written.

        Read from the same source the ingest will read from, ahead of
        it, so the rows for those exact scans can be dropped first —
        see `IngestionRepository.forget_scans`, which drops the Syft and
        the manifest rows of each. Without that, a re-ingest appends
        instead of refreshing: measured once, `db index --language
        python` added 687,000 duplicate rows.

        A record with no commit sha is skipped rather than deleted under
        the empty string: that would match every row whose scan is
        unknown, across every repository.
        """
        scans: list[tuple[int, str]] = []
        for data in records.records(limit):
            repository_id = data.get('id')
            target = data.get('download_target') or {}
            sha = target.get('commit_sha') if isinstance(
                target, dict,
            ) else None
            if isinstance(repository_id, int) and isinstance(sha, str) and sha:
                scans.append((repository_id, sha))
        return scans

    @staticmethod
    def graphs_in(
        records: RecordSource,
        documents: DocumentSource,
        limit: int | None = None,
        depgraph_root: Path | None = None,
    ) -> list[tuple[int, datetime]]:
        """The `(repository_id, observed_at)` of each graph about to be
        written: `scans_in` for the dependency graphs.

        A graph's rows are keyed by the document, by the instant it
        states (`graph_observed_at`), so it is the document source that
        can say which graph the ingest will read, not the ledger. For
        `IngestionRepository.forget_graphs`, which drops that document's
        rows first so that indexing it again does not add a copy.

        The same records, limit and paths the ingest reads, so a
        document forgotten here is the one written back.
        """
        fetched = _fetched_paths(depgraph_root)
        wanted: dict[int, str | None] = {}
        for data in records.records(limit):
            repository_id = data.get('id')
            if isinstance(repository_id, int):
                wanted[repository_id] = _graph_path(
                    data, repository_id, fetched, depgraph_root,
                )
        return sorted(documents.observations(DEPGRAPH, wanted).items())

    def ingest_from_list(
        self,
        records: RecordSource,
        repo_db: IngestionRepository,
        progress_callback: Callable[[], None] | None = None,
        limit: int | None = None,
        documents: DocumentSource = FILES,
        manifests: ManifestSource = FILE_MANIFESTS,
        depgraph_root: Path | None = None,
    ) -> DbStats:
        """Ingest repositories, releases and every artifact source.

        Three sources, and each can be a ledger on disk or the
        `raw_documents` table:

        - `records` decides *which* repositories are ingested, and
          supplies their metadata, releases and download target.
          `db index` hands it `TrackedRecords`, so every repository the
          ledger tracks is ingested, with or without a scan.
        - `documents` supplies the SBOMs and dependency graphs.
        - `manifests` supplies the declared sets behind every
          direct/transitive verdict, and the Gradle files the
          `manifest` rows are read from.

        A repository's artifacts come from up to three sources: Syft's
        scan, GitHub's dependency graph, and what its Gradle build files
        declare (`source = 'manifest'`, `core/gradle.py`). A repository
        with none of them still gets its `repositories` row.
        """
        stats = DbStats()

        fetched = _fetched_paths(depgraph_root)

        repos = Batch(REPOSITORIES, repo_db)
        artifacts = Batch(ARTIFACTS, repo_db)
        releases = Batch(RELEASES, repo_db)

        for data in records.records(limit):
            try:
                # The metadata overlay is the source's business now: the
                # record carries metadata from when the SBOM was
                # generated, and both `LedgerRecords` and `RawRecords`
                # fold the fresher copy in before yielding. Measured
                # once, when nothing did: the ledger knew 722
                # repositories had been pushed in September while
                # `repositories.pushed_at` still topped out at
                # 2026-02-09.
                repo = Repository.model_validate(data)
                read = self._manifests_of(repo, manifests)
                by_ecosystem = relationships_from(read) if read else {}
                target = repo.download_target
                sbom = documents.get(
                    SYFT_KIND, repo.id, data.get('sbom_path'),
                    commit_sha=target.commit_sha if target else None,
                )
                # A second, independent source: GitHub's dependency graph
                # covers the Maven and Composer projects Syft cannot read.
                # Read before the repository row, which records it.
                graph = documents.get(
                    DEPGRAPH, repo.id,
                    _graph_path(data, repo.id, fetched, depgraph_root),
                )
                repo_row = self.parse_repository(repo, by_ecosystem, graph)
                release_rows = self.parse_releases(repo)

                artifact_rows: list[dict[str, Any]] = []

                if sbom is not None:
                    artifact_rows += self.parse_artifacts(
                        sbom, repo.id, repo_row, direct_deps=by_ecosystem,
                    )
                else:
                    stats.inc_skipped()

                if graph is not None:
                    artifact_rows += self.parse_dependency_graph(
                        graph, repo.id, repo_row,
                    )

                # The third: what the Gradle builds declare, which
                # neither Syft nor, reliably, the graph reads (D1).
                artifact_rows += self.parse_manifests(
                    read, repo.id, repo_row,
                    observed_at=sbom.observed_at if sbom else None,
                )

                repo_row['ecosystems'] = ecosystems_of(artifact_rows, read)
                repos.add(repo_row)
                releases.extend(release_rows)
                artifacts.extend(artifact_rows)

                stats.repos += 1
                stats.releases += len(release_rows)
                stats.artifacts += len(artifact_rows)
            except Exception as e:
                logger.error('Failed to process record', error=str(e))
                stats.inc_failed()

            if progress_callback:
                progress_callback()

        for batch in (repos, artifacts, releases):
            batch.flush()

        return stats

    @staticmethod
    def _manifests_of(
        repo: Repository,
        manifests: ManifestSource = FILE_MANIFESTS,
    ) -> list[tuple[str, str | None]]:
        """The repository's manifests at its scan's commit.

        The commit is the one the artifacts are stamped with, so the
        verdicts describe the scan they are stored under: the landing
        zone keeps every commit's manifests, and reading them all let a
        package an old commit declared be `direct` in this one. A
        repository with no scan has none: nothing it declared could be
        stamped with a commit, or judged against one.
        """
        target = repo.download_target
        if target is None:
            return []
        return manifests.for_repository(
            repo.id,
            repo.local_content_path,
            commit_sha=target.commit_sha,
        )

    @staticmethod
    def _direct_dependencies(
        repo: Repository,
        manifests: ManifestSource = FILE_MANIFESTS,
    ) -> dict[str, DirectDependencies]:
        """Declared dependencies of a repo, per ecosystem.

        Empty when nothing was read: every artifact then stays
        `unknown` rather than being guessed at. The repository's
        language is not asked (#55 §4.10): each artifact is judged in
        its own ecosystem (`manifest.classify`).

        `manifests` decides where they are read from. The judgement is
        the same either way -- `relationships_from` owns it -- which is
        what lets `raw_documents` reproduce the direct/transitive
        verdicts without the files.
        """
        read = DbService._manifests_of(repo, manifests)
        return relationships_from(read) if read else {}

    # -- parsing ------------------------------------------------------------

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
        `DocumentSource`'s job, so the same projection runs whether the
        SBOM came off disk or out of `raw_documents`. `observed_at`
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
        """What the repository's Gradle builds declare, as artifact rows.

        `source = 'manifest'` (owner decision D1 on #55). Only for the
        files Syft does not read: `build.gradle(.kts)`, resolved against
        `settings.gradle(.kts)`, `gradle.properties` and the version
        catalogs (`core/gradle.py` says what is and is not resolved). A
        `pom.xml` gives none: Syft's `java-pom-cataloger` reports it.

        Every row is `direct`, since the build declares it, and carries
        a *declared* version: `constraint` when it has one and
        `unversioned` when not, never `resolved`. `found_by` says whether
        the coordinate was a literal or a version-catalog entry.

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
        return rows

    # -- queries ------------------------------------------------------------

    def get_db_stats(self, query_repo: QueryRepository) -> DatabaseStats:
        return query_repo.get_stats()

    def get_language_stats(
        self,
        query_repo: QueryRepository,
    ) -> list[LanguageCount]:
        return query_repo.get_language_stats()

    def get_top_packages(
        self,
        query_repo: QueryRepository,
        limit: int = 20,
        language: str | None = None,
    ) -> list[PackagePopularity]:
        return query_repo.get_top_packages(limit=limit, language=language)

    def get_framework_stats(
        self,
        query_repo: QueryRepository,
    ) -> list[FrameworkStats]:
        results: list[FrameworkStats] = []
        for lang in Language:
            try:
                handler = LanguageFactory.get_handler(lang)
            except ValueError:
                continue

            frameworks = handler.get_frameworks()
            if not frameworks:
                continue

            usage = [
                self._framework_usage(query_repo, lang, fw)
                for fw in frameworks
            ]
            results.append(FrameworkStats(language=lang, frameworks=usage))
        return results

    @staticmethod
    def _framework_usage(
        query_repo: QueryRepository,
        language: Language,
        framework: Framework,
    ) -> FrameworkUsage:
        packages = FrameworkFactory.create(framework).get_package_names()
        return FrameworkUsage(
            framework=framework,
            repository_count=query_repo.get_framework_usage(
                str(language), packages,
            ),
            direct_count=query_repo.get_framework_usage(
                str(language), packages, direct_only=True,
            ),
            samples=query_repo.get_top_projects_by_framework(
                str(language), packages, limit=3,
            ),
        )

    def search_library(
        self,
        query_repo: QueryRepository,
        component: str,
        language: str | None = None,
        limit: int = 10,
    ) -> list[LibraryCandidate]:
        return query_repo.search_library_candidates(
            component, language=language, limit=max(limit, 20),
        )

    def get_library_dependents(
        self,
        query_repo: QueryRepository,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
        direct_only: bool = False,
    ) -> list[Dependent]:
        return query_repo.get_dependents(
            library_name, language=language, limit=limit,
            direct_only=direct_only,
        )


def _graph_path(
    data: Mapping[str, Any],
    repository_id: int,
    fetched: Mapping[int, str] | None = None,
    root: Path | None = None,
) -> str | None:
    """Where a record's graph is: the newest fetch the depgraph stage
    kept, else the record's own `depgraph_path`, else the legacy graph
    `data migrate-layout` moved under the repository's id. Shared by the
    ingest and `graphs_in`, which must read the same document.

    Only a file source reads it: `RawDocuments` finds the newest landed
    graph by the repository alone.
    """
    if fetched and repository_id in fetched:
        return fetched[repository_id]
    recorded = data.get('depgraph_path')
    if recorded:
        return str(recorded)
    if root is not None:
        legacy = (
            depgraph_store.repository_dir(root, repository_id)
            / depgraph_store.LEGACY / depgraph_store.DOCUMENT
        )
        if legacy.is_file():
            return str(legacy)
    return None


def _fetched_paths(root: Path | None) -> dict[int, str]:
    """repository id -> the newest graph the depgraph stage kept under
    `root`, from its `index.jsonl`; empty without one."""
    if root is None:
        return {}
    paths = depgraph_store.newest_paths(root)
    if paths:
        logger.info('Kept dependency-graph fetches', count=len(paths))
    return paths


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


def _github_language(repo: Repository) -> str:
    """GitHub's language, verbatim: the ledger's, else the record's."""
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
