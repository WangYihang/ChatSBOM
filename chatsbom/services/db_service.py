import json
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import Document
from chatsbom.core.documents import DocumentSource
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import ManifestSource
from chatsbom.core.documents import RecordSource
from chatsbom.core.documents import SYFT as SYFT_KIND
from chatsbom.core.instants import utc
from chatsbom.core.manifest import DirectDependencies
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import UNKNOWN
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
from chatsbom.models.provenance import SYFT
from chatsbom.models.query import DatabaseStats
from chatsbom.models.query import Dependent
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
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
    @staticmethod
    def scans_in(
        records: RecordSource,
        language: str,
        limit: int | None = None,
    ) -> list[tuple[int, str]]:
        """The `(repository_id, sbom_commit_sha)` pairs about to be written.

        Read from the same source the ingest will read from, ahead of
        it, so the rows for those exact scans can be dropped first —
        see `IngestionRepository.forget_scans`. Without that, a
        re-ingest appends instead of refreshing: measured once,
        `db index --language python` added 687,000 duplicate rows.

        A record with no commit sha is skipped rather than deleted under
        the empty string: that would match every row whose scan is
        unknown, across every repository.
        """
        scans: list[tuple[int, str]] = []
        for data in records.records(language, limit):
            repository_id = data.get('id')
            target = data.get('download_target') or {}
            sha = target.get('commit_sha') if isinstance(
                target, dict,
            ) else None
            if isinstance(repository_id, int) and isinstance(sha, str) and sha:
                scans.append((repository_id, sha))
        return scans

    def ingest_from_list(
        self,
        records: RecordSource,
        repo_db: IngestionRepository,
        language: str,
        progress_callback: Callable[[], None] | None = None,
        limit: int | None = None,
        depgraph_index: Path | None = None,
        documents: DocumentSource = FILES,
        manifests: ManifestSource = FILE_MANIFESTS,
    ) -> DbStats:
        """Ingest repositories, releases and SBOMs for one language.

        Three sources, and each can be a ledger on disk or the
        `raw_documents` table:

        - `records` decides *which* repositories are ingested, and
          supplies their metadata, releases and download target. It was
          a `Path` to the SBOM ledger, which is why `data/` stayed
          load-bearing after the documents moved.
        - `documents` supplies the SBOMs and dependency graphs.
        - `manifests` supplies the declared sets behind every
          direct/transitive verdict.

        `depgraph_index` remains a path because it only names *extra*
        documents for repositories the graph happens to cover. It was
        once used as the input list on the assumption it was a superset,
        and `github depgraph --limit 120` turned it into a subset that
        silently cut Java from 1,215 indexed repositories to 87.
        """
        stats = DbStats()

        depgraphs = self._depgraph_paths(depgraph_index)

        repos = Batch(REPOSITORIES, repo_db)
        artifacts = Batch(ARTIFACTS, repo_db)
        releases = Batch(RELEASES, repo_db)

        for data in records.records(language, limit):
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
                direct_deps = self._direct_dependencies(
                    repo, manifests,
                )
                repo_row = self.parse_repository(repo, direct_deps)
                release_rows = self.parse_releases(repo)

                artifact_rows: list[dict[str, Any]] = []

                sbom = documents.get(
                    SYFT_KIND, repo.id, data.get('sbom_path'),
                )
                if sbom is not None:
                    artifact_rows += self.parse_artifacts(
                        sbom, repo.id, repo_row, direct_deps=direct_deps,
                    )
                else:
                    stats.inc_skipped()

                # A second, independent source: GitHub's dependency graph
                # covers the Maven and Composer projects Syft cannot read.
                graph = documents.get(
                    DEPGRAPH,
                    repo.id,
                    data.get('depgraph_path') or depgraphs.get(repo.id),
                )
                if graph is not None:
                    artifact_rows += self.parse_dependency_graph(
                        graph, repo.id, repo_row,
                    )

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
    def _depgraph_paths(index: Path | None) -> dict[int, str]:
        """repository id -> stored dependency-graph document.

        Absent or partial is normal: the graph is collected separately and
        covers whatever it has reached.
        """
        if index is None or not index.exists():
            return {}

        paths: dict[int, str] = {}
        with open(index, encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    path = record.get('depgraph_path')
                    if path:
                        paths[int(record['id'])] = str(path)
                except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                    continue

        if paths:
            logger.info('Dependency graphs available', count=len(paths))
        return paths

    @staticmethod
    def _direct_dependencies(
        repo: Repository,
        manifests: ManifestSource = FILE_MANIFESTS,
    ) -> DirectDependencies | None:
        """Declared dependencies of a repo, or None when undeterminable.

        Needs a language we have a manifest parser for, and manifests to
        read; without either, artifacts stay `unknown` rather than being
        guessed at.

        `manifests` decides where they are read from. The judgement is
        the same either way -- `relationships_from` owns it -- which is
        what lets `--from-raw` reproduce the direct/transitive verdicts
        without the 9.8 GiB of files.

        The commit is the one the artifacts are stamped with, so the
        verdicts describe the scan they are stored under: the landing
        zone keeps every commit's manifests, and reading them all let a
        package an old commit declared be `direct` in this one.
        """
        if not repo.language:
            return None
        try:
            language = Language(repo.language.lower())
        except ValueError:
            return None
        target = repo.download_target
        read = manifests.for_repository(
            repo.id,
            repo.local_content_path,
            commit_sha=target.commit_sha if target else None,
        )
        if not read:
            return None
        try:
            return relationships_from(read, language)
        except ValueError:
            return None

    # -- parsing ------------------------------------------------------------

    def parse_repository(
        self,
        repo: Repository,
        direct_deps: DirectDependencies | None = None,
    ) -> dict[str, Any]:
        """Project a Repository into a `repositories` row mapping.

        `direct_deps` carries which manifests were read, which is the
        audit trail behind every direct/transitive verdict: without it a
        `transitive` label is indistinguishable from `unknown`.
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
            'manifest_sources': list(direct_deps.sources) if direct_deps else [],
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
        direct_deps: DirectDependencies | None = None,
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
                'relationship': (
                    direct_deps.relationship_of(art.get('name') or '')
                    if direct_deps else UNKNOWN
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
        `source`. Their `relationship` is always `direct` — GitHub's graph
        is flat — and their versions are classified, since the graph
        reports manifest constraints rather than resolutions.
        """
        return [
            {
                'repository_id': repo_id,
                'sbom_ref': repo_row['sbom_ref'],
                'sbom_commit_sha': repo_row['sbom_commit_sha'],
                'observed_at': document.observed_at,
                **row,
            }
            for row in parse_spdx_document(document.body)
        ]

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
