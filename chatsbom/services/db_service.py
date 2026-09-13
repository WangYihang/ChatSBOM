import json
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.manifest import DirectDependencies
from chatsbom.core.manifest import resolve_relationships
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
from chatsbom.services.dependency_graph_service import load_artifacts

logger = structlog.get_logger('db_service')

BATCH_SIZE = 1000
DEFAULT_DATE = datetime(1970, 1, 2, tzinfo=timezone.utc)


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


def _naive(value: datetime | None) -> datetime:
    """ClickHouse DateTime columns take naive datetimes."""
    return (value or DEFAULT_DATE).replace(tzinfo=None)


class DbService:
    """Service for ingesting repository data and SBOMs into ClickHouse."""

    def __init__(self):
        self.config = get_config()

    # -- ingestion ----------------------------------------------------------

    def ingest_from_list(
        self,
        input_file: Path,
        repo_db: IngestionRepository,
        progress_callback: Callable[[], None] | None = None,
        limit: int | None = None,
    ) -> DbStats:
        """Process a JSONL list file and ingest repositories, releases, SBOMs."""
        stats = DbStats()

        if not input_file.exists():
            logger.warning(f"Input file not found: {input_file}")
            return stats

        repos = Batch(REPOSITORIES, repo_db)
        artifacts = Batch(ARTIFACTS, repo_db)
        releases = Batch(RELEASES, repo_db)

        for data in self._read_records(input_file, limit):
            try:
                repo = Repository.model_validate(data)
                direct_deps = self._direct_dependencies(repo)
                repo_row = self.parse_repository(repo, direct_deps)
                release_rows = self.parse_releases(repo)

                artifact_rows: list[dict[str, Any]] = []

                sbom_path = data.get('sbom_path')
                if sbom_path:
                    artifact_rows += self.parse_artifacts(
                        Path(sbom_path), repo.id, repo_row,
                        direct_deps=direct_deps,
                    )
                else:
                    stats.inc_skipped()

                # A second, independent source: GitHub's dependency graph
                # covers the Maven and Composer projects Syft cannot read.
                depgraph_path = data.get('depgraph_path')
                if depgraph_path:
                    artifact_rows += self.parse_dependency_graph(
                        Path(depgraph_path), repo.id, repo_row,
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
    def _direct_dependencies(repo: Repository) -> DirectDependencies | None:
        """Declared dependencies of a repo, or None when undeterminable.

        Needs both the downloaded content and a language we have a
        manifest parser for; without either, artifacts stay `unknown`.
        """
        if not repo.local_content_path or not repo.language:
            return None
        try:
            language = Language(repo.language.lower())
        except ValueError:
            return None
        try:
            return resolve_relationships(Path(repo.local_content_path), language)
        except ValueError:
            return None

    @staticmethod
    def _read_records(input_file: Path, limit: int | None) -> Iterator[dict]:
        with open(input_file, encoding='utf-8') as f:
            seen = 0
            for line in f:
                if not line.strip():
                    continue
                if limit is not None and seen >= limit:
                    return
                seen += 1
                yield json.loads(line)

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
            'created_at': _naive(repo.created_at),
            'language': repo.language or '',
            'topics': repo.topics,
            'default_branch': repo.default_branch,
            'sbom_ref': target.ref if target else '',
            'sbom_ref_type': target.ref_type if target else '',
            'sbom_commit_sha': target.commit_sha if target else '',
            'sbom_commit_sha_short': target.commit_sha_short if target else '',
            'has_releases': bool(repo.has_releases),
            'latest_release_tag': release.tag_name if release else '',
            'latest_release_published_at': _naive(
                release.published_at if release else None,
            ),
            'total_releases': repo.total_releases,
            'pushed_at': _naive(repo.pushed_at),
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
                'published_at': _naive(r.published_at),
                'target_commitish': r.target_commitish or '',
                'created_at': _naive(r.created_at),
                'release_assets': json.dumps(r.assets),
                'source': r.source,
            }
            for r in (repo.all_releases or [])
        ]

    def parse_artifacts(
        self,
        sbom_path: Path,
        repo_id: int,
        repo_row: Mapping[str, Any],
        direct_deps: DirectDependencies | None = None,
        observed_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Project a Syft SBOM into `artifacts` row mappings.

        SBOM provenance is carried over from the repository row by column
        name, so the artifact and its repository always agree on which
        commit was scanned.
        """
        if not sbom_path.exists():
            return []

        try:
            with open(sbom_path, encoding='utf-8') as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise ValueError(f"unreadable sbom {sbom_path}: {e}") from e

        sbom_ref = repo_row['sbom_ref']
        sbom_commit_sha = repo_row['sbom_commit_sha']
        seen_at = _naive(observed_at or datetime.now(timezone.utc))

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
        path: Path,
        repo_id: int,
        repo_row: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        """Project a stored GitHub dependency-graph document into rows.

        These land in the same table as Syft's output, distinguished by
        `source`. Their `relationship` is always `direct` — GitHub's graph
        is flat — and their versions are classified, since the graph
        reports manifest constraints rather than resolutions.
        """
        if not path.exists():
            return []

        return [
            {
                'repository_id': repo_id,
                'sbom_ref': repo_row['sbom_ref'],
                'sbom_commit_sha': repo_row['sbom_commit_sha'],
                'observed_at': _naive(datetime.now(timezone.utc)),
                **row,
            }
            for row in load_artifacts(path)
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
