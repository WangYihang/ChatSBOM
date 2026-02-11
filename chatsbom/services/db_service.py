import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.stats import BaseStats
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.repository import Repository

logger = structlog.get_logger('db_service')

REPO_COLUMNS = [
    'id', 'owner', 'repo', 'url', 'stars', 'description', 'created_at', 'language', 'topics',
    'default_branch', 'sbom_ref', 'sbom_ref_type', 'sbom_commit_sha', 'sbom_commit_sha_short',
    'has_releases', 'latest_release_tag', 'latest_release_published_at',
    'total_releases', 'pushed_at', 'is_archived', 'is_fork', 'is_template', 'is_mirror',
    'disk_usage', 'fork_count', 'watchers_count', 'license_spdx_id', 'license_name',
]
ARTIFACT_COLUMNS = [
    'repository_id', 'artifact_id', 'name', 'version', 'type', 'purl', 'found_by', 'licenses',
    'sbom_ref', 'sbom_commit_sha',
]
RELEASE_COLUMNS = [
    'repository_id', 'release_id', 'tag_name', 'name', 'is_prerelease', 'is_draft', 'published_at',
    'target_commitish', 'created_at', 'release_assets', 'source',
]
BATCH_SIZE = 1000
DEFAULT_DATE = datetime(1970, 1, 2, tzinfo=timezone.utc)


@dataclass
class DbStats(BaseStats):
    repos: int = 0
    artifacts: int = 0
    releases: int = 0


@dataclass
class ParsedRepository:
    repo_row: list[Any]
    meta_context: dict[str, Any]
    release_rows: list[list[Any]]


class DbService:
    """Service for ingesting repository data and SBOMs into ClickHouse."""

    def __init__(self):
        self.config = get_config()

    def ingest_from_list(self, input_file: Path, repo_db: IngestionRepository, progress_callback=None) -> DbStats:
        """Process a JSONL list file and ingest data."""
        stats = DbStats()
        repo_batch, artifact_batch, release_batch = [], [], []

        if not input_file.exists():
            logger.warning(f"Input file not found: {input_file}")
            return stats

        with open(input_file, encoding='utf-8') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    # Validate Repo Model
                    repo_model = Repository.model_validate(data)

                    # Parse Repo & Releases
                    res = self._parse_repository(repo_model)
                    repo_batch.append(res.repo_row)
                    release_batch.extend(res.release_rows)
                    stats.repos += 1
                    stats.releases += len(res.release_rows)

                    # Parse Artifacts from SBOM file if present
                    sbom_path = data.get('sbom_path')
                    if sbom_path:
                        artifacts = self._parse_artifacts(
                            Path(sbom_path), repo_model.id, res.repo_row,
                        )
                        artifact_batch.extend(artifacts)
                        stats.artifacts += len(artifacts)
                    else:
                        stats.inc_skipped()
                    stats.artifacts += len(artifacts)

                    # Batch Insert
                    if len(repo_batch) >= BATCH_SIZE:
                        t0 = datetime.now()
                        repo_db.insert_batch(
                            'repositories', repo_batch, REPO_COLUMNS,
                        )
                        logger.info(
                            'Batch Inserted', table='repositories',
                            count=len(repo_batch), elapsed=f"{(datetime.now() - t0).total_seconds():.3f}s",
                        )
                        repo_batch = []
                    if len(artifact_batch) >= BATCH_SIZE:
                        t0 = datetime.now()
                        repo_db.insert_batch(
                            'artifacts', artifact_batch, ARTIFACT_COLUMNS,
                        )
                        logger.info(
                            'Batch Inserted', table='artifacts',
                            count=len(artifact_batch), elapsed=f"{(datetime.now() - t0).total_seconds():.3f}s",
                        )
                        artifact_batch = []
                    if len(release_batch) >= BATCH_SIZE:
                        t0 = datetime.now()
                        repo_db.insert_batch(
                            'releases', release_batch, RELEASE_COLUMNS,
                        )
                        logger.info(
                            'Batch Inserted', table='releases',
                            count=len(release_batch), elapsed=f"{(datetime.now() - t0).total_seconds():.3f}s",
                        )
                        release_batch = []

                    if progress_callback:
                        progress_callback()

                except Exception as e:
                    logger.error(f"Failed to process line: {e}")
                    stats.inc_failed()

        # Flush remaining
        if repo_batch:
            repo_db.insert_batch('repositories', repo_batch, REPO_COLUMNS)
        if artifact_batch:
            repo_db.insert_batch('artifacts', artifact_batch, ARTIFACT_COLUMNS)
        if release_batch:
            repo_db.insert_batch('releases', release_batch, RELEASE_COLUMNS)

        return stats

    def _parse_repository(self, repo: Repository) -> ParsedRepository:
        dt = repo.download_target

        latest_tag = ''
        latest_published = DEFAULT_DATE
        if repo.latest_stable_release:
            latest_tag = repo.latest_stable_release.tag_name
            latest_published = repo.latest_stable_release.published_at or DEFAULT_DATE

        created_at_naive = (
            repo.created_at or DEFAULT_DATE
        ).replace(tzinfo=None)
        latest_published_naive = latest_published.replace(tzinfo=None)
        pushed_at_naive = (repo.pushed_at or DEFAULT_DATE).replace(tzinfo=None)

        repo_row = [
            repo.id, repo.owner, repo.repo, repo.url,
            repo.stars, repo.description or '',
            created_at_naive, repo.language or '', repo.topics,
            repo.default_branch, dt.ref if dt else '',
            dt.ref_type if dt else '', dt.commit_sha if dt else '',
            dt.commit_sha_short if dt else '',
            repo.has_releases if repo.has_releases is not None else False,
            latest_tag, latest_published_naive,
            repo.total_releases, pushed_at_naive,
            repo.is_archived, repo.is_fork, repo.is_template, repo.is_mirror,
            repo.disk_usage, repo.fork_count, repo.watchers_count,
            repo.license_spdx_id or '', repo.license_name or '',
        ]

        release_rows = []
        if repo.all_releases:
            for r in repo.all_releases:
                published = (r.published_at or DEFAULT_DATE).replace(
                    tzinfo=None,
                )
                created = (r.created_at or DEFAULT_DATE).replace(tzinfo=None)
                release_rows.append([
                    repo.id, r.id, r.tag_name, r.name or '',
                    r.is_prerelease, r.is_draft,
                    published, r.target_commitish or '', created,
                    json.dumps(r.assets),
                    r.source,
                ])

        return ParsedRepository(repo_row, {}, release_rows)

    def _parse_artifacts(self, sbom_path: Path, repo_id: int, repo_row: list) -> list[list[Any]]:
        if not sbom_path.exists():
            return []

        artifact_rows = []
        # sbom_ref, sbom_commit_sha are at index 11 and 13 in repo_row
        sbom_ref = repo_row[11]
        sbom_commit_sha = repo_row[13]

        try:
            with open(sbom_path, encoding='utf-8') as f:
                data = json.load(f)
                for art in data.get('artifacts', []):
                    licenses = []
                    for lic in art.get('licenses', []):
                        if isinstance(lic, dict):
                            val = lic.get('value') or lic.get(
                                'spdxExpression',
                            ) or lic.get('name')
                            if val:
                                licenses.append(val)
                        elif isinstance(lic, str):
                            licenses.append(lic)

                    artifact_rows.append([
                        repo_id,
                        art.get('id', ''),
                        art.get('name', ''),
                        art.get('version', ''),
                        art.get('type', ''),
                        art.get('purl', ''),
                        art.get('foundBy', ''),
                        licenses,
                        sbom_ref,
                        sbom_commit_sha,
                    ])
        except Exception:
            pass

        return artifact_rows

    def get_db_stats(self, query_repo: QueryRepository) -> dict[str, int]:
        return query_repo.get_stats()

    def get_language_stats(self, query_repo: QueryRepository) -> list[tuple[str, int]]:
        return list(query_repo.get_language_stats())

    def get_framework_stats(self, query_repo: QueryRepository) -> list[dict[str, Any]]:
        results = []
        for lang in Language:
            try:
                handler = LanguageFactory.get_handler(lang)
            except ValueError:
                continue

            frameworks = handler.get_frameworks()
            if not frameworks:
                continue

            lang_frameworks = []
            for fw in frameworks:
                fw_handler = FrameworkFactory.create(fw)
                packages = fw_handler.get_package_names()
                count = query_repo.get_framework_usage(str(lang), packages)
                samples = query_repo.get_top_projects_by_framework(
                    str(lang), packages, limit=3,
                )
                lang_frameworks.append({
                    'framework': str(fw),
                    'count': count,
                    'samples': samples,
                })

            results.append({
                'language': lang.value,
                'frameworks': lang_frameworks,
            })
        return results

    def search_library(self, query_repo: QueryRepository, component: str, language: str | None = None, limit: int = 10):
        candidate_limit = max(limit, 20)
        candidates = query_repo.search_library_candidates(
            component, language=language, limit=candidate_limit,
        )
        return candidates

    def get_library_dependents(self, query_repo: QueryRepository, library_name: str, language: str | None = None, limit: int = 50):
        return query_repo.get_dependents(library_name, language=language, limit=limit)
