import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.repository import IngestionRepository
from chatsbom.models.repository import Repository

logger = structlog.get_logger('indexer_service')

REPO_COLUMNS = [
    'id', 'owner', 'repo', 'full_name', 'url', 'stars', 'description', 'created_at', 'language', 'topics',
    'default_branch', 'sbom_ref', 'sbom_ref_type', 'sbom_commit_sha', 'sbom_commit_sha_short',
    'has_releases', 'latest_release_tag', 'latest_release_published_at',
]
ARTIFACT_COLUMNS = [
    'repository_id', 'artifact_id', 'name', 'version', 'type', 'purl', 'found_by', 'licenses',
    'sbom_ref', 'sbom_commit_sha',
]
RELEASE_COLUMNS = [
    'repository_id', 'release_id', 'tag_name', 'name', 'is_prerelease', 'is_draft', 'published_at',
    'target_commitish', 'created_at',
]
BATCH_SIZE = 1000

DEFAULT_DATE = datetime(1970, 1, 2, tzinfo=timezone.utc)


@dataclass
class ParsedRepository:
    repo_row: list[Any]
    meta_context: dict[str, Any]
    release_rows: list[list[Any]]


class IndexerService:
    """Service for parsing repository data and SBOM files for database ingestion."""

    def process_file(self, input_file: Path, repo: IngestionRepository, progress_callback=None) -> dict:
        """Process a single JSONL file and ingest into database."""
        stats = {'repos': 0, 'artifacts': 0}
        repo_batch, artifact_batch, release_batch = [], [], []

        for line in open(input_file):
            if not line.strip():
                continue

            res = self.parse_repository_line(line, input_file.stem)
            if not res:
                if progress_callback:
                    progress_callback()
                continue

            repo_batch.append(res.repo_row)
            release_batch.extend(res.release_rows)
            stats['repos'] += 1

            artifacts = self.scan_for_artifacts(res.meta_context)
            artifact_batch.extend(artifacts)
            stats['artifacts'] += len(artifacts)

            if len(repo_batch) >= BATCH_SIZE:
                repo.insert_batch('repositories', repo_batch, REPO_COLUMNS)
                repo_batch = []
            if len(artifact_batch) >= BATCH_SIZE:
                repo.insert_batch(
                    'artifacts', artifact_batch, ARTIFACT_COLUMNS,
                )
                artifact_batch = []
            if len(release_batch) >= BATCH_SIZE:
                repo.insert_batch('releases', release_batch, RELEASE_COLUMNS)
                release_batch = []

            if progress_callback:
                progress_callback()

        if repo_batch:
            repo.insert_batch('repositories', repo_batch, REPO_COLUMNS)
        if artifact_batch:
            repo.insert_batch('artifacts', artifact_batch, ARTIFACT_COLUMNS)
        if release_batch:
            repo.insert_batch('releases', release_batch, RELEASE_COLUMNS)

        return stats

    def parse_repository_line(self, line: str, language: str) -> ParsedRepository | None:
        """Parses a single JSONL line into database-ready rows using Pydantic."""
        try:
            repo = Repository.model_validate_json(line)
        except Exception:
            return None

        owner, repo_name = repo.full_name.split(
            '/',
        ) if '/' in repo.full_name else ('', repo.full_name)
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

        repo_row = [
            repo.id, owner, repo_name, repo.full_name, repo.url,
            repo.stars, repo.description or '',
            created_at_naive, language, repo.topics,
            repo.default_branch, dt.ref if dt else '',
            dt.ref_type if dt else '', dt.commit_sha if dt else '',
            dt.commit_sha_short if dt else '', repo.has_releases,
            latest_tag, latest_published_naive,
        ]

        meta_context = {
            'id': repo.id, 'owner': owner,
            'repo': repo_name, 'language': language,
        }

        release_rows = []
        for r in repo.all_releases:
            published = (r.published_at or DEFAULT_DATE).replace(tzinfo=None)
            created = (r.created_at or DEFAULT_DATE).replace(tzinfo=None)

            release_rows.append([
                repo.id, r.id, r.tag_name, r.name or '',
                r.is_prerelease, r.is_draft,
                published, r.target_commitish or '', created,
            ])

        return ParsedRepository(repo_row, meta_context, release_rows)

    def scan_for_artifacts(self, meta_context: dict[str, Any], base_dir: str = 'data/sbom') -> list[list[Any]]:
        """Scans for SBOM files and extracts artifact rows."""
        language, owner, repo_name, repo_id = meta_context['language'], meta_context[
            'owner'
        ], meta_context['repo'], meta_context['id']
        repo_dir = Path(base_dir) / language / owner / repo_name
        if not repo_dir.exists():
            return []

        artifact_rows = []
        for sbom_file in repo_dir.rglob('sbom.json'):
            try:
                parts = sbom_file.parts
                try:
                    repo_idx = parts.index(repo_name)
                    if len(parts) > repo_idx + 2:
                        sbom_ref = parts[repo_idx + 1]
                        sbom_commit_sha = parts[repo_idx + 2]
                    else:
                        continue
                except ValueError:
                    continue

                with open(sbom_file) as f:
                    data = json.load(f)
                    for art_data in data.get('artifacts', []):
                        licenses = []
                        raw_licenses = art_data.get('licenses', [])
                        if raw_licenses:
                            for license_item in raw_licenses:
                                if isinstance(license_item, dict):
                                    v = license_item.get('value') or license_item.get(
                                        'spdxExpression',
                                    ) or license_item.get('name')
                                    if v:
                                        licenses.append(v)
                                elif isinstance(license_item, str):
                                    licenses.append(license_item)

                        artifact_rows.append([
                            repo_id, art_data.get(
                                'id', '',
                            ), art_data.get('name', ''),
                            art_data.get('version', ''), art_data.get(
                                'type', '',
                            ),
                            art_data.get('purl', ''), art_data.get(
                                'foundBy', '',
                            ),
                            licenses, sbom_ref, sbom_commit_sha,
                        ])
            except (ValueError, IndexError, json.JSONDecodeError):
                continue
        return artifact_rows
