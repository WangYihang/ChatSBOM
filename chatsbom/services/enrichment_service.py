import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('enrichment_service')


@dataclass
class EnrichStats:
    total: int = 0
    enriched: int = 0
    skipped: int = 0
    failed: int = 0
    api_requests: int = 0
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)


class EnrichmentService:
    """Service for enriching repository metadata."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def enrich_item(self, repo: Repository, stats: EnrichStats) -> Repository:
        """Enrich a repository item with release, commit, and general metadata."""
        owner, repo_name = repo.full_name.split('/')
        default_branch = repo.default_branch

        logger.info(
            'Enriching repository', repo=repo.full_name,
            default_branch=default_branch,
        )

        # 1. Fetch detailed metadata
        metadata = self.service.get_repository_metadata(owner, repo_name)
        if metadata:
            repo.stars = metadata.get('stargazers_count', repo.stars)
            repo.language = metadata.get('language')
            repo.description = metadata.get('description')
            repo.topics = metadata.get('topics', [])
            if metadata.get('license'):
                repo.license_spdx_id = metadata.get(
                    'license', {},
                ).get('spdx_id')
                repo.license_name = metadata.get('license', {}).get('name')
            stats.api_requests += 1

        # 2. Fetch releases
        releases_data = self.service.get_repository_releases(owner, repo_name)
        stats.api_requests += len(releases_data) // 100 + 1

        releases = [GitHubRelease.model_validate(r) for r in releases_data]

        repo.has_releases = len(releases) > 0
        repo.all_releases = releases[:10]

        latest_stable = None
        for r in releases:
            if not r.is_prerelease and not r.is_draft:
                latest_stable = r
                break

        repo.latest_stable_release = latest_stable
        if latest_stable:
            logger.info(
                'Found stable release', repo=repo.full_name,
                tag=latest_stable.tag_name,
            )

        # 3. Determine download target
        ref = latest_stable.tag_name if latest_stable else default_branch
        ref_type = 'release' if latest_stable else 'branch'

        commit_info = self.service.get_commit_metadata(owner, repo_name, ref)
        stats.api_requests += 1

        if not commit_info and latest_stable:
            ref = default_branch
            ref_type = 'branch'
            commit_info = self.service.get_commit_metadata(
                owner, repo_name, ref,
            )
            stats.api_requests += 1

        if commit_info:
            repo.download_target = DownloadTarget(
                ref=ref,
                ref_type=ref_type,
                commit_sha=commit_info['sha'],
                commit_sha_short=commit_info['sha_short'],
            )

        return repo

    def process_repo(self, repo: Repository, stats: EnrichStats, language: str, force: bool = False) -> dict | None:
        """Process a single repository."""
        if repo.download_target is not None and not force:
            stats.skipped += 1
            return None

        try:
            enriched_repo = self.enrich_item(repo, stats)
            stats.enriched += 1
            self.save_enrichment_metadata(enriched_repo, language)
            return enriched_repo.model_dump(mode='json')
        except Exception as e:
            logger.error(
                f"Failed to enrich {repo.full_name}: {e}",
            )
            stats.failed += 1
            return None

    def save_enrichment_metadata(self, repo: Repository, language: str):
        """Save enrichment metadata to filesystem for future reference."""
        owner, repo_name = repo.full_name.split('/')

        if not repo.download_target:
            return

        ref = repo.download_target.ref
        commit_sha = repo.download_target.commit_sha

        enrich_dir = Path('data/sbom') / language / owner / \
            repo_name / ref / commit_sha
        enrich_dir.mkdir(parents=True, exist_ok=True)

        with open(enrich_dir / 'enrichment.json', 'w', encoding='utf-8') as f:
            f.write(repo.model_dump_json(indent=2))
