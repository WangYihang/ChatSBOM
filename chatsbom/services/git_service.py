import json
import time
from pathlib import Path

import git
import structlog

logger = structlog.get_logger('git_service')


class GitService:
    """Service for interacting with Git using GitPython, bypassing GitHub REST API limits."""

    def __init__(self, token: str | None = None):
        self.token = token
        self.g = git.cmd.Git()

    def get_repo_refs(self, owner: str, repo: str, cache_path: Path | None = None) -> tuple[dict[str, str], bool]:
        """
        Fetch all tags and branches for a repository.
        Returns (refs_dict, is_cached).
        """
        if cache_path and cache_path.exists():
            try:
                # Cache valid for 24 hours
                if time.time() - cache_path.stat().st_mtime < 86400:
                    with open(cache_path, encoding='utf-8') as f:
                        return json.load(f), True
            except Exception as e:
                logger.debug(
                    'Failed to load refs cache',
                    path=str(cache_path), error=str(e),
                )

        url = f"https://github.com/{owner}/{repo}.git"
        if self.token:
            url = f"https://{self.token}@github.com/{owner}/{repo}.git"

        try:
            # Use a 30s timeout to prevent hanging on network issues
            output = self.g.ls_remote(url, kill_after_timeout=30)

            refs = {}
            for line in output.splitlines():
                if not line.strip():
                    continue
                parts = line.split(None, 1)
                if len(parts) < 2:
                    continue
                sha, ref_full = parts

                # Logic: Annotated tags (refs/tags/v1^{}) take precedence over the tag itself
                if ref_full.endswith('^{}'):
                    base_ref = ref_full[:-3]
                    refs[base_ref] = sha
                    short = self._get_short_name(base_ref)
                    if short:
                        refs[short] = sha
                else:
                    if ref_full not in refs:
                        refs[ref_full] = sha
                        short = self._get_short_name(ref_full)
                        if short:
                            refs[short] = sha

            if cache_path and refs:
                try:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    # Temporary file + rename for atomic write
                    temp_cache = cache_path.with_suffix('.tmp')
                    with open(temp_cache, 'w', encoding='utf-8') as f:
                        json.dump(refs, f, indent=2)
                    temp_cache.replace(cache_path)
                except Exception as e:
                    logger.warning(
                        'Failed to save refs cache',
                        path=str(cache_path), error=str(e),
                    )

            return refs, False

        except git.GitCommandError as e:
            logger.error('Git ls-remote failed', url=url, error=str(e))
            return {}, False
        except Exception as e:
            logger.error(
                'Unexpected error in git ls-remote',
                url=url, error=str(e),
            )
            return {}, False

    def _get_short_name(self, ref_full: str) -> str | None:
        if ref_full.startswith('refs/tags/'):
            return ref_full[10:]
        if ref_full.startswith('refs/heads/'):
            return ref_full[11:]
        return None

    def resolve_ref(self, owner: str, repo: str, ref: str, cache_path: Path | None = None) -> tuple[str | None, bool, int]:
        """Resolve a specific ref to a SHA. Returns (sha, is_cached, num_refs)."""
        refs, is_cached = self.get_repo_refs(
            owner, repo, cache_path=cache_path,
        )

        num_refs = len(refs)

        # Priority: Exact match -> with refs/tags/ -> with refs/heads/
        if ref in refs:
            return refs[ref], is_cached, num_refs

        for prefix in ['refs/tags/', 'refs/heads/']:
            if (prefix + ref) in refs:
                return refs[prefix + ref], is_cached, num_refs

        return None, is_cached, num_refs
