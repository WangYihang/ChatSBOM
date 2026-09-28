import json
import shutil
import tempfile
from datetime import datetime
from datetime import timezone
from pathlib import Path

import git
import structlog

from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_text

logger = structlog.get_logger('git_service')

TAG_REF_PREFIX = 'refs/tags/'


class GitService:
    """Service for interacting with Git using GitPython, bypassing GitHub REST API limits."""

    def __init__(self, token: str | None = None):
        self.token = token
        self.g = git.cmd.Git()
        self.config = get_config()

    def get_repo_refs(self, owner: str, repo: str, cache_path: Path | None = None) -> tuple[dict[str, str], bool]:
        """
        Fetch all tags and branches for a repository, for resolving a ref.
        Returns (refs_dict, is_cached).

        Keyed by full ref name and by short name alike, so `main` is here
        without saying whether it is a branch or a tag. For the tags alone,
        use `get_repo_tags`.
        """
        if cache_path and cache_path.exists():
            try:
                with open(cache_path, encoding='utf-8') as f:
                    cache_data = json.load(f)

                updated_at = cache_data.get('updated_at')
                if updated_at:
                    updated_dt = datetime.fromisoformat(updated_at)
                    now = datetime.now(timezone.utc)
                    if (now - updated_dt).total_seconds() < self.config.github.cache_ttl:
                        return cache_data.get('data', {}), True
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
                    # Create structured cache data
                    cache_to_save = {
                        'url': url.replace(self.token + '@', '') if self.token else url,
                        'updated_at': datetime.now(timezone.utc).isoformat(),
                        'data': refs,
                    }
                    # Atomic, under a temporary name of its own: a fixed
                    # `index.tmp` was shared by concurrent writers, and a
                    # failed write left it behind for good.
                    atomic_write_text(
                        cache_path, json.dumps(cache_to_save, indent=2),
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to save refs cache',
                        path=str(cache_path), error=str(e),
                    )

            return refs, False

        except git.GitCommandError as e:
            logger.error(
                'Git ls-remote failed',
                url=self._mask_url(url),
                error=self._mask_url(str(e)),
            )
            return {}, False
        except Exception as e:
            logger.error(
                'Unexpected error in git ls-remote',
                url=self._mask_url(url),
                error=self._mask_url(str(e)),
            )
            return {}, False

    def get_repo_tags(self, owner: str, repo: str, cache_path: Path | None = None) -> tuple[dict[str, str], bool]:
        """
        Fetch a repository's tags, by tag name, each at the commit it points to.
        Returns (tags_dict, is_cached).

        Read from the full `refs/tags/*` names only. The short names
        beside them are shared with branches, and `HEAD` has no prefix
        at all: taken as tags, they made releases of branches, dated by
        their head commit and so newer than any real release.

        An annotated tag's own object is not a commit; `get_repo_refs`
        has already replaced it with the commit it peels to (`^{}`).
        """
        refs, is_cached = self.get_repo_refs(
            owner, repo, cache_path=cache_path,
        )
        tags = {
            ref.removeprefix(TAG_REF_PREFIX): sha
            for ref, sha in refs.items()
            if ref.startswith(TAG_REF_PREFIX)
        }
        return tags, is_cached

    def _get_short_name(self, ref_full: str) -> str | None:
        if ref_full.startswith('refs/tags/'):
            short = ref_full[10:]
        elif ref_full.startswith('refs/heads/'):
            short = ref_full[11:]
        else:
            return None
        # Short and full names share one dict, and git allows a branch
        # called `refs/tags/v1`: its short name would pose as that tag,
        # and, listed before it, take the real tag's place.
        if short.startswith('refs/'):
            return None
        return short

    def _mask_url(self, url: str) -> str:
        """Mask the token in a GitHub URL for safe logging."""
        if self.token and self.token in url:
            return url.replace(self.token, '*****')
        return url

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

    def get_repository_tree(self, owner: str, repo: str, sha: str, cache_path: Path | None = None) -> list[str] | None:
        """
        Fetch the full file tree for a specific commit SHA using git ls-tree.
        Avoids GitHub API rate limits.
        """
        if cache_path and cache_path.exists():
            try:
                with open(cache_path, encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
            except Exception as e:
                logger.debug(
                    'Failed to load tree cache',
                    path=str(cache_path), error=str(e),
                )

        repo_url = f"https://github.com/{owner}/{repo}.git"
        if self.token:
            repo_url = f"https://{self.token}@github.com/{owner}/{repo}.git"

        temp_dir = Path(tempfile.mkdtemp(prefix='chatsbom-tree-'))
        try:
            # 1. Blobless clone (metadata only, no file content)
            self.g.clone(
                '--filter=blob:none',
                '--no-checkout',
                '--depth', '1',
                repo_url,
                str(temp_dir),
            )

            repo_git = git.cmd.Git(str(temp_dir))
            repo_git.fetch('origin', sha, '--depth=1')

            # 3. List tree recursively
            # -r: recurse
            # --name-only: filenames only
            # --full-tree: path relative to root
            output = repo_git.ls_tree('-r', '--name-only', '--full-tree', sha)

            files = [
                line.strip()
                for line in output.splitlines()
                if line.strip()
            ]

            if cache_path and files:
                try:
                    atomic_write_text(
                        cache_path,
                        ''.join(f"{file_path}\n" for file_path in files),
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to save tree cache',
                        path=str(cache_path), error=str(e),
                    )

            return files

        except git.GitCommandError as e:
            logger.error(
                'Git tree fetch failed',
                repo=f"{owner}/{repo}", sha=sha,
                error=self._mask_url(str(e)),
            )
            return None
        except Exception as e:
            logger.error(
                'Unexpected error in git tree fetch',
                repo=f"{owner}/{repo}", sha=sha,
                error=str(e),
            )
            return None
        finally:
            # Cleanup
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
