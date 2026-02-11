import json
import re
import shutil
from collections import defaultdict
from pathlib import Path

import humanize
import structlog
import yaml
from git import Repo

from chatsbom.core.config import get_config
from chatsbom.core.logging import console
from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory

logger = structlog.get_logger('openapi_service')

OPENAPI_FILENAMES = {
    'openapi.yaml', 'openapi.yml', 'openapi.json',
    'swagger.yaml', 'swagger.yml', 'swagger.json',
}


class OpenApiService:
    def __init__(self):
        self.config = get_config()

    def normalize_path(self, path: str) -> str:
        """
        Standardize API paths for matching.
        - Lowercase
        - Remove trailing slash
        - Replace parameters (:id, {id}, <id>) with {}
        """
        if not path:
            return '/'

        # Remove query string if any
        path = path.split('?')[0]

        path = path.lower().strip()
        if path.endswith('/') and path != '/':
            path = path[:-1]

        # Standardize parameters: {id}, :id, <id>, <int:id> -> {}
        path = re.sub(r'\{[^}]+\}', '{}', path)
        path = re.sub(r':[a-zA-Z0-9_]+', '{}', path)
        path = re.sub(r'<[^>]+>', '{}', path)

        if not path.startswith('/'):
            path = '/' + path

        return path

    def parse_openapi_spec(self, content: str, is_yaml: bool = True) -> set[tuple[str, str]]:
        """
        Parse OpenAPI/Swagger spec and extract normalized (method, path) pairs.
        """
        try:
            if is_yaml:
                spec = yaml.safe_load(content)
            else:
                spec = json.loads(content)

            if not spec or not isinstance(spec, dict):
                return set()

            endpoints = set()
            paths = spec.get('paths', {})
            if not isinstance(paths, dict):
                return set()

            for path, methods in paths.items():
                if not isinstance(methods, dict):
                    continue

                norm_path = self.normalize_path(path)
                for method in methods.keys():
                    # Standard HTTP methods only
                    if method.lower() in {'get', 'post', 'put', 'delete', 'patch', 'options', 'head'}:
                        endpoints.add((method.upper(), norm_path))

            return endpoints
        except Exception as e:
            logger.debug('Failed to parse OpenAPI spec', error=str(e))
            return set()

    def find_openapi_files(self, files: list[str]) -> list[str]:
        """
        Check if any files in the tree match OpenAPI filename patterns.
        Returns list of matching file paths.
        """
        matches = []
        for filepath in files:
            basename = filepath.rsplit('/', 1)[-1].lower()
            if basename in OPENAPI_FILENAMES:
                matches.append(filepath)
        return matches

    def get_dir_size(self, path: Path) -> int:
        """Return total size in bytes of a directory tree."""
        total = 0
        try:
            for entry in path.rglob('*'):
                if entry.is_file():
                    total += entry.stat().st_size
        except OSError:
            pass
        return total

    def format_size(self, size_bytes: int) -> str:
        """Format bytes into a human-readable string."""
        return humanize.naturalsize(size_bytes)

    def get_version_path(self, tag: str | None, commit_sha: str | None) -> Path:
        """Build a two-level version path (<ref>/<sha>) matching content_service pattern."""
        ref = tag.strip() if tag else 'HEAD'
        sha = commit_sha.strip() if commit_sha else 'HEAD'
        return Path(ref) / sha

    def find_candidates(self, client) -> list[list[str]]:
        results = []
        for framework_enum in Framework:
            try:
                framework = FrameworkFactory.create(framework_enum)
                package_names = framework.get_package_names()
                if not package_names:
                    continue

                packages_str = "', '".join(package_names)
                query = f"""
                SELECT DISTINCT
                    r.language,
                    '{framework_enum.value}' as framework,
                    a.version as framework_version,
                    r.owner,
                    r.repo,
                    r.stars,
                    r.default_branch,
                    r.latest_release_tag,
                    r.sbom_commit_sha
                FROM artifacts a
                JOIN repositories r ON a.repository_id = r.id
                WHERE a.name IN ('{packages_str}')
                """
                data = client.query(query).result_rows

                framework_total = 0
                framework_matched = 0

                for row in data:
                    language, framework_name, framework_version, owner, repo, stars, default_branch, latest_release, commit_sha = row
                    language = str(language).lower() if language else ''
                    owner = str(owner).lower()
                    repo = str(repo).lower()
                    default_branch = str(default_branch).lower()
                    latest_release = str(
                        latest_release,
                    ).lower() if latest_release else ''
                    commit_sha = str(commit_sha).lower() if commit_sha else ''

                    framework_total += 1
                    ref = latest_release if latest_release else default_branch
                    tree_file = self.config.paths.get_tree_file_path(
                        language, owner, repo, ref, commit_sha,
                    )

                    if not tree_file.exists():
                        continue

                    try:
                        with open(tree_file, encoding='utf-8') as f:
                            tree_files = [
                                line.strip()
                                for line in f if line.strip()
                            ]
                    except OSError:
                        continue

                    openapi_files = self.find_openapi_files(tree_files)
                    if not openapi_files:
                        continue

                    framework_matched += 1
                    url = f'https://github.com/{owner}/{repo}/releases/tag/{latest_release}' if latest_release else \
                        (f'https://github.com/{owner}/{repo}/tree/{commit_sha}' if commit_sha else f'https://github.com/{owner}/{repo}')

                    sha_or_ref = commit_sha if commit_sha else ref
                    for openapi_file in openapi_files:
                        openapi_url = f'https://github.com/{owner}/{repo}/blob/{sha_or_ref}/{openapi_file}'
                        results.append([
                            language, str(framework_name).lower(), str(
                                framework_version,
                            ).lower(),
                            owner, repo, str(
                                stars,
                            ), default_branch, latest_release, commit_sha,
                            url, openapi_file, openapi_url,
                        ])

                console.print(
                    f'[bold]{framework_enum.value}[/bold]: [cyan]{framework_matched}[/cyan]/{framework_total} projects have OpenAPI specs',
                )

            except Exception as e:
                console.print(
                    f'[bold red]Error querying for {framework_enum.value}: {e}[/bold red]',
                )

        return results

    def clone_repo(self, owner: str, repo: str, dest: Path, tag: str | None = None, commit_sha: str | None = None) -> tuple[str, str, bool, str, str]:
        ver = self.get_version_path(tag, commit_sha)
        repo_dir = dest / owner / repo / ver
        if repo_dir.exists():
            return (owner, repo, True, 'already exists', self.format_size(self.get_dir_size(repo_dir)))

        repo_dir.parent.mkdir(parents=True, exist_ok=True)
        clone_url = f'https://github.com/{owner}/{repo}.git'
        try:
            Repo.clone_from(clone_url, str(repo_dir))
            return (owner, repo, True, 'cloned', self.format_size(self.get_dir_size(repo_dir)))
        except Exception as e:
            if repo_dir.exists():
                shutil.rmtree(str(repo_dir), ignore_errors=True)
            return (owner, repo, False, str(e), '')

    def analyze_drift(self, candidates, client, code_dir: Path, repo_base: Path) -> list[dict]:
        projects = defaultdict(list)
        for c in candidates:
            projects[(c['owner'], c['repo'])].append(c)

        drift_results = []
        for (owner, repo_name), group in projects.items():
            console.print(
                f"Analyzing data for [bold cyan]{owner}/{repo_name}[/bold cyan]...",
            )
            query = f"""
            SELECT DISTINCT tag_name, published_at, target_commitish
            FROM releases r
            JOIN repositories repo ON r.repository_id = repo.id
            WHERE repo.owner = '{owner}' AND repo.repo = '{repo_name}'
            ORDER BY published_at ASC
            """
            releases = client.query(query).result_rows
            if not releases:
                continue

            repo_search_dir = repo_base / owner / repo_name
            git_repo_path = next(
                (
                    p for p in repo_search_dir.glob(
                        '*/*',
                    ) if (p / '.git').exists()
                ), None,
            )
            if not git_repo_path:
                console.print(
                    f"  [red]Local git repository not found for {owner}/{repo_name}.[/red]",
                )
                continue

            try:
                git_repo = Repo(git_repo_path)
            except Exception as e:
                console.print(f"  [red]Failed to open git repo: {e}[/red]")
                continue

            prev_tag = None
            for tag, published_at, target in releases:
                # 1. Try ground truth
                code_endpoints = set()
                has_groundtruth = False
                code_json = code_dir / owner / repo_name / f"{tag}.json"
                if code_json.exists():
                    try:
                        with open(code_json, encoding='utf-8') as f:
                            code_raw = json.load(f)
                        code_endpoints = {
                            (
                                item['method'].upper(), self.normalize_path(
                                    item['path'],
                                ),
                            ) for item in code_raw
                        }
                        has_groundtruth = True
                    except Exception:
                        pass

                # 2. Try OpenAPI specs from git
                spec_endpoints = set()
                openapi_files = []
                try:
                    # Use tag to get files in that version
                    files = git_repo.git.ls_tree(
                        '-r', '--name-only', tag,
                    ).split('\n')
                    openapi_files = self.find_openapi_files(files)
                    for f_path in openapi_files:
                        try:
                            content = git_repo.git.show(f"{tag}:{f_path}")
                            is_yaml = f_path.lower().endswith(('.yaml', '.yml'))
                            spec_endpoints.update(
                                self.parse_openapi_spec(content, is_yaml),
                            )
                        except Exception:
                            continue
                except Exception:
                    pass

                # 3. Calculate activity metrics
                code_commits = 0
                spec_commits = 0
                if prev_tag:
                    try:
                        diff_range = f"{prev_tag}..{tag}"
                        code_commits = int(
                            git_repo.git.rev_list('--count', diff_range),
                        )
                        if openapi_files:
                            spec_commits = int(
                                git_repo.git.rev_list(
                                    '--count', diff_range, '--', *openapi_files,
                                ),
                            )
                    except Exception:
                        pass

                # 4. Calculate drift metrics
                overlap_pct: float = 0.0
                stale_pct: float = 0.0
                if has_groundtruth:
                    common = code_endpoints.intersection(spec_endpoints)
                    spec_only = spec_endpoints - code_endpoints
                    overlap_pct = (
                        len(common) / len(code_endpoints) * 100.0
                    ) if code_endpoints else 0.0
                    stale_pct = (
                        len(spec_only) / len(spec_endpoints) * 100.0
                    ) if spec_endpoints else 0.0

                drift_results.append({
                    'owner': owner,
                    'repo': repo_name,
                    'tag': tag,
                    'date': published_at,
                    'code_count': len(code_endpoints) if has_groundtruth else 0,
                    'spec_count': len(spec_endpoints),
                    'overlap_pct': overlap_pct,
                    'stale_pct': stale_pct,
                    'code_commits': code_commits,
                    'spec_commits': spec_commits,
                    'openapi_files': ';'.join(openapi_files),
                })
                prev_tag = tag
        return drift_results
