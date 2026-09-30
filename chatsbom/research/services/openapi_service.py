import contextlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import humanize
import structlog
import yaml
from rich.markup import escape

from chatsbom.core.config import get_config
from chatsbom.core.logging import console
from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory
from chatsbom.research.models.openapi import FrameworkStats
from chatsbom.research.models.openapi import OpenApiCandidate
from chatsbom.research.models.openapi import OpenApiCandidateResult
from chatsbom.services.git_service import _error_text
from chatsbom.services.git_service import _git
from chatsbom.services.git_service import GIT_QUIET_ENV
from chatsbom.warehouse import frameworks

if TYPE_CHECKING:
    import duckdb

logger = structlog.get_logger('openapi_service')

#: Wall-clock limit on each git `clone_repo` runs. A blobless clone
#: still carries every commit and tree of the history, and the archive
#: fetches the file contents of the one tree it writes, so it is
#: generous; a transfer that stalls is stopped, and no longer holds its
#: worker for good.
GIT_TIMEOUT = 1800

#: What GitHub allows in the name of an owner or of a repository.
_GITHUB_NAME = re.compile(r'^[A-Za-z0-9_.-]+$')

OPENAPI_FILENAMES = {
    'openapi.yaml', 'openapi.yml', 'openapi.json',
    'swagger.yaml', 'swagger.yml', 'swagger.json',
    'api.yaml', 'api.yml', 'api.json',
    'api-docs.yaml', 'api-docs.yml', 'api-docs.json',
    'openapi-spec.yaml', 'openapi-spec.yml', 'openapi-spec.json',
    'swagger-spec.yaml', 'swagger-spec.yml', 'swagger-spec.json',
}

IGNORED_DIR_NAMES = {
    'test', 'tests', '__tests__', '__test__',
    'fixture', 'fixtures',
    'example', 'examples',
    'sample', 'samples',
    'seed',
    'demo', 'demos',
    'testdata',
    'node_modules',
    'vendor',
    'bower_components',
    'dist',
    'build',
    'temp',
    'tmp',
    '.github',
    'site-packages',
    'mock', 'mocks',
    'coverage',
    'bin',
    'obj',
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
            path_parts = filepath.lower().split('/')
            basename = path_parts[-1]

            # 1. Check if basename matches known OpenAPI filenames
            if basename not in OPENAPI_FILENAMES:
                continue

            # 2. Skip if any part of the path is in IGNORED_DIR_NAMES
            if any(part in IGNORED_DIR_NAMES for part in path_parts[:-1]):
                continue

            # 3. Skip if path looks like it belongs to tests or fixtures (substring match)
            if any(pattern in part for part in path_parts[:-1] for pattern in ['-test', '_test', 'test-', 'test_', 'fixture', 'example']):
                continue

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

    def find_candidates(
        self, con: 'duckdb.DuckDBPyConnection',
    ) -> OpenApiCandidateResult:
        """The projects of each framework, from the warehouse `con` is
        open on (`warehouse/frameworks.py`), with the OpenAPI file their
        tree has, if any."""
        candidates = []
        stats = []
        for framework_enum in Framework:
            try:
                framework = FrameworkFactory.create(framework_enum)
                package_names = framework.get_package_names()
                openapi_packages = framework.get_openapi_packages()
                excluded_packages = framework.get_excluded_package_names()
                gen_commands = framework.get_generation_commands()
                if not package_names:
                    continue

                # Repositories using the framework, with their OpenAPI
                # packages, less those with a package that excludes them:
                # all of it from the current scan, in one pass.
                data = list(
                    frameworks.candidates(
                        con, package_names, openapi_packages,
                        excluded_packages,
                    ),
                )

                framework_total = 0
                framework_matched = 0
                count_file_only = 0
                count_deps_only = 0
                count_both = 0
                last_lang = ''

                for row in data:
                    language = str(row['language'] or '').lower() or \
                        framework.get_language()
                    last_lang = language
                    # As GitHub spells them. A tag is case-sensitive:
                    # lowercased, Netflix/zuul's `V3.0.0` named no tag, in
                    # the release URL and for every command the CSV is
                    # handed to after this one (#47).
                    owner = str(row['owner'])
                    repo = str(row['repo'])
                    default_branch = str(row['default_branch'] or '')
                    latest_release = str(row['latest_release'] or '')
                    commit_sha = str(row['commit_sha'] or '')
                    framework_version = str(row['framework_version'] or '')
                    repository_id = int(row['repository_id'])

                    matched_deps = list(row['matched_dependencies'])
                    matched_deps_str = ';'.join(matched_deps)

                    framework_total += 1
                    ref = latest_release if latest_release else default_branch
                    # Keyed by repository id and commit, like every stage
                    # directory: neither the language nor the ref is in
                    # the path any more.
                    tree_file = self.config.paths.tree_file(
                        repository_id, commit_sha,
                    )

                    best_openapi_file = ''
                    openapi_url = ''

                    if tree_file.exists():
                        try:
                            with open(tree_file, encoding='utf-8') as f:
                                tree_files = [
                                    line.strip()
                                    for line in f if line.strip()
                                ]

                            openapi_files = self.find_openapi_files(tree_files)
                            if openapi_files:
                                # Sort to find the best candidate
                                openapi_files.sort(
                                    key=lambda p: (
                                        0 if any(
                                            x in Path(p).name.lower()
                                            for x in ['openapi', 'swagger']
                                        ) else 1,
                                        p.count('/'),
                                        len(p),
                                    ),
                                )
                                best_openapi_file = openapi_files[0]
                                sha_or_ref = commit_sha if commit_sha else ref
                                openapi_url = f'https://github.com/{owner}/{repo}/blob/{sha_or_ref}/{best_openapi_file}'
                        except OSError:
                            pass

                    # Determine presence
                    has_file = bool(best_openapi_file)
                    has_deps = len(matched_deps) > 0
                    if not has_file and not has_deps:
                        continue

                    # Granular counters
                    if has_file and has_deps:
                        count_both += 1
                    elif has_file:
                        count_file_only += 1
                    else:
                        count_deps_only += 1

                    # Infer generation command
                    best_cmd = ''
                    for dep in matched_deps:
                        if dep in gen_commands:
                            best_cmd = gen_commands[dep]
                            break
                    # Default for framework if no specific package command found
                    if not best_cmd and framework_enum.value in gen_commands:
                        best_cmd = gen_commands[framework_enum.value]

                    framework_matched += 1
                    url = f'https://github.com/{owner}/{repo}/releases/tag/{latest_release}' if latest_release else \
                        (f'https://github.com/{owner}/{repo}/tree/{commit_sha}' if commit_sha else f'https://github.com/{owner}/{repo}')

                    candidates.append(
                        OpenApiCandidate(
                            language=language,
                            framework=framework_enum.value,
                            framework_version=framework_version,
                            owner=owner,
                            repo=repo,
                            stars=int(row['stars'] or 0),
                            default_branch=default_branch,
                            latest_release=latest_release,
                            commit_sha=commit_sha,
                            url=url,
                            openapi_file=best_openapi_file,
                            openapi_url=openapi_url,
                            matched_dependencies=matched_deps_str,
                            has_openapi_file=has_file,
                            has_openapi_deps=has_deps,
                            generation_command=best_cmd,
                        ),
                    )

                stats.append(
                    FrameworkStats(
                        framework=framework_enum.value,
                        language=last_lang,
                        total_projects=framework_total,
                        matched_projects=framework_matched,
                        count_file_only=count_file_only,
                        count_deps_only=count_deps_only,
                        count_both=count_both,
                    ),
                )
                console.print(
                    f'[bold]{framework_enum.value}[/bold]: [cyan]{framework_matched}[/cyan]/{framework_total} projects have OpenAPI specs',
                )

            except Exception as e:
                console.print(
                    f'[bold red]Error querying for {framework_enum.value}: {escape(str(e))}[/bold red]',
                )

        return OpenApiCandidateResult(candidates=candidates, stats=stats)

    def clone_repo(self, owner: str, repo: str, dest: Path, tag: str | None = None, commit_sha: str | None = None) -> tuple[str, str, bool, str, dict]:
        """A snapshot of `owner/repo` at the commit, else the tag, else
        HEAD, under `dest`, cut from a clone kept in `~/.repositories`.

        The repositories are untrusted, and so is the CSV that names
        them (#47). The clone is bare and blobless and never checked
        out: a checkout runs whatever filters git is configured with,
        git-lfs's for one, on the repository's files. The snapshot is
        `git archive` of the one tree, which fetches that tree's file
        contents alone. Every git has a time limit, no prompt, and no
        user or system config; the tag and the commit come after
        `--end-of-options`, so one spelled `--output=<path>` is a name,
        not where the archive is written.
        """
        start_time = time.time()
        global_path = self.config.paths.global_repos_dir / owner / repo
        repo_dir = dest / owner / repo / self.get_version_path(tag, commit_sha)
        target = commit_sha or tag or 'HEAD'

        def finalize(success: bool, msg: str, stats: dict | None = None) -> tuple[str, str, bool, str, dict]:
            if stats is None:
                stats = {}
            stats['duration'] = round(time.time() - start_time, 2)
            return (owner, repo, success, msg, stats)

        # Both become paths, and a cache directory that is not a
        # repository is removed: `acme/..` was the whole cache.
        if not (_is_github_name(owner) and _is_github_name(repo)):
            return finalize(False, 'Not a GitHub repository name')
        if not _is_below(repo_dir, dest):
            return finalize(False, 'The snapshot would be outside its directory')

        if repo_dir.exists():
            return finalize(True, 'already exists', self._get_stats(repo_dir, global_path))

        env = {**os.environ, **GIT_QUIET_ENV}
        try:
            git_dir = _git_dir(global_path)
            if git_dir is None:
                shutil.rmtree(global_path, ignore_errors=True)
                global_path.parent.mkdir(parents=True, exist_ok=True)
                _git(
                    [
                        'clone', '--quiet', '--bare', '--filter=blob:none',
                        '--end-of-options',
                        f'https://github.com/{owner}/{repo}.git',
                        str(global_path),
                    ],
                    env=env, timeout=GIT_TIMEOUT,
                )
                git_dir = global_path
            try:
                self._archive(git_dir, target, repo_dir, env)
            except subprocess.CalledProcessError:
                # A clone older than the tag or the commit: fetch them.
                for fetch in (
                    ['--tags', '--end-of-options', 'origin'],
                    ['--end-of-options', 'origin', target],
                ):
                    with contextlib.suppress(OSError, subprocess.SubprocessError):
                        _git(
                            [
                                '--git-dir', str(git_dir), 'fetch',
                                '--quiet', *fetch,
                            ],
                            env=env, timeout=GIT_TIMEOUT,
                        )
                self._archive(git_dir, target, repo_dir, env)
        except (OSError, subprocess.SubprocessError, tarfile.TarError) as e:
            shutil.rmtree(repo_dir, ignore_errors=True)
            return finalize(False, f'Archive failed: {_error_text(e)}')

        # Fast cleanup
        heavy_dirs = {
            'node_modules', 'vendor', 'dist',
            'build', 'bin', '.github', 'tests', 'test',
        }
        heavy_exts = {
            '.png', '.jpg', '.jpeg', '.gif', '.mp4',
            '.zip', '.tar', '.pdf', '.exe', '.dll', '.so',
        }

        for p in list(repo_dir.rglob('*')):
            if (p.is_dir() and p.name.lower() in heavy_dirs) or (p.is_file() and p.suffix.lower() in heavy_exts):
                if p.is_dir():
                    shutil.rmtree(p, ignore_errors=True)
                else:
                    p.unlink(missing_ok=True)

        return finalize(True, 'snapshot created', self._get_stats(repo_dir, global_path))

    def _archive(self, git_dir: Path, target: str, repo_dir: Path, env: dict[str, str]) -> None:
        """`target`'s tree, from `git archive`, into `repo_dir`."""
        with tempfile.TemporaryDirectory(prefix='chatsbom-snapshot-') as tmp:
            archive = Path(tmp) / 'snapshot.tar'
            _git(
                [
                    '--git-dir', str(git_dir), 'archive', '--format=tar',
                    f'--output={archive}', '--end-of-options', target,
                ],
                env=env, timeout=GIT_TIMEOUT,
            )
            repo_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(archive) as tar:
                tar.extractall(repo_dir, filter=_files_only)

    def _get_stats(self, repo_dir: Path, global_path: Path) -> dict:
        s, g = self.get_dir_size(repo_dir), self.get_dir_size(global_path)
        return {
            'shadow_size': self.format_size(s),
            'global_size': self.format_size(g),
            'saved': f"{(1 - s / g) * 100:.1f}%" if g > 0 else '0%',
        }

    def analyze_drift(self, candidates, code_dir: Path, repo_base: Path) -> list[dict]:
        drift_results = []
        for c in candidates:
            owner = c['owner']
            repo_name = c['repo']
            latest_release = c.get('latest_release', '').strip()
            commit_sha = c.get('commit_sha', '').strip()

            # Identify the version tag or commit for the candidate
            tag = latest_release if latest_release else commit_sha
            if not tag:
                tag = c.get('default_branch', 'HEAD').strip()

            console.print(
                f"Analyzing data for [bold cyan]{owner}/{repo_name}[/bold cyan] @ {tag}...",
            )

            # Determine snapshot directory
            snapshot_dir = repo_base / owner / repo_name / \
                self.get_version_path(latest_release, commit_sha)
            if not snapshot_dir.exists():
                console.print(
                    f"  [red]Snapshot directory not found for {owner}/{repo_name} at {tag}.[/red]",
                )
                continue

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

            # 2. Try OpenAPI specs from snapshot
            spec_endpoints = set()
            openapi_files = []
            try:
                # Find all files within snapshot_dir, formatted as relative posix paths
                files = []
                file_sizes = {}
                for p in snapshot_dir.rglob('*'):
                    if p.is_file():
                        try:
                            rel_path = p.relative_to(snapshot_dir).as_posix()
                            files.append(rel_path)
                            file_sizes[rel_path] = p.stat().st_size
                        except ValueError:
                            pass

                openapi_files = self.find_openapi_files(files)
                for f_path in openapi_files:
                    try:
                        with open(snapshot_dir / f_path, encoding='utf-8') as f:
                            content = f.read()
                        is_yaml = f_path.lower().endswith(('.yaml', '.yml'))
                        spec_endpoints.update(
                            self.parse_openapi_spec(content, is_yaml),
                        )
                    except Exception:
                        continue
            except Exception as e:
                console.print(
                    f"  [red]Error reading snapshot files: {escape(str(e))}[/red]",
                )

            # Choose the best OpenAPI file for the CSV output
            best_openapi_file = ''
            if openapi_files:
                # Sort: 1st by JSON (True > False), 2nd by file size (descending)
                openapi_files.sort(
                    key=lambda p: (
                        p.lower().endswith('.json'),
                        file_sizes.get(p, 0),
                    ),
                    reverse=True,
                )
                best_openapi_file = openapi_files[0]

            # 3. Calculate drift metrics (Precision, Recall, F1)
            precision: float = 0.0
            recall: float = 0.0
            f1_score: float = 0.0

            code_count = len(code_endpoints) if has_groundtruth else 0
            spec_count = len(spec_endpoints)

            if has_groundtruth:
                common = code_endpoints.intersection(spec_endpoints)
                common_count = len(common)

                if spec_count > 0:
                    precision = common_count / spec_count
                if code_count > 0:
                    recall = common_count / code_count

                if precision + recall > 0:
                    f1_score = 2 * (precision * recall) / (precision + recall)

            drift_results.append({
                'language': c.get('language', ''),
                'framework': c.get('framework', ''),
                'owner': owner,
                'repo': repo_name,
                'stars': c.get('stars', 0),
                'tag': tag,
                'implemented_endpoints': code_count,
                'documented_endpoints': spec_count,
                'precision': round(precision, 4),
                'recall': round(recall, 4),
                'f1_score': round(f1_score, 4),
                'openapi_file': best_openapi_file,
            })
        return drift_results


def _is_github_name(name: str) -> bool:
    """Whether `name` could be a GitHub owner's or repository's."""
    return bool(_GITHUB_NAME.match(name)) and name not in ('.', '..')


def _is_below(path: Path, root: Path) -> bool:
    """Whether `path` is inside `root`, once links and `..` resolve."""
    return root.resolve() in path.resolve().parents


def _git_dir(path: Path) -> Path | None:
    """The repository kept at `path`: bare, as `clone_repo` clones it,
    or the `.git` of a checkout an earlier version made. None if there
    is neither."""
    for candidate in (path, path / '.git'):
        if (candidate / 'HEAD').is_file() and (candidate / 'objects').is_dir():
            return candidate
    return None


def _files_only(member: tarfile.TarInfo, path: str) -> tarfile.TarInfo | None:
    """What a snapshot keeps of an archive: its files and directories.

    Not its links. One in an untrusted repository can point anywhere on
    the machine, at `/etc/passwd` or a key, and the commands that read a
    snapshot would read that. The data filter refuses the rest of what
    is not to be written: absolute paths, and paths out of `path`.
    """
    if not (member.isfile() or member.isdir()):
        return None
    return tarfile.data_filter(member, path)
