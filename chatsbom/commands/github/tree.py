import concurrent.futures
import json
import time
from threading import Lock

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.logging import console
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.language import Language

logger = structlog.get_logger('tree_command')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Force refresh even if tree data exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    workers: int = typer.Option(10, help='Number of concurrent workers'),
):
    """
    Fetch file trees for repositories (without downloading content).
    Reads from: data/04-github-commit
    Writes index to: data/08-github-tree/{language}.jsonl
    Writes trees to: data/08-github-tree/{language}/{owner}/{repo}/{ref}/{sha}/tree.json
    """
    check_github_token(token)
    container = get_container()
    config = container.config
    git_service = container.get_git_service(token)

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_commit_list_path(lang_str)
        output_path = config.paths.get_tree_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No commit data found for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if not repos:
            logger.warning('Empty repo list', language=lang_str)
            continue

        if limit:
            repos = repos[:limit]

        # Use standard Storage for deduplication of processed repos in jsonl
        storage = Storage(output_path)

        fetched = 0
        skipped = 0
        failed = 0
        stats_lock = Lock()

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn('•'),
            TimeElapsedColumn(),
            TextColumn('•'),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Fetching trees {lang_str}...", total=len(repos),
            )

            def process_single_repo(repo):
                nonlocal fetched, skipped, failed

                try:
                    # Get SHA from download_target
                    dt = repo.download_target
                    if not dt or not dt.commit_sha:
                        with stats_lock:
                            failed += 1
                        progress.advance(task)
                        return

                    owner = repo.owner
                    repo_name = repo.repo
                    sha = dt.commit_sha
                    ref = dt.ref

                    # Determine paths
                    tree_file_path = config.paths.get_tree_file_path(
                        lang_str, owner, repo_name, ref, sha,
                    )
                    cache_path = config.paths.get_tree_cache_path(
                        owner, repo_name, sha,
                    )

                    # Check if already processed (result file existence)
                    if not force and repo.id in storage.visited_ids and tree_file_path.exists():
                        with stats_lock:
                            skipped += 1
                        logger.info(
                            'Tree exists (Skipped)',
                            repo=f"{owner}/{repo_name}",
                            ref=ref,
                            sha=sha[:7],
                            _style='dim',
                        )
                        progress.advance(task)
                        return

                    # Fetch tree via Git CLI (handles caching internally)
                    start_time = time.time()
                    files = git_service.get_repository_tree(
                        owner, repo_name, sha, cache_path=cache_path,
                    )
                    elapsed = time.time() - start_time

                    if files is not None:
                        # Save tree to individual JSON file in data/
                        tree_file_path.parent.mkdir(
                            parents=True, exist_ok=True,
                        )
                        with open(tree_file_path, 'w', encoding='utf-8') as f:
                            json.dump(files, f, separators=(',', ':'))

                        # Save metadata to index
                        storage.save(repo)

                        with stats_lock:
                            fetched += 1

                        logger.info(
                            'Tree fetched',
                            repo=f"{owner}/{repo_name}",
                            ref=ref,
                            sha=sha[:7],
                            files=len(files),
                            elapsed=f"{elapsed:.3f}s",
                        )
                    else:
                        with stats_lock:
                            failed += 1
                        logger.error(
                            'Tree fetch failed',
                            repo=f"{owner}/{repo_name}",
                            ref=ref,
                            sha=sha[:7],
                            elapsed=f"{elapsed:.3f}s",
                        )

                    progress.advance(task)
                except Exception as e:
                    logger.error(
                        'Unexpected error in worker thread',
                        repo=f"{repo.owner}/{repo.repo}", error=str(e),
                    )
                    with stats_lock:
                        failed += 1
                    progress.advance(task)

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                executor.map(process_single_repo, repos)

        logger.info(
            'Tree Fetch Complete',
            language=lang_str,
            fetched=fetched,
            skipped=skipped,
            failed=failed,
        )
