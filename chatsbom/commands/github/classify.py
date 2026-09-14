import csv
import json
import threading
from collections.abc import Callable
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import TextIO

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

from chatsbom.core.config import get_config
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.core.repository import QueryRepository
from chatsbom.models.framework_index import FrameworkIndex
from chatsbom.models.repository import Repository
from chatsbom.services.github_analysis_service import GitHubAnalysisService
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('classify_command')
app = typer.Typer(
    help='Batch classify GitHub repositories and extract metadata using LLM.',
)


class OutputFormat(str, Enum):
    JSONL = 'jsonl'
    CSV = 'csv'


#: An analysis function: a repository in, a flat result row out. `None`
#: means "could not classify", which is counted rather than raised.
Analyze = Callable[[Repository], dict[str, Any] | None]

DEFAULT_CONCURRENCY = 8


@dataclass(frozen=True, slots=True)
class ClassificationResult:
    """Outcome counts for one classification run."""

    processed: int = 0
    cached: int = 0
    failed: int = 0


def already_processed_ids(path: Path, output_format: OutputFormat) -> set[int]:
    """Repository ids already present in the output, for resuming a run."""
    if not path.exists():
        return set()

    ids: set[int] = set()
    try:
        with open(path, encoding='utf-8', newline='') as f:
            if output_format == OutputFormat.JSONL:
                rows: Iterable[Any] = (
                    line for line in f if line.strip()
                )
                for line in rows:
                    try:
                        ids.add(int(json.loads(line)['id']))
                    except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                        continue
            else:
                for row in csv.DictReader(f):
                    try:
                        ids.add(int(row['id']))
                    except (KeyError, TypeError, ValueError):
                        continue
    except OSError as e:
        logger.warning('Could not read existing results', error=str(e))
        return set()

    if ids:
        logger.info('Resuming', already_processed=len(ids))
    return ids


class ResultWriter:
    """Appends result rows, serialising concurrent writers.

    Results are flushed per row so an interrupted run stays resumable,
    and the CSV header is written only when the file is new.
    """

    def __init__(self, path: Path, output_format: OutputFormat) -> None:
        self.path = path
        self.output_format = output_format
        self._lock = threading.Lock()
        self._handle: TextIO | None = None
        self._csv_writer: csv.DictWriter | None = None
        self._needs_header = not (path.exists() and path.stat().st_size > 0)

    def __enter__(self) -> 'ResultWriter':
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = open(self.path, 'a', encoding='utf-8', newline='')
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def write(self, row: dict[str, Any]) -> None:
        if self._handle is None:
            raise RuntimeError('ResultWriter used outside its context manager')

        with self._lock:
            if self.output_format == OutputFormat.JSONL:
                self._handle.write(
                    json.dumps(row, ensure_ascii=False) + '\n',
                )
            else:
                if self._csv_writer is None:
                    self._csv_writer = csv.DictWriter(
                        self._handle, fieldnames=list(row),
                    )
                    if self._needs_header:
                        self._csv_writer.writeheader()
                        self._needs_header = False
                self._csv_writer.writerow(row)
            self._handle.flush()


def classify_repositories(
    repositories: list[Repository],
    analyze: Analyze,
    writer: ResultWriter,
    processed_ids: set[int] | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    on_progress: Callable[[], None] | None = None,
) -> ClassificationResult:
    """Classify repositories concurrently, writing each result as it lands.

    Classification is dominated by waiting on an LLM endpoint, so the work
    is IO bound and a thread pool is the right tool. Running it one repo
    at a time made the full corpus impractical.
    """
    if concurrency < 1:
        raise ValueError(f"concurrency must be >= 1, got {concurrency}")

    seen = processed_ids or set()
    pending = [r for r in repositories if r.id not in seen]
    cached = len(repositories) - len(pending)

    if on_progress:
        for _ in range(cached):
            on_progress()

    processed = 0
    failed = 0
    counter_lock = threading.Lock()

    def work(repository: Repository) -> None:
        nonlocal processed, failed
        try:
            row = analyze(repository)
        except Exception as e:
            logger.warning(
                'Classification failed',
                repo=f"{repository.owner}/{repository.repo}", error=str(e),
            )
            row = None

        if row is None:
            with counter_lock:
                failed += 1
        else:
            writer.write(row)
            with counter_lock:
                processed += 1

        if on_progress:
            on_progress()

    if pending:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(work, pending))

    return ClassificationResult(
        processed=processed, cached=cached, failed=failed,
    )


def run_classification(
    repos: list[Repository],
    analyzer: GitHubAnalysisService,
    github_service: GitHubService,
    output_path: Path,
    output_format: OutputFormat,
    query_repo: QueryRepository | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> ClassificationResult:
    """Wire the analysis service and framework enrichment into a batch run."""
    index = FrameworkIndex.build()
    processed_ids = already_processed_ids(output_path, output_format)

    # One query for every repository, instead of one per repository.
    frameworks_by_repo: dict[int, list[tuple[str, str]]] = {}
    if query_repo is not None:
        pending_ids = [r.id for r in repos if r.id not in processed_ids]
        try:
            frameworks_by_repo = query_repo.get_frameworks_for_repositories(
                pending_ids, index.as_framework_map(),
            )
        except Exception as e:
            logger.warning('Framework enrichment unavailable', error=str(e))

    def analyze(repository: Repository) -> dict[str, Any] | None:
        result = analyzer.analyze_repo(repository, github_service)
        if result is None:
            return None

        found = frameworks_by_repo.get(repository.id) or []
        if found:
            # Pick by the order frameworks are declared, not by whatever
            # order the database happened to return.
            primary = index.detect(name for name, _ in found)
            chosen = str(primary) if primary else found[0][0]
            version = next(
                (v for name, v in found if name == chosen), '',
            )
            result.analysis.primary_framework = chosen
            result.analysis.framework_version = version

        return result.to_flat_dict()

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            '[green]Processing repos...', total=len(repos),
        )
        with ResultWriter(output_path, output_format) as writer:
            result = classify_repositories(
                repos, analyze, writer,
                processed_ids=processed_ids,
                concurrency=concurrency,
                on_progress=lambda: progress.advance(task),
            )

    console.print('\n[bold green]✓ Processing Complete![/bold green]')
    console.print(f"  - Newly processed: {result.processed}")
    console.print(f"  - Already cached:  {result.cached}")
    console.print(f"  - Errors/Skipped:  {result.failed}")
    console.print(f"  - Results saved to: [cyan]{output_path}[/cyan]\n")
    return result


@app.callback(invoke_without_command=True)
def main(
    input_path: Path | None = typer.Option(
        None, '--input', '-i', help='Input JSONL file of repositories',
    ),
    output_path: Path | None = typer.Option(
        None, '--output', '-o', help='Output file path',
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.JSONL, '--format', '-f', help='Output format (jsonl or csv)',
    ),
    limit: int | None = typer.Option(
        None, help='Limit number of repositories to process (default: all)',
    ),
    model: str = typer.Option(
        'deepseek-chat', help='LLM model to use (compatible with OpenAI API)',
    ),
    api_key: str = typer.Option(
        None, envvar='OPENAI_API_KEY', help='OpenAI API Key',
    ),
    base_url: str = typer.Option(
        None, envvar='OPENAI_BASE_URL', help='OpenAI Base URL',
    ),
    github_token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token (for fetching README if missing)',
    ),
    concurrency: int = typer.Option(
        DEFAULT_CONCURRENCY, help='Concurrent LLM requests',
    ),
) -> None:
    """
    Classify repositories using LLM and extract structured info.

    Reads a list of repositories (JSONL), classifies them concurrently
    using instructor + pydantic, and appends each result as it lands so an
    interrupted run resumes where it stopped.
    """
    if not api_key and not base_url:
        console.print(
            '[red]Error: OPENAI_API_KEY is required unless base_url is provided.[/red]',
        )
        raise typer.Exit(1)

    # Use a dummy key for local providers if not provided
    api_key = api_key or 'ollama'

    config = get_config()

    # 1. Path Resolution
    if not input_path:
        input_path = config.paths.search_dir / 'all.jsonl'

    if not input_path.exists():
        console.print(f"[red]Error: Input file {input_path} not found.[/red]")
        raise typer.Exit(1)

    if not output_path:
        ext = 'jsonl' if output_format == OutputFormat.JSONL else 'csv'
        output_path = config.paths.base_data_dir / \
            '08-github-classify' / f'all.{ext}'

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 2. Data Loading (with simple mock-friendly logic)
    repos = []
    with open(input_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)

                # Adapt common field variations
                if 'repo_name' in data and 'name' not in data:
                    data['name'] = data['repo_name']
                if 'repo' in data and 'name' not in data:
                    data['name'] = data['repo']
                if '/' in data.get('name', '') and 'owner' not in data:
                    data['owner'], data['name'] = data['name'].split('/', 1)

                # Mock default owner if missing (requirement: repo_name usually implies owner/name)
                if 'owner' not in data:
                    data['owner'] = 'unknown'

                repos.append(Repository.model_validate(data))
                if limit and len(repos) >= limit:
                    break
            except Exception as e:
                logger.warning(
                    'Failed to parse input line',
                    error=str(e), line_snippet=line[:50],
                )

    if not repos:
        console.print('[yellow]No repositories found to process.[/yellow]')
        return

    logger.info(
        'Starting batch classification', count=len(repos),
        model=model,
    )

    # 3. Service Initialization
    analyzer = GitHubAnalysisService(
        api_key=api_key,
        base_url=base_url,
        model=model,
    )
    # Use provided token or fallback to config
    github_service = GitHubService(
        token=github_token or config.github.token or '',
    )

    # 4. Concurrent execution
    container = get_container()
    query_repo = None
    try:
        db_config = container.config.get_db_config('guest')
        query_repo = QueryRepository(db_config)
    except Exception:
        logger.warning(
            'Could not initialize database connection for framework enrichment',
        )

    try:
        run_classification(
            repos, analyzer, github_service, output_path, output_format,
            query_repo, concurrency=concurrency,
        )
    finally:
        if query_repo:
            query_repo.close()


if __name__ == '__main__':
    app()
