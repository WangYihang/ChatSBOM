import concurrent.futures
import json
import os
import time
from pathlib import Path

import dotenv
import requests
import structlog
import typer
from requests.adapters import HTTPAdapter
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from urllib3.util.retry import Retry

from sbom_insight.models.language import Language
from sbom_insight.models.language import LanguageFactory

dotenv.load_dotenv()
console = Console()
logger = structlog.get_logger('downloader')


class SBOMDownloader:
    """Handles concurrent downloading of SBOM files from GitHub."""

    def __init__(self, token: str | None, base_dir: str, timeout: int = 10, pool_size: int = 50):
        self.session = requests.Session()

        # Robust connection pooling configuration
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=Retry(
                total=3, backoff_factor=1,
                status_forcelist=[500, 502, 503, 504],
            ),
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})

        self.base_dir = Path(base_dir)
        self.timeout = timeout

    def download_repo(self, repo: dict, lang: Language) -> str:
        """Downloads SBOM files for a single repository."""
        full_name = repo['full_name']
        owner, name = full_name.split('/')
        branch = repo.get('default_branch', 'master')

        target_dir = self.base_dir / lang / owner / name / branch
        target_dir.mkdir(parents=True, exist_ok=True)

        base_url = f"https://raw.githubusercontent.com/{owner}/{name}/{branch}"
        language_handler = LanguageFactory.get_handler(lang)
        targets: list[str] = language_handler.get_sbom_paths()
        results = []

        for filename in targets:
            file_path = target_dir / filename

            # Skip existing non-empty files
            if file_path.exists() and file_path.stat().st_size > 0:
                continue

            try:
                start_time = time.time()
                url = f"{base_url}/{filename}"
                resp = self.session.get(url, timeout=self.timeout)
                elapsed = time.time() - start_time

                if resp.status_code == 200:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(resp.content)

                    logger.info(
                        'Downloaded',
                        repo=full_name,
                        file=filename,
                        size=len(resp.content),
                        elapsed=f"{elapsed:.2f}s",
                        url=resp.url,
                    )
                    results.append(f"[green]{filename}[/green]")
                elif resp.status_code == 404:
                    results.append(f"[dim yellow]no {filename}[/dim yellow]")
                else:
                    logger.warning(
                        'Download Failed',
                        repo=full_name,
                        file=filename,
                        status=resp.status_code,
                        elapsed=f"{elapsed:.2f}s",
                        url=url,
                    )
                    results.append(f"[red]{filename} {resp.status_code}[/red]")

            except requests.RequestException as e:
                logger.error(
                    'Download Error',
                    repo=full_name,
                    file=filename,
                    error=str(e),
                )
                results.append(f"[red]{filename} Err[/red]")

        if not results:
            return f"[dim]{full_name} skip[/dim]"

        return f"{full_name}: {', '.join(results)}"


def load_targets(jsonl_path: str) -> list[dict]:
    """Loads repository targets from a JSONL file."""
    targets: list[dict] = []
    path = Path(jsonl_path)
    if not path.exists():
        return targets

    with path.open(encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    targets.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return targets


def main(
    input_file: str | None = typer.Option(
        None, help='Input JSONL file path (default: {language}.jsonl)',
    ),
    output_dir: str = typer.Option(
        'data', help='Download destination directory',
    ),
    language: Language = typer.Option(Language.GO, help='Target Language'),
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    concurrency: int = typer.Option(32, help='Number of concurrent threads'),
    limit: int | None = typer.Option(
        None, help='Limit number of processed repos (for testing)',
    ),
):
    """
    Concurrent SBOM Downloader.
    """
    if input_file is None:
        target_file = f"{language}.jsonl"
    else:
        target_file = input_file

    tasks = load_targets(target_file)
    if not tasks:
        logger.error(f"Error: Input file empty or missing: {target_file}")
        raise typer.Exit(1)

    if limit:
        tasks = tasks[:limit]
        logger.warning(f"Test Mode: Limiting to top {limit} tasks")

    logger.info(
        'Starting Direct Download',
        target_file=target_file,
        total_tasks=len(tasks),
        concurrency=concurrency,
        output_path=f"{output_dir}/{language}/...",
    )

    downloader = SBOMDownloader(token, output_dir, pool_size=concurrency)

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TextColumn('•'),
        TextColumn('[green]{task.completed}/{task.total}'),
        TextColumn('•'),
        TextColumn('[dim]{task.fields[status]}', justify='left'),
        console=console,
    ) as progress:

        main_task = progress.add_task(
            'Downloading...', total=len(tasks), status='Starting...',
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_repo = {
                executor.submit(downloader.download_repo, repo, language): repo
                for repo in tasks
            }

            for future in concurrent.futures.as_completed(future_to_repo):
                try:
                    msg = future.result()
                    progress.update(main_task, advance=1, status=msg)
                except Exception:
                    progress.update(
                        main_task, advance=1,
                        status='[red]Error[/red]',
                    )

    logger.info(
        f"Download complete. Data saved in: {os.path.abspath(output_dir)}",
    )


if __name__ == '__main__':
    typer.run(main)
