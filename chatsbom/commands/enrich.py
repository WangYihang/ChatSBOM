import concurrent.futures
import json
import os
import time
from pathlib import Path

import dotenv
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.enrichment_service import EnrichStats

dotenv.load_dotenv()
console = Console()


@handle_errors
def main(
    input_file: str | None = typer.Option(None, help='Input JSONL file path'),
    output_file: str | None = typer.Option(
        None, help='Output JSONL file path',
    ),
    language: Language | None = typer.Option(
        None, help='Target Language (default: all)',
    ),
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    concurrency: int = typer.Option(2, help='Number of concurrent requests'),
    force: bool = typer.Option(
        False, help='Re-enrich already enriched repositories',
    ),
    limit: int | None = typer.Option(
        None, help='Limit number of processed repos',
    ),
):
    """
    Enrich repository metadata with release and commit information.
    """
    container = get_container()
    config = container.config
    service = container.get_enrichment_service(token)
    stats = EnrichStats()

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        target_input = input_file or str(
            config.paths.get_repo_list_path(str(lang), operation='collect'),
        )
        if not os.path.exists(target_input):
            continue

        target_output = output_file or str(
            config.paths.get_repo_list_path(str(lang), operation='enrich'),
        )

        repos = []
        with open(target_input) as f:
            for line in f:
                if line.strip():
                    try:
                        repos.append(Repository.model_validate_json(line))
                    except Exception:
                        pass

        if not repos:
            continue
        if limit:
            repos = repos[:limit]

        stats.total += len(repos)
        Path(target_output).parent.mkdir(parents=True, exist_ok=True)
        temp_file = Path(target_output).with_suffix('.tmp')

        with Progress(
            SpinnerColumn(),
            TextColumn('[bold blue]{task.description}'),
            BarColumn(),
            TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
            TextColumn('•'),
            TextColumn('[green]{task.completed}/{task.total}'),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(f'Enriching {lang}...', total=len(repos))

            with open(temp_file, 'w', encoding='utf-8') as out:
                with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                    futures = {
                        executor.submit(service.process_repo, repo, stats, str(lang), force): repo
                        for repo in repos
                    }
                    for future in concurrent.futures.as_completed(futures):
                        original_repo = futures[future]
                        enriched_dict = future.result()

                        data_to_write = enriched_dict if enriched_dict else original_repo.model_dump(
                            mode='json',
                        )
                        out.write(
                            json.dumps(
                                data_to_write,
                                ensure_ascii=False,
                            ) + '\n',
                        )
                        progress.advance(task)

        temp_file.replace(target_output)

    # Print summary
    table = Table(title='Enrichment Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Total Repositories', str(stats.total))
    table.add_row('Enriched', str(stats.enriched))
    table.add_row('Skipped', str(stats.skipped))
    table.add_row('Failed', str(stats.failed))
    table.add_row('Total Duration', f"{time.time() - stats.start_time:.2f}s")
    console.print(table)


if __name__ == '__main__':
    typer.run(main)
