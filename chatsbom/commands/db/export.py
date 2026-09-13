import csv
from collections.abc import Iterator
from pathlib import Path

import structlog
import typer

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.core.repository import QueryRepository
from chatsbom.models.framework_index import FrameworkIndex
from chatsbom.models.relationship import DIRECT

logger = structlog.get_logger('db_export')
app = typer.Typer()

COLUMNS = [
    'language', 'framework', 'owner', 'repo', 'stars',
    'default_branch', 'latest_release', 'commit_sha', 'url',
    'direct_dependencies', 'total_dependencies',
]

# Aggregates per repository so the framework match needs one pass. Only
# the current scan of each repository contributes.
#
# The `a.name != ''` guards matter: a LEFT JOIN with no match yields a row
# whose artifact columns hold ClickHouse defaults, not NULL, so an
# unguarded countDistinct reports 1 dependency for a repository with none.
EXPORT_QUERY = """
SELECT
    r.language AS language,
    r.owner AS owner,
    r.repo AS repo,
    r.stars AS stars,
    r.default_branch AS default_branch,
    r.latest_release_tag AS latest_release,
    r.sbom_commit_sha AS commit_sha,
    r.url AS url,
    groupUniqArrayIf(a.name, a.name != '') AS packages,
    countDistinctIf(
        a.name, a.name != '' AND a.relationship = {direct:String}
    ) AS direct_dependencies,
    countDistinctIf(a.name, a.name != '') AS total_dependencies
FROM repositories AS r FINAL
LEFT JOIN artifacts AS a
    ON a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha
GROUP BY
    r.id, r.language, r.owner, r.repo, r.stars,
    r.default_branch, r.latest_release_tag, r.sbom_commit_sha, r.url
ORDER BY r.stars DESC, r.owner ASC, r.repo ASC
"""


@app.callback(invoke_without_command=True)
def main(
    output: str = typer.Option(
        'projects.csv', help='Output CSV file path',
    ),
    web_only: bool = typer.Option(
        False,
        '--web-only',
        help='Export only projects with a detected web framework',
    ),
) -> None:
    """Export projects and their frameworks to a CSV file."""
    container = get_container()
    db_config = container.config.get_db_config('guest')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        console=console,
        require_database=True,
    )

    query_repo = container.get_query_repository()
    index = FrameworkIndex.build()

    if web_only:
        console.print(
            '[bold green]Exporting web projects only '
            '(detected by framework)...[/bold green]',
        )
    else:
        console.print('[bold green]Exporting all projects...[/bold green]')

    try:
        written = write_csv(
            Path(output),
            export_rows(query_repo, index, web_only),
        )
    except Exception as e:
        console.print(f"[red]Error exporting: {e}[/red]")
        raise typer.Exit(1) from e

    console.print(
        f'[bold green]Successfully exported {written:,} projects '
        f'to {output}[/bold green]',
    )


def export_rows(
    query_repo: QueryRepository,
    index: FrameworkIndex,
    web_only: bool = False,
) -> Iterator[list[object]]:
    """Stream repositories, resolving the framework by dict lookup.

    Rows arrive block by block: the old code materialised every
    repository at once, each carrying its full package list, and then
    re-walked every framework definition for every row.
    """
    for row in query_repo.stream_rows(
        EXPORT_QUERY, parameters={'direct': DIRECT},
    ):
        framework = index.detect(row['packages'] or [])
        if web_only and framework is None:
            continue
        yield [
            (row['language'] or '').lower(),
            str(framework) if framework else '',
            row['owner'] or '',
            row['repo'] or '',
            row['stars'] or 0,
            row['default_branch'] or '',
            row['latest_release'] or '',
            row['commit_sha'] or '',
            row['url'] or '',
            row['direct_dependencies'] or 0,
            row['total_dependencies'] or 0,
        ]


def write_csv(path: Path, rows: Iterator[list[object]]) -> int:
    """Write the export, returning how many data rows were written."""
    written = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)
        for row in rows:
            writer.writerow(row)
            written += 1
    return written
