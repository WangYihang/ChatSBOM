import csv

import structlog
import typer

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory

logger = structlog.get_logger('db_export')
app = typer.Typer()


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
):
    """
    Export projects and their frameworks to a CSV file.
    """
    container = get_container()
    config = container.config

    # Check Connection (Guest)
    db_config = config.get_db_config('guest')
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
    client = query_repo.client

    if web_only:
        console.print(
            '[bold green]Exporting web projects only (detected by framework)...[/bold green]',
        )
    else:
        console.print('[bold green]Exporting all projects...[/bold green]')

    query = """
    SELECT
        r.language,
        groupArray(DISTINCT a.name) AS pkgs,
        r.owner,
        r.repo,
        r.stars,
        r.default_branch,
        r.latest_release_tag,
        r.sbom_commit_sha,
        r.url
    FROM repositories AS r FINAL
    LEFT JOIN artifacts AS a FINAL ON r.id = a.repository_id
    GROUP BY
        r.id, r.language, r.owner, r.repo, r.stars,
        r.default_branch, r.latest_release_tag, r.sbom_commit_sha, r.url
    ORDER BY r.stars DESC
    """

    try:
        rows = client.query(query).result_rows
        exported_count = 0

        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'language', 'framework', 'owner', 'repo', 'stars',
                'default_branch', 'latest_release', 'commit_sha', 'url',
            ])

            for row in rows:
                lang_str = row[0]
                language = (lang_str or '').lower()
                pkgs = row[1]
                owner = row[2]
                repo = row[3]
                stars = row[4]
                default_branch = row[5]
                latest_release = row[6]
                commit_sha = row[7]
                url = row[8]

                framework = ''
                if lang_str:
                    try:
                        lang_enum = Language(lang_str.lower())
                        handler = LanguageFactory.get_handler(lang_enum)
                        frameworks = handler.get_frameworks()
                        for fw in frameworks:
                            fw_handler = FrameworkFactory.create(fw)
                            fw_pkgs = fw_handler.get_package_names()
                            if any(p in pkgs for p in fw_pkgs):
                                framework = str(fw)
                                break
                    except ValueError:
                        pass

                # All tracked frameworks in this project are web frameworks.
                # When --web-only is enabled, skip repositories without a detected framework.
                if web_only and not framework:
                    continue

                writer.writerow([
                    language, framework, owner or '', repo or '',
                    stars or 0, default_branch or '', latest_release or '',
                    commit_sha or '', url or '',
                ])
                exported_count += 1

        console.print(
            f'[bold green]Successfully exported {exported_count} projects to {output}[/bold green]',
        )

    except Exception as e:
        console.print(f"[red]Error exporting: {e}[/red]")
