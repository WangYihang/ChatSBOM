import typer
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.config import get_config
from chatsbom.core.repository import SBOMRepository
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory

console = Console()


def main(
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    user: str = typer.Option(None, help='ClickHouse user'),
    password: str = typer.Option(None, help='ClickHouse password'),
    database: str = typer.Option(None, help='ClickHouse database'),
):
    """
    Show database statistics and status.
    """
    # Load config and override with CLI arguments
    config = get_config()
    db_config = DatabaseConfig(
        host=host or config.database.host,
        port=port or config.database.port,
        user=user or config.database.user,
        password=password or config.database.password,
        database=database or config.database.database,
    )

    try:
        repo = SBOMRepository(db_config)
    except Exception as e:
        console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
        raise typer.Exit(code=1)

    # 1. Total Counts
    try:
        total_repos = repo.get_repository_count()
        total_artifacts = repo.get_artifact_count()
    except Exception as e:
        console.print(f"[red]Failed to query total counts: {e}[/red]")
        raise typer.Exit(code=1)

    console.print(
        Panel.fit(
            f"[bold blue]Database Summary[/bold blue]\n\n"
            f"Total Repositories: [bold green]{total_repos:,}[/bold green]\n"
            f"Total Artifacts (Dependencies): [bold green]{total_artifacts:,}[/bold green]",
            title='ChatSBOM Statistics',
        ),
    )

    # 2. Languages Distribution
    try:
        lang_res = list(repo.get_repositories_by_language())
    except Exception as e:
        console.print(f"[red]Failed to query language distribution: {e}[/red]")
        lang_res = []

    if lang_res:
        table = Table(title='Top Programming Languages')
        table.add_column('Language', style='cyan')
        table.add_column('Repository Count', style='magenta', justify='right')
        for lang, cnt in lang_res:
            table.add_row(lang or 'Unknown', f"{cnt:,}")
        console.print(table)

    # 3. Top Dependencies Across All Projects
    try:
        top_deps = [(d['name'], d['repo_count'])
                    for d in repo.get_top_dependencies(limit=20)]
    except Exception as e:
        console.print(f"[red]Failed to query top dependencies: {e}[/red]")
        top_deps = []

    if top_deps:
        table = Table(title='Top 20 Most Popular Dependencies')
        table.add_column('Library Name', style='green')
        table.add_column('Project Count', style='magenta', justify='right')
        for name, cnt in top_deps:
            table.add_row(name, f"{cnt:,}")
        console.print(table)

    # 4. Dependency Types Distribution
    try:
        type_res = repo.client.query(
            f'SELECT type, count() as cnt FROM {db_config.artifacts_table} GROUP BY type ORDER BY cnt DESC',
        ).result_rows
    except Exception as e:
        console.print(
            f"[red]Failed to query dependency type distribution: {e}[/red]",
        )
        type_res = []

    if type_res:
        table = Table(title='Dependency Types')
        table.add_column('Type', style='yellow')
        table.add_column('Count', style='magenta', justify='right')
        for dep_type, cnt in type_res:
            table.add_row(dep_type or 'Unknown', f"{cnt:,}")
        console.print(table)

    # 5. Framework Statistics
    console.print(Rule('Framework Statistics'))
    try:
        lang_counts = {
            row[0]: row[1] for row in repo.client.query(
                f'SELECT language, count() FROM {db_config.repositories_table} GROUP BY language',
            ).result_rows
        }
    except Exception as e:
        console.print(f"[red]Failed to query language counts: {e}[/red]")
        lang_counts = {}

    framework_table = Table(title='Framework Usage Statistics')
    framework_table.add_column('Language', style='cyan')
    framework_table.add_column('Framework', style='green')
    framework_table.add_column(
        'Project Count', style='magenta', justify='right',
    )
    framework_table.add_column('Percentage', style='yellow', justify='right')

    for lang_enum in Language:
        try:
            handler = LanguageFactory.get_handler(lang_enum)
            frameworks = handler.get_frameworks()
            lang_total = lang_counts.get(lang_enum.value, 0)

            if not frameworks or lang_total == 0:
                continue

            for fw_enum in frameworks:
                fw_instance = FrameworkFactory.create(fw_enum)
                package_names = fw_instance.get_package_names()

                # Query count of projects in this language using this framework
                fw_count_query = f"""
                SELECT count(DISTINCT r.id)
                FROM {db_config.repositories_table} r
                JOIN {db_config.artifacts_table} a ON r.id = a.repository_id
                WHERE r.language = {{lang:String}} AND a.name IN {{pkgs:Array(String)}}
                """
                fw_count = repo.client.query(
                    fw_count_query,
                    parameters={
                        'lang': lang_enum.value,
                        'pkgs': package_names,
                    },
                ).result_rows[0][0]

                percentage = (
                    fw_count / lang_total *
                    100
                ) if lang_total > 0 else 0
                framework_table.add_row(
                    lang_enum.value,
                    fw_enum.value,
                    f"{fw_count:,}",
                    f"{percentage:.2f}%",
                )
        except Exception:
            continue

    if framework_table.row_count > 0:
        console.print(framework_table)

    # 6. Top Projects per Framework
    console.print(Rule('Top Starred Projects per Framework'))
    top_projects_table = Table(title='Top 3 Starred Projects per Framework')
    top_projects_table.add_column('Language', style='cyan')
    top_projects_table.add_column('Framework', style='green')
    top_projects_table.add_column('Repository', style='bold')
    top_projects_table.add_column('Stars', style='magenta', justify='right')
    top_projects_table.add_column('URL', style='blue')

    for lang_enum in Language:
        try:
            handler = LanguageFactory.get_handler(lang_enum)
            frameworks = handler.get_frameworks()
            if not frameworks:
                continue

            for fw_enum in frameworks:
                fw_instance = FrameworkFactory.create(fw_enum)
                package_names = fw_instance.get_package_names()

                # Query top 3 repositories for this framework
                top_repos_query = f"""
                SELECT DISTINCT r.full_name, r.stars, r.url
                FROM {db_config.repositories_table} r
                JOIN {db_config.artifacts_table} a ON r.id = a.repository_id
                WHERE r.language = {{lang:String}} AND a.name IN {{pkgs:Array(String)}}
                ORDER BY r.stars DESC
                LIMIT 3
                """
                top_repos = repo.client.query(
                    top_repos_query,
                    parameters={
                        'lang': lang_enum.value,
                        'pkgs': package_names,
                    },
                ).result_rows

                for full_name, stars, url in top_repos:
                    top_projects_table.add_row(
                        lang_enum.value,
                        fw_enum.value,
                        full_name,
                        f"{stars:,}",
                        url,
                    )
        except Exception:
            continue

    if top_projects_table.row_count > 0:
        console.print(top_projects_table)
