import clickhouse_connect
import typer
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

from sbom_insight.models.framework import FrameworkFactory
from sbom_insight.models.language import Language
from sbom_insight.models.language import LanguageFactory

console = Console()


def main(
    host: str = typer.Option('localhost', help='ClickHouse host'),
    port: int = typer.Option(8123, help='ClickHouse http port'),
    user: str = typer.Option('guest', help='ClickHouse user'),
    password: str = typer.Option('guest', help='ClickHouse password'),
    database: str = typer.Option('sbom', help='ClickHouse database'),
):
    """
    Summarize database statistics.
    """
    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database,
        )
    except Exception as e:
        console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
        raise typer.Exit(code=1)

    # 1. Total Counts
    try:
        total_repos = client.query(
            'SELECT count() FROM repositories',
        ).result_rows[0][0]
        total_artifacts = client.query(
            'SELECT count() FROM artifacts',
        ).result_rows[0][0]
    except Exception as e:
        console.print(f"[red]Failed to query total counts: {e}[/red]")
        raise typer.Exit(code=1)

    console.print(
        Panel.fit(
            f"[bold blue]Database Summary[/bold blue]\n\n"
            f"Total Repositories: [bold green]{total_repos:,}[/bold green]\n"
            f"Total Artifacts (Dependencies): [bold green]{total_artifacts:,}[/bold green]",
            title='SBOM Insight Statistics',
        ),
    )

    # 2. Languages Distribution
    try:
        lang_res = client.query(
            'SELECT language, count() as cnt FROM repositories GROUP BY language ORDER BY cnt DESC',
        ).result_rows
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
        top_deps = client.query(
            'SELECT name, count() as cnt FROM artifacts GROUP BY name ORDER BY cnt DESC LIMIT 20',
        ).result_rows
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
        type_res = client.query(
            'SELECT type, count() as cnt FROM artifacts GROUP BY type ORDER BY cnt DESC',
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
            row[0]: row[1] for row in client.query(
                'SELECT language, count() FROM repositories GROUP BY language',
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
                fw_count_query = """
                SELECT count(DISTINCT r.id)
                FROM repositories r
                JOIN artifacts a ON r.id = a.repository_id
                WHERE r.language = {lang:String} AND a.name IN {pkgs:Array(String)}
                """
                fw_count = client.query(
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
