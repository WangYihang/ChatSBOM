import typer
from rich.console import Console
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory

console = Console()


@handle_errors
def main(
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    user: str = typer.Option(None, help='ClickHouse user'),
    password: str = typer.Option(None, help='ClickHouse password'),
    database: str = typer.Option(None, help='ClickHouse database'),
):
    """Show database statistics and status (Guest)."""
    container = get_container()

    # CLI Overrides
    if host:
        container.config._db_base.host = host
    if port:
        container.config._db_base.port = port
    if database:
        container.config._db_base.database = database
    # User/Pass ignored for Guest access unless explicit env var overrides via config logic

    with container.get_query_repository() as repo:
        # 1. Overview
        stats = repo.get_stats()
        console.print(
            Panel.fit(
                f"Total Repositories: [bold green]{stats['repositories']:,}[/]\n"
                f"Total Artifacts: [bold green]{stats['artifacts']:,}[/]",
                title='ChatSBOM Overview',
            ),
        )

        # 2. Languages
        lang_table = Table(title='Programming Languages')
        lang_table.add_column('Language', style='cyan')
        lang_table.add_column('Count', justify='right')
        lang_totals = {}
        for lang, count in repo.get_language_stats():
            lang_table.add_row(lang or 'Unknown', f"{count:,}")
            lang_totals[lang] = count
        console.print(lang_table)

        # 3. Top Dependencies
        dep_table = Table(title='Top 20 Dependencies')
        dep_table.add_column('Library', style='green')
        dep_table.add_column('Projects', justify='right')
        for d in repo.get_top_dependencies(limit=20):
            dep_table.add_row(d['name'], f"{d['repo_count']:,}")
        console.print(dep_table)

        # 4. Dependency Types
        type_table = Table(title='Dependency Types')
        type_table.add_column('Type', style='yellow')
        type_table.add_column('Count', justify='right')
        for dtype, count in repo.get_dependency_type_distribution():
            type_table.add_row(dtype or 'Unknown', f"{count:,}")
        console.print(type_table)

        # 5. Frameworks
        console.print(Rule('Framework Usage'))
        fw_table = Table(title='Framework Statistics')
        fw_table.add_column('Language', style='cyan')
        fw_table.add_column('Framework', style='green')
        fw_table.add_column('Count', justify='right')
        fw_table.add_column('Percentage', justify='right')

        for lang_enum in Language:
            handler = LanguageFactory.get_handler(lang_enum)
            frameworks = handler.get_frameworks()
            lang_total = lang_totals.get(lang_enum.value, 0)

            if not frameworks or lang_total == 0:
                continue

            for fw_enum in frameworks:
                fw = FrameworkFactory.create(fw_enum)
                count = repo.get_framework_usage(
                    lang_enum.value, fw.get_package_names(),
                )
                percentage = (
                    count / lang_total *
                    100
                ) if lang_total > 0 else 0
                fw_table.add_row(
                    lang_enum.value, fw_enum.value,
                    f"{count:,}", f"{percentage:.2f}%",
                )
        console.print(fw_table)

        # 6. Top Projects
        console.print(Rule('Top Projects per Framework'))
        top_table = Table(title='Top 3 Starred Projects per Framework')
        top_table.add_column('Framework', style='green')
        top_table.add_column('Repository', style='bold')
        top_table.add_column('Stars', justify='right')
        top_table.add_column('URL', style='blue')

        for lang_enum in Language:
            frameworks = LanguageFactory.get_handler(
                lang_enum,
            ).get_frameworks()
            for fw_enum in frameworks:
                fw = FrameworkFactory.create(fw_enum)
                top_repos = repo.get_top_projects_by_framework(
                    lang_enum.value, fw.get_package_names(),
                )
                for full_name, stars, url in top_repos:
                    top_table.add_row(
                        fw_enum.value, full_name, f"{stars:,}", url,
                    )
        console.print(top_table)


if __name__ == '__main__':
    typer.run(main)
