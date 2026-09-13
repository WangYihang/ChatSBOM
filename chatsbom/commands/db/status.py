import typer
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.services.db_service import DbService

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main():
    """Show database statistics."""

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
    service = DbService()

    try:
        stats = service.get_db_stats(query_repo)

        # --- 1. Overall Statistics ---
        overview = Table(title='Database Statistics')
        overview.add_column('Metric', style='cyan')
        overview.add_column('Value', style='magenta')
        overview.add_row('Repositories', f'{stats.repositories:,}')
        overview.add_row('Artifacts', f'{stats.artifacts:,}')
        overview.add_row('Releases', f'{stats.releases:,}')
        console.print(overview)
        console.print()

        # --- 2. Per-Language Statistics ---
        lang_table = Table(title='Repositories by Language')
        lang_table.add_column('Language', style='cyan')
        lang_table.add_column('Repositories', style='magenta', justify='right')
        for row in service.get_language_stats(query_repo):
            lang_table.add_row(
                row.language or '(unknown)', f'{row.repository_count:,}',
            )
        console.print(lang_table)
        console.print()

        # --- 3. Per-Language Framework Usage + Samples ---
        for lang_stats in service.get_framework_stats(query_repo):
            fw_table = Table(
                title=f'Framework Usage — {str(lang_stats.language).capitalize()}',
            )
            fw_table.add_column('Framework', style='cyan')
            fw_table.add_column('Projects', style='magenta', justify='right')
            fw_table.add_column('Direct', style='green', justify='right')
            fw_table.add_column('Sample Projects', style='dim')

            for usage in lang_stats.frameworks:
                samples = ', '.join(
                    d.full_name for d in usage.samples
                ) or '-'
                fw_table.add_row(
                    str(usage.framework),
                    f'{usage.repository_count:,}',
                    f'{usage.direct_count:,}',
                    samples,
                )

            console.print(fw_table)
            console.print()

    except Exception as e:
        console.print(f"[red]Error fetching status: {e}[/red]")
