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
        for k, v in stats.items():
            overview.add_row(k.replace('_', ' ').title(), f'{v:,}')
        console.print(overview)
        console.print()

        # --- 2. Per-Language Statistics ---
        lang_table = Table(title='Repositories by Language')
        lang_table.add_column('Language', style='cyan')
        lang_table.add_column('Repositories', style='magenta', justify='right')
        for lang_name, count in service.get_language_stats(query_repo):
            lang_table.add_row(lang_name or '(unknown)', f'{count:,}')
        console.print(lang_table)
        console.print()

        # --- 3. Per-Language Framework Usage + Samples ---
        framework_stats = service.get_framework_stats(query_repo)
        for lang_data in framework_stats:
            lang_name = lang_data['language']
            fw_table = Table(
                title=f'Framework Usage — {lang_name.capitalize()}',
            )
            fw_table.add_column('Framework', style='cyan')
            fw_table.add_column('Projects', style='magenta', justify='right')
            fw_table.add_column('Sample Projects', style='dim')

            for fw_data in lang_data['frameworks']:
                sample_str = ', '.join(
                    f'{owner}/{repo}' for owner, repo, *_ in fw_data['samples']
                ) if fw_data['samples'] else '-'
                fw_table.add_row(
                    fw_data['framework'],
                    f"{fw_data['count']:,}", sample_str,
                )

            console.print(fw_table)
            console.print()

    except Exception as e:
        console.print(f"[red]Error fetching status: {e}[/red]")
