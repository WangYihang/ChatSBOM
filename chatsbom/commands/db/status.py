import structlog
import typer
from rich.markup import escape
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.diagnostics import fail
from chatsbom.core.logging import console
from chatsbom.services.db_service import DbService

logger = structlog.get_logger('db_status')
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

        # --- 2. The corpus, and what covers it ---
        # The denominator is every repository of the current search
        # snapshot, collected or not (#55, D2): a ratio over only the
        # repositories that were scanned would hide the ones that were
        # not.
        coverage = service.get_corpus_coverage(query_repo)
        corpus_table = Table(
            title=(
                'Corpus: snapshot '
                f'{escape(coverage.snapshot or "(none recorded)")}'
            ),
        )
        corpus_table.add_column('Repositories', style='cyan')
        corpus_table.add_column('Count', style='magenta', justify='right')
        corpus_table.add_column(
            'Of the corpus', style='green', justify='right',
        )
        for label, count in (
            ('In the snapshot', coverage.tracked),
            ('With any dependency data', coverage.with_dependencies),
            ('With a Syft scan', coverage.with_syft),
            ('With a dependency graph', coverage.with_depgraph),
            ('With Gradle declarations', coverage.with_manifest),
        ):
            corpus_table.add_row(
                label, f'{count:,}', _share(count, coverage.tracked),
            )
        console.print(corpus_table)
        console.print()

        # --- 3. Per ecosystem ---
        # A repository counts under every ecosystem it has, so these
        # rows overlap and do not sum to the corpus.
        eco_table = Table(title='Repositories by Ecosystem')
        eco_table.add_column('Ecosystem', style='cyan')
        eco_table.add_column('Repositories', style='magenta', justify='right')
        eco_table.add_column('Syft', justify='right')
        eco_table.add_column('Dependency graph', justify='right')
        eco_table.add_column('Gradle', justify='right')
        for eco in service.get_ecosystem_stats(query_repo):
            eco_table.add_row(
                escape(eco.ecosystem),
                f'{eco.repository_count:,}',
                f'{eco.syft_count:,}',
                f'{eco.depgraph_count:,}',
                f'{eco.manifest_count:,}',
            )
        console.print(eco_table)
        console.print()

        # --- 4. GitHub's language, an attribute: top 12 and the rest ---
        lang_table = Table(title='GitHub Languages')
        lang_table.add_column('Language', style='cyan')
        lang_table.add_column('Repositories', style='magenta', justify='right')
        for row in service.get_language_stats(query_repo):
            lang_table.add_row(
                escape(row.language or '(none)'), f'{row.repository_count:,}',
            )
        console.print(lang_table)
        console.print()

        # --- 5. Framework usage, by the framework's ecosystem ---
        for eco_stats in service.get_framework_stats(query_repo):
            fw_table = Table(
                title=f'Framework Usage — {escape(eco_stats.ecosystem)}',
            )
            fw_table.add_column('Framework', style='cyan')
            fw_table.add_column('Projects', style='magenta', justify='right')
            fw_table.add_column('Direct', style='green', justify='right')
            fw_table.add_column('Sample Projects', style='dim')

            for usage in eco_stats.frameworks:
                samples = ', '.join(
                    d.full_name for d in usage.samples
                ) or '-'
                fw_table.add_row(
                    str(usage.framework),
                    f'{usage.repository_count:,}',
                    f'{usage.direct_count:,}',
                    escape(samples),
                )

            console.print(fw_table)
            console.print()

    except Exception as e:
        # On stderr, exiting 1. Printed among the tables and exiting 0,
        # it was read by a script as part of the status, and the run as
        # a success. The tables printed before it stay: they are true.
        fail(
            f'[red]Error fetching status: {escape(str(e))}[/red]',
            'Error fetching status', logger, error=str(e),
        )


def _share(part: int, whole: int) -> str:
    """`part` as a percentage of `whole`, or a dash with no whole."""
    return f'{part / whole:.1%}' if whole else '-'
