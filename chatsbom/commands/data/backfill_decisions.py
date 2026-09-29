"""Write the release and commit decisions from `raw_documents`, once (#147).

Until the release and commit stages kept what they decided in the store,
it was kept in one place: the record `RecordStore` lands in ClickHouse's
`raw_documents` at the end of `chatsbom run`'s chain. The stages keep
their decisions now, from the next push each repository is collected
at; this writes the ones made before, from each repository's newest
complete record, keyed by the record's own push and chosen tag:

    chatsbom data backfill-decisions            # report what it would write
    chatsbom data backfill-decisions --apply    # write it

A record is complete when it has a push and the release stage's output,
its releases, and the newest complete one of a repository is taken even
where a newer record is not complete (its releases could not be
fetched). The files are written once, as the stages write them
(`core/decisions.py`): a decision the store has already is left, so a
second run writes nothing, and one it has differently stands, and is
counted. Run once, before phase 5 of #128 takes `raw_documents` away;
DEPLOY.md says when.
"""
from __future__ import annotations

import structlog
import typer
from rich.table import Table

from chatsbom.core import decisions
from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.decisions import Outcome
from chatsbom.core.decorators import handle_errors
from chatsbom.core.documents import RawRecords
from chatsbom.core.logging import console

logger = structlog.get_logger('backfill_decisions')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    apply: bool = typer.Option(
        False, '--apply', help='Write the files. Without this, report.',
    ),
) -> None:
    """Write the release and commit decisions from raw_documents' records.

    Each repository's newest complete record (a push, and its releases)
    gives its release decision, the release list it names and its commit
    decision, keyed by the record's own push and chosen tag. Reads only
    the database; writes only `03-github-release` and `04-github-commit`,
    never over a file that is there. Reports by default.
    """
    container = get_container()
    paths = container.config.paths
    db_config = container.config.get_db_config('admin')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
    )
    repo_db = container.get_ingestion_repository()
    try:
        report = decisions.backfill(
            RawRecords(repo_db.client).newest_with(decisions.incomplete),
            paths, apply=apply,
        )
    finally:
        close = getattr(repo_db, 'close', None)
        if callable(close):
            close()

    doing = 'Written' if apply else 'To write'
    table = Table(title='Decisions from raw_documents')
    table.add_column('', style='cyan')
    table.add_column(doing, style='green', justify='right')
    table.add_column('Kept already', justify='right')
    table.add_column('Kept differently', style='yellow', justify='right')
    table.add_column('No key', justify='right')
    for label, counted in (
        ('release decisions (03)', report.releases),
        ('release lists (03)', report.lists),
        ('commit decisions (04)', report.commits),
    ):
        table.add_row(
            label, *(
                f'{counted[outcome]:,}' for outcome in (
                    Outcome.WRITTEN, Outcome.KEPT, Outcome.CONFLICT,
                    Outcome.UNKEYED,
                )
            ),
        )
    console.print(table)

    reasons = ', '.join(
        f'{why} {count:,}' for why, count in sorted(report.incomplete.items())
    )
    console.print(f'Repositories with a record: {report.repositories:,}')
    console.print(
        f'  taken, the newest complete record of each: {report.taken:,}'
        + (
            f' ({report.older:,} older than a newer record that is not '
            'complete)' if report.older else ''
        ),
    )
    if report.incomplete:
        console.print(
            f'  with no complete record: '
            f'{sum(report.incomplete.values()):,} ({reasons})',
        )
    if report.unusable:
        console.print(
            f'  whose releases the model would not read: {report.unusable:,}',
        )
    conflicts = report.releases[Outcome.CONFLICT] + report.commits[Outcome.CONFLICT]
    if conflicts:
        console.print(
            f'[yellow]{conflicts:,} decisions the store has already, '
            'differently, stand:[/] the stages made them for the same '
            'push or key since, and the store keeps the first.',
        )
    if not report.writes:
        console.print('[green]Nothing to write.[/]')
    elif not apply:
        console.print(
            '[dim]Dry run — nothing written. Pass --apply to write.[/dim]',
        )
    logger.info(
        'Decisions backfilled' if apply else 'Decisions to backfill',
        repositories=report.repositories,
        taken=report.taken,
        writes=report.writes,
        conflicts=conflicts,
        applied=apply,
    )
