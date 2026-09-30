"""Move `data/` and `.cache/` to the repository-keyed layout (#55, §7).

    chatsbom data migrate-layout --inventory     # pre.tsv: every file
    chatsbom data migrate-layout                 # dry run: plan.tsv
    chatsbom data migrate-layout --apply         # move, adopt
    chatsbom data migrate-layout --verify        # compare with pre.tsv
    chatsbom data migrate-layout --rollback      # undo all of it

The dry run is the default and writes nothing but its report and
`plan.tsv`, into `--workdir` (by default `data/_migration`): it opens the
ledger read-only and immutable. See `chatsbom/core/migrate_layout.py` for
what makes the apply safe to interrupt and to undo.

Files and the ledger alone: the rewrite of ClickHouse's `raw_documents`
paths, and the scratch database its equivalence check was run in, went
with the server (#153).
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import humanize
import structlog
import typer
from rich.markup import escape
from rich.table import Table

from chatsbom.core import migrate_layout as ml
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console

logger = structlog.get_logger('migrate_layout')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    apply: bool = typer.Option(
        False, '--apply',
        help='Carry out plan.tsv: move, and adopt the ledger',
    ),
    verify: bool = typer.Option(
        False, '--verify', help='Check the result against pre.tsv',
    ),
    rollback: bool = typer.Option(
        False, '--rollback',
        help='Undo the journal and the ledger',
    ),
    inventory: bool = typer.Option(
        False, '--inventory',
        help='Write pre.tsv: every file, with a 1% sample hashed',
    ),
    workdir: Path | None = typer.Option(
        None, '--workdir',
        help='Where plan, journal and inventories go (default data/_migration)',
    ),
    resolve: str | None = typer.Option(
        None, '--resolve',
        help='`newest`: set each conflict\'s older copy aside, not abort',
    ),
    batch: int = typer.Option(
        ml.DEFAULT_BATCH, help='Renames per journal fsync',
    ),
    search_lists: bool = typer.Option(
        True, '--search-lists/--no-search-lists',
        help='Also read 01-github-search to name repositories',
    ),
    archive_lists: bool = typer.Option(
        False, '--archive-lists',
        help='Also plan moving the per-language <lang>.jsonl lists to '
        '_legacy-lists/ and all.jsonl to all-<date>.jsonl (design §7.1). '
        'Off by default: the stage-major commands still read them.',
    ),
) -> None:
    """
    Move every stage artefact under its repository's id, journaled.

    `<stage>/<lang>/<owner>/<repo>/<ref>/<sha>` becomes
    `<stage>/<repository_id>/<sha>`; the legacy dependency graph goes to
    `09-github-depgraph/<id>/legacy/`; the Syft and tree caches lose
    their ref level; the unversioned Syft cache goes to
    `.cache/syft/_unversioned/`. Renames only, on one filesystem: nothing
    is copied, fetched or deleted.

    Reports by default. Stop every collector first.
    """
    container = get_container()
    paths = container.config.paths
    roots = ml.Roots.of(paths.base_data_dir, paths.cache_dir)
    work = Path(workdir) if workdir else roots.data / '_migration'
    # Each refusal, here and below, is said where the logs go: stdout is
    # for the report, and these were printed there (#124). These two are
    # usage errors, and exit 2, as they did.
    if resolve not in (None, 'newest'):
        say(
            '[bold red]--resolve takes only `newest`.[/]',
            '--resolve takes only newest', logger, 'error', given=resolve,
        )
        raise typer.Exit(2)
    modes = {
        '--inventory': inventory, '--apply': apply, '--verify': verify,
        '--rollback': rollback,
    }
    if sum(modes.values()) > 1:
        say(
            '[bold red]One of --inventory, --apply, --verify, '
            '--rollback at a time.[/]',
            'One of --inventory, --apply, --verify and --rollback at a '
            'time', logger, 'error',
            options=[flag for flag, given in modes.items() if given],
        )
        raise typer.Exit(2)

    if inventory:
        _inventory(roots, work)
    elif apply:
        _apply(container, roots, work, batch)
    elif verify:
        _verify(container, roots, work)
    elif rollback:
        _rollback(container, roots, work)
    else:
        _dry_run(
            container, roots, work, resolve == 'newest', search_lists,
            archive_lists,
        )


# -- dry run ---------------------------------------------------------------

def _dry_run(
    container: Any,
    roots: ml.Roots,
    work: Path,
    resolve_newest: bool,
    search_lists: bool,
    archive_lists: bool = False,
) -> None:
    started = time.monotonic()
    ledger_path = container.config.paths.ledger_path
    # How far it has got goes through the logger, on stderr: stdout is
    # the report, and a machine reading stderr gets JSON.
    logger.info('Reading names and ids', ledger='read-only')
    resolver = ml.build_resolver(
        roots.data, ledger_path, search_lists=search_lists,
    )
    names = time.monotonic()

    def progress(label: str, count: int) -> None:
        if count % 10000 == 0:
            logger.info('Walking the old layout', root=label, units=count)

    logger.info('Walking the old layout')
    plan = ml.make_plan(
        roots, resolver, resolve_newest=resolve_newest,
        archive_lists=archive_lists, progress=progress,
    )
    walked = time.monotonic()

    work.mkdir(parents=True, exist_ok=True)
    ml.write_plan(plan, work / ml.PLAN)
    summary = plan.summary()
    summary['seconds'] = {
        'names': round(names - started, 1),
        'walk': round(walked - names, 1),
        'total': round(time.monotonic() - started, 1),
    }
    (work / 'dry-run.json').write_text(
        json.dumps(summary, indent=2, sort_keys=True) + '\n',
        encoding='utf-8',
    )
    _report(summary)
    # Every path, name and error below is escaped where it meets markup:
    # a `[bold]` in a directory name was taken for a tag, and a `[/dim]`
    # raised MarkupError.
    console.print(f'\n[dim]Plan: {escape(str(work / ml.PLAN))}[/dim]')
    if summary['unresolved']:
        fail(
            f"[bold red]{summary['unresolved']:,} conflicts[/] — the plan "
            'cannot be applied. See the `# conflict` lines in plan.tsv, '
            'or plan again with [cyan]--resolve newest[/].',
            'The plan has conflicts', logger,
            conflicts=summary['unresolved'], plan=str(work / ml.PLAN),
        )


def _report(summary: dict[str, Any]) -> None:
    table = Table(title='Planned moves')
    for column in (
        'Root', 'Units', 'Files', 'Size', 'Renames', 'Dedup', 'Aside', 'Meta',
    ):
        table.add_column(
            column, justify='left' if column ==
            'Root' else 'right',
        )
    for root, row in summary['roots'].items():
        table.add_row(
            root, f"{row['units']:,}", f"{row['files']:,}",
            humanize.naturalsize(row['bytes'], binary=True),
            f"{row['moves']:,}", f"{row['dedup']:,}", f"{row['aside']:,}",
            f"{row['meta']:,}",
        )
    console.print(table)
    console.print(f"Conflicts: {summary['conflicts'] or 'none'}")
    if summary.get('lists_archived'):
        console.print(f"Lists archived: {summary['lists_archived']:,}")
    for name, how in (summary.get('settled') or {}).items():
        console.print(
            '[yellow]Name worn by two ids, settled[/]: '
            f'{escape(name)}: {escape(how)}',
        )
    console.print(
        f"Repositories under more than one spelling: "
        f"{summary['renamed_or_recased']:,}",
    )


# -- inventory ---------------------------------------------------------------

def _inventory(roots: ml.Roots, work: Path) -> None:
    work.mkdir(parents=True, exist_ok=True)
    totals = ml.inventory(roots, work / ml.PRE)
    table = Table(title='Inventory')
    table.add_column('Root')
    table.add_column('Files', justify='right')
    table.add_column('Size', justify='right')
    for root in ml.ROOTS:
        counts = totals.get(root)
        if counts:
            table.add_row(
                root, f"{counts['files']:,}",
                humanize.naturalsize(counts['bytes'], binary=True),
            )
    console.print(table)
    console.print(f'[dim]{escape(str(work / ml.PRE))}[/dim]')


# -- apply -----------------------------------------------------------------

def _apply(container: Any, roots: ml.Roots, work: Path, batch: int) -> None:
    plan_path = work / ml.PLAN
    if not plan_path.exists():
        fail(
            '[bold red]No plan.[/] Run the dry run first.',
            'No plan to apply', logger, plan=str(plan_path),
        )
    if not (work / ml.PRE).exists():
        fail(
            '[bold red]No inventory.[/] Run [cyan]--inventory[/] first: '
            '`--verify` compares with it.',
            'No inventory to verify against', logger,
            inventory=str(work / ml.PRE),
        )
    ops, summary, unresolved = ml.read_plan(plan_path)
    if unresolved:
        fail(
            f'[bold red]{unresolved:,} open conflicts in the plan.[/] '
            'Resolve them or plan with --resolve newest.',
            'The plan has open conflicts', logger,
            conflicts=unresolved, plan=str(plan_path),
        )

    paths = container.config.paths
    # Step 2: the ledger as it was, before anything moves.
    if paths.ledger_path.exists() and ml.backup_ledger(
        paths.ledger_path, work / ml.LEDGER_BACKUP,
    ):
        console.print(
            '[dim]Ledger backed up to '
            f'{escape(str(work / ml.LEDGER_BACKUP))}[/dim]',
        )

    # Step 5: the renames. How far they have got goes to the logger, as
    # the dry run's does.
    def progress(done: int, total: int) -> None:
        if done == total or done % (batch * 40) == 0:
            logger.info('Renaming', done=done, total=total)

    started = time.monotonic()
    result = ml.apply_plan(
        ops, work, batch=batch,
        meta_for=lambda document: ml.legacy_graph_meta(
            document, ml.repository_of(document),
        ),
        progress=progress,
        stops=ml.stops_for(roots),
    )
    console.print(
        f'[green]Moved[/] {result.renamed:,} · wrote {result.written:,} · '
        f'resumed {result.resumed:,} · removed {result.removed_dirs:,} empty '
        f'directories in {time.monotonic() - started:.0f}s',
    )

    # Step 7: the ledger. Opening it adopts the watermarks.
    with Ledger(paths.ledger_path) as ledger:
        ledger.adopt_watermarks()
        rows = ledger._db.execute(
            'SELECT count(*) FROM stage_state',
        ).fetchone()[0]
    console.print(
        f'[green]Ledger[/]: {rows:,} stage_state rows (watermarks adopted)',
    )
    console.print(
        '[dim]Next: [cyan]chatsbom data migrate-layout --verify[/cyan][/dim]',
    )


# -- verify ------------------------------------------------------------------

def _verify(container: Any, roots: ml.Roots, work: Path) -> None:
    for required in (ml.PLAN, ml.PRE, ml.JOURNAL):
        if not (work / required).exists():
            fail(
                f'[bold red]Missing {escape(str(work / required))}.[/]',
                'A file --verify needs is missing', logger,
                path=str(work / required),
            )
    checks = ml.verify_files(roots, work)
    with Ledger(container.config.paths.ledger_path) as ledger:
        rows = ledger._db.execute(
            'SELECT count(*) FROM stage_state',
        ).fetchone()[0]
        unadopted = ledger._db.execute(
            """
            SELECT count(*) FROM repository_state AS r, json_each(r.stage_watermarks) AS w
            WHERE w.key IN ('release', 'commit', 'tree', 'content', 'lock',
                            'sbom', 'depgraph')
              AND NOT EXISTS (SELECT 1 FROM stage_state AS s
                              WHERE s.repository_id = r.repository_id
                                AND s.stage = w.key)
            """,
        ).fetchone()[0]
    checks.append(
        ml.Check(
            'ledger: every watermark has a stage_state row', unadopted == 0,
            f'{rows:,} rows; {unadopted:,} watermarks without one',
        ),
    )

    table = Table(title='Verification')
    table.add_column('Check')
    table.add_column('', justify='center')
    table.add_column('Detail', overflow='fold')
    for check in checks:
        # A cell is read as markup, and a check names the files it found.
        table.add_row(
            escape(check.name),
            '[green]ok[/]' if check.ok else '[bold red]FAIL[/]',
            escape(check.detail),
        )
    console.print(table)
    (work / 'verify.json').write_text(
        json.dumps(
            [check.__dict__ for check in checks], indent=2,
        ) + '\n',
        encoding='utf-8',
    )
    if not all(check.ok for check in checks):
        raise typer.Exit(1)


# -- rollback ------------------------------------------------------------------

def _rollback(container: Any, roots: ml.Roots, work: Path) -> None:
    result = ml.rollback(work)
    console.print(
        f'[green]Restored[/] {result.restored:,} renames · deleted '
        f'{result.deleted:,} written files · recreated '
        f'{result.recreated_dirs:,} directories · removed '
        f'{result.removed_dirs:,}',
    )
    backup = work / ml.LEDGER_BACKUP
    if backup.exists():
        ml.restore_ledger(backup, container.config.paths.ledger_path)
        console.print(f'[green]Ledger restored[/] from {escape(str(backup))}')
    console.print(
        '[dim]Then: check out the code from before this change, and run '
        '`--verify`-style counts against pre.tsv (`--inventory --workdir '
        '<elsewhere>` and compare).[/dim]',
    )
