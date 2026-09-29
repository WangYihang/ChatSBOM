"""Move `data/` and `.cache/` to the repository-keyed layout (#55, §7).

    chatsbom data migrate-layout --inventory     # pre.tsv: every file
    chatsbom data migrate-layout                 # dry run: plan.tsv
    chatsbom data migrate-layout --apply         # move, rewrite, adopt
    chatsbom data migrate-layout --verify        # compare with pre.tsv
    chatsbom data migrate-layout --rollback      # undo all of it

The dry run is the default and writes nothing but its report and
`plan.tsv`, into `--workdir` (by default `data/_migration`): it opens the
ledger read-only and immutable, and asks the database only read-only
questions. See `chatsbom/core/migrate_layout.py` for what makes the
apply safe to interrupt and to undo.
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
from chatsbom.core.redact import redact_urls

logger = structlog.get_logger('migrate_layout')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    apply: bool = typer.Option(
        False, '--apply',
        help='Carry out plan.tsv: move, rewrite raw_documents, adopt the ledger',
    ),
    verify: bool = typer.Option(
        False, '--verify', help='Check the result against pre.tsv',
    ),
    rollback: bool = typer.Option(
        False, '--rollback',
        help='Undo the journal, the raw_documents rewrite and the ledger',
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
    no_db: bool = typer.Option(
        False, '--no-db', help='Leave the database out: files and ledger only',
    ),
    prepare_scratch: str | None = typer.Option(
        None, '--prepare-scratch',
        help='Create this database with a copy of raw_documents, for '
        '`CLICKHOUSE_DB=<it> chatsbom db index --rebuild`',
    ),
    scratch_db: str | None = typer.Option(
        None, '--scratch-db',
        help='With --verify: compare current artifacts with this database',
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
        '--rollback': rollback, '--prepare-scratch': bool(prepare_scratch),
    }
    if sum(modes.values()) > 1:
        say(
            '[bold red]One of --inventory, --apply, --verify, '
            '--rollback, --prepare-scratch at a time.[/]',
            'One of --inventory, --apply, --verify, --rollback and '
            '--prepare-scratch at a time', logger, 'error',
            options=[flag for flag, given in modes.items() if given],
        )
        raise typer.Exit(2)

    if inventory:
        _inventory(roots, work)
    elif apply:
        _apply(container, roots, work, batch, no_db)
    elif verify:
        _verify(container, roots, work, no_db, scratch_db)
    elif rollback:
        _rollback(container, roots, work, no_db)
    elif prepare_scratch:
        _prepare_scratch(container, prepare_scratch)
    else:
        _dry_run(
            container, roots, work, resolve == 'newest', no_db, search_lists,
            archive_lists,
        )


# -- dry run ---------------------------------------------------------------

def _dry_run(
    container: Any,
    roots: ml.Roots,
    work: Path,
    resolve_newest: bool,
    no_db: bool,
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

    raw: dict[str, Any] = {}
    if not no_db:
        try:
            client = container.get_query_repository().client
            raw['counts'] = ml.raw_counts(client)
            raw['consistency'] = {
                kind: dict(counts)
                for kind, counts in ml.raw_consistency(
                    ml.raw_targets(client), roots.data, plan.ops,
                ).items()
            }
        except Exception as error:  # noqa: BLE001 - reported, not fatal
            # Kept in dry-run.json and printed: the server's words,
            # without the query of any URL they quote.
            raw['error'] = redact_urls(str(error))

    work.mkdir(parents=True, exist_ok=True)
    ml.write_plan(plan, work / ml.PLAN)
    summary = plan.summary()
    summary['raw_documents'] = raw
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
    raw = summary.get('raw_documents') or {}
    if raw.get('counts'):
        console.print(
            'raw_documents rewrites by kind: ' + ', '.join(
                f"{kind} {c['rewrite']:,}/{c['rows']:,}"
                for kind, c in raw['counts'].items()
            ),
        )
    if raw.get('consistency'):
        console.print(
            'raw_documents rows that will name a file: ' + ', '.join(
                f'{kind} {dict(c)}' for kind, c in raw['consistency'].items()
            ),
        )
    if raw.get('error'):
        console.print(
            f"[yellow]raw_documents not read:[/] {escape(raw['error'])}",
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

def _apply(
    container: Any, roots: ml.Roots, work: Path, batch: int, no_db: bool,
) -> None:
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

    raw_before: dict[str, Any] = {}
    client = None
    repo_db = None
    if not no_db:
        repo_db = container.get_ingestion_repository()
        client = repo_db.client
        raw_before = ml.raw_counts(client)
        record = work / 'raw-before.json'
        if not record.exists():
            record.write_text(
                json.dumps(
                    raw_before, indent=2,
                ), encoding='utf-8',
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

    # Step 6: raw_documents paths, the columns added first.
    if client is not None and repo_db is not None:
        repo_db.ensure_schema()
        changed = ml.rewrite_raw(client)
        console.print(
            '[green]Rewrote raw_documents paths[/]: '
            + ', '.join(f'{k} {v:,}' for k, v in changed.items()),
        )

    # Step 7: the ledger. Opening it adopts the watermarks; GitHub's
    # language is filled from the newest metadata where it is empty.
    with Ledger(paths.ledger_path) as ledger:
        ledger.adopt_watermarks()
        rows = ledger._db.execute(
            'SELECT count(*) FROM stage_state',
        ).fetchone()[0]
        filled = _fill_github_language(ledger, client)
    console.print(
        f'[green]Ledger[/]: {rows:,} stage_state rows (watermarks adopted), '
        f'github_language filled {filled:,}',
    )
    console.print(
        '[dim]Next: [cyan]chatsbom data migrate-layout --verify[/cyan][/dim]',
    )


def _fill_github_language(ledger: Ledger, client: Any) -> int:
    """GitHub's language from the newest `repo-metadata`, where the
    ledger has none. An attribute only; nothing is keyed by it."""
    if client is None:
        return 0
    rows = client.query(
        "SELECT repository_id, argMax(JSONExtractString(body, 'language'), "
        "fetched_at) FROM raw_documents WHERE kind = 'repo-metadata' "
        'GROUP BY repository_id',
    ).result_rows
    filled = 0
    with ledger.transaction():
        for repository_id, language in rows:
            if not language:
                continue
            filled += ledger._db.execute(
                'UPDATE repository_state SET github_language = ? '
                "WHERE repository_id = ? AND github_language = ''",
                (str(language), int(repository_id)),
            ).rowcount
    return filled


# -- verify ------------------------------------------------------------------

def _verify(
    container: Any,
    roots: ml.Roots,
    work: Path,
    no_db: bool,
    scratch_db: str | None,
) -> None:
    for required in (ml.PLAN, ml.PRE, ml.JOURNAL):
        if not (work / required).exists():
            fail(
                f'[bold red]Missing {escape(str(work / required))}.[/]',
                'A file --verify needs is missing', logger,
                path=str(work / required),
            )
    checks = ml.verify_files(roots, work)
    if not no_db:
        client = container.get_export_repository().client
        before_path = work / 'raw-before.json'
        before = (
            json.loads(before_path.read_text(encoding='utf-8'))
            if before_path.exists() else {}
        )
        checks += ml.verify_raw(client, roots.data, before)
        if scratch_db:
            from chatsbom.core.repository import QueryRepository
            config = container.config.get_db_config('admin')
            config.database = scratch_db
            with QueryRepository(config) as scratch:
                checks.append(ml.compare_current(client, scratch.client))
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

def _rollback(
    container: Any, roots: ml.Roots, work: Path, no_db: bool,
) -> None:
    result = ml.rollback(work)
    console.print(
        f'[green]Restored[/] {result.restored:,} renames · deleted '
        f'{result.deleted:,} written files · recreated '
        f'{result.recreated_dirs:,} directories · removed '
        f'{result.removed_dirs:,}',
    )
    if not no_db:
        repo_db = container.get_ingestion_repository()
        restored = ml.restore_raw(repo_db.client, repo_db.config.database)
        console.print(f'[green]raw_documents paths restored[/]: {restored:,}')
    backup = work / ml.LEDGER_BACKUP
    if backup.exists():
        ml.restore_ledger(backup, container.config.paths.ledger_path)
        console.print(f'[green]Ledger restored[/] from {escape(str(backup))}')
    console.print(
        '[dim]Then: check out the code from before this change, and run '
        '`--verify`-style counts against pre.tsv (`--inventory --workdir '
        '<elsewhere>` and compare).[/dim]',
    )


# -- the scratch database ------------------------------------------------------

def _prepare_scratch(container: Any, name: str) -> None:
    """A database for the transform equivalence check: the production
    schema, and a copy of `raw_documents` as it is now (rewritten)."""
    from chatsbom.core.repository import IngestionRepository
    from chatsbom.core.schema import identifier
    production = container.config.get_db_config('admin')
    if name == production.database:
        # A usage error, as it was: status 2.
        say(
            '[bold red]The scratch database cannot be production.[/]',
            'The scratch database cannot be production', logger, 'error',
            database=name,
        )
        raise typer.Exit(2)
    config = container.config.get_db_config('admin')
    config.database = name
    with IngestionRepository(config) as scratch:
        scratch.ensure_schema()
        existing = scratch.client.query(
            'SELECT count() FROM raw_documents',
        ).result_rows[0][0]
        if existing:
            # Refused, not failed, so the status stays 0; and said where
            # the logs go, since nothing was made.
            say(
                f'[yellow]{escape(name)}.raw_documents already has '
                f'{existing:,} rows; left as it is.[/]',
                'The scratch database has raw_documents already, left as '
                'it is', logger, database=name, rows=existing,
            )
            return
        columns = (
            'kind, repository_id, path, sha256, fetched_at, body, ref, '
            'commit_sha'
        )
        # Both names quoted: each is what was typed or configured, and
        # a hyphen in one was read as a minus sign (#120).
        scratch.client.command(
            f'INSERT INTO {identifier(name)}.raw_documents ({columns}) '
            f'SELECT {columns} FROM '
            f'{identifier(production.database)}.raw_documents',
        )
        copied = scratch.client.query(
            'SELECT count() FROM raw_documents',
        ).result_rows[0][0]
    # The name is what was typed after --prepare-scratch.
    shown = escape(name)
    console.print(
        f'[green]{shown}[/] ready: {copied:,} raw_documents rows. Now:\n'
        f'  [cyan]CLICKHOUSE_DB={shown} chatsbom db index --rebuild[/]\n'
        f'  [cyan]chatsbom data migrate-layout --verify --scratch-db {shown}[/]',
    )
