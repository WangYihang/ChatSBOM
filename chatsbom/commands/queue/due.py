"""`chatsbom queue due`: what is due, derived from the store, beside what
the ledger has due (#100, PR 1 of step 1).

The next version schedules the way a build system does: a stage is due
for a repository of the newest complete search snapshot when its output
for the current input is not in the store (`core/due.py`). Before any
worker is scheduled that way, this says what that set is on the corpus
as it stands, and with `--compare` how it differs from the ledger's and
why, reason by reason.

It runs beside the collector, on the same ledger, while the collector
runs and is redeployed. So it reads and writes nothing: the ledger is
opened read-only and left as it was, bytes, times and WAL, and nothing
is made beside it; the store is only read. What it reports is on stdout,
with `--json` in a file; what it has to say besides is on stderr.

    chatsbom queue due              # where each stage stands
    chatsbom queue due --compare    # and why the ledger differs
    chatsbom queue due --compare --shard 0/16 --json due.json
"""
from __future__ import annotations

import json
import time
from collections.abc import Mapping
from datetime import datetime
from datetime import timezone
from enum import Enum
from pathlib import Path
from typing import Any

import structlog
import typer
from rich import box
from rich.markup import escape
from rich.table import Table

from chatsbom.core.catalog import Catalog
from chatsbom.core.catalog import ledger_catalog
from chatsbom.core.catalog import newest_complete
from chatsbom.core.catalog import read_snapshot
from chatsbom.core.config import PathConfig
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.due import COMPARED
from chatsbom.core.due import derive
from chatsbom.core.due import DERIVED_ONLY
from chatsbom.core.due import inventory as take_inventory
from chatsbom.core.due import LEDGER_ONLY
from chatsbom.core.due import LedgerView
from chatsbom.core.due import read_ledger
from chatsbom.core.due import Report
from chatsbom.core.due import Scans
from chatsbom.core.due import Settings
from chatsbom.core.due import State
from chatsbom.core.due import Store
from chatsbom.core.due import Tally
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console
from chatsbom.core.syft import get_syft_version

logger = structlog.get_logger('queue_due')
app = typer.Typer()


class Universe(str, Enum):
    """Which repositories are collected."""

    #: The newest complete search snapshot: derived scheduling's own.
    SNAPSHOT = 'snapshot'
    #: The ledger's set, to compare stage by stage without the
    #: differences between the two lists.
    LEDGER = 'ledger'


class Derived(str, Enum):
    """The stages it derives: the walk's, and the dependency graph."""

    RELEASE = 'release'
    COMMIT = 'commit'
    TREE = 'tree'
    CONTENT = 'content'
    SBOM = 'sbom'
    DEPGRAPH = 'depgraph'


def _now() -> datetime:
    """The moment every verdict is judged at: a function, so that a test
    can hold it still."""
    return datetime.now(timezone.utc)


def _parse_shard(value: str) -> tuple[int, int]:
    index, _, count = value.partition('/')
    shard = int(index), int(count)
    if shard[1] < 1 or not 0 <= shard[0] < shard[1]:
        raise ValueError(value)
    return shard


def _check_shard(value: str | None) -> str | None:
    """`K/N` with `0 <= K < N`, or a usage error."""
    if value is not None:
        try:
            _parse_shard(value)
        except ValueError:
            raise typer.BadParameter(
                'K/N, where 0 <= K < N: 0/16 is the first of sixteen',
            ) from None
    return value


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    compare: bool = typer.Option(
        False, '--compare',
        help="Hold each stage beside the ledger's due set, and say why "
        'they differ',
    ),
    stage: Derived | None = typer.Option(
        None, '--stage', help='This stage alone',
    ),
    shard: str | None = typer.Option(
        None, '--shard', metavar='K/N', callback=_check_shard,
        help='Only the repositories whose id is K modulo N',
    ),
    universe: Universe = typer.Option(
        Universe.SNAPSHOT, '--universe',
        help="The newest complete search snapshot, or the ledger's own set",
    ),
    syft_version: str | None = typer.Option(
        None, '--syft-version',
        help='The Syft an SBOM must record to be current (default: the '
        'one installed here)',
    ),
    rediscover: bool = typer.Option(
        False, '--rediscover',
        help='Discover the tree again for a content root no stage version '
        'vouches for (reads each such tree whole)',
    ),
    inventory: bool = typer.Option(
        False, '--inventory',
        help='Count the scans nothing points to',
    ),
    json_file: Path | None = typer.Option(
        None, '--json', dir_okay=False,
        help='Write the report here too, for a machine',
    ),
    samples: int = typer.Option(
        10, '--samples', min=0, help='Repository ids shown per reason',
    ),
) -> None:
    """
    What is due, derived from the store, and with --compare how the
    ledger's due set differs, and why.

    A stage is due for a repository of the universe when its output for
    the current input is not in the store; one whose input is not
    produced yet is waiting. Release and commit have no output files yet,
    so what they last produced is read from the ledger.

    Reads only: the ledger is opened read-only and left as it was, and
    nothing is written but --json. Safe beside a running collector.
    """
    started = time.perf_counter()
    now = _now()
    paths: PathConfig = get_container().config.paths
    stages = COMPARED if stage is None else (Stage(stage.value),)
    shard_of = _parse_shard(shard) if shard is not None else None

    if not paths.ledger_path.is_file():
        if compare or universe is Universe.LEDGER:
            fail(
                f'[red]No ledger at {escape(str(paths.ledger_path))}[/]: '
                'there is nothing to compare with.',
                'No ledger', logger, path=str(paths.ledger_path),
            )
        say(
            f'[yellow]No ledger at {escape(str(paths.ledger_path))}[/]: '
            'release and commit are read from it, so every release is '
            'due and every stage after it waiting.',
            'No ledger', logger, path=str(paths.ledger_path),
        )

    catalog = _universe(paths, universe, now)
    if shard_of is not None:
        catalog = catalog.shard(*shard_of)

    if syft_version is None and Stage.SBOM in stages:
        syft_version = get_syft_version()
        if syft_version is None:
            say(
                '[yellow]No Syft here to ask its version[/]: SBOMs are '
                'judged by their times alone. Name the Syft the collector '
                'runs with --syft-version.',
                'Syft version unknown', logger,
            )
    settings = Settings(
        now=now, syft_version=syft_version, rediscover=rediscover,
    )
    catalogued = time.perf_counter()

    def read(repos: set[int] | None) -> LedgerView:
        return read_ledger(
            paths.ledger_path, now, repos=repos, shard=shard_of,
            compare=compare,
        )

    report = derive(
        catalog, read, Store(paths), settings, stages=stages,
        compare=compare, samples=samples,
    )
    elapsed = {'catalog': catalogued - started, **report.elapsed}
    scans: list[Scans] | None = None
    if inventory:
        counted = time.perf_counter()
        scans = take_inventory(
            paths, catalog,
            read_ledger(paths.ledger_path, now, shard=shard_of, compare=False),
            shard=shard_of, samples=samples,
        )
        elapsed['inventory'] = time.perf_counter() - counted
    elapsed['total'] = time.perf_counter() - started
    report.elapsed = elapsed

    _print(report, catalog, shard, scans)
    if json_file is not None:
        document: dict[str, Any] = report.as_json()
        document['universe']['unusable'] = catalog.unusable
        document['shard'] = shard
        document['inventory'] = (
            [scan.as_json() for scan in scans] if scans is not None
            else None
        )
        atomic_write_text(json_file, json.dumps(document, indent=2) + '\n')


def _universe(paths: PathConfig, universe: Universe, now: datetime) -> Catalog:
    """The repositories to collect, or the command stops saying why."""
    if universe is Universe.LEDGER:
        with Ledger.open_readonly(paths.ledger_path) as ledger:
            return ledger_catalog(ledger)
    snapshot = newest_complete(paths.search_dir, now.date())
    if snapshot is None:
        fail(
            '[red]No complete search snapshot[/] in '
            f'{escape(str(paths.search_dir))}: one dated before today '
            '(UTC), or marked complete. Run `github search`, or compare '
            'with the ledger\'s own set: --universe ledger.',
            'No complete search snapshot', logger,
            directory=str(paths.search_dir),
        )
    catalog = read_snapshot(snapshot)
    if catalog.unusable:
        lines = 'line' if catalog.unusable == 1 else 'lines'
        say(
            f'[yellow]{catalog.unusable:,} {lines} of '
            f'{escape(snapshot.path.name)} name no repository[/]: left out.',
            'Snapshot lines left out', logger,
            snapshot=snapshot.name, lines=catalog.unusable,
        )
    return catalog


# -- the report --------------------------------------------------------------


def _ids(tally: Tally) -> str:
    if not tally.samples:
        return ''
    return f"({', '.join(str(i) for i in tally.samples)})"


def _table(*columns: str) -> Table:
    table = Table(box=box.SIMPLE, pad_edge=False)
    for position, column in enumerate(columns):
        table.add_column(
            column, justify='left' if position == 0 else 'right',
        )
    return table


def _reasons(found: Mapping[str, Tally]) -> list[tuple[str, Tally]]:
    return sorted(found.items(), key=lambda item: (-item[1].count, item[0]))


def _print(
    report: Report,
    catalog: Catalog,
    shard: str | None,
    scans: list[Scans] | None,
) -> None:
    header = [
        f'universe {catalog.source}', f'{report.repositories:,} repositories',
    ]
    if shard is not None:
        header.append(f'shard {shard}')
    header.append(
        f'ledger {report.tracked:,} tracked' if report.tracked is not None
        else 'no ledger',
    )
    header.append(f'{report.now:%Y-%m-%d %H:%M} UTC')
    console.print(f"[bold]Derived due set[/] · {escape(' · '.join(header))}")
    console.print(
        '[dim]Release and commit are read from the ledger until they have '
        'records of their own, and so is content only its row vouches '
        'for: "of which ledger". Waiting: an upstream stage has not '
        'produced its input yet.[/dim]',
    )

    states = _table(
        'Stage', 'Due', 'Waiting', 'Blocked', 'Deferred', 'Present',
        'Of which ledger',
    )
    for stage, found in report.stages.items():
        states.add_row(
            str(stage),
            *(
                f'{found.states[state]:,}' for state in (
                    State.DUE, State.WAITING, State.BLOCKED, State.DEFERRED,
                    State.PRESENT,
                )
            ),
            f'{found.ledger_backed:,}',
        )
    console.print(states)

    why = Table(box=box.SIMPLE, pad_edge=False, title='Why')
    why.add_column('Stage')
    why.add_column('State')
    why.add_column('Why', no_wrap=True)
    why.add_column('Count', justify='right')
    why.add_column('Ids')
    for stage, found in report.stages.items():
        for state in (State.DUE, State.BLOCKED, State.DEFERRED):
            for reason, tally in _reasons(found.why.get(state, {})):
                why.add_row(
                    str(stage), str(state), reason, f'{tally.count:,}',
                    _ids(tally),
                )
    if why.row_count:
        console.print(why)

    if report.compared:
        compared = _table(
            'Stage', 'Derived', 'Ledger', 'Both', 'Derived only',
            'Ledger only',
        )
        compared.title = 'Compared with the ledger'
        for stage, found in report.stages.items():
            compared.add_row(
                str(stage), f'{found.due:,}', f'{found.ledger_due or 0:,}',
                f'{found.both:,}',
                f'{sum(t.count for t in found.derived_only.values()):,}',
                f'{sum(t.count for t in found.ledger_only.values()):,}',
            )
        console.print(compared)
        differ = Table(box=box.SIMPLE, pad_edge=False, title='Why they differ')
        differ.add_column('Stage')
        differ.add_column('Due in')
        differ.add_column('Reason', no_wrap=True)
        differ.add_column('Count', justify='right')
        differ.add_column('Ids')
        for stage, found in report.stages.items():
            for side, label, tallies in (
                (DERIVED_ONLY, 'derived', found.derived_only),
                (LEDGER_ONLY, 'ledger', found.ledger_only),
            ):
                for reason, tally in _reasons(tallies):
                    differ.add_row(
                        str(stage), label, reason, f'{tally.count:,}',
                        _ids(tally),
                    )
        if differ.row_count:
            console.print(differ)
        else:
            console.print('The ledger has exactly these stages due.')
        console.print(
            f'Second look: {report.rechecked:,} differences read again, '
            f'{report.converged:,} agreed then (timing).',
        )

    if scans is not None:
        nothing = _table(
            'Root', 'Scans', 'Pointed to', 'Superseded', 'Outside',
        )
        nothing.title = 'Nothing points to'
        for scan in scans:
            nothing.add_row(
                scan.root, f'{scan.count:,}', f'{scan.pointed:,}',
                f'{scan.superseded.count:,}', f'{scan.outside.count:,}',
            )
        console.print(nothing)
        for scan in scans:
            for label, kind in (
                ('superseded', scan.superseded), ('outside', scan.outside),
            ):
                if kind.samples:
                    console.print(
                        f'[dim]{scan.root} {label}: '
                        f"{escape(', '.join(kind.samples))}[/dim]",
                    )

    phases = ' · '.join(
        f"{phase.replace('_', ' ')} {seconds:.2f} s"
        for phase, seconds in report.elapsed.items() if phase != 'total'
    )
    console.print(
        f"Elapsed {report.elapsed.get('total', 0.0):.2f} s: {phases}",
    )
