"""`chatsbom collect repo <owner/name | id>`: one repository's due
stages, now (#161).

What the collector's process (#155, 6e) would do for one repository,
done by hand: ask GitHub how it stands now, as the hourly sweep asks
(GraphQL's `nodes(ids:)`, #160), and keep that in collector.sqlite, a
push other than the last observed marked a change as the sweep marks
one; then run every stage due for that push, one after another, as the
store says (`collector/due.py`), keep what each did, and mark it
collected as of what it observed. It writes what the process would,
where the process would: the store and collector.sqlite, which one
process holds at a time. With the process running, this is refused.

What it prints on stdout is what it did: each stage that ran, where the
repository stands after, and what it asked for. A stage that failed
exits 1; one backing off after an earlier failure is left alone, and
said to be, unless `--retry` runs it now.
"""
from __future__ import annotations

import asyncio
import re
from collections import Counter
from datetime import datetime
from typing import Any
from typing import TYPE_CHECKING

import structlog
import typer
from rich.markup import escape

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.logging import console

if TYPE_CHECKING:
    from chatsbom.collector.runner import Collected
    from chatsbom.collector.stages import Tools
    from chatsbom.collector.state import CollectorState
    from chatsbom.collector.state import Member

logger = structlog.get_logger('collect_repo')

# A group takes no option after an argument otherwise: `collect repo
# octo/one --retry` would be a usage error.
app = typer.Typer(context_settings={'allow_interspersed_args': True})

#: `owner/name`, as GitHub allows either.
_NAME = re.compile(r'^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')

#: How a time is said.
_AT = '%Y-%m-%d %H:%M:%S UTC'


class Missing(Exception):
    """GitHub has no such repository, or has it no more."""


def _check(value: str) -> str:
    """`owner/name`, or an id: a usage error otherwise."""
    if value.isdigit() and int(value) > 0 or _NAME.match(value):
        return value
    raise typer.BadParameter(
        f'{value!r} is neither owner/name nor a repository id',
    )


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    repository: str = typer.Argument(
        ..., metavar='OWNER/NAME|ID', callback=_check,
        help='The repository: owner/name, or its numeric id',
    ),
    retry: bool = typer.Option(
        False, '--retry',
        help='Run a stage backing off after a failure too, now',
    ),
    wait: float = typer.Option(
        300.0, '--wait', min=0,
        help='Seconds a request may wait for a token with room',
    ),
) -> None:
    """
    Run one repository's due stages now, and say what each did.

    Asks GitHub how the repository stands now and keeps it in
    data/collector.sqlite, as the collector's sweep would; then runs
    each stage due for its push: the release and commit decisions, the
    tree, the content and the SBOM, the store's as today's stages leave
    them. A push that comes to a commit collected already stops there.
    Configured as the collector is: GITHUB_TOKEN and
    CHATSBOM_GITHUB_TOKENS, CHATSBOM_GITHUB_RESERVE, and Syft's
    CHATSBOM_SYFT_SLOTS, CHATSBOM_SYFT_TIMEOUT and CHATSBOM_SYFT_MEMORY.
    """
    # Here, not at the top: the CLI imports every command at start-up
    # (#26), and only this one needs the collector.
    from chatsbom.collector.settings import settings_from
    from chatsbom.collector.settings import SettingsError
    from chatsbom.collector.state import CollectorState
    from chatsbom.collector.state import state_path
    from chatsbom.collector.state import StateError
    from chatsbom.collector.syftpool import syft_settings

    try:
        settings = settings_from()
        syft = syft_settings()
    except SettingsError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'The collector is not configured', logger,
            setting=error.setting, problem=str(error),
        )

    paths = get_container().config.paths
    try:
        state = CollectorState.open(state_path(paths.base_data_dir))
    except StateError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'collector.sqlite cannot be opened', logger, error=str(error),
        )

    from chatsbom.collector.errors import RateLimited
    from chatsbom.collector.errors import Unauthorized

    with state:
        try:
            collected, spent = asyncio.run(
                _collect(
                    paths, state, settings, syft,
                    repository, retry, wait,
                ),
            )
        except Missing as missing:
            fail(
                f'[bold red]Error:[/] {escape(str(missing))}',
                'No such repository', logger, repository=repository,
            )
        except (RateLimited, Unauthorized) as error:
            fail(
                f'[bold red]Error:[/] {escape(str(error))}',
                'GitHub would not answer', logger, error=str(error),
            )
    show(collected, spent)
    if collected.failed:
        raise typer.Exit(1)


async def _collect(
    paths: Any,
    state: CollectorState,
    settings: Any,
    syft: Any,
    given: str,
    retry: bool,
    wait: float,
) -> tuple[Collected, Counter[str]]:
    from chatsbom.collector import runner
    from chatsbom.collector.due import CHAIN

    async with runner.tools_for(
        paths, state, settings, syft, wait=wait,
    ) as tools:
        member = await _member(tools, given)
        observed = await runner.observe_now(tools, member)
        if observed is None:
            raise Missing(f'GitHub has no repository {given} any more')
        if retry:
            for stage in CHAIN:
                state.clear(observed.repository_id, str(stage))
        collected = await runner.collect(tools, observed)
        return collected, tools.spent


async def _member(tools: Tools, given: str) -> Member:
    """The repository `given` names, by its id and node id, as the sweep
    asks after one: from collector.sqlite where it was observed, else
    from GitHub's REST API, following a rename once."""
    from chatsbom.collector.errors import Gone
    from chatsbom.collector.errors import NotFound
    from chatsbom.collector.state import Member

    state = tools.state
    known = (
        state.observed(int(given)) if given.isdigit()
        else state.observed_name(given)
    )
    if known is not None:
        return Member(known.repository_id, known.node_id)
    where = (
        f'/repositories/{int(given)}' if given.isdigit() else f'/repos/{given}'
    )
    for _ in range(2):
        try:
            answer = await tools.github.get(
                where, conditional=False, wait=tools.wait,
            )
        except NotFound:
            raise Missing(f'GitHub has no repository {given}') from None
        except Gone as gone:
            if gone.moved_to is None:
                raise Missing(f'GitHub has {given} no more: {gone}') from None
            where = gone.moved_to
            continue
        tools.count(answer)
        body = answer.json()
        if not isinstance(body, dict):
            body = {}
        repository_id, node = body.get('id'), body.get('node_id')
        if not isinstance(repository_id, int) or not isinstance(node, str):
            raise Missing(f'GitHub gave no id and node id for {given}')
        return Member(repository_id, node)
    raise Missing(f'GitHub moved {given} more than once')


# -- what it says -------------------------------------------------------------


def _at(value: datetime | None) -> str:
    return value.strftime(_AT) if value is not None else 'a time unknown'


def _asked(spent: Counter[str]) -> str:
    """What a collection asked for, in a line."""
    def many(count: int, one: str) -> str:
        return f'{count:,} {one}' + ('' if count == 1 else 's')

    said = []
    for bucket in sorted(set(spent) - {'raw', 'syft'}, key=lambda b: (b != 'core', b)):
        unit = 'point' if bucket == 'graphql' else 'request'
        said.append(many(spent[bucket], f'{bucket} {unit}'))
    if spent['raw']:
        said.append(many(spent['raw'], 'raw file'))
    if spent['syft']:
        said.append(many(spent['syft'], 'Syft scan'))
    return 'Asked: ' + (', '.join(said) if said else 'nothing') + '.'


def show(collected: Collected, spent: Counter[str]) -> None:
    """What a collection did, on stdout: it is the command's output."""
    from chatsbom.collector.due import State

    target = collected.target
    pushed = (
        f'pushed {_at(collected.push)}' if collected.push is not None
        else 'no push known'
    )
    console.print(
        escape(f'{target.full_name} ({target.repository_id}): {pushed}'),
    )
    for ran in collected.ran:
        console.print(
            f'  {ran.stage!s:<8} {ran.result:<8} {escape(ran.summary)}',
            soft_wrap=True,
        )
    standing = collected.standing
    last = collected.ran[-1] if collected.ran else None
    if standing is None:
        pass
    elif standing.current and not collected.ran:
        console.print('Current: nothing was due.')
    elif standing.current and collected.cut_off and standing.commit:
        console.print(
            'Current: the push comes to '
            f'{standing.commit.commit_sha[:7]}, collected already: nothing '
            'after it was due.',
        )
    elif standing.current:
        console.print('Current: every stage is done for this push.')
    elif last is not None and last.result != 'done':
        how = 'failed' if last.result == 'failed' else 'found nothing'
        console.print(
            f'Not current: {last.stage} {how}; it is due again at '
            f'{_at(last.due_at)}, and the stages after it wait for it.',
        )
    else:
        held = next(
            (v for v in standing.verdicts if v.state is State.BACKING_OFF),
            None,
        )
        if held is not None:
            how = 'failed' if held.why == 'failed' else 'found nothing'
            console.print(
                f'Not current: {held.stage} is backing off after it {how}; '
                f'due again at {_at(held.due_at)}. --retry runs it now.',
            )
        else:
            console.print('Not current: GitHub gives no push for it.')
    console.print(_asked(spent))
