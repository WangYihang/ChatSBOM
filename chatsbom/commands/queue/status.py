import os
from datetime import datetime
from datetime import timezone

import humanize
import typer
from rich.markup import escape
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console
from chatsbom.core.metrics import render_prometheus
from chatsbom.services.dependency_graph_service import closed_reason
from chatsbom.services.depgraph_stage import DEPGRAPH_REFRESH

app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    metrics: bool = typer.Option(
        False,
        '--metrics',
        help='Emit Prometheus text format instead of tables',
    ),
) -> None:
    """
    Queue health: what is tracked, outstanding, stale and stuck.

    These are the numbers to alarm on. A growing `due` count means the
    slice size or cadence is too low; a growing `failing` count means
    something is wrong that backoff is hiding. Repositories GitHub
    answers 404 for are counted as absent, not failing.

    `--metrics` emits Prometheus text format for a textfile collector.
    """
    container = get_container()
    now = datetime.now(timezone.utc)

    if metrics:
        # Machine-readable output goes to stdout unadorned, so it can be
        # piped straight into a textfile collector.
        with Ledger(container.config.paths.ledger_path) as ledger:
            print(render_prometheus(ledger.health(now), now), end='')
        return

    with Ledger(container.config.paths.ledger_path) as ledger:
        if ledger.count() == 0:
            console.print(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
            )
            return
        health = ledger.health(now)
        depgraph_due = ledger.count_due_for_stage(
            Stage.DEPGRAPH, now, refresh=DEPGRAPH_REFRESH,
        )
        depgraph_outcomes = ledger.stage_outcomes(Stage.DEPGRAPH)
        worst = sorted(
            (s for s in ledger.all() if s.failure_count),
            key=lambda s: -s.failure_count,
        )[:5]

    overview = Table(title='Queue')
    overview.add_column('Metric', style='cyan')
    overview.add_column('Value', style='magenta', justify='right')
    overview.add_row('Tracked', f'{health.tracked:,}')
    overview.add_row('Never checked', f'{health.never_checked:,}')
    overview.add_row('In backoff', f'{health.failing:,}')
    overview.add_row('Absent (404)', f'{health.absent:,}')
    overview.add_row('Claimed now', f'{health.claimed:,}')
    overview.add_row(
        'Oldest check',
        humanize.naturaltime(now - health.oldest_check)
        if health.oldest_check else '—',
    )
    console.print(overview)

    stages = Table(title='Outstanding by stage')
    stages.add_column('Stage', style='cyan')
    stages.add_column('Due', style='magenta', justify='right')
    stages.add_column('', style='dim')
    for stage, count in health.due.items():
        if stage is Stage.DEPGRAPH:
            # Scheduled from `stage_state` now, not from the watermarks.
            continue
        share = count / health.tracked if health.tracked else 0
        stages.add_row(str(stage), f'{count:,}', f'{share:.0%}')
    console.print(stages)

    reason = closed_reason(os.getenv('CHATSBOM_DEPGRAPH_API'), now.date())
    graphs = Table(
        title='Dependency graph' + (' (closed)' if reason else ''),
    )
    graphs.add_column('', style='cyan')
    graphs.add_column('Repositories', style='magenta', justify='right')
    for why, label in (
        ('never', 'Due: never asked'),
        ('refresh', 'Due: refresh (30 days)'),
        ('expired', 'Due: negative cache expired'),
    ):
        graphs.add_row(label, f'{depgraph_due.get(why, 0):,}')
    for outcome, count in sorted(depgraph_outcomes.items()):
        graphs.add_row(f'Last answer: {outcome}', f'{count:,}')
    console.print(graphs)
    if reason:
        console.print(f'[yellow]Dependency graph stage closed:[/] {reason}')

    if worst:
        failing = Table(title='Repositories in backoff')
        failing.add_column('Repository', style='green')
        failing.add_column('Failures', style='yellow', justify='right')
        failing.add_column('Retry', style='dim')
        failing.add_column('Last error', style='dim')
        for state in worst:
            # A cell is read as markup too, and the error is whatever
            # requests or a server said: a `[/dim]` in it raised.
            failing.add_row(
                escape(state.full_name),
                str(state.failure_count),
                humanize.naturaltime(state.next_attempt_at - now)
                if state.next_attempt_at else '—',
                escape(state.last_error[:60]),
            )
        console.print(failing)
