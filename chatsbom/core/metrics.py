"""Queue health in Prometheus text format.

`queue status` renders tables for a person. A timer-driven pipeline also
needs something a scraper can read, because the failures that matter are
gradual: a backlog that grows a little every hour, or a failure count
that climbs while backoff quietly hides it.

Ages are exported as seconds-since rather than timestamps, so an alert is
a threshold (`chatsbom_queue_oldest_check_seconds > 86400`) instead of
arithmetic in the alerting rule.
"""
from datetime import datetime

from chatsbom.core.ledger import LedgerHealth


def _gauge(name: str, help_text: str, samples: list[tuple[str, float]]) -> list[str]:
    lines = [f'# HELP {name} {help_text}', f'# TYPE {name} gauge']
    lines.extend(f'{name}{labels} {value:g}' for labels, value in samples)
    return lines


def render_prometheus(health: LedgerHealth, now: datetime) -> str:
    """Render one health snapshot as Prometheus exposition text."""
    oldest_age = (
        (now - health.oldest_check).total_seconds()
        if health.oldest_check else 0.0
    )

    lines: list[str] = []
    lines += _gauge(
        'chatsbom_queue_tracked',
        'Repositories registered in the work queue.',
        [('', health.tracked)],
    )
    lines += _gauge(
        'chatsbom_queue_failing',
        'Repositories currently in backoff after a failure.',
        [('', health.failing)],
    )
    lines += _gauge(
        'chatsbom_queue_claimed',
        'Repositories leased by a worker right now.',
        [('', health.claimed)],
    )
    lines += _gauge(
        'chatsbom_queue_never_checked',
        'Repositories the queue has never observed.',
        [('', health.never_checked)],
    )
    lines += _gauge(
        'chatsbom_queue_oldest_check_seconds',
        'Age of the least recently checked repository, in seconds.',
        [('', oldest_age)],
    )
    lines += _gauge(
        'chatsbom_queue_due',
        'Repositories with outstanding work, by pipeline stage.',
        [
            (f'{{stage="{stage}"}}', count)
            for stage, count in health.due.items()
        ],
    )

    return '\n'.join(lines) + '\n'
