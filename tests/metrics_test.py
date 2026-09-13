"""Queue health as machine-readable metrics.

`queue status` is for a person. A timer-driven pipeline also needs
something a scraper can read, so a growing backlog or a rising failure
count is noticed without anyone looking.
"""
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.metrics import render_prometheus

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


def seeded(tmp_path):
    book = Ledger(tmp_path / 'l.sqlite3')
    book.track(1, 'o', 'a', 'ruby')
    book.track(2, 'o', 'b', 'ruby')
    book.record_success(1, Stage.REPO, NOW)
    book.record_failure(2, Stage.REPO, NOW, 'boom')
    return book


def metrics(tmp_path) -> str:
    with seeded(tmp_path) as book:
        return render_prometheus(book.health(NOW), now=NOW)


def test_every_series_is_named_and_typed(tmp_path):
    text = metrics(tmp_path)
    for name in (
        'chatsbom_queue_tracked',
        'chatsbom_queue_failing',
        'chatsbom_queue_never_checked',
        'chatsbom_queue_due',
        'chatsbom_queue_oldest_check_seconds',
    ):
        assert f'# TYPE {name} gauge' in text, name
        assert f'# HELP {name} ' in text, name


def test_values_match_the_ledger(tmp_path):
    text = metrics(tmp_path)
    assert 'chatsbom_queue_tracked 2' in text
    assert 'chatsbom_queue_failing 1' in text


def test_due_is_labelled_per_stage(tmp_path):
    text = metrics(tmp_path)
    assert 'chatsbom_queue_due{stage="sbom"}' in text
    assert 'chatsbom_queue_due{stage="repo"}' in text


def test_oldest_check_is_an_age_not_a_timestamp(tmp_path):
    """A scraper wants seconds-since, so an alert is a threshold."""
    with seeded(tmp_path) as book:
        health = book.health(NOW)
        text = render_prometheus(health, now=NOW + timedelta(hours=2))
    line = next(
        ln for ln in text.splitlines()
        if ln.startswith('chatsbom_queue_oldest_check_seconds ')
    )
    assert float(line.split()[1]) == 7200.0


def test_an_empty_queue_still_renders(tmp_path):
    with Ledger(tmp_path / 'empty.sqlite3') as book:
        text = render_prometheus(book.health(NOW), now=NOW)
    assert 'chatsbom_queue_tracked 0' in text
    assert 'chatsbom_queue_oldest_check_seconds 0' in text


def test_output_is_newline_terminated(tmp_path):
    """Prometheus text format requires a trailing newline."""
    assert metrics(tmp_path).endswith('\n')


def test_no_help_or_type_is_repeated(tmp_path):
    lines = metrics(tmp_path).splitlines()
    directives = [ln for ln in lines if ln.startswith('# ')]
    assert len(directives) == len(set(directives))
