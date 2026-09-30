"""The universe of repositories to collect, read from the store (#100).

Derived scheduling needs a list of what to collect that is not the
ledger: the newest complete unfiltered search snapshot. Complete, since
a search written today may still be running (a re-run on the same day
resumes it), and a snapshot cut short would drop everything it had not
reached yet from the universe.
"""
from __future__ import annotations

import json
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path

from chatsbom.core.catalog import COMPLETE_MARKER
from chatsbom.core.catalog import newest_complete
from chatsbom.core.catalog import read_snapshot
from chatsbom.core.catalog import snapshots
from chatsbom.core.catalog import Tracked

TODAY = date(2026, 9, 29)


def _snapshot(directory: Path, name: str, *records: object) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_text(
        ''.join(
            (record if isinstance(record, str) else json.dumps(record))
            + '\n'
            for record in records
        ),
        encoding='utf-8',
    )
    return path


# --- which snapshot ---------------------------------------------------------

def test_the_newest_snapshot_dated_before_today_is_the_universe(tmp_path):
    for day in ('2026-09-20', '2026-09-28', '2026-09-29'):
        _snapshot(tmp_path, f'all-{day}.jsonl', {'id': 1, 'owner': 'o'})

    chosen = newest_complete(tmp_path, TODAY)

    assert chosen is not None
    assert chosen.name == 'all-2026-09-28'
    assert [s.name for s in snapshots(tmp_path)] == [
        'all-2026-09-20', 'all-2026-09-28', 'all-2026-09-29',
    ], 'oldest first'


def test_todays_snapshot_counts_once_it_is_marked_complete(tmp_path):
    """Today's may still be running; a marker says it finished, as the
    collector's universe writes one (#160)."""
    _snapshot(tmp_path, 'all-2026-09-28.jsonl', {'id': 1, 'owner': 'o'})
    today = _snapshot(tmp_path, 'all-2026-09-29.jsonl', {'id': 1})
    (tmp_path / f'{today.name}{COMPLETE_MARKER}').touch()

    chosen = newest_complete(tmp_path, TODAY)

    assert chosen is not None and chosen.name == 'all-2026-09-29'


def test_a_snapshot_dated_after_today_is_not_complete_unmarked(tmp_path):
    _snapshot(tmp_path, 'all-2026-09-30.jsonl', {'id': 1})

    assert newest_complete(tmp_path, TODAY) is None


def test_only_dated_unfiltered_snapshots_are_candidates(tmp_path):
    """A language's list, the undated `all.jsonl` an older search wrote,
    and anything else that only looks like a snapshot are not one."""
    for name in (
        'all.jsonl', 'java.jsonl', 'all-latest.jsonl',
        'all-2026-09-2.jsonl', 'all-2026-09-20.jsonl.tmp',
        '.all-2026-09-20.jsonl.0123.tmp',
    ):
        _snapshot(tmp_path, name, {'id': 1})

    assert snapshots(tmp_path) == []
    assert newest_complete(tmp_path, TODAY) is None
    assert newest_complete(tmp_path / 'missing', TODAY) is None


# --- what it lists ----------------------------------------------------------

def test_a_snapshot_lists_repositories_in_the_shape_the_ledger_tracks(
    tmp_path,
):
    path = _snapshot(
        tmp_path, 'all-2026-09-28.jsonl',
        {
            'id': 1, 'owner': 'halo-dev', 'repo': 'halo', 'stars': 35000,
            'language': 'Java', 'default_branch': 'main',
            'pushed_at': '2026-09-27T10:00:00Z',
        },
        # GitHub's own spellings, as a search item has them.
        {
            'id': 2, 'owner': {'login': 'o'}, 'name': 'cpp',
            'stargazers_count': 1200, 'language': None,
        },
        'not json',
        {'owner': 'no', 'repo': 'id'},
        {'id': 'x', 'owner': 'o', 'repo': 'r'},
        ['not', 'a', 'record'],
    )
    [chosen] = snapshots(tmp_path)

    catalog = read_snapshot(chosen)

    assert catalog.source == 'all-2026-09-28'
    assert catalog.repositories == {
        1: Tracked(
            repository_id=1, owner='halo-dev', repo='halo',
            github_language='Java', stars=35000, default_branch='main',
            snapshot='all-2026-09-28',
        ),
        2: Tracked(
            repository_id=2, owner='o', repo='cpp', github_language='',
            stars=1200, default_branch='', snapshot='all-2026-09-28',
        ),
    }
    assert catalog.pushed_at == {
        1: datetime(2026, 9, 27, 10, 0, tzinfo=timezone.utc),
    }
    assert catalog.unusable == 4
    assert path.read_text(encoding='utf-8').count('\n') == 6, 'unchanged'


def test_a_repository_listed_twice_is_one_with_its_last_line(tmp_path):
    _snapshot(
        tmp_path, 'all-2026-09-28.jsonl',
        {'id': 1, 'owner': 'o', 'repo': 'old', 'stars': 1},
        {'id': 1, 'owner': 'o', 'repo': 'new', 'stars': 2},
    )
    [chosen] = snapshots(tmp_path)

    catalog = read_snapshot(chosen)

    assert len(catalog) == 1
    assert catalog.repositories[1].repo == 'new'


def test_names_resolve_to_ids_as_github_matches_them(tmp_path):
    _snapshot(
        tmp_path, 'all-2026-09-28.jsonl',
        {'id': 1, 'owner': 'Halo-Dev', 'repo': 'Halo'},
        {'id': 2, 'owner': 'o', 'repo': 'r'},
    )
    [chosen] = snapshots(tmp_path)

    found, missing = read_snapshot(chosen).resolve(
        ['halo-dev/halo', '2', 'nobody/here', '# a comment', ''],
    )

    assert found == {1, 2}
    assert missing == ['nobody/here']
