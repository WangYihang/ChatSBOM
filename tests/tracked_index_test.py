"""The ledger masters on the records (#55 §4.11): `TrackedRecords`.

`db index` mastered on the records a finished walk filed: a repository
without one never got a `repositories` row, so the ~28 k repositories a
search snapshot seeded had their dependency graphs fetched and landed
and never indexed, and every coverage ratio was measured against the
repositories that had already succeeded. Every tracked repository is
read now, with a record or without, as the warehouse reads them
(`warehouse/store.py`); how `db index` did it against ClickHouse went
with the server (#153).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from chatsbom.core.documents import TrackedRecords
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import resolve_names
from chatsbom.core.ledger import tracked_repositories

COMMIT = 'c' * 40


def record(repository_id: int, name: str) -> dict[str, Any]:
    return {
        'id': repository_id, 'owner': 'acme', 'name': name,
        'language': 'Java', 'stargazers_count': 100,
        'default_branch': 'main',
        'download_target': {
            'ref': 'v1.0.0', 'ref_type': 'release',
            'commit_sha': COMMIT, 'commit_sha_short': COMMIT[:7],
        },
    }


def ledger(path: Path) -> None:
    with Ledger(path) as kept:
        # Collected, with a record, under the Java list, and listed by
        # the snapshot too (a repository no snapshot lists is outside
        # the corpus: `corpus_test.py`).
        kept.track(1, 'acme', 'scanned', 'java')
        kept.seed(1, 'acme', 'scanned', snapshot='all-2026-03-09')
        # Seeded from a snapshot: no record, a graph and its metadata.
        kept.seed(
            2, 'acme', 'graphed', snapshot='all-2026-03-09',
            github_language='Kotlin', stars=5000, default_branch='trunk',
        )
        # Seeded, and nothing collected at all yet.
        kept.seed(
            3, 'acme', 'bare', snapshot='all-2026-03-09',
            github_language='C++', stars=1200, default_branch='master',
        )
        # Tracked with no language: its record is in `07-sbom/index.jsonl`.
        kept.seed(4, 'acme', 'unlisted', snapshot='all-2026-03-09')


# --- the pieces ------------------------------------------------------------

class Records:
    def __init__(self, *records: dict[str, Any]) -> None:
        self._records = records

    def records(self, limit: int | None = None):
        yield from self._records[:limit]


def test_tracked_records_fill_in_what_has_no_record(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    assert tracked is not None and sorted(tracked) == [1, 2, 3, 4]

    source = TrackedRecords(
        Records(record(1, 'scanned')), tracked,
        metadata=lambda ids: {2: {'id': 2, 'stargazers_count': 5100}},
    )
    found = {r['id']: r for r in source.records()}
    assert sorted(found) == [1, 2, 3, 4]
    assert 'download_target' in found[1]
    assert found[2]['stargazers_count'] == 5100
    assert found[3] == {
        'id': 3, 'owner': 'acme', 'name': 'bare',
        'html_url': 'https://github.com/acme/bare',
        'stargazers_count': 1200, 'default_branch': 'master',
        'language': 'C++', 'github_language': 'C++',
        'snapshot': 'all-2026-03-09',
    }


def test_a_record_with_placeholders_takes_the_ledgers_values(tmp_path):
    """A record `chatsbom run` filed from four ledger columns had stars
    0, no URL and no branch; with no metadata document to overlay, the
    index kept them (every pilot repository had `url = ''`)."""
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    placeholder = {
        'id': 2, 'owner': 'acme', 'repo': 'graphed', 'stars': 0,
        'url': '', 'default_branch': '',
    }
    stated = {
        'id': 3, 'owner': 'acme', 'repo': 'bare', 'stars': 7,
        'url': 'https://github.com/acme/bare', 'default_branch': 'dev',
    }
    found = {
        r['id']: r
        for r in TrackedRecords(Records(placeholder, stated), tracked).records()
    }

    assert found[2]['stars'] == 5000
    assert found[2]['default_branch'] == 'trunk'
    assert found[2]['url'] == 'https://github.com/acme/graphed'
    # Stars the record states are newer than the snapshot's, and stand.
    assert found[3]['stars'] == 7
    # The branch is the ledger's, which the commit stage keeps as HEAD:
    # a record filed before it has the placeholder `'main'`.
    assert found[3]['default_branch'] == 'master'


def test_a_record_the_ledger_does_not_track_is_kept(tmp_path):
    """Dropping it would delete a repository from the dataset because
    the ledger was never seeded with it."""
    source = TrackedRecords(Records(record(9, 'old')), {})
    assert [r['id'] for r in source.records()] == [9]


def test_a_limit_counts_both_kinds(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    source = TrackedRecords(Records(record(1, 'scanned')), tracked)
    assert [r['id'] for r in source.records(2)] == [1, 2]
    assert [r['id'] for r in source.records(2)] == [1, 2], 'the same twice'


def test_names_resolve_against_the_ledger(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    assert tracked is not None
    assert resolve_names(tracked, ['ACME/Bare', '4', 'x/y', '', '# c']) == (
        {3, 4}, ['x/y'],
    )


def test_no_ledger_is_none(tmp_path):
    assert tracked_repositories(tmp_path / 'absent.sqlite3') is None
    assert not (tmp_path / 'absent.sqlite3').exists()


def test_an_older_ledger_reads_with_empty_snapshot_columns(tmp_path):
    import sqlite3
    from contextlib import closing

    path = tmp_path / 'old.sqlite3'
    with closing(sqlite3.connect(path)) as db:
        db.execute(
            'CREATE TABLE repository_state ('
            'repository_id INTEGER PRIMARY KEY, owner TEXT, repo TEXT)',
        )
        db.execute("INSERT INTO repository_state VALUES (7, 'o', 'r')")
        db.commit()

    tracked = tracked_repositories(path)
    assert tracked is not None
    assert tracked[7].github_language == '' and tracked[7].stars is None


def test_the_metadata_overlay_carries_the_creation_date():
    """A record `chatsbom run` files has no `created_at`: the pilot's 82
    repositories were all indexed as created in 1970."""
    from chatsbom.core.documents import _wanted

    body = {'created_at': '2013-10-19T18:26:32Z', 'sbom_path': 'x'}
    assert _wanted(body) == {'created_at': '2013-10-19T18:26:32Z'}
