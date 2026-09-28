"""`db edges` replaces the edge table, and its two rollups follow (#23).

`edges` is a SummingMergeTree and each run counts every document again,
so a second plain run added every count to itself: `--rebuild` was the
right call every time and the wrong default. Nothing refreshed
`mv_edges_forward` or `mv_edge_ambiguity` either, so on a fresh install
the forward-edge panels stayed empty until the daily refresh. And
`--rebuild` declared `dict_repositories` again, which has nothing to do
with edges.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from tests.conftest import requires_clickhouse
from tests.current_state_test import rows_of
from tests.definitions_test import uuids

pytestmark = requires_clickhouse


def graph(*edges: tuple[str, str]) -> dict[str, Any]:
    """A dependency-graph document whose packages depend on each other
    as `edges` say, below a repository root that depends on them all."""
    names = sorted({name for edge in edges for name in edge})
    return {
        'sbom': {
            'creationInfo': {'created': '2026-09-14T03:56:20Z'},
            'packages': [
                {'SPDXID': f'SPDXRef-{name}', 'name': name}
                for name in ('app', *names)
            ],
            'relationships': [
                {
                    'relationshipType': 'DESCRIBES',
                    'spdxElementId': 'SPDXRef-DOCUMENT',
                    'relatedSpdxElement': 'SPDXRef-app',
                },
                *(
                    {
                        'relationshipType': 'DEPENDS_ON',
                        'spdxElementId': f'SPDXRef-{parent}',
                        'relatedSpdxElement': f'SPDXRef-{child}',
                    }
                    for parent, child in edges
                ),
            ],
        },
    }


class Documents:
    """The depgraph directory `db edges` walks."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, repository: str, *edges: tuple[str, str]) -> None:
        path = self.root / 'javascript' / 'acme' / repository
        path.mkdir(parents=True, exist_ok=True)
        (path / 'sbom.spdx.json').write_text(json.dumps(graph(*edges)))


@pytest.fixture
def documents(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Documents:
    root = tmp_path / 'depgraph'
    monkeypatch.setattr('chatsbom.commands.db.edges.DEPGRAPH_ROOT', root)
    return Documents(root)


def pairs(query: QueryRepository) -> list[tuple[Any, ...]]:
    """Each pair's count, as the reverse-edge panel sums it."""
    return rows_of(
        query,
        'SELECT parent, child, sum(repositories) FROM edges '
        'GROUP BY parent, child',
    )


class TestRunningItAgain:

    def test_changes_no_count(self, query, documents, db_command):
        documents.write('one', ('debug', 'ms'), ('express', 'debug'))
        documents.write('two', ('debug', 'ms'))

        db_command('edges')
        once = pairs(query)
        db_command('edges')

        assert once == [('debug', 'ms', 2), ('express', 'debug', 1)]
        assert pairs(query) == once

    def test_the_rebuild_flag_is_still_accepted(
        self, query, documents, db_command,
    ):
        documents.write('one', ('debug', 'ms'))
        db_command('edges')
        db_command('edges', '--rebuild')
        assert pairs(query) == [('debug', 'ms', 1)]

    def test_a_recount_replaces_what_the_documents_no_longer_say(
        self, query, documents, db_command,
    ):
        documents.write('one', ('debug', 'ms'), ('express', 'debug'))
        db_command('edges')
        documents.write('one', ('debug', 'ms'))

        db_command('edges')

        assert pairs(query) == [('debug', 'ms', 1)]


class TestTheRollups:

    def test_they_follow_at_once(self, query, documents, db_command):
        """Not a day later, when the timer would refresh them."""
        documents.write('one', ('debug', 'ms'), ('express', 'debug'))

        db_command('edges')

        assert rows_of(
            query, 'SELECT parent, child, repositories FROM mv_edges_forward',
        ) == [('debug', 'ms', 1), ('express', 'debug', 1)]
        assert rows_of(query, 'SELECT edges FROM mv_edge_ambiguity') == [(2,)]

        documents.write('two', ('ms', 'supports-color'))
        db_command('edges')

        assert rows_of(query, 'SELECT edges FROM mv_edge_ambiguity') == [(3,)]


class TestWhatItLeavesAlone:

    def test_the_old_edges_answer_until_the_swap(
        self, query, documents, db_command,
    ):
        documents.write('one', ('debug', 'ms'), ('express', 'debug'))
        db_command('edges')
        answers: list[Any] = []

        def watch(repository: IngestionRepository) -> None:
            from tests.rebuild_test import ask_after_every_write
            ask_after_every_write(
                repository, query,
                'SELECT sum(repositories) FROM edges', answers,
            )

        db_command.on_open.append(watch)

        db_command('edges')

        assert answers
        assert {str(answer) for answer in answers} == {'[(2,)]'}

    def test_the_dictionary(self, query, clickhouse_db, documents, db_command):
        """`--rebuild` declared `dict_repositories` again, which reads
        `repositories` and nothing about edges."""
        documents.write('one', ('debug', 'ms'))
        before = uuids(query.client, clickhouse_db)

        db_command('edges')
        db_command('edges', '--rebuild')

        after = uuids(query.client, clickhouse_db)
        assert after['dict_repositories'] == before['dict_repositories']
