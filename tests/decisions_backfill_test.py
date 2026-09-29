"""The one-time backfill of the decisions from `raw_documents` (#147).

Until the release and commit stages kept their decisions, what they
decided was only in the records `RecordStore` landed in ClickHouse.
`data backfill-decisions` writes the files from each repository's
newest complete record, keyed by the record's own push and chosen tag,
before phase 5 of #128 takes `raw_documents` away. It reports by
default, writes with `--apply`, and a second run writes nothing.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core import decisions
from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.config import DatabaseConfig
from chatsbom.core.config import PathConfig
from chatsbom.core.decisions import Outcome
from chatsbom.core.documents import RawRecords
from chatsbom.core.documents import RecordStore
from chatsbom.core.repository import IngestionRepository
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import requires_clickhouse

UTC = timezone.utc
S1 = '1' * 40
S2 = '2' * 40

V1 = {
    'id': 1, 'tag_name': 'v1.0.0', 'name': 'v1.0.0',
    'published_at': '2026-01-01T00:00:00Z',
    'created_at': '2026-01-01T00:00:00Z', 'is_prerelease': False,
    'is_draft': False, 'target_commitish': 'main', 'source': 'github_release',
    'assets': [{'name': 'a.tgz', 'size': 1, 'download_count': 5}],
}
V2 = {
    **V1, 'id': 2, 'tag_name': 'v2.0.0', 'name': 'v2.0.0',
    'published_at': '2026-08-01T00:00:00Z',
}


def record(repository_id: int, **fields: Any) -> dict[str, Any]:
    """A record as `chatsbom run` lands it: `Repository`, dumped."""
    return {
        'id': repository_id, 'owner': 'acme', 'repo': f'r{repository_id}',
        'pushed_at': '2026-09-01T00:00:00Z', 'has_releases': True,
        'total_releases': 2, 'all_releases': [V2, V1],
        'latest_stable_release': V2,
        'download_target': {
            'ref': 'v2.0.0', 'ref_type': 'release', 'commit_sha': S1,
            'commit_sha_short': S1[:7],
        },
        **fields,
    }


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


def every_file(paths: PathConfig) -> list[str]:
    root = paths.base_data_dir
    return sorted(
        str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()
    ) if root.exists() else []


class TestWhatIsComplete:

    @pytest.mark.parametrize(
        'fields,why', [
            ({}, None),
            ({'all_releases': []}, None),
            ({'pushed_at': None}, 'no push'),
            ({'all_releases': None}, 'no releases'),
            ({'id': 'x'}, 'no id'),
        ],
    )
    def test_a_record_carries_a_release_decision_or_says_why_not(
        self, fields: dict[str, Any], why: str | None,
    ) -> None:
        assert decisions.incomplete(record(1, **fields)) == why


class TestTheBackfill:

    def test_it_reports_and_writes_nothing_by_default(
        self, paths: PathConfig,
    ) -> None:
        found = [(1, record(1), None), (2, record(2), None)]

        report = decisions.backfill(found, paths, apply=False)

        assert report.repositories == 2 and report.taken == 2
        assert report.releases == Counter({Outcome.WRITTEN: 2})
        assert report.lists == Counter({Outcome.WRITTEN: 2})
        assert report.commits == Counter({Outcome.WRITTEN: 2})
        assert every_file(paths) == []

    def test_it_writes_with_apply_and_a_second_run_writes_nothing(
        self, paths: PathConfig,
    ) -> None:
        found = [(1, record(1), None), (2, record(2), None)]

        decisions.backfill(found, paths, apply=True)
        written = every_file(paths)
        again = decisions.backfill(found, paths, apply=True)

        assert len(written) == 6
        assert every_file(paths) == written
        assert again.releases == Counter({Outcome.KEPT: 2})
        assert again.lists == Counter({Outcome.KEPT: 2})
        assert again.commits == Counter({Outcome.KEPT: 2})

    def test_the_files_are_keyed_by_the_records_push_and_tag(
        self, paths: PathConfig,
    ) -> None:
        decisions.backfill([(1, record(1), None)], paths, apply=True)

        chain = decisions.newest(paths, 1)
        assert chain is not None and chain.commit is not None
        assert chain.release.push == datetime(2026, 9, 1, tzinfo=UTC)
        assert chain.release.tag == 'v2.0.0'
        assert chain.commit.key == decisions.CommitKey.tag('v2.0.0')
        target = decisions.as_record(chain)['download_target']
        assert target['commit_sha'] == S1

    def test_what_it_could_not_take_is_counted_by_why(
        self, paths: PathConfig,
    ) -> None:
        found: list[tuple[int, dict[str, Any] | None, str | None]] = [
            (1, None, 'no push'),
            (2, None, 'no releases'),
            (3, record(3), 'no releases'),
            (4, record(4, download_target=None), None),
        ]

        report = decisions.backfill(found, paths, apply=True)

        assert report.incomplete == Counter({'no push': 1, 'no releases': 1})
        # Its newest record had no releases; an older one was taken.
        assert (report.taken, report.older) == (2, 1)
        assert report.commits == Counter({
            Outcome.WRITTEN: 1, Outcome.UNKEYED: 1,
        })

    def test_a_decision_kept_differently_is_counted_and_stands(
        self, paths: PathConfig,
    ) -> None:
        decisions.backfill([(1, record(1), None)], paths, apply=True)
        stored = every_file(paths)

        report = decisions.backfill(
            [(1, record(1, latest_stable_release=V1), None)], paths, apply=True,
        )

        assert report.releases == Counter({Outcome.CONFLICT: 1})
        assert every_file(paths)[:2] == stored[:2]

    def test_a_record_the_model_will_not_take_is_counted(
        self, paths: PathConfig,
    ) -> None:
        broken = record(1, all_releases=[{'id': 1}])

        report = decisions.backfill([(1, broken, None)], paths, apply=True)

        assert report.unusable == 1
        assert every_file(paths) == []


# -- against ClickHouse ----------------------------------------------------


@requires_clickhouse
class TestTheNewestCompleteRecord:

    def remember(
        self, ingest: IngestionRepository, body: dict[str, Any], day: int,
    ) -> None:
        RecordStore(ingest.client).remember(
            body, 'data/07-sbom/go.jsonl',
            taken_at=datetime(2026, 9, day, tzinfo=UTC),
        )

    def test_the_newest_complete_record_of_each_repository(
        self, ingest: IngestionRepository,
    ) -> None:
        self.remember(ingest, record(1, pushed_at='2026-08-01T00:00:00Z'), 1)
        self.remember(ingest, record(1), 2)
        # Its releases could not be fetched the last time.
        self.remember(ingest, record(2), 1)
        self.remember(
            ingest, record(
                2, all_releases=None, has_releases=None,
                pushed_at='2026-09-20T00:00:00Z',
            ), 3,
        )
        self.remember(ingest, record(3, pushed_at=None), 1)

        found = list(
            RawRecords(ingest.client).newest_with(decisions.incomplete),
        )

        assert [(i, bool(r), why) for i, r, why in found] == [
            (1, True, None), (2, True, 'no releases'), (3, False, 'no push'),
        ]
        assert found[0][1] is not None
        assert found[0][1]['pushed_at'] == '2026-09-01T00:00:00Z'
        assert found[1][1] is not None
        assert found[1][1]['pushed_at'] == '2026-09-01T00:00:00Z'


@pytest.fixture
def command(
    clickhouse_db: str,
    paths: PathConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    """`chatsbom data backfill-decisions`, against the test database."""
    config = ChatSBOMConfig(
        paths=paths,
        _db_base=DatabaseConfig(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=clickhouse_db,
        ),
    )
    opened: list[IngestionRepository] = []

    def repository() -> IngestionRepository:
        opened.append(IngestionRepository(config.get_db_config('admin')))
        return opened[-1]

    container = SimpleNamespace(
        config=config, get_ingestion_repository=repository,
    )
    module = 'chatsbom.commands.data.backfill_decisions'
    monkeypatch.setattr(f'{module}.get_container', lambda: container)
    checked: list[dict[str, Any]] = []

    def check(**arguments: Any) -> bool:
        checked.append(arguments)
        return True

    monkeypatch.setattr(f'{module}.check_clickhouse_connection', check)
    return SimpleNamespace(
        config=config, paths=paths, repository=repository, checked=checked,
    )


@requires_clickhouse
def test_the_command_reports_then_writes_then_writes_nothing(
    command: SimpleNamespace,
) -> None:
    with command.repository() as ingest:
        RecordStore(ingest.client).remember(record(1), 'data/07-sbom/go.jsonl')
        RecordStore(ingest.client).remember(
            record(
                2, all_releases=[], latest_stable_release=None,
                has_releases=False, total_releases=0,
                download_target={
                    'ref': 'main', 'ref_type': 'branch', 'commit_sha': S2,
                    'commit_sha_short': S2[:7],
                },
            ),
            'data/07-sbom/go.jsonl',
        )
    runner = CliRunner()

    reported = runner.invoke(app, ['data', 'backfill-decisions'])
    assert reported.exit_code == 0, reported.output
    assert 'Dry run' in reported.output
    assert every_file(command.paths) == []
    assert command.checked, 'the connection is checked first'

    applied = runner.invoke(app, ['data', 'backfill-decisions', '--apply'])
    assert applied.exit_code == 0, applied.output
    assert every_file(command.paths) == sorted([
        '03-github-release/1/20260901T000000Z/release@2.json',
        '03-github-release/2/20260901T000000Z/release@2.json',
        *(
            f'03-github-release/{r}/releases/{p.name}'
            for r in (1, 2)
            for p in (command.paths.release_dir / str(r) / 'releases').iterdir()
        ),
        '04-github-commit/1/tag-v2.0.0/commit@1.json',
        '04-github-commit/2/head-20260901T000000Z/commit@1.json',
    ])

    again = runner.invoke(app, ['data', 'backfill-decisions', '--apply'])
    assert again.exit_code == 0, again.output
    assert 'Nothing to write' in again.output
