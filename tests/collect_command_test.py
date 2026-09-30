"""`chatsbom collect repo`: one repository's due stages, now (#161).

The collector's first command, before the process that runs it all
(#155, 6e). It asks GitHub how the repository stands now, as the sweep
would, keeps that in collector.sqlite, runs every stage due for its push,
and says what each did: what the process would do for it, and writes
what the process would write.

Run as the CLI runs it, against the stand-ins: the API, git on disk,
raw content and a Syft on PATH.
"""
from __future__ import annotations

import functools
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.collector import runner
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import STATE_FILE
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Reply
from tests.fake_upstream_test import FakeSyft
from tests.fake_upstream_test import Repository
from tests.fake_upstream_test import Upstream

TOKEN = 'ghp_collect_command_0000000000000000000000'

cli = CliRunner()


class Stand:
    """The stand-ins, and what the command writes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.github = FakeGitHub(FakeClock())
        self.github.token(TOKEN, 'alice')
        self.upstream = Upstream(root / 'github.com', self.github)
        self.syft = FakeSyft(root / 'bin')

    @property
    def data(self) -> Path:
        return self.root / 'data'

    def state(self) -> Any:
        return CollectorState.open(self.data / STATE_FILE)

    def universe(self, *ids: int) -> None:
        """The repositories `ids`, the universe the sweep asks after."""
        from datetime import datetime
        from datetime import timezone

        from chatsbom.collector.state import Member
        from chatsbom.collector.state import UniverseSnapshot

        with self.state() as state:
            state.keep_universe(
                UniverseSnapshot(
                    snapshot='all-2026-09-28', stamp='all-2026-09-28:1',
                    repositories=len(ids),
                    loaded_at=datetime(2026, 9, 28, tzinfo=timezone.utc),
                ),
                [Member(i, self.github.repos[i].node_id) for i in ids],
            )

    def repository(self) -> Repository:
        """octo/one: a release at a commit with a manifest."""
        repository = self.upstream.add(1, 'octo/one')
        sha = repository.commit(
            {'package.json': '{"name": "one"}', 'src/index.js': 'code'},
            date='2026-09-02T00:00:00+00:00',
        )
        repository.tag('v1.0.0', sha)
        repository.release('v1.0.0', '2026-09-02T00:00:00Z')
        return repository


@pytest.fixture
def stand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Stand:
    stand = Stand(tmp_path)
    monkeypatch.chdir(tmp_path)
    # The collector quiets httpx2 as it runs: put back after.
    quiet = logging.getLogger('httpx2')
    monkeypatch.setattr(quiet, 'level', quiet.level)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setenv('GITHUB_TOKEN', TOKEN)
    monkeypatch.delenv('CHATSBOM_GITHUB_TOKENS', raising=False)
    monkeypatch.setenv(
        'PATH', f'{stand.syft.directory}:{__import__("os").environ["PATH"]}',
    )
    for name in (
        'CHATSBOM_SYFT_SLOTS', 'CHATSBOM_SYFT_TIMEOUT', 'CHATSBOM_SYFT_MEMORY',
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        runner, 'tools_for', functools.partial(
            runner.tools_for,
            clock=stand.github.clock,
            sleep=stand.github.clock.sleep,
            github_transport=stand.github.transport(),
            raw_transport=stand.upstream.raw.transport(),
            git_base=stand.upstream.git_base,
        ),
    )
    return stand


def collect(*arguments: str) -> Any:
    return cli.invoke(app, ['collect', 'repo', *arguments])


def said(result: Any) -> str:
    """What a command printed, on one line: Rich wraps a long one."""
    return ' '.join(result.output.split())


class TestCollectRepo:
    def test_collects_a_repository_from_its_push_to_its_sbom(self, stand):
        repository = stand.repository()

        result = collect('octo/one')

        assert result.exit_code == 0, result.output
        text = said(result)
        assert 'octo/one (1)' in text
        assert 'pushed 2026-09-02 00:00:00 UTC' in text
        for line in (
            'release done decided v1.0.0 of 1 release',
            'commit done resolved tag:v1.0.0 to',
            'tree done listed 2 paths at',
            'content done 1 of 1 manifest stored',
            'sbom done scanned by Syft 1.52.0: 1 package',
        ):
            assert line in text, text
        assert 'Current: every stage is done for this push.' in text
        [sha] = repository.files_at
        sbom = stand.data / '07-sbom' / '1' / sha / 'sbom.json'
        assert json.loads(sbom.read_text())['descriptor']['version'] == (
            '1.52.0'
        )

    def test_keeps_how_it_saw_the_repository(self, stand):
        stand.repository()
        collect('octo/one')
        with stand.state() as state:
            observed = state.observed(1)
        assert observed is not None
        assert observed.full_name == 'octo/one'
        assert observed.release_tag == 'v1.0.0'

    def test_marks_it_collected_as_of_what_it_observed(self, stand):
        """#160: what 6e collects next is what detection found changed,
        or never collected; this is neither now."""
        stand.repository()
        stand.universe(1)
        with stand.state() as state:
            assert state.never_collected() == []

        result = collect('octo/one')

        assert result.exit_code == 0, result.output
        with stand.state() as state:
            assert state.never_collected() == []
            assert state.changed() == []

    def test_a_push_it_saw_and_could_not_collect_stays_a_change(
        self, stand,
    ):
        """It observes before it collects: the push it saw is a change,
        as the sweep would have marked it, until it is collected."""
        repository = stand.repository()
        stand.universe(1)
        collect('octo/one')
        repository.pushed('2026-09-10T00:00:00+00:00')
        stand.github.clock.advance(60)
        stand.github.script(
            Reply(401, {'message': 'Bad credentials'}),
            path='/repositories/1/releases',
        )

        result = collect('octo/one')

        assert result.exit_code == 1
        assert 'GitHub took none of the tokens' in said(result)
        with stand.state() as state:
            assert [o.repository_id for o in state.changed()] == [1]

    def test_says_what_it_asked_for(self, stand):
        stand.repository()
        result = collect('octo/one')
        text = said(result)
        # The repository by name, its node, and one page of releases.
        assert 'Asked: 2 core requests, 1 graphql point' in text
        assert '1 raw file' in text
        assert '1 Syft scan' in text

    def test_by_id_after_by_name_has_nothing_due(self, stand):
        stand.repository()
        collect('octo/one')

        result = collect('1')

        assert result.exit_code == 0, result.output
        assert 'Current: nothing was due.' in said(result)
        # Known by its node now: GraphQL alone.
        assert 'Asked: 1 graphql point' in said(result)

    def test_says_where_a_push_came_to_a_commit_collected_already(
        self, stand,
    ):
        repository = stand.repository()
        collect('octo/one')
        repository.commit({'x.txt': 'x'}, branch='feature')
        repository.pushed('2026-09-10T00:00:00+00:00')

        result = collect('octo/one')

        text = said(result)
        assert 'release done decided v1.0.0' in text
        assert 'commit done' not in text
        assert 'tree done' not in text
        assert 'nothing after it was due' in text

    def test_a_stage_that_fails_says_when_it_is_due_again(self, stand):
        stand.repository()
        stand.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases',
        )

        result = collect('octo/one')

        assert result.exit_code == 1, result.output
        text = said(result)
        assert 'release failed' in text
        assert '502' in text
        assert 'due again at 2026-09-21 14:28:20 UTC' in text
        assert 'the stages after it wait for it' in text

    def test_a_stage_backing_off_is_left_alone_unless_retried(self, stand):
        stand.repository()
        stand.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases',
        )
        collect('octo/one')

        waiting = collect('octo/one')
        assert waiting.exit_code == 0, waiting.output
        assert 'release is backing off' in said(waiting)
        assert 'due again at 2026-09-21 14:28:20 UTC' in said(waiting)
        assert 'done' not in said(waiting)

        retried = collect('octo/one', '--retry')
        assert retried.exit_code == 0, retried.output
        assert 'Current: every stage is done for this push.' in said(retried)

    def test_a_repository_github_does_not_have_is_refused(self, stand):
        result = collect('octo/missing')
        assert result.exit_code == 1
        assert 'GitHub has no repository octo/missing' in said(result)

    @pytest.mark.parametrize('given', ['octo', 'a/b/c', '-1', '1.5', 'x y/z'])
    def test_a_name_that_is_neither_is_a_usage_error(self, stand, given):
        result = collect('--', given)
        assert result.exit_code == 2, result.output
        assert 'owner/name' in said(result)

    def test_without_a_token_it_says_which_to_set(self, stand, monkeypatch):
        monkeypatch.delenv('GITHUB_TOKEN')
        result = collect('octo/one')
        assert result.exit_code == 1
        assert 'GITHUB_TOKEN' in said(result)

    def test_a_setting_it_cannot_use_is_named(self, stand, monkeypatch):
        monkeypatch.setenv('CHATSBOM_SYFT_SLOTS', 'many')
        result = collect('octo/one')
        assert result.exit_code == 1
        assert 'CHATSBOM_SYFT_SLOTS' in said(result)

    def test_logs_no_line_a_request(self, stand):
        """httpx2 says each request at INFO: a line for every raw file of
        every repository, in the process. The client says what matters
        of each at DEBUG (`collector.github`)."""
        stand.repository()
        result = collect('octo/one')
        assert result.exit_code == 0, result.output
        assert 'HTTP Request:' not in result.output
        assert 'Stage done' not in result.output
        assert 'Current: every stage is done for this push.' in said(result)

    def test_collector_sqlite_held_by_another_is_refused(self, stand):
        stand.repository()
        with stand.state():
            result = collect('octo/one')
        assert result.exit_code == 1
        assert 'in use' in said(result)
        assert 'collector.sqlite' in said(result)


class TestARepositoryByName:
    """`collect repo owner/name` finds what the sweep observed by name,
    as GitHub matches one: whatever the case."""

    def _observe(
        self, state: Any, repository_id: int, full_name: str, at: Any,
    ) -> None:
        from chatsbom.collector.state import Observed

        state.observe(
            Observed(
                repository_id=repository_id, node_id=f'R_{repository_id}',
                full_name=full_name, stars=1, archived=False,
                pushed_at=None, default_branch='main', head=None,
                release_tag=None, release_at=None, observed_at=at,
            ),
        )

    def test_is_found_whatever_the_case(self, tmp_path):
        from datetime import datetime
        from datetime import timezone

        at = datetime(2026, 9, 30, tzinfo=timezone.utc)
        with CollectorState.open(tmp_path / STATE_FILE) as state:
            self._observe(state, 1, 'Octo/One', at)
            found = state.observed_name('octo/ONE')
            assert found is not None and found.repository_id == 1
            assert state.observed_name('octo/two') is None

    def test_taken_over_after_a_rename_is_the_one_observed_last(
        self, tmp_path,
    ):
        """octo/one renamed away, and a new octo/one made: until the
        sweep sees the rename, both are observed by the name."""
        from datetime import datetime
        from datetime import timedelta
        from datetime import timezone

        at = datetime(2026, 9, 30, tzinfo=timezone.utc)
        with CollectorState.open(tmp_path / STATE_FILE) as state:
            self._observe(state, 1, 'octo/one', at)
            self._observe(state, 2, 'octo/one', at + timedelta(hours=1))
            found = state.observed_name('octo/one')
            assert found is not None and found.repository_id == 2


def test_the_cli_loads_the_collector_only_when_it_runs(tmp_path):
    """#26: the command's module is light; the collector's clients, its
    state, its stages and Syft's pool are imported when it runs. The
    stages' rules (`content`, `releases`) are the old services' too."""
    probe = (
        'import json, sys\n'
        'import chatsbom.__main__\n'
        'print(json.dumps(sorted(m for m in sys.modules '
        "if m.startswith('chatsbom.collector') or m == 'httpx2')))\n"
    )
    result = subprocess.run(
        [sys.executable, '-c', probe], cwd=tmp_path,
        capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout.splitlines()[-1]) == [
        'chatsbom.collector', 'chatsbom.collector.content',
        'chatsbom.collector.releases',
    ]
