"""scripts/probe_github.py: what the collector relies on of GitHub,
measured on a live token (#171), which the owner runs once before the
cutover. Held here to what it says of the stand-in GitHub
(tests/fake_github_test.py), whose answers are known: a 100-id
`nodes(ids:)` call's cost, the dependency graph's bucket, a window's
reset, and two tokens of one account sharing its limits, a third of
another's apart.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from chatsbom.collector.tokens import Token
from tests.collector_scenario_test import graph_of
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Repo

ROOT = Path(__file__).resolve().parent.parent

ONE = 'ghp_probe_one_00000000000000000000000000'
TWO = 'ghp_probe_two_00000000000000000000000000'
OTHER = 'ghp_probe_other_000000000000000000000000'


def script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        'probe_github', ROOT / 'scripts' / 'probe_github.py',
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Where its dataclasses look their module up.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def probe_module() -> ModuleType:
    return script()


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub(FakeClock())
    fake.token(ONE, 'alice')
    fake.token(TWO, 'alice')
    fake.token(OTHER, 'bob')
    fake.graphql_cost = 1
    for number in range(1, 121):
        fake.add(
            Repo(
                number, 'octo', f'r{number}', stars=1_000 + number,
                graph=graph_of(f'octo/r{number}', 'left-pad'),
            ),
        )
    return fake


def tokens(*secrets: str) -> tuple[Token, ...]:
    return tuple(
        Token(f'token {number}', secret)
        for number, secret in enumerate(secrets, start=1)
    )


def run(
    module: ModuleType, fake: FakeGitHub, *secrets: str,
) -> Any:
    return asyncio.run(
        module.probe(
            tokens(*secrets), transport=fake.transport(),
            sleep=fake.clock.sleep, pause=1.0,
        ),
    )


class TestWhatItMeasures:
    def test_the_cost_of_a_100_id_nodes_call(self, probe_module, fake):
        fake.graphql_cost = 3
        report = run(probe_module, fake, ONE)
        assert report.nodes == {
            'ids': 100, 'status': 200, 'resource': 'graphql', 'cost': 3,
            'fell_by': 3, 'resolved': 100,
        }
        [asked] = [
            seen for seen in fake.seen('/graphql')
            if 'nodes' in seen.body['query']
        ]
        # The sweep's own query, of the most-starred repositories.
        from chatsbom.collector.sweep import QUERY

        assert asked.body['query'] == QUERY
        assert len(asked.body['variables']['ids']) == 100

    def test_the_dependency_graphs_bucket(self, probe_module, fake):
        report = run(probe_module, fake, ONE)
        assert report.depgraph == {
            'repository': 'octo/r120',
            'generate_report': {
                'status': 201, 'resource': 'dependency_sbom', 'limit': 200,
            },
            'fetch_report': {
                'status': 302, 'resource': 'dependency_sbom', 'limit': 200,
            },
        }
        # The report is looked at, and never downloaded: its link is not
        # followed.
        assert not [
            seen for seen in fake.requests if seen.host != 'api.github.com'
        ]

    def test_that_a_windows_reset_holds(self, probe_module, fake):
        report = run(probe_module, fake, ONE)
        core = report.resets['token 1 core']
        assert core['answers'] == 3
        assert len(core['resets']) == 1
        assert all(bucket['holds'] for bucket in report.resets.values())

    def test_a_reset_that_moves_within_a_window_is_said(self, probe_module):
        module = probe_module
        earlier = module.Answered(
            'token 1', 'core', 200, 'core', 5000, 10, 100,
        )
        slid = module.Answered('token 1', 'core', 200, 'core', 5000, 9, 160)
        renewed = module.Answered(
            'token 1', 'core', 200, 'core', 5000, 4999, 3700,
        )
        assert module._one_window(earlier, slid) is False
        assert module._one_window(earlier, renewed) is True

    def test_whether_tokens_share_their_accounts_limits(
        self, probe_module, fake,
    ):
        report = run(probe_module, fake, ONE, TWO, OTHER)
        assert [
            (pair['tokens'], pair['fell_by'], pair['shared'])
            for pair in report.sharing
        ] == [
            (['token 1', 'token 2'], 2, True),
            (['token 1', 'token 3'], 1, False),
        ]


class TestWhatItSpends:
    @pytest.mark.parametrize('count', [1, 3])
    def test_no_more_than_it_says_and_a_few_dozen_at_most(
        self, probe_module, fake, count,
    ):
        secrets = (ONE, TWO, OTHER)[:count]
        report = run(probe_module, fake, *secrets)
        assert report.problems == []
        assert report.spent == len(fake.requests) <= report.most
        assert report.most == sum(probe_module.plan(count).values())
        assert probe_module.plan(10)['core'] + 5 <= 36

    def test_it_stops_at_what_it_said(self, probe_module, fake):
        probe = probe_module.Probe(
            tokens(ONE), transport=fake.transport(), sleep=fake.clock.sleep,
        )
        probe.report.most = 2
        report = asyncio.run(probe.run())
        assert report.spent == 2 == len(fake.requests)
        assert any('are spent' in problem for problem in report.problems)


class TestWhatItSays:
    def test_says_first_what_it_spends_then_what_it_found_and_no_token(
        self, probe_module, fake, capsys,
    ):
        status = probe_module.main(
            [], environ={
                'GITHUB_TOKEN': ONE, 'CHATSBOM_GITHUB_TOKENS': f'{TWO},{OTHER}',
            },
            transport=fake.transport(), sleep=fake.clock.sleep, pause=1.0,
        )
        out, err = capsys.readouterr()
        assert status == 0
        assert err.startswith('This probe spends at most 12 requests')
        assert '1. The sweep\'s call, nodes(ids:) of 100 ids' in out
        assert "from 'dependency_sbom'" in out
        assert '3. X-RateLimit-Reset within a window: it holds' in out
        assert 'token 1 and token 2: they share their limits' in out
        assert 'token 1 and token 3: their limits are apart' in out
        assert 'Requests spent: 12 of 12.' in out
        for secret in (ONE, TWO, OTHER):
            assert secret not in out + err

    def test_as_json(self, probe_module, fake, capsys):
        probe_module.main(
            ['--json'], environ={'GITHUB_TOKEN': ONE},
            transport=fake.transport(), sleep=fake.clock.sleep, pause=1.0,
        )
        said = json.loads(capsys.readouterr().out)
        assert said['tokens'] == ['token 1']
        assert said['nodes']['cost'] == 1

    def test_without_a_token_it_says_how_and_asks_nothing(
        self, probe_module, fake, capsys,
    ):
        assert probe_module.main([], environ={}) == 2
        assert 'GITHUB_TOKEN' in capsys.readouterr().err
        assert fake.requests == []

    def test_a_token_github_refuses_is_said_by_its_place(
        self, probe_module, fake, capsys,
    ):
        status = probe_module.main(
            [],
            environ={'GITHUB_TOKEN': 'ghp_unknown_to_github_000000000000000'},
            transport=fake.transport(), sleep=fake.clock.sleep, pause=1.0,
        )
        out, err = capsys.readouterr()
        assert status == 2
        assert 'GitHub refused token 1 (401, bad credentials)' in out
        assert 'Requests spent: 1 of' in out
        assert 'ghp_unknown' not in out + err
