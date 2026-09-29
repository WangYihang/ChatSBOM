"""POST /api/ask: a question, answered on the server as it streams (#140).

The Worker relayed one model turn per request, and the page ran the
loop and the tools, and posted each result back (`web/src/chat.ts`,
`web/src/agent.ts`). Here the loop is the server's (#128, section 2.6,
and the owner's decision on Q5): one request a question, answered as
server-sent events, with the tools run in-process against the dataset,
so no client supplies what the model reads of the data. The model is
DeepSeek's `deepseek-flash` (Q8), here a stand-in on a socket of its
own (`tests/fake_deepseek_test.py`): no test calls DeepSeek.

The Worker's cases are ported where they apply (`web/test/chat.test.ts`,
`web/test/agent.test.ts`): the refusals, what may reach the model, the
spend cap's reservations, and why the model stopped. What had to do
with a conversation posted back turn by turn, and with Turnstile, went
with them; a challenge solved once a question stands in for both.
"""
import json
import socket
import sqlite3
import threading
import time
import urllib.request
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from chatsbom.server.app import create_app
from chatsbom.server.ask import MAX_CALLS_PER_TURN
from chatsbom.server.ask import MAX_TURNS
from chatsbom.server.ask import Pacing
from chatsbom.server.challenge import Challenges
from chatsbom.server.model import MAX_OUTPUT_TOKENS
from chatsbom.server.model import most_input_tokens
from chatsbom.server.pricing import Prices
from chatsbom.server.pricing import Usage
from chatsbom.server.prompt import SYSTEM_PROMPT
from chatsbom.server.prompt import TOOLS
from chatsbom.server.settings import settings_from
from chatsbom.server.spend import spend_day
from chatsbom.server.spend import SpendLedger
from chatsbom.server.spend import USED_UP
from chatsbom.server.state import WebState
from tests.dataset_contract_test import corpus
from tests.fake_deepseek_test import Call
from tests.fake_deepseek_test import FakeDeepSeek
from tests.fake_deepseek_test import Reply
from tests.fake_deepseek_test import usage
from tests.server_app_test import INDEX
from tests.server_app_test import Serving
from tests.server_challenge_test import solved

UTC = timezone.utc
KEY = 'k' * 32

#: A Tuesday at noon, UTC: off peak. And its 02:00: peak.
NOON = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
PEAK_HOUR = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)

#: Where TestClient's requests are sent: the page's own origin.
ORIGIN = 'http://testserver'

QUESTION = 'Who declares mail?'

PRICES = Prices()


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    root = tmp_path / 'client'
    (root / 'assets').mkdir(parents=True)
    (root / 'index.html').write_text(INDEX)
    return root


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    return corpus(tmp_path)


class Clock:
    """The service's clock: each moment given, in turn, and the last
    of them from then on."""

    def __init__(self, *moments: datetime) -> None:
        self.moments = list(moments) or [NOON]

    def __call__(self) -> datetime:
        return self.moments.pop(0) if len(self.moments) > 1 else self.moments[0]


@dataclass(frozen=True)
class Gone:
    """Where a model was, and nothing listens now."""

    url: str


@dataclass
class Chat:
    """The service, with the chat on, answering from the stand-in."""

    app: FastAPI
    state_dir: Path
    challenges: Challenges

    @property
    def ledger(self) -> SpendLedger:
        return SpendLedger(WebState(self.state_dir))

    def rows(self) -> list[tuple[str, str, float, int]]:
        with WebState(self.state_dir).connect() as db:
            return db.execute(
                'SELECT id, day, usd, settled FROM spend ORDER BY rowid',
            ).fetchall()

    def in_flight(self) -> int:
        count: int = self.app.state.asking.in_flight.count
        return count


def chat(
    spa: Path,
    tmp_path: Path,
    fake: FakeDeepSeek | Gone,
    snapshot: Path,
    *,
    clock: Callable[[], datetime] = Clock(),
    pacing: Pacing = Pacing(turn_seconds=10, keepalive_seconds=15),
    **environ: str,
) -> Chat:
    state_dir = tmp_path / 'state'
    settings = settings_from(
        {
            'ALTCHA_HMAC_KEY': KEY,
            'WEB_STATE_DIR': str(state_dir),
            'DEEPSEEK_API_KEY': 'sk-test',
            'DEEPSEEK_BASE_URL': fake.url,
            'WEB_SNAPSHOT': str(snapshot),
            **environ,
        },
        spa=spa,
    )
    # A challenge solved at once: a counter of 5 to 10, one iteration.
    challenges = Challenges(
        settings.altcha_key, WebState(state_dir), cost=1, counters=(5, 10),
    )
    app = create_app(
        settings, challenges=challenges, clock=clock, pacing=pacing,
    )
    return Chat(app, state_dir, challenges)


def visit(app: FastAPI, peer: str = '198.51.100.20') -> TestClient:
    return TestClient(app, client=(peer, 50000))


@dataclass
class Answered:
    status: int
    headers: Any
    body: str

    @property
    def events(self) -> list[tuple[str, Any]]:
        return events(self.body)

    @property
    def names(self) -> list[str]:
        return [name for name, _ in self.events]

    @property
    def text(self) -> str:
        return ''.join(
            data['delta'] for name, data in self.events if name == 'text'
        )

    @property
    def last(self) -> tuple[str, Any]:
        return self.events[-1]

    def json(self) -> Any:
        return json.loads(self.body)


def events(body: str) -> list[tuple[str, Any]]:
    """Server-sent events as a browser's EventSource reads them:
    comments skipped."""
    found = []
    for block in body.split('\n\n'):
        name, data = None, []
        for line in block.split('\n'):
            if not line or line.startswith(':'):
                continue
            field, _, value = line.partition(':')
            value = value[1:] if value.startswith(' ') else value
            if field == 'event':
                name = value
            elif field == 'data':
                data.append(value)
        if name is not None:
            found.append((name, json.loads('\n'.join(data))))
    return found


#: Solve a fresh challenge for the question, as the page is to.
SOLVE = object()


def ask(
    client: TestClient,
    question: str = QUESTION,
    *,
    prior: object = None,
    altcha: object = SOLVE,
    headers: dict[str, str] | None = None,
    body: object = None,
) -> Answered:
    """A question as the page is to ask it: JSON, from its own origin,
    with a challenge it solved first."""
    if body is None:
        asked: dict[str, object] = {'question': question}
        if prior is not None:
            asked['prior'] = prior
        if altcha is SOLVE:
            asked['altcha'] = solve(client)
        elif altcha is not None:
            asked['altcha'] = altcha
        body = asked
    response = client.post(
        '/api/ask',
        content=json.dumps(body),
        headers={
            'content-type': 'application/json', 'origin': ORIGIN,
            **(headers or {}),
        },
    )
    return Answered(response.status_code, response.headers, response.text)


def solve(client: TestClient, **headers: str) -> str:
    issued = client.get('/api/ask/challenge', headers=headers)
    assert issued.status_code == 200, issued.text
    return solved(issued.json())


def worst_case(
    messages: list[dict[str, Any]], moment: datetime, seconds: float = 10,
) -> float:
    """What the loop holds for a turn that sends `messages` at `moment`."""
    return PRICES.worst_case(
        most_input_tokens(messages), MAX_OUTPUT_TOKENS,
        moment, moment + timedelta(seconds=seconds),
    )


def cost(reported: dict[str, Any], start: datetime, end: datetime) -> float:
    counted = Usage.reported(reported)
    assert counted is not None
    return PRICES.cost(counted, start, end)


def calls(*called: tuple[str, str], finish: str = 'tool_calls', **more: Any) -> Reply:
    """A turn that calls tools: each a name and its arguments' text."""
    return Reply(
        reasoning='Look it up.',
        calls=[
            Call(f'call_{index:02d}', name, arguments)
            for index, (name, arguments) in enumerate(called)
        ],
        finish=finish,
        **more,
    )


@pytest.fixture
def ledger_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str, float]]:
    """Every reservation, settlement and refund, in order."""
    seen: list[tuple[str, str, float]] = []
    reserve, settle, refund = (
        SpendLedger.reserve, SpendLedger.settle, SpendLedger.refund,
    )

    def reserving(self: SpendLedger, reservation: str, usd: float, *args: Any) -> bool:
        seen.append(('reserve', reservation, usd))
        return reserve(self, reservation, usd, *args)

    def settling(self: SpendLedger, reservation: str, usd: float) -> None:
        seen.append(('settle', reservation, usd))
        settle(self, reservation, usd)

    def refunding(self: SpendLedger, reservation: str) -> None:
        seen.append(('refund', reservation, 0.0))
        refund(self, reservation)

    monkeypatch.setattr(SpendLedger, 'reserve', reserving)
    monkeypatch.setattr(SpendLedger, 'settle', settling)
    monkeypatch.setattr(SpendLedger, 'refund', refunding)
    return seen


# ---- the answer, as events --------------------------------------------


class TestTheAnswer:
    def test_streams_as_text_then_says_it_is_done(self, spa, tmp_path, snapshot):
        answer = '17 projects declare mail themselves.'
        with FakeDeepSeek(Reply(content=answer, reasoning='Count them.')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.status == 200, answered.body
        assert answered.headers['content-type'] == (
            'text/event-stream; charset=utf-8'
        )
        assert answered.headers['cache-control'] == 'no-store'
        assert answered.text == answer
        assert set(answered.names[:-1]) == {'text'}
        name, done = answered.last
        assert name == 'done'
        assert done == {
            'turns': 1,
            'usage': {
                'prompt_tokens': 1_000,
                'prompt_cache_hit_tokens': 800,
                'prompt_cache_miss_tokens': 200,
                'completion_tokens': 100,
                'reasoning_tokens': 40,
            },
            'cost_usd': pytest.approx(
                (800 * 0.003 + 200 * 0.15 + 100 * 0.60) / 1_000_000,
            ),
        }
        # To the billionth of a dollar, without a float's tail.
        assert done['cost_usd'] == round(done['cost_usd'], 9)
        assert '"cost_usd":9.24e-05}' in answered.body
        # Its reasoning is the model's, and not sent on.
        assert 'Count them' not in answered.body

    def test_carries_the_pages_headers_like_every_response(
        self, spa, tmp_path, snapshot,
    ):
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)
        assert answered.headers['content-security-policy']
        assert answered.headers['x-content-type-options'] == 'nosniff'

    def test_runs_a_tool_and_feeds_its_result_back(self, spa, tmp_path, snapshot):
        replies = (
            calls(('dependents_of', '{"name": "mail", "direct_only": true}')),
            Reply(content='rails/rails and 3 others declare it.'),
        )
        with FakeDeepSeek(*replies) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.names[0] == 'tool'
        assert answered.events[0][1] == {
            'name': 'dependents_of',
            'arguments': {'name': 'mail', 'direct_only': True},
        }
        assert answered.text == 'rails/rails and 3 others declare it.'
        assert answered.last[0] == 'done'
        assert answered.last[1]['turns'] == 2

        first, second = (request.body for request in fake.requests)
        # Append-only: the second turn is the first, and what came of it.
        assert second['messages'][:2] == first['messages']
        assistant, result = second['messages'][2:]
        assert assistant == {
            'role': 'assistant',
            'content': '',
            'reasoning_content': 'Look it up.',
            'tool_calls': [{
                'id': 'call_00', 'type': 'function',
                'function': {
                    'name': 'dependents_of',
                    'arguments': '{"name": "mail", "direct_only": true}',
                },
            }],
        }
        assert result['role'] == 'tool'
        assert result['tool_call_id'] == 'call_00'
        # The answer, and not an error standing in for it: counted from
        # the snapshot the question pinned.
        read = json.loads(result['content'])
        assert read['total'] == 4
        assert read['rows_shown'] == len(read['rows'])
        assert 'direct_total' not in read

    def test_answers_parallel_calls_each_by_its_id(self, spa, tmp_path, snapshot):
        replies = (
            calls(
                ('search_packages', '{"fragment": "mai"}'),
                ('language_coverage', '{}'),
            ),
            Reply(content='done'),
        )
        with FakeDeepSeek(*replies) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.names[:2] == ['tool', 'tool']
        results = fake.requests[1].body['messages'][3:]
        assert [m['tool_call_id'] for m in results] == ['call_00', 'call_01']
        assert json.loads(results[0]['content'])['rows'][0]['name'] == 'mail'
        assert 'error' not in json.loads(results[1]['content'])

    @pytest.mark.parametrize(
        'name,arguments,shown,said',
        [
            ('no_such_tool', '{}', {}, 'unknown tool: no_such_tool'),
            ('dependents_of', '{"name": ', None, 'the arguments are not JSON'),
            (
                'dependents_of', '{"limit": 5}', {
                    'limit': 5,
                }, 'requires a package name',
            ),
        ],
    )
    def test_reports_a_call_that_failed_instead_of_dropping_it(
        self, spa, tmp_path, snapshot, name, arguments, shown, said,
    ):
        """The model can recover, and a call left unanswered is a
        malformed conversation."""
        with FakeDeepSeek(calls((name, arguments)), Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        [result] = fake.requests[1].body['messages'][3:]
        assert said in json.loads(result['content'])['error']
        assert answered.last[0] == 'done'
        # Said as the model made it; arguments that do not parse, as none.
        assert answered.events[0] == (
            'tool', {'name': name, 'arguments': shown},
        )

    def test_gives_up_after_eight_turns(self, spa, tmp_path, snapshot):
        """Each turn is a paid call, and a model that kept calling tools
        would otherwise spend without bound (`MAX_TURNS`, agent.ts)."""
        assert MAX_TURNS == 8
        looping = [calls(('language_coverage', '{}')) for _ in range(12)]
        with FakeDeepSeek(*looping) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert len(fake.requests) == 8
        assert answered.names.count('tool') == 8
        name, error = answered.last
        assert name == 'error'
        assert error['code'] == 'turns'
        assert error['turns'] == 8
        # Each turn reserved, and settled at its cost.
        rows = service.rows()
        assert len(rows) == 8
        assert all(settled == 1 for *_, settled in rows)

    def test_runs_at_most_so_many_calls_in_one_turn(self, spa, tmp_path, snapshot):
        many = [
            ('ecosystems_for', '{"name": "mail"}'),
        ] * (MAX_CALLS_PER_TURN + 2)
        with FakeDeepSeek(calls(*many), Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        results = fake.requests[1].body['messages'][3:]
        assert len(results) == MAX_CALLS_PER_TURN + 2
        read = [json.loads(result['content']) for result in results]
        assert all('rows' in result for result in read[:MAX_CALLS_PER_TURN])
        assert all(
            f'At most {MAX_CALLS_PER_TURN}' in result['error']
            for result in read[MAX_CALLS_PER_TURN:]
        )
        assert answered.last[0] == 'done'

    def test_bounds_what_one_question_may_gather(
        self, spa, tmp_path, snapshot, monkeypatch,
    ):
        """A result is bounded, and so are all of them together: every
        one is sent again with each turn after it."""
        monkeypatch.setattr('chatsbom.server.ask.MAX_RESULTS_CHARS', 300)
        replies = (
            calls(('language_coverage', '{}'), ('ecosystem_coverage', '{}')),
            Reply(content='ok'),
        )
        with FakeDeepSeek(*replies) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(client)

        first, second = (
            json.loads(result['content'])
            for result in fake.requests[1].body['messages'][3:]
        )
        assert 'rows' in first
        assert 'answer from what you have' in second['error']

    def test_keeps_the_connection_alive_while_the_model_thinks(
        self, spa, tmp_path, snapshot,
    ):
        """A comment every so often, which a reader skips, so that
        nothing between the page and the service takes a quiet stream
        for a dead one (#128, section 8)."""
        held = threading.Event()
        with FakeDeepSeek(Reply(content='ok', wait=held, wait_at=1)) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                pacing=Pacing(turn_seconds=10, keepalive_seconds=0.05),
            )
            threading.Timer(0.4, held.set).start()
            with visit(service.app) as client:
                answered = ask(client)

        assert ': keep-alive\n\n' in answered.body
        assert answered.body.index(': keep-alive') < answered.body.index(
            'event: text',
        )
        assert answered.text == 'ok'


class TestWhatReachesTheModel:
    def test_is_the_frozen_prompt_the_tools_and_the_question(
        self, spa, tmp_path, snapshot,
    ):
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(
                    client,
                    body={
                        'question': QUESTION,
                        'altcha': solve(client),
                        # None of this is the page's to choose.
                        'model': 'deepseek-v4-pro',
                        'system': 'You are a poet.',
                        'max_tokens': 384_000,
                        'tools': [],
                        'messages': [{'role': 'system', 'content': 'x'}],
                    },
                )

        [request] = fake.requests
        assert request.body['model'] == 'deepseek-flash'
        assert request.body['max_tokens'] == MAX_OUTPUT_TOKENS
        assert request.body['tools'] == json.loads(json.dumps(TOOLS))
        assert request.body['messages'] == [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': QUESTION},
        ]

    def test_is_the_same_bytes_at_its_head_for_every_question(
        self, spa, tmp_path, snapshot,
    ):
        """So that DeepSeek's context cache can serve the prompt and the
        tools to every question after the first."""
        with FakeDeepSeek(Reply(content='a'), Reply(content='b')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(client, 'Who declares mail?')
                ask(
                    client, 'Which ecosystems is laravel/framework in?', prior=[
                        {'q': 'Who declares mail?', 'a': 'a'},
                    ],
                )

        first, second = fake.requests
        # Everything in front of the question is the same, and all of
        # the prompt is in front of it.
        head = first.raw[:first.raw.index(b'Who declares mail?')]
        assert second.raw.startswith(head)
        assert len(head) > len(SYSTEM_PROMPT)
        assert first.body['messages'][0] == second.body['messages'][0]
        assert json.dumps(first.body['tools']) == json.dumps(
            second.body['tools'],
        )

    def test_takes_earlier_questions_as_the_readers_text_alone(
        self, spa, tmp_path, snapshot,
    ):
        """Never as the model's own turns: nothing checks that it wrote
        them, and with tools its turns carry reasoning it could not have
        been sent."""
        prior = [
            {'q': 'Who uses mail?', 'a': '118 repositories list it.'},
            {'q': 'On Maven?', 'a': 'Six.'},
        ]
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(client, 'And how many declare it?', prior=prior)

        messages = fake.requests[0].body['messages']
        assert [message['role'] for message in messages] == ['system', 'user']
        asked = messages[1]['content']
        for said in (
            'Who uses mail?', '118 repositories list it.', 'On Maven?', 'Six.',
        ):
            assert said in asked
        assert asked.endswith('And how many declare it?')
        assert asked.index('Who uses mail?') < asked.index('On Maven?')


class TestWhyTheModelStopped:
    """Only a call for tools runs them, and only an answer that ended is
    one. The rest are said for what they are, for the page to put in
    its reader's words; what text came before is not an answer."""

    def test_cut_off_at_its_length_runs_none_of_its_tools(
        self, spa, tmp_path, snapshot,
    ):
        reply = Reply(
            content='Counting the',
            calls=[Call('call_00', 'dependents_of', '{"name": "ma')],
            finish='length',
        )
        with FakeDeepSeek(reply) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert 'tool' not in answered.names
        assert len(fake.requests) == 1
        name, error = answered.last
        assert (name, error['code']) == ('error', 'cut-off')
        assert 'cut off' in error['message']

    def test_filtered_is_declined(self, spa, tmp_path, snapshot):
        with FakeDeepSeek(Reply(content='Here is how', finish='content_filter')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)
        name, error = answered.last
        assert (name, error['code']) == ('error', 'declined')

    @pytest.mark.parametrize(
        'reason', ['insufficient_system_resource', 'aborted', 'something_new'],
    )
    def test_any_other_reason_is_named(self, spa, tmp_path, snapshot, reason):
        with FakeDeepSeek(Reply(content='x', finish=reason)) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)
        name, error = answered.last
        assert (name, error['code'], error['reason']) == (
            'error', 'stopped', reason,
        )

    @pytest.mark.parametrize(
        'reply',
        [
            Reply(content='Counting', finish=None),
            Reply(finish='tool_calls'),
            Reply(
                calls=[Call('', 'dependents_of', '{"name": "mail"}')],
                finish='tool_calls',
            ),
        ],
        ids=['no finish', 'tools and no calls', 'a call with no id'],
    )
    def test_a_turn_not_understood_is_garbled(self, spa, tmp_path, snapshot, reply):
        with FakeDeepSeek(reply) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)
        name, error = answered.last
        assert (name, error['code']) == ('error', 'garbled')
        assert len(fake.requests) == 1


# ---- the budget, turn by turn ----------------------------------------


class TestTheBudget:
    """Each turn's worst case is held against the day's cap before its
    call, and settled at what the stream said it cost after (#33,
    `spend`), at the prices of the hours it ran in (`pricing`)."""

    def test_holds_each_turn_before_its_call_and_settles_it_after(
        self, spa, tmp_path, snapshot, ledger_calls,
    ):
        replies = (
            calls(
                ('ecosystems_for', '{"name": "mail"}'),
                usage=usage(hit=0, miss=1_500, output=80),
            ),
            Reply(
                content='Three ecosystems.',
                usage=usage(hit=1_400, miss=300, output=40),
            ),
        )
        with FakeDeepSeek(*replies) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.last[0] == 'done'
        first, second = (request.body['messages'] for request in fake.requests)
        (r1, reserve1, hold1), (s1, settle1, cost1), (r2, reserve2, hold2), (
            s2, settle2, cost2,
        ) = ledger_calls
        assert (r1, s1, r2, s2) == ('reserve', 'settle', 'reserve', 'settle')
        assert reserve1 == settle1 and reserve2 == settle2 != reserve1
        assert hold1 == pytest.approx(worst_case(first, NOON))
        assert hold2 == pytest.approx(worst_case(second, NOON))
        assert hold2 > hold1
        assert cost1 == pytest.approx(
            cost(usage(hit=0, miss=1_500, output=80), NOON, NOON),
        )
        assert cost2 == pytest.approx(
            cost(usage(hit=1_400, miss=300, output=40), NOON, NOON),
        )
        day = service.ledger.usage(spend_day(NOON))
        assert day.held == 0
        assert day.spent == pytest.approx(cost1 + cost2)
        assert answered.last[1]['cost_usd'] == pytest.approx(cost1 + cost2)

    def test_holds_before_the_model_is_asked(
        self, spa: Path, tmp_path: Path, snapshot: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Before the call, not after: a hold that came after could not
        refuse it."""
        asked_when_held: list[int] = []
        with FakeDeepSeek(calls(('language_coverage', '{}')), Reply(content='ok')) as fake:
            reserve = SpendLedger.reserve

            def reserving(self: SpendLedger, *args: Any) -> bool:
                asked_when_held.append(len(fake.requests))
                return reserve(self, *args)

            monkeypatch.setattr(SpendLedger, 'reserve', reserving)
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(client)
        assert asked_when_held == [0, 1]

    def test_at_peak_holds_and_settles_at_peak(self, spa, tmp_path, snapshot, ledger_calls):
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                clock=Clock(PEAK_HOUR),
            )
            with visit(service.app) as client:
                answered = ask(client)

        messages = fake.requests[0].body['messages']
        (_, _, held), (_, _, settled) = ledger_calls
        assert held == pytest.approx(worst_case(messages, PEAK_HOUR))
        assert held == pytest.approx(2 * worst_case(messages, NOON))
        assert settled == pytest.approx(
            (800 * 0.006 + 200 * 0.30 + 100 * 1.20) / 1_000_000,
        )
        assert answered.last[1]['cost_usd'] == pytest.approx(settled)

    def test_prices_cache_hits_at_their_own_rate(self, spa, tmp_path, snapshot, ledger_calls):
        replies = (
            Reply(
                content='a', usage=usage(
                    hit=10_000, miss=0, output=0, reasoning=0,
                ),
            ),
            Reply(
                content='b', usage=usage(
                    hit=0, miss=10_000, output=0, reasoning=0,
                ),
            ),
        )
        with FakeDeepSeek(*replies) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                ask(client)
                ask(client)
        settled = [usd for kind, _, usd in ledger_calls if kind == 'settle']
        assert settled == [
            pytest.approx(10_000 * 0.003 / 1_000_000),
            pytest.approx(10_000 * 0.15 / 1_000_000),
        ]

    def test_a_turn_that_ran_into_peak_is_settled_at_peak(
        self, spa, tmp_path, snapshot, ledger_calls,
    ):
        """Held at 00:59:58, begun at 00:59:59, done at 01:00:01: which
        moment DeepSeek prices by is not said, and the dearer is the
        one the cap can afford to be wrong by."""
        clock = Clock(
            NOON.replace(hour=0, minute=59, second=58),
            NOON.replace(hour=0, minute=59, second=59),
            NOON.replace(hour=1, minute=0, second=1),
        )
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot, clock=clock)
            with visit(service.app) as client:
                ask(client)
        (_, _, held), (_, _, settled) = ledger_calls
        messages = fake.requests[0].body['messages']
        assert held == pytest.approx(
            worst_case(messages, NOON.replace(hour=0, minute=59, second=58)),
        )
        assert settled == pytest.approx(
            (800 * 0.006 + 200 * 0.30 + 100 * 1.20) / 1_000_000,
        )

    def test_counts_against_the_day_that_held_it(
        self, spa, tmp_path, snapshot,
    ):
        """Held at 23:59:59, settled after midnight: against the day that
        admitted it, and the next starts clean."""
        before = NOON.replace(hour=23, minute=59, second=59)
        clock = Clock(before, before, before + timedelta(seconds=2))
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(spa, tmp_path, fake, snapshot, clock=clock)
            with visit(service.app) as client:
                ask(client)
        [(_, day, usd, settled)] = service.rows()
        assert (day, settled) == ('2026-09-29', 1)
        assert service.ledger.usage('2026-09-30').spent == 0

    def test_refunds_a_turn_the_api_refused(self, spa, tmp_path, snapshot, ledger_calls):
        """An error the API answered with: nothing was generated, so
        nothing was billed. Tried once (#115)."""
        with FakeDeepSeek(Reply(status=500)) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.status == 200
        name, error = answered.last
        assert (name, error['code']) == ('error', 'model')
        assert len(fake.requests) == 1
        assert [kind for kind, *_ in ledger_calls] == ['reserve', 'refund']
        assert service.rows() == []

    def test_refunds_a_turn_the_model_was_never_reached_for(
        self, spa, tmp_path, snapshot, ledger_calls,
    ):
        """No connection, so no request: nothing can have been billed."""
        with FakeDeepSeek() as fake:
            gone = Gone(fake.url)
        service = chat(spa, tmp_path, gone, snapshot)
        with visit(service.app) as client:
            answered = ask(client)
        assert answered.last[1]['code'] == 'model'
        assert [kind for kind, *_ in ledger_calls] == ['reserve', 'refund']
        assert service.rows() == []

    @pytest.mark.parametrize(
        'reply,code',
        [
            (Reply(content='Counting the dependants', drop_after=3), 'model'),
            (Reply(content='Counting the dependants', error_after=2), 'model'),
        ],
        ids=['cut mid-stream', 'an error inside the stream'],
    )
    def test_keeps_holding_a_turn_lost_on_the_way(
        self, spa, tmp_path, snapshot, reply, code,
    ):
        """It may have been answered, and billed: refunding it would be
        the one way past the cap."""
        with FakeDeepSeek(reply) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.last[1]['code'] == code
        messages = fake.requests[0].body['messages']
        day = service.ledger.usage(spend_day(NOON))
        assert day.spent == 0
        assert day.held == pytest.approx(worst_case(messages, NOON))

    def test_keeps_holding_a_turn_past_its_time(self, spa, tmp_path, snapshot):
        held = threading.Event()
        with FakeDeepSeek(Reply(content='slow', wait=held)) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                pacing=Pacing(turn_seconds=0.3, keepalive_seconds=15),
            )
            with visit(service.app) as client:
                answered = ask(client)

        name, error = answered.last
        assert (name, error['code']) == ('error', 'timeout')
        messages = fake.requests[0].body['messages']
        day = service.ledger.usage(spend_day(NOON))
        assert day.held == pytest.approx(
            worst_case(messages, NOON, seconds=0.3),
        )

    def test_keeps_the_worst_case_of_a_turn_that_said_nothing_of_its_cost(
        self, spa, tmp_path, snapshot,
    ):
        with FakeDeepSeek(Reply(content='ok', usage=None)) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            with visit(service.app) as client:
                answered = ask(client)

        messages = fake.requests[0].body['messages']
        day = service.ledger.usage(spend_day(NOON))
        assert (day.held, day.spent) == (
            0, pytest.approx(worst_case(messages, NOON)),
        )
        assert answered.last[1]['cost_usd'] is None

    def test_answers_429_when_the_day_cannot_pay_for_a_first_turn(
        self, spa, tmp_path, snapshot,
    ):
        with FakeDeepSeek() as fake:
            service = chat(
                spa, tmp_path, fake, snapshot, DAILY_SPEND_CAP_USD='0.000001',
            )
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.status == 429
        assert answered.json() == {'error': USED_UP, 'code': 'budget'}
        assert answered.headers['cache-control'] == 'no-store'
        assert fake.requests == []
        assert service.in_flight() == 0

    def test_says_so_when_the_day_cannot_pay_for_a_later_turn(
        self, spa, tmp_path, snapshot,
    ):
        """The first turn's worst case is exactly what is left: it is
        admitted, and costs something, and the second cannot be."""
        opening = [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': QUESTION},
        ]
        cap = worst_case(opening, NOON)
        with FakeDeepSeek(calls(('language_coverage', '{}')), Reply(content='x')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot, DAILY_SPEND_CAP_USD=repr(cap),
            )
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.status == 200
        assert len(fake.requests) == 1
        name, error = answered.last
        assert (name, error['code']) == ('error', 'budget')
        assert error['message'] == USED_UP
        assert [settled for *_, settled in service.rows()] == [1]

    def test_refuses_what_it_cannot_count_rather_than_go_uncounted(
        self, spa, tmp_path, snapshot, monkeypatch,
    ):
        def unwritable(self: SpendLedger, *args: Any) -> bool:
            raise sqlite3.OperationalError('disk I/O error')

        with FakeDeepSeek() as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            monkeypatch.setattr(SpendLedger, 'reserve', unwritable)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.status == 503
        assert answered.json()['code'] == 'unavailable'
        assert fake.requests == []

    def test_still_answers_when_a_turn_cannot_be_settled(
        self, spa, tmp_path, snapshot, monkeypatch,
    ):
        """The model is paid for either way; its worst case stays held."""

        def unwritable(self: SpendLedger, *args: Any) -> None:
            raise sqlite3.OperationalError('disk I/O error')

        with FakeDeepSeek(Reply(content='answered')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            monkeypatch.setattr(SpendLedger, 'settle', unwritable)
            with visit(service.app) as client:
                answered = ask(client)

        assert answered.text == 'answered'
        assert answered.last[0] == 'done'
        assert service.ledger.usage(spend_day(NOON)).held > 0

    def test_keeps_no_ledger_with_no_cap(self, spa, tmp_path, snapshot):
        with FakeDeepSeek(Reply(content='ok')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                DAILY_SPEND_CAP_USD='0',
            )
            with visit(service.app) as client:
                answered = ask(client)
        assert answered.last[0] == 'done'
        assert answered.last[1]['cost_usd'] > 0
        assert service.rows() == []

    def test_admits_no_more_questions_at_once_than_the_cap_can_pay_for(
        self, spa: Path, tmp_path: Path, snapshot: Path,
    ) -> None:
        """20 questions at once against a $5 cap were all admitted, once,
        and about $44 of calls made (#33)."""
        opening = [
            {'role': 'system', 'content': SYSTEM_PROMPT},
            {'role': 'user', 'content': QUESTION},
        ]
        fits = 2
        cap = worst_case(opening, NOON) * (fits + 0.5)
        held = threading.Event()
        replies = [Reply(content='ok', wait=held) for _ in range(fits)]
        statuses: list[int] = []
        with FakeDeepSeek(*replies) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                DAILY_SPEND_CAP_USD=repr(cap), CHAT_MAX_IN_FLIGHT='10',
            )
            with visit(service.app) as client:
                payloads = [solve(client) for _ in range(6)]

                def asking(payload: str) -> None:
                    answered = ask(
                        client, body={'question': QUESTION, 'altcha': payload},
                    )
                    statuses.append(answered.status)

                threads = [
                    threading.Thread(target=asking, args=(payload,))
                    for payload in payloads
                ]
                for thread in threads:
                    thread.start()
                waited = time.monotonic()
                while len(statuses) < 6 - fits or len(fake.requests) < fits:
                    assert time.monotonic() - waited < 20, (statuses, fake.requests)
                    time.sleep(0.01)
                held.set()
                for thread in threads:
                    thread.join(timeout=20)

        assert sorted(statuses) == [200] * fits + [429] * (6 - fits)
        assert len(fake.requests) == fits
        day = service.ledger.usage(spend_day(NOON))
        assert day.held == 0
        assert day.spent <= cap


# ---- a client that goes away -------------------------------------------


class TestAClientThatGoes:
    """Stops the loop after the turn in flight, which is heard out and
    settled: cut off, the turn could still be billed, and its cost would
    never be known. Served as `web serve` serves it, on uvicorn, since
    only a real connection can be dropped."""

    def test_stops_after_the_turn_in_flight_and_settles_it(
        self, spa, tmp_path, snapshot,
    ):
        held = threading.Event()
        turn = Reply(
            content='Let me look that up.',
            calls=[Call('call_00', 'language_coverage', '{}')],
            finish='tool_calls',
            wait=held,
            wait_at=2,
        )
        with FakeDeepSeek(turn, Reply(content='never asked')) as fake:
            service = chat(spa, tmp_path, fake, snapshot)
            asking = service.app.state.asking
            with Serving(service.app) as base:
                host, port = base.removeprefix('http://').split(':')
                with urllib.request.urlopen(f'{base}/api/ask/challenge') as issued:
                    payload = solved(json.load(issued))
                body = json.dumps({'question': QUESTION, 'altcha': payload})
                with socket.create_connection((host, int(port)), timeout=20) as sock:
                    sock.sendall(
                        f'POST /api/ask HTTP/1.1\r\n'
                        f'Host: {host}:{port}\r\n'
                        f'Origin: {base}\r\n'
                        f'Content-Type: application/json\r\n'
                        f'Content-Length: {len(body)}\r\n\r\n{body}'.encode(),
                    )
                    heard = b''
                    while b'event: text' not in heard:
                        more = sock.recv(4096)
                        assert more, heard
                        heard += more
                # Gone, with the turn still streaming: wait for the
                # service to see it, then let the turn finish.
                [question] = list(asking.questions)
                wait_for(lambda: question.gone)
                held.set()
                wait_for(lambda: asking.in_flight.count == 0)

        assert b' 200 ' in heard.split(b'\r\n')[0]
        # The turn in flight, and no other: its tools were not run, for
        # no one, and no turn was made after it.
        assert question.outcome == 'client gone'
        assert question.turns == 1
        assert question.calls == 0
        assert len(fake.requests) == 1
        [(_, day, usd, settled)] = service.rows()
        assert (day, settled) == ('2026-09-29', 1)
        assert usd == pytest.approx(cost(usage(), NOON, NOON))


def wait_for(condition: Callable[[], bool], seconds: float = 20) -> None:
    waited = time.monotonic()
    while True:
        try:
            if condition():
                return
        except RuntimeError:
            # A set the service changed while the test read it.
            pass
        assert time.monotonic() - waited < seconds, 'never happened'
        time.sleep(0.01)


# ---- the refusals, cheapest first -------------------------------------


class TestTheRefusals:
    """Before anything is spent on a question: same origin, the client's
    rate, the questions in flight, the proof of work, then the day's
    cap (#128, section 2.6). None of them reaches the model."""

    @pytest.fixture
    def service(self, spa, tmp_path, snapshot) -> Iterator[tuple[Chat, FakeDeepSeek]]:
        with FakeDeepSeek(Reply(content='answered'), Reply(content='again')) as fake:
            yield chat(spa, tmp_path, fake, snapshot), fake

    def refused(self, answered: Answered, status: int, code: str) -> dict[str, Any]:
        assert answered.status == status, answered.body
        assert answered.headers['cache-control'] == 'no-store'
        payload: dict[str, Any] = answered.json()
        assert payload['code'] == code
        assert payload['error']
        return payload

    def test_without_a_key_the_chat_is_off(self, spa, tmp_path):
        settings = settings_from(
            {'ALTCHA_HMAC_KEY': KEY, 'WEB_STATE_DIR': str(tmp_path / 's')},
            spa=spa,
        )
        with visit(create_app(settings)) as client:
            answered = ask(client, altcha='whatever')
            challenge = client.get('/api/ask/challenge')
        payload = self.refused(answered, 503, 'off')
        assert payload['error'] == (
            'AI answers are not configured on this deployment.'
        )
        # Said before the page solves a challenge for a question that
        # could not be answered.
        assert challenge.status_code == 503
        assert challenge.json()['code'] == 'off'

    @pytest.mark.parametrize(
        'headers',
        [
            {
                'origin': 'https://elsewhere.example',
                'sec-fetch-site': 'cross-site',
            },
            {'origin': 'null'},
            {'origin': 'http://testserver:8080'},
        ],
        ids=['another site', 'an opaque origin', 'another port'],
    )
    def test_from_another_origin(self, service, headers):
        """Or any page could have its visitors' browsers ask, each from
        another address, where no per-client limit sees a pattern."""
        chat_, fake = service
        with visit(chat_.app) as client:
            answered = ask(client, headers=headers)
        self.refused(answered, 403, 'origin')
        assert fake.requests == []

    def test_from_nowhere_it_says(self, service):
        """Every browser names the origin of a POST: a caller that names
        none is not the page (`isSameOrigin`, chat.ts)."""
        chat_, fake = service
        with visit(chat_.app) as client:
            response = client.post(
                '/api/ask',
                content=json.dumps(
                    {'question': QUESTION, 'altcha': solve(client)},
                ),
                headers={'content-type': 'application/json'},
            )
        assert response.status_code == 403
        assert fake.requests == []

    def test_takes_sec_fetch_site_without_an_origin(self, service):
        chat_, _ = service
        with visit(chat_.app) as client:
            response = client.post(
                '/api/ask',
                content=json.dumps(
                    {'question': QUESTION, 'altcha': solve(client)},
                ),
                headers={
                    'content-type': 'application/json',
                    'sec-fetch-site': 'same-origin',
                },
            )
        assert response.status_code == 200

    def test_takes_the_origin_the_tunnel_forwards(self, service):
        """cloudflared hands on the visitor's Host, and speaks HTTP to
        the service: the page's origin is https all the same."""
        chat_, _ = service
        with visit(chat_.app) as client:
            answered = ask(
                client,
                headers={
                    'host': 'sbom.example.org',
                    'origin': 'https://sbom.example.org',
                },
            )
        assert answered.status == 200

    def test_a_body_not_declared_as_json(self, service):
        """A cross-site text/plain POST needs no preflight; JSON does."""
        chat_, fake = service
        with visit(chat_.app) as client:
            answered = ask(
                client, headers={
                    'content-type': 'text/plain;charset=UTF-8',
                },
            )
        self.refused(answered, 415, 'json')
        assert fake.requests == []

    def test_takes_json_declared_with_a_charset(self, service):
        chat_, _ = service
        with visit(chat_.app) as client:
            answered = ask(
                client, headers={
                    'content-type': 'application/json; charset=utf-8',
                },
            )
        assert answered.status == 200

    def test_never_grants_the_preflight_a_cross_site_json_request_needs(
        self, service,
    ):
        chat_, _ = service
        with visit(chat_.app) as client:
            response = client.options(
                '/api/ask',
                headers={
                    'origin': 'https://elsewhere.example',
                    'access-control-request-method': 'POST',
                    'access-control-request-headers': 'content-type',
                },
            )
        assert response.status_code == 405
        assert 'access-control-allow-origin' not in response.headers

    def test_a_body_declared_too_large_is_not_read(self, service):
        chat_, fake = service
        with visit(chat_.app) as client:
            answered = ask(
                client, headers={
                    'content-length': str(2 * 1024 * 1024),
                },
            )
        self.refused(answered, 413, 'size')
        assert fake.requests == []

    def test_a_body_too_large_that_declares_no_length_is_abandoned(self, service):
        chat_, fake = service
        padded = json.dumps({'question': QUESTION}) + ' ' * (2 * 1024 * 1024)

        def body() -> Iterator[bytes]:
            data = padded.encode()
            for at in range(0, len(data), 64 * 1024):
                yield data[at:at + 64 * 1024]

        with visit(chat_.app) as client:
            response = client.post(
                '/api/ask', content=body(),
                headers={'content-type': 'application/json', 'origin': ORIGIN},
            )
        assert response.status_code == 413
        assert fake.requests == []

    @pytest.mark.parametrize(
        'body,said',
        [
            ('{oops', 'not valid JSON'),
            ('[1, 2]', 'a JSON object'),
            ('{}', 'a question'),
            ('{"question": 7}', 'a question'),
            ('{"question": "   "}', 'a question'),
            (json.dumps({'question': 'x' * 4_001}), 'Question too long: 4001'),
            ('{"question": "\\ud800"}', 'text'),
            ('{"question": "q", "prior": "earlier"}', 'prior'),
            (
                json.dumps({
                    'question': 'q', 'prior': [
                        {'q': 'a', 'a': 'b'},
                    ] * 4,
                }), 'at most 3',
            ),
            (json.dumps({'question': 'q', 'prior': [{'q': 'a'}]}), 'prior'),
            (
                json.dumps({
                    'question': 'q', 'prior': [
                        {'q': 'a', 'a': 5},
                    ],
                }), 'prior',
            ),
            (
                json.dumps({
                    'question': 'q', 'prior': [
                        {'q': 'x' * 6_000, 'a': 'y' * 6_001},
                    ],
                }),
                '12000',
            ),
            ('{"question": "q", "altcha": 5}', 'altcha'),
            ('[' * 100_000, 'not valid JSON'),
        ],
        ids=[
            'not JSON', 'not an object', 'no question', 'a number',
            'a blank', 'too long', 'half a surrogate pair',
            'prior not a list', 'four earlier exchanges',
            'an exchange with no answer', 'an answer not text',
            'earlier exchanges too long', 'a challenge not text',
            'nested too deep',
        ],
    )
    def test_a_body_that_is_not_a_question(self, service, body, said):
        chat_, fake = service
        with visit(chat_.app) as client:
            response = client.post(
                '/api/ask', content=body,
                headers={'content-type': 'application/json', 'origin': ORIGIN},
            )
        assert response.status_code == 400, response.text
        assert said in response.json()['error']
        assert response.json()['code'] == 'invalid'
        assert fake.requests == []

    def test_a_question_as_long_as_a_question_may_be(self, service):
        chat_, _ = service
        with visit(chat_.app) as client:
            answered = ask(client, 'x' * 4_000)
        assert answered.status == 200

    def test_earlier_exchanges_as_long_as_they_may_be(self, service):
        chat_, _ = service
        prior = [{'q': 'x' * 2_000, 'a': 'y' * 2_000}] * 3
        with visit(chat_.app) as client:
            answered = ask(client, prior=prior)
        assert answered.status == 200

    def test_over_the_clients_rate(self, spa, tmp_path, snapshot):
        """Counted as the challenge is, against CHAT_RATE_LIMIT: two a
        question, per client."""
        with FakeDeepSeek(Reply(content='a')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                CHAT_RATE_LIMIT='3/60',
            )
            with visit(service.app) as client:
                first = ask(client)
                spare = solve(client)
                over = ask(client, altcha=spare)
            with visit(service.app, '203.0.113.99') as other:
                elsewhere = other.get('/api/ask/challenge')
        assert first.status == 200
        payload = over.json()
        assert over.status == 429
        assert payload == {
            'error': 'Too many questions. Wait a moment.', 'code': 'rate',
        }
        assert elsewhere.status_code == 200
        assert len(fake.requests) == 1

    def test_over_the_questions_in_flight(
        self, spa: Path, tmp_path: Path, snapshot: Path,
    ) -> None:
        """The cap is the service's, whoever asks: a full house answers
        503, and spends nothing of the question it turns away, its
        challenge included."""
        held = threading.Event()
        with FakeDeepSeek(Reply(content='first', wait=held), Reply(content='second')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                CHAT_MAX_IN_FLIGHT='1',
            )
            with visit(service.app) as client:
                answers: list[Answered] = []
                first = threading.Thread(
                    target=lambda: answers.append(ask(client)),
                )
                first.start()
                waited = time.monotonic()
                while not fake.requests:
                    assert time.monotonic() - waited < 20
                    time.sleep(0.01)
                payload = solve(client)
                busy = ask(client, altcha=payload)
                held.set()
                first.join(timeout=20)
                waited = time.monotonic()
                while service.in_flight():
                    assert time.monotonic() - waited < 20
                    time.sleep(0.01)
                again = ask(client, altcha=payload)

        assert busy.status == 503
        assert busy.json()['code'] == 'busy'
        assert answers[0].text == 'first'
        assert again.status == 200
        assert again.text == 'second'

    def test_without_a_challenge(self, service):
        chat_, fake = service
        with visit(chat_.app) as client:
            answered = ask(client, altcha=None)
        self.refused(answered, 400, 'verification-required')
        assert fake.requests == []

    def test_with_a_challenge_not_solved(self, service):
        chat_, fake = service
        with visit(chat_.app) as client:
            answered = ask(client, altcha='bm90IGEgc29sdXRpb24=')
        payload = self.refused(answered, 403, 'verification-failed')
        assert payload['verdict'] == 'malformed'
        assert fake.requests == []

    def test_with_another_clients_challenge(self, service):
        chat_, fake = service
        with visit(chat_.app, '203.0.113.8') as theirs:
            payload = solve(theirs)
        with visit(chat_.app) as client:
            answered = ask(client, altcha=payload)
        assert self.refused(answered, 403, 'verification-failed')['verdict'] == (
            'other client'
        )
        assert fake.requests == []

    def test_with_a_challenge_used_before(self, service):
        chat_, fake = service
        with visit(chat_.app) as client:
            payload = solve(client)
            first = ask(client, altcha=payload)
            again = ask(client, altcha=payload)
        assert first.status == 200
        assert self.refused(
            again, 403, 'verification-failed',
        )['verdict'] == 'replayed'
        assert len(fake.requests) == 1

    def test_with_a_challenge_past_its_time(self, service):
        chat_, fake = service
        issued = chat_.challenges.issue(
            '198.51.100.20', now=time.time() - 3_600,
        )
        with visit(chat_.app) as client:
            answered = ask(client, altcha=solved(issued))
        assert self.refused(
            answered, 403, 'verification-failed',
        )['verdict'] == 'expired'
        assert fake.requests == []

    def test_checks_the_challenge_once_a_question_for_its_client_alone(
        self, spa, tmp_path, snapshot,
    ):
        """The address the edge names, when the peer is the edge: the
        challenge's client and the question's are one key."""
        with FakeDeepSeek(Reply(content='ok'), Reply(content='ok')) as fake:
            service = chat(
                spa, tmp_path, fake, snapshot,
                EDGE_SUBNET='172.30.0.0/24',
            )
            with visit(service.app, '172.30.0.2') as tunnel:
                mine = solve(tunnel, **{'cf-connecting-ip': '203.0.113.7'})
                theirs = ask(
                    tunnel, altcha=mine,
                    headers={'cf-connecting-ip': '203.0.113.9'},
                )
                ours = ask(
                    tunnel, altcha=mine,
                    headers={'cf-connecting-ip': '203.0.113.7'},
                )
        assert theirs.status == 403
        assert ours.status == 200

    def test_a_method_it_does_not_take(self, service):
        chat_, _ = service
        with visit(chat_.app) as client:
            response = client.get('/api/ask')
        assert response.status_code == 405
        assert response.headers['allow'] == 'POST'
