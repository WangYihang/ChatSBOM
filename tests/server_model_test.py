"""One turn of the chat's model, streamed (#140).

The model is DeepSeek's `deepseek-flash`, reached through its
OpenAI-format API with the `openai` SDK. A turn is one streamed call:
its answer comes as deltas, sent on as they arrive; its tool calls come
in pieces, put together here; and its usage, which the spend cap
settles at, comes on the last chunk. Here against a stand-in on a
socket of its own (`tests/fake_deepseek_test.py`): no test calls DeepSeek.

How a turn fails decides what its reservation becomes (`spend`):

  - refused: the API answered with an error, or was never reached.
    Nothing was generated, so nothing was billed, and the reservation
    is refunded.
  - lost: a timeout, a connection cut mid-stream, an error inside the
    stream. It may have been answered and billed all the same, so its
    worst case stays held.

And it is one attempt either way: the SDK's retries are off (#115).
"""
import asyncio
import json
import socket
import threading
import time
from typing import Any

import pytest

from chatsbom.server.model import client
from chatsbom.server.model import Lost
from chatsbom.server.model import MAX_OUTPUT_TOKENS
from chatsbom.server.model import Refused
from chatsbom.server.model import ToolCall
from chatsbom.server.model import Turn
from chatsbom.server.model import turn
from chatsbom.server.pricing import Prices
from chatsbom.server.pricing import Usage
from chatsbom.server.prompt import SYSTEM_PROMPT
from chatsbom.server.prompt import TOOLS
from chatsbom.server.settings import ChatSettings
from tests.fake_deepseek_test import Call
from tests.fake_deepseek_test import FakeDeepSeek
from tests.fake_deepseek_test import Reply
from tests.fake_deepseek_test import usage

MESSAGES: list[dict[str, Any]] = [
    {'role': 'system', 'content': SYSTEM_PROMPT},
    {'role': 'user', 'content': 'Who declares mail?'},
]


def settings(url: str) -> ChatSettings:
    return ChatSettings(
        api_key='sk-test', base_url=url, model='deepseek-flash',
        prices=Prices(), max_in_flight=3,
    )


def take(
    url: str, *, seconds: float = 30.0,
) -> tuple[Turn, list[str]]:
    """One turn against the model at `url`, and each delta of its
    answer, in the order they were handed on."""
    deltas: list[str] = []

    async def asking() -> Turn:
        async with client(settings(url)) as model:
            return await turn(
                model, 'deepseek-flash', MESSAGES, deltas.append,
                seconds=seconds,
            )

    return asyncio.run(asking()), deltas


def failed(url: str, **options: Any) -> tuple[BaseException, list[str]]:
    """How a turn against `url` failed, and what it handed on first."""
    deltas: list[str] = []

    async def asking() -> None:
        async with client(settings(url)) as model:
            await turn(model, 'deepseek-flash', MESSAGES, deltas.append, **options)

    with pytest.raises((Refused, Lost)) as failure:
        asyncio.run(asking())
    return failure.value, deltas


class TestAnAnswer:
    def test_is_handed_on_as_it_comes(self):
        answer = '17 projects declare mail themselves; 118 inherit it.'
        with FakeDeepSeek(Reply(content=answer, reasoning='Count them.')) as fake:
            result, deltas = take(fake.url)
        assert len(deltas) > 1
        assert ''.join(deltas) == answer
        assert result.text == answer
        assert result.reasoning == 'Count them.'
        assert result.finish == 'stop'
        assert result.calls == []

    def test_says_what_it_used(self):
        reply = Reply(content='ok', usage=usage(hit=900, miss=50, output=30))
        with FakeDeepSeek(reply) as fake:
            result, _ = take(fake.url)
        assert result.usage == Usage(
            cache_hit=900, cache_miss=50, output=30, reasoning=40,
        )

    def test_says_it_where_openai_puts_it_too(self):
        """DeepSeek puts the usage on the last chunk beside the finish;
        OpenAI on a chunk of its own after it. Either is read."""
        with FakeDeepSeek(Reply(content='ok', usage_apart=True)) as fake:
            result, _ = take(fake.url)
        assert result.usage == Usage(
            cache_hit=800, cache_miss=200, output=100, reasoning=40,
        )

    def test_says_nothing_of_a_usage_it_was_not_told(self):
        """And the spend cap keeps the worst case held."""
        with FakeDeepSeek(Reply(content='ok', usage=None)) as fake:
            result, _ = take(fake.url)
        assert result.usage is None

    def test_that_ends_without_a_reason_says_so(self):
        with FakeDeepSeek(Reply(content='Counting', finish=None)) as fake:
            result, _ = take(fake.url)
        assert result.finish is None

    def test_is_read_past_deepseeks_keepalive_comments(self):
        """Sent while a request waits for the model (its Rate Limit page,
        read on 2026-09-29)."""
        with FakeDeepSeek(Reply(content='still here', keepalive=True)) as fake:
            result, _ = take(fake.url)
        assert result.text == 'still here'


class TestToolCalls:
    def test_are_put_together_from_their_pieces(self):
        calls = [
            Call('call_00_a', 'ecosystems_for', '{"name": "mail"}'),
            Call('call_01_b', 'language_coverage', '{}'),
        ]
        reply = Reply(
            content='Checking.', reasoning='Two lookups.', calls=calls,
            finish='tool_calls',
        )
        with FakeDeepSeek(reply) as fake:
            result, _ = take(fake.url)
        assert result.finish == 'tool_calls'
        assert result.calls == [
            ToolCall('call_00_a', 'ecosystems_for', '{"name": "mail"}'),
            ToolCall('call_01_b', 'language_coverage', '{}'),
        ]

    def test_go_back_to_the_model_as_it_made_them_reasoning_and_all(self):
        """With tools, DeepSeek's thinking mode wants every earlier
        turn's reasoning sent back, or answers 400 (its Thinking Mode
        guide, read on 2026-09-29); and the arguments go back as their
        text, the same bytes the cache holds."""
        calls = [
            Call(
                'call_00_a', 'dependents_of',
                '{"name":"mail", "limit": 5}',
            ),
        ]
        reply = Reply(
            reasoning='Look it up.', calls=calls, finish='tool_calls',
        )
        with FakeDeepSeek(reply) as fake:
            result, _ = take(fake.url)
        assert result.assistant() == {
            'role': 'assistant',
            'content': '',
            'reasoning_content': 'Look it up.',
            'tool_calls': [{
                'id': 'call_00_a',
                'type': 'function',
                'function': {
                    'name': 'dependents_of',
                    'arguments': '{"name":"mail", "limit": 5}',
                },
            }],
        }


class TestTheRequest:
    def test_is_made_as_deepseek_documents_it(self):
        with FakeDeepSeek(Reply(content='ok')) as fake:
            take(fake.url)
        [request] = fake.requests
        assert request.path == '/chat/completions'
        assert request.headers['authorization'] == 'Bearer sk-test'
        body = request.body
        assert body['model'] == 'deepseek-flash'
        assert body['messages'] == MESSAGES
        assert body['tools'] == json.loads(json.dumps(TOOLS))
        assert body['stream'] is True
        assert body['stream_options'] == {'include_usage': True}
        assert body['max_tokens'] == MAX_OUTPUT_TOKENS
        # Thinking mode, and its effort, stated rather than left to a
        # default that could change under the budget.
        assert body['thinking'] == {'type': 'enabled'}
        assert body['reasoning_effort'] == 'high'

    def test_names_no_reader(self):
        """DeepSeek keys its context cache by `user_id` when one is sent
        (its Rate Limit page), and every question shares the prompt."""
        with FakeDeepSeek(Reply(content='ok')) as fake:
            take(fake.url)
        body = fake.requests[0].body
        assert 'user_id' not in body
        assert 'user' not in body

    def test_bounds_the_output_well_inside_the_models_own(self):
        """384K is the most DeepSeek allows; 64K its default in thinking
        mode. The bound is what the spend cap reserves for."""
        assert 0 < MAX_OUTPUT_TOKENS <= 64 * 1024


class TestARefusal:
    """The API answered with an error: nothing generated, nothing
    billed. Refunded, and never sent again."""

    @pytest.mark.parametrize('status', [400, 401, 402, 422, 429, 500, 503])
    def test_is_an_error_status_tried_once(self, status):
        with FakeDeepSeek(Reply(status=status)) as fake:
            refusal, deltas = failed(fake.url)
        assert isinstance(refusal, Refused)
        assert refusal.status == status
        assert deltas == []
        assert len(fake.requests) == 1

    def test_is_a_model_that_cannot_be_reached(self):
        """No connection, so no request: nothing can have been billed."""
        closed = socket.socket()
        closed.bind(('127.0.0.1', 0))
        port = closed.getsockname()[1]
        closed.close()

        refusal, _ = failed(f'http://127.0.0.1:{port}')

        assert isinstance(refusal, Refused)
        assert refusal.status is None

    def test_never_says_what_the_api_said(self):
        """Upstream text can echo the request back."""
        with FakeDeepSeek(Reply(status=400)) as fake:
            refusal, _ = failed(fake.url)
        assert 'amused' not in str(refusal)


class TestALoss:
    """It may have been answered and billed all the same: the
    reservation's worst case stays held."""

    def test_is_a_connection_cut_mid_stream(self):
        with FakeDeepSeek(Reply(content='Counting the dependants', drop_after=3)) as fake:
            loss, deltas = failed(fake.url)
        assert isinstance(loss, Lost)
        assert not loss.timed_out
        # What came before the cut, the role's chunk and two of the
        # answer's, was handed on as it came.
        assert ''.join(deltas) == 'Counting the d'
        assert len(fake.requests) == 1

    def test_is_an_error_inside_the_stream(self):
        with FakeDeepSeek(Reply(content='Counting the dependants', error_after=2)) as fake:
            loss, _ = failed(fake.url)
        assert isinstance(loss, Lost)

    def test_is_a_turn_past_its_time(self):
        held = threading.Event()
        with FakeDeepSeek(Reply(content='slow', wait=held)) as fake:
            started = time.monotonic()
            loss, _ = failed(fake.url, seconds=0.3)
            took = time.monotonic() - started
        assert isinstance(loss, Lost)
        assert loss.timed_out
        assert 0.3 <= took < 5
