"""One turn of the chat's model, streamed (#140).

The model is DeepSeek's `deepseek-flash` (#128, the owner's decision on
Q8), reached through its OpenAI-format API with the `openai` SDK. A turn
is one streamed call, as DeepSeek's Chat Completions reference describes
its streams (read on 2026-09-29):

  - the answer comes as deltas, handed on as they arrive;
  - the model's reasoning comes as `reasoning_content` deltas, kept to
    be sent back: with tools, DeepSeek's thinking mode wants every
    earlier turn's reasoning in each request after it, and answers 400
    without it;
  - a tool call comes in pieces, keyed by its index, the first with its
    id and name and the rest with its arguments;
  - the usage comes on the last chunk, beside the finish reason, which
    the spend cap settles at. OpenAI's own chunk for it is read too.

A turn is one attempt. The SDK sends a call again after a dropped
connection, a timeout, a 429 or a 5xx, and after a drop or a timeout
the first attempt may have been answered and billed: a second billed
call under the one reservation (#115). So its retries are off, and how a
turn fails says what its reservation becomes (`spend`):

  - `Refused`: the API answered with an error, or no connection was
    made to send the call on. Nothing was generated, so nothing was
    billed: the reservation is refunded.
  - `Lost`: a turn past its time, a connection cut mid-stream, an error
    inside the stream. It may have been answered and billed all the
    same: its worst case stays held for the day.
"""
import asyncio
import json
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import cast

import httpx2
import structlog
from openai import APIConnectionError
from openai import APIError
from openai import APIStatusError
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletionChunk
from openai.types.shared import ReasoningEffort

from chatsbom.server.pricing import Usage
from chatsbom.server.prompt import TOOLS
from chatsbom.server.settings import ChatSettings

logger = structlog.get_logger('chat')

#: The most one turn may generate, reasoning included, which the spend
#: cap reserves for before the call. DeepSeek allows 384K and defaults
#: to 64K in thinking mode; a turn that picks a tool or writes a short
#: answer from a few results needs a small part of that, and at 60
#: tokens a second this much is about TURN_SECONDS. A turn cut off at
#: it says so (`ask`).
MAX_OUTPUT_TOKENS = 16_384

#: DeepSeek's thinking mode, at the effort its guide gives agent work:
#: stated, rather than left to a default that could change under the
#: budget.
THINKING = {'type': 'enabled'}
REASONING_EFFORT: ReasoningEffort = 'high'

#: The most one turn may take, from the call to its last chunk. A turn
#: still going is abandoned, and its worst case stays held.
TURN_SECONDS = 300.0

#: The longest wait for a connection, and for any byte of an answer:
#: DeepSeek sends a comment every so often while a call waits for the
#: model, so a silence this long is a connection that has gone.
CONNECT_SECONDS = 10.0
READ_SECONDS = 120.0

#: The failures that happen before a call is sent: nothing reached the
#: API, so nothing can have been billed.
NEVER_SENT = (httpx2.ConnectError, httpx2.ConnectTimeout, httpx2.PoolTimeout)

#: Tokens the API adds of its own around what is sent: the markers
#: between messages, and the words that introduce the tools. The
#: Worker allowed as many for Anthropic's.
FRAMING_TOKENS = 2_048

#: What every call sends beside its messages: the tools, as JSON.
_TOOLS_BYTES = len(json.dumps(list(TOOLS)).encode())


def most_input_tokens(messages: Sequence[Mapping[str, Any]]) -> int:
    """The most prompt tokens a call with `messages` can be billed for:
    what the spend cap holds for its input before it is made.

    A bound rather than a guess, since a hold the call could exceed
    would make the cap one too. A token is never less than a byte,
    however text is split, so every byte of the messages and the tools,
    as JSON with every character outside ASCII escaped, is counted a
    token, and the framing on top (`chat.ts`, `worstCaseUsd`).
    """
    return (
        _TOOLS_BYTES + len(json.dumps(list(messages)).encode())
        + FRAMING_TOKENS
    )


class Refused(Exception):
    """The API answered with an error, `status`, or could not be reached
    at all (None): nothing was billed. What it said is not kept here:
    it can echo the request."""

    def __init__(self, status: int | None) -> None:
        super().__init__(
            f'the model refused the call ({status})' if status
            else 'the model could not be reached',
        )
        self.status = status


class Lost(Exception):
    """The turn may have been answered, and billed, and was not heard
    out: past its time (`timed_out`), or cut off."""

    def __init__(self, why: str, *, timed_out: bool = False) -> None:
        super().__init__(why)
        self.timed_out = timed_out


@dataclass(frozen=True)
class ToolCall:
    """A tool call as the model wrote it: its arguments are its text."""

    id: str
    name: str
    arguments: str


@dataclass
class Turn:
    """What one turn of the model said, and what it used."""

    text: str = ''
    reasoning: str = ''
    calls: list[ToolCall] = field(default_factory=list)
    #: Why the model stopped: None when the stream never said.
    finish: str | None = None
    #: None when the stream never said, or said what cannot be read.
    usage: Usage | None = None

    def assistant(self) -> dict[str, Any]:
        """The turn as the next request carries it: as the model wrote
        it, its reasoning included, so that the request's head is the
        same bytes DeepSeek has cached."""
        message: dict[str, Any] = {
            'role': 'assistant',
            'content': self.text,
            'reasoning_content': self.reasoning,
        }
        if self.calls:
            message['tool_calls'] = [
                {
                    'id': call.id,
                    'type': 'function',
                    'function': {'name': call.name, 'arguments': call.arguments},
                }
                for call in self.calls
            ]
        return message


def client(settings: ChatSettings) -> AsyncOpenAI:
    """A client for the model the settings name, which the caller
    closes: one attempt a call, and never a minute's wait for a byte."""
    return AsyncOpenAI(
        api_key=settings.api_key,
        base_url=settings.base_url,
        max_retries=0,
        timeout=httpx2.Timeout(READ_SECONDS, connect=CONNECT_SECONDS),
    )


class _Heard:
    """A turn, as its chunks arrive."""

    def __init__(self, on_text: Callable[[str], None]) -> None:
        self.on_text = on_text
        self.text: list[str] = []
        self.reasoning: list[str] = []
        self.calls: dict[int, dict[str, str]] = {}
        self.finish: str | None = None
        self.usage: Usage | None = None

    def chunk(self, chunk: ChatCompletionChunk) -> None:
        if chunk.usage is not None:
            self.usage = Usage.reported(chunk.usage.model_dump())
        for choice in chunk.choices or ():
            if choice.index != 0:
                continue
            delta = choice.delta
            if delta.content:
                self.text.append(delta.content)
                self.on_text(delta.content)
            reasoning = (delta.model_extra or {}).get('reasoning_content')
            if isinstance(reasoning, str):
                self.reasoning.append(reasoning)
            for piece in delta.tool_calls or ():
                call = self.calls.setdefault(
                    getattr(piece, 'index', 0),
                    {'id': '', 'name': '', 'arguments': ''},
                )
                call['id'] = call['id'] or piece.id or ''
                if piece.function is not None:
                    call['name'] = call['name'] or piece.function.name or ''
                    call['arguments'] += piece.function.arguments or ''
            if choice.finish_reason:
                self.finish = choice.finish_reason

    def turn(self) -> Turn:
        return Turn(
            text=''.join(self.text),
            reasoning=''.join(self.reasoning),
            calls=[
                ToolCall(call['id'], call['name'], call['arguments'])
                for _, call in sorted(self.calls.items())
            ],
            finish=self.finish,
            usage=self.usage,
        )


async def turn(
    model: AsyncOpenAI,
    name: str,
    messages: Sequence[Mapping[str, Any]],
    on_text: Callable[[str], None],
    *,
    seconds: float = TURN_SECONDS,
) -> Turn:
    """One turn of the model `name`, on `messages`, with the tools,
    handing each delta of its answer to `on_text` as it comes.

    Raises `Refused` for a call nothing billed, and `Lost` for one that
    may have been; said in the log, and never in what it raises, since
    what an API says of a request can repeat it.
    """
    heard = _Heard(on_text)
    try:
        async with asyncio.timeout(seconds):
            stream = await model.chat.completions.create(
                model=name,
                messages=cast(Any, list(messages)),
                tools=cast(Any, list(TOOLS)),
                max_tokens=MAX_OUTPUT_TOKENS,
                reasoning_effort=REASONING_EFFORT,
                stream=True,
                stream_options={'include_usage': True},
                extra_body={'thinking': THINKING},
            )
            async with stream:
                async for chunk in stream:
                    heard.chunk(chunk)
    except TimeoutError:
        logger.warning('model turn past its time', seconds=seconds)
        raise Lost('the turn ran past its time', timed_out=True) from None
    except APIStatusError as error:
        logger.warning(
            'model refused the call', status=error.status_code,
            error=error.message,
        )
        raise Refused(error.status_code) from None
    except APIConnectionError as error:
        logger.warning(
            'model not reached', error=repr(error.__cause__ or error),
        )
        if isinstance(error.__cause__, NEVER_SENT):
            raise Refused(None) from None
        raise Lost('the connection to the model failed') from None
    except APIError as error:
        logger.warning('model stream failed', error=error.message)
        raise Lost('the model reported an error mid-stream') from None
    except (httpx2.HTTPError, httpx2.StreamError) as error:
        logger.warning('model stream broke off', error=repr(error))
        raise Lost('the model stream broke off') from None
    return heard.turn()
