"""POST /api/ask: a question, answered on the server as it streams (#140).

The Worker relayed one model turn a request: the page ran the loop, ran
the tools, and posted each result back, and the Worker checked each
result's shape and never its origin (`web/src/chat.ts`,
`web/src/agent.ts`). Here the loop is the server's (#128, section 2.6,
and the owner's decision on Q5): one request a question, and one answer,
as server-sent events:

  tool   {"name", "arguments"}: a tool the model called, as it is run
  text   {"delta"}: the answer's next words, as the model writes them
  done   {"turns", "usage", "cost_usd"}: the answer is complete
  error  {"code", "message", "turns", ...}: it is not, and why, as a
         code the page puts in its reader's words; any text before it
         is not an answer

The question is `{question, prior, altcha}`: the question, at most three
earlier questions and their answers as text, and a solved challenge
(`challenge`). Each question is a conversation of its own with the
model, which opens with the same system prompt and tools, byte for byte,
so that DeepSeek's cache serves them (`prompt`); the earlier exchanges
go in after them, as the reader's own record, and no earlier turn of the
model's is replayed.

The loop runs at most MAX_TURNS turns, and a turn's tools run here,
against the snapshot the question pinned when it started (`tools`), so
what the model reads of the data is the data. Each turn's worst case is
held against the day's cap before its call, and settled at what it cost
after (`spend`, `pricing`).

Before any of it, the checks, cheapest first (#128, section 2.6):

  1. the page's own origin, and a body declared as JSON (`chat.ts`);
  2. the client's rate, CHAT_RATE_LIMIT, counted with its challenges;
  3. the questions in flight, whoever asks, CHAT_MAX_IN_FLIGHT;
  4. the proof of work, once a question, for this client;
  5. the day's cap, which the first turn's worst case must fit.

Each refusal is JSON, `{"error", "code"}`, with its status, and none of
them reaches the model. A client that goes away mid-answer stops the
loop after the turn in flight, which is heard out and settled: cut off,
it could not be.

The codes, for the page to say in its reader's words:

  refusals   off 503, origin 403, json 415, size 413, rate 429,
             invalid 400, busy 503, verification-required 400,
             verification-failed 403 (with the `verdict`), budget 429,
             unavailable 503
  error      budget, unavailable, model, timeout, cut-off, declined,
  events     stopped (with the `reason`), garbled, turns, failed
"""
import asyncio
import enum
import json
import math
import sqlite3
import time
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import structlog
from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from starlette.requests import ClientDisconnect
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from chatsbom.server import model
from chatsbom.server import tools
from chatsbom.server.challenge import Challenges
from chatsbom.server.challenge import Verdict
from chatsbom.server.pricing import Usage
from chatsbom.server.prompt import SYSTEM_PROMPT
from chatsbom.server.ratelimit import RateLimiter
from chatsbom.server.settings import ChatSettings
from chatsbom.server.spend import Budget
from chatsbom.server.spend import Hold
from chatsbom.server.spend import LedgerUnavailable
from chatsbom.server.spend import OverBudget
from chatsbom.server.spend import UNAVAILABLE
from chatsbom.server.spend import USED_UP

logger = structlog.get_logger('chat')

#: A loop that never runs forever: each turn is a paid call, and a model
#: that kept calling tools would spend without bound (`agent.ts`).
MAX_TURNS = 8

#: A question is typed into a one-line box (`chat.ts`).
MAX_QUESTION_CHARS = 4_000

#: The earlier exchanges a question may bring: as text, and no more
#: than a reader's last few.
MAX_PRIOR = 3
MAX_PRIOR_CHARS = 12_000

#: The most a request's body may be: a question and its earlier
#: exchanges, every character written as a JSON escape, and a solved
#: challenge, with room to spare.
MAX_REQUEST_BYTES = 128 * 1024

#: The tools one turn may run: calls past it are answered with an error,
#: and the model may make them in its next turn.
MAX_CALLS_PER_TURN = 16

#: The results one question may gather, in characters: the Worker's
#: bound on a whole conversation. Each result is sent again with every
#: turn after it; one call may take the total past this, and none after
#: it is run.
MAX_RESULTS_CHARS = tools.MAX_CONVERSATION_CHARS

#: A tool's name as it is said in a `tool` event: the longest DeepSeek
#: allows.
MAX_TOOL_NAME = 128

#: How often a comment is sent while nothing else is, so that nothing
#: between the page and the service takes a quiet answer for a dead
#: connection (#128, section 8): the model can think for a while before
#: it writes anything.
KEEPALIVE_SECONDS = 15.0

#: An answer, or a refusal, is for the one who asked, at that moment:
#: nothing is to keep it.
NO_STORE = 'no-store'

# What each refusal and failure says, in English: the page says it in
# its reader's language, by its code.
OFF = 'AI answers are not configured on this deployment.'
CROSS_ORIGIN = "Requests must come from this site's own page."
NOT_JSON = 'Expected content-type: application/json.'
TOO_LARGE = 'Request too large.'
UNREADABLE = 'The request body could not be read.'
TOO_MANY = 'Too many questions. Wait a moment.'
BUSY = 'AI answers are busy. Try again in a moment.'
VERIFICATION_REQUIRED = 'Human verification is required.'
VERIFICATION_FAILED = 'Human verification failed. Reload and retry.'
UNREACHABLE = 'The model could not be reached. Try again shortly.'
TOO_SLOW = 'The model took too long to answer. Try again shortly.'
CUT_OFF = (
    'The answer was cut off at its length limit before it finished. '
    'Try a narrower question.'
)
DECLINED = 'The model declined to answer this question.'
STOPPED = 'The model stopped without an answer ({reason}).'
GARBLED = "The model's answer was not understood."
GAVE_UP = f'Gave up after {MAX_TURNS} turns without a final answer.'
UNEXPECTED = 'Unexpected failure.'
GATHERED = (
    'This question has gathered as much from the dataset as one may: '
    'answer from what you have.'
)
TOO_MANY_CALLS = (
    f'At most {MAX_CALLS_PER_TURN} tools are run in one turn: call the '
    'rest in the next.'
)


def refused(
    status: int, message: str, code: str, **more: object,
) -> JSONResponse:
    """A refusal, as the API answers one: JSON, kept by no one."""
    return JSONResponse(
        {'error': message, 'code': code, **more}, status,
        headers={'Cache-Control': NO_STORE},
    )


class Invalid(Exception):
    """A body that is not a question: a 400, unless it was too large."""

    def __init__(
        self, message: str, *, status: int = 400, code: str = 'invalid',
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class Exchange:
    """An earlier question, and the answer the reader was given."""

    q: str
    a: str


@dataclass(frozen=True)
class Asked:
    """A question, as the page asks it."""

    question: str
    prior: tuple[Exchange, ...]
    #: The solved challenge, as the widget writes it; None if none came.
    altcha: str | None


def _length(value: str) -> int:
    """A text's length as the page counts it, in UTF-16 code units."""
    return len(value.encode('utf-16-le', 'surrogatepass')) // 2


def _text(value: str, what: str) -> str:
    """`value`, if it is text: half of a surrogate pair, which JSON can
    spell, is not, and could not be sent on."""
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        raise Invalid(f'The {what} must be text.') from None
    return value


def parse(body: bytes) -> Asked:
    """The question a body asks, or why it asks none."""
    try:
        document = json.loads(body)
    except (ValueError, RecursionError):
        raise Invalid('Body is not valid JSON.') from None
    if not isinstance(document, dict):
        raise Invalid('Expected a JSON object.')
    question = document.get('question')
    if not isinstance(question, str) or not question.strip():
        raise Invalid('Expected a question.')
    _text(question, 'question')
    if _length(question) > MAX_QUESTION_CHARS:
        raise Invalid(
            f'Question too long: {_length(question)} characters, limit '
            f'{MAX_QUESTION_CHARS}.',
        )
    altcha = document.get('altcha')
    if altcha is not None and not isinstance(altcha, str):
        raise Invalid('Expected altcha to be the solved challenge, as text.')
    return Asked(question, _prior(document.get('prior')), altcha)


def _prior(value: object) -> tuple[Exchange, ...]:
    """The earlier exchanges: text, and only their text."""
    if value is None:
        return ()
    if not isinstance(value, list):
        raise Invalid(
            'Expected prior to be a list of earlier questions and answers.',
        )
    if len(value) > MAX_PRIOR:
        raise Invalid(
            f'Expected at most {MAX_PRIOR} earlier questions and answers '
            'in prior.',
        )
    exchanges = []
    for item in value:
        if not (
            isinstance(item, dict)
            and isinstance(item.get('q'), str)
            and isinstance(item.get('a'), str)
        ):
            raise Invalid('Each of prior is {"q": ..., "a": ...}, both text.')
        exchanges.append(
            Exchange(
                _text(item['q'], 'earlier question'),
                _text(item['a'], 'earlier answer'),
            ),
        )
    total = sum(_length(e.q) + _length(e.a) for e in exchanges)
    if total > MAX_PRIOR_CHARS:
        raise Invalid(
            f'The earlier questions and answers are {total} characters, '
            f'limit {MAX_PRIOR_CHARS}.',
        )
    return tuple(exchanges)


def opening(asked: Asked) -> list[dict[str, Any]]:
    """A question's first turn: the system prompt, the same for every
    question, and then the question, after the earlier exchanges as the
    reader's own record of them."""
    text = asked.question
    if asked.prior:
        earlier = '\n\n'.join(
            f'Question: {exchange.q}\nAnswer: {exchange.a}'
            for exchange in asked.prior
        )
        text = (
            "The reader's earlier questions in this conversation, and the "
            f'answers they were given:\n\n{earlier}\n\n'
            f'Their question now:\n{asked.question}'
        )
    return [
        {'role': 'system', 'content': SYSTEM_PROMPT},
        {'role': 'user', 'content': text},
    ]


def same_origin(
    fetch_site: str | None, origin: str | None, host: str | None,
) -> bool:
    """Whether a request came from this deployment's own page.

    Every call here is paid for, so no other site may make one: without
    this, any page could have its visitors' browsers post questions,
    each from a different address, so the per-client limit never sees a
    pattern (`isSameOrigin`, chat.ts). Browsers say where a request came
    from in two headers a page cannot set: `Sec-Fetch-Site`, which every
    current browser sends, and `Origin`, which they have put on every
    POST for longer. Either one naming this origin is enough, and a
    request with neither is refused: it did not come from a browser.

    The origin is compared by its host alone, with the Host the request
    was sent to: cloudflared hands the visitor's Host on, and speaks
    HTTP to the service, so the scheme the service sees is not the
    page's. Nor is this authentication: it stops a browser being turned
    against the service by another site, and a determined script is
    bounded by the rest.
    """
    if fetch_site == 'same-origin':
        return True
    if origin is None or host is None:
        return False
    parts = urlsplit(origin)
    return (
        parts.scheme in ('http', 'https')
        and bool(parts.netloc)
        and not parts.path
        and parts.netloc.lower() == host.lower()
    )


def is_json(content_type: str | None) -> bool:
    """Whether a body is declared as JSON. Not pedantry: a cross-site
    POST of `text/plain` is sent without asking first, and one of JSON
    must be preflighted, which is never granted (`isJson`, chat.ts)."""
    kind = (content_type or '').split(';')[0].strip().lower()
    return kind == 'application/json'


async def read(request: Request) -> bytes:
    """A request's body, abandoned at the chunk that takes it past
    MAX_REQUEST_BYTES rather than buffered whole."""
    body = bytearray()
    try:
        async for chunk in request.stream():
            body += chunk
            if len(body) > MAX_REQUEST_BYTES:
                raise Invalid(TOO_LARGE, status=413, code='size')
    except ClientDisconnect:
        raise Invalid(UNREADABLE) from None
    return bytes(body)


def sse(event: str, data: Mapping[str, object]) -> bytes:
    """One server-sent event: its name, and its data as one line of
    JSON."""
    line = json.dumps(
        data, ensure_ascii=False, separators=(',', ':'), allow_nan=False,
    )
    return f'event: {event}\ndata: {line}\n\n'.encode()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Pacing:
    """How long a turn may take, and how long the answer may be quiet."""

    turn_seconds: float = model.TURN_SECONDS
    keepalive_seconds: float = KEEPALIVE_SECONDS


class InFlight:
    """The questions being answered, whoever asked them, against a cap.

    Taken and released on the event loop alone, one step at a time: no
    lock is needed.
    """

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.count = 0

    def take(self) -> bool:
        if self.count >= self.limit:
            return False
        self.count += 1
        return True

    def release(self) -> None:
        self.count = max(0, self.count - 1)


class Asking:
    """The chat: what it asks with, and what it holds a question to."""

    def __init__(
        self,
        chat: ChatSettings,
        snapshot: Path,
        *,
        challenges: Challenges,
        limit: RateLimiter,
        budget: Budget | None,
        clock: Callable[[], datetime] = utc_now,
        pacing: Pacing = Pacing(),
    ) -> None:
        self.chat = chat
        self.snapshot = snapshot
        self.challenges = challenges
        self.limit = limit
        self.budget = budget
        self.clock = clock
        self.pacing = pacing
        self.in_flight = InFlight(chat.max_in_flight)
        #: The questions being answered, for the log and the tests.
        self.questions: set[Question] = set()

    def pin(self) -> Path:
        """The snapshot a question answers from, chosen once, as it
        starts, so that its every tool reads one dataset.

        One file, WEB_SNAPSHOT, until #132 publishes snapshots, with a
        CURRENT naming the newest: this is where it is to be read.
        """
        return self.snapshot

    def worst_case(
        self, messages: Sequence[Mapping[str, Any]], now: datetime,
    ) -> float:
        """The most a turn sending `messages` at `now` can cost: at the
        dearest rates of any hour it can run in (`pricing`)."""
        return self.chat.prices.worst_case(
            model.most_input_tokens(messages), model.MAX_OUTPUT_TOKENS,
            now, now + timedelta(seconds=self.pacing.turn_seconds),
        )

    async def hold(
        self, messages: Sequence[Mapping[str, Any]],
    ) -> Hold | None:
        """A turn's worst case, held against the day's cap, or None with
        no cap to hold it against. Raises `OverBudget` or
        `LedgerUnavailable`."""
        if self.budget is None:
            return None
        now = self.clock()
        return await run_in_threadpool(
            self.budget.hold, self.worst_case(messages, now), now,
        )

    async def ask(self, request: Request, client: str) -> Response:
        """A question from `client`: refused, or answered as it streams."""
        headers = request.headers
        if not same_origin(
            headers.get('sec-fetch-site'), headers.get('origin'),
            headers.get('host'),
        ):
            return refused(403, CROSS_ORIGIN, 'origin')
        if not is_json(headers.get('content-type')):
            return refused(415, NOT_JSON, 'json')
        # Free to check, so checked before anything else is spent on the
        # request; a claim, which `read` does not take on trust.
        declared = headers.get('content-length', '')
        if declared.isdigit() and int(declared) > MAX_REQUEST_BYTES:
            return refused(413, TOO_LARGE, 'size')
        if not self.limit.admit(client):
            return refused(429, TOO_MANY, 'rate')
        try:
            asked = parse(await read(request))
        except Invalid as invalid:
            return refused(invalid.status, str(invalid), invalid.code)
        # Before the challenge, which one use spends: a full house turns
        # a question away and leaves its challenge good.
        if not self.in_flight.take():
            return refused(503, BUSY, 'busy')
        try:
            admitted = await self._admit(asked, client)
        except BaseException:
            self.in_flight.release()
            raise
        if isinstance(admitted, Response):
            self.in_flight.release()
            return admitted
        return EventStream(admitted, self.pacing.keepalive_seconds)

    async def _admit(self, asked: Asked, client: str) -> 'Question | Response':
        """The proof of work, then the first turn's worst case: the
        question, or why it is refused."""
        if asked.altcha is None:
            return refused(400, VERIFICATION_REQUIRED, 'verification-required')
        try:
            verdict = await run_in_threadpool(
                self.challenges.verify, asked.altcha, client,
            )
        except (sqlite3.Error, OSError) as error:
            logger.error('challenge not checked', error=str(error))
            return refused(503, UNAVAILABLE, 'unavailable')
        if verdict is not Verdict.VERIFIED:
            return refused(
                403, VERIFICATION_FAILED, 'verification-failed',
                verdict=verdict.value,
            )
        messages = opening(asked)
        # Last of the checks, so that nothing refused for another reason
        # holds any of the day.
        try:
            first = await self.hold(messages)
        except OverBudget:
            return refused(429, USED_UP, 'budget')
        except LedgerUnavailable:
            return refused(503, UNAVAILABLE, 'unavailable')
        return Question(self, messages, first)


class Mark(enum.Enum):
    """What the stream is told besides the answer's events."""

    #: The question is answered, or has failed: its last event is sent.
    END = 'end'
    #: The client went away.
    GONE = 'gone'


class Question:
    """One question, answered: the loop, on the server."""

    def __init__(
        self,
        asking: Asking,
        messages: list[dict[str, Any]],
        first: Hold | None,
    ) -> None:
        self.asking = asking
        self.messages = messages
        self.first = first
        self.snapshot = asking.pin()
        self.events: asyncio.Queue[bytes | Mark] = asyncio.Queue()
        #: Whether the client has gone away: the turn in flight is heard
        #: out and settled, and no other is made.
        self.gone = False
        self.turns = 0
        self.calls = 0
        self.gathered = 0
        self.usage = Usage(cache_hit=0, cache_miss=0, output=0)
        self.cost = 0.0
        self.cost_known = True
        self.outcome = 'running'
        self._released = False
        asking.questions.add(self)

    def stop(self) -> None:
        """The client went away: stop after the turn in flight."""
        self.gone = True

    async def run(self) -> None:
        """Answer the question, and end its events however it goes."""
        started = time.monotonic()
        try:
            await self._answer()
        except Exception:
            logger.exception('question failed')
            self._error('failed', UNEXPECTED)
        finally:
            self.release()
            self.events.put_nowait(Mark.END)
            logger.info(
                'question',
                outcome=self.outcome, turns=self.turns, tools=self.calls,
                client_gone=self.gone,
                prompt_tokens=self.usage.prompt,
                prompt_cache_hit_tokens=self.usage.cache_hit,
                completion_tokens=self.usage.output,
                cost_usd=self.spent(),
                seconds=round(time.monotonic() - started, 3),
            )

    def spent(self) -> float | None:
        """What the question's turns cost, to the billionth of a dollar,
        or None if a turn did not say."""
        return round(self.cost, 9) if self.cost_known else None

    async def _answer(self) -> None:
        asking = self.asking
        hold = self.first
        async with model.client(asking.chat) as llm:
            for number in range(1, MAX_TURNS + 1):
                if self.gone:
                    # Held for a turn that is not to be made.
                    if hold is not None:
                        await run_in_threadpool(hold.refund)
                    self.outcome = 'client gone'
                    return
                if hold is None:
                    try:
                        hold = await asking.hold(self.messages)
                    except OverBudget:
                        self._error('budget', USED_UP)
                        return
                    except LedgerUnavailable:
                        self._error('unavailable', UNAVAILABLE)
                        return
                started = asking.clock()
                try:
                    heard = await model.turn(
                        llm, asking.chat.model, self.messages, self._delta,
                        seconds=asking.pacing.turn_seconds,
                    )
                except model.Refused:
                    # Answered with an error, or never sent: not billed.
                    if hold is not None:
                        await run_in_threadpool(hold.refund)
                    self._error('model', UNREACHABLE)
                    return
                except model.Lost as lost:
                    # It may have been answered, and billed: its worst
                    # case stays held for the day.
                    if lost.timed_out:
                        self._error('timeout', TOO_SLOW)
                    else:
                        self._error('model', UNREACHABLE)
                    return
                self.turns = number
                await self._settle(heard, hold, started, asking.clock())
                hold = None
                if not await self._next(heard):
                    return
        self._error('turns', GAVE_UP)

    async def _settle(
        self,
        heard: model.Turn,
        hold: Hold | None,
        started: datetime,
        ended: datetime,
    ) -> None:
        """A turn's cost, at the rates of the hours it ran in, in place
        of its worst case. A cost that cannot be read keeps the worst
        case, which is known to be enough."""
        usd = math.nan
        if heard.usage is None:
            self.cost_known = False
        else:
            usd = self.asking.chat.prices.cost(heard.usage, started, ended)
            self.usage += heard.usage
            self.cost += usd
        if hold is not None:
            await run_in_threadpool(hold.settle, usd)

    async def _next(self, heard: model.Turn) -> bool:
        """What a turn's end comes to: its tools, run, and another turn
        to go (True), or the answer, or why there is none.

        Only a call for tools runs them, and only a turn that ended is
        an answer (`agent.ts`, #42): a turn cut off at its length can
        hold a call half written.
        """
        if heard.finish == 'tool_calls':
            if not heard.calls or not all(call.id for call in heard.calls):
                self._error('garbled', GARBLED)
                return False
            if self.gone:
                self.outcome = 'client gone'
                return False
            self.messages.append(heard.assistant())
            for index, call in enumerate(heard.calls):
                self.messages.append(await self._call(call, index))
            return True
        if heard.finish == 'stop':
            self._done()
        elif heard.finish == 'length':
            self._error('cut-off', CUT_OFF)
        elif heard.finish == 'content_filter':
            self._error('declined', DECLINED)
        elif heard.finish is None:
            self._error('garbled', GARBLED)
        else:
            self._error(
                'stopped', STOPPED.format(reason=heard.finish),
                reason=heard.finish,
            )
        return False

    async def _call(self, call: model.ToolCall, index: int) -> dict[str, Any]:
        """A tool call, run against the pinned snapshot, and its result
        as the model is to read it: its data, or what went wrong."""
        self._event(
            'tool', {
                'name': call.name[:MAX_TOOL_NAME],
                'arguments': _shown(call.arguments),
            },
        )
        if index >= MAX_CALLS_PER_TURN:
            content = tools.text({'error': TOO_MANY_CALLS})
        elif self.gathered >= MAX_RESULTS_CHARS:
            content = tools.text({'error': GATHERED})
        else:
            content = await run_in_threadpool(
                tools.call, self.snapshot, call.name, call.arguments,
            )
            self.gathered += len(content)
            self.calls += 1
        return {'role': 'tool', 'tool_call_id': call.id, 'content': content}

    def _delta(self, text: str) -> None:
        self._event('text', {'delta': text})

    def _done(self) -> None:
        self.outcome = 'done'
        self._event(
            'done', {
                'turns': self.turns,
                'usage': {
                    'prompt_tokens': self.usage.prompt,
                    'prompt_cache_hit_tokens': self.usage.cache_hit,
                    'prompt_cache_miss_tokens': self.usage.cache_miss,
                    'completion_tokens': self.usage.output,
                    'reasoning_tokens': self.usage.reasoning,
                },
                'cost_usd': self.spent(),
            },
        )

    def _error(self, code: str, message: str, **more: object) -> None:
        self.outcome = code
        self._event(
            'error', {
                'code': code, 'message': message, 'turns': self.turns, **more,
            },
        )

    def _event(self, name: str, data: Mapping[str, object]) -> None:
        self.events.put_nowait(sse(name, data))

    def release(self) -> None:
        """Give its place in flight back, once."""
        if not self._released:
            self._released = True
            self.asking.in_flight.release()
            self.asking.questions.discard(self)


def _shown(arguments: str) -> object:
    """A call's arguments as a `tool` event says them: the object the
    model wrote, or none, if it wrote no object."""
    try:
        parsed = json.loads(arguments) if arguments.strip() else {}
    except (ValueError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


class EventStream(Response):
    """A question's answer, as server-sent events, while the question
    runs beside it.

    Its own ASGI response rather than a StreamingResponse, which cancels
    what it streams when the client goes: here the loop is told, and
    stops after the turn in flight, which it hears out and settles. And
    the response ends only once the loop has, so that a server shutting
    down waits for a turn it is paying for.
    """

    media_type = 'text/event-stream'

    def __init__(self, question: Question, keepalive: float) -> None:
        # No body is set, so no length is declared.
        self.question = question
        self.keepalive = keepalive
        self.status_code = 200
        self.background = None
        self.init_headers({
            'Cache-Control': NO_STORE,
            # Nothing between here and the page is to hold it back.
            'X-Accel-Buffering': 'no',
        })

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        question = self.question
        answering = asyncio.create_task(question.run())
        watching = asyncio.create_task(_watch(receive, question))
        try:
            try:
                await self._stream(send)
            except OSError:
                # A server that says the client went away by failing to
                # send to it.
                pass
            finally:
                question.stop()
                watching.cancel()
            await answering
        except BaseException:
            answering.cancel()
            raise
        finally:
            # Its place in flight, whatever became of it: a question
            # cancelled before it began never reaches its own `finally`.
            question.release()
            await asyncio.gather(watching, return_exceptions=True)

    async def _stream(self, send: Send) -> None:
        await send({
            'type': 'http.response.start',
            'status': self.status_code,
            'headers': self.raw_headers,
        })
        while True:
            try:
                async with asyncio.timeout(self.keepalive):
                    event = await self.question.events.get()
            except TimeoutError:
                await send(_body(b': keep-alive\n\n'))
                continue
            if event is Mark.GONE:
                return
            if event is Mark.END:
                break
            await send(_body(event))
        await send({'type': 'http.response.body', 'body': b'', 'more_body': False})


def _body(data: bytes) -> Message:
    return {'type': 'http.response.body', 'body': data, 'more_body': True}


async def _watch(receive: Receive, question: Question) -> None:
    """Wait for the client to go, and tell the question and its stream
    that it has. The body has been read by now, so all that comes is
    the disconnect."""
    while (await receive())['type'] != 'http.disconnect':
        pass
    question.stop()
    question.events.put_nowait(Mark.GONE)
