"""A stand-in for DeepSeek's Chat Completions API, on 127.0.0.1 (#140).

No test calls DeepSeek. The chat's tests call this instead: a real HTTP
server on a socket of its own, which answers each `POST
/chat/completions` with the next reply of the script it was given, and
records what it was sent. Closed, socket and all, when its block ends.

A reply streams as DeepSeek documents its streams (the Chat Completions
reference, read on 2026-09-29): a first chunk naming the role, the
reasoning and the answer as deltas, each tool call in pieces whose first
carries its id and name and whose others carry only arguments, and the
usage on the last chunk, beside the finish reason, before `data:
[DONE]`. What can go wrong goes wrong here too: an error answered in
place of a stream, an error inside one, a connection cut part-way, a
stream that stops to wait, and DeepSeek's `: keep-alive` comments.
`TestTheStandIn` holds it to that.
"""
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from typing import Any

from openai.types.chat import ChatCompletionChunk

#: What a turn used, unless a reply says otherwise: 1,000 tokens in, of
#: which 800 came from the cache, and 100 out, 40 of them reasoning.
USAGE: Mapping[str, Any] = {
    'prompt_tokens': 1_000,
    'completion_tokens': 100,
    'total_tokens': 1_100,
    'prompt_tokens_details': {'cached_tokens': 800},
    'prompt_cache_hit_tokens': 800,
    'prompt_cache_miss_tokens': 200,
    'completion_tokens_details': {'reasoning_tokens': 40},
}


def usage(
    hit: int = 800, miss: int = 200, output: int = 100, reasoning: int = 40,
) -> dict[str, Any]:
    """A turn's usage, as DeepSeek reports it."""
    return {
        'prompt_tokens': hit + miss,
        'completion_tokens': output,
        'total_tokens': hit + miss + output,
        'prompt_tokens_details': {'cached_tokens': hit},
        'prompt_cache_hit_tokens': hit,
        'prompt_cache_miss_tokens': miss,
        'completion_tokens_details': {'reasoning_tokens': reasoning},
    }


@dataclass(frozen=True)
class Call:
    """A tool call as the model writes it: its arguments are its text."""

    id: str
    name: str
    arguments: str = '{}'


@dataclass
class Reply:
    """What the stand-in answers one request with."""

    content: str = ''
    reasoning: str = ''
    calls: Sequence[Call] = ()
    #: None: the stream ends with no finish reason, and no usage.
    finish: str | None = 'stop'
    usage: Mapping[str, Any] | None = field(
        default_factory=lambda: dict(USAGE),
    )
    #: An error status answered in place of the stream, with `error`.
    status: int = 200
    error: Mapping[str, Any] = field(
        default_factory=lambda: {
            'error': {
                'message': 'Invalid request: the model is not amused',
                'type': 'invalid_request_error',
            },
        },
    )
    #: Chunks sent before an `error` event arrives in the stream.
    error_after: int | None = None
    #: Chunks sent before the connection is cut, mid-body.
    drop_after: int | None = None
    #: Waited on before the chunk at `wait_at` is sent.
    wait: threading.Event | None = None
    wait_at: int = 1
    #: Seconds between chunks.
    delay: float = 0.0
    #: The usage on a chunk of its own, with no choices, as OpenAI sends
    #: it, rather than on the last one, as DeepSeek does.
    usage_apart: bool = False
    #: A keep-alive comment before each chunk, as DeepSeek sends while a
    #: request waits.
    keepalive: bool = False

    def chunks(self) -> Iterator[dict[str, Any]]:
        """The stream's chunks, in order, before `data: [DONE]`."""
        def chunk(delta: Mapping[str, Any], **more: Any) -> dict[str, Any]:
            return {
                'id': 'chatcmpl-fake',
                'object': 'chat.completion.chunk',
                'created': 1_790_000_000,
                'model': 'deepseek-flash',
                'system_fingerprint': 'fp_fake',
                'choices': [{
                    'index': 0, 'delta': dict(delta), 'logprobs': None,
                    'finish_reason': more.pop('finish_reason', None),
                }],
                **more,
            }

        yield chunk({'role': 'assistant', 'content': ''})
        for piece in pieces(self.reasoning):
            yield chunk({'reasoning_content': piece})
        for piece in pieces(self.content):
            yield chunk({'content': piece})
        for index, call in enumerate(self.calls):
            yield chunk({
                'tool_calls': [{
                    'index': index, 'id': call.id, 'type': 'function',
                    'function': {'name': call.name, 'arguments': ''},
                }],
            })
            half = len(call.arguments) // 2
            for part in (call.arguments[:half], call.arguments[half:]):
                if part:
                    yield chunk({
                        'tool_calls': [{
                            'index': index, 'function': {'arguments': part},
                        }],
                    })
        if self.finish is None:
            return
        if self.usage_apart:
            yield chunk({'content': ''}, finish_reason=self.finish)
            if self.usage is not None:
                yield {
                    'id': 'chatcmpl-fake', 'object': 'chat.completion.chunk',
                    'created': 1_790_000_000, 'model': 'deepseek-flash',
                    'choices': [], 'usage': dict(self.usage),
                }
        else:
            more = {} if self.usage is None else {'usage': dict(self.usage)}
            yield chunk({'content': ''}, finish_reason=self.finish, **more)


def pieces(text: str) -> list[str]:
    """`text` as a model streams it: a few characters at a time."""
    return [text[at:at + 7] for at in range(0, len(text), 7)]


@dataclass
class Request:
    """What the stand-in was sent."""

    path: str
    headers: dict[str, str]
    body: dict[str, Any]
    #: The body as the bytes it came in.
    raw: bytes


class FakeDeepSeek:
    """The stand-in, serving `replies` in order, one a request, on a
    free port of 127.0.0.1 until the block ends.

    A request past the end of the script is answered 500 and recorded
    in `unexpected`, which a test can hold to be empty.
    """

    def __init__(self, *replies: Reply) -> None:
        self.replies = list(replies)
        #: Every reply's wait, sent or not: all are let go at the end.
        self._waits = [reply.wait for reply in replies if reply.wait]
        self.requests: list[Request] = []
        self.unexpected: list[Request] = []
        self._lock = threading.Lock()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, format: str, *args: Any) -> None:
                pass

            def do_POST(self) -> None:
                length = int(self.headers.get('content-length', '0'))
                raw = self.rfile.read(length)
                request = Request(
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    body=json.loads(raw),
                    raw=raw,
                )
                with fake._lock:
                    fake.requests.append(request)
                    reply = fake.replies.pop(0) if fake.replies else None
                    if reply is None:
                        fake.unexpected.append(request)
                self.close_connection = True
                if reply is None:
                    self._whole(500, {'error': {'message': 'no more replies'}})
                elif reply.status != 200:
                    self._whole(reply.status, reply.error)
                else:
                    try:
                        self._stream(reply)
                    except (BrokenPipeError, ConnectionResetError):
                        # The client went away, as a timed-out one does.
                        pass

            def _whole(self, status: int, body: Mapping[str, Any]) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Connection', 'close')
                self.end_headers()
                self.wfile.write(data)

            def _stream(self, reply: Reply) -> None:
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Transfer-Encoding', 'chunked')
                self.send_header('Connection', 'close')
                self.end_headers()
                for index, chunk in enumerate(reply.chunks()):
                    if reply.drop_after == index:
                        # Mid-body: the chunked encoding never ends, so
                        # the client knows the message was cut short.
                        self.connection.shutdown(socket.SHUT_RDWR)
                        return
                    if reply.error_after == index:
                        self._send(
                            b'data: ' + json.dumps(reply.error).encode(),
                        )
                        break
                    if reply.wait is not None and reply.wait_at == index:
                        reply.wait.wait(timeout=30)
                    if reply.delay:
                        time.sleep(reply.delay)
                    if reply.keepalive:
                        self._write(b': keep-alive\n\n')
                    self._send(b'data: ' + json.dumps(chunk).encode())
                else:
                    self._send(b'data: [DONE]')
                self.wfile.write(b'0\r\n\r\n')
                self.wfile.flush()

            def _send(self, event: bytes) -> None:
                self._write(event + b'\n\n')

            def _write(self, data: bytes) -> None:
                self.wfile.write(b'%X\r\n%s\r\n' % (len(data), data))
                self.wfile.flush()

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        # Polled this often for the close, where the default half a
        # second would be spent by every test that uses it.
        self._thread = threading.Thread(
            target=self.server.serve_forever, kwargs={'poll_interval': 0.01},
            name='fake-deepseek',
        )

    @property
    def url(self) -> str:
        host, port = self.server.server_address[:2]
        return f'http://{host!s}:{port}'

    def __enter__(self) -> 'FakeDeepSeek':
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        # Nothing may be left waiting: a reply held for a test that
        # failed before letting it go would hold its handler, and the
        # close waits for every handler.
        self.release()
        self.server.shutdown()
        self.server.server_close()
        self._thread.join(timeout=30)

    def release(self) -> None:
        """Let go of every reply held waiting."""
        for wait in self._waits:
            wait.set()


def posted(fake: FakeDeepSeek) -> tuple[int, str]:
    """A request to the stand-in, as the SDK makes one, and what came
    back: the status and the whole body."""
    request = urllib.request.Request(
        f'{fake.url}/chat/completions',
        data=json.dumps({'model': 'deepseek-flash', 'messages': []}).encode(),
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        with error:
            return error.code, error.read().decode()


class TestTheStandIn:
    """Every test of the chat believes it, so it is held to the stream
    DeepSeek documents (the Chat Completions reference, read on
    2026-09-29)."""

    def test_streams_as_deepseeks_reference_shows(self):
        reply = Reply(
            content='Hello!', reasoning='Say hello.',
            calls=[Call('call_00', 'ecosystems_for', '{"name": "mail"}')],
            finish='tool_calls',
        )
        with FakeDeepSeek(reply) as fake:
            status, body = posted(fake)

        assert status == 200
        data = [
            line.removeprefix('data: ') for line in body.split('\n')
            if line.startswith('data: ')
        ]
        assert data[-1] == '[DONE]'
        chunks = [json.loads(line) for line in data[:-1]]
        # Each is a chunk as the SDK reads one.
        for chunk in chunks:
            ChatCompletionChunk.model_validate(chunk)
        deltas = [chunk['choices'][0]['delta'] for chunk in chunks]
        assert deltas[0] == {'role': 'assistant', 'content': ''}
        # The usage on the last chunk, beside the finish reason, and on
        # no other: DeepSeek sends no chunk of its own for it.
        assert chunks[-1]['choices'][0]['finish_reason'] == 'tool_calls'
        assert chunks[-1]['usage'] == USAGE
        assert all('usage' not in chunk for chunk in chunks[:-1])
        # A call's first piece carries its id and name; the rest carry
        # its arguments alone.
        pieces = [
            delta['tool_calls'][0] for delta in deltas if 'tool_calls' in delta
        ]
        assert pieces[0]['id'] == 'call_00'
        assert pieces[0]['function']['name'] == 'ecosystems_for'
        for piece in pieces[1:]:
            assert set(piece) == {'index', 'function'}
            assert set(piece['function']) == {'arguments'}
        assert ''.join(
            piece['function']['arguments'] for piece in pieces
        ) == '{"name": "mail"}'
        assert ''.join(d.get('reasoning_content', '') for d in deltas) == (
            'Say hello.'
        )
        assert ''.join(d.get('content') or '' for d in deltas) == 'Hello!'

    def test_answers_past_its_script_with_a_500_and_says_so(self):
        with FakeDeepSeek() as fake:
            status, _ = posted(fake)
        assert status == 500
        assert len(fake.unexpected) == 1

    def test_answers_an_error_status_as_json(self):
        with FakeDeepSeek(Reply(status=429)) as fake:
            status, body = posted(fake)
        assert status == 429
        assert json.loads(body)['error']['type'] == 'invalid_request_error'
