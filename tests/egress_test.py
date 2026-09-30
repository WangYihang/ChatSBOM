"""The resolvers' one way out: a forward proxy that lets through
`CONNECT` to port 443 of an allowed host, and nothing else (#168).

A resolver runs project-controlled code, a Gemfile is Ruby, on a network
with no route out (sandbox_test). Its proxy is the one container it can
reach, and the one with a route: so what the proxy lets through is all
a resolution can reach. These run the proxy in this process, on the
loopback, with the registry it tunnels to stood in for by one end of a
socket pair; and once as its container runs it, from its source, in an
interpreter of its own. Nothing here reaches the network.
"""
import ast
import json
import socket
import ssl
import subprocess
import sys
import threading
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core import egress

#: What a Bundler resolution may reach.
ALLOWED = ('rubygems.org', 'index.rubygems.org')

#: How long a test waits on a socket before it fails, not hangs.
WAIT = 10


def client_hello(name: str | None) -> bytes:
    """The first flight of a TLS client asking for `name`, as OpenSSL
    sends it: a ClientHello, its server name (SNI) `name`, or none."""
    context = ssl.create_default_context()
    if name is None:
        context.check_hostname = False
    incoming, outgoing = ssl.MemoryBIO(), ssl.MemoryBIO()
    tls = context.wrap_bio(incoming, outgoing, server_hostname=name)
    with pytest.raises(ssl.SSLWantReadError):
        tls.do_handshake()
    return outgoing.read()


@dataclass
class Registry:
    """The far end of the tunnels: what the proxy connected to, and the
    registry's end of each connection."""

    asked: list[tuple[str, int]] = field(default_factory=list)
    ends: list[socket.socket] = field(default_factory=list)
    kept: list[socket.socket] = field(default_factory=list)

    def upstream(self, host: str, port: int) -> socket.socket:
        ours, theirs = socket.socketpair()
        ours.settimeout(WAIT)
        self.asked.append((host, port))
        self.ends.append(ours)
        self.kept.append(theirs)
        return theirs

    def close(self) -> None:
        for end in self.ends:
            end.close()


@dataclass
class Running:
    """A proxy serving on the loopback, and what it logged."""

    proxy: egress.Proxy
    address: tuple[str, int]
    events: list[dict[str, Any]]
    registry: Registry

    def connect(self) -> socket.socket:
        client = socket.create_connection(self.address, timeout=WAIT)
        return client

    def refusals(self) -> list[str]:
        return [e['reason'] for e in self.events if e['event'] == 'refused']


@pytest.fixture
def serve() -> Iterator[Callable[..., Running]]:
    """Starts a proxy on the loopback; stops each after the test."""
    started: list[tuple[Running, threading.Thread]] = []

    def start(
        allowed: tuple[str, ...] = ALLOWED, **options: Any,
    ) -> Running:
        registry = Registry()
        events: list[dict[str, Any]] = []
        options.setdefault('upstream', registry.upstream)
        options.setdefault('timeout', WAIT)
        proxy = egress.Proxy(allowed, log=events.append, **options)
        listener = socket.create_server(('127.0.0.1', 0))
        thread = threading.Thread(target=proxy.serve, args=(listener,))
        thread.start()
        running = Running(proxy, listener.getsockname(), events, registry)
        started.append((running, thread))
        return running

    yield start
    for running, thread in started:
        running.proxy.close()
        thread.join(WAIT)
        running.registry.close()
        assert not thread.is_alive(), 'the proxy did not stop'


def ask(client: socket.socket, request: bytes) -> bytes:
    """Sends `request`, and reads the answer's head."""
    client.sendall(request)
    answer = b''
    while b'\r\n\r\n' not in answer:
        chunk = client.recv(4096)
        if not chunk:
            break
        answer += chunk
    return answer


def status(answer: bytes) -> int:
    return int(answer.split(b' ', 2)[1])


def connect_request(target: str) -> bytes:
    return (
        f'CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n'
        'User-Agent: Bundler\r\n\r\n'
    ).encode()


def receive(end: socket.socket, count: int) -> bytes:
    """`count` bytes from `end`, or the test fails: never a loop on a
    socket that has closed."""
    received = b''
    while len(received) < count:
        chunk = end.recv(65536)
        assert chunk, f'closed after {len(received)} of {count} bytes'
        received += chunk
    return received


def closed(end: socket.socket) -> bool:
    """Whether the other side of `end` closed without a byte more."""
    try:
        return end.recv(1) == b''
    except ConnectionResetError:
        return True


# --- what it lets through ------------------------------------------------------

def test_connect_to_an_allowed_host_passes(serve):
    """A tunnel to the registry, both ways, once the client's TLS asks
    for the host it named."""
    running = serve()
    hello = client_hello('rubygems.org')
    with running.connect() as client:
        answer = ask(client, connect_request('rubygems.org:443'))
        assert status(answer) == 200, answer

        client.sendall(hello)
        [registry] = running.registry.ends
        assert receive(registry, len(hello)) == hello
        registry.sendall(b'the registry answers')
        assert client.recv(4096) == b'the registry answers'

    assert running.registry.asked == [('rubygems.org', 443)]
    assert running.refusals() == []
    [opened] = [e for e in running.events if e['event'] == 'tunnel']
    assert opened['host'] == 'rubygems.org'


@pytest.mark.parametrize(
    'asked, named', [
        ('RubyGems.ORG', 'rubygems.org'),
        ('rubygems.org', 'RubyGems.ORG'),
    ],
)
def test_a_host_is_matched_whatever_its_case(serve, asked, named):
    """As DNS matches it: to the proxy, and in the name TLS asks for."""
    running = serve()
    hello = client_hello(named)
    with running.connect() as client:
        answer = ask(client, connect_request(f'{asked}:443'))
        assert status(answer) == 200, answer
        client.sendall(hello)
        [registry] = running.registry.ends
        assert receive(registry, len(hello)) == hello
    assert running.registry.asked == [('rubygems.org', 443)]


def test_bytes_sent_with_the_request_reach_the_tunnel(serve):
    """A client may send its ClientHello behind the request, in one
    write, before it has the answer."""
    running = serve()
    hello = client_hello('index.rubygems.org')
    with running.connect() as client:
        answer = ask(client, connect_request('index.rubygems.org:443') + hello)
        assert status(answer) == 200, answer
        [registry] = running.registry.ends
        assert receive(registry, len(hello)) == hello


# --- what it refuses -----------------------------------------------------------

#: Each request refused before anything is connected: why, and with what
#: status.
REFUSED = {
    'another host': (connect_request('example.com:443'), 'host', 403),
    'another port': (connect_request('rubygems.org:80'), 'port', 403),
    'another port, TLS': (
        connect_request('rubygems.org:8443'), 'port', 403,
    ),
    'plain HTTP to an allowed host': (
        b'GET http://rubygems.org/versions HTTP/1.1\r\n'
        b'Host: rubygems.org\r\n\r\n',
        'method', 403,
    ),
    'plain HTTP, origin form': (
        b'GET /versions HTTP/1.1\r\nHost: rubygems.org\r\n\r\n',
        'method', 403,
    ),
    'an IPv4 literal': (
        connect_request('151.101.1.227:443'), 'ip-literal', 403,
    ),
    'an IPv6 literal': (
        connect_request('[2a04:4e42::483]:443'), 'ip-literal', 403,
    ),
    'a number for an address': (
        connect_request('2130706433:443'), 'ip-literal', 403,
    ),
    'a short dotted address': (
        connect_request('127.1:443'), 'ip-literal', 403,
    ),
    'a name that only ends like an allowed one': (
        connect_request('evilrubygems.org:443'), 'host', 403,
    ),
    'a subdomain of an allowed one': (
        connect_request('x.rubygems.org:443'), 'host', 403,
    ),
    'a name that begins like an allowed one': (
        connect_request('rubygems.org.evil.example:443'), 'host', 403,
    ),
    'an allowed name, and a dot': (
        connect_request('rubygems.org.:443'), 'host', 403,
    ),
    'no port': (connect_request('rubygems.org'), 'malformed', 400),
    'a user before the host': (
        connect_request('me@rubygems.org:443'), 'malformed', 400,
    ),
    'not HTTP': (b'\x16\x03\x01\x00\x05hello\r\n\r\n', 'malformed', 400),
}


@pytest.mark.parametrize(
    'request_, reason, code', REFUSED.values(), ids=list(REFUSED),
)
def test_what_is_refused_is_answered_and_logged(
    serve, request_, reason, code,
):
    running = serve()
    with running.connect() as client:
        answer = ask(client, request_)
        assert status(answer) == code, answer
        assert b'Connection: close' in answer
        assert closed(client)

    assert running.registry.asked == [], 'something was connected'
    [refused] = [e for e in running.events if e['event'] == 'refused']
    assert refused['reason'] == reason
    assert refused['client'].startswith('127.0.0.1:')
    assert refused['request'] == request_.split(b'\r\n')[0].decode('latin-1')


@pytest.mark.parametrize('ends', [True, False], ids=['long', 'endless'])
def test_a_head_too_long_is_refused(serve, ends):
    """A client that sends headers forever holds no more than a head's
    worth of the proxy: refused once it has sent that much, whether or
    not it would have stopped."""
    running = serve()
    head = (
        connect_request('rubygems.org:443')[:-2]
        + b'X-Filler: ' + b'x' * (2 * egress.MAX_HEAD) + b'\r\n'
        + (b'\r\n' if ends else b'')
    )
    with running.connect() as client:
        answer = ask(client, head)
        assert status(answer) == 400, answer
    assert running.refusals() == ['malformed']


def test_a_client_that_says_nothing_is_let_go(serve):
    running = serve(timeout=0.5)
    with running.connect() as client:
        assert closed(client)
    assert running.refusals() == ['malformed']


@pytest.mark.parametrize(
    'name', ['evil.example', 'x.rubygems.org', 'index.rubygems.org'],
)
def test_a_tunnel_whose_tls_asks_for_another_host_is_closed(serve, name):
    """Named to the proxy as one host, and to TLS as another: the other
    would be served by whatever else the registry's addresses serve, a
    CDN's other sites among them. Nothing reaches the registry."""
    running = serve()
    with running.connect() as client:
        answer = ask(client, connect_request('rubygems.org:443'))
        assert status(answer) == 200, answer
        client.sendall(client_hello(name))
        assert closed(client)
    [registry] = running.registry.ends
    assert closed(registry), 'the ClientHello was sent on'
    assert running.refusals() == ['sni']


@pytest.mark.parametrize(
    'first', [
        pytest.param(None, id='no server name'),
        pytest.param(
            b'GET / HTTP/1.1\r\nHost: rubygems.org\r\n\r\n', id='http',
        ),
        pytest.param(b'\x16\x03\x01\x00', id='cut short'),
    ],
)
def test_a_tunnel_that_does_not_open_with_tls_naming_its_host_is_closed(
    serve, first,
):
    running = serve(timeout=0.5)
    with running.connect() as client:
        answer = ask(client, connect_request('rubygems.org:443'))
        assert status(answer) == 200, answer
        client.sendall(client_hello(None) if first is None else first)
        assert closed(client)
    [registry] = running.registry.ends
    assert closed(registry)
    assert running.refusals() == ['sni']


def test_the_registry_unreachable_is_a_bad_gateway(serve):
    def unreachable(host: str, port: int) -> socket.socket:
        raise egress.Refused('unreachable', 502, 'connection refused')

    running = serve(upstream=unreachable)
    with running.connect() as client:
        answer = ask(client, connect_request('rubygems.org:443'))
    assert status(answer) == 502
    assert running.refusals() == ['unreachable']


def test_more_clients_than_it_serves_at_once_wait_their_turn(serve):
    """Project code can open connections until every one the proxy
    serves is held: it holds up its own resolution, whose proxy this is,
    and no more of the proxy than that."""
    running = serve(connections=1)
    first = running.connect()
    try:
        assert status(ask(first, connect_request('rubygems.org:443'))) == 200
        with running.connect() as second:
            second.sendall(connect_request('rubygems.org:443'))
            second.settimeout(0.5)
            with pytest.raises(TimeoutError):
                second.recv(1)
            first.close()
            second.settimeout(WAIT)
            assert status(ask(second, b'')) == 200
    finally:
        first.close()
    assert running.registry.asked == [('rubygems.org', 443)] * 2


# --- where it connects ------------------------------------------------------------

def _answers(*addresses: str) -> Callable[..., list[Any]]:
    """`getaddrinfo`, answering `addresses` for any name."""
    def resolve(host: str, port: int, *args: Any, **kwargs: Any) -> list[Any]:
        return [
            (
                socket.AF_INET6 if ':' in address else socket.AF_INET,
                socket.SOCK_STREAM, 6, '',
                (address, port) if ':' not in address
                else (address, port, 0, 0),
            )
            for address in addresses
        ]
    return resolve


@pytest.mark.parametrize(
    'address', [
        '10.1.2.3', '172.17.0.1', '192.168.1.1', '127.0.0.1', '0.0.0.0',
        '169.254.169.254', '100.64.0.1', '224.0.0.1', '::1', 'fe80::1',
        'fd00::1', '::ffff:10.0.0.1',
    ],
)
def test_a_registry_name_that_resolves_inside_is_refused(address):
    """A registry's name answered with an address inside: the daemon's,
    another container's, the cloud's metadata service. Nothing is
    connected."""
    connected: list[Any] = []

    def connect(address: Any, timeout: float) -> socket.socket:
        connected.append(address)
        raise AssertionError('connected')

    with pytest.raises(egress.Refused) as refused:
        egress.open_upstream(
            'rubygems.org', 443, resolve=_answers(address), connect=connect,
        )
    assert refused.value.reason == 'not-public'
    assert refused.value.status == 502
    assert connected == []


def test_only_a_public_address_is_connected_to():
    connected: list[Any] = []
    ours, theirs = socket.socketpair()

    def connect(address: Any, timeout: float) -> socket.socket:
        connected.append(address)
        return theirs

    try:
        found = egress.open_upstream(
            'rubygems.org', 443,
            resolve=_answers('10.0.0.1', '151.101.1.227'), connect=connect,
        )
        assert found is theirs
    finally:
        ours.close()
        theirs.close()
    assert connected == [('151.101.1.227', 443)]


def test_the_next_public_address_is_tried_when_one_fails():
    connected: list[Any] = []
    ours, theirs = socket.socketpair()

    def connect(address: Any, timeout: float) -> socket.socket:
        connected.append(address[0])
        if address[0] == '151.101.1.227':
            raise ConnectionRefusedError('refused')
        return theirs

    try:
        egress.open_upstream(
            'rubygems.org', 443,
            resolve=_answers('151.101.1.227', '151.101.65.227'),
            connect=connect,
        )
    finally:
        ours.close()
        theirs.close()
    assert connected == ['151.101.1.227', '151.101.65.227']


def test_a_name_that_does_not_resolve_is_unreachable():
    def resolve(*args: Any, **kwargs: Any) -> list[Any]:
        raise socket.gaierror('Name or service not known')

    with pytest.raises(egress.Refused) as refused:
        egress.open_upstream('rubygems.org', 443, resolve=resolve)
    assert refused.value.reason == 'unreachable'


# --- reading the server name --------------------------------------------------------

@pytest.mark.parametrize('name', ['rubygems.org', 'repo.packagist.org'])
def test_the_server_name_is_read_from_a_client_hello(name):
    assert egress.server_name(client_hello(name)) == name


def test_a_client_hello_without_a_server_name_names_none():
    assert egress.server_name(client_hello(None)) is None


def test_a_client_hello_split_over_records_is_read_whole():
    """TLS may carry one handshake message in several records."""
    hello = client_hello('rubygems.org')
    kind, version, payload = hello[:1], hello[1:3], hello[5:]
    half = len(payload) // 2
    split = b''.join(
        kind + version + len(part).to_bytes(2, 'big') + part
        for part in (payload[:half], payload[half:])
    )
    assert egress.server_name(split) == 'rubygems.org'


@pytest.mark.parametrize(
    'data', [
        b'', b'\x16\x03\x01', b'GET / HTTP/1.1\r\n\r\n',
        b'\x16\x03\x01\x00\x04\x02\x00\x00\x00',
        b'\x16\x03\x01\xff\xff' + b'\x01' * 100,
    ],
    ids=[
        'nothing', 'a header cut short', 'http', 'a server hello',
        'cut short',
    ],
)
def test_what_is_no_whole_client_hello_names_none(data):
    assert egress.server_name(data) is None


# --- as its container runs it ---------------------------------------------------------

def test_it_needs_nothing_but_the_standard_library():
    """Its container runs its source on a Python image and nothing else:
    no chatsbom, no package of any kind."""
    tree = ast.parse(Path(egress.__file__).read_text(encoding='utf-8'))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split('.')[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, 'a relative import'
            imported.add((node.module or '').split('.')[0])
    assert imported, 'nothing found: the walk is wrong'
    assert imported <= set(sys.stdlib_module_names) | {'__future__'}, (
        sorted(imported - set(sys.stdlib_module_names))
    )


def test_its_source_runs_as_its_container_runs_it():
    """`python -I -B -c <source> --listen ... --allow ...`: says where it
    listens once it does, and refuses what it should, each in a line of
    JSON."""
    command = [
        sys.executable, '-I', '-B', '-c', egress.source(),
        '--listen', '127.0.0.1:0', '--allow', 'rubygems.org',
    ]
    with subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) as process:
        try:
            assert process.stdout is not None
            listening = json.loads(process.stdout.readline())
            assert listening['event'] == 'listening'
            assert listening['allow'] == ['rubygems.org']
            host, _, port = listening['address'].rpartition(':')

            with socket.create_connection(
                (host, int(port)), timeout=WAIT,
            ) as client:
                answer = ask(client, connect_request('evil.example:443'))
            assert status(answer) == 403

            refused = json.loads(process.stdout.readline())
            assert refused['event'] == 'refused'
            assert refused['reason'] == 'host'
        finally:
            process.terminate()
            process.wait(WAIT)
            assert process.stderr is not None
            process.stderr.read()


def test_it_refuses_to_start_with_nothing_allowed():
    completed = subprocess.run(
        [sys.executable, '-I', '-B', '-c', egress.source()],
        capture_output=True, text=True, timeout=WAIT * 3,
    )
    assert completed.returncode != 0
    assert '--allow' in completed.stderr
