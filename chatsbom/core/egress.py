"""The resolvers' one way out (#168): a forward proxy that lets through
`CONNECT` to port 443 of an allowed host, and nothing else.

A resolution runs project-controlled code, a Gemfile being Ruby, on a
network of its own with no route out (`core/sandbox.py`). The one other
container on that network is its proxy, this, which is on a network
with a route out as well: so what it lets through is all the resolution
can reach. A tunnel, and only one:

- asked for with `CONNECT host:443`, where `host` is one of the hosts it
  was started with (`--allow`), as it is spelled there, whatever its
  case;
- to a public address of that host, as DNS answers for it: never one
  inside, the nested daemon's, another container's, or a cloud's
  metadata service;
- whose first bytes are a TLS ClientHello asking for that same host
  (SNI). A CDN serves many sites at one address, by the name TLS asks
  for; named to the proxy as the registry and to TLS as another site,
  the tunnel would reach that other site.

Everything else is refused, and each refusal is a line of JSON on
stdout, which `sbom lock` logs with the directory it was resolving:

- `method`: any method but CONNECT, plain HTTP among them (403);
- `port`: a port other than 443 (403);
- `ip-literal`: an address in place of a name, dotted, bracketed, or a
  bare number (403);
- `host`: any other name, one that only ends or begins like an allowed
  one among them (403);
- `malformed`: a request it cannot read, or one that did not come in
  time (400, or nothing where nothing came);
- `not-public` and `unreachable`: a host with no public address, or
  none that answers (502);
- `sni`: a tunnel that does not open with a ClientHello naming its host.
  It is closed: nothing can be answered by then.

Its container runs this module's source on the image's Python, `python
-I -B -c <source> --allow <host> ...` (`source`; `sandbox.proxy_command`):
no file of ours is mounted or copied in, since the nested daemon sees
its own filesystem and not the one `sbom lock` runs in. So this module
takes nothing but the standard library, and the tests run the same
source.
"""
import argparse
import ipaddress
import json
import re
import selectors
import socket
import sys
import threading
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from pathlib import Path
from typing import Any

#: The one port a tunnel may go to: HTTPS's.
PORT = 443

#: Where it listens in its container, as the resolver's environment
#: names it (`sandbox.PROXY_URL`).
LISTEN = '0.0.0.0:3128'

#: The most bytes a request's head may take, its request line and its
#: headers.
MAX_HEAD = 8 * 1024

#: The most bytes a ClientHello may take, in however many records. TLS
#: allows more, and no client sends more than a few thousand.
MAX_HELLO = 64 * 1024

#: The largest TLS record: 2**14 bytes of plaintext.
MAX_RECORD = 2 ** 14

#: Seconds a client has to send its request, and its ClientHello; a
#: registry has to answer a connection; and a tunnel may be idle.
TIMEOUT = 60.0

#: Clients served at once. More wait for one to be done, in the
#: listening socket's queue. A resolution has a proxy of its own, so a
#: project that holds them all holds its own resolution up and nobody
#: else's.
CONNECTIONS = 64

#: How often a thread waiting for a client or a slot looks at whether
#: the proxy was closed.
POLL = 0.2

_CHUNK = 64 * 1024

#: `host:port`, as CONNECT names where to: a name or a bracketed
#: address, and digits. No user, no path.
_AUTHORITY = re.compile(
    r'(?P<host>\[[^\]]*\]|[^\s:@/\[\]]+):(?P<port>\d{1,5})',
)

#: What each status is called, in an answer.
_STATUS = {
    200: 'Connection established',
    400: 'Bad Request',
    403: 'Forbidden',
    502: 'Bad Gateway',
}

#: TLS's record type and handshake type for a ClientHello, and the
#: extension that names the server.
_HANDSHAKE = 22
_CLIENT_HELLO = 1
_SERVER_NAME = 0
_HOST_NAME = 0

#: What a line of the log says of a request at most.
_SAID = 256


class Refused(Exception):
    """A request not let through: why, in a word (`reason`), and the
    status it is answered with; 0 where nothing can be answered."""

    def __init__(self, reason: str, status: int, detail: str = '') -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.status = status
        self.detail = detail or reason


def _print(event: dict[str, Any]) -> None:
    print(json.dumps(event), flush=True)


def _is_address(host: str) -> bool:
    """Whether `host` is an address rather than a name, in any of the
    forms a resolver takes one in: `151.101.1.227`, `127.1`,
    `2130706433` or `0x7f.1` among them."""
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return True
    try:
        socket.inet_aton(host)
    except OSError:
        return False
    return True


def destination(request: str, allowed: Iterable[str]) -> str:
    """The host a request's line may tunnel to, or `Refused`, saying
    why."""
    words = request.split(' ')
    if len(words) != 3 or not words[2].startswith('HTTP/1.'):
        raise Refused('malformed', 400, 'not an HTTP/1 request')
    method, target, _ = words
    if method != 'CONNECT':
        raise Refused(
            'method', 403, f'{method} is not CONNECT: only HTTPS goes out',
        )
    match = _AUTHORITY.fullmatch(target)
    if match is None:
        raise Refused('malformed', 400, f'{target} is not host:port')
    host, port = match['host'], int(match['port'])
    if host.startswith('[') or _is_address(host):
        raise Refused('ip-literal', 403, f'{host} is an address, not a name')
    if port != PORT:
        raise Refused('port', 403, f'port {port} is not {PORT}')
    name = host.lower()
    if name not in {each.lower() for each in allowed}:
        raise Refused(
            'host', 403, f'{host} is not a registry this resolution may reach',
        )
    return name


# -- where it connects ----------------------------------------------------------


def _is_public(address: str) -> bool:
    """Whether an address is on the internet at large: not private,
    shared, loopback, link-local, multicast, reserved or unspecified. An
    IPv4 address mapped into IPv6 is none of these, whatever it maps."""
    try:
        ip = ipaddress.ip_address(address.split('%', 1)[0])
    except ValueError:
        return False
    return ip.is_global and not ip.is_multicast


def open_upstream(
    host: str,
    port: int,
    *,
    resolve: Callable[..., Sequence[Any]] = socket.getaddrinfo,
    connect: Callable[..., socket.socket] = socket.create_connection,
    timeout: float = TIMEOUT,
) -> socket.socket:
    """A connection to `host` at `port`, on the first of its public
    addresses that answers. `Refused` when it has none, or none
    answers."""
    try:
        answers = resolve(host, port, type=socket.SOCK_STREAM)
    except OSError as error:
        raise Refused('unreachable', 502, f'{host}: {error}') from None
    addresses = list(
        dict.fromkeys(
            answer[4][0] for answer in answers if _is_public(answer[4][0])
        ),
    )
    if not addresses:
        raise Refused(
            'not-public', 502, f'{host} has no public address',
        )
    failure: OSError | None = None
    for address in addresses:
        try:
            return connect((address, port), timeout)
        except OSError as error:
            failure = error
    raise Refused('unreachable', 502, f'{host}: {failure}')


# -- what TLS says first ------------------------------------------------------------


class _Reader:
    """Bytes read in order, each read refused past the end."""

    def __init__(self, data: bytes) -> None:
        self.data = data
        self.at = 0

    @property
    def left(self) -> int:
        return len(self.data) - self.at

    def take(self, count: int) -> bytes:
        if count > self.left:
            raise ValueError('cut short')
        taken = self.data[self.at:self.at + count]
        self.at += count
        return taken

    def number(self, width: int) -> int:
        return int.from_bytes(self.take(width), 'big')


def _hello(data: bytes) -> bytes | None:
    """The ClientHello the TLS records at the start of `data` carry,
    whole: its handshake type, length and body. None while it is not
    all there. `ValueError` for what is no ClientHello."""
    payload = b''
    reader = _Reader(data)
    while reader.left >= 5:
        kind, _, length = reader.number(1), reader.take(2), reader.number(2)
        if kind != _HANDSHAKE or length > MAX_RECORD:
            raise ValueError('not a TLS handshake')
        if reader.left < length:
            return None
        payload += reader.take(length)
        if len(payload) >= 4:
            if payload[0] != _CLIENT_HELLO:
                raise ValueError('not a ClientHello')
            size = int.from_bytes(payload[1:4], 'big')
            if size > MAX_HELLO:
                raise ValueError('a ClientHello too large')
            if len(payload) >= 4 + size:
                return payload[:4 + size]
    return None


def server_name(data: bytes) -> str | None:
    """The host name a TLS ClientHello asks for (SNI), lower-cased; None
    if `data` does not start with a whole ClientHello, or it names
    none."""
    try:
        hello = _hello(data)
        if hello is None:
            return None
        body = _Reader(hello[4:])
        body.take(2 + 32)  # its version, and its random
        body.take(body.number(1))  # the session's id
        body.take(body.number(2))  # the cipher suites
        body.take(body.number(1))  # the compression methods
        if not body.left:
            return None
        extensions = _Reader(body.take(body.number(2)))
        while extensions.left:
            kind = extensions.number(2)
            extension = _Reader(extensions.take(extensions.number(2)))
            if kind != _SERVER_NAME:
                continue
            names = _Reader(extension.take(extension.number(2)))
            while names.left:
                name_type = names.number(1)
                name = names.take(names.number(2))
                if name_type == _HOST_NAME:
                    return name.decode('ascii').lower()
        return None
    except (ValueError, UnicodeDecodeError):
        return None


# -- the proxy ---------------------------------------------------------------------


def _answer(status: int, text: str = '') -> bytes:
    if status == 200:
        return b'HTTP/1.1 200 Connection established\r\n\r\n'
    body = f'{text}\n'.encode()
    return (
        f'HTTP/1.1 {status} {_STATUS[status]}\r\n'
        'Content-Type: text/plain; charset=utf-8\r\n'
        f'Content-Length: {len(body)}\r\n'
        'Connection: close\r\n\r\n'
    ).encode() + body


def _read_head(client: socket.socket) -> tuple[bytes, bytes]:
    """A request's head, and what came after it."""
    data = b''
    while b'\r\n\r\n' not in data:
        if len(data) > MAX_HEAD:
            raise Refused('malformed', 400, 'a request head too long')
        try:
            chunk = client.recv(_CHUNK)
        except TimeoutError:
            raise Refused('malformed', 0, 'no request in time') from None
        if not chunk:
            raise Refused('malformed', 0, 'closed before a request')
        data += chunk
    head, _, rest = data.partition(b'\r\n\r\n')
    if len(head) > MAX_HEAD:
        raise Refused('malformed', 400, 'a request head too long')
    return head, rest


def _read_hello(client: socket.socket, data: bytes) -> bytes:
    """What the client sent first in its tunnel, up to the end of its
    ClientHello."""
    while True:
        try:
            if _hello(data) is not None:
                return data
        except ValueError as error:
            raise Refused('sni', 0, str(error)) from None
        if len(data) > MAX_HELLO:
            raise Refused('sni', 0, 'no ClientHello in time')
        try:
            chunk = client.recv(_CHUNK)
        except TimeoutError:
            raise Refused('sni', 0, 'no ClientHello in time') from None
        if not chunk:
            raise Refused('sni', 0, 'closed before a ClientHello')
        data += chunk


def _splice(one: socket.socket, other: socket.socket, idle: float) -> None:
    """Bytes from each to the other, until both are done, or neither
    has sent any for `idle` seconds."""
    with selectors.DefaultSelector() as selector:
        selector.register(one, selectors.EVENT_READ, (one, other))
        selector.register(other, selectors.EVENT_READ, (other, one))
        while selector.get_map():
            ready = selector.select(idle)
            if not ready:
                return
            for key, _ in ready:
                source, sink = key.data
                data = source.recv(_CHUNK)
                if data:
                    sink.sendall(data)
                    continue
                selector.unregister(source)
                try:
                    sink.shutdown(socket.SHUT_WR)
                except OSError:
                    pass


def _refuse(client: socket.socket, answer: bytes) -> None:
    """Answers a refused client, and reads what it goes on sending until
    it is done, for a while: closed with its request unread, the socket
    would be reset, and the client could lose the answer with it."""
    client.sendall(answer)
    client.shutdown(socket.SHUT_WR)
    for _ in range(MAX_HELLO // _CHUNK * 16):
        if not client.recv(_CHUNK):
            return


def _peer(address: Any) -> str:
    if isinstance(address, tuple) and len(address) >= 2:
        return f'{address[0]}:{address[1]}'
    return str(address or '')


class Proxy:
    """The proxy: serves clients on a listening socket until `close`."""

    def __init__(
        self,
        allowed: Iterable[str],
        *,
        upstream: Callable[[str, int], socket.socket] | None = None,
        log: Callable[[dict[str, Any]], None] = _print,
        timeout: float = TIMEOUT,
        connections: int = CONNECTIONS,
    ) -> None:
        self.allowed = frozenset(host.lower() for host in allowed)
        if not self.allowed:
            raise ValueError('no host is allowed: name one with --allow')
        self.timeout = timeout
        self._upstream = upstream or (
            lambda host, port: open_upstream(host, port, timeout=timeout)
        )
        self._log = log
        self._said = threading.Lock()
        self._slots = threading.BoundedSemaphore(connections)
        self._closed = threading.Event()
        self._open: set[socket.socket] = set()
        self._threads: set[threading.Thread] = set()
        self._held = threading.Lock()

    def log(self, event: dict[str, Any]) -> None:
        with self._said:
            self._log(event)

    def serve(self, listener: socket.socket) -> None:
        """Serves clients on `listener`, each in a thread, `connections`
        at once, until `close`; then closes it."""
        listener.settimeout(POLL)
        try:
            while not self._closed.is_set():
                if not self._slots.acquire(timeout=POLL):
                    continue
                try:
                    client, address = listener.accept()
                except TimeoutError:
                    self._slots.release()
                    continue
                except OSError:
                    # Out of descriptors, say: wait, rather than spin.
                    self._slots.release()
                    self._closed.wait(POLL)
                    continue
                thread = threading.Thread(
                    target=self._serve, args=(client, _peer(address)),
                    daemon=True,
                )
                with self._held:
                    self._threads.add(thread)
                try:
                    thread.start()
                except RuntimeError:
                    # No thread to be had: the client waits no longer.
                    with self._held:
                        self._threads.discard(thread)
                    client.close()
                    self._slots.release()
        finally:
            listener.close()
            with self._held:
                threads = list(self._threads)
            for thread in threads:
                thread.join(self.timeout)

    def close(self) -> None:
        """Stops serving, and ends every connection it holds."""
        self._closed.set()
        with self._held:
            held = list(self._open)
        for connection in held:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _hold(self, connection: socket.socket) -> None:
        with self._held:
            self._open.add(connection)
        if self._closed.is_set():
            connection.shutdown(socket.SHUT_RDWR)

    def _serve(self, client: socket.socket, peer: str) -> None:
        upstream: socket.socket | None = None
        try:
            self._hold(client)
            client.settimeout(self.timeout)
            upstream = self._tunnel(client, peer)
            if upstream is not None:
                _splice(client, upstream, self.timeout)
        except OSError:
            pass
        finally:
            with self._held:
                self._open.discard(client)
                if upstream is not None:
                    self._open.discard(upstream)
                self._threads.discard(threading.current_thread())
            if upstream is not None:
                upstream.close()
            client.close()
            self._slots.release()

    def _tunnel(
        self, client: socket.socket, peer: str,
    ) -> socket.socket | None:
        """The registry's end of the tunnel `client` asked for, once its
        ClientHello has gone there; None, with the refusal logged and
        answered, when it may not have one."""
        request = ''
        upstream: socket.socket | None = None
        try:
            head, rest = _read_head(client)
            request = head.split(b'\r\n', 1)[0].decode('latin-1')
            host = destination(request, self.allowed)
            upstream = self._upstream(host, PORT)
            upstream.settimeout(self.timeout)
            self._hold(upstream)
            client.sendall(_answer(200))
            hello = _read_hello(client, rest)
            named = server_name(hello)
            if named != host:
                raise Refused(
                    'sni', 0, f'TLS asks for {named or "no name"}, not {host}',
                )
            self.log({
                'event': 'tunnel', 'client': peer, 'host': host,
                'address': _peer(_address(upstream)),
            })
            upstream.sendall(hello)
            tunnel, upstream = upstream, None
            return tunnel
        except Refused as refused:
            self.log({
                'event': 'refused', 'client': peer, 'reason': refused.reason,
                'request': request[:_SAID], 'detail': refused.detail[:_SAID],
            })
            if refused.status:
                answer = _answer(
                    refused.status, f'egress refused: {refused.detail}',
                )
                _refuse(client, answer)
            return None
        finally:
            if upstream is not None:
                with self._held:
                    self._open.discard(upstream)
                upstream.close()


def _address(connection: socket.socket) -> Any:
    try:
        return connection.getpeername()
    except OSError:
        return ''


def source() -> str:
    """This module's source, as its container runs it (`python -I -B
    -c`)."""
    return Path(__file__).read_text(encoding='utf-8')


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog='egress',
        description=(
            'A forward proxy that lets through CONNECT to port 443 of '
            'the hosts named, and nothing else.'
        ),
    )
    parser.add_argument(
        '--listen', default=LISTEN, metavar='ADDRESS:PORT',
        help=f'where to listen (default {LISTEN})',
    )
    parser.add_argument(
        '--allow', action='append', required=True, metavar='HOST',
        help='a host a tunnel may go to; once for each',
    )
    arguments = parser.parse_args(argv)
    address, _, port = arguments.listen.rpartition(':')
    proxy = Proxy(arguments.allow)
    listener = socket.create_server((address, int(port)), backlog=CONNECTIONS)
    bound = listener.getsockname()
    _print({
        'event': 'listening', 'address': f'{bound[0]}:{bound[1]}',
        'allow': sorted(proxy.allowed),
    })
    proxy.serve(listener)
    return 0


if __name__ == '__main__':
    sys.exit(main())
