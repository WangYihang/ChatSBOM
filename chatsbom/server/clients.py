"""Who a request is from: the key the rate limits and the challenges use.

The Worker keyed its limiters on `CF-Connecting-IP` when the edge had
vouched for the request by adding a shared secret, and on one bucket
for every request it had not (web/src/ratelimit.ts). Here the edge is a
network instead (#128, section 2.7): cloudflared is alone on the
compose network `edge` (#130), no port is published, and Cloudflare
overwrites the header before cloudflared passes it on. So the header is
believed from a peer inside that subnet and from no other. Any other
peer, the host or the container's own healthcheck among them, is keyed
by its own address, whatever it claims: the header came from it.

Nothing configured as the edge believes no header at all. That is right
for a run on the host, where anyone who reaches the port can write it.

The key is an IPv4 address, or an IPv6 /64. The Worker keyed the full
address, and a client holds a /64 at the least, so it could rotate its
way to a fresh budget without leaving its own network.
"""
from collections.abc import Iterable
from collections.abc import Mapping
from ipaddress import ip_address
from ipaddress import IPv4Address
from ipaddress import IPv4Network
from ipaddress import IPv6Address
from ipaddress import IPv6Network

#: A subnet the edge may be in.
Network = IPv4Network | IPv6Network

#: The header Cloudflare's edge sets to the visitor's address, which it
#: overwrites whatever the visitor sent. Lowercase, as the ASGI server
#: hands headers over.
CONNECTING_IP = 'cf-connecting-ip'

#: The prefix an IPv6 client is keyed by: the least a network hands one
#: subscriber, so an address a client can choose for itself.
IPV6_PREFIX = 64

#: The key of a request whose peer has no address at all: a Unix
#: socket's.
UNKNOWN = 'unknown'

#: The longest key taken as it is, as the Worker's MAX_CLIENT. An
#: address or a /64 is at most 49 characters; a peer that is no address
#: names itself, and nothing bounds what it says.
MAX_KEY = 128


def address(text: str | None) -> IPv4Address | IPv6Address | None:
    """`text` as an address, IPv4-mapped IPv6 as the IPv4 address it
    maps; None if it is not one.

    A socket bound to `::` reports an IPv4 peer as `::ffff:a.b.c.d`,
    and the edge's subnet is written as IPv4.
    """
    if text is None:
        return None
    try:
        parsed = ip_address(text.strip())
    except ValueError:
        return None
    if isinstance(parsed, IPv6Address) and parsed.ipv4_mapped is not None:
        return parsed.ipv4_mapped
    return parsed


def address_key(parsed: IPv4Address | IPv6Address) -> str:
    """An IPv4 address as it is; an IPv6 one as its /64, without the
    scope a link-local address may carry."""
    if isinstance(parsed, IPv4Address):
        return str(parsed)
    host_bits = 128 - IPV6_PREFIX
    network = int(parsed) >> host_bits << host_bits
    return str(IPv6Network((network, IPV6_PREFIX)))


def from_edge(peer: str | None, edge: Iterable[Network]) -> bool:
    """Whether the TCP peer `peer` is in the edge's subnet."""
    parsed = address(peer)
    return parsed is not None and any(parsed in subnet for subnet in edge)


def client_key(
    peer: str | None, headers: Mapping[str, str], edge: Iterable[Network],
) -> str:
    """The key for a request from the TCP peer `peer` with `headers`.

    The address the edge names when the peer is the edge, and the
    peer's own otherwise. A request through the tunnel that names no
    address, or one that is not an address, is keyed by the tunnel's:
    one bucket for all of them, as the Worker's `anonymous` was.
    """
    edge = tuple(edge)
    if from_edge(peer, edge):
        named = address(headers.get(CONNECTING_IP))
        if named is not None:
            return address_key(named)
    parsed = address(peer)
    if parsed is not None:
        return address_key(parsed)
    return (peer or UNKNOWN)[:MAX_KEY]
