"""Who a request is from, as the web service's limits see it (#134).

The Worker keyed its limiters on `CF-Connecting-IP` whenever the edge
vouched for the request with a shared secret (web/src/ratelimit.ts). In
the Python service the edge is a network: cloudflared, alone on the
compose network `edge` (#130), and Cloudflare overwrites the header
before it gets there. So the header is believed from a peer inside that
subnet and nowhere else (#128, section 2.7): any other peer, the host
or the healthcheck among them, could write it, and a new address on
every request would be a new budget on every request (#18, #31).

And an IPv6 client is keyed by its /64. The Worker keyed the full
address, and a client holds a /64 at the least, so it could rotate its
way to a fresh budget without leaving its own network.
"""
import ipaddress

import pytest

from chatsbom.server.clients import client_key
from chatsbom.server.clients import from_edge

#: Where cloudflared is, as compose would name it: two networks, as a
#: dual-stack `edge` network has.
EDGE = (
    ipaddress.ip_network('172.30.0.0/24'),
    ipaddress.ip_network('fd00:ed9e::/64'),
)

#: cloudflared's own address, inside the edge.
TUNNEL = '172.30.0.2'


def named(address: str) -> dict[str, str]:
    """The header Cloudflare's edge sets, naming `address`."""
    return {'cf-connecting-ip': address}


class TestTheTrustRule:
    def test_the_edge_names_the_visitor(self):
        assert client_key(TUNNEL, named('203.0.113.7'), EDGE) == '203.0.113.7'

    def test_a_peer_outside_the_edge_is_keyed_by_its_own_address(self):
        """Whatever it claims: the header came from the client itself."""
        keys = {
            client_key('198.51.100.1', named(claimed), EDGE)
            for claimed in ('203.0.113.7', '203.0.113.8', '192.0.2.1')
        }
        assert keys == {'198.51.100.1'}

    def test_a_spoofed_header_from_the_host_is_not_believed(self):
        """The host reaches the service from outside the edge network,
        as a healthcheck or anyone else on it would."""
        assert client_key('127.0.0.1', named('203.0.113.7'), EDGE) == (
            '127.0.0.1'
        )

    def test_no_edge_configured_believes_no_header(self):
        """Unset, nothing is known to be the edge, so nothing is."""
        assert client_key(TUNNEL, named('203.0.113.7'), ()) == TUNNEL

    def test_the_edge_naming_no_one_is_one_bucket_the_tunnels(self):
        """Cloudflare always names the visitor; a request through the
        tunnel that names none shares one bucket, as the Worker's
        `anonymous` one."""
        assert client_key(TUNNEL, {}, EDGE) == TUNNEL
        assert client_key('172.30.0.3', {}, EDGE) == '172.30.0.3'

    @pytest.mark.parametrize(
        'header', ['', 'unknown', '203.0.113.7, 198.51.100.1', '999.1.1.1'],
    )
    def test_a_header_that_names_no_address_is_not_a_key(self, header):
        assert client_key(TUNNEL, named(header), EDGE) == TUNNEL

    def test_the_header_may_have_space_around_it(self):
        assert client_key(TUNNEL, named(' 203.0.113.7 '), EDGE) == (
            '203.0.113.7'
        )

    def test_an_ipv6_edge_is_the_edge_too(self):
        assert client_key('fd00:ed9e::2', named('203.0.113.7'), EDGE) == (
            '203.0.113.7'
        )

    def test_an_ipv4_peer_on_a_dual_stack_socket_is_still_in_the_edge(self):
        """A socket bound to `::` reports an IPv4 peer as IPv4-mapped."""
        mapped = f'::ffff:{TUNNEL}'
        assert client_key(mapped, named('203.0.113.7'), EDGE) == (
            '203.0.113.7'
        )
        assert from_edge(mapped, EDGE)

    def test_a_peer_that_is_no_address_is_a_key_of_its_own(self):
        """A Unix socket, or a test client, gives a name or nothing."""
        assert client_key('testclient', named('203.0.113.7'), EDGE) == (
            'testclient'
        )
        assert client_key(None, named('203.0.113.7'), EDGE) == 'unknown'
        assert not from_edge('testclient', EDGE)
        assert not from_edge(None, EDGE)


class TestIPv6Keys:
    def test_a_client_is_its_64(self):
        assert client_key(TUNNEL, named('2001:db8:1:2:aaaa::1'), EDGE) == (
            '2001:db8:1:2::/64'
        )

    def test_rotating_within_the_64_is_the_same_client(self):
        keys = {
            client_key(TUNNEL, named(address), EDGE)
            for address in (
                '2001:db8:1:2::1',
                '2001:db8:1:2:ffff:ffff:ffff:ffff',
                '2001:DB8:1:2:0:0:0:abcd',
            )
        }
        assert keys == {'2001:db8:1:2::/64'}

    def test_another_64_is_another_client(self):
        assert client_key(TUNNEL, named('2001:db8:1:3::1'), EDGE) != (
            client_key(TUNNEL, named('2001:db8:1:2::1'), EDGE)
        )

    def test_a_peer_of_its_own_is_keyed_by_its_64_too(self):
        assert client_key('2001:db8:9:9::5', {}, EDGE) == '2001:db8:9:9::/64'

    def test_an_ipv4_mapped_address_is_the_ipv4_address(self):
        assert client_key(TUNNEL, named('::ffff:203.0.113.7'), EDGE) == (
            '203.0.113.7'
        )

    def test_a_scoped_address_is_keyed_without_its_scope(self):
        assert client_key('fe80::1%eth0', {}, EDGE) == 'fe80::/64'


class TestFromEdge:
    def test_inside(self):
        assert from_edge(TUNNEL, EDGE)
        assert from_edge('fd00:ed9e::9', EDGE)

    def test_outside(self):
        assert not from_edge('172.30.1.2', EDGE)
        assert not from_edge('127.0.0.1', EDGE)
        assert not from_edge(TUNNEL, ())
