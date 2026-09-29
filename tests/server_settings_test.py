"""What `web serve` is configured with, read before it starts (#134).

The Worker could not refuse to start. A rate limit that was not one, or
a cap that was not a number of dollars, refused every request instead,
and the first visitor found out. Here each setting is read before the
service listens, and one that is missing where it is needed, or not
what it must be, stops it there, naming itself.
"""
from ipaddress import ip_network
from pathlib import Path

import pytest

from chatsbom.server.ratelimit import RateLimit
from chatsbom.server.settings import settings_from
from chatsbom.server.settings import SettingsError

KEY = 'k' * 32


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    """A built page, as `npm run build` leaves one."""
    root = tmp_path / 'client'
    (root / 'assets').mkdir(parents=True)
    (root / 'index.html').write_text('<!doctype html>')
    return root


def read(spa: Path, **environ: str) -> object:
    return settings_from({'ALTCHA_HMAC_KEY': KEY, **environ}, spa=spa)


def refusal(spa: Path, environ: dict[str, str]) -> SettingsError:
    with pytest.raises(SettingsError) as refused:
        settings_from(environ, spa=spa)
    return refused.value


class TestTheDefaults:
    def test_are_the_workers_where_it_had_them(self, spa):
        """web/wrangler.jsonc's limits and cap, and compose's."""
        settings = settings_from({'ALTCHA_HMAC_KEY': KEY}, spa=spa)
        assert settings.chat_limit == RateLimit(20, 60)
        assert settings.query_limit == RateLimit(100, 10)
        assert settings.daily_cap_usd == 5
        assert settings.altcha_key == KEY.encode()
        assert settings.spa == spa

    def test_believe_no_header(self, spa):
        """No edge configured is none to believe."""
        assert settings_from({'ALTCHA_HMAC_KEY': KEY}, spa=spa).edge == ()

    def test_keep_web_sqlite_under_data(self, spa):
        """Beside the collector's ledger, where the CLI keeps its state."""
        settings = settings_from({'ALTCHA_HMAC_KEY': KEY}, spa=spa)
        assert settings.state_dir == Path('data')

    @pytest.mark.parametrize(
        'name', ['CHAT_RATE_LIMIT', 'QUERY_RATE_LIMIT', 'DAILY_SPEND_CAP_USD'],
    )
    def test_are_what_an_empty_setting_means(self, spa, name):
        """Set but empty is unset, as compose's `${X:-...}` reads it: an
        empty cap must not lift it."""
        default = settings_from({'ALTCHA_HMAC_KEY': KEY}, spa=spa)
        empty = settings_from({'ALTCHA_HMAC_KEY': KEY, name: ''}, spa=spa)
        assert empty == default


class TestTheKey:
    """What signs the challenges. Missing, anyone could sign their own:
    the service does not start, rather than failing at the first
    question."""

    @pytest.mark.parametrize('environ', [{}, {'ALTCHA_HMAC_KEY': ''}])
    def test_is_required(self, spa, environ):
        error = refusal(spa, environ)
        assert error.setting == 'ALTCHA_HMAC_KEY'
        assert 'not set' in str(error)
        assert 'openssl rand -hex 32' in str(error)

    def test_is_at_least_32_characters(self, spa):
        """A short key can be found from one signed challenge, offline."""
        error = refusal(spa, {'ALTCHA_HMAC_KEY': 'k' * 31})
        assert error.setting == 'ALTCHA_HMAC_KEY'
        assert '32' in str(error)
        assert 'k' * 31 not in str(error)


class TestTheEdge:
    def test_is_a_subnet(self, spa):
        assert read(spa, EDGE_SUBNET='172.30.0.0/24').edge == (
            ip_network('172.30.0.0/24'),
        )

    def test_may_be_several_as_a_dual_stack_network_is(self, spa):
        assert read(spa, EDGE_SUBNET='172.30.0.0/24, fd00:ed9e::/64,').edge == (
            ip_network('172.30.0.0/24'), ip_network('fd00:ed9e::/64'),
        )

    @pytest.mark.parametrize(
        'value',
        ['cloudflared', '172.30.0.0/33', '172.30.0.1/24', '172.30.0.0/24;x'],
    )
    def test_refuses_what_is_not_a_subnet(self, spa, value):
        error = refusal(spa, {'ALTCHA_HMAC_KEY': KEY, 'EDGE_SUBNET': value})
        assert error.setting == 'EDGE_SUBNET'

    # Not the documentation ranges, which Python counts as private.
    @pytest.mark.parametrize(
        'value', ['0.0.0.0/0', '::/0', '104.16.0.0/13', '2606:4700::/32'],
    )
    def test_refuses_a_subnet_on_the_internet(self, spa, value):
        """The edge is a compose network, which only cloudflared is on.
        A public one would believe the header from strangers."""
        error = refusal(spa, {'ALTCHA_HMAC_KEY': KEY, 'EDGE_SUBNET': value})
        assert error.setting == 'EDGE_SUBNET'
        assert 'private' in str(error)


class TestTheRateLimits:
    @pytest.mark.parametrize(
        'value',
        ['5/600', ' 5 / 600 ', '{"limit": 5, "period": 600}'],
        ids=['LIMIT/PERIOD', 'with spaces', "wrangler.jsonc's JSON"],
    )
    def test_are_a_limit_in_a_period(self, spa, value):
        """`5/600`, which `.env`'s three readers agree on, or the JSON
        object web/wrangler.jsonc writes, which they do not."""
        assert read(spa, CHAT_RATE_LIMIT=value).chat_limit == RateLimit(5, 600)

    def test_may_have_a_period_that_is_not_whole(self, spa):
        assert read(spa, QUERY_RATE_LIMIT='100/2.5').query_limit == (
            RateLimit(100, 2.5)
        )

    @pytest.mark.parametrize(
        'value',
        [
            'five', '0/60', '5/0', '-5/60', '1.5/60', '5/60/1', 'NaN',
            '{"limit": 5}', '{"limit": true, "period": 60}', '[5, 60]',
            '{"limit": 5, "period": "soon"}',
        ],
    )
    def test_refuse_what_is_not_one(self, spa, value):
        """The Worker refused every request under such a setting."""
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'QUERY_RATE_LIMIT': value},
        )
        assert error.setting == 'QUERY_RATE_LIMIT'
        assert repr(value) in str(error)


class TestTheCap:
    def test_is_dollars(self, spa):
        assert read(spa, DAILY_SPEND_CAP_USD='12.5').daily_cap_usd == 12.5

    @pytest.mark.parametrize('value', ['0', '0.0'])
    def test_is_none_at_0(self, spa, value):
        assert read(spa, DAILY_SPEND_CAP_USD=value).daily_cap_usd is None

    @pytest.mark.parametrize('value', ['five', '-1', 'Infinity', 'nan'])
    def test_refuses_what_is_not_a_number_of_dollars(self, spa, value):
        """Rather than lift the cap: a typo in a bound is no reason for
        it to stop being one."""
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'DAILY_SPEND_CAP_USD': value},
        )
        assert error.setting == 'DAILY_SPEND_CAP_USD'


class TestThePage:
    def test_is_where_the_state_is_not(self, spa, tmp_path):
        settings = read(spa, WEB_STATE_DIR=str(tmp_path / 'state'))
        assert settings.state_dir == tmp_path / 'state'

    @pytest.mark.parametrize('missing', ['index.html', 'assets'])
    def test_must_have_been_built(self, spa, missing):
        target = spa / missing
        if target.is_dir():
            target.rmdir()
        else:
            target.unlink()
        error = refusal(spa, {'ALTCHA_HMAC_KEY': KEY})
        assert error.setting == '--spa'
        assert 'npm run build' in str(error)
