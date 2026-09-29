"""What `web serve` is configured with, read before it starts (#134).

The Worker could not refuse to start. A rate limit that was not one, or
a cap that was not a number of dollars, refused every request instead,
and the first visitor found out. Here each setting is read before the
service listens, and one that is missing where it is needed, or not
what it must be, stops it there, naming itself.
"""
import sqlite3
from contextlib import closing
from ipaddress import ip_network
from pathlib import Path

import pytest

from chatsbom.server.pricing import OFF_PEAK
from chatsbom.server.pricing import PEAK
from chatsbom.server.pricing import PEAK_HOURS
from chatsbom.server.pricing import Prices
from chatsbom.server.pricing import Window
from chatsbom.server.ratelimit import RateLimit
from chatsbom.server.settings import ChatSettings
from chatsbom.server.settings import Settings
from chatsbom.server.settings import settings_from
from chatsbom.server.settings import SettingsError
from chatsbom.snapshot.publish import publish
from chatsbom.snapshot.write import WRITING
from chatsbom.snapshot.write import Written
from tests.dataset_contract_test import CONTRACT
from tests.dataset_contract_test import corpus

KEY = 'k' * 32

#: A snapshot's id, as `CURRENT` names it.
ID = '0123456789abcdef'


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


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    """The contract's corpus, as a snapshot is published (#132)."""
    return corpus(tmp_path)


def published(directory: Path, made: Path, id: str) -> Path:
    """`made`, published in `directory` as `snapshot build` publishes a
    snapshot (#132): renamed to its id, then named first by `CURRENT`."""
    directory.mkdir(parents=True, exist_ok=True)
    writing = made.rename(directory / f'{WRITING}{id}.sqlite')
    return publish(
        Written(path=writing, id=id, rows={}, seconds={}), directory,
    ).path


def chat_on(spa: Path, snapshot: Path, **environ: str) -> Settings:
    return settings_from(
        {
            'ALTCHA_HMAC_KEY': KEY,
            'DEEPSEEK_API_KEY': 'sk-test',
            'WEB_SNAPSHOT': str(snapshot),
            **environ,
        },
        spa=spa,
    )


#: Each price setting, and the rate it sets: dollars per million tokens.
PRICE_SETTINGS = {
    'CHAT_INPUT_USD_PER_MTOK': ('peak', 'input'),
    'CHAT_CACHED_INPUT_USD_PER_MTOK': ('peak', 'cached_input'),
    'CHAT_OUTPUT_USD_PER_MTOK': ('peak', 'output'),
    'CHAT_OFF_PEAK_INPUT_USD_PER_MTOK': ('off_peak', 'input'),
    'CHAT_OFF_PEAK_CACHED_INPUT_USD_PER_MTOK': ('off_peak', 'cached_input'),
    'CHAT_OFF_PEAK_OUTPUT_USD_PER_MTOK': ('off_peak', 'output'),
}


class TestTheChat:
    """The chat runs on DeepSeek (#140, the owner's decision on Q8), and
    is off without a key to reach it: the route says so, and the rest
    of the service runs."""

    @pytest.mark.parametrize('environ', [{}, {'DEEPSEEK_API_KEY': ''}])
    def test_is_off_without_a_deepseek_key(self, spa, environ):
        assert read(spa, **environ).chat is None

    def test_is_on_with_a_key_and_a_snapshot_to_answer_from(
        self, spa, snapshot,
    ):
        settings = chat_on(spa, snapshot)
        assert settings.snapshot == snapshot
        assert settings.chat == ChatSettings(
            api_key='sk-test',
            base_url='https://api.deepseek.com',
            model='deepseek-flash',
            prices=Prices(),
            max_in_flight=3,
        )

    def test_never_shows_its_key(self, spa, snapshot):
        """Settings are printed as a whole when something goes wrong."""
        settings = chat_on(spa, snapshot, DEEPSEEK_API_KEY='sk-secret-value')
        assert 'sk-secret-value' not in repr(settings)
        assert 'sk-secret-value' not in str(settings)

    def test_needs_a_snapshot_to_answer_from(self, spa):
        """A key and no dataset is a chat that could answer nothing."""
        error = refusal(spa, {'ALTCHA_HMAC_KEY': KEY, 'DEEPSEEK_API_KEY': 'k'})
        assert error.setting == 'WEB_SNAPSHOT'
        assert 'DEEPSEEK_API_KEY' in str(error)

    def test_takes_its_model_and_where_to_reach_it(self, spa, snapshot):
        chat = chat_on(
            spa, snapshot,
            CHAT_MODEL='deepseek-v4-pro',
            DEEPSEEK_BASE_URL='http://127.0.0.1:9/stand-in',
        ).chat
        assert chat is not None
        assert chat.model == 'deepseek-v4-pro'
        assert chat.base_url == 'http://127.0.0.1:9/stand-in'

    @pytest.mark.parametrize(
        'value',
        ['api.deepseek.com', 'ftp://api.deepseek.com', 'https://', 'file:///x'],
    )
    def test_refuses_an_endpoint_that_is_not_a_web_address(
        self, spa, snapshot, value,
    ):
        with pytest.raises(SettingsError) as refused:
            chat_on(spa, snapshot, DEEPSEEK_BASE_URL=value)
        assert refused.value.setting == 'DEEPSEEK_BASE_URL'

    def test_holds_as_many_questions_at_once_as_it_is_told(self, spa, snapshot):
        chat = chat_on(spa, snapshot, CHAT_MAX_IN_FLIGHT='7').chat
        assert chat is not None and chat.max_in_flight == 7

    @pytest.mark.parametrize('value', ['0', '-1', 'three', '2.5', 'true'])
    def test_refuses_an_in_flight_cap_that_is_not_one(
        self, spa, snapshot, value,
    ):
        with pytest.raises(SettingsError) as refused:
            chat_on(spa, snapshot, CHAT_MAX_IN_FLIGHT=value)
        assert refused.value.setting == 'CHAT_MAX_IN_FLIGHT'

    @pytest.mark.parametrize(
        'name',
        [
            'DEEPSEEK_BASE_URL', 'CHAT_MODEL', 'CHAT_MAX_IN_FLIGHT',
            'CHAT_PEAK_HOURS', *PRICE_SETTINGS,
        ],
    )
    def test_takes_an_empty_setting_for_its_default(self, spa, snapshot, name):
        assert chat_on(spa, snapshot, **{name: ''}) == chat_on(spa, snapshot)


class TestThePrices:
    """DeepSeek's, as its pricing page said them on 2026-09-29, unless
    set: a change of price is a change of setting."""

    def test_are_deepseeks_unless_set(self, spa, snapshot):
        chat = chat_on(spa, snapshot).chat
        assert chat is not None
        assert chat.prices == Prices(
            peak=PEAK, off_peak=OFF_PEAK, peak_hours=PEAK_HOURS,
        )

    @pytest.mark.parametrize('name', sorted(PRICE_SETTINGS))
    def test_each_sets_its_own_rate(self, spa, snapshot, name):
        chat = chat_on(spa, snapshot, **{name: '9.75'}).chat
        assert chat is not None

        def rate(setting: str, prices: Prices) -> float:
            hours, kind = PRICE_SETTINGS[setting]
            value: float = getattr(getattr(prices, hours), kind)
            return value

        assert rate(name, chat.prices) == 9.75
        for other in PRICE_SETTINGS:
            if other != name:
                assert rate(other, chat.prices) == rate(other, Prices())

    @pytest.mark.parametrize('name', sorted(PRICE_SETTINGS))
    @pytest.mark.parametrize('value', ['free', '-0.1', 'nan', 'inf'])
    def test_refuse_what_is_not_a_number_of_dollars(
        self, spa, snapshot, name, value,
    ):
        with pytest.raises(SettingsError) as refused:
            chat_on(spa, snapshot, **{name: value})
        assert refused.value.setting == name

    def test_are_checked_with_the_chat_off_too(self, spa):
        """Set, a setting is read, whether or not it is used yet."""
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'CHAT_OUTPUT_USD_PER_MTOK': 'free'},
        )
        assert error.setting == 'CHAT_OUTPUT_USD_PER_MTOK'

    @pytest.mark.parametrize(
        'value,hours',
        [
            ('02:00-05:00', (Window(120, 300),)),
            (' 01:00-04:00 , 06:30-10:00 ', (Window(60, 240), Window(390, 600))),
            ('22:00-24:00', (Window(1320, 1440),)),
        ],
    )
    def test_peak_hours_are_utc_spans_of_a_weekday(
        self, spa, snapshot, value, hours,
    ):
        chat = chat_on(spa, snapshot, CHAT_PEAK_HOURS=value).chat
        assert chat is not None and chat.prices.peak_hours == hours

    @pytest.mark.parametrize(
        'value',
        [
            '1-4', '04:00-01:00', '01:00-01:00', '25:00-26:00', '01:60-02:00',
            '24:00-24:00', 'always', '01:00-04:00,', '01:00-04:00;06:00-10:00',
        ],
    )
    def test_refuse_peak_hours_that_are_not_spans(self, spa, snapshot, value):
        with pytest.raises(SettingsError) as refused:
            chat_on(spa, snapshot, CHAT_PEAK_HOURS=value)
        assert refused.value.setting == 'CHAT_PEAK_HOURS'
        assert repr(value) in str(refused.value)


class TestTheSnapshot:
    """The dataset the chat's tools read: a snapshot, or the directory
    snapshots are published in (#132), opened as a snapshot is (#138):
    read-only, and checked before the service starts."""

    def test_is_none_unless_set(self, spa):
        assert read(spa).snapshot is None

    def test_may_be_set_with_the_chat_off(self, spa, snapshot):
        assert read(spa, WEB_SNAPSHOT=str(snapshot)).snapshot == snapshot

    def test_is_opened_read_only_and_left_as_it_was(self, spa, snapshot):
        before = sorted(snapshot.parent.iterdir())
        stamp = snapshot.stat().st_mtime_ns
        read(spa, WEB_SNAPSHOT=str(snapshot))
        assert sorted(snapshot.parent.iterdir()) == before
        assert snapshot.stat().st_mtime_ns == stamp

    def test_must_be_there(self, spa, tmp_path):
        missing = tmp_path / 'snapshots' / 'missing.sqlite'
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'WEB_SNAPSHOT': str(missing)},
        )
        assert error.setting == 'WEB_SNAPSHOT'
        assert str(missing) in str(error)
        # Not made in trying.
        assert not missing.exists()

    def test_must_be_sqlite(self, spa, tmp_path):
        text = tmp_path / 'notes.sqlite'
        text.write_text('not a database, though it says it is')
        error = refusal(
            spa, {
                'ALTCHA_HMAC_KEY': KEY,
                'WEB_SNAPSHOT': str(text),
            },
        )
        assert error.setting == 'WEB_SNAPSHOT'

    def test_must_be_the_datasets(self, spa, tmp_path):
        other = tmp_path / 'other.sqlite'
        with closing(sqlite3.connect(other)) as db:
            db.execute('CREATE TABLE unrelated (x)')
            db.commit()
        error = refusal(
            spa, {
                'ALTCHA_HMAC_KEY': KEY,
                'WEB_SNAPSHOT': str(other),
            },
        )
        assert error.setting == 'WEB_SNAPSHOT'
        assert 'snapshot build' in str(error)

    def test_must_have_the_page_table_a_d1_export_has_not(
        self, spa, tmp_path,
    ):
        """`export d1`'s scripts, applied to a file, made the dataset
        before #132. They make no table of a package's dependants, which
        the dataset reads them from now, so every such call would fail:
        the service does not start with one."""
        exported = tmp_path / 'd1.sqlite'
        with closing(sqlite3.connect(exported)) as db:
            db.executescript((CONTRACT / 'd1.sql').read_text(encoding='utf-8'))
            db.commit()
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'WEB_SNAPSHOT': str(exported)},
        )
        assert error.setting == 'WEB_SNAPSHOT'
        assert 'snapshot build' in str(error)

    def test_may_be_the_directory_snapshots_are_published_in(
        self, spa, tmp_path,
    ):
        """Where `snapshot build` publishes: each question reads the
        snapshot `CURRENT` names as it starts (`Asking.pin`), so a new
        one is served without a restart."""
        snapshots = tmp_path / 'snapshots'
        published(snapshots, corpus(tmp_path), ID)
        settings = chat_on(spa, snapshots)
        assert settings.snapshot == snapshots
        assert settings.chat is not None

    def test_a_directory_must_have_one_published(self, spa, tmp_path):
        snapshots = tmp_path / 'snapshots'
        snapshots.mkdir()
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'WEB_SNAPSHOT': str(snapshots)},
        )
        assert error.setting == 'WEB_SNAPSHOT'
        assert str(snapshots) in str(error)
        assert 'snapshot build' in str(error)
        # Nothing made in looking.
        assert list(snapshots.iterdir()) == []

    def test_the_one_a_directory_names_is_checked_as_one_named_is(
        self, spa, tmp_path,
    ):
        snapshots = tmp_path / 'snapshots'
        other = tmp_path / 'other.sqlite'
        with closing(sqlite3.connect(other)) as db:
            db.execute('CREATE TABLE unrelated (x)')
            db.commit()
        published(snapshots, other, ID)
        error = refusal(
            spa, {'ALTCHA_HMAC_KEY': KEY, 'WEB_SNAPSHOT': str(snapshots)},
        )
        assert error.setting == 'WEB_SNAPSHOT'
        assert f'{ID}.sqlite' in str(error)
