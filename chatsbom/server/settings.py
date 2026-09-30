"""What `web serve` is configured with: the environment, and `.env`,
read once, before the service listens.

The Worker could not refuse to start. A rate limit that was not one, or
a cap that was not a number of dollars, refused every request instead,
and the first visitor found out (web/src/ratelimit.ts, chat.ts). Here a
setting that is missing where it is needed, or not what it must be,
stops the service before it starts, naming itself (`SettingsError`).

The names are the Worker's where it had them, CHAT_RATE_LIMIT,
QUERY_RATE_LIMIT and DAILY_SPEND_CAP_USD, with the defaults it gave
them. The new ones are what only this service needs: ALTCHA_HMAC_KEY,
EDGE_SUBNET and WEB_STATE_DIR; the chat's (#140), DEEPSEEK_API_KEY and
the rest, with their defaults from DeepSeek's own documentation, and
WEB_SNAPSHOT, the dataset its tools read; and the export's (#154),
WEB_EXPORT_DIR and EXPORT_RATE_LIMIT. `.env.example` describes each.
"""
import math
import re
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
from dataclasses import field
from ipaddress import ip_network
from pathlib import Path
from urllib.parse import urlsplit

from chatsbom.dataset.open import connect
from chatsbom.dataset.open import current
from chatsbom.server.clients import Network
from chatsbom.server.pricing import OFF_PEAK
from chatsbom.server.pricing import PEAK
from chatsbom.server.pricing import PEAK_HOURS
from chatsbom.server.pricing import Prices
from chatsbom.server.pricing import Rates
from chatsbom.server.pricing import Window
from chatsbom.server.ratelimit import RateLimit

#: The Worker's: every question is a paid model call.
CHAT_RATE_LIMIT = RateLimit(20, 60)
#: The Worker's: a page view was about 25 queries.
QUERY_RATE_LIMIT = RateLimit(100, 10)
#: The export's own (#154). A reader asks for a file a range at a time:
#: DuckDB 1.5 a HEAD and two ranges for the footer, then one for each
#: row group it reads, every column in it at once. Measured, 23 requests
#: for a table of 20 row groups read whole, and 3 for its count, so
#: about 90 for the artifacts' 84 at the documented shape. A minute's
#: 600 lets a reader take that one whole six times over, at once, and
#: then a request every tenth of a second, the page's rate.
EXPORT_RATE_LIMIT = RateLimit(600, 60)
#: The Worker's, in US dollars a UTC day.
DAILY_SPEND_CAP_USD = 5.0

#: Where web.sqlite is kept unless WEB_STATE_DIR says: `data`, where the
#: CLI keeps the collector's ledger, in the directory it runs in.
STATE_DIR = Path('data')

#: Where the export is unless WEB_EXPORT_DIR says: where the collector's
#: loop exports it (deploy/collector-loop.sh), in the directory it runs
#: in.
EXPORT_DIR = Path('data/export')

#: The fewest characters ALTCHA_HMAC_KEY may have. Anyone who has one
#: signed challenge can try keys against it offline, as fast as they can
#: compute HMACs; 32 random hex digits, 128 bits, are out of reach, and
#: `openssl rand -hex 32` makes 64.
MIN_KEY_LENGTH = 32

#: A limit as `LIMIT/PERIOD`: at most LIMIT requests in PERIOD seconds.
SHORTHAND = re.compile(r'\s*(\d+)\s*/\s*(\d+(?:\.\d+)?)\s*')

#: Where DeepSeek's API takes OpenAI's format, as its documentation said
#: on 2026-09-29 (https://api-docs.deepseek.com/quick_start/pricing/).
DEEPSEEK_BASE_URL = 'https://api.deepseek.com'

#: The owner's choice (#128, Q8): DeepSeek-V4.1-Flash, by the name its
#: pricing page gave it on 2026-09-29.
CHAT_MODEL = 'deepseek-flash'

#: Questions answered at once, whoever asks them (#128, section 2.6):
#: each holds a model call and its reservation open for as long as it
#: runs.
CHAT_MAX_IN_FLIGHT = 3

#: A span of peak hours, `HH:MM-HH:MM`, in UTC.
SPAN = re.compile(r'(\d\d):(\d\d)-(\d\d):(\d\d)')


class SettingsError(ValueError):
    """A setting the service cannot start with: `setting` names it."""

    def __init__(self, setting: str, problem: str) -> None:
        super().__init__(problem)
        self.setting = setting


@dataclass(frozen=True)
class ChatSettings:
    """The chat's (#140): how it reaches its model, what that costs, and
    how many questions it answers at once."""

    #: DeepSeek's key: never shown, whole settings being printed when
    #: something goes wrong.
    api_key: str = field(repr=False)
    #: Where its OpenAI-format API is.
    base_url: str
    model: str
    prices: Prices
    max_in_flight: int


@dataclass(frozen=True)
class Settings:
    """The web service's configuration."""

    #: The built page: index.html, and the content-hashed assets/.
    spa: Path
    #: Where web.sqlite is.
    state_dir: Path
    #: Where cloudflared is: CF-Connecting-IP is believed from a peer
    #: there, and nowhere else (`clients`).
    edge: tuple[Network, ...]
    #: What the challenges are signed with.
    altcha_key: bytes
    chat_limit: RateLimit
    query_limit: RateLimit
    #: The most the AI answers may spend in a UTC day; None for no cap.
    daily_cap_usd: float | None
    #: The dataset, which the page's reads and the chat's tools read: a
    #: snapshot, or the directory snapshots are published in, where each
    #: question reads the one `CURRENT` names as it starts (#132), and
    #: the page may read any `CURRENT` lists (#144). None when none is
    #: set.
    snapshot: Path | None = None
    #: The chat; None when it is off, with no DEEPSEEK_API_KEY.
    chat: ChatSettings | None = None
    #: The weekly Parquet export, which the service serves as it finds
    #: it (#154): the manifest, and the files it names. Missing until
    #: the collector makes it.
    export_dir: Path = EXPORT_DIR
    export_limit: RateLimit = EXPORT_RATE_LIMIT


def rate_limit(setting: str, value: str | None, default: RateLimit) -> RateLimit:
    """`value`, the setting `setting`, as a limit: `20/60`.

    A plain value, because `.env` is read by python-dotenv, compose and
    systemd, which agree on one and not on one with quotes in it
    (`.env.example`): so not the JSON object the Worker's wrangler.jsonc
    wrote. Unset or empty is `default`.
    """
    if not value or not value.strip():
        return default
    try:
        shorthand = SHORTHAND.fullmatch(value)
        if shorthand is None:
            raise ValueError('not LIMIT/PERIOD')
        return RateLimit(int(shorthand[1]), float(shorthand[2]))
    except ValueError as error:
        raise SettingsError(
            setting,
            f'{setting} is not at most LIMIT requests in PERIOD seconds, '
            f'as `20/60`: {value!r} ({error})',
        ) from None


def daily_cap(value: str | None) -> float | None:
    """DAILY_SPEND_CAP_USD: dollars, 0 for no cap, and unset or empty
    for DAILY_SPEND_CAP_USD's default, as compose passes it. Anything
    else refuses to start rather than lift the cap: a typo in a bound is
    no reason for it to stop being one."""
    if not value or not value.strip():
        return DAILY_SPEND_CAP_USD
    try:
        cap = float(value)
    except ValueError:
        cap = math.nan
    if not (math.isfinite(cap) and cap >= 0):
        raise SettingsError(
            'DAILY_SPEND_CAP_USD',
            f'DAILY_SPEND_CAP_USD is not a number of dollars: {value!r}',
        )
    return cap or None


def edge_subnets(value: str | None) -> tuple[Network, ...]:
    """EDGE_SUBNET: the edge network's subnets, comma-separated, as a
    dual-stack network has two. Each must be private: the edge is a
    compose network only cloudflared is on, and a public subnet would
    believe the header from strangers."""
    subnets = []
    for part in (value or '').split(','):
        if not part.strip():
            continue
        try:
            subnet = ip_network(part.strip())
        except ValueError as error:
            raise SettingsError(
                'EDGE_SUBNET',
                f'EDGE_SUBNET is not subnets, as 172.30.0.0/24: {part!r} '
                f'({error})',
            ) from None
        if not subnet.is_private:
            raise SettingsError(
                'EDGE_SUBNET',
                f'EDGE_SUBNET names {subnet}, which is not a private '
                'subnet: CF-Connecting-IP would be believed from anyone '
                'in it',
            )
        subnets.append(subnet)
    return tuple(subnets)


def altcha_key(value: str | None) -> bytes:
    """ALTCHA_HMAC_KEY, which has no default: without it, anyone could
    sign their own challenges."""
    how = (
        'The proof-of-work challenges are signed with it. Make one with '
        '`openssl rand -hex 32`, and set it in the environment or .env.'
    )
    if not value:
        raise SettingsError(
            'ALTCHA_HMAC_KEY', f'ALTCHA_HMAC_KEY is not set. {how}',
        )
    if len(value) < MIN_KEY_LENGTH:
        raise SettingsError(
            'ALTCHA_HMAC_KEY',
            f'ALTCHA_HMAC_KEY has {len(value)} characters, fewer than '
            f'{MIN_KEY_LENGTH}. {how}',
        )
    return value.encode()


def built_page(spa: Path) -> Path:
    """`spa`, if the page has been built there."""
    if not ((spa / 'index.html').is_file() and (spa / 'assets').is_dir()):
        raise SettingsError(
            '--spa',
            f'No built page in {spa}: it needs index.html and assets/. '
            'Build it with `npm ci && npm run build` in web/, or name '
            'where it was built with --spa.',
        )
    return spa


def _set(value: str | None) -> str | None:
    """A setting's value, or None for one unset or empty, as compose's
    `${X:-...}` reads an empty one."""
    return value.strip() if value and value.strip() else None


def base_url(value: str | None) -> str:
    """DEEPSEEK_BASE_URL: where the model is, over HTTP or HTTPS. A
    stand-in for it, in a test or a smoke run, is one."""
    url = _set(value)
    if url is None:
        return DEEPSEEK_BASE_URL
    parts = urlsplit(url)
    if parts.scheme not in ('http', 'https') or not parts.hostname:
        raise SettingsError(
            'DEEPSEEK_BASE_URL',
            f'DEEPSEEK_BASE_URL is not an http or https address, as '
            f'{DEEPSEEK_BASE_URL}: {value!r}',
        )
    return url


def in_flight(value: str | None) -> int:
    """CHAT_MAX_IN_FLIGHT: a whole number of questions, 1 or more."""
    setting = _set(value)
    if setting is None:
        return CHAT_MAX_IN_FLIGHT
    if not setting.isdigit() or int(setting) < 1:
        raise SettingsError(
            'CHAT_MAX_IN_FLIGHT',
            f'CHAT_MAX_IN_FLIGHT is not a whole number of questions, 1 or '
            f'more: {value!r}',
        )
    return int(setting)


def price(setting: str, value: str | None, default: float) -> float:
    """A price setting: US dollars per million tokens. Anything else
    refuses to start, rather than price every call at nothing."""
    text = _set(value)
    if text is None:
        return default
    try:
        dollars = float(text)
    except ValueError:
        dollars = math.nan
    if not (math.isfinite(dollars) and dollars >= 0):
        raise SettingsError(
            setting,
            f'{setting} is not a number of US dollars per million tokens: '
            f'{value!r}',
        )
    return dollars


def peak_hours(value: str | None) -> tuple[Window, ...]:
    """CHAT_PEAK_HOURS: the spans of a weekday, in UTC, when DeepSeek
    charges its peak rates, comma-separated, as `01:00-04:00,06:00-10:00`
    (`pricing`). Unset, DeepSeek's own."""
    text = _set(value)
    if text is None:
        return PEAK_HOURS
    windows = []
    for part in text.split(','):
        span = SPAN.fullmatch(part.strip())
        try:
            if span is None:
                raise ValueError('not HH:MM-HH:MM')
            hours = [int(number) for number in span.groups()]
            if hours[1] > 59 or hours[3] > 59:
                raise ValueError('an hour has 60 minutes')
            windows.append(
                Window(hours[0] * 60 + hours[1], hours[2] * 60 + hours[3]),
            )
        except ValueError as error:
            raise SettingsError(
                'CHAT_PEAK_HOURS',
                f'CHAT_PEAK_HOURS is not spans of hours in UTC, as '
                f'01:00-04:00,06:00-10:00: {value!r} ({error})',
            ) from None
    return tuple(windows)


def prices(environ: Mapping[str, str]) -> Prices:
    """The chat model's prices, peak and off peak, and its peak hours:
    DeepSeek's, unless set. Each is read by its name, as every setting
    is, so that `.env.example`'s test finds it read."""
    return Prices(
        peak=Rates(
            input=price(
                'CHAT_INPUT_USD_PER_MTOK',
                environ.get('CHAT_INPUT_USD_PER_MTOK'), PEAK.input,
            ),
            cached_input=price(
                'CHAT_CACHED_INPUT_USD_PER_MTOK',
                environ.get('CHAT_CACHED_INPUT_USD_PER_MTOK'),
                PEAK.cached_input,
            ),
            output=price(
                'CHAT_OUTPUT_USD_PER_MTOK',
                environ.get('CHAT_OUTPUT_USD_PER_MTOK'), PEAK.output,
            ),
        ),
        off_peak=Rates(
            input=price(
                'CHAT_OFF_PEAK_INPUT_USD_PER_MTOK',
                environ.get('CHAT_OFF_PEAK_INPUT_USD_PER_MTOK'),
                OFF_PEAK.input,
            ),
            cached_input=price(
                'CHAT_OFF_PEAK_CACHED_INPUT_USD_PER_MTOK',
                environ.get('CHAT_OFF_PEAK_CACHED_INPUT_USD_PER_MTOK'),
                OFF_PEAK.cached_input,
            ),
            output=price(
                'CHAT_OFF_PEAK_OUTPUT_USD_PER_MTOK',
                environ.get('CHAT_OFF_PEAK_OUTPUT_USD_PER_MTOK'),
                OFF_PEAK.output,
            ),
        ),
        peak_hours=peak_hours(environ.get('CHAT_PEAK_HOURS')),
    )


def snapshot(value: str | None) -> Path | None:
    """WEB_SNAPSHOT: the dataset, a snapshot `snapshot build` published,
    or the directory it publishes them in (`data/snapshots` unless told
    otherwise), where each question reads the one `CURRENT` names as it
    starts (#132, `ask.Asking.pin`). The snapshot, or the one `CURRENT`
    names now, is opened as the chat's tools open it, read-only, to see
    that it is one: a file of D1's tables alone is not, without the
    table a package's dependants are read from."""
    text = _set(value)
    if text is None:
        return None
    path = Path(text)
    if path.is_dir():
        try:
            named = current(path)
        except (OSError, ValueError) as error:
            raise SettingsError(
                'WEB_SNAPSHOT',
                f'WEB_SNAPSHOT names no snapshot to serve: {error}',
            ) from None
    elif path.is_file():
        named = path
    else:
        raise SettingsError(
            'WEB_SNAPSHOT',
            f'No snapshot at {path}: WEB_SNAPSHOT names no file or '
            'directory. `chatsbom snapshot build` publishes snapshots in '
            'data/snapshots.',
        )
    try:
        with closing(connect(named)) as db:
            db.execute('SELECT count(*) FROM meta').fetchone()
            db.execute('SELECT 1 FROM dependants LIMIT 1').fetchone()
    except sqlite3.Error as error:
        raise SettingsError(
            'WEB_SNAPSHOT',
            f'{named} is not a snapshot ({error}): WEB_SNAPSHOT is to name '
            'the directory `chatsbom snapshot build` publishes snapshots '
            'in, or one of them.',
        ) from None
    return path


def export_dir(value: str | None) -> Path:
    """WEB_EXPORT_DIR: the directory the collector exports into, which
    need not be there yet. The collector makes it as it starts, and
    until the first export is written in it there is nothing to serve:
    each request under /export/ answers 404. A file is not one."""
    text = _set(value)
    path = EXPORT_DIR if text is None else Path(text)
    if path.exists() and not path.is_dir():
        raise SettingsError(
            'WEB_EXPORT_DIR',
            f'{path} is not a directory: WEB_EXPORT_DIR is to name the '
            'directory `chatsbom export parquet` writes the export in, '
            'data/export as the collector runs it.',
        )
    return path


def chat(
    environ: Mapping[str, str], dataset: Path | None,
) -> ChatSettings | None:
    """The chat's settings, or None with no DEEPSEEK_API_KEY: the chat
    is then off, and says so to anyone who asks. Each of its settings
    is read either way: one that is set must be one."""
    key = _set(environ.get('DEEPSEEK_API_KEY'))
    settings = ChatSettings(
        api_key=key or '',
        base_url=base_url(environ.get('DEEPSEEK_BASE_URL')),
        model=_set(environ.get('CHAT_MODEL')) or CHAT_MODEL,
        prices=prices(environ),
        max_in_flight=in_flight(environ.get('CHAT_MAX_IN_FLIGHT')),
    )
    if key is None:
        return None
    if dataset is None:
        raise SettingsError(
            'WEB_SNAPSHOT',
            'The chat needs a dataset to answer from, and WEB_SNAPSHOT '
            'names none: set it, or unset DEEPSEEK_API_KEY to keep the '
            'chat off.',
        )
    return settings


def settings_from(environ: Mapping[str, str], *, spa: Path) -> Settings:
    """The settings `environ` holds, for the page built in `spa`, or the
    first reason it cannot be served with them."""
    state = environ.get('WEB_STATE_DIR')
    dataset = snapshot(environ.get('WEB_SNAPSHOT'))
    return Settings(
        spa=built_page(spa),
        state_dir=Path(state) if state else STATE_DIR,
        edge=edge_subnets(environ.get('EDGE_SUBNET')),
        altcha_key=altcha_key(environ.get('ALTCHA_HMAC_KEY')),
        chat_limit=rate_limit(
            'CHAT_RATE_LIMIT', environ.get('CHAT_RATE_LIMIT'),
            CHAT_RATE_LIMIT,
        ),
        query_limit=rate_limit(
            'QUERY_RATE_LIMIT', environ.get('QUERY_RATE_LIMIT'),
            QUERY_RATE_LIMIT,
        ),
        daily_cap_usd=daily_cap(environ.get('DAILY_SPEND_CAP_USD')),
        snapshot=dataset,
        chat=chat(environ, dataset),
        export_dir=export_dir(environ.get('WEB_EXPORT_DIR')),
        export_limit=rate_limit(
            'EXPORT_RATE_LIMIT', environ.get('EXPORT_RATE_LIMIT'),
            EXPORT_RATE_LIMIT,
        ),
    )
