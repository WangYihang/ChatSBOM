"""What `web serve` is configured with: the environment, and `.env`,
read once, before the service listens.

The Worker could not refuse to start. A rate limit that was not one, or
a cap that was not a number of dollars, refused every request instead,
and the first visitor found out (web/src/ratelimit.ts, chat.ts). Here a
setting that is missing where it is needed, or not what it must be,
stops the service before it starts, naming itself (`SettingsError`).

The names are the Worker's where it had them, CHAT_RATE_LIMIT,
QUERY_RATE_LIMIT and DAILY_SPEND_CAP_USD, with their defaults from
web/wrangler.jsonc. The new ones are what only this service needs:
ALTCHA_HMAC_KEY, EDGE_SUBNET and WEB_STATE_DIR. `.env.example`
describes each.
"""
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from ipaddress import ip_network
from pathlib import Path

from chatsbom.server.clients import Network
from chatsbom.server.ratelimit import RateLimit

#: web/wrangler.jsonc's: every question is a paid model call.
CHAT_RATE_LIMIT = RateLimit(20, 60)
#: web/wrangler.jsonc's: a page view was about 25 queries.
QUERY_RATE_LIMIT = RateLimit(100, 10)
#: web/wrangler.jsonc's and compose's, in US dollars a UTC day.
DAILY_SPEND_CAP_USD = 5.0

#: Where web.sqlite is kept unless WEB_STATE_DIR says: `data`, where the
#: CLI keeps the collector's ledger, in the directory it runs in.
STATE_DIR = Path('data')

#: The fewest characters ALTCHA_HMAC_KEY may have. Anyone who has one
#: signed challenge can try keys against it offline, as fast as they can
#: compute HMACs; 32 random hex digits, 128 bits, are out of reach, and
#: `openssl rand -hex 32` makes 64.
MIN_KEY_LENGTH = 32

#: A limit as `LIMIT/PERIOD`: at most LIMIT requests in PERIOD seconds.
SHORTHAND = re.compile(r'\s*(\d+)\s*/\s*(\d+(?:\.\d+)?)\s*')


class SettingsError(ValueError):
    """A setting the service cannot start with: `setting` names it."""

    def __init__(self, setting: str, problem: str) -> None:
        super().__init__(problem)
        self.setting = setting


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


def rate_limit(setting: str, value: str | None, default: RateLimit) -> RateLimit:
    """`value`, the setting `setting`, as a limit: `20/60`, or the JSON
    object web/wrangler.jsonc writes, `{"limit": 20, "period": 60}`.

    Both, because `.env` is read by python-dotenv, compose and systemd,
    which agree on a plain value and not on one with quotes in it
    (`.env.example`), and because a value copied from wrangler.jsonc
    should mean what it meant there. Unset or empty is `default`.
    """
    if not value or not value.strip():
        return default
    try:
        if value.lstrip().startswith('{'):
            document = json.loads(value)
            if not isinstance(document, dict):
                raise ValueError('not an object')
            return RateLimit(document['limit'], document['period'])
        shorthand = SHORTHAND.fullmatch(value)
        if shorthand is None:
            raise ValueError('not LIMIT/PERIOD')
        return RateLimit(int(shorthand[1]), float(shorthand[2]))
    except (KeyError, TypeError, ValueError) as error:
        raise SettingsError(
            setting,
            f'{setting} is not at most LIMIT requests in PERIOD seconds, '
            f'as `20/60` or {{"limit": 20, "period": 60}}: {value!r} '
            f'({error})',
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


def settings_from(environ: Mapping[str, str], *, spa: Path) -> Settings:
    """The settings `environ` holds, for the page built in `spa`, or the
    first reason it cannot be served with them."""
    state = environ.get('WEB_STATE_DIR')
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
    )
