"""The collector's settings (#156): its GitHub tokens, and what it leaves
of each bucket for manual work. Read from the environment, which `.env`
fills, before it asks GitHub anything; a value it cannot use stops it,
naming the setting (`SettingsError`).

The tokens are GITHUB_TOKEN, `token 1`, and then CHATSBOM_GITHUB_TOKENS,
`token 2` on, comma- or whitespace-separated, each once. Every token
serves every bucket: which one a request takes is the budget manager's
choice, by what each has left, and there is no split between them.
GitHub meters an account, not a token, so a token adds to the budget
only when it is another account's.

CHATSBOM_DEPGRAPH_TOKENS, which the old pipeline's dependency-graph
stage read beside GITHUB_TOKEN, is not read here: it named tokens for
one bucket, and the collector's serve them all. It folded into
CHATSBOM_GITHUB_TOKENS when the collector replaced the old pipeline
(#171).

A token is cleaned as #117 cleans one: the whitespace around it is left
out, and one that still holds a character no GitHub token holds is
refused, by the character and where it is, never shown. A request would
have refused it anyway, quoting the header it was in (#113).

CHATSBOM_GITHUB_RESERVE names, per bucket, how much of each token's
bucket the collector leaves: `core=500,search=5`. A bucket it names is
set, the rest keep `DEFAULT_RESERVE`, and one neither names keeps
nothing. What is left is for manual work, a command run by hand with the
same tokens beside the collector.

CHATSBOM_SWEEP_INTERVAL and CHATSBOM_UNIVERSE_INTERVAL say how often the
sweep asks after every repository of the universe, and how often the
universe is searched again (#160): a whole number and a unit, `s`, `m`,
`h`, `d` or `w`, as `90m`, `1h` or `7d`.

CHATSBOM_REPOSITORIES_AT_ONCE and CHATSBOM_INDEX_INTERVAL are the
process's, `chatsbom collect` (#171): how many repositories it collects
at once, each by one task, and how often at most its index pass runs,
said as the intervals are.
"""
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

from chatsbom.collector.tokens import Token

#: A tenth of the REST API's and of GraphQL's hour, and a sixth of
#: search's minute.
DEFAULT_RESERVE: Mapping[str, int] = {'core': 500, 'graphql': 500, 'search': 5}

#: Hourly, about 650 of a token's 5,000 GraphQL points for 65,000
#: repositories (#128).
DEFAULT_SWEEP_INTERVAL = timedelta(hours=1)

#: Weekly, about 700 search requests and 25 minutes (#128).
DEFAULT_UNIVERSE_INTERVAL = timedelta(days=7)

#: Repositories collected at once: #128's four network tasks in flight a
#: token, and a repository's stages one after another, three or four of
#: them paced by one token (DEPLOY.md).
DEFAULT_AT_ONCE = 4

#: The index pass at most daily: the warehouse is built of the whole
#: store, minutes of I/O, as the daily pass of the loop before it did.
DEFAULT_INDEX_INTERVAL = timedelta(days=1)

#: An interval: a whole number and its unit.
_INTERVAL = re.compile(r'(\d+)([smhdw])')

#: Each unit, in seconds.
_UNITS: Mapping[str, int] = {
    's': 1, 'm': 60, 'h': 3_600, 'd': 86_400, 'w': 604_800,
}

#: A bucket, as GitHub's `X-RateLimit-Resource` names one.
_BUCKET = re.compile(r'[a-z][a-z0-9_]*')

#: Between the tokens of a list.
_SEPARATORS = re.compile(r'[\s,]+')


class SettingsError(ValueError):
    """A setting the collector cannot start with: `setting` names it."""

    def __init__(self, setting: str, problem: str) -> None:
        super().__init__(problem)
        self.setting = setting


@dataclass(frozen=True)
class CollectorSettings:
    """What the collector is configured with."""

    #: Every token, in order, each once.
    tokens: tuple[Token, ...]
    #: What each bucket keeps of every token's, by bucket.
    reserve: Mapping[str, int]
    #: How often the sweep asks after the universe.
    sweep_interval: timedelta = DEFAULT_SWEEP_INTERVAL
    #: How often the universe is searched again.
    universe_interval: timedelta = DEFAULT_UNIVERSE_INTERVAL
    #: Repositories `chatsbom collect` collects at once.
    at_once: int = DEFAULT_AT_ONCE
    #: How often at most its index pass runs.
    index_interval: timedelta = DEFAULT_INDEX_INTERVAL


def _clean(value: str, setting: str, named: str) -> str:
    """`value`, without the whitespace around it, or refused."""
    token = value.strip()
    for position, character in enumerate(token, start=1):
        if not '!' <= character <= '~':
            raise SettingsError(
                setting,
                f'U+{ord(character):04X}, at character {position} of '
                f'{named}, is a character no GitHub token holds. The '
                'whitespace around a token is left out; this is inside it. '
                'Copy the token again, and set it.',
            )
    return token


def tokens(environ: Mapping[str, str]) -> tuple[Token, ...]:
    """GITHUB_TOKEN, then CHATSBOM_GITHUB_TOKENS: each token once, named
    by its place."""
    found = []
    primary = _clean(
        environ.get('GITHUB_TOKEN') or '', 'GITHUB_TOKEN', 'GITHUB_TOKEN',
    )
    if primary:
        found.append(primary)
    listed = _SEPARATORS.split(environ.get('CHATSBOM_GITHUB_TOKENS') or '')
    for position, value in enumerate(filter(None, listed), start=1):
        token = _clean(
            value, 'CHATSBOM_GITHUB_TOKENS',
            f'token {position} of CHATSBOM_GITHUB_TOKENS',
        )
        if token not in found:
            found.append(token)
    if not found:
        raise SettingsError(
            'GITHUB_TOKEN',
            'No GitHub token: set GITHUB_TOKEN, and any more in '
            'CHATSBOM_GITHUB_TOKENS. A fine-grained token with read access '
            'to public repositories is enough: '
            'https://github.com/settings/personal-access-tokens',
        )
    return tuple(
        Token(f'token {number}', secret)
        for number, secret in enumerate(found, start=1)
    )


def reserve(value: str | None) -> dict[str, int]:
    """CHATSBOM_GITHUB_RESERVE over `DEFAULT_RESERVE`."""
    kept = dict(DEFAULT_RESERVE)
    for pair in (value or '').split(','):
        if not pair.strip():
            continue
        bucket, equals, count = (part.strip() for part in pair.partition('='))
        if not equals or not _BUCKET.fullmatch(bucket) or not count.isdigit():
            raise SettingsError(
                'CHATSBOM_GITHUB_RESERVE',
                'CHATSBOM_GITHUB_RESERVE is not buckets and what each keeps, '
                f'as core=500,search=5: {value!r}',
            )
        kept[bucket] = int(count)
    return kept


def interval(setting: str, value: str | None, default: timedelta) -> timedelta:
    """`setting`, as `value` says it, or `default` where it says none."""
    if value is None or not value.strip():
        return default
    match = _INTERVAL.fullmatch(value.strip().lower())
    if match is None or int(match[1]) == 0:
        raise SettingsError(
            setting,
            f'{setting} is not an interval, a whole number and a unit, as '
            f'90m, 1h or 7d: {value!r}',
        )
    return timedelta(seconds=int(match[1]) * _UNITS[match[2]])


def at_once(value: str | None) -> int:
    """CHATSBOM_REPOSITORIES_AT_ONCE: a whole number, 1 or more."""
    if value is None or not value.strip():
        return DEFAULT_AT_ONCE
    said = value.strip()
    if not said.isdigit() or int(said) < 1:
        raise SettingsError(
            'CHATSBOM_REPOSITORIES_AT_ONCE',
            'CHATSBOM_REPOSITORIES_AT_ONCE is how many repositories are '
            f'collected at once, 1 or more: {value!r}',
        )
    return int(said)


def settings_from(
    environ: Mapping[str, str] | None = None,
) -> CollectorSettings:
    """The collector's settings, from `environ`: the process's
    environment unless given."""
    if environ is None:
        environ = os.environ
    return CollectorSettings(
        tokens=tokens(environ),
        reserve=reserve(environ.get('CHATSBOM_GITHUB_RESERVE')),
        sweep_interval=interval(
            'CHATSBOM_SWEEP_INTERVAL', environ.get('CHATSBOM_SWEEP_INTERVAL'),
            DEFAULT_SWEEP_INTERVAL,
        ),
        universe_interval=interval(
            'CHATSBOM_UNIVERSE_INTERVAL',
            environ.get('CHATSBOM_UNIVERSE_INTERVAL'),
            DEFAULT_UNIVERSE_INTERVAL,
        ),
        at_once=at_once(environ.get('CHATSBOM_REPOSITORIES_AT_ONCE')),
        index_interval=interval(
            'CHATSBOM_INDEX_INTERVAL', environ.get('CHATSBOM_INDEX_INTERVAL'),
            DEFAULT_INDEX_INTERVAL,
        ),
    )
