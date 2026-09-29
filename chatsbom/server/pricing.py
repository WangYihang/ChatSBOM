"""What the chat's model turns cost, and the most they can (#140).

The chat runs on DeepSeek's `deepseek-flash` (#128, the owner's decision
on Q8). DeepSeek prices a million tokens of each kind, and by the hour:

  - a prompt token served from its context cache, far below one that is
    not, and a generated token, reasoning included, above both;
  - peak hours, 01:00-04:00 and 06:00-10:00 UTC Monday to Friday, at
    twice the rate of every other hour, the weekend whole.

The defaults here are its pricing page as read on 2026-09-29
(https://api-docs.deepseek.com/quick_start/pricing/), and each is a
setting (`settings`), so that a change of price is a change of `.env`.

The spend cap needs two figures of each turn (`spend`):

  - `worst_case`, held before the call: every token of input at the
    dearest input rate, and the whole output limit at the output rate,
    at the dearest hour the call can run in. A bound, not a guess: a
    hold the call could exceed would make the cap one too.
  - `cost`, settled after it: the tokens the stream reported, each kind
    at its rate, at the hours the call ran.

Which moment of a call DeepSeek prices it by, its start or its end, the
page does not say. A call that touched peak hours is settled at peak:
the dearer reading, which the cap can afford to be wrong by. Nor are
the Chinese public holidays the page counts as off peak known here: a
weekday's peak hours on one are settled at peak too.
"""
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone

#: Rates are per million tokens.
MILLION = 1_000_000

#: The minutes of a day.
DAY_MINUTES = 24 * 60


@dataclass(frozen=True)
class Rates:
    """US dollars per million tokens of each kind."""

    #: A prompt token that missed the context cache.
    input: float
    #: One the cache served.
    cached_input: float
    #: A generated token, reasoning included.
    output: float

    def __post_init__(self) -> None:
        for name in ('input', 'cached_input', 'output'):
            rate = getattr(self, name)
            # Written so that NaN, which compares false, is refused.
            if not (math.isfinite(rate) and rate >= 0):
                raise ValueError(
                    f'{name} is not a number of dollars: {rate!r}',
                )

    def dearest(self, other: 'Rates') -> 'Rates':
        """Each kind of token at the dearer of the two."""
        return Rates(
            input=max(self.input, other.input),
            cached_input=max(self.cached_input, other.cached_input),
            output=max(self.output, other.output),
        )


#: `deepseek-flash`'s rates in peak hours, as the pricing page said them
#: on 2026-09-29: DeepSeek-V4.1-Flash, priced so from 2026-09-10.
PEAK = Rates(input=0.30, cached_input=0.006, output=1.20)
#: And in every other hour: half.
OFF_PEAK = Rates(input=0.15, cached_input=0.003, output=0.60)


@dataclass(frozen=True)
class Window:
    """Peak hours on a weekday, in UTC: from `start` minutes after
    midnight, and until `end`, which is not in them."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if not 0 <= self.start < self.end <= DAY_MINUTES:
            raise ValueError(
                f'not a span of hours within a day: {self.start} to '
                f'{self.end} minutes',
            )


#: 01:00-04:00 and 06:00-10:00 UTC, Monday to Friday.
PEAK_HOURS = (Window(60, 240), Window(360, 600))


def _utc(moment: datetime) -> datetime:
    """`moment` in UTC. A time with no zone could be any hour."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f'a time with no zone names no hour: {moment}')
    return moment.astimezone(timezone.utc)


@dataclass(frozen=True)
class Usage:
    """The tokens a turn used, as its stream reported them."""

    #: Prompt tokens the context cache served.
    cache_hit: int
    #: Prompt tokens it did not.
    cache_miss: int
    #: Generated tokens, reasoning included.
    output: int
    #: How many of those were reasoning: shown, never priced again.
    reasoning: int = 0

    @property
    def prompt(self) -> int:
        return self.cache_hit + self.cache_miss

    def __add__(self, other: 'Usage') -> 'Usage':
        return Usage(
            cache_hit=self.cache_hit + other.cache_hit,
            cache_miss=self.cache_miss + other.cache_miss,
            output=self.output + other.output,
            reasoning=self.reasoning + other.reasoning,
        )

    @classmethod
    def reported(cls, usage: object) -> 'Usage | None':
        """The usage a stream reported, or None if it cannot be read,
        which keeps the turn's worst case held rather than guess.

        DeepSeek's `prompt_cache_hit_tokens` and
        `prompt_cache_miss_tokens` first, OpenAI's
        `prompt_tokens_details.cached_tokens`, which DeepSeek sends too,
        when they are missing, and every prompt token a miss, the
        dearer, with neither. A prompt token neither field names is a
        miss: never fewer than the prompt is priced.
        """
        if not isinstance(usage, Mapping):
            return None
        prompt = _count(usage.get('prompt_tokens'))
        output = _count(usage.get('completion_tokens'))
        if prompt is None or output is None:
            return None
        hits = _count(usage.get('prompt_cache_hit_tokens'))
        if hits is None:
            hits = _count(
                _field(usage, 'prompt_tokens_details', 'cached_tokens'),
            )
        hits = hits or 0
        if hits > prompt:
            return None
        misses = max(
            prompt - hits, _count(usage.get('prompt_cache_miss_tokens')) or 0,
        )
        reasoning = _count(
            _field(usage, 'completion_tokens_details', 'reasoning_tokens'),
        )
        return cls(
            cache_hit=hits, cache_miss=misses, output=output,
            reasoning=reasoning or 0,
        )


def _field(usage: Mapping[str, object], within: str, name: str) -> object:
    """`usage[within][name]`, or None if there is none."""
    details = usage.get(within)
    return details.get(name) if isinstance(details, Mapping) else None


def _count(value: object) -> int | None:
    """A count of tokens, or None: `true` is not one."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


@dataclass(frozen=True)
class Prices:
    """The rates at each hour, and the hours that are peak."""

    peak: Rates = PEAK
    off_peak: Rates = OFF_PEAK
    peak_hours: tuple[Window, ...] = PEAK_HOURS

    def peak_during(self, start: datetime, end: datetime) -> bool:
        """Whether any moment from `start` to `end`, both included, is
        in peak hours: those of a weekday, in UTC."""
        start, end = _utc(start), _utc(end)
        day = start.replace(hour=0, minute=0, second=0, microsecond=0)
        while day <= end:
            # Monday is 0 and Friday 4.
            if day.weekday() < 5:
                for window in self.peak_hours:
                    opens = day + timedelta(minutes=window.start)
                    closes = day + timedelta(minutes=window.end)
                    if start < closes and opens <= end:
                        return True
            day += timedelta(days=1)
        return False

    def rates(self, start: datetime, end: datetime) -> Rates:
        """What a call that ran from `start` to `end` is settled at:
        peak if any of it was."""
        return self.peak if self.peak_during(start, end) else self.off_peak

    def worst_rates(self, start: datetime, end: datetime) -> Rates:
        """The most each kind of token can cost in a call that may run
        from `start` to `end`: whichever rates are dearer, where peak
        can apply, and whatever the settings make them."""
        if self.peak_during(start, end):
            return self.peak.dearest(self.off_peak)
        return self.off_peak

    def cost(self, usage: Usage, start: datetime, end: datetime) -> float:
        """What a turn that used `usage`, from `start` to `end`, cost."""
        rates = self.rates(start, end)
        return (
            usage.cache_hit * rates.cached_input
            + usage.cache_miss * rates.input
            + usage.output * rates.output
        ) / MILLION

    def worst_case(
        self, input_tokens: int, output_tokens: int,
        start: datetime, end: datetime,
    ) -> float:
        """The most a turn of at most `input_tokens` in, and
        `output_tokens` out, can cost, run between `start` and `end`."""
        rates = self.worst_rates(start, end)
        dearest_input = max(rates.input, rates.cached_input)
        return (
            input_tokens * dearest_input + output_tokens * rates.output
        ) / MILLION
