"""What a model turn costs, and what it may cost, by the hour (#140).

The chat runs on DeepSeek's `deepseek-flash` (#128, the owner's
decision on Q8), whose tokens are priced by kind, a prompt token served
from the context cache far below one that is not, and by the hour: off
peak is half the peak rate. The spend cap holds each turn's worst case
before it is made and settles it at what it cost after (#33), so both
are here: what a turn reported costs, at the hours it ran, and the most
a turn can cost, at the dearest rate that can apply to it.

The defaults are DeepSeek's pricing page as read on 2026-09-29
(https://api-docs.deepseek.com/quick_start/pricing/); the settings can
change each (`settings`).
"""
import math
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.server.pricing import MILLION
from chatsbom.server.pricing import OFF_PEAK
from chatsbom.server.pricing import PEAK
from chatsbom.server.pricing import PEAK_HOURS
from chatsbom.server.pricing import Prices
from chatsbom.server.pricing import Rates
from chatsbom.server.pricing import Usage
from chatsbom.server.pricing import Window

UTC = timezone.utc

#: A Tuesday, and the Saturday after it.
TUESDAY = datetime(2026, 9, 29, tzinfo=UTC)
SATURDAY = datetime(2026, 10, 3, tzinfo=UTC)


def at(day: datetime, hours: int, minutes: int = 0, seconds: int = 0) -> datetime:
    return day + timedelta(hours=hours, minutes=minutes, seconds=seconds)


PRICES = Prices()


class TestDeepSeeksPrices:
    """As its pricing page says them, read on 2026-09-29, for
    `deepseek-flash`, per million tokens."""

    def test_at_peak(self):
        assert PEAK == Rates(input=0.30, cached_input=0.006, output=1.20)

    def test_off_peak_at_half(self):
        assert OFF_PEAK == Rates(input=0.15, cached_input=0.003, output=0.60)

    def test_peak_hours_are_01_to_04_and_06_to_10_utc(self):
        assert PEAK_HOURS == (Window(60, 240), Window(360, 600))

    def test_are_the_defaults(self):
        assert Prices() == Prices(
            peak=PEAK, off_peak=OFF_PEAK, peak_hours=PEAK_HOURS,
        )


class TestTheHours:
    """Peak is 01:00-04:00 and 06:00-10:00 UTC, Monday to Friday, and
    every other hour off peak, the weekend whole."""

    @pytest.mark.parametrize(
        'moment,peak',
        [
            ((0, 59, 59), False),
            ((1, 0, 0), True),
            ((3, 59, 59), True),
            ((4, 0, 0), False),
            ((5, 59, 59), False),
            ((6, 0, 0), True),
            ((9, 59, 59), True),
            ((10, 0, 0), False),
            ((23, 59, 59), False),
        ],
    )
    def test_on_a_weekday(self, moment, peak):
        instant = at(TUESDAY, *moment)
        assert PRICES.peak_during(instant, instant) is peak

    @pytest.mark.parametrize('day', [SATURDAY, SATURDAY + timedelta(days=1)])
    @pytest.mark.parametrize('hour', [2, 7])
    def test_never_at_the_weekend(self, day, hour):
        assert PRICES.peak_during(at(day, hour), at(day, hour)) is False

    def test_are_utcs_whatever_zone_a_time_is_given_in(self):
        beijing = timezone(timedelta(hours=8))
        # 09:30 in Beijing is 01:30 UTC.
        instant = datetime(2026, 9, 29, 9, 30, tzinfo=beijing)
        assert PRICES.peak_during(instant, instant) is True

    def test_refuse_a_time_with_no_zone(self):
        """It could name any hour."""
        naive = datetime(2026, 9, 29, 2, 0)
        with pytest.raises(ValueError, match='zone'):
            PRICES.peak_during(naive, naive)

    @pytest.mark.parametrize(
        'start,end',
        [
            # Into peak, out of it, and across a whole off-peak gap.
            (at(TUESDAY, 0, 59, 30), at(TUESDAY, 1, 0, 30)),
            (at(TUESDAY, 3, 59, 30), at(TUESDAY, 4, 0, 30)),
            (at(TUESDAY, 3, 59), at(TUESDAY, 6, 1)),
            # Sunday night into Monday's first peak second.
            (at(SATURDAY, 47, 59), at(SATURDAY, 49)),
        ],
    )
    def test_a_call_that_touches_them_ran_at_peak(self, start, end):
        """DeepSeek does not say whether a call is priced at its start
        or its end, so one that touched peak hours is taken to have run
        in them: the dearer reading, which the cap can afford to be
        wrong by."""
        assert PRICES.peak_during(start, end) is True
        assert PRICES.rates(start, end) == PEAK

    @pytest.mark.parametrize(
        'start,end',
        [
            (at(TUESDAY, 4), at(TUESDAY, 5, 59, 59)),
            (at(TUESDAY, 10), at(TUESDAY, 23)),
            # Friday night into Saturday.
            (at(SATURDAY, -1, 1), at(SATURDAY, 0, 1)),
        ],
    )
    def test_a_call_wholly_outside_them_ran_off_peak(self, start, end):
        assert PRICES.peak_during(start, end) is False
        assert PRICES.rates(start, end) == OFF_PEAK

    def test_are_whatever_the_setting_says(self):
        evenings = Prices(peak_hours=(Window(18 * 60, 24 * 60),))
        assert evenings.peak_during(at(TUESDAY, 23, 59), at(TUESDAY, 23, 59))
        assert not evenings.peak_during(at(TUESDAY, 2), at(TUESDAY, 2))


class TestAWindow:
    @pytest.mark.parametrize('start,end', [(-1, 60), (60, 60), (120, 60), (0, 1441)])
    def test_is_a_span_within_a_day(self, start, end):
        with pytest.raises(ValueError):
            Window(start, end)

    def test_may_run_to_midnight(self):
        assert Window(0, 24 * 60).end == 1440


class TestRates:
    @pytest.mark.parametrize('bad', [-0.01, math.nan, math.inf])
    def test_are_numbers_of_dollars(self, bad):
        with pytest.raises(ValueError):
            Rates(input=bad, cached_input=0, output=0)
        with pytest.raises(ValueError):
            Rates(input=0, cached_input=0, output=bad)


class TestWhatATurnCost:
    """What the stream reported, at the rates of the hours it ran."""

    def test_prices_each_kind_of_token_at_its_own_rate(self):
        usage = Usage(cache_hit=MILLION, cache_miss=MILLION, output=MILLION)
        assert PRICES.cost(usage, at(TUESDAY, 2), at(TUESDAY, 2)) == (
            pytest.approx(0.006 + 0.30 + 1.20)
        )

    def test_a_cache_hit_at_a_fiftieth_of_a_miss(self):
        hit = Usage(cache_hit=MILLION, cache_miss=0, output=0)
        miss = Usage(cache_hit=0, cache_miss=MILLION, output=0)
        start = at(TUESDAY, 2)
        assert PRICES.cost(hit, start, start) == pytest.approx(0.006)
        assert PRICES.cost(miss, start, start) == pytest.approx(0.30)

    def test_off_peak_at_half(self):
        usage = Usage(cache_hit=800, cache_miss=200, output=100)
        peak = PRICES.cost(usage, at(TUESDAY, 2), at(TUESDAY, 2, 1))
        off = PRICES.cost(usage, at(SATURDAY, 2), at(SATURDAY, 2, 1))
        assert peak == pytest.approx(
            (800 * 0.006 + 200 * 0.30 + 100 * 1.20) / MILLION,
        )
        assert off == pytest.approx(peak / 2)

    def test_reasoning_is_output_and_is_not_counted_twice(self):
        """DeepSeek counts reasoning tokens among the completion's."""
        usage = Usage(cache_hit=0, cache_miss=0, output=100, reasoning=60)
        start = at(TUESDAY, 2)
        assert PRICES.cost(usage, start, start) == pytest.approx(
            100 * 1.20 / MILLION,
        )


class TestWhatATurnMayCost:
    """What is held against the cap before the call: every token of
    input at the dearest input rate, and the whole output limit at the
    output rate, at the dearest hour the call can run in."""

    def test_is_at_least_anything_the_turn_can_cost(self):
        start, end = at(TUESDAY, 2), at(TUESDAY, 2, 5)
        worst = PRICES.worst_case(10_000, 4_096, start, end)
        for hits in (0, 5_000, 10_000):
            usage = Usage(
                cache_hit=hits, cache_miss=10_000 - hits, output=4_096,
            )
            assert worst >= PRICES.cost(usage, start, end)
        assert worst == pytest.approx((10_000 * 0.30 + 4_096 * 1.20) / MILLION)

    def test_is_off_peak_only_when_the_whole_call_must_be(self):
        off = PRICES.worst_case(
            1_000, 1_000, at(
                TUESDAY, 4,
            ), at(TUESDAY, 4, 5),
        )
        edge = PRICES.worst_case(
            1_000, 1_000, at(TUESDAY, 5, 58), at(TUESDAY, 6, 3),
        )
        assert off == pytest.approx((1_000 * 0.15 + 1_000 * 0.60) / MILLION)
        assert edge == pytest.approx(off * 2)

    def test_takes_the_dearest_of_each_whatever_the_setting(self):
        """A setting that made off peak dearer, or a cache hit dearer
        than a miss, is still covered: each kind of token at the most
        it can cost."""
        odd = Prices(
            peak=Rates(input=1.0, cached_input=3.0, output=1.0),
            off_peak=Rates(input=2.0, cached_input=0.0, output=5.0),
        )
        start, end = at(TUESDAY, 3, 58), at(TUESDAY, 4, 3)
        assert odd.worst_case(MILLION, MILLION, start, end) == pytest.approx(
            3.0 + 5.0,
        )


class TestTheUsageReported:
    """What the stream's last chunk says, read into the counts priced.

    DeepSeek's own fields first, `prompt_cache_hit_tokens` and
    `prompt_cache_miss_tokens` (the Chat Completions reference, read on
    2026-09-29); OpenAI's `prompt_tokens_details.cached_tokens`, which
    DeepSeek sends too, when they are missing; and, with neither, every
    prompt token as a miss, the dearer.
    """

    def test_reads_deepseeks_fields(self):
        assert Usage.reported({
            'prompt_tokens': 1_000,
            'completion_tokens': 100,
            'total_tokens': 1_100,
            'prompt_tokens_details': {'cached_tokens': 800},
            'prompt_cache_hit_tokens': 800,
            'prompt_cache_miss_tokens': 200,
            'completion_tokens_details': {'reasoning_tokens': 40},
        }) == Usage(cache_hit=800, cache_miss=200, output=100, reasoning=40)

    def test_falls_back_on_openais(self):
        assert Usage.reported({
            'prompt_tokens': 1_000,
            'completion_tokens': 50,
            'prompt_tokens_details': {'cached_tokens': 300},
            'completion_tokens_details': None,
        }) == Usage(cache_hit=300, cache_miss=700, output=50)

    def test_counts_every_prompt_token_a_miss_when_none_is_said_to_hit(self):
        assert Usage.reported({
            'prompt_tokens': 1_000, 'completion_tokens': 50,
        }) == Usage(cache_hit=0, cache_miss=1_000, output=50)

    def test_counts_prompt_tokens_neither_field_names_as_misses(self):
        """Never fewer than the prompt: a count that falls short of it
        would price tokens that were billed as though they were not."""
        assert Usage.reported({
            'prompt_tokens': 1_000,
            'completion_tokens': 50,
            'prompt_cache_hit_tokens': 600,
            'prompt_cache_miss_tokens': 100,
        }) == Usage(cache_hit=600, cache_miss=400, output=50)

    @pytest.mark.parametrize(
        'reported',
        [
            None,
            {},
            {'prompt_tokens': 1_000},
            {'prompt_tokens': -1, 'completion_tokens': 5},
            {'prompt_tokens': '1000', 'completion_tokens': 5},
            {'prompt_tokens': True, 'completion_tokens': 5},
            {
                'prompt_tokens': 10, 'completion_tokens': 5,
                'prompt_cache_hit_tokens': 20,
            },
            [1, 2],
        ],
    )
    def test_is_unreadable_rather_than_guessed(self, reported):
        """An unreadable cost keeps the worst case held (`spend`)."""
        assert Usage.reported(reported) is None

    def test_adds_up_across_turns(self):
        first = Usage(cache_hit=1, cache_miss=2, output=3, reasoning=1)
        second = Usage(cache_hit=10, cache_miss=20, output=30, reasoning=10)
        assert first + second == Usage(
            cache_hit=11, cache_miss=22, output=33, reasoning=11,
        )
        assert (first + second).prompt == 33
