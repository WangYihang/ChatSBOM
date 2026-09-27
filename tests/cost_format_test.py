"""Cost display: USD always, a converted figure only when configured."""
import pytest

from chatsbom.commands import chat


def configured(monkeypatch, **env):
    """`chat`, with the display currency set as given.

    Not reloaded: the rate is read each time a cost is shown. It has to
    be, because `.env` is loaded by the root callback, after every module
    is imported — a rate read at import would never see it.
    """
    for key in ('CHATSBOM_COST_RATE', 'CHATSBOM_COST_SYMBOL'):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return chat


def test_usd_only_by_default(monkeypatch):
    chat = configured(monkeypatch)
    assert chat.format_cost(1.2345) == '$1.2345'


def test_converted_figure_when_a_rate_is_configured(monkeypatch):
    chat = configured(
        monkeypatch, CHATSBOM_COST_RATE='7.2', CHATSBOM_COST_SYMBOL='¥',
    )
    assert chat.format_cost(1.0) == '$1.0000 / ¥7.2000'


@pytest.mark.parametrize('rate', ['0', '', 'not-a-number'])
def test_unusable_rates_fall_back_to_usd(monkeypatch, rate):
    chat = configured(monkeypatch, CHATSBOM_COST_RATE=rate)
    try:
        shown = chat.format_cost(2.0)
    except ValueError:
        pytest.fail('a bad rate must not stop the cost from being shown')
    assert shown == '$2.0000'
