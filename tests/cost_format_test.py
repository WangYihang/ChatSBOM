"""Cost display: USD always, a converted figure only when configured."""
import importlib

import pytest


def reloaded(monkeypatch, **env):
    for key in ('CHATSBOM_COST_RATE', 'CHATSBOM_COST_SYMBOL'):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import chatsbom.commands.chat as chat
    return importlib.reload(chat)


def test_usd_only_by_default(monkeypatch):
    chat = reloaded(monkeypatch)
    assert chat.format_cost(1.2345) == '$1.2345'


def test_converted_figure_when_a_rate_is_configured(monkeypatch):
    chat = reloaded(
        monkeypatch, CHATSBOM_COST_RATE='7.2', CHATSBOM_COST_SYMBOL='¥',
    )
    assert chat.format_cost(1.0) == '$1.0000 / ¥7.2000'


@pytest.mark.parametrize('rate', ['0', '', 'not-a-number'])
def test_unusable_rates_fall_back_to_usd(monkeypatch, rate):
    try:
        chat = reloaded(monkeypatch, CHATSBOM_COST_RATE=rate)
    except ValueError:
        pytest.fail('a bad rate must not crash the module import')
    assert chat.format_cost(2.0) == '$2.0000'
