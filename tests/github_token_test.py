"""A token's presence is not its validity.

Nor is a token all that GITHUB_TOKEN holds. One read from a file ends
with the file's line ending when what read it kept it: `$(cat token)`
of a file saved on Windows keeps a carriage return, and a secret file
read whole ends with a newline. requests refused the header then, with
an `InvalidHeader` that quoted it, token and all, and the log printed
that (#113).

The old pipeline's commands checked their tokens with GitHub before
anything else; they went, and their check with them (#171). The
collector cleans its own tokens (collector_settings_test), and the
research tools theirs (tests/research/github_token_test.py).
"""
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

from chatsbom.core.config import GitHubConfig

#: A token of the shape GitHub gives one, and half of one: whatever
#: holds a token cut in two is looked for by either half.
HALF = 'ghp_5ec7e7' + '5ec7e7' * 2
TOKEN = HALF + 'a1b2c3d4e5f6a1b2c3'


def test_the_configured_token_is_read_without_the_whitespace_around_it(
    monkeypatch,
):
    """`GitHubConfig`'s, which the research tools fall back on when they
    are given no token."""
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')

    assert GitHubConfig().token == TOKEN


# --- the script that reads it itself --------------------------------------

def probe_language(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """`scripts/probe_language.py`, loaded as a module, as it is run for
    C++; and each session it makes, a failure. It reads GITHUB_TOKEN
    itself, and sends it as `Authorization: token ...`."""
    path = Path(__file__).resolve().parent.parent / 'scripts'
    spec = importlib.util.spec_from_file_location(
        'probe_language', path / 'probe_language.py',
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Where `dataclass` looks its annotations up, as it makes `Probed`.
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(sys, 'argv', ['probe_language.py', 'C++'])
    return module


def test_the_language_probe_uses_the_token_without_its_line_ending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    probe = probe_language(monkeypatch)
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')
    made: list[str] = []

    def session(token: str) -> None:
        made.append(token)
        raise SystemExit(0)

    monkeypatch.setattr(probe, 'session', session)

    with pytest.raises(SystemExit):
        probe.main()

    assert made == [TOKEN]


def test_the_language_probe_refuses_a_token_holding_a_control_character(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """Saying so, and never the token."""
    probe = probe_language(monkeypatch)
    monkeypatch.setenv('GITHUB_TOKEN', f'{HALF}\x1b{HALF}')

    def session(token: str) -> None:
        raise AssertionError('the token was sent')

    monkeypatch.setattr(probe, 'session', session)

    assert probe.main() == 2
    said = capsys.readouterr().err
    assert 'control character' in said
    assert HALF not in said
