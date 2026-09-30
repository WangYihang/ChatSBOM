"""The research commands that take a token use it without its line
ending (#113), and refuse one that holds a control character, saying so
and never showing it.

`readme` and `classify` were `github readme` and `github classify`
until the research tools had a command of their own (#167). The core's
commands cleaned their tokens the same way until the collector replaced
them (#171): the cleaning is the research tools' own now
(`chatsbom/research/tokens.py`).
"""
import io
import json
from pathlib import Path

import pytest
import typer
from rich.console import Console
from typer.testing import CliRunner

from chatsbom.research.__main__ import app
from chatsbom.research.tokens import clean_github_token
from tests.github_token_test import HALF
from tests.github_token_test import TOKEN

runner = CliRunner()


#: The commands that take a token without needing one, and the module
#: each makes its `GitHubService` in.
OPTIONAL = {
    'readme': 'chatsbom.research.commands.readme',
    'classify': 'chatsbom.research.commands.classify',
}


@pytest.mark.parametrize('command', list(OPTIONAL))
def test_a_command_that_may_go_without_a_token_uses_it_without_its_line_ending(
    command: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The service is the first thing the token reaches; the command
    stops there, before anything is asked of anyone."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')
    # `classify`'s key, for OpenAI's API, which is never asked.
    monkeypatch.setenv('OPENAI_API_KEY', 'sk-test')
    listing = tmp_path / 'repos.jsonl'
    listing.write_text(
        json.dumps({'id': 1, 'owner': 'shop', 'name': 'app'}) + '\n',
    )
    made: list[str] = []

    class Service:
        def __init__(self, token: str, **_: object) -> None:
            made.append(token)
            raise typer.Exit(0)

    monkeypatch.setattr(f'{OPTIONAL[command]}.GitHubService', Service)

    result = runner.invoke(app, [*command.split(), '--input', str(listing)])

    assert result.exit_code == 0, result.output
    assert made == [TOKEN]


def words(text: str) -> str:
    """`text` as words: without a panel's borders, and with any run of
    whitespace as one space."""
    return ' '.join(text.replace('│', ' ').split())


@pytest.mark.parametrize(
    'given',
    [
        # From files saved on Windows and elsewhere, read whole.
        f'{TOKEN}\r\n', f'{TOKEN}\n', f'{TOKEN}\r',
        # Pasted with what was around it.
        f' {TOKEN} ', f'\t{TOKEN}',
    ],
    ids=repr,
)
def test_the_whitespace_around_a_token_is_left_out(given):
    assert clean_github_token(given, console=Console(quiet=True)) == TOKEN


def test_whitespace_alone_is_no_token():
    """What `GITHUB_TOKEN=` with a carriage return after it gives."""
    assert clean_github_token('\r\n', console=Console(quiet=True)) is None
    assert clean_github_token(None) is None


#: Control characters a token may still hold, inside it, where leaving
#: out what surrounds it does not reach: C0's, DEL and C1's.
CONTROL = ['\r', '\n', '\x00', '\x1b', '\x7f', '\x85']


@pytest.mark.parametrize('character', CONTROL, ids=repr)
def test_a_token_holding_a_control_character_is_refused(character):
    """No GitHub token holds one. Sent, a carriage return or a newline
    is refused by requests, with the header quoted in its error, and any
    other by GitHub, or by a proxy on the way."""
    console = Console(file=io.StringIO(), record=True, width=400)

    with pytest.raises(typer.Exit) as stopped:
        clean_github_token(f'{HALF}{character}{HALF}', console=console)

    assert stopped.value.exit_code == 1
    said = words(console.export_text())
    assert 'GitHub Token Malformed' in said
    assert f'U+{ord(character):04X}' in said
    # Said to be there, never shown.
    assert HALF not in said
