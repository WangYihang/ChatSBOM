"""The research commands that take a token use it without its line
ending (#113), as the core's do (tests/github_token_test.py).

`readme` and `classify` were `github readme` and `github classify`
until the research tools had a command of their own (#167).
"""
import json
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from chatsbom.research.__main__ import app
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
