"""A token's presence is not its validity.

Nor is a token all that GITHUB_TOKEN holds. One read from a file ends
with the file's line ending when what read it kept it: `$(cat token)`
of a file saved on Windows keeps a carriage return, and a secret file
read whole ends with a newline. requests refused the header then, with
an `InvalidHeader` that quoted it, token and all, and the log printed
that (#113).
"""
import importlib.util
import io
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
import requests
import typer
from rich.console import Console
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import GitHubConfig
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token

runner = CliRunner()


class FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


def test_missing_token_is_rejected():
    with pytest.raises(typer.Exit):
        check_github_token(None, console=Console(quiet=True))


def test_present_token_is_returned():
    assert check_github_token('tok', console=Console(quiet=True)) == 'tok'


def test_valid_token_reports_the_login():
    login = verify_github_token(
        'tok',
        fetch=lambda t: FakeResponse(200, {'login': 'octocat'}),
        console=Console(quiet=True),
    )
    assert login == 'octocat'


def test_expired_token_exits_with_a_specific_message():
    # quiet=True suppresses recording, so capture into a buffer instead.
    console = Console(file=io.StringIO(), record=True, width=100)
    with pytest.raises(typer.Exit):
        verify_github_token(
            'tok',
            fetch=lambda t: FakeResponse(401),
            console=console,
        )
    assert 'expired' in console.export_text().lower()


def test_insufficient_scope_exits():
    with pytest.raises(typer.Exit):
        verify_github_token(
            'tok',
            fetch=lambda t: FakeResponse(403),
            console=Console(quiet=True),
        )


def test_network_failure_does_not_block_the_run():
    """An unreachable API is not proof the token is bad."""
    def boom(token: str):
        raise requests.RequestException('no route to host')

    assert verify_github_token(
        'tok', fetch=boom, console=Console(quiet=True),
    ) is None


# --- what is read as the token -------------------------------------------

#: A token of the shape GitHub gives one, and half of one: whatever
#: holds a token cut in two is looked for by either half.
HALF = 'ghp_5ec7e7' + '5ec7e7' * 2
TOKEN = HALF + 'a1b2c3d4e5f6a1b2c3'


def recording() -> Console:
    """A console that keeps what is printed, on lines long enough that
    a message is not wrapped."""
    return Console(file=io.StringIO(), record=True, width=400)


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
    assert check_github_token(given, console=Console(quiet=True)) == TOKEN


def test_whitespace_alone_is_no_token():
    """What `GITHUB_TOKEN=` with a carriage return after it gives: the
    token is missing, and is said to be."""
    console = recording()

    with pytest.raises(typer.Exit):
        check_github_token('\r\n', console=console)

    assert 'GitHub Token Missing' in words(console.export_text())


#: Control characters a token may still hold, inside it, where leaving
#: out what surrounds it does not reach: C0's, DEL and C1's.
CONTROL = ['\r', '\n', '\x00', '\x1b', '\x7f', '\x85']


@pytest.mark.parametrize('character', CONTROL, ids=repr)
def test_a_token_holding_a_control_character_is_refused(character):
    """No GitHub token holds one. Sent, a carriage return or a newline
    is refused by requests, with the header quoted in its error, and any
    other by GitHub, or by a proxy on the way."""
    console = recording()

    with pytest.raises(typer.Exit) as stopped:
        check_github_token(f'{HALF}{character}{HALF}', console=console)

    assert stopped.value.exit_code == 1
    said = words(console.export_text())
    assert 'GitHub Token Malformed' in said
    assert f'U+{ord(character):04X}' in said
    # Said to be there, never shown.
    assert HALF not in said


def test_the_configured_token_is_read_without_the_whitespace_around_it(
    monkeypatch,
):
    """`GitHubConfig`'s, which the services fall back on when they are
    given no token."""
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')

    assert GitHubConfig().token == TOKEN


#: Every command that reads `--token`, or GITHUB_TOKEN in its place, and
#: checks it first: the module it is verified in next, and the command.
CHECKED = {
    'github search': ('chatsbom.commands.github.search', ['github', 'search']),
    'github repo': ('chatsbom.commands.github.repo', ['github', 'repo']),
    'github release': (
        'chatsbom.commands.github.release', ['github', 'release'],
    ),
    'github commit': ('chatsbom.commands.github.commit', ['github', 'commit']),
    'github tree': ('chatsbom.commands.github.tree', ['github', 'tree']),
    'github content': (
        'chatsbom.commands.github.content', ['github', 'content'],
    ),
    'github depgraph': (
        'chatsbom.commands.github.depgraph', ['github', 'depgraph'],
    ),
    'queue sync': ('chatsbom.commands.queue.sync', ['queue', 'sync']),
    'run': ('chatsbom.commands.run', ['run']),
}


@pytest.fixture
def verified(tmp_path, monkeypatch) -> list[str]:
    """The tokens the commands go on to verify, from a directory with no
    `.env`. Each command stops there, before it asks GitHub anything."""
    monkeypatch.chdir(tmp_path)
    seen: list[str] = []

    def verify(token: str, **_: object) -> None:
        seen.append(token)
        raise typer.Exit(0)

    for module, _ in CHECKED.values():
        monkeypatch.setattr(f'{module}.verify_github_token', verify)
    return seen


@pytest.mark.parametrize(
    'command', [command for _, command in CHECKED.values()], ids=list(CHECKED),
)
def test_every_command_uses_the_token_without_its_line_ending(
    command, verified, monkeypatch,
):
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')

    result = runner.invoke(app, command)

    assert result.exit_code == 0, result.output
    assert verified == [TOKEN]


def test_a_command_given_a_token_holding_a_control_character_stops(
    verified, monkeypatch,
):
    """With the reason, and no traceback; and before the token is
    verified, which would send it."""
    monkeypatch.setenv('COLUMNS', '400')

    result = runner.invoke(
        app, ['github', 'repo', '--token', f'{HALF}\r{HALF}'],
    )

    assert result.exit_code == 1, result.output
    assert verified == []
    said = words(result.output)
    assert 'GitHub Token Malformed' in said
    assert 'Traceback' not in said
    assert HALF not in result.output


# --- where a token's error is said (#124) --------------------------------

@pytest.mark.parametrize(
    'command', [command for _, command in CHECKED.values()], ids=list(CHECKED),
)
def test_a_missing_token_is_said_on_stderr(command, verified, monkeypatch):
    """It was printed on stdout, where what a command prints for its
    reader goes."""
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    monkeypatch.setenv('COLUMNS', '400')

    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    assert verified == []
    assert result.stdout == ''
    assert 'GitHub Token Missing' in words(result.stderr)


def test_a_malformed_token_is_said_on_stderr(verified, monkeypatch):
    monkeypatch.setenv('COLUMNS', '400')

    result = runner.invoke(
        app, ['github', 'repo', '--token', f'{HALF}\r{HALF}'],
    )

    assert result.exit_code == 1, result.output
    assert verified == []
    assert result.stdout == ''
    said = words(result.stderr)
    assert 'GitHub Token Malformed' in said
    assert 'U+000D' in said
    assert HALF not in result.stderr


@pytest.mark.parametrize(
    'given, event, fields',
    [
        pytest.param(
            None, 'GitHub token not set',
            {'requires': 'GITHUB_TOKEN or --token'}, id='missing',
        ),
        pytest.param(
            f'{HALF}\r{HALF}', 'GitHub token malformed',
            {'character': 'U+000D', 'position': len(HALF) + 1},
            id='malformed',
        ),
    ],
)
def test_a_token_that_cannot_be_used_is_one_json_event(
    verified, monkeypatch, json_logs, given, event, fields,
):
    """A machine reads stderr then: what it reads is one event, which
    says what is wrong with the token as the words do, and never holds
    it."""
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    token = [] if given is None else ['--token', given]

    result = runner.invoke(app, ['github', 'repo', *token])

    assert result.exit_code == 1, result.output
    assert verified == []
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level'], line['logger']) == (
        event, 'error', 'github_auth',
    )
    assert {name: line[name] for name in fields} == fields
    assert HALF not in result.stderr


#: The commands that take a token without needing one, and the module
#: each makes its `GitHubService` in.
OPTIONAL = {
    'github readme': 'chatsbom.commands.github.readme',
    'github classify': 'chatsbom.commands.github.classify',
}


@pytest.mark.parametrize('command', list(OPTIONAL))
def test_a_command_that_may_go_without_a_token_uses_it_without_its_line_ending(
    command: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The service is the first thing the token reaches; the command
    stops there, before anything is asked of anyone."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('GITHUB_TOKEN', f'{TOKEN}\r\n')
    # `github classify`'s key, for OpenAI's API, which is never asked.
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
