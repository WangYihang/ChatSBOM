"""The CLI's commands, which the README documents, enforced rather than
remembered.

The CLI had grown to 19 subcommands while the README described 9. Docs
drift silently; a test does not.

Since the collector replaced the old pipeline (#171) the commands are
what runs the collector and what it runs: `collect` and `collect repo`;
its index pass's `warehouse build`, `snapshot build`, `export parquet`
and `data prune`; `export schema`, the export's contract; `web serve`,
the site; and `sbom lock`, the resolver. The old pipeline's went, with
nothing kept for compatibility: `queue`, `run`, the stage-major `github`
commands, `sbom generate`, and `data migrate-layout` and `data slim`.
"""
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import typer
from typer.testing import CliRunner

from chatsbom.__main__ import app

REPO_ROOT = Path(__file__).resolve().parent.parent
README = (REPO_ROOT / 'README.md').read_text(encoding='utf-8')

runner = CliRunner()


def _commands(command: Any, path: tuple[str, ...] = ()) -> Iterator[
    tuple[str, ...]
]:
    """Every command the CLI runs, by the words that name it: a group that
    runs without a subcommand is one, and so is each of its own."""
    if not hasattr(command, 'list_commands'):
        yield path
        return
    if path and getattr(command, 'invoke_without_command', False):
        yield path
    context = typer.Context(command)
    for name in command.list_commands(context):
        sub = command.get_command(context, name)
        assert sub is not None, name
        yield from _commands(sub, (*path, name))


def commands() -> list[tuple[str, ...]]:
    return sorted(_commands(typer.main.get_command(app)))


#: What the CLI runs, and nothing else (#171).
COMMANDS = [
    ('collect',),
    ('collect', 'repo'),
    ('data', 'prune'),
    ('export', 'parquet'),
    ('export', 'schema'),
    ('sbom', 'lock'),
    ('snapshot', 'build'),
    ('warehouse', 'build'),
    ('web', 'serve'),
]


def test_its_commands_are_the_collector_and_what_it_runs():
    assert commands() == COMMANDS


@pytest.mark.parametrize('command', COMMANDS, ids=' '.join)
def test_readme_documents_every_command(command):
    assert f'`{command[-1]}`' in README, (
        f'`chatsbom {" ".join(command)}` is not mentioned in README.md'
    )


@pytest.mark.parametrize('command', COMMANDS, ids=' '.join)
def test_every_command_has_help_text(command):
    """`--help` must work without a database, token or network."""
    result = runner.invoke(app, [*command, '--help'])
    assert result.exit_code == 0, result.output
    assert command[-1] in result.output or 'Usage' in result.output


def test_top_level_help_lists_every_group():
    result = runner.invoke(app, ['--help'])
    assert result.exit_code == 0
    for group in sorted({command[0] for command in COMMANDS}):
        assert group in result.output


#: Commands deleted outright, with no stand-in. `chat`, the terminal chat
#: over ClickHouse on Claude: the chat is the web's, on the snapshot
#: (#143). `data backfill-decisions`, which wrote the release and commit
#: decisions from the records in `raw_documents`, which go without being
#: migrated: the collector decides them again. `db`, the ClickHouse
#: server's commands: the warehouse, rebuilt from the store, is the
#: index (`warehouse build`), and the DuckDB CLI the query shell
#: (DEPLOY.md). All three #153.
#:
#: And the old pipeline's (#171), which `chatsbom collect` replaced: the
#: ledger's `queue`, `run`, the stage-major `github` commands, `sbom
#: generate`, and `data migrate-layout` and `data slim`, which moved and
#: slimmed the store the old pipeline wrote.
GONE: tuple[tuple[str, ...], ...] = (
    ('chat',),
    ('data', 'backfill-decisions'),
    ('db',),
    ('db', 'index'),
    ('db', 'query'),
    ('queue',),
    ('queue', 'status'),
    ('queue', 'sync'),
    ('queue', 'track'),
    ('queue', 'due'),
    ('queue', 'backfill'),
    ('run',),
    ('github',),
    ('github', 'search'),
    ('github', 'repo'),
    ('github', 'release'),
    ('github', 'commit'),
    ('github', 'tree'),
    ('github', 'content'),
    ('github', 'depgraph'),
    ('sbom', 'generate'),
    ('data', 'migrate-layout'),
    ('data', 'slim'),
)


@pytest.mark.parametrize('command', GONE, ids=' '.join)
def test_a_deleted_command_is_gone(command: tuple[str, ...]) -> None:
    result = runner.invoke(app, [*command, '--help'])
    assert result.exit_code == 2, result.output
    assert 'No such command' in result.output


def test_export_writes_parquet_and_the_contract_alone():
    """`export d1` wrote SQL for the Cloudflare D1 the Worker read, and
    went with the Worker (#151): the site serves a snapshot."""
    assert [
        command for command in commands() if command[0] == 'export'
    ] == [('export', 'parquet'), ('export', 'schema')]
    result = runner.invoke(app, ['export', 'd1', '--help'])
    assert result.exit_code == 2
    assert "No such command 'd1'" in result.output


def _limits() -> dict[str, Any]:
    """Each `--limit` a command takes, by the words that name it, as its
    parser takes it."""
    root: Any = typer.main.get_command(app)
    found = {}
    for command in COMMANDS:
        found_command = root
        for word in command:
            found_command = found_command.commands[word]
        for param in found_command.params:
            if param.name == 'limit':
                found[' '.join(command)] = param
    return found


def test_every_limit_takes_one_or_more():
    """Below 1, a usage error (#110, #114): what `--limit 0` means is then
    the same everywhere, which is nothing."""
    limits = _limits()

    # The introspection itself, so that finding none cannot pass.
    assert 'sbom lock' in limits
    # The bounds, where the parser has them: an integer type with none
    # takes any number, and a range may be open.
    ranges = {
        command: tuple(
            getattr(param.type, bound, None)
            for bound in ('min', 'min_open', 'max')
        )
        for command, param in limits.items()
    }
    assert ranges == {command: (1, False, None) for command in limits}
