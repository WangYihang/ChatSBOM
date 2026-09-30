"""The README documents every command, enforced rather than remembered.

The CLI had grown to 19 subcommands while the README described 9. Docs
drift silently; a test does not.
"""
from pathlib import Path
from typing import Any

import pytest
import typer
from typer.testing import CliRunner

from chatsbom.__main__ import app

REPO_ROOT = Path(__file__).resolve().parent.parent
README = (REPO_ROOT / 'README.md').read_text(encoding='utf-8')

runner = CliRunner()


def _subcommands() -> list[tuple[str, str]]:
    """Every (group, command) pair registered on the Typer app."""
    pairs = []
    for group in app.registered_groups:
        group_name = group.name
        typer_instance = group.typer_instance
        if group_name is None or typer_instance is None:
            continue
        for sub in typer_instance.registered_groups:
            if sub.name:
                pairs.append((group_name, sub.name))
    return sorted(pairs)


def test_the_cli_exposes_subcommands():
    """Guard the introspection itself, so an empty list cannot pass."""
    assert len(_subcommands()) >= 19


@pytest.mark.parametrize('group,command', _subcommands(), ids=lambda x: x)
def test_readme_documents_every_subcommand(group, command):
    assert f'`{command}`' in README, (
        f'`chatsbom {group} {command}` is not mentioned in README.md'
    )


@pytest.mark.parametrize('group,command', _subcommands(), ids=lambda x: x)
def test_every_subcommand_has_help_text(group, command):
    """`--help` must work without a database, token or network."""
    result = runner.invoke(app, [group, command, '--help'])
    assert result.exit_code == 0, result.output
    assert command in result.output or 'Usage' in result.output


def test_top_level_help_lists_every_group():
    result = runner.invoke(app, ['--help'])
    assert result.exit_code == 0
    for group in ('github', 'sbom', 'db', 'openapi'):
        assert group in result.output


#: Commands deleted outright, with no stand-in (#153). `chat`, the
#: terminal chat over ClickHouse on Claude: the chat is the web's, on
#: the snapshot (#143). `data backfill-decisions`, which wrote the
#: release and commit decisions from the records in `raw_documents`,
#: which go without being migrated: the collector decides them again.
GONE: tuple[tuple[str, ...], ...] = (
    ('chat',),
    ('data', 'backfill-decisions'),
)


@pytest.mark.parametrize('command', GONE, ids=' '.join)
def test_a_deleted_command_is_gone(command: tuple[str, ...]) -> None:
    result = runner.invoke(app, [*command, '--help'])
    assert result.exit_code == 2, result.output
    assert 'No such command' in result.output


#: Options deleted outright (#153): `--from-raw` took a stage's
#: repositories from the records in `raw_documents`; a stage reads its
#: ledger, in `data/`, alone.
GONE_OPTIONS: tuple[tuple[str, ...], ...] = (
    ('github', 'release', '--from-raw'),
    ('github', 'commit', '--from-raw'),
)


@pytest.mark.parametrize('command', GONE_OPTIONS, ids=' '.join)
def test_a_deleted_option_is_gone(command: tuple[str, ...]) -> None:
    result = runner.invoke(app, [*command, '--help'])
    assert result.exit_code == 2, result.output
    assert 'No such option' in result.output


def test_export_writes_parquet_and_the_contract_alone():
    """`export d1` wrote SQL for the Cloudflare D1 the Worker read, and
    went with the Worker (#151): the site serves a snapshot."""
    assert [
        command for group, command in _subcommands() if group == 'export'
    ] == ['parquet', 'schema']
    result = runner.invoke(app, ['export', 'd1', '--help'])
    assert result.exit_code == 2
    assert "No such command 'd1'" in result.output


#: The groups whose commands take a `--limit` of 1 or more (#114).
#: `--limit 0` meant a different thing to each: nothing to one, one root
#: to another, and to `db query` a query for no rows. The `github`
#: commands are left to the stage runner that replaces them (#36).
LIMITED = ('db', 'sbom', 'openapi')


def _limits() -> dict[str, Any]:
    """Each `--limit` of a command in LIMITED, by `group command`, as
    its parser takes it."""
    root: Any = typer.main.get_command(app)
    found = {}
    for group in LIMITED:
        for name, command in root.commands[group].commands.items():
            for param in command.params:
                if param.name == 'limit':
                    found[f'{group} {name}'] = param
    return found


def test_every_limit_takes_one_or_more():
    """Below 1, a usage error, as `sbom generate` made it (#110): what
    `--limit 0` means is then the same everywhere, which is nothing."""
    limits = _limits()

    # The introspection itself, so that finding none cannot pass.
    assert {
        'db index', 'db query', 'db raw', 'sbom generate', 'sbom lock',
    } <= set(limits)
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
