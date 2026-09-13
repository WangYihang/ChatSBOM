"""The README documents every command, enforced rather than remembered.

The CLI had grown to 19 subcommands while the README described 9. Docs
drift silently; a test does not.
"""
from pathlib import Path

import pytest
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
    for group in ('github', 'sbom', 'db', 'openapi', 'chat'):
        assert group in result.output
