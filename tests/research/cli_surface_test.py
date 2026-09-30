"""`chatsbom-research`, the research tools' own command (#167).

`openapi`'s five commands, and `github classify` and `github readme`,
study the corpus: the collector, the warehouse, the snapshot and the web
service never run them. So they left the core CLI, `chatsbom`, for a
second command of the same distribution, where `classify` and `readme`
are at the top, with no `github` group to be in. Each takes the options
it took and writes what it wrote (their tests, beside this one): this
holds where each is, and that README says so.
"""
import re
import tomllib
from collections.abc import Iterator
from pathlib import Path

import pytest
import typer
from typer.core import TyperGroup
from typer.testing import CliRunner

from chatsbom.__main__ import app as core
from chatsbom.research.__main__ import app

ROOT = Path(__file__).resolve().parents[2]
PROJECT = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
README = (ROOT / 'README.md').read_text(encoding='utf-8')

#: README's section for the research tools, and the core CLI's.
RESEARCH_SECTION = '## The research tools: `chatsbom-research`'
CORE_SECTION = '## Command Reference'

runner = CliRunner()

#: Each research command, by the words that run it, and the words that
#: ran it in the core CLI.
MOVED = {
    ('classify',): ('github', 'classify'),
    ('openapi', 'candidates'): ('openapi', 'candidates'),
    ('openapi', 'clone'): ('openapi', 'clone'),
    ('openapi', 'drift'): ('openapi', 'drift'),
    ('openapi', 'list-paths'): ('openapi', 'list-paths'),
    ('openapi', 'stats'): ('openapi', 'stats'),
    ('readme',): ('github', 'readme'),
}


def commands(
    command: object, path: tuple[str, ...] = (),
) -> Iterator[tuple[str, ...]]:
    """The words that run each command under `command`: a group's
    commands', or its own where it has none. typer's group, not click's:
    from 0.26 typer carries a click of its own."""
    if isinstance(command, TyperGroup):
        context = typer.Context(command)
        names = command.list_commands(context)
        for name in names:
            sub = command.get_command(context, name)
            assert sub is not None, name
            yield from commands(sub, (*path, name))
        if names:
            return
    yield path


def section(text: str, heading: str) -> str:
    """What `text` says under `heading`, a line of its own, up to the
    next heading of the same level or above: not a shell's comment in a
    block of code, which starts with `#` too."""
    level = len(heading.split(' ', 1)[0])
    lines = text.splitlines()
    start = lines.index(heading)
    fenced = False
    for number in range(start + 1, len(lines)):
        if lines[number].startswith('```'):
            fenced = not fenced
        elif not fenced and re.match(rf'#{{1,{level}}} ', lines[number]):
            return '\n'.join(lines[start:number])
    return '\n'.join(lines[start:])


def test_it_is_a_second_command_of_the_same_distribution():
    """A console script beside `chatsbom`, not a package of its own:
    whatever installs chatsbom installs both."""
    assert PROJECT['project']['scripts'] == {
        'chatsbom': 'chatsbom.__main__:app',
        'chatsbom-research': 'chatsbom.research.__main__:app',
    }


def test_its_commands_are_the_research_tools():
    found = sorted(commands(typer.main.get_command(app)))
    assert found == sorted(MOVED)


def test_its_help_lists_them():
    result = runner.invoke(app, ['--help'])
    assert result.exit_code == 0, result.output
    for command in ('openapi', 'classify', 'readme'):
        assert command in result.output

    result = runner.invoke(app, ['openapi', '--help'])
    assert result.exit_code == 0, result.output
    for command in ('candidates', 'clone', 'drift', 'list-paths', 'stats'):
        assert command in result.output


@pytest.mark.parametrize('command', list(MOVED), ids=' '.join)
def test_each_is_no_longer_the_cores(command: tuple[str, ...]) -> None:
    """`chatsbom --help` lists none of them, and none runs there."""
    result = runner.invoke(core, [*MOVED[command], '--help'])
    assert result.exit_code == 2, result.output
    assert 'No such command' in result.output


@pytest.mark.parametrize('command', list(MOVED), ids=' '.join)
def test_readme_documents_each_under_its_command(
    command: tuple[str, ...],
) -> None:
    """In a section of its own, apart from `chatsbom`'s commands."""
    assert f'`{command[-1]}`' in section(README, RESEARCH_SECTION), (
        f'`chatsbom-research {" ".join(command)}` is not in README.md, '
        f'under "{RESEARCH_SECTION}"'
    )


def test_the_cores_reference_documents_none_of_them():
    reference = section(README, CORE_SECTION)
    assert '`chatsbom openapi`' not in reference
    github = section(reference, '### `chatsbom github` — collection')
    listed = re.findall(r'^\| `([^`]+)` \|', github, re.MULTILINE)
    assert 'classify' not in listed
    assert 'readme' not in listed
    # The introspection itself, so that a table it cannot read passes
    # nothing.
    assert 'search' in listed
