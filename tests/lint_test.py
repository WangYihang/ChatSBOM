"""The Python lint is one ruff, the dev group's, for a commit, for CI and
for `uv run ruff check` alike (#45).

Four pre-commit hooks linted the tree, each at the version its own
`rev:` named, in an environment of its own: flake8, autoflake,
reorder-python-imports and pyupgrade. `ruff check` does their work now,
configured in pyproject.toml. A local hook runs it through uv, as it runs
mypy, so the ruff a commit runs is the one uv.lock holds and Dependabot
moves.

It lints; it does not format. autopep8, add-trailing-comma and
double-quote-string-fixer go on formatting the tree: `ruff format` would
rewrite nearly every file, and when to do that is a decision of its own.
"""
from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = tomllib.loads((ROOT / 'pyproject.toml').read_text('utf-8'))

#: The hooks `ruff check` replaced, and the repository each came from.
REPLACED = {
    'flake8': 'https://github.com/PyCQA/flake8',
    'autoflake': 'https://github.com/PyCQA/autoflake',
    'reorder-python-imports': (
        'https://github.com/asottile/reorder-python-imports'
    ),
    'pyupgrade': 'https://github.com/asottile/pyupgrade',
}

#: The hooks that format the tree, which ruff's fixes must come before.
FORMATTERS = {'add-trailing-comma', 'autopep8'}


def hooks() -> Iterator[tuple[str, dict[str, Any]]]:
    """Each hook, in the order pre-commit runs them, with its repository."""
    config = yaml.safe_load((ROOT / '.pre-commit-config.yaml').read_text())
    for repo in config['repos']:
        for hook in repo['hooks']:
            yield repo['repo'], hook


def runs_ruff(repo: str, hook: dict[str, Any]) -> bool:
    return repo == 'local' and 'ruff' in hook['entry'].split()


def ruff() -> dict[str, Any]:
    tool: dict[str, Any] = PYPROJECT.get('tool', {})
    return tool.get('ruff', {})


def pinned() -> str:
    """The version the dev group pins ruff to."""
    pins = [
        requirement for requirement in PYPROJECT['dependency-groups']['dev']
        if isinstance(requirement, str) and re.match(r'ruff\b', requirement)
    ]
    assert len(pins) == 1, f'the dev group names ruff {len(pins)} times'
    exact = re.fullmatch(r'ruff==(\d+(?:\.\d+)*)', pins[0])
    assert exact, f'{pins[0]}: not pinned exactly'
    return exact.group(1)


def run_ruff(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    """The ruff this environment has, which is the dev group's."""
    return subprocess.run(
        [sys.executable, '-m', 'ruff', *args, '--no-cache'],
        cwd=cwd,
        capture_output=True,
        text=True,
    )


def test_the_hooks_ruff_replaced_are_gone():
    left = [
        hook['id'] for repo, hook in hooks()
        if hook['id'] in REPLACED or repo in REPLACED.values()
    ]
    assert left == []


def test_pre_commit_runs_the_dev_groups_ruff():
    """Through uv, as it runs mypy: the version uv.lock holds, where a
    hook repository brings one of its own, with pyproject.toml's
    configuration."""
    [hook] = [hook for repo, hook in hooks() if runs_ruff(repo, hook)]
    assert hook['entry'].startswith('uv run --frozen ruff check --fix')
    assert hook['language'] == 'system'
    assert {'python', 'pyi'} <= set(hook['types_or'])


def test_ruff_fixes_before_the_formatters_format():
    """What its fixes write, a wrapped import or one of pyupgrade's
    rewrites, the formatters format in the same run, so that a second run
    finds nothing to change. And ruff only lints: `ruff format` beside
    autopep8 would undo what autopep8 wrote, and autopep8 what it
    wrote."""
    order = [
        'ruff' if runs_ruff(repo, hook) else hook['id']
        for repo, hook in hooks()
    ]
    assert FORMATTERS <= set(order)
    for formatter in FORMATTERS:
        assert order.index('ruff') < order.index(formatter), formatter
    formatting = [
        hook['id'] for repo, hook in hooks()
        if hook['id'] == 'ruff-format'
        or runs_ruff(repo, hook) and 'format' in hook['entry'].split()
    ]
    assert formatting == []


def test_ruff_is_pinned_exactly_in_the_dev_group():
    """A new release can fail the check with no change here, as a new
    mypy can, so each one comes as its own update."""
    assert pinned()


def test_the_lockfile_holds_the_pinned_ruff():
    lock = tomllib.loads((ROOT / 'uv.lock').read_text(encoding='utf-8'))
    locked = [p['version'] for p in lock['package'] if p['name'] == 'ruff']
    assert locked == [pinned()]


def test_dependabot_brings_a_new_ruff_on_its_own():
    """Grouped with the week's other updates, a release that failed the
    lint would hold every one of them back."""
    config = yaml.safe_load((ROOT / '.github' / 'dependabot.yml').read_text())
    [uv] = [
        update for update in config['updates']
        if update['package-ecosystem'] == 'uv'
    ]
    groups = uv.get('groups', {})
    assert groups
    for name, group in groups.items():
        assert 'ruff' in group.get('exclude-patterns', []), name


def test_ruff_reads_the_python_the_project_requires():
    """Which syntax it parses and which rewrites pyupgrade's rules ask
    for: requires-python's floor, the version mypy checks for too."""
    floor = re.fullmatch(
        r'>=(\d+)\.(\d+)', PYPROJECT['project']['requires-python'],
    )
    assert floor
    major, minor = floor.groups()
    assert ruff().get('target-version') == f'py{major}{minor}' == 'py312'
    assert PYPROJECT['tool']['mypy']['python_version'] == f'{major}.{minor}'


def test_ruff_checks_what_the_replaced_hooks_checked():
    """pyflakes and pycodestyle, flake8's, less the line length it
    ignored; unused imports, autoflake's; the imports' order,
    reorder-python-imports'; and pyupgrade's rewrites."""
    lint = ruff().get('lint', {})
    assert {'F', 'E', 'W', 'I', 'UP'} <= set(lint.get('select', []))
    assert 'E501' in lint.get('ignore', [])


def test_imports_are_one_per_line_and_chatsbom_is_first_party():
    """reorder-python-imports' style, in which the tree is written: one
    name to a `from` import, in sections, and within a section in
    alphabetical order, whatever the case."""
    isort = ruff().get('lint', {}).get('isort', {})
    assert isort.get('force-single-line') is True
    assert isort.get('order-by-type') is False
    assert 'chatsbom' in isort.get('known-first-party', [])


def test_ruff_checks_every_python_file_git_has_and_no_other():
    """What `uv run ruff check` walks is what pre-commit hands it: the
    dataset, the workspaces and the environment are git-ignored, and no
    directory pytest skips holds a Python file git tracks."""
    shown = run_ruff('check', '--show-files', cwd=ROOT)
    assert shown.returncode == 0, shown.stderr
    walked = {
        Path(line).relative_to(ROOT).as_posix()
        for line in shown.stdout.splitlines()
        if line.endswith(('.py', '.pyi'))
    }
    listing = subprocess.run(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    tracked = {
        name for name in listing.stdout.decode().split('\0')
        if name.endswith(('.py', '.pyi')) and (ROOT / name).is_file()
    }
    assert walked
    assert walked == tracked


def test_the_fix_removes_unused_imports_but_not_a_re_export(tmp_path):
    """autoflake removed every unused import, in an `__init__.py` too,
    where one is how a package re-exports a name. ruff's fix removes
    them elsewhere. In an `__init__.py` it reports them, for `__all__` or
    `import x as x` to say which is meant, and leaves the file as it
    was."""
    package = tmp_path / 'package'
    package.mkdir()
    init = package / '__init__.py'
    init.write_text('from package.names import NAME\n')
    names = package / 'names.py'
    names.write_text('import os\n\nNAME = 1\n')
    result = run_ruff(
        'check', '--fix', '--output-format', 'concise',
        '--config', str(ROOT / 'pyproject.toml'), '.',
        cwd=tmp_path,
    )
    assert 'import os' not in names.read_text()
    assert init.read_text() == 'from package.names import NAME\n'
    assert result.returncode == 1
    assert 'package/__init__.py:1:' in result.stdout
    assert 'F401' in result.stdout
