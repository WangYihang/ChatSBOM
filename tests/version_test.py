"""The version is written once, in pyproject.toml, and a bump carries it
to every other place that states it (#46).

It was stated in four places, and a bump rewrote one. The files it was
meant to rewrite were listed under `[tool.bump-my-version.files]`, a
table bump-my-version never reads: its own is `tool.bumpversion`. So
CITATION.cff, and the provenance label compose gives the dashboard,
`chatsbom/0.5.4 clickhouse`, would have gone on naming 0.5.4 through
every release after it.

These read the configuration rather than run a bump: bump-my-version is
not a dependency, and a test may not reach the network for it. What a
bump does with the configuration is its own documented behaviour; what
is checked here is that the configuration names everything there is.
"""
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = tomllib.loads((ROOT / 'pyproject.toml').read_text('utf-8'))
VERSION: str = PYPROJECT['project']['version']

#: The version as a version, not as part of another: `0.5.4` in
#: `chatsbom/0.5.4` or `v0.5.4`, but not in `10.5.4`, `0.5.40` or
#: `0.5.4.1`. A full stop after it may end a sentence.
TOKEN = re.compile(r'(?<![\d.])' + re.escape(VERSION) + r'(?!\d|\.\d)')

#: What a `files` entry searches for when it names nothing itself.
DEFAULT_SEARCH = '{current_version}'

#: Where the version may be written without a bump rewriting it, and why.
#: A path, or a directory ending in `/`.
NOT_REWRITTEN = {
    'uv.lock': (
        'rewritten by `uv lock`, which the bump runs; it names every '
        "locked package's version, and one may be ours"
    ),
    'web/package-lock.json': (
        "npm's record of the dashboard's dependencies, whose versions "
        'may be ours'
    ),
    'tests/': (
        'examples: a label put through the code under test, which says '
        'nothing about the release'
    ),
    'web/test/': "the same, for the dashboard's tests",
}


def bump() -> dict[str, Any]:
    tool: dict[str, Any] = PYPROJECT.get('tool', {})
    return tool.get('bumpversion', {})


def files() -> list[str]:
    """What is in the tree: tracked, or new and not ignored."""
    listing = subprocess.run(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    return [name for name in listing.stdout.decode().split('\0') if name]


def not_rewritten(name: str) -> bool:
    return any(
        name == allowed
        or (allowed.endswith('/') and name.startswith(allowed))
        for allowed in NOT_REWRITTEN
    )


def rendered(entry: dict[str, Any]) -> re.Pattern[str]:
    """What an entry of `[[tool.bumpversion.files]]` looks for, as the
    current version fills it in."""
    tool = bump()
    search: str = entry.get('search', tool.get('search', DEFAULT_SEARCH))
    regex: bool = entry.get('regex', tool.get('regex', False))
    filled = search.replace(DEFAULT_SEARCH, VERSION)
    assert '{' not in filled, (
        f'{entry["filename"]}: this test fills in {DEFAULT_SEARCH} alone, '
        f'and the search is {search!r}'
    )
    return re.compile(filled if regex else re.escape(filled))


def covered(name: str, text: str) -> list[tuple[int, int]]:
    """Where in a file a bump rewrites the version."""
    spans = [
        match.span()
        for entry in bump().get('files', [])
        if entry['filename'] == name
        for match in rendered(entry).finditer(text)
    ]
    if name == 'pyproject.toml':
        # The source: with no `current_version` of its own, the bump
        # reads it from `project.version` and writes it back there.
        source = re.compile(rf'^version = "{re.escape(VERSION)}"$', re.M)
        spans += [match.span() for match in source.finditer(text)]
    return spans


def test_the_bump_is_configured_where_bump_my_version_reads_it():
    assert 'bumpversion' in PYPROJECT['tool']
    assert 'bump-my-version' not in PYPROJECT['tool'], (
        '[tool.bump-my-version] is a table bump-my-version never reads; '
        'its files go in [[tool.bumpversion.files]]'
    )


def test_the_version_is_written_once_in_pyproject():
    """bump-my-version reads `project.version` when its own table names
    no `current_version`; a second copy could only disagree with it."""
    assert 'current_version' not in bump()


def test_every_file_the_bump_rewrites_holds_the_current_version():
    """As a bump would find it: with `ignore_missing_version` false, a
    search that finds nothing stops the bump, after the files before it
    were rewritten."""
    entries = bump().get('files', [])
    assert entries, 'the bump rewrites pyproject.toml alone'
    for entry in entries:
        path = ROOT / entry['filename']
        assert path.is_file(), entry['filename']
        assert rendered(entry).search(path.read_text(encoding='utf-8')), (
            f'{entry["filename"]} does not hold '
            f'{entry.get("search", DEFAULT_SEARCH)!r} for {VERSION}'
        )


def test_the_bump_rewrites_every_version_the_tree_states():
    stale = []
    for name in files():
        path = ROOT / name
        if not_rewritten(name) or not path.is_file():
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            continue
        spans = covered(name, text)
        lines = text.splitlines()
        for match in TOKEN.finditer(text):
            if any(start <= match.start() < end for start, end in spans):
                continue
            line = text.count('\n', 0, match.start()) + 1
            stale.append(f'{name}:{line}: {lines[line - 1].strip()}')
    assert stale == [], (
        f'{VERSION} is written here, and a bump would leave it: add the '
        'file to [[tool.bumpversion.files]] in pyproject.toml, or stop '
        'writing the version out\n  ' + '\n  '.join(stale)
    )


def test_the_lockfile_names_the_current_version():
    """`uv sync --locked`, which CI runs, refuses a lockfile that names
    another. `uv lock` after a bump is what rewrites it."""
    lock = tomllib.loads((ROOT / 'uv.lock').read_text(encoding='utf-8'))
    ours = [p for p in lock['package'] if p['name'] == 'chatsbom']
    assert [p['version'] for p in ours] == [VERSION]


def test_the_bump_brings_the_lockfile_along():
    assert 'uv lock' in bump().get('pre_commit_hooks', [])
