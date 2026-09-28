"""`sbom lock` and the lockfiles `sbom generate` merges, end to end (#14).

`sbom lock` resolves a lockfile for a project that ships none, and
`sbom generate` merges it into the tree Syft scans. Neither asked
whether the project shipped one already:

- `sbom lock` resolved every project, so a committed `composer.lock` or
  `Gemfile.lock` got a second copy beside it, pinned to whatever the
  registry offered that day.
- `sbom generate` copied every file in the lock directory over the
  project, so that copy replaced the committed one. Reproduced: the
  committed lockfile pinned x/y 1.0.0, the resolved one 1.9.3, and Syft
  reported 1.9.3.
- It merged anything else it found there as well, following symlinks,
  although the resolver runs project-controlled code with that
  directory writable.

The Java and Python recipes wrote `dependency-tree.txt` and
`requirements.lock`, and Syft reads neither.

Only the container run and Syft are faked, so the real commands,
service and paths do the work, under a fresh working directory.
"""
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.sbom import lock as lock_command
from chatsbom.core.container import Container
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import LockResult
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.models.language import Language
from chatsbom.services import sbom_service
from tests.sbom_generate_test import syft_document

SYFT_VERSION = '1.41.2'
SHA = '0123456789abcdef0123456789abcdef01234567'

#: Repository name -> id.
REPOSITORIES = {'a': 1, 'b': 2}

TARGET = {
    'ref': 'main', 'ref_type': 'branch',
    'commit_sha': SHA, 'commit_sha_short': SHA[:7],
}

#: Per ecosystem: the manifest, the lockfile, what the project committed
#: and what resolving it again wrote.
ECOSYSTEMS = {
    'php': (
        'composer.json',
        'composer.lock',
        '{"packages": [{"name": "x/y", "version": "1.0.0"}]}\n',
        '{"packages": [{"name": "x/y", "version": "1.9.3"}]}\n',
    ),
    'ruby': (
        'Gemfile',
        'Gemfile.lock',
        'GEM\n  specs:\n    rack (2.2.8)\n',
        'GEM\n  specs:\n    rack (3.1.7)\n',
    ),
}

MANIFEST = {
    'composer.json': '{"require": {"x/y": "^1.0"}}\n',
    'Gemfile': "source 'https://rubygems.org'\ngem 'rack'\n",
    'pom.xml': '<project><artifactId>a</artifactId></project>\n',
    'requirements.txt': 'requests\n',
}

COMMITTED = ECOSYSTEMS['php'][2]
RESOLVED = ECOSYSTEMS['php'][3]

runner = CliRunner()


def _project(name: str, language: str = 'php') -> Path:
    return Path(f'data/06-github-content/{REPOSITORIES[name]}/{SHA}')


def _lock_dir(name: str, language: str = 'php') -> Path:
    return Path(f'data/10-generated-lock/{REPOSITORIES[name]}/{SHA}')


def _downloaded(language: str, projects: dict[str, dict[str, str]]) -> None:
    """What the content stage left: each project's files, and the ledger
    both commands read."""
    ledger = Path(f'data/06-github-content/{language}.jsonl')
    ledger.parent.mkdir(parents=True, exist_ok=True)
    with ledger.open('w', encoding='utf-8') as handle:
        for name, files in projects.items():
            project = _project(name, language)
            project.mkdir(parents=True, exist_ok=True)
            for filename, body in files.items():
                (project / filename).write_text(body, encoding='utf-8')
            handle.write(
                json.dumps({
                    'id': REPOSITORIES[name],
                    'owner': 'o',
                    'name': name,
                    'download_target': TARGET,
                    'local_content_path': str(project),
                }) + '\n',
            )


def _resolved(
    name: str, files: dict[str, str], language: str = 'php',
) -> Path:
    """What an earlier `sbom lock` left for `name`."""
    lock_dir = _lock_dir(name, language)
    lock_dir.mkdir(parents=True, exist_ok=True)
    for filename, body in files.items():
        (lock_dir / filename).write_text(body, encoding='utf-8')
    return lock_dir


def _said(result: Any) -> str:
    """The output as words. Rich wraps long lines; compare words, not
    layout."""
    return ' '.join(result.output.split())


@pytest.fixture
def workdir(tmp_path, monkeypatch, no_database) -> Path:
    """A fresh working directory and container for each test. `data/`
    and `.cache/` both resolve against it, so nothing here reaches the
    real ones, and no database is reached at all."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    return tmp_path


# --- sbom lock --------------------------------------------------------------

class FakeResolver:
    """`generate_lockfile` as `sbom lock` calls it, without Docker.

    It records which project it was asked to resolve, and leaves the
    recipe's lockfile in the output directory as a resolution that
    succeeded would.
    """

    def __init__(self) -> None:
        self.resolved: list[str] = []

    def __call__(
        self,
        language: Language,
        project_dir: Path,
        output_dir: Path,
        limits: SandboxLimits | None = None,
    ) -> LockResult:
        # data/06-github-content/<repository_id>/<sha>
        names = {str(v): k for k, v in REPOSITORIES.items()}
        self.resolved.append(names[project_dir.parts[-2]])
        lock = output_dir / lock_recipe_for(language).produces[0]
        atomic_write_text(lock, 'resolved\n')
        return LockResult(produced=(lock,), returncode=0, stderr='')


@pytest.fixture
def resolver(workdir, monkeypatch) -> FakeResolver:
    monkeypatch.setattr(lock_command, 'docker_available', lambda: True)
    fake = FakeResolver()
    monkeypatch.setattr(lock_command, 'generate_lockfile', fake)
    return fake


def lock(*args: str) -> Any:
    return runner.invoke(app, ['sbom', 'lock', *args])


@pytest.mark.parametrize('language', ['php', 'ruby'])
def test_a_project_that_ships_a_lockfile_is_not_resolved(resolver, language):
    """Its lockfile is what it pins, and what Syft should read.

    Resolving it again only produced a second lockfile, pinned to what
    the registry offered that day, for `sbom generate` to scan in its
    place. README's own end-to-end check was one of these: discourse
    commits its `Gemfile.lock`.
    """
    manifest, lockfile, committed, _ = ECOSYSTEMS[language]
    _downloaded(
        language, {
            'a': {manifest: MANIFEST[manifest], lockfile: committed},
            'b': {manifest: MANIFEST[manifest]},
        },
    )

    result = lock('--language', language)

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['b'], 'a ships its own lockfile'
    assert not _lock_dir('a', language).exists()
    assert (
        f'{language}: resolved 1 · cached 0 · ships a lockfile 1 · '
        'failed 0 · skipped 0'
    ) in _said(result)


def test_force_does_not_resolve_over_a_committed_lockfile(resolver):
    """`--force` re-resolves what `sbom lock` wrote, never what the
    project committed. The lockfile here is what a run before this fix
    left beside it."""
    _downloaded(
        'php', {
            'a': {
                'composer.json': MANIFEST['composer.json'],
                'composer.lock': COMMITTED,
            },
        },
    )
    _resolved('a', {'composer.lock': RESOLVED})

    result = lock('--language', 'php', '--force')

    assert result.exit_code == 0, result.output
    assert resolver.resolved == []
    assert 'ships a lockfile 1' in _said(result)


def test_a_symlink_in_the_output_is_not_a_resolved_lockfile(resolver, workdir):
    """The resolver runs project-controlled code with the output
    directory writable, so what it leaves there is not evidence of
    anything. A link named like the lockfile counted as one, so the
    project was never resolved again."""
    elsewhere = workdir / 'elsewhere'
    elsewhere.write_text('not a lockfile\n', encoding='utf-8')
    _downloaded('php', {'b': {'composer.json': MANIFEST['composer.json']}})
    (_resolved('b', {}) / 'composer.lock').symlink_to(elsewhere)

    result = lock('--language', 'php')

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['b']
    assert 'resolved 1 · cached 0' in _said(result)


@pytest.mark.parametrize(
    'language,manifest,wrote', [
        ('java', 'pom.xml', 'dependency-tree.txt'),
        ('python', 'requirements.txt', 'requirements.lock'),
    ],
)
def test_java_and_python_are_not_resolved_and_the_run_says_why(
    resolver, language, manifest, wrote,
):
    """Their recipes wrote a file Syft never reads, so every resolution
    ran a container for a scan that came out the same."""
    _downloaded(language, {'b': {manifest: MANIFEST[manifest]}})

    result = lock('--language', language)

    assert result.exit_code == 0, result.output
    assert resolver.resolved == []
    said = _said(result)
    assert f'no lockfile recipe for {language}' in said
    assert wrote in said


# --- sbom generate ----------------------------------------------------------

class FakeSyft:
    """`subprocess.run` as `sbom generate` calls it: `syft dir:... -o json`.

    It records each tree it was pointed at as {relative path: contents},
    read while the scan runs: a merged tree is a temporary directory and
    is gone once the scan ends.
    """

    def __init__(self) -> None:
        self.scans: list[dict[str, str]] = []

    def __call__(
        self, command: list[str], **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == 'syft' and command[2:] == ['-o', 'json'], command
        tree = Path(command[1].removeprefix('dir:'))
        self.scans.append({
            path.relative_to(tree).as_posix(): path.read_text(encoding='utf-8')
            for path in sorted(tree.rglob('*'))
            if path.is_file()
        })
        return subprocess.CompletedProcess(
            command, 0, stdout=syft_document(), stderr='',
        )


@pytest.fixture
def syft(workdir, monkeypatch) -> FakeSyft:
    monkeypatch.setattr(sbom_service, 'check_syft_installed', lambda: True)
    monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: SYFT_VERSION)
    fake = FakeSyft()
    monkeypatch.setattr(sbom_service.subprocess, 'run', fake)
    return fake


def generate(language: str) -> Any:
    return runner.invoke(app, ['sbom', 'generate', '--language', language])


@pytest.mark.parametrize('language', ['php', 'ruby'])
def test_a_committed_lockfile_is_what_syft_scans(syft, language):
    """The resolved copy was merged over it, and Syft reported the
    versions the registry offered on the day `sbom lock` ran rather
    than the ones the project pins."""
    manifest, lockfile, committed, resolved = ECOSYSTEMS[language]
    _downloaded(
        language, {
            'a': {manifest: MANIFEST[manifest], lockfile: committed},
        },
    )
    _resolved('a', {lockfile: resolved}, language)

    result = generate(language)

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {manifest: MANIFEST[manifest], lockfile: committed},
    ]


def test_a_resolved_lockfile_is_merged_where_none_was_committed(syft):
    """What `sbom lock` is for, and what the rest must leave working."""
    _downloaded('php', {'b': {'composer.json': MANIFEST['composer.json']}})
    _resolved('b', {'composer.lock': RESOLVED})

    result = generate('php')

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {
            'composer.json': MANIFEST['composer.json'],
            'composer.lock': RESOLVED,
        },
    ]


def test_a_symlink_in_the_lock_directory_is_not_followed(syft, workdir):
    """A link there is the resolver's doing, and can point anywhere on
    the host. `copy2` followed it, so the collector read the file it
    named, with its own privileges, and Syft scanned that as the
    project's lockfile."""
    elsewhere = workdir / 'elsewhere'
    elsewhere.write_text('a file elsewhere on the host\n', encoding='utf-8')
    _downloaded('php', {'b': {'composer.json': MANIFEST['composer.json']}})
    (_resolved('b', {}) / 'composer.lock').symlink_to(elsewhere)

    result = generate('php')

    assert result.exit_code == 0, result.output
    assert syft.scans == [{'composer.json': MANIFEST['composer.json']}]


def test_only_what_the_recipe_declares_is_merged(syft):
    """Anything else in the lock directory was merged as well, so a
    hostile resolver could add packages to the SBOM by leaving another
    ecosystem's lockfile there."""
    _downloaded('php', {'b': {'composer.json': MANIFEST['composer.json']}})
    _resolved(
        'b', {
            'composer.lock': RESOLVED,
            'package-lock.json': '{"packages": {}}\n',
        },
    )

    result = generate('php')

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {
            'composer.json': MANIFEST['composer.json'],
            'composer.lock': RESOLVED,
        },
    ]


@pytest.mark.parametrize(
    'language,manifest,leftover', [
        ('java', 'pom.xml', 'dependency-tree.txt'),
        ('python', 'requirements.txt', 'requirements.lock'),
    ],
)
def test_what_a_withdrawn_recipe_left_is_not_merged(
    syft, language, manifest, leftover,
):
    """Earlier runs left these on disk. Syft never read them, so the
    scan is the project's own tree, as it would have been without."""
    _downloaded(language, {'b': {manifest: MANIFEST[manifest]}})
    _resolved('b', {leftover: 'resolved\n'}, language)

    result = generate(language)

    assert result.exit_code == 0, result.output
    assert syft.scans == [{manifest: MANIFEST[manifest]}]
