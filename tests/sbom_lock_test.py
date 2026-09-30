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

Since manifests are discovered at any depth (#51), a recipe is chosen
per directory from the manifests present there, and what it resolved
is merged back at that directory.

Only the container run and Syft are faked, so the real commands,
service and paths do the work, under a fresh working directory.
"""
import json
import os
import subprocess
import threading
import time
from collections.abc import Callable
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
from chatsbom.core.sandbox import SandboxError
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.services import sbom_service
from tests.sbom_generate_test import syft_document

SYFT_VERSION = '1.52.0'
SHA = '0123456789abcdef0123456789abcdef01234567'

#: Repository name -> id.
REPOSITORIES = {'a': 1, 'b': 2}

#: Per ecosystem (the lock recipes' keys): the manifest, the lockfile, what the project committed
#: and what resolving it again wrote.
ECOSYSTEMS = {
    'composer': (
        'composer.json',
        'composer.lock',
        '{"packages": [{"name": "x/y", "version": "1.0.0"}]}\n',
        '{"packages": [{"name": "x/y", "version": "1.9.3"}]}\n',
    ),
    'gem': (
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

COMMITTED = ECOSYSTEMS['composer'][2]
RESOLVED = ECOSYSTEMS['composer'][3]

runner = CliRunner()


def _project(name: str) -> Path:
    return Path(f'data/06-github-content/{REPOSITORIES[name]}/{SHA}')


def _lock_dir(name: str, directory: str = '') -> Path:
    root = Path(f'data/10-generated-lock/{REPOSITORIES[name]}/{SHA}')
    return root / directory if directory else root


def _downloaded(projects: dict[str, dict[str, str]]) -> None:
    """What the content stage left: each project's files, at their paths
    in the repository. Both commands walk the content roots."""
    for name, files in projects.items():
        project = _project(name)
        project.mkdir(parents=True, exist_ok=True)
        for filename, body in files.items():
            path = project / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding='utf-8')


def _resolved(
    name: str, files: dict[str, str], directory: str = '',
) -> Path:
    """What an earlier `sbom lock` left for `name`'s `directory`."""
    lock_dir = _lock_dir(name, directory)
    lock_dir.mkdir(parents=True, exist_ok=True)
    for filename, body in files.items():
        (lock_dir / filename).write_text(body, encoding='utf-8')
    return lock_dir


def _said(result: Any) -> str:
    """The output as words. Rich wraps long lines; compare words, not
    layout."""
    return ' '.join(result.output.split())


@pytest.fixture
def workdir(tmp_path, monkeypatch) -> Path:
    """A fresh working directory and container for each test. `data/`
    and `.cache/` both resolve against it, so nothing here reaches the
    real ones, and no database is reached at all."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    return tmp_path


# --- sbom lock --------------------------------------------------------------

class FakeResolver:
    """`generate_lockfile` as `sbom lock` calls it, without Docker.

    It records which project it was asked to resolve (the repository,
    and the directory within it if not the root), and leaves the
    recipe's lockfile in the output directory as a resolution that
    succeeded would.
    """

    def __init__(self) -> None:
        self.resolved: list[str] = []
        self.ecosystems: list[str] = []

    def __call__(
        self,
        ecosystem: str,
        project_dir: Path,
        output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        # data/06-github-content/<repository_id>/<sha>[/<directory>]
        names = {str(v): k for k, v in REPOSITORIES.items()}
        parts = project_dir.parts
        at = parts.index('06-github-content')
        directory = '/'.join(parts[at + 3:])
        name = names[parts[at + 1]]
        self.resolved.append(f'{name}/{directory}' if directory else name)
        self.ecosystems.append(ecosystem)
        lock = output_dir / lock_recipe_for(ecosystem).produces[0]
        atomic_write_text(lock, 'resolved\n')
        return LockResult(produced=(lock,), returncode=0, stderr='')


def _a_daemon_is_there(monkeypatch: pytest.MonkeyPatch) -> None:
    """What `sbom lock` asks of Docker before it resolves anything, as a
    daemon that has it would answer."""
    monkeypatch.setattr(lock_command, 'docker_available', lambda: True)
    monkeypatch.setattr(lock_command, 'lock_network', lambda: 'chatsbom-lock')


@pytest.fixture
def resolver(workdir, monkeypatch) -> FakeResolver:
    _a_daemon_is_there(monkeypatch)
    fake = FakeResolver()
    monkeypatch.setattr(lock_command, 'generate_lockfile', fake)
    return fake


def lock(*args: str) -> Any:
    return runner.invoke(app, ['sbom', 'lock', *args])


@pytest.mark.parametrize('ecosystem', ['composer', 'gem'])
def test_a_project_that_ships_a_lockfile_is_not_resolved(resolver, ecosystem):
    """Its lockfile is what it pins, and what Syft should read.

    Resolving it again only produced a second lockfile, pinned to what
    the registry offered that day, for `sbom generate` to scan in its
    place. README's own end-to-end check was one of these: discourse
    commits its `Gemfile.lock`.
    """
    manifest, lockfile, committed, _ = ECOSYSTEMS[ecosystem]
    _downloaded({
        'a': {manifest: MANIFEST[manifest], lockfile: committed},
        'b': {manifest: MANIFEST[manifest]},
    })

    result = lock()

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['b'], 'a ships its own lockfile'
    assert resolver.ecosystems == [ecosystem]
    assert not _lock_dir('a').exists()
    assert (
        '2 content roots · 1 directories to resolve · resolved 1 · '
        'cached 0 · failed 0'
    ) in _said(result)


def test_force_does_not_resolve_over_a_committed_lockfile(resolver):
    """`--force` re-resolves what `sbom lock` wrote, never what the
    project committed. The lockfile here is what a run before this fix
    left beside it."""
    _downloaded({
        'a': {
            'composer.json': MANIFEST['composer.json'],
            'composer.lock': COMMITTED,
        },
    })
    _resolved('a', {'composer.lock': RESOLVED})

    result = lock('--force')

    assert result.exit_code == 0, result.output
    assert resolver.resolved == []
    assert '0 directories to resolve' in _said(result)


def test_a_symlink_in_the_output_is_not_a_resolved_lockfile(resolver, workdir):
    """The resolver runs project-controlled code with the output
    directory writable, so what it leaves there is not evidence of
    anything. A link named like the lockfile counted as one, so the
    project was never resolved again."""
    elsewhere = workdir / 'elsewhere'
    elsewhere.write_text('not a lockfile\n', encoding='utf-8')
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})
    (_resolved('b', {}) / 'composer.lock').symlink_to(elsewhere)

    result = lock()

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['b']
    assert 'resolved 1 · cached 0' in _said(result)


@pytest.mark.parametrize(
    'ecosystem,manifest,wrote', [
        ('maven', 'pom.xml', 'dependency-tree.txt'),
        ('pypi', 'requirements.txt', 'requirements.lock'),
    ],
)
def test_maven_and_pypi_are_not_resolved_and_the_run_says_why(
    resolver, ecosystem, manifest, wrote,
):
    """Their recipes wrote a file Syft never reads, so every resolution
    ran a container for a scan that came out the same."""
    _downloaded({'b': {manifest: MANIFEST[manifest]}})

    result = lock()
    assert result.exit_code == 0, result.output
    assert resolver.resolved == []

    result = lock('--ecosystem', ecosystem)
    assert result.exit_code == 0, result.output
    assert resolver.resolved == []
    said = _said(result)
    assert f'no lockfile recipe for {ecosystem}' in said
    assert wrote in said


def test_a_recipe_runs_in_every_directory_that_needs_it(resolver):
    """A Composer project under `backend/` of a repository whose root is
    npm: resolved where it is, and written under that directory. A
    directory shipping its lockfile, and the npm root, are left alone."""
    _downloaded({
        'a': {
            'package.json': '{}\n',
            'backend/composer.json': MANIFEST['composer.json'],
            'legacy/composer.json': MANIFEST['composer.json'],
            'legacy/composer.lock': COMMITTED,
            'docs/Gemfile': MANIFEST['Gemfile'],
        },
    })

    result = lock()

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['a/backend', 'a/docs']
    assert resolver.ecosystems == ['composer', 'gem']
    assert (_lock_dir('a', 'backend') / 'composer.lock').is_file()
    assert (_lock_dir('a', 'docs') / 'Gemfile.lock').is_file()
    assert not (_lock_dir('a') / 'composer.lock').exists()

    # And a second run finds them resolved.
    again = lock()
    assert resolver.resolved == ['a/backend', 'a/docs']
    assert 'resolved 0 · cached 2' in _said(again)


@pytest.mark.parametrize('limit', ['0', '-1'])
def test_a_limit_below_one_is_refused_and_nothing_is_resolved(
    resolver, limit,
):
    """`--limit 0` resolved nothing and reported a run like any other,
    and `--limit -1`, a slice, every root but the last. A usage error,
    status 2, before anything runs, as `sbom generate`'s is (#110,
    #114)."""
    _downloaded({
        'a': {'composer.json': MANIFEST['composer.json']},
        'b': {'composer.json': MANIFEST['composer.json']},
    })

    result = lock('--limit', limit)

    assert result.exit_code == 2, result.output
    assert result.stdout == ''
    assert '--limit' in result.stderr
    assert resolver.resolved == []


def test_a_limit_of_one_resolves_one(resolver):
    _downloaded({
        'a': {'composer.json': MANIFEST['composer.json']},
        'b': {'composer.json': MANIFEST['composer.json']},
    })

    result = lock('--limit', '1')

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['a']


def test_one_ecosystem_can_be_asked_for(resolver):
    _downloaded({
        'a': {
            'backend/composer.json': MANIFEST['composer.json'],
            'docs/Gemfile': MANIFEST['Gemfile'],
        },
    })

    result = lock('--ecosystem', 'gem')

    assert result.exit_code == 0, result.output
    assert resolver.resolved == ['a/docs']


# --- what sbom lock says, and where (#124) ---------------------------------

def _never_resolved(monkeypatch: pytest.MonkeyPatch) -> None:
    """A resolution that fails the test if it runs."""
    def resolve(*args: object, **kwargs: object) -> LockResult:
        raise AssertionError('a lockfile was resolved')

    monkeypatch.setattr(lock_command, 'generate_lockfile', resolve)


def _no_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    """No Docker to ask."""
    monkeypatch.setattr(lock_command, 'docker_available', lambda: False)
    _never_resolved(monkeypatch)


def _a_shared_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon whose `chatsbom-lock` network lets its containers talk:
    one made by hand, which `lock_network` refuses."""
    def refuse() -> str:
        raise SandboxError(
            'the network chatsbom-lock lets its containers reach each '
            'other; remove it (docker network rm chatsbom-lock) and it is '
            'made again as it should be',
        )

    monkeypatch.setattr(lock_command, 'docker_available', lambda: True)
    monkeypatch.setattr(lock_command, 'lock_network', refuse)
    _never_resolved(monkeypatch)


#: What `sbom lock` stops on before it resolves anything: how it is made
#: to, the words it says, the event it logs with JSON logs, its level,
#: and the status it exits with. An ecosystem with no recipe leaves
#: nothing to resolve, which is no failure.
REFUSED = {
    'no recipe': (
        _never_resolved, ['--ecosystem', 'maven'],
        'Nothing to resolve: no lockfile recipe for maven:',
        'Nothing to resolve', 'warning', 0,
    ),
    'no docker': (
        _no_daemon, [],
        'Error: Docker is required to resolve lockfiles in isolation.',
        'Docker is required to resolve lockfiles', 'error', 1,
    ),
    'shared network': (
        _a_shared_network, [],
        'Error: the network chatsbom-lock lets its containers reach each '
        'other;',
        'The sandbox cannot be set up', 'error', 1,
    ),
}


@pytest.mark.parametrize(
    'arrange, args, said, event, level, status',
    REFUSED.values(), ids=list(REFUSED),
)
def test_what_stops_it_is_said_on_stderr(
    workdir, monkeypatch, arrange, args, said, event, level, status,
):
    """Each was printed on stdout, where the counts of a run go."""
    arrange(monkeypatch)
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})

    result = lock(*args)

    assert result.exit_code == status, result.output
    assert result.stdout == ''
    assert said in ' '.join(result.stderr.split())


@pytest.mark.parametrize(
    'arrange, args, said, event, level, status',
    REFUSED.values(), ids=list(REFUSED),
)
def test_what_stops_it_is_one_json_event(
    workdir, monkeypatch, json_logs, arrange, args, said, event, level,
    status,
):
    """A machine reads stderr then, and what it reads is one event."""
    arrange(monkeypatch)
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})

    result = lock(*args)

    assert result.exit_code == status, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level'], line['logger']) == (
        event, level, 'sbom_lock',
    )


def test_an_ecosystem_with_no_recipe_says_why_and_what_there_is(
    workdir, json_logs,
):
    """As the words do: the recipe's reason, and the ecosystems that
    have one."""
    result = lock('--ecosystem', 'pypi')

    assert result.exit_code == 0, result.output
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert line['ecosystem'] == 'pypi'
    assert 'requirements.lock' in line['reason']
    assert line['supported'] == ['composer', 'gem']


# --- sbom lock --workers ----------------------------------------------------

#: `generate_lockfile`'s signature, as the fakes below take it.
Resolve = Callable[..., LockResult]


def _resolves(resolve: Resolve, monkeypatch: pytest.MonkeyPatch) -> None:
    """`sbom lock` with `resolve` in place of `generate_lockfile`."""
    _a_daemon_is_there(monkeypatch)
    monkeypatch.setattr(lock_command, 'generate_lockfile', resolve)


def _repository_of(project_dir: Path) -> str:
    """Which repository a directory of a content root belongs to."""
    names = {str(v): k for k, v in REPOSITORIES.items()}
    parts = project_dir.parts
    return names[parts[parts.index('06-github-content') + 1]]


def _wrote(name: str, output_dir: Path, ecosystem: str) -> LockResult:
    """A resolution that succeeded, and says whose it was."""
    lock = output_dir / lock_recipe_for(ecosystem).produces[0]
    atomic_write_text(lock, f'resolved for {name}\n')
    return LockResult(produced=(lock,), returncode=0, stderr='')


def test_workers_resolve_at_once_and_keep_their_results_apart(
    workdir, monkeypatch,
):
    """One container at a time, PHP took 1h54m. At once, each
    resolution still has its own project, output directory and
    result."""
    both = threading.Barrier(2, timeout=10)

    def resolve(
        ecosystem: str, project_dir: Path, output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        # Breaks, and fails the resolution, unless the other one is in
        # flight at the same time.
        both.wait()
        return _wrote(_repository_of(project_dir), output_dir, ecosystem)

    _resolves(resolve, monkeypatch)
    _downloaded({
        'a': {'composer.json': MANIFEST['composer.json']},
        'b': {'Gemfile': MANIFEST['Gemfile']},
    })

    result = lock('--workers', '2')

    assert result.exit_code == 0, result.output
    assert (_lock_dir('a') / 'composer.lock').read_text() == 'resolved for a\n'
    assert (_lock_dir('b') / 'Gemfile.lock').read_text() == 'resolved for b\n'
    assert sorted(
        p.name for p in _lock_dir(
            'a',
        ).iterdir()
    ) == ['composer.lock']
    assert sorted(p.name for p in _lock_dir('b').iterdir()) == ['Gemfile.lock']
    assert 'resolved 2 · cached 0 · failed 0' in _said(result)


def test_one_resolution_at_a_time_by_default(workdir, monkeypatch):
    """Each is a container of up to --memory and --cpus: running several
    at once is a decision, as it was before `--workers`."""
    running = most = 0
    guard = threading.Lock()

    def resolve(
        ecosystem: str, project_dir: Path, output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        nonlocal running, most
        with guard:
            running += 1
            most = max(most, running)
        time.sleep(0.05)
        with guard:
            running -= 1
        return _wrote(_repository_of(project_dir), output_dir, ecosystem)

    _resolves(resolve, monkeypatch)
    _downloaded({
        'a': {
            'composer.json': MANIFEST['composer.json'],
            'docs/Gemfile': MANIFEST['Gemfile'],
        },
        'b': {'Gemfile': MANIFEST['Gemfile']},
    })

    result = lock()

    assert result.exit_code == 0, result.output
    assert 'resolved 3 · cached 0 · failed 0' in _said(result)
    assert most == 1


def test_a_failure_leaves_the_other_resolutions_alone(workdir, monkeypatch):
    """Composer and Bundler in one directory, and a third resolution
    elsewhere, all at once. Bundler's fails after Composer's has
    written: what Composer resolved stays. The failure elsewhere leaves
    no directory behind for `sbom generate`."""
    everyone = threading.Barrier(3, timeout=10)
    composer_wrote = threading.Event()

    def resolve(
        ecosystem: str, project_dir: Path, output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        everyone.wait()
        name = _repository_of(project_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        if (name, ecosystem) == ('a', 'composer'):
            written = _wrote(name, output_dir, ecosystem)
            composer_wrote.set()
            return written
        composer_wrote.wait(10)
        return LockResult(produced=(), returncode=1, stderr='no resolution')

    _resolves(resolve, monkeypatch)
    _downloaded({
        'a': {
            'composer.json': MANIFEST['composer.json'],
            'Gemfile': MANIFEST['Gemfile'],
        },
        'b': {'composer.json': MANIFEST['composer.json']},
    })

    result = lock('--workers', '3')

    assert result.exit_code == 0, result.output
    assert 'resolved 1 · cached 0 · failed 2' in _said(result)
    assert sorted(
        p.name for p in _lock_dir(
            'a',
        ).iterdir()
    ) == ['composer.lock']
    assert (_lock_dir('a') / 'composer.lock').read_text() == 'resolved for a\n'
    assert not _lock_dir('b').exists()


def test_an_interrupt_stops_every_resolution_in_flight(workdir, monkeypatch):
    """Ctrl-C reaches the main thread alone. The resolutions running in
    the other workers are told, and stop, each removing its container
    (sandbox_test), before the command ends; they are not left to run
    to their deadline."""
    b_running = threading.Event()
    told: list[bool] = []

    def resolve(
        ecosystem: str, project_dir: Path, output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        if _repository_of(project_dir) == 'a':
            b_running.wait(10)
            raise KeyboardInterrupt
        b_running.set()
        told.append(cancel is not None and cancel.wait(10))
        return LockResult(produced=(), returncode=130, stderr='cancelled')

    _resolves(resolve, monkeypatch)
    _downloaded({
        'a': {'composer.json': MANIFEST['composer.json']},
        'b': {'composer.json': MANIFEST['composer.json']},
    })

    result = lock('--workers', '2')

    assert result.exit_code == 130, result.output
    assert told == [True], 'the other resolution was never told to stop'


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


def generate() -> Any:
    return runner.invoke(app, ['sbom', 'generate'])


@pytest.mark.parametrize('ecosystem', ['composer', 'gem'])
def test_a_committed_lockfile_is_what_syft_scans(syft, ecosystem):
    """The resolved copy was merged over it, and Syft reported the
    versions the registry offered on the day `sbom lock` ran rather
    than the ones the project pins."""
    manifest, lockfile, committed, resolved = ECOSYSTEMS[ecosystem]
    _downloaded({
        'a': {manifest: MANIFEST[manifest], lockfile: committed},
    })
    _resolved('a', {lockfile: resolved})

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {manifest: MANIFEST[manifest], lockfile: committed},
    ]


def test_a_resolved_lockfile_is_merged_where_none_was_committed(syft):
    """What `sbom lock` is for, and what the rest must leave working."""
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})
    _resolved('b', {'composer.lock': RESOLVED})

    result = generate()

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
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})
    (_resolved('b', {}) / 'composer.lock').symlink_to(elsewhere)

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [{'composer.json': MANIFEST['composer.json']}]


def test_only_what_the_recipe_declares_is_merged(syft):
    """Anything else in the lock directory was merged as well, so a
    hostile resolver could add packages to the SBOM by leaving another
    ecosystem's lockfile there."""
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})
    _resolved(
        'b', {
            'composer.lock': RESOLVED,
            'package-lock.json': '{"packages": {}}\n',
        },
    )

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {
            'composer.json': MANIFEST['composer.json'],
            'composer.lock': RESOLVED,
        },
    ]


@pytest.mark.parametrize(
    'manifest,leftover', [
        ('pom.xml', 'dependency-tree.txt'),
        ('requirements.txt', 'requirements.lock'),
    ],
)
def test_what_a_withdrawn_recipe_left_is_not_merged(
    syft, manifest, leftover,
):
    """Earlier runs left these on disk. Syft never read them, so the
    scan is the project's own tree, as it would have been without."""
    _downloaded({'b': {manifest: MANIFEST[manifest]}})
    _resolved('b', {leftover: 'resolved\n'})

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [{manifest: MANIFEST[manifest]}]


def test_a_resolved_lockfile_is_merged_at_its_own_directory(syft):
    """Merged at the root, a lockfile resolved for `backend/` would
    describe the root, and one resolved for each of two directories
    would overwrite the other."""
    _downloaded({
        'b': {
            'package.json': '{}\n',
            'backend/composer.json': MANIFEST['composer.json'],
            'api/composer.json': MANIFEST['composer.json'],
        },
    })
    _resolved('b', {'composer.lock': RESOLVED}, 'backend')
    _resolved('b', {'composer.lock': COMMITTED}, 'api')

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {
            'api/composer.json': MANIFEST['composer.json'],
            'api/composer.lock': COMMITTED,
            'backend/composer.json': MANIFEST['composer.json'],
            'backend/composer.lock': RESOLVED,
            'package.json': '{}\n',
        },
    ]


def test_a_resolved_lockfile_for_a_directory_that_ships_one_is_not_merged(
    syft,
):
    _downloaded({
        'b': {
            'backend/composer.json': MANIFEST['composer.json'],
            'backend/composer.lock': COMMITTED,
        },
    })
    _resolved('b', {'composer.lock': RESOLVED}, 'backend')

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scans == [
        {
            'backend/composer.json': MANIFEST['composer.json'],
            'backend/composer.lock': COMMITTED,
        },
    ]


def test_a_new_resolution_makes_the_sbom_stale(syft):
    """An SBOM is current only while it is newer than every file it was
    generated from, the generated lockfiles included."""
    _downloaded({'b': {'composer.json': MANIFEST['composer.json']}})
    assert generate().exit_code == 0
    assert generate().exit_code == 0
    assert len(syft.scans) == 1, 'unchanged, so not scanned again'

    lock_dir = _resolved('b', {'composer.lock': RESOLVED})
    later = time.time() + 5
    os.utime(lock_dir / 'composer.lock', (later, later))

    assert generate().exit_code == 0
    assert len(syft.scans) == 2
    assert syft.scans[-1]['composer.lock'] == RESOLVED
