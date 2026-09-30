"""Every git the collector starts (#47).

Watched from outside: a `git` of the test's own comes first on PATH,
writes down how it was started, then runs the real one, with github.com
replaced by repositories on disk. So these see what `ps` would, whether
GitPython or `subprocess` started it, and nothing reaches the network.

What they hold every git to:

- no token on a command line, where any user of the machine reads it in
  `/proc/<pid>/cmdline`; git's environment carries it, as a header;
- no prompt for credentials, and no user or system config;
- a time limit;
- `--end-of-options` before anything that came from data, so that a tag
  or a commit spelled like an option is taken for a name. `git fetch
  --upload-pack=<command>` runs that command, and `git archive
  --output=<path>` writes wherever it says.

The OpenAPI tools' clones are held to the same, by this harness, where
the research tools' tests are (tests/research/openapi_clone_test.py,
#167).
"""
import io
import os
import shutil
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from chatsbom.services import git_service
from chatsbom.services.git_service import GitService

REAL_GIT = shutil.which('git') or 'git'
TOKEN = 'ghp_secret'
HEADER = 'http.https://github.com/.extraheader'

#: The `git` first on PATH. Fields are separated by \x1f, and the
#: environment follows the arguments after \x1e. `FAKE_GIT_HANG` names a
#: git command to hang in, for the time limits.
WRAPPER = r"""#!/bin/sh
{
    printf 'git'
    for a in "$@"; do printf '\037%s' "$a"; done
    printf '\036%s\037%s\037%s\037%s\n' \
        "$GIT_TERMINAL_PROMPT" "$GIT_CONFIG_GLOBAL" "$GIT_CONFIG_NOSYSTEM" \
        "$GIT_CONFIG_KEY_0"
} >> "$FAKE_GIT_LOG"
if [ -n "$FAKE_GIT_HANG" ]; then
    for a in "$@"; do
        if [ "$a" = "$FAKE_GIT_HANG" ]; then exec sleep 60; fi
    done
fi
for a in "$@"; do
    shift
    case "$a" in
        https://github.com/* | https://*@github.com/*)
            a="file://$FAKE_GITHUB/${a#*github.com/}" ;;
    esac
    set -- "$@" "$a"
done
exec "$FAKE_GIT_REAL" "$@"
"""


@dataclass(frozen=True)
class Call:
    """One git, as it was started."""
    argv: list[str]
    prompt: str
    global_config: str
    no_system: str
    config_key: str

    @property
    def command(self) -> str:
        """The git command: the first argument that is neither an option
        nor the directory `-C` or `--git-dir` names."""
        rest = iter(self.argv)
        for arg in rest:
            if arg in ('-C', '--git-dir'):
                next(rest, None)
            elif not arg.startswith('-'):
                return arg
        return ''

    def after_end_of_options(self, value: str) -> bool:
        return (
            '--end-of-options' in self.argv and value in self.argv
            and self.argv.index('--end-of-options') < self.argv.index(value)
        )


class FakeGitHub:
    """github.com, as repositories on disk, and a log of every git."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.log = root / 'git.log'

    def calls(self) -> list[Call]:
        if not self.log.exists():
            return []
        calls = []
        # Not `splitlines`, which breaks at \x1e too.
        for line in filter(None, self.log.read_text().split('\n')):
            argv, _, env = line.partition('\x1e')
            calls.append(Call(argv.split('\x1f')[1:], *env.split('\x1f')))
        return calls

    def repository(self, owner: str, repo: str, **files: str) -> Path:
        """`owner/repo` on "github.com", with `files` committed on
        `main`; tagged `V3.0.0`."""
        work = self.root / 'github' / owner / f'{repo}.git'
        work.mkdir(parents=True)
        git(work, 'init', '--quiet', '-b', 'main')
        # What github.com allows, and a local server does not unasked.
        git(work, 'config', 'uploadpack.allowFilter', 'true')
        git(work, 'config', 'uploadpack.allowAnySHA1InWant', 'true')
        self.commit(work, **files)
        git(work, 'tag', 'V3.0.0')
        return work

    def commit(self, work: Path, **files: str) -> str:
        for name, text in files.items():
            path = work / name.replace('__', '/')
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        git(work, 'add', '--all')
        git(work, 'commit', '--quiet', '-m', 'files')
        return git(work, 'rev-parse', 'HEAD')


def git(cwd: Path, *args: str) -> str:
    """The real git, on the fixtures themselves."""
    env = {
        **os.environ,
        'GIT_AUTHOR_NAME': 'a', 'GIT_AUTHOR_EMAIL': 'a@example.com',
        'GIT_COMMITTER_NAME': 'a', 'GIT_COMMITTER_EMAIL': 'a@example.com',
        'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1',
    }
    return subprocess.run(
        [REAL_GIT, *args], cwd=cwd, env=env, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def github(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeGitHub:
    fake = FakeGitHub(tmp_path)
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    wrapper = bin_dir / 'git'
    wrapper.write_text(WRAPPER)
    wrapper.chmod(0o755)
    monkeypatch.setenv('PATH', f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv('FAKE_GIT_LOG', str(fake.log))
    monkeypatch.setenv('FAKE_GIT_REAL', REAL_GIT)
    monkeypatch.setenv('FAKE_GITHUB', str(tmp_path / 'github'))
    # Whatever the wrapper misses stays on the machine.
    monkeypatch.setenv('GIT_ALLOW_PROTOCOL', 'file')
    # What the code has to set itself, not find already set, as it is on
    # some machines (this one's sandbox sets GIT_TERMINAL_PROMPT).
    for name in (
        'GIT_TERMINAL_PROMPT', 'GIT_CONFIG_GLOBAL', 'GIT_CONFIG_NOSYSTEM',
        'GIT_CONFIG_COUNT', 'GIT_CONFIG_KEY_0', 'GIT_CONFIG_VALUE_0',
        'GIT_ASKPASS', 'SSH_ASKPASS', 'FAKE_GIT_HANG',
    ):
        monkeypatch.delenv(name, raising=False)
    # `~/.repositories`, the OpenAPI clones' cache, and `~/.gitconfig`.
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))
    return fake


def assert_quiet(calls: list[Call]) -> None:
    assert calls
    for call in calls:
        assert TOKEN not in ' '.join(call.argv), call.argv
        assert call.prompt == '0', call.argv
        assert call.global_config == os.devnull, call.argv
        assert call.no_system == '1', call.argv


# --- the collector's git ------------------------------------------------------

def test_refs_are_listed_with_the_token_in_the_environment(github):
    github.repository('acme', 'shop', **{'app.py': 'print(1)\n'})

    refs, _ = GitService(token=TOKEN).get_repo_refs('acme', 'shop')

    assert 'refs/tags/V3.0.0' in refs
    [call] = github.calls()
    assert call.command == 'ls-remote'
    assert_quiet([call])
    assert call.config_key == HEADER


def test_the_tree_is_listed_with_the_token_in_the_environment(github):
    work = github.repository('acme', 'shop', **{'api__openapi.yaml': 'x'})
    sha = git(work, 'rev-parse', 'HEAD')

    files = GitService(token=TOKEN).get_repository_tree('acme', 'shop', sha)

    assert files == ['api/openapi.yaml']
    calls = github.calls()
    assert_quiet(calls)
    network = [c for c in calls if c.command in ('clone', 'fetch')]
    assert network and all(c.config_key == HEADER for c in network)
    for call in calls:
        if sha in call.argv:
            assert call.after_end_of_options(sha), call.argv


def test_a_commit_spelled_like_an_option_is_not_one(github, tmp_path):
    github.repository('acme', 'shop', **{'app.py': 'print(1)\n'})
    marker = tmp_path / 'ran'

    files = GitService().get_repository_tree(
        'acme', 'shop', f'--upload-pack=touch {marker}',
    )

    assert files is None
    assert not marker.exists()


def test_the_default_branch_is_asked_anonymously_and_quietly(github):
    work = github.repository('acme', 'shop', **{'app.py': 'print(1)\n'})

    head = GitService(token=TOKEN).default_branch_head('acme', 'shop')

    assert head == ('main', git(work, 'rev-parse', 'HEAD'))
    [call] = github.calls()
    assert_quiet([call])
    assert call.config_key != HEADER


HUNG: list[tuple[str, str, Callable[[GitService], object], object]] = [
    (
        'LS_REMOTE_TIMEOUT', 'ls-remote',
        lambda s: s.get_repo_refs('acme', 'shop')[0], {},
    ),
    (
        'LS_REMOTE_TIMEOUT', 'ls-remote',
        lambda s: s.default_branch_head('acme', 'shop'), None,
    ),
    (
        'TREE_FETCH_TIMEOUT', 'clone',
        lambda s: s.get_repository_tree('acme', 'shop', 'a' * 40), None,
    ),
]


def ps_run_to_its_end(args: list[str], stdout: int) -> SimpleNamespace:
    """`subprocess.Popen`, as GitPython's `kill_after_timeout` uses it.

    To stop a git that has run out its time, GitPython lists the git's
    children with `ps`, reads what it prints, and never waits for it or
    closes its pipe (`kill_process`, git/cmd.py, 3.1.62): an unreaped
    process and an open file, which the suite fails on. Here `ps` is
    run to its end, and what it printed is handed over already read.
    """
    finished = subprocess.run(args, stdout=stdout, check=False)
    return SimpleNamespace(stdout=io.BytesIO(finished.stdout))


@pytest.mark.parametrize(
    'limit,command,ask,failed', HUNG, ids=['refs', 'head', 'tree'],
)
def test_a_git_that_hangs_is_stopped(
    github, monkeypatch, limit, command, ask, failed,
):
    github.repository('acme', 'shop', **{'app.py': 'print(1)\n'})
    monkeypatch.setattr(git_service, limit, 1)
    monkeypatch.setenv('FAKE_GIT_HANG', command)
    # The one `Popen` git/cmd.py looks up when it runs: git itself is
    # started through `safer_popen`, bound when GitPython is imported.
    monkeypatch.setattr('git.cmd.Popen', ps_run_to_its_end)

    started = time.monotonic()
    answer = ask(GitService(token=TOKEN))

    assert time.monotonic() - started < 20
    assert answer == failed
