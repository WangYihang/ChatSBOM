"""Stand-ins for what the collector's stages read besides GitHub's API
(#161): git, raw content, and Syft.

No test reaches github.com, raw.githubusercontent.com or a Syft it did
not write:

- **git** (`Upstream`): bare repositories on disk, one per repository,
  at `<root>/<owner>/<name>.git`, reached as `file://`, with the
  commits, branches and tags a test makes. They serve `git ls-remote`,
  the tag fetch and the tree's clone as github.com serves them to the
  stages, and allow what those ask for: filters, and any commit by its
  sha. Each repository is served by the API stand-in too
  (`tests/fake_github_test.py`), its push, HEAD and releases kept in
  step with what git has.
- **raw content** (`FakeRaw`): `raw.githubusercontent.com`, as an ASGI
  app reached through `httpx2.ASGITransport`, answering
  `/<owner>/<name>/<sha>/<path>` with the file git has at that commit,
  or a status a test scripts. It records every request, with its
  headers, so that a test can hold the client to asking without a
  token.
- **Syft** (`FakeSyft`): a script that answers `syft version -o json`
  and `syft dir:<path> -o json` in Syft's shape, with a version, a delay,
  an allocation, an exit status and stderr a test sets. It logs every
  scan with how many were running at once.

`TestTheStandIns` holds them to what the stages count on.
"""
from __future__ import annotations

import asyncio
import json
import os
import stat
import subprocess
import sys
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx2
import pytest

from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Release
from tests.fake_github_test import Repo

Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

#: Where the raw stand-in says it is.
RAW = 'https://raw.githubusercontent.com'

#: The Syft the stand-in says it is, unless told otherwise.
SYFT_VERSION = '1.52.0'


def syft_document(project: str = 'a', version: str = SYFT_VERSION) -> str:
    """What `syft dir:<project> -o json` prints, trimmed to the keys that
    anything reads. It is compact, on one line and ends in a newline, as
    Syft writes it, and its keys come in Syft's order. `project` names
    the scan, so a test can tell which one produced a file, and
    `version` is the Syft its descriptor says wrote it."""
    return json.dumps(
        {
            'artifacts': [{
                'id': f'{project}-requests',
                'name': 'requests',
                'version': '2.31.0',
                'type': 'python',
                'foundBy': 'python-package-cataloger',
                'purl': 'pkg:pypi/requests@2.31.0',
            }],
            'artifactRelationships': [],
            'source': {
                'id': project,
                'name': project,
                'type': 'directory',
                'metadata': {'path': project},
            },
            'distro': {},
            'descriptor': {'name': 'syft', 'version': version},
            'schema': {
                'version': '16.1.10',
                'url': 'https://raw.githubusercontent.com/anchore/syft/'
                'main/schema/json/schema-16.1.10.json',
            },
        },
        separators=(',', ':'),
    ) + '\n'


def cut_short(document: str) -> str:
    """`document` as a write killed partway through left it."""
    return document[:document.index('"descriptor"')]


# -- git ---------------------------------------------------------------------


def git(cwd: Path, *args: str, date: str = '2026-09-01T00:00:00+00:00') -> str:
    """git, as a person runs it: the user's and the system's config left
    out, and every date `date`."""
    env = {
        **os.environ,
        'GIT_AUTHOR_NAME': 'a', 'GIT_AUTHOR_EMAIL': 'a@example.com',
        'GIT_COMMITTER_NAME': 'a', 'GIT_COMMITTER_EMAIL': 'a@example.com',
        'GIT_AUTHOR_DATE': date, 'GIT_COMMITTER_DATE': date,
        'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1',
    }
    return subprocess.run(
        ['git', *args], cwd=cwd, env=env, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


@dataclass
class Repository:
    """One repository, in git, in the API stand-in and in raw content."""

    upstream: Upstream
    repo: Repo
    #: Its bare repository, which the stages clone and list.
    bare: Path
    #: Where commits are made, then pushed to `bare`.
    work: Path
    #: Every file at each commit, by the commit's sha.
    files_at: dict[str, dict[str, bytes]] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return self.repo.full_name

    @property
    def id(self) -> int:
        return self.repo.id

    def commit(
        self,
        files: Mapping[str, bytes | str | None],
        *,
        date: str = '2026-09-01T00:00:00+00:00',
        branch: str | None = None,
    ) -> str:
        """A commit on `branch` (the default one unless named) that
        writes `files`, or deletes those given as None; pushed, and its
        sha. The API stand-in's push and HEAD follow a commit on the
        default branch."""
        branch = branch or self.repo.default_branch
        current = git(self.work, 'symbolic-ref', '--short', 'HEAD')
        if branch != current:
            exists = subprocess.run(
                [
                    'git', 'rev-parse', '--verify',
                    '--quiet', f'refs/heads/{branch}',
                ],
                cwd=self.work, capture_output=True,
            ).returncode == 0
            git(
                self.work, 'checkout', '--quiet',
                *(() if exists else ('-b',)), branch,
            )
        for path, body in files.items():
            target = self.work.joinpath(*path.split('/'))
            if body is None:
                target.unlink()
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(
                body.encode() if isinstance(body, str) else body,
            )
        git(self.work, 'add', '--all')
        git(
            self.work, 'commit', '--quiet', '--allow-empty', '-m', 'change',
            date=date,
        )
        sha = git(self.work, 'rev-parse', 'HEAD')
        git(self.work, 'push', '--quiet', '--force', 'origin', branch)
        tracked = git(self.work, 'ls-files', '-z')
        self.files_at[sha] = {
            path: self.work.joinpath(*path.split('/')).read_bytes()
            for path in tracked.split('\0') if path
        }
        if branch == self.repo.default_branch:
            self.pushed(date)
            self.repo.head = sha
        return sha

    def tag(
        self, name: str, sha: str | None = None, *, annotated: bool = False,
        date: str = '2026-09-01T00:00:00+00:00',
    ) -> None:
        """A tag of `sha`, HEAD unless given, pushed; moved if it is
        there."""
        options = ['-a', '-m', name] if annotated else []
        git(
            self.work, 'tag', '--force', *options, name, sha or 'HEAD',
            date=date,
        )
        git(
            self.work, 'push', '--quiet', '--force',
            'origin', f'refs/tags/{name}',
        )

    def release(
        self, tag: str, published_at: str = '2026-09-01T00:00:00Z', *,
        prerelease: bool = False, draft: bool = False,
    ) -> None:
        """A GitHub release of `tag`, the newest."""
        self.repo.releases.insert(
            0, Release(tag, published_at, prerelease=prerelease, draft=draft),
        )

    def pushed(self, at: str) -> None:
        """GitHub says it was pushed `at`: the API stand-in's pushedAt."""
        self.repo.pushed_at = at.replace('+00:00', 'Z')


class Upstream:
    """Repositories, served by git on disk, the API stand-in and the raw
    stand-in alike."""

    def __init__(self, root: Path, github: FakeGitHub) -> None:
        self.root = root
        self.github = github
        self.raw = FakeRaw(self)
        self.repositories: dict[str, Repository] = {}

    @property
    def git_base(self) -> str:
        """What stands in for `https://github.com`: `<base>/<owner>/
        <name>.git` is a repository."""
        return f'file://{self.root}'

    def add(
        self, repository_id: int, full_name: str, *,
        default_branch: str = 'main', stars: int = 1_000,
    ) -> Repository:
        owner, name = full_name.split('/')
        bare = self.root / owner / f'{name}.git'
        bare.mkdir(parents=True)
        git(bare, 'init', '--quiet', '--bare', '-b', default_branch)
        # What the stages ask of github.com: filters, and a commit by its
        # sha whatever points at it.
        git(bare, 'config', 'uploadpack.allowFilter', 'true')
        git(bare, 'config', 'uploadpack.allowAnySHA1InWant', 'true')
        work = self.root / '.work' / owner / name
        work.mkdir(parents=True)
        git(work, 'init', '--quiet', '-b', default_branch)
        git(work, 'remote', 'add', 'origin', str(bare))
        repo = self.github.add(
            Repo(
                repository_id, owner, name, stars=stars,
                default_branch=default_branch, head='0' * 40,
            ),
        )
        repository = Repository(self, repo, bare, work)
        self.repositories[full_name.lower()] = repository
        return repository

    def file(self, full_name: str, sha: str, path: str) -> bytes | None:
        repository = self.repositories.get(full_name.lower())
        if repository is None:
            return None
        return repository.files_at.get(sha, {}).get(path)


# -- raw content -------------------------------------------------------------


@dataclass(frozen=True)
class RawRequest:
    path: str
    #: Lower-cased.
    headers: dict[str, str]
    status: int


class FakeRaw:
    """`raw.githubusercontent.com`, for what `Upstream` holds."""

    def __init__(self, upstream: Upstream) -> None:
        self.upstream = upstream
        self.requests: list[RawRequest] = []
        #: A status to answer in place of a file, by its path in the
        #: repository, for as many requests as its count says.
        self.statuses: dict[str, list[int]] = {}
        #: Set, a `Content-Length` is sent with each file.
        self.declare = True

    def transport(self) -> httpx2.ASGITransport:
        return httpx2.ASGITransport(app=self)

    def fail(self, path: str, *statuses: int) -> None:
        """Answer `path` with each of `statuses` in turn, then the file."""
        self.statuses.setdefault(path, []).extend(statuses)

    async def __call__(
        self, scope: Message, receive: Receive, send: Send,
    ) -> None:
        if scope['type'] != 'http':
            return
        headers = {
            name.decode('latin-1').lower(): value.decode('latin-1')
            for name, value in scope['headers']
        }
        owner, name, sha, quoted = scope['path'].lstrip('/').split('/', 3)
        path = unquote(quoted)
        scripted = self.statuses.get(path)
        body: bytes | None = None
        if scripted:
            status = scripted.pop(0)
        else:
            body = self.upstream.file(f'{owner}/{name}', sha, path)
            status = 404 if body is None else 200
        self.requests.append(RawRequest(path, headers, status))
        sent = [(b'content-type', b'text/plain; charset=utf-8')]
        if body is not None and self.declare:
            sent.append((b'content-length', str(len(body)).encode()))
        await send({
            'type': 'http.response.start', 'status': status, 'headers': sent,
        })
        await send({'type': 'http.response.body', 'body': body or b''})


# -- Syft --------------------------------------------------------------------

#: The stand-in, run by the interpreter the tests run on.
_SYFT = '''\
"""A stand-in for Syft (tests/fake_upstream_test.py)."""
import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONFIG = json.loads((HERE / 'syft.json').read_text())


def scan(target):
    running = HERE / 'running'
    running.mkdir(exist_ok=True)
    marker = running / str(os.getpid())
    marker.touch()
    try:
        with open(HERE / 'syft.log', 'a') as log:
            log.write(json.dumps({
                'scan': str(target), 'pid': os.getpid(),
                'running': len(list(running.iterdir())),
                'env': {
                    name: os.environ.get(name)
                    for name in ('GOMEMLIMIT', 'SYFT_CHECK_FOR_APP_UPDATE')
                },
            }) + '\\n')
        if CONFIG.get('allocate'):
            held = bytearray(CONFIG['allocate'])
        time.sleep(CONFIG.get('delay', 0))
        if CONFIG.get('exit'):
            sys.stderr.write(CONFIG.get('stderr', 'syft failed') + '\\n')
            return CONFIG['exit']
        if CONFIG.get('output') is not None:
            sys.stdout.write(CONFIG['output'])
            return 0
        files = sorted(
            path.relative_to(target).as_posix()
            for path in target.rglob('*') if path.is_file()
        )
        document = {
            'artifacts': [
                {
                    'id': f'{index}', 'name': path, 'version': '1.0.0',
                    'type': 'npm', 'foundBy': 'stand-in',
                    'purl': f'pkg:npm/{index}@1.0.0',
                    'locations': [{'path': '/' + path}],
                }
                for index, path in enumerate(files)
            ],
            'artifactRelationships': [],
            'source': {
                'id': str(target), 'name': str(target),
                'type': 'directory', 'metadata': {'path': str(target)},
            },
            'distro': {},
            'descriptor': {'name': 'syft', 'version': CONFIG['version']},
            'schema': {'version': '16.1.10', 'url': 'https://example.com'},
        }
        sys.stdout.write(json.dumps(document, separators=(',', ':')) + '\\n')
        return 0
    finally:
        marker.unlink(missing_ok=True)


def main(argv):
    if argv[:1] == ['version']:
        if CONFIG.get('no_version'):
            return 1
        said = {'application': 'syft', 'version': CONFIG['version']}
        print(json.dumps(said))
        return 0
    if not argv or not argv[0].startswith('dir:'):
        sys.stderr.write(f'unknown command: {argv}\\n')
        return 2
    return scan(Path(argv[0].removeprefix('dir:')))


sys.exit(main(sys.argv[1:]))
'''


class FakeSyft:
    """A Syft of the test's own, in `directory`, as `syft`: put the
    directory first on PATH, or name `path`."""

    def __init__(self, directory: Path, version: str = SYFT_VERSION) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.path = directory / 'syft'
        self.path.write_text(f'#!{sys.executable}\n{_SYFT}', encoding='utf-8')
        self.path.chmod(self.path.stat().st_mode | stat.S_IXUSR)
        self.configure(version=version)

    def configure(
        self, *, version: str | None = None, delay: float = 0.0,
        allocate: int = 0, exit: int = 0, stderr: str = '',
        output: str | None = None, no_version: bool = False,
    ) -> None:
        """How it answers from now on: the version it says it is; how
        long a scan takes, and how many bytes it holds; the status it
        exits with and what it says on stderr then; what it writes in
        place of a document; and, with `no_version`, no version at all."""
        config_path = self.directory / 'syft.json'
        kept = (
            json.loads(config_path.read_text()) if config_path.exists() else {}
        )
        config_path.write_text(
            json.dumps({
                'version': version or kept.get('version', SYFT_VERSION),
                'delay': delay, 'allocate': allocate, 'exit': exit,
                'stderr': stderr, 'output': output, 'no_version': no_version,
            }),
            encoding='utf-8',
        )

    @property
    def scans(self) -> list[dict[str, Any]]:
        """Every scan so far, in the order they started."""
        log = self.directory / 'syft.log'
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    @property
    def peak(self) -> int:
        """The most scans running at once."""
        return max((scan['running'] for scan in self.scans), default=0)


# -- the stand-ins, held to what the stages count on --------------------------


@pytest.fixture
def upstream(tmp_path: Path) -> Upstream:
    return Upstream(tmp_path / 'github.com', FakeGitHub())


class TestTheStandIns:
    def test_git_serves_the_listing_the_stages_read(self, upstream):
        repository = upstream.add(1, 'octo/one')
        first = repository.commit({'package.json': '{}'})
        repository.tag('v1.0.0', annotated=True)
        second = repository.commit({'go.mod': 'module x\n'})
        listing = git(
            upstream.root, 'ls-remote', '--symref',
            f'{upstream.git_base}/octo/one.git',
        )
        assert 'ref: refs/heads/main\tHEAD' in listing
        assert f'{second}\trefs/heads/main' in listing
        assert f'{first}\trefs/tags/v1.0.0^{{}}' in listing
        assert upstream.github.repos[1].head == second

    def test_the_api_stand_in_follows_a_push_to_the_default_branch(
        self, upstream,
    ):
        repository = upstream.add(1, 'octo/one')
        repository.commit({'a': 'a'}, date='2026-09-02T03:04:05+00:00')
        assert upstream.github.repos[1].pushed_at == '2026-09-02T03:04:05Z'
        head = upstream.github.repos[1].head
        repository.commit({'b': 'b'}, branch='feature')
        assert upstream.github.repos[1].head == head

    def test_raw_serves_each_commits_files_and_404_otherwise(self, upstream):
        repository = upstream.add(1, 'octo/one')
        first = repository.commit({'app/package.json': '{"v": 1}'})
        repository.commit({'app/package.json': '{"v": 2}'})

        async def fetch(path: str) -> httpx2.Response:
            async with httpx2.AsyncClient(
                transport=upstream.raw.transport(), base_url=RAW,
            ) as client:
                return await client.get(path)

        found = asyncio.run(fetch(f'/octo/one/{first}/app/package.json'))
        assert (found.status_code, found.content) == (200, b'{"v": 1}')
        assert found.headers['content-length'] == '8'
        missing = asyncio.run(fetch(f'/octo/one/{first}/go.mod'))
        assert missing.status_code == 404
        upstream.raw.fail('go.mod', 502)
        assert asyncio.run(
            fetch(f'/octo/one/{first}/go.mod'),
        ).status_code == 502
        assert [r.status for r in upstream.raw.requests] == [200, 404, 502]

    def test_syft_answers_its_version_and_a_scan_in_syfts_shape(
        self, tmp_path,
    ):
        syft = FakeSyft(tmp_path / 'bin', version='9.9.9')
        root = tmp_path / 'root'
        (root / 'app').mkdir(parents=True)
        (root / 'app' / 'package.json').write_text('{}')
        version = subprocess.run(
            [str(syft.path), 'version', '-o', 'json'],
            capture_output=True, text=True, check=True,
        )
        assert json.loads(version.stdout)['version'] == '9.9.9'
        scanned = subprocess.run(
            [str(syft.path), f'dir:{root}', '-o', 'json'],
            capture_output=True, text=True, check=True,
        )
        document = json.loads(scanned.stdout)
        assert list(document) == [
            'artifacts', 'artifactRelationships', 'source', 'distro',
            'descriptor', 'schema',
        ]
        assert document['descriptor'] == {'name': 'syft', 'version': '9.9.9'}
        assert [a['name'] for a in document['artifacts']] == [
            'app/package.json',
        ]
        assert syft.scans[0]['scan'] == str(root)

    def test_syft_fails_as_told(self, tmp_path):
        syft = FakeSyft(tmp_path / 'bin')
        syft.configure(exit=3, stderr='fatal error: out of memory')
        failed = subprocess.run(
            [str(syft.path), f'dir:{tmp_path}', '-o', 'json'],
            capture_output=True, text=True,
        )
        assert failed.returncode == 3
        assert failed.stderr.strip() == 'fatal error: out of memory'
