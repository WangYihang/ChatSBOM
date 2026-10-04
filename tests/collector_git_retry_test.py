"""git's transient network failures, asked again in place (#189).

What git says, classified: the transient ones are what the live
collector's git said in October 2026, and what git and curl say of the
same troubles; the permanent ones, and anything not recognised, are not
asked again.
"""
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import pytest
import structlog.testing

from chatsbom.collector import gitremote
from chatsbom.collector.gitremote import GitFailed
from chatsbom.collector.gitremote import transient

URL = "'https://github.com/acme/shop.git/'"

TRANSIENT = [
    # Seen on the live collector, 2026-10-01..04.
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Could not resolve host: github.com',
    f'git fetch exited 128: fatal: unable to access {URL}: '
    'Could not resolve host: github.com\nfatal: could not fetch '
    f'{"a" * 40} from promisor remote',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Error in the HTTP2 framing layer',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Recv failure: Connection reset by peer',
    # What curl and git say of the same troubles.
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Failed to connect to github.com port 443 after 129 ms: '
    'Connection refused',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Failed to connect to github.com port 443 after 133994 ms: '
    "Couldn't connect to server",
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'Connection timed out after 300000 milliseconds',
    f'git fetch exited 128: fatal: unable to access {URL}: '
    'Operation timed out after 300000 milliseconds with 0 out of 0 '
    'bytes received',
    f'git clone exited 128: fatal: unable to access {URL}: '
    'SSL connection timeout',
    f'git clone exited 128: fatal: unable to access {URL}: '
    'gnutls_handshake() failed: The TLS connection was non-properly '
    'terminated.',
    f'git clone exited 128: fatal: unable to access {URL}: '
    'OpenSSL SSL_connect: SSL_ERROR_SYSCALL in connection to '
    'github.com:443',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'The requested URL returned error: 502',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'The requested URL returned error: 503',
    'git clone exited 128: error: RPC failed; HTTP 504 curl 22 The '
    'requested URL returned error: 504\nfatal: expected flush after ref '
    'listing',
    'git clone exited 128: error: RPC failed; curl 92 HTTP/2 stream 5 '
    'was not closed cleanly: CANCEL (err 8)\nerror: 1234 bytes of body '
    'are still expected\nfetch-pack: unexpected disconnect while reading '
    'sideband packet\nfatal: early EOF\nfatal: fetch-pack: invalid '
    'index-pack output',
    'git fetch exited 128: error: RPC failed; curl 56 GnuTLS recv error '
    '(-54): Error in the pull function.\nfatal: the remote end hung up '
    'unexpectedly',
    'git fetch exited 128: fatal: early EOF',
]

PERMANENT = [
    # A repository gone, or never there.
    'git ls-remote exited 128: remote: Repository not found.\n'
    f'fatal: repository {URL} not found',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'The requested URL returned error: 404',
    # Authentication: no pause mends a token GitHub does not take.
    'git ls-remote exited 128: remote: Invalid username or password.\n'
    f'fatal: Authentication failed for {URL}',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'The requested URL returned error: 401',
    f'git ls-remote exited 128: fatal: unable to access {URL}: '
    'The requested URL returned error: 403',
    'git ls-remote exited 128: fatal: could not read Username for '
    "'https://github.com': terminal prompts disabled",
    # A ref that is not there.
    'git fetch exited 128: fatal: couldn\'t find remote ref v9.9.9',
    'git fetch exited 128: fatal: remote error: upload-pack: not our '
    f'ref {"a" * 40}',
    # A repository on disk that is not one.
    "git ls-remote exited 128: fatal: '/nowhere/acme/shop.git' does not "
    'appear to be a git repository\nfatal: Could not read from remote '
    'repository.',
    # Anything else.
    'git ls-remote exited 129: error: unknown option `frobnicate\'',
    'git ls-remote exited 128',
]


@pytest.mark.parametrize('said', TRANSIENT)
def test_a_network_failure_that_passes_is_transient(said):
    assert transient(GitFailed(said))


@pytest.mark.parametrize('said', PERMANENT)
def test_a_failure_no_pause_mends_is_not(said):
    assert not transient(GitFailed(said))


#: The `git` first on PATH: each run is counted in `$FAKE_GIT_COUNT`;
#: the first `$FAKE_GIT_FAILS` of them sleep `$FAKE_GIT_SLEEP` seconds,
#: say `$FAKE_GIT_SAY` and exit 128, and the rest say `$FAKE_GIT_OUT`.
FAKE_GIT = r"""#!/bin/sh
n=$(($(cat "$FAKE_GIT_COUNT" 2>/dev/null || echo 0) + 1))
echo "$n" > "$FAKE_GIT_COUNT"
if [ "$n" -le "${FAKE_GIT_FAILS:-0}" ]; then
    [ -n "$FAKE_GIT_SLEEP" ] && exec sleep "$FAKE_GIT_SLEEP"
    printf '%s\n' "$FAKE_GIT_SAY" >&2
    exit 128
fi
printf '%s' "$FAKE_GIT_OUT"
"""


class FakeGit:
    """A git that fails as it is told to, then answers; nothing it does
    reaches the network."""

    def __init__(self, root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.count = root / 'count'
        self._monkeypatch = monkeypatch
        bin_dir = root / 'bin'
        bin_dir.mkdir()
        (bin_dir / 'git').write_text(FAKE_GIT)
        (bin_dir / 'git').chmod(0o755)
        monkeypatch.setenv(
            'PATH', f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        )
        monkeypatch.setenv('FAKE_GIT_COUNT', str(self.count))
        for name in ('FAKE_GIT_FAILS', 'FAKE_GIT_SLEEP', 'FAKE_GIT_SAY'):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setenv('FAKE_GIT_OUT', '')

    def fails(
        self, times: int, say: str = '', *, sleep: float | None = None,
    ) -> None:
        self._monkeypatch.setenv('FAKE_GIT_FAILS', str(times))
        self._monkeypatch.setenv('FAKE_GIT_SAY', say)
        if sleep is not None:
            self._monkeypatch.setenv('FAKE_GIT_SLEEP', str(sleep))

    def answers(self, out: str) -> None:
        self._monkeypatch.setenv('FAKE_GIT_OUT', out)

    @property
    def runs(self) -> int:
        return int(self.count.read_text()) if self.count.exists() else 0


@pytest.fixture
def fake_git(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FakeGit:
    return FakeGit(tmp_path, monkeypatch)


def test_a_git_killed_at_its_time_limit_is_transient(fake_git):
    fake_git.fails(1, sleep=30)

    with pytest.raises(GitFailed) as failed:
        asyncio.run(
            gitremote.run(['ls-remote', 'x'], env=os.environ, timeout=0.5),
        )

    assert 'killed after' in str(failed.value)
    assert transient(failed.value)


SHA = 'a' * 40
LISTING = (
    f'ref: refs/heads/main\tHEAD\n{SHA}\tHEAD\n{SHA}\trefs/heads/main\n'
)
DNS = (
    "fatal: unable to access 'https://github.com/acme/shop.git/': "
    'Could not resolve host: github.com'
)
GONE = (
    'remote: Repository not found.\n'
    "fatal: repository 'https://github.com/acme/shop.git/' not found"
)


@pytest.fixture(autouse=True)
def short_pauses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gitremote, 'PAUSE', 0.01)


def list_remote() -> gitremote.RemoteRefs:
    return asyncio.run(gitremote.GitRemote().list_remote('acme/shop'))


def test_a_listing_that_fails_on_the_way_is_asked_again(fake_git):
    fake_git.fails(gitremote.ATTEMPTS - 1, DNS)
    fake_git.answers(LISTING)

    listing = list_remote()

    assert listing.error == ''
    assert listing.head == 'main'
    assert listing.refs['refs/heads/main'] == SHA
    assert fake_git.runs == gitremote.ATTEMPTS


def test_a_listing_that_keeps_failing_on_the_way_fails_as_before(fake_git):
    fake_git.fails(gitremote.ATTEMPTS + 5, DNS)

    listing = list_remote()

    assert listing.refs == {}
    assert listing.error == f'git ls-remote exited 128: {DNS}'
    assert fake_git.runs == gitremote.ATTEMPTS


def test_a_repository_not_found_is_asked_once(fake_git):
    fake_git.fails(1, GONE)
    fake_git.answers(LISTING)

    listing = list_remote()

    assert listing.refs == {}
    assert 'not found' in listing.error
    assert fake_git.runs == 1


def test_asking_again_is_logged(fake_git):
    fake_git.fails(1, DNS)
    fake_git.answers(LISTING)

    with structlog.testing.capture_logs() as logged:
        list_remote()

    [again] = [
        entry for entry in logged
        if entry['event'] == 'Git failed: asking again after a pause'
    ]
    assert again['repo'] == 'acme/shop'
    assert again['attempt'] == 1
    assert 'Could not resolve host' in again['error']


def test_asking_again_adds_no_more_than_its_patience(fake_git, monkeypatch):
    """A git that runs out its time each time: run again within what is
    left of the operation's patience, not for its whole time limit."""
    monkeypatch.setattr(gitremote.git, 'LS_REMOTE_TIMEOUT', 2.0)
    monkeypatch.setattr(gitremote, 'PATIENCE', 1.0)
    monkeypatch.setattr(gitremote, 'PAUSE', 0.1)
    fake_git.fails(gitremote.ATTEMPTS + 5, sleep=30)

    started = time.monotonic()
    listing = list_remote()
    took = time.monotonic() - started

    assert listing.refs == {}
    assert 'killed after' in listing.error
    # Run again once, for what was left of the patience, 0.9 s.
    assert fake_git.runs == 2
    assert 'killed after 0.9 s' in listing.error
    # The first run's time limit, and the patience; no more.
    assert took < 2.0 + 1.0 + 0.5


def test_no_more_asking_than_patience_allows(fake_git, monkeypatch):
    """A pause past the patience left is not taken: the failure is
    raised instead."""
    monkeypatch.setattr(gitremote, 'PATIENCE', 1.0)
    monkeypatch.setattr(gitremote, 'PAUSE', 5.0)
    fake_git.fails(gitremote.ATTEMPTS, DNS)
    fake_git.answers(LISTING)

    started = time.monotonic()
    listing = list_remote()

    assert time.monotonic() - started < 2
    assert listing.error == f'git ls-remote exited 128: {DNS}'
    assert fake_git.runs == 1


def test_a_listing_given_up_on_in_its_pause_stops(fake_git, monkeypatch):
    """As `chatsbom collect` gives up a collection when it stops: a
    listing waiting to ask again is cancelled at once, and asks no
    more."""
    monkeypatch.setattr(gitremote, 'PAUSE', 30.0)
    monkeypatch.setattr(gitremote, 'PATIENCE', 600.0)
    fake_git.fails(gitremote.ATTEMPTS, DNS)

    async def giving_up() -> float:
        listing = asyncio.ensure_future(
            gitremote.GitRemote().list_remote('acme/shop'),
        )
        while fake_git.runs < 1:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)
        started = time.monotonic()
        listing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listing
        return time.monotonic() - started

    assert asyncio.run(giving_up()) < 1
    assert fake_git.runs == 1
