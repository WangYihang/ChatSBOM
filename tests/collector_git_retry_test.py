"""git's transient network failures, asked again in place (#189).

What git says, classified: the transient ones are what the live
collector's git said in October 2026, and what git and curl say of the
same troubles; the permanent ones, and anything not recognised, are not
asked again.
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

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
