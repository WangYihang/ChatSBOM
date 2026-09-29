"""The collector's image run bare, with no mounts: `docker run <image>`.

The image keeps nothing of its own. Its WORKDIR, /app, is read-only to
the uid it runs as, and data/, .cache/ and .requests-cache/ are meant to
be a checkout's, mounted there (docker-compose.yaml). Run without them,
its default command, `queue status`, went to make data/ in /app and
exited with a traceback, `PermissionError: [Errno 13] Permission denied:
'data'` (#118). It says what to mount now (#122).

These run the CLI as that run did, in a directory where making anything
is refused as the image refuses it: the suite may run as root, whom no
directory's mode refuses.
"""
import errno
import json
import os
import re
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from chatsbom.__main__ import app

ROOT = Path(__file__).resolve().parent.parent

runner = CliRunner()


def said(text: str) -> str:
    return ' '.join(text.split())


@pytest.fixture
def read_only_workdir(tmp_path, monkeypatch) -> Path:
    """A working directory in which `mkdir` is refused with EACCES, as
    the kernel refused uid 10001 in the image's /app."""
    monkeypatch.chdir(tmp_path)
    real_mkdir = os.mkdir

    def mkdir(path, mode=0o777, *, dir_fd=None):
        if Path(os.path.abspath(path)).parent == tmp_path:
            raise PermissionError(
                errno.EACCES, os.strerror(errno.EACCES), os.fspath(path),
            )
        return real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, 'mkdir', mkdir)
    return tmp_path


def the_mounts_compose_gives_cli() -> list[str]:
    """`cli`'s bind mounts, as `docker run` flags from a checkout: each
    over the directory of the same name in the image's WORKDIR, where
    it runs the CLI."""
    [workdir] = re.findall(
        r'^WORKDIR (\S+)$', (ROOT / 'Dockerfile').read_text(), re.M,
    )
    compose = yaml.safe_load((ROOT / 'docker-compose.yaml').read_text())
    flags = []
    for volume in compose['services']['cli']['volumes']:
        source, target = volume.split(':')[:2]
        name = source.removeprefix('./')
        assert target == f'{workdir}/{name}', volume
        flags.append(f'-v "$PWD/{name}:{target}"')
    return flags


def test_a_bare_run_says_what_to_mount(read_only_workdir):
    """On stderr, and without a traceback: the directories the image
    needs, mounted where compose mounts them for `cli`, which runs the
    same image, as the invoking user."""
    result = runner.invoke(app, ['queue', 'status'])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    assert 'Traceback' not in result.output
    message = said(result.stderr)
    assert (
        f'cannot write data in {read_only_workdir} as uid {os.getuid()} '
        f'(gid {os.getgid()}): Permission denied'
    ) in message
    assert 'docker run --rm --user "$(id -u):$(id -g)"' in message
    for mount in the_mounts_compose_gives_cli():
        assert mount in message
    assert 'docker compose --profile tools run --rm cli' in message
    assert 'mkdir -p data .cache .requests-cache' in message


def test_with_debug_the_traceback_is_printed(read_only_workdir):
    """The reason is for everyone, the traceback for whoever asks, as
    for any other error a command stops on."""
    result = runner.invoke(app, ['--debug', 'queue', 'status'])

    assert result.exit_code == 1
    assert 'Traceback (most recent call last)' in result.stderr
    assert 'docker run --rm --user' in result.stderr


def test_in_json_it_is_one_event(read_only_workdir, monkeypatch):
    """What a log collector reads: no lines for a person among it."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, ['queue', 'status'])

    assert result.exit_code == 1
    [event] = [json.loads(line) for line in result.stderr.splitlines()]
    assert event['level'] == 'error'
    assert event['path'] == 'data'
    assert event['directory'] == str(read_only_workdir)
    assert event['uid'] == os.getuid()
