"""The collector's image run bare, with no mounts: `docker run <image>`.

The image keeps nothing of its own. Its WORKDIR, /app, is read-only to
the uid it runs as, and data/ and .cache/ are meant to be a checkout's,
mounted there (docker-compose.yaml). Run without them, its default
command, `queue status` then, went to make data/ in /app and exited
with a traceback, `PermissionError: [Errno 13] Permission denied:
'data'` (#118). It says what to mount now (#122), and its default
command is the collector, `collect` (#171), which says so before it
starts, as any command says so where it stops.

These run the CLI as that run did, in a directory where making anything
is refused as the image refuses it: the suite may run as root, whom no
directory's mode refuses. The collector is given a token, which it asks
for before it looks at the disk (collector_process_run_test).
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


@pytest.fixture
def token(monkeypatch) -> None:
    monkeypatch.setenv('GITHUB_TOKEN', 'ghp_' + 'a' * 36)


def test_a_bare_run_says_what_to_mount(read_only_workdir, token):
    """On stderr, and without a traceback: the directories the image
    needs, mounted where compose mounts them for `cli`, which runs the
    same image, as the invoking user; and, where Docker made them, who
    to give them to."""
    result = runner.invoke(app, ['collect'])

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
    assert 'mkdir -p data .cache' in message
    assert f'sudo chown -R {os.getuid()}:{os.getgid()} data .cache' in message


def test_the_image_runs_the_collector():
    """`docker run <image>` runs what the Dockerfile's CMD names."""
    [cmd] = re.findall(
        r'^CMD (\[.*\])$', (ROOT / 'Dockerfile').read_text(), re.M,
    )
    assert json.loads(cmd) == ['collect']


def test_any_command_says_it_where_it_stops(read_only_workdir, token):
    """The reason is for everyone, the traceback for whoever asks, as
    for any other error a command stops on: here `collect repo`, run by
    hand in the same image, which opens collector.sqlite in data/."""
    result = runner.invoke(app, ['collect', 'repo', 'octo/one'])

    assert result.exit_code == 1, result.output
    assert 'Traceback' not in result.output
    assert 'docker run --rm --user' in result.stderr

    result = runner.invoke(app, ['--debug', 'collect', 'repo', 'octo/one'])

    assert result.exit_code == 1
    assert 'Traceback (most recent call last)' in result.stderr
    assert 'docker run --rm --user' in result.stderr


def test_in_json_it_is_one_event(read_only_workdir, token, monkeypatch):
    """What a log collector reads: no lines for a person among it."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, ['collect'])

    assert result.exit_code == 1
    [event] = [json.loads(line) for line in result.stderr.splitlines()]
    assert event['level'] == 'error'
    assert event['path'] == 'data'
    assert event['directory'] == str(read_only_workdir)
    assert event['uid'] == os.getuid()
