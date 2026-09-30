"""Dockerfile.site: the Python web service's image (#145), read without
building it.

Three stages. Node builds the page from web/; uv installs the
package and its `web` extra from uv.lock; and the image
is Python with the two copied in, and nothing that made them: no Node,
no node_modules, no uv. It runs `chatsbom web serve` as a uid of its
own, checks itself by asking /healthz with Python's standard library,
and stops on SIGTERM. Compose runs it as the `site` service
(compose_test), read-only, with nothing to write but its state volume.
"""
import os
import re
import shlex
import subprocess
import sys
import threading
import tomllib
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from typing import Any

import pytest

from tests.compose_test import _copied_from
from tests.compose_test import _dockerignored
from tests.compose_test import _exec_form
from tests.compose_test import _extras
from tests.compose_test import _image_env
from tests.compose_test import _instructions
from tests.compose_test import _site_command
from tests.compose_test import _site_port
from tests.compose_test import _stages
from tests.compose_test import _uv_syncs
from tests.compose_test import ROOT
from tests.compose_test import SITE_DOCKERFILE
from tests.compose_test import Stage

#: The stages, in order: the page, the environment, and the image.
STAGES = ['page', 'venv', 'site']


@pytest.fixture(scope='module')
def site() -> str:
    return SITE_DOCKERFILE.read_text()


def _stage(dockerfile: str, name: str) -> Stage:
    [stage] = [stage for stage in _stages(dockerfile) if stage.name == name]
    return stage


def _runs(stage: Stage) -> list[str]:
    return [a for keyword, a in stage.instructions if keyword == 'RUN']


def _env(stage: Stage) -> dict[str, str]:
    """Every `ENV KEY=value` of one stage."""
    return {
        key: value
        for keyword, arguments in stage.instructions if keyword == 'ENV'
        for key, _, value in (w.partition('=') for w in shlex.split(arguments))
    }


def _copies(stage: Stage, source: str) -> list[tuple[list[str], str]]:
    """What each `COPY --from=<source>` of a stage copies: (from, to)."""
    copies = []
    for keyword, arguments in stage.instructions:
        words = shlex.split(arguments)
        if keyword == 'COPY' and f'--from={source}' in words:
            *paths, target = [w for w in words if not w.startswith('--')]
            copies.append((paths, target))
    return copies


def _base(path: str) -> str:
    """The image the first stage of a Dockerfile of this checkout is on."""
    return _stages((ROOT / path).read_text())[0].base


def _user(stage: Stage) -> str:
    [user] = [a for k, a in stage.instructions if k == 'USER']
    return user


def test_three_stages_and_the_last_is_the_image(site):
    """The page, the environment, and the image, which is what a build
    with no target makes and the one compose names (compose_test)."""
    assert [stage.name for stage in _stages(site)] == STAGES


def test_node_builds_the_page_from_the_lockfile(site):
    """`npm ci`, the lockfile being the input, then the build, which
    writes the page to web/dist/client, on Node's own image: the Node CI
    builds the page on (workflows_test)."""
    page = _stage(site, 'page')
    assert page.base.startswith('node:'), page.base
    assert _runs(page) == ['npm ci', 'npm run build']


def test_the_image_has_no_node_and_no_node_modules(site):
    """Built on Python, and given the built page alone from the stage
    that built it: not web/, where node_modules is, nor Node."""
    stages = _stages(site)
    image = stages[-1]
    assert image.base == _base('Dockerfile')
    assert image.base.lower() not in {stage.name for stage in stages}
    [(paths, target)] = _copies(image, 'page')
    assert paths == ['/app/web/dist/client'] == [target]
    for arguments in _runs(image):
        assert not re.search(r'\b(node|npm|npx)\b', arguments), arguments


def test_the_image_has_no_installer(site):
    """The environment uv made, copied to where it was made, so that its
    scripts and its interpreter's link hold; and not uv, nor anything
    installed at run time."""
    image = _stage(site, 'site')
    assert set(_copied_from(image)) == {'page', 'venv'}
    assert _copies(image, 'venv') == [(['/app/.venv'], '/app/.venv')]
    for arguments in _runs(image):
        assert not re.search(
            r'\buv\b|\bpip\b|apt-get|\bapk\b', arguments,
        ), arguments


def test_the_environment_is_the_package_and_its_web_extra(site):
    """What `web serve` needs and nothing else: no development group,
    which brings every extra and pytest; the `web` extra, which is
    FastAPI, uvicorn, ALTCHA and the OpenAI SDK; from the lockfile as it
    is. The project installed rather than linked back to /app, which the
    image does not have, and byte-compiled, since nothing can write the
    image's files at run time."""
    venv = _stage(site, 'venv')
    syncs = _uv_syncs(venv)
    assert len(syncs) == 2
    compiling = _image_env(site).get('UV_COMPILE_BYTECODE') == '1'
    for sync in syncs:
        assert '--frozen' in sync, sync
        assert '--no-dev' in sync, sync
        assert _extras(sync) == {'web'}, sync
        assert compiling or '--compile-bytecode' in sync, sync
    dependencies, project = syncs
    assert '--no-install-project' in dependencies
    assert '--no-editable' in project


def test_the_environment_links_the_interpreter_the_image_has(site):
    """A virtual environment is a link to the interpreter it was made
    with. Made on the image's own Python, and never one uv would fetch,
    which would stay behind in the stage."""
    assert _stage(site, 'venv').base == _stage(site, 'site').base
    assert _env(_stage(site, 'venv')).get('UV_PYTHON_DOWNLOADS') == 'never'


def test_the_uv_is_the_collectors(site):
    """The one uv, moved by hand in both files (Dockerfile says how)."""
    [uv] = _copied_from(_stage(site, 'venv'))
    assert uv.startswith('ghcr.io/astral-sh/uv:')
    collector = _stages((ROOT / 'Dockerfile').read_text())[0]
    assert uv in _copied_from(collector)


def test_the_installed_project_carries_its_licence(site):
    """Building the wheel reads the files pyproject.toml names, its
    licence among them, which hatchling leaves out without a word when
    it is not there (#28)."""
    declared = tomllib.loads(
        (ROOT / 'pyproject.toml').read_text(),
    )['project']['license-files']
    copied: set[str] = set()
    for keyword, arguments in _stage(site, 'venv').instructions:
        if keyword == 'COPY' and '--from=' not in arguments:
            *sources, _ = shlex.split(arguments)
            copied.update(sources)
        elif keyword == 'RUN' and '--no-install-project' not in arguments:
            break
    assert set(declared) <= copied, copied


def test_it_runs_as_an_unprivileged_uid(site):
    """A uid of its own, made in the image, and named by its number, so
    that a runtime can tell it is not root without reading /etc/passwd.
    Nothing runs as root after it."""
    image = _stage(site, 'site')
    user = _user(image)
    uid = user.partition(':')[0]
    assert uid.isdigit() and int(uid) >= 1000, user
    keywords = [keyword for keyword, _ in image.instructions]
    assert 'RUN' not in keywords[keywords.index('USER'):]
    [useradd] = [run for run in _runs(image) if 'useradd' in run]
    words = shlex.split(useradd)
    assert words[words.index('--uid') + 1] == uid


def test_the_cli_starts_without_git_as_the_image_runs_it(site, tmp_path):
    """The CLI imports every command as it starts, the collector's
    among them, and GitPython refuses to be imported where there is no
    git to run: in the image, which has none, `web serve` stopped with
    `Bad git executable` before it read a setting. It runs no git, so
    the image tells GitPython to be quiet about it."""
    quiet = _env(_stage(site, 'site')).get('GIT_PYTHON_REFRESH')
    assert quiet == 'quiet'
    no_git = tmp_path / 'bin'
    no_git.mkdir()

    def start(**env: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, '-c', 'import chatsbom.__main__'],
            env={'PATH': str(no_git), **env},
            capture_output=True,
            text=True,
            timeout=120,
        )

    refused = start()
    assert refused.returncode != 0
    assert 'Bad git executable' in refused.stderr
    started = start(GIT_PYTHON_REFRESH=quiet)
    assert started.returncode == 0, started.stderr


def test_it_serves_on_every_interface_on_its_port(site):
    """`web serve`, on the edge's address, the default network's and
    the loopback, where the healthcheck asks; and the page the image
    has, where the first stage built it."""
    command = _site_command(site)
    assert command[:3] == ['chatsbom', 'web', 'serve']
    options = dict(zip(command[3::2], command[4::2]))
    assert options['--host'] == '0.0.0.0'
    assert options['--port'].isdigit()
    [(_, target)] = _copies(_stage(site, 'site'), 'page')
    assert options['--spa'] == target


def _healthcheck(site: str) -> tuple[list[str], list[str]]:
    """The image's HEALTHCHECK: its options, and the command it runs."""
    [check] = [
        arguments for keyword, arguments in _stage(site, 'site').instructions
        if keyword == 'HEALTHCHECK'
    ]
    options, _, command = check.partition('CMD ')
    return options.split(), _exec_form(command)


def test_the_healthcheck_asks_healthz_with_python(site):
    """Inside the container, from the loopback, which is not the edge,
    so /healthz answers it (#139). With Python's own urllib: the image
    has no curl, and a shell is not needed to run it."""
    _, command = _healthcheck(site)
    assert command[:2] == ['python', '-c']
    assert f"'http://127.0.0.1:{_site_port()}/healthz'" in command[2]
    for keyword, arguments in _instructions(site):
        assert not re.search(r'\b(curl|wget)\b', arguments), keyword


class Health:
    """A stand-in for the service's /healthz, answering `status`."""

    def __init__(self, status: int) -> None:
        self.paths: list[str] = []
        paths = self.paths

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                paths.append(self.path)
                self.send_response(status)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, format: str, *args: Any) -> None:
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever)

    def __enter__(self) -> 'Health':
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


@pytest.fixture
def unused_port() -> Iterator[int]:
    """A port nothing listens on, for as long as the test runs."""
    with Health(200) as health:
        port = health.port
    yield port


def _check(site: str, port: int) -> subprocess.CompletedProcess[str]:
    """The healthcheck's command, asking `port` in the image's stead,
    with a proxy in the environment that answers nothing: Docker hands
    a container the client's proxy settings, and a check sent there
    would ask the proxy, not the service."""
    _, (_, flag, code) = _healthcheck(site)
    code = code.replace(f'127.0.0.1:{_site_port()}/', f'127.0.0.1:{port}/')
    dead = 'http://127.0.0.1:9'
    return subprocess.run(
        [sys.executable, flag, code],
        env={
            'PATH': os.environ['PATH'],
            'http_proxy': dead, 'HTTP_PROXY': dead,
            'https_proxy': dead, 'HTTPS_PROXY': dead,
        },
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_the_healthcheck_passes_on_an_answer(site):
    with Health(200) as health:
        result = _check(site, health.port)
    assert result.returncode == 0, result.stderr
    assert health.paths == ['/healthz']


@pytest.mark.parametrize('status', [404, 500, 503])
def test_the_healthcheck_fails_on_a_refusal(site, status):
    """404 included: what /healthz answers a peer in the edge's subnet,
    which the check never is."""
    with Health(status) as health:
        result = _check(site, health.port)
    assert result.returncode != 0
    assert health.paths == ['/healthz']


def test_the_healthcheck_fails_when_nothing_answers(site, unused_port):
    assert _check(site, unused_port).returncode != 0


def test_a_stop_is_a_sigterm(site):
    """uvicorn stops on SIGTERM as it should: it takes no new request,
    answers those in flight, runs the service's shutdown, then exits by
    the signal, 143 (#139). Compose gives it the time (compose_test)."""
    [signal] = [
        a for k, a in _stage(site, 'site').instructions if k == 'STOPSIGNAL'
    ]
    assert signal == 'SIGTERM'


@pytest.mark.parametrize(
    'path', ['web/node_modules', 'web/dist', '.venv', 'data'],
)
def test_the_build_is_given_nothing_it_makes_itself(path):
    """`COPY web/ ./` copies what is there: a node_modules of the
    host's, whose binaries may be another platform's, or a page built
    there, would be the image's in place of its own build."""
    patterns = (ROOT / '.dockerignore').read_text().splitlines()
    assert _dockerignored(path, patterns)
