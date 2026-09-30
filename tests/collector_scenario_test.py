"""A small universe for `chatsbom collect` to run on, for real (#171):
the stand-ins for GitHub's API, git, raw content and Syft
(tests/fake_github_test.py, tests/fake_upstream_test.py), on the clock
on the wall, as the process runs its own.

Three repositories of 1,000 stars or more, each at a commit with a
manifest: octo/one released, with a dependency graph; octo/two with no
release, and no graph; octo/three released, with a graph. Built the
same every time: a commit's sha is the same from one build to the
next, so a store collected from one build stands for the next.

As a program, `python -m tests.collector_scenario_test <workdir>` builds it
in a directory of its own, then runs the CLI's `chatsbom collect` in
`<workdir>` against it: the process as the command starts it, with its
signal handlers, its child processes and its heartbeat, and the stand-
ins in place of the network. The host proof runs it, and the tests that
stop the process with a signal. Its environment says what else:
`SCENARIO_SYFT_DELAY`, seconds each scan takes; `SCENARIO_SYFT`, `fake`
(the default) or `real`, the Syft on PATH; `SCENARIO_STAND_INS`, a
directory to build the stand-ins in and keep, where a test watches the
stand-in Syft.
"""
from __future__ import annotations

import asyncio
import functools
import os
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_upstream_test import FakeSyft
from tests.fake_upstream_test import Repository
from tests.fake_upstream_test import Upstream

#: The stand-in's token, which the process is given.
TOKEN = 'ghp_collector_scenario_00000000000000000'


class WallClock(FakeClock):
    """The stand-in on the clock on the wall, as the process's."""

    def __call__(self) -> float:
        return time.time()

    def advance(self, seconds: float) -> None:
        raise AssertionError('the clock on the wall moves by itself')

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(seconds, 0.0))


def graph_of(name: str, *packages: str) -> dict[str, Any]:
    """An SPDX document, as a finished report downloads one."""
    return {
        'SPDXID': 'SPDXRef-DOCUMENT',
        'spdxVersion': 'SPDX-2.3',
        'creationInfo': {
            'created': '2026-09-14T03:56:20Z',
            'creators': ['Tool: GitHub.com-Dependency-Graph'],
        },
        'name': name,
        'packages': [
            {
                'name': f'npm:{package}',
                'SPDXID': f'SPDXRef-npm-{package}',
                'versionInfo': '1.0.0',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': f'pkg:npm/{package}@1.0.0',
                }],
            }
            for package in packages
        ],
    }


@dataclass
class Scenario:
    """The stand-ins, and what they hold."""

    github: FakeGitHub
    upstream: Upstream
    syft: FakeSyft | None
    repositories: dict[str, Repository]

    def upstream_for_process(self) -> Any:
        """What the process takes in place of GitHub."""
        from chatsbom.collector.process import Upstream as ProcessUpstream

        return ProcessUpstream(
            github=self.github.transport(),
            downloads=self.github.transport(),
            raw=self.upstream.raw.transport(),
            git_base=self.upstream.git_base,
        )


def build(root: Path, *, syft: str = 'fake', delay: float = 0.0) -> Scenario:
    """The stand-ins, built under `root`, which must not exist yet."""
    github = FakeGitHub(WallClock())
    github.token(TOKEN, 'octocat')
    upstream = Upstream(root / 'github.com', github)

    one = upstream.add(1, 'octo/one', stars=1_500)
    sha = one.commit(
        {
            'package.json': '{"name": "one", "dependencies": '
            '{"left-pad": "1.3.0"}}',
            'index.js': 'module.exports = 1\n',
        },
        date='2026-09-02T00:00:00+00:00',
    )
    one.tag('v1.0.0', sha)
    one.release('v1.0.0', '2026-09-02T00:00:00Z')
    one.repo.graph = graph_of('octo/one', 'left-pad')

    two = upstream.add(2, 'octo/two', stars=1_200)
    two.commit(
        {'requirements.txt': 'requests==2.32.5\n', 'app.py': 'print(2)\n'},
        date='2026-09-03T00:00:00+00:00',
    )

    three = upstream.add(3, 'octo/three', stars=1_100)
    sha = three.commit(
        {'go.mod': 'module example.com/three\n\ngo 1.22\n'},
        date='2026-09-04T00:00:00+00:00',
    )
    three.tag('v0.1.0', sha)
    three.release('v0.1.0', '2026-09-04T00:00:00Z')
    three.repo.graph = graph_of('octo/three', 'is-odd')

    fake: FakeSyft | None = None
    if syft == 'fake':
        fake = FakeSyft(root / 'bin')
        fake.configure(delay=delay)
    return Scenario(
        github, upstream, fake,
        {'octo/one': one, 'octo/two': two, 'octo/three': three},
    )


def main(argv: list[str]) -> int:
    """`chatsbom collect` in `argv[0]`, on the scenario, built in a
    directory of its own that goes after."""
    workdir = Path(argv[0]).resolve()
    named = os.environ.get('SCENARIO_STAND_INS')
    stand_ins = (
        Path(named) if named
        else Path(tempfile.mkdtemp(prefix='collector-scenario-'))
    )
    try:
        scenario = build(
            stand_ins / 'upstream',
            syft=os.environ.get('SCENARIO_SYFT', 'fake'),
            delay=float(os.environ.get('SCENARIO_SYFT_DELAY', '0')),
        )
        if scenario.syft is not None:
            os.environ['PATH'] = (
                f'{scenario.syft.directory}{os.pathsep}{os.environ["PATH"]}'
            )
        os.environ['GITHUB_TOKEN'] = TOKEN
        os.chdir(workdir)

        from chatsbom.__main__ import app
        from chatsbom.collector import process

        process.run = functools.partial(
            process.run, upstream=scenario.upstream_for_process(),
        )
        try:
            app(['collect'], prog_name='chatsbom')
        except SystemExit as exit:
            return int(exit.code or 0)
        return 0
    finally:
        if not named:
            shutil.rmtree(stand_ins, ignore_errors=True)


def test_it_is_built_the_same_every_time(tmp_path: Path) -> None:
    """Each repository at the same commit, pushed at the same instant:
    a store collected from one build stands for the next, as a restart
    finds it."""
    def made(root: Path) -> dict[str, tuple[str, str]]:
        scenario = build(root)
        return {
            name: (repository.repo.head, repository.repo.pushed_at)
            for name, repository in scenario.repositories.items()
        }

    assert made(tmp_path / 'one') == made(tmp_path / 'two')


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
