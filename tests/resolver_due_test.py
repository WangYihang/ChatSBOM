"""What the resolver has to resolve, from the store (#168).

A directory is due when, at the repository's current commit in the
store, it holds a manifest a recipe reads and none of the lockfiles the
recipe makes (`sandbox.recipes_for`); it has no result under
`generated_lock_path`; and it has no failure still backing off in
resolver.sqlite. Older commits are not resolved, and the repositories
with the most stars go first. The current commit is #161's to say
(`collector.due.standing`): the newest push the store has decided, its
commit, and that commit's content, whole and stamped.

These build the store by hand, as the collector's stages leave it: the
universe (a complete search snapshot), the release and commit
decisions (#147), the tree and the content root with its
`manifests.json`.
"""
from __future__ import annotations

import json
import os
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import LIMITS
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.due import Priority
from chatsbom.collector.due import standing
from chatsbom.collector.due import walk_universe
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Member
from chatsbom.collector.state import Observed
from chatsbom.collector.state import UniverseSnapshot
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import discovery_document
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.layout import push_text
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import LockTarget
from chatsbom.resolver import due as resolver_due
from chatsbom.resolver.state import ResolverState
from chatsbom.resolver.state import state_path
from tests.sbom_generate_test import syft_document

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
P1 = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
P2 = datetime(2026, 9, 29, 9, 30, tzinfo=UTC)
S1 = '1' * 40
S2 = '2' * 40

COMPOSER = {'composer.json': '{"require": {"x/y": "^1.0"}}\n'}
GEMFILE = {'Gemfile': "source 'https://rubygems.org'\ngem 'rack'\n"}
LOCKED = {**COMPOSER, 'composer.lock': '{"packages": []}\n'}


# -- the store, as the collector leaves it -----------------------------------------


def searched(
    paths: PathConfig, stars: dict[int, int | None],
    day: date = NOW.date() - timedelta(days=1),
) -> Path:
    """The universe: a complete search snapshot, `all-<day>.jsonl`,
    listing each repository as `octo/r<id>` with its stars."""
    path = paths.search_dir / f'all-{day.isoformat()}.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(
            json.dumps({
                'id': repository_id, 'owner': 'octo',
                'repo': f'r{repository_id}', 'stars': count,
                'pushed_at': push_text(P1),
            }) + '\n'
            for repository_id, count in stars.items()
        ),
        encoding='utf-8',
    )
    return path


def collected(
    paths: PathConfig, repository_id: int, files: dict[str, str], *,
    push: datetime = P1, sha: str = S1, content: bool = True,
) -> Path:
    """The repository collected for `push` down to the content of `sha`:
    the release decision (no release, so the key is the head at the
    push), the commit decision, the tree of `files`, and, unless
    `content` is off, the content root with the files discovery selects
    and its `manifests.json`, stamped. Its content root."""
    kept = decisions.keep_release(
        paths, {
            'id': repository_id, 'pushed_at': push_text(push),
            'all_releases': [], 'latest_stable_release': None,
        },
    )
    assert kept.decision is decisions.Outcome.WRITTEN
    assert decisions.keep_commit(
        paths, {
            'id': repository_id, 'pushed_at': push_text(push),
            'has_releases': False, 'latest_stable_release': None,
            'download_target': {
                'ref': 'main', 'ref_type': 'branch', 'commit_sha': sha,
                'commit_sha_short': sha[:7],
            },
        },
    ) in (decisions.Outcome.WRITTEN, decisions.Outcome.KEPT)
    tree = paths.tree_file(repository_id, sha)
    tree.parent.mkdir(parents=True, exist_ok=True)
    tree.write_text(''.join(f'{path}\n' for path in files), encoding='utf-8')
    root = paths.content_root(repository_id, sha)
    if not content:
        return root
    discovery = discover(list(files))
    fetched: dict[str, dict[str, Any]] = {}
    written = []
    for item in discovery.selected:
        body = files[item.path].encode()
        target = root.joinpath(*item.path.split('/'))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        fetched[item.path] = {'status': 'ok', 'size': len(body)}
        written.append((item.path, len(body)))
    root.mkdir(parents=True, exist_ok=True)
    document = discovery_document(
        discovery, repository_id=repository_id, commit_sha=sha,
        fetched=fetched, max_file_bytes=LIMITS['max_file_bytes'],
    )
    document['bytes'] = sum(size for _, size in written)
    document['digest'] = content_digest(written)
    document[VERSION_FIELD] = CONTENT_VERSION
    paths.discovery_file(repository_id, sha).write_text(
        json.dumps(document), encoding='utf-8',
    )
    return root


def resolved(
    paths: PathConfig, repository_id: int, directory: str = '',
    name: str = 'composer.lock', sha: str = S1,
) -> Path:
    """What a resolution left: its lockfile, under the generated-lock
    root of `sha`, at `directory`."""
    path = paths.generated_lock_path(repository_id, sha) / directory / name
    atomic_write_text(path, 'resolved\n')
    return path


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


@pytest.fixture
def state(paths: PathConfig) -> Any:
    with ResolverState.open(state_path(paths.base_data_dir)) as state:
        yield state


def walk(
    paths: PathConfig, state: ResolverState, **options: Any,
) -> resolver_due.Walk:
    options.setdefault('now', NOW)
    return resolver_due.walk(paths, state, **options)


def due(paths: PathConfig, state: ResolverState, **options: Any) -> list[str]:
    """What is due, in order, as `<id>:<sha>/<directory>:<ecosystem>`."""
    return [
        f'{d.repository_id}:{d.sha[0]}/{d.target.directory}:'
        f'{d.target.ecosystem}'
        for d in walk(paths, state, **options).due
    ]


def target(directory: str = '', ecosystem: str = 'composer') -> LockTarget:
    return LockTarget(directory, ecosystem, LOCK_RECIPES[ecosystem])


# -- what is due --------------------------------------------------------------------


def test_a_manifest_without_a_lockfile_is_due(paths, state):
    searched(paths, {1: 5000})
    root = collected(paths, 1, COMPOSER)

    found = walk(paths, state)

    [only] = found.due
    assert (only.repository_id, only.full_name, only.sha) == (1, 'octo/r1', S1)
    assert only.target == target()
    assert only.project == root
    assert only.output == paths.generated_lock_path(1, S1)
    assert (found.roots, found.directories, found.resolved) == (1, 1, 0)


@pytest.mark.parametrize(
    'files', [LOCKED, {**GEMFILE, 'Gemfile.lock': 'GEM\n'}],
    ids=['composer', 'gem'],
)
def test_one_that_ships_its_lockfile_is_not_due(paths, state, files):
    """What it pins is what Syft reads: nothing to resolve."""
    searched(paths, {1: 5000})
    collected(paths, 1, files)

    found = walk(paths, state)

    assert found.due == []
    assert (found.roots, found.directories) == (1, 0)


def test_one_with_a_result_is_not_due(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    resolved(paths, 1)

    found = walk(paths, state)

    assert found.due == []
    assert (found.directories, found.resolved) == (1, 1)


def test_a_link_named_like_the_lockfile_is_no_result(paths, state, tmp_path):
    """A resolver runs project-controlled code: what is at the path is
    a result only if it is a regular file (`LockRecipe.generated_in`)."""
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    elsewhere = tmp_path / 'elsewhere'
    elsewhere.write_text('not a lockfile\n')
    link = paths.generated_lock_path(1, S1) / 'composer.lock'
    link.parent.mkdir(parents=True)
    link.symlink_to(elsewhere)

    assert due(paths, state) == ['1:1/:composer']


def test_one_with_a_failure_in_its_backoff_is_not_due(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    state.failed(1, S1, target(), FAILED, now=NOW - timedelta(minutes=10))

    found = walk(paths, state)

    assert found.due == []
    assert found.backing_off == 1


def test_it_is_due_again_once_its_backoff_has_passed(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    state.failed(1, S1, target(), FAILED, now=NOW - timedelta(minutes=20))

    assert due(paths, state) == ['1:1/:composer']


def test_a_failure_holds_back_its_own_directory_alone(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, {**COMPOSER, **GEMFILE, 'api/composer.json': '{}'})
    state.failed(1, S1, target(), FAILED, now=NOW)

    assert due(paths, state) == ['1:1/:gem', '1:1/api:composer']


def test_a_new_commit_makes_it_due_again(paths, state):
    """Resolved at the commit it was, and pushed since: the result is
    the old commit's, and the new commit's directory is due."""
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    resolved(paths, 1)
    assert due(paths, state) == []

    collected(paths, 1, COMPOSER, push=P2, sha=S2)

    assert due(paths, state) == ['1:2/:composer']


def test_a_failure_at_the_old_commit_holds_nothing_back(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    state.failed(1, S1, target(), FAILED, now=NOW)
    collected(paths, 1, COMPOSER, push=P2, sha=S2)

    assert due(paths, state) == ['1:2/:composer']


def test_older_commits_are_not_resolved(paths, state):
    """S1 needs a lockfile and S2, the repository's commit now, does
    not: nothing is due."""
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    collected(paths, 1, LOCKED, push=P2, sha=S2)

    found = walk(paths, state)

    assert found.due == []
    assert (found.roots, found.directories) == (1, 0)


def test_a_commit_whose_content_is_not_in_yet_waits(paths, state):
    """P2 is decided and its commit resolved, and its content is not
    fetched yet: the collector is on its way there. S1 is no longer the
    repository's commit, and S2 has nothing to read yet."""
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    collected(paths, 1, COMPOSER, push=P2, sha=S2, content=False)

    found = walk(paths, state)

    assert found.due == []
    assert found.roots == 0


def test_content_the_collector_will_fetch_again_waits(paths, state):
    """A root the old pipeline fetched, without the content stage's
    stamp, is fetched again whole (#100 Q4): not read until it is."""
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    index = paths.discovery_file(1, S1)
    document = json.loads(index.read_text())
    del document[VERSION_FIELD]
    index.write_text(json.dumps(document))

    assert due(paths, state) == []


def test_a_repository_never_collected_has_nothing_due(paths, state):
    searched(paths, {1: 5000, 2: 4000})
    collected(paths, 2, COMPOSER)

    assert due(paths, state) == ['2:1/:composer']


def test_the_repositories_with_the_most_stars_go_first(paths, state):
    searched(paths, {1: 1000, 2: 90000, 3: None, 4: 5000})
    for repository_id in (1, 2, 3, 4):
        collected(paths, repository_id, COMPOSER)

    assert due(paths, state) == [
        '2:1/:composer', '4:1/:composer', '1:1/:composer', '3:1/:composer',
    ]


def test_a_repository_outside_the_universe_is_not_resolved(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, COMPOSER)
    collected(paths, 2, COMPOSER)

    assert due(paths, state) == ['1:1/:composer']


def test_the_universe_is_the_newest_complete_snapshot(paths, state):
    """One dated today is complete only once its search has said so."""
    searched(paths, {1: 5000}, day=NOW.date() - timedelta(days=8))
    searched(paths, {2: 5000}, day=NOW.date())
    collected(paths, 1, COMPOSER)
    collected(paths, 2, COMPOSER)

    found = walk(paths, state)

    assert found.universe == f'all-{NOW.date() - timedelta(days=8)}'
    assert [d.repository_id for d in found.due] == [1]


def test_without_a_universe_nothing_is_due(paths, state):
    collected(paths, 1, COMPOSER)

    found = walk(paths, state)

    assert found.universe is None
    assert found.due == []


def test_a_recipe_runs_in_every_directory_that_needs_it(paths, state):
    """Wherever its manifest is, shallowest first: a directory that
    ships its lockfile, and one no recipe reads, are left alone."""
    searched(paths, {1: 5000})
    collected(
        paths, 1, {
            'package.json': '{}\n',
            'backend/composer.json': '{}\n',
            'legacy/composer.json': '{}\n',
            'legacy/composer.lock': '{}\n',
            'docs/Gemfile': "source 'https://rubygems.org'\n",
        },
    )

    found = walk(paths, state)

    assert due(paths, state) == ['1:1/backend:composer', '1:1/docs:gem']
    backend = found.due[0]
    assert backend.project == paths.content_root(1, S1) / 'backend'
    assert backend.output == paths.generated_lock_path(1, S1) / 'backend'


def test_one_ecosystem_can_be_asked_for(paths, state):
    searched(paths, {1: 5000})
    collected(paths, 1, {**COMPOSER, **GEMFILE})

    assert due(paths, state, ecosystems={'gem'}) == ['1:1/:gem']


def test_the_repositories_named_alone(paths, state):
    searched(paths, {1: 5000, 2: 4000})
    collected(paths, 1, COMPOSER)
    collected(paths, 2, COMPOSER)

    assert due(paths, state, repositories={2}) == ['2:1/:composer']


def test_a_limit_stops_the_walk(paths, state):
    searched(paths, {1: 5000, 2: 4000, 3: 3000})
    for repository_id in (1, 2, 3):
        collected(paths, repository_id, COMPOSER)

    assert due(paths, state, limit=2) == ['1:1/:composer', '2:1/:composer']


def test_forced_a_result_is_due_again(paths, state):
    """`--force`: what the resolver wrote, resolved again; never what
    the project committed."""
    searched(paths, {1: 5000})
    collected(paths, 1, {**COMPOSER, 'api/composer.json': '{}', **GEMFILE})
    resolved(paths, 1)
    resolved(paths, 1, 'api')
    resolved(paths, 1, name='Gemfile.lock')

    assert due(paths, state) == []
    assert due(paths, state, force=True) == [
        '1:1/:composer', '1:1/:gem', '1:1/api:composer',
    ]


def test_deleting_resolver_sqlite_loses_the_backoff_and_nothing_else(
    paths, tmp_path,
):
    """It never says a directory is resolved: its lockfile does. Gone,
    what failed is tried again, and what was resolved stays resolved."""
    searched(paths, {1: 5000})
    collected(paths, 1, {**COMPOSER, **GEMFILE})
    resolved(paths, 1, name='Gemfile.lock')
    path = state_path(paths.base_data_dir)
    with ResolverState.open(path) as state:
        state.failed(1, S1, target(), FAILED, now=NOW)
        assert due(paths, state) == []

    for leftover in path.parent.glob(f'{path.name}*'):
        leftover.unlink()

    with ResolverState.open(path) as state:
        assert due(paths, state) == ['1:1/:composer']


# -- Syft runs again after a result -------------------------------------------------


def _aged(root: Path, seconds_ago: float) -> None:
    """Every file under `root` as written `seconds_ago`."""
    when = datetime.now(UTC).timestamp() - seconds_ago
    for path in [root, *root.rglob('*')]:
        os.utime(path, (when, when))


def test_a_resolved_lockfile_makes_the_sbom_due_again(paths, tmp_path):
    """#161's due set, at the store's level: an SBOM is current only
    while it is newer than every file it was made from, the lockfiles
    `sbom lock` generated among them (`generated_lock_path`). So once a
    lockfile is written, Syft's stage is due again for that commit, and
    the collector's walk of the universe finds it."""
    collected(paths, 1, COMPOSER)
    sbom = paths.sbom_file(1, S1)
    sbom.parent.mkdir(parents=True)
    sbom.write_text(syft_document(version='1.52.0'), encoding='utf-8')
    _aged(paths.content_root(1, S1), 7200)
    _aged(paths.tree_dir, 7200)
    _aged(sbom.parent, 3600)

    with CollectorState.open(tmp_path / 'collector.sqlite') as collector:
        collector.keep_universe(
            UniverseSnapshot('all-2026-09-29', 'stamp', 1, NOW),
            [Member(1, 'R_1')],
        )
        collector.observe(
            Observed(
                repository_id=1, node_id='R_1', full_name='octo/r1',
                stars=5000, archived=False, pushed_at=P1,
                default_branch='main', head=S1, release_tag=None,
                release_at=None, observed_at=NOW,
            ),
        )

        def sbom_verdict() -> tuple[str, str]:
            found = standing(
                1, P1, paths=paths, outcomes=collector,
                syft_version='1.52.0', now=NOW,
            )
            verdict = found.verdicts[-1]
            return str(verdict.state), verdict.why

        assert sbom_verdict() == ('present', 'current')
        assert walk_universe(
            collector, paths=paths, syft_version='1.52.0', now=NOW,
        ).candidates == []

        resolved(paths, 1)

        assert sbom_verdict() == ('due', 'input-changed')
        [candidate] = walk_universe(
            collector, paths=paths, syft_version='1.52.0', now=NOW,
        ).candidates
        assert candidate.priority is Priority.CHANGED
        assert candidate.observed.repository_id == 1
