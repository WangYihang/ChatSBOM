"""What is due for a repository, derived from the store (#161; #100
section 2, "the chain").

A stage is due when its output for its current key is not in the
store, and the keys follow the chain: the push P, the release decision
for P (the tag T, or none), K (`tag:T`, or `head:P` with no release),
the commit decision for K (the commit S), then the tree, the content
(stamped with its stage's version, #100 Q4) and the SBOM of S, by the
Syft now running (#110). The first stage whose output is missing is
due; those after it wait for it. A stage that produced nothing or
failed backs off for as long as collector.sqlite says (#100 Q5).

These tests build the store by hand, with the writers of the decisions
(#147) and of the documents, and read what is due from it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import LIMITS
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.due import CHAIN
from chatsbom.collector.due import sbom_key
from chatsbom.collector.due import standing
from chatsbom.collector.due import State
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import STATE_FILE
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import discovery_document
from chatsbom.core.ledger import Stage
from tests.sbom_generate_test import syft_document

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
P1 = datetime(2026, 9, 20, 8, 0, tzinfo=UTC)
P2 = datetime(2026, 9, 29, 9, 30, tzinfo=UTC)
S1 = '1' * 40
S2 = '2' * 40
SYFT = '1.52.0'
TREE = ['README.md', 'package.json', 'package-lock.json', 'src/app.js']


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


@pytest.fixture
def state(tmp_path: Path) -> Any:
    with CollectorState.open(tmp_path / 'data' / STATE_FILE) as state:
        yield state


# -- the store, as the stages leave it ------------------------------------------


def _released(
    paths: PathConfig, repository_id: int, push: datetime,
    tag: str | None = 'v1.0.0',
) -> None:
    """The release decision for `push`: `tag`, or no release."""
    releases = [] if tag is None else [{
        'tag_name': tag, 'name': tag, 'published_at': '2026-09-01T00:00:00Z',
    }]
    kept = decisions.keep_release(
        paths, {
            'id': repository_id, 'pushed_at': push.isoformat(),
            'all_releases': releases,
            'latest_stable_release': releases[0] if releases else None,
        },
    )
    assert kept.decision is decisions.Outcome.WRITTEN


def _resolved(
    paths: PathConfig, repository_id: int, push: datetime, sha: str,
    tag: str | None = 'v1.0.0',
) -> None:
    """The commit decision for the key `push`'s release decision names."""
    kept = decisions.keep_commit(
        paths, {
            'id': repository_id, 'pushed_at': push.isoformat(),
            'has_releases': tag is not None,
            'latest_stable_release': None if tag is None else {
                'tag_name': tag,
            },
            'download_target': {
                'ref': tag or 'main', 'ref_type': 'release' if tag else 'branch',
                'commit_sha': sha, 'commit_sha_short': sha[:7],
            },
        },
    )
    assert kept in (decisions.Outcome.WRITTEN, decisions.Outcome.KEPT)


def _tree(
    paths: PathConfig, repository_id: int, sha: str, text: str | None = None,
) -> None:
    path = paths.tree_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(f'{p}\n' for p in TREE) if text is None else text,
        encoding='utf-8',
    )


def _content(
    paths: PathConfig, repository_id: int, sha: str, *,
    stamp: int | None = CONTENT_VERSION,
    statuses: dict[str, str] | None = None,
    **changes: Any,
) -> None:
    """What the content stage leaves: the root, and `manifests.json`
    beside the tree, stamped with the stage's version unless `stamp` is
    None."""
    discovery = discover(TREE)
    root = paths.content_root(repository_id, sha)
    root.mkdir(parents=True, exist_ok=True)
    fetched: dict[str, dict[str, Any]] = {}
    written = []
    for item in discovery.selected:
        status = (statuses or {}).get(item.path, 'ok')
        if status == 'ok':
            target = root.joinpath(*item.path.split('/'))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('{}', encoding='utf-8')
            fetched[item.path] = {'status': 'ok', 'size': 2}
            written.append((item.path, 2))
        else:
            fetched[item.path] = {'status': status}
    document = discovery_document(
        discovery, repository_id=repository_id, commit_sha=sha,
        fetched=fetched, max_file_bytes=LIMITS['max_file_bytes'],
    )
    document['bytes'] = 2 * len(written)
    document['digest'] = content_digest(written)
    if stamp is not None:
        document[VERSION_FIELD] = stamp
    document.update(changes)
    path = paths.discovery_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=1), encoding='utf-8')


def _sbom(
    paths: PathConfig, repository_id: int, sha: str, *,
    version: str = SYFT, after_content: bool = True, text: str | None = None,
) -> None:
    path = paths.sbom_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        syft_document(version=version) if text is None else text,
        encoding='utf-8',
    )
    later = datetime.now(UTC).timestamp() + (3600 if after_content else -3600)
    os.utime(path, (later, later))


def _collected(
    paths: PathConfig, repository_id: int, push: datetime = P1,
    sha: str = S1, tag: str | None = 'v1.0.0',
) -> None:
    """A repository collected from its push to its SBOM."""
    _released(paths, repository_id, push, tag)
    _resolved(paths, repository_id, push, sha, tag)
    _tree(paths, repository_id, sha)
    _content(paths, repository_id, sha)
    _sbom(paths, repository_id, sha)


def _standing(
    paths: PathConfig, state: CollectorState, repository_id: int = 1,
    push: datetime | None = P1, *, syft: str | None = SYFT,
    now: datetime = NOW,
):
    return standing(
        repository_id, push, paths=paths, outcomes=state,
        syft_version=syft, now=now,
    )


def _states(found) -> dict[str, tuple[str, str]]:
    return {
        str(verdict.stage): (str(verdict.state), verdict.why)
        for verdict in found.verdicts
    }


# -- the chain ------------------------------------------------------------------


class TestTheChain:
    def test_is_the_five_stages_in_order(self):
        assert CHAIN == (
            Stage.RELEASE, Stage.COMMIT, Stage.TREE, Stage.CONTENT,
            Stage.SBOM,
        )

    def test_without_a_push_every_stage_waits_for_one(self, paths, state):
        found = _standing(paths, state, push=None)
        assert _states(found) == {
            str(stage): ('waiting', 'push') for stage in CHAIN
        }
        assert found.next is None
        assert found.rescan is False
        assert found.current is False

    def test_a_push_the_store_has_nothing_of_makes_its_release_due(
        self, paths, state,
    ):
        found = _standing(paths, state)
        assert _states(found) == {
            'release': ('due', 'undecided'),
            'commit': ('waiting', 'release'),
            'tree': ('waiting', 'release'),
            'content': ('waiting', 'release'),
            'sbom': ('waiting', 'release'),
        }
        assert found.next is not None
        assert found.next.stage is Stage.RELEASE
        # The push, as a decision states it: what an outcome is kept by.
        assert found.next.key == '2026-09-20T08:00:00Z'

    def test_a_release_decided_makes_the_commit_of_its_tag_due(
        self, paths, state,
    ):
        _released(paths, 1, P1, 'v1.0.0')
        found = _standing(paths, state)
        assert found.verdict(Stage.RELEASE).state is State.PRESENT
        assert found.verdict(Stage.RELEASE).output == 'v1.0.0'
        assert found.next is not None
        assert (found.next.stage, found.next.key, found.next.why) == (
            Stage.COMMIT, 'tag:v1.0.0', 'unresolved',
        )
        assert found.verdict(Stage.TREE).why == 'commit'

    def test_no_release_makes_the_head_at_the_push_the_key(
        self, paths, state,
    ):
        _released(paths, 1, P1, tag=None)
        found = _standing(paths, state)
        assert found.verdict(Stage.RELEASE).output == ''
        assert found.next is not None
        assert found.next.key == 'head:2026-09-20T08:00:00Z'

    def test_a_commit_resolved_makes_its_tree_due(self, paths, state):
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)
        found = _standing(paths, state)
        assert found.verdict(Stage.COMMIT).output == S1
        assert found.next is not None
        assert (found.next.stage, found.next.key, found.next.why) == (
            Stage.TREE, S1, 'missing',
        )
        assert found.commit is not None and found.commit.commit_sha == S1

    def test_a_whole_tree_makes_the_content_due(self, paths, state):
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)
        _tree(paths, 1, S1)
        found = _standing(paths, state)
        assert found.next is not None
        assert (found.next.stage, found.next.key, found.next.why) == (
            Stage.CONTENT, S1, 'missing',
        )

    def test_a_tree_cut_short_or_empty_is_not_there(self, paths, state):
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)
        _tree(paths, 1, S1, text='package.json\nsrc/ap')
        assert _standing(paths, state).verdict(Stage.TREE).why == 'cut-short'
        _tree(paths, 1, S1, text='')
        assert _standing(paths, state).verdict(Stage.TREE).why == 'empty'

    def test_the_content_makes_the_sbom_due(self, paths, state):
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)
        _tree(paths, 1, S1)
        _content(paths, 1, S1)
        found = _standing(paths, state)
        assert found.verdict(Stage.CONTENT).state is State.PRESENT
        assert found.next is not None
        assert (found.next.stage, found.next.why) == (Stage.SBOM, 'missing')
        # Kept by the commit and the Syft that scans it: a new Syft is
        # not held back by an old one's failure.
        assert found.next.key == sbom_key(S1, SYFT)
        assert found.next.key == f'{S1} syft@{SYFT}'

    def test_every_stage_present_is_current(self, paths, state):
        _collected(paths, 1)
        found = _standing(paths, state)
        assert {v.state for v in found.verdicts} == {State.PRESENT}
        assert found.current is True
        assert found.next is None
        assert found.rescan is False


class TestTheEarlyCutoff:
    """A push whose chain comes to a commit already collected finds the
    rest present: nothing after it is due."""

    def test_a_push_that_decides_the_same_release_needs_its_decision_alone(
        self, paths, state,
    ):
        _collected(paths, 1, P1, S1, tag='v1.0.0')
        found = _standing(paths, state, push=P2)
        assert found.next is not None
        assert found.next.stage is Stage.RELEASE
        assert found.rescan is False

        _released(paths, 1, P2, 'v1.0.0')

        after = _standing(paths, state, push=P2)
        assert after.current is True
        assert after.verdict(Stage.COMMIT).output == S1

    def test_a_push_resolved_to_the_same_commit_needs_nothing_more(
        self, paths, state,
    ):
        _collected(paths, 1, P1, S1, tag=None)
        _released(paths, 1, P2, tag=None)
        found = _standing(paths, state, push=P2)
        assert found.next is not None
        assert found.next.key == 'head:2026-09-29T09:30:00Z'

        _resolved(paths, 1, P2, S1, tag=None)

        assert _standing(paths, state, push=P2).current is True

    def test_a_push_resolved_to_another_commit_makes_its_tree_due(
        self, paths, state,
    ):
        _collected(paths, 1, P1, S1, tag=None)
        _released(paths, 1, P2, tag=None)
        _resolved(paths, 1, P2, S2, tag=None)
        found = _standing(paths, state, push=P2)
        assert found.next is not None
        assert (found.next.stage, found.next.key) == (Stage.TREE, S2)


class TestWaitingUpstreams:
    def test_a_stage_after_a_missing_one_waits_rather_than_is_due(
        self, paths, state,
    ):
        _released(paths, 1, P1)
        found = _standing(paths, state)
        waiting = [v for v in found.verdicts if v.state is State.WAITING]
        assert [str(v.stage) for v in waiting] == ['tree', 'content', 'sbom']
        assert {v.why for v in waiting} == {'commit'}
        assert [str(v.stage) for v in found.verdicts if v.state is State.DUE] == [
            'commit',
        ]

    def test_a_stage_backing_off_holds_the_rest_waiting(self, paths, state):
        _released(paths, 1, P1)
        state.record(
            1, 'commit', 'tag:v1.0.0', FAILED, now=NOW - timedelta(minutes=5),
            detail='git ls-remote failed',
        )
        found = _standing(paths, state)
        commit = found.verdict(Stage.COMMIT)
        assert commit.state is State.BACKING_OFF
        assert commit.why == 'failed'
        assert commit.due_at == NOW + timedelta(minutes=10)
        assert found.next is None
        assert found.rescan is False
        assert found.due_at == NOW + timedelta(minutes=10)
        assert found.verdict(Stage.TREE).state is State.WAITING


class TestTheContentStamp:
    """#100 Q4, strict: a content root whose `manifests.json` has no
    stamp of the content stage's version is fetched again."""

    def _through_tree(self, paths: PathConfig) -> None:
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)
        _tree(paths, 1, S1)
        _sbom(paths, 1, S1)

    def test_a_root_with_no_stamp_is_due(self, paths, state):
        self._through_tree(paths)
        _content(paths, 1, S1, stamp=None)
        verdict = _standing(paths, state).verdict(Stage.CONTENT)
        assert (verdict.state, verdict.why) == (State.DUE, 'unstamped')

    def test_a_root_an_earlier_version_stamped_is_due(self, paths, state):
        self._through_tree(paths)
        _content(paths, 1, S1, stamp=CONTENT_VERSION - 1)
        verdict = _standing(paths, state).verdict(Stage.CONTENT)
        assert (verdict.state, verdict.why) == (State.DUE, 'content-version')

    @pytest.mark.parametrize('stamp', [CONTENT_VERSION, CONTENT_VERSION + 1])
    def test_a_root_this_version_or_a_later_one_stamped_stands(
        self, paths, state, stamp,
    ):
        self._through_tree(paths)
        _content(paths, 1, S1, stamp=stamp)
        verdict = _standing(paths, state).verdict(Stage.CONTENT)
        assert verdict.state is State.PRESENT
        assert verdict.output  # the content's digest

    def test_a_stamp_is_a_number_not_true(self, paths, state):
        self._through_tree(paths)
        _content(paths, 1, S1, stamp=None, **{VERSION_FIELD: True})
        assert _standing(paths, state).verdict(Stage.CONTENT).why == (
            'unstamped'
        )

    @pytest.mark.parametrize(
        'changes,why', [
            ({'commit_sha': S2}, 'wrong-commit'),
            ({'limits': {**LIMITS, 'max_files': 1}}, 'limits-changed'),
        ],
    )
    def test_a_stamped_root_is_still_held_to_its_commit_and_limits(
        self, paths, state, changes, why,
    ):
        self._through_tree(paths)
        _content(paths, 1, S1, **changes)
        assert _standing(paths, state).verdict(Stage.CONTENT).why == why

    def test_a_root_with_a_file_that_may_yet_be_fetched_is_due(
        self, paths, state,
    ):
        self._through_tree(paths)
        _content(paths, 1, S1, statuses={'package.json': 'http-502'})
        assert _standing(paths, state).verdict(Stage.CONTENT).why == (
            'unsettled'
        )

    def test_a_root_whose_files_are_gone_is_due(self, paths, state):
        self._through_tree(paths)
        _content(paths, 1, S1)
        root = paths.content_root(1, S1)
        for path in sorted(root.rglob('*'), reverse=True):
            path.unlink() if path.is_file() else path.rmdir()
        root.rmdir()
        assert _standing(paths, state).verdict(Stage.CONTENT).why == (
            'root-missing'
        )

    def test_unreadable_manifests_are_due(self, paths, state):
        self._through_tree(paths)
        _content(paths, 1, S1)
        paths.discovery_file(1, S1).write_text('{"format": 1', 'utf-8')
        assert _standing(paths, state).verdict(Stage.CONTENT).why == (
            'unreadable'
        )


class TestTheSbom:
    """#110's rule: whole, by the Syft now running, and newer than every
    file it was made from."""

    def test_one_another_syft_wrote_is_due_as_a_rescan(self, paths, state):
        _collected(paths, 1)
        found = _standing(paths, state, syft='1.53.0')
        assert found.next is not None
        assert (found.next.stage, found.next.why) == (
            Stage.SBOM, 'another-syft',
        )
        assert found.next.key == sbom_key(S1, '1.53.0')
        assert found.rescan is True

    def test_one_older_than_its_content_is_due(self, paths, state):
        _collected(paths, 1)
        _sbom(paths, 1, S1, after_content=False)
        found = _standing(paths, state)
        assert found.next is not None
        assert found.next.why == 'input-changed'
        assert found.rescan is False

    def test_one_cut_short_is_due(self, paths, state):
        _collected(paths, 1)
        _sbom(paths, 1, S1, text=syft_document()[:100])
        assert _standing(paths, state).verdict(Stage.SBOM).why == 'unusable'

    def test_with_the_running_syft_unknown_the_times_decide(
        self, paths, state,
    ):
        _collected(paths, 1)
        found = _standing(paths, state, syft=None)
        assert found.current is True
        _sbom(paths, 1, S1, after_content=False)
        found = _standing(paths, state, syft=None)
        assert found.next is not None
        assert found.next.key == f'{S1} syft@unknown'


class TestBackoff:
    """#100 Q5: a stage that produced nothing for its key, or failed, is
    not due again until its backoff has passed."""

    def _through_commit(self, paths: PathConfig) -> None:
        _released(paths, 1, P1)
        _resolved(paths, 1, P1, S1)

    def test_nothing_backs_off_as_a_failure_does(self, paths, state):
        self._through_commit(paths)
        state.record(1, 'tree', S1, NOTHING, now=NOW, detail='no files')
        verdict = _standing(paths, state).verdict(Stage.TREE)
        assert (verdict.state, verdict.why) == (State.BACKING_OFF, 'nothing')
        assert verdict.due_at == NOW + timedelta(minutes=15)

    def test_it_is_due_again_once_the_backoff_has_passed(self, paths, state):
        self._through_commit(paths)
        state.record(1, 'tree', S1, FAILED, now=NOW, detail='git failed')
        later = NOW + timedelta(minutes=15)
        verdict = _standing(paths, state, now=later).verdict(Stage.TREE)
        assert (verdict.state, verdict.why) == (State.DUE, 'missing')

    def test_the_backoff_doubles_with_each_attempt(self, paths, state):
        self._through_commit(paths)
        state.record(1, 'tree', S1, FAILED, now=NOW)
        later = NOW + timedelta(minutes=15)
        state.record(1, 'tree', S1, FAILED, now=later)
        verdict = _standing(paths, state, now=later).verdict(Stage.TREE)
        assert verdict.due_at == later + timedelta(minutes=30)

    def test_an_outcome_of_another_key_holds_nothing_back(self, paths, state):
        self._through_commit(paths)
        state.record(1, 'tree', S2, FAILED, now=NOW)
        assert _standing(paths, state).verdict(Stage.TREE).state is State.DUE

    def test_an_sbom_failure_holds_back_that_syft_alone(self, paths, state):
        _collected(paths, 1)
        state.record(1, 'sbom', sbom_key(S1, '1.53.0'), FAILED, now=NOW)
        held = _standing(paths, state, syft='1.53.0')
        assert held.verdict(Stage.SBOM).state is State.BACKING_OFF
        free = _standing(paths, state, syft='1.54.0')
        assert free.verdict(Stage.SBOM).state is State.DUE


class TestRescans:
    """A stage due for a tool's new version, the Syft now running or the
    content stage's own, rather than for anything the repository did:
    CPU and downloads, and no quota, and the lowest priority (#128
    section 2.1). The two above it, a repository pushed and changed and
    one never collected, are collector.sqlite's to say (#160)."""

    def test_content_without_the_stamp_or_with_an_earlier_one_is_one(
        self, paths, state,
    ):
        _collected(paths, 1)
        _content(paths, 1, S1, stamp=None)
        assert _standing(paths, state).rescan is True
        _content(paths, 1, S1, stamp=CONTENT_VERSION - 1)
        assert _standing(paths, state).rescan is True

    def test_content_under_other_limits_is_one(self, paths, state):
        _collected(paths, 1)
        _content(paths, 1, S1, limits={**LIMITS, 'max_files': 1})
        assert _standing(paths, state).rescan is True

    def test_an_sbom_another_syft_wrote_is_one(self, paths, state):
        _collected(paths, 1)
        assert _standing(paths, state, syft='1.53.0').rescan is True

    def test_what_the_repository_did_is_none(self, paths, state):
        # A new push; a file that may yet be fetched; content newer than
        # its SBOM, a lockfile `sbom lock` resolved, say.
        _collected(paths, 1)
        assert _standing(paths, state, push=P2).rescan is False
        _content(paths, 1, S1, statuses={'package.json': 'http-502'})
        assert _standing(paths, state).rescan is False
        _content(paths, 1, S1)
        _sbom(paths, 1, S1, after_content=False)
        assert _standing(paths, state).rescan is False

    def test_a_rescan_backing_off_is_none_for_now(self, paths, state):
        _collected(paths, 1)
        state.record(1, 'sbom', sbom_key(S1, '1.53.0'), FAILED, now=NOW)
        found = _standing(paths, state, syft='1.53.0')
        assert found.next is None
        assert found.rescan is False
