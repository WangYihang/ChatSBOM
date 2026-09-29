"""The due set derived from the store, and how it differs from the
ledger's (#100, PR 1 of step 1).

A stage is done for a repository when its output for the current input
is in the store (`docs/design/first-principles.md`, §2.2). Release and
commit have no output files yet, so theirs is read from the ledger and
said to be. A stage whose input is not produced yet is waiting, not
due: the ledger counts it due, and the walk would find nothing to run.

Every difference from the ledger is given a reason, from the evidence
on either side.
"""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core import depgraph_store
from chatsbom.core import ledger as ledger_module
from chatsbom.core.catalog import Catalog
from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import discovery_document
from chatsbom.core.due import COMPARED
from chatsbom.core.due import derive
from chatsbom.core.due import read_ledger
from chatsbom.core.due import REASONS
from chatsbom.core.due import Settings
from chatsbom.core.due import State
from chatsbom.core.due import Store
from chatsbom.core.due import walk
from chatsbom.core.ledger import DERIVED_STAGES
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState
from chatsbom.core.ledger import STALE_INPUT
from chatsbom.core.ledger import Tracked
from chatsbom.services.content_service import MAX_FILE_BYTES
from chatsbom.services.run_service import STAGES
from tests.sbom_generate_test import cut_short
from tests.sbom_generate_test import syft_document
from tests.sbom_generate_test import SYFT_VERSION

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
PUSH = NOW - timedelta(days=2)
SHA = 'a' * 40
OTHER = 'b' * 40
TAG = 'v1.0.0'
TREE = ['README.md', 'package.json', 'package-lock.json', 'src/app.js']


@pytest.fixture
def paths(tmp_path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


@pytest.fixture
def ledger(paths) -> Iterable[Ledger]:
    with Ledger(paths.ledger_path) as handle:
        yield handle


# --- the store --------------------------------------------------------------

def _tree(
    paths: PathConfig, repository_id: int, sha: str = SHA,
    text: str | None = None,
) -> Path:
    path = paths.tree_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        ''.join(f'{p}\n' for p in TREE) if text is None else text,
        encoding='utf-8',
    )
    return path


def _content(
    paths: PathConfig,
    repository_id: int,
    sha: str = SHA,
    *,
    tree: list[str] | None = None,
    statuses: dict[str, dict[str, Any]] | None = None,
    **document_changes: Any,
) -> dict[str, Any]:
    """What the content stage leaves: `manifests.json` beside the tree,
    and the content root with every file it fetched."""
    discovery = discover(TREE if tree is None else tree)
    root = paths.content_root(repository_id, sha)
    root.mkdir(parents=True, exist_ok=True)
    fetched: dict[str, dict[str, Any]] = {}
    written = []
    for item in discovery.selected:
        outcome = (statuses or {}).get(item.path, {'status': 'ok'})
        if outcome.get('status') == 'ok':
            target = root.joinpath(*item.path.split('/'))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text('{}', encoding='utf-8')
            outcome = {'status': 'ok', 'size': 2}
            written.append((item.path, 2))
        fetched[item.path] = outcome
    document = discovery_document(
        discovery, repository_id=repository_id, commit_sha=sha,
        fetched=fetched, max_file_bytes=MAX_FILE_BYTES,
    )
    document['bytes'] = 2 * len(written)
    document['digest'] = content_digest(written)
    document.update(document_changes)
    path = paths.discovery_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, indent=1), encoding='utf-8')
    return document


def _sbom(
    paths: PathConfig, repository_id: int, document: str | None = None,
    *, sha: str = SHA, after_content: bool = True,
) -> Path:
    path = paths.sbom_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        syft_document() if document is None else document, encoding='utf-8',
    )
    later = datetime.now(timezone.utc).timestamp() + (
        3600 if after_content else -3600
    )
    os.utime(path, (later, later))
    return path


def _fetch(paths: PathConfig, repository_id: int, when: datetime) -> Path:
    stored = depgraph_store.store(
        paths.depgraph_dir, repository_id=repository_id, owner='o',
        repo=f'r{repository_id}', payload={'sbom': {'id': repository_id}},
        fetched_at=when, ref='main', head_sha=SHA, http_status=200,
    )
    return stored.fetch.document


# --- the ledger -------------------------------------------------------------

def _record(
    ledger: Ledger, repository_id: int, stage: Stage,
    input_key: str, output_key: str, **fields: Any,
) -> None:
    ledger.record_stage(
        StageState(
            repository_id=repository_id, stage=stage,
            done_at=fields.pop('done_at', NOW - timedelta(hours=1)),
            stage_version=fields.pop(
                'stage_version', ledger_module.STAGE_VERSION[stage],
            ),
            input_key=input_key, output_key=output_key,
            outcome=fields.pop('outcome', 'ok'), **fields,
        ),
    )


def _tracked(
    ledger: Ledger, repository_id: int, push: datetime | None = PUSH,
    snapshot: str = 'all-2026-09-27',
) -> str:
    ledger.seed(
        repository_id, 'o', f'r{repository_id}', snapshot=snapshot,
        default_branch='main', stars=100 + repository_id,
    )
    if push is not None:
        ledger.record_push(repository_id, push, NOW - timedelta(hours=3))
    state = ledger.get(repository_id)
    assert state is not None
    return state.pushed_at_seen.isoformat() if state.pushed_at_seen else ''


def _collected(
    ledger: Ledger,
    paths: PathConfig,
    repository_id: int,
    *,
    through: Stage = Stage.SBOM,
    files: bool = True,
) -> None:
    """A repository the walk collected through `through`, recorded as it
    records, and with its files where it leaves them."""
    push = _tracked(ledger, repository_id)
    document = _content(paths, repository_id) if files else {'digest': 'd'}
    keys = {
        Stage.RELEASE: (push, TAG),
        Stage.COMMIT: (TAG, SHA),
        Stage.TREE: (SHA, SHA),
        Stage.CONTENT: (SHA, document['digest']),
        Stage.SBOM: (document['digest'], 'h'),
    }
    for stage in STAGES:
        _record(ledger, repository_id, stage, *keys[stage])
        if stage is through:
            break
    if not files:
        return
    if STAGES.index(through) < STAGES.index(Stage.CONTENT):
        # Only the stages it reached have their files.
        paths.discovery_file(repository_id, SHA).unlink()
        for path in sorted(
            paths.content_root(repository_id, SHA).rglob('*'), reverse=True,
        ):
            path.unlink() if path.is_file() else path.rmdir()
        paths.content_root(repository_id, SHA).rmdir()
    if STAGES.index(through) >= STAGES.index(Stage.TREE):
        _tree(paths, repository_id)
    if through is Stage.SBOM:
        _sbom(paths, repository_id)


def _settings(**changes: Any) -> Settings:
    values: dict[str, Any] = {'now': NOW, 'syft_version': SYFT_VERSION}
    values.update(changes)
    return Settings(**values)


def _walk(
    paths: PathConfig, repository_id: int, *,
    listed_push: datetime | None = None, **settings: Any,
):
    view = read_ledger(paths.ledger_path, NOW)
    return walk(
        repository_id, view, Store(paths), _settings(**settings),
        listed_push=listed_push,
    )


def _states(verdicts) -> dict[Stage, State]:
    return {stage: verdict.state for stage, verdict in verdicts.items()}


def _chain(*states: State, graph: State | None = None) -> dict[Stage, State]:
    """The walk's stages in these states, and the graph in its own."""
    chain = dict(zip(STAGES, states))
    if graph is not None:
        chain[Stage.DEPGRAPH] = graph
    return chain


P, D, W, B, F = (
    State.PRESENT, State.DUE, State.WAITING, State.BLOCKED, State.DEFERRED,
)


# --- the tree ---------------------------------------------------------------

class TestTheTree:

    def test_a_whole_tree_is_present(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.TREE)

        tree = _walk(paths, 1)[Stage.TREE]

        assert (tree.state, tree.why, tree.ledger_backed) == (
            P, 'whole', False,
        )

    def test_a_tree_cut_short_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.TREE)
        _tree(paths, 1, text='README.md\npackage.js')

        tree = _walk(paths, 1)[Stage.TREE]

        assert (tree.state, tree.why) == (D, 'cut-short')

    def test_an_empty_tree_is_present_where_the_ledger_recorded_it(
        self, ledger, paths,
    ):
        """A commit with no files: `github tree` writes an empty list, and
        records the stage. A crash before the first flush leaves the same
        empty file, recorded as nothing."""
        _collected(ledger, paths, 1, through=Stage.TREE)
        _tree(paths, 1, text='')
        _collected(ledger, paths, 2, through=Stage.COMMIT)
        _tree(paths, 2, text='')

        one, two = _walk(paths, 1)[Stage.TREE], _walk(paths, 2)[Stage.TREE]

        assert (one.state, one.why, one.ledger_backed) == (
            P, 'empty-recorded', True,
        )
        assert (two.state, two.why) == (D, 'empty')

    def test_a_missing_tree_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.COMMIT)

        tree = _walk(paths, 1)[Stage.TREE]

        assert (tree.state, tree.why) == (D, 'missing')


# --- the content ------------------------------------------------------------

class TestTheContent:

    def test_a_settled_root_the_ledger_stamped_is_present(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)

        content = _walk(paths, 1)[Stage.CONTENT]

        assert (content.state, content.why, content.ledger_backed) == (
            P, 'ledger-stamp', True,
        )
        assert content.output == json.loads(
            paths.discovery_file(1, SHA).read_text(),
        )['digest']

    def test_a_document_for_another_commit_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        _content(paths, 1, commit_sha=OTHER)

        assert _walk(paths, 1)[Stage.CONTENT].why == 'wrong-commit'

    def test_a_document_under_other_limits_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        _content(
            paths, 1,
            limits={'max_files': 50, 'max_bytes': 1, 'max_file_bytes': 1},
        )

        content = _walk(paths, 1)[Stage.CONTENT]

        assert (content.state, content.why) == (D, 'limits-changed')

    @pytest.mark.parametrize(
        'status', ['error', 'http-503', 'http-500', 'http-429'],
    )
    def test_a_file_that_may_yet_be_fetched_is_unsettled(
        self, ledger, paths, status,
    ):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        _content(paths, 1, statuses={'package.json': {'status': status}})

        content = _walk(paths, 1)[Stage.CONTENT]

        assert (content.state, content.why) == (D, 'unsettled')

    @pytest.mark.parametrize(
        'outcome',
        [
            {'status': 'absent'}, {'status': 'http-404'},
            {'status': 'http-403'},
            {'status': 'over-file-byte-cap', 'size': 17 << 20},
        ],
    )
    def test_a_file_whose_answer_stands_is_settled(
        self, ledger, paths, outcome,
    ):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        _content(paths, 1, statuses={'package.json': outcome})

        assert _walk(paths, 1)[Stage.CONTENT].state is P

    def test_what_the_byte_cap_left_out_is_settled(self, ledger, paths):
        """The files after the cap have no status of their own, and are
        listed as skipped `over-byte-cap`: the content stage stops there
        again, rather than asking."""
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        document = _content(paths, 1)
        for entry in document['selected'][1:]:
            entry.pop('status')
            entry.pop('size')
            document['skipped'].append(
                {'path': entry['path'], 'reason': 'over-byte-cap'},
            )
        paths.discovery_file(1, SHA).write_text(json.dumps(document))

        assert _walk(paths, 1)[Stage.CONTENT].state is P

    def test_a_file_never_asked_for_is_unsettled(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        document = _content(paths, 1)
        document['selected'][0].pop('status')
        paths.discovery_file(1, SHA).write_text(json.dumps(document))

        assert _walk(paths, 1)[Stage.CONTENT].why == 'unsettled'

    @pytest.mark.parametrize(
        'break_it,why',
        [
            (lambda p: p.discovery_file(1, SHA).unlink(), 'missing'),
            (
                lambda p: p.discovery_file(1, SHA).write_text('{"format'),
                'unreadable',
            ),
            (
                lambda p: p.discovery_file(1, SHA).write_text('[]'),
                'unreadable',
            ),
        ],
    )
    def test_no_readable_document_is_due(self, ledger, paths, break_it, why):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        break_it(paths)

        assert _walk(paths, 1)[Stage.CONTENT].why == why

    def test_a_document_without_its_root_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        root = paths.content_root(1, SHA)
        for path in sorted(root.rglob('*'), reverse=True):
            path.unlink() if path.is_file() else path.rmdir()
        root.rmdir()

        assert _walk(paths, 1)[Stage.CONTENT].why == 'root-missing'


class TestTheContentVersion:
    """`manifests.json` says nothing of the version of the stage that
    wrote it. A stamp in the document would; until one is written, the
    ledger's row for that commit does, or discovering the tree again
    shows the selection is what this version would make."""

    def test_a_stamp_in_the_document_says_it(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.TREE)
        current = ledger_module.STAGE_VERSION[Stage.CONTENT]
        _content(paths, 1, stage_version=current)
        _collected(ledger, paths, 2, through=Stage.TREE)
        _content(paths, 2, stage_version=current - 1)

        one, two = (
            _walk(paths, 1)[Stage.CONTENT], _walk(paths, 2)[Stage.CONTENT],
        )

        assert (one.state, one.why, one.ledger_backed) == (
            P, 'stamped', False,
        )
        assert (two.state, two.why) == (D, 'content-version')

    def test_the_stamp_outranks_the_ledger(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        _content(paths, 1, stage_version=1)

        assert _walk(paths, 1)[Stage.CONTENT].why == 'content-version'

    def test_an_older_ledger_row_is_no_stamp(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        ledger._db.execute(
            'UPDATE stage_state SET stage_version = 2 '
            "WHERE stage = 'content'",
        )

        content = _walk(paths, 1)[Stage.CONTENT]

        assert (content.state, content.why) == (D, 'content-version')

    def test_a_row_for_another_commit_is_no_stamp(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        ledger._db.execute(
            "UPDATE stage_state SET input_key = ? WHERE stage = 'content'",
            (OTHER,),
        )

        assert _walk(paths, 1)[Stage.CONTENT].why == 'content-version'

    def test_rediscovery_finds_the_selection_unchanged(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        ledger._db.execute(
            "UPDATE stage_state SET stage_version = 2 WHERE stage = 'content'",
        )

        content = _walk(paths, 1, rediscover=True)[Stage.CONTENT]

        assert (content.state, content.why, content.ledger_backed) == (
            P, 'rediscovered', False,
        )

    def test_rediscovery_finds_what_this_version_adds(self, ledger, paths):
        """A pod's spec was not discovered before version 3: a root
        written without it is filled out."""
        _collected(ledger, paths, 1, through=Stage.CONTENT)
        ledger._db.execute(
            "UPDATE stage_state SET stage_version = 2 WHERE stage = 'content'",
        )
        _tree(paths, 1, text=''.join(f'{p}\n' for p in [*TREE, 'X.podspec']))

        content = _walk(paths, 1, rediscover=True)[Stage.CONTENT]

        assert (content.state, content.why) == (D, 'selection-changed')


# --- the SBOM ---------------------------------------------------------------

class TestTheSbom:
    """Every reason `staleness` (#110) gives, and its absence."""

    def test_a_current_sbom_is_present(self, ledger, paths):
        _collected(ledger, paths, 1)

        sbom = _walk(paths, 1)[Stage.SBOM]

        assert (sbom.state, sbom.why) == (P, 'current')

    def test_a_missing_sbom_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.CONTENT)

        assert _walk(paths, 1)[Stage.SBOM].why == 'missing'

    def test_an_sbom_cut_short_is_unusable(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, cut_short(syft_document()))

        assert _walk(paths, 1)[Stage.SBOM].why == 'unusable'

    def test_one_another_syft_wrote_is_due(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, syft_document(version='1.41.2'))

        sbom = _walk(paths, 1)[Stage.SBOM]

        assert (sbom.state, sbom.why) == (D, 'another-syft')

    def test_the_version_can_be_named(self, ledger, paths):
        """The collector's Syft is in its image; the host's, if any, is
        another. `--syft-version` names the one the SBOMs are for."""
        _collected(ledger, paths, 1)
        _sbom(paths, 1, syft_document(version='1.41.2'))

        assert _walk(paths, 1, syft_version='1.41.2')[Stage.SBOM].state is P

    def test_one_older_than_its_content_is_due(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, after_content=False)

        assert _walk(paths, 1)[Stage.SBOM].why == 'input-changed'

    def test_with_no_version_known_times_alone_decide(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, syft_document(version='1.41.2'))

        assert _walk(paths, 1, syft_version=None)[Stage.SBOM].state is P


# --- the dependency graph ---------------------------------------------------

class TestTheDependencyGraph:
    """Due on the ledger's clock (`depgraph_stage.next_state` set it),
    and present when the store has the graph."""

    def _graph(self, ledger, repository_id, outcome, retry, **fields):
        _record(
            ledger, repository_id, Stage.DEPGRAPH, '', fields.pop('key', ''),
            outcome=outcome, next_attempt_at=NOW + retry,
            stage_version=2, **fields,
        )

    def test_never_asked_is_due(self, ledger, paths):
        _tracked(ledger, 1)

        graph = _walk(paths, 1)[Stage.DEPGRAPH]

        assert (graph.state, graph.why) == (D, 'never-asked')

    def test_a_graph_fetched_and_kept_is_present(self, ledger, paths):
        _tracked(ledger, 1)
        self._graph(ledger, 1, 'ok', timedelta(days=20))
        _fetch(paths, 1, NOW - timedelta(days=10))

        assert _walk(paths, 1)[Stage.DEPGRAPH].state is P

    def test_a_graph_the_ledger_has_and_the_store_has_not_is_due(
        self, ledger, paths,
    ):
        _tracked(ledger, 1)
        self._graph(ledger, 1, 'ok', timedelta(days=20))

        graph = _walk(paths, 1)[Stage.DEPGRAPH]

        assert (graph.state, graph.why) == (D, 'missing')

    def test_the_graph_kept_from_before_every_fetch_is_one(
        self, ledger, paths,
    ):
        _tracked(ledger, 1)
        self._graph(ledger, 1, 'ok', timedelta(days=20))
        legacy = paths.legacy_depgraph_file(1)
        legacy.parent.mkdir(parents=True)
        legacy.write_text('{"sbom": {}}')

        assert _walk(paths, 1)[Stage.DEPGRAPH].why == 'legacy'

    def test_the_refresh_is_due(self, ledger, paths):
        _tracked(ledger, 1)
        self._graph(ledger, 1, 'ok', -timedelta(days=1))
        _fetch(paths, 1, NOW - timedelta(days=31))

        graph = _walk(paths, 1)[Stage.DEPGRAPH]

        assert (graph.state, graph.why) == (D, 'refresh')

    def test_no_graph_is_deferred_until_its_negative_cache_expires(
        self, ledger, paths,
    ):
        _tracked(ledger, 1)
        _tracked(ledger, 2)
        self._graph(ledger, 1, 'absent', timedelta(days=5))
        self._graph(ledger, 2, 'absent', -timedelta(days=5))

        one, two = _walk(paths, 1), _walk(paths, 2)

        assert (one[Stage.DEPGRAPH].state, one[Stage.DEPGRAPH].why) == (
            F, 'no-graph',
        )
        assert (two[Stage.DEPGRAPH].state, two[Stage.DEPGRAPH].why) == (
            D, 'expired',
        )

    @pytest.mark.parametrize('outcome', ['failed', 'too_large'])
    def test_a_failure_backing_off_is_blocked(self, ledger, paths, outcome):
        _tracked(ledger, 1)
        self._graph(ledger, 1, outcome, timedelta(hours=1), failure_count=1)

        assert _walk(paths, 1)[Stage.DEPGRAPH].state is B

    def test_a_repository_that_is_gone_is_deferred(self, ledger, paths):
        _tracked(ledger, 1)
        ledger.record_absent(1, NOW, retry_at=NOW - timedelta(days=1))

        graph = _walk(paths, 1)[Stage.DEPGRAPH]

        assert (graph.state, graph.why) == (F, 'absent')

    def test_a_fresh_graph_the_ledger_never_recorded_is_present(
        self, ledger, paths,
    ):
        _tracked(ledger, 1)
        _tracked(ledger, 2)
        _fetch(paths, 1, NOW - timedelta(days=2))
        _fetch(paths, 2, NOW - timedelta(days=45))

        assert _walk(paths, 1)[Stage.DEPGRAPH].why == 'fetched'
        assert _walk(paths, 2)[Stage.DEPGRAPH].why == 'never-asked'

    def test_a_watermark_within_the_refresh_is_present(self, ledger, paths):
        """A graph fetched before `stage_state`, not yet adopted."""
        _tracked(ledger, 1)
        state = ledger.get(1)
        assert state is not None
        state.stage_watermarks[Stage.DEPGRAPH] = NOW - timedelta(days=3)
        ledger.upsert(state)

        graph = _walk(paths, 1)[Stage.DEPGRAPH]

        assert (graph.state, graph.why, graph.ledger_backed) == (
            P, 'watermark', True,
        )


# --- the chain --------------------------------------------------------------

class TestTheChain:

    def test_a_repository_never_seen_has_its_release_due_and_waits(
        self, paths,
    ):
        view = read_ledger(paths.ledger_path, NOW)

        verdicts = walk(7, view, Store(paths), _settings())

        assert _states(verdicts) == _chain(D, W, W, W, W, graph=D)
        assert verdicts[Stage.RELEASE].why == 'never-run'
        assert verdicts[Stage.TREE].why == 'release', 'waiting on release'

    def test_a_collected_repository_has_nothing_due(self, ledger, paths):
        _collected(ledger, paths, 1)

        verdicts = _walk(paths, 1)

        assert _states(verdicts) == _chain(P, P, P, P, P, graph=D)
        assert verdicts[Stage.RELEASE].ledger_backed
        assert verdicts[Stage.COMMIT].output == SHA

    def test_a_push_makes_release_due(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger.record_push(1, NOW - timedelta(hours=1), NOW)

        verdicts = _walk(paths, 1)

        assert _states(verdicts) == _chain(D, W, W, W, W, graph=D)
        assert verdicts[Stage.RELEASE].why == 'push'

    def test_the_same_tag_leaves_the_commit_alone(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger.record_push(1, NOW - timedelta(hours=1), NOW)
        state = ledger.get(1)
        assert state is not None and state.pushed_at_seen is not None
        _record(
            ledger, 1, Stage.RELEASE, state.pushed_at_seen.isoformat(), TAG,
        )

        assert _states(_walk(paths, 1))[Stage.COMMIT] is P

    def test_another_tag_makes_the_commit_due(self, ledger, paths):
        _collected(ledger, paths, 1)
        state = ledger.get(1)
        assert state is not None and state.pushed_at_seen is not None
        _record(
            ledger, 1, Stage.RELEASE, state.pushed_at_seen.isoformat(), 'v2',
        )

        verdicts = _walk(paths, 1)

        assert _states(verdicts) == _chain(P, D, W, W, W, graph=D)
        assert verdicts[Stage.COMMIT].why == 'upstream-moved'

    def test_the_same_commit_leaves_nothing_to_do(self, ledger, paths):
        """A new tag at the same commit: commit runs, and everything after
        it is in the store already."""
        _collected(ledger, paths, 1)
        state = ledger.get(1)
        assert state is not None and state.pushed_at_seen is not None
        _record(
            ledger, 1, Stage.RELEASE, state.pushed_at_seen.isoformat(), 'v2',
        )
        _record(ledger, 1, Stage.COMMIT, 'v2', SHA)

        assert _states(_walk(paths, 1)) == _chain(P, P, P, P, P, graph=D)

    def test_a_blocked_stage_holds_the_walk(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.COMMIT)
        ledger.record_stage_failure(1, Stage.TREE, NOW, 'git exploded')

        verdicts = _walk(paths, 1)

        assert _states(verdicts) == _chain(P, P, B, W, W, graph=D)
        assert verdicts[Stage.CONTENT].why == 'tree'

    def test_a_failure_whose_backoff_ran_out_is_due(self, ledger, paths):
        _collected(ledger, paths, 1, through=Stage.COMMIT)
        ledger.record_stage_failure(
            1, Stage.TREE, NOW - timedelta(days=1), 'git exploded',
        )

        tree = _walk(paths, 1)[Stage.TREE]

        assert (tree.state, tree.why) == (D, 'missing')

    def test_a_deferred_repository_is_left_alone(self, ledger, paths):
        """`queue sync`'s backoff holds every stage the walk runs; the
        dependency graph is scheduled on its own, as the ledger does."""
        _collected(ledger, paths, 1)
        ledger.record_push(1, NOW - timedelta(hours=1), NOW)
        ledger.record_failure(1, Stage.REPO, NOW, 'HTTP 502')

        verdicts = _walk(paths, 1)

        assert _states(verdicts) == _chain(F, F, F, F, F, graph=D)
        assert verdicts[Stage.RELEASE].why == 'backoff'

    def test_a_repository_gone_is_deferred_everywhere(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger.record_absent(1, NOW, retry_at=NOW + timedelta(days=14))

        verdicts = _walk(paths, 1)

        assert set(_states(verdicts).values()) == {F}
        assert verdicts[Stage.RELEASE].why == 'absent'

    def test_a_push_the_snapshot_saw_first_makes_release_due(
        self, ledger, paths,
    ):
        _collected(ledger, paths, 1)

        later = _walk(paths, 1, listed_push=NOW - timedelta(hours=1))
        same = _walk(paths, 1, listed_push=PUSH)

        assert (later[Stage.RELEASE].state, later[Stage.RELEASE].why) == (
            D, 'head-moved',
        )
        assert same[Stage.RELEASE].state is P

    def test_a_commit_whose_output_the_ledger_never_learned_is_due(
        self, ledger, paths,
    ):
        """Adopted from a watermark, a row says the stage ran and not
        what it produced: with no commit, no store key can be read."""
        _collected(ledger, paths, 1)
        ledger._db.execute(
            "UPDATE stage_state SET output_key = '' WHERE stage = 'commit'",
        )

        verdicts = _walk(paths, 1)

        assert _states(verdicts)[Stage.COMMIT] is D
        assert verdicts[Stage.COMMIT].why == 'output-unknown'

    def test_nothing_after_a_due_stage_is_read(
        self, ledger, paths, monkeypatch,
    ):
        """Lazily: a stage waiting on another is not probed."""
        _collected(ledger, paths, 1, through=Stage.COMMIT)

        def unread(*args: object, **kwargs: object):
            raise AssertionError('read past the due stage')

        monkeypatch.setattr(Store, 'discovery', unread)
        monkeypatch.setattr(Store, 'sbom', unread)

        assert _states(_walk(paths, 1))[Stage.TREE] is D

    def test_only_the_stages_asked_for_are_walked(
        self, ledger, paths, monkeypatch,
    ):
        _collected(ledger, paths, 1)

        def unread(*args: object, **kwargs: object):
            raise AssertionError('read a stage not asked for')

        monkeypatch.setattr(Store, 'sbom', unread)
        monkeypatch.setattr(Store, 'depgraph', unread)
        view = read_ledger(paths.ledger_path, NOW)

        verdicts = walk(
            1, view, Store(paths), _settings(),
            stages=(Stage.TREE, Stage.CONTENT),
        )

        assert set(verdicts) == {Stage.TREE, Stage.CONTENT}


# --- the ledger's own view, walk by walk ------------------------------------

def test_consistent_files_make_the_walks_the_ledgers_would(
    paths, monkeypatch,
):
    """Every combination the adoption test holds the push rule to
    (`stage_state_test.COMBINATIONS`), recorded with real keys, and the
    store holding exactly the outputs the ledger says are current: the
    first stage a walk finds due is the first stage the ledger has due.
    The later stages differ, as they should: the ledger counts a stage
    whose input is not produced yet as due, and derived calls it
    waiting."""
    from tests.stage_state_test import PUSHED
    from tests.stage_state_test import TestAdoptingTheWatermarks

    for stage in DERIVED_STAGES:
        monkeypatch.setitem(ledger_module.STAGE_VERSION, stage, 1)
    outputs = {
        Stage.RELEASE: lambda rid: TAG,
        Stage.COMMIT: lambda rid: f'{rid:040x}',
        Stage.TREE: lambda rid: f'{rid:040x}',
        Stage.CONTENT: lambda rid: 'digest',
        Stage.LOCK: lambda rid: 'lock',
        Stage.SBOM: lambda rid: 'h',
    }
    expected: dict[int, Stage | None] = {}
    with Ledger(paths.ledger_path) as ledger:
        for number, marks in enumerate(
            TestAdoptingTheWatermarks.COMBINATIONS, start=1,
        ):
            for pushed in (PUSHED, None):
                rid = number * 2 + (pushed is None)
                sha = f'{rid:040x}'
                ledger.track(rid, 'o', f'r{rid}', 'ruby')
                if pushed is not None:
                    ledger.record_push(rid, pushed, NOW)
                produced: dict[Stage, str] = {}
                current: dict[Stage, bool] = {}
                for stage, mark in zip(DERIVED_STAGES, marks):
                    if mark is None:
                        continue
                    upstream = ledger_module.UPSTREAM[stage]
                    consumed = (
                        (pushed.isoformat() if pushed else '')
                        if upstream is Stage.REPO
                        else produced.get(upstream, '')
                    )
                    current[stage] = pushed is None or mark >= pushed
                    produced[stage] = outputs[stage](rid)
                    _record(
                        ledger, rid, stage,
                        consumed if current[stage] else STALE_INPUT,
                        produced[stage], done_at=mark, stage_version=1,
                    )
                if current.get(Stage.TREE):
                    _tree(paths, rid, sha)
                if current.get(Stage.CONTENT):
                    _content(paths, rid, sha)
                if current.get(Stage.SBOM):
                    _sbom(paths, rid, sha=sha)
        due = {stage: set(ledger._due_ids(stage, NOW)) for stage in STAGES}
        for rid in [
            row[0] for row in ledger._db.execute(
                'SELECT repository_id FROM repository_state',
            )
        ]:
            expected[rid] = next(
                (stage for stage in STAGES if rid in due[stage]), None,
            )

    view = read_ledger(paths.ledger_path, NOW)
    store = Store(paths)
    found: dict[int, Stage | None] = {}
    for rid in expected:
        verdicts = walk(rid, view, store, _settings(), stages=STAGES)
        assert not {B, F} & set(_states(verdicts).values()), rid
        found[rid] = next(
            (s for s in STAGES if verdicts[s].state is D), None,
        )

    assert found == expected
    assert set(found.values()) == {*STAGES, None}, 'every walk is covered'


# --- differences from the ledger, and why -----------------------------------

def _catalog(
    *ids: int, pushed: dict[int, datetime] | None = None,
    source: str = 'all-2026-09-27',
) -> Catalog:
    return Catalog(
        source,
        {
            i: Tracked(
                repository_id=i, owner='o', repo=f'r{i}', snapshot=source,
            )
            for i in ids
        },
        pushed or {},
    )


def _reasons(report, stage: Stage) -> dict[str, dict[str, list[int]]]:
    """`side -> code -> ids` for one stage of a report, each code one
    `REASONS` explains."""
    compared = report.stages[stage]
    assert {*compared.derived_only, *compared.ledger_only} <= set(REASONS)
    return {
        side: {code: tally.samples for code, tally in reasons.items()}
        for side, reasons in (
            ('derived-only', compared.derived_only),
            ('ledger-only', compared.ledger_only),
        )
        if reasons
    }


def _derive(
    paths: PathConfig, catalog: Catalog, *,
    stages: Iterable[Stage] = COMPARED, **settings: Any,
):
    return derive(
        catalog, lambda repos: read_ledger(
            paths.ledger_path, NOW, repos=repos,
        ),
        Store(paths), _settings(**settings), stages=tuple(stages),
        compare=True, samples=10,
    )


class TestWhyTheyDiffer:

    def test_a_repository_the_ledger_has_and_the_snapshot_does_not(
        self, ledger, paths,
    ):
        _tracked(ledger, 1, snapshot='')
        _tracked(ledger, 2, snapshot='all-2026-09-20')
        _tracked(ledger, 3, snapshot='all-2026-09-29')
        _tracked(ledger, 4)
        ledger.record_absent(4, NOW, retry_at=NOW - timedelta(days=1))
        _tracked(ledger, 5, snapshot='all-2026-09-27')
        _tracked(ledger, 6, snapshot='java-2026-09-01')

        report = _derive(paths, _catalog(9), stages=[Stage.RELEASE])

        assert _reasons(report, Stage.RELEASE) == {
            'derived-only': {'universe:snapshot-only': [9]},
            'ledger-only': {
                'universe:ledger-only:unlisted': [1],
                'universe:ledger-only:older-snapshot': [2],
                'universe:ledger-only:newer-snapshot': [3],
                'universe:ledger-only:absent': [4],
                'universe:ledger-only:snapshot-differs': [5],
                'universe:ledger-only:other-snapshot': [6],
            },
        }

    def test_a_push_only_the_snapshot_saw(self, ledger, paths):
        _collected(ledger, paths, 1)

        report = _derive(
            paths, _catalog(1, pushed={1: NOW - timedelta(hours=1)}),
            stages=[Stage.RELEASE],
        )

        assert _reasons(report, Stage.RELEASE) == {
            'derived-only': {'head-moved': [1]},
        }

    def test_a_stage_that_ran_for_another_head(self, ledger, paths):
        """`github tree` walks commit for its hand-off without recording
        it: the tree it records consumed a commit the ledger's commit row
        never produced."""
        _collected(ledger, paths, 1)
        _record(ledger, 1, Stage.TREE, OTHER, OTHER)

        report = _derive(paths, _catalog(1), stages=[Stage.TREE])

        assert _reasons(report, Stage.TREE) == {
            'ledger-only': {'head-moved': [1]},
        }

    def test_a_content_root_whose_selection_is_unchanged(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger._db.execute(
            "UPDATE stage_state SET stage_version = 2 WHERE stage = 'content'",
        )

        plain = _derive(paths, _catalog(1), stages=[Stage.CONTENT])
        rediscovered = _derive(
            paths, _catalog(1), stages=[Stage.CONTENT], rediscover=True,
        )

        assert _reasons(plain, Stage.CONTENT) == {}
        assert plain.stages[Stage.CONTENT].both == 1
        assert _reasons(rediscovered, Stage.CONTENT) == {
            'ledger-only': {'selection-unchanged': [1]},
        }

    def test_a_content_root_this_version_would_fill_out(self, ledger, paths):
        """The ledger's content row is for the commit a hand-off walk found
        (so it vouches for no root of this one), and the tree of this one
        holds a pod's spec its root was never given."""
        _collected(ledger, paths, 1)
        _record(ledger, 1, Stage.TREE, OTHER, OTHER)
        _record(ledger, 1, Stage.CONTENT, OTHER, 'd')
        _tree(paths, 1, text=''.join(f'{p}\n' for p in [*TREE, 'X.podspec']))

        report = _derive(
            paths, _catalog(1), stages=[Stage.CONTENT], rediscover=True,
        )

        assert _reasons(report, Stage.CONTENT) == {
            'derived-only': {'selection-changed': [1]},
        }

    def test_an_sbom_another_syft_wrote(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, syft_document(version='1.41.2'))

        report = _derive(paths, _catalog(1), stages=[Stage.SBOM])

        assert _reasons(report, Stage.SBOM) == {
            'derived-only': {'another-syft': [1]},
        }

    @pytest.mark.parametrize(
        'stage,remove',
        [
            (Stage.TREE, lambda p: p.tree_file(1, SHA).unlink()),
            (Stage.CONTENT, lambda p: p.discovery_file(1, SHA).unlink()),
            (Stage.SBOM, lambda p: p.sbom_file(1, SHA).unlink()),
        ],
    )
    def test_a_file_the_ledger_counts_on_is_gone(
        self, ledger, paths, stage, remove,
    ):
        _collected(ledger, paths, 1)
        remove(paths)

        report = _derive(paths, _catalog(1), stages=[stage])

        assert _reasons(report, stage) == {
            'derived-only': {'file-missing': [1]},
        }

    def test_files_the_ledger_has_no_row_for(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger._db.execute("DELETE FROM stage_state WHERE stage = 'tree'")

        report = _derive(paths, _catalog(1), stages=[Stage.TREE])

        assert _reasons(report, Stage.TREE) == {
            'ledger-only': {'lost-record': [1]},
        }

    def test_what_waits_on_a_stage_not_run_yet(self, ledger, paths):
        _tracked(ledger, 1)

        report = _derive(paths, _catalog(1))

        for stage in (Stage.COMMIT, Stage.TREE, Stage.CONTENT, Stage.SBOM):
            assert _reasons(report, stage) == {
                'ledger-only': {'upstream-not-run': [1]},
            }, stage
        assert report.stages[Stage.RELEASE].both == 1
        assert report.stages[Stage.TREE].states[W] == 1

    def test_an_sbom_row_from_an_older_stage_version(self, ledger, paths):
        _collected(ledger, paths, 1)
        ledger._db.execute(
            "UPDATE stage_state SET stage_version = 1 WHERE stage = 'sbom'",
        )

        report = _derive(paths, _catalog(1), stages=[Stage.SBOM])

        assert _reasons(report, Stage.SBOM) == {
            'ledger-only': {'stage-version': [1]},
        }

    def test_a_commit_the_ledger_adopted_without_its_output(
        self, ledger, paths,
    ):
        _collected(ledger, paths, 1)
        ledger._db.execute(
            "UPDATE stage_state SET output_key = '' WHERE stage = 'commit'",
        )

        report = _derive(paths, _catalog(1), stages=[Stage.COMMIT])

        assert _reasons(report, Stage.COMMIT) == {
            'derived-only': {'output-unknown': [1]},
        }

    def test_what_only_the_store_can_see_of_the_content(self, ledger, paths):
        for repository_id in (1, 2, 3):
            _collected(ledger, paths, repository_id)
        _content(
            paths, 1,
            limits={'max_files': 50, 'max_bytes': 1, 'max_file_bytes': 1},
        )
        _content(paths, 2, statuses={'package.json': {'status': 'error'}})
        _content(paths, 3, stage_version=1)
        for repository_id in (1, 2, 3):
            _sbom(paths, repository_id)

        report = _derive(paths, _catalog(1, 2, 3), stages=[Stage.CONTENT])

        assert _reasons(report, Stage.CONTENT) == {
            'derived-only': {
                'limits-changed': [1], 'unsettled': [2],
                'content-version': [3],
            },
        }

    def test_content_newer_than_its_sbom(self, ledger, paths):
        _collected(ledger, paths, 1)
        _sbom(paths, 1, after_content=False)

        report = _derive(paths, _catalog(1), stages=[Stage.SBOM])

        assert _reasons(report, Stage.SBOM) == {
            'derived-only': {'input-changed': [1]},
        }

    def test_a_graph_a_worker_is_fetching(self, ledger, paths):
        _tracked(ledger, 1)
        assert ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'depgraph')

        report = _derive(paths, _catalog(1), stages=[Stage.DEPGRAPH])

        assert _reasons(report, Stage.DEPGRAPH) == {
            'derived-only': {'leased': [1]},
        }

    def test_graphs_the_ledger_and_the_store_disagree_on(self, ledger, paths):
        _tracked(ledger, 1)
        _record(
            ledger, 1, Stage.DEPGRAPH, '', 'x', stage_version=2,
            next_attempt_at=NOW + timedelta(days=20),
        )
        _tracked(ledger, 2)
        _fetch(paths, 2, NOW - timedelta(days=1))

        report = _derive(paths, _catalog(1, 2), stages=[Stage.DEPGRAPH])

        assert _reasons(report, Stage.DEPGRAPH) == {
            'derived-only': {'file-missing': [1]},
            'ledger-only': {'lost-record': [2]},
        }

    def test_what_converges_on_a_second_look_is_timing(
        self, ledger, paths, monkeypatch,
    ):
        """The collector writes while this reads: a tree written between
        the ledger's read and the store's is the ledger's to catch up
        on, and the second look finds they agree."""
        _collected(ledger, paths, 1, through=Stage.COMMIT)
        reads = []

        def reading(repos):
            reads.append(repos)
            if len(reads) == 2:
                with Ledger(paths.ledger_path) as writer:
                    _record(writer, 1, Stage.TREE, SHA, SHA)
            return read_ledger(paths.ledger_path, NOW, repos=repos)

        _tree(paths, 1)
        report = derive(
            _catalog(1), reading, Store(paths), _settings(),
            stages=(Stage.TREE,), compare=True, samples=10,
        )

        assert _reasons(report, Stage.TREE) == {
            'ledger-only': {'timing': [1]},
        }
        assert (report.rechecked, report.converged) == (1, 1)
        assert reads == [None, {1}]

    def test_a_difference_no_rule_explains(self, ledger, paths, monkeypatch):
        from chatsbom.core import due

        _collected(ledger, paths, 1)
        real = due.walk

        def odd(*args: Any, **kwargs: Any):
            verdicts = real(*args, **kwargs)
            if Stage.TREE in verdicts:
                verdicts[Stage.TREE] = due.Verdict(State.DUE, 'odd')
            return verdicts

        monkeypatch.setattr(due, 'walk', odd)

        report = _derive(paths, _catalog(1), stages=[Stage.TREE])

        assert _reasons(report, Stage.TREE) == {
            'derived-only': {'unexplained': [1]},
        }

    def test_samples_are_the_lowest_ids_and_counts_are_whole(
        self, ledger, paths,
    ):
        for repository_id in range(1, 16):
            _tracked(ledger, repository_id)

        report = derive(
            _catalog(), lambda repos: read_ledger(
                paths.ledger_path, NOW, repos=repos,
            ),
            Store(paths), _settings(), stages=(Stage.RELEASE,),
            compare=True, samples=3,
        )

        tally = report.stages[Stage.RELEASE].ledger_only[
            'universe:ledger-only:snapshot-differs'
        ]
        assert (tally.count, tally.samples) == (15, [1, 2, 3])


def test_the_digest_of_a_content_root_is_its_output(ledger, paths):
    """What the SBOM would be due against, read from the document."""
    _collected(ledger, paths, 1)
    document = json.loads(paths.discovery_file(1, SHA).read_text())

    assert _walk(paths, 1)[Stage.CONTENT].output == document['digest']
    assert document['digest'] == hashlib.sha256(
        b'package-lock.json\x002\npackage.json\x002\n',
    ).hexdigest()
