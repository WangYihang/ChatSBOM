"""`data migrate-layout`, on a synthetic tree in both layouts' shapes.

What it must never do is lose or change a byte of the only copy of the
corpus. So each test takes a snapshot of every file first, and the ones
that end in a rollback compare against it byte for byte.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core import migrate_layout as ml
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import setup_logging
from tests.conftest import requires_clickhouse

SHA = '0123456789abcdef0123456789abcdef01234567'
SHA2 = 'fedcba9876543210fedcba9876543210fedcba98'
HASH = 'ab' * 32

runner = CliRunner()


def _write(path: Path, body: str | bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(body, str):
        body = body.encode()
    path.write_bytes(body)
    return path


def _jsonl(path: Path, *records: dict) -> None:
    _write(path, ''.join(json.dumps(r) + '\n' for r in records))


def build(root: Path) -> ml.Roots:
    """A corpus in the old layout: two repositories, and the shapes the
    real one has — a scan under two refs at one commit, a Syft cache
    entry under two refs, a legacy graph, a generated lockfile, and the
    unversioned Syft cache nothing reads."""
    data, cache = root / 'data', root / '.cache'
    tree = data / '05-github-tree'
    content = data / '06-github-content'
    sbom = data / '07-sbom'
    graph = data / '09-github-depgraph'
    lock = data / '10-generated-lock'

    _write(tree / f'go/o/r/v1/{SHA}/tree.txt', 'go.mod\nsub/go.mod\n')
    # The release bug: one commit under `HEAD` and `main`, byte-identical.
    _write(tree / f'go/o/r/HEAD/{SHA2}/tree.txt', 'go.mod\n')
    _write(tree / f'go/o/r/main/{SHA2}/tree.txt', 'go.mod\n')
    _write(tree / f'ruby/Mikel/Mail/v2/{SHA}/tree.txt', 'Gemfile\n')
    _write(content / f'go/o/r/v1/{SHA}/go.mod', 'module o/r\n')
    _write(content / f'go/o/r/v1/{SHA}/sub/go.mod', 'module o/r/sub\n')
    _write(content / f'ruby/Mikel/Mail/v2/{SHA}/Gemfile', "gem 'rack'\n")
    _write(sbom / f'go/o/r/v1/{SHA}/sbom.json', '{"artifacts": []}')
    _write(sbom / f'ruby/Mikel/Mail/v2/{SHA}/sbom.json', '{"artifacts": [1]}')
    _write(
        graph / 'go/o/r/sbom.spdx.json',
        json.dumps(
            {'sbom': {'creationInfo': {'created': '2026-09-14T03:56:20Z'}}},
        ),
    )
    _write(lock / f'ruby/Mikel/Mail/{SHA}/Gemfile.lock', 'GEM\n')
    # A ref with slashes spans directories.
    _write(
        sbom /
        f'typescript/Shopify/polaris-react/@shopify/polaris@13.9.5/{SHA2}/sbom.json',
        '{"artifacts": [2]}',
    )
    _write(
        cache /
        f'git-tree/Shopify/polaris-react/@shopify/polaris@13.9.5/{SHA2}/tree.txt',
        'package.json\n',
    )
    # The lists, as the pipeline wrote them.
    _jsonl(
        sbom / 'go.jsonl',
        {
            'id': 11, 'owner': 'o', 'repo': 'r', 'language': 'Go',
            'local_content_path': f'data/06-github-content/go/o/r/v1/{SHA}',
            'sbom_path': f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json',
        },
    )
    _jsonl(
        sbom / 'typescript.jsonl',
        {'id': 13, 'owner': 'Shopify', 'repo': 'polaris-react'},
    )
    _jsonl(
        graph / 'go.jsonl',
        {
            'id': 11, 'owner': 'o', 'repo': 'r',
            'depgraph_path': 'data/09-github-depgraph/go/o/r/sbom.spdx.json',
        },
    )
    # The caches.
    _write(cache / f'syft/1.41.2/o/r/v1/{HASH}.json', '{"a": 1}')
    _write(cache / f'syft/1.41.2/o/r/HEAD/{HASH}.json', '{"a": 1}')
    _write(cache / 'syft/someone/thing/main/cd.json', '{"old": true}')
    _write(cache / f'git-tree/mikel/mail/v2/{SHA}/tree.txt', 'Gemfile\n')
    # The ledger knows one of them, under today's spelling.
    with Ledger(data / 'ledger.sqlite3') as ledger:
        ledger.track(12, 'mikel', 'mail', 'ruby')
    return ml.Roots.of(data, cache)


def snapshot(root: Path) -> dict[str, str]:
    """Every file under `root` and its sha256; the ledger aside."""
    out = {}
    for directory, _, names in os.walk(root):
        for name in names:
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if 'ledger.sqlite3' in relative:
                continue
            out[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def plan_for(roots: ml.Roots, **kwargs) -> ml.Plan:
    resolver = ml.build_resolver(roots.data, roots.data / 'ledger.sqlite3')
    return ml.make_plan(roots, resolver, **kwargs)


@pytest.fixture
def corpus(tmp_path) -> ml.Roots:
    return build(tmp_path)


class TestThePlan:

    def test_every_root_is_planned_with_no_conflict(self, corpus):
        plan = plan_for(corpus)
        summary = plan.summary()
        assert summary['unresolved'] == 0, plan.conflicts
        roots = summary['roots']
        assert roots['05-github-tree']['moves'] == 3
        assert roots['05-github-tree']['dedup'] == 1, 'HEAD and main are one'
        assert roots['06-github-content']['moves'] == 2
        assert roots['06-github-content']['move_files'] == 3
        assert roots['07-sbom']['moves'] == 3
        assert roots['09-github-depgraph']['moves'] == 1
        assert roots['09-github-depgraph']['meta'] == 1
        assert roots['10-generated-lock']['moves'] == 1
        assert roots[ml.SYFT_CACHE]['moves'] == 2, 'one entry, and the unversioned'
        assert roots[ml.SYFT_CACHE]['dedup'] == 1
        assert roots[ml.TREE_CACHE]['moves'] == 2

    def test_destinations_are_keyed_by_repository_and_commit(self, corpus):
        moves = {
            str(op.src.relative_to(corpus.data.parent)):
            str(op.dst.relative_to(corpus.data.parent))
            for op in plan_for(corpus).ops if op.op == ml.MOVE
        }
        assert moves[f'data/07-sbom/go/o/r/v1/{SHA}'] == f'data/07-sbom/11/{SHA}'
        # Named by the ledger, whatever the case on disk.
        assert moves[f'data/07-sbom/ruby/Mikel/Mail/v2/{SHA}'] == (
            f'data/07-sbom/12/{SHA}'
        )
        assert moves['data/09-github-depgraph/go/o/r/sbom.spdx.json'] == (
            'data/09-github-depgraph/11/legacy/sbom.spdx.json'
        )
        assert moves[f'data/10-generated-lock/ruby/Mikel/Mail/{SHA}'] == (
            f'data/10-generated-lock/12/{SHA}'
        )
        assert moves[f'.cache/syft/1.41.2/o/r/v1/{HASH}.json'] == (
            f'.cache/syft/1.41.2/11/{HASH}.json'
        )
        assert moves['.cache/syft/someone'] == '.cache/syft/_unversioned/someone'
        assert moves[
            f'data/07-sbom/typescript/Shopify/polaris-react/@shopify/'
            f'polaris@13.9.5/{SHA2}'
        ] == f'data/07-sbom/13/{SHA2}'
        assert moves[f'.cache/git-tree/mikel/mail/v2/{SHA}'] == (
            f'.cache/git-tree/12/{SHA}'
        )

    def test_the_dry_run_touches_nothing(self, corpus, tmp_path):
        before = snapshot(tmp_path)
        plan_for(corpus)
        assert snapshot(tmp_path) == before

    def test_a_repository_nobody_named_is_a_conflict(self, corpus):
        _write(corpus.data / f'07-sbom/go/ghost/town/v1/{SHA}/sbom.json', '{}')
        plan = plan_for(corpus)
        assert [c.kind for c in plan.conflicts] == ['unmapped']
        assert plan.summary()['unresolved'] == 1

    def test_the_id_more_places_recorded_outranks_a_stray(self, corpus):
        """The real corpus has `psf/requests` twice: 1362490, which every
        stage's list names, and 2, which the ledger and one leaked test
        line in `02-github-repo` name."""
        with Ledger(corpus.data / 'ledger.sqlite3') as ledger:
            ledger.track(2, 'o', 'r', 'python')
        plan = plan_for(corpus)
        assert plan.summary()['unresolved'] == 0
        assert plan.summary()['settled'] == {
            'o/r': '11 (07-sbom,09-github-depgraph), 2 (ledger) -> 11',
        }
        assert any(
            op.dst.name == SHA and op.dst.parent.name == '11'
            for op in plan.ops if op.root == '07-sbom'
        )

    def test_one_name_worn_by_two_repositories_is_a_conflict(self, corpus):
        with Ledger(corpus.data / 'ledger.sqlite3') as ledger:
            ledger.track(99, 'Mikel', 'MAIL', 'ruby')
        plan = plan_for(corpus)
        assert {c.kind for c in plan.conflicts} == {'ambiguous'}

    def test_two_different_copies_of_one_scan_are_a_conflict(self, corpus):
        _write(
            corpus.data /
            f'05-github-tree/go/o/r/HEAD/{SHA2}/tree.txt', 'other\n',
        )
        plan = plan_for(corpus)
        assert [c.kind for c in plan.conflicts] == ['collision']

    def test_resolve_newest_sets_the_older_copy_aside(self, corpus):
        head = corpus.data / f'05-github-tree/go/o/r/HEAD/{SHA2}'
        _write(head / 'tree.txt', 'other\n')
        os.utime(head, (1000, 1000))
        plan = plan_for(corpus, resolve_newest=True)
        assert plan.summary()['unresolved'] == 0
        aside = [op for op in plan.ops if op.op == ml.ASIDE]
        assert [op.src for op in aside] == [head]
        assert '_migration/conflicts' in str(aside[0].dst)

    def test_a_move_across_filesystems_is_refused(self, corpus, monkeypatch):
        real = ml._device

        def device(path):
            return real(path) + (1 if path.name == 'data' else 0)

        monkeypatch.setattr(ml, '_device', device)
        kinds = {c.kind for c in plan_for(corpus).conflicts}
        assert kinds == {'cross-device'}

    def test_the_plan_file_round_trips(self, corpus, tmp_path):
        plan = plan_for(corpus)
        ml.write_plan(plan, tmp_path / 'work' / ml.PLAN)
        ops, summary, unresolved = ml.read_plan(tmp_path / 'work' / ml.PLAN)
        assert [op.line() for op in ops] == [op.line() for op in plan.ops]
        assert summary['ops'] == len(ops)
        assert unresolved == 0


def _apply(corpus, work, **kwargs):
    ops, _, unresolved = ml.read_plan(work / ml.PLAN)
    assert unresolved == 0
    return ml.apply_plan(
        ops, work,
        meta_for=lambda document: ml.legacy_graph_meta(
            document, ml.repository_of(document),
        ),
        stops=ml.stops_for(corpus),
        **kwargs,
    )


@pytest.fixture
def planned(corpus, tmp_path):
    work = tmp_path / 'work'
    ml.inventory(corpus, work / ml.PRE)
    ml.write_plan(plan_for(corpus), work / ml.PLAN)
    return corpus, work


class TestApply:

    def test_the_new_layout(self, planned):
        corpus, work = planned
        result = _apply(corpus, work)
        data = corpus.data
        assert (data / f'06-github-content/11/{SHA}/sub/go.mod').read_text() == (
            'module o/r/sub\n'
        )
        assert (data / f'07-sbom/12/{SHA}/sbom.json').exists()
        assert (data / f'05-github-tree/11/{SHA2}/tree.txt').exists()
        assert (data / '_migration/dedup/05-github-tree').is_dir()
        graph = data / '09-github-depgraph/11/legacy'
        meta = json.loads((graph / 'meta.json').read_text())
        assert meta['fetched_at'] == '2026-09-14T03:56:20Z'
        assert meta['commit_sha'] == '' and meta['legacy'] is True
        assert (corpus.cache / f'syft/1.41.2/11/{HASH}.json').exists()
        assert (
            corpus.cache /
            'syft/_unversioned/someone/thing/main/cd.json'
        ).exists()
        # The old layout is gone, directories and all; the lists stay.
        for root in ('05-github-tree', '06-github-content', '07-sbom'):
            assert sorted(p.name for p in (data / root).iterdir()) == sorted(
                ['11', '12'] + (
                    ['13', 'go.jsonl', 'typescript.jsonl']
                    if root == '07-sbom' else []
                ),
            ), root
        assert not (corpus.cache / 'syft/1.41.2/o').exists()
        assert not (corpus.cache / 'git-tree/mikel').exists()
        assert result.removed_dirs > 0

    def test_verify_passes(self, planned):
        corpus, work = planned
        _apply(corpus, work)
        checks = ml.verify_files(corpus, work)
        assert all(c.ok for c in checks), [c for c in checks if not c.ok]

    def test_verify_notices_a_file_that_changed(self, planned):
        corpus, work = planned
        _apply(corpus, work)
        _write(
            corpus.data /
            f'07-sbom/11/{SHA}/sbom.json', '{"artifacts": [9]}',
        )
        failed = [c.name for c in ml.verify_files(corpus, work) if not c.ok]
        assert failed == ['07-sbom: files and bytes'] or failed

    def test_a_run_killed_midway_resumes_and_finishes(self, planned):
        corpus, work = planned
        _apply(corpus, work, stop_after=3, batch=2)
        done, pending = ml._state(work / ml.JOURNAL)
        assert len(done) == 3 and pending, 'a batch begun, not finished'
        _apply(corpus, work, batch=2)
        checks = ml.verify_files(corpus, work)
        assert all(c.ok for c in checks), [c for c in checks if not c.ok]

    def test_a_rename_whose_done_was_lost_is_recognised(self, planned):
        """Killed between the rename and the line saying so."""
        corpus, work = planned
        ops, _, _ = ml.read_plan(work / ml.PLAN)
        first = next(op for op in ops if op.op == ml.MOVE)
        journal = ml.Journal(work / ml.JOURNAL)
        journal.write('BEGIN', first.op, str(first.src), str(first.dst))
        journal.close()
        first.dst.parent.mkdir(parents=True, exist_ok=True)
        os.rename(first.src, first.dst)
        result = _apply(corpus, work)
        assert result.resumed == 1
        assert all(c.ok for c in ml.verify_files(corpus, work))

    def test_a_stale_plan_is_refused_rather_than_overwriting(self, planned):
        corpus, work = planned
        _write(corpus.data / f'07-sbom/11/{SHA}/sbom.json', 'written since')
        with pytest.raises(FileExistsError):
            _apply(corpus, work)


class TestRollback:

    def test_it_restores_every_byte(self, planned, tmp_path):
        corpus, work = planned
        before = snapshot(tmp_path / 'data') | {
            f'.cache/{k}': v for k, v in snapshot(tmp_path / '.cache').items()
        }
        _apply(corpus, work)
        ml.rollback(work)
        after = snapshot(tmp_path / 'data') | {
            f'.cache/{k}': v for k, v in snapshot(tmp_path / '.cache').items()
        }
        assert after == before
        # And the directories, empty ones too: nothing new is left.
        assert not (tmp_path / 'data/07-sbom/11').exists()
        assert not (tmp_path / 'data/09-github-depgraph/11').exists()

    def test_it_is_safe_to_run_twice(self, planned, tmp_path):
        corpus, work = planned
        before = snapshot(tmp_path / 'data')
        _apply(corpus, work)
        ml.rollback(work)
        ml.rollback(work)
        assert snapshot(tmp_path / 'data') == before

    def test_it_undoes_a_run_that_was_killed(self, planned, tmp_path):
        corpus, work = planned
        before = snapshot(tmp_path / 'data')
        _apply(corpus, work, stop_after=4, batch=3)
        ml.rollback(work)
        assert snapshot(tmp_path / 'data') == before


class TestReaders:
    """What was recorded before the move still finds its file after."""

    def test_a_list_path_is_translated(self, planned):
        from chatsbom.core.documents import FileDocuments
        from chatsbom.core.documents import FileManifests
        corpus, work = planned
        _apply(corpus, work)
        os.chdir(corpus.data.parent)
        document = FileDocuments().get(
            'syft', 11, f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json',
        )
        assert document is not None and document.body == {'artifacts': []}
        graph = FileDocuments().get(
            'github-depgraph', 11, 'data/09-github-depgraph/go/o/r/sbom.spdx.json',
        )
        assert graph is not None
        manifests = FileManifests().for_repository(
            11, f'data/06-github-content/go/o/r/v1/{SHA}',
        )
        assert sorted(path for path, _ in manifests) == [
            'go.mod', 'sub/go.mod',
        ]

    def test_the_depgraph_store_finds_a_moved_legacy_graph(self, planned):
        from chatsbom.core.depgraph_store import current_documents
        corpus, work = planned
        _apply(corpus, work)
        assert list(current_documents(corpus.data / '09-github-depgraph')) == [
            corpus.data / '09-github-depgraph/11/legacy/sbom.spdx.json',
        ]


class TestTheCommand:
    """The runbook's commands, in order, over the synthetic corpus."""

    @pytest.fixture
    def cli(self, corpus, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(Container, '_instance', None)

        def run(*args, code=0):
            result = runner.invoke(
                app, [
                    'data', 'migrate-layout', '--no-db',
                    '--workdir', str(tmp_path / 'work'), *args,
                ],
            )
            assert result.exit_code == code, result.output
            return result
        return run

    def test_inventory_dry_run_apply_verify_rollback(self, cli, tmp_path):
        before = snapshot(tmp_path / 'data')
        cli('--inventory')
        assert snapshot(tmp_path / 'data') == before
        cli()
        assert snapshot(
            tmp_path / 'data',
        ) == before, 'the dry run wrote nothing'
        assert not (tmp_path / 'data/_migration').exists()
        assert (tmp_path / 'work' / ml.PLAN).exists()
        cli('--apply')
        with Ledger(tmp_path / 'data/ledger.sqlite3') as ledger:
            assert ledger.count() == 1
        assert (tmp_path / 'work' / ml.LEDGER_BACKUP).exists()
        result = cli('--verify')
        assert 'FAIL' not in result.output
        cli('--rollback')
        assert snapshot(tmp_path / 'data') == before

    @pytest.fixture
    def json_logs(self, monkeypatch):
        """JSON logs for one test, and the console format after it."""
        monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
        yield
        monkeypatch.delenv('CHATSBOM_LOG_FORMAT')
        setup_logging('INFO')

    def test_the_dry_run_logs_how_far_it_got_and_reports_on_stdout(
        self, cli, json_logs, tmp_path,
    ):
        """How far the walk has got goes to the logger: on stderr, and as
        JSON when a machine reads it. The report is stdout's, and a path
        in it is printed as it is, `[bold]` and all (#25)."""
        work = tmp_path / '[bold]' / 'work'

        result = cli('--workdir', str(work))

        events = [
            json.loads(line)['event'] for line in result.stderr.splitlines()
        ]
        assert 'Walking the old layout' in events
        assert f'Plan:{work / ml.PLAN}' in ''.join(result.stdout.split())

    def test_a_database_error_is_kept_and_shown_as_it_is(
        self, corpus, tmp_path, monkeypatch,
    ):
        """In dry-run.json and in the report: without the query of a URL
        it quotes, and without markup read into it (#25)."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(Container, '_instance', None)

        def refuse(self):
            raise ConnectionError(
                'HTTPDriver for https://clickhouse.example/?password=hunter2 '
                'failed [/dim]',
            )

        monkeypatch.setattr(Container, 'get_query_repository', refuse)
        work = tmp_path / 'work'

        result = runner.invoke(
            app, ['data', 'migrate-layout', '--workdir', str(work)],
        )

        assert result.exit_code == 0, result.output
        said = 'https://clickhouse.example/?***** failed [/dim]'
        kept = json.loads((work / 'dry-run.json').read_text())
        assert kept['raw_documents']['error'] == f'HTTPDriver for {said}'
        assert said in ' '.join(result.stdout.split())
        assert 'hunter2' not in result.output

    def test_with_the_lists_archived_too(self, cli, tmp_path):
        before = snapshot(tmp_path / 'data')
        cli('--inventory')
        cli('--archive-lists')
        cli('--apply')
        assert (tmp_path / 'data/07-sbom/_legacy-lists/go.jsonl').exists()
        assert 'FAIL' not in cli('--verify').output
        cli('--rollback')
        assert snapshot(tmp_path / 'data') == before

    def test_apply_needs_a_plan_and_an_inventory(self, cli):
        cli('--apply', code=1)
        cli()
        cli('--apply', code=1)

    def test_a_plan_with_a_conflict_is_not_applied(self, cli, tmp_path):
        _write(
            tmp_path / f'data/07-sbom/go/ghost/town/v1/{SHA}/sbom.json', '{}',
        )
        cli('--inventory')
        cli(code=1)
        cli('--apply', code=1)
        assert (tmp_path / f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json').exists()

    def test_the_ledger_is_restored_by_the_rollback(self, cli, tmp_path):
        path = tmp_path / 'data/ledger.sqlite3'
        with Ledger(path) as ledger:
            ledger.record_push(
                12, datetime(2026, 9, 1, tzinfo=timezone.utc),
                datetime(2026, 9, 1, tzinfo=timezone.utc),
            )
            ledger.record_success(
                12, Stage.SBOM, datetime(
                    2026, 9, 2, tzinfo=timezone.utc,
                ),
            )
        cli('--inventory')
        cli()
        cli('--apply')
        with Ledger(path) as ledger:
            ledger.track(77, 'new', 'one', 'go')
        cli('--rollback')
        with sqlite3.connect(path) as db:
            ids = {
                row[0] for row in db.execute(
                    'SELECT repository_id FROM repository_state',
                )
            }
        assert ids == {12}


@requires_clickhouse
class TestTheLandingZone:
    """The `raw_documents` rewrite, its rollback, and what readers see."""

    ROWS = [
        ('syft', 11, f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json', '1' * 64),
        (
            'content', 11,
            f'data/06-github-content/go/o/r/v1/{SHA}/go.mod', '2' * 64,
        ),
        (
            'content', 11,
            f'data/06-github-content/go/o/r/v1/{SHA}/sub/go.mod', '3' * 64,
        ),
        ('github-depgraph', 11, 'data/09-github-depgraph/go/o/r/sbom.spdx.json', '4' * 64),
        ('repo', 11, 'data/07-sbom/go.jsonl', '5' * 64),
        ('repo-metadata', 11, 'data/02-github-repo/go.jsonl', '6' * 64),
    ]

    @pytest.fixture
    def landed(self, ingest):
        ingest.client.insert(
            'raw_documents',
            [
                [
                    kind, rid, path, digest,
                    datetime(2026, 2, 11, tzinfo=timezone.utc),
                    'module o/r\n' if kind == 'content' else '{"language": "Go"}',
                ]
                for kind, rid, path, digest in self.ROWS
            ],
            column_names=[
                'kind', 'repository_id',
                'path', 'sha256', 'fetched_at', 'body',
            ],
        )
        return ingest

    def _paths(self, ingest):
        return {
            (k, s): p for k, s, p in ingest.client.query(
                'SELECT kind, sha256, path FROM raw_documents FINAL',
            ).result_rows
        }

    def test_rewrite_verify_restore(self, landed, planned):
        corpus, work = planned
        before_counts = ml.raw_counts(landed.client)
        assert before_counts['syft'] == {'rows': 1, 'rewrite': 1}
        assert before_counts['repo']['rewrite'] == 0, 'a ledger label stays'
        before = self._paths(landed)

        _apply(corpus, work)
        changed = ml.rewrite_raw(landed.client)
        assert changed == {'syft': 1, 'content': 2, 'github-depgraph': 1}
        after = self._paths(landed)
        assert after[('syft', '1' * 64)] == f'07-sbom/11/{SHA}/sbom.json'
        assert after[
            ('content', '3' * 64)
        ] == f'06-github-content/11/{SHA}/sub/go.mod'
        assert after[('github-depgraph', '4' * 64)] == (
            '09-github-depgraph/11/legacy/sbom.spdx.json'
        )
        assert after[('repo', '5' * 64)] == 'data/07-sbom/go.jsonl'
        shas = dict(
            landed.client.query(
                "SELECT sha256, commit_sha FROM raw_documents FINAL WHERE kind = 'content'",
            ).result_rows,
        )
        assert set(shas.values()) == {SHA}
        assert ml.rewrite_raw(landed.client) == {
            'syft': 0, 'content': 0, 'github-depgraph': 0,
        }, 'idempotent'
        checks = ml.verify_raw(landed.client, corpus.data, before_counts)
        assert all(c.ok for c in checks), [c for c in checks if not c.ok]

        restored = ml.restore_raw(landed.client, landed.config.database)
        assert restored == 4
        assert self._paths(landed) == before

    def test_a_scratch_database_whose_name_is_not_a_bare_identifier(
        self, landed,
    ):
        """`--prepare-scratch` wrote both names into its copy as they
        were given, so a hyphen in either read as a minus sign (#120)."""
        from types import SimpleNamespace

        from chatsbom.commands.data.migrate_layout import _prepare_scratch
        from chatsbom.core.config import ChatSBOMConfig
        from chatsbom.core.config import DatabaseConfig

        production = landed.config
        config = ChatSBOMConfig(
            _db_base=DatabaseConfig(
                host=production.host, port=production.port,
                database=production.database,
            ),
        )
        scratch = f'{production.database}-scratch'
        try:
            _prepare_scratch(SimpleNamespace(config=config), scratch)
            copied = landed.client.query(
                f'SELECT count() FROM `{scratch}`.raw_documents',
            ).result_rows[0][0]
        finally:
            landed.client.command(f'DROP DATABASE IF EXISTS `{scratch}`')
        assert copied == len(self.ROWS)

    def test_readers_see_the_same_manifests_and_scan(self, landed, planned):
        from chatsbom.core.documents import RawDocuments
        from chatsbom.core.documents import RawManifests
        corpus, work = planned

        def read():
            manifests = sorted(
                RawManifests(landed.client, 'data/06-github-content')
                .for_repository(11, commit_sha=SHA),
            )
            sbom = RawDocuments(landed.client).get('syft', 11, commit_sha=SHA)
            return manifests, sbom.body if sbom else None

        before = read()
        assert [p for p, _ in before[0]] == ['go.mod', 'sub/go.mod']
        _apply(corpus, work)
        ml.rewrite_raw(landed.client)
        assert read() == before


class TestArchivingTheLists:
    """Design §7.1's last rows, opt-in until nothing reads the lists."""

    def test_off_by_default(self, corpus):
        assert not [op for op in plan_for(corpus).ops if op.note == 'list']

    def test_lists_go_to_legacy_lists_and_the_snapshot_gets_its_date(self, corpus):
        search = corpus.data / '01-github-search'
        _jsonl(search / 'all.jsonl', {'id': 11, 'owner': 'o', 'repo': 'r'})
        _jsonl(search / 'go.jsonl', {'id': 11, 'owner': 'o', 'repo': 'r'})
        march = datetime(2026, 3, 9, 12, tzinfo=timezone.utc).timestamp()
        os.utime(search / 'all.jsonl', (march, march))
        moves = {
            str(op.src.relative_to(corpus.data)): str(op.dst.relative_to(corpus.data))
            for op in plan_for(corpus, archive_lists=True).ops if op.note == 'list'
        }
        assert moves == {
            '01-github-search/all.jsonl': '01-github-search/all-2026-03-09.jsonl',
            '01-github-search/go.jsonl': '01-github-search/_legacy-lists/go.jsonl',
            '07-sbom/go.jsonl': '07-sbom/_legacy-lists/go.jsonl',
            '07-sbom/typescript.jsonl': '07-sbom/_legacy-lists/typescript.jsonl',
            '09-github-depgraph/go.jsonl': '09-github-depgraph/_legacy-lists/go.jsonl',
        }


def test_a_meta_written_but_not_yet_done_is_rewritten_or_rolled_back(planned, tmp_path):
    """Killed after writing a legacy graph's `meta.json` but before its
    batch was synced and marked done: the next run writes it again if
    it is not whole, and a rollback removes it either way."""
    corpus, work = planned
    before = snapshot(tmp_path / 'data')
    ops, _, _ = ml.read_plan(work / ml.PLAN)
    meta = next(op for op in ops if op.op == ml.META)
    at = next(i for i, op in enumerate(ops) if op.op == ml.META)
    _apply(corpus, work, stop_after=at + 1, batch=1000)
    assert meta.dst.exists()
    done, pending = ml._state(work / ml.JOURNAL)
    assert (ml.META, str(meta.src), str(meta.dst)) not in done
    # As a power cut can leave it: the name, and nothing in it.
    meta.dst.write_text('')
    _apply(corpus, work)
    assert json.loads(meta.dst.read_text())['legacy'] is True
    ml.rollback(work)
    assert snapshot(tmp_path / 'data') == before
