"""What is due, derived from the store, and how it differs from the ledger.

Step 1 of `docs/design/first-principles.md` (#100) schedules the way a
build system does: a stage is done for a repository exactly when its
output for the current input is in the store,

    due = { (repository, stage) : repository in the snapshot,
                                  output(stage, input key) not in store }

so that no watermark has to be kept in step with the files: the files
are the watermark. This module computes that set, read-only, beside the
ledger that schedules the collection today, so the two can be compared
before anything is scheduled from it (`chatsbom queue due`).

## The walk

A repository's stages form a chain, each consuming what the one before
produced (`ledger.UPSTREAM`): the push, the release chosen for it, the
commit that release is at, the tree of that commit, its manifests, and
the SBOM of those. The walk follows it and stops at the first stage whose
output is not in the store; that one is *due*, or *blocked* while its
own backoff after a failure runs, and every stage after it is *waiting*,
since its input does not exist yet. The ledger counts a waiting stage
as due (its row is missing, or consumed something else); the walk
would find nothing to run there, so it is counted apart. A repository
`queue sync` has deferred (a 404, its backoff) is *deferred* throughout.

Release and commit keep their decisions in the store since #147
(`core/decisions.py`), but only from the push each repository is next
collected at: what they last produced is read from the ledger, which
has it for every repository, and every verdict that rests on it says so
(`ledger_backed`). The rest is read from the store:

- **tree**: `05-github-tree/<id>/<sha>/tree.txt` written to the end
  (`fs.is_whole_tree`), or empty where the ledger recorded the stage: a
  commit with no files at all;
- **content**: `manifests.json` beside the tree, for that commit, under
  the discovery limits in force, with every selected file settled
  (`collector/content.settled`: an error, a 5xx or a 429 may yet pass),
  written by the stage version in force. The collector stamps the
  version in the document (`VERSION_FIELD`, #161), so a version stamped
  there is read first, then the ledger's row for that commit at that
  version, and with `--rediscover` the tree is discovered again: a
  selection the version in force would make the same way is as good as
  its own;
- **SBOM**: `sbom_service.staleness` (#110) says it is current: whole,
  written by the Syft in force, and newer than every file it was made
  from;
- **dependency graph**: due on the ledger's clock, which
  `depgraph_stage.next_state` sets (a refresh after 30 days, the
  negative cache after a 404, the backoff after a failure), and present
  when `09-github-depgraph/<id>/` keeps a whole graph for it.

Each verdict carries a `why` in a word, from the evidence.

## Reading lazily

Up to about six stats or opens per repository (the tree's last byte,
`manifests.json`, the content root, the SBOM's two ends, one listing of
the graphs) plus the walk of the content root that `staleness` makes
for an SBOM: a stage waiting on another is never read, and nothing is
read for a stage not asked for (`--stage`).

## How it differs from the ledger

Where the two disagree on whether a stage is due, `derive` says why,
with a code (`REASONS`) chosen from the evidence on either side; and
looks a second time at every repository they disagree on, once the rest
is done, to count what converged meanwhile (`timing`): the collector
writes to both while this reads.
"""
from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from collections import Counter
from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import Enum
from pathlib import Path
from typing import Any

from chatsbom.collector.content import LIMITS
from chatsbom.collector.content import settled_document
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.core import depgraph_store
from chatsbom.core.catalog import Catalog
from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import MAX_FILES
from chatsbom.core.discovery import read_tree
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.layout import is_sha
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import STAGE_VERSION
from chatsbom.core.ledger import StageState
from chatsbom.core.ledger import UNFILTERED_SNAPSHOT_PREFIX
from chatsbom.services.depgraph_stage import DEPGRAPH_REFRESH
from chatsbom.services.run_service import STAGES
from chatsbom.services.sbom_service import Stale
from chatsbom.services.sbom_service import staleness

#: The chain the walk runs (`run_service.STAGES`), and the dependency
#: graph, scheduled on its own: every stage this compares.
COMPARED: tuple[Stage, ...] = (*STAGES, Stage.DEPGRAPH)


class State(str, Enum):
    """Where one stage of one repository stands."""

    #: Its output for the current input is in the store (or, for release
    #: and commit, recorded in the ledger).
    PRESENT = 'present'
    #: Its input is there and its output is not: work to do now.
    DUE = 'due'
    #: Its input is not produced yet: a stage before it is not present.
    WAITING = 'waiting'
    #: Due, but its own backoff after a failure still runs.
    BLOCKED = 'blocked'
    #: The repository is held: a 404, `queue sync`'s backoff, or for the
    #: dependency graph its negative cache.
    DEFERRED = 'deferred'

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class Verdict:
    """One stage of one repository, and why it stands there."""

    state: State
    #: In a word, from the evidence: for a waiting stage, the stage it
    #: waits on.
    why: str
    #: What the stage produced, where it is known: the release's tag,
    #: the commit, the content's digest.
    output: str = ''
    #: The key its output was looked for under: the commit, for the
    #: stages the store keeps by commit.
    input: str = ''
    #: Read from the ledger, not the store.
    ledger_backed: bool = False


@dataclass(frozen=True, slots=True)
class Row:
    """What the ledger holds of one repository, as the walk reads it."""

    repository_id: int
    #: The search snapshot that last listed it; '' when none does.
    snapshot: str
    #: `pushed_at_seen` as stored: release consumed it, and is compared
    #: with it as text, as the ledger compares it.
    pushed_at: str
    next_attempt_at: datetime | None
    absent_since: datetime | None
    #: A graph fetched before `stage_state`, by its watermark.
    depgraph_watermark: datetime | None


@dataclass
class LedgerView:
    """The ledger's rows, read at one moment."""

    rows: dict[int, Row]
    stages: dict[int, dict[Stage, StageState]]
    #: What the ledger has due, by the statements its workers claim by
    #: (`_due_ids`, `depgraph_due_ids`); None when not asked for.
    due: dict[Stage, frozenset[int]] | None
    #: Whether there was a ledger to read at all.
    exists: bool = True


def _instant(value: object) -> datetime | None:
    """A time the ledger stored, aware, as the ledger parses it."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _footprint(path: Path) -> tuple[bool, int, int]:
    """Whether a `-wal` is beside the ledger, and its size and mtime."""
    try:
        status = path.stat()
    except OSError:
        return False, -1, -1
    return (
        Path(f'{path}-wal').exists(), status.st_size, status.st_mtime_ns,
    )


def read_ledger(
    path: Path,
    now: datetime,
    *,
    repos: Collection[int] | None = None,
    shard: tuple[int, int] | None = None,
    compare: bool = True,
    refresh: timedelta = DEPGRAPH_REFRESH,
) -> LedgerView:
    """The ledger's rows (only `repos`, or one shard, where given), and
    with `compare` its due sets, in one read.

    Opened read-only (`Ledger.open_readonly`). A ledger in use is read in
    one transaction, so the rows and the due sets are of one moment. One
    nothing had open is read immutable, without locks; if a writer opened
    it meanwhile the read may have seen half a write, and it is read
    again, with the writer's WAL this time. No ledger at all reads as an
    empty one.
    """
    path = Path(path)
    if not path.is_file():
        return LedgerView(
            {}, {},
            {stage: frozenset() for stage in COMPARED} if compare else None,
            exists=False,
        )
    for _ in range(3):
        before = _footprint(path)
        with Ledger.open_readonly(path) as ledger:
            try:
                view = _read(ledger, now, repos, shard, compare, refresh)
            except sqlite3.OperationalError:
                raise
            except sqlite3.DatabaseError:
                # A page a writer was writing, read without a lock: only
                # an immutable read, and only if the ledger changed.
                if before[0] or _footprint(path) == before:
                    raise
                continue
        if before[0] or _footprint(path) == before:
            return view
    raise RuntimeError(f'{path} kept changing while it was read')


def _scope(
    repos: Collection[int] | None, shard: tuple[int, int] | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if repos is not None:
        clauses.append(
            'repository_id IN (SELECT value FROM json_each(:repos))',
        )
        params['repos'] = json.dumps(sorted({int(i) for i in repos}))
    if shard is not None:
        clauses.append('repository_id % :count = :index')
        params['index'], params['count'] = shard
    return (f"WHERE {' AND '.join(clauses)}" if clauses else ''), params


def _read(
    ledger: Ledger,
    now: datetime,
    repos: Collection[int] | None,
    shard: tuple[int, int] | None,
    compare: bool,
    refresh: timedelta,
) -> LedgerView:
    # The ledger's own connection: it has no bulk reader of its rows,
    # and this is the one reader that needs them all at once.
    db = ledger._db
    where, params = _scope(repos, shard)
    db.execute('BEGIN')
    try:
        columns = {
            row['name'] for row in db.execute(
                'PRAGMA table_info(repository_state)',
            )
        }
        snapshot = 'snapshot' if 'snapshot' in columns else "''"
        absent = 'absent_since' if 'absent_since' in columns else 'NULL'
        rows: dict[int, Row] = {}
        for row in db.execute(
            f"""
            SELECT repository_id, {snapshot} AS snapshot,
                   coalesce(pushed_at_seen, '') AS pushed_at,
                   next_attempt_at, {absent} AS absent_since,
                   CASE WHEN json_valid(stage_watermarks)
                        THEN json_extract(stage_watermarks, '$.depgraph')
                   END AS depgraph_watermark
            FROM repository_state {where}
            """,
            params,
        ):
            repository_id = int(row['repository_id'])
            rows[repository_id] = Row(
                repository_id=repository_id,
                snapshot=row['snapshot'] or '',
                pushed_at=row['pushed_at'],
                next_attempt_at=_instant(row['next_attempt_at']),
                absent_since=_instant(row['absent_since']),
                depgraph_watermark=_instant(row['depgraph_watermark']),
            )
        stages: dict[int, dict[Stage, StageState]] = {}
        names = ', '.join(f"'{stage}'" for stage in COMPARED)
        stage_where = (
            f'{where} AND stage IN ({names})' if where
            else f'WHERE stage IN ({names})'
        )
        for row in db.execute(
            f'SELECT * FROM stage_state {stage_where}', params,
        ):
            state = StageState(
                repository_id=int(row['repository_id']),
                stage=Stage(row['stage']),
                done_at=_instant(row['done_at']),
                stage_version=int(row['stage_version']),
                input_key=row['input_key'],
                output_key=row['output_key'],
                outcome=row['outcome'],
                http_status=row['http_status'],
                failure_count=int(row['failure_count']),
                next_attempt_at=_instant(row['next_attempt_at']),
                last_error=row['last_error'],
                claimed_by=row['claimed_by'],
                claim_expires_at=_instant(row['claim_expires_at']),
            )
            stages.setdefault(state.repository_id, {})[state.stage] = state
        due: dict[Stage, frozenset[int]] | None = None
        if compare:
            # A shard's due sets are the ledger's own statements over the
            # shard's rows: each scans a sixteenth of the ledger, not all
            # of it, for `--shard 0/16`.
            narrowed = set(rows) if repos is None and shard else repos
            found = {
                stage: ledger._due_ids(stage, now, repos=narrowed)
                for stage in STAGES
            }
            found[Stage.DEPGRAPH] = ledger.depgraph_due_ids(
                now, refresh=refresh, repos=narrowed,
            )
            due = {
                stage: frozenset(
                    i for i in ids
                    if shard is None or i % shard[1] == shard[0]
                )
                for stage, ids in found.items()
            }
    finally:
        db.execute('COMMIT')
    return LedgerView(rows, stages, due)


# -- the store ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Graph:
    """A dependency graph the store keeps for a repository."""

    #: When it was fetched; None for the one kept from before every
    #: fetch was, whose time only its document says.
    fetched_at: datetime | None


class Store:
    """The stage outputs on disk, read one repository at a time.

    Each method reads as little as answers it: the last byte of a tree,
    one document, one listing.
    """

    def __init__(self, paths: PathConfig) -> None:
        self.paths = paths

    def tree(self, repository_id: int, sha: str) -> str:
        """`whole`, `empty`, `cut-short` or `missing`."""
        path = self.paths.tree_file(repository_id, sha)
        if is_whole_tree(path):
            return 'whole'
        try:
            status = path.stat()
        except OSError:
            return 'missing'
        if not stat.S_ISREG(status.st_mode):
            return 'missing'
        return 'empty' if status.st_size == 0 else 'cut-short'

    def tree_paths(self, repository_id: int, sha: str) -> list[str] | None:
        """The tree's paths, to discover the manifests from again."""
        try:
            text = self.paths.tree_file(repository_id, sha).read_text(
                encoding='utf-8', errors='replace',
            )
        except OSError:
            return None
        return read_tree(text)

    def discovery(self, repository_id: int, sha: str) -> dict[str, Any] | str:
        """`manifests.json`, or why there is none: `missing` or
        `unreadable`."""
        try:
            raw = self.paths.discovery_file(repository_id, sha).read_bytes()
        except FileNotFoundError:
            return 'missing'
        except OSError:
            return 'unreadable'
        try:
            document = json.loads(raw)
        except ValueError:
            return 'unreadable'
        return document if isinstance(document, dict) else 'unreadable'

    def root(self, repository_id: int, sha: str) -> bool:
        """Whether the content root is there: the content stage makes it
        even for a commit with no manifests."""
        return self.paths.content_root(repository_id, sha).is_dir()

    def sbom(
        self, repository_id: int, sha: str, syft_version: str | None,
    ) -> str | None:
        """None while the SBOM is current (`staleness`), else why not:
        `missing`, or what `Stale` says."""
        output = self.paths.sbom_file(repository_id, sha)
        stale = staleness(
            output,
            self.paths.content_root(repository_id, sha),
            self.paths.generated_lock_path(repository_id, sha),
            syft_version=syft_version,
        )
        if stale is None:
            return None
        if stale is Stale.UNUSABLE and not output.exists():
            return 'missing'
        return str(stale.value)

    def depgraph(self, repository_id: int) -> Graph | None:
        """The newest whole graph kept for a repository, else the one
        kept from before every fetch was; None when there is neither.

        One listing, and one open per fetch until a whole one is found:
        the newest, nearly always.
        """
        directory = depgraph_store.repository_dir(
            self.paths.depgraph_dir, repository_id,
        )
        try:
            names = os.listdir(directory)
        except OSError:
            return None
        fetches = []
        for name in names:
            stamp = depgraph_store.stamp_of(name)
            if stamp is not None:
                fetches.append((stamp[0], name))
        for fetched_at, name in sorted(fetches, reverse=True):
            if looks_like_whole_json_object(
                directory / name / depgraph_store.DOCUMENT,
            ):
                return Graph(fetched_at)
        legacy = directory / depgraph_store.LEGACY / depgraph_store.DOCUMENT
        return Graph(None) if looks_like_whole_json_object(legacy) else None


# -- the walk ----------------------------------------------------------------


@dataclass(frozen=True)
class Settings:
    """What the verdicts are judged against."""

    now: datetime
    #: The Syft an SBOM must record; None judges by times alone, as
    #: `sbom_service` does when it cannot tell.
    syft_version: str | None
    #: Discover the tree again for a content root no version vouches
    #: for: slow, since it reads every such tree whole.
    rediscover: bool = False
    #: How long a dependency graph stands (`depgraph_stage`).
    refresh: timedelta = DEPGRAPH_REFRESH


def walk(
    repository_id: int,
    view: LedgerView,
    store: Store,
    settings: Settings,
    *,
    listed_push: datetime | None = None,
    stages: Iterable[Stage] = COMPARED,
) -> dict[Stage, Verdict]:
    """Where each of `stages` stands for one repository.

    `listed_push` is the push the snapshot saw. The chain is followed as
    far as the last stage asked for and no further, and only until a
    stage is not present.
    """
    wanted = set(stages)
    row = view.rows.get(repository_id)
    records = view.stages.get(repository_id, {})
    verdicts: dict[Stage, Verdict] = {}
    chain = [stage for stage in STAGES if stage in wanted]
    if chain:
        verdicts.update(
            _chain(
                repository_id, row, records, store, settings, listed_push,
                STAGES[:STAGES.index(chain[-1]) + 1],
            ),
        )
    if Stage.DEPGRAPH in wanted:
        verdicts[Stage.DEPGRAPH] = _graph(
            repository_id, row, records.get(Stage.DEPGRAPH), store, settings,
        )
    return {stage: verdicts[stage] for stage in COMPARED if stage in wanted}


def _chain(
    repository_id: int,
    row: Row | None,
    records: Mapping[Stage, StageState],
    store: Store,
    settings: Settings,
    listed_push: datetime | None,
    chain: Iterable[Stage],
) -> dict[Stage, Verdict]:
    now = settings.now
    if row is not None and row.next_attempt_at is not None and (
        row.next_attempt_at > now
    ):
        held = 'absent' if row.absent_since is not None else 'backoff'
        return {stage: Verdict(State.DEFERRED, held) for stage in chain}
    verdicts: dict[Stage, Verdict] = {}
    holder: Stage | None = None
    tag = sha = ''
    for stage in chain:
        if holder is not None:
            verdicts[stage] = Verdict(State.WAITING, str(holder))
            continue
        record = records.get(stage)
        if stage is Stage.RELEASE:
            verdict = _release(row, record, listed_push, now)
            tag = verdict.output
        elif stage is Stage.COMMIT:
            verdict = _commit(record, tag, now)
            sha = verdict.output
        elif stage is Stage.TREE:
            verdict = _tree(repository_id, sha, record, store, now)
        elif stage is Stage.CONTENT:
            verdict = _content(repository_id, sha, record, store, settings)
        else:
            verdict = _sbom(repository_id, sha, record, store, settings)
        verdicts[stage] = verdict
        if verdict.state is not State.PRESENT:
            holder = stage
    return verdicts


def _stale(
    record: StageState | None, stage: Stage, consumed: str,
) -> str | None:
    """Why the ledger's row for `stage` is not current against what its
    upstream produced, `consumed`, as `Ledger._due_ids` judges it; None
    when it is current."""
    if record is None:
        return 'never-run'
    if record.stage_version < STAGE_VERSION[stage]:
        return 'stage-version'
    if record.outcome == 'failed':
        return 'retry'
    if record.input_key != consumed:
        return 'push' if stage is Stage.RELEASE else 'upstream-moved'
    return None


def _not_present(
    record: StageState | None, why: str, now: datetime, key: str = '',
) -> Verdict:
    """Due, unless the stage's own backoff after a failure still runs:
    the ledger leaves a row alone until its `next_attempt_at`."""
    if record is not None and record.next_attempt_at is not None and (
        record.next_attempt_at > now
    ):
        return Verdict(State.BLOCKED, 'backoff', input=key)
    return Verdict(State.DUE, why, input=key)


def _release(
    row: Row | None,
    record: StageState | None,
    listed_push: datetime | None,
    now: datetime,
) -> Verdict:
    """The release chosen for the last push, as the ledger records it.

    A push the snapshot saw after the one the ledger has (or, with none,
    after the release was chosen) makes it due: the head moved, and
    `queue sync` has not looked since.
    """
    pushed = row.pushed_at if row is not None else ''
    why = _stale(record, Stage.RELEASE, pushed)
    if why is None and record is not None:
        known = _instant(pushed) or record.done_at
        if listed_push is None or known is None or listed_push <= known:
            return Verdict(
                State.PRESENT, 'current', record.output_key, pushed,
                ledger_backed=True,
            )
        why = 'head-moved'
    return _not_present(record, why or 'never-run', now, pushed)


def _commit(record: StageState | None, tag: str, now: datetime) -> Verdict:
    """The commit the chosen release is at, as the ledger records it. A
    row adopted from a watermark says the stage ran and not what it
    produced; with no commit no store key can be read, so it is due."""
    why = _stale(record, Stage.COMMIT, tag)
    if why is None and record is not None:
        if is_sha(record.output_key):
            return Verdict(
                State.PRESENT, 'current', record.output_key, tag,
                ledger_backed=True,
            )
        why = 'output-unknown'
    return _not_present(record, why or 'never-run', now, tag)


def _tree(
    repository_id: int,
    sha: str,
    record: StageState | None,
    store: Store,
    now: datetime,
) -> Verdict:
    shape = store.tree(repository_id, sha)
    if shape == 'whole':
        return Verdict(State.PRESENT, 'whole', sha, sha)
    if shape == 'empty' and _stale(record, Stage.TREE, sha) is None:
        return Verdict(
            State.PRESENT, 'empty-recorded', sha, sha, ledger_backed=True,
        )
    return _not_present(record, shape, now, sha)


def _content(
    repository_id: int,
    sha: str,
    record: StageState | None,
    store: Store,
    settings: Settings,
) -> Verdict:
    document = store.discovery(repository_id, sha)
    if isinstance(document, str):
        return _not_present(record, document, settings.now, sha)
    if document.get('commit_sha') != sha:
        why = 'wrong-commit'
    elif document.get('limits') != LIMITS:
        why = 'limits-changed'
    elif not settled_document(document):
        why = 'unsettled'
    else:
        why, current, backed = _content_version(
            repository_id, sha, document, record, store, settings,
        )
        if current:
            if not store.root(repository_id, sha):
                return _not_present(record, 'root-missing', settings.now, sha)
            return Verdict(
                State.PRESENT, why, str(document.get('digest') or ''), sha,
                ledger_backed=backed,
            )
    return _not_present(record, why, settings.now, sha)


def _content_version(
    repository_id: int,
    sha: str,
    document: Mapping[str, Any],
    record: StageState | None,
    store: Store,
    settings: Settings,
) -> tuple[str, bool, bool]:
    """`(why, current, ledger_backed)`: whether the stage version in force
    wrote the document, or would have selected the same files."""
    version = STAGE_VERSION[Stage.CONTENT]
    stamped = document.get(VERSION_FIELD)
    if isinstance(stamped, int) and not isinstance(stamped, bool):
        if stamped >= version:
            return 'stamped', True, False
        return 'content-version', False, False
    if (
        record is not None and record.stage_version >= version
        and record.input_key == sha
    ):
        return 'ledger-stamp', True, True
    if not settings.rediscover:
        return 'content-version', False, False
    lines = store.tree_paths(repository_id, sha)
    listed = [
        entry.get('path') for entry in document.get('selected') or ()
        if isinstance(entry, dict)
    ]
    if lines is not None and discover(lines, max_files=MAX_FILES).paths == (
        listed
    ):
        return 'rediscovered', True, False
    return 'selection-changed', False, False


def _sbom(
    repository_id: int,
    sha: str,
    record: StageState | None,
    store: Store,
    settings: Settings,
) -> Verdict:
    why = store.sbom(repository_id, sha, settings.syft_version)
    if why is None:
        return Verdict(State.PRESENT, 'current', sha, sha)
    return _not_present(record, why, settings.now, sha)


def _graph(
    repository_id: int,
    row: Row | None,
    record: StageState | None,
    store: Store,
    settings: Settings,
) -> Verdict:
    """The dependency graph, on the ledger's clock, and the store's
    graphs: `claim_stage`'s rule, with what is kept on disk beside it."""
    now = settings.now
    if row is not None and row.absent_since is not None:
        return Verdict(State.DEFERRED, 'absent')
    outcome = record.outcome if record is not None else ''
    if record is not None and record.next_attempt_at is not None and (
        record.next_attempt_at > now
    ):
        if outcome == 'ok':
            graph = store.depgraph(repository_id)
            if graph is None:
                return Verdict(State.DUE, 'missing')
            return Verdict(
                State.PRESENT, 'fetched' if graph.fetched_at else 'legacy',
            )
        if outcome == 'absent':
            return Verdict(State.DEFERRED, 'no-graph')
        if outcome == 'pending':
            return Verdict(State.DEFERRED, 'pending')
        return Verdict(State.BLOCKED, 'backoff')
    watermark = row.depgraph_watermark if row is not None else None
    if outcome == '':
        if watermark is not None and watermark > now - settings.refresh:
            return Verdict(State.PRESENT, 'watermark', ledger_backed=True)
        why = 'refresh' if watermark is not None else 'never-asked'
    else:
        why = {'ok': 'refresh', 'absent': 'expired'}.get(outcome, 'retry')
    graph = store.depgraph(repository_id)
    if graph is not None and graph.fetched_at is not None and (
        graph.fetched_at > now - settings.refresh
    ):
        return Verdict(State.PRESENT, 'fetched')
    return Verdict(State.DUE, why)


# -- the comparison ----------------------------------------------------------

#: Why a derived verdict is due, when the evidence is a file missing,
#: cut short or unreadable where the ledger counts on one.
_FILE_MISSING = frozenset({
    'missing', 'cut-short', 'empty', 'unreadable', 'wrong-commit',
    'root-missing', 'unusable',
})

#: Why a derived verdict is due, when only the store (or the snapshot)
#: can see it: each is its own code.
_SEEN_IN_THE_STORE = frozenset({
    'head-moved', 'output-unknown', 'limits-changed', 'unsettled',
    'content-version', 'selection-changed', 'another-syft', 'input-changed',
})

DERIVED_ONLY = 'derived-only'
LEDGER_ONLY = 'ledger-only'

_SNAPSHOT_CAUSES = {
    'absent': 'GitHub answered 404 for it',
    'unlisted': 'no snapshot lists it: `queue track` unlisted it, or it '
    'was tracked from a language list',
    'older-snapshot': 'only an older snapshot listed it, and `queue track` '
    'has not read the newest',
    'newer-snapshot': 'a newer snapshot lists it, not complete yet',
    'snapshot-differs': 'the ledger has this snapshot listing it, and the '
    'file does not now',
    'other-snapshot': 'a snapshot that is not an unfiltered one listed it',
}

#: Every reason `derive` gives for a difference, and what shows it. The
#: README explains the same codes (`due_test` holds the two together).
REASONS: dict[str, str] = {
    'universe:snapshot-only': 'the snapshot lists it and the ledger does '
    'not track it: `queue track` has not seeded it',
    **{
        f'universe:ledger-only:{cause}': (
            f'the ledger tracks it and the snapshot does not list it: '
            f'{meaning}'
        )
        for cause, meaning in _SNAPSHOT_CAUSES.items()
    },
    'upstream-not-run': 'the ledger has it due and its input is not '
    'produced yet: a stage before it is not present',
    'head-moved': 'the snapshot saw a push the ledger has not (release), '
    'or the ledger recorded the stage for another commit than its commit '
    'row produced (tree, content)',
    'output-unknown': 'the ledger has the commit current but not what it '
    'produced, a row adopted from a watermark',
    'file-missing': 'the ledger has the stage done and its file is not in '
    'the store, or cut short, or unreadable',
    'lost-record': 'the store has the output and the ledger has no row for '
    'it, or one for another input',
    'stage-version': 'the ledger has the stage due for its code version, '
    'and the store has an output the version cannot be told of',
    'selection-unchanged': 'content the ledger has due for its version, '
    'and discovering the tree again selects exactly its files',
    'content-version': 'content whose stamped version is older, or that no '
    'version vouches for, where the ledger has it current',
    'selection-changed': 'discovering the tree again selects other files '
    'than the content root holds',
    'limits-changed': 'content fetched under other discovery limits than '
    'the ones in force',
    'unsettled': 'content with a file that failed in a way that may pass: '
    'an error, a 5xx or a 429',
    'another-syft': 'an SBOM another Syft wrote than the one in force',
    'input-changed': 'an SBOM older than a file it was made from',
    'leased': 'a dependency graph a worker holds right now',
    'timing': 'they differed, and agreed on a second look: the collector '
    'wrote meanwhile',
    'unexplained': 'no rule above explains it',
}


@dataclass
class Tally:
    """How many, and the first few by id."""

    count: int = 0
    samples: list[int] = field(default_factory=list)

    def add(self, repository_id: int, limit: int) -> None:
        self.count += 1
        if len(self.samples) < limit:
            self.samples.append(repository_id)


@dataclass
class StageReport:
    """One stage: where it stands, and how that differs from the ledger."""

    stage: Stage
    states: Counter[State] = field(default_factory=Counter)
    #: Why, by state.
    why: dict[State, dict[str, Tally]] = field(default_factory=dict)
    #: Verdicts that rest on the ledger rather than the store: release
    #: and commit, and content only the ledger's row vouches for.
    ledger_backed: int = 0
    #: The ledger's due count; None when not compared.
    ledger_due: int | None = None
    #: Due by both.
    both: int = 0
    #: Due by one side alone, by reason.
    derived_only: dict[str, Tally] = field(default_factory=dict)
    ledger_only: dict[str, Tally] = field(default_factory=dict)

    @property
    def due(self) -> int:
        return self.states[State.DUE]


@dataclass
class Report:
    """What `derive` found."""

    now: datetime
    universe: str
    repositories: int
    stages: dict[Stage, StageReport]
    #: Repositories the ledger tracks, in scope; None without a ledger.
    tracked: int | None = None
    syft_version: str | None = None
    rediscover: bool = False
    compared: bool = False
    #: Disagreements looked at a second time, and how many then agreed.
    rechecked: int = 0
    converged: int = 0
    #: Seconds, by phase.
    elapsed: dict[str, float] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        def tallies(found: Mapping[str, Tally]) -> dict[str, Any]:
            return {
                code: {'count': tally.count, 'samples': tally.samples}
                for code, tally in sorted(found.items())
            }

        stages: dict[str, Any] = {}
        for stage, compared in self.stages.items():
            entry: dict[str, Any] = {
                'derived': {
                    str(state): {
                        'count': compared.states[state],
                        'why': tallies(compared.why.get(state, {})),
                    }
                    for state in State
                },
                'ledger_backed': compared.ledger_backed,
            }
            if self.compared:
                entry['ledger'] = {'due': compared.ledger_due}
                # Keys spelled as every other key of the report, so a
                # reader such as `jq .stages.sbom.compared.derived_only`
                # needs no quoting; the reasons under them are codes.
                entry['compared'] = {
                    'both': compared.both,
                    'derived_only': tallies(compared.derived_only),
                    'ledger_only': tallies(compared.ledger_only),
                }
            stages[str(stage)] = entry
        return {
            'now': self.now.isoformat(),
            'universe': {
                'source': self.universe, 'repositories': self.repositories,
            },
            'ledger': {'tracked': self.tracked},
            'syft_version': self.syft_version,
            'rediscover': self.rediscover,
            'stages': stages,
            'second_look': (
                {'rechecked': self.rechecked, 'converged': self.converged}
                if self.compared else None
            ),
            'elapsed_seconds': {
                phase: round(seconds, 3)
                for phase, seconds in self.elapsed.items()
            },
        }


#: Reads the ledger: every repository in scope with None, only these ids
#: with a set.
Reader = Callable[[set[int] | None], LedgerView]


def derive(
    catalog: Catalog,
    read: Reader,
    store: Store,
    settings: Settings,
    *,
    stages: Iterable[Stage] = COMPARED,
    compare: bool = True,
    samples: int = 10,
) -> Report:
    """Walk every repository of `catalog`, and with `compare` hold each
    stage beside the ledger's due set and say why they differ."""
    started = time.perf_counter()
    wanted = tuple(stage for stage in COMPARED if stage in set(stages))
    view = read(None)
    read_at = time.perf_counter()
    verdicts = {
        repository_id: walk(
            repository_id, view, store, settings,
            listed_push=catalog.pushed_at.get(repository_id), stages=wanted,
        )
        for repository_id in sorted(catalog.repositories)
    }
    walked_at = time.perf_counter()
    report = Report(
        now=settings.now,
        universe=catalog.source,
        repositories=len(catalog),
        stages={stage: StageReport(stage) for stage in wanted},
        tracked=len(view.rows) if view.exists else None,
        syft_version=settings.syft_version,
        rediscover=settings.rediscover,
        compared=compare and view.due is not None,
        elapsed={'ledger': read_at - started, 'store': walked_at - read_at},
    )
    for repository_id, found in verdicts.items():
        for stage, verdict in found.items():
            compared = report.stages[stage]
            compared.states[verdict.state] += 1
            compared.ledger_backed += verdict.ledger_backed
            compared.why.setdefault(verdict.state, {}).setdefault(
                verdict.why, Tally(),
            ).add(repository_id, samples)
    if not report.compared or view.due is None:
        report.elapsed['total'] = time.perf_counter() - started
        return report

    ids = sorted(set(catalog.repositories) | set(view.rows))
    differences: dict[tuple[Stage, int], tuple[str, str]] = {}
    for stage in wanted:
        compared = report.stages[stage]
        ledger_due = view.due[stage]
        compared.ledger_due = len(ledger_due)
        for repository_id in ids:
            judged = verdicts.get(repository_id, {}).get(stage)
            derived = judged is not None and judged.state is State.DUE
            if derived and repository_id in ledger_due:
                compared.both += 1
            difference = _difference(
                stage, repository_id, catalog, view, judged, settings.now,
            )
            if difference is not None:
                differences[(stage, repository_id)] = difference
    compared_at = time.perf_counter()
    report.elapsed['compare'] = compared_at - walked_at

    # A second look at what differs, now that the rest is done: the
    # collector writes to the ledger and the store while this reads, and
    # what agrees now differed only in time.
    if differences:
        again_ids = {repository_id for _, repository_id in differences}
        fresh = read(again_ids)
        again = {
            repository_id: walk(
                repository_id, fresh, store, settings,
                listed_push=catalog.pushed_at.get(repository_id),
                stages=wanted,
            )
            for repository_id in sorted(again_ids)
            if repository_id in catalog
        }
        report.rechecked = len(differences)
        for (stage, repository_id), (side, _) in list(differences.items()):
            now_differs = _difference(
                stage, repository_id, catalog, fresh,
                again.get(repository_id, {}).get(stage), settings.now,
            )
            if now_differs is None:
                differences[(stage, repository_id)] = (side, 'timing')
                report.converged += 1
            else:
                differences[(stage, repository_id)] = now_differs
    report.elapsed['second_look'] = time.perf_counter() - compared_at

    for (stage, repository_id), (side, code) in sorted(
        differences.items(), key=lambda item: item[0][1],
    ):
        compared = report.stages[stage]
        tallies = (
            compared.derived_only if side == DERIVED_ONLY
            else compared.ledger_only
        )
        tallies.setdefault(code, Tally()).add(repository_id, samples)
    report.elapsed['total'] = time.perf_counter() - started
    return report


def _difference(
    stage: Stage,
    repository_id: int,
    catalog: Catalog,
    view: LedgerView,
    verdict: Verdict | None,
    now: datetime,
) -> tuple[str, str] | None:
    """`(side, code)` where the walk and the ledger disagree on whether
    `stage` is due for a repository; None where they agree."""
    derived = verdict is not None and verdict.state is State.DUE
    assert view.due is not None
    if derived == (repository_id in view.due[stage]):
        return None
    side = DERIVED_ONLY if derived else LEDGER_ONLY
    row = view.rows.get(repository_id)
    if verdict is None:
        if row is None:
            # Due in a ledger that does not track it: nothing makes that.
            return side, 'unexplained'
        return side, f'universe:ledger-only:{_cause(row, catalog.source)}'
    if row is None:
        return side, 'universe:snapshot-only'
    record = view.stages.get(repository_id, {}).get(stage)
    if derived:
        return side, _derived_only(stage, verdict, record, now)
    return side, _ledger_only(stage, verdict, record)


def _cause(row: Row, source: str) -> str:
    """Why a repository the ledger has is not in the universe."""
    if row.absent_since is not None:
        return 'absent'
    if not row.snapshot:
        return 'unlisted'
    prefix = UNFILTERED_SNAPSHOT_PREFIX
    if not (row.snapshot.startswith(prefix) and source.startswith(prefix)):
        return 'other-snapshot'
    if row.snapshot > source:
        return 'newer-snapshot'
    if row.snapshot < source:
        return 'older-snapshot'
    return 'snapshot-differs'


def _leased(record: StageState | None, now: datetime) -> bool:
    """Held by a worker, as `claim_stage` sees a lease."""
    return (
        record is not None and bool(record.claimed_by)
        and record.claim_expires_at is not None
        and record.claim_expires_at > now
    )


def _derived_only(
    stage: Stage,
    verdict: Verdict,
    record: StageState | None,
    now: datetime,
) -> str:
    """Why the walk has a stage due that the ledger has not."""
    if stage is Stage.DEPGRAPH and _leased(record, now):
        return 'leased'
    if verdict.why in _FILE_MISSING:
        return 'file-missing'
    if verdict.why in _SEEN_IN_THE_STORE:
        return verdict.why
    return 'unexplained'


def _ledger_only(
    stage: Stage, verdict: Verdict, record: StageState | None,
) -> str:
    """Why the ledger has a stage due that the walk has not."""
    if verdict.state is State.WAITING:
        return 'upstream-not-run'
    if verdict.state is not State.PRESENT:
        return 'unexplained'
    if record is None or stage is Stage.DEPGRAPH:
        return 'lost-record'
    if record.stage_version < STAGE_VERSION[stage]:
        return (
            'selection-unchanged' if verdict.why == 'rediscovered'
            else 'stage-version'
        )
    if (
        stage in (Stage.TREE, Stage.CONTENT) and is_sha(record.input_key)
        and record.input_key != verdict.input
    ):
        return 'head-moved'
    return 'lost-record'


# -- what nothing points to --------------------------------------------------


@dataclass
class Found:
    """How many, and the first few, as `<id>/<directory>`."""

    count: int = 0
    samples: list[str] = field(default_factory=list)

    def add(self, sample: str, limit: int) -> None:
        self.count += 1
        if len(self.samples) < limit:
            self.samples.append(sample)


@dataclass
class Scans:
    """The directories under one stage root, by what points to them."""

    root: str
    count: int = 0
    #: Kept under a commit a repository of the universe is at: the one
    #: its commit stage produced, or one its tree or content stage ran
    #: for.
    pointed: int = 0
    #: A repository of the universe, at a commit nothing points to: a
    #: scan the head has moved on from, kept until `data prune`.
    superseded: Found = field(default_factory=Found)
    #: A repository outside the universe.
    outside: Found = field(default_factory=Found)

    def as_json(self) -> dict[str, Any]:
        return {
            'root': self.root,
            'count': self.count,
            'pointed': self.pointed,
            'superseded': {
                'count': self.superseded.count,
                'samples': self.superseded.samples,
            },
            'outside': {
                'count': self.outside.count, 'samples': self.outside.samples,
            },
        }


def inventory(
    paths: PathConfig,
    catalog: Catalog,
    view: LedgerView,
    *,
    shard: tuple[int, int] | None = None,
    samples: int = 10,
) -> list[Scans]:
    """Every scan under the stage roots kept by commit, and every
    repository's graphs, by whether anything points to them.

    A listing of each root and each repository in it: a few seconds for
    the corpus. The dependency graph's fetches are kept for good by
    design, so only a repository outside the universe counts there.
    """
    pointed: dict[int, set[str]] = {}
    for repository_id in catalog.repositories:
        records = view.stages.get(repository_id, {})
        keys: set[str] = set()
        commit = records.get(Stage.COMMIT)
        if commit is not None and is_sha(commit.output_key):
            keys.add(commit.output_key)
        for stage in (Stage.TREE, Stage.CONTENT):
            record = records.get(stage)
            if record is not None and is_sha(record.input_key):
                keys.add(record.input_key)
        pointed[repository_id] = keys
    found: list[Scans] = []
    for root in (
        paths.tree_dir, paths.content_dir, paths.sbom_dir,
        paths.generated_lock_dir,
    ):
        scans = Scans(root.name)
        for repository_id, sha in _scans(root, shard):
            scans.count += 1
            if repository_id not in catalog:
                scans.outside.add(f'{repository_id}/{sha}', samples)
            elif sha in pointed[repository_id]:
                scans.pointed += 1
            else:
                scans.superseded.add(f'{repository_id}/{sha}', samples)
        found.append(scans)
    graphs = Scans(paths.depgraph_dir.name)
    for repository_id in _repositories(paths.depgraph_dir, shard):
        graphs.count += 1
        if repository_id in catalog:
            graphs.pointed += 1
        else:
            graphs.outside.add(str(repository_id), samples)
    found.append(graphs)
    return found


def _repositories(root: Path, shard: tuple[int, int] | None) -> list[int]:
    """The repository directories under a stage root, in id order."""
    try:
        entries = list(os.scandir(root))
    except OSError:
        return []
    return sorted(
        int(entry.name) for entry in entries
        if entry.name.isdigit() and entry.is_dir()
        and (shard is None or int(entry.name) % shard[1] == shard[0])
    )


def _scans(
    root: Path, shard: tuple[int, int] | None,
) -> Iterable[tuple[int, str]]:
    """`(repository_id, sha)` of every `<id>/<sha>` scan below a root."""
    for repository_id in _repositories(root, shard):
        try:
            entries = list(os.scandir(root / str(repository_id)))
        except OSError:
            continue
        for sha in sorted(
            entry.name for entry in entries
            if is_sha(entry.name) and entry.is_dir()
        ):
            yield repository_id, sha
