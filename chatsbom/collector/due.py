"""What is due for each repository, derived from the store (#161; #100
section 2, "the chain"; #128 section 2.1).

A stage is due exactly when its output for its current key is not in
the store. The keys follow the chain, each from what the stage before it
decided:

1. P, the push: the one the observation collected for saw (#160). A
   repository never observed has none to be collected for.
2. The release decision for P gives T, the latest stable release's tag,
   or none (#147).
3. K is `tag:T`, or `head:P` when there is none.
4. The commit decision for K gives S, the commit: the resolution that
   stands for P (`decisions.standing`).
5. Then the tree of S; the content of S, stamped with the content
   stage's version (#100 Q4, strict: a root without the stamp is
   fetched again); and the SBOM of S, whole, by the Syft now running and
   newer than every file it was made from (#110).

The chain is walked in order and stops at the first stage whose output
is not there: that stage is due, and every stage after it is waiting,
since its input is not produced yet. So a push that decides a release
already resolved, or resolves to a commit already collected, finds the
rest present, and nothing after it is due: the early cutoff.

A stage that last produced nothing for its key, or failed, is backing
off until collector.sqlite says it is due again (#100 Q5). Its outcome
is kept by the key the stage ran for (`Verdict.key`): the push, K, S,
and for the SBOM S and the Syft (`sbom_key`), so that a new Syft is not
held back by an old one's failure.

Priority, highest first (#128 section 2.1): a repository pushed and
changed since it was collected; one never collected; and a rescan for a
new version of a tool, Syft or the content stage itself, which costs CPU
and downloads and no quota. Which repositories are in the first two is
collector.sqlite's to say, from what the sweep observed and what was
collected (#160); whether a stage due is a rescan is the store's
(`Standing.rescan`).

Reading is lazy: a stage waiting on another is never read, and each
read is a stat or an open or two, about as many per repository as
`core/due.py` counts for the ledger's comparison.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from enum import IntEnum
from typing import Protocol

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import LIMITS
from chatsbom.collector.content import read_document
from chatsbom.collector.content import settled_document
from chatsbom.collector.content import stamp_of
from chatsbom.collector.state import Outcome
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.decisions import CommitDecision
from chatsbom.core.decisions import ReleaseDecision
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.layout import is_sha
from chatsbom.core.layout import push_instant
from chatsbom.core.layout import push_name
from chatsbom.core.layout import push_text
from chatsbom.core.ledger import Stage
from chatsbom.services.sbom_service import Stale
from chatsbom.services.sbom_service import staleness

#: The stages, in the order each needs the one before it.
CHAIN: tuple[Stage, ...] = (
    Stage.RELEASE, Stage.COMMIT, Stage.TREE, Stage.CONTENT, Stage.SBOM,
)

#: Why a stage is due when what it redoes is a tool's new version, not a
#: change of the repository's.
RESCANS = frozenset({
    'another-syft', 'unstamped', 'content-version', 'limits-changed',
})


class State(str, Enum):
    """Where one stage of one repository stands."""

    #: Its output for its current key is in the store.
    PRESENT = 'present'
    #: Its input is there and its output is not: to run now.
    DUE = 'due'
    #: Its input is not produced yet: a stage before it is not present.
    WAITING = 'waiting'
    #: Due, but it last produced nothing for this key, or failed, and its
    #: backoff has not passed.
    BACKING_OFF = 'backing-off'

    def __str__(self) -> str:
        return self.value


class Priority(IntEnum):
    """How soon a repository's due stage runs: the lowest first."""

    #: Pushed and changed since it was collected.
    CHANGED = 1
    #: Never collected.
    NEW = 2
    #: A tool's new version, the Syft now running or the content stage's:
    #: CPU and downloads, and no quota.
    RESCAN = 3


@dataclass(frozen=True)
class Verdict:
    """One stage of one repository, and why it stands there."""

    stage: Stage
    state: State
    #: In a word: what is missing, `nothing` or `failed` for a stage
    #: backing off, and for a stage waiting, the stage it waits on.
    why: str
    #: The key its output is looked for under, and its outcome kept by in
    #: collector.sqlite; '' while it waits.
    key: str = ''
    #: What it produced, where it is present: T (or '' for none), S, the
    #: content's digest.
    output: str = ''
    #: When a stage backing off is due again.
    due_at: datetime | None = None


@dataclass(frozen=True)
class Standing:
    """Where every stage of one repository stands, for one push."""

    repository_id: int
    push: datetime | None
    #: One per stage, in the chain's order.
    verdicts: tuple[Verdict, ...]
    #: The decisions the chain went through, where it reached them.
    release: ReleaseDecision | None = None
    commit: CommitDecision | None = None

    def verdict(self, stage: Stage) -> Verdict:
        return next(v for v in self.verdicts if v.stage is stage)

    @property
    def next(self) -> Verdict | None:
        """The stage to run now, if one is: at most one is due, since
        each stage after it waits for it."""
        return next((v for v in self.verdicts if v.state is State.DUE), None)

    @property
    def current(self) -> bool:
        """Every stage's output is in the store."""
        return all(v.state is State.PRESENT for v in self.verdicts)

    @property
    def due_at(self) -> datetime | None:
        """When the stage backing off is due again, if one is."""
        return next(
            (
                v.due_at for v in self.verdicts
                if v.state is State.BACKING_OFF
            ),
            None,
        )

    @property
    def rescan(self) -> bool:
        """Whether the stage due now is due for a tool's new version, the
        Syft now running or the content stage's, and for nothing the
        repository did."""
        step = self.next
        return step is not None and step.why in RESCANS


class Outcomes(Protocol):
    """Where outcomes are kept: `state.CollectorState`."""

    def outcome(
        self, repository_id: int, stage: str, key: str,
    ) -> Outcome | None:
        ...


def sbom_key(sha: str, syft_version: str | None) -> str:
    """What an SBOM's outcome is kept by: the commit, and the Syft."""
    return f'{sha} syft@{syft_version or "unknown"}'


def standing(
    repository_id: int,
    push: datetime | None,
    *,
    paths: PathConfig,
    outcomes: Outcomes,
    syft_version: str | None,
    now: datetime,
) -> Standing:
    """Where each stage of one repository stands for `push`, as the store
    and collector.sqlite say: the chain, walked as far as its first stage
    that is not present."""
    verdicts: list[Verdict] = []

    def found(
        release: ReleaseDecision | None = None,
        commit: CommitDecision | None = None,
    ) -> Standing:
        holder = verdicts[-1].stage if verdicts else None
        for stage in CHAIN[len(verdicts):]:
            verdicts.append(
                Verdict(stage, State.WAITING, str(holder or 'push')),
            )
        return Standing(repository_id, push, tuple(verdicts), release, commit)

    def absent(stage: Stage, key: str, why: str) -> None:
        """Not in the store: due, unless it is backing off."""
        outcome = outcomes.outcome(repository_id, str(stage), key)
        if outcome is not None and outcome.backing_off(now):
            verdicts.append(
                Verdict(
                    stage, State.BACKING_OFF, outcome.kind, key,
                    due_at=outcome.due_at,
                ),
            )
        else:
            verdicts.append(Verdict(stage, State.DUE, why, key))

    push = push_instant(push)
    if push is None:
        return found()

    # The release decision for P.
    key = push_text(push) or ''
    release = decisions.read_release(
        decisions.releases_dir(paths, repository_id) / str(push_name(push)),
        repository_id,
    )
    if release is None:
        absent(Stage.RELEASE, key, 'undecided')
        return found()
    verdicts.append(
        Verdict(
            Stage.RELEASE, State.PRESENT,
            'decided', key, release.tag or '',
        ),
    )

    # The commit decision for K, as it stands for P.
    key = str(release.key)
    commit = decisions.commit_decision(
        paths, repository_id, release.key, release.push,
    )
    if commit is None or not is_sha(commit.commit_sha):
        absent(
            Stage.COMMIT, key, 'unresolved' if commit is None else 'no-commit',
        )
        return found(release)
    sha = commit.commit_sha
    verdicts.append(
        Verdict(Stage.COMMIT, State.PRESENT, 'resolved', key, sha),
    )

    # The tree of S.
    tree = paths.tree_file(repository_id, sha)
    if not is_whole_tree(tree):
        absent(Stage.TREE, sha, _shape(tree))
        return found(release, commit)
    verdicts.append(Verdict(Stage.TREE, State.PRESENT, 'whole', sha, sha))

    # The content of S, stamped.
    why, digest = _content(paths, repository_id, sha)
    if why is not None:
        absent(Stage.CONTENT, sha, why)
        return found(release, commit)
    verdicts.append(
        Verdict(Stage.CONTENT, State.PRESENT, 'stamped', sha, digest),
    )

    # The SBOM of S, by the Syft now running.
    key = sbom_key(sha, syft_version)
    stale = staleness(
        paths.sbom_file(repository_id, sha),
        paths.content_root(repository_id, sha),
        paths.generated_lock_path(repository_id, sha),
        syft_version=syft_version,
    )
    if stale is not None:
        absent(Stage.SBOM, key, _stale(paths, repository_id, sha, stale))
        return found(release, commit)
    verdicts.append(Verdict(Stage.SBOM, State.PRESENT, 'current', key, sha))
    return found(release, commit)


# -- what the store has -------------------------------------------------------


def _shape(tree: os.PathLike[str]) -> str:
    """Why a tree is not whole: `missing`, `empty` or `cut-short`."""
    try:
        size = os.stat(tree).st_size
    except OSError:
        return 'missing'
    return 'empty' if size == 0 else 'cut-short'


def _content(
    paths: PathConfig, repository_id: int, sha: str,
) -> tuple[str | None, str]:
    """Why the content of `sha` is not current, or None while it is; and
    its digest."""
    index = paths.discovery_file(repository_id, sha)
    if not index.exists():
        return 'missing', ''
    document = read_document(index)
    if document is None:
        return 'unreadable', ''
    if document.get('commit_sha') != sha:
        return 'wrong-commit', ''
    stamp = stamp_of(document)
    if stamp is None:
        return 'unstamped', ''
    if stamp < CONTENT_VERSION:
        return 'content-version', ''
    if document.get('limits') != LIMITS:
        return 'limits-changed', ''
    if not settled_document(document):
        return 'unsettled', ''
    if not paths.content_root(repository_id, sha).is_dir():
        return 'root-missing', ''
    return None, str(document.get('digest') or '')


def _stale(
    paths: PathConfig, repository_id: int, sha: str, stale: Stale,
) -> str:
    """Why an SBOM is not current, in a word."""
    if stale is Stale.UNUSABLE and not paths.sbom_file(
        repository_id, sha,
    ).exists():
        return 'missing'
    return str(stale.value)
