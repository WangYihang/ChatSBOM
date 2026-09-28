"""Collect one repository at a time, driven by the ledger.

The pipeline is nine commands, each walking every repository through one
stage before the next begins. That is stage-major, and it has three
costs the ledger was built to avoid: a killed run loses the pass, an
unchanged repository costs the same as a changed one, and nothing
records that a particular repository keeps failing.

`chatsbom queue sync` already closed half the loop -- it revalidates the
repository resource conditionally, and a 304 costs no rate limit -- but
nothing consumed what it learned. A repository whose `pushed_at` moved
became due for seven stages and then waited for someone to run seven
commands by hand.

This is the other half. One repository, all its due stages, in order.

## Why the chain is walked whole

A stage needs what the stage before it produced: `content` needs the
`download_target` that `commit` resolved, `sbom` needs the directory
`content` wrote. Those hand-offs live in the language-major JSONL
ledgers, which are 5.2 GB for `07-sbom` alone because each record
embeds the repository *and every one of its releases* -- so indexing
them by repository id is minutes and gigabytes, not a lookup.

It is also unnecessary. Every path is a pure function of the
repository and its download target:

    content_dir / language / owner / repo / ref / commit_sha

and each service checks its own per-repository cache before reaching
for the network. So the worker walks the full chain for a claimed
repository and lets those caches make the stages that are not due
nearly free, rather than storing hand-offs a second time. What the
ledger records is which stages did work.

## What bounds a run

`--quota`, counted in requests the services actually made, because a
cache miss on a stage that was not due still spends rate limit. The
worker stops between repositories, never mid-repository: a partially
collected repository whose watermarks say it is done is worse than one
that is plainly not done yet.
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import RepositoryState
from chatsbom.core.ledger import Stage
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository

logger = structlog.get_logger('run')

#: The stages this worker runs, in the order their outputs are needed.
#:
#: `Stage.LOCK` is absent on purpose. Generating a lockfile runs a
#: package manager over untrusted source, so it belongs in a container
#: (compose's `lock` service, the `lock` stage of `Dockerfile`) and not
#: in a loop that also holds a GitHub token. `Stage.REPO` is absent because `queue sync` owns it: that is
#: the conditional request whose 304 is free, and doing it here as well
#: would spend rate limit to learn what sync already knows.
#:
#: `Stage.DEPGRAPH` is absent too. It needs nothing from these stages,
#: is due for every tracked repository whether or not its SBOM
#: succeeded, and is metered by a bucket of its own, so it is scheduled
#: per stage by `services/depgraph_stage.py` (`chatsbom run --stage
#: depgraph`) rather than walked here.
STAGES: tuple[Stage, ...] = (
    Stage.RELEASE,
    Stage.COMMIT,
    Stage.TREE,
    Stage.CONTENT,
    Stage.SBOM,
)


@dataclass
class RunResult:
    """What a pass did, in terms someone can act on."""

    repositories: int = 0
    completed: Counter[str] = field(default_factory=Counter)
    failed: int = 0
    unusable: int = 0
    remembered: int = 0
    spent_quota: int = 0
    stopped_early: bool = False

    @property
    def stages_run(self) -> int:
        return sum(self.completed.values())


class RunService:
    """Walk claimed repositories through their due stages.

    The stage callables are injected rather than constructed here so a
    test can drive the whole loop without a GitHub token: what is worth
    testing is the scheduling -- which stages run, what gets recorded,
    when the pass stops -- and that is independent of what a stage does.
    """

    def __init__(
        self,
        ledger: Ledger,
        # Mapping, not dict: the runners are only read, and `dict` is
        # invariant in its value type -- so a caller whose callables are
        # inferred as returning `Any` could not pass them.
        runners: Mapping[
            Stage,
            Callable[[Repository, dict[str, Any]], dict[str, Any] | None],
        ],
        spent: Callable[[], int],
        worker: str = 'run',
        remember: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> None:
        self._ledger = ledger
        self._runners = runners
        self._spent = spent
        self._worker = worker
        # Called with the finished record, once per repository. Injected
        # rather than constructed here for the same reason the runners
        # are: what this class owns is the scheduling, and a test should
        # be able to drive it without a database.
        self._remember = remember

    def advance(
        self,
        now: datetime,
        limit: int,
        quota_budget: int,
        language: str | None = None,
    ) -> RunResult:
        result = RunResult()
        start_quota = self._spent()

        for state in self._claim(now, limit, language):
            if self._spent() - start_quota >= quota_budget:
                # Released rather than left leased: the lease would
                # expire eventually, but a repository this pass decided
                # not to touch should be immediately available to the
                # next one.
                self._ledger.release(state.repository_id)
                result.stopped_early = True
                break

            result.repositories += 1
            try:
                self._advance_one(state, now, result)
            finally:
                self._ledger.release(state.repository_id)

        result.spent_quota = self._spent() - start_quota
        return result

    def _claim(
        self,
        now: datetime,
        limit: int,
        language: str | None,
    ) -> list[RepositoryState]:
        """Repositories needing at least one of `STAGES`, deduplicated.

        Claimed per stage because that is what the ledger offers, then
        collapsed by repository id: a repository due for four stages
        must be one unit of work, or the chain gets walked four times
        and the quota pays for it.
        """
        seen: dict[int, RepositoryState] = {}
        for stage in STAGES:
            if len(seen) >= limit:
                break
            for state in self._ledger.claim(
                stage,
                now,
                limit=limit - len(seen),
                worker=self._worker,
                language=language,
                # Not the repositories only a search snapshot listed:
                # this walk keys its paths by language, and they have
                # none. The dependency graph takes them.
                keyed_only=True,
            ):
                seen.setdefault(state.repository_id, state)
        return list(seen.values())

    def _advance_one(
        self,
        state: RepositoryState,
        now: datetime,
        result: RunResult,
    ) -> None:
        """One repository, its stages in order, recorded as they finish.

        A stage that fails stops this repository rather than the pass:
        the stages after it need its output, and running them against a
        missing input produces rows that look collected. The ledger's
        `record_failure` then backs it off, so a repository that keeps
        failing stops costing a slot.
        """
        repository = self._repository_for(state)
        if repository is None:
            result.unusable += 1
            self._ledger.record_failure(
                state.repository_id,
                Stage.RELEASE,
                now,
                'no repository record to start from',
            )
            return

        carried: dict[str, Any] = {}
        for stage in STAGES:
            runner = self._runners.get(stage)
            if runner is None:
                continue
            was_due = state.needs(stage, now)
            try:
                produced = runner(repository, carried)
            except Exception as error:  # noqa: BLE001 - recorded, not raised
                logger.warning(
                    'Stage failed',
                    repo=state.full_name,
                    stage=str(stage),
                    error=str(error),
                )
                self._ledger.record_failure(
                    state.repository_id, stage, now, str(error),
                )
                result.failed += 1
                return

            if produced is None:
                # Not a failure: a repository with no releases has
                # nothing for the release stage to do, and recording
                # that as an error would back off a repository that is
                # working exactly as expected.
                continue

            carried.update(produced)
            repository = self._merged(repository, produced)
            if was_due:
                self._ledger.record_success(state.repository_id, stage, now)
                result.completed[str(stage)] += 1

        # The finished record, kept once per repository rather than once
        # per stage.
        #
        # The stage ledgers each append their own copy of the whole
        # record to carry it to the next stage, which is why the release
        # list was on disk four times and 21 of the 22 GB of ledgers was
        # that repetition. Writing it here — at the end of the chain,
        # when it is complete — is what lets those ledgers keep only a
        # line saying which repository reached which stage.
        #
        # After the loop, not inside it: a record written per stage
        # would be seven rows per repository, six of them describing
        # states nothing wants to read.
        if self._remember is not None:
            self._remember({
                **repository.model_dump(mode='json'),
                **carried,
            })
            result.remembered += 1

    def _repository_for(self, state: RepositoryState) -> Repository | None:
        """The repository to start the chain from.

        Built from the ledger's own columns. The services enrich it as
        they go, and each reads its own cache first, so this only has to
        be enough to name the repository -- not a faithful copy of what
        the last pass stored.
        """
        try:
            return Repository.model_validate({
                'id': state.repository_id,
                'owner': state.owner,
                'name': state.repo,
                'language': state.language,
            })
        except Exception as error:  # noqa: BLE001
            logger.warning(
                'Unusable ledger row',
                repo=state.full_name,
                error=str(error),
            )
            return None

    @staticmethod
    def _merged(
        repository: Repository,
        produced: dict[str, Any],
    ) -> Repository:
        """`repository` with what a stage produced folded in.

        Rebuilt through the model rather than mutated so a stage cannot
        put a field into a shape the next stage does not expect -- the
        validators are the only thing that knows, for instance, that
        `download_target` is an object and not a string.
        """
        merged = {**repository.model_dump(mode='json'), **produced}
        try:
            return Repository.model_validate(merged)
        except Exception:  # noqa: BLE001
            # The stage's output did not fit. Keeping the repository as
            # it was is the conservative answer: the next stage reads
            # `carried` too, so nothing is lost that was not already
            # unusable.
            return repository


def content_path(
    base: Path,
    language: str,
    owner: str,
    repo: str,
    ref: str,
    commit_sha: str,
) -> Path:
    """Where `content` wrote a repository's manifests.

    Spelled out here because it is the hand-off `sbom` needs, and
    deriving it is what makes the 5.2 GB of JSONL ledgers unnecessary to
    the worker. Must match `ContentService.process_repo`.
    """
    value = language.value if isinstance(language, Language) else str(language)
    return base / value / owner / repo / ref / commit_sha
