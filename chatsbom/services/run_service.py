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

    content_dir / repository_id / commit_sha

and each service checks its own per-repository cache before reaching
for the network. So the worker walks the full chain for a claimed
repository and lets those caches make the stages that are not due
nearly free, rather than storing hand-offs a second time. What the
ledger records is, per stage, what it consumed and what it produced
(`stage_state`): a stage is due again when its upstream produces
something else, or its `STAGE_VERSION` moves.

## One stage at a time

`chatsbom run --stage tree` claims only what TREE is due for and records
only TREE. The stages before it are walked for their hand-offs, served
from their own caches, and recorded by whoever holds them; each stage's
lease and backoff are its own, so a stage that keeps failing no longer
backs off the others.

## What bounds a run

`--quota`, counted in requests the services actually made, because a
cache miss on a stage that was not due still spends rate limit. The
worker stops between repositories, never mid-repository: a partially
collected repository whose watermarks say it is done is worse than one
that is plainly not done yet.
"""
from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.ledger import DEFAULT_LEASE
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import RepositoryState
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageClaim
from chatsbom.core.ledger import UPSTREAM
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


def output_key(stage: Stage, produced: Mapping[str, Any], consumed: str) -> str:
    """What a stage produced, as the key its downstream is due against.

    * RELEASE: the tag chosen, or '' for the default branch;
    * COMMIT: the commit resolved;
    * TREE: the commit it is for, which is what it consumed;
    * CONTENT: the digest of the `(path, size)` list it stored
      (`discovery.content_digest`), so the SBOM is due again exactly
      when the files it would scan changed;
    * SBOM: the Syft document's sha256.
    """
    if stage is Stage.RELEASE:
        release = produced.get('latest_stable_release')
        if isinstance(release, Mapping):
            return str(release.get('tag_name') or '')
        return ''
    if stage is Stage.COMMIT:
        target = produced.get('download_target')
        if isinstance(target, Mapping):
            return str(target.get('commit_sha') or '')
        return ''
    if stage is Stage.CONTENT:
        digest = produced.get('content_digest')
        if digest:
            return str(digest)
        return consumed
    if stage is Stage.SBOM:
        stored = produced.get('sbom_path')
        if stored:
            try:
                return hashlib.sha256(Path(str(stored)).read_bytes()).hexdigest()
            except OSError:
                pass
    return consumed


@dataclass
class RunResult:
    """What a pass did, in terms someone can act on."""

    repositories: int = 0
    completed: Counter[str] = field(default_factory=Counter)
    failed: int = 0
    unusable: int = 0
    spent_quota: int = 0
    stopped_early: bool = False
    #: Walks stopped at a stage still backing off from a failure.
    blocked: int = 0
    #: Claimed, but taken by another worker before this one reached it.
    taken: int = 0

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
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._ledger = ledger
        self._runners = runners
        self._spent = spent
        # Unique per process: a lease is renewed and dropped by whoever
        # holds it, which a name shared by every worker cannot say.
        self._worker = worker
        # The lease runs on the wall clock, not the pass's `now`.
        self._clock = clock

    def advance(
        self,
        now: datetime,
        limit: int,
        quota_budget: int,
        stage: Stage | None = None,
        repos: Iterable[int] | None = None,
    ) -> RunResult:
        """One pass: every due stage, or with `stage` that stage alone.

        `repos` narrows the pass to those repositories (`--repos-file`).
        """
        if stage is not None and stage not in STAGES:
            raise ValueError(
                f'{stage} is not one of {", ".join(map(str, STAGES))}',
            )
        result = RunResult()
        start_quota = self._spent()
        wanted = STAGES if stage is None else (stage,)

        claims = self._ledger.claim_stages(
            wanted,
            now,
            limit,
            self._worker,
            # The walk runs the whole chain, so it holds the whole chain;
            # one stage alone holds that stage.
            lease_stages=wanted,
            # Every tracked repository, a search snapshot's with no
            # language among them: content picks manifests from the
            # tree, so the ledger's language selects nothing.
            repos=repos,
        )
        for index, claim in enumerate(claims):
            if self._spent() - start_quota >= quota_budget:
                # Released rather than left leased: the lease would
                # expire eventually, but a repository this pass decided
                # not to touch should be immediately available to the
                # next one.
                for rest in claims[index:]:
                    self._ledger.release_stages(
                        rest.state.repository_id, rest.leased, self._worker,
                    )
                result.stopped_early = True
                break

            if not self._ledger.renew_stages(
                claim.state.repository_id, claim.leased, self._worker,
                DEFAULT_LEASE, self._clock(),
            ):
                result.taken += 1
                continue

            result.repositories += 1
            try:
                self._advance_one(claim, now, result, stage)
            finally:
                self._ledger.release_stages(
                    claim.state.repository_id, claim.leased, self._worker,
                )

        result.spent_quota = self._spent() - start_quota
        return result

    def _advance_one(
        self,
        claim: StageClaim,
        now: datetime,
        result: RunResult,
        target: Stage | None,
    ) -> None:
        """One repository, its stages in order, recorded as they finish.

        A stage that fails stops this repository rather than the pass:
        the stages after it need its output, and running them against a
        missing input produces rows that look collected. The failure is
        recorded on that stage alone (`record_stage_failure`), so a stage
        that keeps failing stops costing a slot without holding back the
        others.

        Every stage the walk holds and runs is recorded with what it
        consumed and produced, due or not: it did run, and its downstream
        is judged against what it produced. `completed` counts only the
        ones that were due, which is the work the pass did.
        """
        state = claim.state
        chain = STAGES if target is None else STAGES[:STAGES.index(target) + 1]
        recordable = set(claim.leased)
        repository = self._repository_for(state)
        if repository is None:
            result.unusable += 1
            self._ledger.record_stage_failure(
                state.repository_id,
                target or Stage.RELEASE,
                now,
                'no repository record to start from',
            )
            return

        produced_keys: dict[Stage, str] = {}
        carried: dict[str, Any] = {}
        for stage in chain:
            runner = self._runners.get(stage)
            if runner is None:
                continue
            if stage in claim.blocked:
                # Still backing off from a failure: the stages after it
                # would run on its missing output.
                result.blocked += 1
                return

            upstream = UPSTREAM[stage]
            consumed = (
                produced_keys[upstream] if upstream in produced_keys
                else self._ledger.upstream_key(state.repository_id, stage)
            )
            try:
                produced = runner(repository, carried)
            except Exception as error:  # noqa: BLE001 - recorded, not raised
                logger.warning(
                    'Stage failed',
                    repo=state.full_name,
                    stage=str(stage),
                    error=str(error),
                )
                if stage in recordable:
                    self._ledger.record_stage_failure(
                        state.repository_id, stage, now, str(error),
                    )
                else:
                    # An upstream walked for its hand-off failed: the
                    # stage asked for cannot run, and backs off with it.
                    self._ledger.record_stage_failure(
                        state.repository_id, chain[-1], now,
                        f'upstream {stage}: {error}',
                    )
                result.failed += 1
                return

            if produced is None:
                # Not a failure: a repository with no releases has
                # nothing for the release stage to do, and recording
                # that as an error would back off a repository that is
                # working exactly as expected. Not recorded either, so
                # it stays due; what it produced before stands.
                previous = self._ledger.stage_state(state.repository_id, stage)
                produced_keys[stage] = previous.output_key if previous else ''
                continue

            carried.update(produced)
            repository = self._merged(repository, produced)
            if stage is Stage.COMMIT:
                # What `ls-remote` said HEAD is, for the next reader of
                # the ledger: the depgraph stamp, the warehouse.
                self._ledger.observe_default_branch(
                    state.repository_id, repository.default_branch,
                )
            key = output_key(stage, produced, consumed)
            produced_keys[stage] = key
            if stage in recordable:
                self._ledger.record_stage_success(
                    state.repository_id, stage, now, consumed, key,
                )
                if stage in claim.due:
                    result.completed[str(stage)] += 1

    def _repository_for(self, state: RepositoryState) -> Repository | None:
        """The repository to start the chain from.

        Built from the ledger's own columns. The services enrich it as
        they go, and each reads its own cache first, so this only has to
        be enough to name the repository -- not a faithful copy of what
        the last pass stored.

        But everything the ledger does know goes in: the stages build on
        it, and what it leaves out is the model's placeholder. Left out,
        `default_branch` was `'main'` and the commit stage asked for a
        branch most of the corpus does not have; `stars` was 0 and the
        URL empty, in the index, for every repository without a metadata
        document (#55 pilot).
        """
        data: dict[str, Any] = {
            'id': state.repository_id,
            'owner': state.owner,
            'name': state.repo,
            'html_url': f'https://github.com/{state.owner}/{state.repo}',
            'language': state.language,
        }
        if state.default_branch:
            data['default_branch'] = state.default_branch
        if state.stars is not None:
            data['stargazers_count'] = state.stars
        if state.github_language:
            data['github_language'] = state.github_language
        if state.pushed_at_seen is not None:
            data['pushed_at'] = state.pushed_at_seen
        try:
            return Repository.model_validate(data)
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
