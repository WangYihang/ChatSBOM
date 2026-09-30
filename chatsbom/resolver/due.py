"""What the resolver has to resolve, from the store (#168; #128 section
2.1: "it has its own due set: content roots that need a lockfile").

A directory is due when, at the repository's current commit in the
store:

- it holds a manifest a recipe reads, and none of the lockfiles the
  recipe makes (`sandbox.recipes_for`): what a project ships is what it
  pins, and is never resolved over;
- it has no result, a lockfile of the recipe's under
  `PathConfig.generated_lock_path` (`LockRecipe.generated_in`);
- it has no failure still backing off in resolver.sqlite (`state`).

The repository's current commit is #161's to say, not this module's
(`collector.due.standing`): the commit that stands for the newest push
the store has decided, once that commit's content is in, whole and
stamped by the content stage in force. Older commits are not resolved:
a result is for the SBOM of the commit the repository is at, and a
commit whose content is not in yet waits for the collector to fetch
it. The push is the store's newest decided, not the one collector.sqlite
last observed, which is the collector's own and not read here.

The repositories are the universe's, the newest complete search
snapshot (`core/catalog.py`, #100 Q1), the most starred first; a
repository outside it is not resolved, as it is not collected.

A walk reads, for each repository, its newest release decision and what
`standing` reads to reach the content (a few files), then the content
root's list of files: about what the collector's own walk of the
universe reads, plus the listing.
"""
from __future__ import annotations

from collections.abc import Collection
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path

from chatsbom.collector.due import standing
from chatsbom.collector.state import Outcome
from chatsbom.core import decisions
from chatsbom.core.catalog import Catalog
from chatsbom.core.catalog import newest_complete
from chatsbom.core.catalog import read_snapshot
from chatsbom.core.config import PathConfig
from chatsbom.core.sandbox import LockTarget
from chatsbom.core.sandbox import recipes_for
from chatsbom.resolver.state import ResolverState
from chatsbom.services.content_service import stored_files

#: The stage whose output is the content root a recipe reads.
CONTENT = 'content'


@dataclass(frozen=True)
class Due:
    """A directory to resolve: of a repository's current commit, and the
    recipe to resolve it with."""

    repository_id: int
    #: `owner/name`, as the universe lists it.
    full_name: str
    stars: int | None
    #: The repository's current commit.
    sha: str
    target: LockTarget
    #: The directory in the content root, which the resolver reads.
    project: Path
    #: The directory under the generated-lock root, where its lockfile
    #: goes.
    output: Path


@dataclass
class Walk:
    """What a walk of the universe found."""

    #: The snapshot the universe is, `all-<date>`; None when there is no
    #: complete one.
    universe: str | None = None
    #: The directories due, in the order to resolve them.
    due: list[Due] = field(default_factory=list)
    #: Current content roots walked.
    roots: int = 0
    #: Their directories a recipe reads, that ship no lockfile.
    directories: int = 0
    #: Of those, the ones with a result already.
    resolved: int = 0
    #: And the ones whose failure is still backing off.
    backing_off: int = 0
    #: The names asked for that nothing in the universe answers to.
    unknown: list[str] = field(default_factory=list)


class _Unkept:
    """collector.sqlite's outcomes, as the resolver has them: none. It
    is the collector's, held by it alone, and says only whether a stage
    missing from the store is due or backing off: of no matter to which
    commit's content the store has."""

    def outcome(
        self, repository_id: int, stage: str, key: str,
    ) -> Outcome | None:
        return None


def universe(paths: PathConfig, now: datetime) -> Catalog | None:
    """The universe on the day of `now` (UTC): the newest complete search
    snapshot, or None when there is none."""
    snapshot = newest_complete(paths.search_dir, now.date())
    return None if snapshot is None else read_snapshot(snapshot)


def current_commit(
    paths: PathConfig, repository_id: int, now: datetime,
) -> str | None:
    """The repository's current commit in the store, once its content is
    in; else None."""
    for decision in decisions.release_decisions(paths, repository_id):
        found = standing(
            repository_id, decision.push, paths=paths, outcomes=_Unkept(),
            syft_version=None, now=now,
        )
        content = next(v for v in found.verdicts if v.stage == CONTENT)
        if content.state != 'present' or found.commit is None:
            return None
        return found.commit.commit_sha
    return None


def _by_stars(
    catalog: Catalog, repositories: Collection[int] | None,
) -> Iterator[tuple[int, str, int | None]]:
    """The universe's repositories, or those of them named: the most
    stars first, then by id."""
    for repository_id, tracked in sorted(
        catalog.repositories.items(),
        key=lambda item: (
            item[1].stars is None, -(item[1].stars or 0), item[0],
        ),
    ):
        if repositories is None or repository_id in repositories:
            yield repository_id, tracked.full_name, tracked.stars


def walk(
    paths: PathConfig,
    state: ResolverState,
    *,
    now: datetime,
    ecosystems: Iterable[str] | None = None,
    names: Iterable[str] | None = None,
    limit: int | None = None,
    force: bool = False,
) -> Walk:
    """The directories due, the most-starred repositories first, and
    what the walk found on the way; at most `limit` of them, where the
    walk stops. `names`, `owner/name` or an id each, are the
    repositories to walk alone, found in the universe as GitHub finds a
    name, whatever its case. `force` counts a result as none: what the
    resolver wrote is resolved again, and never what a project ships."""
    catalog = universe(paths, now)
    found = Walk(universe=None if catalog is None else catalog.source)
    if catalog is None:
        return found
    repositories: Collection[int] | None = None
    if names is not None:
        repositories, found.unknown = catalog.resolve(names)
    wanted = None if ecosystems is None else set(ecosystems)
    for repository_id, full_name, stars in _by_stars(catalog, repositories):
        sha = current_commit(paths, repository_id, now)
        if sha is None:
            continue
        found.roots += 1
        root = paths.content_root(repository_id, sha)
        output_root = paths.generated_lock_path(repository_id, sha)
        for target in recipes_for(stored_files(root)):
            if wanted is not None and target.ecosystem not in wanted:
                continue
            found.directories += 1
            output = target.within(output_root)
            if target.recipe.generated_in(output) and not force:
                found.resolved += 1
                continue
            failure = state.failure(repository_id, sha, target)
            if failure is not None and failure.backing_off(now):
                found.backing_off += 1
                continue
            found.due.append(
                Due(
                    repository_id, full_name, stars, sha, target,
                    target.within(root), output,
                ),
            )
            if limit is not None and len(found.due) >= limit:
                return found
    return found
