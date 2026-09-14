"""Where a dependency record came from, and how firm its version is.

Two sources with different guarantees, so consumers must be able to tell
them apart:

`syft` reads lockfiles, so it reports the *resolved closure* with exact
versions — but only where a lockfile exists. That is why Java and PHP
coverage is thin: Maven and Composer projects often ship none.

`github-depgraph` is GitHub's own dependency graph, which parses
manifests server-side. It covers those projects (Syft found 0 packages in
spring-boot; the dependency graph finds 303) but reports only *declared*
dependencies, and its versions are the manifest's constraints — `>= 0`,
or nothing at all — not resolutions.

Recording both the source and the version kind keeps a constraint from
being mistaken for a resolved version in a chart.
"""
from typing import Any
from typing import get_args
from typing import Literal
from typing import TypeAlias
from typing import TypeGuard

ArtifactSource: TypeAlias = Literal['syft', 'github-depgraph']

SYFT: ArtifactSource = 'syft'
DEPGRAPH: ArtifactSource = 'github-depgraph'

#: Every member of `ArtifactSource`, derived from the type itself.
ARTIFACT_SOURCES: tuple[ArtifactSource, ...] = get_args(ArtifactSource)


VersionKind: TypeAlias = Literal['resolved', 'constraint', 'unversioned']

#: An exact version, from a lockfile.
RESOLVED: VersionKind = 'resolved'
#: A requirement such as `>= 0` or `^4.18`, not a resolution.
CONSTRAINT: VersionKind = 'constraint'
#: No version information at all.
UNVERSIONED: VersionKind = 'unversioned'

VERSION_KINDS: tuple[VersionKind, ...] = get_args(VersionKind)


def is_artifact_source(value: Any) -> TypeGuard[ArtifactSource]:
    return isinstance(value, str) and value in ARTIFACT_SOURCES


def as_artifact_source(value: Any) -> ArtifactSource:
    """Validate a source read from outside the process."""
    if is_artifact_source(value):
        return value
    raise ValueError(
        f"invalid artifact source {value!r}; expected one of "
        f"{', '.join(ARTIFACT_SOURCES)}",
    )


def is_version_kind(value: Any) -> TypeGuard[VersionKind]:
    return isinstance(value, str) and value in VERSION_KINDS


def as_version_kind(value: Any) -> VersionKind:
    """Validate a version kind read from outside the process."""
    if is_version_kind(value):
        return value
    raise ValueError(
        f"invalid version kind {value!r}; expected one of "
        f"{', '.join(VERSION_KINDS)}",
    )


#: Characters that mark a version string as a requirement rather than a
#: resolution: `>= 0`, `^4.18.0`, `~> 2.8`, `1.0 - 2.0`, `*`.
_CONSTRAINT_MARKERS = frozenset('><=^~*|, ')


def classify_version(version: str | None) -> tuple[str, VersionKind]:
    """Normalise a version string and say what kind of version it is."""
    text = (version or '').strip()
    if not text:
        return '', UNVERSIONED
    if text in {'*', 'latest'} or any(c in _CONSTRAINT_MARKERS for c in text):
        return text, CONSTRAINT
    return text, RESOLVED
