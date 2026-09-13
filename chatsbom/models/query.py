"""Typed rows returned by the query layer.

`clickhouse_connect` hands back `Sequence[Sequence[Any]]`. Annotating
that as `list[tuple[str, str, int, str, str]]` was a lie mypy caught, and
positional unpacking at the call site is the same hazard that corrupted
two artifact columns. Every query result is built from named columns
instead, so a renamed column fails at the seam.
"""
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import fields
from datetime import datetime
from typing import Any
from typing import Protocol
from typing import Self
from typing import TypeVar

from chatsbom.models.relationship import as_relationship
from chatsbom.models.relationship import Relationship

Row = Mapping[str, Any]


def _text(value: Any) -> str:
    """ClickHouse Nullable(String) arrives as None; callers want ''."""
    return '' if value is None else str(value)


def _count(value: Any) -> int:
    return int(value or 0)


class FromRow(Protocol):
    """A query row model constructible from a named ClickHouse row."""

    @classmethod
    def from_row(cls, row: Row) -> Self:
        ...


RowModelT = TypeVar('RowModelT', bound=FromRow)


def row_mapper(
    model: type[RowModelT],
) -> Callable[[Iterable[Row]], list[RowModelT]]:
    """Build a mapper from a result set to a list of `model`."""
    def mapper(rows: Iterable[Row]) -> list[RowModelT]:
        return [model.from_row(row) for row in rows]
    return mapper


def _require(row: Row, column: str) -> Any:
    try:
        return row[column]
    except KeyError:
        raise KeyError(
            f"query result is missing column {column!r}; "
            f"got {sorted(row)}",
        ) from None


@dataclass(frozen=True, slots=True)
class Dependent:
    """A repository that depends on some package."""

    owner: str
    repo: str
    stars: int
    version: str
    url: str
    relationship: Relationship

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.repo}'

    @classmethod
    def from_row(cls, row: Row) -> 'Dependent':
        return cls(
            owner=_text(_require(row, 'owner')),
            repo=_text(_require(row, 'repo')),
            stars=_count(_require(row, 'stars')),
            version=_text(_require(row, 'version')),
            url=_text(_require(row, 'url')),
            relationship=as_relationship(_require(row, 'relationship')),
        )


@dataclass(frozen=True, slots=True)
class LibraryCandidate:
    """A package name matching a search, with how many repos use it."""

    name: str
    repository_count: int

    @classmethod
    def from_row(cls, row: Row) -> 'LibraryCandidate':
        return cls(
            name=_text(_require(row, 'name')),
            repository_count=_count(_require(row, 'repository_count')),
        )


@dataclass(frozen=True, slots=True)
class LanguageCount:
    """Repository count for one language."""

    language: str
    repository_count: int

    @classmethod
    def from_row(cls, row: Row) -> 'LanguageCount':
        return cls(
            language=_text(_require(row, 'language')),
            repository_count=_count(_require(row, 'repository_count')),
        )


@dataclass(frozen=True, slots=True)
class PackagePopularity:
    """How many repositories use a package, split by how they got it."""

    name: str
    repository_count: int
    direct_count: int

    @property
    def transitive_count(self) -> int:
        return self.repository_count - self.direct_count

    @classmethod
    def from_row(cls, row: Row) -> 'PackagePopularity':
        return cls(
            name=_text(_require(row, 'name')),
            repository_count=_count(_require(row, 'repository_count')),
            direct_count=_count(_require(row, 'direct_count')),
        )


@dataclass(frozen=True, slots=True)
class VersionObservation:
    """A package version as seen at one point in time."""

    version: str
    repository_count: int
    observed_at: datetime

    @classmethod
    def from_row(cls, row: Row) -> 'VersionObservation':
        return cls(
            version=_text(_require(row, 'version')),
            repository_count=_count(_require(row, 'repository_count')),
            observed_at=_require(row, 'observed_at'),
        )


@dataclass(frozen=True, slots=True)
class AdoptionPoint:
    """How many repositories used a package in one month."""

    month: str
    repository_count: int
    direct_count: int

    @classmethod
    def from_row(cls, row: Row) -> 'AdoptionPoint':
        return cls(
            month=_text(_require(row, 'month')),
            repository_count=_count(_require(row, 'repository_count')),
            direct_count=_count(row.get('direct_count')),
        )


@dataclass(frozen=True, slots=True)
class DatabaseStats:
    """High-level row counts, as shown by `db status`."""

    repositories: int
    artifacts: int
    releases: int

    @classmethod
    def from_row(cls, row: Row) -> 'DatabaseStats':
        return cls(
            repositories=_count(row.get('repositories')),
            artifacts=_count(row.get('artifacts')),
            releases=_count(row.get('releases')),
        )


def model_columns(model: type) -> tuple[str, ...]:
    """Field names of a row model, for building SELECT lists in tests."""
    return tuple(f.name for f in fields(model))
