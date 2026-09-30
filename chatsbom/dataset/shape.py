"""How much one question may ask for, and what a row becomes.

The port of the Worker's `web/src/dataset/shape.ts` (#151 deleted it).
The bounds are its stores' own, so a value clamps to the page it did;
the shaping is what both of its stores did to a row, one copy for the
same reason it had one copy there: the copies drifted (#31).

What is refused outright, a limit of -1 or 2.5, is `params.py`'s. What
is here clamps a value that is only more than anyone gets.
"""
from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import TypeVar

from chatsbom.dataset.types import Answer
from chatsbom.dataset.types import Dependent
from chatsbom.dataset.types import PackageEdge
from chatsbom.dataset.types import TreeEdge
from chatsbom.dataset.types import VersionShare
from chatsbom.dataset.types import VersionSpread
from chatsbom.models.relationship import Relationship
from chatsbom.models.relationship import RELATIONSHIPS

AnswerT = TypeVar('AnswerT', bound=Answer)

#: A row as the snapshot returns it, keyed by the names its statement
#: gave the columns.
Row = Mapping[str, Any]

DEFAULT_LIMIT = 50
MAX_LIMIT = 500

#: The furthest a page may start. ClickHouse binds an offset as UInt32
#: and failed on 1e12 rather than returning the empty page it describes;
#: one bound for every store, and no package has four billion rows.
MAX_OFFSET = 2 ** 32 - 1

#: How wide a drawn tree may get: display bounds, not data bounds. Past
#: roughly this many marks the diagram stops being one; `express` pulls
#: in 31 packages directly and each of those pulls in more.
TREE_CHILDREN = 14
TREE_CHILDREN_MAX = 30
TREE_BRANCH = 4
TREE_BRANCH_MAX = 12


def _clamp(value: int | None, low: int, high: int, fallback: int) -> int:
    """Within [low, high], or the fallback for no number at all."""
    if value is None:
        return fallback
    return min(max(value, low), high)


def bounded_limit(limit: int | None) -> int:
    """Rows to return: the default when none is given or none can be
    used, and never more than the ceiling."""
    if limit is None or limit < 1:
        return DEFAULT_LIMIT
    return min(limit, MAX_LIMIT)


def bounded_offset(offset: int | None) -> int:
    """Rows to skip, for paging: never negative, never past MAX_OFFSET."""
    return _clamp(offset, 0, MAX_OFFSET, 0)


def tree_shape(children: int | None, branch: int | None) -> tuple[int, int]:
    """A tree's first hop, and its second hop per parent.

    Less than one is the smallest tree rather than the default one:
    one child is what was asked for, as near as the diagram can come.
    """
    return (
        _clamp(children, 1, TREE_CHILDREN_MAX, TREE_CHILDREN),
        _clamp(branch, 1, TREE_BRANCH_MAX, TREE_BRANCH),
    )


def num(value: Any) -> int:
    """A count, however the store spelled it; absent is none."""
    return 0 if value is None else int(value)


def text(value: Any) -> str:
    """A text column; absent is empty."""
    return '' if value is None else str(value)


def relationship_of(value: Any) -> Relationship:
    """A relationship, or `unknown` for anything the contract does not
    name: a row is shown, not refused, for an odd value."""
    for relationship in RELATIONSHIPS:
        if value == relationship:
            return relationship
    return 'unknown'


def shape_read(answer: type[AnswerT], row: Row) -> AnswerT:
    """A row of a stored table (`reads.py`): each field from the column
    of its name as the page spells it, a count as a number and the rest
    as text, as `shapeRead` makes them."""
    values: dict[str, Any] = {}
    for field_name, field in answer.model_fields.items():
        column = field.alias or field_name
        value = row.get(column)
        values[column] = num(value) if field.annotation is int else text(value)
    return answer.model_validate(values)


def shape_dependant(row: Row) -> Dependent:
    """A dependants row, from the columns its statement names."""
    manifests = row.get('manifests')
    return Dependent(
        owner=text(row.get('owner')),
        repo=text(row.get('repo')),
        stars=num(row.get('stars')),
        version=text(row.get('version')),
        url=text(row.get('url')),
        language=text(row.get('language')),
        ecosystem=text(row.get('ecosystem')),
        relationship=relationship_of(row.get('relationship')),
        observed_at=text(row.get('observed_on')),
        manifests=num(1 if manifests is None else manifests),
    )


def shape_spread(rows: Sequence[VersionShare], limit: int) -> VersionSpread:
    """The resolved versions, widest first, and what was set aside.

    `rows` are the resolved versions and one row for each other kind,
    counting its repositories once: `constrained` and `unversioned` are
    those counts, of every repository and not only those a list would
    show. A count of repositories is not a sum of counts (#120).
    """
    def set_aside(kind: str) -> int:
        return next(
            (row.repository_count for row in rows if row.kind == kind), 0,
        )

    return VersionSpread(
        versions=[row for row in rows if row.kind == 'resolved'][:limit],
        constrained=set_aside('constraint'),
        unversioned=set_aside('unversioned'),
    )


def shape_edge(row: Row) -> PackageEdge:
    """An edge from the named end: `name` is the package at the other."""
    return PackageEdge(
        name=text(row.get('name')), repositories=num(row.get('repositories')),
    )


def shape_tree_edge(row: Row) -> TreeEdge:
    """A second-hop edge, naming the first-hop package it hangs from."""
    return TreeEdge(
        parent=text(row.get('parent')),
        child=text(row.get('child')),
        repositories=num(row.get('repositories')),
    )
