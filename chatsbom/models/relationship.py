"""How a dependency reached a project.

A lockfile-derived SBOM lists the whole resolved closure. `direct` means
the project's manifest asks for the package; `transitive` means it was
pulled in by something else; `unknown` means no manifest was readable, so
the question is unanswered rather than answered "no".

Modelled as a Literal so mypy rejects a typo at the call site instead of
ClickHouse silently storing it in a LowCardinality(String).
"""
from typing import Any
from typing import get_args
from typing import Literal
from typing import TypeAlias
from typing import TypeGuard

Relationship: TypeAlias = Literal['direct', 'transitive', 'unknown']

DIRECT: Relationship = 'direct'
TRANSITIVE: Relationship = 'transitive'
UNKNOWN: Relationship = 'unknown'

#: Every member of `Relationship`, derived from the type itself so the
#: two cannot drift apart.
RELATIONSHIPS: tuple[Relationship, ...] = get_args(Relationship)


def is_relationship(value: Any) -> TypeGuard[Relationship]:
    """Narrow an arbitrary value to `Relationship` for the type checker."""
    return isinstance(value, str) and value in RELATIONSHIPS


def as_relationship(value: Any) -> Relationship:
    """Validate a value read from outside the process (DB, JSON, CLI)."""
    if is_relationship(value):
        return value
    raise ValueError(
        f"invalid relationship {value!r}; expected one of "
        f"{', '.join(RELATIONSHIPS)}",
    )
