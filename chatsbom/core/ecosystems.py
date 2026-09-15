"""One name per ecosystem, because the two collectors disagree.

Syft and GitHub's dependency graph label the same registry
differently, and counting the spellings separately makes one ecosystem
look like two. That was visible on the dashboard's ecosystem filter —
`laravel/framework` offered `composer · 183` and `php-composer · 97` as
though they were different choices — and it was quietly inflating a
much more important number:

    cross-ecosystem names   39,658 (17.6%)  ->   2,730 (1.2%)
    edges affected         316,546 (51.5%)  ->  63,384 (10.3%)

93% of the "ambiguity" the edge panels warned about was this table
missing. The warning said more than half the edges might merge two
registries; the truth is about a tenth.

`web/src/ecosystems.ts` holds the same mapping for the browser, and
`tests/ecosystems_test.py` fails if the two drift apart. Two copies is
worse than one, and a generator for five lines of data would be worse
than either — so the check is the thing that keeps them honest.
"""
from __future__ import annotations

#: Canonical name -> the raw `artifacts.type` values it covers.
#:
#: The canonical name is the one a person recognises from the registry
#: — `cargo`, not `rust-crate`; `pypi`, not `python` — because it is
#: what the interface shows.
MEMBERS: dict[str, tuple[str, ...]] = {
    'npm': ('npm',),
    'cargo': ('cargo', 'rust-crate'),
    'pypi': ('pypi', 'python'),
    'go': ('golang', 'go-module'),
    'maven': ('maven', 'java-archive'),
    'gem': ('gem',),
    'composer': ('composer', 'php-composer'),
    'pub': ('pub',),
    'nuget': ('nuget',),
    'swift': ('swift',),
    'deb': ('deb',),
    'jenkins-plugin': ('jenkins-plugin',),
    'swid': ('swid',),
}

#: Raw value -> canonical name, for the pairs that actually differ.
#: A type equal to its own canonical name needs no entry.
RENAMES: dict[str, str] = {
    raw: canonical
    for canonical, members in MEMBERS.items()
    for raw in members
    if raw != canonical
}


def canonical_sql(column: str = 'type') -> str:
    """A ClickHouse expression mapping a raw type to its canonical name.

    `transform` with a trailing default, so a type this table has never
    seen passes through as itself rather than becoming an empty string.
    A new ecosystem should read as itself in the interface, and that is
    also the signal this table needs a line adding.
    """
    if not RENAMES:
        return column
    sources = ', '.join(f"'{raw}'" for raw in RENAMES)
    targets = ', '.join(f"'{name}'" for name in RENAMES.values())
    return f'transform({column}, [{sources}], [{targets}], {column})'
