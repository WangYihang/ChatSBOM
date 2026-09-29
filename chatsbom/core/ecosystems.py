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
    # Syft types a Dart package `dart-pub` (1.41.2 and 1.52.0 alike); its
    # purl, the graph and discovery say `pub`.
    'pub': ('pub', 'dart-pub'),
    'nuget': ('nuget',),
    'swift': ('swift',),
    # Syft types a pod from `Podfile.lock` `pod`; a podspec's rows, and
    # the purl, say `cocoapods`, as discovery does.
    'cocoapods': ('cocoapods', 'pod'),
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


def canonical(artifact_type: str) -> str:
    """The canonical name of a raw `artifacts.type`, as `canonical_sql`."""
    return RENAMES.get(artifact_type, artifact_type)


#: A purl's type -> canonical name, where the two differ.
_PURL_TYPES: dict[str, str] = {'golang': 'go'}


def artifact_ecosystem(artifact_type: str = '', purl: str = '') -> str | None:
    """The ecosystem an artifact belongs to: its type, else its purl's.

    Syft types its artifacts (`java-archive`, `go-module`, `python`),
    and those are canonicalised. An artifact with no type falls back to
    its purl (`pkg:maven/…` is `maven`, `pkg:golang/…` is `go`). None
    when neither says.
    """
    if artifact_type:
        return canonical(artifact_type)
    if purl.startswith('pkg:'):
        kind = purl[4:].split('/', 1)[0].lower()
        if kind:
            return canonical(_PURL_TYPES.get(kind, kind))
    return None


#: The ecosystem a language list used to stand for, for the one release
#: that still accepts a `language` filter where an ecosystem is meant
#: (#55 §4.13), and for the frameworks, which are packages of one
#: ecosystem named by a language (`models/framework.py`). The web
#: client's copy is `LANGUAGE_ECOSYSTEM` in `web/src/ecosystems.ts`.
LANGUAGE_ECOSYSTEM: dict[str, str] = {
    'go': 'go',
    'java': 'maven',
    'javascript': 'npm',
    'php': 'composer',
    'python': 'pypi',
    'ruby': 'gem',
    'rust': 'cargo',
    'typescript': 'npm',
}
