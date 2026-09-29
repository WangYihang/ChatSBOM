"""Which files of a repository are its manifests, read from its tree.

The content stage used to ask for a fixed list of names at the
repository root, chosen by the repository's language (#51). Three
things went missing that way:

- every manifest below the root, so `jeecg-boot/pom.xml`,
  `application/build.gradle` (halo) and `app/client/package.json`
  (appsmith) were never fetched, and 2,300 repositories with manifests
  only in subdirectories got none at all;
- every manifest of another ecosystem, so a repository labelled
  TypeScript with a Java backend (Stirling-PDF, appsmith) was only ever
  searched for `package.json`;
- every repository whose language was not one of the nine lists.

So the list comes from the stored tree (`05-github-tree/<id>/<sha>/
tree.txt`), at any depth, and from one set of names covering every
ecosystem. The repository's language plays no part.

`discover` is a pure function of the tree: the same tree always selects
the same files in the same order, which is what makes a cap
reproducible and the content stage's output key stable.

## What is left out, and why

- **Vendored and generated trees** (`node_modules/`, `vendor/`,
  `target/`, …) hold other projects' manifests. Reading them would
  report every dependency's dependencies as the project's own. Go's
  `vendor/modules.txt` is the exception: Go writes it there on purpose,
  and it lists the vendored modules.
- **Tests, fixtures and benchmarks** declare what a test needs, often a
  deliberately broken or ancient manifest.
- **Examples, samples and demos** only when the repository has
  manifests elsewhere as well (owner decision D5). A repository that
  *is* a collection of examples keeps them: they are all it has.
- `docs/` is never left out. A documentation site is built with the
  project, and its dependencies are the project's.

Every file left out is recorded with its reason, so "why was this
manifest not scanned" is answered by `manifests.json`, not by rerunning
the rules.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from chatsbom.core.gradle import is_build_logic_source

#: Files whose *contents* decide what Syft reports, by exact name, with
#: the ecosystem each belongs to. The canonical ecosystem names are
#: those of `core/ecosystems.py` where one exists.
#:
#: This is the one registry. `sbom_service` keys the Syft cache by it,
#: and it replaced `BaseLanguage.get_sbom_paths()`, whose names are all
#: here: `environment.yml` and the `requirements-dev.txt` spellings were
#: the only ones Syft's list did not already have.
NAME_ECOSYSTEM: dict[str, str] = {
    # Ruby
    'Gemfile': 'gem', 'Gemfile.lock': 'gem', 'gems.locked': 'gem',
    # JavaScript
    'package.json': 'npm', 'package-lock.json': 'npm', 'yarn.lock': 'npm',
    'pnpm-lock.yaml': 'npm', 'npm-shrinkwrap.json': 'npm',
    'bun.lock': 'npm', 'bun.lockb': 'npm',
    # Go
    'go.mod': 'go', 'go.sum': 'go', 'vendor/modules.txt': 'go',
    'Gopkg.toml': 'go', 'Gopkg.lock': 'go',
    'glide.yaml': 'go', 'glide.lock': 'go',
    # Rust
    'Cargo.toml': 'cargo', 'Cargo.lock': 'cargo',
    # Python
    'pyproject.toml': 'pypi', 'poetry.lock': 'pypi', 'uv.lock': 'pypi',
    'Pipfile': 'pypi', 'Pipfile.lock': 'pypi', 'setup.py': 'pypi',
    'setup.cfg': 'pypi', 'pdm.lock': 'pypi',
    'requirements-dev.txt': 'pypi', 'requirements_dev.txt': 'pypi',
    'environment.yml': 'conda',
    # PHP
    'composer.json': 'composer', 'composer.lock': 'composer',
    # JVM. The Gradle build-logic files are read by nothing in Syft,
    # 1.41.2 or 1.52.0; they are fetched because the declared-manifest
    # source (PR D, owner decision D1) resolves `libs.x.y` references
    # and subproject lists against them.
    'pom.xml': 'maven', 'build.gradle': 'maven', 'build.gradle.kts': 'maven',
    'gradle.lockfile': 'maven',
    'settings.gradle': 'maven', 'settings.gradle.kts': 'maven',
    'gradle.properties': 'maven',
    # Elixir, Dart, Swift, CocoaPods, C/C++, R, Haskell
    'mix.exs': 'hex', 'mix.lock': 'hex',
    'pubspec.yaml': 'pub', 'pubspec.lock': 'pub',
    'Package.swift': 'swift', 'Package.resolved': 'swift',
    'Podfile': 'cocoapods', 'Podfile.lock': 'cocoapods',
    'conanfile.txt': 'conan', 'conan.lock': 'conan', 'vcpkg.json': 'vcpkg',
    'DESCRIPTION': 'cran', 'renv.lock': 'cran',
    'cabal.project.freeze': 'hackage', 'stack.yaml.lock': 'hackage',
}

#: Name endings, for families of names. `requirements.txt` covers
#: `dev-requirements.txt` and friends; `.versions.toml` covers Gradle
#: version catalogs, `gradle/libs.versions.toml` and any other.
SUFFIX_ECOSYSTEM: dict[str, str] = {
    '.gemspec': 'gem',
    # A pod's own spec: what a CocoaPods library declares it depends on.
    # Syft reads only `Podfile.lock`; the declared-manifest source reads
    # these (`core/podspec.py`), as it reads Gradle builds.
    '.podspec': 'cocoapods',
    '.podspec.json': 'cocoapods',
    'requirements.txt': 'pypi',
    '.csproj': 'nuget',
    '.fsproj': 'nuget',
    '.versions.toml': 'maven',
}

MANIFEST_NAMES: frozenset[str] = frozenset(NAME_ECOSYSTEM)
MANIFEST_SUFFIXES: tuple[str, ...] = tuple(SUFFIX_ECOSYSTEM)

#: Lockfiles: what a project pins. Ordered before manifests at the same
#: depth, so that a cap never keeps a manifest and drops the lockfile
#: that says what it resolved to.
LOCKFILE_NAMES: frozenset[str] = frozenset({
    'Gemfile.lock', 'gems.locked',
    'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml',
    'npm-shrinkwrap.json', 'bun.lock', 'bun.lockb',
    'go.sum', 'vendor/modules.txt', 'Gopkg.lock', 'glide.lock',
    'Cargo.lock',
    'poetry.lock', 'uv.lock', 'Pipfile.lock', 'pdm.lock',
    'composer.lock',
    'gradle.lockfile',
    'mix.lock', 'pubspec.lock', 'Package.resolved', 'Podfile.lock',
    'conan.lock', 'renv.lock', 'cabal.project.freeze', 'stack.yaml.lock',
})

#: Other projects' trees: installed, vendored or built. Their manifests
#: describe dependencies, not this project. `manifest.VENDOR_DIRS` is
#: this set, so the classifier and discovery agree on what is vendored.
VENDORED_DIRS: frozenset[str] = frozenset({
    'node_modules', 'vendor', 'third_party', 'third-party',
    '.venv', 'venv', 'site-packages', 'bower_components', 'Pods',
    '.yarn', 'dist', 'target', 'build',
})

#: What a test or a benchmark needs, not what the project ships.
TEST_DIRS: frozenset[str] = frozenset({
    'test', 'tests', '__tests__', 'testdata', 'test-data',
    'fixtures', '__fixtures__',
    'benchmark', 'benchmarks', 'e2e',
})

#: Left out only when the repository has manifests outside them as well
#: (owner decision D5, 2026-09-28).
EXAMPLE_DIRS: frozenset[str] = frozenset({
    'example', 'examples', 'sample', 'samples', 'demo', 'demos',
})

#: Always left out, wherever they appear in a path. Case-sensitive: a
#: `Test/` directory is somebody's module, not a test suite, often
#: enough that guessing would lose more than it saves.
EXCLUDED_DIRS: frozenset[str] = VENDORED_DIRS | TEST_DIRS

#: `buildSrc` sources are fetched for the constants a Gradle build names
#: its dependencies by (`deps.x.y`, `core/gradle.py`), and at most this
#: many: a `buildSrc` of convention plugins can be hundreds of files, and
#: they are not manifests to crowd out. Shallowest, then by name.
MAX_BUILD_LOGIC_SOURCES = 20

#: Per-repository caps (design §4.4). The file cap is applied here; the
#: byte cap needs sizes, which the tree does not have, so the content
#: stage applies it while downloading, in this same order.
MAX_FILES = 200
MAX_BYTES = 64 * 2**20

#: Why a file of the tree was not selected.
EXCLUDED_DIR = 'excluded-dir'
EXAMPLE_DIR = 'example-dir'
OVER_FILE_CAP = 'over-file-cap'
OVER_BUILD_LOGIC_CAP = 'over-build-logic-cap'
OVER_BYTE_CAP = 'over-byte-cap'
UNSAFE_PATH = 'unsafe-path'

#: The version of `manifests.json`'s shape.
DISCOVERY_FORMAT = 1


def ecosystem_of(path: str) -> str | None:
    """The ecosystem a repository path is a manifest of, or None."""
    if path == 'vendor/modules.txt' or path.endswith('/vendor/modules.txt'):
        return 'go'
    if is_build_logic_source(path):
        return 'maven'
    name = path.rpartition('/')[2]
    found = NAME_ECOSYSTEM.get(name)
    if found is not None:
        return found
    for suffix, ecosystem in SUFFIX_ECOSYSTEM.items():
        if name.endswith(suffix):
            return ecosystem
    return None


def is_lockfile(path: str) -> bool:
    if path == 'vendor/modules.txt' or path.endswith('/vendor/modules.txt'):
        return True
    return path.rpartition('/')[2] in LOCKFILE_NAMES


def _directories(path: str) -> list[str]:
    """The directory segments of `path`, the vendored-modules exception
    taken out: `vendor/` holding `modules.txt` is Go's own record."""
    segments = path.split('/')[:-1]
    if path == 'vendor/modules.txt' or path.endswith('/vendor/modules.txt'):
        segments = segments[:-1]
    return segments


def unquote_git_path(line: str) -> str:
    """A path as `git ls-tree` printed it, unquoted.

    Git quotes a path with a special or non-ASCII character in C style,
    `"caf\\303\\251/package.json"`, unless told not to. The stored trees
    were listed without `-z`, so a manifest under such a directory is
    stored that way and must be unquoted to be fetched.
    """
    if len(line) < 2 or not (line.startswith('"') and line.endswith('"')):
        return line
    body = line[1:-1]
    out = bytearray()
    escapes = {
        'a': 7, 'b': 8, 't': 9, 'n': 10, 'v': 11, 'f': 12, 'r': 13,
        '"': 34, '\\': 92,
    }
    index = 0
    while index < len(body):
        char = body[index]
        if char != '\\' or index + 1 >= len(body):
            out.extend(char.encode('utf-8'))
            index += 1
            continue
        following = body[index + 1]
        if following in escapes:
            out.append(escapes[following])
            index += 2
        elif body[index + 1:index + 4].isdigit() and len(body) >= index + 4:
            out.append(int(body[index + 1:index + 4], 8) & 0xFF)
            index += 4
        else:
            out.extend(char.encode('utf-8'))
            index += 1
    return out.decode('utf-8', errors='replace')


def is_safe_path(path: str) -> bool:
    """Whether `path` can be joined under a content root as it is.

    Git itself never records `..`, `.git` or an absolute path, but the
    tree is a file on disk and the result is used to build a path to
    write to, so it is checked rather than trusted.
    """
    if not path or path.startswith('/') or '\0' in path or '\\' in path:
        return False
    segments = path.split('/')
    return all(
        segment not in ('', '.', '..') and segment.lower() != '.git'
        for segment in segments
    )


@dataclass(frozen=True, slots=True)
class Selected:
    """One file the content stage should fetch."""

    path: str
    ecosystem: str
    lockfile: bool


@dataclass(slots=True)
class Discovery:
    """What a tree offered, what was selected and what was left out."""

    #: In fetch order: depth, then lockfiles, then name, then path.
    selected: list[Selected] = field(default_factory=list)
    #: `(path, reason)`, in tree order within each reason.
    skipped: list[tuple[str, str]] = field(default_factory=list)
    #: Manifests the tree has, before any rule; the cap's denominator.
    candidates: int = 0

    @property
    def ecosystems(self) -> list[str]:
        return sorted({item.ecosystem for item in self.selected})

    @property
    def paths(self) -> list[str]:
        return [item.path for item in self.selected]

    @property
    def only_below_root(self) -> bool:
        """Whether every selected file is in a subdirectory: the #51 gap."""
        return bool(self.selected) and all(
            '/' in item.path for item in self.selected
        )

    def skipped_for(self, reason: str) -> list[str]:
        return [path for path, why in self.skipped if why == reason]


def _order(path: str) -> tuple[int, int, str, str]:
    return (
        path.count('/'),
        0 if is_lockfile(path) else 1,
        path.rpartition('/')[2],
        path,
    )


def discover(
    tree_paths: Iterable[str],
    *,
    max_files: int = MAX_FILES,
) -> Discovery:
    """Choose a repository's manifests from its tree.

    `tree_paths` are the lines of `tree.txt`, quoted as git printed them
    or not. Pure: no filesystem, no network.
    """
    result = Discovery()
    kept: list[str] = []
    examples: list[str] = []
    seen: set[str] = set()
    for raw in tree_paths:
        path = unquote_git_path(raw.strip('\n'))
        if not path or path in seen:
            continue
        seen.add(path)
        if ecosystem_of(path) is None:
            continue
        result.candidates += 1
        if not is_safe_path(path):
            result.skipped.append((path, UNSAFE_PATH))
            continue
        directories = _directories(path)
        if any(segment in EXCLUDED_DIRS for segment in directories):
            result.skipped.append((path, EXCLUDED_DIR))
        elif any(segment in EXAMPLE_DIRS for segment in directories):
            examples.append(path)
        else:
            kept.append(path)

    # D5: a repository with manifests of its own skips its examples; one
    # whose only manifests are examples keeps them, since that is all it
    # has.
    if kept:
        result.skipped.extend((path, EXAMPLE_DIR) for path in examples)
    else:
        kept = examples

    kept.sort(key=_order)
    logic = [path for path in kept if is_build_logic_source(path)]
    if len(logic) > MAX_BUILD_LOGIC_SOURCES:
        over = set(logic[MAX_BUILD_LOGIC_SOURCES:])
        result.skipped.extend(
            (path, OVER_BUILD_LOGIC_CAP) for path in logic
            if path in over
        )
        kept = [path for path in kept if path not in over]
    for path in kept[max_files:]:
        result.skipped.append((path, OVER_FILE_CAP))
    for path in kept[:max_files]:
        ecosystem = ecosystem_of(path)
        assert ecosystem is not None
        result.selected.append(Selected(path, ecosystem, is_lockfile(path)))
    return result


def read_tree(text: str) -> list[str]:
    """The paths of a stored `tree.txt`."""
    return [line for line in text.splitlines() if line.strip()]


def content_digest(written: Iterable[tuple[str, int]]) -> str:
    """The content stage's output key: sha256 of the sorted
    `(path, size)` list actually on disk.

    Sizes rather than bytes, as for the Syft cache key of non-manifests:
    the files are immutable at a commit, so a new path or a new size is
    what a change looks like, and reading every byte back to hash it
    would double the stage's disk reads.
    """
    hasher = hashlib.sha256()
    for path, size in sorted(written):
        hasher.update(f'{path}\0{size}\n'.encode())
    return hasher.hexdigest()


def discovery_document(
    discovery: Discovery,
    *,
    repository_id: int,
    commit_sha: str,
    fetched: Mapping[str, Mapping[str, Any]] | None = None,
    extra_skipped: Iterable[tuple[str, str]] = (),
    max_files: int = MAX_FILES,
    max_bytes: int = MAX_BYTES,
    max_file_bytes: int | None = None,
) -> dict[str, Any]:
    """`manifests.json`: the discovery list and what became of it.

    `fetched` maps a selected path to what the download found
    (`status`, `size`); `extra_skipped` adds what the download left out
    (the byte caps).
    """
    fetched = fetched or {}
    selected = []
    for item in discovery.selected:
        entry: dict[str, Any] = {
            'path': item.path,
            'ecosystem': item.ecosystem,
            'lockfile': item.lockfile,
        }
        entry.update(fetched.get(item.path, {}))
        selected.append(entry)
    skipped = [
        {'path': path, 'reason': reason}
        for path, reason in (*discovery.skipped, *extra_skipped)
    ]
    reasons: dict[str, int] = {}
    for entry in skipped:
        reasons[entry['reason']] = reasons.get(entry['reason'], 0) + 1
    return {
        'format': DISCOVERY_FORMAT,
        'repository_id': repository_id,
        'commit_sha': commit_sha,
        'limits': {
            'max_files': max_files,
            'max_bytes': max_bytes,
            'max_file_bytes': max_file_bytes,
        },
        'candidates': discovery.candidates,
        'ecosystems': discovery.ecosystems,
        'selected': selected,
        'skipped': skipped,
        'skipped_by_reason': dict(sorted(reasons.items())),
    }


def dumps(document: Mapping[str, Any]) -> str:
    return json.dumps(document, indent=1, sort_keys=False) + '\n'
