"""What a Gradle build declares, read from its text (owner decision D1).

Syft reads nothing from `build.gradle`, `build.gradle.kts`,
`settings.gradle(.kts)` or a version catalog (`gradle/libs.versions.toml`),
1.41.2 or 1.52.0, and GitHub's dependency graph is partial for Gradle:
for halo-dev/halo it lists 105 packages and no Spring starter (#51). A
Gradle-only repository therefore had no row saying it uses Spring Boot,
from either source.

This module reads those files as text and says which Maven coordinates
the build declares. The warehouse stores each as an artifact row with
`source = 'manifest'` (`manifest_rows`), and the classifier reads the
same declarations to decide which Syft rows are direct (`core/manifest`).

Nothing here runs Gradle. A build script is a program, so what can be
read from its text is what it states literally, and this is exactly
what is read:

- **Dependency declarations** in any configuration (`implementation`,
  `api`, `compileOnly`, `runtimeOnly`, `annotationProcessor`, `kapt`,
  `ksp`, `developmentOnly`, the `test…`/`<sourceSet>…` variants, and the
  old `compile`/`runtime`), Groovy or Kotlin DSL:
  - string coordinates, `'group:name:version'`, with `$name` and
    `${name}` substituted from `gradle.properties` and literal
    assignments (`ext { x = '1' }`, `ext.x = '1'`, `val x = "1"`,
    `extra["x"] = "1"`, `set('x', '1')`) anywhere in the build;
  - map notation, `group: 'g', name: 'n', version: 'v'`;
  - `kotlin("x")`, which is `org.jetbrains.kotlin:kotlin-x`;
  - `platform(…)`, `enforcedPlatform(…)` and `testFixtures(…)` around
    any of these;
  - **version-catalog references**, `libs.spring.boot.starter.web` and
    `libs.bundles.x`, resolved against every `*.versions.toml` in the
    repository (`gradle/libs.versions.toml` is `libs`; any other file
    is named by its stem) and against catalogs declared inline in
    `settings.gradle(.kts)` (`library('alias', 'g:n:v')`,
    `library("alias", "g", "n").version("v")`, `version('x', '1')`).
    Gradle turns `-`, `_` and `.` in an alias into `.` in the accessor,
    and so is the lookup.
  - **constants a build names its coordinates by**, the idiom before
    version catalogs: `deps.android.support.appCompat` or `Libs.retrofit`
    is a string constant in `buildSrc/src/main/{kotlin,java,groovy}`
    (`object deps { object android { … const val appCompat =
    "g:n:${versions.support}" } }`, or Java's `static final String`),
    its templates resolved against its enclosing objects; and a Groovy
    map in the build itself, `ext.deps = [okhttp: 'g:n:1']`, named as
    `deps.okhttp`, `deps['okhttp']` or `rootProject.ext.deps.okhttp`.
    ZacSweers/CatchUp declares every dependency the first way, and had
    no row from any source (#55 pilot).
- **Versions a declaration leaves out**, filled from:
  - a `constraints { … }` block or a Spring `dependencyManagement {
    dependencies { dependency 'g:n:v' } }` anywhere in the build, as a
    `java-platform` subproject pins them (halo's `platform/`);
  - the Spring Boot version, for `org.springframework.boot` artifacts,
    which are all released together: from the `org.springframework.boot`
    plugin (`id … version`, or a catalog `[plugins]` entry), the
    `spring-boot-gradle-plugin` on the buildscript classpath, a
    `spring-boot-dependencies` BOM coordinate, or a `springBootVersion`
    property. `SpringBootPlugin.BOM_COORDINATES` is that BOM.

What is **not** resolved, and why:

- **Plugins** (`plugins { id … }`, `alias(libs.plugins.x)`,
  `apply plugin:`) are build tooling, not dependencies of the project.
  They produce no rows; the Spring Boot plugin's version is read only to
  date the starters.
- **The buildscript classpath** (`classpath …`) is the build's own, for
  the same reason, and so is everything under `buildSrc/`,
  `build-logic/` and `build-conventions/`.
- **Dynamic code.** A declaration built in a loop (`[…].each {
  implementation "x:$it" }`), inside a method, by a convention plugin
  from `buildSrc`, by `apply from: 'other.gradle'` or from a value
  computed at build time is not seen. A reference this cannot follow
  (a constant a function builds, `"$group:web"`) makes the file
  *incomplete* (`Declaration.complete` in `core/manifest`), so the
  classifier does not call anything else in the ecosystem transitive.
- **What BOMs manage.** A version a BOM supplies is not read out of the
  BOM, which is not in the repository; such a row is unversioned,
  except for Spring Boot's own artifacts as above.
- **Resolution.** Gradle's conflict resolution may pick another
  version than the one declared, and a declaration may be excluded or
  substituted later in the script. Every row is therefore a *declared*
  version, never a resolved one: `version_kind` is `constraint` when
  it has a version and `unversioned` when not, never `resolved`.

Unversioned rows and rows whose version was filled in say so in
`found_by` (`MANIFEST_FOUND_BY`).
"""
from __future__ import annotations

import re
import tomllib
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import Any
from urllib.parse import quote

#: File names this module reads.
BUILD_FILES: frozenset[str] = frozenset({'build.gradle', 'build.gradle.kts'})
SETTINGS_FILES: frozenset[str] = frozenset({
    'settings.gradle', 'settings.gradle.kts',
})
PROPERTIES_FILE = 'gradle.properties'
CATALOG_SUFFIX = '.versions.toml'
#: The catalog Gradle names `libs` by default.
DEFAULT_CATALOG = 'libs'

#: Directories whose builds are the build's own logic, not the product.
BUILD_LOGIC_DIRS: frozenset[str] = frozenset({
    'buildSrc', 'build-logic', 'build-conventions',
})

#: Where a build's own sources are, which may hold the constants its
#: build files name their dependencies by (`buildSrc/src/main/kotlin/
#: Deps.kt`). Only `buildSrc`'s: `build-logic` is an included build of
#: plugins, whose constants a build script does not see.
BUILD_LOGIC_SOURCE_RE = re.compile(
    r'(?:^|/)buildSrc/src/main/(?:kotlin|java|groovy)/(?:[^/]+/)*'
    r'[^/]+\.(?:kt|java|groovy)$',
)

SPRING_BOOT_GROUP = 'org.springframework.boot'
SPRING_BOOT_PLUGIN = 'org.springframework.boot'
SPRING_BOOT_BOM = 'spring-boot-dependencies'
SPRING_BOOT_GRADLE_PLUGIN = 'spring-boot-gradle-plugin'

#: How a declaration was read, for `found_by`: the literal text of a
#: build file, or a version-catalog entry it named.
VIA_LITERAL = 'literal'
VIA_CATALOG = 'catalog'
#: A constant, in `buildSrc` or a map the build assigns.
VIA_CONSTANT = 'constant'

#: Where the version came from, when the declaration did not state it.
VERSION_DECLARED = 'declared'
VERSION_CONSTRAINT = 'constraint'
VERSION_SPRING_BOOT = 'spring-boot'
VERSION_NONE = ''


def is_build_file(path: str) -> bool:
    return path.rpartition('/')[2] in BUILD_FILES


def is_build_logic(path: str) -> bool:
    return any(part in BUILD_LOGIC_DIRS for part in path.split('/')[:-1])


def is_build_logic_source(path: str) -> bool:
    """A `buildSrc` source file, which may hold coordinate constants."""
    return BUILD_LOGIC_SOURCE_RE.search(path) is not None


def is_gradle_input(path: str) -> bool:
    """Whether `path` is a file this module reads."""
    name = path.rpartition('/')[2]
    return (
        name in BUILD_FILES or name in SETTINGS_FILES
        or name == PROPERTIES_FILE or name.endswith(CATALOG_SUFFIX)
        or is_build_logic_source(path)
    )


# --- the text ---------------------------------------------------------------

_TRIPLE = ('"""', "'''")


def _mask(text: str) -> tuple[str, str]:
    """The text with comments blanked, and again with strings blanked too.

    Both keep every character's offset, so a match in one is a match in
    the other: the structure (braces, statement starts) is read from the
    second, where a `{` or a `//` inside a string cannot mislead it, and
    the values from the first.
    """
    code = list(text)
    masked = list(text)
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith('//', i):
            end = text.find('\n', i)
            end = n if end < 0 else end
            for j in range(i, end):
                code[j] = masked[j] = ' '
            i = end
        elif text.startswith('/*', i):
            end = text.find('*/', i + 2)
            end = n if end < 0 else end + 2
            for j in range(i, end):
                if text[j] != '\n':
                    code[j] = masked[j] = ' '
            i = end
        elif text.startswith(_TRIPLE, i):
            quote_ = text[i:i + 3]
            end = text.find(quote_, i + 3)
            end = n if end < 0 else end + 3
            for j in range(i + 3, max(i + 3, end - 3)):
                if text[j] != '\n':
                    masked[j] = ' '
            i = end
        elif c in ('"', "'"):
            j = i + 1
            while j < n and text[j] != c and text[j] != '\n':
                j += 2 if text[j] == '\\' else 1
            for k in range(i + 1, min(j, n)):
                masked[k] = ' '
            i = j + 1
        else:
            i += 1
    return ''.join(code), ''.join(masked)


def _block_ranges(masked: str, names: Iterable[str]) -> list[tuple[int, int]]:
    """`(start, end)` of every `name { … }` block, braces matched."""
    pattern = re.compile(
        r'(?<![\w.])(?:' + '|'.join(re.escape(n) for n in names) +
        r')\s*(?:\([^()]*\)\s*)?\{',
    )
    ranges = []
    for match in pattern.finditer(masked):
        depth, i = 0, match.end() - 1
        while i < len(masked):
            if masked[i] == '{':
                depth += 1
            elif masked[i] == '}':
                depth -= 1
                if depth == 0:
                    break
            i += 1
        ranges.append((match.start(), i + 1))
    return ranges


def _inside(offset: int, ranges: Iterable[tuple[int, int]]) -> bool:
    return any(start <= offset < end for start, end in ranges)


# --- properties -------------------------------------------------------------

_ASSIGNMENT_RE = re.compile(
    r'^[ \t]*(?:ext\.|project\.ext\.|rootProject\.ext\.|extra\.|'
    r'def[ \t]+|val[ \t]+|var[ \t]+|String[ \t]+)?'
    r'(?P<name>[A-Za-z_][\w.]*)[ \t]*=[ \t]*'
    r'(?P<q>[\'"])(?P<value>[^\'"$\n]*)(?P=q)[ \t]*;?[ \t]*$',
    re.MULTILINE,
)
_SET_RE = re.compile(
    r'''\bset\(\s*(['"])(?P<name>[\w.]+)\1\s*,\s*(['"])(?P<value>[^'"$\n]*)\3''',
)
_EXTRA_RE = re.compile(
    r'''\bextra\[\s*(['"])(?P<name>[\w.]+)\1\s*\]\s*=\s*(['"])'''
    r'''(?P<value>[^'"$\n]*)\3''',
)
_BY_EXTRA_RE = re.compile(
    r'''\b(?:val|var)\s+(?P<name>\w+)\s+by\s+extra\(\s*(['"])'''
    r'''(?P<value>[^'"$\n]*)\2''',
)


def properties_of(text: str) -> dict[str, str]:
    """Literal assignments in a build script, name -> value."""
    code, _ = _mask(text)
    found: dict[str, str] = {}
    for regex in (_ASSIGNMENT_RE, _SET_RE, _EXTRA_RE, _BY_EXTRA_RE):
        for match in regex.finditer(code):
            found.setdefault(match.group('name'), match.group('value'))
    return found


def parse_properties(text: str) -> dict[str, str]:
    """`gradle.properties`: `key=value` or `key: value` lines."""
    found: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(('#', '!')):
            continue
        match = re.match(r'([^=:\s]+)\s*[=:]\s*(.*)$', line)
        if match:
            found.setdefault(match.group(1), match.group(2).strip())
    return found


# --- version catalogs -------------------------------------------------------

@dataclass(frozen=True)
class Coordinate:
    group: str
    name: str
    version: str = ''

    @property
    def module(self) -> str:
        return f'{self.group}:{self.name}'


@dataclass
class Catalog:
    """One version catalog: its libraries, bundles and plugins."""

    #: accessor key (`_accessor`) -> coordinate.
    libraries: dict[str, Coordinate] = field(default_factory=dict)
    #: accessor key -> the accessor keys of its libraries.
    bundles: dict[str, list[str]] = field(default_factory=dict)
    #: accessor key -> (plugin id, version).
    plugins: dict[str, tuple[str, str]] = field(default_factory=dict)
    versions: dict[str, str] = field(default_factory=dict)

    def merge(self, other: Catalog) -> Catalog:
        """Add what `other` has and this does not; this wins a conflict."""
        self.libraries = {**other.libraries, **self.libraries}
        self.bundles = {**other.bundles, **self.bundles}
        self.plugins = {**other.plugins, **self.plugins}
        self.versions = {**other.versions, **self.versions}
        return self


def _accessor(alias: str) -> str:
    """How Gradle spells an alias as an accessor, compared loosely.

    `spring-boot-starter-web`, `spring_boot_starter_web` and
    `spring.boot.starter.web` are all `libs.spring.boot.starter.web`.
    """
    return re.sub(r'[-_.]+', '.', alias).lower()


def _version_of(value: Any, versions: Mapping[str, str]) -> str:
    """A catalog entry's version: a string, `{ref = …}`, or a rich one."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if isinstance(value.get('ref'), str):
            return versions.get(value['ref'], '')
        for key in ('strictly', 'require', 'prefer'):
            if isinstance(value.get(key), str):
                return value[key]
    return ''


def _coordinate(text: str) -> Coordinate | None:
    parts = text.strip().split(':')
    if len(parts) < 2 or not parts[0] or not parts[1]:
        return None
    return Coordinate(parts[0], parts[1], parts[2] if len(parts) > 2 else '')


def parse_catalog(text: str) -> Catalog | None:
    """A `*.versions.toml` file, or None when it is not TOML."""
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return None
    catalog = Catalog()
    raw_versions = data.get('versions')
    if isinstance(raw_versions, dict):
        for key, value in raw_versions.items():
            version = _version_of(value, {})
            if version:
                catalog.versions[key] = version

    libraries = data.get('libraries')
    if isinstance(libraries, dict):
        for alias, value in libraries.items():
            found = _catalog_library(value, catalog.versions)
            if found is not None:
                catalog.libraries[_accessor(alias)] = found

    bundles = data.get('bundles')
    if isinstance(bundles, dict):
        for alias, members in bundles.items():
            if isinstance(members, list):
                catalog.bundles[_accessor(alias)] = [
                    _accessor(m) for m in members if isinstance(m, str)
                ]

    plugins = data.get('plugins')
    if isinstance(plugins, dict):
        for alias, value in plugins.items():
            plugin = _catalog_plugin(value, catalog.versions)
            if plugin is not None:
                catalog.plugins[_accessor(alias)] = plugin
    return catalog


def _catalog_library(value: Any, versions: Mapping[str, str]) -> Coordinate | None:
    if isinstance(value, str):
        return _coordinate(value)
    if not isinstance(value, dict):
        return None
    version = ''
    if 'version' in value:
        version = _version_of(value['version'], versions)
    if isinstance(value.get('module'), str):
        found = _coordinate(value['module'])
        return replace(found, version=version) if found else None
    group, name = value.get('group'), value.get('name')
    if isinstance(group, str) and isinstance(name, str) and group and name:
        return Coordinate(group, name, version)
    return None


def _catalog_plugin(value: Any, versions: Mapping[str, str]) -> tuple[str, str] | None:
    if isinstance(value, str):
        plugin_id, _, version = value.partition(':')
        return (plugin_id, version) if plugin_id else None
    if isinstance(value, dict) and isinstance(value.get('id'), str):
        return value['id'], _version_of(value.get('version'), versions)
    return None


_SETTINGS_CATALOG_RE = re.compile(r'(?<![\w.])(?P<name>\w+)\s*\{')
_SETTINGS_VERSION_RE = re.compile(
    r'''\bversion\(\s*(['"])(?P<alias>[^'"]+)\1\s*,\s*(['"])(?P<value>[^'"]*)\3''',
)
_SETTINGS_LIBRARY_RE = re.compile(
    r'''\blibrary\(\s*(['"])(?P<alias>[^'"]+)\1\s*,\s*(['"])(?P<first>[^'"]+)\3'''
    r'''(?:\s*,\s*(['"])(?P<second>[^'"]+)\5)?\s*\)'''
    r'''(?P<rest>(?:\s*\.\s*(?:version|versionRef)\(\s*(?:['"][^'"]*['"])\s*\))?)''',
)
_SETTINGS_BUNDLE_RE = re.compile(
    r'''\bbundle\(\s*(['"])(?P<alias>[^'"]+)\1\s*,\s*(?:listOf\(|\[)(?P<members>[^\])]*)''',
)
_SETTINGS_PLUGIN_RE = re.compile(
    r'''\bplugin\(\s*(['"])(?P<alias>[^'"]+)\1\s*,\s*(['"])(?P<id>[^'"]+)\3\s*\)'''
    r'''(?:\s*\.\s*version\(\s*(['"])(?P<version>[^'"]*)\5\s*\))?''',
)


def settings_catalogs(text: str) -> dict[str, Catalog]:
    """Catalogs declared inline in `settings.gradle(.kts)`.

    `dependencyResolutionManagement { versionCatalogs { libs { … } } }`,
    where each named block is one catalog.
    """
    code, masked = _mask(text)
    catalogs: dict[str, Catalog] = {}
    for start, end in _block_ranges(masked, ['versionCatalogs']):
        inner_masked = masked[start:end]
        inner_code = code[start:end]
        opening = inner_masked.find('{')
        for match in _SETTINGS_CATALOG_RE.finditer(inner_masked, opening + 1):
            name = match.group('name')
            if name in ('create', 'register'):
                continue
            depth, i = 0, match.end() - 1
            while i < len(inner_masked):
                if inner_masked[i] == '{':
                    depth += 1
                elif inner_masked[i] == '}':
                    depth -= 1
                    if depth == 0:
                        break
                i += 1
            body = inner_code[match.end():i]
            catalogs.setdefault(name, Catalog()).merge(_settings_catalog(body))
    # Kotlin: `create("libs") { … }`.
    for match in re.finditer(
        r'''\b(?:create|register)\(\s*(['"])(?P<name>\w+)\1\s*\)\s*\{''', code,
    ):
        depth, i = 0, match.end() - 1
        while i < len(masked):
            if masked[i] == '{':
                depth += 1
            elif masked[i] == '}':
                depth -= 1
                if depth == 0:
                    break
            i += 1
        catalogs.setdefault(match.group('name'), Catalog()).merge(
            _settings_catalog(code[match.end():i]),
        )
    return {k: v for k, v in catalogs.items() if v.libraries or v.plugins}


def _settings_catalog(body: str) -> Catalog:
    catalog = Catalog()
    for match in _SETTINGS_VERSION_RE.finditer(body):
        catalog.versions.setdefault(match.group('alias'), match.group('value'))
    for match in _SETTINGS_LIBRARY_RE.finditer(body):
        first, second = match.group('first'), match.group('second')
        found = (
            _coordinate(f'{first}:{second}') if second else _coordinate(first)
        )
        if found is None:
            continue
        rest = match.group('rest') or ''
        version = re.search(r'''\(\s*['"]([^'"]*)['"]''', rest)
        if version:
            value = version.group(1)
            if 'versionRef' in rest:
                value = catalog.versions.get(value, '')
            found = replace(found, version=value)
        catalog.libraries.setdefault(_accessor(match.group('alias')), found)
    for match in _SETTINGS_BUNDLE_RE.finditer(body):
        members = re.findall(r'''['"]([^'"]+)['"]''', match.group('members'))
        catalog.bundles.setdefault(
            _accessor(match.group('alias')), [_accessor(m) for m in members],
        )
    for match in _SETTINGS_PLUGIN_RE.finditer(body):
        catalog.plugins.setdefault(
            _accessor(match.group('alias')),
            (match.group('id'), match.group('version') or ''),
        )
    return catalog


# --- constants ---------------------------------------------------------------

_SCOPE_RE = re.compile(
    r'\b(?:(?:data|enum|sealed|internal|private|public|abstract|final|'
    r'static)\s+)*(?:object|class|interface)\s+(?P<name>\w+)[^{};=]*\{',
)
_KOTLIN_CONSTANT_RE = re.compile(
    r'\b(?:const\s+)?va[lr]\s+(?P<name>\w+)\s*(?::\s*String\s*)?=\s*'
    r'"(?P<value>[^"\n]*)"',
)
_JAVA_CONSTANT_RE = re.compile(
    r'''\b(?:String|def)\s+(?P<name>\w+)\s*=\s*(?P<q>['"])(?P<value>[^'"\n]*)(?P=q)''',
)
_TEMPLATE_RE = re.compile(r'\$\{(?P<braced>[\w.]+)\}|\$(?P<bare>[A-Za-z_]\w*)')


def _scopes(masked: str) -> list[tuple[int, int, str]]:
    """`(body start, body end, name)` of every named class, object or
    interface."""
    found = []
    for match in _SCOPE_RE.finditer(masked):
        depth, i = 0, match.end() - 1
        while i < len(masked):
            if masked[i] == '{':
                depth += 1
            elif masked[i] == '}':
                depth -= 1
                if depth == 0:
                    break
            i += 1
        found.append((match.end(), i, match.group('name')))
    return found


def source_constants(text: str) -> dict[str, str]:
    """String constants of one `buildSrc` source, by qualified name.

    `object deps { object versions { const val okhttp = "3.10.0" } }` is
    `deps.versions.okhttp`; a top-level `val x = "…"` is `x`. Templates
    (`"g:n:${versions.okhttp}"`, `"$okhttp"`) are resolved against the
    constants of the enclosing scopes, innermost first, then the file's.
    Kotlin, and Java's or Groovy's `static final String`.

    Only what a scope declares directly: a local in a function body is
    not a constant a build script can name.
    """
    code, masked = _mask(text)
    scopes = _scopes(masked)
    raw: dict[str, str] = {}
    where: dict[str, tuple[str, ...]] = {}
    for regex in (_KOTLIN_CONSTANT_RE, _JAVA_CONSTANT_RE):
        for match in regex.finditer(code):
            offset = match.start()
            enclosing = sorted(
                (start, name) for start, end, name in scopes
                if start <= offset < end
            )
            inner_start = enclosing[-1][0] if enclosing else 0
            between = masked[inner_start:offset]
            if between.count('{') != between.count('}'):
                continue
            path = tuple(name for _, name in enclosing)
            qualified = '.'.join((*path, match.group('name')))
            raw.setdefault(qualified, match.group('value'))
            where.setdefault(qualified, path)
    return {
        name: _resolve_template(value, where[name], raw)
        for name, value in raw.items()
    }


def _resolve_template(
    value: str,
    scope: tuple[str, ...],
    table: Mapping[str, str],
    depth: int = 0,
) -> str:
    """`${a.b}` and `$a` in `value`, looked up from `scope` outwards."""
    if '$' not in value or depth > 8:
        return value

    def substitute(match: re.Match[str]) -> str:
        name = match.group('braced') or match.group('bare')
        for cut in range(len(scope), -1, -1):
            candidate = '.'.join((*scope[:cut], name))
            if candidate in table:
                return _resolve_template(
                    table[candidate], tuple(candidate.split('.')[:-1]),
                    table, depth + 1,
                )
        return match.group(0)
    return _TEMPLATE_RE.sub(substitute, value)


_MAP_ASSIGNMENT_RE = re.compile(
    r'(?:^|(?<=[\s;{]))(?:ext\.|project\.ext\.|rootProject\.ext\.|def\s+)?'
    r'(?P<name>[A-Za-z_]\w*)\s*=\s*\[',
    re.MULTILINE,
)
_MAP_ENTRY_RE = re.compile(
    r'''\s*(?:(?P<q>['"])(?P<qkey>[^'"]+)(?P=q)|(?P<key>[\w.-]+))\s*:\s*''',
)


def map_constants(text: str) -> dict[str, str]:
    """Groovy maps of coordinates in a build script, by accessor.

    `ext.deps = [okhttp: 'g:n:1', support: [appCompat: 'g:m:2']]` is
    `deps.okhttp` and `deps.support.appCompat`; `ext { deps = […] }`
    the same. Values are the strings as written: `$x` is left for the
    caller to interpolate from the properties.
    """
    code, masked = _mask(text)
    found: dict[str, str] = {}
    for match in _MAP_ASSIGNMENT_RE.finditer(masked):
        _read_map(code, masked, match.end(), (match.group('name'),), found)
    return found


def _read_map(
    code: str,
    masked: str,
    i: int,
    prefix: tuple[str, ...],
    found: dict[str, str],
    depth: int = 0,
) -> int:
    """Read `key: value, …]` from offset `i`; the offset after its `]`."""
    n = len(code)
    while i < n:
        while i < n and masked[i] in ' \t\r\n,':
            i += 1
        if i >= n or masked[i] == ']':
            return i + 1
        entry = _MAP_ENTRY_RE.match(code, i)
        if entry is None:
            # A list, or a map this does not read: past its end.
            return _skip_to_close(masked, i)
        key = entry.group('qkey') or entry.group('key')
        i = entry.end()
        if i < n and masked[i] == '[' and depth < 6:
            i = _read_map(
                code, masked, i + 1,
                (*prefix, key), found, depth + 1,
            )
            continue
        string = _STRING_RE.match(code, i)
        if string is not None:
            found.setdefault('.'.join((*prefix, key)), string.group('value'))
            i = string.end()
        # Whatever else follows (a number, a call), up to the next entry.
        while i < n and masked[i] not in ',]':
            i = _skip_to_close(masked, i + 1) if masked[i] == '[' else i + 1
    return i


def _skip_to_close(masked: str, i: int) -> int:
    depth = 1
    while i < len(masked) and depth:
        if masked[i] == '[':
            depth += 1
        elif masked[i] == ']':
            depth -= 1
        i += 1
    return i


_ACCESSOR_PREFIXES = (
    'rootProject.ext.', 'project.ext.', 'rootProject.', 'project.', 'ext.',
)


def _constant_key(reference: str) -> str:
    """`rootProject.ext.deps['okhttp']` as `deps.okhttp`."""
    key = re.sub(r'''\[\s*['"]([^'"]+)['"]\s*\]''', r'.\1', reference)
    key = re.sub(r'\.get\(\)$', '', key)
    for prefix in _ACCESSOR_PREFIXES:
        if key.startswith(prefix):
            return key[len(prefix):]
    return key


# --- the build's context ----------------------------------------------------

@dataclass
class Context:
    """What a build file's references are resolved against: the whole
    repository's catalogs, properties and pinned versions."""

    catalogs: dict[str, Catalog] = field(default_factory=dict)
    properties: dict[str, str] = field(default_factory=dict)
    #: `group:name` -> a version a platform or constraint pins.
    pinned: dict[str, str] = field(default_factory=dict)
    spring_boot_version: str = ''
    #: Qualified name -> string, from `buildSrc` sources and the maps a
    #: build assigns (`source_constants`, `map_constants`).
    constants: dict[str, str] = field(default_factory=dict)

    def constant(self, reference: str) -> str | None:
        """The string a constant reference names, or None."""
        return self.constants.get(_constant_key(reference))

    def library(self, reference: str) -> list[Coordinate] | None:
        """The coordinates a catalog reference names, or None.

        `libs.x.y` is a library, `libs.bundles.x` a bundle of them;
        `libs.plugins.x` and `libs.versions.x` are not dependencies
        and name nothing (`[]`).
        """
        head, _, rest = reference.partition('.')
        catalog = self.catalogs.get(head)
        if catalog is None or not rest:
            return None
        rest = re.sub(r'\.(?:get|asProvider)\(\)$', '', rest)
        rest = re.sub(r'\.(?:get|asProvider)\(\)', '', rest)
        kind, _, alias = rest.partition('.')
        if kind in ('plugins', 'versions'):
            return []
        if kind == 'bundles':
            members = catalog.bundles.get(_accessor(alias))
            if members is None:
                return None
            return [
                catalog.libraries[m] for m in members if m in catalog.libraries
            ]
        found = catalog.libraries.get(_accessor(rest))
        return [found] if found is not None else None


def _catalog_name(path: str) -> str:
    name = path.rpartition('/')[2][:-len(CATALOG_SUFFIX)]
    return name or DEFAULT_CATALOG


def _depth(path: str) -> int:
    return path.count('/')


def context_from(files: Iterable[tuple[str, str | None]]) -> Context:
    """The context of one repository's Gradle files.

    `files` is `(path within the repository, text)`, as the manifest
    sources give them; files this module does not read are ignored.
    Shallower files win a conflict: the root's properties and catalog
    are the ones a subproject sees.
    """
    readable = sorted(
        ((p, t) for p, t in files if t is not None and is_gradle_input(p)),
        key=lambda pair: (_depth(pair[0]), pair[0]),
    )
    context = Context()
    for path, text in readable:
        assert text is not None
        name = path.rpartition('/')[2]
        if name.endswith(CATALOG_SUFFIX):
            catalog = parse_catalog(text)
            if catalog is not None:
                context.catalogs.setdefault(
                    _catalog_name(path), Catalog(),
                ).merge(catalog)
        elif name == PROPERTIES_FILE:
            for key, value in parse_properties(text).items():
                context.properties.setdefault(key, value)
        elif is_build_logic_source(path):
            for key, value in source_constants(text).items():
                context.constants.setdefault(key, value)
        elif name in SETTINGS_FILES:
            for catalog_name, catalog in settings_catalogs(text).items():
                context.catalogs.setdefault(catalog_name, Catalog()).merge(
                    catalog,
                )
            for key, value in properties_of(text).items():
                context.properties.setdefault(key, value)
        else:
            for key, value in properties_of(text).items():
                context.properties.setdefault(key, value)
            for key, value in map_constants(text).items():
                context.constants.setdefault(key, value)

    # Pinned versions and the Spring Boot version, now that every
    # property and catalog is known.
    for path, text in readable:
        assert text is not None
        name = path.rpartition('/')[2]
        if name in BUILD_FILES or name in SETTINGS_FILES:
            _collect_pins(text, context)
    if not context.spring_boot_version:
        for catalog in context.catalogs.values():
            for plugin_id, version in catalog.plugins.values():
                if plugin_id == SPRING_BOOT_PLUGIN and version:
                    context.spring_boot_version = version
                    break
            for coordinate in catalog.libraries.values():
                if (
                    coordinate.group == SPRING_BOOT_GROUP
                    and coordinate.name in (
                        SPRING_BOOT_BOM, SPRING_BOOT_GRADLE_PLUGIN,
                    )
                    and coordinate.version
                ):
                    context.spring_boot_version = coordinate.version
    if not context.spring_boot_version:
        for key in (
            'springBootVersion', 'spring_boot_version',
            'spring-boot.version', 'springBoot.version',
            'spring.boot.version',
        ):
            stated = context.properties.get(key)
            if stated:
                context.spring_boot_version = stated
                break
    return context


_PLUGIN_VERSION_RE = re.compile(
    r'''\bid\s*\(?\s*(['"])''' + re.escape(SPRING_BOOT_PLUGIN) +
    r'''\1\s*\)?\s*version\s*\(?\s*(['"])(?P<version>[^'"]+)\2''',
)
_BOM_RE = re.compile(
    re.escape(SPRING_BOOT_GROUP) + r':(?:' + re.escape(SPRING_BOOT_BOM) +
    '|' + re.escape(SPRING_BOOT_GRADLE_PLUGIN) +
    r'''):(?P<version>[^'"\s)]+)''',
)
_MANAGED_DEPENDENCY_RE = re.compile(
    r'''(?<![\w.])dependency\s*\(?\s*(['"])(?P<coordinate>[^'"]+)\1''',
)


def _collect_pins(text: str, context: Context) -> None:
    code, masked = _mask(text)
    if not context.spring_boot_version:
        for regex in (_PLUGIN_VERSION_RE, _BOM_RE):
            match = regex.search(code)
            if match:
                version = _interpolate(match.group('version'), context)
                if version and '$' not in version:
                    context.spring_boot_version = version
                    break
    for declaration in _declarations(code, masked, context, constraints=True):
        if declaration.pinning and declaration.coordinate.version:
            context.pinned.setdefault(
                declaration.coordinate.module, declaration.coordinate.version,
            )
    for start, end in _block_ranges(masked, ['dependencyManagement']):
        for match in _MANAGED_DEPENDENCY_RE.finditer(code, start, end):
            found = _coordinate(
                _interpolate(
                    match.group('coordinate'), context,
                ),
            )
            if found and found.version and '$' not in found.module:
                context.pinned.setdefault(found.module, found.version)


# --- declarations -----------------------------------------------------------

_CONFIGURATIONS = (
    'implementation', 'api', 'compileOnly', 'compileOnlyApi', 'runtimeOnly',
    'annotationProcessor', 'kapt', 'ksp', 'classpath', 'compile', 'runtime',
    'testCompile', 'testRuntime', 'developmentOnly', 'providedCompile',
    'providedRuntime', 'compileClasspath', 'runtimeClasspath',
    r'\w+(?:Implementation|Api|CompileOnly|RuntimeOnly|AnnotationProcessor)',
)

_DECLARATION_RE = re.compile(
    # A declaration starts a statement, which `api` in a description
    # string or after `extendsFrom` does not.
    r'(?:^|[{;])[ \t]*'
    r'(?P<configuration>' + '|'.join(_CONFIGURATIONS) + r')'
    # `implementation(` may break the line; `implementation 'x'` does not.
    r'(?:[ \t]*\(\s*|[ \t]+)'
    # A BOM or a test-fixtures variant wraps the coordinate.
    r'(?:(?P<wrapper>platform|enforcedPlatform|testFixtures)[ \t]*\(\s*)?'
    r'(?P<argument>[^\n;]*)',
    re.MULTILINE,
)
# group: 'g', name: 'a' (Groovy) or group = "g", name = "a" (Kotlin).
_MAP_RE = re.compile(r'(?:group|name|version)\s*[:=]')
_MAP_FIELD_RE = re.compile(
    r'''\b(?P<key>group|name|version)\s*[:=]\s*(['"])(?P<value>[^'"]*)\2''',
)
_KOTLIN_RE = re.compile(r'''kotlin[ \t]*\(\s*(['"])(?P<module>[^'"]+)\1''')
_LOCAL_RE = re.compile(
    r'(?:project|files|fileTree|gradleApi|gradleTestKit|localGroovy)'
    r'[ \t]*\(|projects\.',
)
#: A reference: a catalog accessor, or a constant, with Groovy's
#: `deps['okhttp']` subscripts too.
_REFERENCE_RE = re.compile(
    r'''(?P<reference>[A-Za-z_]\w*(?:\.\w+(?:\(\))?|\[\s*['"][^'"]+['"]\s*\])*)''',
)
_STRING_RE = re.compile(r'''(['"])(?P<value>[^'"\n]*)\1''')
_COORDINATE_RE = re.compile(
    r'^[^\s:]+:[^\s:]+(?::[^\s:@]*)?(?::[^\s:@]+)?(?:@\w+)?$',
)
_INTERPOLATION_RE = re.compile(
    r'\$\{?(?P<name>[A-Za-z_][\w.]*?)(?:\(\))?\}?(?=[^\w.]|$)',
)
_BOM_COORDINATES = 'SpringBootPlugin.BOM_COORDINATES'


@dataclass(frozen=True)
class Declared:
    """One dependency a build file declares."""

    coordinate: Coordinate
    configuration: str
    #: `VIA_LITERAL` or `VIA_CATALOG`.
    via: str
    #: Where the version came from: `VERSION_*`.
    version_source: str
    #: Inside `platform(…)`/`enforcedPlatform(…)`.
    platform: bool = False
    #: Inside a `constraints { … }` block: pins a version, declares
    #: nothing.
    pinning: bool = False


@dataclass(frozen=True)
class BuildFile:
    """What one build file declares, and whether that is all of it."""

    declared: tuple[Declared, ...]
    #: References this could not follow (a variable, an unknown catalog
    #: entry, an interpolated group or name).
    unresolved: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not self.unresolved


def _interpolate(value: str, context: Context) -> str:
    """`$x` and `${x}` replaced from the properties, where known."""
    def substitute(match: re.Match[str]) -> str:
        name = match.group('name')
        for prefix in (
            'rootProject.ext.', 'project.ext.', 'rootProject.',
            'project.', 'ext.', 'extra.',
        ):
            if name.startswith(prefix):
                name = name[len(prefix):]
                break
        if name in context.properties:
            return context.properties[name]
        if name in context.constants:
            return context.constants[name]
        if name.startswith('libs.versions.') or '.versions.' in name:
            catalog_name, _, alias = name.partition('.versions.')
            catalog = context.catalogs.get(catalog_name)
            if catalog is not None:
                for key, version in catalog.versions.items():
                    if _accessor(key) == _accessor(re.sub(r'\.get$', '', alias)):
                        return version
        return match.group(0)
    return _INTERPOLATION_RE.sub(substitute, value)


def _declarations(
    code: str,
    masked: str,
    context: Context,
    constraints: bool = False,
) -> Iterable[Declared]:
    """Every declaration in the text; with `constraints`, only those
    inside `constraints { … }` blocks, else only those outside."""
    for declared, _ in _read(code, masked, context, constraints):
        if declared is not None:
            yield declared


def _read(
    code: str,
    masked: str,
    context: Context,
    constraints: bool,
) -> Iterable[tuple[Declared | None, str | None]]:
    pins = _block_ranges(masked, ['constraints'])
    skipped = _block_ranges(masked, ['plugins', 'pluginManagement'])
    for match in _DECLARATION_RE.finditer(masked):
        offset = match.start('configuration')
        if _inside(offset, skipped):
            continue
        if _inside(offset, pins) != constraints:
            continue
        configuration = match.group('configuration')
        wrapper = match.group('wrapper') or ''
        start, end = match.span('argument')
        argument = code[start:end].strip()
        platform = wrapper in ('platform', 'enforcedPlatform')

        def made(coordinate: Coordinate, via: str) -> Declared:
            return _complete(
                coordinate, configuration, via, platform, constraints, context,
            )

        if argument.startswith(_BOM_COORDINATES):
            if context.spring_boot_version:
                yield made(
                    Coordinate(
                        SPRING_BOOT_GROUP, SPRING_BOOT_BOM,
                        context.spring_boot_version,
                    ), VIA_LITERAL,
                ), None
            continue
        if argument[:1] in ('"', "'"):
            strings = [
                m.group('value') for m in _STRING_RE.finditer(argument)
            ]
            coordinates = [s for s in strings if _COORDINATE_RE.match(s)]
            if not coordinates:
                # `':name:version'` has no group: a jar from a `flatDir`
                # repository, which is the build's own file.
                if strings and ':' in strings[0] and not strings[0].startswith(':'):
                    yield None, strings[0]
                continue
            for raw in coordinates:
                value = _interpolate(raw, context)
                found = _coordinate(value)
                if found is None or '$' in found.module:
                    yield None, raw
                    continue
                if '$' in found.version:
                    found = replace(found, version='')
                yield made(found, VIA_LITERAL), None
        elif _MAP_RE.match(argument):
            fields = {
                m.group('key'): _interpolate(m.group('value'), context)
                for m in _MAP_FIELD_RE.finditer(argument)
            }
            group, name = fields.get('group', ''), fields.get('name', '')
            if group and name and '$' not in group + name:
                version = fields.get('version', '')
                yield made(
                    Coordinate(group, name, '' if '$' in version else version),
                    VIA_LITERAL,
                ), None
            else:
                yield None, argument
        elif kotlin := _KOTLIN_RE.match(argument):
            yield made(
                Coordinate(
                    'org.jetbrains.kotlin', f"kotlin-{kotlin.group('module')}",
                ), VIA_LITERAL,
            ), None
        elif _LOCAL_RE.match(argument):
            continue
        elif reference := _REFERENCE_RE.match(argument):
            name = reference.group('reference')
            libraries = context.library(name)
            if libraries is not None:
                for library in libraries:
                    yield made(library, VIA_CATALOG), None
                continue
            stated = context.constant(name)
            found = (
                None if stated is None
                else _coordinate(_interpolate(stated, context))
            )
            if found is None or '$' in found.module:
                yield None, name
                continue
            if '$' in found.version:
                found = replace(found, version='')
            yield made(found, VIA_CONSTANT), None


def _complete(
    coordinate: Coordinate,
    configuration: str,
    via: str,
    platform: bool,
    pinning: bool,
    context: Context,
) -> Declared:
    """The declaration, with a version filled in where one is known."""
    source = VERSION_DECLARED if coordinate.version else VERSION_NONE
    if not coordinate.version and not pinning:
        pinned = context.pinned.get(coordinate.module)
        if pinned:
            coordinate = replace(coordinate, version=pinned)
            source = VERSION_CONSTRAINT
        elif (
            coordinate.group == SPRING_BOOT_GROUP
            and context.spring_boot_version
        ):
            coordinate = replace(
                coordinate, version=context.spring_boot_version,
            )
            source = VERSION_SPRING_BOOT
    return Declared(
        coordinate=coordinate,
        configuration=configuration,
        via=via,
        version_source=source,
        platform=platform,
        pinning=pinning,
    )


def read_build(text: str, context: Context | None = None) -> BuildFile:
    """What one `build.gradle(.kts)` declares, against `context`."""
    context = context or Context()
    code, masked = _mask(text)
    declared: list[Declared] = []
    unresolved: list[str] = []
    for found, missing in _read(code, masked, context, constraints=False):
        if found is not None:
            declared.append(found)
        elif missing is not None:
            unresolved.append(missing)
    return BuildFile(tuple(declared), tuple(unresolved))


# --- artifact rows ----------------------------------------------------------

#: `artifacts.found_by` of a manifest row: how its coordinate was read.
MANIFEST_FOUND_BY: dict[str, str] = {
    VIA_LITERAL: 'chatsbom-gradle',
    VIA_CATALOG: 'chatsbom-gradle-catalog',
    VIA_CONSTANT: 'chatsbom-gradle-constant',
}

#: Configurations whose dependencies are the build's own, not the
#: project's.
BUILD_CONFIGURATIONS: frozenset[str] = frozenset({'classpath'})


def purl_of(coordinate: Coordinate) -> str:
    purl = (
        f'pkg:maven/{quote(coordinate.group, safe=".-_~")}/'
        f'{quote(coordinate.name, safe=".-_~")}'
    )
    if coordinate.version:
        purl += '@' + quote(coordinate.version, safe='.-_~')
    return purl


def declarations(
    manifests: Iterable[tuple[str, str | None]],
) -> list[tuple[str, Declared]]:
    """`(path, declaration)` for every dependency the repository's Gradle
    build files declare, resolved against all of its Gradle files.

    One per `(path, group, name, version)`: a coordinate declared in two
    configurations of one file is one declaration. Build logic
    (`buildSrc/` and friends), the buildscript classpath, constraints
    and plugins are left out (module docstring).
    """
    files = list(manifests)
    builds = [
        (path, text) for path, text in files
        if text is not None and is_build_file(path)
        and not is_build_logic(path)
    ]
    if not builds:
        return []
    context = context_from(files)
    out: list[tuple[str, Declared]] = []
    for path, text in sorted(builds):
        assert text is not None
        seen: set[tuple[str, str, str]] = set()
        for declared in read_build(text, context).declared:
            if declared.configuration in BUILD_CONFIGURATIONS:
                continue
            c = declared.coordinate
            key = (c.group, c.name, c.version)
            if key in seen:
                continue
            seen.add(key)
            out.append((path, declared))
    return out
