"""Direct-dependency extraction from package manifests.

Syft reads lockfiles, so an SBOM is the resolved dependency closure: a
Rails app's SBOM lists `mail` whether the project asked for it or merely
inherited it through `actionmailer`. Answering "who actually depends on
X" needs the *declared* set, which only the manifest has.

Each ecosystem gets a parser over manifest text plus a name normaliser,
so manifest names can be compared against the names Syft reports.

A parser also says whether it understood the manifest in full. One it
did not is *incomplete*: it did not parse, or it declares dependencies
somewhere we do not read — a Gemfile's `gemspec`, `dynamic =
["dependencies"]` in pyproject.toml, `file:` or `attr:` in setup.cfg, a
Gradle version catalog or variable. setup.py is code, so it is always
incomplete, and so is a manifest that could not be read at all.

A name in the SBOM is then:

- `direct` if any manifest declares it, complete or not;
- `transitive` if none does and every manifest was understood in full;
- `unknown` if none does and one was not: that one may declare it;
- `unknown` whatever it is when no manifest was read.

Within one directory, one manifest can settle what another leaves open.
A .gemspec is what a Gemfile's `gemspec` loads. A pyproject.toml
`[project]` or Poetry table, or setup.cfg's `install_requires`, is where
a build takes its dependencies from instead of setup.py.
"""
import configparser
import json
import re
import tomllib
import xml.etree.ElementTree as ET
from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.discovery import VENDORED_DIRS
from chatsbom.models.language import Language

logger = structlog.get_logger('manifest')

DIRECT = 'direct'
TRANSITIVE = 'transitive'
UNKNOWN = 'unknown'

# Vendored dependency trees contain their own manifests; reading them
# would mark every transitive package as direct. The same set manifest
# discovery leaves out (`core/discovery.py`), so nothing is downloaded
# that the classifier would then skip, or the other way round.
VENDOR_DIRS = VENDORED_DIRS

MAX_MANIFEST_BYTES = 4 * 1024 * 1024


def _identity(name: str) -> str:
    return name


def _pep503(name: str) -> str:
    """PEP 503 normalisation: lowercase, runs of -_. become a single -."""
    return re.sub(r'[-_.]+', '-', name).lower()


def _lower(name: str) -> str:
    return name.lower()


def _crate(name: str) -> str:
    """Cargo treats - and _ as interchangeable in crate names."""
    return name.lower().replace('_', '-')


# --- per-ecosystem parsing -------------------------------------------------

@dataclass(frozen=True)
class Declaration:
    """What one manifest declares, and whether that is all of it."""

    names: frozenset[str] = frozenset()
    #: False when the manifest did not parse, or declares dependencies
    #: somewhere we do not read. Its names are still declared, but what
    #: it does not name is not thereby undeclared.
    complete: bool = True
    #: Manifests in the same directory, by file name, whose gap this one
    #: settles.
    settles: frozenset[str] = frozenset()


#: A manifest that did not parse: nothing seen, so nothing ruled out.
_NOT_UNDERSTOOD = Declaration(complete=False)


def _table(value: object) -> dict[str, Any]:
    """`value` if it is a TOML table, else an empty one."""
    return value if isinstance(value, dict) else {}


def _array(value: object) -> list[Any]:
    """`value` if it is a TOML array, else an empty one."""
    return value if isinstance(value, list) else []


_GEMFILE_RE = re.compile(r"""^\s*gem\s+['"]([^'"]+)['"]""", re.MULTILINE)
_GEMSPEC_RE = re.compile(
    r"""add(?:_runtime|_development)?_dependency\s*\(?\s*['"]([^'"]+)['"]""",
)
# Adds whatever the gem's .gemspec declares.
_GEMSPEC_DIRECTIVE_RE = re.compile(r'^\s*gemspec\b', re.MULTILINE)


def _parse_ruby(filename: str, text: str) -> Declaration:
    if filename.endswith('.gemspec'):
        # What a Gemfile's `gemspec` loads, the only gap a Gemfile has.
        return Declaration(
            frozenset(_GEMSPEC_RE.findall(text)),
            settles=frozenset({'Gemfile'}),
        )
    code = _strip_ruby_comments(text)
    return Declaration(
        frozenset(_GEMFILE_RE.findall(code)),
        # The content stage does not download *.gemspec, so what the
        # directive adds is normally unseen.
        complete=not _GEMSPEC_DIRECTIVE_RE.search(code),
    )


def _strip_ruby_comments(text: str) -> str:
    return '\n'.join(
        line for line in text.splitlines() if not line.lstrip().startswith('#')
    )


_NPM_SECTIONS = (
    'dependencies', 'devDependencies',
    'peerDependencies', 'optionalDependencies',
)


def _parse_npm(filename: str, text: str) -> Declaration:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return _NOT_UNDERSTOOD
    if not isinstance(data, dict):
        return _NOT_UNDERSTOOD
    names: set[str] = set()
    for section in _NPM_SECTIONS:
        block = data.get(section)
        if isinstance(block, dict):
            names.update(block)
    return Declaration(frozenset(names))


def _parse_go(filename: str, text: str) -> Declaration:
    names: set[str] = set()
    in_block = False

    for line in text.splitlines():
        # A comment can hold anything, a `)` included, so it comes off
        # before the line is read. Only the marker in it matters: go.mod
        # marks transitive requirements explicitly.
        code, _, comment = line.partition('//')
        marker = comment.strip()
        indirect = marker == 'indirect' or marker.startswith('indirect;')
        tokens = code.replace('(', ' ( ').replace(')', ' ) ').split()
        if not tokens:
            continue

        if in_block:
            if tokens[0] == ')':
                in_block = False
            elif not indirect:
                names.add(tokens[0])
        elif tokens[0] == 'require' and len(tokens) > 1:
            if tokens[1] == '(':
                in_block = ')' not in tokens
            elif len(tokens) >= 3 and not indirect:
                names.add(tokens[1])

    return Declaration(frozenset(names))


_CARGO_DEP_SECTIONS = (
    'dependencies', 'dev-dependencies', 'build-dependencies',
)


def _parse_cargo(filename: str, text: str) -> Declaration:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return _NOT_UNDERSTOOD

    names: set[str] = set()

    def collect(table: dict[str, Any]) -> None:
        for section in _CARGO_DEP_SECTIONS:
            block = table.get(section)
            if isinstance(block, dict):
                names.update(block)

    collect(data)
    for target in _table(data.get('target')).values():
        collect(_table(target))
    collect(_table(data.get('workspace')))

    return Declaration(frozenset(names))


_PY_NAME_RE = re.compile(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]*)')
# PEP 508 direct reference: `name[extras] @ https://...` names its package.
_PY_URL_REFERENCE_RE = re.compile(
    r'^\s*[A-Za-z0-9][A-Za-z0-9._-]*\s*(?:\[[^\]]*\])?\s*@',
)

# What a build ignores once pyproject.toml or setup.cfg declares the
# dependencies itself.
_SETUP_PY = frozenset({'setup.py'})


def _requirement_name(spec: str) -> str | None:
    """Leading distribution name of a PEP 508 requirement string."""
    spec = spec.split(';', 1)[0].split('#', 1)[0].strip()
    if not spec or spec.startswith('-'):
        return None
    match = _PY_NAME_RE.match(spec)
    return match.group(1) if match else None


def _parse_python(filename: str, text: str) -> Declaration:
    if filename == 'pyproject.toml':
        return _parse_pyproject(text)
    if filename == 'setup.cfg':
        return _parse_setup_cfg(text)
    if filename == 'setup.py':
        # Code, which we do not run. Syft reads the pinned requirements
        # out of it; here, what it passes to setup() is unseen.
        return _NOT_UNDERSTOOD
    return _parse_requirements(text)


def _parse_pyproject(text: str) -> Declaration:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return _NOT_UNDERSTOOD

    project = _table(data.get('project'))
    tool = _table(data.get('tool'))

    specs: list[Any] = list(_array(project.get('dependencies')))
    for extra in _table(project.get('optional-dependencies')).values():
        specs.extend(_array(extra))
    for group in _table(data.get('dependency-groups')).values():
        specs.extend(_array(group))
    specs.extend(_array(_table(tool.get('uv')).get('dev-dependencies')))
    pdm = _table(tool.get('pdm'))
    for group in _table(pdm.get('dev-dependencies')).values():
        specs.extend(_array(group))
    names = {_requirement_name(s) for s in specs if isinstance(s, str)}

    poetry = _table(tool.get('poetry'))
    poetry_blocks = [
        poetry.get('dependencies'),
        poetry.get('dev-dependencies'),
    ]
    poetry_blocks.extend(
        _table(group).get('dependencies')
        for group in _table(poetry.get('group')).values()
    )
    for block in poetry_blocks:
        if isinstance(block, dict):
            names.update(k for k in block if k.lower() != 'python')

    dynamic = {d for d in _array(project.get('dynamic')) if isinstance(d, str)}
    # A build backend fills these in, from files we did not read.
    unseen = dynamic & {'dependencies', 'optional-dependencies'}

    # PEP 621 has a build take the dependencies from `[project]`, not
    # from setup.py, unless they are dynamic. A Poetry build does not
    # read setup.py at all.
    takes_over = (
        (isinstance(data.get('project'), dict) and 'dependencies' not in dynamic)
        or isinstance(poetry.get('dependencies'), dict)
    )
    return Declaration(
        frozenset(n for n in names if n),
        complete=not unseen,
        settles=_SETUP_PY if takes_over else frozenset(),
    )


def _parse_setup_cfg(text: str) -> Declaration:
    """`[options] install_requires` and `[options.extras_require]`.

    Every other key in setup.cfg is metadata or tool configuration, which
    reading it as a requirements file took for package names.
    """
    config = configparser.ConfigParser(interpolation=None, strict=False)
    try:
        config.read_string(text)
    except configparser.Error:
        return _NOT_UNDERSTOOD

    declares = config.has_option('options', 'install_requires')
    values = [config.get('options', 'install_requires')] if declares else []
    if config.has_section('options.extras_require'):
        values.extend(v for _, v in config.items('options.extras_require'))

    names: set[str] = set()
    complete = True
    for value in values:
        if value.strip().startswith(('file:', 'attr:')):
            # setuptools resolves these from a file or a module we did
            # not read.
            complete = False
            continue
        # As setuptools reads it: one requirement per line, or a single
        # line of them separated by `;`.
        for entry in value.splitlines() if '\n' in value else value.split(';'):
            name = _requirement_name(entry)
            if name:
                names.add(name)

    return Declaration(
        frozenset(names),
        complete=complete,
        settles=_SETUP_PY if declares else frozenset(),
    )


def _parse_requirements(text: str) -> Declaration:
    names: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(('#', '-')):
            continue
        if '#egg=' in line:
            names.add(line.split('#egg=', 1)[1].strip())
            continue
        if '://' in line and not _PY_URL_REFERENCE_RE.match(line):
            # A bare URL does not say which package it is.
            continue
        name = _requirement_name(line)
        if name:
            names.add(name)
    return Declaration(frozenset(names))


# Platform requirements in composer.json are not packages.
_COMPOSER_PLATFORM_RE = re.compile(
    r'^(?:php(?:-[\w.]+)?|hhvm|composer(?:-[\w.]+)?)$|^(?:ext|lib)-',
)


def _parse_composer(filename: str, text: str) -> Declaration:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return _NOT_UNDERSTOOD
    if not isinstance(data, dict):
        return _NOT_UNDERSTOOD
    names: set[str] = set()
    for section in ('require', 'require-dev'):
        block = data.get(section)
        if isinstance(block, dict):
            names.update(
                k for k in block if not _COMPOSER_PLATFORM_RE.match(k)
            )
    return Declaration(frozenset(names))


def _parse_java(filename: str, text: str) -> Declaration:
    if filename == 'pom.xml':
        return _parse_pom(text)
    return _parse_gradle(text)


def _parse_pom(text: str) -> Declaration:
    """The artifactIds of the project's `<dependencies>` and its profiles'.

    Syft 1.41.2 reports both. `<dependencyManagement>` only pins versions
    for whoever declares a package, and a plugin's `<dependencies>` are
    that plugin's classpath, so neither is the project declaring one.
    """
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return _NOT_UNDERSTOOD

    names: set[str] = set()
    for owner in [root, *_xml_path(root, 'profiles', 'profile')]:
        for artifact in _xml_path(
            owner, 'dependencies', 'dependency', 'artifactId',
        ):
            if artifact.text and artifact.text.strip():
                names.add(artifact.text.strip())
    return Declaration(frozenset(names))


def _xml_path(element: ET.Element, *tags: str) -> list[ET.Element]:
    """The elements along a path of tag names, in any XML namespace."""
    found = [element]
    for tag in tags:
        found = [
            child for parent in found for child in parent
            if child.tag.rsplit('}', 1)[-1] == tag
        ]
    return found


#: The configurations builds commonly declare dependencies in.
_GRADLE_CONFIGURATIONS = (
    'implementation', 'api', 'compileOnly', 'runtimeOnly',
    'testImplementation', 'testRuntimeOnly', 'annotationProcessor',
    'kapt', 'ksp', 'classpath', 'compile', 'testCompile',
)

_GRADLE_DECLARATION_RE = re.compile(
    # A declaration starts a statement, which `api` in a description
    # string or after `extendsFrom` does not.
    r'(?:^|[{;])[ \t]*'
    r'(?:' + '|'.join(_GRADLE_CONFIGURATIONS) + r')'
    # `implementation(` may break the line; `implementation 'x'` does not.
    r'(?:[ \t]*\(\s*|[ \t]+)'
    # A BOM or a test-fixtures variant wraps the coordinate.
    r'(?:(?:platform|enforcedPlatform|testFixtures)[ \t]*\(\s*)?'
    r'(?P<argument>[^\n;]*)',
    re.MULTILINE,
)
# group: 'g', name: 'a' (Groovy) or group = "g", name = "a" (Kotlin).
_GRADLE_MAP_RE = re.compile(r'(?:group|name|version)\s*[:=]')
_GRADLE_MAP_NAME_RE = re.compile(
    r"""\bname\s*[:=]\s*(['"])(?P<name>[^'"]+)\1""",
)
# `kotlin("test")` is org.jetbrains.kotlin:kotlin-test.
_GRADLE_KOTLIN_RE = re.compile(
    r"""kotlin[ \t]*\(\s*(['"])(?P<module>[^'"]+)\1""",
)
# The build's own modules and files, which are not packages.
_GRADLE_LOCAL_RE = re.compile(
    r'(?:project|files|fileTree|gradleApi|gradleTestKit|localGroovy)'
    r'[ \t]*\(|projects\.',
)
# Whatever else starts with a name refers to one: a version catalog
# (`libs.x.y`), a constant or a variable.
_GRADLE_REFERENCE_RE = re.compile(r'[A-Za-z_$]')


def _parse_gradle(text: str) -> Declaration:
    names: set[str] = set()
    complete = True

    for match in _GRADLE_DECLARATION_RE.finditer(text):
        argument = match.group('argument').strip()
        if argument[:1] in ('"', "'"):
            parts = argument[1:].split(argument[0], 1)[0].split(':')
            if len(parts) < 2:
                continue
            if '$' in parts[1]:
                # Interpolated: only the build knows which artifact.
                complete = False
            else:
                names.add(parts[1])
        elif _GRADLE_MAP_RE.match(argument):
            found = _GRADLE_MAP_NAME_RE.search(argument)
            if found:
                names.add(found.group('name'))
            else:
                complete = False
        elif kotlin := _GRADLE_KOTLIN_RE.match(argument):
            names.add(f"kotlin-{kotlin.group('module')}")
        elif _GRADLE_LOCAL_RE.match(argument):
            continue
        elif _GRADLE_REFERENCE_RE.match(argument):
            # Resolving it needs gradle/libs.versions.toml or buildSrc,
            # which the content stage does not download.
            complete = False

    return Declaration(frozenset(names), complete)


# --- parser registry -------------------------------------------------------

@dataclass(frozen=True)
class ManifestParser:
    """Which files declare dependencies, and how to read them."""

    filenames: tuple[str, ...]
    extract: Callable[[str, str], Declaration]
    normalise: Callable[[str], str] = _identity
    suffixes: tuple[str, ...] = ()

    def matches(self, filename: str) -> bool:
        return filename in self.filenames or filename.endswith(self.suffixes)

    def read(self, filename: str, text: str) -> Declaration:
        """What the manifest declares, normalised to compare with the SBOM."""
        found = self.extract(filename, text)
        return replace(
            found,
            names=frozenset(self.normalise(n) for n in found.names if n),
        )

    def parse(self, filename: str, text: str) -> set[str]:
        """Declared names, normalised so they compare against SBOM names."""
        return set(self.read(filename, text).names)


_NPM = ManifestParser(
    filenames=('package.json',),
    extract=_parse_npm,
    normalise=_lower,
)

_PARSERS: dict[Language, ManifestParser] = {
    Language.RUBY: ManifestParser(
        filenames=('Gemfile',),
        suffixes=('.gemspec',),
        extract=_parse_ruby,
        normalise=_lower,
    ),
    Language.JAVASCRIPT: _NPM,
    Language.TYPESCRIPT: _NPM,
    Language.NODE: _NPM,
    Language.GO: ManifestParser(
        filenames=('go.mod',),
        extract=_parse_go,
        normalise=_identity,
    ),
    Language.RUST: ManifestParser(
        filenames=('Cargo.toml',),
        extract=_parse_cargo,
        normalise=_crate,
    ),
    Language.PYTHON: ManifestParser(
        filenames=(
            'pyproject.toml', 'requirements.txt', 'requirements-dev.txt',
            'requirements_dev.txt', 'dev-requirements.txt', 'setup.cfg',
            'setup.py',
        ),
        extract=_parse_python,
        normalise=_pep503,
    ),
    Language.PHP: ManifestParser(
        filenames=('composer.json',),
        extract=_parse_composer,
        normalise=_lower,
    ),
    Language.JAVA: ManifestParser(
        filenames=('pom.xml', 'build.gradle', 'build.gradle.kts'),
        extract=_parse_java,
        normalise=_lower,
    ),
}


def parser_for(language: Language) -> ManifestParser:
    """The manifest parser for a language. Raises for unknown languages."""
    try:
        return _PARSERS[language]
    except KeyError:
        raise ValueError(f"no manifest parser for {language}") from None


# --- classification --------------------------------------------------------

@dataclass(frozen=True)
class DirectDependencies:
    """The declared dependency set of one project."""

    names: frozenset[str]
    sources: tuple[str, ...]
    normalise: Callable[[str], str]
    #: Manifests found but not understood in full, including any that
    #: could not be read. What no manifest names may be declared there.
    incomplete: tuple[str, ...] = ()

    def relationship_of(self, artifact_name: str) -> str:
        """Classify a name from the SBOM against the declared set.

        With no manifest read, everything is `unknown` — absence of
        evidence is not evidence of transitivity. So is a name no
        manifest declares while one of them was not understood in full:
        that one may declare it.
        """
        if not self.sources:
            return UNKNOWN
        if self.normalise(artifact_name) in self.names:
            return DIRECT
        return UNKNOWN if self.incomplete else TRANSITIVE


#: Byte-order mark -> the codec for what follows it.
#:
#: Longest first, so UTF-32-LE is not read as UTF-16-LE: `ff fe 00 00`
#: starts with `ff fe`, and the shorter match would decode the whole
#: file one byte-pair out of step.
#:
#: The codecs are the fixed-endian ones, and the mark is *sliced off*
#: before decoding. Handing the mark to the codec instead leaves
#: `\ufeff` at the front of the text, which is invisible on screen and
#: makes the first requirement a package named `\ufeffrequests` -- a
#: different package from `requests`, and one nothing depends on.
BOMS: tuple[tuple[bytes, str], ...] = (
    (b'\xff\xfe\x00\x00', 'utf-32-le'),
    (b'\x00\x00\xfe\xff', 'utf-32-be'),
    (b'\xef\xbb\xbf', 'utf-8'),
    (b'\xff\xfe', 'utf-16-le'),
    (b'\xfe\xff', 'utf-16-be'),
)


def _decoded(path: Path) -> str:
    """The manifest's text, whatever encoding it announces.

    `read_text(encoding='utf-8')` was raising on 21 `requirements.txt`
    files in this corpus -- "'utf-8' codec can't decode byte 0xff in
    position 0", which is a UTF-16 byte-order mark. Editors on Windows
    write these, and the failure was invisible in the outcome: the
    exception is caught and the manifest skipped, so those repositories
    indexed with every dependency labelled `unknown` rather than
    direct or transitive, and nothing said why.

    A byte-order mark is the file stating its own encoding, so it is
    read rather than guessed. No mark means UTF-8, which is both the
    overwhelming majority and the right thing to fail on when a file is
    genuinely not text.
    """
    raw = path.read_bytes()
    for mark, encoding in BOMS:
        if raw.startswith(mark):
            return raw[len(mark):].decode(encoding)
    return raw.decode('utf-8')


def read_manifest(path: Path, max_bytes: int | None = None) -> str | None:
    """A manifest file's text, or None when it cannot be read at all.

    None for a file over `max_bytes` (MAX_MANIFEST_BYTES unless given),
    one that cannot be opened, and one that is not text in the encoding
    it announces (`_decoded`). Returned rather than skipped: a manifest
    nobody read may declare anything, so `relationships_from` counts it
    as incomplete.
    """
    cap = MAX_MANIFEST_BYTES if max_bytes is None else max_bytes
    try:
        if path.stat().st_size > cap:
            logger.debug('Manifest too large', path=str(path))
            return None
        return _decoded(path)
    except (OSError, UnicodeDecodeError) as e:
        logger.debug('Unreadable manifest', path=str(path), error=str(e))
        return None


def resolve_relationships(
    content_dir: Path,
    language: Language,
    max_depth: int = 3,
) -> DirectDependencies:
    """Read every manifest under `content_dir` and merge the declared sets.

    Monorepos declare dependencies in nested manifests, so the search
    descends a few levels, skipping vendored trees. The module docstring
    says when an undeclared name is `transitive` and when `unknown`.
    """
    parser = parser_for(language)
    read = [
        (str(path.relative_to(content_dir)), read_manifest(path))
        for path in _find_manifests(content_dir, parser, max_depth)
    ]
    return relationships_from(read, language)


def relationships_from(
    manifests: Iterable[tuple[str, str | None]],
    language: Language,
) -> DirectDependencies:
    """The declared set, from manifests already read.

    Split out from `resolve_relationships` so the same judgement runs
    whether the manifests came off disk or out of `raw_documents`. The
    reading is I/O and belongs to the source; deciding what a manifest
    declares is this.

    `manifests` is `(path within the repository, text)`. The path is
    what picks the parser -- `Gemfile` and `Gemfile.lock` are read
    differently -- and is reported as `sources`, which is the audit
    trail behind every direct/transitive verdict. The text is None for
    a manifest the source found but could not read: whatever it declares
    is unseen, so it is incomplete.

    Manifests are judged a directory at a time, because one can settle
    what another beside it leaves open (`Declaration.settles`).
    """
    parser = parser_for(language)
    names: set[str] = set()
    sources: list[str] = []
    incomplete: list[str] = []

    directories: dict[str, list[tuple[str, str | None]]] = {}
    for relative, text in manifests:
        directory, _, name = relative.rpartition('/')
        if parser.matches(name):
            directories.setdefault(directory, []).append((relative, text))

    for found in directories.values():
        read: list[tuple[str, Declaration]] = []
        for relative, text in found:
            declaration = (
                None if text is None
                else _declaration_of(relative, text, parser)
            )
            if declaration is None:
                # Unread, so whatever it declares is unseen, and no
                # manifest beside it can settle that.
                incomplete.append(relative)
            else:
                read.append((relative, declaration))

        settled = {name for _, d in read for name in d.settles}
        for relative, declaration in read:
            sources.append(relative)
            names.update(declaration.names)
            if (
                not declaration.complete
                and relative.rpartition('/')[2] not in settled
            ):
                logger.debug('Manifest not understood in full', path=relative)
                incomplete.append(relative)

    return DirectDependencies(
        names=frozenset(names),
        sources=tuple(sorted(sources)),
        normalise=parser.normalise,
        incomplete=tuple(sorted(incomplete)),
    )


def _declaration_of(
    relative: str,
    text: str,
    parser: ManifestParser,
) -> Declaration | None:
    """What one manifest declares, or None if its parser gave up on it."""
    try:
        return parser.read(relative.rpartition('/')[2], text)
    except Exception as e:
        logger.warning('Manifest parse failed', path=relative, error=str(e))
        return None


def _find_manifests(
    root: Path,
    parser: ManifestParser,
    max_depth: int,
) -> Iterable[Path]:
    if not root.is_dir():
        return

    queue = [(root, 0)]
    while queue:
        directory, depth = queue.pop(0)
        try:
            entries = sorted(directory.iterdir())
        except OSError:
            continue

        for entry in entries:
            if entry.is_dir():
                if depth < max_depth and entry.name not in VENDOR_DIRS:
                    queue.append((entry, depth + 1))
            elif parser.matches(entry.name):
                yield entry
