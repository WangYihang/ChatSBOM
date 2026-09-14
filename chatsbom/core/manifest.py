"""Direct-dependency extraction from package manifests.

Syft reads lockfiles, so an SBOM is the resolved dependency closure: a
Rails app's SBOM lists `mail` whether the project asked for it or merely
inherited it through `actionmailer`. Answering "who actually depends on
X" needs the *declared* set, which only the manifest has.

Each ecosystem gets a parser over manifest text plus a name normaliser,
so manifest names can be compared against the names Syft reports.
"""
import json
import re
import tomllib
import xml.etree.ElementTree as ET
from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import structlog

from chatsbom.models.language import Language

logger = structlog.get_logger('manifest')

DIRECT = 'direct'
TRANSITIVE = 'transitive'
UNKNOWN = 'unknown'

# Vendored dependency trees contain their own manifests; reading them
# would mark every transitive package as direct.
VENDOR_DIRS = frozenset({
    'node_modules', 'vendor', 'third_party', '.venv', 'venv',
    'site-packages', 'bower_components', 'Pods', 'target', 'build',
})

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

_GEMFILE_RE = re.compile(r"""^\s*gem\s+['"]([^'"]+)['"]""", re.MULTILINE)
_GEMSPEC_RE = re.compile(
    r"""add(?:_runtime|_development)?_dependency\s*\(?\s*['"]([^'"]+)['"]""",
)


def _parse_ruby(filename: str, text: str) -> set[str]:
    if filename.endswith('.gemspec'):
        return set(_GEMSPEC_RE.findall(text))
    return set(_GEMFILE_RE.findall(_strip_ruby_comments(text)))


def _strip_ruby_comments(text: str) -> str:
    return '\n'.join(
        line for line in text.splitlines() if not line.lstrip().startswith('#')
    )


_NPM_SECTIONS = (
    'dependencies', 'devDependencies',
    'peerDependencies', 'optionalDependencies',
)


def _parse_npm(filename: str, text: str) -> set[str]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return set()
    if not isinstance(data, dict):
        return set()
    names: set[str] = set()
    for section in _NPM_SECTIONS:
        block = data.get(section)
        if isinstance(block, dict):
            names.update(block)
    return names


_GO_REQUIRE_BLOCK_RE = re.compile(r'require\s*\(\s*(.*?)\s*\)', re.DOTALL)
_GO_REQUIRE_LINE_RE = re.compile(
    r'^[ \t]*require[ \t]+([^\s()]+)[ \t]+\S+[^\n]*$', re.MULTILINE,
)


def _parse_go(filename: str, text: str) -> set[str]:
    names: set[str] = set()

    for block in _GO_REQUIRE_BLOCK_RE.findall(text):
        for line in block.splitlines():
            line = line.strip()
            if not line or line.startswith('//'):
                continue
            # go.mod marks transitive requirements explicitly.
            if '// indirect' in line:
                continue
            names.add(line.split()[0])

    for match in _GO_REQUIRE_LINE_RE.finditer(text):
        line = match.group(0)
        if '// indirect' not in line:
            names.add(match.group(1))

    return names


_CARGO_DEP_SECTIONS = (
    'dependencies', 'dev-dependencies', 'build-dependencies',
)


def _parse_cargo(filename: str, text: str) -> set[str]:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return set()

    names: set[str] = set()

    def collect(table: dict[str, Any]) -> None:
        for section in _CARGO_DEP_SECTIONS:
            block = table.get(section)
            if isinstance(block, dict):
                names.update(block)

    collect(data)
    for target in (data.get('target') or {}).values():
        if isinstance(target, dict):
            collect(target)
    for workspace in [data.get('workspace') or {}]:
        if isinstance(workspace, dict):
            collect(workspace)

    return names


_PY_NAME_RE = re.compile(r'^\s*([A-Za-z0-9][A-Za-z0-9._-]*)')


def _requirement_name(spec: str) -> str | None:
    """Leading distribution name of a PEP 508 requirement string."""
    spec = spec.split(';', 1)[0].split('#', 1)[0].strip()
    if not spec or spec.startswith('-'):
        return None
    match = _PY_NAME_RE.match(spec)
    return match.group(1) if match else None


def _parse_python(filename: str, text: str) -> set[str]:
    if filename == 'pyproject.toml':
        return _parse_pyproject(text)
    return _parse_requirements(text)


def _parse_pyproject(text: str) -> set[str]:
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return set()

    specs: list[str] = []
    project = data.get('project') or {}
    specs.extend(project.get('dependencies') or [])
    for extra in (project.get('optional-dependencies') or {}).values():
        specs.extend(extra or [])
    for group in (data.get('dependency-groups') or {}).values():
        specs.extend(g for g in (group or []) if isinstance(g, str))

    poetry = ((data.get('tool') or {}).get('poetry') or {})
    for section in ('dependencies', 'dev-dependencies'):
        block = poetry.get(section)
        if isinstance(block, dict):
            specs.extend(k for k in block if k.lower() != 'python')

    names = {_requirement_name(s) for s in specs if isinstance(s, str)}
    return {n for n in names if n}


def _parse_requirements(text: str) -> set[str]:
    names = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(('#', '-')):
            continue
        if '#egg=' in line:
            names.add(line.split('#egg=', 1)[1].strip())
            continue
        if '://' in line:
            continue
        name = _requirement_name(line)
        if name:
            names.add(name)
    return names


# Platform requirements in composer.json are not packages.
_COMPOSER_PLATFORM_RE = re.compile(
    r'^(?:php(?:-[\w.]+)?|hhvm|composer(?:-[\w.]+)?)$|^(?:ext|lib)-',
)


def _parse_composer(filename: str, text: str) -> set[str]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return set()
    if not isinstance(data, dict):
        return set()
    names: set[str] = set()
    for section in ('require', 'require-dev'):
        block = data.get(section)
        if isinstance(block, dict):
            names.update(
                k for k in block if not _COMPOSER_PLATFORM_RE.match(k)
            )
    return names


_GRADLE_RE = re.compile(
    r"""(?:implementation|api|compileOnly|runtimeOnly|testImplementation)"""
    r"""\s*\(?\s*['"]([^'"]+)['"]""",
)


def _parse_java(filename: str, text: str) -> set[str]:
    if filename == 'pom.xml':
        return _parse_pom(text)
    return _parse_gradle(text)


def _parse_pom(text: str) -> set[str]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return set()
    names = set()
    for dep in root.iter():
        if not dep.tag.endswith('dependency'):
            continue
        for child in dep:
            if child.tag.endswith('artifactId') and child.text:
                names.add(child.text.strip())
    return names


def _parse_gradle(text: str) -> set[str]:
    names = set()
    for coord in _GRADLE_RE.findall(text):
        parts = coord.split(':')
        if len(parts) >= 2:
            names.add(parts[1])
    return names


# --- parser registry -------------------------------------------------------

@dataclass(frozen=True)
class ManifestParser:
    """Which files declare dependencies, and how to read them."""

    filenames: tuple[str, ...]
    extract: Callable[[str, str], set[str]]
    normalise: Callable[[str], str] = _identity
    suffixes: tuple[str, ...] = ()

    def matches(self, filename: str) -> bool:
        return filename in self.filenames or filename.endswith(self.suffixes)

    def parse(self, filename: str, text: str) -> set[str]:
        """Declared names, normalised so they compare against SBOM names."""
        return {self.normalise(n) for n in self.extract(filename, text) if n}


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

    def relationship_of(self, artifact_name: str) -> str:
        """Classify a name from the SBOM against the declared set.

        With no manifest read, everything is `unknown` — absence of
        evidence is not evidence of transitivity.
        """
        if not self.sources:
            return UNKNOWN
        return DIRECT if self.normalise(artifact_name) in self.names else TRANSITIVE


def resolve_relationships(
    content_dir: Path,
    language: Language,
    max_depth: int = 3,
) -> DirectDependencies:
    """Read every manifest under `content_dir` and merge the declared sets.

    Monorepos declare dependencies in nested manifests, so the search
    descends a few levels, skipping vendored trees.
    """
    parser = parser_for(language)
    names: set[str] = set()
    sources: list[str] = []

    for path in _find_manifests(content_dir, parser, max_depth):
        try:
            if path.stat().st_size > MAX_MANIFEST_BYTES:
                continue
            text = path.read_text(encoding='utf-8')
        except (OSError, UnicodeDecodeError) as e:
            logger.debug('Unreadable manifest', path=str(path), error=str(e))
            continue

        try:
            found = parser.parse(path.name, text)
        except Exception as e:
            logger.warning(
                'Manifest parse failed',
                path=str(path), error=str(e),
            )
            continue

        sources.append(str(path.relative_to(content_dir)))
        names.update(found)

    return DirectDependencies(
        names=frozenset(names),
        sources=tuple(sorted(sources)),
        normalise=parser.normalise,
    )


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
