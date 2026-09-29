"""What chatsbom imports is what pyproject.toml declares, and where (#27).

Six declared packages were never imported: pygithub, ratelimit, whose
last release was in 2018, prompt_toolkit, mcp, httpx, and litellm once
`openapi stats` stopped asking it for context windows (#26). One was
declared twice. And two that are imported were not declared at all:
pydantic, which the models are written in, and urllib3, which
`core/client.py` builds its retries from. Both arrived through something
else, pydantic through the LLM stack, so moving that stack out of the
core would have broken the collector's own models.

What a single command used, everyone installed: the Claude Agent SDK,
218 MB, for `chat`; pyarrow, 152 MB, for `export parquet`; pandas and
matplotlib for two `openapi` commands. Those are extras now. So an
import is checked against the core dependencies, or, in a module only a
command needing an extra runs, against the core and that extra.

Import names are mapped to distributions by the metadata of the
environment the suite runs in, which has every extra: the dev group
brings them all.
"""
import ast
import sys
import tomllib
from collections.abc import Iterator
from importlib.metadata import packages_distributions
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = ROOT / 'chatsbom'
PROJECT = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
NAME = canonicalize_name(PROJECT['project']['name'])

#: What `pip install 'chatsbom[...]'` takes. Documented, so a rename
#: breaks every install command written down anywhere.
EXTRAS = {'chat', 'classify', 'openapi', 'export', 'web', 'all'}

#: The modules that import an extra's libraries, and that extra. Only a
#: command that checks for the extra before anything else runs them
#: (extras_test). Every other module is core, and imports only what
#: the core dependencies install.
EXTRA_OF = {
    'chatsbom.commands.chat_agent': 'chat',
    'chatsbom.commands.chat_tui': 'chat',
    'chatsbom.services.github_analysis_service': 'classify',
    'chatsbom.commands.openapi.drift': 'openapi',
    'chatsbom.commands.openapi.list_paths': 'openapi',
    'chatsbom.commands.openapi.stats': 'openapi',
    'chatsbom.export.parquet': 'export',
    'chatsbom.server.app': 'web',
    'chatsbom.server.ask': 'web',
    'chatsbom.server.challenge': 'web',
    'chatsbom.server.model': 'web',
}

CORE = 'core'

#: Each importable name, and the distributions that install it.
DISTRIBUTIONS = packages_distributions()


def optional() -> dict[str, list[str]]:
    return PROJECT['project'].get('optional-dependencies', {})


def names_in(specs: list[str]) -> list[str]:
    """Each requirement's distribution, as it is compared."""
    return [canonicalize_name(Requirement(spec).name) for spec in specs]


def installed_by(specs: list[str]) -> set[str]:
    """The distributions `specs` install, `chatsbom[...]` expanded."""
    names: set[str] = set()
    for spec in specs:
        requirement = Requirement(spec)
        name = canonicalize_name(requirement.name)
        if name == NAME:
            for extra in requirement.extras:
                names |= installed_by(optional()[extra])
        else:
            names.add(name)
    return names


def declared() -> dict[str, set[str]]:
    """What the core installs, and each extra on top of it."""
    found = {CORE: installed_by(PROJECT['project']['dependencies'])}
    for extra, specs in optional().items():
        found[extra] = installed_by(specs)
    return found


def modules() -> Iterator[tuple[str, Path]]:
    """Each module of the package, by dotted name."""
    for path in sorted(PACKAGE.rglob('*.py')):
        parts = path.relative_to(ROOT).with_suffix('').parts
        if parts[-1] == '__init__':
            parts = parts[:-1]
        yield '.'.join(parts), path


def third_party_imports(path: Path) -> Iterator[tuple[str, int]]:
    """Each import of neither ours nor the standard library's, by the
    top-level name it imports and its line: at module level, in a
    function, or for type checking alone, since each is an import of it.
    """
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and not node.level and node.module:
            names = [node.module]
        else:
            continue
        for name in names:
            top = name.partition('.')[0]
            if top != 'chatsbom' and top not in sys.stdlib_module_names:
                yield top, node.lineno


def distributions(name: str) -> set[str]:
    """The distributions that install the importable `name`."""
    return {canonicalize_name(d) for d in DISTRIBUTIONS.get(name, [])}


def test_the_extras_are_the_documented_ones():
    assert set(optional()) == EXTRAS


def test_the_map_names_modules_and_extras_that_exist():
    """Or a module renamed out from under it would count as core."""
    names = {module for module, _ in modules()}
    assert set(EXTRA_OF) - names == set()
    assert set(EXTRA_OF.values()) - set(optional()) == set()


def test_every_import_is_declared_where_its_module_runs():
    found = declared()
    undeclared = []
    for module, path in modules():
        extra = EXTRA_OF.get(module)
        allowed = found[CORE] | (found.get(extra, set()) if extra else set())
        scope = f'the core or `{extra}`' if extra else 'the core'
        for name, line in third_party_imports(path):
            providers = distributions(name)
            if not providers & allowed:
                undeclared.append(
                    f"{path.relative_to(ROOT)}:{line} imports {name} "
                    f"({', '.join(sorted(providers)) or 'nothing installs it'}), "
                    f'which {scope} does not declare',
                )
    assert not undeclared, '\n'.join(undeclared)


def test_every_dependency_is_imported_where_it_is_declared():
    """One no module imports is weight for nothing, and one in the core
    that only an extra's modules import is weight for everyone else:
    the collector image carried the Claude Agent SDK and pyarrow, and
    ran neither."""
    imported: dict[str, set[str]] = {}
    for module, path in modules():
        scope = EXTRA_OF.get(module, CORE)
        for name, _ in third_party_imports(path):
            imported.setdefault(scope, set()).update(distributions(name))

    direct = {CORE: PROJECT['project']['dependencies'], **optional()}
    unused = []
    for scope, specs in direct.items():
        for name in names_in(specs):
            if name == NAME or name in imported.get(scope, set()):
                continue
            users = sorted(s for s, names in imported.items() if name in names)
            by = (
                f"only {' and '.join(users)} modules import it" if users
                else 'no module imports it'
            )
            unused.append(f'{name}, in {scope}: {by}')
    assert not unused, '\n'.join(unused)


def test_nothing_is_declared_twice():
    """Twice in one list, or in an extra as well as in the core, where
    installing the extra adds nothing."""
    core = names_in(PROJECT['project']['dependencies'])
    twice = {name for name in core if core.count(name) > 1}
    assert twice == set()
    for extra, specs in optional().items():
        names = names_in(specs)
        assert {name for name in names if names.count(name) > 1} == set()
        assert set(names) & set(core) == set(), extra


def test_all_is_every_extra():
    found = declared()
    extras = [
        names for extra, names in found.items() if extra not in (CORE, 'all')
    ]
    assert extras
    assert found.get('all') == set().union(*extras)


def test_the_development_environment_has_every_extra():
    """`uv sync` makes the environment the suite runs in, and the suite
    imports pandas, pyarrow and textual, among the rest. The dev group
    brings every extra, so a plain `uv sync`, as CI runs it, is enough;
    `--no-dev`, as the image and the systemd units sync, is the core."""
    every = set().union(
        *(names for extra, names in declared().items() if extra != CORE),
    )
    dev = installed_by(PROJECT['dependency-groups']['dev'])
    assert every
    assert every - dev == set()
