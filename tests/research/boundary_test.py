"""The core never imports the research tools (#167).

They left it for a command of their own, `chatsbom-research`, and their
libraries for an extra of their own, `research`: the collector, the
warehouse, the snapshot and the web service never run them. What keeps
them out is that nothing else imports them, and nothing else imports
what only their extra installs. Checked on the source, every import of
every module wherever it is made, and in a fresh interpreter, on what
starting the core CLI loads.

And the other way: what only they use is theirs, in chatsbom/research/,
and they use nothing of the pipeline that is to be deleted (#155, 6e).
"""
import ast
import importlib.util
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

from chatsbom.services import github_service as pipeline
from tests.dependencies_test import CORE
from tests.dependencies_test import declared
from tests.dependencies_test import distributions
from tests.dependencies_test import EXTRA_OF
from tests.dependencies_test import modules
from tests.dependencies_test import ROOT
from tests.dependencies_test import third_party_imports

RESEARCH = 'chatsbom.research'


def is_research(module: str) -> bool:
    return module == RESEARCH or module.startswith(f'{RESEARCH}.')


def first_party_imports(
    module: str, path: Path,
) -> Iterator[tuple[str, int]]:
    """Each module of ours `module` imports, by its absolute name, and
    the line: `from .x import y` resolved against `module`'s package,
    both what `from` names and what it names then, which may be a module
    of its own; and a name given to `importlib.import_module` or
    `__import__`."""
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    package = (
        module if path.name == '__init__.py'
        else module.rpartition('.')[0]
    )
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            base = importlib.util.resolve_name(
                '.' * node.level + (node.module or ''), package,
            )
            names = [base, *(f'{base}.{alias.name}' for alias in node.names)]
        elif isinstance(node, ast.Call) and (named := imported(node)):
            names = [named]
        else:
            continue
        for name in names:
            if name == 'chatsbom' or name.startswith('chatsbom.'):
                yield name, node.lineno


def imported(call: ast.Call) -> str | None:
    """The module `call` imports, where it is one that imports a module
    its first argument names: `importlib.import_module('...')`,
    `import_module('...')` or `__import__('...')`."""
    function = call.func
    importing = (
        isinstance(function, ast.Name)
        and function.id in ('__import__', 'import_module')
        or isinstance(function, ast.Attribute)
        and function.attr == 'import_module'
    )
    if not (importing and call.args):
        return None
    first = call.args[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return None


def test_no_module_outside_them_imports_them():
    found = [
        f'{path.relative_to(ROOT)}:{line} imports {name}'
        for module, path in modules() if not is_research(module)
        for name, line in first_party_imports(module, path)
        if is_research(name)
    ]
    assert found == [], '\n'.join(found)


def test_the_check_sees_every_way_to_import_them(tmp_path):
    """Or the test above would pass on a check that saw nothing."""
    probe = tmp_path / 'probe.py'
    probe.write_text(
        'import chatsbom.research.frameworks\n'
        'from chatsbom import research\n'
        'from ..research.models import analysis\n'
        'from . import typer\n'
        'def later():\n'
        '    import importlib\n'
        "    importlib.import_module('chatsbom.research.commands')\n"
        "    __import__('chatsbom.research')\n",
        encoding='utf-8',
    )

    found = first_party_imports('chatsbom.commands.probe', probe)

    assert sorted({line for name, line in found if is_research(name)}) == [
        1, 2, 3, 7, 8,
    ]


def test_the_research_modules_are_where_the_check_looks():
    """Each module the research tools are, by the name the check reads,
    and their own imports of each other, which it sees."""
    research = {module for module, _ in modules() if is_research(module)}
    assert {
        f'{RESEARCH}.__main__',
        f'{RESEARCH}.commands.classify',
        f'{RESEARCH}.commands.openapi.candidates',
        f'{RESEARCH}.commands.readme',
        f'{RESEARCH}.services.openapi_service',
    } <= research
    main = ROOT / 'chatsbom' / 'research' / '__main__.py'
    assert any(
        is_research(name)
        for name, _ in first_party_imports(f'{RESEARCH}.__main__', main)
    )


def test_only_the_research_tools_need_the_research_extra():
    """The modules that may import what it installs are theirs."""
    needing = {
        module for module, extra in EXTRA_OF.items() if extra == 'research'
    }
    assert needing
    assert {module for module in needing if not is_research(module)} == set()


def test_the_core_imports_nothing_the_research_extra_alone_installs():
    """The web service's modules import openai, which the `web` extra
    installs too, for its own chat: that is theirs, not research's."""
    found = declared()
    research = found['research'] - found[CORE]
    assert {'instructor', 'openai', 'pandas', 'tiktoken'} <= research
    imported = []
    for module, path in modules():
        if is_research(module):
            continue
        extra = EXTRA_OF.get(module)
        allowed = found[CORE] | (found.get(extra, set()) if extra else set())
        for name, line in third_party_imports(path):
            providers = distributions(name)
            if providers & research and not providers & allowed:
                imported.append(
                    f'{path.relative_to(ROOT)}:{line} imports {name}',
                )
    assert imported == [], '\n'.join(imported)


#: Imports the core CLI, and prints each research module that loaded.
PROBE = """
import json
import sys

import chatsbom.__main__

print(json.dumps(sorted(
    name for name in sys.modules
    if name == 'chatsbom.research' or name.startswith('chatsbom.research.')
)))
"""


def test_starting_the_core_cli_loads_none_of_them(tmp_path):
    """What the source does not show, an import made of a name put
    together as it runs, the interpreter does."""
    result = subprocess.run(
        [sys.executable, '-c', PROBE],
        cwd=tmp_path, capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout.splitlines()[-1]) == []


# --- what only they use is theirs ---------------------------------------------

def test_what_they_import_of_the_core_the_core_imports_too():
    """A module of the core that only the research tools import is
    research, in the core: as the warehouse's queries of the frameworks
    each project uses, and the frameworks themselves, were."""
    known = {module for module, _ in modules()}
    core: set[str] = set()
    theirs: set[str] = set()
    for module, path in modules():
        names = {
            name for name, _ in first_party_imports(module, path)
            if name in known
        }
        (theirs if is_research(module) else core).update(names)

    assert sorted(
        name for name in theirs - core if not is_research(name)
    ) == []
    # The introspection itself, so that seeing nothing cannot pass.
    assert {'chatsbom.core.config', 'chatsbom.warehouse'} <= theirs & core


#: What the research tools took from the pipeline, which is to be
#: deleted (#155, 6e): its GitHub client, for the README fetch, and its
#: container, for the configuration alone.
PIPELINE = ('chatsbom.services.github_service', 'chatsbom.core.container')


def test_they_use_nothing_the_pipeline_takes_with_it():
    """The README fetch `readme` and `classify` share is theirs
    (chatsbom/research/services/github_service.py), and the
    configuration is `get_config`'s: so the pipeline's client and its
    container can go with it."""
    found = [
        f'{path.relative_to(ROOT)}:{line} imports {name}'
        for module, path in modules() if is_research(module)
        for name, line in first_party_imports(module, path)
        if any(name == gone or name.startswith(f'{gone}.') for gone in PIPELINE)
    ]
    assert found == [], '\n'.join(found)


def test_the_pipelines_client_fetches_no_readme():
    """One fetch, theirs, where two would drift apart."""
    from chatsbom.research.services.github_service import GitHubService

    assert not hasattr(pipeline.GitHubService, 'get_readme')
    assert hasattr(GitHubService, 'get_readme')
