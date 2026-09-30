"""The cutover (#171): `chatsbom collect` replaced the old pipeline, and
nothing of the pipeline stays for compatibility.

Its commands went, the modules only they used went with them, and
nothing imports any of it, whatever way it is imported; nor does
anything run the collector's loop, the scripts and units that ran it
went too. The commands the CLI keeps are cli_surface_test's.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.dependencies_test import modules
from tests.research.boundary_test import first_party_imports

ROOT = Path(__file__).resolve().parent.parent

#: What went, by module or package.
GONE = (
    # The old pipeline's commands.
    'chatsbom.commands.github',
    'chatsbom.commands.queue',
    'chatsbom.commands.run',
    'chatsbom.commands.sbom.generate',
    'chatsbom.commands.data.migrate_layout',
    'chatsbom.commands.data.slim',
    # What they alone used: the ledger, the due set derived for it, its
    # metrics, the layout migration, the stage services and the
    # container that made them, and their GitHub client, with its
    # conditional requests and its counters. The HTTP client under it
    # is the research tools' (`research/client.py`).
    'chatsbom.core.client',
    'chatsbom.core.conditional',
    'chatsbom.core.container',
    'chatsbom.core.due',
    'chatsbom.core.github',
    'chatsbom.core.ledger',
    'chatsbom.core.metrics',
    'chatsbom.core.migrate_layout',
    'chatsbom.core.stats',
    'chatsbom.core.storage',
    'chatsbom.services.commit_service',
    'chatsbom.services.content_service',
    'chatsbom.services.depgraph_stage',
    'chatsbom.services.git_service',
    'chatsbom.services.github_service',
    'chatsbom.services.release_service',
    'chatsbom.services.repo_service',
    'chatsbom.services.run_service',
    'chatsbom.services.search_service',
    'chatsbom.services.sync_service',
)

#: What went, by file: the collector's loop, and the timers that ran the
#: pipeline's slices and its retention.
GONE_FILES = (
    'deploy/collector-loop.sh',
    'deploy/systemd/chatsbom-sync@.service',
    'deploy/systemd/chatsbom-sync@.timer',
    'deploy/systemd/chatsbom-prune@.service',
    'deploy/systemd/chatsbom-prune@.timer',
)


@pytest.mark.parametrize('module', GONE)
def test_a_deleted_module_is_gone(module: str) -> None:
    """No source of it is left. A checkout that pulled the deletion
    keeps the package's `__pycache__/`, which git ignores, so its
    directory may stay behind with nothing of it in it."""
    path = ROOT / (module.replace('.', '/') + '.py')
    package = ROOT / module.replace('.', '/')
    assert not path.exists()
    assert not any(package.rglob('*.py'))


@pytest.mark.parametrize('name', GONE_FILES)
def test_a_deleted_file_is_gone(name: str) -> None:
    assert not (ROOT / name).exists()


def test_no_module_imports_one_deleted() -> None:
    found = [
        f'{path.relative_to(ROOT)}:{line} imports {name}'
        for module, path in modules()
        for name, line in first_party_imports(module, path)
        if any(name == gone or name.startswith(f'{gone}.') for gone in GONE)
    ]
    assert found == [], '\n'.join(found)


def test_no_test_imports_one_deleted() -> None:
    """The tests of what went went with it."""
    found = []
    for path in sorted((ROOT / 'tests').rglob('*.py')):
        module = '.'.join(path.relative_to(ROOT).with_suffix('').parts)
        found += [
            f'{path.relative_to(ROOT)}:{line} imports {name}'
            for name, line in first_party_imports(module, path)
            if any(
                name == gone or name.startswith(f'{gone}.') for gone in GONE
            )
        ]
    assert found == [], '\n'.join(found)
