"""`sbom generate` end to end, over a faked Syft.

Only `subprocess.run` is faked, so the real service, cache, ledgers and
paths do the work, under a fresh working directory.

A repository's SBOM could go wrong for good in three ways (#13):

- The SBOM and its Syft cache entry were written in place. A kill or a
  full disk midway left a prefix. The next run's skip accepted it, since
  it only asked for a non-empty file, and `db index` then failed that
  repository on every run with "unreadable sbom".
- A cache entry was used whenever it existed. A zero-byte or cut-short
  entry was copied out as the SBOM and reported as generated, every time.
- Syft ran with no timeout, so one hung scan held a worker for good.

`sbom generate` walks the content roots (`06-github-content/<id>/<sha>`)
rather than a per-language list, and an SBOM is current while it is
whole, written by the Syft now running, and newer than every file it
was generated from.
"""
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import get_config
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.services import sbom_service
from chatsbom.services.sbom_service import content_fingerprint
from chatsbom.services.sbom_service import is_current_sbom

SYFT_VERSION = '1.52.0'
SHA = '0123456789abcdef0123456789abcdef01234567'

#: Repository name -> id.
REPOSITORIES = {'a': 1, 'b': 2}
NAMES = {str(v): k for k, v in REPOSITORIES.items()}

runner = CliRunner()


def syft_document(project: str = 'a', version: str = SYFT_VERSION) -> str:
    """What `syft dir:<project> -o json` prints, trimmed to the keys that
    anything reads. It is compact, on one line and ends in a newline, as
    Syft writes it, and its keys come in Syft's order. `project` names
    the scan, so a test can tell which one produced a file, and
    `version` is the Syft its descriptor says wrote it."""
    return json.dumps(
        {
            'artifacts': [{
                'id': f'{project}-requests',
                'name': 'requests',
                'version': '2.31.0',
                'type': 'python',
                'foundBy': 'python-package-cataloger',
                'purl': 'pkg:pypi/requests@2.31.0',
            }],
            'artifactRelationships': [],
            'source': {
                'id': project,
                'name': project,
                'type': 'directory',
                'metadata': {'path': project},
            },
            'distro': {},
            'descriptor': {'name': 'syft', 'version': version},
            'schema': {
                'version': '16.1.10',
                'url': 'https://raw.githubusercontent.com/anchore/syft/'
                'main/schema/json/schema-16.1.10.json',
            },
        },
        separators=(',', ':'),
    ) + '\n'


def cut_short(document: str) -> str:
    """`document` as a write killed partway through left it."""
    return document[:document.index('"descriptor"')]


def cut_after_a_brace(document: str) -> str:
    """Cut where the cheap check cannot see it: just after a `}`."""
    return document[:document.index(',"descriptor"')]


class FakeSyft:
    """`subprocess.run` as the SBOM stage calls it: `syft dir:... -o json`.

    It answers with `syft_document` for the project scanned. A project in
    `hangs` never finishes. Given a timeout, the call raises
    `TimeoutExpired` once it passes, as `subprocess.run` does. Given none,
    it would wait forever, and says so instead.
    """

    def __init__(self) -> None:
        self.scanned: list[str] = []
        self.timeouts: list[Any] = []
        self.hangs: set[str] = set()

    def __call__(
        self, command: list[str], **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == 'syft' and command[2:] == ['-o', 'json'], command
        # dir:<cwd>/data/06-github-content/<repository_id>/<sha>
        project = NAMES[Path(command[1].removeprefix('dir:')).parts[-2]]
        timeout = kwargs.get('timeout')
        self.scanned.append(project)
        self.timeouts.append(timeout)
        if project in self.hangs:
            if timeout is None:
                raise AssertionError(f'syft hung on {project}, for good')
            raise subprocess.TimeoutExpired(command, timeout)
        return subprocess.CompletedProcess(
            command, 0, stdout=syft_document(project), stderr='',
        )


@pytest.fixture
def syft(tmp_path, monkeypatch, no_database) -> FakeSyft:
    """A fresh working directory, container and Syft for each test.

    `data/` and `.cache/` both resolve against the working directory, so
    nothing here reaches the real ones, and no real Syft is needed. Nor
    any database: the records go to the ledger alone.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setattr(sbom_service, 'check_syft_installed', lambda: True)
    monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: SYFT_VERSION)
    fake = FakeSyft()
    monkeypatch.setattr(sbom_service.subprocess, 'run', fake)
    return fake


def _project(name: str) -> Path:
    return Path(f'data/06-github-content/{REPOSITORIES[name]}/{SHA}')


def _downloaded(*names: str) -> None:
    """What the content stage left: each project's manifest."""
    for name in names:
        project = _project(name)
        project.mkdir(parents=True, exist_ok=True)
        (project / 'requirements.txt').write_text(
            f'requests==2.31.0  # {name}\n', encoding='utf-8',
        )


def _sbom(name: str) -> Path:
    return Path(f'data/07-sbom/{REPOSITORIES[name]}/{SHA}/sbom.json')


def _cache_entry(name: str) -> Path:
    return get_config().paths.get_sbom_cache_path(
        REPOSITORIES[name], content_fingerprint(_project(name)), SYFT_VERSION,
    )


def _generated(name: str, stored: str) -> None:
    """What an earlier run left for `name`: the SBOM, holding `stored`,
    written after the content it was generated from."""
    sbom = _sbom(name)
    sbom.parent.mkdir(parents=True, exist_ok=True)
    sbom.write_text(stored, encoding='utf-8')


def _recorded() -> set[int]:
    """The repository ids with an SBOM a later run would skip."""
    return {
        repository_id for name, repository_id in REPOSITORIES.items()
        if is_current_sbom(
            _sbom(name), _project(name), syft_version=SYFT_VERSION,
        )
    }


def generate(*args: str):
    return runner.invoke(app, ['sbom', 'generate', *args])


def said(result) -> str:
    """What a command printed, on one line: Rich wraps a long one."""
    return ' '.join(result.output.split())


# --- a stored SBOM is trusted only if it looks whole ------------------------

def test_a_stored_sbom_cut_short_is_regenerated(syft):
    """An SBOM is there, and says otherwise.

    Its size was all the skip asked about, so a prefix passed, and
    `db index` failed the repository on every run with "unreadable sbom".
    """
    _downloaded('a', 'b')
    _generated('a', cut_short(syft_document('a')))
    _generated('b', syft_document('b'))

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['a'], 'b is whole, and is not scanned again'
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')
    assert _recorded() == {1, 2}


# --- the Syft cache ---------------------------------------------------------

@pytest.mark.parametrize(
    'entry',
    [
        '',
        cut_short(syft_document()),
        cut_after_a_brace(syft_document()),
        json.dumps({'cached': 'sbom'}),
    ],
    ids=['zero-byte', 'cut-short', 'cut-after-a-brace', 'not-a-syft-document'],
)
def test_an_unusable_cache_entry_is_a_miss_and_is_replaced(syft, entry):
    """A cache entry was used whenever it existed, so a zero-byte one was
    copied out as the SBOM and counted as generated, on every run. It is
    parsed in full, so a cut that the cheap check cannot see is caught
    here as well."""
    _downloaded('a')
    cache = _cache_entry('a')
    cache.parent.mkdir(parents=True)
    cache.write_text(entry, encoding='utf-8')

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['a']
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')
    assert cache.read_text(encoding='utf-8') == syft_document('a')


def test_a_whole_cache_entry_is_still_used(syft):
    _downloaded('a')
    cache = _cache_entry('a')
    cache.parent.mkdir(parents=True)
    cache.write_text(syft_document('cached'), encoding='utf-8')

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == []
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('cached')
    assert _recorded() == {1}


# --- nothing is left half written -------------------------------------------

def test_an_sbom_cut_short_by_a_full_disk_is_not_left_behind(syft, full_disk):
    _downloaded('a')
    full_disk.fill(_sbom('a').parent)

    result = generate()

    assert result.exit_code == 0, result.output
    assert list(_sbom('a').parent.iterdir()) == [], 'nor a temporary file'
    assert _recorded() == set()

    full_disk.free()
    generate()

    assert syft.scanned == ['a', 'a']
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')
    assert _recorded() == {1}


def test_a_cache_entry_cut_short_by_a_full_disk_is_not_left_behind(
    syft, full_disk,
):
    """The SBOM itself is still written and recorded: the cache only
    saves a later scan."""
    _downloaded('a')
    cache = _cache_entry('a')
    full_disk.fill(cache.parent)

    result = generate()

    assert result.exit_code == 0, result.output
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')
    assert _recorded() == {1}
    assert list(cache.parent.iterdir()) == []


# --- a hung scan ------------------------------------------------------------

def test_a_hung_scan_fails_that_repository_and_the_batch_goes_on(syft):
    _downloaded('a', 'b')
    syft.hangs.add('a')

    result = generate('--syft-timeout', '5')

    assert result.exit_code == 0, result.output
    assert sorted(syft.scanned) == ['a', 'b']
    assert syft.timeouts == [5, 5]
    assert not _sbom('a').exists()
    assert _sbom('b').read_text(encoding='utf-8') == syft_document('b')
    assert _recorded() == {2}, 'a failed, so it is not recorded as done'

    # And so the next run tries it again.
    syft.hangs.clear()
    generate()

    assert _recorded() == {1, 2}


def test_a_scan_has_a_timeout_unless_told_otherwise(syft):
    """Ten minutes. Without one, a hung scan held its worker for good."""
    _downloaded('a')

    generate()

    assert syft.timeouts == [600]


# --- an SBOM is current only while its content is unchanged -----------------

def test_a_content_root_given_more_manifests_is_scanned_again(syft):
    """The content stage used to fetch manifests at the root only; now
    it adds every one the tree lists below it, to the same commit's
    content root. An SBOM there from before would have been skipped for
    good, keeping the root-only scan."""
    _downloaded('a')
    _generated('a', syft_document('a'))
    assert generate().exit_code == 0
    assert syft.scanned == []

    nested = _project('a') / 'server' / 'pom.xml'
    nested.parent.mkdir()
    nested.write_text('<project/>\n', encoding='utf-8')
    later = time.time() + 5
    os.utime(nested, (later, later))

    assert generate().exit_code == 0
    assert syft.scanned == ['a']


def test_every_content_root_is_found_without_a_list(syft):
    """No per-language list: a repository needs no language to be
    scanned, and a root that is not a `<id>/<sha>` scan is not one."""
    _downloaded('a', 'b')
    Path('data/06-github-content/python').mkdir(parents=True)
    Path('data/06-github-content/python.jsonl').write_text('')

    assert generate().exit_code == 0
    assert sorted(syft.scanned) == ['a', 'b']


def test_repos_file_narrows_the_scan(syft, tmp_path):
    _downloaded('a', 'b')
    with Ledger(Path('data/ledger.sqlite3')) as ledger:
        ledger.track(1, 'o', 'a', '')
        ledger.track(2, 'o', 'b', '')
    wanted = tmp_path / 'repos.txt'
    wanted.write_text('o/b\n', encoding='utf-8')

    assert generate('--repos-file', str(wanted)).exit_code == 0
    assert syft.scanned == ['b']


# --- an SBOM is current only while the Syft now running wrote it ------------

#: The Syft the collector ran before 1.52.0.
OLD_SYFT = '1.41.2'


def test_an_sbom_another_syft_wrote_is_regenerated(syft):
    """Skipped whatever wrote it, every SBOM an upgrade found kept the old
    Syft for good, while each new root got the new one, and the corpus
    mixed the two: 1.52.0 leaves out yarn.lock's dev-only packages (138
    rows to 70) and reads bun.lock, where 1.41.2 did neither. `a` is
    whole and newer than its content, and is scanned again all the
    same."""
    _downloaded('a', 'b')
    _generated('a', syft_document('a', version=OLD_SYFT))
    _generated('b', syft_document('b'))

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['a'], "b is this Syft's, and is not scanned again"
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')
    assert _recorded() == {1, 2}


def test_it_is_regenerated_once(syft):
    """What it is regenerated with records the Syft now running, so the
    next run skips it: an upgrade costs one scan of each root."""
    _downloaded('a')
    _generated('a', syft_document('a', version=OLD_SYFT))
    assert generate().exit_code == 0

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['a']
    assert 'Nothing to scan. 1 SBOM(s) are current.' in said(result)


def test_an_sbom_the_running_syft_wrote_is_still_skipped(syft):
    _downloaded('a', 'b')
    _generated('a', syft_document('a'))
    _generated('b', syft_document('b'))

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == []
    assert 'Nothing to scan. 2 SBOM(s) are current.' in said(result)


@pytest.mark.parametrize(
    'stored',
    [
        {'artifacts': []},
        {'artifacts': [], 'descriptor': {'name': 'syft'}},
    ],
    ids=['no-descriptor', 'no-version'],
)
def test_a_whole_document_that_names_no_syft_version_is_regenerated(
    syft, stored,
):
    """Whole, and newer than its content, but nothing says which Syft
    wrote it, so nothing says it is this one's."""
    _downloaded('a')
    _generated('a', json.dumps(stored) + '\n')

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['a']
    assert _sbom('a').read_text(encoding='utf-8') == syft_document('a')


def test_force_still_scans_every_root(syft):
    _downloaded('a', 'b')
    _generated('a', syft_document('a', version=OLD_SYFT))
    _generated('b', syft_document('b'))

    assert generate('--force').exit_code == 0
    assert sorted(syft.scanned) == ['a', 'b']


def test_with_the_running_version_unknown_times_alone_decide(
    syft, monkeypatch,
):
    """`syft version` failed, or said nothing that reads as a version.
    Judged against nothing, every SBOM would be regenerated, and judged
    against nothing again on the next run: so the times decide, as they
    did before versions were compared."""
    monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: None)
    _downloaded('a', 'b')
    _generated('a', syft_document('a', version=OLD_SYFT))

    result = generate()

    assert result.exit_code == 0, result.output
    assert syft.scanned == ['b'], 'a is whole and newer than its content'
