"""The content stage: the manifests a repository's tree lists, fetched
at the commit and stored at their own paths (#51).

It used to ask for a fixed list of names at the repository root, chosen
by the repository's language. Now the list comes from the stored tree
(`core/discovery.py`), every file is stored under its path in the
repository, and what was fetched and left out is written beside the
tree as `manifests.json`.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import patch
from urllib.parse import unquote

import pytest
import requests

from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.repository import Repository
from chatsbom.services.content_service import ContentFetchError
from chatsbom.services.content_service import ContentService
from chatsbom.services.content_service import ContentStats
from chatsbom.services.content_service import raw_url

SHA = '0123456789abcdef0123456789abcdef01234567'
BASE = f'https://raw.githubusercontent.com/owner/repo/{SHA}/'


class FakeResponse:
    def __init__(self, status: int, body: bytes = b'', declare: bool = True):
        self.status_code = status
        self._body = body
        self.headers = (
            {'Content-Length': str(len(body))} if declare and status == 200
            else {}
        )
        self.read = 0

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def iter_content(self, size: int):
        for start in range(0, len(self._body), size):
            chunk = self._body[start:start + size]
            self.read += len(chunk)
            yield chunk


class FakeRaw:
    """`raw.githubusercontent.com` at one commit: path -> body, a status,
    or an exception to raise."""

    def __init__(self, files: dict[str, Any], declare: bool = True):
        self.files = files
        self.declare = declare
        self.urls: list[str] = []
        self.streamed: list[bool] = []

    def get(self, url: str, timeout: Any = None, stream: bool = False):
        self.urls.append(url)
        self.streamed.append(stream)
        # https://raw.githubusercontent.com/<owner>/<repo>/<sha>/<path>
        prefix = 'https://raw.githubusercontent.com/'
        assert url.startswith(prefix), url
        _owner, _repo, sha, quoted = url[len(prefix):].split('/', 3)
        assert sha == SHA, url
        path = unquote(quoted)
        answer = self.files.get(path, 404)
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, int):
            return FakeResponse(answer)
        return FakeResponse(200, answer, self.declare)


@pytest.fixture
def paths(tmp_path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path)


def _service(paths: PathConfig, **kwargs: Any) -> ContentService:
    with patch('chatsbom.services.content_service.get_config') as config:
        config.return_value.paths = paths
        return ContentService('fake_token', **kwargs)


def _repository(repository_id: int = 1) -> Repository:
    repository = Repository.model_validate({
        'id': repository_id, 'owner': 'owner', 'repo': 'repo',
        'stargazers_count': 10, 'default_branch': 'main',
    })
    repository.download_target = DownloadTarget(
        ref='v1.0.0', ref_type='release',
        commit_sha=SHA, commit_sha_short=SHA[:7],
    )
    return repository


def _tree(paths: PathConfig, *files: str, repository_id: int = 1) -> None:
    stored = paths.tree_file(repository_id, SHA)
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_text(''.join(f'{f}\n' for f in files), encoding='utf-8')


def _root(paths: PathConfig) -> Path:
    return paths.content_root(1, SHA)


def _document(paths: PathConfig) -> dict[str, Any]:
    return json.loads(
        paths.discovery_file(1, SHA).read_text(encoding='utf-8'),
    )


def test_content_service_init(paths):
    assert _service(
        paths,
    ).session.headers['Authorization'] == 'Bearer fake_token'


def test_content_service_timeout():
    service = ContentService('token', timeout=30)
    assert service.timeout == 30


def test_the_manifests_below_the_root_are_fetched_at_their_paths(paths):
    """halo's `application/build.gradle`, appsmith's
    `app/client/package.json` and `app/server/pom.xml`: what the #51
    repositories lost, because only the root was asked for."""
    _tree(
        paths,
        'README.md', 'app/client/package.json', 'app/client/yarn.lock',
        'app/server/pom.xml', 'app/server/src/Main.java',
        'app/client/node_modules/x/package.json',
    )
    service = _service(paths)
    service.session = FakeRaw({
        'app/client/package.json': b'{"name": "client"}',
        'app/client/yarn.lock': b'# yarn\n',
        'app/server/pom.xml': b'<project/>',
    })

    produced = service.process_repo(_repository())

    assert produced is not None
    root = _root(paths)
    assert produced['local_content_path'] == str(root)
    assert (
        root / 'app/client/package.json'
    ).read_bytes() == b'{"name": "client"}'
    assert (root / 'app/server/pom.xml').read_bytes() == b'<project/>'
    assert not (root / 'README.md').exists()
    assert not (root / 'app/client/node_modules').exists()
    assert produced['content_digest'] == content_digest([
        ('app/client/package.json', 18),
        ('app/client/yarn.lock', 7),
        ('app/server/pom.xml', 10),
    ])
    assert all(service.session.streamed)


def test_the_language_plays_no_part(paths):
    """A repository labelled TypeScript with a Java backend, or with no
    language at all, gets every ecosystem its tree has."""
    _tree(paths, 'package.json', 'backend/build.gradle', 'go.mod')
    service = _service(paths)
    service.session = FakeRaw({
        'package.json': b'{}', 'backend/build.gradle': b'plugins {}',
        'go.mod': b'module x',
    })
    repository = _repository()
    repository.language = None

    assert service.process_repo(repository) is not None
    assert _document(paths)['ecosystems'] == ['go', 'maven', 'npm']


def test_a_release_is_downloaded_by_commit_not_by_tag(paths):
    """The files are stored under the commit the commit stage resolved.

    Release targets were fetched by tag name instead. A tag that had
    moved on since (`v1`, `latest`, `nightly`) stored another commit's
    files under the resolved sha.
    """
    _tree(paths, 'go.mod')
    service = _service(paths)

    service.session = FakeRaw({'go.mod': b'module x'})

    assert service.process_repo(_repository()) is not None

    urls = service.session.urls
    assert urls == [BASE + 'go.mod']
    for url in urls:
        assert '/refs/tags/' not in url
    assert (_root(paths) / 'go.mod').exists()


def test_a_path_is_quoted_in_the_url():
    assert raw_url('o', 'r', SHA, 'dir with space/#x/package.json') == (
        f'https://raw.githubusercontent.com/o/r/{SHA}/'
        'dir%20with%20space/%23x/package.json'
    )


def test_manifests_json_records_what_became_of_every_file(paths):
    _tree(
        paths,
        'package.json', 'missing/go.mod', 'test/fixtures/package.json',
        'examples/demo/package.json',
    )
    service = _service(paths)

    service.session = FakeRaw({'package.json': b'{}'})

    service.process_repo(_repository())
    document = _document(paths)

    assert document['repository_id'] == 1
    assert document['commit_sha'] == SHA
    assert document['candidates'] == 4
    assert [(e['path'], e['status']) for e in document['selected']] == [
        ('package.json', 'ok'), ('missing/go.mod', 'absent'),
    ]
    assert {e['path']: e['reason'] for e in document['skipped']} == {
        'test/fixtures/package.json': 'excluded-dir',
        'examples/demo/package.json': 'example-dir',
    }
    assert document['skipped_by_reason'] == {
        'example-dir': 1, 'excluded-dir': 1,
    }
    assert document['limits']['max_files'] == 200
    assert document['bytes'] == 2


def test_a_file_already_stored_is_not_fetched_again(paths):
    _tree(paths, 'package.json', 'api/go.mod')
    root = _root(paths)
    root.mkdir(parents=True)
    (root / 'package.json').write_bytes(b'{"kept": true}')
    service = _service(paths)

    service.session = FakeRaw({'api/go.mod': b'module x'})

    produced = service.process_repo(_repository())

    assert produced is not None
    assert service.session.urls == [BASE + 'api/go.mod']
    assert produced['content_digest'] == content_digest(
        [('package.json', 14), ('api/go.mod', 8)],
    )


def test_no_stored_tree_is_nothing_to_do_yet(paths):
    service = _service(paths)
    service.session = FakeRaw({})
    assert service.process_repo(_repository()) is None
    assert service.session.urls == []


def test_a_tree_cut_short_is_not_read(paths):
    stored = paths.tree_file(1, SHA)
    stored.parent.mkdir(parents=True)
    stored.write_text('package.json\npom.x', encoding='utf-8')
    service = _service(paths)
    assert service.process_repo(_repository()) is None


def test_a_repository_without_manifests_still_has_a_content_root(paths):
    """So the SBOM stage records an empty scan rather than waiting on it
    for ever."""
    _tree(paths, 'README.md', 'main.c')
    service = _service(paths)
    service.session = FakeRaw({})

    produced = service.process_repo(_repository())

    assert produced is not None
    assert _root(paths).is_dir()
    assert produced['content_digest'] == content_digest([])
    assert _document(paths)['selected'] == []


# --- caps -------------------------------------------------------------------

@pytest.mark.parametrize('declare', [True, False], ids=['declared', 'streamed'])
def test_a_file_over_the_per_file_cap_is_skipped_and_the_rest_fetched(
    paths, declare,
):
    """Read no further than the cap, whether or not the size was
    declared up front."""
    _tree(paths, 'package-lock.json', 'package.json')
    service = _service(paths, max_file_bytes=1000)
    service.session = FakeRaw(
        {'package-lock.json': b'x' * 5000, 'package.json': b'{}'},
        declare=declare,
    )

    produced = service.process_repo(_repository())

    assert produced is not None
    root = _root(paths)
    assert not (root / 'package-lock.json').exists()
    assert (root / 'package.json').exists()
    statuses = {e['path']: e['status'] for e in _document(paths)['selected']}
    assert statuses['package-lock.json'] == 'over-file-byte-cap'


def test_the_per_repository_byte_cap_records_the_rest(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod', 'c/go.mod', 'd/go.mod')
    service = _service(paths, max_bytes=250)
    service.session = FakeRaw({
        f'{name}/go.mod': b'm' * 100 for name in 'abcd'
    })

    produced = service.process_repo(_repository())

    assert produced is not None
    root = _root(paths)
    assert (root / 'a/go.mod').exists() and (root / 'b/go.mod').exists()
    assert not (root / 'c/go.mod').exists()
    document = _document(paths)
    assert [e['path'] for e in document['skipped']] == ['c/go.mod', 'd/go.mod']
    assert {e['reason'] for e in document['skipped']} == {'over-byte-cap'}
    assert document['bytes'] == 200
    assert service.session.urls == [
        BASE + 'a/go.mod', BASE + 'b/go.mod', BASE + 'c/go.mod',
    ]


def test_the_file_cap_is_recorded(paths):
    _tree(paths, *(f'm{i:03d}/package.json' for i in range(12)))
    service = _service(paths, max_files=10)
    service.session = FakeRaw({
        f'm{i:03d}/package.json': b'{}' for i in range(12)
    })

    service.process_repo(_repository())
    document = _document(paths)

    assert len(document['selected']) == 10
    assert document['skipped_by_reason'] == {'over-file-cap': 2}


# --- failures ---------------------------------------------------------------

@pytest.mark.parametrize(
    'answer',
    [503, 429, requests.ConnectionError('reset')],
    ids=['server-error', 'rate-limited', 'connection-reset'],
)
def test_a_failure_that_may_pass_fails_the_stage_after_trying_the_rest(
    paths, answer,
):
    """Recorded as a failure, so the stage backs off and is retried; what
    was fetched is kept, and not asked for again."""
    _tree(paths, 'a/go.mod', 'b/go.mod')
    service = _service(paths)
    service.session = FakeRaw({
        'a/go.mod': answer, 'b/go.mod': b'module b',
    })

    with pytest.raises(ContentFetchError, match='1 of 2'):
        service.process_repo(_repository())

    assert (_root(paths) / 'b/go.mod').exists()
    service.session = FakeRaw({
        'a/go.mod': b'module a', 'b/go.mod': b'module b',
    })
    assert service.process_repo(_repository()) is not None
    assert service.session.urls == [BASE + 'a/go.mod']


def test_a_refusal_is_recorded_not_retried(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod')
    service = _service(paths)
    service.session = FakeRaw({
        'a/go.mod': 403, 'b/go.mod': b'module b',
    })

    assert service.process_repo(_repository()) is not None
    statuses = {e['path']: e['status'] for e in _document(paths)['selected']}
    assert statuses == {'a/go.mod': 'http-403', 'b/go.mod': 'ok'}


def test_an_unsafe_path_in_a_list_is_never_written(paths, tmp_path):
    """`discover` refuses them, and the service checks again for a list
    built elsewhere."""
    _tree(paths, 'package.json')
    service = _service(paths)
    service.session = FakeRaw({})
    listed = discover(['package.json'])
    listed.selected[0] = type(listed.selected[0])(
        '../../escape/package.json', 'npm', False,
    )

    service.process_repo(_repository(), listed)

    assert service.session.urls == []
    assert not (tmp_path / 'escape').exists()


def test_a_download_cut_short_by_a_full_disk_is_not_left_behind(
    tmp_path, monkeypatch, full_disk,
):
    """Each manifest was written in place and skipped once it existed.

    A full disk or a kill midway left a prefix. Every later run skipped
    the download, so Syft scanned the prefix, for good (#13).
    """
    monkeypatch.chdir(tmp_path)
    paths = PathConfig(base_data_dir=tmp_path)
    content_dir = tmp_path / '06-github-content'
    content_dir.mkdir()
    _tree(paths, 'go.mod')
    go_mod = b'module github.com/owner/repo\n\ngo 1.22\n'
    service = _service(paths)
    service.session = FakeRaw({'go.mod': go_mod})
    target_dir = content_dir / '1' / SHA

    full_disk.fill(content_dir)
    with pytest.raises(OSError):
        service.process_repo(_repository())

    written = [p for p in target_dir.rglob('*') if p.is_file()]
    assert written == [], 'nothing half written, and no temporary file'

    # With room again, the next run downloads it rather than trusting a
    # prefix.
    full_disk.free()
    assert service.process_repo(_repository()) is not None
    assert (target_dir / 'go.mod').read_bytes() == go_mod


class TestContentStats:
    """Tests for ContentStats dataclass."""

    def test_default_values(self):
        """Test default values are correct."""
        result = ContentStats(repo='test/repo')
        assert result.downloaded_files == 0
        assert result.missing_files == 0
        assert result.failed == 0
        assert result.skipped == 0
        assert result.cache_hits == 0


def test_what_was_not_there_is_not_asked_for_again(paths):
    """The walk passes through this stage for every claimed repository,
    due or not: a file that was absent, too large or refused at this
    commit stays so, and is recorded rather than requested again."""
    _tree(paths, 'a/go.mod', 'b/go.mod', 'c/go.mod', 'd/go.mod')
    service = _service(paths, max_file_bytes=10)
    service.session = FakeRaw({
        'a/go.mod': b'module a', 'b/go.mod': 404, 'c/go.mod': b'x' * 50,
        'd/go.mod': 403,
    })
    first = service.process_repo(_repository())
    before = paths.discovery_file(1, SHA).stat().st_mtime_ns

    service.session = FakeRaw({})
    second = service.process_repo(_repository())

    assert service.session.urls == []
    assert first is not None and second is not None
    assert first['content_digest'] == second['content_digest']
    assert paths.discovery_file(1, SHA).stat().st_mtime_ns == before, (
        'an unchanged list is not written again'
    )

    service.session = FakeRaw({'b/go.mod': b'module b'})
    third = service.process_repo(_repository(), force=True)
    assert third is not None
    assert (_root(paths) / 'b/go.mod').exists()


def test_a_server_error_is_asked_again(paths):
    _tree(paths, 'a/go.mod')
    service = _service(paths)
    service.session = FakeRaw({'a/go.mod': 502})
    with pytest.raises(ContentFetchError):
        service.process_repo(_repository())
    service.session = FakeRaw({'a/go.mod': b'module a'})
    assert service.process_repo(_repository()) is not None
    assert (_root(paths) / 'a/go.mod').exists()


def test_the_byte_cap_holds_on_the_next_walk_without_a_request(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod')
    service = _service(paths, max_bytes=150)
    service.session = FakeRaw({'a/go.mod': b'm' * 100, 'b/go.mod': b'm' * 100})
    service.process_repo(_repository())
    service.session = FakeRaw({})
    service.process_repo(_repository())
    assert service.session.urls == []
    assert _document(paths)['skipped_by_reason'] == {'over-byte-cap': 1}
