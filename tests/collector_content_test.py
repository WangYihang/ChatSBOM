"""The content stage's rules (#51): the manifests a repository's tree
lists, fetched at the commit and stored at their own paths, with
`manifests.json` beside the tree saying what became of each.

It used to ask for a fixed list of names at the repository root, chosen
by the repository's language. The list comes from the stored tree now
(`core/discovery.py`), and the rules are `collector/content.py`'s: the
walk, which yields each file it needs and is sent what came back, and
`settle`, which writes `manifests.json`. They held the old pipeline's
content service, whose tests these were, and went on holding the
collector's stage when that service went (#171). So they are driven as
the stage drives them (`RepositoryStages.content`), with caps small
enough to reach, answer by answer; the stage itself is
collector_stages_test's, and the client it fetches with is below.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx2
import pytest

from chatsbom.collector.content import Answer
from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import Got
from chatsbom.collector.content import Lost
from chatsbom.collector.content import MAX_FILE_BYTES
from chatsbom.collector.content import outcomes_of
from chatsbom.collector.content import read_document
from chatsbom.collector.content import settle
from chatsbom.collector.content import stored_discovery
from chatsbom.collector.content import stored_files
from chatsbom.collector.content import TooLarge
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.content import walk
from chatsbom.collector.content import Walked
from chatsbom.collector.content import Wanted
from chatsbom.collector.raw import _CHUNK
from chatsbom.collector.raw import RAW
from chatsbom.collector.raw import RawClient
from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import Discovery
from chatsbom.core.discovery import MAX_BYTES
from chatsbom.core.discovery import MAX_FILES

SHA = '0123456789abcdef0123456789abcdef01234567'

#: A connection that failed, in place of an answer.
LOST = object()


class Raw:
    """`raw.githubusercontent.com` at one commit, as the walk meets it
    through the raw client: path -> body, a status, or `LOST`. A body
    past the room the walk gave is `TooLarge`, as the client stops
    reading it there."""

    def __init__(self, files: dict[str, Any]) -> None:
        self.files = files
        self.asked: list[str] = []

    def answer(self, wanted: Wanted) -> Answer:
        self.asked.append(wanted.path)
        found = self.files.get(wanted.path, 404)
        if found is LOST:
            return Lost('ConnectError: reset')
        if isinstance(found, int):
            return Got(found)
        if len(found) > wanted.room:
            return TooLarge(len(found))
        return Got(200, found)


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path)


def _tree(paths: PathConfig, *files: str) -> None:
    stored = paths.tree_file(1, SHA)
    stored.parent.mkdir(parents=True, exist_ok=True)
    stored.write_text(''.join(f'{f}\n' for f in files), encoding='utf-8')


def _root(paths: PathConfig) -> Path:
    return paths.content_root(1, SHA)


def _document(paths: PathConfig) -> dict[str, Any]:
    return json.loads(
        paths.discovery_file(1, SHA).read_text(encoding='utf-8'),
    )


def collect(
    paths: PathConfig,
    raw: Raw,
    *,
    force: bool = False,
    discovery: Discovery | None = None,
    max_files: int = MAX_FILES,
    max_bytes: int = MAX_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
) -> tuple[str, Walked]:
    """The walk of the stored tree's list, each file it wants answered
    by `raw`, then `manifests.json` settled: the content stage's round,
    at these caps. The digest, and what the walk did."""
    if discovery is None:
        discovery = stored_discovery(paths, 1, SHA, max_files=max_files)
        assert discovery is not None
    root = _root(paths)
    root.mkdir(parents=True, exist_ok=True)
    index = paths.discovery_file(1, SHA)
    known = {} if force else outcomes_of(read_document(index), SHA)
    walking = walk(
        discovery, root, known=known, force=force,
        max_bytes=max_bytes, max_file_bytes=max_file_bytes,
    )
    try:
        wanted = next(walking)
        while True:
            wanted = walking.send(raw.answer(wanted))
    except StopIteration as stop:
        done: Walked = stop.value
    digest, _ = settle(
        discovery, done, repository_id=1, sha=SHA, index=index,
        max_files=max_files, max_bytes=max_bytes,
        max_file_bytes=max_file_bytes, stamp=CONTENT_VERSION,
    )
    return digest, done


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
    raw = Raw({
        'app/client/package.json': b'{"name": "client"}',
        'app/client/yarn.lock': b'# yarn\n',
        'app/server/pom.xml': b'<project/>',
    })

    digest, _ = collect(paths, raw)

    root = _root(paths)
    assert (
        root / 'app/client/package.json'
    ).read_bytes() == b'{"name": "client"}'
    assert (root / 'app/server/pom.xml').read_bytes() == b'<project/>'
    assert not (root / 'README.md').exists()
    assert not (root / 'app/client/node_modules').exists()
    assert digest == content_digest([
        ('app/client/package.json', 18),
        ('app/client/yarn.lock', 7),
        ('app/server/pom.xml', 10),
    ])
    assert stored_files(root) == [
        'app/client/package.json', 'app/client/yarn.lock',
        'app/server/pom.xml',
    ]


def test_every_ecosystem_the_tree_has_is_fetched(paths):
    """A repository labelled TypeScript with a Java backend, or with no
    language at all, gets every ecosystem its tree has: the language
    plays no part."""
    _tree(paths, 'package.json', 'backend/build.gradle', 'go.mod')
    raw = Raw({
        'package.json': b'{}', 'backend/build.gradle': b'plugins {}',
        'go.mod': b'module x',
    })

    collect(paths, raw)

    assert _document(paths)['ecosystems'] == ['go', 'maven', 'npm']


def test_manifests_json_records_what_became_of_every_file(paths):
    _tree(
        paths,
        'package.json', 'missing/go.mod', 'test/fixtures/package.json',
        'examples/demo/package.json',
    )

    collect(paths, Raw({'package.json': b'{}'}))
    document = _document(paths)

    assert document['repository_id'] == 1
    assert document['commit_sha'] == SHA
    assert document[VERSION_FIELD] == CONTENT_VERSION
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
    raw = Raw({'api/go.mod': b'module x'})

    digest, _ = collect(paths, raw)

    assert raw.asked == ['api/go.mod']
    assert digest == content_digest([('package.json', 14), ('api/go.mod', 8)])


def test_no_stored_tree_is_nothing_to_read_yet(paths):
    assert stored_discovery(paths, 1, SHA) is None


def test_a_tree_cut_short_is_not_read(paths):
    stored = paths.tree_file(1, SHA)
    stored.parent.mkdir(parents=True)
    stored.write_text('package.json\npom.x', encoding='utf-8')
    assert stored_discovery(paths, 1, SHA) is None


def test_a_tree_without_manifests_settles_empty(paths):
    _tree(paths, 'README.md', 'main.c')
    raw = Raw({})

    digest, _ = collect(paths, raw)

    assert raw.asked == []
    assert digest == content_digest([])
    assert _document(paths)['selected'] == []


def test_stored_files_are_what_is_under_the_root_by_repository_path(
    paths,
):
    """What the resolver chooses its recipes by (`resolver/due.py`)."""
    assert stored_files(_root(paths)) == []
    root = _root(paths)
    (root / 'web').mkdir(parents=True)
    (root / 'web' / 'package.json').write_text('{}')
    (root / 'go.mod').write_text('module x\n')
    assert stored_files(root) == ['go.mod', 'web/package.json']


# --- caps -------------------------------------------------------------------


def test_a_file_over_the_per_file_cap_is_skipped_and_the_rest_fetched(paths):
    _tree(paths, 'package-lock.json', 'package.json')
    raw = Raw({'package-lock.json': b'x' * 5000, 'package.json': b'{}'})

    collect(paths, raw, max_file_bytes=1000)

    root = _root(paths)
    assert not (root / 'package-lock.json').exists()
    assert (root / 'package.json').exists()
    statuses = {e['path']: e['status'] for e in _document(paths)['selected']}
    assert statuses['package-lock.json'] == 'over-file-byte-cap'


def test_the_per_repository_byte_cap_records_the_rest(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod', 'c/go.mod', 'd/go.mod')
    raw = Raw({f'{name}/go.mod': b'm' * 100 for name in 'abcd'})

    collect(paths, raw, max_bytes=250)

    root = _root(paths)
    assert (root / 'a/go.mod').exists() and (root / 'b/go.mod').exists()
    assert not (root / 'c/go.mod').exists()
    document = _document(paths)
    assert [e['path'] for e in document['skipped']] == ['c/go.mod', 'd/go.mod']
    assert {e['reason'] for e in document['skipped']} == {'over-byte-cap'}
    assert document['bytes'] == 200
    assert raw.asked == ['a/go.mod', 'b/go.mod', 'c/go.mod']


def test_the_file_cap_is_recorded(paths):
    _tree(paths, *(f'm{i:03d}/package.json' for i in range(12)))
    raw = Raw({f'm{i:03d}/package.json': b'{}' for i in range(12)})

    collect(paths, raw, max_files=10)
    document = _document(paths)

    assert len(document['selected']) == 10
    assert document['skipped_by_reason'] == {'over-file-cap': 2}


def test_the_byte_cap_holds_on_the_next_walk_without_a_request(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod')
    collect(
        paths, Raw({'a/go.mod': b'm' * 100, 'b/go.mod': b'm' * 100}),
        max_bytes=150,
    )
    raw = Raw({})
    collect(paths, raw, max_bytes=150)
    assert raw.asked == []
    assert _document(paths)['skipped_by_reason'] == {'over-byte-cap': 1}


# --- failures ---------------------------------------------------------------


@pytest.mark.parametrize(
    'answer', [503, 429, LOST],
    ids=['server-error', 'rate-limited', 'connection-reset'],
)
def test_a_failure_that_may_pass_is_left_for_later_after_the_rest(
    paths, answer,
):
    """Left out, so that the stage fails and backs off, after the rest
    are tried; what was fetched is kept, and not asked for again."""
    _tree(paths, 'a/go.mod', 'b/go.mod')

    raw = Raw({'a/go.mod': answer, 'b/go.mod': b'module b'})
    _, done = collect(paths, raw)

    assert done.transient == ['a/go.mod']
    assert (_root(paths) / 'b/go.mod').exists()
    raw = Raw({'a/go.mod': b'module a', 'b/go.mod': b'module b'})
    _, done = collect(paths, raw)
    assert done.transient == []
    assert raw.asked == ['a/go.mod']


def test_a_refusal_is_recorded_not_retried(paths):
    _tree(paths, 'a/go.mod', 'b/go.mod')

    _, done = collect(paths, Raw({'a/go.mod': 403, 'b/go.mod': b'module b'}))

    assert done.transient == []
    statuses = {e['path']: e['status'] for e in _document(paths)['selected']}
    assert statuses == {'a/go.mod': 'http-403', 'b/go.mod': 'ok'}
    raw = Raw({})
    collect(paths, raw)
    assert raw.asked == []


def test_an_unsafe_path_in_a_list_is_never_written(paths, tmp_path):
    """`discover` refuses them, and the walk checks again for a list
    built elsewhere."""
    _tree(paths, 'package.json')
    listed = discover(['package.json'])
    listed.selected[0] = type(listed.selected[0])(
        '../../escape/package.json', 'npm', False,
    )
    raw = Raw({})

    collect(paths, raw, discovery=listed)

    assert raw.asked == []
    assert not (tmp_path / 'escape').exists()


def test_a_download_cut_short_by_a_full_disk_is_not_left_behind(
    tmp_path, monkeypatch, full_disk,
):
    """Each manifest was written in place and skipped once it existed.

    A full disk or a kill midway left a prefix. Every later walk skipped
    the download, so Syft scanned the prefix, for good (#13).
    """
    monkeypatch.chdir(tmp_path)
    paths = PathConfig(base_data_dir=tmp_path)
    _tree(paths, 'go.mod')
    go_mod = b'module github.com/owner/repo\n\ngo 1.22\n'
    root = _root(paths)
    root.mkdir(parents=True)

    full_disk.fill(root)
    with pytest.raises(OSError):
        collect(paths, Raw({'go.mod': go_mod}))

    written = [p for p in root.rglob('*') if p.is_file()]
    assert written == [], 'nothing half written, and no temporary file'

    # With room again, the next walk downloads it rather than trusting a
    # prefix.
    full_disk.free()
    collect(paths, Raw({'go.mod': go_mod}))
    assert (root / 'go.mod').read_bytes() == go_mod


def test_what_was_not_there_is_not_asked_for_again(paths):
    """A file that was absent, too large or refused at this commit stays
    so, and is recorded rather than requested again, until the walk is
    forced."""
    _tree(paths, 'a/go.mod', 'b/go.mod', 'c/go.mod', 'd/go.mod')
    first, _ = collect(
        paths, Raw({
            'a/go.mod': b'module a', 'b/go.mod': 404, 'c/go.mod': b'x' * 50,
            'd/go.mod': 403,
        }),
        max_file_bytes=10,
    )
    before = paths.discovery_file(1, SHA).stat().st_mtime_ns

    raw = Raw({})
    second, _ = collect(paths, raw, max_file_bytes=10)

    assert raw.asked == []
    assert first == second
    assert paths.discovery_file(1, SHA).stat().st_mtime_ns == before, (
        'an unchanged list is not written again'
    )

    collect(paths, Raw({'b/go.mod': b'module b'}), force=True)
    assert (_root(paths) / 'b/go.mod').exists()


def test_a_server_error_is_asked_again(paths):
    _tree(paths, 'a/go.mod')
    collect(paths, Raw({'a/go.mod': 502}))
    raw = Raw({'a/go.mod': b'module a'})
    collect(paths, raw)
    assert raw.asked == ['a/go.mod']
    assert (_root(paths) / 'a/go.mod').exists()


# --- the raw client ---------------------------------------------------------


class Body(httpx2.AsyncByteStream):
    """A body of `size` bytes, sent a chunk at a time, counting what was
    taken."""

    def __init__(self, size: int) -> None:
        self.size = size
        self.sent = 0

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while self.sent < self.size:
            chunk = b'x' * min(1 << 12, self.size - self.sent)
            self.sent += len(chunk)
            yield chunk


def fetch(path: str, size: int, room: int, *, declare: bool) -> tuple[
    Answer, list[str], Body,
]:
    """`path` of octo/one at `SHA`, a body of `size` bytes, fetched by
    the raw client with `room` for it; the URLs asked."""
    body = Body(size)
    asked: list[str] = []

    def answering(request: httpx2.Request) -> httpx2.Response:
        asked.append(str(request.url))
        headers = {'content-length': str(size)} if declare else {}
        return httpx2.Response(200, headers=headers, stream=body)

    async def fetching() -> Answer:
        async with RawClient(
            transport=httpx2.MockTransport(answering),
        ) as raw:
            return await raw.fetch('octo/one', SHA, path, room)

    return asyncio.run(fetching()), asked, body


def test_a_file_is_asked_for_at_the_commit_each_segment_quoted():
    """At the commit the commit stage resolved, for a release too: a tag
    (`v1`, `latest`, `nightly`) can have moved on since, and another
    commit's files would be stored under this one."""
    answer, asked, _ = fetch(
        'dir with space/#x/package.json', 2, 100, declare=True,
    )
    assert answer == Got(200, b'xx')
    assert asked == [
        f'{RAW}/octo/one/{SHA}/dir%20with%20space/%23x/package.json',
    ]


@pytest.mark.parametrize('declare', [True, False], ids=['declared', 'streamed'])
def test_a_body_is_read_no_further_than_its_room(declare):
    """Whether or not its size was declared up front: a declared one
    not at all, and one that was not up to the chunk that went past."""
    answer, _, body = fetch(
        'package-lock.json', 1 << 20, 5000, declare=declare,
    )

    assert isinstance(answer, TooLarge) and answer.size > 5000
    assert body.sent <= (0 if declare else 5000 + _CHUNK)
