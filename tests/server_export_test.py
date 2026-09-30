"""The weekly Parquet export, served by the site itself (#154).

#128's Q11 publishes the export for others, and the owner decided on
2026-09-30 that the site serves it: the manifest at
/export/manifest.json, kept five minutes, and each file it names at
/export/<file>, kept for good, since its name is its content's. In
ranges, so that DuckDB or pyarrow reads a table over HTTP without
fetching all of it. Anything else under /export/ is a 404: nothing is
listed, and nothing the manifest does not name is served, so no path
leads out of the directory. Every request counts against a limit of its
own, EXPORT_RATE_LIMIT.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import time
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from structlog.testing import capture_logs

from chatsbom.export.parquet import content_addressed_name
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import MANIFEST_NAME
from chatsbom.server.app import create_app
from chatsbom.server.app import POLICY
from tests.server_app_test import configure
from tests.server_app_test import INDEX
from tests.server_app_test import Serving
from tests.server_app_test import visit
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

IMMUTABLE = 'public, max-age=31536000, immutable'
PARQUET = 'application/vnd.apache.parquet'

#: Bytes that stand for a table's file, where what is in it does not
#: matter: 1,000 of them, each its offset's last digit, so that a range
#: of them says where it came from.
BYTES = bytes(ord('0') + i % 10 for i in range(1000))

#: Two visitors, whom the limits count apart.
VISITOR = '198.51.100.20'
OTHER = '198.51.100.21'


def exported(directory: Path, **tables: bytes) -> dict[str, str]:
    """An export of `tables` in `directory`, as `export parquet` writes
    one: each table's bytes in a file named by them, and a manifest
    naming the files with their sizes and checksums. The files' names,
    by table."""
    directory.mkdir(parents=True, exist_ok=True)
    names, files = {}, []
    for table, data in tables.items():
        digest = hashlib.sha256(data).hexdigest()
        name = content_addressed_name(f'{table}.parquet', digest)
        (directory / name).write_bytes(data)
        names[table] = name
        files.append({'name': name, 'bytes': len(data), 'sha256': digest})
    manifest(directory, files)
    return names


def manifest(directory: Path, files: list[dict[str, Any]]) -> bytes:
    """A manifest naming `files`, written as the export writes one; its
    bytes."""
    text = json.dumps(
        {'schemaVersion': '8', 'files': files}, indent=2,
    ) + '\n'
    (directory / MANIFEST_NAME).write_text(text, encoding='utf-8')
    return text.encode()


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    """A built page."""
    root = tmp_path / 'client'
    (root / 'assets').mkdir(parents=True)
    (root / 'index.html').write_text(INDEX)
    return root


@pytest.fixture
def export(tmp_path: Path) -> Path:
    """Where the collector exports: WEB_EXPORT_DIR."""
    return tmp_path / 'export'


def serving(
    spa: Path, tmp_path: Path, export: Path, **environ: str,
) -> TestClient:
    return visit(
        create_app(
            configure(spa, tmp_path, WEB_EXPORT_DIR=str(export), **environ),
        ),
    )


@pytest.fixture
def client(spa: Path, tmp_path: Path, export: Path) -> Iterator[TestClient]:
    with serving(spa, tmp_path, export) as client:
        yield client


def not_found(response: Any) -> None:
    """The 404 every path the service does not have answers: JSON, as
    the API's, and kept by no one."""
    assert response.status_code == 404, response.text
    assert response.json() == {'error': 'Not Found'}
    assert response.headers['cache-control'] == 'no-store'


class TestTheManifest:

    def test_is_served_as_the_export_wrote_it(self, client, export):
        exported(export, repositories=b'r', artifacts=b'a')
        response = client.get('/export/manifest.json')
        assert response.status_code == 200
        assert response.content == (export / MANIFEST_NAME).read_bytes()
        assert response.headers['content-type'] == 'application/json'
        # With the page's headers, as everything the service answers.
        assert response.headers['content-security-policy'] == POLICY
        assert response.headers['x-content-type-options'] == 'nosniff'

    def test_is_kept_five_minutes(self, client, export):
        """Its name never changes, and a new export is seen within
        them: the files it names are kept for good."""
        exported(export, repositories=b'r')
        response = client.get('/export/manifest.json')
        assert response.headers['cache-control'] == 'max-age=300'

    def test_answers_head(self, client, export):
        exported(export, repositories=b'r')
        response = client.head('/export/manifest.json')
        assert response.status_code == 200
        assert response.content == b''
        assert response.headers['cache-control'] == 'max-age=300'

    def test_a_new_export_is_served_without_a_restart(self, client, export):
        """It is read as each request is answered, as `CURRENT` is."""
        exported(export, repositories=b'r')
        first = client.get('/export/manifest.json').content
        exported(export, repositories=b'r2')
        second = client.get('/export/manifest.json').content
        assert second != first
        assert second == (export / MANIFEST_NAME).read_bytes()

    @pytest.mark.parametrize(
        'made', [False, True], ids=['no-directory', 'empty'],
    )
    def test_without_an_export_is_a_404(self, client, export, made):
        """Before the first: the collector makes the directory as it
        starts, and exports into it once a warehouse is built."""
        if made:
            export.mkdir()
        not_found(client.get('/export/manifest.json'))

    @pytest.mark.parametrize(
        'text',
        [
            '{"schemaVersion": "8", "files": [',
            '[]',
            '{"files": {"a": 1}}',
            '{"files": [{"name": 7}]}',
            '\xff not even text',
        ],
    )
    def test_one_that_is_not_a_manifest_is_a_503_that_names_nothing(
        self, client, export, text,
    ):
        """What it is, and why, is for the log: answered, the error
        would name the directory."""
        export.mkdir()
        (export / MANIFEST_NAME).write_bytes(text.encode('latin-1'))
        with capture_logs() as logged:
            response = client.get('/export/manifest.json')
        assert response.status_code == 503
        assert response.json() == {
            'error': 'The export cannot be read for a moment. Try again '
            'shortly.',
        }
        assert response.headers['cache-control'] == 'no-store'
        assert str(export) not in response.text
        assert [entry['event'] for entry in logged] == ['export unreadable']
        assert str(export) in logged[0]['error']


class TestTheFiles:

    def test_each_the_manifest_names_is_served_for_good(self, client, export):
        """Named by its content: a changed table is a new name, and one
        a reader holds is good for good."""
        names = exported(export, repositories=BYTES, artifacts=b'a' * 10)
        for table, data in (('repositories', BYTES), ('artifacts', b'a' * 10)):
            response = client.get(f'/export/{names[table]}')
            assert response.status_code == 200, table
            assert response.content == data
            assert response.headers['cache-control'] == IMMUTABLE
            assert response.headers['content-type'] == PARQUET
            assert response.headers['content-length'] == str(len(data))
            assert response.headers['accept-ranges'] == 'bytes'
            assert response.headers['content-security-policy'] == POLICY

    def test_are_tagged_by_their_checksum(self, client, export):
        """The manifest's SHA-256, not the file's time: the next export
        writes an unchanged table again, under the name it had, and a
        reader part way through it is not to be told it has changed.
        DuckDB checks that a file's ETag stays the same as it reads."""
        names = exported(export, repositories=BYTES)
        digest = hashlib.sha256(BYTES).hexdigest()
        first = client.get(f'/export/{names["repositories"]}')
        path = export / names['repositories']
        os.utime(path, (time.time() + 60, time.time() + 60))
        again = client.get(f'/export/{names["repositories"]}')
        assert first.headers['etag'] == again.headers['etag'] == f'"{digest}"'

    def test_answer_head(self, client, export):
        """As DuckDB asks first: how long the file is, and whether it
        can be read in ranges."""
        names = exported(export, repositories=BYTES)
        response = client.head(f'/export/{names["repositories"]}')
        assert response.status_code == 200
        assert response.content == b''
        assert response.headers['content-length'] == str(len(BYTES))
        assert response.headers['accept-ranges'] == 'bytes'
        assert response.headers['cache-control'] == IMMUTABLE

    @pytest.mark.parametrize(
        'asked,start,end',
        [
            ('bytes=0-3', 0, 4),
            ('bytes=990-', 990, 1000),
            # The end of the file, as a reader asks for Parquet's footer.
            ('bytes=-8', 992, 1000),
            ('bytes=500-2000', 500, 1000),
        ],
    )
    def test_answer_a_range_with_that_range_alone(
        self, client, export, asked, start, end,
    ):
        names = exported(export, repositories=BYTES)
        response = client.get(
            f'/export/{names["repositories"]}', headers={'range': asked},
        )
        assert response.status_code == 206
        assert response.content == BYTES[start:end]
        assert response.headers['content-range'] == (
            f'bytes {start}-{end - 1}/{len(BYTES)}'
        )
        assert response.headers['content-length'] == str(end - start)
        assert response.headers['cache-control'] == IMMUTABLE

    def test_answer_several_ranges_at_once(self, client, export):
        names = exported(export, repositories=BYTES)
        response = client.get(
            f'/export/{names["repositories"]}',
            headers={'range': 'bytes=0-1,998-999'},
        )
        assert response.status_code == 206
        assert response.headers['content-type'].startswith(
            'multipart/byteranges; boundary=',
        )
        assert b'Content-Range: bytes 0-1/1000' in response.content
        assert b'Content-Range: bytes 998-999/1000' in response.content

    def test_refuse_a_range_past_the_end(self, client, export):
        """Said with the file's length, and kept by no one: a refusal is
        not the file."""
        names = exported(export, repositories=BYTES)
        response = client.get(
            f'/export/{names["repositories"]}',
            headers={'range': 'bytes=1000-'},
        )
        assert response.status_code == 416
        assert response.headers['content-range'] == 'bytes */1000'
        assert response.headers.get('cache-control') != IMMUTABLE

    @pytest.mark.parametrize('tag,status', [('same', 206), ('other', 200)])
    def test_answer_a_range_only_of_the_file_it_was_asked_of(
        self, client, export, tag, status,
    ):
        """`If-Range`: a reader that holds part of a file asks for the
        rest only if it is still the file it holds, and otherwise gets
        all of it."""
        names = exported(export, repositories=BYTES)
        etag = client.head(f'/export/{names["repositories"]}').headers['etag']
        response = client.get(
            f'/export/{names["repositories"]}',
            headers={
                'range': 'bytes=0-3',
                'if-range': etag if tag == 'same' else '"0000"',
            },
        )
        assert response.status_code == status
        assert response.content == (BYTES[:4] if status == 206 else BYTES)

    def test_one_that_is_not_what_the_manifest_says_is_not_served(
        self, client, export,
    ):
        """Cut short, say: served, it would be kept for good. A 503,
        kept by no one, and the log says which."""
        names = exported(export, repositories=BYTES)
        (export / names['repositories']).write_bytes(BYTES[:100])
        with capture_logs() as logged:
            response = client.get(f'/export/{names["repositories"]}')
        assert response.status_code == 503
        assert response.headers['cache-control'] == 'no-store'
        assert [entry['event'] for entry in logged] == ['export unreadable']
        assert names['repositories'] in logged[0]['error']


class TestWhatIsNotServed:

    def test_nothing_is_listed(self, client, export):
        exported(export, repositories=b'r')
        not_found(client.get('/export/'))

    def test_export_alone_is_the_page(self, client):
        """As /api alone is: a path of the page's."""
        response = client.get('/export')
        assert response.status_code == 200
        assert response.text == INDEX

    def test_nor_is_a_file_the_manifest_does_not_name(self, client, export):
        """Whatever is in the directory: the last export's files, until
        they go, a file named as one of the export's, and what the
        export writes aside before it renames it."""
        old = exported(export, repositories=b'old')
        exported(export, repositories=b'new')
        stray = content_addressed_name('artifacts.parquet', '0' * 64)
        (export / stray).write_bytes(b'stray')
        (export / '.manifest.json.0123.tmp').write_text('{}')
        (export / 'notes.txt').write_text('mine')
        for name in (
            old['repositories'], stray, '.manifest.json.0123.tmp',
            'notes.txt', 'manifest.json.bak',
        ):
            not_found(client.get(f'/export/{name}'))

    def test_a_file_stops_being_served_when_the_manifest_moves_on(
        self, client, export,
    ):
        old = exported(export, repositories=b'old')
        assert client.get(f'/export/{old["repositories"]}').status_code == 200
        new = exported(export, repositories=b'new')
        assert (export / old['repositories']).exists()
        not_found(client.get(f'/export/{old["repositories"]}'))
        assert client.get(f'/export/{new["repositories"]}').content == b'new'

    def test_no_path_leads_out_of_the_directory(
        self, client, export, tmp_path,
    ):
        """Not by the path asked for, nor by a name the manifest gives:
        a name the export writes is a table's, a hyphen, its checksum's
        first eight hex digits and `.parquet`, and nothing else is
        served."""
        secret = b'not for anyone'
        outside = content_addressed_name(
            'secret.parquet', hashlib.sha256(secret).hexdigest(),
        )
        (tmp_path / outside).write_bytes(secret)
        export.mkdir()
        (export / 'sub').mkdir()
        (export / 'sub' / outside).write_bytes(secret)
        manifest(
            export,
            [
                {
                    'name': name, 'bytes': len(secret),
                    'sha256': hashlib.sha256(secret).hexdigest(),
                }
                for name in (f'../{outside}', f'sub/{outside}', outside)
            ],
        )
        # `/export/../` a client resolves before it asks: these reach the
        # service as they are, and the service decodes them.
        for path in (
            f'/export/..%2F{outside}', f'/export/%2e%2e/{outside}',
            f'/export/%2e%2e%2F{outside}', f'/export/sub/{outside}',
            f'/export/sub%2F{outside}', f'/export/{outside}',
        ):
            response = client.get(path)
            assert secret not in response.content, path
            assert response.status_code == 404, path

    def test_a_link_is_not_followed(self, client, export, tmp_path):
        """A symbolic link, named as the export names a file and named
        by the manifest: to a file outside the directory, or to anything
        the service can read. Whoever can write data/export is not to
        read through the service what only it may."""
        secret = b'the service reads this'
        (tmp_path / 'secret').write_bytes(secret)
        digest = hashlib.sha256(secret).hexdigest()
        name = content_addressed_name('repositories.parquet', digest)
        export.mkdir()
        (export / name).symlink_to(tmp_path / 'secret')
        manifest(
            export, [{'name': name, 'bytes': len(secret), 'sha256': digest}],
        )
        response = client.get(f'/export/{name}')
        not_found(response)
        assert secret not in response.content

    @pytest.mark.parametrize('kind', ['directory', 'fifo'])
    def test_nor_is_what_is_not_a_file(self, client, export, kind):
        """At once: a FIFO is not waited on for a writer."""
        name = content_addressed_name('repositories.parquet', '1' * 64)
        export.mkdir()
        if kind == 'directory':
            (export / name).mkdir()
        else:
            os.mkfifo(export / name)
        manifest(export, [{'name': name, 'bytes': 0, 'sha256': '1' * 64}])
        started = time.monotonic()
        not_found(client.get(f'/export/{name}'))
        assert time.monotonic() - started < 5

    @pytest.mark.parametrize('method', ['POST', 'PUT', 'DELETE'])
    def test_nor_anything_but_get_and_head(self, client, export, method):
        names = exported(export, repositories=b'r')
        file = f'/export/{names["repositories"]}'
        for path in ('/export/manifest.json', file):
            response = client.request(method, path)
            assert response.status_code == 405, path
            assert response.headers['cache-control'] == 'no-store'
        assert (export / names['repositories']).read_bytes() == b'r'


class TestTheRateLimit:
    """Every request under /export/ counts against EXPORT_RATE_LIMIT:
    the manifest's, each range of a file, and each 404."""

    def test_counts_every_request_for_the_export(
        self, spa, tmp_path, export,
    ):
        names = exported(export, repositories=BYTES)
        paths = [
            '/export/manifest.json', f'/export/{names["repositories"]}',
            '/export/missing',
        ]
        with serving(
            spa, tmp_path, export, EXPORT_RATE_LIMIT='3/60',
        ) as client:
            statuses = [client.get(path).status_code for path in paths]
            refused = client.get(
                f'/export/{names["repositories"]}',
                headers={'range': 'bytes=0-3'},
            )
        assert statuses == [200, 200, 404]
        assert refused.status_code == 429
        assert refused.json() == {
            'error': 'Too many requests for the export. Wait a moment.',
        }
        assert refused.headers['cache-control'] == 'no-store'

    def test_counts_each_client_apart(self, spa, tmp_path, export):
        exported(export, repositories=b'r')
        app = create_app(
            configure(
                spa, tmp_path, WEB_EXPORT_DIR=str(export),
                EXPORT_RATE_LIMIT='1/60',
            ),
        )
        with visit(app, VISITOR) as first, visit(app, OTHER) as second:
            assert first.get('/export/manifest.json').status_code == 200
            assert first.get('/export/manifest.json').status_code == 429
            assert second.get('/export/manifest.json').status_code == 200

    def test_is_its_own_and_not_the_pages(self, spa, tmp_path, export):
        """A reader of the export asks for a file a range at a time, a
        hundred ranges or more for a table, where a page view asks some
        25 questions: neither spends the other's budget."""
        exported(export, repositories=b'r')
        with serving(
            spa, tmp_path, export,
            EXPORT_RATE_LIMIT='1/60', QUERY_RATE_LIMIT='1/60',
        ) as client:
            assert client.get('/export/manifest.json').status_code == 200
            assert client.get('/export/manifest.json').status_code == 429
            assert client.get('/api/meta').status_code == 200
            assert client.get('/api/meta').status_code == 429


class Ranged(io.RawIOBase):
    """A file on the service, read as DuckDB reads one over HTTP: its
    length from a HEAD, then each read a GET of that range alone."""

    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self.position = 0
        #: The status of each GET, and the bytes they brought.
        self.statuses: list[int] = []
        self.fetched = 0
        with urllib.request.urlopen(
            urllib.request.Request(url, method='HEAD'), timeout=30,
        ) as response:
            assert response.headers['accept-ranges'] == 'bytes'
            self.size = int(response.headers['content-length'])

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        start = {
            io.SEEK_SET: 0, io.SEEK_CUR: self.position, io.SEEK_END: self.size,
        }[whence]
        self.position = start + offset
        return self.position

    def readinto(self, buffer: Any) -> int:
        wanted = min(len(buffer), self.size - self.position)
        if wanted <= 0:
            return 0
        last = self.position + wanted - 1
        request = urllib.request.Request(
            self.url, headers={'Range': f'bytes={self.position}-{last}'},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            self.statuses.append(response.status)
            data = response.read()
        buffer[:len(data)] = data
        self.position += len(data)
        self.fetched += len(data)
        return len(data)


class TestOverHTTP:
    """As uvicorn serves it, as `web serve` runs it."""

    def test_a_reader_takes_one_column_in_ranges_alone(
        self, spa, tmp_path, export,
    ):
        """A table of a small column beside a large one: pyarrow reads
        the small one through ranges, the footer's and the column's, and
        never the file."""
        rows = 20_000
        table = pa.table({
            'name': [f'package-{i % 100}' for i in range(rows)],
            'blob': [os.urandom(100) for _ in range(rows)],
        })
        sink = io.BytesIO()
        pq.write_table(table, sink, compression='zstd')
        names = exported(export, artifacts=sink.getvalue())
        app = create_app(configure(spa, tmp_path, WEB_EXPORT_DIR=str(export)))

        with Serving(app) as base:
            with Ranged(f'{base}/export/{names["artifacts"]}') as remote:
                read = pq.ParquetFile(remote).read(columns=['name'])
                statuses, fetched, size = (
                    remote.statuses, remote.fetched, remote.size,
                )

        assert read.column('name').to_pylist(
        ) == table.column('name').to_pylist()
        assert set(statuses) == {206}
        assert size == len(sink.getvalue())
        assert fetched < size / 10

    def test_the_export_is_read_as_it_was_written(self, spa, tmp_path, export):
        """Of a warehouse, by `export parquet`: each table, read through
        the manifest the service serves, is the file on disk."""
        export_warehouse(warehouse(tmp_path / 'w.duckdb', shop()), export)
        app = create_app(configure(spa, tmp_path, WEB_EXPORT_DIR=str(export)))

        with Serving(app) as base:
            with urllib.request.urlopen(
                f'{base}/export/manifest.json', timeout=30,
            ) as response:
                said = json.load(response)
            tables = {}
            for entry in said['files']:
                with Ranged(f'{base}/export/{entry["name"]}') as remote:
                    tables[entry['name']] = pq.ParquetFile(remote).read()

        assert sorted(tables) == sorted(
            path.name for path in export.glob('*.parquet')
        )
        for name, read in tables.items():
            assert read.equals(pq.read_table(export / name)), name
