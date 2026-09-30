"""The weekly Parquet export, served as the collector writes it (#154).

The collector exports the warehouse into data/export when the last
export is a week old (deploy/collector-loop.sh): a Parquet file per
table, named by its content, and manifest.json, which names them with
their sizes and checksums (`chatsbom/export/parquet.py`). #128's Q11
publishes it for others, and the owner decided on 2026-09-30 that the
site serves it itself:

  /export/manifest.json  the manifest, kept five minutes: its name never
                         changes, and a new export is seen within them
  /export/<file>         a file the manifest names now, kept for good,
                         since its name is its content's; in ranges, so
                         that DuckDB or pyarrow reads a table over HTTP
                         without fetching all of it (Starlette's
                         `FileResponse` answers `Range` and `If-Range`)

Anything else under /export/ is a 404: nothing is listed, and no file
the manifest does not name is served, whatever else is in the
directory, so no path leads out of it. The manifest is read as each
request is answered, as `CURRENT` is: a new export is served without a
restart, and a file of the last one is not, once the manifest has moved
on, though it is on disk until the export removes it.

What is in the directory is the collector's to write, and the service
trusts no more of it than it must. A name the manifest gives is served
only if it is one the export writes, a table's name, a hyphen, eight
hex digits and `.parquet`; the file is opened as it is answered, never
through a link, never waiting on a FIFO, and only if it is as long as
the manifest says; and it is sent from what was opened, whatever the
name is by then. A file's ETag is its checksum, not its time, since the
next export writes an unchanged table again under the name it had.

Each request for the manifest or a file, found or not, counts against
EXPORT_RATE_LIMIT, a limit of its own rather than QUERY_RATE_LIMIT: a
reader asks for a table a range at a time, some 90 ranges for the
largest at the documented shape, where a page view asks some 25
questions, and neither is to spend the other's budget (`settings`).
"""
from __future__ import annotations

import errno
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

import structlog
from fastapi.responses import FileResponse
from fastapi.responses import Response
from starlette.exceptions import HTTPException
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from chatsbom.export.parquet import DIGEST_PREFIX
from chatsbom.export.parquet import MANIFEST_NAME
from chatsbom.server.ratelimit import RateLimiter

logger = structlog.get_logger('export')

#: The manifest's name never changes: a new export is seen within five
#: minutes.
MANIFEST_AGE = 'max-age=300'
#: A file's name is its content's, so a changed table is a new name.
IMMUTABLE = 'public, max-age=31536000, immutable'

JSON = 'application/json'
#: As the Apache Parquet project registered it with IANA.
PARQUET = 'application/vnd.apache.parquet'

#: A name the export gives a table's file (`content_addressed_name`),
#: and the only kind served: no path, and nothing hidden.
FILE = re.compile(rf'[a-z][a-z0-9_]*-[0-9a-f]{{{DIGEST_PREFIX}}}\.parquet')
SHA256 = re.compile(r'[0-9a-f]{64}')

#: The most of a manifest that is read: an export's is some 10 KB.
MAX_MANIFEST = 1 << 20

#: How what is served is opened: never through a link put in the
#: file's place, and never waiting on a FIFO for a writer.
OPEN = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK

# What each refusal says, beside its status.
TOO_MANY = 'Too many requests for the export. Wait a moment.'
UNREADABLE = 'The export cannot be read for a moment. Try again shortly.'


class Unreadable(ValueError):
    """What is in the directory is not what the export writes there."""


@dataclass(frozen=True)
class Named:
    """A file the manifest names."""

    size: int
    sha256: str


@dataclass(frozen=True)
class Manifest:
    """The manifest, as a request found it."""

    #: Its bytes, which are served as they are.
    raw: bytes
    #: The files it names that may be served, by name.
    files: Mapping[str, Named]


def opened(path: Path) -> tuple[int, os.stat_result]:
    """`path`, opened to be read, if it is a file, reached by no link:
    its descriptor, and what it is. FileNotFoundError when it is not."""
    try:
        descriptor = os.open(path, OPEN)
    except OSError as error:
        if error.errno == errno.ELOOP:
            raise FileNotFoundError(
                errno.ENOENT, 'a link', str(path),
            ) from None
        raise
    try:
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode):
            raise FileNotFoundError(errno.ENOENT, 'not a file', str(path))
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, status


def read_manifest(directory: Path) -> Manifest:
    """The manifest in `directory`. FileNotFoundError when there is
    none; `Unreadable`, or another OSError, when it cannot be read as
    one."""
    path = directory / MANIFEST_NAME
    descriptor, _ = opened(path)
    with open(descriptor, 'rb') as file:
        raw = file.read(MAX_MANIFEST + 1)
    if len(raw) > MAX_MANIFEST:
        raise Unreadable(f'{path} is over {MAX_MANIFEST} bytes')
    try:
        files = {}
        for entry in json.loads(raw)['files']:
            name, size, sha256 = entry['name'], entry['bytes'], entry['sha256']
            if not (
                isinstance(name, str)
                and type(size) is int and size >= 0
                and isinstance(sha256, str) and SHA256.fullmatch(sha256)
            ):
                raise ValueError(f'not a file the export wrote: {entry!r}')
            # A name the export does not give is never served.
            if FILE.fullmatch(name):
                files[name] = Named(size, sha256)
    except (ValueError, TypeError, KeyError) as error:
        raise Unreadable(f'{path} is not a manifest: {error!r}') from None
    return Manifest(raw, files)


def unreadable(error: Exception) -> NoReturn:
    """A 503, kept by no one, that says nothing of `error`: it names
    the directory, and is for the log."""
    logger.error('export unreadable', error=str(error))
    raise HTTPException(503, UNREADABLE)


class Opened(FileResponse):
    """A file the route opened, sent from what it opened (through
    /dev/fd, as Starlette sends a file by its path): by the time it is
    sent, its name may be another file's, or a link's, and what is sent
    is still what was checked. The descriptor is closed once it is
    answered, however that ends."""

    def __init__(
        self,
        descriptor: int,
        status: os.stat_result,
        *,
        media_type: str,
        headers: Mapping[str, str],
    ) -> None:
        super().__init__(
            f'/dev/fd/{descriptor}', headers=headers, media_type=media_type,
            stat_result=status,
        )
        self.descriptor = descriptor

    async def __call__(
        self, scope: Scope, receive: Receive, send: Send,
    ) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            os.close(self.descriptor)


class Export:
    """The export in `directory`, WEB_EXPORT_DIR, each request for it
    counted against `limit`, EXPORT_RATE_LIMIT.

    Blocking: each request reads the manifest, and opens the file it
    asks for, in the thread it runs in.
    """

    def __init__(self, directory: Path, limit: RateLimiter) -> None:
        self.directory = directory
        self.limit = limit

    def manifest(self, client: str) -> Response:
        """GET /export/manifest.json, for `client`."""
        self._admit(client)
        return Response(
            self._manifest().raw, media_type=JSON,
            headers={'Cache-Control': MANIFEST_AGE},
        )

    def file(self, client: str, name: str) -> Response:
        """GET /export/<name>, for `client`: the file of that name, if
        the manifest names it now."""
        self._admit(client)
        if not FILE.fullmatch(name):
            raise HTTPException(404)
        named = self._manifest().files.get(name)
        if named is None:
            raise HTTPException(404)
        path = self.directory / name
        try:
            descriptor, status = opened(path)
        except FileNotFoundError:
            raise HTTPException(404) from None
        except OSError as error:
            unreadable(error)
        if status.st_size != named.size:
            os.close(descriptor)
            unreadable(
                Unreadable(
                    f'{path} is {status.st_size} bytes, and the manifest '
                    f'says {named.size}',
                ),
            )
        return Opened(
            descriptor, status, media_type=PARQUET,
            headers={
                'Cache-Control': IMMUTABLE,
                # Strong, and the same for as long as the name is.
                'ETag': f'"{named.sha256}"',
            },
        )

    def _admit(self, client: str) -> None:
        if not self.limit.admit(client):
            raise HTTPException(429, TOO_MANY)

    def _manifest(self) -> Manifest:
        """The manifest now, or a 404 without one."""
        try:
            return read_manifest(self.directory)
        except FileNotFoundError:
            raise HTTPException(404) from None
        except (OSError, Unreadable) as error:
            unreadable(error)
