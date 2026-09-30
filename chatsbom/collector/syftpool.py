"""Syft in a CPU pool (#161; #128 section 2.1).

Syft is the collector's only work bound by the CPU, about 1.6 CPU-seconds
a content root, where everything else waits on the network. So it runs
in subprocesses, in a pool of `cores - 1` slots: the event loop, and
with it every request in flight, keeps a core of its own.

- **A slot** is held for as long as a scan runs. A scan that finds every
  slot taken waits, and the waiting take the slots highest priority
  first (`collector/due.Priority`): a changed repository's scan before a
  rescan for a new Syft, whatever the order they asked in.
- **A timeout** kills a scan that runs past it, and Syft's children with
  it: each scan leads a process group of its own.
- **A memory limit** caps what a scan may hold: the data it writes
  (`RLIMIT_DATA`), set in the process that then becomes Syft. Not the
  address space (`RLIMIT_AS`): Syft is Go, which reserves far more
  than it uses, and Syft 1.52.0 refused to start under 800 MB of it
  while holding under 200 MB. Go is also told the limit, three quarters
  of it (`GOMEMLIMIT`), so that its collector works harder near it
  rather than running into it. A scan that runs out is `memory`, as Go,
  Python or the kernel say it.
- Syft is asked **not to look for a newer version of itself** on every
  scan (`SYFT_CHECK_FOR_APP_UPDATE`): it would ask a server of
  Anchore's.

What a scan writes is Syft's JSON document, held to having Syft's keys
(`SYFT_DOCUMENT_KEYS`) before it is given back; where it is kept is the
SBOM stage's (`collector/stages.py`).

The settings: CHATSBOM_SYFT_SLOTS (`cores - 1`), CHATSBOM_SYFT_TIMEOUT
(`10m`, as `sbom generate` had it; a whole number and a unit, as
detection's intervals are said) and CHATSBOM_SYFT_MEMORY (2 GiB, as
`2GiB` or `1500MB` or bytes; 0 is no limit).
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import json
import os
import re
import shutil
import signal
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import structlog

from chatsbom.collector.settings import interval
from chatsbom.collector.settings import SettingsError
from chatsbom.core.syft import parse_syft_version
from chatsbom.services.sbom_service import DEFAULT_SYFT_TIMEOUT
from chatsbom.services.sbom_service import SYFT_DOCUMENT_KEYS

logger = structlog.get_logger('collector.syft')

#: Seconds a scan may run: what `sbom generate` gave one.
DEFAULT_TIMEOUT = float(DEFAULT_SYFT_TIMEOUT)

#: Bytes a scan may hold. Syft held under 200 MB for a content root of
#: one manifest; a root holds 64 MiB at most (`core/discovery.py`).
DEFAULT_MEMORY = 2 * 2**30

#: Seconds `syft version` may take.
VERSION_TIMEOUT = 30.0

#: What of Syft's stderr an error quotes, at most.
QUOTED = 300

#: Go's soft limit, as a share of the hard one.
_GO_SHARE = (3, 4)

#: The process a limited scan starts as: it sets the limit on the data
#: it may write, then becomes Syft, which keeps the limit.
_LIMITED = (
    'import os, resource, sys\n'
    'limit = int(sys.argv[1])\n'
    'resource.setrlimit(resource.RLIMIT_DATA, (limit, limit))\n'
    'os.execv(sys.argv[2], sys.argv[2:])\n'
)

#: How a process says it ran out of memory: Go, Python, and errno.
_OUT_OF_MEMORY = re.compile(
    r'out of memory|MemoryError|cannot allocate memory', re.IGNORECASE,
)

#: A size, as DuckDB's settings spell one: bytes, or with a unit.
_SIZE = re.compile(r'^\s*(\d+(?:\.\d+)?)\s*([kmgt]i?b|b)?\s*$', re.IGNORECASE)
_UNITS = {
    'b': 1, 'kb': 10**3, 'mb': 10**6, 'gb': 10**9, 'tb': 10**12,
    'kib': 2**10, 'mib': 2**20, 'gib': 2**30, 'tib': 2**40,
}


def default_slots() -> int:
    """A slot per core this process may run on, but one: that one is the
    event loop's."""
    try:
        cores = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        cores = os.cpu_count() or 1
    return max(1, cores - 1)


@dataclass(frozen=True)
class SyftSettings:
    """How Syft is run."""

    #: Scans at once.
    slots: int
    #: Seconds one may run.
    timeout: float
    #: Bytes one may hold; 0 for no limit.
    memory: int
    #: The Syft to run: a name on PATH, or a path.
    command: str = 'syft'


def _slots(value: str | None) -> int:
    if value is None or not value.strip():
        return default_slots()
    try:
        slots = int(value)
    except ValueError:
        slots = 0
    if slots < 1:
        raise SettingsError(
            'CHATSBOM_SYFT_SLOTS',
            f'CHATSBOM_SYFT_SLOTS is how many scans run at once, 1 or more: '
            f'{value!r}',
        )
    return slots


def _timeout(value: str | None) -> float:
    """Seconds, from an interval as detection's are said: `10m`."""
    return interval(
        'CHATSBOM_SYFT_TIMEOUT', value, timedelta(seconds=DEFAULT_TIMEOUT),
    ).total_seconds()


def _memory(value: str | None) -> int:
    if value is None or not value.strip():
        return DEFAULT_MEMORY
    match = _SIZE.match(value)
    if match is None:
        raise SettingsError(
            'CHATSBOM_SYFT_MEMORY',
            'CHATSBOM_SYFT_MEMORY is how much a scan may hold, as 2GiB, '
            f'1500MB or bytes, and 0 for no limit: {value!r}',
        )
    number, unit = match.groups()
    return int(float(number) * _UNITS[(unit or 'b').lower()])


def syft_settings(environ: Mapping[str, str] | None = None) -> SyftSettings:
    """The pool's settings, from `environ`: the process's environment
    unless given. A value it cannot use is refused, naming the setting."""
    if environ is None:
        environ = os.environ
    return SyftSettings(
        slots=_slots(environ.get('CHATSBOM_SYFT_SLOTS')),
        timeout=_timeout(environ.get('CHATSBOM_SYFT_TIMEOUT')),
        memory=_memory(environ.get('CHATSBOM_SYFT_MEMORY')),
    )


def size_text(size: int) -> str:
    """Bytes, as a person reads them: `2 GiB`, `64 MiB`."""
    for unit, scale in (('GiB', 2**30), ('MiB', 2**20), ('KiB', 2**10)):
        if size >= scale and size % scale == 0:
            return f'{size // scale} {unit}'
    return f'{size} bytes'


class SyftFailed(Exception):
    """A scan that gave no document: `kind` says how.

    - `missing`: there is no Syft to run;
    - `timeout`: it ran past the timeout, and was killed;
    - `memory`: it ran out of the memory it may hold;
    - `exit`: it exited with an error;
    - `output`: it exited 0 and wrote no Syft document.
    """

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def _said(stderr: bytes) -> str:
    """What a process's stderr says of why it stopped, in a line: the
    first that says it is an error, else the last."""
    lines = [
        line.strip()
        for line in stderr.decode('utf-8', 'replace').splitlines()
        if line.strip()
    ]
    for line in lines:
        if line.lower().startswith(('fatal error', 'error', 'panic')):
            said = line
            break
    else:
        said = lines[-1] if lines else ''
    return said if len(said) <= QUOTED else f'{said[:QUOTED]}...'


def _kill(process: asyncio.subprocess.Process) -> None:
    """Kill a scan and everything it started: its process group."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


class SyftPool:
    """Syft, run in `settings.slots` subprocesses at most."""

    def __init__(
        self,
        settings: SyftSettings,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self.settings = settings
        #: What each scan's environment starts from: this process's,
        #: unless given.
        self._environ = environ
        #: Scans running now, and at most so far.
        self.running = 0
        self.peak = 0
        self._queue: list[tuple[int, int, asyncio.Future[None]]] = []
        self._order = itertools.count()

    @property
    def waiting(self) -> int:
        """Scans waiting for a slot."""
        return sum(1 for _, _, future in self._queue if not future.done())

    def _command(self) -> str | None:
        return shutil.which(self.settings.command)

    def _environment(self) -> dict[str, str]:
        environment = dict(
            os.environ if self._environ is None else self._environ,
        )
        environment['SYFT_CHECK_FOR_APP_UPDATE'] = 'false'
        if self.settings.memory:
            part, whole = _GO_SHARE
            environment['GOMEMLIMIT'] = str(
                self.settings.memory * part // whole,
            )
        return environment

    async def version(self) -> str | None:
        """The version of the Syft a scan runs now, as it says it; None if
        it cannot be told. Asked every time: a Syft upgraded on disk is
        the one the next scan runs."""
        command = self._command()
        if command is None:
            return None
        process = await asyncio.create_subprocess_exec(
            command, 'version', '-o', 'json',
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=self._environment(),
            start_new_session=True,
        )
        try:
            output, _ = await asyncio.wait_for(
                process.communicate(), VERSION_TIMEOUT,
            )
        except TimeoutError:
            _kill(process)
            await process.communicate()
            return None
        except BaseException:
            _kill(process)
            await process.communicate()
            raise
        if process.returncode != 0:
            return None
        return parse_syft_version(output.decode('utf-8', 'replace'))

    async def scan(self, directory: Path, *, priority: int = 0) -> bytes:
        """Syft's JSON document of `directory`, once a slot is free:
        highest `priority` first, the lowest number. Raises `SyftFailed`
        for a scan that gave none."""
        command = self._command()
        if command is None:
            raise SyftFailed(
                'missing',
                f'No Syft to run: {self.settings.command} is not on PATH',
            )
        await self._acquire(priority)
        try:
            return await self._scan(command, directory)
        finally:
            self._release()

    async def _scan(self, command: str, directory: Path) -> bytes:
        settings = self.settings
        target = f'dir:{directory.absolute()}'
        argv = [command, target, '-o', 'json']
        if settings.memory:
            argv = [
                sys.executable, '-c', _LIMITED,
                str(settings.memory), *argv,
            ]
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._environment(),
            start_new_session=True,
        )
        try:
            output, errors = await asyncio.wait_for(
                process.communicate(), settings.timeout,
            )
        except TimeoutError:
            _kill(process)
            await process.communicate()
            raise SyftFailed(
                'timeout',
                f'Syft killed after {settings.timeout:g} s, scanning {target}',
            ) from None
        except BaseException:
            # Given up on: the scan goes too.
            _kill(process)
            await process.communicate()
            raise
        # Waited for by `communicate`: it has one.
        returncode = (
            process.returncode if process.returncode is not None else -1
        )
        if returncode != 0:
            said = _said(errors)
            if _OUT_OF_MEMORY.search(said) or _OUT_OF_MEMORY.search(
                errors.decode('utf-8', 'replace'),
            ) or returncode == -signal.SIGKILL:
                raise SyftFailed(
                    'memory',
                    'Syft ran out of the memory a scan may hold, '
                    f'{size_text(settings.memory)}, scanning {target}'
                    + (f': {said}' if said else ''),
                )
            how = (
                f'was killed by signal {-returncode}' if returncode < 0
                else f'exited {returncode}'
            )
            raise SyftFailed(
                'exit',
                f'Syft {how}, scanning {target}' +
                (f': {said}' if said else ''),
            )
        if not await asyncio.to_thread(_is_document, output):
            raise SyftFailed(
                'output',
                f'Syft wrote no Syft document, scanning {target}: '
                f'{len(output)} bytes',
            )
        return output

    # -- the slots ----------------------------------------------------------

    async def _acquire(self, priority: int) -> None:
        if self.running < self.settings.slots and not self.waiting:
            self._take()
            return
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._queue, (priority, next(self._order), future))
        try:
            await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                # Handed a slot as it was given up on: it goes on.
                self._release()
            raise

    def _take(self) -> None:
        self.running += 1
        self.peak = max(self.peak, self.running)

    def _release(self) -> None:
        self.running -= 1
        while self._queue and self.running < self.settings.slots:
            _, _, future = heapq.heappop(self._queue)
            if future.done():
                continue
            self._take()
            future.set_result(None)


def _is_document(output: bytes) -> bool:
    """Whether `output` is a Syft JSON document: an object with Syft's
    keys."""
    try:
        document = json.loads(output)
    except ValueError:
        return False
    return isinstance(document, dict) and SYFT_DOCUMENT_KEYS <= document.keys()
