"""Whether `chatsbom collect` is making progress (#171): its heartbeat,
and the check compose's healthcheck runs of it.

A process that has stopped making progress does not exit, so nothing
restarts it, and nothing that watches it for an exit sees it. So the
process says what each of its parts is doing (`Heartbeat`): idle by
choice until a time, as the sweep waits for its next hour, or until it
is woken; or busy with a unit of work that has a deadline, a sweep, a
refresh of the universe, one repository's collection, a step of the
dependency graph or of the index pass. Each deadline is far past what
the unit takes, a wait for a rate limit's window included: a part past
its deadline, or idle past the time it was to wake, is wedged.

Every `TICK` the process writes that down, in `collector.heartbeat` in
the data directory: its pid, when it wrote it, each part's state, and
which are stalled; and a last time as it stops, saying so. The check
(`python -m chatsbom.collector.health`) fails when there is no
heartbeat, when the last was written more than `STALE` ago, the process
no longer running its loop, when it stopped, and when a part is
stalled; it says which, and exits 1. Compose's healthcheck runs it
in the collector's container, from its working directory.

Written by rename, and not synced: a heartbeat a crash loses is one the
next tick writes again.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

#: Its name, in the data directory.
HEARTBEAT = 'collector.heartbeat'

#: How often the process writes it.
TICK = timedelta(seconds=30)

#: How old it may be before the process is taken for one no longer
#: running its loop: ten ticks.
STALE = timedelta(minutes=5)

#: How long past the time it was to wake a part may be idle before it is
#: taken for one that will not: a loop running late is not wedged.
LATE = timedelta(minutes=10)

#: How an instant is written.
_INSTANT = '%Y-%m-%dT%H:%M:%SZ'


def heartbeat_path(data_dir: Path) -> Path:
    """Where the heartbeat is, in the data directory."""
    return Path(data_dir) / HEARTBEAT


def _instant(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(_INSTANT)


def _read_instant(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, _INSTANT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@dataclass(frozen=True)
class PartState:
    """What one part is doing."""

    #: `idle` or `busy`.
    state: str
    since: datetime
    #: Idle: when it is to wake, or None until it is woken. Busy: its
    #: deadline.
    until: datetime | None
    #: For a person: what it waits for, or does.
    doing: str = ''

    def stalled(self, now: datetime) -> bool:
        if self.until is None:
            return False
        if self.state == 'busy':
            return now > self.until
        return now > self.until + LATE


class Heartbeat:
    """What each part of the process is doing, written to `path`."""

    def __init__(self, path: Path, clock: Callable[[], float]) -> None:
        self.path = Path(path)
        self._clock = clock
        self.parts: dict[str, PartState] = {}

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._clock(), timezone.utc)

    def idle(
        self, part: str, until: datetime | None, doing: str = '',
    ) -> None:
        """`part` waits by choice, until `until`, or until it is woken."""
        self.parts[part] = PartState('idle', self.now(), until, doing)

    def busy(self, part: str, deadline: timedelta, doing: str = '') -> None:
        """`part` works on something that ends within `deadline`."""
        now = self.now()
        self.parts[part] = PartState('busy', now, now + deadline, doing)

    def done(self, part: str) -> None:
        """`part` is no more: a collection that ended."""
        self.parts.pop(part, None)

    def stalled(self) -> list[str]:
        """The parts past their deadline, or idle past their time, each
        with what it was doing."""
        now = self.now()
        return [
            f'{name}: {part.state} since {_instant(part.since)}'
            + (f', {part.doing}' if part.doing else '')
            + (
                f', due by {_instant(part.until)}' if part.until is not None
                else ''
            )
            for name, part in sorted(self.parts.items())
            if part.stalled(now)
        ]

    def write(self, *, stopped: bool = False) -> None:
        """The heartbeat, as it stands now; the last, `stopped`, as the
        process ends."""
        document = {
            'pid': os.getpid(),
            'at': _instant(self.now()),
            'stopped': stopped,
            'stalled': self.stalled(),
            'parts': {
                name: {
                    'state': part.state,
                    'since': _instant(part.since),
                    'until': (
                        None if part.until is None else _instant(part.until)
                    ),
                    'doing': part.doing,
                }
                for name, part in sorted(self.parts.items())
            },
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        writing = self.path.with_name(f'.{self.path.name}.{os.getpid()}.tmp')
        writing.write_text(json.dumps(document, indent=1) + '\n')
        os.replace(writing, self.path)


def check(path: Path, now: datetime, *, stale: timedelta = STALE) -> str:
    """Why the process at `path`'s heartbeat is not healthy, or '' while
    it is."""
    try:
        said = json.loads(Path(path).read_text(encoding='utf-8'))
    except FileNotFoundError:
        return (
            f'no heartbeat at {path}: `chatsbom collect` is not running, '
            'or has not started'
        )
    except (OSError, ValueError) as error:
        return f'the heartbeat at {path} cannot be read: {error}'
    if not isinstance(said, dict):
        return f'the heartbeat at {path} is not one'
    written = _read_instant(said.get('at'))
    if written is None:
        return f'the heartbeat at {path} says no time'
    if now - written > stale:
        return (
            f'the last heartbeat was written at {_instant(written)}, '
            f'{int((now - written).total_seconds())} s ago: the collector '
            f'(pid {said.get("pid")}) is not running its loop'
        )
    if said.get('stopped'):
        return (
            f'the collector (pid {said.get("pid")}) stopped at '
            f'{_instant(written)}'
        )
    stalled = said.get('stalled')
    if stalled:
        return 'stalled: ' + '; '.join(str(part) for part in stalled)
    return ''


def main(argv: Sequence[str] | None = None) -> int:
    """`python -m chatsbom.collector.health [DATA_DIR]`: 0 while the
    collector whose data directory it is, `data` unless given, is
    healthy; 1, and why, when it is not."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    data_dir = Path(arguments[0]) if arguments else Path('data')
    now = datetime.fromtimestamp(time.time(), timezone.utc)
    problem = check(heartbeat_path(data_dir), now)
    if problem:
        print(f'unhealthy: {problem}')
        return 1
    print('healthy')
    return 0


if __name__ == '__main__':
    sys.exit(main())
