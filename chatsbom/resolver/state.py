"""resolver.sqlite: what the resolver could not resolve, and when it
tries again (#168).

A failed resolution was not remembered: every run of `sbom lock` tried
it again, a container of project-controlled code each time. Now each is
kept with the collector's backoff (#100 Q5): due again after a quarter
of an hour, doubling with each failure, up to a week.

Kept as collector.sqlite is, on its machinery (`collector.state
.StateFile`): one process writes it, the resolver, and a second is
refused; and it is never what says a directory is resolved, which its
lockfile in the store says (`PathConfig.generated_lock_path`). So
deleting it loses the backoff and nothing else: what failed is tried
again at the next pass.

A failure is one directory's, of one commit, by one recipe. It is kept
as an outcome, by the repository, the ecosystem for its stage, and for
its key the commit, the recipe's fingerprint and the directory: a
recipe whose image, script or hosts moved tries again what the last
one could not.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from chatsbom.collector.state import _instant
from chatsbom.collector.state import BACKOFF_CAP
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Migration
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import Outcome
from chatsbom.collector.state import StateFile
from chatsbom.core.sandbox import LockTarget

#: Its name, in `data/`.
STATE_FILE = 'resolver.sqlite'

#: `PRAGMA application_id` of a resolver.sqlite, `CSBR`: what tells it
#: from a collector.sqlite, `CSBC`, named where it should be.
APPLICATION_ID = 0x43534252

#: A failure not tried again for this long is forgotten (`forget`): its
#: commit is no longer the repository's, or its directory resolved. One
#: still failing is tried weekly, and each try keeps it.
FORGET_AFTER = 2 * BACKOFF_CAP


def _v1(db: sqlite3.Connection) -> None:
    """The first schema: outcomes, as collector.sqlite keeps them."""
    db.execute(f'''
        CREATE TABLE outcome (
            repository_id   INTEGER NOT NULL,
            stage           TEXT NOT NULL,
            key             TEXT NOT NULL,
            kind            TEXT NOT NULL
                            CHECK (kind IN ('{NOTHING}', '{FAILED}')),
            attempts        INTEGER NOT NULL,
            due_at          TEXT NOT NULL,
            detail          TEXT NOT NULL,
            first_at        TEXT NOT NULL,
            last_at         TEXT NOT NULL,
            PRIMARY KEY (repository_id, stage, key)
        )
    ''')
    db.execute('CREATE INDEX outcome_last ON outcome (last_at)')


#: Each step, in order; one that shipped is never changed.
MIGRATIONS: tuple[Migration, ...] = (_v1,)


def state_path(data_dir: Path) -> Path:
    """Where resolver.sqlite is, in the data directory."""
    return Path(data_dir) / STATE_FILE


def outcome_key(sha: str, target: LockTarget) -> tuple[str, str]:
    """The stage and key a resolution of `target` at `sha` is kept by."""
    directory = target.directory or '.'
    return (
        target.ecosystem, f'{sha} {target.recipe.fingerprint} {directory}',
    )


class ResolverState(StateFile):
    """resolver.sqlite, open for the one process that writes it."""

    FILE = STATE_FILE
    APPLICATION_ID = APPLICATION_ID
    MIGRATIONS = MIGRATIONS
    WRITER = 'resolver'
    HOLDS = 'the backoff of what it could not resolve'

    def failure(
        self, repository_id: int, sha: str, target: LockTarget,
    ) -> Outcome | None:
        """The failure kept of `target` at `sha`, if one is."""
        return self.outcome(repository_id, *outcome_key(sha, target))

    def failed(
        self, repository_id: int, sha: str, target: LockTarget, kind: str,
        *, now: datetime, detail: str = '',
    ) -> Outcome:
        """One more resolution of `target` at `sha` that came to nothing
        (`NOTHING`) or failed (`FAILED`), and when it is due again."""
        stage, key = outcome_key(sha, target)
        return self.record(
            repository_id, stage, key, kind, now=now, detail=detail,
        )

    def resolved(
        self, repository_id: int, sha: str, target: LockTarget,
    ) -> None:
        """`target` at `sha` is resolved: what failed of it is forgotten."""
        self.clear(repository_id, *outcome_key(sha, target))

    def forget(self, *, now: datetime) -> int:
        """Forgets each failure not tried again for `FORGET_AFTER`, and
        says how many."""
        cursor = self._db.execute(
            'DELETE FROM outcome WHERE last_at < ?',
            (_instant(now - FORGET_AFTER),),
        )
        return cursor.rowcount
