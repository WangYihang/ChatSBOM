"""Data access layer implementing CQRS (Command Query Responsibility Segregation).

Two design notes that the SQL below depends on:

No query joins against `artifacts` with `FINAL`. Deduplication comes from
the join condition instead: a Syft row belongs to the current scan only
if its `sbom_commit_sha` matches the one recorded on its repository, and
a dependency-graph row only if it came from the graph document recorded
there, so superseded scans and graphs drop out without a merge pass over
millions of rows.
`FINAL` appears only on `repositories` — tens of thousands of rows — and
in `get_stats`, where a raw `count()` would report un-merged duplicates.

That condition is `ON_CURRENT_SCAN`, the same one the `current_artifacts`
view is built on. A query that joins `repositories` for its owner, stars
or language applies it in that join; one that needs nothing from
`repositories` reads the view. Reading the view *and* joining for the
metadata would join `repositories FINAL` twice for one answer.

Counts are always `count(DISTINCT repository_id)`. A repository can
contribute several artifact rows for one package — two catalogers finding
it, or a package appearing at several versions — and counting rows made
"how many projects use X" overstate itself.
"""
from __future__ import annotations

from abc import ABC
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import datetime
from types import MappingProxyType
from typing import Any
from typing import Self
from typing import TYPE_CHECKING

import structlog

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.definitions import fingerprint
from chatsbom.core.definitions import reads
from chatsbom.core.definitions import renamed
from chatsbom.core.definitions import replacing
from chatsbom.core.definitions import stamped
from chatsbom.core.dictionaries import DICTIONARIES
from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.instants import utc
from chatsbom.core.rollups import OBSOLETE_ROLLUPS
from chatsbom.core.rollups import REFRESH_SETTINGS
from chatsbom.core.rollups import ROLLUPS
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ddl_column_definitions
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import ddl_engine
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import language_bucket_sql
from chatsbom.core.schema import ON_CURRENT_SCAN
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.schema import TABLE_DDL
from chatsbom.core.schema import VIEW_DDL
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT
from chatsbom.models.query import AdoptionPoint
from chatsbom.models.query import CorpusCoverage
from chatsbom.models.query import DatabaseStats
from chatsbom.models.query import Dependent
from chatsbom.models.query import EcosystemCoverage
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
from chatsbom.models.query import Row
from chatsbom.models.query import row_mapper
from chatsbom.models.query import VersionObservation
from chatsbom.models.relationship import DIRECT

if TYPE_CHECKING:
    import pyarrow as pa
    from clickhouse_connect.driver.client import Client

logger = structlog.get_logger('repository')

Parameters = dict[str, Any]
#: ClickHouse settings for one query, by name.
Settings = Mapping[str, Any]


class BaseRepository(ABC):
    """Abstract base repository handling connection lifecycle."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._client: Client | None = None

    @property
    def client(self) -> Client:
        if self._client is None:
            # Imported here rather than at the top: clickhouse-connect
            # imports pandas, numpy and pyarrow when they are installed,
            # and the CLI imports this module at start-up, whichever
            # command runs. Most never connect.
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                **self.config.get_connection_params(),
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()


class IngestionRepository(BaseRepository):
    """Write-only repository for Admin operations (Collect, Enrich, Index)."""

    #: Tables being rebuilt, and where their writes go meanwhile. Empty
    #: and shared until `rebuilding` gives an instance its own.
    _rebuilding: Mapping[str, str] = MappingProxyType({})

    def ensure_schema(self, rebuild: set[str] | None = None) -> None:
        """Bring the schema to the declared state.

        `rebuild` names tables about to be rebuilt (`rebuilding`), the
        escape hatch for drift the additive path cannot repair. It has
        to be part of this method rather than a separate call, because
        calling `ensure_schema` first is what blocked the rebuild: the
        engine check aborted before the one command able to fix the
        drift could run. So a table named here and on another engine is
        left as it is, to serve its readers until its rebuild swaps it
        out; one on the declared engine gains any missing column, so
        that its rows can be carried into the rebuild.

        Nothing is discarded here any more. This dropped the table,
        and the ingest after it refilled what readers saw from empty.

        A rebuild of `repositories` declares the dictionary that loads
        it again, and no other rebuild touches it.
        """
        rebuild = rebuild or set()
        managed = {name for name, _ in TABLE_DDL}
        for table in rebuild:
            if table not in managed:
                raise ValueError(
                    f"{table!r} is not a managed table; "
                    f"expected one of {', '.join(sorted(managed))}",
                )
        # Outside the `try`, which would take a missing driver for an
        # unreachable `default`. Imported here as in `client`.
        import clickhouse_connect

        try:
            bootstrap = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.user,
                password=self.config.password,
                database='default',
            )
        except Exception:
            # The target database may already exist and be reachable even
            # when `default` is not; fall through to the DDL below.
            pass
        else:
            with bootstrap:
                bootstrap.command(
                    f"CREATE DATABASE IF NOT EXISTS {self.config.database}",
                )

        for table, ddl in TABLE_DDL:
            self.client.command(ddl)
            if table in rebuild and self._engine(table) != ddl_engine(ddl):
                logger.info('Table left for its rebuild', table=table)
                continue
            self._assert_engine(table, ddl)
            self._reconcile_columns(table, ddl)

        # Views and the dictionary before the rollups, which read them.
        # Each is declared again when its definition differs from the
        # one it carries (`core/definitions.py`), and what that changed
        # is passed on: a rollup reading a replaced view is refreshed,
        # or it goes on describing the old one for up to a day.
        changed = self._ensure_views()
        changed |= self._ensure_dictionaries(
            recreate=REPOSITORIES.name in rebuild,
        )
        self._ensure_rollups(changed)

    def _declared(self, names: Iterable[str]) -> dict[str, str]:
        """The fingerprint each object was declared with, by name.

        Read from its COMMENT. An object that is not there is not in the
        answer, and one declared before the fingerprints has an empty
        one, which matches nothing.
        """
        rows = self.client.query(
            'SELECT name, comment FROM system.tables '
            'WHERE database = {db:String} AND name IN {names:Array(String)}',
            parameters={'db': self.config.database, 'names': list(names)},
        ).result_rows
        return {str(name): str(comment) for name, comment in rows}

    def _ensure_views(self) -> set[str]:
        """Declare the views, replacing any declared differently.

        `CREATE OR REPLACE VIEW` is one atomic step in an Atomic
        database, so a reader finds the old view or the new one and
        never neither. A view stores no rows, so replacing one costs
        nothing but its readers' next answer.

        Not guarded the way the rollups are: a view that cannot be
        declared is a schema bug, and every current-state reader would
        fail on it anyway.

        Returns the views whose answers may have changed: those
        replaced, and those reading one.
        """
        declared = self._declared(name for name, _ in VIEW_DDL)
        changed: set[str] = set()
        for name, ddl in VIEW_DDL:
            stamp = self._view_fingerprint(ddl)
            if declared.get(name) != stamp:
                self.client.command(replacing(stamped(ddl, stamp)))
                logger.info('View declared', view=name)
                changed.add(name)
            elif any(reads(ddl, other) for other in changed):
                changed.add(name)
        return changed

    @staticmethod
    def _view_fingerprint(ddl: str) -> str:
        """A view's fingerprint, taken with the tables it reads.

        ClickHouse fixes a view's columns when it creates the view, so
        `current_artifacts`' `SELECT a.*` is the columns `artifacts` had
        that day, and a column declared since has to declare the view
        again to be read through it.
        """
        return fingerprint(
            ddl,
            *(declared for table, declared in TABLE_DDL if reads(ddl, table)),
        )

    def _ensure_dictionaries(self, recreate: bool = False) -> set[str]:
        """Declare the dimension dictionaries, replacing any declared
        differently.

        Before the rollups, because a rollup could read one. The
        credentials are interpolated rather than bound: this is DDL, and
        a dictionary's SOURCE clause takes them as literals. They come
        from this process's own configuration, never from a request, and
        stay out of the fingerprint (`core/definitions.py`).

        Without the fingerprint a corrected dictionary never reached a
        database that already had the old one: the `QUERY ... FINAL`
        fix nearly did not, and the two attributes #21 and #22 added did
        not, which the dashboard's dependants query then failed on.

        `recreate` declares them again whatever they carry.

        Returns the dictionaries declared, for `ensure_schema` to pass
        on to the rollups.
        """
        declared = self._declared(name for name, _ in DICTIONARIES)
        changed: set[str] = set()
        for name, ddl in DICTIONARIES:
            try:
                if not recreate and declared.get(name) == (
                    self._dictionary_fingerprint(ddl)
                ):
                    continue
                self._declare_dictionary(name, ddl)
                changed.add(name)
            except Exception as error:
                # The dashboard's dependants query fails without it —
                # it has no join to fall back on — but an ingest must
                # not: its rows matter more, and the next
                # `ensure_schema` declares the dictionary again.
                logger.warning(
                    'Could not declare dictionary',
                    dictionary=name, error=str(error),
                )
        return changed

    def _dictionary_fingerprint(self, ddl: str) -> str:
        """The dictionary's fingerprint: its database filled in, its
        credentials left as placeholders."""
        return fingerprint(ddl.replace('{database}', self.config.database))

    def _declare_dictionary(self, name: str, ddl: str) -> None:
        """Declare a dictionary in one step, then load it.

        `CREATE OR REPLACE`: a dictionary dropped first was, until the
        CREATE, not there at all, and the dependants panel failed with
        "Dictionary not found". The load puts the cost of the first read
        of a replaced dictionary here rather than on a visitor.
        """
        self.client.command(
            replacing(
                stamped(
                    ddl.format(
                        database=self.config.database,
                        user=self.config.user,
                        password=self.config.password,
                    ),
                    self._dictionary_fingerprint(ddl),
                ),
            ),
        )
        self.client.command(f'SYSTEM RELOAD DICTIONARY {name}')
        logger.info('Dictionary declared', dictionary=name)

    def _ensure_rollups(self, changed: Iterable[str] = ()) -> None:
        """Declare the refreshable rollups, in dependency order, and
        keep each one's rows in step with its definition.

        - One that is missing is created, which starts its first
          refresh.
        - One declared differently is replaced (`_replace_rollup`).
        - One reading something replaced before it — a view in
          `changed`, or a rollup above it — is refreshed. Otherwise it
          would summarise the previous definition until the daily
          refresh: replaced alone, `mv_packages` left `mv_totals`
          counting the packages it no longer held.
        - Any other is left alone. Its stored rows are the expensive
          part, and nothing it reads has moved.

        A created rollup is waited for only before this computes
        something from it. Waiting on each as it was created made the
        fifteen first refreshes of an empty database run one after
        another, 193 ms of a 383 ms `ensure_schema` — once per test,
        through the ClickHouse fixture — where nothing reads them.
        """
        self._drop_obsolete_rollups()
        declared = self._declared(name for name, _ in ROLLUPS)
        changed = set(changed)
        # Created here, their first refresh perhaps still running.
        filling: list[str] = []
        for name, ddl in ROLLUPS:
            try:
                if name not in declared:
                    self._create_rollup(name, ddl)
                    filling.append(name)
                else:
                    current = declared[name] == fingerprint(ddl)
                    if current and not any(
                        reads(ddl, other) for other in changed
                    ):
                        continue
                    while filling:
                        self._wait(filling.pop())
                    if current:
                        self._refresh(name)
                    else:
                        self._replace_rollup(name, ddl)
            except Exception as error:
                # A missing rollup costs latency, not correctness: every
                # panel has a base-table query behind it. So this must
                # not abort an ingest.
                logger.warning(
                    'Could not declare rollup', view=name, error=str(error),
                )
                continue
            changed.add(name)

    def _drop_obsolete_rollups(self) -> None:
        """Drop the rollups an earlier release declared and this one
        does not (`OBSOLETE_ROLLUPS`).

        Left in place, each would go on refreshing daily, keyed by what
        no longer selects anything, and a reader of the old name would
        get an answer rather than an error.
        """
        for name in self._declared(OBSOLETE_ROLLUPS):
            try:
                self.client.command(f'DROP VIEW IF EXISTS {name}')
                logger.info('Obsolete rollup dropped', view=name)
            except Exception as error:
                logger.warning(
                    'Could not drop obsolete rollup',
                    view=name, error=str(error),
                )

    def _create_rollup(self, name: str, ddl: str) -> None:
        """Create a rollup that is not there, which starts its first
        refresh (`_wait` for it)."""
        self.client.command(
            stamped(ddl, fingerprint(ddl)), settings=REFRESH_SETTINGS,
        )
        logger.info('Rollup declared', view=name)

    def _wait(self, name: str) -> None:
        """Return once a rollup's running refresh has finished.

        After a CREATE this is its first refresh, and WAIT alone saw the
        rows there 200 times in 200, where a REFRESH as well would
        compute them twice.
        """
        self.client.command(
            f'SYSTEM WAIT VIEW {name}', settings=REFRESH_SETTINGS,
        )

    def _replace_rollup(self, name: str, ddl: str) -> None:
        """Swap in the rollup `ddl` declares, already refreshed.

        A refreshable view has no `CREATE OR REPLACE`, and dropping it
        first left the panel it serves failing until the CREATE and
        empty until the refresh after that. So the new one is built
        aside, EMPTY so that its one refresh is the one waited for
        here, and exchanged with the old in a single atomic step: a
        reader finds the old rows until then and the new rows after.
        An exchange carries each view's rows and COMMENT with it.

        One whose refresh fails is dropped, and the old one keeps
        serving; the next `ensure_schema` tries again.
        """
        staged = f'{name}_next'
        # What an interrupted replacement left.
        self.client.command(f'DROP VIEW IF EXISTS {staged}')
        self.client.command(
            renamed(stamped(ddl, fingerprint(ddl), empty=True), staged),
            settings=REFRESH_SETTINGS,
        )
        try:
            self._refresh(staged)
            self.client.command(f'EXCHANGE TABLES {staged} AND {name}')
        finally:
            # Once exchanged, this is the old one.
            self.client.command(f'DROP VIEW IF EXISTS {staged}')
        logger.info('Rollup replaced', view=name)

    def _refresh(self, name: str) -> None:
        """Recompute one rollup, and return once it has."""
        # REFRESH only *schedules*; it returns before the view has any
        # rows. Without the WAIT, `mv_totals` computed itself from a
        # `mv_package_language` that was still empty and stored four
        # wrong numbers — measured, not hypothesised.
        self.client.command(
            f'SYSTEM REFRESH VIEW {name}', settings=REFRESH_SETTINGS,
        )
        self.client.command(
            f'SYSTEM WAIT VIEW {name}', settings=REFRESH_SETTINGS,
        )

    def reload_dictionaries(self) -> None:
        """Pull the dimension tables into memory again.

        `LIFETIME` already refreshes them within ten minutes, so this is
        for the one case that cannot wait: an ingest has just rewritten
        `repositories`, and until the reload the dependants panel shows
        the previous run's stars beside this run's dependencies.

        A reload the server refuses to authenticate is the one thing a
        password change does to a dictionary: its SOURCE holds the
        credentials it was declared with, which its fingerprint leaves
        out. That one is declared again from this process's
        configuration. Any other failure is left as it is, and
        ClickHouse goes on serving the last load.
        """
        for name, ddl in DICTIONARIES:
            try:
                try:
                    self.client.command(f'SYSTEM RELOAD DICTIONARY {name}')
                except Exception as error:
                    if 'AUTHENTICATION_FAILED' not in str(error):
                        raise
                    self._declare_dictionary(name, ddl)
                logger.info('Dictionary reloaded', dictionary=name)
            except Exception as error:
                logger.warning(
                    'Could not reload dictionary',
                    dictionary=name, error=str(error),
                )

    def refresh_rollups(
        self,
        recreate: bool = False,
        reading: Iterable[str] | None = None,
    ) -> None:
        """Recompute the rollups from the base tables.

        Called at the end of an ingest rather than left to the daily
        timer: the data changes only when an ingest runs, and a panel
        reading yesterday's rollup beside today's point lookups would
        disagree with itself.

        `recreate` declares each one again first, whatever it carries,
        the way `ensure_schema` replaces a changed one: built aside and
        swapped in, so no panel finds its rollup missing or empty.

        `reading` refreshes only the rollups that read one of these
        tables, directly or through a view or another rollup. `db edges`
        changes `edges` alone, and recomputing the other thirteen would
        be work that cannot change an answer.

        Order matters: `mv_totals` and `mv_top_packages` read the
        rollups above them, so refreshing a derived view before its
        source summarises the previous run.
        """
        moved: set[str] | None = None
        if reading is not None:
            moved = set(reading)
            for name, ddl in VIEW_DDL:
                if any(reads(ddl, other) for other in moved):
                    moved.add(name)
        declared = (
            self._declared(name for name, _ in ROLLUPS) if recreate else {}
        )
        for name, ddl in ROLLUPS:
            if moved is not None:
                if not any(reads(ddl, other) for other in moved):
                    continue
                moved.add(name)
            if not recreate:
                self._refresh(name)
            elif name in declared:
                self._replace_rollup(name, ddl)
            else:
                self._create_rollup(name, ddl)
                self._wait(name)
            logger.info('Rollup refreshed', view=name)

    def _assert_engine(self, table: str, ddl: str) -> None:
        """Refuse to half-migrate a table whose engine has changed.

        Column reconciliation is additive; it cannot convert an engine or
        rewrite a sort key. `artifacts` moved from ReplacingMergeTree to
        MergeTree when it became append-only, and without this check the
        old table would quietly gain the new column and keep the old
        engine — surfacing much later as an unresolved-identifier error
        during export, which says nothing about the cause.
        """
        wanted = ddl_engine(ddl)
        if not wanted:
            return

        actual = self._engine(table)
        if not actual or actual == wanted:
            return

        raise RuntimeError(
            f"Table {table!r} uses {actual} but the schema now declares "
            f"{wanted}. Adding columns cannot convert an engine, so this "
            f"needs a rebuild:\n\n"
            f"    chatsbom db index --rebuild\n\n"
            f"Existing rows are discarded; they are re-ingested from "
            f"data/07-sbom.",
        )

    def _engine(self, table: str) -> str:
        """The engine a table is on, or '' if it is not there."""
        rows = self.client.query(
            'SELECT engine FROM system.tables '
            'WHERE database = {db:String} AND name = {table:String}',
            parameters={'db': self.config.database, 'table': table},
        ).result_rows
        return str(rows[0][0]) if rows else ''

    def _reconcile_columns(self, table: str, ddl: str) -> None:
        """Add columns the DDL declares but the existing table lacks.

        `CREATE TABLE IF NOT EXISTS` does nothing when the table exists,
        so a database created before a column was declared kept working
        until an insert failed inside the driver with "Unrecognized
        column". Migration is additive and metadata-only in ClickHouse for
        a defaulted column, so it is safe to run on every startup; columns
        the DDL no longer mentions are left alone rather than dropped.
        """
        existing = {
            str(row[0])
            for row in self.client.query(
                'SELECT name FROM system.columns '
                'WHERE database = {db:String} AND table = {table:String}',
                parameters={'db': self.config.database, 'table': table},
            ).result_rows
        }
        if not existing:
            return

        for column, definition in ddl_column_definitions(ddl).items():
            if column in existing:
                continue
            self.client.command(
                f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS '
                f'{column} {definition}',
            )
            logger.info(
                'Schema migrated', table=table, added_column=column,
            )

    def reset_schema(self) -> None:
        """Drop and recreate schema (Destructive)."""
        for table in (ARTIFACTS, RELEASES, REPOSITORIES):
            self.client.command(f'DROP TABLE IF EXISTS {table.name}')
        self.ensure_schema()

    def insert_batch(
        self,
        table: str,
        data: list[list[Any]],
        columns: list[str],
    ) -> None:
        """Generic batch insert, into a table's rebuild while one runs."""
        if not data:
            return
        self.client.insert(self._into(table), data, column_names=columns)

    def _into(self, table: str) -> str:
        """Where a write to `table` goes: its rebuild, while one runs."""
        return self._rebuilding.get(table, table)

    @staticmethod
    def _declaration_of(table: str) -> str:
        for name, ddl in TABLE_DDL:
            if name == table:
                return ddl
        raise ValueError(
            f"{table!r} is not a managed table; "
            f"expected one of {', '.join(sorted(n for n, _ in TABLE_DDL))}",
        )

    @contextmanager
    def rebuilding(self, table: str, carry: bool = True) -> Iterator[None]:
        """Build `table` again from its declaration, and swap it in once
        it is full.

        The rebuild used to drop the table and refill it: for the seven
        minutes that takes over the corpus, every reader saw a table
        filling up from empty, and an ingest that failed left it that
        way. Now the new table is built as `<table>_next`, and inside
        this block every write to `table` goes there: `insert_batch`,
        `forget_scans`, `forget_graphs`. The table itself serves its
        readers, untouched, until `EXCHANGE TABLES` swaps the two in one
        atomic step as the block ends. An exception drops the new table
        and leaves the old one as it was.

        The views and rollups read the table by name, so they read the
        new one from the exchange on. The rollups hold what they last
        computed, and want a refresh after it.

        `carry` copies the rows already there into the new one first.
        For `artifacts` those are the history: an older scan exists
        nowhere else, since the ledger keeps one record per repository,
        `data prune` deletes the older SBOMs, and `RawDocuments` reads a
        repository's current documents. Re-deriving only what the
        documents say discarded it for good. The ingest then forgets and
        writes again the observations it re-reads, as a plain `db index`
        does, so each is there once.

        Not from a table on another engine. That is the drift
        `_assert_engine` refuses to migrate in place, and its rows are
        not to be read as this table's: the refusal says they are
        discarded, and they are.

        One at a time, and not beside another ingest: a second rebuild
        starts by dropping this one's table, and whatever another
        process writes to `table` meanwhile goes to the one the swap
        retires.
        """
        ddl = self._declaration_of(table)
        staged = f'{table}_next'
        # What a rebuild that did not finish left.
        self.client.command(f'DROP TABLE IF EXISTS {staged}')
        self.client.command(renamed(ddl, staged))
        if carry and self._engine(table) == ddl_engine(ddl):
            columns = ', '.join(ddl_columns(ddl))
            self.client.command(
                f'INSERT INTO {staged} ({columns}) '
                f'SELECT {columns} FROM {table}',
            )
        self._rebuilding = {**self._rebuilding, table: staged}
        try:
            yield
        except BaseException:
            self.client.command(f'DROP TABLE IF EXISTS {staged}')
            raise
        finally:
            self._rebuilding = {
                name: into for name, into in self._rebuilding.items()
                if name != table
            }
        self.client.command(f'EXCHANGE TABLES {staged} AND {table}')
        # Once exchanged, this is the old one.
        self.client.command(f'DROP TABLE {staged}')
        logger.info('Table rebuilt', table=table)

    def rebuild_table(self, table: str) -> None:
        """Drop one table and recreate it from the current DDL.

        Needed when a schema change makes existing rows unreachable
        rather than merely incomplete. The off-by-one fix did exactly
        that: 6.1M artifact rows carry a 7-character `sbom_commit_sha`,
        so the scan-matching join excludes them and no amount of
        re-ingestion replaces them.
        """
        ddl = self._declaration_of(table)
        self.client.command(f'DROP TABLE IF EXISTS {table}')
        self.client.command(ddl)
        logger.info('Table rebuilt', table=table)

    def optimize(self) -> None:
        """Merge what an ingest wrote.

        `repositories` with FINAL. Every reader of it applies FINAL —
        `rollups_test.py` holds the rollups, views and dictionary to
        that — so this is for what FINAL costs them, not for what they
        answer. Measured after a re-index wrote all 28,075 repositories
        again, 1,000 to an insert: FINAL over the parts it left took the
        dictionary's load from 8.4 ms to 18.4 ms, and to 15.7 ms after
        ten seconds of background merges; the `current_artifacts` join
        from 57.9 ms to 76.6 ms. This takes 62 ms, 26 ms after
        `--limit 10`, and the dictionary loads the table every five to
        ten minutes.

        Not `releases`. Its one reader, `db status`, counts it through
        FINAL, and `OPTIMIZE ... FINAL` rewrote the whole table on every
        run, whatever was indexed: 1.74 s for a million releases,
        measured, against 51 ms that count waits until background
        merges catch up.

        `artifacts` is a plain MergeTree, merged for read efficiency
        with nothing to collapse.
        """
        self.client.command(f'OPTIMIZE TABLE {REPOSITORIES.name} FINAL')
        self.client.command(f'OPTIMIZE TABLE {ARTIFACTS.name}')

    def forget_scans(self, scans: Sequence[tuple[int, str]]) -> int:
        """Drop the rows of these exact scans: Syft's and the manifests'.

        `artifacts` is append-only on purpose: a row is an observation,
        and a repository re-scanned at a new commit should keep the old
        rows — "how long did projects take to move off mail 2.7" is
        unanswerable once they are gone. Which is why nothing here
        deduplicates it.

        The gap that leaves: re-running the transform over *unchanged*
        documents writes the same observation again. `db index
        --language python` appended 687,000 duplicate rows, and the
        refusal message for `--rebuild --language` recommended that
        command as the way to refresh one language.

        So the unit deleted is the scan — `(repository_id,
        sbom_commit_sha)` — and not the repository. Re-ingesting a scan
        replaces itself; a scan at a different commit is a different
        observation and is left alone, which is exactly the history the
        table exists to keep.

        Syft rows, and the `manifest` rows read from the same scan's
        Gradle files (`core/gradle.py`): they are stamped with its
        commit and written with it, so they are forgotten with it. A
        dependency-graph row carries the scan's commit
        and is not part of the scan: GitHub produced it from the default
        branch, at another time, and it is its own document. Keyed on the
        commit alone, this deleted every graph indexed beside the scan —
        the current one, and the history of the ones before it (#22).
        `forget_graphs` forgets a graph.

        Returns the number of scans named, not rows deleted: ClickHouse
        lightweight deletes are asynchronous masks, so a row count here
        would be a guess dressed as a measurement.
        """
        if not scans:
            return 0
        # Chunked because the predicate is inlined: 24,451 pairs in one
        # statement is a query ClickHouse parses for longer than it
        # spends deleting.
        for start in range(0, len(scans), _FORGET_CHUNK):
            chunk = scans[start:start + _FORGET_CHUNK]
            pairs = ', '.join(
                f"({int(repository_id)}, '{_quoted(sha)}')"
                for repository_id, sha in chunk
            )
            self.client.command(
                f'DELETE FROM {self._into(ARTIFACTS.name)} WHERE '
                f"source IN ('{SYFT}', '{MANIFEST}') AND "
                f'(repository_id, sbom_commit_sha) IN ({pairs})',
            )
        logger.info('Scans forgotten', scans=len(scans))
        return len(scans)

    def forget_graphs(self, graphs: Sequence[tuple[int, datetime]]) -> int:
        """Drop the rows of these exact dependency-graph documents.

        The graph's counterpart to `forget_scans`, for the same gap:
        indexing the same graph again writes the same observation
        again. A repository with no Syft target was never forgotten at
        all, so every `db index` added another copy of its graph.

        A document is named by the instant it states, which its rows
        carry as `observed_at` (`DbService.graph_observed_at`), and that
        is the unit deleted: another document of the same repository is
        history and stays. So do copies of this one written before #22
        under some other commit, which are deleted with it — the same
        observation, stored twice.

        As seconds since the epoch on both sides, which no zone moves.
        A statement of 500 documents took 274 ms on 2,000,000 synthetic
        rows, where one of 500 scans took 338 ms.
        """
        if not graphs:
            return 0
        for start in range(0, len(graphs), _FORGET_CHUNK):
            chunk = graphs[start:start + _FORGET_CHUNK]
            pairs = ', '.join(
                f'({int(repository_id)}, {int(utc(observed).timestamp())})'
                for repository_id, observed in chunk
            )
            self.client.command(
                f'DELETE FROM {self._into(ARTIFACTS.name)} WHERE '
                f"source = '{DEPGRAPH}' AND "
                '(repository_id, toUnixTimestamp(observed_at)) '
                f'IN ({pairs})',
            )
        logger.info('Graphs forgotten', graphs=len(graphs))
        return len(graphs)


#: Scans per DELETE statement.
_FORGET_CHUNK = 500


def _quoted(value: str) -> str:
    """A commit sha, safe to inline.

    These are hex from the GitHub API, so nothing should need escaping —
    which is the argument for doing it anyway rather than for trusting
    it, since the one that does not look like a sha is the one that
    matters.
    """
    return value.replace('\\', '\\\\').replace("'", "\\'")


# The corpus's repositories (the current search snapshot, D2 on #55),
# deduplicated once by the view so joins do not need FINAL. Joined on
# `ON_CURRENT_SCAN`, so an artifact belongs to the current observations
# of its repository: the Syft scan and the graph document it records.
_CURRENT_REPOS = """
SELECT id, owner, repo, stars, url, github_language, sbom_commit_sha,
       depgraph_observed_at
FROM corpus
"""

#: An artifact row's ecosystem, as the rollups key it.
_ARTIFACT_ECOSYSTEM = canonical_sql('a.type')

#: The same scans, for a query with no other reason to join
#: `repositories`.
_CURRENT_ARTIFACTS = 'current_artifacts'


class QueryRepository(BaseRepository):
    """Read-only repository for Guest operations (Query, Chat, Status)."""

    def _rows(self, sql: str, parameters: Parameters | None = None) -> list[Row]:
        """Run a query and return rows keyed by column name."""
        result = self.client.query(sql, parameters=parameters or {})
        return list(result.named_results())

    def stream_rows(
        self,
        sql: str,
        parameters: Parameters | None = None,
        settings: Settings | None = None,
    ) -> Iterator[Row]:
        """Stream a result set block by block, keyed by column name.

        For exports large enough that materialising every row at once is
        the wrong shape. `settings` apply to this query alone.
        """
        with self.client.query_row_block_stream(
            sql, parameters=parameters or {}, settings=dict(settings or {}),
        ) as stream:
            columns: list[str] | None = None
            for block in stream:
                if columns is None:
                    columns = list(stream.source.column_names)
                for row in block:
                    yield dict(zip(columns, row))

    def stream_arrow(
        self,
        sql: str,
        parameters: Parameters | None = None,
        settings: Settings | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Stream a result set as Arrow record batches, one per block.

        For a file written as Arrow: the rows never become Python
        objects, and a batch is let go once it is written. Strings come
        as strings rather than bytes, or the query fails here, on an
        account not allowed to ask for them.
        """
        with self.client.query_arrow_stream(
            sql,
            parameters=parameters or {},
            settings=dict(settings or {}),
            use_strings=True,
        ) as stream:
            yield from stream
            # The rest of the response, read before it is closed, as the
            # driver reads its own streams (`ResponseSource.close`).
            # Arrow's reader stops at its end-of-stream marker, before
            # the response ends, and a response closed unread takes its
            # connection with it. The next query then went out on a new
            # connection while the server could still be finishing this
            # one, in the same session: `SESSION_IS_LOCKED`, now and
            # then, on the Parquet export's next table. Read to the end,
            # the connection goes back to the pool, and the next query
            # waits behind this one on it.
            stream.source.drain_conn()

    def has_edges(self) -> bool:
        """Whether `db edges` has stored any package-to-package edge."""
        [row] = self._rows(f'SELECT count() AS n FROM {EDGES.name}')
        return int(row['n']) > 0

    def stream_edges(
        self,
        settings: Settings | None = None,
    ) -> Iterator[tuple[str, str, int]]:
        """Package-to-package edges as `db edges` stored them: each pair
        once, as `(parent, child, repositories)`.

        Summed, because `edges` is a SummingMergeTree: a pair can sit in
        more than one part until a merge folds them together, and only
        `sum()` is right whether or not one has. Ordered, so the same
        edges export as the same file.
        """
        sql = f"""
        SELECT parent, child, sum(repositories) AS repositories
        FROM {EDGES.name}
        GROUP BY parent, child
        ORDER BY parent ASC, child ASC
        """
        for row in self.stream_rows(sql, settings=settings):
            yield (
                str(row['parent']), str(row['child']),
                int(row['repositories']),
            )

    @staticmethod
    def _filters(
        language: str | None,
        direct_only: bool,
        ecosystem: str | None = None,
    ) -> tuple[str, str, Parameters]:
        """Optional predicates, as (repo_clause, artifact_clause, params).

        `language` is an attribute of the repository: GitHub's language,
        matched case-insensitively. `ecosystem` is one of the artifact:
        its canonical ecosystem (`core/ecosystems.py`), which is what
        selects a package's registry now that a repository can have
        several (#55 §4.12).
        """
        params: Parameters = {}
        repo_clause = ''
        artifact_clause = ''
        if language:
            repo_clause = 'WHERE lower(github_language) = {language:String}'
            params['language'] = language.lower()
        if direct_only:
            artifact_clause = 'AND a.relationship = {relationship:String}'
            params['relationship'] = DIRECT
        if ecosystem:
            artifact_clause += (
                f' AND {_ARTIFACT_ECOSYSTEM} = {{ecosystem:String}}'
            )
            params['ecosystem'] = ecosystem.lower()
        return repo_clause, artifact_clause, params

    # -- statistics ---------------------------------------------------------

    def get_stats(self) -> DatabaseStats:
        """High-level row counts per table."""
        sql = f"""
        SELECT
            (SELECT count() FROM {REPOSITORIES.name} FINAL) AS repositories,
            (SELECT count() FROM {ARTIFACTS.name}) AS artifacts,
            (SELECT count() FROM {RELEASES.name} FINAL) AS releases
        """
        return DatabaseStats.from_row(self._rows(sql)[0])

    def get_language_stats(self) -> list[LanguageCount]:
        """Repositories of the corpus per GitHub language, folded to the
        top twelve, `other` and `none` (D7 on #55)."""
        sql = f"""
        SELECT {language_bucket_sql()} AS language,
               count() AS repository_count
        FROM corpus
        GROUP BY language
        ORDER BY repository_count DESC, language ASC
        """
        return row_mapper(LanguageCount)(self._rows(sql))

    def get_ecosystem_stats(self) -> list[EcosystemCoverage]:
        """Per ecosystem: repositories of the corpus that have it, and
        how many of those each source covers.

        Read from the tables rather than `mv_ecosystem_coverage`, so
        `scripts/verify_rollups.py` can hold the rollup to it.
        """
        sql = f"""
        SELECT
            e AS ecosystem,
            count() AS repository_count,
            countIf(has(s.syft, e)) AS syft_count,
            countIf(has(s.depgraph, e)) AS depgraph_count,
            countIf(has(s.manifest, e)) AS manifest_count
        FROM (SELECT id, ecosystems FROM corpus) AS r
        ARRAY JOIN r.ecosystems AS e
        LEFT JOIN (
            SELECT
                a.repository_id AS repository_id,
                groupUniqArrayIf({_ARTIFACT_ECOSYSTEM}, a.source = '{SYFT}')
                    AS syft,
                groupUniqArrayIf({_ARTIFACT_ECOSYSTEM},
                                 a.source = '{DEPGRAPH}') AS depgraph,
                groupUniqArrayIf({_ARTIFACT_ECOSYSTEM},
                                 a.source = '{MANIFEST}') AS manifest
            FROM {_CURRENT_ARTIFACTS} AS a
            GROUP BY a.repository_id
        ) AS s ON s.repository_id = r.id
        GROUP BY e
        ORDER BY repository_count DESC, ecosystem ASC
        """
        return row_mapper(EcosystemCoverage)(self._rows(sql))

    def get_corpus_coverage(self) -> CorpusCoverage:
        """The corpus and how much of it each source covers.

        The denominator is every repository of the current search
        snapshot, whether or not anything was collected for it.
        """
        sql = f"""
        SELECT
            any(r.snapshot) AS snapshot,
            count() AS tracked,
            countIf(s.repository_id != 0) AS with_dependencies,
            countIf(s.syft > 0) AS with_syft,
            countIf(s.depgraph > 0) AS with_depgraph,
            countIf(s.manifest > 0) AS with_manifest
        FROM (SELECT id, snapshot FROM corpus) AS r
        LEFT JOIN (
            SELECT repository_id,
                   countIf(source = '{SYFT}') AS syft,
                   countIf(source = '{DEPGRAPH}') AS depgraph,
                   countIf(source = '{MANIFEST}') AS manifest
            FROM {_CURRENT_ARTIFACTS}
            GROUP BY repository_id
        ) AS s ON s.repository_id = r.id
        """
        return CorpusCoverage.from_row(self._rows(sql)[0])

    def get_top_packages(
        self,
        limit: int = 20,
        language: str | None = None,
        ecosystem: str | None = None,
    ) -> list[PackagePopularity]:
        """Most depended-upon packages, split by direct vs transitive.

        The split matters: without it the ranking is dominated by npm
        micro-packages that no project ever asks for by name.
        """
        repo_clause, eco_clause, params = self._filters(
            language, direct_only=False, ecosystem=ecosystem,
        )
        params['limit'] = limit
        sql = f"""
        SELECT
            a.name AS name,
            count(DISTINCT a.repository_id) AS repository_count,
            count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
                AS direct_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {ON_CURRENT_SCAN}
        WHERE 1 {eco_clause}
        GROUP BY a.name
        ORDER BY repository_count DESC, name ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(PackagePopularity)(self._rows(sql, params))

    def get_dependency_type_distribution(self) -> Iterator[tuple[str, int]]:
        sql = f"""
        SELECT type, count(DISTINCT repository_id) AS repository_count
        FROM {_CURRENT_ARTIFACTS}
        GROUP BY type
        ORDER BY repository_count DESC
        """
        for row in self._rows(sql):
            yield (str(row['type']), int(row['repository_count']))

    # -- history ------------------------------------------------------------

    def get_version_history(
        self,
        library_name: str,
        since: datetime | None = None,
        limit: int = 500,
    ) -> list[VersionObservation]:
        """Every version of a package, with when it was observed.

        Reads the whole append-only table rather than the current scan:
        this is the question a snapshot cannot answer.
        """
        params: Parameters = {'library': library_name, 'limit': limit}
        window = ''
        if since is not None:
            window = 'AND a.observed_at >= {since:DateTime}'
            params['since'] = since

        sql = f"""
        SELECT
            a.version AS version,
            count(DISTINCT a.repository_id) AS repository_count,
            min(a.observed_at) AS observed_at
        FROM {ARTIFACTS.name} AS a
        WHERE a.name = {{library:String}} AND a.version != '' {window}
        GROUP BY a.version
        ORDER BY observed_at ASC, version ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(VersionObservation)(self._rows(sql, params))

    def get_adoption_over_time(
        self,
        library_name: str,
        since: datetime | None = None,
    ) -> list[AdoptionPoint]:
        """Monthly repository counts for a package.

        The partition key is `toYYYYMM(observed_at)`, so a bounded window
        prunes whole partitions rather than scanning.
        """
        params: Parameters = {'library': library_name}
        window = ''
        if since is not None:
            window = 'AND a.observed_at >= {since:DateTime}'
            params['since'] = since

        sql = f"""
        SELECT
            formatDateTime(a.observed_at, '%Y-%m') AS month,
            count(DISTINCT a.repository_id) AS repository_count,
            count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
                AS direct_count
        FROM {ARTIFACTS.name} AS a
        WHERE a.name = {{library:String}} {window}
        GROUP BY month
        ORDER BY month ASC
        """
        return row_mapper(AdoptionPoint)(self._rows(sql, params))

    # -- library lookup -----------------------------------------------------

    def search_library_candidates(
        self,
        pattern: str,
        language: str | None = None,
        limit: int = 20,
        ecosystem: str | None = None,
    ) -> list[LibraryCandidate]:
        """Package names matching `pattern`, ranked by how many repos use them."""
        repo_clause, eco_clause, params = self._filters(
            language, direct_only=False, ecosystem=ecosystem,
        )
        params.update({'pattern': f"%{pattern}%", 'limit': limit})
        sql = f"""
        SELECT a.name AS name, count(DISTINCT a.repository_id) AS repository_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {ON_CURRENT_SCAN}
        WHERE a.name ILIKE {{pattern:String}} {eco_clause}
        GROUP BY a.name
        ORDER BY repository_count DESC, name ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(LibraryCandidate)(self._rows(sql, params))

    def get_dependent_count(
        self,
        library_name: str,
        language: str | None = None,
        direct_only: bool = False,
        ecosystem: str | None = None,
    ) -> int:
        """How many repositories depend on `library_name`."""
        repo_clause, artifact_clause, params = self._filters(
            language, direct_only, ecosystem,
        )
        params['library'] = library_name
        sql = f"""
        SELECT count(DISTINCT a.repository_id) AS repository_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {ON_CURRENT_SCAN}
        WHERE a.name = {{library:String}} {artifact_clause}
        """
        return int(self._rows(sql, params)[0]['repository_count'])

    def get_dependents(
        self,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
        direct_only: bool = False,
        ecosystem: str | None = None,
    ) -> list[Dependent]:
        """Repositories depending on `library_name`, most starred first.

        `LIMIT 1 BY r.id` keeps one row per repository when a package is
        catalogued more than once in the same scan.
        """
        repo_clause, artifact_clause, params = self._filters(
            language, direct_only, ecosystem,
        )
        params.update({'library': library_name, 'limit': limit})
        sql = f"""
        SELECT
            r.owner AS owner,
            r.repo AS repo,
            r.stars AS stars,
            a.version AS version,
            r.url AS url,
            a.relationship AS relationship
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {ON_CURRENT_SCAN}
        WHERE a.name = {{library:String}} {artifact_clause}
        ORDER BY r.stars DESC, r.owner ASC, r.repo ASC
        LIMIT 1 BY r.id
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(Dependent)(self._rows(sql, params))

    # -- framework lookup ---------------------------------------------------

    def get_framework_usage(
        self,
        ecosystem: str,
        packages: list[str],
        direct_only: bool = False,
    ) -> int:
        """Repositories of the corpus with one of a framework's packages,
        in the framework's ecosystem.

        Keyed by the package's ecosystem, not the repository's language:
        a TypeScript-labelled repository with a Spring Boot backend uses
        Spring Boot, and was left out when this filtered on
        `lower(language) = 'java'` (#51).
        """
        if not packages:
            return 0
        _, artifact_clause, params = self._filters(
            None, direct_only, ecosystem,
        )
        params['pkgs'] = packages
        sql = f"""
        SELECT count(DISTINCT a.repository_id) AS repository_count
        FROM {_CURRENT_ARTIFACTS} AS a
        WHERE a.name IN {{pkgs:Array(String)}} {artifact_clause}
        """
        return int(self._rows(sql, params)[0]['repository_count'])

    def get_top_projects_by_framework(
        self,
        ecosystem: str,
        packages: list[str],
        limit: int = 3,
    ) -> list[Dependent]:
        if not packages:
            return []
        _, artifact_clause, params = self._filters(None, False, ecosystem)
        params.update({'pkgs': packages, 'limit': limit})
        sql = f"""
        SELECT
            r.owner AS owner,
            r.repo AS repo,
            r.stars AS stars,
            a.version AS version,
            r.url AS url,
            a.relationship AS relationship
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS}) AS r ON {ON_CURRENT_SCAN}
        WHERE a.name IN {{pkgs:Array(String)}} {artifact_clause}
        ORDER BY r.stars DESC, r.owner ASC, r.repo ASC
        LIMIT 1 BY r.id
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(Dependent)(self._rows(sql, params))

    def get_repository_frameworks(
        self,
        repository_id: int,
        framework_map: dict[str, list[str]],
    ) -> list[tuple[str, str]]:
        """Frameworks used by one repository, as (framework, version)."""
        return self.get_frameworks_for_repositories(
            [repository_id], framework_map,
        ).get(repository_id, [])

    def get_frameworks_for_repositories(
        self,
        repository_ids: list[int],
        framework_map: dict[str, list[str]],
    ) -> dict[int, list[tuple[str, str]]]:
        """Batched form of `get_repository_frameworks`.

        Classifying thousands of repositories one query at a time was the
        N+1 in `github classify`.

        The current scan only. `classify` takes the first version it is
        given for the framework it picks, so reading every scan could
        report a version the repository moved off long ago.
        """
        package_to_framework = {
            package: framework
            for framework, packages in framework_map.items()
            for package in packages
            if package
        }
        if not repository_ids or not package_to_framework:
            return {}

        sql = f"""
        SELECT repository_id, name, version
        FROM {_CURRENT_ARTIFACTS}
        WHERE repository_id IN {{repo_ids:Array(UInt64)}}
          AND name IN {{pkgs:Array(String)}}
        """
        params: Parameters = {
            'repo_ids': repository_ids,
            'pkgs': list(package_to_framework),
        }

        found: dict[int, list[tuple[str, str]]] = {}
        for row in self._rows(sql, params):
            framework = package_to_framework.get(str(row['name']))
            if not framework:
                continue
            repo_id = int(row['repository_id'])
            version = '' if row['version'] is None else str(row['version'])
            found.setdefault(repo_id, []).append((framework, version))

        for entries in found.values():
            entries.sort()
        return found
