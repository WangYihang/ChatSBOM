"""A changed definition reaches a database that already has the object.

Views, the repository dictionary and the rollups are created with
`IF NOT EXISTS`, which cannot notice that a definition changed. #21
rewrote every current-state rollup, added the `current_artifacts` and
`facts` views and gave `dict_repositories` the `sbom_commit_sha` and
`depgraph_observed_at` attributes, and none of it reached a database
created before: the dashboard's dependants query failed there with "No
such attribute 'depgraph_observed_at'", and the overview went on
counting every scan ever taken.

The fixture here is that database, built from `main` (de60629), and the
tests ask it what the dashboard and the CLI would, after the next
`ensure_schema` and the next `db index`.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.dictionaries import DICTIONARIES
from chatsbom.core.ecosystems import canonical_sql
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.rollups import REFRESH_SETTINGS
from chatsbom.core.rollups import ROLLUPS
from chatsbom.core.schema import VIEW_DDL
from tests.conftest import requires_clickhouse
from tests.current_state_test import CURRENT_STATE
from tests.current_state_test import dependants
from tests.current_state_test import HISTORY
from tests.current_state_test import rows_of
from tests.current_state_test import seed_two_scans

# --- what `main` declared ----------------------------------------------------
#
# Copied from `git show de60629:chatsbom/core/rollups.py` and
# `.../dictionaries.py`, the SQL only. No views: `current_artifacts` and
# `facts` arrived with #21.

BEFORE_21_DICTIONARY = """
CREATE DICTIONARY IF NOT EXISTS dict_repositories (
    id UInt64,
    owner String,
    repo String,
    url String,
    stars UInt64,
    language String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT id, owner, repo, url, stars, language
           FROM {database}.repositories FINAL'
    USER '{user}' PASSWORD '{password}'))
LIFETIME(MIN 300 MAX 600)
LAYOUT(HASHED())
"""

#: Every scan, deduplicated on the fact key but never on the scan.
_EVERY_FACT = """(
    SELECT DISTINCT repository_id, name, version, type, found_by,
                    relationship, source, version_kind
    FROM artifacts
)"""

BEFORE_21_ROLLUPS: tuple[tuple[str, str], ...] = (
    (
        'mv_package_language', f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_language
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, language)
AS SELECT
    a.name AS name,
    lower(r.language) AS language,
    uniqExact(a.repository_id) AS repositories,
    uniqExactIf(a.repository_id, a.relationship = 'direct')
        AS direct_repositories,
    count() AS records,
    countIf(a.relationship = 'direct') AS direct_records,
    countIf(a.relationship = 'transitive') AS transitive_records,
    countIf(a.relationship = 'unknown') AS unknown_records,
    countIf(a.source = 'syft') AS syft_records,
    countIf(a.source = 'github-depgraph') AS depgraph_records
FROM {_EVERY_FACT} a
INNER JOIN repositories r ON r.id = a.repository_id
GROUP BY a.name, lower(r.language)
""",
    ),
    (
        'mv_repository_deps', f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_repository_deps
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY repository_id
AS SELECT
    repository_id,
    uniqExact(name) AS packages,
    uniqExactIf(name, relationship = 'direct') AS direct_packages,
    count() AS records
FROM {_EVERY_FACT}
GROUP BY repository_id
""",
    ),
    (
        'mv_licenses', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_licenses
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY license
AS SELECT
    l AS license,
    uniqExact(repository_id) AS repositories,
    uniqExact(name) AS packages
FROM artifacts
ARRAY JOIN licenses AS l
GROUP BY l
UNION ALL
SELECT
    '' AS license,
    uniqExact(repository_id) AS repositories,
    uniqExact(name) AS packages
FROM artifacts
WHERE empty(licenses)
""",
    ),
    (
        'mv_language_totals', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_language_totals
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY language
AS SELECT
    language,
    sum(direct_records) AS direct_records,
    sum(transitive_records) AS transitive_records,
    sum(unknown_records) AS unknown_records,
    sum(syft_records) AS syft_records,
    sum(depgraph_records) AS depgraph_records,
    sum(records) AS records
FROM mv_package_language
GROUP BY language
""",
    ),
    (
        'mv_packages', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY name
AS SELECT
    name,
    sum(repositories) AS repositories,
    sum(direct_repositories) AS direct_repositories
FROM mv_package_language
GROUP BY name
""",
    ),
    (
        'mv_edges_forward', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edges_forward
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (parent, child)
AS SELECT
    parent,
    child,
    sum(repositories) AS repositories
FROM edges
GROUP BY parent, child
""",
    ),
    (
        'mv_package_month', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_month
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, source, month)
AS SELECT
    name,
    source,
    formatDateTime(observed_at, '%Y-%m') AS month,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct')
        AS direct_repositories
FROM artifacts
GROUP BY name, source, month
""",
    ),
    (
        'mv_package_type', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_type
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, type)
AS SELECT
    name,
    type,
    uniqExact(repository_id) AS repositories,
    uniqExactIf(repository_id, relationship = 'direct')
        AS direct_repositories
FROM artifacts
GROUP BY name, type
""",
    ),
    (
        'mv_package_version', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_package_version
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (name, version_kind, version)
AS SELECT
    name,
    version_kind,
    version,
    uniqExact(repository_id) AS repositories
FROM artifacts
GROUP BY name, version_kind, version
""",
    ),
    (
        'mv_dependency_buckets', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dependency_buckets
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY position
AS SELECT
    multiIf(packages < 10, 0, packages < 25, 1, packages < 100, 2,
            packages < 250, 3, packages < 1000, 4, 5) AS position,
    multiIf(packages < 10, '1-9', packages < 25, '10-24',
            packages < 100, '25-99', packages < 250, '100-249',
            packages < 1000, '250-999', '1000+') AS bucket,
    count() AS repositories
FROM mv_repository_deps
GROUP BY position, bucket
""",
    ),
    (
        'mv_version_kinds', f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_version_kinds
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY version_kind
AS SELECT
    version_kind,
    count() AS records
FROM {_EVERY_FACT}
GROUP BY version_kind
""",
    ),
    (
        'mv_language_coverage', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_language_coverage
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY language
AS SELECT
    lower(r.language) AS language,
    count() AS repositories,
    countIf(d.repository_id != 0) AS with_sbom
FROM repositories AS r
LEFT JOIN mv_repository_deps AS d ON d.repository_id = r.id
GROUP BY language
""",
    ),
    (
        'mv_totals', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_totals
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS SELECT
    (SELECT count() FROM mv_repository_deps) AS repositories,
    (SELECT sum(records) FROM mv_repository_deps) AS dependencies,
    (SELECT count() FROM mv_packages) AS packages,
    (SELECT sum(direct_records + transitive_records)
     FROM mv_language_totals) AS classified
""",
    ),
    (
        'mv_top_packages', """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY (language, direct_only, rank)
AS
WITH by_name AS (
    SELECT '' AS language, name, repositories, direct_repositories
    FROM mv_packages
    UNION ALL
    SELECT language, name, repositories, direct_repositories
    FROM mv_package_language
)
SELECT language, direct_only, name, repositories, direct_repositories, rank
FROM (
    SELECT language, 0 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY language
                              ORDER BY repositories DESC, name) AS rank
    FROM by_name
    UNION ALL
    SELECT language, 1 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY language
                              ORDER BY direct_repositories DESC, name) AS rank
    FROM by_name
)
WHERE rank <= 100
""",
    ),
    (
        'mv_edge_ambiguity', f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_edge_ambiguity
REFRESH EVERY 1 DAY
ENGINE = TinyLog
AS WITH ambiguous AS (
    SELECT name FROM mv_package_type GROUP BY name
    HAVING uniqExact({canonical_sql('type')}) > 1
)
SELECT
    (SELECT count() FROM mv_packages) AS names,
    (SELECT count() FROM ambiguous) AS ambiguous_names,
    (SELECT count() FROM mv_edges_forward) AS edges,
    (SELECT count() FROM mv_edges_forward
     WHERE child IN (SELECT name FROM ambiguous)
        OR parent IN (SELECT name FROM ambiguous)) AS ambiguous_edges,
    (SELECT max(packages) FROM mv_repository_deps) AS largest_repository
""",
    ),
)

#: Every view, dictionary and rollup `ensure_schema` declares.
DERIVED = tuple(
    name for name, _ in (*VIEW_DDL, *DICTIONARIES, *ROLLUPS)
)


def install_before_21(ingest: IngestionRepository) -> None:
    """Swap the derived objects for the ones `main` declared.

    The tables stay as they are: a column the schema added since
    arrives in place (`migration_test.py`), and it is the derived
    objects that `IF NOT EXISTS` left behind.
    """
    client = ingest.client
    for name, _ in reversed(ROLLUPS):
        client.command(f'DROP VIEW {name}')
    client.command('DROP DICTIONARY dict_repositories')
    for name, _ in reversed(VIEW_DDL):
        client.command(f'DROP VIEW {name}')
    client.command(
        BEFORE_21_DICTIONARY.format(
            database=ingest.config.database,
            user=ingest.config.user,
            password=ingest.config.password,
        ),
    )
    for _, ddl in BEFORE_21_ROLLUPS:
        client.command(ddl, settings=REFRESH_SETTINGS)


def refresh_what_is_there(ingest: IngestionRepository) -> None:
    """Refresh the old rollups by name, as the daily timer would."""
    for name, _ in BEFORE_21_ROLLUPS:
        ingest.client.command(
            f'SYSTEM REFRESH VIEW {name}', settings=REFRESH_SETTINGS,
        )
        ingest.client.command(
            f'SYSTEM WAIT VIEW {name}', settings=REFRESH_SETTINGS,
        )


@pytest.fixture
def before_21(
    ingest: IngestionRepository,
    query: QueryRepository,
) -> QueryRepository:
    """A deployment of `main`, holding a repository scanned twice.

    Its rollups have been refreshed, so the overview counts both scans,
    as it does on that deployment today.
    """
    install_before_21(ingest)
    seed_two_scans(ingest)
    refresh_what_is_there(ingest)
    return query


def uuids(client: Any, database: str) -> dict[str, str]:
    """Every derived object, by the UUID it was created with.

    A replaced object has a new one, whatever it is called.
    """
    return {
        str(name): str(uuid)
        for name, uuid in client.query(
            'SELECT name, uuid FROM system.tables '
            'WHERE database = {db:String} AND name IN {names:Array(String)}',
            parameters={'db': database, 'names': list(DERIVED)},
        ).result_rows
    }


class Statements:
    """What a repository sends, statement by statement."""

    def __init__(
        self,
        repository: IngestionRepository,
        monkeypatch: pytest.MonkeyPatch,
        after: Callable[[str], None] | None = None,
    ) -> None:
        self.sent: list[str] = []
        send = repository.client.command

        def command(sql: str, *args: Any, **kwargs: Any) -> Any:
            result = send(sql, *args, **kwargs)
            self.sent.append(' '.join(str(sql).split()))
            if after is not None:
                after(str(sql))
            return result

        monkeypatch.setattr(repository.client, 'command', command)

    def touching(self, names: tuple[str, ...] = DERIVED) -> list[str]:
        """The statements that name a derived object.

        As a word: `artifacts` contains `facts`.
        """
        return [
            sql for sql in self.sent
            if any(re.search(rf'\b{name}\b', sql) for name in names)
        ]


@requires_clickhouse
class TestTheDeploymentBefore21:
    """The scenario PR #54 deploys onto."""

    def test_the_dashboard_cannot_ask_it_yet(self, before_21):
        """The fixture is what it claims to be: the new dashboard's
        dependants query fails on the old dictionary, which has neither
        attribute it asks for."""
        with pytest.raises(Exception, match='No such attribute'):
            dependants(before_21, 'mail')

    def test_ensure_schema_brings_every_reader_up_to_date(
        self, ingest, before_21,
    ):
        ingest.ensure_schema()

        assert dependants(before_21, 'mail') == [(1, '2.9.1')]
        assert dependants(before_21, 'rails') == [(1, '~> 7.1')]
        assert dependants(before_21, 'left-pad') == []
        assert {
            name: rows_of(before_21, sql)
            for name, (sql, _) in CURRENT_STATE.items()
        } == {
            name: sorted(rows) for name, (_, rows) in CURRENT_STATE.items()
        }
        for name, (sql, rows) in HISTORY.items():
            assert rows_of(before_21, sql) == sorted(rows), name

    def test_the_views_arrive(self, ingest, before_21):
        ingest.ensure_schema()
        assert rows_of(
            before_21,
            'SELECT name, version FROM current_artifacts '
            'WHERE repository_id = 1',
        ) == [('mail', '2.9.1'), ('rails', '~> 7.1'), ('rails', '~> 7.1')]
        assert rows_of(
            before_21, 'SELECT count() FROM facts WHERE repository_id = 1',
        ) == [(2,)]

    def test_a_second_run_recreates_nothing(
        self, ingest, before_21, clickhouse_db, monkeypatch,
    ):
        ingest.ensure_schema()
        created = uuids(ingest.client, clickhouse_db)
        statements = Statements(ingest, monkeypatch)

        ingest.ensure_schema()

        assert uuids(ingest.client, clickhouse_db) == created
        assert statements.touching() == []

    def test_the_next_db_index_applies_them(
        self, ingest, before_21, db_command,
    ):
        """The acceptance criterion: no `--rebuild`, no data to index,
        and the definitions arrive anyway."""
        # A deployment merges in the background; the seed stopped that
        # to hold two rows of one repository apart.
        ingest.client.command('SYSTEM START MERGES repositories')

        db_command('index')

        assert dependants(before_21, 'mail') == [(1, '2.9.1')]
        assert {
            name: rows_of(before_21, sql)
            for name, (sql, _) in CURRENT_STATE.items()
        } == {
            name: sorted(rows) for name, (_, rows) in CURRENT_STATE.items()
        }


# --- a definition changed on a database that has the object --------------

def _replaced(name: str, ddl: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (rollup, ddl if rollup == name else declared)
        for rollup, declared in ROLLUPS
    )


#: `mv_packages`, declared without `rails`: a definition that changes
#: what the rollups derived from it must say.
PACKAGES_WITHOUT_RAILS = """
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_packages
REFRESH EVERY 1 DAY
ENGINE = MergeTree ORDER BY name
AS SELECT
    name,
    sum(repositories) AS repositories,
    sum(direct_repositories) AS direct_repositories
FROM mv_package_language
WHERE name != 'rails'
GROUP BY name
""".strip()


@pytest.fixture
def refreshed(ingest, query):
    """Today's definitions, holding the two-scan fixture."""
    seed_two_scans(ingest)
    ingest.refresh_rollups()
    return query


@requires_clickhouse
class TestAChangedRollup:

    def test_it_is_replaced_and_what_reads_it_follows(
        self, ingest, refreshed, monkeypatch,
    ):
        """`mv_totals`, `mv_top_packages` and `mv_edge_ambiguity` read
        `mv_packages`. Replaced alone, they would go on describing the
        old one until the next refresh: a day, if nothing runs."""
        monkeypatch.setattr(
            'chatsbom.core.repository.ROLLUPS',
            _replaced('mv_packages', PACKAGES_WITHOUT_RAILS),
        )

        ingest.ensure_schema()

        assert rows_of(refreshed, 'SELECT name FROM mv_packages') == [
            ('mail',), ('rack',),
        ]
        assert rows_of(refreshed, 'SELECT packages FROM mv_totals') == [(2,)]
        assert rows_of(
            refreshed, 'SELECT names FROM mv_edge_ambiguity',
        ) == [(2,)]
        assert ('rails',) not in rows_of(
            refreshed, "SELECT name FROM mv_top_packages WHERE language = ''",
        )

    def test_the_rest_are_left_alone(
        self, ingest, refreshed, clickhouse_db, monkeypatch,
    ):
        """Replacing a rollup reruns its query; one that neither changed
        nor reads a changed one has no reason to."""
        before = uuids(ingest.client, clickhouse_db)
        monkeypatch.setattr(
            'chatsbom.core.repository.ROLLUPS',
            _replaced('mv_packages', PACKAGES_WITHOUT_RAILS),
        )
        statements = Statements(ingest, monkeypatch)

        ingest.ensure_schema()

        after = uuids(ingest.client, clickhouse_db)
        assert {name for name in DERIVED if after[name] != before[name]} == {
            'mv_packages',
        }
        refreshed_views = {
            sql.split()[-1] for sql in statements.sent
            if sql.startswith('SYSTEM REFRESH VIEW')
        }
        assert refreshed_views == {
            'mv_packages_next', 'mv_totals', 'mv_top_packages',
            'mv_edge_ambiguity',
        }

    def test_a_new_rollup_is_whole_before_what_reads_it_is_refreshed(
        self, ingest, refreshed,
    ):
        """A rollup this pass creates starts its first refresh on its
        own, and nothing waits for it unless something is computed from
        it: here, the three that read `mv_packages`, which exist and are
        refreshed because it changed."""
        ingest.client.command('DROP VIEW mv_packages')

        ingest.ensure_schema()

        assert rows_of(refreshed, 'SELECT packages FROM mv_totals') == [(3,)]
        assert rows_of(
            refreshed, 'SELECT names FROM mv_edge_ambiguity',
        ) == [(3,)]

    def test_a_definition_that_fails_leaves_the_old_one_serving(
        self, ingest, refreshed, clickhouse_db, monkeypatch,
    ):
        """A replacement is built and refreshed before it is swapped in,
        so one whose query fails is dropped rather than installed."""
        before = uuids(ingest.client, clickhouse_db)
        monkeypatch.setattr(
            'chatsbom.core.repository.ROLLUPS',
            _replaced(
                'mv_packages',
                PACKAGES_WITHOUT_RAILS.replace(
                    "WHERE name != 'rails'",
                    "WHERE throwIf(name = 'rails', 'refused') = 0",
                ),
            ),
        )

        ingest.ensure_schema()

        assert uuids(ingest.client, clickhouse_db)['mv_packages'] == (
            before['mv_packages']
        )
        assert rows_of(refreshed, 'SELECT name FROM mv_packages') == [
            ('mail',), ('rack',), ('rails',),
        ]
        assert rows_of(
            refreshed,
            'SELECT name FROM system.tables '
            "WHERE database = currentDatabase() AND name LIKE '%\\_next'",
        ) == []


@requires_clickhouse
class TestAChangedView:

    def test_a_column_added_to_artifacts_is_readable_through_the_view(
        self, ingest, query, monkeypatch,
    ):
        """ClickHouse stores a view's columns when the view is created,
        so `current_artifacts`' `SELECT a.*` froze the columns
        `artifacts` had then. A column added to the table later was
        unknown through the view (measured on 25.12: UNKNOWN_IDENTIFIER)
        until the view was declared again."""
        from chatsbom.core.schema import ARTIFACTS_DDL
        from chatsbom.core.schema import TABLE_DDL

        widened = ARTIFACTS_DDL.replace(
            "    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time'",
            "    updated_at DateTime DEFAULT now() COMMENT 'Last Updated Time',\n"
            "    scanner LowCardinality(String) DEFAULT 'syft' COMMENT 'x'",
        )
        assert widened != ARTIFACTS_DDL
        monkeypatch.setattr(
            'chatsbom.core.repository.TABLE_DDL',
            tuple(
                (name, widened if name == 'artifacts' else ddl)
                for name, ddl in TABLE_DDL
            ),
        )

        ingest.ensure_schema()

        assert rows_of(
            query, 'SELECT count(scanner) FROM current_artifacts',
        ) == [(0,)]


@requires_clickhouse
class TestNoWindow:
    """What the dashboard sees while a definition is replaced.

    ClickHouse applies each statement atomically, so a window a reader
    can fall into opens *between* statements: after a DROP and before
    the CREATE, or after a CREATE and before the refresh that fills it.
    Asking after every statement finds each such window every time,
    where a thread polling in a loop finds one only when it happens to
    be scheduled into it.
    """

    @staticmethod
    def ask_after_every_statement(
        writer: IngestionRepository,
        reader: QueryRepository,
        question: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> list[Any]:
        answers: list[Any] = []

        def ask(_: str) -> None:
            try:
                answers.append(reader.client.query(question).result_rows)
            except Exception as error:  # noqa: BLE001 - the finding
                answers.append(str(error).split('\n')[0][:120])

        Statements(writer, monkeypatch, after=ask)
        return answers

    def test_a_rollup_is_never_missing_or_empty(
        self, ingest, refreshed, monkeypatch,
    ):
        answers = self.ask_after_every_statement(
            ingest, refreshed, 'SELECT count() FROM mv_packages', monkeypatch,
        )

        ingest.refresh_rollups(recreate=True)

        assert answers
        assert {str(answer) for answer in answers} == {'[(3,)]'}

    def test_the_dictionary_is_never_missing(
        self, ingest, refreshed, monkeypatch,
    ):
        answers = self.ask_after_every_statement(
            ingest, refreshed,
            "SELECT dictGet('dict_repositories', 'owner', toUInt64(1))",
            monkeypatch,
        )

        ingest._ensure_dictionaries(recreate=True)

        assert answers
        assert {str(answer) for answer in answers} == {"[('mastodon',)]"}

    def test_a_view_is_never_missing(self, ingest, refreshed, monkeypatch):
        """Replaced because its definition changed: here, to one that
        selects the same rows."""
        views = dict(VIEW_DDL)
        views['current_artifacts'] += '\nWHERE 1 = 1'
        monkeypatch.setattr(
            'chatsbom.core.repository.VIEW_DDL', tuple(views.items()),
        )
        answers = self.ask_after_every_statement(
            ingest, refreshed,
            'SELECT count() FROM current_artifacts', monkeypatch,
        )

        ingest.ensure_schema()

        assert answers
        assert {str(answer) for answer in answers} == {'[(4,)]'}
        [(query,)] = rows_of(
            refreshed,
            'SELECT create_table_query FROM system.tables '
            'WHERE database = currentDatabase() '
            "AND name = 'current_artifacts'",
        )
        assert 'WHERE 1 = 1' in query


@requires_clickhouse
class TestCredentials:
    """The dictionary's SOURCE carries the admin user and password.

    They are configuration, not definition: they stay out of the
    fingerprint, so changing the password does not by itself declare
    the dictionary again. The one case that has to is a stored copy the
    server now refuses, and that shows up as a failed reload.
    """

    @staticmethod
    def declared_with(ingest: IngestionRepository, password: str) -> None:
        """The dictionary as a process configured with `password`
        would have declared it: the same definition, fingerprinted the
        same way."""
        from chatsbom.core.definitions import fingerprint
        from chatsbom.core.definitions import replacing
        from chatsbom.core.definitions import stamped

        [(_, ddl)] = DICTIONARIES
        database = ingest.config.database
        ingest.client.command(
            replacing(
                stamped(
                    ddl.format(
                        database=database, user=ingest.config.user,
                        password=password,
                    ),
                    fingerprint(ddl.replace('{database}', database)),
                ),
            ),
        )

    def test_a_password_change_alone_declares_nothing(
        self, ingest, query, clickhouse_db, monkeypatch,
    ):
        self.declared_with(ingest, 'rotated-away')
        before = uuids(ingest.client, clickhouse_db)
        statements = Statements(ingest, monkeypatch)

        ingest.ensure_schema()

        assert uuids(ingest.client, clickhouse_db) == before
        assert statements.touching(('dict_repositories',)) == []

    def test_a_refused_copy_is_declared_again_when_it_reloads(
        self, ingest, query,
    ):
        seed_two_scans(ingest)
        self.declared_with(ingest, 'rotated-away')
        with pytest.raises(Exception, match='Authentication failed'):
            rows_of(
                query,
                "SELECT dictGet('dict_repositories', 'owner', toUInt64(1))",
            )

        ingest.reload_dictionaries()

        assert rows_of(
            query,
            "SELECT dictGet('dict_repositories', 'owner', toUInt64(1))",
        ) == [('mastodon',)]


class _Recorder:
    """A client that answers "nothing declared yet" and keeps what it is
    sent, so a statement can be read without a database."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def query(self, *_: Any, **__: Any) -> Any:
        return SimpleNamespace(result_rows=[])

    def command(self, sql: str, *_: Any, **__: Any) -> None:
        self.sent.append(sql)


def _declaring(password: str) -> list[str]:
    """What `ensure_schema` would send for the dictionary, configured
    with `password`."""
    recorder = _Recorder()
    repository = IngestionRepository.__new__(IngestionRepository)
    repository.config = DatabaseConfig(
        host='h', port=1, user='admin', password=password, database='db',
    )
    repository._client = recorder
    repository._ensure_dictionaries()
    return [sql for sql in recorder.sent if sql.startswith('CREATE')]


class TestTheFingerprint:

    def test_every_declaration_names_what_it_creates(self) -> None:
        """The replacements are the declarations rewritten — `OR
        REPLACE`, or another name to build aside under — so each has to
        open the way the rewrite expects."""
        from chatsbom.core.definitions import declared_name
        for name, ddl in (*VIEW_DDL, *DICTIONARIES, *ROLLUPS):
            assert declared_name(ddl) == name

    def test_a_note_in_the_sql_changes_nothing(self) -> None:
        """Most of these statements are `--` lines explaining them.
        Rewording one must not rebuild a rollup."""
        from chatsbom.core.definitions import fingerprint
        ddl = dict(ROLLUPS)['mv_packages']
        noted = ddl.replace(
            '\nGROUP BY', '\n-- a sentence about the grouping\nGROUP BY',
        )
        assert noted != ddl
        assert fingerprint(noted) == fingerprint(ddl)

    def test_a_change_to_the_sql_does(self) -> None:
        from chatsbom.core.definitions import fingerprint
        assert fingerprint(PACKAGES_WITHOUT_RAILS) != fingerprint(
            dict(ROLLUPS)['mv_packages'],
        )

    def test_the_password_is_sent_and_not_fingerprinted(self) -> None:
        """Two processes with different passwords declare the same
        dictionary, each with its own password, under one fingerprint."""
        [first] = _declaring('first-password')
        [second] = _declaring('second-password')
        assert "PASSWORD 'first-password'" in first
        assert "PASSWORD 'second-password'" in second

        def comment(sql: str) -> str:
            return re.findall(r"COMMENT '([^']*)'", sql)[-1]

        assert comment(first) == comment(second)
        assert 'password' not in comment(first)


@requires_clickhouse
class TestAFreshDatabase:

    def test_every_object_carries_its_fingerprint(self, ingest, query):
        """Created from the same statements a replacement uses, so the
        COMMENT has to parse on every one of them: after a bare `FROM x`
        ClickHouse reads `COMMENT` as an alias of `x`."""
        comments = dict(
            rows_of(
                query,
                'SELECT name, comment FROM system.tables '
                'WHERE database = currentDatabase() '
                f'AND name IN {tuple(DERIVED)!r}',
            ),
        )
        assert set(comments) == set(DERIVED)
        for name, comment in comments.items():
            assert comment.startswith('ddl-sha256:'), name

    def test_a_second_run_declares_nothing(
        self, ingest, clickhouse_db, monkeypatch,
    ):
        before = uuids(ingest.client, clickhouse_db)
        statements = Statements(ingest, monkeypatch)

        ingest.ensure_schema()

        assert uuids(ingest.client, clickhouse_db) == before
        assert statements.touching() == []
