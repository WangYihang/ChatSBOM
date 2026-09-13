"""Append-only observations, so the dataset answers questions about time.

Overwriting a repository's current state destroys information on every
update: "how long did projects take to move off mail 2.7" is unanswerable
if the rows that knew are gone. Storage is not the reason to avoid it —
6.1M rows compress to 17 MB, and a year of weekly deltas is about 220 MB.
"""
from datetime import datetime
from datetime import timezone

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ARTIFACTS_DDL
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import REPOSITORIES
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse

JAN = datetime(2026, 1, 15, tzinfo=timezone.utc).replace(tzinfo=None)
JUN = datetime(2026, 6, 15, tzinfo=timezone.utc).replace(tzinfo=None)
SEP = datetime(2026, 9, 15, tzinfo=timezone.utc).replace(tzinfo=None)

SHA_OLD = 'a' * 40
SHA_NEW = 'b' * 40


def test_artifacts_declares_observed_at():
    assert 'observed_at' in ddl_columns(ARTIFACTS_DDL)
    assert 'observed_at' in ARTIFACTS.columns


def test_artifacts_is_append_only(ingest):
    """MergeTree, not ReplacingMergeTree: an observation is never replaced."""
    engine = ingest.client.query(
        'SELECT engine FROM system.tables '
        'WHERE database = currentDatabase() AND name = %s' % "'artifacts'",
    ).result_rows[0][0]
    assert engine == 'MergeTree', engine


def test_artifacts_is_partitioned_by_month(ingest):
    expression = ingest.client.query(
        'SELECT partition_key FROM system.tables '
        "WHERE database = currentDatabase() AND name = 'artifacts'",
    ).result_rows[0][0]
    assert 'observed_at' in expression


def _seed_two_observations(ingest):
    """The same repository, scanned in January and again in September."""
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=1, owner='mastodon', repo='mastodon', stars=300,
                sbom_commit_sha=SHA_NEW,
                sbom_commit_sha_short=SHA_NEW[:7],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([
            artifact_row(
                repository_id=1, artifact_id='a', name='mail',
                version='2.7.1', sbom_commit_sha=SHA_OLD,
                observed_at=JAN,
            ),
            artifact_row(
                repository_id=1, artifact_id='a', name='mail',
                version='2.9.1', sbom_commit_sha=SHA_NEW,
                observed_at=SEP,
            ),
        ]),
        ARTIFACTS.column_names,
    )


def test_both_observations_survive(ingest, query):
    _seed_two_observations(ingest)
    rows = ingest.client.query(
        'SELECT version, observed_at FROM artifacts ORDER BY observed_at',
    ).result_rows
    assert [r[0] for r in rows] == ['2.7.1', '2.9.1']


def test_current_queries_see_only_the_latest_scan(ingest, query):
    """History must not leak into "who depends on X now"."""
    _seed_two_observations(ingest)
    deps = query.get_dependents('mail')
    assert len(deps) == 1
    assert deps[0].version == '2.9.1'


def test_version_history_is_queryable(ingest, query):
    _seed_two_observations(ingest)
    history = query.get_version_history('mail')
    assert [(h.version, h.repository_count) for h in history] == [
        ('2.7.1', 1), ('2.9.1', 1),
    ]
    assert history[0].observed_at < history[1].observed_at


def test_history_can_be_bounded_by_time(ingest, query):
    _seed_two_observations(ingest)
    recent = query.get_version_history('mail', since=JUN)
    assert [h.version for h in recent] == ['2.9.1']


def test_a_package_never_seen_has_no_history(ingest, query):
    _seed_two_observations(ingest)
    assert query.get_version_history('does-not-exist') == []


def test_adoption_over_time_counts_distinct_repositories(ingest, query):
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=1, sbom_commit_sha=SHA_NEW,
                sbom_commit_sha_short=SHA_NEW[:7],
            ),
            repo_row(
                id=2, owner='o', repo='two', sbom_commit_sha=SHA_NEW,
                sbom_commit_sha_short=SHA_NEW[:7],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([
            artifact_row(
                repository_id=1, artifact_id='a', name='mail',
                sbom_commit_sha=SHA_NEW, observed_at=JAN,
            ),
            artifact_row(
                repository_id=1, artifact_id='a', name='mail',
                sbom_commit_sha=SHA_NEW, observed_at=SEP,
            ),
            artifact_row(
                repository_id=2, artifact_id='a', name='mail',
                sbom_commit_sha=SHA_NEW, observed_at=SEP,
            ),
        ]),
        ARTIFACTS.column_names,
    )
    series = query.get_adoption_over_time('mail')
    by_month = {p.month: p.repository_count for p in series}
    assert by_month['2026-01'] == 1
    assert by_month['2026-09'] == 2


def test_reindexing_the_same_scan_does_not_double_count(ingest, query):
    """Re-running `db index` must not inflate the current-state counts."""
    rows = ARTIFACTS.rows([
        artifact_row(
            repository_id=1, artifact_id='a', name='mail',
            sbom_commit_sha=SHA_NEW, observed_at=SEP,
        ),
    ])
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=1, sbom_commit_sha=SHA_NEW,
                sbom_commit_sha_short=SHA_NEW[:7],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(ARTIFACTS.name, rows, ARTIFACTS.column_names)
    ingest.insert_batch(ARTIFACTS.name, rows, ARTIFACTS.column_names)

    assert query.get_dependent_count('mail') == 1
    assert len(query.get_dependents('mail')) == 1
