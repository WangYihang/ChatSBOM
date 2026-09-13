"""Query-layer semantics, exercised against a real ClickHouse.

These cover the three defects the old layer had: `FINAL` on both sides of
every JOIN, counts that mixed artifact rows with repositories, and no
deduplication of repeat scans or repeat catalogers.
"""
from datetime import datetime

import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.provenance import RESOLVED
from chatsbom.models.provenance import SYFT
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse

pytestmark = requires_clickhouse

EPOCH = datetime(1970, 1, 2)
CURRENT_SHA = 'a' * 40
STALE_SHA = 'b' * 40


def repo_row(**over):
    row = {
        'id': 1, 'owner': 'o', 'repo': 'r', 'url': 'https://x', 'stars': 1,
        'description': '', 'created_at': EPOCH, 'language': 'ruby',
        'topics': [], 'default_branch': 'main',
        'sbom_ref': 'v1', 'sbom_ref_type': 'release',
        'sbom_commit_sha': CURRENT_SHA, 'sbom_commit_sha_short': CURRENT_SHA[:7],
        'has_releases': True, 'latest_release_tag': 'v1',
        'latest_release_published_at': EPOCH, 'total_releases': 1,
        'pushed_at': EPOCH, 'is_archived': False, 'is_fork': False,
        'is_template': False, 'is_mirror': False, 'disk_usage': 0,
        'fork_count': 0, 'watchers_count': 0,
        'license_spdx_id': 'MIT', 'license_name': 'MIT',
        'manifest_sources': ['Gemfile'],
    }
    row.update(over)
    return row


def artifact_row(**over):
    row = {
        'repository_id': 1, 'artifact_id': 'art-1', 'name': 'mail',
        'version': '2.9.0', 'type': 'gem', 'purl': 'pkg:gem/mail@2.9.0',
        'found_by': 'ruby-gemfile-cataloger', 'licenses': ['MIT'],
        'relationship': TRANSITIVE,
        'source': SYFT, 'version_kind': RESOLVED,
        'sbom_ref': 'v1', 'sbom_commit_sha': CURRENT_SHA,
    }
    row.update(over)
    return row


@pytest.fixture
def seeded(ingest, query):
    """Four repositories covering every deduplication hazard."""
    repos = [
        # direct dependant, most stars
        repo_row(id=1, owner='mastodon', repo='mastodon', stars=300),
        # transitive dependant
        repo_row(id=2, owner='rails', repo='rails', stars=200),
        # different language
        repo_row(id=3, owner='py', repo='app', stars=500, language='python'),
        # scanned twice: only the current sha should count
        repo_row(id=4, owner='stale', repo='scan', stars=400),
    ]
    artifacts = [
        artifact_row(repository_id=1, relationship=DIRECT, version='2.9.1'),
        artifact_row(repository_id=2, relationship=TRANSITIVE),
        artifact_row(repository_id=3, relationship=DIRECT, type='python'),
        # same package, two catalogers -> two rows, one repository
        artifact_row(repository_id=4, artifact_id='art-a'),
        artifact_row(repository_id=4, artifact_id='art-b'),
        # a previous scan of repo 4 that is no longer current
        artifact_row(
            repository_id=4, artifact_id='art-old',
            version='2.7.0', sbom_commit_sha=STALE_SHA,
        ),
    ]
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repos), REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )
    return query


# --- dependents ------------------------------------------------------------

def test_dependents_are_ordered_by_stars(seeded):
    deps = seeded.get_dependents('mail')
    assert [d.full_name for d in deps] == [
        'py/app', 'stale/scan', 'mastodon/mastodon', 'rails/rails',
    ]


def test_each_repository_appears_once(seeded):
    """repo 4 has two catalogers plus a stale scan."""
    deps = seeded.get_dependents('mail')
    names = [d.full_name for d in deps]
    assert len(names) == len(set(names))


def test_stale_scans_are_excluded(seeded):
    """Only artifacts from the repository's current sbom_commit_sha count."""
    versions = {d.full_name: d.version for d in seeded.get_dependents('mail')}
    assert versions['stale/scan'] == '2.9.0', 'stale scan said 2.7.0'


def test_dependents_carry_their_relationship(seeded):
    by_name = {
        d.full_name: d.relationship for d in seeded.get_dependents('mail')
    }
    assert by_name['mastodon/mastodon'] == DIRECT
    assert by_name['rails/rails'] == TRANSITIVE


def test_direct_only_filters_transitive_dependants(seeded):
    deps = seeded.get_dependents('mail', direct_only=True)
    assert {d.full_name for d in deps} == {'mastodon/mastodon', 'py/app'}


def test_language_filter(seeded):
    deps = seeded.get_dependents('mail', language='ruby')
    assert {d.full_name for d in deps} == {
        'mastodon/mastodon', 'rails/rails', 'stale/scan',
    }


def test_limit_is_applied_after_ordering(seeded):
    deps = seeded.get_dependents('mail', limit=2)
    assert [d.full_name for d in deps] == ['py/app', 'stale/scan']


def test_unknown_library_has_no_dependents(seeded):
    assert seeded.get_dependents('does-not-exist') == []


# --- counts ----------------------------------------------------------------

def test_dependent_count_counts_repositories_not_artifact_rows(seeded):
    """Repo 4 contributes 3 artifact rows but is one dependant."""
    assert seeded.get_dependent_count('mail') == 4


def test_dependent_count_respects_direct_only(seeded):
    assert seeded.get_dependent_count('mail', direct_only=True) == 2


def test_dependent_count_respects_language(seeded):
    assert seeded.get_dependent_count('mail', language='python') == 1


def test_candidate_counts_are_repository_counts(seeded):
    candidates = seeded.search_library_candidates('mai')
    assert [(c.name, c.repository_count) for c in candidates] == [('mail', 4)]


def test_candidate_search_is_case_insensitive(seeded):
    assert seeded.search_library_candidates('MAIL')[0].name == 'mail'


# --- aggregates ------------------------------------------------------------

def test_language_stats_count_distinct_repositories(seeded):
    counts = {c.language: c.repository_count for c in seeded.get_language_stats()}
    assert counts == {'ruby': 3, 'python': 1}


def test_top_packages_split_direct_from_total(seeded):
    top = seeded.get_top_packages(limit=5)
    mail = next(p for p in top if p.name == 'mail')
    assert mail.repository_count == 4
    assert mail.direct_count == 2
    assert mail.transitive_count == 2


def test_database_stats_report_every_table(seeded):
    stats = seeded.get_stats()
    assert stats.repositories == 4
    assert stats.artifacts == 6
    assert stats.releases == 0


# --- deduplication of repeated ingestion -----------------------------------

def test_reingesting_a_repository_does_not_duplicate_it(ingest, query):
    first = repo_row(id=7, stars=10)
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows([first]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows([repo_row(id=7, stars=99)]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows([artifact_row(repository_id=7)]),
        ARTIFACTS.column_names,
    )

    deps = query.get_dependents('mail')
    assert len(deps) == 1
    assert deps[0].stars == 99, 'the later row wins'
    assert query.get_stats().repositories == 1
