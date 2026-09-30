"""The contract corpus: the seed `d1.sql`, `calls.json` and `urls.json`
were recorded from (`web/test/fixtures/contract/`).

Copied, rows and comments as they were, from `web/test/fixtures/contract/
build.py` at 119be7f, whose `seed` wrote it into ClickHouse to record
those fixtures there. ClickHouse is gone (#153), and with it the only
way that script had to run, so the seed the Python suite reads is here:
a warehouse, a snapshot and an export are made of it, and held to what
was recorded then (`tests/golden/`, `tests/snapshot/parity_test.py`).

The rows are ClickHouse's shape, as `db index` wrote them, which
`chatsbom/warehouse/rows.py` loads. Every row is here for a reason given
beside it: each is a case where two readers of this data have
disagreed.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

#: Syft's scans. Rails' is earlier than the rest, so the earliest current
#: row (1 February) is not the earliest of the repositories' newest
#: observations (11 February): the two definitions of the span differ.
FEB_EARLY = datetime(2026, 2, 1, 9, 30)
FEB = datetime(2026, 2, 11, 9, 30)
#: A scan of Rails that a later one replaced: history, not current.
JAN = datetime(2026, 1, 20, 9, 30)
#: The dependency graphs, seven months after Syft.
SEP = datetime(2026, 9, 13, 8, 0)
#: A later graph, late in the UTC day: a date made in another zone
#: would be the 15th.
SEP_LATE = datetime(2026, 9, 14, 23, 30)
#: `db index` found no graph (`depgraph_observed_at`'s unset date).
NO_GRAPH = datetime(1970, 1, 2)


def repository(
    id: int,
    owner: str,
    repo: str,
    stars: int,
    language: str,
    ecosystems: list[str],
    commit: str = '',
    graph: datetime = NO_GRAPH,
) -> dict[str, Any]:
    """A `repositories` row, as `db index` writes one."""
    return {
        'id': id, 'owner': owner, 'repo': repo,
        'url': f'https://github.com/{owner}/{repo}', 'stars': stars,
        'description': '', 'created_at': NO_GRAPH, 'language': language,
        'topics': [], 'default_branch': 'main',
        'sbom_ref': 'main' if commit else '', 'sbom_ref_type': 'branch',
        'sbom_commit_sha': commit, 'sbom_commit_sha_short': commit[:7],
        'has_releases': False, 'latest_release_tag': '',
        'latest_release_published_at': NO_GRAPH, 'total_releases': 0,
        'pushed_at': FEB, 'is_archived': False, 'is_fork': False,
        'is_template': False, 'is_mirror': False, 'disk_usage': 0,
        'fork_count': 0, 'watchers_count': 0,
        'license_spdx_id': 'MIT', 'license_name': 'MIT License',
        'manifest_sources': [], 'depgraph_observed_at': graph,
        'depgraph_ref': 'main' if graph != NO_GRAPH else '',
        'depgraph_commit_sha': '', 'github_language': language,
        'ecosystems': ecosystems, 'snapshot': '',
    }


def syft(
    repository_id: int,
    commit: str,
    name: str,
    version: str,
    type: str,
    relationship: str,
    licenses: list[str],
    observed_at: datetime = FEB,
    found_by: str = 'cataloger',
    source: str = 'syft',
    version_kind: str = 'resolved',
    artifact_id: str = '',
) -> dict[str, Any]:
    """An `artifacts` row of a scan at `commit`."""
    return {
        'repository_id': repository_id,
        'artifact_id': artifact_id or f'{repository_id}-{name}-{version}',
        'name': name, 'version': version, 'type': type,
        'purl': f'pkg:{type}/{name}@{version}', 'found_by': found_by,
        'licenses': licenses, 'relationship': relationship,
        'source': source, 'version_kind': version_kind,
        'sbom_ref': 'main', 'sbom_commit_sha': commit,
        'observed_at': observed_at,
    }


def graph(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    relationship: str,
    version_kind: str,
    observed_at: datetime = SEP,
    artifact_id: str = '',
) -> dict[str, Any]:
    """An `artifacts` row of a dependency-graph document: current by the
    instant it states, which its repository records."""
    return syft(
        repository_id, '', name, version, type, relationship, [],
        observed_at=observed_at, found_by='github-dependency-graph',
        source='github-depgraph', version_kind=version_kind,
        artifact_id=artifact_id,
    )


REPOSITORIES_SEED = [
    # Seen by both collectors: every current row of it is not from the
    # same day, and D1 showed the newer date on all of them (#24).
    repository(1, 'rails', 'rails', 58000, 'Ruby', ['gem'], 'r1', SEP),
    # Tied on stars with discourse: an order that stops at the stars
    # cannot say which comes first.
    repository(2, 'mastodon', 'mastodon', 47000, 'Ruby', ['gem'], 'm1'),
    repository(
        3, 'discourse', 'discourse', 47000, 'Ruby', ['gem', 'npm'], 'd1',
        SEP,
    ),
    repository(
        4, 'apache', 'james', 900, 'Java', ['maven'], 'j1',
    ),
    # Composer under both of its spellings: the graph's `composer` here
    # and in firefly and koel, Syft's `php-composer` in monica, firefly
    # and akaunting.
    repository(
        5, 'laravel', 'laravel', 80000, 'PHP', ['composer'], graph=SEP,
    ),
    repository(6, 'monicahq', 'monica', 22000, 'PHP', ['composer'], 'mo1'),
    repository(
        7, 'firefly-iii', 'firefly-iii', 16000, 'PHP', ['composer'], 'f1',
        SEP,
    ),
    repository(
        8, 'koel', 'koel', 16000, 'PHP', ['composer'], graph=SEP_LATE,
    ),
    repository(
        9, 'akaunting', 'akaunting', 9000, 'PHP', ['composer'], 'a1',
    ),
    repository(10, 'psf', 'app', 500, 'Python', ['pypi'], 'p1'),
    # No language on GitHub: the filter's `none`.
    repository(11, 'expressjs', 'site', 100, '', ['npm'], 'e1'),
    # Tracked, never collected: in the denominators, in no row.
    repository(12, 'golang', 'tools', 50, 'Go', []),
]

ARTIFACTS_SEED = [
    # rails: Syft on 1 February, the graph on 13 September, and a scan
    # from January that the February one replaced.
    syft(1, 'r1', 'mail', '2.8.1', 'gem', 'transitive', ['MIT'], FEB_EARLY),
    syft(
        1, 'r1', 'mini_mime', '1.1.5', 'gem', 'transitive', ['MIT'],
        FEB_EARLY,
    ),
    syft(1, 'r0', 'mail', '2.7.0', 'gem', 'transitive', ['MIT'], JAN),
    graph(1, 'mail', '~> 2.8', 'gem', 'direct', 'constraint'),
    # A second constraint string in a second manifest: rails is still
    # one repository with a constraint on `mail`, not two (#120).
    graph(
        1, 'mail', '>= 2.7', 'gem', 'direct', 'constraint',
        artifact_id='1-rails.gemspec',
    ),
    syft(2, 'm1', 'mail', '2.8.1', 'gem', 'direct', ['MIT']),
    syft(2, 'm1', 'mini_mime', '1.1.5', 'gem', 'transitive', ['MIT']),
    # discourse: one version from two cataloguers, and again from the
    # graph. The two Syft rows are one line of the table, the graph's
    # another: they were observed on different days.
    syft(
        3, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemfile-cataloger',
    ),
    syft(
        3, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemspec-cataloger', artifact_id='3-mail-gemspec',
    ),
    graph(3, 'mail', '2.8.1', 'gem', 'direct', 'resolved'),
    graph(3, 'debug', '4.3.4', 'npm', 'transitive', 'resolved'),
    graph(3, 'ms', '2.1.2', 'npm', 'transitive', 'resolved'),
    # `mail` is also a Maven artifact and a PyPI package.
    syft(4, 'j1', 'mail', '1.4.7', 'java-archive', 'direct', ['Apache-2.0']),
    # And a Gradle declaration, for the third source.
    syft(
        4, 'j1', 'jakarta.mail', '2.1.0', 'maven', 'direct', [],
        source='manifest', version_kind='constraint',
    ),
    # The same declaration in two manifests: two rows, one fact.
    graph(
        5, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='5-composer.json',
    ),
    graph(
        5, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='5-packages/app/composer.json',
    ),
    syft(
        6, 'mo1', 'laravel/framework', 'v12.49.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    syft(
        7, 'f1', 'laravel/framework', 'v12.49.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    graph(
        7, 'laravel/framework', '^11.0|^12.0', 'composer', 'direct',
        'constraint',
    ),
    # A third constraint string, so there are more of them than a
    # version spread of two lists.
    graph(
        8, 'laravel/framework', '^10.0', 'composer', 'direct', 'constraint',
        SEP_LATE,
    ),
    # And a fourth, in a second manifest of koel's: four strings, three
    # repositories (#120).
    graph(
        8, 'laravel/framework', '^9.0', 'composer', 'direct', 'constraint',
        SEP_LATE, artifact_id='8-packages/legacy/composer.json',
    ),
    graph(
        8, 'laravel/framework', '', 'composer', 'transitive', 'unversioned',
        SEP_LATE,
    ),
    syft(
        9, 'a1', 'laravel/framework', 'v11.2.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    syft(
        9, 'a1', 'laravel/framework', 'v10.48.0', 'php-composer',
        'transitive', ['MIT'],
    ),
    syft(10, 'p1', 'requests', '2.32.0', 'python', 'direct', ['Apache-2.0']),
    syft(10, 'p1', 'mail', '0.0.1', 'python', 'transitive', []),
    # No manifest said how it arrived: classified is not every record.
    syft(10, 'p1', 'certifi', '2024.2.2', 'python', 'unknown', ['MPL-2.0']),
    # The packages the edges name, so the D1 export can reference them.
    syft(11, 'e1', 'express', '4.19.2', 'npm', 'direct', ['MIT']),
    syft(11, 'e1', 'body-parser', '1.20.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'debug', '2.6.9', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'ms', '2.0.0', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'qs', '6.11.0', 'npm', 'transitive', ['BSD-3-Clause']),
    syft(11, 'e1', 'bytes', '3.1.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'raw-body', '2.5.2', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'side-channel', '1.0.4', 'npm', 'transitive', ['MIT']),
    syft(11, 'e1', 'send', '0.18.0', 'npm', 'transitive', ['MIT']),
]

#: Ties in the second hop (`debug` at 3 under two parents), an edge back
#: to the root, and a pair naming a package no artifact names, which the
#: D1 export leaves out.
EDGES_SEED = [
    ('express', 'body-parser', 5), ('express', 'debug', 4),
    ('express', 'qs', 3), ('express', 'send', 2),
    ('body-parser', 'debug', 3), ('body-parser', 'qs', 3),
    ('body-parser', 'bytes', 2), ('body-parser', 'raw-body', 2),
    ('debug', 'ms', 4), ('send', 'ms', 3), ('send', 'debug', 3),
    ('qs', 'side-channel', 2), ('raw-body', 'bytes', 2),
    ('bytes', 'body-parser', 1), ('mail-dev', 'mail', 1),
]
