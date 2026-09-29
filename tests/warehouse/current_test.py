"""What is current: the newest scan of each source, of the corpus.

One rule in place of ClickHouse's `corpus`, `current_artifacts` and
`facts` views and the pointers a repository row keeps (#128 §2.3): each
repository's newest scan from each source, of the repositories the
newest complete snapshot lists.
"""
from __future__ import annotations

from datetime import date

import pytest

from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import rows
from tests.warehouse.conftest import spdx
from tests.warehouse.conftest import Store

A = 'a' * 40
B = 'b' * 40

APP = Listed(1, 'acme', 'app', language='Java')
WEB = Listed(2, 'acme', 'web', language='TypeScript')
OLD = Listed(3, 'acme', 'old', language='Go')

GRADLE = """
dependencies {
    implementation 'com.google.guava:guava:33.0.0-jre'
}
"""


@pytest.fixture
def scanned(store: Store) -> Store:
    """`acme/app` scanned at two commits, its graph fetched twice, the
    newer before the second scan. `acme/old` is in an older snapshot
    only, so outside the corpus, with a scan of its own."""
    name = store.snapshot(date(2026, 9, 1), APP, WEB)
    store.seed(name, APP, WEB)
    older = store.snapshot(date(2026, 3, 1), APP, WEB, OLD)
    store.seed(older, OLD)

    store.sbom(
        1, A, artifact('guava', '32.1.0-jre', 'java-archive'),
        artifact('commons-lang3', '3.14.0', 'java-archive'),
        at=at(2026, 2, 11, 9, 30),
    )
    store.content(1, A, {'build.gradle': GRADLE})
    store.sbom(
        1, B, artifact('guava', '33.0.0-jre', 'java-archive'),
        at=at(2026, 9, 14, 10, 0),
    )
    store.content(1, B, {'settings.gradle': "rootProject.name = 'app'"})
    store.record(1, 'acme', 'app', commit=B)
    store.graph(
        1, spdx('2026-03-05T08:00:00Z', [('guava', '31.0', 'maven')]),
        fetched=at(2026, 3, 5, 8, 0),
    )
    store.graph(
        1,
        spdx(
            '2026-08-01T08:00:00Z',
            [('guava', '33.0.0-jre', 'maven'), ('okio', '3.9.0', 'maven')],
        ),
        fetched=at(2026, 8, 1, 8, 0),
    )

    store.sbom(
        3, A, artifact(
            'cobra', '1.8.0',
            'go-module',
        ), at=at(2026, 2, 1),
    )
    store.record(3, 'acme', 'old', commit=A)
    return store


def test_the_newest_scan_of_each_source_is_current(
    scanned: Store, built: Build,
) -> None:
    con = built()
    assert rows(
        con,
        'SELECT repository_id, source, observed_at FROM current_scans '
        'ORDER BY repository_id, source',
    ) == [
        (1, 'github-depgraph', at(2026, 8, 1, 8, 0).replace(tzinfo=None)),
        (1, 'manifest', at(2026, 9, 14, 10, 0).replace(tzinfo=None)),
        (1, 'syft', at(2026, 9, 14, 10, 0).replace(tzinfo=None)),
    ]


def test_an_older_scans_observations_are_history(
    scanned: Store, built: Build,
) -> None:
    """guava 32.1.0 and commons-lang3 were at the first commit: kept,
    not current. The manifests of the second commit declare nothing, so
    the first commit's Gradle declaration is not current either."""
    con = built()
    assert rows(
        con,
        'SELECT source, name, version FROM current_observations '
        'ORDER BY source, name',
    ) == [
        ('github-depgraph', 'guava', '33.0.0-jre'),
        ('github-depgraph', 'okio', '3.9.0'),
        ('syft', 'guava', '33.0.0-jre'),
    ]
    assert rows(
        con,
        'SELECT count(*) FROM observations WHERE repository_id = 1',
    ) == [(7,)]


def test_a_graph_is_current_by_its_own_date(
    scanned: Store, built: Build,
) -> None:
    """Fetched from the default branch when asked, a graph is an
    observation of its own (#22): the newer fetch is current whatever
    commit Syft last read."""
    scanned.graph(
        1, spdx('2026-09-20T08:00:00Z', [('guava', '33.1.0', 'maven')]),
        fetched=at(2026, 9, 20, 8, 0),
    )
    con = built()
    assert rows(
        con,
        'SELECT name, version FROM current_observations '
        "WHERE source = 'github-depgraph'",
    ) == [('guava', '33.1.0')]


def test_a_newer_scan_that_found_nothing_is_current(
    scanned: Store, built: Build,
) -> None:
    """Nothing found is an observation too: the previous scan's
    packages are not current once a newer one saw none."""
    C = 'c' * 40
    scanned.sbom(1, C, at=at(2026, 9, 20))
    scanned.record(1, 'acme', 'app', commit=C)
    con = built()
    assert rows(
        con,
        "SELECT count(*) FROM current_observations WHERE source = 'syft'",
    ) == [(0,)]


def test_only_the_corpus_is_current(scanned: Store, built: Build) -> None:
    """`acme/old` keeps its scan and its observations; the current
    snapshot does not list it, so none of them is current (D2)."""
    con = built()
    assert rows(
        con, 'SELECT count(*) FROM scans WHERE repository_id = 3',
    ) == [(2,)]
    assert rows(
        con,
        'SELECT count(*) FROM current_scans WHERE repository_id = 3',
    ) == [(0,)]


def test_a_fact_is_one_however_many_manifests_report_it(
    scanned: Store, built: Build,
) -> None:
    """The graph reports per manifest: two rows that differ only in
    `artifact_id` are one fact (`facts` in `core/schema.py`)."""
    document = spdx('2026-09-21T08:00:00Z', [('okio', '3.9.0', 'maven')])
    package = document['sbom']['packages'][1]
    document['sbom']['packages'].append(
        {**package, 'SPDXID': 'SPDXRef-maven-okio-2'},
    )
    scanned.graph(1, document, fetched=at(2026, 9, 21, 8, 0))
    con = built()
    assert rows(
        con,
        'SELECT count(*) FROM current_observations '
        "WHERE source = 'github-depgraph'",
    ) == [(2,)]
    assert rows(
        con,
        'SELECT name, version, relationship FROM facts '
        "WHERE source = 'github-depgraph'",
    ) == [('okio', '3.9.0', 'transitive')]
