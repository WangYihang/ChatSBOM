"""Typed query rows replace the positional tuples the old API returned."""
import pytest

from chatsbom.models.query import Dependent
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
from chatsbom.models.query import row_mapper
from chatsbom.models.relationship import DIRECT


def test_dependent_from_named_row():
    dep = Dependent.from_row({
        'owner': 'mikel',
        'repo': 'mail',
        'stars': 3670,
        'version': '2.9.1',
        'url': 'https://github.com/mikel/mail',
        'relationship': 'direct',
    })
    assert dep.full_name == 'mikel/mail'
    assert dep.relationship == DIRECT
    assert dep.stars == 3670


def test_dependent_rejects_bad_relationship():
    with pytest.raises(ValueError, match='relationship'):
        Dependent.from_row({
            'owner': 'a', 'repo': 'b', 'stars': 1,
            'version': '1', 'url': '', 'relationship': 'DIRECT',
        })


def test_dependent_is_immutable():
    dep = Dependent.from_row({
        'owner': 'a', 'repo': 'b', 'stars': 1,
        'version': '1', 'url': '', 'relationship': 'direct',
    })
    with pytest.raises(AttributeError):
        dep.stars = 2


def test_dependent_missing_column_names_the_column():
    with pytest.raises(KeyError, match='stars'):
        Dependent.from_row({
            'owner': 'a', 'repo': 'b',
            'version': '1', 'url': '', 'relationship': 'direct',
        })


def test_dependent_ignores_extra_columns():
    """A query may select more than the model needs."""
    dep = Dependent.from_row({
        'owner': 'a', 'repo': 'b', 'stars': 1, 'version': '1',
        'url': '', 'relationship': 'direct', 'id': 99,
    })
    assert dep.owner == 'a'


def test_null_strings_from_clickhouse_become_empty():
    dep = Dependent.from_row({
        'owner': 'a', 'repo': 'b', 'stars': 1,
        'version': None, 'url': None, 'relationship': 'direct',
    })
    assert dep.version == ''
    assert dep.url == ''


def test_library_candidate_from_row():
    cand = LibraryCandidate.from_row({'name': 'mail', 'repository_count': 118})
    assert (cand.name, cand.repository_count) == ('mail', 118)


def test_language_count_from_row():
    row = LanguageCount.from_row({'language': 'ruby', 'repository_count': 863})
    assert (row.language, row.repository_count) == ('ruby', 863)


def test_package_popularity_counts_direct_and_total():
    row = PackagePopularity.from_row({
        'name': 'mail', 'repository_count': 118, 'direct_count': 17,
    })
    assert row.direct_count == 17
    assert row.transitive_count == 101


def test_row_mapper_maps_a_whole_result_set():
    to_candidates = row_mapper(LibraryCandidate)
    out = to_candidates([
        {'name': 'mail', 'repository_count': 2},
        {'name': 'mini_mime', 'repository_count': 1},
    ])
    assert out == [
        LibraryCandidate('mail', 2),
        LibraryCandidate('mini_mime', 1),
    ]
