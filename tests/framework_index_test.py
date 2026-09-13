"""A package -> framework index, built once instead of per row."""
import pytest

from chatsbom.models.framework import Framework
from chatsbom.models.framework_index import FrameworkIndex
from chatsbom.models.language import Language


@pytest.fixture(scope='module')
def index() -> FrameworkIndex:
    return FrameworkIndex.build()


def test_known_package_resolves_to_its_framework(index):
    assert index.framework_for('github.com/gin-gonic/gin') is Framework.GIN
    assert index.framework_for('express') is Framework.EXPRESS


def test_unknown_package_resolves_to_none(index):
    assert index.framework_for('left-pad') is None


def test_lookup_is_case_insensitive(index):
    assert index.framework_for('Express') is Framework.EXPRESS


def test_detect_picks_the_framework_from_a_package_set(index):
    assert index.detect(['left-pad', 'express', 'ms']) is Framework.EXPRESS


def test_detect_returns_none_when_nothing_matches(index):
    assert index.detect(['left-pad', 'ms']) is None


def test_detect_is_deterministic_when_several_frameworks_match(index):
    """Declaration order decides, not iteration order of the input."""
    forwards = index.detect(['express', 'django'])
    backwards = index.detect(['django', 'express'])
    assert forwards is backwards


def test_every_framework_is_indexed(index):
    indexed = {index.framework_for(p) for p in index.packages}
    assert indexed == set(Framework)


def test_packages_for_a_language(index):
    packages = index.packages_for_language(Language.GO)
    assert 'github.com/gin-gonic/gin' in packages
    assert 'express' not in packages


def test_frameworks_of_a_language(index):
    assert Framework.GIN in index.frameworks_for_language(Language.GO)
    assert Framework.EXPRESS not in index.frameworks_for_language(Language.GO)


def test_framework_map_shape_matches_the_query_layer(index):
    """get_frameworks_for_repositories takes {name: [packages]}."""
    mapping = index.as_framework_map()
    assert mapping[str(Framework.GIN)] == ['github.com/gin-gonic/gin']
    assert all(isinstance(v, list) for v in mapping.values())


def test_build_is_cached(index):
    assert FrameworkIndex.build() is index
