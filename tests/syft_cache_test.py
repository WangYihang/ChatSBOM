"""SBOM cache keys must pin the tool that produced them."""
from chatsbom.core.config import PathConfig


def test_sbom_cache_path_includes_the_syft_version():
    paths = PathConfig()
    p = paths.get_sbom_cache_path('owner', 'repo', 'v1', 'deadbeef', '1.41.2')
    parts = p.parts
    assert '1.41.2' in parts, f'syft version not in cache path: {p}'
    assert parts.index('1.41.2') < parts.index('owner'), (
        'version must come before the repo so an upgrade partitions cleanly'
    )


def test_different_syft_versions_do_not_share_a_cache_entry():
    paths = PathConfig()
    a = paths.get_sbom_cache_path('o', 'r', 'v1', 'abc', '1.41.2')
    b = paths.get_sbom_cache_path('o', 'r', 'v1', 'abc', '1.42.0')
    assert a != b


def test_unknown_version_is_partitioned_too():
    paths = PathConfig()
    p = paths.get_sbom_cache_path('o', 'r', 'v1', 'abc', None)
    assert 'unknown' in p.parts
    assert p != paths.get_sbom_cache_path('o', 'r', 'v1', 'abc', '1.41.2')
