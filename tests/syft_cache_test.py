"""SBOM cache keys must pin the tool that produced them."""
from chatsbom.core.config import PathConfig


def test_sbom_cache_path_includes_the_syft_version():
    paths = PathConfig()
    p = paths.get_sbom_cache_path(42, 'deadbeef', '1.52.0')
    parts = p.parts
    assert '1.52.0' in parts, f'syft version not in cache path: {p}'
    assert parts.index('1.52.0') < parts.index('42'), (
        'version must come before the repo so an upgrade partitions cleanly'
    )


def test_different_syft_versions_do_not_share_a_cache_entry():
    paths = PathConfig()
    a = paths.get_sbom_cache_path(1, 'abc', '1.41.2')
    b = paths.get_sbom_cache_path(1, 'abc', '1.52.0')
    assert a != b


def test_unknown_version_is_partitioned_too():
    paths = PathConfig()
    p = paths.get_sbom_cache_path(1, 'abc', None)
    assert 'unknown' in p.parts
    assert p != paths.get_sbom_cache_path(1, 'abc', '1.52.0')


def test_the_ref_is_not_part_of_the_key():
    """The content hash identifies the input; two refs at one commit
    scanned the same content and are one entry (#55)."""
    paths = PathConfig()
    p = paths.get_sbom_cache_path(42, 'abc', '1.52.0')
    assert p.parts[-3:] == ('1.52.0', '42', 'abc.json')
