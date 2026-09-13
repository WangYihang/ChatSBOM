"""The SBOM cache key must be cheap to compute and still change when it should."""
import pytest

from chatsbom.services.sbom_service import content_fingerprint


def make_project(root, files: dict[str, str]):
    for name, body in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body)
    return root


def test_identical_trees_hash_the_same(tmp_path):
    a = make_project(tmp_path / 'a', {'Gemfile': "gem 'mail'\n"})
    b = make_project(tmp_path / 'b', {'Gemfile': "gem 'mail'\n"})
    assert content_fingerprint(a) == content_fingerprint(b)


def test_manifest_content_change_changes_the_hash(tmp_path):
    root = make_project(tmp_path / 'p', {'Gemfile': "gem 'mail'\n"})
    before = content_fingerprint(root)
    (root / 'Gemfile').write_text("gem 'mail'\ngem 'rails'\n")
    assert content_fingerprint(root) != before


def test_lockfile_content_change_changes_the_hash(tmp_path):
    root = make_project(tmp_path / 'p', {'Gemfile.lock': 'mail (2.9.0)\n'})
    before = content_fingerprint(root)
    (root / 'Gemfile.lock').write_text('mail (2.9.1)\n')
    assert content_fingerprint(root) != before


def test_added_file_changes_the_hash(tmp_path):
    root = make_project(tmp_path / 'p', {'Gemfile': 'x\n'})
    before = content_fingerprint(root)
    (root / 'README.md').write_text('hello')
    assert content_fingerprint(root) != before


def test_renamed_file_changes_the_hash(tmp_path):
    root = make_project(tmp_path / 'p', {'a.rb': 'same'})
    before = content_fingerprint(root)
    (root / 'a.rb').rename(root / 'b.rb')
    assert content_fingerprint(root) != before


def test_resized_source_file_changes_the_hash(tmp_path):
    root = make_project(tmp_path / 'p', {'app.rb': 'short'})
    before = content_fingerprint(root)
    (root / 'app.rb').write_text('much longer contents')
    assert content_fingerprint(root) != before


def test_source_files_are_not_read_byte_for_byte(tmp_path):
    """Syft only reports packages from manifests, so only those are read.

    The old implementation hashed every byte of every file and then Syft
    read them all again.
    """
    root = make_project(tmp_path / 'p', {'Gemfile': 'x\n', 'app.rb': 'a' * 64})
    before = content_fingerprint(root)
    # Same length, different bytes: not a change Syft could observe.
    (root / 'app.rb').write_text('b' * 64)
    assert content_fingerprint(root) == before


def test_empty_directory_is_stable(tmp_path):
    root = tmp_path / 'empty'
    root.mkdir()
    assert content_fingerprint(root) == content_fingerprint(root)


def test_missing_directory_raises(tmp_path):
    with pytest.raises(OSError):
        content_fingerprint(tmp_path / 'nope')
