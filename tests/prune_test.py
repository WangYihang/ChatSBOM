"""Retention, without which continuous collection fills the disk.

data/ is already 46 GB and .cache/ 30 GB for a single snapshot. Under
continuous operation every new commit produces another content tree and
another SBOM, so the growth is unbounded. The intermediate artefacts are
recomputable inputs — the history that matters is in ClickHouse — so they
are what gets pruned.
"""
import pytest

from chatsbom.core.prune import prune_scan_dirs
from chatsbom.core.prune import PruneReport
from chatsbom.core.prune import scan_dirs_for


def make_scan(root, owner, repo, ref, sha, size=64):
    """data/<stage>/<lang>/<owner>/<repo>/<ref>/<sha>/file"""
    d = root / 'ruby' / owner / repo / ref / sha
    d.mkdir(parents=True)
    (d / 'Gemfile.lock').write_bytes(b'x' * size)
    return d


# --- discovery ------------------------------------------------------------

def test_scans_are_found_per_repository(tmp_path):
    make_scan(tmp_path, 'o', 'r', 'main', 'a' * 40)
    make_scan(tmp_path, 'o', 'r', 'main', 'b' * 40)
    make_scan(tmp_path, 'o', 'other', 'main', 'c' * 40)

    grouped = scan_dirs_for(tmp_path)
    assert set(grouped) == {('ruby', 'o', 'r'), ('ruby', 'o', 'other')}
    assert len(grouped[('ruby', 'o', 'r')]) == 2


def test_a_missing_directory_yields_nothing(tmp_path):
    assert scan_dirs_for(tmp_path / 'absent') == {}


def test_unexpected_depths_are_ignored(tmp_path):
    (tmp_path / 'ruby' / 'o').mkdir(parents=True)
    (tmp_path / 'stray.json').write_text('{}')
    assert scan_dirs_for(tmp_path) == {}


# --- pruning --------------------------------------------------------------

def test_the_newest_scan_is_kept(tmp_path):
    old = make_scan(tmp_path, 'o', 'r', 'main', 'a' * 40)
    new = make_scan(tmp_path, 'o', 'r', 'main', 'b' * 40)
    # Make the ordering unambiguous.
    import os
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    prune_scan_dirs(tmp_path, keep=1)

    assert new.exists()
    assert not old.exists()


def test_keep_n_retains_the_n_newest(tmp_path):
    import os
    dirs = []
    for i in range(5):
        d = make_scan(tmp_path, 'o', 'r', 'main', chr(97 + i) * 40)
        os.utime(d, (1000 + i, 1000 + i))
        dirs.append(d)

    prune_scan_dirs(tmp_path, keep=2)

    assert [d.exists() for d in dirs] == [False, False, False, True, True]


def test_repositories_are_pruned_independently(tmp_path):
    import os
    a1 = make_scan(tmp_path, 'o', 'one', 'main', 'a' * 40)
    a2 = make_scan(tmp_path, 'o', 'one', 'main', 'b' * 40)
    b1 = make_scan(tmp_path, 'o', 'two', 'main', 'c' * 40)
    os.utime(a1, (1000, 1000))
    os.utime(a2, (2000, 2000))

    prune_scan_dirs(tmp_path, keep=1)

    assert not a1.exists()
    assert a2.exists()
    assert b1.exists(), 'a repository with one scan loses nothing'


def test_nothing_is_removed_when_within_the_limit(tmp_path):
    d = make_scan(tmp_path, 'o', 'r', 'main', 'a' * 40)
    report = prune_scan_dirs(tmp_path, keep=3)
    assert d.exists()
    assert report.removed == 0


def test_report_counts_and_sizes(tmp_path):
    import os
    old = make_scan(tmp_path, 'o', 'r', 'main', 'a' * 40, size=100)
    new = make_scan(tmp_path, 'o', 'r', 'main', 'b' * 40, size=100)
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    report = prune_scan_dirs(tmp_path, keep=1)
    assert report.removed == 1
    assert report.bytes_freed >= 100
    assert report.kept == 1


def test_dry_run_removes_nothing_but_still_reports(tmp_path):
    import os
    old = make_scan(tmp_path, 'o', 'r', 'main', 'a' * 40)
    new = make_scan(tmp_path, 'o', 'r', 'main', 'b' * 40)
    os.utime(old, (1000, 1000))
    os.utime(new, (2000, 2000))

    report = prune_scan_dirs(tmp_path, keep=1, dry_run=True)

    assert old.exists() and new.exists()
    assert report.removed == 1, 'reports what it would remove'
    assert report.dry_run


@pytest.mark.parametrize('keep', [0, -1])
def test_keep_must_be_positive(tmp_path, keep):
    """Refuse to delete every scan: that is a mistake, not a policy."""
    with pytest.raises(ValueError, match='keep'):
        prune_scan_dirs(tmp_path, keep=keep)


def test_reports_add_up(tmp_path):
    a = PruneReport(removed=2, kept=1, bytes_freed=100)
    b = PruneReport(removed=3, kept=2, bytes_freed=50)
    total = a + b
    assert (total.removed, total.kept, total.bytes_freed) == (5, 3, 150)
