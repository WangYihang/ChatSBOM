"""The repository-keyed layout, and the mapping from the one before it.

`data migrate-layout` moves the files by this mapping and rewrites
`raw_documents.path` by the same one in SQL; the readers translate paths
recorded before the move by it. Three spellings of one function, held
to agreeing here.
"""
from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest

from chatsbom.core.config import PathConfig
from chatsbom.core.layout import content_inside
from chatsbom.core.layout import landed
from chatsbom.core.layout import parse_legacy
from chatsbom.core.layout import parse_scan
from chatsbom.core.layout import relocate
from chatsbom.core.layout import rewritten
from chatsbom.core.layout import rewritten_sha
from chatsbom.core.layout import sql_needs_rewrite
from chatsbom.core.layout import sql_rewritten_path
from chatsbom.core.layout import sql_rewritten_sha
from tests.conftest import requires_clickhouse

SHA = '0123456789abcdef0123456789abcdef01234567'
OTHER = 'fedcba9876543210fedcba9876543210fedcba98'

#: (stored path, repository id) -> what it becomes.
CASES: list[tuple[str, int, str]] = [
    (
        f'data/07-sbom/go/go-gorm/gorm/v1.31.1/{SHA}/sbom.json', 13855476,
        f'07-sbom/13855476/{SHA}/sbom.json',
    ),
    (
        f'data/06-github-content/rust/microsoft/windows-drivers-rs/'
        f'cargo-wdk-v0.1.1/{SHA}/Cargo.toml', 7,
        f'06-github-content/7/{SHA}/Cargo.toml',
    ),
    (
        f'/abs/data/06-github-content/javascript/o/r/HEAD/{SHA}/'
        'packages/app/package.json', 9,
        f'06-github-content/9/{SHA}/packages/app/package.json',
    ),
    (
        'data/09-github-depgraph/ruby/macournoyer/thin/sbom.spdx.json', 5,
        '09-github-depgraph/5/legacy/sbom.spdx.json',
    ),
    (
        f'data/09-github-depgraph/5/20260920T101010Z-{SHA}/sbom.spdx.json', 5,
        f'09-github-depgraph/5/20260920T101010Z-{SHA}/sbom.spdx.json',
    ),
    # A ref with slashes spans directories; the scan is the first commit.
    (
        f'data/07-sbom/typescript/Shopify/polaris-react/@shopify/'
        f'polaris@13.9.5/{SHA}/sbom.json', 13,
        f'07-sbom/13/{SHA}/sbom.json',
    ),
    (
        f'data/06-github-content/javascript/o/r/release/1.4.0/{SHA}/'
        f'vendor/{OTHER}/package.json', 13,
        f'06-github-content/13/{SHA}/vendor/{OTHER}/package.json',
    ),
    # Already relative and repository-keyed: left alone.
    (f'07-sbom/3/{SHA}/sbom.json', 3, f'07-sbom/3/{SHA}/sbom.json'),
    # Not a stage file at all: a record's ledger label.
    ('data/07-sbom/ruby.jsonl', 4, 'data/07-sbom/ruby.jsonl'),
    ('data/02-github-repo/ruby.jsonl', 4, 'data/02-github-repo/ruby.jsonl'),
    # A language directory the old layout never used is not the old
    # layout.
    (f'data/07-sbom/cobol/o/r/main/{SHA}/sbom.json', 4,
     f'data/07-sbom/cobol/o/r/main/{SHA}/sbom.json'),
]


@pytest.mark.parametrize('stored,repository_id,expected', CASES)
def test_the_mapping(stored: str, repository_id: int, expected: str) -> None:
    assert rewritten(stored, repository_id) == expected


def test_the_sha_a_legacy_scan_row_is_at() -> None:
    assert rewritten_sha(
        f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json',
    ) == SHA
    assert rewritten_sha('data/07-sbom/ruby.jsonl') is None


def test_the_ref_is_dropped_so_two_refs_at_one_commit_are_one_scan() -> None:
    head = rewritten(f'data/05-github-tree/go/o/r/HEAD/{SHA}/tree.txt', 1)
    main = rewritten(f'data/05-github-tree/go/o/r/main/{SHA}/tree.txt', 1)
    assert head == main == f'05-github-tree/1/{SHA}/tree.txt'


def test_relocate_keeps_the_prefix_a_reader_resolves_against() -> None:
    """A list's path is relative to the working directory, and the
    relocated one must be too."""
    assert relocate(
        f'data/06-github-content/ruby/o/r/main/{SHA}', 12,
    ) == Path(f'data/06-github-content/12/{SHA}')
    assert relocate(
        f'data/10-generated-lock/php/o/r/{SHA}', 12,
    ) == Path(f'data/10-generated-lock/12/{SHA}')
    assert relocate(
        'data/09-github-depgraph/go/o/r/sbom.spdx.json', 12,
    ) == Path('data/09-github-depgraph/12/legacy/sbom.spdx.json')
    # Nothing to do, or nothing known to do it with.
    new = f'data/07-sbom/12/{SHA}/sbom.json'
    assert relocate(new, 12) == Path(new)
    assert relocate(f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json', None) == Path(
        f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json',
    )


def test_the_paths_config_builds_agree_with_the_mapping() -> None:
    paths = PathConfig(base_data_dir=Path('data'))
    assert str(paths.sbom_file(3, SHA)) == str(
        relocate(f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json', 3),
    )
    assert paths.content_root(3, SHA) == relocate(
        f'data/06-github-content/go/o/r/v1/{SHA}', 3,
    )
    assert paths.tree_file(3, SHA) == relocate(
        f'data/05-github-tree/go/o/r/v1/{SHA}/tree.txt', 3,
    )
    assert paths.generated_lock_path(3, SHA) == relocate(
        f'data/10-generated-lock/go/o/r/{SHA}', 3,
    )
    assert paths.legacy_depgraph_file(3) == relocate(
        'data/09-github-depgraph/go/o/r/sbom.spdx.json', 3,
    )
    assert paths.get_tree_cache_path(3, SHA) == Path(
        f'.cache/git-tree/3/{SHA}/tree.txt',
    )


def test_a_manifest_path_within_its_repository_in_either_layout() -> None:
    assert content_inside(
        f'06-github-content/7/{SHA}/app/client/package.json',
    ) == 'app/client/package.json'
    assert content_inside(
        f'data/06-github-content/java/o/r/main/{SHA}/pom.xml',
    ) == 'pom.xml'
    assert content_inside('data/07-sbom/ruby.jsonl') is None


def test_landed_paths_are_relative_to_the_data_directory() -> None:
    assert landed(f'/mnt/x/data/07-sbom/1/{SHA}/sbom.json') == (
        f'07-sbom/1/{SHA}/sbom.json'
    )
    assert landed(
        'data/02-github-repo/go.jsonl',
    ) == 'data/02-github-repo/go.jsonl'


def test_both_layouts_parse_and_cannot_be_mistaken_for_each_other() -> None:
    legacy = parse_legacy(f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json')
    assert legacy is not None
    assert (legacy.language, legacy.owner, legacy.repo, legacy.ref) == (
        'go', 'o', 'r', 'v1',
    )
    assert parse_scan(f'data/07-sbom/go/o/r/v1/{SHA}/sbom.json') is None
    scan = parse_scan(f'data/07-sbom/12/{SHA}/sbom.json')
    assert scan is not None and scan.repository_id == 12
    assert parse_legacy(f'data/07-sbom/12/{SHA}/sbom.json') is None


@requires_clickhouse
def test_the_sql_mapping_is_the_python_mapping(ingest) -> None:
    """The mutation `data migrate-layout` runs, row for row."""
    rows = [
        [
            kind, repository_id, stored, f'{index:064x}',
            datetime(2026, 2, 11, tzinfo=timezone.utc), '{}',
        ]
        for index, (stored, repository_id, _) in enumerate(CASES)
        for kind in ('syft',)
    ]
    ingest.client.insert(
        'raw_documents', rows,
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
        ],
    )
    got = ingest.client.query(
        f'SELECT path, repository_id, {sql_rewritten_path()}, '
        f'{sql_rewritten_sha()}, {sql_needs_rewrite()} FROM raw_documents',
    ).result_rows
    assert len(got) == len(CASES)
    for stored, repository_id, new, sha, needs in got:
        assert new == rewritten(stored, repository_id), stored
        assert sha == (rewritten_sha(stored) or ''), stored
        assert bool(needs) == (new != stored), stored
