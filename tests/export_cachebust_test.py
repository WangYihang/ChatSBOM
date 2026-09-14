"""Immutable files must have immutable names.

Parquet is served `immutable, max-age=31536000` because a client should
never re-fetch a file it already has -- that is what makes a query cost
one ranged GET instead of a download. But the filenames were fixed, so a
new export reused every URL, and a browser holding last week's
`repositories.parquet` kept it for a year while revalidating a manifest
that described a different file.

Measured, not theorised: after adding a column, the manifest advertised
sha 659592a2 while the browser still held e8e84bf5, and every query
failed with `Binder Error: Table "r" does not have a column named
"observed_at"`. A stale cache presented as a schema bug.

Naming each file after its own content makes `immutable` true: new
content is a new URL, and the old one can be cached forever without
lying.
"""
from __future__ import annotations

from chatsbom.export.parquet import content_addressed_name


class TestContentAddressedNames:

    def test_name_carries_a_checksum_prefix(self) -> None:
        assert content_addressed_name(
            'repositories.parquet', '659592a2ffff',
        ) == 'repositories-659592a2.parquet'

    def test_different_content_gives_a_different_url(self) -> None:
        """The whole point: a re-export cannot reuse a cached file."""
        before = content_addressed_name('repositories.parquet', 'e8e84bf5aaaa')
        after = content_addressed_name('repositories.parquet', '659592a2bbbb')
        assert before != after

    def test_identical_content_gives_an_identical_url(self) -> None:
        """So an unchanged table is still served from cache."""
        assert content_addressed_name(
            'artifacts.parquet', 'abc123deffff',
        ) == content_addressed_name('artifacts.parquet', 'abc123deffff')

    def test_extension_survives_so_the_content_type_is_inferable(self) -> None:
        name = content_addressed_name('artifacts.parquet', 'abc123deffff')
        assert name.endswith('.parquet')

    def test_manifest_is_not_content_addressed(self) -> None:
        """It is the one file whose URL must be stable to be found."""
        assert content_addressed_name(
            'manifest.json', 'abc123de',
        ) == 'manifest.json'
