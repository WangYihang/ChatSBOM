import re
from datetime import datetime
from datetime import timezone
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

#: Asset fields worth keeping, of the sixteen GitHub returns.
#:
#: `release_assets` is written and never read — no query, no rollup, no
#: panel touches it — and it was the largest column in the database:
#: 5.00 GiB uncompressed against about 90 MiB for every other column in
#: `releases` combined, and 9.7 GiB of the ledgers on disk. A single
#: asset averaged 1,555 bytes, of which `uploader` was a complete
#: GitHub user object.
#:
#: Kept rather than dropped entirely, because the column being unread
#: today is not evidence nobody will ask: "which releases ship a
#: binary, how large, and does it carry a checksum" is a reasonable
#: question of a supply-chain dataset, and this project has twice paid
#: for discarding what it had not yet needed.
#:
#: `digest` is on 11.2% of assets and is the checksum, so it stays even
#: though most rows lack it. Measured: 1,555 -> 301 bytes, 81% smaller,
#: which takes the column from 5.00 GiB to about 0.97 GiB.
ASSET_FIELDS: frozenset[str] = frozenset({
    'name',
    'content_type',
    'size',
    'download_count',
    'browser_download_url',
    'created_at',
    'digest',
})


def trimmed_assets(
    assets: object,
    fields: frozenset[str] = ASSET_FIELDS,
) -> list[dict[str, object]]:
    """Release assets, carrying only the `fields` worth storing.

    Anything that is not a list of mappings is returned as an empty
    list rather than raised on: this runs inside an ingest over 28,000
    repositories, and one oddly-shaped release is not a reason to lose
    the rest.
    """
    if not isinstance(assets, list):
        return []
    trimmed = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        trimmed.append({
            key: value for key, value in asset.items()
            if key in fields
        })
    return trimmed


#: A pre-release marker in a tag's name, matched case-insensitively:
#:
#: - SemVer-style words after a separator or a digit: `-rc.1`, `-rc5`,
#:   `-beta2`, `-alpha`, `-pre`, `-preview`, `-dev`, `-snapshot`,
#:   `-nightly`, `-canary`, `-next`, and `.RC1`/`-SNAPSHOT` as Maven
#:   spells them;
#: - PEP 440's short forms right after a digit: `1.2.0a1`, `1.2.0b2`,
#:   `1.2.0rc1`, and `.dev0` (`.post1` is a release: not listed);
#: - Maven milestones: `-M1`.
#:
#: A word must end there (a digit or a separator may follow, a letter
#: may not), so `-alphabet` or `-devtools` is no marker, and `-final`
#: is none either.
_PRERELEASE_WORD = re.compile(
    r'(?:(?<=\d)|[-._])'
    r'(?:alpha|beta|rc|cr|preview|pre|dev|snapshot|nightly|canary|next)'
    r'(?:[-._]?\d+)*(?![a-z])'
    r'|(?<=\d)[ab]\d+(?![a-z])'
    r'|[-._]m\d+(?![a-z])',
    re.IGNORECASE,
)


def looks_like_prerelease(tag: str) -> bool:
    """Whether a tag's name marks a pre-release (`v7.3-rc5`, `1.2.0b2`,
    `2.0.0-M1`, `5.0.0.BUILD-SNAPSHOT`).

    For bare tags only: a GitHub release's own `prerelease` flag wins.
    Build metadata (`+build.rc1`) says nothing about the version and is
    ignored. The marker must follow a version number, so a name that
    merely contains a word (`pre-commit-hooks`) is not one.
    """
    name = tag.split('+', 1)[0]
    for match in _PRERELEASE_WORD.finditer(name):
        if any(ch.isdigit() for ch in name[:match.start() + 1]):
            return True
    return False


class GitHubRelease(BaseModel):
    id: int = 0
    tag_name: str
    name: str | None = ''
    published_at: datetime | None = None
    target_commitish: str | None = ''
    # The API spells these `prerelease` and `draft`. Without the alias,
    # `extra='ignore'` dropped them and every release candidate was
    # stable. Ledgers are dumped by field name, hence populate_by_name.
    is_prerelease: bool = Field(default=False, alias='prerelease')
    is_draft: bool = Field(default=False, alias='draft')
    created_at: datetime | None = None
    assets: list[dict] = []
    source: str = 'github_release'

    model_config = ConfigDict(extra='ignore', populate_by_name=True)

    @field_validator('published_at', 'created_at', mode='before')
    @classmethod
    def parse_datetime(cls, v: Any) -> datetime | None:
        if not v:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(str(v).replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return None


def is_stable(release: GitHubRelease) -> bool:
    """Whether the release stage may take `release` as the latest stable
    one: neither a draft nor a pre-release, as a GitHub release says for
    itself, or as a bare tag's name says (`looks_like_prerelease`).

    The release stage takes the first of these in its list; the store's
    readers find that one again by its tag (`decisions.chosen`)."""
    if release.is_prerelease or release.is_draft:
        return False
    return not (
        release.source == 'git_tag' and looks_like_prerelease(release.tag_name)
    )


#: Bumped whenever what a cached `ReleaseCache` means changes; a cache of
#: any other version is refetched, not trusted. Version 1 stored every
#: short ref name `git ls-remote` listed as a tag, branches and HEAD
#: included, and once written a branch cannot be told from a tag.
RELEASE_CACHE_VERSION = 2


class ReleaseCache(BaseModel):
    """Formal model for cached release and tag data."""
    # Version 1 wrote no version, so that is what its absence means. A
    # writer must say RELEASE_CACHE_VERSION; if it forgets, the cache is
    # merely refetched, where the opposite default would trust old files.
    version: int = 1
    releases: list[dict[str, Any]] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    #: `{tag: ISO date}` for the tags in `tags` with no GitHub release,
    #: once they have been dated; a tag nothing could date is left out.
    #: None means not dated yet: a version-2 cache written before tags
    #: were dated with git. Its tags are still right, so it is dated and
    #: rewritten rather than fetched again.
    tag_dates: dict[str, str] | None = None
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    model_config = ConfigDict(extra='ignore')
