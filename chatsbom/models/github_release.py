from datetime import datetime
from datetime import timezone
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


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
