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
    is_prerelease: bool = False
    is_draft: bool = False
    created_at: datetime | None = None
    assets: list[dict] = []
    source: str = 'github_release'

    model_config = ConfigDict(extra='ignore')

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


class ReleaseCache(BaseModel):
    """Formal model for cached release and tag data."""
    releases: list[dict[str, Any]] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    model_config = ConfigDict(extra='ignore')
