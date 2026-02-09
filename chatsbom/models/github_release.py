from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator


class GitHubRelease(BaseModel):
    id: int
    tag_name: str
    name: str | None = ''
    published_at: datetime | None = None
    target_commitish: str | None = ''
    is_prerelease: bool = False
    is_draft: bool = False
    created_at: datetime | None = None
    assets: list[dict] = []

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
