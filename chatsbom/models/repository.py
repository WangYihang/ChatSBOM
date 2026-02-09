from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator

from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.github_release import GitHubRelease


class Repository(BaseModel):
    """Core repository data structure used across the pipeline."""
    id: int
    full_name: str
    stars: int = Field(alias='stargazers_count', default=0)
    url: str | None = Field(alias='html_url', default='')
    created_at: datetime | None = None
    default_branch: str = 'main'
    description: str | None = ''
    topics: list[str] = Field(default_factory=list)

    language: str | None = None
    license_spdx_id: str | None = None
    license_name: str | None = None

    has_releases: bool | None = None
    latest_stable_release: GitHubRelease | None = None
    all_releases: list[GitHubRelease] | None = None
    download_target: DownloadTarget | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra='allow',
    )

    @field_validator('created_at', mode='before')
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

    @field_validator('license_spdx_id', mode='before')
    @classmethod
    def extract_license_id(cls, v: Any) -> str | None:
        if isinstance(v, dict):
            return v.get('spdx_id')
        return v

    @field_validator('license_name', mode='before')
    @classmethod
    def extract_license_name(cls, v: Any) -> str | None:
        if isinstance(v, dict):
            return v.get('name')
        return v
