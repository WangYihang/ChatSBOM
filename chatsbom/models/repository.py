from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.github_release import GitHubRelease

#: SPDX's words for "no licence id to give", which are not licence ids.
#:
#: GitHub sends `NOASSERTION` for a licence file it found and could not
#: identify — `{"key": "other", "name": "Other", "spdx_id":
#: "NOASSERTION"}` — and it is blanked rather than stored. The column is
#: published, in Parquet and D1 alike, as "SPDX licence id, or empty";
#: unknown is keyed empty wherever this project counts licences; and
#: package licences from GitHub's dependency graph already drop both
#: words (`_licenses_of`). Stored, it would be counted beside `MIT` as
#: though it were a licence.
#:
#: Nothing is lost by it: `license_name` keeps GitHub's "Other", which is
#: what tells a licence nobody could identify from no licence at all,
#: where both fields are empty.
NOT_A_LICENSE_ID: frozenset[str] = frozenset({'NOASSERTION', 'NONE'})


def _spdx_id(value: object) -> str | None:
    """The SPDX id in GitHub's `license` object, or the id itself."""
    if isinstance(value, Mapping):
        value = value.get('spdx_id')
    if not isinstance(value, str) or value in NOT_A_LICENSE_ID:
        return None
    return value


def _license_name(value: object) -> str | None:
    """The name in GitHub's `license` object, or the name itself."""
    if isinstance(value, Mapping):
        value = value.get('name')
    return value if isinstance(value, str) else None


def license_fields(record: Mapping[str, Any]) -> dict[str, str | None]:
    """`license_spdx_id` and `license_name`, as a raw record implies them.

    Read from GitHub's `license` object, which is how the REST API sends
    a licence — the repository endpoint and the search API alike — and
    which every ledger line written so far keeps as an extra.

    A field already set wins over the object. `None` does not count as
    set: until now it was the only value either field ever held, and
    `model_dump` writes it out — the repository cache is stored that
    way — so counting it would leave those records unlicensed for good.

    Empty when the record has no `license` key, leaving what it has
    alone. Shared with `db index`'s metadata overlay, which reads its
    ledger as raw JSON rather than through this model.
    """
    if 'license' not in record:
        return {}
    licence = record['license']
    spdx_id = record.get('license_spdx_id')
    name = record.get('license_name')
    return {
        'license_spdx_id': _spdx_id(licence if spdx_id is None else spdx_id),
        'license_name': _license_name(licence if name is None else name),
    }


class Repository(BaseModel):
    """Core repository data structure used across the pipeline."""
    id: int
    owner: str
    repo: str = Field(alias='name')
    stars: int = Field(alias='stargazers_count', default=0)
    url: str | None = Field(alias='html_url', default='')
    created_at: datetime | None = None
    updated_at: datetime | None = None
    pushed_at: datetime | None = None
    default_branch: str = 'main'
    description: str | None = ''
    topics: list[str] = Field(default_factory=list)

    language: str | None = None
    license_spdx_id: str | None = None
    license_name: str | None = None

    is_archived: bool = Field(alias='archived', default=False)
    is_fork: bool = Field(alias='fork', default=False)
    is_template: bool = Field(default=False)
    is_mirror: bool = Field(default=False)
    disk_usage: int = Field(alias='size', default=0)
    fork_count: int = Field(alias='forks_count', default=0)
    watchers_count: int = Field(default=0)

    has_releases: bool | None = None
    total_releases: int = 0
    latest_stable_release: GitHubRelease | None = None
    all_releases: list[GitHubRelease] | None = None
    download_target: DownloadTarget | None = None

    # Pipeline state
    local_content_path: str | None = None
    sbom_path: str | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra='allow',
    )

    @model_validator(mode='before')
    @classmethod
    def fill_from_github_keys(cls, data: Any) -> Any:
        """Fill the fields GitHub's payload implies but does not name.

        The API sends the licence as a `license` object and says
        "mirror" only as `mirror_url`. Nothing mapped either, so
        `extra='allow'` kept both in `model_extra`, and every repository
        was indexed unlicensed and not a mirror.
        """
        if not isinstance(data, Mapping):
            return data
        filled = {**data, **license_fields(data)}
        # A `mirror_url` makes a mirror even beside `is_mirror: false`.
        # Nothing ever set the flag, so every ledger line written so far
        # says false, mirrors included, next to the URL saying otherwise.
        if data.get('mirror_url') is not None:
            filled['is_mirror'] = True
        return filled

    @field_validator('owner', mode='before')
    @classmethod
    def extract_owner(cls, v: Any) -> str:
        if isinstance(v, dict):
            return v.get('login', '')
        return str(v)

    @field_validator('created_at', 'updated_at', 'pushed_at', mode='before')
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

    # The same readings `license_fields` makes, so a value stated
    # outright is held to the same rule as one read out of the object.
    @field_validator('license_spdx_id', mode='before')
    @classmethod
    def extract_license_id(cls, v: Any) -> str | None:
        return _spdx_id(v)

    @field_validator('license_name', mode='before')
    @classmethod
    def extract_license_name(cls, v: Any) -> str | None:
        return _license_name(v)
