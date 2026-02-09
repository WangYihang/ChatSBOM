from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator


class Artifact(BaseModel):
    """Represents a single artifact/dependency found in an SBOM."""
    id: str | None = ''
    name: str
    version: str
    type: str
    purl: str | None = ''
    found_by: str | None = Field(alias='foundBy', default='')
    licenses: list[str] = Field(default_factory=list)

    # Contextual fields (for DB ingestion)
    sbom_ref: str = ''
    sbom_commit_sha: str = ''

    model_config = ConfigDict(extra='ignore')

    @field_validator('licenses', mode='before')
    @classmethod
    def parse_licenses(cls, v: Any) -> list[str]:
        if not v:
            return []
        if isinstance(v, list):
            parsed = []
            for license_item in v:
                if isinstance(license_item, str):
                    parsed.append(license_item)
                elif isinstance(license_item, dict):
                    val = license_item.get('value') or license_item.get(
                        'spdxExpression',
                    ) or license_item.get('name')
                    if val:
                        parsed.append(val)
            return parsed
        return []
