from typing import Literal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import ValidationInfo


class RepoClassification(BaseModel):
    """Structured output for repository classification."""
    category: Literal[
        'Web Application',
        'Web Framework',
        'Library/Component',
        'Dev/Security Tool',
        'Infrastructure',
        'Other',
    ] = Field(..., description='Project category')

    description_en: str = Field(
        ..., description='One-sentence core function description in English, limit 20 words',
    )
    description_zh: str = Field(
        ..., description='One-sentence core function description in Chinese, limit 30 characters',
    )

    tags: list[str] = Field(
        default_factory=list,
        description='Extract 3-5 tags ONLY when category is Web Application (business area, architecture, or key technology)',
    )

    reasoning: str = Field(
        ..., description='Short explanation for the classification decision (in Chinese)',
    )

    @field_validator('tags', mode='after')
    @classmethod
    def validate_tags_by_category(cls, v: list[str], info: ValidationInfo) -> list[str]:
        # If category is not Web Application, force empty tags
        category = info.data.get('category')
        if category != 'Web Application':
            return []
        # If it is Web Application, ensure count is reasonable
        return v[:5]


class RepoAnalysis(BaseModel):
    """The full analysis result including original data and LLM classification."""
    repo_name: str
    owner: str
    description: str | None = ''
    topics: list[str] = []
    language: str | None = ''
    analysis: RepoClassification
