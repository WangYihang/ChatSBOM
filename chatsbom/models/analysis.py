from datetime import datetime
from datetime import timezone
from enum import Enum

from pydantic import BaseModel
from pydantic import computed_field
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_validator
from pydantic import ValidationInfo

from chatsbom.models.repository import Repository


class RepoCategory(str, Enum):
    """MECE-compliant categories for all GitHub repositories."""

    # End-user products (CMS, SaaS, Dashboards)
    WEB_APP = 'Web Application'
    # Development foundations (Django, FastAPI, Spring)
    WEB_FRAMEWORK = 'Web Framework'
    # Reusable code/SDKs (non-web-framework)
    GENERAL_LIBRARY = 'General Library'
    DEV_TOOL = 'Dev/Security Tool'     # CLI tools, scanners, compilers, CI/CD
    INFRASTRUCTURE = 'Infrastructure'  # DBs, OS, Middleware, Networking
    TUTORIAL = 'Tutorial/Course'       # Learning materials, book samples, demo projects
    # Awesome lists, datasets, docs, static resources
    DATA_RESOURCE = 'Data/Resource'
    OTHER = 'Other'


class LocalizedDescription(BaseModel):
    """Standardized multi-language description structure."""

    en: str = Field(
        ...,
        description='One-sentence core function description in English, limit 20 words',
    )
    zh: str = Field(
        ...,
        description='One-sentence core function description in Chinese, limit 30 characters',
    )


class RepoClassification(BaseModel):
    """Structured output for repository classification."""

    category: RepoCategory = Field(..., description='Project category')
    description: LocalizedDescription = Field(
        ..., description='Project descriptions in English and Chinese',
    )
    tags: list[str] = Field(
        default_factory=list,
        description='Extract 3-5 tags ONLY when category is Web Application (business area, architecture, or key technology)',
    )
    reasoning: str = Field(
        ..., description='Short explanation for the classification decision (in Chinese)',
    )

    @computed_field
    def is_web_application(self) -> bool:
        """Helper to check if the repo is a Web Application."""
        return self.category == RepoCategory.WEB_APP

    @computed_field
    def is_web_framework(self) -> bool:
        """Helper to check if the repo is a Web Framework."""
        return self.category == RepoCategory.WEB_FRAMEWORK

    @field_validator('tags', mode='after')
    @classmethod
    def validate_tags_by_category(cls, v: list[str], info: ValidationInfo) -> list[str]:
        # If category is not Web Application, force empty tags
        category = info.data.get('category')
        if category != RepoCategory.WEB_APP:
            return []
        # If it is Web Application, ensure count is reasonable
        return v[:5]


class RepoAnalysis(BaseModel):
    """The full analysis result including original data and LLM classification."""

    # Repository metadata
    repo_id: int | None = Field(
        None, description='Unique GitHub ID of the repository',
    )
    repo_name: str = Field(..., description='Repository name')
    owner: str = Field(..., description='Repository owner login')
    default_branch: str = Field('main', description='Default branch name')
    latest_release: str | None = Field(
        None, description='Latest stable release tag',
    )
    original_description: str | None = Field(
        '', description='Original GitHub description',
    )
    topics: list[str] = Field(
        default_factory=list,
        description='GitHub topics',
    )
    language: str | None = Field(
        '', description='Primary programming language',
    )

    # Analysis result
    analysis: RepoClassification = Field(
        ..., description='LLM classification and analysis results',
    )

    # Metadata about the analysis process
    analyzed_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description='Timestamp when the analysis was performed',
    )

    model_config = ConfigDict(
        populate_by_name=True,
        extra='allow',
    )

    @classmethod
    def from_repository(cls, repo: Repository, analysis: RepoClassification) -> 'RepoAnalysis':
        """Helper to create RepoAnalysis from a Repository instance."""
        latest_tag = None
        if repo.latest_stable_release:
            latest_tag = repo.latest_stable_release.tag_name

        return cls(
            repo_id=repo.id,
            repo_name=repo.repo,
            owner=repo.owner,
            default_branch=repo.default_branch,
            latest_release=latest_tag,
            original_description=repo.description,
            topics=repo.topics,
            language=repo.language,
            analysis=analysis,
        )

    def to_flat_dict(self) -> dict:
        """Flatten the nested analysis structure for easier export (e.g., to CSV)."""
        return {
            'id': self.repo_id,
            'owner': self.owner,
            'repo': self.repo_name,
            'default_branch': self.default_branch,
            'latest_release': self.latest_release or '',
            'description': self.original_description or '',
            'language': self.language or '',
            'topics': ', '.join(self.topics) if self.topics else '',
            'category': self.analysis.category.value,
            'description_en': self.analysis.description.en,
            'description_zh': self.analysis.description.zh,
            'tags': ', '.join(self.analysis.tags) if self.analysis.tags else '',
            'reasoning': self.analysis.reasoning,
            'is_web_application': self.analysis.is_web_application,
            'is_web_framework': self.analysis.is_web_framework,
            'analyzed_at': self.analyzed_at.isoformat(),
        }
