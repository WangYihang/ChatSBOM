import instructor
import structlog
from openai import OpenAI

from chatsbom.models.analysis import RepoAnalysis
from chatsbom.models.analysis import RepoClassification
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('github_analysis_service')


class GitHubAnalysisService:
    """Service for analyzing GitHub repositories using LLMs with structured output."""

    def __init__(self, api_key: str, base_url: str = 'https://api.openai.com/v1', model: str = 'gpt-4o-mini'):
        self.base_url = base_url
        # Determine instructor mode: Local models often perform better with MD_JSON or JSON
        # Standard OpenAI-compatible API usually works best with TOOLS.
        is_local = any(
            x in base_url.lower()
            for x in ['localhost', '127.0.0.1', 'ollama']
        )
        self.mode = instructor.Mode.MD_JSON if is_local else instructor.Mode.TOOLS

        self.client = instructor.from_openai(
            OpenAI(api_key=api_key, base_url=base_url),
            mode=self.mode,
        )
        self.model = model

    def analyze_repo(self, repo: Repository, github_service: GitHubService) -> RepoAnalysis | None:
        """Perform classification and info extraction for a repo, with retries."""
        owner = repo.owner
        name = repo.repo

        try:
            # 1. Use existing readme_snippet if provided, otherwise fetch it
            readme_snippet = getattr(repo, 'readme_snippet', None)
            if not readme_snippet:
                readme = github_service.get_readme(owner, name) or ''
                readme_snippet = readme[:1000]
            else:
                # Ensure snippet is capped at 1000 as requested
                readme_snippet = str(readme_snippet)[:1000]

            # 2. Refined System Prompt (More explicit for local models)
            system_prompt = (
                '你是一位资深的开源软件架构师。请根据提供的 GitHub 仓库元数据，'
                '将其归类为以下类别之一：\n'
                '- Web Application: 完整的 Web 业务产品（如 CMS、网盘、电商、后台管理）。\n'
                '- Web Framework: Web 基础框架（如 Django, FastAPI, Spring Boot）。\n'
                '- General Library: 非 Web 框架的通用组件、SDK 或库。\n'
                '- Dev/Security Tool: 开发者工具、静态扫描、CI 脚本、CLI 工具。\n'
                '- Infrastructure: 数据库、消息队列、内核等基础设施。\n'
                '- Tutorial/Course: 教学、Demo、面试题、图书源码。\n'
                '- Data/Resource: Awesome 列表、文档、数据集、模型权重。\n'
                '- Other: 其他。\n\n'
                '输出要求：\n'
                '1. description.en: 20 词以内英文描述。\n'
                '2. description.zh: 30 字以内中文描述。\n'
                '3. tags: 仅当类别为 Web Application 时提取 3-5 个技术标签，否则为空列表。\n'
                '4. reasoning: 简短的归类理由（中文）。\n'
                '直接输出 JSON，不要包含任何额外的对话或废话。'
            )

            user_content = (
                f"Repo: {owner}/{name}\n"
                f"Language: {repo.language or 'Unknown'}\n"
                f"Topics: {', '.join(repo.topics or [])}\n"
                f"Original Description: {repo.description or 'No description provided.'}\n"
                f"README Snippet:\n---\n{readme_snippet}\n---"
            )

            # 3. Structured Output Call with Instructor
            classification = self.client.chat.completions.create(
                model=self.model,
                response_model=RepoClassification,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_content},
                ],
                max_retries=3,
            )

            return RepoAnalysis.from_repository(repo, classification)

        except Exception as e:
            logger.error(
                'Repository analysis failed',
                repo=f"{owner}/{name}",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            return None
