import instructor
import structlog
from openai import OpenAI

from chatsbom.models.analysis import AnalysisMetadata
from chatsbom.models.analysis import RepoAnalysis
from chatsbom.models.analysis import RepoClassification
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('github_analysis_service')


class GitHubAnalysisService:
    """Service for analyzing GitHub repositories using LLMs with structured output."""

    def __init__(self, api_key: str, base_url: str = 'https://api.openai.com/v1', model: str = 'gpt-4o-mini', version: str = '0.5.4'):
        # instructor wraps OpenAI to handle tool calls and Pydantic validation automatically
        self.client = instructor.from_openai(
            OpenAI(api_key=api_key, base_url=base_url),
        )
        self.model = model
        self.version = version

        # Determine provider based on base_url
        url_lower = base_url.lower()
        if 'openai.com' in url_lower:
            self.provider = 'OpenAI'
        elif 'deepseek.com' in url_lower:
            self.provider = 'DeepSeek'
        elif any(x in url_lower for x in ['localhost', '127.0.0.1', '0.0.0.0']):
            self.provider = 'Local'
        else:
            self.provider = 'Other'

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

            # 2. Refined System Prompt (MECE-compliant)
            system_prompt = (
                '你是一位资深的开源软件架构师。请根据提供的 GitHub 仓库元数据，'
                '对其进行准确的分类和信息提取。分类定义（符合 MECE 原则）：\n'
                '- Web Application: 完整的、面向用户的 Web 业务产品（如 CMS、网盘、电商、后台管理系统）。\n'
                '- Web Framework: 用于构建 Web 应用的基础开发框架（如 Django, FastAPI, Spring Boot）。\n'
                '- General Library: 非 Web 框架的可复用代码库、SDK 或组件（如算法库、日志库、驱动）。\n'
                '- Dev/Security Tool: 辅助开发、测试、安全扫描或运维的工具类软件（如 CLI 工具、静态扫描、CI 脚本）。\n'
                '- Infrastructure: 数据库、消息队列、网关、代理、内核等底层中间件或基础设施。\n'
                '- Resource/Data: 非软件产品项目（如 Awesome 列表、教程、文档翻译、纯数据集、AI 权重）。\n'
                '- Other: 无法归入上述类别的剩余项目。\n\n'
                '严格遵守输出格式限制：\n'
                '- description: 包含 en (英文) 和 zh (中文) 的一句话核心功能描述。\n'
                '  - en: 限 20 个单词以内。\n'
                '  - zh: 限 30 个汉字以内。\n'
                '- tags: 仅当分类为 Web Application 时提取 3-5 个（涵盖业务领域、架构或关键技术），否则必须为空列表。'
            )

            user_content = (
                f"Repo: {owner}/{name}\n"
                f"Language: {repo.language or 'Unknown'}\n"
                f"Topics: {', '.join(repo.topics or [])}\n"
                f"Original Description: {repo.description or 'No description provided.'}\n"
                f"README Snippet:\n---\n{readme_snippet}\n---"
            )

            # 3. Structured Output Call with Instructor
            # Max retries handles Pydantic validation errors (e.g. LLM hallucinates a category)
            classification = self.client.chat.completions.create(
                model=self.model,
                response_model=RepoClassification,
                messages=[
                    {'role': 'system', 'content': system_prompt},
                    {'role': 'user', 'content': user_content},
                ],
                max_retries=3,
            )

            return RepoAnalysis(
                repo_id=repo.id,
                repo_name=name,
                owner=owner,
                original_description=repo.description,
                topics=repo.topics,
                language=repo.language,
                analysis=classification,
                metadata=AnalysisMetadata(
                    provider=self.provider,
                    model=self.model,
                    version=self.version,
                ),
            )

        except Exception as e:
            # Per requirement: log error and repo name, do not crash the whole queue
            logger.error(
                'Repository analysis failed',
                repo=f"{owner}/{name}",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            return None
