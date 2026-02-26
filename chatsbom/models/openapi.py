from dataclasses import dataclass


@dataclass
class OpenApiCandidate:
    language: str
    framework: str
    framework_version: str
    owner: str
    repo: str
    stars: int
    default_branch: str
    latest_release: str
    commit_sha: str
    url: str
    openapi_file: str  # Path if exists, else empty
    openapi_url: str   # URL if exists, else empty
    matched_dependencies: str = ''
    has_openapi_file: bool = False
    has_openapi_deps: bool = False
    generation_command: str = ''

    def to_csv_row(self) -> list[str]:
        return [
            self.language, self.framework, self.framework_version,
            self.owner, self.repo, str(self.stars),
            self.default_branch, self.latest_release, self.commit_sha,
            self.url, self.openapi_file, self.openapi_url,
            self.matched_dependencies,
            str(self.has_openapi_file),
            str(self.has_openapi_deps),
            self.generation_command,
        ]


@dataclass
class FrameworkStats:
    framework: str
    language: str
    total_projects: int
    matched_projects: int
    count_file_only: int = 0
    count_deps_only: int = 0
    count_both: int = 0

    @property
    def percentage(self) -> float:
        if self.total_projects == 0:
            return 0.0
        return (self.matched_projects / self.total_projects) * 100


@dataclass
class OpenApiCandidateResult:
    candidates: list[OpenApiCandidate]
    stats: list[FrameworkStats]
