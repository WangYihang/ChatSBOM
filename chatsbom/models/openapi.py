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
    openapi_file: str
    openapi_url: str

    def to_csv_row(self) -> list[str]:
        return [
            self.language, self.framework, self.framework_version,
            self.owner, self.repo, str(self.stars),
            self.default_branch, self.latest_release, self.commit_sha,
            self.url, self.openapi_file, self.openapi_url,
        ]


@dataclass
class FrameworkStats:
    framework: str
    language: str
    total_projects: int
    matched_projects: int

    @property
    def percentage(self) -> float:
        if self.total_projects == 0:
            return 0.0
        return (self.matched_projects / self.total_projects) * 100


@dataclass
class OpenApiCandidateResult:
    candidates: list[OpenApiCandidate]
    stats: list[FrameworkStats]
