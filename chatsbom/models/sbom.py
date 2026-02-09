from dataclasses import dataclass
from dataclasses import field


@dataclass
class SBOMArtifact:
    """Represents a single artifact/dependency found in an SBOM."""
    id: str
    name: str
    version: str
    type: str
    purl: str | None = None
    found_by: str | None = None
    licenses: list[str] = field(default_factory=list)

    # Contextual fields (for DB ingestion)
    sbom_ref: str = ''
    sbom_commit_sha: str = ''
