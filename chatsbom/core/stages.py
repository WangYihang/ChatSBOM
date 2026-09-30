"""The stages a repository is collected in, and each one's version.

Moved here from the ledger (`core/ledger.py`), which goes with the old
pipeline (#171). The collector's chain is five of them, in its order
(`collector/due.CHAIN`), and a decision is stamped with the version of
the stage that made it (`core/decisions.py`).
"""
from enum import Enum


class Stage(str, Enum):
    """Pipeline stages that advance independently per repository.

    `REPO` is different in kind from the rest. It is the *change
    detector*: fetching it is how we learn a repository was pushed. So it
    is due on a clock, not on a comparison against the push it would
    itself discover. The others are *derived* — due only once a newly
    observed push overtakes their watermark.
    """

    REPO = 'repo'
    RELEASE = 'release'
    COMMIT = 'commit'
    TREE = 'tree'
    CONTENT = 'content'
    DEPGRAPH = 'depgraph'
    LOCK = 'lock'
    SBOM = 'sbom'
    INDEX = 'index'

    def __str__(self) -> str:
        return self.value


#: The code version of each stage. A `stage_state` row recorded by an
#: older version is due again, with no push and no manual reset: bumping
#: a stage's number is how a change to what it *does* reaches the
#: corpus. The dependency graph is at 2 since it became its own stage.
#: Content, lock and SBOM are at 2 since manifests are discovered from
#: the tree at any depth and of every ecosystem, and resolved and
#: scanned per directory (PR C of #55): every repository's content root
#: is due to be filled out, and its SBOM regenerated from it. Release
#: is at 2 since only `refs/tags/*` are tags and tags are dated with git
#: (PR F of #55): every stored release history counted branches as
#: tags, so every repository's latest release is chosen again. Commit
#: follows through its input key, and only where the tag chosen changed.
#: Content is at 3 since discovery also takes podspecs and the `buildSrc`
#: sources a Gradle build's constants are in (#55 pilot): a content root
#: filled at 2 lacks them. Its SBOM follows only where the files changed.
STAGE_VERSION: dict[Stage, int] = {
    Stage.REPO: 1,
    Stage.RELEASE: 2,
    Stage.COMMIT: 1,
    Stage.TREE: 1,
    Stage.CONTENT: 3,
    Stage.LOCK: 2,
    Stage.SBOM: 2,
    Stage.DEPGRAPH: 2,
    Stage.INDEX: 1,
}
