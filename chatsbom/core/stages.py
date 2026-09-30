"""The stages a repository is collected in, and each one's version.

Moved here from the ledger, `core/ledger.py`, which went with the old
pipeline (#171): the collector's chain is these five, in its order
(`collector/due.CHAIN`), and a decision is stamped with the version of
the stage that made it (`core/decisions.py`).
"""
from enum import Enum


class Stage(str, Enum):
    """The stages a repository's push is collected in, each after the one
    before it: a stage is due when its output for its key is not in the
    store (#100). The ledger's others, the change detector, the
    dependency graph, the resolver and the index, are the collector's
    sweep and graph, the resolver's service and the index pass now."""

    RELEASE = 'release'
    COMMIT = 'commit'
    TREE = 'tree'
    CONTENT = 'content'
    SBOM = 'sbom'

    def __str__(self) -> str:
        return self.value


#: The code version of each stage (#100). What a stage wrote at an older
#: version than this code's is due again, with no push and no manual
#: reset, where what it wrote says which version wrote it: the release
#: and commit decisions (`core/decisions.py`) and a content root's stamp
#: (`collector/content.py`). Bumping a stage's number is how a change to
#: what it *does* reaches the corpus. Content and SBOM are at 2 since
#: manifests are discovered from the tree at any depth and of every
#: ecosystem, and resolved and scanned per directory (PR C of #55).
#: Release is at 2 since only `refs/tags/*` are tags and tags are dated
#: with git (PR F of #55): every stored release history counted
#: branches as tags, so every repository's latest release is chosen
#: again. Commit follows through its input key, and only where the tag
#: chosen changed. Content is at 3 since discovery also takes podspecs
#: and the `buildSrc` sources a Gradle build's constants are in (#55
#: pilot): a content root filled at 2 lacks them. Its SBOM follows only
#: where the files changed.
STAGE_VERSION: dict[Stage, int] = {
    Stage.RELEASE: 2,
    Stage.COMMIT: 1,
    Stage.TREE: 1,
    Stage.CONTENT: 3,
    Stage.SBOM: 2,
}
