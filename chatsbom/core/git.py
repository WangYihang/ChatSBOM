"""git, as the collector and the research tools run it (#47, #171).

What both ask of git, and how: the environment every git runs in, with
no prompt for credentials, no user or system config, and the token, if
there is one, as a header in git config the environment carries, never
on a command line (`GIT_QUIET_ENV`, `git_auth_env`); a git run to its
end, for the research tools' clones (`run_git`); and what the answers
say, read the one way: `git ls-remote --symref` (`parse_ls_remote`,
`RemoteRefs`, `tags_of`, `ref_commit`, `head_commit`) and the tag
listing the release stage dates tags by (`parse_tag_listing`).

The collector runs git as children it can stop, in a process group each,
killed with whatever they started when a collection is given up
(`collector/gitremote.py`). The time limits are here, for both.
"""
from __future__ import annotations

import base64
import os
import re
import subprocess
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field

TAG_REF_PREFIX = 'refs/tags/'

#: Wall-clock limit on one repository's tag fetch. The largest tag sets
#: in the corpus (several thousand tags) fetch in well under a minute;
#: past this the stage falls back to the capped API lookups.
TAG_FETCH_TIMEOUT = 300

#: Wall-clock limit on `git ls-remote`, one round trip.
LS_REMOTE_TIMEOUT = 30

#: Wall-clock limit on each of the tree stage's clone and fetch, of one
#: commit's trees and no file contents.
TREE_FETCH_TIMEOUT = 300

#: Wall-clock limit on a git that asks nothing of the network.
LOCAL_TIMEOUT = 60

#: What the tag fetch leaves out, tried in order. `tree:0` fetches
#: commits and tags only. A tag of a tree (Linux has `v2.6.11-tree`)
#: needs that tree, which a server may refuse to send under `tree:0`
#: ("remote did not send all necessary objects"); `blob:none` sends
#: trees and still no file contents.
TAG_FETCH_FILTERS = ('tree:0', 'blob:none')

#: No prompt for credentials (a repository gone private would hang the
#: worker on one), and no user or system config: this is a scratch
#: repository, and a `url.<x>.insteadOf` there could send it elsewhere.
GIT_QUIET_ENV = {
    'GIT_TERMINAL_PROMPT': '0',
    'GIT_CONFIG_NOSYSTEM': '1',
    'GIT_CONFIG_GLOBAL': os.devnull,
}

#: One line per tag, NUL-separated: name, the object the ref names and
#: its type, the object that dereferences to (for an annotated tag) and
#: its type, then the committer date of each and the tagger date.
_TAG_FIELDS = (
    '%(refname:strip=2)',
    '%(objectname)', '%(objecttype)',
    '%(*objectname)', '%(*objecttype)',
    '%(committerdate:iso-strict)', '%(*committerdate:iso-strict)',
    '%(creatordate:iso-strict)',
)
TAG_FORMAT = '%00'.join(_TAG_FIELDS)


@dataclass(frozen=True)
class RemoteRefs:
    """What `git ls-remote --symref` says of a repository."""
    #: Full and short ref names, and `HEAD`, each to its commit.
    refs: dict[str, str] = field(default_factory=dict)
    #: The branch HEAD points at; '' when not said.
    head: str = ''
    cached: bool = False
    #: Why git could not list the refs; '' when it listed them. A
    #: listing that failed is empty, as one of an empty repository is.
    error: str = ''


def parse_ls_remote(
    output: str, short_name: Callable[[str], str | None],
) -> tuple[dict[str, str], str]:
    """`(refs, head_branch)` from `git ls-remote --symref <url>`.

    Annotated tags (`refs/tags/v1^{}`) take precedence over the tag
    object itself, so a tag resolves to its commit. The symref line,
    `ref: refs/heads/<branch>\tHEAD`, names the default branch; it is
    not a ref.
    """
    refs: dict[str, str] = {}
    head = ''
    for line in output.splitlines():
        if not line.strip():
            continue
        if line.startswith('ref: '):
            target, _, name = line[len('ref: '):].partition('\t')
            if name.strip() == 'HEAD' and target.startswith('refs/heads/'):
                head = target.removeprefix('refs/heads/').strip()
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        sha, ref_full = parts
        if ref_full.endswith('^{}'):
            base_ref = ref_full[:-3]
            refs[base_ref] = sha
            short = short_name(base_ref)
            if short:
                refs[short] = sha
        elif ref_full not in refs:
            refs[ref_full] = sha
            short = short_name(ref_full)
            if short:
                refs[short] = sha
    return refs, head


def short_name(ref_full: str) -> str | None:
    """A branch's or a tag's name without `refs/heads/` or `refs/tags/`,
    as a listing keys it beside its full name; None for any other ref.

    Short and full names share one dict, and git allows a branch called
    `refs/tags/v1`: its short name would pose as that tag, and, listed
    before it, take the real tag's place.
    """
    if ref_full.startswith('refs/tags/'):
        short = ref_full[10:]
    elif ref_full.startswith('refs/heads/'):
        short = ref_full[11:]
    else:
        return None
    if short.startswith('refs/'):
        return None
    return short


def tags_of(refs: Mapping[str, str]) -> dict[str, str]:
    """A listing's tags, by name, each at the commit it points to.

    Read from the full `refs/tags/*` names only. The short names beside
    them are shared with branches, and `HEAD` has no prefix at all:
    taken as tags, they made releases of branches, dated by their head
    commit and so newer than any real release.
    """
    return {
        ref.removeprefix(TAG_REF_PREFIX): sha
        for ref, sha in refs.items()
        if ref.startswith(TAG_REF_PREFIX)
    }


def ref_commit(refs: Mapping[str, str], ref: str) -> str | None:
    """The commit `ref` names in a listing: exactly, then as a tag, then
    as a branch."""
    if ref in refs:
        return refs[ref]
    for prefix in ['refs/tags/', 'refs/heads/']:
        if (prefix + ref) in refs:
            return refs[prefix + ref]
    return None


def head_commit(listing: RemoteRefs) -> tuple[str, str | None]:
    """The default branch, as HEAD names it ('' when not said), and its
    commit: None when the remote has no HEAD, an empty repository."""
    sha = listing.refs.get('HEAD')
    if listing.head:
        sha = listing.refs.get(f'refs/heads/{listing.head}', sha)
    return listing.head, sha


@dataclass(frozen=True)
class TagDate:
    """What git says about one tag: the commit it names, and its date."""
    #: The commit an annotated tag points to, or a lightweight tag's own
    #: object: what `ls-remote` lists as the tag's `^{}` or bare sha.
    sha: str
    #: ISO 8601, with its offset; '' when git has no date for it.
    date: str


#: `GIT_CONFIG_COUNT` as git reads it, with C's `strtoul`: a number, with
#: any space and a sign before it, and nothing after it.
_GIT_CONFIG_COUNT = re.compile(r'[ \t\n\v\f\r]*\+?([0-9]+)')

#: The most entries git reads from its environment: an `int`'s most.
_MOST_GIT_CONFIG_ENTRIES = 2**31 - 1


def _git_config_count(count: str | None) -> int:
    """How many entries of git config `count`, a `GIT_CONFIG_COUNT`,
    says the environment holds, read as git reads it: 0 for none, and
    for a count git refuses, a word or a negative number among them."""
    match = _GIT_CONFIG_COUNT.fullmatch(count or '')
    if match is None:
        return 0
    entries = int(match[1])
    return entries if entries < _MOST_GIT_CONFIG_ENTRIES else 0


def git_auth_env(
    token: str | None, environ: Mapping[str, str] = os.environ,
) -> dict[str, str]:
    """git config, as environment variables, that authenticates to GitHub.

    Environment rather than `-c` or a URL: both of those are on the
    command line, which any user of the machine can read in `ps`.

    To put over `environ`, the environment git is started with, and
    after the entries of git config it holds already: `GIT_CONFIG_COUNT`
    of them, each a `GIT_CONFIG_KEY_<n>` and a `GIT_CONFIG_VALUE_<n>`,
    which may set a proxy, a CA bundle or a URL rewrite for every git on
    the machine. The token was entry 0 of a count of 1, which replaced
    the first and dropped the rest (#113). A count git refuses, over
    which it runs no command at all, is replaced still: git runs, with
    the token's entry alone.
    """
    if not token:
        return {}
    index = _git_config_count(environ.get('GIT_CONFIG_COUNT'))
    basic = base64.b64encode(f'x-access-token:{token}'.encode()).decode()
    return {
        'GIT_CONFIG_COUNT': str(index + 1),
        f'GIT_CONFIG_KEY_{index}': 'http.https://github.com/.extraheader',
        f'GIT_CONFIG_VALUE_{index}': f'Authorization: Basic {basic}',
    }


def run_git(
    args: list[str], *, env: dict[str, str], timeout: float = LOCAL_TIMEOUT,
) -> str:
    """Run git to its end; its stdout, or raise `CalledProcessError` or
    `TimeoutExpired`."""
    return subprocess.run(
        ['git', *args], env=env, timeout=timeout, check=True,
        capture_output=True, text=True, stdin=subprocess.DEVNULL,
    ).stdout


def error_text(error: BaseException) -> str:
    """What a git that failed said, and how it failed."""
    stderr = getattr(error, 'stderr', None)
    return f'{stderr.strip()} ({error})' if stderr else str(error)


def parse_tag_listing(listing: str) -> dict[str, TagDate]:
    """`{tag: TagDate}` from `git for-each-ref --format=<TAG_FORMAT>`."""
    tags: dict[str, TagDate] = {}
    for line in listing.splitlines():
        fields = line.split('\0')
        if len(fields) != len(_TAG_FIELDS):
            continue
        name, sha, kind, peeled, peeled_kind, date, peeled_date, created = fields
        if kind == 'commit':
            tags[name] = TagDate(sha=sha, date=date)
        elif kind == 'tag' and peeled_kind == 'commit':
            tags[name] = TagDate(sha=peeled, date=peeled_date)
        else:
            # A tag of a tag, a tree or a blob. `ls-remote` peels to the
            # end, so its sha will not match this one and the tag is
            # dated by the fallback; the tagger date is better than none
            # if it is ever used.
            tags[name] = TagDate(sha=peeled or sha, date=created)
    return tags
