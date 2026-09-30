"""The repository-keyed layout, and the language-keyed one it replaced.

Every stage artefact used to live under the language list its
repository was collected from, its owner and name as GitHub spelled
them then, and the ref that was resolved:

    <stage>/<language>/<owner>/<repo>/<ref>/<sha>/...

Now it lives under the repository's numeric id and the commit (#55,
owner decision D3):

    <stage>/<repository_id>/<sha>/...

An id does not move when a repository is renamed or transferred, a
repository no longer needs a language to have a path, and two refs at
one commit are one scan. `core/config.py` builds the new paths; this
module recognises the old ones, maps them onto the new, and says how a
stored path is spelled in `raw_documents`.

It is used by `data migrate-layout`, which moves the files, and by the
readers of paths recorded before the move (the per-language JSONL lists
and the records in `raw_documents`), which translate what they read
rather than trust it.

The release and commit stages make no scan. What each produces is a
decision, kept under the key it was made for (#147, owner decision Q3
on #100), beside the release lists they name:

    03-github-release/<repository_id>/<P>/release@<version>.json
    03-github-release/<repository_id>/releases/<sha256>.json
    04-github-commit/<repository_id>/<K>/commit@<version>.json
    04-github-commit/<repository_id>/<K>/<P>/commit@<version>.json

`P` is a push and `K` what the commit stage resolved for it: its tag,
or the default branch's head when it has no release. `<K>/` holds the
key's first resolution, and `<K>/<P>/` a later one that differs, made
for the push `P`: a tag moved, or gone and the default branch's head
taken in its place. Their names are
spelled here (`push_name`, `key_name`) so that they sort, parse back
(`push_of`, `key_of`), and are the same name on every file system the
store is kept on: only lower-case ASCII letters, digits and `._-@%`, no
capital a file system that ignores case would fold into another name,
no trailing dot Windows would drop, and at most `MAX_NAME` bytes.
`core/decisions.py` writes and reads the files.
"""
from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from pathlib import PurePosixPath

from chatsbom.core.instants import stated
from chatsbom.core.instants import utc
from chatsbom.models.language import Language

TREE_ROOT = '05-github-tree'
CONTENT_ROOT = '06-github-content'
SBOM_ROOT = '07-sbom'
DEPGRAPH_ROOT = '09-github-depgraph'
LOCK_ROOT = '10-generated-lock'

#: Stage roots holding one directory per scan: `<lang>/<o>/<r>/<ref>/<sha>`
#: before, `<id>/<sha>` after.
SCAN_ROOTS: tuple[str, ...] = (TREE_ROOT, CONTENT_ROOT, SBOM_ROOT)
#: Every stage root whose paths carry a repository.
STAGE_ROOTS: tuple[str, ...] = (*SCAN_ROOTS, DEPGRAPH_ROOT, LOCK_ROOT)

#: The language directories the old layout used: the `Language` values,
#: and `node`, an older spelling some lists were collected under.
LEGACY_LANGUAGES: frozenset[str] = frozenset(
    {str(language) for language in Language} | {'node'},
)

#: A legacy dependency graph's new home, beside the kept fetches.
LEGACY_DEPGRAPH_DIR = 'legacy'
DEPGRAPH_DOCUMENT = 'sbom.spdx.json'

RELEASE_ROOT = '03-github-release'
COMMIT_ROOT = '04-github-commit'
#: The stage roots holding one directory per decision rather than per
#: scan: `<id>/<P>` and `<id>/<K>`.
DECISION_ROOTS: tuple[str, ...] = (RELEASE_ROOT, COMMIT_ROOT)
#: A repository's release lists, beside its release decisions: never a
#: push's name, which is all digits but a `T` and a `Z`.
RELEASE_LISTS = 'releases'

_SHA = re.compile(r'^[0-9a-f]{40}$')


def is_sha(value: str) -> bool:
    return bool(_SHA.match(value or ''))


# -- the decisions' names ------------------------------------------------

#: The most bytes a key's name takes. A name may hold 255 on ext4, XFS,
#: APFS and NTFS, but 143 under eCryptfs; a tag whose name would take
#: more is named by its digest instead (`key_name`).
MAX_NAME = 128

#: A push's name: its instant in UTC, as `09-github-depgraph` names a
#: fetch. Fixed width, so names sort as the instants do.
_PUSH = '%Y%m%dT%H%M%SZ'
_PUSH_NAME = re.compile(r'^\d{8}T\d{6}Z$')
#: A push as a decision states it, as GitHub's `pushed_at` spells it.
_PUSH_TEXT = '%Y-%m-%dT%H:%M:%SZ'

#: The bytes a tag keeps of itself in its name; any other is `%xx`, in
#: lower-case hex, a capital letter included.
_KEPT = frozenset(b'abcdefghijklmnopqrstuvwxyz0123456789._-@')
_HEX = frozenset('0123456789abcdef')

TAG = 'tag'
HEAD = 'head'


def push_instant(value: datetime | str | None) -> datetime | None:
    """The push `value` states, in UTC to the second; None for none.

    A time with no zone is UTC, as everywhere in this project
    (`core/instants.py`), and GitHub states the second and no finer.
    """
    if isinstance(value, datetime):
        return utc(value)
    if not value:
        return None
    return stated(str(value))


def push_text(value: datetime | str | None) -> str | None:
    """A push as a decision states it: `2026-09-29T12:28:14Z`."""
    instant = push_instant(value)
    return instant.strftime(_PUSH_TEXT) if instant is not None else None


def push_name(value: datetime | str | None) -> str | None:
    """`P`, a push's name: `20260929T122814Z`. None for no push."""
    instant = push_instant(value)
    return instant.strftime(_PUSH) if instant is not None else None


def push_of(name: str) -> datetime | None:
    """The push a name is, or None when it is not a push's name."""
    if not _PUSH_NAME.match(name or ''):
        return None
    try:
        return datetime.strptime(name, _PUSH).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


@dataclass(frozen=True)
class CommitKey:
    """`K`: what a commit decision is for. `tag:T`, the release a push's
    release decision chose; or `head:P`, the default branch's head at the
    push P, when it chose none (#100, §2)."""

    kind: str
    #: The tag, or the push as `push_text` states it.
    value: str

    @classmethod
    def tag(cls, tag: str) -> CommitKey:
        return cls(TAG, tag)

    @classmethod
    def head(cls, pushed_at: datetime | str | None) -> CommitKey:
        text = push_text(pushed_at)
        if text is None:
            raise ValueError(f'no push to key a head by: {pushed_at!r}')
        return cls(HEAD, text)

    @classmethod
    def parse(cls, text: str) -> CommitKey | None:
        """`tag:T` or `head:P`, as a decision states its key."""
        kind, colon, value = str(text).partition(':')
        if not colon:
            return None
        if kind == TAG:
            return cls.tag(value)
        if kind == HEAD and push_instant(value) is not None:
            return cls.head(value)
        return None

    def __str__(self) -> str:
        return f'{self.kind}:{self.value}'


def key_name(key: CommitKey) -> str:
    """`K`'s name: `head-<P>`, or `tag-<T>` with every byte of the tag
    outside `a-z 0-9 . _ - @` as `%xx`.

    So `v1.2.3` is `tag-v1.2.3`, `release/1.4.0` `tag-release%2f1.4.0`
    and `V1.0` `tag-%561.0`, which a file system that ignores case keeps
    apart from `tag-v1.0`. A name longer than `MAX_NAME` is `tag~` and the
    tag's sha256; its key is read from the file.
    """
    if key.kind == HEAD:
        return f'{HEAD}-{push_name(key.value)}'
    raw = key.value.encode('utf-8')
    spelled = ''.join(chr(b) if b in _KEPT else f'%{b:02x}' for b in raw)
    if spelled.endswith('.'):
        spelled = spelled[:-1] + '%2e'
    name = f'{TAG}-{spelled}'
    if len(name) > MAX_NAME:
        return f'{TAG}~{hashlib.sha256(raw).hexdigest()}'
    return name


def key_of(name: str) -> CommitKey | None:
    """The key a name is, or None: a name `key_name` would not write, and
    a tag named by its digest, whose key is in its file."""
    kind, dash, rest = (name or '').partition('-')
    if not dash:
        return None
    if kind == HEAD:
        instant = push_of(rest)
        return CommitKey.head(instant) if instant is not None else None
    if kind != TAG:
        return None
    raw = _unspelled(rest)
    if raw is None:
        return None
    try:
        key = CommitKey.tag(raw.decode('utf-8'))
    except UnicodeDecodeError:
        return None
    # One key, one name: `%61` is not a second way to write `a`.
    return key if key_name(key) == name else None


def _unspelled(text: str) -> bytes | None:
    """The bytes `key_name` spelled as `text`, or None if it did not."""
    out = bytearray()
    at = 0
    while at < len(text):
        character = text[at]
        if character == '%':
            pair = text[at + 1:at + 3]
            if len(pair) != 2 or not set(pair) <= _HEX:
                return None
            out.append(int(pair, 16))
            at += 3
            continue
        code = ord(character)
        if code >= 128 or code not in _KEPT:
            return None
        out.append(code)
        at += 1
    return bytes(out)


def decision_file(stage: str, version: int) -> str:
    """A decision's file: `release@2.json`, the stage and its version."""
    return f'{stage}@{int(version)}.json'


def decision_version(name: str, stage: str) -> int | None:
    """The version a decision file of `stage` was written by, or None."""
    match = re.fullmatch(re.escape(stage) + r'@([1-9][0-9]*)\.json', name)
    return int(match[1]) if match else None


def scan_dirs(
    root: Path, repos: Iterable[int] | None = None,
) -> Iterator[tuple[int, str, Path]]:
    """`(repository_id, sha, directory)` of every `<id>/<sha>` scan
    below a stage root, in id then sha order; only `repos` if given.

    Anything else under the root (a tree not yet migrated, `_migration`)
    is not a scan and is passed over.
    """
    wanted = None if repos is None else {int(i) for i in repos}
    try:
        children = [c for c in root.iterdir() if c.name.isdigit()]
    except OSError:
        return
    for child in sorted(children, key=lambda c: int(c.name)):
        repository_id = int(child.name)
        if wanted is not None and repository_id not in wanted:
            continue
        try:
            scans = sorted(
                c for c in child.iterdir() if is_sha(c.name) and c.is_dir()
            )
        except OSError:
            continue
        for scan in scans:
            yield repository_id, scan.name, scan


def _first_sha(parts: tuple[str, ...], start: int) -> int | None:
    for index in range(start, len(parts)):
        if is_sha(parts[index]):
            return index
    return None


@dataclass(frozen=True)
class LegacyPath:
    """A path in the language-keyed layout, taken apart."""

    #: Everything before the stage root, e.g. `data`.
    prefix: tuple[str, ...]
    root: str
    language: str
    owner: str
    repo: str
    #: '' for `10-generated-lock` and the dependency graph, which had none.
    ref: str
    #: '' for the dependency graph, which had none.
    sha: str
    #: What followed the scan directory: the file within it.
    rest: tuple[str, ...]


def parse_legacy(path: str | Path) -> LegacyPath | None:
    """Recognise a language-keyed stage path, or None.

    Scans are `<root>/<lang>/<o>/<r>/<ref>/<sha>[/...]`, a generated
    lockfile `10-generated-lock/<lang>/<o>/<r>/<sha>[/...]` and a
    dependency graph `09-github-depgraph/<lang>/<o>/<r>/sbom.spdx.json`.
    The first stage root in the path is the one taken.
    """
    parts = PurePosixPath(str(path)).parts
    for index, part in enumerate(parts):
        if part not in STAGE_ROOTS:
            continue
        prefix, tail = parts[:index], parts[index + 1:]
        if len(tail) < 3 or tail[0] not in LEGACY_LANGUAGES:
            continue
        language, owner, repo = tail[0], tail[1], tail[2]
        if part in SCAN_ROOTS:
            # A ref may hold slashes (`release/1.4.0`, `@scope/pkg@1.0`):
            # the scan directory is the first commit-named one after it.
            at = _first_sha(tail, 4)
            if at is None:
                continue
            return LegacyPath(
                prefix, part, language, owner, repo, '/'.join(tail[3:at]),
                tail[at], tuple(tail[at + 1:]),
            )
        if part == LOCK_ROOT:
            if len(tail) < 4 or not is_sha(tail[3]):
                continue
            return LegacyPath(
                prefix, part, language, owner, repo, '', tail[3],
                tuple(tail[4:]),
            )
        # The dependency graph: one document per repository.
        if tail[3:] != (DEPGRAPH_DOCUMENT,):
            continue
        return LegacyPath(prefix, part, language, owner, repo, '', '', ())
    return None


def relocated(legacy: LegacyPath, repository_id: int) -> PurePosixPath:
    """Where `legacy` lives in the repository-keyed layout."""
    head = PurePosixPath(*legacy.prefix, legacy.root, str(int(repository_id)))
    if legacy.root == DEPGRAPH_ROOT:
        return head / LEGACY_DEPGRAPH_DIR / DEPGRAPH_DOCUMENT
    return head.joinpath(legacy.sha, *legacy.rest)


def relocate(path: str | Path, repository_id: int | None) -> Path:
    """`path` in the repository-keyed layout, if it was in the old one.

    For paths recorded before `data migrate-layout` ran: the per-language
    JSONL lists, and records kept in `raw_documents`. A path already in
    the new layout, or in neither, is returned as it is; so is any path
    when the repository is not known.
    """
    if repository_id is None:
        return Path(path)
    legacy = parse_legacy(path)
    if legacy is None:
        return Path(path)
    return Path(relocated(legacy, repository_id))


def landed(path: str | Path) -> str:
    """How a stage path is spelled in `raw_documents.path`.

    Relative to the data directory: `07-sbom/<id>/<sha>/sbom.json`, not
    whatever the working directory made of it; a decision's too,
    `03-github-release/<id>/<P>/release@2.json`. A path under no stage
    root (a JSONL list, say) is kept as it was given.
    """
    parts = PurePosixPath(str(path)).parts
    for index, part in enumerate(parts):
        # A decision's root holds per-language lists too, beside them.
        repository = parts[index + 1] if index + 1 < len(parts) else ''
        if part in STAGE_ROOTS or (
            part in DECISION_ROOTS and repository.isdigit()
        ):
            return str(PurePosixPath(*parts[index:]))
    return str(path)


@dataclass(frozen=True)
class Scan:
    """A path in the repository-keyed layout, taken apart."""

    root: str
    repository_id: int
    #: The commit, or for a dependency graph the fetch directory's name.
    directory: str
    rest: tuple[str, ...]


def parse_scan(path: str | Path) -> Scan | None:
    """Recognise a repository-keyed stage path, or None."""
    parts = PurePosixPath(str(path)).parts
    for index, part in enumerate(parts):
        if part not in STAGE_ROOTS:
            continue
        tail = parts[index + 1:]
        if len(tail) < 2 or not tail[0].isdigit():
            continue
        if part != DEPGRAPH_ROOT and not is_sha(tail[1]):
            continue
        return Scan(part, int(tail[0]), tail[1], tuple(tail[2:]))
    return None


def content_inside(path: str | Path) -> str | None:
    """A stored manifest's path within its repository, or None.

    In either layout: after `06-github-content/<id>/<sha>/` in the new
    one, after `06-github-content/<lang>/<o>/<r>/<ref>/<sha>/` in the old.
    """
    scan = parse_scan(path)
    if scan is not None and scan.root == CONTENT_ROOT and scan.rest:
        return '/'.join(scan.rest)
    legacy = parse_legacy(path)
    if legacy is not None and legacy.root == CONTENT_ROOT and legacy.rest:
        return '/'.join(legacy.rest)
    return None


# -- the same mapping, for ClickHouse ------------------------------------
#
# `data migrate-layout` rewrites `raw_documents.path` with one mutation
# per kind rather than row by row. These are the mapping above in RE2;
# `layout_test.py` holds the two to agreeing.

_LANGUAGES_RE = '|'.join(sorted(LEGACY_LANGUAGES))

#: A scan's file: groups 1 root, 2 sha, 3 the rest. The ref is one or
#: more directories (`release/1.4.0`); the scan is the first commit after.
SQL_LEGACY_SCAN = (
    r'^(?:.*?/)?(' + '|'.join(SCAN_ROOTS) + r')/(?:' + _LANGUAGES_RE +
    r')/[^/]+/[^/]+/.+?/([0-9a-f]{40})(/.*)?$'
)
#: A legacy dependency graph.
SQL_LEGACY_DEPGRAPH = (
    r'^(?:.*?/)?' + DEPGRAPH_ROOT + r'/(?:' + _LANGUAGES_RE +
    r')/[^/]+/[^/]+/' + re.escape(DEPGRAPH_DOCUMENT) + '$'
)
#: A path already in the new layout but landed with a prefix
#: (`data/09-github-depgraph/<id>/...`): group 1 is what is kept.
SQL_PREFIXED = (
    r'^.+?/((?:' + '|'.join(STAGE_ROOTS) + r')/[0-9]+/.*)$'
)


def sql_rewritten_path() -> str:
    """The ClickHouse expression for a row's new `path`.

    Rows in neither layout, and rows already relative, keep theirs.
    """
    return (
        'multiIf('
        f"match(path, '{_sql(SQL_LEGACY_SCAN)}'), "
        f"concat(extractGroups(path, '{_sql(SQL_LEGACY_SCAN)}')[1], '/', "
        f"toString(repository_id), '/', "
        f"extractGroups(path, '{_sql(SQL_LEGACY_SCAN)}')[2], "
        f"extractGroups(path, '{_sql(SQL_LEGACY_SCAN)}')[3]), "
        f"match(path, '{_sql(SQL_LEGACY_DEPGRAPH)}'), "
        f"concat('{DEPGRAPH_ROOT}/', toString(repository_id), "
        f"'/{LEGACY_DEPGRAPH_DIR}/{DEPGRAPH_DOCUMENT}'), "
        f"match(path, '{_sql(SQL_PREFIXED)}'), "
        f"extractGroups(path, '{_sql(SQL_PREFIXED)}')[1], "
        'path)'
    )


def sql_rewritten_sha() -> str:
    """The ClickHouse expression for the commit a rewritten scan row is at."""
    return (
        f"if(match(path, '{_sql(SQL_LEGACY_SCAN)}'), "
        f"extractGroups(path, '{_sql(SQL_LEGACY_SCAN)}')[2], commit_sha)"
    )


def sql_needs_rewrite() -> str:
    """The rows `sql_rewritten_path` changes."""
    return (
        f"(match(path, '{_sql(SQL_LEGACY_SCAN)}') "
        f"OR match(path, '{_sql(SQL_LEGACY_DEPGRAPH)}') "
        f"OR match(path, '{_sql(SQL_PREFIXED)}'))"
    )


def rewritten(path: str, repository_id: int) -> str:
    """`sql_rewritten_path`, in Python: for the dry run's report and for
    the tests that hold the two to agreeing. The same patterns, in the
    same order."""
    match = re.match(SQL_LEGACY_SCAN, path)
    if match:
        return (
            f'{match[1]}/{int(repository_id)}/{match[2]}{match[3] or ""}'
        )
    if re.match(SQL_LEGACY_DEPGRAPH, path):
        return (
            f'{DEPGRAPH_ROOT}/{int(repository_id)}/'
            f'{LEGACY_DEPGRAPH_DIR}/{DEPGRAPH_DOCUMENT}'
        )
    match = re.match(SQL_PREFIXED, path)
    if match:
        return match[1]
    return path


def rewritten_sha(path: str) -> str | None:
    """`sql_rewritten_sha` in Python: the commit a legacy scan row is at."""
    match = re.match(SQL_LEGACY_SCAN, path)
    return match[2] if match else None


def _sql(pattern: str) -> str:
    """A regular expression as a ClickHouse string literal's contents."""
    return pattern.replace('\\', '\\\\').replace("'", "\\'")
