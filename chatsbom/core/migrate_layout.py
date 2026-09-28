"""Move the stage artefacts from the language-keyed layout to the
repository-keyed one, with nothing re-fetched (#55, design §7).

    <stage>/<lang>/<owner>/<repo>/<ref>/<sha>/...  ->  <stage>/<id>/<sha>/...

Everything here is a `rename(2)` on one filesystem, so it is O(1) per
directory, atomic, and needs no space. What makes that safe to do to the
only copy of 30 GB:

* **A plan first, applied as written.** The dry run walks every root and
  writes `plan.tsv`: one line per rename, what it moves and why. It
  refuses to be applied while it holds a conflict — a destination two
  different sources would land on, a directory whose repository cannot be
  named, a move that would cross filesystems.
* **A journal.** Before each batch of renames, `BEGIN` lines naming them
  are appended to `journal.tsv` and fsynced; `DONE` lines after. A run
  that dies is resumed from it: a `BEGIN` without a `DONE` is done if its
  destination exists and its source does not, and retried otherwise.
* **A way back.** `rollback` replays the journal backwards — every
  rename undone, every directory it removed made again, every file it
  wrote deleted — and is itself idempotent.
* **Evidence.** An inventory of every file (size, mtime, and a sha256
  for a 1% sample) before; the same after; `verify` compares them.

Identical copies — the same commit under two refs, a Syft cache entry of
the same content under two refs — collapse into one; the other copy is
moved aside to `_migration/dedup/`, never deleted.

The roots it knows are `ROOTS`. Nothing outside them is touched.
"""
from __future__ import annotations

import filecmp
import hashlib
import json
import os
import re
import sqlite3
from collections import Counter
from collections import defaultdict
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.layout import CONTENT_ROOT
from chatsbom.core.layout import DEPGRAPH_DOCUMENT
from chatsbom.core.layout import DEPGRAPH_ROOT
from chatsbom.core.layout import is_sha
from chatsbom.core.layout import LEGACY_DEPGRAPH_DIR
from chatsbom.core.layout import LEGACY_LANGUAGES
from chatsbom.core.layout import LOCK_ROOT
from chatsbom.core.layout import SBOM_ROOT
from chatsbom.core.layout import TREE_ROOT

logger = structlog.get_logger('migrate_layout')

#: The stage roots under the data directory, and the one level of the
#: layout each moves. `scan`: `<lang>/<o>/<r>/<ref>/<sha>` -> `<id>/<sha>`.
DATA_SCAN_ROOTS = (TREE_ROOT, CONTENT_ROOT, SBOM_ROOT)

#: Labels of every root, in the order they are planned and reported.
SYFT_CACHE = '.cache/syft'
TREE_CACHE = '.cache/git-tree'
ROOTS: tuple[str, ...] = (
    TREE_ROOT, CONTENT_ROOT, SBOM_ROOT, DEPGRAPH_ROOT, LOCK_ROOT,
    SYFT_CACHE, TREE_CACHE,
)

#: Where the unversioned Syft cache — which no code reads (F17) — is put
#: aside, whole, inside `.cache/syft`.
UNVERSIONED = '_unversioned'

#: Operations in `plan.tsv` and `journal.tsv`.
MOVE = 'move'          # the layout change itself
DEDUP = 'dedup'        # an identical copy, moved aside
ASIDE = 'aside'        # a conflict resolved by --resolve newest
META = 'meta'          # a file written: a legacy graph's `meta.json`
RMDIR = 'rmdir'        # an empty directory the moves left, removed
MKDIR = 'mkdir'        # a directory created for a destination

PLAN = 'plan.tsv'
JOURNAL = 'journal.tsv'
PRE = 'pre.tsv'
POST = 'post.tsv'
LEDGER_BACKUP = 'ledger.pre.sqlite3'

#: One in this many files is hashed by the inventory, for `verify`.
SAMPLE_ONE_IN = 100

#: Renames per fsync of the journal.
DEFAULT_BATCH = 256

_VERSION = re.compile(r'^(unknown|\d+(\.\d+)+.*)$')


# -- where things are -----------------------------------------------------

@dataclass(frozen=True)
class Roots:
    """The two directories the stage roots live in, resolved."""

    data: Path
    cache: Path

    @classmethod
    def of(cls, data: Path, cache: Path) -> Roots:
        return cls(Path(os.path.realpath(data)), Path(os.path.realpath(cache)))

    def path(self, label: str) -> Path:
        if label == SYFT_CACHE:
            return self.cache / 'syft'
        if label == TREE_CACHE:
            return self.cache / 'git-tree'
        return self.data / label

    def aside(self, label: str) -> Path:
        """Where copies set aside from `label` go: the same filesystem."""
        base = self.cache if label.startswith('.cache') else self.data
        return base / '_migration'


# -- which repository a directory belongs to ------------------------------

@dataclass
class Resolver:
    """`owner/repo` as the old layout spelled it -> repository id.

    Built from every place a name and an id were recorded together: the
    ledger, and the per-language lists of every stage. Names are matched
    case-insensitively, as GitHub matches them. The lists also name each
    repository's own stage paths, which settles a name two ids have
    worn (a repository deleted and another created under its name).
    """

    by_name: dict[tuple[str, str], set[int]] = field(
        default_factory=lambda: defaultdict(set),
    )
    by_path: dict[str, int] = field(default_factory=dict)
    #: By name: each id, and the places that recorded it with the name.
    attested: dict[tuple[str, str], dict[int, set[str]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(set)),
    )
    #: Names two ids have worn that were settled, and how, for the report.
    settled: dict[str, str] = field(default_factory=dict)
    #: What each source contributed, for the report.
    sources: Counter[str] = field(default_factory=Counter)

    def add(self, owner: Any, repo: Any, repository_id: Any, source: str) -> None:
        if not isinstance(repository_id, int) or isinstance(repository_id, bool):
            return
        if not owner or not repo:
            return
        name = (str(owner).lower(), str(repo).lower())
        self.by_name[name].add(repository_id)
        self.attested[name][repository_id].add(source)
        self.sources[source] += 1

    def add_path(self, path: Any, repository_id: Any) -> None:
        if not path or not isinstance(repository_id, int):
            return
        self.by_path[_path_key(str(path))] = repository_id

    def resolve(
        self, owner: str, repo: str, key: str | None = None,
    ) -> tuple[int | None, str]:
        """`(id, '')`, or `(None, 'unmapped' | 'ambiguous')`."""
        if key is not None and key in self.by_path:
            return self.by_path[key], ''
        name = (owner.lower(), repo.lower())
        ids = self.by_name.get(name, set())
        if len(ids) == 1:
            return next(iter(ids)), ''
        if not ids:
            return None, 'unmapped'
        # Two ids for one name: the one more places recorded it under,
        # strictly. The real ledger has `psf/requests` as 1362490, which
        # every stage's list names, and as 2, which only the ledger and
        # one line of `02-github-repo` name: a test's fixture, leaked.
        by_count = sorted(
            ((len(where), rid) for rid, where in self.attested[name].items()),
            reverse=True,
        )
        if len(by_count) > 1 and by_count[0][0] > by_count[1][0]:
            chosen = by_count[0][1]
            self.settled[f'{owner}/{repo}'] = ', '.join(
                f'{rid} ({",".join(sorted(self.attested[name][rid]))})'
                for _, rid in by_count
            ) + f' -> {chosen}'
            return chosen, ''
        return None, 'ambiguous'


def _path_key(path: str) -> str:
    """A recorded stage path, as the part from its stage root on."""
    parts = Path(path).parts
    for index, part in enumerate(parts):
        if part in (TREE_ROOT, CONTENT_ROOT, SBOM_ROOT, DEPGRAPH_ROOT, LOCK_ROOT):
            return '/'.join(parts[index:])
    return path


def _owner_repo(record: dict[str, Any]) -> tuple[Any, Any]:
    owner = record.get('owner')
    if isinstance(owner, dict):
        owner = owner.get('login')
    repo = record.get('repo') or record.get('name')
    if (not owner or not repo) and isinstance(record.get('full_name'), str):
        head, _, tail = record['full_name'].partition('/')
        owner, repo = owner or head, repo or tail
    return owner, repo


def _jsonl(path: Path) -> Iterator[dict[str, Any]]:
    try:
        handle = path.open(encoding='utf-8')
    except OSError:
        return
    with handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict):
                yield record


def build_resolver(
    data: Path,
    ledger_path: Path | None,
    *,
    search_lists: bool = True,
) -> Resolver:
    """Every name/id pair on record. Reads only; opens the ledger
    read-only and immutable, so not even its WAL is touched."""
    resolver = Resolver()
    if ledger_path is not None and ledger_path.exists():
        uri = f'file:{ledger_path}?mode=ro&immutable=1'
        with sqlite3.connect(uri, uri=True) as db:
            for repository_id, owner, repo in db.execute(
                'SELECT repository_id, owner, repo FROM repository_state',
            ):
                resolver.add(owner, repo, int(repository_id), 'ledger')
    stages = [
        '02-github-repo', '03-github-release', '04-github-commit',
        TREE_ROOT, CONTENT_ROOT, SBOM_ROOT, DEPGRAPH_ROOT,
    ]
    if search_lists:
        stages.insert(0, '01-github-search')
    for stage in stages:
        for listing in sorted((data / stage).glob('*.jsonl')):
            language = listing.stem
            for record in _jsonl(listing):
                repository_id = record.get('id')
                owner, repo = _owner_repo(record)
                resolver.add(owner, repo, repository_id, stage)
                resolver.add_path(
                    record.get(
                        'local_content_path',
                    ), repository_id,
                )
                resolver.add_path(record.get('depgraph_path'), repository_id)
                sbom = record.get('sbom_path')
                if isinstance(sbom, str) and sbom.endswith('/sbom.json'):
                    # The scan directory, as `_units` names it.
                    resolver.add_path(sbom[:-len('/sbom.json')], repository_id)
                target = record.get('download_target')
                if (
                    stage == TREE_ROOT and isinstance(target, dict)
                    and owner and repo and language in LEGACY_LANGUAGES
                ):
                    resolver.add_path(
                        f'{TREE_ROOT}/{language}/{owner}/{repo}/'
                        f"{target.get('ref')}/{target.get('commit_sha')}",
                        repository_id,
                    )
    return resolver


# -- the plan ------------------------------------------------------------

@dataclass(frozen=True)
class Unit:
    """One thing the old layout holds: a scan directory, a document, a
    cache entry. Found by `_units`."""

    root: str
    src: Path
    owner: str
    repo: str
    ref: str
    #: The commit, or for a Syft cache entry its file name.
    leaf: str
    is_dir: bool
    #: How the lists recorded it, for `Resolver.by_path`.
    key: str | None = None


@dataclass
class Op:
    """One line of `plan.tsv`."""

    op: str
    root: str
    src: Path
    dst: Path
    files: int = 0
    size: int = 0
    note: str = ''

    def line(self) -> str:
        return '\t'.join((
            self.op, self.root, str(self.src), str(self.dst),
            str(self.files), str(self.size), self.note,
        ))

    @classmethod
    def parse(cls, line: str) -> Op:
        op, root, src, dst, files, size, note = line.rstrip('\n').split('\t')
        return cls(op, root, Path(src), Path(dst), int(files), int(size), note)


@dataclass(frozen=True)
class Conflict:
    kind: str
    root: str
    paths: tuple[Path, ...]
    detail: str = ''
    #: Set aside by `--resolve newest`: planned, so no longer blocking.
    resolved: bool = False


@dataclass
class Plan:
    ops: list[Op] = field(default_factory=list)
    conflicts: list[Conflict] = field(default_factory=list)
    #: Per root: units, files and bytes found in the old layout.
    found: dict[str, Counter[str]] = field(
        default_factory=lambda: defaultdict(Counter),
    )
    #: Repositories seen under more than one spelling of their name.
    spellings: dict[int, set[tuple[str, str]]] = field(
        default_factory=lambda: defaultdict(set),
    )
    resolver_sources: dict[str, int] = field(default_factory=dict)
    #: Names two ids had worn, settled by `Resolver.resolve`.
    settled: dict[str, str] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        per_root: dict[str, dict[str, int]] = {}
        for root in ROOTS:
            moves = [o for o in self.ops if o.root == root and o.op == MOVE]
            aside = [
                o for o in self.ops if o.root ==
                root and o.op in (DEDUP, ASIDE)
            ]
            per_root[root] = {
                'units': self.found[root]['units'],
                'files': self.found[root]['files'],
                'bytes': self.found[root]['bytes'],
                'moves': len(moves),
                'move_files': sum(o.files for o in moves),
                'move_bytes': sum(o.size for o in moves),
                'dedup': sum(1 for o in aside if o.op == DEDUP),
                'aside': sum(1 for o in aside if o.op == ASIDE),
                'aside_files': sum(o.files for o in aside),
                'aside_bytes': sum(o.size for o in aside),
                'meta': sum(1 for o in self.ops if o.root == root and o.op == META),
            }
        return {
            'roots': per_root,
            'conflicts': dict(Counter(c.kind for c in self.conflicts)),
            'unresolved': sum(1 for c in self.conflicts if not c.resolved),
            'renamed_or_recased': sum(
                1 for names in self.spellings.values() if len(names) > 1
            ),
            'resolver': self.resolver_sources,
            'settled': self.settled,
            'ops': len(self.ops),
            'lists_archived': sum(1 for o in self.ops if o.note == 'list'),
        }


def _device(path: Path) -> int:
    """The filesystem `path` is on: a rename across two is a copy."""
    return os.stat(path).st_dev


def _listdir(path: Path) -> list[os.DirEntry[str]]:
    try:
        with os.scandir(path) as it:
            return sorted(it, key=lambda e: e.name)
    except OSError:
        return []


def _size_of(path: Path) -> tuple[int, int]:
    """(files, bytes) under `path`, or of `path` if it is a file."""
    if path.is_file():
        return 1, path.stat().st_size
    files = size = 0
    for directory, _, names in os.walk(path):
        for name in names:
            try:
                size += os.lstat(os.path.join(directory, name)).st_size
            except OSError:
                continue
            files += 1
    return files, size


def _units(roots: Roots, label: str) -> Iterator[Unit]:
    """Everything under `label` still in the old layout."""
    base = roots.path(label)
    if label in DATA_SCAN_ROOTS:
        for lang in _listdir(base):
            if not lang.is_dir() or lang.name not in LEGACY_LANGUAGES:
                continue
            for owner in _listdir(Path(lang.path)):
                for repo in _listdir(Path(owner.path)):
                    for ref, sha in _scans_below(Path(repo.path)):
                        yield Unit(
                            label, sha, owner.name, repo.name, ref,
                            sha.name, True,
                            key=(
                                f'{label}/{lang.name}/{owner.name}/'
                                f'{repo.name}/{ref}/{sha.name}'
                            ),
                        )
    elif label == LOCK_ROOT:
        for lang in _listdir(base):
            if not lang.is_dir() or lang.name not in LEGACY_LANGUAGES:
                continue
            for owner in _listdir(Path(lang.path)):
                for repo in _listdir(Path(owner.path)):
                    for commit in _listdir(Path(repo.path)):
                        if commit.is_dir() and is_sha(commit.name):
                            yield Unit(
                                label, Path(commit.path), owner.name,
                                repo.name, '', commit.name, True,
                            )
    elif label == DEPGRAPH_ROOT:
        for lang in _listdir(base):
            if not lang.is_dir() or lang.name not in LEGACY_LANGUAGES:
                continue
            for owner in _listdir(Path(lang.path)):
                for repo in _listdir(Path(owner.path)):
                    document = Path(repo.path) / DEPGRAPH_DOCUMENT
                    if document.is_file():
                        yield Unit(
                            label, document, owner.name, repo.name, '', '',
                            False,
                            key=(
                                f'{label}/{lang.name}/{owner.name}/'
                                f'{repo.name}/{DEPGRAPH_DOCUMENT}'
                            ),
                        )
    elif label == SYFT_CACHE:
        for top in _listdir(base):
            if not top.is_dir() or top.name == UNVERSIONED:
                continue
            if not _VERSION.match(top.name):
                # Unversioned: `<owner>/...`, which no code reads. Moved
                # aside whole, one rename per owner.
                yield Unit(label, Path(top.path), top.name, '', '', '', True)
                continue
            for owner in _listdir(Path(top.path)):
                if owner.name.isdigit() and _is_new_syft(Path(owner.path)):
                    continue
                for repo in _listdir(Path(owner.path)):
                    for ref, entry in _entries_below(Path(repo.path)):
                        yield Unit(
                            label, entry, owner.name, repo.name, ref,
                            entry.name, False,
                        )
    elif label == TREE_CACHE:
        for owner in _listdir(base):
            for repo in _listdir(Path(owner.path)):
                if not repo.is_dir() or is_sha(repo.name):
                    continue
                for ref, sha in _scans_below(Path(repo.path)):
                    yield Unit(
                        label, sha, owner.name, repo.name, ref, sha.name,
                        True,
                    )


#: How many directories a ref may span (`release/1.4.0` is two).
MAX_REF_PARTS = 8


def _scans_below(repo: Path) -> Iterator[tuple[str, Path]]:
    """`(ref, scan directory)` under `<owner>/<repo>/`: the first
    directory named by a commit below each ref, which may itself hold
    slashes (`release/1.4.0`, `@scope/pkg@1.0`)."""
    frontier: list[tuple[Path, tuple[str, ...]]] = [(repo, ())]
    while frontier:
        directory, ref = frontier.pop()
        for entry in _listdir(directory):
            if not entry.is_dir(follow_symlinks=False):
                continue
            if ref and is_sha(entry.name):
                yield '/'.join(ref), Path(entry.path)
            elif len(ref) < MAX_REF_PARTS:
                frontier.append((Path(entry.path), (*ref, entry.name)))


def _entries_below(repo: Path) -> Iterator[tuple[str, Path]]:
    """`(ref, cache file)` under a Syft cache's `<owner>/<repo>/`."""
    frontier: list[tuple[Path, tuple[str, ...]]] = [(repo, ())]
    while frontier:
        directory, ref = frontier.pop()
        for entry in _listdir(directory):
            if entry.is_dir(follow_symlinks=False):
                if len(ref) < MAX_REF_PARTS:
                    frontier.append((Path(entry.path), (*ref, entry.name)))
            elif ref and entry.name.endswith('.json'):
                yield '/'.join(ref), Path(entry.path)


def _is_new_syft(directory: Path) -> bool:
    """Whether `<version>/<digits>` is already `<version>/<id>`: its
    entries are cache files, not repository directories."""
    return any(
        entry.is_file() and entry.name.endswith('.json')
        for entry in _listdir(directory)
    )


def _destination(roots: Roots, unit: Unit, repository_id: int) -> Path:
    base = roots.path(unit.root)
    rid = str(repository_id)
    if unit.root == DEPGRAPH_ROOT:
        return base / rid / LEGACY_DEPGRAPH_DIR / DEPGRAPH_DOCUMENT
    if unit.root == SYFT_CACHE:
        version = unit.src.relative_to(base).parts[0]
        return base / version / rid / unit.leaf
    return base / rid / unit.leaf


def _identical(a: Path, b: Path) -> bool:
    """Byte for byte, file for file."""
    if a.is_file() or b.is_file():
        return a.is_file() and b.is_file() and filecmp.cmp(a, b, shallow=False)
    left = sorted(_relative_files(a))
    right = sorted(_relative_files(b))
    if left != right:
        return False
    return all(filecmp.cmp(a / rel, b / rel, shallow=False) for rel in left)


def _relative_files(root: Path) -> Iterator[str]:
    for directory, _, names in os.walk(root):
        for name in names:
            yield os.path.relpath(os.path.join(directory, name), root)


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


#: Stage directories holding per-language JSONL lists.
LIST_STAGES: tuple[str, ...] = (
    '01-github-search', '02-github-repo', '03-github-release',
    '04-github-commit', TREE_ROOT, CONTENT_ROOT, SBOM_ROOT, DEPGRAPH_ROOT,
)
#: Where `--archive-lists` puts them.
LEGACY_LISTS = '_legacy-lists'


def plan_list_archive(roots: Roots, plan: Plan) -> None:
    """Design §7.1's last two rows: every `<stage>/<lang>.jsonl` to
    `<stage>/_legacy-lists/`, and `01-github-search/all.jsonl` to
    `all-<its date>.jsonl`, immutable from then on.

    Opt-in (`--archive-lists`): the stage-major commands (`github
    release/commit/tree/content`, `sbom lock/generate`), `db index
    --from-files` and `db raw`'s metadata overlay still read these lists
    until the changes that stop keying them by language land.
    """
    for stage in LIST_STAGES:
        base = roots.data / stage
        for entry in _listdir(base):
            name = entry.name
            if not entry.is_file() or not name.endswith('.jsonl'):
                continue
            if name.removesuffix('.jsonl') in LEGACY_LANGUAGES:
                dst = base / LEGACY_LISTS / name
            elif stage == '01-github-search' and name == 'all.jsonl':
                taken = datetime.fromtimestamp(
                    entry.stat().st_mtime, tz=timezone.utc,
                ).strftime('%Y-%m-%d')
                dst = base / f'all-{taken}.jsonl'
            else:
                continue
            if dst.exists():
                plan.conflicts.append(
                    Conflict(
                        'destination-exists', stage,
                        (Path(entry.path), dst),
                    ),
                )
                continue
            plan.ops.append(
                Op(
                    MOVE, stage, Path(entry.path), dst, 1,
                    entry.stat().st_size, 'list',
                ),
            )


def make_plan(
    roots: Roots,
    resolver: Resolver,
    *,
    resolve_newest: bool = False,
    archive_lists: bool = False,
    progress: Callable[[str, int], None] | None = None,
) -> Plan:
    """Walk every root and decide every rename. Reads only."""
    plan = Plan(resolver_sources=dict(resolver.sources))
    plan.settled = resolver.settled
    for label in ROOTS:
        base = roots.path(label)
        if not base.is_dir():
            continue
        # Moves must stay on one filesystem, or they are copies.
        aside = roots.aside(label)
        aside_parent = aside if aside.exists() else aside.parent
        if _device(base) != _device(aside_parent):
            plan.conflicts.append(
                Conflict('cross-device', label, (base, aside_parent)),
            )
        groups: dict[Path, list[Unit]] = defaultdict(list)
        count = 0
        for unit in _units(roots, label):
            count += 1
            if progress is not None and count % 1000 == 0:
                progress(label, count)
            files, size = _size_of(unit.src)
            found = plan.found[label]
            found['units'] += 1
            found['files'] += files
            found['bytes'] += size
            if label == SYFT_CACHE and not unit.repo:
                plan.ops.append(
                    Op(
                        MOVE, label, unit.src, base / UNVERSIONED / unit.owner,
                        files, size, 'unversioned',
                    ),
                )
                continue
            repository_id, why = resolver.resolve(
                unit.owner, unit.repo, unit.key,
            )
            if repository_id is None:
                plan.conflicts.append(
                    Conflict(
                        why, label, (unit.src,),
                        f'{unit.owner}/{unit.repo}', resolved=resolve_newest,
                    ),
                )
                if resolve_newest:
                    plan.ops.append(
                        _aside(roots, label, unit.src, files, size, why),
                    )
                continue
            plan.spellings[repository_id].add((unit.owner, unit.repo))
            groups[_destination(roots, unit, repository_id)].append(unit)
        for dst, units in groups.items():
            _plan_group(
                plan, roots, label, dst, units,
                resolver, resolve_newest,
            )
        if label == DEPGRAPH_ROOT:
            for op in [o for o in plan.ops if o.root == label and o.op == MOVE]:
                plan.ops.append(
                    Op(META, label, op.dst, op.dst.parent / 'meta.json', 1, 0),
                )
    if archive_lists:
        plan_list_archive(roots, plan)
    # Destinations that exist already, planned or not, are checked once
    # more at apply time; here the plan itself must not collide.
    return plan


def _aside(roots: Roots, label: str, src: Path, files: int, size: int, why: str) -> Op:
    base = roots.path(label)
    relative = src.relative_to(base)
    return Op(
        ASIDE, label, src,
        roots.aside(label) / 'conflicts' / label.replace('/', '_') / relative,
        files, size, why,
    )


def _plan_group(
    plan: Plan,
    roots: Roots,
    label: str,
    dst: Path,
    units: list[Unit],
    resolver: Resolver,
    resolve_newest: bool,
) -> None:
    """One destination: one unit moves there, identical others are
    set aside, and different ones are a conflict."""
    # The copy the lists name first (what `raw_documents` rows point at),
    # then any ref but `HEAD`, then the newest.
    def rank(unit: Unit) -> tuple[bool, bool, float]:
        return (
            unit.key is None or unit.key not in resolver.by_path,
            unit.ref == 'HEAD',
            -_mtime(unit.src),
        )

    ordered = sorted(units, key=rank)
    keep = ordered[0]
    base = roots.path(label)
    if dst.exists():
        # Already in the new layout: written since, or by an earlier run.
        if _identical(keep.src, dst):
            for unit in ordered:
                files, size = _size_of(unit.src)
                plan.ops.append(_dedup(roots, label, unit.src, files, size))
            return
        plan.conflicts.append(
            Conflict(
                'destination-exists', label, (keep.src, dst),
                resolved=resolve_newest,
            ),
        )
        if resolve_newest:
            for unit in ordered:
                files, size = _size_of(unit.src)
                plan.ops.append(
                    _aside(
                        roots, label, unit.src,
                        files, size, 'destination-exists',
                    ),
                )
        return
    files, size = _size_of(keep.src)
    plan.ops.append(Op(MOVE, label, keep.src, dst, files, size))
    for other in ordered[1:]:
        files, size = _size_of(other.src)
        if _identical(keep.src, other.src):
            plan.ops.append(_dedup(roots, label, other.src, files, size))
            continue
        plan.conflicts.append(
            Conflict(
                'collision', label, (keep.src, other.src),
                str(dst.relative_to(base)), resolved=resolve_newest,
            ),
        )
        if resolve_newest:
            # The kept one is the newest unless the lists named it; the
            # other is set aside, not deleted.
            plan.ops.append(
                _aside(
                    roots, label, other.src,
                    files, size, 'collision',
                ),
            )


def _dedup(roots: Roots, label: str, src: Path, files: int, size: int) -> Op:
    base = roots.path(label)
    return Op(
        DEDUP, label, src,
        roots.aside(label) / 'dedup' / label.replace('/', '_') /
        src.relative_to(base),
        files, size,
    )


# -- plan files ------------------------------------------------------------

def write_plan(plan: Plan, path: Path) -> None:
    lines = [
        '# ' + json.dumps({'summary': plan.summary()}, sort_keys=True),
        *(
            '# conflict\t' + '\t'.join(
                (
                    'resolved' if c.resolved else 'open', c.kind, c.root,
                    c.detail, *map(str, c.paths),
                ),
            )
            for c in plan.conflicts
        ),
        *(op.line() for op in plan.ops),
    ]
    atomic_write_text(path, '\n'.join(lines) + '\n')


def read_plan(path: Path) -> tuple[list[Op], dict[str, Any], int]:
    """(ops, summary, conflicts still open)."""
    ops: list[Op] = []
    summary: dict[str, Any] = {}
    unresolved = 0
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        if line.startswith('# conflict\t'):
            if line.split('\t')[1] != 'resolved':
                unresolved += 1
            continue
        if line.startswith('# '):
            summary = json.loads(line[2:]).get('summary', {})
            continue
        ops.append(Op.parse(line))
    return ops, summary, unresolved


# -- the journal -----------------------------------------------------------

class Journal:
    """`journal.tsv`: append-only, fsynced before the renames it names."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open('a', encoding='utf-8')

    def write(self, *fields: str) -> None:
        self._handle.write('\t'.join(fields) + '\n')

    def sync(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self.sync()
        self._handle.close()

    @staticmethod
    def entries(path: Path) -> list[list[str]]:
        if not path.exists():
            return []
        out = []
        for line in path.read_text(encoding='utf-8').splitlines():
            if line.strip():
                out.append(line.split('\t'))
        return out


def _state(journal: Path) -> tuple[set[tuple[str, str, str]], list[tuple[str, str, str]]]:
    """(done, begun but not done) `(op, src, dst)` in the journal."""
    begun: dict[tuple[str, str, str], None] = {}
    done: set[tuple[str, str, str]] = set()
    for entry in Journal.entries(journal):
        if entry[0] == 'BEGIN' and len(entry) >= 4:
            begun[(entry[1], entry[2], entry[3])] = None
        elif entry[0] == 'DONE' and len(entry) >= 4:
            done.add((entry[1], entry[2], entry[3]))
    return done, [key for key in begun if key not in done]


@dataclass
class ApplyResult:
    renamed: int = 0
    written: int = 0
    resumed: int = 0
    removed_dirs: int = 0
    skipped: int = 0


def apply_plan(
    ops: list[Op],
    workdir: Path,
    *,
    batch: int = DEFAULT_BATCH,
    meta_for: Callable[[Path], dict[str, Any]] | None = None,
    progress: Callable[[int, int], None] | None = None,
    stop_after: int | None = None,
    stops: Iterable[Path] = (),
) -> ApplyResult:
    """Carry out `ops`, journaled, resuming a run that was cut short.

    `stop_after` stops after that many operations, as a kill would: for
    the tests of resuming.
    """
    result = ApplyResult()
    journal_path = workdir / JOURNAL
    done, pending = _state(journal_path)

    journal = Journal(journal_path)
    try:
        # A BEGIN with no DONE: the rename happened or it did not. If
        # its source is gone and its destination is there, it did.
        # Anything else is simply done again below.
        for op_name, src, dst in pending:
            if op_name == META:
                continue
            if not Path(src).exists() and Path(dst).exists():
                journal.write('DONE', op_name, src, dst)
                done.add((op_name, src, dst))
                result.resumed += 1
        journal.sync()

        todo = [
            op for op in ops
            if op.op in (MOVE, DEDUP, ASIDE, META)
            and (op.op, str(op.src), str(op.dst)) not in done
        ]
        total = len(todo)
        count = 0
        for start in range(0, total, batch):
            chunk = todo[start:start + batch]
            written: list[Op] = []
            for op in chunk:
                journal.write('BEGIN', op.op, str(op.src), str(op.dst))
            journal.sync()
            for op in chunk:
                if stop_after is not None and count >= stop_after:
                    # As a kill would: what the batch wrote is not synced
                    # and not said to be done.
                    return result
                if op.op == META:
                    # Written again unless whole: a crash before its batch
                    # was synced can leave the name with nothing in it.
                    if not looks_like_whole_json_object(op.dst):
                        meta = meta_for(op.src) if meta_for else {}
                        _write_unsynced(
                            op.dst, json.dumps(
                                meta, ensure_ascii=False, indent=2,
                            ),
                        )
                        result.written += 1
                    # Said DONE only once the batch is on disk: see below.
                    written.append(op)
                    count += 1
                    continue
                if not op.src.exists():
                    if op.dst.exists():
                        journal.write('DONE', op.op, str(op.src), str(op.dst))
                        count += 1
                        continue
                    raise FileNotFoundError(
                        f'neither {op.src} nor {op.dst} exists',
                    )
                if op.dst.exists():
                    raise FileExistsError(
                        f'{op.dst} exists: the plan is stale; plan again',
                    )
                for created in _make_parents(op.dst.parent):
                    journal.write(MKDIR, str(created))
                os.rename(op.src, op.dst)
                journal.write('DONE', op.op, str(op.src), str(op.dst))
                result.renamed += 1
                count += 1
            _flush_written(journal, written)
            journal.sync()
            if progress is not None:
                progress(min(start + batch, total), total)

        # The directories the moves emptied: `<lang>/<o>/<r>/<ref>` and up.
        for directory in _emptied(ops, stops):
            try:
                os.rmdir(directory)
            except OSError:
                continue
            journal.write(RMDIR, str(directory))
            result.removed_dirs += 1
        journal.sync()
    finally:
        journal.close()
    return result


def _write_unsynced(path: Path, text: str) -> None:
    """Whole or not at all, like `atomic_write_text`, but not fsynced
    one by one: 24,946 of these at two fsyncs each is minutes on a disk
    that spins. `_flush_written` syncs a batch of them at once."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{path.name}.tmp')
    temporary.write_text(text, encoding='utf-8')
    os.replace(temporary, path)


def _flush_written(journal: Journal, written: list[Op]) -> None:
    """Put a batch of written files on disk, then say they are done.

    Until the DONE lines are, a crash leaves them begun, and the next
    run writes any that did not survive.
    """
    if not written:
        return
    os.sync()
    for op in written:
        journal.write('DONE', op.op, str(op.src), str(op.dst))
    written.clear()


def _make_parents(directory: Path) -> list[Path]:
    """mkdir -p, returning what it created, outermost first."""
    missing: list[Path] = []
    current = directory
    while not current.exists():
        missing.append(current)
        current = current.parent
    for path in reversed(missing):
        path.mkdir()
    return list(reversed(missing))


def _emptied(ops: Iterable[Op], stops: Iterable[Path]) -> list[Path]:
    """Every ancestor of a moved source below the directory it must
    stop at (a stage root; a Syft cache's version directory), deepest
    first."""
    stop = {Path(p) for p in stops}
    candidates: set[Path] = set()
    for op in ops:
        if op.op not in (MOVE, DEDUP, ASIDE):
            continue
        parent = op.src.parent
        while parent not in stop and parent.parent != parent:
            if any(parent == s or s in parent.parents for s in stop):
                candidates.add(parent)
            else:
                break
            parent = parent.parent
    return sorted(candidates, key=lambda p: len(p.parts), reverse=True)


def stops_for(roots: Roots) -> list[Path]:
    """Where `_emptied` stops: every root, and the Syft cache's
    version directories, which hold the moved entries now."""
    out = [roots.path(label) for label in ROOTS]
    syft = roots.path(SYFT_CACHE)
    for entry in _listdir(syft):
        if entry.is_dir() and (_VERSION.match(entry.name) or entry.name == UNVERSIONED):
            out.append(Path(entry.path))
    return out


@dataclass
class RollbackResult:
    restored: int = 0
    deleted: int = 0
    recreated_dirs: int = 0
    removed_dirs: int = 0


def rollback(workdir: Path) -> RollbackResult:
    """Undo the journal, newest first. Safe to run twice."""
    result = RollbackResult()
    for entry in reversed(Journal.entries(workdir / JOURNAL)):
        kind = entry[0]
        if kind == RMDIR:
            directory = Path(entry[1])
            if not directory.exists():
                directory.mkdir(parents=True)
                result.recreated_dirs += 1
        elif kind == MKDIR:
            directory = Path(entry[1])
            try:
                directory.rmdir()
                result.removed_dirs += 1
            except OSError:
                pass
        elif kind in ('DONE', 'BEGIN') and len(entry) >= 4:
            op, src, dst = entry[1], Path(entry[2]), Path(entry[3])
            if op == META:
                # Begun is enough: the plan writes one only where none
                # was, so whatever is there is ours, done or cut short.
                for written in (dst, dst.with_name(f'.{dst.name}.tmp')):
                    if written.exists():
                        written.unlink()
                        result.deleted += written == dst
                continue
            if dst.exists() and not src.exists():
                src.parent.mkdir(parents=True, exist_ok=True)
                os.rename(dst, src)
                result.restored += 1
    # Directories the moves created and left empty once undone.
    return result


# -- the legacy dependency graph's stamp ----------------------------------

def legacy_graph_meta(document: Path, repository_id: int) -> dict[str, Any]:
    """`meta.json` for a legacy graph: when GitHub says it made it, and
    that its head is unknown — it was never recorded."""
    body = document.read_bytes()
    created = ''
    try:
        parsed = json.loads(body)
        sbom = parsed.get('sbom', parsed) if isinstance(parsed, dict) else {}
        info = sbom.get('creationInfo') if isinstance(sbom, dict) else None
        if isinstance(info, dict) and isinstance(info.get('created'), str):
            created = info['created']
    except ValueError:
        pass
    return {
        'repository_id': int(repository_id),
        'ref': '',
        'commit_sha': '',
        'fetched_at': created,
        'http_status': 200,
        'sha256': hashlib.sha256(body).hexdigest(),
        'legacy': True,
    }


def repository_of(path: Path) -> int:
    """The id a moved legacy graph sits under: `<id>/legacy/<doc>`."""
    return int(path.parent.parent.name)


# -- inventory and verification -------------------------------------------

def _sampled(label: str, relative: str) -> bool:
    digest = hashlib.md5(
        f'{label}/{relative}'.encode(), usedforsecurity=False,
    ).hexdigest()
    return int(digest[:8], 16) % SAMPLE_ONE_IN == 0


def inventory(roots: Roots, path: Path, *, hash_sample: bool = True) -> dict[str, Counter[str]]:
    """Every file under every root: `root, relative path, size, mtime,
    sha256` (the hash for a 1% sample, else `-`). Returns per-root
    totals, which lead the file as a `#` line."""
    totals: dict[str, Counter[str]] = defaultdict(Counter)
    rows: list[str] = []
    for label in ROOTS:
        base = roots.path(label)
        if not base.is_dir():
            continue
        for directory, _, names in os.walk(base):
            for name in names:
                full = os.path.join(directory, name)
                try:
                    stat = os.lstat(full)
                except OSError:
                    continue
                relative = os.path.relpath(full, base)
                digest = '-'
                if hash_sample and _sampled(label, relative):
                    digest = _sha256(Path(full))
                rows.append(
                    f'{label}\t{relative}\t{stat.st_size}\t'
                    f'{int(stat.st_mtime)}\t{digest}',
                )
                totals[label]['files'] += 1
                totals[label]['bytes'] += stat.st_size
    header = '# ' + \
        json.dumps({'totals': totals, 'taken_at': _now()}, sort_keys=True)
    atomic_write_text(path, header + '\n' + '\n'.join(rows) + '\n')
    return totals


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open('rb') as handle:
        while chunk := handle.read(1 << 20):
            hasher.update(chunk)
    return hasher.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_inventory(path: Path) -> tuple[dict[str, dict[str, int]], list[tuple[str, str, int, str]]]:
    """(totals, sampled rows `(root, relative, size, sha256)`)."""
    totals: dict[str, dict[str, int]] = {}
    sampled: list[tuple[str, str, int, str]] = []
    with path.open(encoding='utf-8') as handle:
        for line in handle:
            if line.startswith('# '):
                totals = json.loads(line[2:]).get('totals', {})
                continue
            parts = line.rstrip('\n').split('\t')
            if len(parts) == 5 and parts[4] != '-':
                sampled.append((parts[0], parts[1], int(parts[2]), parts[4]))
    return totals, sampled


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ''


def verify_files(roots: Roots, workdir: Path) -> list[Check]:
    """The file-side checks of design §7 step 8."""
    checks: list[Check] = []
    ops, summary, _ = read_plan(workdir / PLAN)

    done, pending = _state(workdir / JOURNAL)
    planned = {(o.op, str(o.src), str(o.dst)) for o in ops if o.op != RMDIR}
    missing = planned - done
    checks.append(
        Check(
            'journal complete', not pending and not missing,
            f'{len(pending)} begun without done, '
            f'{len(missing)} planned without done',
        ),
    )

    bad = [
        o for o in ops if o.op in (MOVE, DEDUP, ASIDE)
        and (not o.dst.exists() or o.src.exists())
    ]
    checks.append(
        Check(
            'every destination exists and no source does', not bad,
            '; '.join(f'{o.src} -> {o.dst}' for o in bad[:5]),
        ),
    )

    pre_totals, sample = read_inventory(workdir / PRE)
    post_totals = inventory(roots, workdir / POST, hash_sample=False)
    for label in ROOTS:
        before = pre_totals.get(label, {})
        after = post_totals.get(label, Counter())
        aside = [o for o in ops if o.root == label and o.op in (DEDUP, ASIDE)]
        metas = [o for o in ops if o.root == label and o.op == META]
        meta_bytes = sum(_file_size(o.dst) for o in metas)
        expected_files = (
            before.get('files', 0) - sum(o.files for o in aside) + len(metas)
        )
        expected_bytes = (
            before.get('bytes', 0) - sum(o.size for o in aside) + meta_bytes
        )
        ok = (
            after.get('files', 0) == expected_files
            and after.get('bytes', 0) == expected_bytes
        )
        checks.append(
            Check(
                f'{label}: files and bytes', ok,
                f"before {before.get('files', 0):,} files "
                f"{before.get('bytes', 0):,} B; set aside "
                f'{sum(o.files for o in aside):,} files; meta written '
                f"{len(metas):,}; after {after.get('files', 0):,} files "
                f"{after.get('bytes', 0):,} B",
            ),
        )

    # A 1% sample, hashed before, found and hashed again where it went.
    index = {
        str(o.src): o for o in ops if o.op in (MOVE, DEDUP, ASIDE)
    }
    mismatched: list[str] = []
    checked = 0
    for label, relative, _, digest in sample:
        original = roots.path(label) / relative
        now = _where_now(original, index)
        checked += 1
        try:
            if _sha256(now) != digest:
                mismatched.append(str(original))
        except OSError:
            mismatched.append(f'{original} (missing at {now})')
    checks.append(
        Check(
            'sampled files are byte-identical where they went',
            not mismatched,
            f'{checked:,} checked; ' + '; '.join(mismatched[:5]),
        ),
    )
    return checks


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _where_now(original: Path, moved: dict[str, Op]) -> Path:
    """Where a file that was at `original` is now: under the destination
    of the move whose source contained it."""
    current = original
    while True:
        op = moved.get(str(current))
        if op is not None:
            return op.dst / original.relative_to(current)
        if current.parent == current:
            return original
        current = current.parent


# -- what the rename left for the ledger -----------------------------------

def backup_ledger(ledger_path: Path, target: Path) -> bool:
    """A consistent copy of the ledger, once; False if it existed."""
    if target.exists():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(ledger_path)
    try:
        destination = sqlite3.connect(target)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()
    return True


def restore_ledger(backup: Path, ledger_path: Path) -> None:
    """Put the pre-migration ledger back, in place."""
    source = sqlite3.connect(backup)
    try:
        destination = sqlite3.connect(ledger_path)
        try:
            source.backup(destination)
        finally:
            destination.close()
    finally:
        source.close()


# -- raw_documents ---------------------------------------------------------
#
# `raw_documents.path` named the file each row was read from, and two
# readers derive meaning from it: the commit a Syft SBOM or a manifest is
# at (`/<sha>/` in the path) and a manifest's path within its repository
# (what follows the content root). So the paths move with the files: one
# mutation per kind, the same mapping as `core/layout`, with the old
# paths kept in a side table first so that `rollback` can put them back.

#: The kinds whose paths name a stage file.
RAW_KINDS: tuple[str, ...] = ('syft', 'content', 'github-depgraph')
#: The side table: every rewritten row's path before, keyed as the table.
RAW_BACKUP = 'raw_documents_layout_backup'
#: A Join-engine copy of it, for `joinGet` in the restoring mutation.
RAW_RESTORE = 'raw_documents_layout_restore'


def raw_counts(client: Any) -> dict[str, dict[str, int]]:
    """Per kind: rows, and rows whose path the rewrite would change."""
    from chatsbom.core.layout import sql_needs_rewrite
    rows = client.query(
        'SELECT kind, count(), countIf(' + sql_needs_rewrite() + ') '
        'FROM raw_documents GROUP BY kind ORDER BY kind',
    ).result_rows
    return {
        str(kind): {'rows': int(total), 'rewrite': int(rewrite)}
        for kind, total, rewrite in rows
    }


def raw_targets(client: Any) -> Iterator[tuple[str, int, str]]:
    """`(kind, repository_id, path)` standing for every scan or graph
    a stage-file row points at: one per scan directory for manifests,
    whose files are many. Under 100,000 rows a query, as the read-only
    account allows."""
    for kind in ('syft', 'github-depgraph'):
        for repository_id, path in client.query(
            'SELECT repository_id, any(path) FROM raw_documents '
            'WHERE kind = {kind:String} GROUP BY repository_id, path',
            parameters={'kind': kind},
        ).result_rows:
            yield kind, int(repository_id), str(path)
    for repository_id, path in client.query(
        'SELECT repository_id, any(path) FROM raw_documents '
        "WHERE kind = 'content' "
        "GROUP BY repository_id, extract(path, '/([0-9a-f]{40})/')",
    ).result_rows:
        yield 'content', int(repository_id), str(path)


def raw_consistency(
    targets: Iterable[tuple[str, int, str]],
    data: Path,
    ops: Iterable[Op],
) -> dict[str, Counter[str]]:
    """Whether every landed row's rewritten path will name a file.

    The rows' ids come from the records that named each file; the
    plan's ids from the names on disk. Were the two ever to disagree, a
    row would point at a directory nobody moved there; this counts, per
    kind, the rows that would (`missing`) and would not (`found`).
    """
    from chatsbom.core.layout import parse_scan
    from chatsbom.core.layout import rewritten
    destinations: set[str] = set()
    for op in ops:
        if op.op == MOVE:
            destinations.add(str(op.dst))
    result: dict[str, Counter[str]] = defaultdict(Counter)
    for kind, repository_id, path in targets:
        new = rewritten(path, repository_id)
        scan = parse_scan(new)
        if scan is None:
            result[kind]['unparsed'] += 1
            continue
        if kind == 'github-depgraph':
            target = data / new
        else:
            target = data / scan.root / \
                str(scan.repository_id) / scan.directory
        if str(target) in destinations or target.exists():
            result[kind]['found'] += 1
        else:
            result[kind]['missing'] += 1
    return result


def rewrite_raw(client: Any) -> dict[str, int]:
    """Rewrite `raw_documents.path` (and fill `commit_sha`) in place.

    Idempotent: a row already rewritten no longer matches. The old paths
    go to `RAW_BACKUP` first, once.
    """
    from chatsbom.core.layout import sql_needs_rewrite
    from chatsbom.core.layout import sql_rewritten_path
    from chatsbom.core.layout import sql_rewritten_sha
    client.command(
        f'CREATE TABLE IF NOT EXISTS {RAW_BACKUP} ('
        'kind LowCardinality(String), repository_id UInt64, '
        'sha256 String, path String) '
        'ENGINE = MergeTree ORDER BY (kind, repository_id, sha256)',
    )
    kinds = ', '.join(f"'{k}'" for k in RAW_KINDS)
    client.command(
        f'INSERT INTO {RAW_BACKUP} '
        'SELECT kind, repository_id, sha256, path FROM raw_documents '
        f'WHERE kind IN ({kinds}) AND {sql_needs_rewrite()} '
        f'AND (kind, repository_id, sha256, path) NOT IN '
        f'(SELECT kind, repository_id, sha256, path FROM {RAW_BACKUP})',
    )
    changed: dict[str, int] = {}
    for kind in RAW_KINDS:
        before = int(
            client.query(
                'SELECT count() FROM raw_documents WHERE kind = {kind:String} '
                f'AND {sql_needs_rewrite()}',
                parameters={'kind': kind},
            ).result_rows[0][0],
        )
        changed[kind] = before
        if not before:
            continue
        client.command(
            'ALTER TABLE raw_documents UPDATE '
            f'commit_sha = {sql_rewritten_sha()}, '
            f'path = {sql_rewritten_path()} '
            f"WHERE kind = '{kind}' AND {sql_needs_rewrite()}",
            settings={'mutations_sync': 2},
        )
    return changed


def restore_raw(client: Any, database: str) -> int:
    """Put every rewritten path back, from `RAW_BACKUP`."""
    exists = client.query(
        'SELECT count() FROM system.tables '
        'WHERE database = {db:String} AND name = {name:String}',
        parameters={'db': database, 'name': RAW_BACKUP},
    ).result_rows[0][0]
    if not exists:
        return 0
    client.command(f'DROP TABLE IF EXISTS {RAW_RESTORE}')
    client.command(
        f'CREATE TABLE {RAW_RESTORE} '
        '(kind String, repository_id UInt64, sha256 String, path String) '
        'ENGINE = Join(ANY, LEFT, kind, repository_id, sha256)',
    )
    client.command(
        f'INSERT INTO {RAW_RESTORE} '
        f'SELECT toString(kind), repository_id, sha256, path FROM {RAW_BACKUP}',
    )
    restored = int(
        client.query(f'SELECT count() FROM {RAW_BACKUP}').result_rows[0][0],
    )
    client.command(
        'ALTER TABLE raw_documents UPDATE '
        f"path = joinGet('{database}.{RAW_RESTORE}', 'path', "
        'toString(kind), repository_id, sha256), '
        "commit_sha = '' "
        f'WHERE (toString(kind), repository_id, sha256) IN '
        f'(SELECT toString(kind), repository_id, sha256 FROM {RAW_BACKUP})',
        settings={
            'mutations_sync': 2, 'allow_nondeterministic_mutations': 1,
        },
    )
    client.command(f'DROP TABLE IF EXISTS {RAW_RESTORE}')
    return restored


def verify_raw(
    client: Any,
    data: Path,
    before: dict[str, dict[str, int]],
) -> list[Check]:
    """The `raw_documents` checks of design §7 step 8."""
    checks: list[Check] = []
    now = raw_counts(client)
    same = all(
        now.get(kind, {}).get('rows', 0) == counts.get('rows', 0)
        for kind, counts in before.items()
    )
    checks.append(
        Check(
            'raw_documents: rows per kind unchanged', same,
            ', '.join(
                f"{kind} {counts.get('rows', 0):,} -> "
                f"{now.get(kind, {}).get('rows', 0):,}"
                for kind, counts in sorted(before.items())
            ),
        ),
    )
    left = {k: v['rewrite'] for k, v in now.items() if v['rewrite']}
    checks.append(
        Check(
            'raw_documents: no path left in the old layout', not left,
            ', '.join(f'{k} {v:,}' for k, v in sorted(left.items())),
        ),
    )
    languages = '|'.join(sorted(LEGACY_LANGUAGES))
    legacy = int(
        client.query(
            'SELECT count() FROM raw_documents WHERE kind IN '
            "('syft', 'content', 'github-depgraph') "
            f"AND match(path, '(^|/)(05-github-tree|06-github-content|07-sbom|"
            f"09-github-depgraph)/({languages})/')",
        ).result_rows[0][0],
    )
    checks.append(
        Check(
            'raw_documents: no language directory in a path',
            legacy == 0, f'{legacy:,}',
        ),
    )
    missing: list[str] = []
    total = 0
    for kind in ('syft', 'content', 'github-depgraph'):
        offset = 0
        while True:
            rows = client.query(
                'SELECT DISTINCT path FROM raw_documents '
                'WHERE kind = {kind:String} ORDER BY path '
                'LIMIT 50000 OFFSET {offset:UInt64}',
                parameters={'kind': kind, 'offset': offset},
            ).result_rows
            if not rows:
                break
            for (path,) in rows:
                total += 1
                if not (data / str(path)).is_file():
                    missing.append(f'{kind}:{path}')
            offset += len(rows)
    checks.append(
        Check(
            'raw_documents: every stage-file path resolves to a file',
            not missing,
            f'{total:,} paths; {len(missing):,} missing '
            + '; '.join(missing[:5]),
        ),
    )
    return checks


#: One row per repository and source: what the equivalence check compares.
_EQUIVALENCE = (
    'SELECT repository_id, source, count(), uniqExact(name) '
    'FROM current_artifacts GROUP BY repository_id, source'
)


def compare_current(production: Any, scratch: Any) -> Check:
    """The transform equivalence check: every repository's current
    artifacts, per source, the same in both databases.

    Rows and distinct names per `(repository_id, source)`. A dependency
    graph's rows may differ only in the commit they are stamped with,
    which neither count sees.
    """
    def table(client: Any) -> dict[tuple[int, str], tuple[int, int]]:
        return {
            (int(r), str(s)): (int(n), int(u))
            for r, s, n, u in client.query(_EQUIVALENCE).result_rows
        }
    left, right = table(production), table(scratch)
    keys = set(left) | set(right)
    differing = sorted(k for k in keys if left.get(k) != right.get(k))
    repositories = {k[0] for k in keys}
    bad = {k[0] for k in differing}
    return Check(
        'transform equivalence: rows and names per repository and source',
        not differing,
        f'{len(repositories) - len(bad):,} of {len(repositories):,} '
        f'repositories identical; differing: '
        + '; '.join(
            f'{k[0]}/{k[1]} {left.get(k)} vs {right.get(k)}'
            for k in differing[:5]
        ),
    )
