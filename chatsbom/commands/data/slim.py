"""Drop from a stage ledger everything its readers do not read.

A record in `07-sbom/ruby.jsonl` is 63.1 KiB, of which **98% is
`all_releases`** and the stage's own contribution — one path — is
0.4 KiB. Each stage appends its own copy of the whole record, so the
release list is on disk four times and roughly 21 of the 22 GB of
ledgers are that repetition. The same releases are already in
ClickHouse as 1,154,743 rows, and now in `raw_documents` as well.

What each ledger is actually read for was measured, not assumed:

    05-github-tree       5.7 GB   nothing reads it
    06-github-content    5.2 GB   `sbom generate`, `sbom lock`
    09-github-depgraph   5.2 GB   `db index`, for `depgraph_path` alone
    07-sbom              5.2 GB   `github depgraph`, `db index`

So the first three can be reduced to the fields their readers name,
which reclaims about 16 GB. `07-sbom` is deliberately not in that list:
it is where `db raw` derives the `repo` record from, so slimming it
would leave the record with no home. Ask for it explicitly and this
refuses.

    chatsbom data slim              # report what would be dropped
    chatsbom data slim --apply      # rewrite them

Rewritten through a temporary file and renamed, so an interrupted run
leaves the original ledger intact rather than a truncated one.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

import structlog
import typer
from rich.markup import escape
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.logging import console

logger = structlog.get_logger('data_slim')
app = typer.Typer()

#: Fields every slimmed record keeps.
#:
#: `Storage` validates each line as a `Repository` when it loads a
#: ledger and reads `stars` to report the weakest repository it has
#: seen, so a record stripped below this stops being loadable at all.
#:
#: `repo`, not `name`. The model calls the field `repo` and dumps it
#: under that key; an earlier version of this list said `name`, which
#: is not in the stored records, so every slimmed line failed
#: validation. Nothing said so — `load_jsonl` catches the error per
#: line and returns what it could parse, which was nothing, and the
#: stage then reported an empty language and moved on. That is why
#: `_slim` validates what it writes before replacing anything.
IDENTITY: tuple[str, ...] = (
    'id', 'owner', 'repo', 'language', 'stars', 'url',
)


@dataclass(frozen=True)
class Target:
    """A ledger, and the fields its readers actually name."""

    directory: str
    keeps: tuple[str, ...]
    readers: str


#: What may be slimmed, and to what. Derived by grepping for each
#: ledger's readers and listing the fields they touch — not by
#: judgement about what looks important.
TARGETS: tuple[Target, ...] = (
    Target(
        '05-github-tree',
        ('download_target',),
        'nothing — written and never read',
    ),
    Target(
        '06-github-content',
        ('download_target', 'local_content_path'),
        '`sbom generate` and `sbom lock`',
    ),
    Target(
        '09-github-depgraph',
        ('depgraph_path',),
        '`db index`, via `_depgraph_paths`',
    ),
    Target(
        '07-sbom',
        # `db raw` finds the syft documents by `sbom_path` and the
        # manifest directories by `local_content_path`, both read from
        # *this* ledger. `download_target` is what `github depgraph`
        # and `sbom generate` need to name a scan.
        ('sbom_path', 'local_content_path', 'download_target'),
        '`db raw`, `github depgraph`, `sbom generate`',
    ),
)

#: Refused. `02-github-repo` is the metadata overlay's only source,
#: and `db raw` lands it verbatim as `repo-metadata`; there is nothing
#: in it that is not read.
#:
#: `07-sbom` was here too, and came off once the record had a home.
#: `db raw` derived the `repo` record from it, so slimming it would
#: have left the repository record nowhere to live — now the collector
#: writes that record directly (`RecordStore`, called by `chatsbom run`
#: and `sbom generate`) and `db index` reads it from `raw_documents` by
#: default.
PROTECTED: frozenset[str] = frozenset({'02-github-repo'})


@app.callback(invoke_without_command=True)
def main(
    apply: bool = typer.Option(
        False, '--apply', help='Rewrite the ledgers. Without this, report.',
    ),
    language: str | None = typer.Option(
        None, help='One language, for a trial run.',
    ),
    directory: str | None = typer.Option(
        None, '--directory', help='One stage directory, e.g. 05-github-tree',
    ),
) -> None:
    """Rewrite stage ledgers without the fields nothing reads."""
    if directory and directory in PROTECTED:
        console.print(
            f'[bold red]Refusing[/] to slim [cyan]{directory}[/].\n\n'
            'It is where `db raw` reads the repository record from, and '
            'the record does not live anywhere else yet — slimming it '
            'would lose the releases and the metadata for every '
            'repository.\n\n'
            '[dim]Slimmable: '
            + ', '.join(t.directory for t in TARGETS) + '[/dim]',
        )
        raise typer.Exit(1)

    root = get_container().config.paths.base_data_dir
    targets = [t for t in TARGETS if not directory or t.directory == directory]
    if not targets:
        console.print(f'[yellow]No such target: {escape(str(directory))}[/]')
        raise typer.Exit(1)

    table = Table(title='Stage ledgers')
    table.add_column('ledger')
    table.add_column('before', justify='right')
    table.add_column('after', justify='right')
    table.add_column('saved', justify='right')
    table.add_column('read by')

    before_total = after_total = 0
    for target in targets:
        before = after = 0
        for listing in sorted((root / target.directory).glob('*.jsonl')):
            if language and listing.stem != language:
                continue
            was, now = _slim(listing, target, apply)
            before += was
            after += now
        if not before:
            continue
        before_total += before
        after_total += after
        table.add_row(
            target.directory,
            _size(before), _size(after), _size(before - after),
            target.readers,
        )

    console.print(table)
    if not before_total:
        console.print('[yellow]Nothing to slim.[/]')
        return

    console.print(
        f'[bold]{_size(before_total)}[/] → [bold]{_size(after_total)}[/] '
        f'({_size(before_total - after_total)} reclaimed, '
        f'{100 * (before_total - after_total) / before_total:.0f}%)',
    )
    if not apply:
        console.print(
            '\n[dim]Dry run — nothing written. Pass --apply to rewrite.[/dim]',
        )
        return
    logger.info(
        'Ledgers slimmed', before=before_total, after=after_total,
    )


def _slim(listing: Path, target: Target, apply: bool) -> tuple[int, int]:
    """Rewrite one ledger, returning its size before and after.

    Reports without writing when `apply` is false, by measuring the
    lines it would have written — so the dry run's number is the real
    one rather than an estimate.
    """
    keep = set(IDENTITY) | set(target.keeps)
    before = listing.stat().st_size
    after = 0
    temp = listing.with_suffix(listing.suffix + '.tmp')

    handle = temp.open('w', encoding='utf-8') if apply else None
    try:
        with listing.open(encoding='utf-8') as source:
            for raw in source:
                if not raw.strip():
                    continue
                try:
                    record = json.loads(raw)
                except json.JSONDecodeError:
                    # Kept verbatim rather than dropped: an unparsable
                    # line is not this command's to discard.
                    after += len(raw.encode('utf-8'))
                    if handle:
                        handle.write(raw)
                    continue
                line = json.dumps(
                    {k: v for k, v in record.items() if k in keep},
                    separators=(',', ':'),
                ) + '\n'
                # Validated before it is written, not after it is
                # renamed into place. A slimmed record that no longer
                # loads makes the stage that reads this ledger report
                # an empty language and continue, which is a silent
                # 5 GB of data becoming unusable.
                if not _loadable(line):
                    raise _Unloadable(record.get('id'))
                after += len(line.encode('utf-8'))
                if handle:
                    handle.write(line)
        if handle:
            handle.flush()
            os.fsync(handle.fileno())
    except _Unloadable as error:
        if handle:
            handle.close()
        temp.unlink(missing_ok=True)
        console.print(
            f'[bold red]Refusing to slim[/] {escape(str(listing))}: repository '
            f'{error.repository_id} would no longer load.\n'
            f'[dim]The kept fields are not enough for the model. '
            f'Nothing was written.[/dim]',
        )
        raise typer.Exit(1) from error
    except OSError as error:
        if handle:
            handle.close()
        temp.unlink(missing_ok=True)
        logger.error('Could not slim', path=str(listing), error=str(error))
        return before, before
    finally:
        if handle:
            handle.close()

    if apply:
        temp.replace(listing)
    return before, after


class _Unloadable(Exception):
    """A slimmed record the model would reject."""

    def __init__(self, repository_id: object) -> None:
        super().__init__(repository_id)
        self.repository_id = repository_id


def _loadable(line: str) -> bool:
    """Whether a slimmed line still parses as a repository.

    The readers all go through `Repository`, so this is the actual
    contract — not a guess about which fields look required.
    """
    from chatsbom.models.repository import Repository
    try:
        Repository.model_validate_json(line)
    except Exception:  # noqa: BLE001 - any rejection is a rejection
        return False
    return True


def _size(value: int) -> str:
    """Bytes, in the unit a person would say them in."""
    if abs(value) < 1024:
        return f"{value} B"
    scaled = float(value)
    for unit in ('KiB', 'MiB', 'GiB'):
        scaled /= 1024.0
        if abs(scaled) < 1024 or unit == 'GiB':
            return f"{scaled:.1f} {unit}"
    return f"{scaled:.1f} GiB"
