"""The tools the chat's model may call: the dataset API, bounded (#140).

Parameterised questions, never SQL. The model can name a tool and give
its arguments; it cannot ask for a statement to be run, because no such
tool exists. That is the containment: a model that could pass SQL, or a
reader who could talk one into it, could pass any SQL.

Each tool is made of the dataset API's own methods (#138), so the
vocabulary here is the dashboard's, and a question cannot reach data
the page could not. They run here, in-process, against the snapshot the
question pinned when it started (`call`), where the Worker's page ran
them and posted their results back: so what the model reads of the data
is the data, by construction, and no client can supply it (#128,
section 2.6).

Ported from the Worker's `web/src/tools.ts`, bounds and all: every
result is its counts, then as many rows as fit in RESULT_ROWS and
RESULT_CHARS, cut from the end and said to be cut. What the model is
told about the tools is `prompt`.
"""
import json
import math
import sqlite3
from collections.abc import Mapping
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.ecosystems import canonical
from chatsbom.dataset import Dataset
from chatsbom.dataset import InvalidParameter
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset

logger = structlog.get_logger('chat')

#: What the Worker let a whole conversation carry, in characters. The
#: loop is the server's now, but the model reads what it did, so the
#: bound on one result is set against it as it was.
MAX_CONVERSATION_CHARS = 200_000

#: How long one result may be, as the model receives it: a tenth of a
#: conversation. Every result is sent again with each turn after it,
#: and two `dependents_of` calls at 500 rows, over 100,000 characters
#: each, were once enough to have the next turn refused. Fifty
#: dependants, the default, come to about 12,000 characters.
RESULT_CHARS = MAX_CONVERSATION_CHARS // 10

#: And no more rows than this, whatever `limit` asks for: rows are
#: there to be named in an answer, and no answer names a hundred. A
#: limit is clamped to this before the dataset is asked.
RESULT_ROWS = 100

#: The tools there are, by name: `prompt` describes each, in this order.
TOOL_NAMES = (
    'dependents_of',
    'ecosystems_for',
    'search_packages',
    'top_packages',
    'version_spread',
    'language_coverage',
    'ecosystem_coverage',
)

#: What the model is told of a call the dataset failed on: the log says
#: why, and nothing of it is the model's to repeat.
FAILED = 'The dataset could not answer this call.'


class ToolError(Exception):
    """A call that cannot be run, and what the model is told of it: it
    can recover, and a call left unanswered would be a malformed
    conversation."""


def call(snapshot: Path, name: str, arguments: str) -> str:
    """Run the call the model made, `name` with the JSON text
    `arguments`, against `snapshot`, and say what the model is to read
    of it: the result, or `{"error": ...}`, as compact JSON.

    Blocking, and opened and closed here: a connection to a snapshot is
    for the thread that opened it, and opening one costs a fraction of
    a millisecond.
    """
    try:
        if name not in TOOL_NAMES:
            raise ToolError(f'unknown tool: {name}')
        parsed = _arguments(arguments)
        with open_dataset(snapshot) as dataset:
            return text(execute(dataset, name, parsed))
    except (ToolError, InvalidParameter) as error:
        return text({'error': str(error)})
    except (sqlite3.Error, OSError) as error:
        logger.error('tool call failed', tool=name, error=str(error))
        return text({'error': FAILED})


def _arguments(arguments: str) -> object:
    """The model's arguments, parsed as JSON is: no NaN, no Infinity.
    None at all, for a tool that takes none, is none."""
    if not arguments.strip():
        return None
    try:
        return json.loads(arguments, parse_constant=_not_json)
    except (ValueError, RecursionError):
        raise ToolError('the arguments are not JSON') from None


def _not_json(constant: str) -> object:
    raise ValueError(f'{constant} is not JSON')


def text(result: Mapping[str, Any]) -> str:
    """A result as the model reads it: JSON as compact as the Worker's
    `JSON.stringify`, the characters themselves rather than escapes."""
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


def execute(dataset: Dataset, name: str, arguments: object) -> dict[str, Any]:
    """One call against `dataset`, its result bounded.

    The arguments come from a model, so each is checked again here
    rather than trusted from the schema: a schema constrains a shape,
    not a range, and the dataset is the last line before the data. The
    dataset checks its own as well (`InvalidParameter`).
    """
    if name not in TOOL_NAMES:
        raise ToolError(f'unknown tool: {name}')
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, Mapping):
        raise ToolError('the arguments are not a JSON object')
    counts, rows = _answer(dataset, name, arguments)
    return bounded(counts, jsonable(rows))


def _answer(
    dataset: Dataset, name: str, args: Mapping[str, Any],
) -> tuple[dict[str, Any], Sequence[object]]:
    """A tool's counts, and its rows, before either is bounded."""
    if name == 'dependents_of':
        package = _string(args.get('name'))
        if package is None:
            raise ToolError('dependents_of requires a package name')
        filters: dict[str, Any] = {
            'type': _ecosystem(args.get('type')),
            'language': _string(args.get('language')),
        }
        direct = args.get('direct_only') is True
        # Counted, never measured. The rows stop at a limit, and come one
        # per repository, version and relationship, so their length is
        # neither the dependant count nor bounded by it. The counts take
        # the rows' own filters, as the dashboard's table does.
        rows = dataset.dependents_of(
            package, **filters, direct_only=direct,
            limit=_limit(args.get('limit')),
        )
        counts: dict[str, Any] = {
            'total': dataset.count_dependents(
                package, **filters, direct_only=direct,
            ),
        }
        if not direct:
            counts['direct_total'] = dataset.count_dependents(
                package, **filters, direct_only=True,
            )
        return counts, rows
    if name == 'ecosystems_for':
        package = _string(args.get('name'))
        if package is None:
            raise ToolError('ecosystems_for requires a package name')
        return {}, dataset.ecosystems_for(package)
    if name == 'search_packages':
        fragment = _string(args.get('fragment'))
        if fragment is None:
            raise ToolError('search_packages requires a fragment')
        return {}, dataset.search_packages(fragment, _limit(args.get('limit')))
    if name == 'top_packages':
        return {}, dataset.top_packages(
            direct_only=args.get('direct_only') is True,
            ecosystem=_ecosystem(args.get('ecosystem')),
            limit=_limit(args.get('limit')),
        )
    if name == 'version_spread':
        package = _string(args.get('name'))
        if package is None:
            raise ToolError('version_spread requires a package name')
        spread = dataset.version_spread(package, _limit(args.get('limit')))
        # The versions are its rows, like any other tool's, so the same
        # bound applies to them; what was set aside stays beside them.
        return (
            {'constrained': spread.constrained, 'unversioned': spread.unversioned},
            spread.versions,
        )
    if name == 'language_coverage':
        return {}, dataset.language_coverage()
    return {}, dataset.ecosystem_coverage()


def _string(value: object) -> str | None:
    """A string the model gave, or None for anything else."""
    return value if isinstance(value, str) and value else None


def _ecosystem(value: object) -> str | None:
    """An ecosystem as the page names it, however the model spelled it:
    `PHP-Composer` is `composer`. The ranking lowercases what it is
    given and stops there, so a collector's spelling found nothing
    (#142); a scope on the dependants canonicalised it, and did not
    lowercase it."""
    spelled = _string(value)
    return canonical(spelled.lower()) if spelled else None


def _limit(value: object) -> int | None:
    """A limit as the Worker took one: a number, floored and held
    between 1 and RESULT_ROWS, or none. `true` is no number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    return min(max(math.floor(value), 1), RESULT_ROWS)


def _length(value: str) -> int:
    """A text's length as JavaScript counts it, in UTF-16 code units: the
    unit the Worker's bound was set in."""
    return len(value.encode('utf-16-le', 'surrogatepass')) // 2


def bounded(counts: Mapping[str, Any], rows: Sequence[Any]) -> dict[str, Any]:
    """An answer as the model receives it: its counts, then as many rows
    as fit in RESULT_ROWS and RESULT_CHARS.

    Applied to every tool rather than to the one that prompted it: any
    of them can outgrow a conversation. Rows are cut from the end, so
    the head of each ranking survives, and a cut is always declared: a
    result that lost rows silently would be read as complete, which is
    the mistake of reading rows as a count, made one step earlier.
    """
    def keep(shown: int) -> dict[str, Any]:
        result = {**counts, 'rows_shown': shown}
        if shown < len(rows):
            result['truncated'] = True
            result['rows_dropped'] = len(rows) - shown
        result['rows'] = list(rows[:shown])
        return result

    def fits(result: Mapping[str, Any]) -> bool:
        return _length(text(result)) <= RESULT_CHARS

    most = min(len(rows), RESULT_ROWS)
    whole = keep(most)
    if fits(whole):
        return whole
    # The longest head that fits. Another row never shortens the
    # result, so bisection finds it; and the counts beside the rows are
    # a few numbers, so no rows at all always fits.
    least = 0
    while most - least > 1:
        middle = (least + most) // 2
        if fits(keep(middle)):
            least = middle
        else:
            most = middle
    return keep(least)
