"""What each dataset method takes, checked before the snapshot is asked.

The page's endpoint, `web/src/d1/api.ts`, refuses a value that no store
should have to guess at (#31): a count or a position that is not a whole
number at least its minimum, a string longer than what it names, a flag
that is not true or false. The Python API has no endpoint in front of
it, and every caller it serves, the web routes, the chat's tools and the
CLI, reaches the methods directly, so each method checks its own
arguments with these, the same rules at the same limits. How *large* a
value may be is not refused here: past a ceiling a value is not wrong,
only more than anyone gets, and the method clamps it (`shape.py`).

Refused rather than coerced. -1 and 2.5 mean nothing as a limit, and a
store left to guess read -1 as "no limit given".
"""
from __future__ import annotations

#: A package name, or the start of one. npm's own limit is 214
#: characters, the longest rule of any registry here, and 256 leaves
#: room above it while still bounding what reaches a statement.
NAME_CHARS = 256

#: A language or an ecosystem is a word.
WORD_CHARS = 64

#: The largest whole number a JSON number holds exactly, in JavaScript:
#: `Number.MAX_SAFE_INTEGER`. Past it, 2 ** 60 is refused there as a
#: limit "too large to be exact", and here alike.
MAX_SAFE_INTEGER = 2 ** 53 - 1

#: The names every JavaScript object has: what `value in
#: Object.prototype` finds, in Node 22 and 26. The endpoint refuses an
#: ecosystem so called, since no registry is, and the lookup that once
#: found a function under `toString` failed as a 500 (#31). Refused here
#: too, so that while the page can be answered by either service, both
#: refuse the same values.
JAVASCRIPT_NAMES = frozenset({
    '__defineGetter__', '__defineSetter__', '__lookupGetter__',
    '__lookupSetter__', '__proto__', 'constructor', 'hasOwnProperty',
    'isPrototypeOf', 'propertyIsEnumerable', 'toLocaleString', 'toString',
    'valueOf',
})


class InvalidParameter(ValueError):
    """A value a method does not take. The page's endpoint answers such
    a call 400, before it asks the store anything."""


def _length(value: str) -> int:
    """A string's length as JavaScript counts it, in UTF-16 code units.

    `value.length` counts a character past the Basic Multilingual Plane
    as two, so a name of 129 rockets is 258 long there. Counted in
    characters, the Python would take a name the page's endpoint
    refuses.
    """
    return len(value.encode('utf-16-le', 'surrogatepass')) // 2


def _capped(key: str, value: str, cap: int) -> str:
    if _length(value) > cap:
        raise InvalidParameter(f'"{key}" must be at most {cap} characters')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError:
        # Half of a surrogate pair, which JSON can spell and SQLite cannot
        # bind: refused here as the bad value it is, rather than failing
        # in the statement.
        raise InvalidParameter(f'"{key}" must be text') from None
    return value


def name(key: str, value: object, cap: int = NAME_CHARS) -> str:
    """A string the method cannot do without: a package's name, a
    search's term."""
    if not isinstance(value, str) or value == '':
        raise InvalidParameter(f'"{key}" must be a non-empty string')
    return _capped(key, value, cap)


def word(key: str, value: object, cap: int = WORD_CHARS) -> str | None:
    """A string that may be left out, as None or as nothing at all."""
    if value is None or value == '':
        return None
    if not isinstance(value, str):
        raise InvalidParameter(f'"{key}" must be a string')
    return _capped(key, value, cap)


def ecosystem(key: str, value: object) -> str | None:
    """An ecosystem, as `ecosystems_for` spells it.

    Not held to the table in `chatsbom/core/ecosystems.py`: a type the
    table does not map reads as itself, so the page offers, and sends
    back, types it has never heard of (`github-action`). Only what no
    registry is called is refused.
    """
    checked = word(key, value)
    if checked in JAVASCRIPT_NAMES:
        raise InvalidParameter(f'"{key}" is not an ecosystem')
    return checked


def flag(key: str, value: object) -> bool:
    """True or false; left out, false."""
    if value is None:
        return False
    if not isinstance(value, bool):
        raise InvalidParameter(f'"{key}" must be true or false')
    return value


def whole(key: str, value: object, minimum: int = 1) -> int | None:
    """A count, or a position: a whole number no smaller than `minimum`.

    As JSON has it, a number: `2.0` is the whole number 2, as it is to
    the page's endpoint, and a flag is not a number, although Python's
    `True` is an int. None is a value left out.
    """
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidParameter(_not_whole(key, minimum))
    # Infinity and NaN are not whole either.
    if isinstance(value, float) and not value.is_integer():
        raise InvalidParameter(_not_whole(key, minimum))
    number = int(value)
    if abs(number) > MAX_SAFE_INTEGER or number < minimum:
        raise InvalidParameter(_not_whole(key, minimum))
    return number


def _not_whole(key: str, minimum: int) -> str:
    return f'"{key}" must be a whole number, at least {minimum}'
