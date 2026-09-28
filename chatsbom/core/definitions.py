"""What the database stores, kept to what the code declares.

The views, the repository dictionary and the rollups are created with
`IF NOT EXISTS`. For a rollup that is not negotiable — its stored rows
are the expensive part, and a plain CREATE would empty the panel it
serves — but it meant a changed definition never reached a database that
already had the object. #21 and #22 rewrote every current-state rollup,
added two views and gave `dict_repositories` two attributes, and on a
database created before them none of that arrived: the dashboard's
dependants query failed with "No such attribute 'depgraph_observed_at'",
and the overview went on counting every scan ever taken.

So each object carries, in its COMMENT, a fingerprint of the statement
that declared it, and `IngestionRepository.ensure_schema` replaces one
that carries another — or none, as everything declared before this did.

**What is fingerprinted** is the statement as ClickHouse reads it:

- without its `--` lines. They are most of these statements, and
  rewording one must not rebuild a rollup.
- with `{database}` filled in: it names the table a dictionary loads.
- without credentials. A dictionary's SOURCE carries the admin user and
  password. A hash of them does not belong in a COMMENT every account
  can read, and a new password does not change what the dictionary is.
  The one case that does need it declared again — a stored copy the
  server now refuses — shows up as a failed reload, and
  `IngestionRepository.reload_dictionaries` answers it there.
- for a view, with the tables it reads. ClickHouse stores a view's
  columns when it creates the view, so `current_artifacts`' `SELECT a.*`
  froze the columns `artifacts` had that day: a column added to the
  table later was unknown through the view (UNKNOWN_IDENTIFIER, on
  25.12) until the view was declared again.

**What each kind accepts**, checked on ClickHouse 25.12:

    view             COMMENT    CREATE OR REPLACE VIEW
    dictionary       COMMENT    CREATE OR REPLACE DICTIONARY
    refreshable MV   COMMENT    EXCHANGE TABLES; no OR REPLACE (a syntax error)

The COMMENT goes before a view's query. After it, `FROM facts COMMENT
'...'` reads `COMMENT` as an alias of `facts`, and fails on the string.
"""
from __future__ import annotations

import hashlib
import re

#: How a fingerprint starts, so an operator reading `system.tables`
#: can tell what the COMMENT is.
PREFIX = 'ddl-sha256:'

#: How every declaration opens, and what every rewrite here relies on.
#: `tests/definitions_test.py` holds each declared statement to it. A
#: table is not fingerprinted, but it is rebuilt under another name.
_DECLARATION = re.compile(
    r'^CREATE (?P<kind>MATERIALIZED VIEW|VIEW|DICTIONARY|TABLE) '
    r'IF NOT EXISTS (?P<name>\w+)',
)

#: Where a view's query starts: the first `AS` before a SELECT or WITH.
_QUERY = re.compile(r'\bAS\s+(?=(?:SELECT|WITH)\b)')


def executable(ddl: str) -> str:
    """The statement as ClickHouse reads it: without its `--` lines."""
    return '\n'.join(
        line for line in ddl.split('\n')
        if not line.strip().startswith('--')
    )


def fingerprint(ddl: str, *reads: str) -> str:
    """What a declaration is, as the COMMENT that records it.

    `reads` are the statements of what it depends on beyond its own
    text: for a view, the tables whose columns it froze.
    """
    digest = hashlib.sha256()
    for statement in (ddl, *reads):
        digest.update(executable(statement).encode('utf-8'))
        digest.update(b'\0')
    return f'{PREFIX}{digest.hexdigest()}'


def _declaration(ddl: str) -> re.Match[str]:
    match = _DECLARATION.match(ddl)
    if match is None:
        raise ValueError(f'not a declaration this can rewrite: {ddl[:80]!r}')
    return match


def declared_name(ddl: str) -> str:
    """The object a declaration creates."""
    return _declaration(ddl)['name']


def stamped(ddl: str, stamp: str, empty: bool = False) -> str:
    """The declaration, carrying `stamp` where ClickHouse parses it.

    Sent as ClickHouse reads it, without the `--` lines, so that no note
    can hold the word a clause is placed by.

    `empty` creates a refreshable view without the refresh it otherwise
    starts on creation: a replacement is refreshed explicitly, and
    waited for, before it is swapped in. It has to precede the COMMENT.
    """
    ddl = executable(ddl)
    declaration = _declaration(ddl)
    clause = f"COMMENT '{stamp}'"
    if declaration['kind'] == 'DICTIONARY':
        return f'{ddl}\n{clause}'
    if empty:
        clause = f'EMPTY {clause}'
    query = _QUERY.search(ddl, declaration.end())
    if query is None:
        raise ValueError(f'no query in {declaration["name"]}')
    return f'{ddl[:query.start()]}{clause}\n{ddl[query.start():]}'


def replacing(ddl: str) -> str:
    """The declaration as a statement that replaces what it declares.

    `CREATE OR REPLACE`, which an Atomic database performs as a create
    under a temporary name and an exchange: a reader finds the old
    object or the new one, never neither. A refreshable view has no such
    statement; `IngestionRepository` builds one aside and exchanges it.
    """
    declaration = _declaration(ddl)
    if declaration['kind'] == 'MATERIALIZED VIEW':
        raise ValueError(
            f'{declaration["name"]}: a refreshable view has no OR REPLACE',
        )
    return (
        f'CREATE OR REPLACE {declaration["kind"]} {declaration["name"]}'
        f'{ddl[declaration.end():]}'
    )


def renamed(ddl: str, name: str) -> str:
    """The declaration, creating `name` instead: a replacement, built
    aside under another name, for a rollup or a table."""
    declaration = _declaration(ddl)
    return f'CREATE {declaration["kind"]} {name}{ddl[declaration.end():]}'


def reads(ddl: str, name: str) -> bool:
    """Whether the declared query mentions `name`, as a word.

    Generous on purpose: a column that shares a table's name counts. It
    decides what is refreshed after `name` changed, and a needless
    refresh costs less than a missed one.
    """
    text = executable(ddl)
    body = text[_declaration(text).end():]
    return re.search(rf'\b{re.escape(name)}\b', body) is not None
