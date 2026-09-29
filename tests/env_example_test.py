"""`.env.example`: complete, safe to copy as it is, and read alike by
everything that reads it.

It held three lines, one an active ANTHROPIC_BASE_URL pointing at
api.deepseek.com. Copy it, put in an Anthropic token as the README said,
and the token went to a third party. Most of what the code, compose and
the deploy scripts read was not in it at all.
"""
import ast
import os
import re
import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
import yaml
from dotenv import dotenv_values

from chatsbom.commands import chat
from chatsbom.core import config
from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.logging import log_format

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / '.env.example'
COMPOSE = ROOT / 'docker-compose.yaml'


# --- what reads the environment ----------------------------------------------

#: A shell or compose expansion: `$NAME`, or `${NAME` with whatever
#: operator follows (`:-`, `-`, `:?`, `:+`, ...).
EXPANSION = re.compile(r'\$(?:\{([A-Za-z_]\w*)|([A-Za-z_]\w*))')


def _names(matches: list[tuple[str, str]]) -> set[str]:
    return {first or second for first, second in matches}


def _is_environ(node: ast.expr) -> bool:
    """`os.environ`, or `environ` imported from os."""
    if isinstance(node, ast.Attribute):
        return (
            node.attr == 'environ'
            and isinstance(node.value, ast.Name) and node.value.id == 'os'
        )
    return isinstance(node, ast.Name) and node.id == 'environ'


def _reads_by_name(func: ast.expr) -> bool:
    """`os.getenv` or `getenv`, or `get`/`setdefault` on `os.environ`."""
    if isinstance(func, ast.Name):
        return func.id == 'getenv'
    if isinstance(func, ast.Attribute):
        if func.attr == 'getenv':
            return isinstance(func.value, ast.Name) and func.value.id == 'os'
        return func.attr in ('get', 'setdefault') and _is_environ(func.value)
    return False


def _literal_names(node: ast.expr, line: int) -> set[str]:
    if isinstance(node, ast.Constant) and node.value is None:
        return set()
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return {node.value}
    if isinstance(node, (ast.List, ast.Tuple)):
        return set().union(*(_literal_names(e, line) for e in node.elts))
    raise ValueError(f'line {line}: reads a variable it does not name')


def python_reads(source: str) -> set[str]:
    """Every environment variable a module reads.

    `os.getenv`, `os.environ.get`, `os.environ[...]`, `in os.environ`,
    and Typer's `envvar=`. From the syntax tree rather than the text, so
    a call split over lines counts and a comment does not. A name that is
    not a literal cannot be checked, so it is refused rather than missed.
    """
    names: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            if _reads_by_name(node.func) and node.args:
                names |= _literal_names(node.args[0], node.lineno)
            for keyword in node.keywords:
                if keyword.arg == 'envvar':
                    names |= _literal_names(keyword.value, node.lineno)
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Load)
            and _is_environ(node.value)
        ):
            names |= _literal_names(node.slice, node.lineno)
        elif isinstance(node, ast.Compare) and any(
            isinstance(op, (ast.In, ast.NotIn)) and _is_environ(right)
            for op, right in zip(node.ops, node.comparators)
        ):
            names |= _literal_names(node.left, node.lineno)
    return names


def strings(node: object) -> Iterator[str]:
    """Every string in a parsed YAML document, keys included."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from strings(key)
            yield from strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from strings(item)
    elif isinstance(node, str):
        yield node


def compose_reads(document: object) -> set[str]:
    """Every variable compose takes from the shell or `.env`.

    From the parsed file, so comments do not count. `$$` is compose's
    escape for a literal `$`, which reaches the container unexpanded.
    """
    names: set[str] = set()
    for text in strings(document):
        names |= _names(EXPANSION.findall(text.replace('$$', '')))
    return names


#: Where one shell command ends and the next begins.
SEPARATOR = re.compile(r';|&&|\|\|?')

#: An assignment opening a command — after a `case` pattern or a
#: keyword, if there is one — or the variable of a `for` loop.
ASSIGNMENT = re.compile(
    r'^\s*(?:\S*\)\s*)?(?:(?:do|then|else)\s+)?'
    r'(?:(?:export|local|readonly)\s+)?([A-Za-z_]\w*)='
    r'|^\s*for\s+([A-Za-z_]\w*)\s+in\b',
)


def shell_reads(script: str) -> set[str]:
    """Variables a script expands before it has assigned them itself.

    Command by command, so `WEB_DIR="${WEB_DIR:-/app/web}"` — the
    environment's value, with a fallback — is a read, while after
    `SLICE="${SYNC_SLICE:-500}"` the script's `${SLICE}` is its own.
    Comment lines do not count, and `$$` is the shell's pid.
    """
    assigned: set[str] = set()
    reads: set[str] = set()
    for line in script.splitlines():
        if line.lstrip().startswith('#'):
            continue
        for command in SEPARATOR.split(line.replace('$$', '')):
            reads |= _names(EXPANSION.findall(command)) - assigned
            assigned |= _names(ASSIGNMENT.findall(command))
    return reads


def environment_reads() -> dict[str, set[str]]:
    """Each variable read where it counts, and the files that read it."""
    found: dict[str, set[str]] = {}

    def note(names: set[str], where: Path) -> None:
        for name in names:
            found.setdefault(name, set()).add(str(where.relative_to(ROOT)))

    for module in sorted((ROOT / 'chatsbom').rglob('*.py')):
        note(python_reads(module.read_text(encoding='utf-8')), module)
    note(compose_reads(yaml.safe_load(COMPOSE.read_text())), COMPOSE)
    for script in sorted((ROOT / 'deploy').glob('*.sh')):
        note(shell_reads(script.read_text()), script)
    return found


def test_the_python_scanner_finds_every_way_of_reading():
    source = """
import os
from os import environ, getenv

a = os.getenv('A')
b = os.getenv(
    'B', 'default',
)
c = os.environ.get('C')
d = os.environ['D']
e = typer.Option(None, envvar='E')
f = typer.Option(None, envvar=['F', 'G'])
h = 'H' in os.environ
i = getenv('I')
j = environ.get('J')
os.environ['WRITTEN'] = 'a write, not a read'
# os.getenv('IN_A_COMMENT')
"""
    assert python_reads(source) == set('ABCDEFGHIJ')


def test_the_python_scanner_refuses_a_name_it_cannot_read():
    with pytest.raises(ValueError, match='line 2'):
        python_reads('import os\nos.getenv(NAME)\n')


def test_the_compose_scanner_finds_every_interpolation():
    document = yaml.safe_load("""
services:
  s:
    # ${IN_A_COMMENT} is not read
    user: "${A}:${B:-1000}"
    environment:
      C: ${C-x}
      D: ${D:?set D}
      E: $E
      ESCAPED: $$NOT_READ
      SET_BY_COMPOSE: fixed
    ports:
      - "${F:-0.0.0.0}:8787:8787"
""")
    assert compose_reads(document) == set('ABCDEF')


def test_the_shell_scanner_skips_what_the_script_assigns():
    script = """#!/bin/sh
# $IN_A_COMMENT is not read
SLICE="${SYNC_SLICE:-500}"
WEB_DIR="${WEB_DIR:-/app/web}"
echo "slice=${SLICE} in $WEB_DIR as $USER_NAME, pid $$"
for repo in a b; do echo "$repo"; done
case "$1" in
    *"'"*) quote='`' ;;
esac
printf '%s' "$quote" "${TOKEN:+set}"
"""
    assert shell_reads(script) == {
        'SYNC_SLICE', 'WEB_DIR', 'USER_NAME', 'TOKEN',
    }


# --- what the example says ---------------------------------------------------

#: A setting, active or commented out: `KEY=value` or `# KEY=value`.
SETTING = re.compile(r'^(# )?([A-Za-z_]\w*)=(.*)$')


def lines() -> list[str]:
    return EXAMPLE.read_text(encoding='utf-8').splitlines()


def listed() -> dict[str, str]:
    """Every setting in the example, active or not, and the value shown."""
    return {
        match[2]: match[3]
        for match in map(SETTING.match, lines()) if match
    }


def commented_out() -> dict[str, str]:
    return {
        match[2]: match[3]
        for match in map(SETTING.match, lines()) if match and match[1]
    }


def active() -> dict[str, str | None]:
    """What python-dotenv makes of the file: the settings that are live."""
    return dict(dotenv_values(EXAMPLE))


#: Read by the code, compose or a deploy script, and deliberately not in
#: `.env.example`: none of them is something a user sets.
EXCLUDED = {
    'CLICKHOUSE_URL': (
        'compose sets it for the web container, which reaches ClickHouse '
        'by service name'
    ),
    'CLICKHOUSE_USER': (
        'compose sets it for the web container, which is always guest'
    ),
    'CLICKHOUSE_PASSWORD': (
        'compose sets it for the web container from '
        'CLICKHOUSE_GUEST_PASSWORD, which is the setting'
    ),
    'GENERATOR': (
        'compose sets it for the web container: the provenance label, '
        'naming this release, which a bump rewrites. A fact about the '
        'build, not a choice'
    ),
    'WEB_DIR': 'lets the tests run the web entrypoint in a scratch directory',
    'SQLITE_TMPDIR': (
        "SQLite's own: where it builds VACUUM's copy, read to check there "
        'is room for it before the HTTP cache is rebuilt'
    ),
    'TMPDIR': (
        "the system's: SQLite builds VACUUM's copy there when "
        'SQLITE_TMPDIR is unset'
    ),
    'COMPOSE_PROJECT_NAME': (
        "compose's own: it sets it to the project's name, the checkout "
        "directory's unless told otherwise, and names the collector's "
        'image after it'
    ),
}

#: The settings the example leaves active, each with why: a user must
#: fill it in, or compose genuinely needs it. Everything else is
#: commented out, so a copy changes nothing until it is edited.
ACTIVE = {
    'GITHUB_TOKEN': (
        'every collection stage needs one; empty is no token to Click, to '
        'the config and to compose alike'
    ),
    'UID': (
        'compose runs the collector, lock and cli containers as it, and '
        'the README calls it not optional; the CLI never reads it'
    ),
    'GID': 'as UID',
}


def test_the_example_points_no_token_at_an_endpoint():
    """A base URL decides where the key or token goes. Uncommenting one
    is a decision; copying the file is not."""
    assert 'ANTHROPIC_BASE_URL' not in active()
    assert 'OPENAI_BASE_URL' not in active()


def test_only_what_must_be_filled_in_is_active():
    live = active()
    assert sorted(set(live) - set(ACTIVE)) == []
    # Filled in by whoever copies it, never by the example.
    assert {name: value for name, value in live.items() if value} == {}


def test_every_line_reads_the_same_to_all_three_parsers():
    """python-dotenv, compose and systemd's EnvironmentFile= all read it.

    Their common ground is narrow. systemd keeps a trailing `# ...` as
    part of the value where the other two drop it; the three unquote and
    unescape differently; dotenv accepts an `export ` that is noise to
    the rest. A commented-out setting is held to the same rule, since
    uncommenting it is how it gets used.
    """
    plain = re.compile(r'[A-Z][A-Z0-9_]*=[^\s#\'"`\\]*')
    for number, line in enumerate(lines(), start=1):
        if not line or (line.startswith('#') and not SETTING.match(line)):
            continue
        assert plain.fullmatch(line.removeprefix('# ')), (
            f'.env.example:{number}: {line!r}'
        )
    # And python-dotenv agrees about which lines are live.
    written_live = {
        match[2] for match in map(SETTING.match, lines())
        if match and not match[1]
    }
    assert set(active()) == written_live


def test_every_setting_says_what_it_is_for():
    """One comment per setting, directly above it."""
    text = lines()
    for number, line in enumerate(text):
        if SETTING.match(line):
            above = text[number - 1] if number else ''
            assert above.startswith('#') and not SETTING.match(above), (
                f'.env.example:{number + 1}: {line!r} has no comment above it'
            )


def cli_settings() -> dict[str, object]:
    """What the CLI makes of the environment, setting by setting."""
    settings = ChatSBOMConfig()
    return {
        'admin': settings.get_db_config('admin').get_connection_params(),
        'guest': settings.get_db_config('guest').get_connection_params(),
        # Every use tests the token for truth: '' is no token.
        'github token': settings.github.token or None,
        # `chat` passes it on only when it is non-empty.
        'chat endpoint': os.getenv('ANTHROPIC_BASE_URL') or None,
        # The OpenAI client reads it itself, and takes '' as an address.
        'classify endpoint': os.getenv('OPENAI_BASE_URL'),
        'cost': chat.format_cost(1.0),
        # CHATSBOM_LOG_FORMAT, or ENV as its alias.
        'log format': log_format(),
    }


def test_an_unedited_copy_changes_nothing(env_file_workdir, monkeypatch):
    """Set but empty is not unset: an active `CLICKHOUSE_PORT=` would be
    `int('')`, and an active `CLICKHOUSE_ADMIN_PASSWORD=` would replace
    `admin` with nothing."""
    for name in listed():
        monkeypatch.delenv(name, raising=False)
    before = cli_settings()
    shutil.copy(EXAMPLE, env_file_workdir / '.env')

    loaded = config.load_env_file()

    assert loaded is not None
    assert loaded.resolve() == (env_file_workdir / '.env').resolve()
    assert cli_settings() == before


def test_compose_reads_every_active_setting_empty_as_unset():
    """`${X:-d}` and `${X:?e}` treat an empty X as unset; `${X-d}`,
    `${X}` and `$X` take the empty value at its word."""
    operators = re.compile(r'\$\{(\w+)(:?[-?+]|\})|\$(\w+)')
    document = yaml.safe_load(COMPOSE.read_text())
    for text in strings(document):
        for braced, operator, bare in operators.findall(text):
            if (braced or bare) in active():
                assert operator.startswith(':'), text


def test_a_commented_out_setting_shows_the_fallback_compose_and_scripts_use():
    """So uncommenting one to change it starts from what was in effect."""
    fallback = re.compile(r'\$\{(\w+):?-([^}]*)\}')
    shown = commented_out()
    texts = [
        *strings(yaml.safe_load(COMPOSE.read_text())),
        *(script.read_text() for script in (ROOT / 'deploy').glob('*.sh')),
    ]
    mismatched = {
        name: (shown[name], default)
        for text in texts
        for name, default in fallback.findall(text)
        if name in shown and shown[name] != default
    }
    assert mismatched == {}


def test_the_clickhouse_settings_shown_are_the_cli_defaults(monkeypatch):
    shown = {
        name: value for name, value in commented_out().items()
        if name.startswith('CLICKHOUSE_')
    }
    for name in shown:
        monkeypatch.delenv(name, raising=False)
    defaults = cli_settings()

    for name, value in shown.items():
        monkeypatch.setenv(name, value)

    assert cli_settings() == defaults


def test_every_variable_read_is_in_the_example_or_excluded():
    listing = listed()
    missing = [
        f'{name} (read in {", ".join(sorted(where))})'
        for name, where in sorted(environment_reads().items())
        if name not in listing and name not in EXCLUDED
    ]
    assert not missing, (
        'read, but neither in .env.example nor in EXCLUDED here:\n  '
        + '\n  '.join(missing)
    )


def test_every_exclusion_is_still_read_and_not_listed():
    """An exclusion for a variable nothing reads is a stale excuse, and
    one the example lists after all contradicts it."""
    reads = environment_reads()
    assert sorted(name for name in EXCLUDED if name not in reads) == []
    assert sorted(name for name in EXCLUDED if name in listed()) == []


def test_every_setting_listed_is_read_somewhere():
    """A setting nothing reads is a knob attached to nothing."""
    reads = environment_reads()
    assert sorted(name for name in listed() if name not in reads) == []


# --- what the docs say (#46) -------------------------------------------------
#
# `.env.example` is where every setting is described, and README says
# so. The docs name settings too, where they are used, and a name there
# is only worth reading if it is one something reads.

DOCS = ('README.md', 'DEPLOY.md', 'web/README.md')

#: A doc naming a variable as one to set or use: first in a table row,
#: `NAME=value`, `wrangler secret put NAME`, or `$NAME` in a command.
DOCUMENTED = re.compile(
    r'^\|\s*`([A-Z][A-Z0-9_]*)`\s*\|'
    r'|(?<![\w$./-])([A-Z][A-Z0-9_]*[A-Z0-9])='
    r'|\bsecret put ([A-Z][A-Z0-9_]*)'
    r'|\$\{?([A-Z][A-Z0-9_]*[A-Z0-9])',
    re.MULTILINE,
)

#: The shell's own, which a command in the docs may use.
SHELL = {'HOME', 'PATH', 'PWD', 'USER'}

#: Passed to the collector's services from `.env`, and deliberately not
#: in DEPLOY.md's table of what tunes them: an account or a token, which
#: the text around the table, and `.env.example`, say how to set.
CREDENTIALS = {
    'GITHUB_TOKEN', 'CHATSBOM_DEPGRAPH_TOKENS',
    'CLICKHOUSE_ADMIN_USER', 'CLICKHOUSE_ADMIN_PASSWORD',
}


def documented(text: str) -> set[str]:
    return {
        next(name for name in match.groups() if name)
        for match in DOCUMENTED.finditer(text)
    }


def compose_sets() -> set[str]:
    """Every variable compose puts in a container's environment."""
    document = yaml.safe_load(COMPOSE.read_text())
    return {
        name
        for service in document['services'].values()
        for name in (service.get('environment') or {})
    }


def worker_reads() -> set[str]:
    """Every setting the dashboard's Worker reads: `env.NAME`."""
    names: set[str] = set()
    for source in sorted((ROOT / 'web' / 'src').rglob('*.ts*')):
        names |= set(
            re.findall(r'\benv\.([A-Z][A-Z0-9_]*)\b', source.read_text()),
        )
    return names


def deploy_table() -> dict[str, str]:
    """DEPLOY.md's table of what tunes the collector: name, default."""
    lines = (ROOT / 'DEPLOY.md').read_text(encoding='utf-8').splitlines()
    start = lines.index('| Variable | Default | Meaning |')
    row = re.compile(r'\|\s*`(\w+)`\s*\|\s*`([^`]*)`\s*\|')
    table = {}
    for line in lines[start + 2:]:
        if not line.startswith('|'):
            break
        match = row.match(line)
        assert match, f'DEPLOY.md: {line!r}'
        table[match[1]] = match[2]
    return table


def collect_profile_settings() -> dict[str, str]:
    """What compose passes the `collect` profile's services from `.env`,
    with the fallback it uses, bar the credentials."""
    document = yaml.safe_load(COMPOSE.read_text())
    passed = re.compile(r'\$\{(\w+):-([^}]*)\}')
    found = {}
    for service in document['services'].values():
        if 'collect' not in service.get('profiles', []):
            continue
        for name, value in (service.get('environment') or {}).items():
            match = passed.fullmatch(str(value))
            if match and match[1] == name and name not in CREDENTIALS:
                found[name] = match[2]
    return found


def test_the_docs_scanner_finds_every_way_of_naming_a_setting():
    text = """
| `TABLE_ROW` | `1` | a row |
| `lower` | not a setting |
    WEB_BIND=172.17.0.1 docker compose up -d
`X_LOCAL_EXPLORER=false`, and export ANTHROPIC_AUTH_TOKEN="..."
npx wrangler secret put SECRET_NAME
for f in "$D"/*.sql; do echo "$EXPANDED ${BRACED:-x}"; done
a URL?q=1, a_lower=1, obj.ATTR=2, --flag-NAME=3
"""
    assert documented(text) == {
        'TABLE_ROW', 'WEB_BIND', 'X_LOCAL_EXPLORER', 'ANTHROPIC_AUTH_TOKEN',
        'SECRET_NAME', 'EXPANDED', 'BRACED',
    }


def test_every_variable_the_docs_name_is_one_something_reads():
    known = (
        set(environment_reads()) | set(listed()) | compose_sets()
        | worker_reads() | SHELL
    )
    unknown = {}
    for doc in DOCS:
        named = documented((ROOT / doc).read_text(encoding='utf-8'))
        unknown[doc] = sorted(named - known)
    assert unknown == {doc: [] for doc in DOCS}, (
        'a doc names a variable nothing reads: a setting since removed, or '
        'misspelt. A variable a command sets for its own use can be lower '
        "case; one of the shell's own belongs in SHELL here"
    )


def test_deploys_table_is_what_the_collectors_take_from_the_env_file():
    """Every setting compose hands the collector and depgraph services,
    with compose's own fallback, and nothing else (#46: the table had
    left out PRUNE_EVERY_SLICES, and every one of the depgraph
    service's)."""
    assert deploy_table() == collect_profile_settings()
