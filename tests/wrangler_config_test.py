"""web/wrangler.jsonc binds what the Worker reads, and names what else it
reads (#46).

It bound an R2 bucket, `DATA`, that nothing had read since the Parquet
left the serving path and that DEPLOY.md never has anyone create, under
comments describing a `/data/*` route that no longer existed. And it
listed the Worker's secrets, but not the ClickHouse settings that make
`/api/q` answer from ClickHouse rather than D1.

What the Worker reads is found as it reads it: every setting and binding
is `env.NAME` in its sources, typed by the `*Env` interfaces.
"""
import json
import re
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
WRANGLER = ROOT / 'web' / 'wrangler.jsonc'
SOURCES = ROOT / 'web' / 'src'


def split_jsonc(text: str) -> tuple[str, str]:
    """JSONC as JSON, and the text of its comments.

    Comments are cut outside strings only: `"https://..."` holds a `//`.
    """
    data: list[str] = []
    comments: list[str] = []
    at = 0
    while at < len(text):
        char = text[at]
        if char == '"':
            end = at + 1
            while text[end] != '"':
                end += 2 if text[end] == '\\' else 1
            data.append(text[at:end + 1])
            at = end + 1
        elif text.startswith('//', at):
            end = text.find('\n', at)
            end = len(text) if end == -1 else end
            comments.append(text[at + 2:end])
            at = end
        elif text.startswith('/*', at):
            end = text.index('*/', at)
            comments.append(text[at + 2:end])
            at = end + 2
        else:
            data.append(char)
            at += 1
    # wrangler takes a trailing comma, as JSONC does; json does not.
    plain = re.sub(r',(\s*[}\]])', r'\1', ''.join(data))
    return plain, '\n'.join(comments)


def config() -> dict[str, Any]:
    plain, _ = split_jsonc(WRANGLER.read_text(encoding='utf-8'))
    loaded: dict[str, Any] = json.loads(plain)
    return loaded


def comments() -> str:
    return split_jsonc(WRANGLER.read_text(encoding='utf-8'))[1]


def bindings(wrangler: dict[str, Any]) -> set[str]:
    """Every name the configuration puts in the Worker's `env`."""
    names = set(wrangler.get('vars', {}))
    if 'assets' in wrangler and 'binding' in wrangler['assets']:
        names.add(wrangler['assets']['binding'])
    names |= {limit['name'] for limit in wrangler.get('ratelimits', [])}
    names |= {
        binding['name']
        for binding in wrangler.get('durable_objects', {}).get('bindings', [])
    }
    for kind in ('d1_databases', 'kv_namespaces', 'r2_buckets', 'services'):
        names |= {binding['binding'] for binding in wrangler.get(kind, [])}
    return names


def worker_reads() -> set[str]:
    """Every `env.NAME` in the Worker's and the page's sources."""
    names: set[str] = set()
    for source in sorted(SOURCES.rglob('*.ts*')):
        names |= set(
            re.findall(r'\benv\.([A-Z][A-Z0-9_]*)\b', source.read_text()),
        )
    return names


def test_the_reader_splits_comments_from_strings():
    plain, said = split_jsonc(
        '{\n  // a comment, "quoted"\n  "url": "https://x/*y*/",'
        ' /* inline */ "a": [1,],\n}\n',
    )
    assert json.loads(plain) == {'url': 'https://x/*y*/', 'a': [1]}
    assert 'a comment' in said and 'inline' in said


def test_the_readers_find_what_they_look_for():
    """So an empty set cannot pass the tests below."""
    assert {'ASSETS', 'DB', 'SPEND_COUNTER'} <= bindings(config())
    assert {'ASSETS', 'DB', 'ANTHROPIC_API_KEY'} <= worker_reads()


def test_no_r2_bucket_is_bound():
    """The dataset is served from D1 or ClickHouse, never from R2."""
    assert 'r2_buckets' not in config()


def test_every_binding_is_one_the_worker_reads():
    """A binding nothing reads is a resource a deploy provisions, or
    looks for, for nothing."""
    assert sorted(bindings(config()) - worker_reads()) == []


def test_every_setting_the_worker_reads_is_bound_or_named_here():
    """What is not bound here — the secrets, and what compose passes
    `wrangler dev` — is named in a comment, so this file is the one
    place to learn what the Worker can be given."""
    said = comments()
    missing = [
        name for name in sorted(worker_reads() - bindings(config()))
        if not re.search(rf'\b{name}\b', said)
    ]
    assert missing == []
