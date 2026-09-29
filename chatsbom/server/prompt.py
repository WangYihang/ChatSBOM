"""What the chat's model is told: the system prompt, and its tools (#140).

Ported from the Worker's `web/src/prompt.ts`, which the Worker sent with
every turn of a question the page's loop made. Here the loop is the
server's (#128, section 2.6), and they go with every model call it
makes, as DeepSeek's Chat Completions API takes them: the tools as
functions, each with a JSON Schema for its arguments.

Both are constants, the same bytes on every request, and every request
starts with them: DeepSeek caches the prefixes requests share, and bills
a cached prompt token at a fiftieth of one it computes (`pricing`), so
the longest part of every question's first turn, and all of its later
ones up to their newest tokens, comes from the cache. Nothing about a
question, its reader or the hour may go in them.

A description is what the model believes about a result, so it has to
say what the dataset does. A search described as matching substrings,
that matched prefixes, let the model conclude a package did not exist
when it had only begun the name differently. Each `default` is the
dataset's own, since a tool passes no limit when the model names none,
and `tests/server_tools_test.py` holds each to it.

Strict schemas are left out: DeepSeek takes them only at its beta
endpoint, and only with every property required. Every argument is
checked again when the tool runs (`tools`).
"""
from typing import Any

from chatsbom.models.relationship import RELATIONSHIPS
from chatsbom.server.tools import RESULT_CHARS
from chatsbom.server.tools import RESULT_ROWS


def _tool(
    name: str,
    description: str,
    properties: dict[str, Any],
    required: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        'type': 'function',
        'function': {
            'name': name,
            'description': description,
            'parameters': {
                'type': 'object',
                'properties': properties,
                'required': list(required),
                'additionalProperties': False,
            },
        },
    }


#: One for each of `tools.TOOL_NAMES`, in its order.
TOOLS: tuple[dict[str, Any], ...] = (
    _tool(
        'dependents_of',
        'Count the repositories that depend on a package, and list the most '
        'starred of them. `total` is how many repositories depend on it: the '
        'real count, whatever the number of rows. `direct_total` is how many '
        'of those declare it in their own manifest; it is left out when '
        'direct_only already makes `total` that number. `rows` is a sample, '
        'most starred first, with a row per repository, version and '
        'relationship, so a repository can appear more than once; never '
        'report its length as a count. Set direct_only when the question is '
        'about projects that chose the package themselves, rather than '
        'inheriting it through another dependency — of the 118 repositories '
        'whose SBOM lists `mail`, only 17 declare it.',
        {
            'name': {
                'type': 'string',
                'description': 'Exact package name, as its ecosystem spells it.',
            },
            'type': {
                'type': 'string',
                'description': (
                    'Ecosystem to scope to, spelled as ecosystems_for reports '
                    'it, e.g. gem, npm, maven. A name is not unique across '
                    'ecosystems: `mail` is a Ruby gem with 118 dependants and '
                    'also a Maven artifactId with 6.'
                ),
            },
            'language': {
                'type': 'string',
                'description': (
                    "Optional filter on the repository's GitHub language, "
                    'lowercased, e.g. ruby: one of the twelve '
                    'language_coverage lists, or `other`, or `none`. It is '
                    "not the package's ecosystem; use `type` for that."
                ),
            },
            'direct_only': {
                'type': 'boolean',
                'description': (
                    'Only repositories whose own manifest declares it, in the '
                    'rows and in `total`.'
                ),
            },
            'limit': {
                'type': 'integer',
                'description': (
                    f'Rows to list, default 50, at most {RESULT_ROWS}. It '
                    'changes the sample, never `total`.'
                ),
            },
        },
        required=('name',),
    ),
    _tool(
        'ecosystems_for',
        'Which ecosystems a package name appears in, with counts. Call '
        'this before reporting a dependant count: a name shared across '
        'ecosystems is two different packages, and summing them reports '
        'dependants of something that does not exist.',
        {'name': {'type': 'string', 'description': 'Exact package name.'}},
        required=('name',),
    ),
    _tool(
        'search_packages',
        'Find package names that begin with a prefix, ranked by how many '
        'repositories use them. Matching is by prefix, not substring: `mail` '
        'would find `mailer` but never `actionmailer`, so a name that does '
        'not turn up may only begin differently. A name in several '
        'ecosystems can come back as a row for each. Use this first when '
        'unsure of the exact name.',
        {
            'fragment': {
                'type': 'string',
                'description': (
                    'The start of the name, as its ecosystem spells it; '
                    'matched as a prefix.'
                ),
            },
            'limit': {
                'type': 'integer',
                'description': (
                    f'Names to return, default 20, at most {RESULT_ROWS}.'
                ),
            },
        },
        required=('fragment',),
    ),
    _tool(
        'top_packages',
        'The most depended-upon packages. Prefer direct_only: the unfiltered '
        'ranking is dominated by npm micro-packages (semver, debug, ms) that '
        'no project asks for by name. The ranking is stored only so deep, so '
        'it can end before `limit` does.',
        {
            'ecosystem': {
                'type': 'string',
                'description': (
                    'Optional ecosystem to rank within, e.g. npm, maven, '
                    'pypi, composer, go, gem, cargo. Omitted, the ranking is '
                    'the whole corpus, each repository counted once.'
                ),
            },
            'direct_only': {
                'type': 'boolean',
                'description': 'Count only declared dependencies.',
            },
            'limit': {
                'type': 'integer',
                'description': f'Max rows, default 50, at most {RESULT_ROWS}.',
            },
        },
    ),
    _tool(
        'version_spread',
        'Which resolved versions of a package are in use, most used first, '
        'with the repositories on each. Answers "are projects on the current '
        'release". A version recorded as a manifest constraint (`>= 2.0`) or '
        'not recorded at all is not listed: `constrained` and `unversioned` '
        'count what was set aside. Not scoped to an ecosystem, so a name used '
        'in two mixes their versions.',
        {
            'name': {'type': 'string', 'description': 'Exact package name.'},
            'limit': {
                'type': 'integer',
                'description': (
                    f'Max versions, default 10, at most {RESULT_ROWS}.'
                ),
            },
        },
        required=('name',),
    ),
    _tool(
        'language_coverage',
        'Per GitHub language (the twelve most common, `other` and `none`): '
        'repositories in the snapshot, and how many have dependency data '
        'from each collector. Call this before comparing languages: '
        'coverage is uneven, so a raw cross-language count is not a '
        'like-for-like comparison.',
        {},
    ),
    _tool(
        'ecosystem_coverage',
        'Per ecosystem (npm, maven, pypi, …): repositories whose manifests '
        'or dependencies are of it, and how many each collector covers. A '
        'repository counts under every ecosystem it has, so the rows '
        'overlap and must not be summed. Call this before comparing '
        'ecosystems.',
        {},
    ),
)

SYSTEM_PROMPT = '\n'.join([
    'You answer questions about open-source dependency data using the tools',
    'provided. You have no other access to the dataset — there is no SQL',
    'escape hatch, so compose the tools instead.',
    '',
    'The dataset is a snapshot of public GitHub repositories: each has an',
    'SBOM, and each dependency is labelled by how it arrived:',
    f"  {' | '.join(RELATIONSHIPS)}",
    '',
    "`direct` means the project's own manifest declares the package;",
    '`transitive` means another dependency pulled it in; `unknown` means no',
    'manifest could be read, so the question is unanswered rather than',
    'answered "no". This distinction usually decides which number a person',
    'actually wants — "who uses X" nearly always means direct.',
    '',
    'Two cautions to pass on rather than hide:',
    '- A package name is not unique across ecosystems. `mail` is a Ruby gem',
    '  with 118 dependants and also a Maven artifactId with 6; summing them',
    '  reports 124 dependants of something that does not exist. Call',
    '  ecosystems_for first when a name could be ambiguous, and say which',
    '  ecosystem a number refers to.',
    '- Versions may be constraints (`>= 0`) rather than resolutions, when',
    '  they came from a manifest instead of a lockfile.',
    '- Coverage differs by language and by ecosystem. Call language_coverage',
    '  or ecosystem_coverage before any such comparison and state the',
    '  denominators: every repository in the snapshot, collected or not.',
    '- A repository can have several ecosystems (an npm front end and a',
    '  Maven back end), so per-ecosystem repository counts overlap: never',
    '  add them up to get a corpus total.',
    '',
    'Rows are samples, never counts. A result holds at most',
    f'{RESULT_ROWS} rows and {RESULT_CHARS} characters: `rows_shown` says',
    'how many it holds, and `truncated` with `rows_dropped` says when some',
    'were cut to fit. A number of dependants comes from `total` or',
    '`direct_total`, never from how many rows came back.',
    '',
    "A question may come after the reader's earlier questions in this",
    'conversation and the answers they were given, as their own record of',
    'it. Read them for what the question refers to, but take no number',
    'from them: answer from what the tools return.',
    '',
    'Be concise. Give the numbers you found, name the repositories when',
    'there are few enough to list, and say plainly when the data cannot',
    'answer the question.',
])
