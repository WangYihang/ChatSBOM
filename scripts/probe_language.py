#!/usr/bin/env python3
"""Is a language worth adding to the corpus? Measure before deciding.

Adding a language costs collection quota and disk, and the interesting
cases are the ones where it buys nothing. C and C++ are the example this
was written for: they have no single package manager, so the question
"can this pipeline extract dependencies from them" has a real chance of
answering no.

Two independent measurements, because the first one alone is misleading:

1. **Manifests.** One tree request per repository, checking for files a
   parser could read -- `conanfile.txt`, `vcpkg.json`, and the things
   that look like manifests but are not (`CMakeLists.txt` is imperative
   code, `.gitmodules` names repositories rather than packages).

2. **GitHub's dependency graph.** Whether GitHub has data, *and what
   ecosystems that data is in*. This second half is the one that
   matters and the one that is easy to skip: for C++ the graph answers
   for 88% of repositories, which reads like good coverage until you
   look at what is in it.

Usage:

    GITHUB_TOKEN=... python scripts/probe_language.py 'C++' --repos 200

Costs roughly one request per repository for the trees, plus one per
sampled repository for the graphs. The dependency-graph endpoint is
metered separately at about 100/hour, so `--graph-sample` is small by
default.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import requests

#: Filename -> what kind of dependency declaration it is.
#:
#: The distinction that matters is manifest vs not. `CMakeLists.txt`
#: declares dependencies in imperative CMake, so reading it means
#: evaluating CMake; `.gitmodules` names dependencies by repository URL
#: with a commit sha for a version, which is not a package at all.
DECLARATIONS = {
    'conanfile.txt': 'conan',
    'conanfile.py': 'conan',
    'conan.lock': 'conan',
    'vcpkg.json': 'vcpkg',
    'cmakelists.txt': 'cmake',
    'meson.build': 'meson',
    '.gitmodules': 'submodules',
}

#: Kinds a parser could actually read into (name, version) pairs.
PARSEABLE = {'conan', 'vcpkg'}

#: Ecosystems that say nothing about the language being probed.
#:
#: GitHub's graph reports whatever it finds in the repository, and in a
#: C++ project that is the CI workflow files, the docs site's
#: `package.json`, and the build scripts' `requirements.txt`.
INCIDENTAL = {'githubactions', 'github'}


@dataclass(frozen=True)
class Probed:
    """One repository, and what it declares.

    A dataclass rather than a dict because the two questions asked of it
    -- "is this parseable" and "what is in its graph" -- read the same
    two fields, and a dict makes every such read a lookup that could be
    misspelled.
    """

    repo: str
    stars: int
    declarations: frozenset[str] = field(default_factory=frozenset)

    @property
    def parseable(self) -> bool:
        """Whether a parser could read a manifest here.

        `CMakeLists.txt` and `.gitmodules` do not count: one is
        imperative code, the other names repositories rather than
        packages.
        """
        return bool(PARSEABLE & self.declarations)


def session(token: str) -> requests.Session:
    handle = requests.Session()
    handle.headers.update({
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
    })
    return handle


def search(api: requests.Session, language: str, stars: int, page: int):
    response = api.get(
        'https://api.github.com/search/repositories',
        params={
            'q': f'language:{language} stars:>={stars}',
            'sort': 'stars', 'order': 'desc',
            'per_page': '100', 'page': str(page),
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.json().get('items', [])


def declarations(api: requests.Session, owner: str, repo: str, branch: str):
    """What dependency declarations the repository's tree contains."""
    response = api.get(
        f'https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}',
        params={'recursive': '1'}, timeout=60,
    )
    if response.status_code != 200:
        return None
    found = set()
    for entry in response.json().get('tree', []):
        name = str(entry.get('path', '')).rsplit('/', 1)[-1].lower()
        kind = DECLARATIONS.get(name)
        if kind:
            found.add(kind)
    return found


def graph_ecosystems(api: requests.Session, owner: str, repo: str):
    """Ecosystems GitHub's dependency graph reports, per repository.

    Per repository rather than per package: one repository with a large
    npm tree would otherwise dominate the totals and make the whole
    sample look like a JavaScript corpus.
    """
    response = api.get(
        f'https://api.github.com/repos/{owner}/{repo}'
        '/dependency-graph/sbom',
        timeout=60,
    )
    if response.status_code != 200:
        return None
    # The first package is the repository itself.
    packages = response.json().get('sbom', {}).get('packages', [])[1:]
    if not packages:
        return set()
    found = set()
    for package in packages:
        for ref in package.get('externalRefs', []):
            if ref.get('referenceType') != 'purl':
                continue
            locator = str(ref.get('referenceLocator', ''))
            if locator.startswith('pkg:'):
                found.add(locator.split(':')[1].split('/')[0])
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('language', help="GitHub language, e.g. 'C++'")
    parser.add_argument('--repos', type=int, default=200)
    parser.add_argument('--stars', type=int, default=1000)
    parser.add_argument('--graph-sample', type=int, default=30)
    parser.add_argument('--out', default='')
    args = parser.parse_args()

    # Without the whitespace around it: a token read from a file keeps
    # the file's line ending, and requests refuses a header holding one
    # with an error that quotes it, token and all (#113).
    token = (os.environ.get('GITHUB_TOKEN') or '').strip()
    if not token:
        print('set GITHUB_TOKEN', file=sys.stderr)
        return 2
    if any(unicodedata.category(character) == 'Cc' for character in token):
        print(
            'GITHUB_TOKEN holds a control character, which no token '
            'holds: set it again',
            file=sys.stderr,
        )
        return 2
    api = session(token)

    repos: list[dict[str, Any]] = []
    for page in range(1, args.repos // 100 + 2):
        if len(repos) >= args.repos:
            break
        repos += search(api, args.language, args.stars, page)
    repos = repos[:args.repos]
    print(f'{args.language}: {len(repos)} repositories, stars >= {args.stars}')

    kinds: Counter[str] = Counter()
    rows: list[Probed] = []
    for item in repos:
        owner = item['owner']['login']
        name = item['name']
        found = declarations(
            api, owner, name, item.get('default_branch') or 'HEAD',
        )
        if found is None:
            continue
        for kind in found or {'nothing'}:
            kinds[kind] += 1
        rows.append(
            Probed(
                repo=f'{owner}/{name}',
                stars=int(item.get('stargazers_count') or 0),
                declarations=frozenset(found),
            ),
        )

    checked = len(rows)
    if not checked:
        print('nothing readable', file=sys.stderr)
        return 1

    print(f'\ndeclarations found, across {checked} repositories:')
    for kind, count in kinds.most_common():
        print(f'  {kind:<12}{count:>5}  {100 * count / checked:5.1f}%')

    parseable = len([row for row in rows if row.parseable])
    print(f'\n  a manifest a parser could read  '
          f'{parseable:>4}  {100 * parseable / checked:5.1f}%')

    # The second measurement. Sampled, because this endpoint is metered
    # separately and far more tightly than the core quota.
    sample = rows[:args.graph_sample]
    per_repo: Counter[str] = Counter()
    with_data = 0
    for row in sample:
        owner, name = row.repo.split('/')
        ecosystems = graph_ecosystems(api, owner, name)
        if not ecosystems:
            continue
        with_data += 1
        for ecosystem in ecosystems:
            per_repo[ecosystem] += 1

    if with_data:
        print(f'\ndependency graph, {with_data} of {len(sample)} sampled '
              f'repositories had data:')
        for ecosystem, count in per_repo.most_common():
            mark = '  (incidental)' if ecosystem in INCIDENTAL else ''
            print(f'  {ecosystem:<16}{count:>3}/{with_data}  '
                  f'{100 * count / with_data:5.0f}%{mark}')
        native = len([row for row in sample if row.parseable])
        print(f'\n  Read the ecosystem column, not the coverage number: a '
              f'graph\n  full of `githubactions` and `npm` describes the '
              f'repository\'s CI\n  and docs tooling, not what the '
              f'{args.language} code depends on.')
        print(f'  ({native} of the {len(sample)} sampled carry a '
              f'{args.language} manifest at all.)')

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as handle:
            json.dump(
                {
                    'language': args.language, 'checked': checked,
                    'declarations': dict(kinds),
                    'graph_per_repo': dict(per_repo),
                    'rows': [
                        {
                            'repo': row.repo, 'stars': row.stars,
                            'declarations': sorted(row.declarations),
                        }
                        for row in rows
                    ],
                }, handle, indent=2,
            )
        print(f'\nwrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
