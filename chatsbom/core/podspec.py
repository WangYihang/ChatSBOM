"""What a CocoaPods podspec declares, read from its text.

A `.podspec` is how a CocoaPods library states what it depends on, as a
`.gemspec` is for a gem. Syft reads `Podfile.lock` and nothing else of
CocoaPods, 1.41.2 or 1.52.0, and GitHub's dependency graph does not
support CocoaPods at all, so a pod's own dependencies were in no source.
jasnig/ZJScrollPageView, an Objective-C library whose only manifest is
`ZJScrollPageView.podspec`, had none fetched: discovery did not know
the name (#55 pilot).

The warehouse stores each declaration as an artifact row with `source =
'manifest'`, as it does a Gradle build's (`core/gradle.py`): what the
file states, never a resolution. Every row is `direct`, since the spec
declares it; its version is the requirement as written (`~> 2.0`), a
`constraint`, or `unversioned` when there is none.

What is read:

- `.podspec`, which is Ruby: every `<spec>.dependency 'Name'[, 'req',
  …]` call, in the spec and in its subspecs, whatever the block
  variable is called (`s.`, `spec.`, `ss.`);
- `.podspec.json`, the same spec as JSON (what `pod ipc spec` writes and
  the Specs repository stores): `dependencies` and each subspec's.

A subspec's dependency on its own pod (`s.dependency 'Foo/Core'` in
`Foo.podspec`) names the pod itself and is not a dependency. A
dependency on another pod's subspec, `AFNetworking/NSURLSession`, is on
that pod, `AFNetworking`: pods are published whole, and the purl names
the pod. Nothing else is evaluated: a dependency added in a loop or
from a variable is not seen.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import quote

SUFFIXES: tuple[str, ...] = ('.podspec', '.podspec.json')

#: `artifacts.type` and purl type of a manifest row: CocoaPods' own
#: name, which `core/ecosystems.py` also files Syft's `pod` under.
ECOSYSTEM = 'cocoapods'

#: `artifacts.found_by` of a manifest row.
FOUND_BY = 'chatsbom-podspec'

_DEPENDENCY_RE = re.compile(
    r'''\.dependency\s*\(?\s*(['"])(?P<name>[^'"\n]+)\1(?P<rest>[^\n#]*)''',
)
_STRING_RE = re.compile(r'''(['"])([^'"\n]*)\1''')
_NAME_RE = re.compile(r'''\.name\s*=\s*(['"])(?P<name>[^'"\n]+)\1''')


@dataclass(frozen=True)
class Pod:
    """One pod a spec depends on."""

    name: str
    #: The requirement as written, `~> 2.0` or `>= 1.0, < 2.0`; '' for
    #: none.
    requirement: str = ''

    @property
    def purl(self) -> str:
        purl = f'pkg:cocoapods/{quote(self.name, safe=".-_~+")}'
        if self.requirement:
            purl += '@' + quote(self.requirement, safe='.-_~')
        return purl


def is_podspec(path: str) -> bool:
    return path.endswith(SUFFIXES)


def _pod(name: str) -> str:
    """The pod a dependency names: `AFNetworking/NSURLSession` is on
    `AFNetworking`."""
    return name.strip().split('/', 1)[0]


def read(path: str, text: str) -> list[Pod]:
    """The pods one podspec depends on, in order, each once."""
    own = _own_name(path, text)
    if path.endswith('.podspec.json'):
        found = _from_json(text)
    else:
        found = _from_ruby(text)
    out: list[Pod] = []
    seen: set[tuple[str, str]] = set()
    for name, requirement in found:
        pod = _pod(name)
        if not pod or pod == own:
            continue
        key = (pod, requirement)
        if key in seen:
            continue
        seen.add(key)
        out.append(Pod(pod, requirement))
    return out


def _own_name(path: str, text: str) -> str:
    """The pod this spec is: its `name`, else its file name."""
    if path.endswith('.podspec.json'):
        try:
            data = json.loads(text)
        except ValueError:
            data = None
        if isinstance(data, dict) and isinstance(data.get('name'), str):
            return data['name']
    else:
        match = _NAME_RE.search(_uncommented(text))
        if match:
            return match.group('name')
    base = path.rpartition('/')[2]
    for suffix in SUFFIXES[::-1]:
        if base.endswith(suffix):
            return base[:-len(suffix)]
    return base


def _uncommented(text: str) -> str:
    """Ruby with its `#` line comments blanked, strings respected."""
    lines = []
    for line in text.splitlines():
        quote_ = ''
        for i, c in enumerate(line):
            if quote_:
                if c == quote_:
                    quote_ = ''
            elif c in ('"', "'"):
                quote_ = c
            elif c == '#' and not line.startswith('#{', i):
                line = line[:i]
                break
        lines.append(line)
    return '\n'.join(lines)


def _from_ruby(text: str) -> Iterable[tuple[str, str]]:
    for match in _DEPENDENCY_RE.finditer(_uncommented(text)):
        requirements = [
            m.group(2).strip() for m in _STRING_RE.finditer(match.group('rest'))
        ]
        yield match.group('name'), ', '.join(r for r in requirements if r)


def _from_json(text: str) -> Iterable[tuple[str, str]]:
    try:
        data = json.loads(text)
    except ValueError:
        return
    yield from _json_spec(data, 0)


def _json_spec(spec: object, depth: int) -> Iterable[tuple[str, str]]:
    if not isinstance(spec, dict) or depth > 8:
        return
    dependencies = spec.get('dependencies')
    if isinstance(dependencies, dict):
        for name, requirements in dependencies.items():
            if isinstance(requirements, str):
                requirements = [requirements]
            if not isinstance(requirements, list):
                requirements = []
            yield str(name), ', '.join(
                str(r).strip() for r in requirements if str(r).strip()
            )
    for subspec in spec.get('subspecs') or []:
        yield from _json_spec(subspec, depth + 1)


def declarations(
    manifests: Iterable[tuple[str, str | None]],
) -> list[tuple[str, Pod]]:
    """`(path, pod)` for every dependency the repository's podspecs
    declare. Specs under a vendored tree were never fetched
    (`core/discovery`)."""
    out: list[tuple[str, Pod]] = []
    for path, text in sorted(
        (p, t) for p, t in manifests if t is not None and is_podspec(p)
    ):
        assert text is not None
        out.extend((path, pod) for pod in read(path, text))
    return out
