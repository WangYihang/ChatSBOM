"""Reverse index from package name to framework.

`db export` used to walk every framework's package list for every row it
wrote — O(rows x frameworks x packages) to answer a question that is a
dict lookup. `github classify` did the same per repository. Building the
index once turns both into O(1) per package.
"""
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache

from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory


@dataclass(frozen=True, slots=True)
class FrameworkIndex:
    """Immutable lookup tables over the framework definitions."""

    _by_package: Mapping[str, Framework]
    _by_language: Mapping[Language, tuple[Framework, ...]]
    _packages_by_framework: Mapping[Framework, tuple[str, ...]]

    # -- construction -------------------------------------------------------

    @staticmethod
    @cache
    def build() -> 'FrameworkIndex':
        """Build the index once per process."""
        by_package: dict[str, Framework] = {}
        packages_by_framework: dict[Framework, tuple[str, ...]] = {}

        # Iterate the enum so declaration order decides ties deterministically.
        for framework in Framework:
            packages = tuple(
                FrameworkFactory.create(framework).get_package_names(),
            )
            packages_by_framework[framework] = packages
            for package in packages:
                by_package.setdefault(package.lower(), framework)

        by_language: dict[Language, tuple[Framework, ...]] = {}
        for language in Language:
            try:
                handler = LanguageFactory.get_handler(language)
            except ValueError:
                continue
            by_language[language] = tuple(handler.get_frameworks())

        return FrameworkIndex(
            _by_package=by_package,
            _by_language=by_language,
            _packages_by_framework=packages_by_framework,
        )

    # -- lookup -------------------------------------------------------------

    @property
    def packages(self) -> tuple[str, ...]:
        """Every indexed package name."""
        return tuple(self._by_package)

    def framework_for(self, package: str) -> Framework | None:
        """The framework a package belongs to, if any."""
        return self._by_package.get(package.lower())

    def detect(self, packages: Iterable[str]) -> Framework | None:
        """The framework used by a project, given its package names.

        When a project uses more than one tracked framework, the winner is
        the one declared first in `Framework` — a stable rule, unlike
        "whichever the query returned first".
        """
        present = {p.lower() for p in packages}
        for framework in Framework:
            if any(p.lower() in present for p in self._packages_by_framework[framework]):
                return framework
        return None

    def frameworks_for_language(self, language: Language) -> tuple[Framework, ...]:
        return self._by_language.get(language, ())

    def packages_for_language(self, language: Language) -> tuple[str, ...]:
        return tuple(
            package
            for framework in self.frameworks_for_language(language)
            for package in self._packages_by_framework[framework]
        )

    def packages_of(self, framework: Framework) -> tuple[str, ...]:
        return self._packages_by_framework.get(framework, ())

    def as_framework_map(self) -> dict[str, list[str]]:
        """Shape expected by `QueryRepository.get_frameworks_for_repositories`."""
        return {
            str(framework): list(packages)
            for framework, packages in self._packages_by_framework.items()
        }
