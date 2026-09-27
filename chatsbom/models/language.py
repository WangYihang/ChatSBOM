from abc import ABC
from abc import abstractmethod
from collections.abc import Iterable
from enum import Enum

from chatsbom.models.framework import Framework


class Language(str, Enum):
    GO = 'go'
    PYTHON = 'python'
    JAVA = 'java'
    RUST = 'rust'
    RUBY = 'ruby'
    NODE = 'node'
    PHP = 'php'
    JAVASCRIPT = 'javascript'
    TYPESCRIPT = 'typescript'
    #: Not a GitHub language: every repository in the unfiltered sweep
    #: (`01-github-search/all.jsonl`) that none of the languages above
    #: took. GitHub labels a repository by its largest language, so a
    #: Django app with a Svelte frontend is "Svelte" and a Spring Boot
    #: app with a JavaScript UI is "JavaScript" — and neither entered
    #: the pipeline. `repositories.language` keeps GitHub's label; this
    #: value names only the pipeline lane. See `chatsbom data other`.
    OTHER = 'other'

    def __str__(self) -> str:
        return self.value.lower()

    def __repr__(self) -> str:
        return self.value.lower()


class BaseLanguage(ABC):
    @abstractmethod
    def get_sbom_paths(self) -> list[str]:
        ...

    @abstractmethod
    def get_frameworks(self) -> list[Framework]:
        ...

    @abstractmethod
    def get_source_extensions(self) -> list[str]:
        ...


class Go(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'go.mod',
            'go.sum',
            'vendor/modules.txt',
            'Gopkg.toml',
            'Gopkg.lock',
            'glide.yaml',
            'glide.lock',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.GIN,
            Framework.ECHO,
            Framework.CHI,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.go']


class Python(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'requirements.txt',
            'uv.lock',
            'poetry.lock',
            'pyproject.toml',
            'Pipfile.lock',
            'Pipfile',
            'environment.yml',
            'setup.py',
            'setup.cfg',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.FLASK,
            Framework.DJANGO,
            Framework.FASTAPI,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.py']


class Java(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'pom.xml',
            'build.gradle',
            'build.gradle.kts',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.SPRINGBOOT,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.java', '.kt', '.scala']


class Rust(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'Cargo.toml',
            'Cargo.lock',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.ACTIX,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.rs']


class Ruby(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'Gemfile',
            'Gemfile.lock',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.RAILS,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.rb']


class Node(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'package-lock.json',
            'yarn.lock',
            'pnpm-lock.yaml',
            'package.json',
            'npm-shrinkwrap.json',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.EXPRESS,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.js', '.ts', '.jsx', '.tsx', '.mjs', '.cjs']


class JavaScript(Node):
    pass


class TypeScript(Node):
    pass


class PHP(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'composer.lock',
            'composer.json',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.LARAVEL,
            Framework.SYMFONY,
        ]

    def get_source_extensions(self) -> list[str]:
        return ['.php']


def _union[T](lists: Iterable[Iterable[T]]) -> list[T]:
    """Concatenate, dropping repeats, keeping first-seen order."""
    seen: dict[T, None] = {}
    for items in lists:
        for item in items:
            seen.setdefault(item, None)
    return list(seen)


class Other(BaseLanguage):
    """Every ecosystem at once, for repositories of no pipeline language.

    GitHub's primary language says nothing about which manifests a
    repository ships — mathesar is "Svelte" and depends on Django — so
    this lane asks for all of them. A manifest that is absent costs one
    404 from raw.githubusercontent.com, not an API call.
    """

    @staticmethod
    def _handlers() -> list[BaseLanguage]:
        return [
            LanguageFactory.get_handler(language)
            for language in Language
            if language is not Language.OTHER
        ]

    def get_sbom_paths(self) -> list[str]:
        return _union([h.get_sbom_paths() for h in self._handlers()])

    def get_frameworks(self) -> list[Framework]:
        return _union([h.get_frameworks() for h in self._handlers()])

    def get_source_extensions(self) -> list[str]:
        return _union([h.get_source_extensions() for h in self._handlers()])


class LanguageFactory:
    _MAPPING = {
        Language.GO: lambda: Go(),
        Language.PYTHON: lambda: Python(),
        Language.JAVA: lambda: Java(),
        Language.RUST: lambda: Rust(),
        Language.RUBY: lambda: Ruby(),
        Language.NODE: lambda: Node(),
        Language.PHP: lambda: PHP(),
        Language.JAVASCRIPT: lambda: JavaScript(),
        Language.TYPESCRIPT: lambda: TypeScript(),
        Language.OTHER: lambda: Other(),
    }

    @staticmethod
    def get_handler(language: Language) -> BaseLanguage:
        handler_cls = LanguageFactory._MAPPING.get(language)
        if handler_cls:
            return handler_cls()
        else:
            raise ValueError(f"Unsupported language: {language}")
