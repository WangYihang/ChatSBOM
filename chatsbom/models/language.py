from abc import ABC
from abc import abstractmethod
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


class Go(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'go.mod',
            'go.sum',
            'vendor/modules.txt',
            'gopkg.toml',
            'gopkg.lock',
            'glide.yaml',
            'glide.lock',
        ]

    def get_frameworks(self) -> list[Framework]:
        return [
            Framework.GIN,
            Framework.ECHO,
        ]


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
    }

    @staticmethod
    def get_handler(language: Language) -> BaseLanguage:
        handler_cls = LanguageFactory._MAPPING.get(language)
        if handler_cls:
            return handler_cls()
        else:
            raise ValueError(f"Unsupported language: {language}")
