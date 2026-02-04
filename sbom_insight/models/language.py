from abc import ABC
from abc import abstractmethod
from enum import Enum


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
        pass


class Go(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'go.mod',
            'go.sum',
            'vendor/modules.txt',
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
        ]


class Java(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'pom.xml',
            'build.gradle',
            'build.gradle.kts',
        ]


class Rust(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'Cargo.toml',
            'Cargo.lock',
        ]


class Ruby(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'Gemfile',
            'Gemfile.lock',
        ]


class Node(BaseLanguage):
    def get_sbom_paths(self) -> list[str]:
        return [
            'package-lock.json',
            'yarn.lock',
            'pnpm-lock.yaml',
            'package.json',
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
